#!/usr/bin/env python3
"""Resumable, bounded raw-minute migration. No proxy, graph selectors or resampling."""
import re
import contextlib,fcntl
import argparse,collections,datetime as dt,hashlib,json,math,os,pathlib,sqlite3,subprocess,time,urllib.parse,urllib.request
class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):raise RuntimeError('Archive API redirects are not allowed')
OPENER=urllib.request.build_opener(urllib.request.ProxyHandler({}),NoRedirect())
MAX128=(1<<128)-1
METRIC='asstat_traffic_bytes'
def configuration(path):
    globals_,archives={},{};current=globals_
    for raw in pathlib.Path(path).read_text().splitlines():
        line=raw.strip()
        if not line or line.startswith('#'):continue
        if line.startswith('[archive '):
            name=line[9:-1].strip();current={};archives[name]=current
        else:
            key,value=line.split('=',1);current[key.strip()]=value.strip()
    return globals_,archives

def database(path,limit):
    db=sqlite3.connect(path,timeout=10);db.execute('PRAGMA foreign_keys=ON');db.execute('PRAGMA synchronous=FULL')
    db.execute(f'PRAGMA max_page_count={limit//db.execute("PRAGMA page_size").fetchone()[0]}')
    return db

def export(url,start,end):
    data=urllib.parse.urlencode({'match[]':METRIC,'start':f'{start:.3f}','end':f'{end-0.001:.3f}'}).encode()
    request=urllib.request.Request(url+'/api/v1/export',data=data)
    with OPENER.open(request,timeout=60) as response:
        while True:
            line=response.readline(16*1024*1024+1)
            if not line:break
            if len(line)>16*1024*1024:raise RuntimeError('Raw export line exceeds bounded 16 MiB')
            row=json.loads(line);labels=row['metric']
            if set(labels)!={'__name__','link_id','asn','direction','ip_version'} or labels['__name__']!=METRIC:raise RuntimeError('Unexpected series identity in raw export')
            key=(labels['link_id'],int(labels['asn']),0 if labels['direction']=='in' else 1,int(labels['ip_version']))
            if labels['direction'] not in ('in','out') or not 0<=key[1]<=0xffffffff or key[3] not in (4,6):raise RuntimeError('Invalid source labels')
            if len(row['values'])!=len(row['timestamps']):raise RuntimeError('Raw export arrays differ')
            for timestamp,value in zip(row['timestamps'],row['values']):
                f=float(value)
                if not math.isfinite(f) or f<0 or f!=math.floor(f) or f>MAX128:raise RuntimeError('Nonintegral/nonfinite/out-of-range raw bytes')
                yield key,int(timestamp),int(f)

def atomic(path,value):
    path=pathlib.Path(path);tmp=path.with_suffix('.tmp')
    with tmp.open('w') as f:json.dump(value,f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
    os.replace(tmp,path)
    fd=os.open(path.parent,os.O_RDONLY|os.O_DIRECTORY);os.fsync(fd);os.close(fd)

def payload(rows):
    return ''.join(f'{k[0]}\t{k[1]}\t{k[2]}\t{k[3]}\t{v}\n' for k,v in sorted(rows.items()))
def canonical(rows,partial):
    return ('partial\n' if partial else 'full\n')+''.join(f'{len(k[0])}:{k[0]}:{k[1]}:{k[2]}:{k[3]}:{v}\n' for k,v in sorted(rows.items()))
def native_cursor(db):return db.execute("SELECT value FROM meta WHERE key='next'").fetchone()[0]
@contextlib.contextmanager
def migration_lock(work):
    fd=os.open(work/'.backfill.lock',os.O_CREAT|os.O_RDWR|os.O_CLOEXEC|os.O_NOFOLLOW,0o600)
    try:
        st=os.fstat(fd)
        if st.st_uid!=os.geteuid() or st.st_nlink!=1 or st.st_mode&0o077:
            raise RuntimeError('Unsafe migration lock ownership or permissions')
        try:fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except BlockingIOError:raise RuntimeError('Another backfill owns this reference directory') from None
        yield
    finally:os.close(fd)

def wait_native(native,target,idle_timeout,pump,report=None,clock=time.monotonic,sleep=time.sleep):
    # Verification releases historical gates even while native processing is
    # backpressured. Track progress independently so one archive cannot hide
    # another archive's stall, and slow advancing cursors remain healthy.
    now=clock();previous={name:native_cursor(db) for name,db in native.items()}
    last_progress={name:now for name in native};last_report=now
    while any(cursor<target for cursor in previous.values()):
        pump()
        now=clock();current={name:native_cursor(db) for name,db in native.items()}
        for name,cursor in current.items():
            if cursor<previous[name]:raise RuntimeError(f'Native cursor moved backwards: {name}')
            if cursor>previous[name]:last_progress[name]=now
        stalled={name:dict(cursor=cursor,target=target,idle_seconds=round(now-last_progress[name],3))
                 for name,cursor in current.items() if cursor<target and now-last_progress[name]>=idle_timeout}
        if report and (stalled or now-last_report>=10):report(current,stalled);last_report=now
        if stalled:raise RuntimeError('Native worker made no cursor progress; prepared state retained: '+json.dumps(stalled,sort_keys=True))
        previous=current
        if any(cursor<target for cursor in current.values()):sleep(.05)

def stage(db,minute,partial,text):
    with db:
        cursor=native_cursor(db)
        if minute<cursor:
            old=db.execute('SELECT canonical FROM receipts WHERE minute=?',(minute,)).fetchone()
            if old:
                rows={tuple([x[0],int(x[1]),int(x[2]),int(x[3])]):int(x[4]) for x in (line.split('\t') for line in text.splitlines())}
                if old[0]!=canonical(rows,partial):raise RuntimeError('Conflicting already processed migration minute')
            return
        old=db.execute('SELECT partial,payload FROM inbox WHERE minute=?',(minute,)).fetchone()
        if old and old!=(int(partial),text):raise RuntimeError('Conflicting immutable migration inbox')
        if not old:db.execute('INSERT INTO inbox VALUES(?,?,?)',(minute,int(partial),text))

def checked_add(a,b):
    n=int(a)+int(b)
    if n>MAX128:raise OverflowError('uint128 backfill sum overflow')
    return str(n)

def run(args):
    work=pathlib.Path(args.work)
    if (work/"cancelled.json").exists():raise RuntimeError("Historical backfill cancelled administratively; reference preserved")
    work.mkdir(mode=0o700,parents=True,exist_ok=True)
    with migration_lock(work):return migrate(args)

def migrate(args):
    global_,configs=configuration(args.config);names=args.archives.split(',')
    if len(names)!=len(set(names)) or any(n not in configs for n in names):raise RuntimeError('Invalid selected archive names')
    if not re.fullmatch(r'http://127\.0\.0\.1:[0-9]{1,5}',args.source_url):raise RuntimeError('Source URL must be a local literal HTTP endpoint')
    with OPENER.open(args.source_url+'/flags',timeout=5) as response:flags=response.read(65536).decode()
    match=re.search(r'^-retentionPeriod="([0-9]+)d"$',flags,re.M)
    if not match:raise RuntimeError('Cannot establish source retention for history bounds')
    if args.start<time.time()-int(match.group(1))*86400:raise RuntimeError('Requested history precedes source retention; timestamps are not shifted')
    state=pathlib.Path(global_.get('archive_state_directory','/var/lib/asstat/archives'))
    limit=int(global_.get('archive_state_max_bytes','1073741824'))//(len(configs)+1)
    native={name:database(state/name/'archives.sqlite',limit) for name in names}
    cutoffs={name:db.execute("SELECT value FROM meta WHERE key='history_boundary'").fetchone()[0] for name,db in native.items()}
    if len(set(cutoffs.values()))!=1 or max(cutoffs.values())>time.time()+60:raise RuntimeError('Workers must activate one finite backfill/live boundary first')
    cutoff=next(iter(cutoffs.values()))
    start=args.start
    coverage=json.loads(pathlib.Path(args.coverage).read_text())
    intervals=coverage['complete_intervals']
    work=pathlib.Path(args.work)
    if (work/"cancelled.json").exists():raise RuntimeError("Historical backfill cancelled administratively; reference preserved")
    work.mkdir(mode=0o700,parents=True,exist_ok=True)
    db=database(work/'reference.sqlite',limit);db.create_function('u128_add',2,checked_add)
    db.executescript('''CREATE TABLE IF NOT EXISTS settings(key TEXT PRIMARY KEY,value TEXT);
    CREATE TABLE IF NOT EXISTS jobs(minute INTEGER PRIMARY KEY,partial INTEGER,payload TEXT);
    CREATE TABLE IF NOT EXISTS active(archive TEXT,start INTEGER,link TEXT,asn INTEGER,direction INTEGER,family INTEGER,bytes TEXT,PRIMARY KEY(archive,link,asn,direction,family));
    CREATE TABLE IF NOT EXISTS windows(archive TEXT PRIMARY KEY,start INTEGER,partial INTEGER);
    CREATE TABLE IF NOT EXISTS expected(archive TEXT,start INTEGER,link TEXT,asn INTEGER,direction INTEGER,family INTEGER,bytes TEXT,PRIMARY KEY(archive,start,link,asn,direction,family));
    CREATE TABLE IF NOT EXISTS closed(archive TEXT,start INTEGER,end INTEGER,partial INTEGER,PRIMARY KEY(archive,start));
    CREATE TABLE IF NOT EXISTS audit(archive TEXT,start INTEGER,points INTEGER,digest TEXT,partial INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(archive,start));
    CREATE TABLE IF NOT EXISTS totals(archive TEXT,link TEXT,direction INTEGER,family INTEGER,bytes TEXT,points INTEGER,PRIMARY KEY(archive,link,direction,family));''')
    if 'partial' not in [r[1] for r in db.execute('PRAGMA table_info(audit)')]:
        db.execute('ALTER TABLE audit ADD COLUMN partial INTEGER NOT NULL DEFAULT 0');db.commit()
    contract=json.dumps(dict(names=names,configs={n:configs[n] for n in names},cutoff=cutoff,start=start,source=args.source_url,coverage=coverage),sort_keys=True)
    prior=db.execute("SELECT value FROM settings WHERE key='contract'").fetchone()
    if prior and prior[0]!=contract:raise RuntimeError('Migration contract changed; existing reference is preserved')
    if not prior:
        with db:
            db.execute('INSERT INTO settings VALUES(?,?)',('contract',contract));db.execute('INSERT INTO settings VALUES(?,?)',('next',str(start)))
            for name in names:
                i=int(configs[name]['interval_seconds']);db.execute('INSERT INTO windows VALUES(?,?,?)',(name,start//i*i,int(start%i!=0)))
    def summary(status):
        value=dict(status=status,start=start,boundary=cutoff,next=int(db.execute("SELECT value FROM settings WHERE key='next'").fetchone()[0]),archives={})
        for name in names:
            q=db.execute('SELECT coalesce(sum(points),0),sum(CASE WHEN partial=0 AND points>0 THEN 1 ELSE 0 END),sum(partial),sum(CASE WHEN partial=0 AND points=0 THEN 1 ELSE 0 END) FROM audit WHERE archive=?',(name,)).fetchone()
            value['archives'][name]=dict(verified_points=q[0],verified_windows=q[1] or 0,excluded_partial_windows=q[2] or 0,covered_empty_windows=q[3] or 0,native_next=native_cursor(native[name]),remaining_prepared=db.execute('SELECT count(*) FROM expected WHERE archive=?',(name,)).fetchone()[0],group_sums=[dict(link_id=l,direction=('out' if d else 'in'),ip_version=f,bytes=b,points=p) for l,d,f,b,p in db.execute('SELECT link,direction,family,bytes,points FROM totals WHERE archive=? ORDER BY link,direction,family',(name,))])
        atomic(work/'progress.json',value)
    def verify_closed(wait=False):
        pending=db.execute('SELECT archive,start,end,partial FROM closed ORDER BY start,archive').fetchall()
        for name,window,end,partial in pending:
            if native_cursor(native[name])<end:continue
            expected={tuple(k[:4]):int(k[4]) for k in db.execute('SELECT link,asn,direction,family,bytes FROM expected WHERE archive=? AND start=?',(name,window))}
            actual={tuple(k[:4]):int(k[4]) for k in native[name].execute('SELECT o.link,o.asn,o.direction,o.family,o.bytes FROM output o JOIN batches b ON b.id=o.batch WHERE b.start=?',(window,))}
            already=db.execute("SELECT 1 FROM settings WHERE key=?",(f'authorized:{name}:{window}',)).fetchone()
            if not already:
                # Output is held behind the validation gate until this comparison succeeds.
                if expected!=actual:raise RuntimeError(f'Native/reference mismatch for {name} at {window}')
                destination={}
                for key,timestamp,value in export(configs[name]['url'],window,end):
                    if timestamp!=window*1000:raise RuntimeError('Destination timestamp conflict')
                    if key in destination:raise RuntimeError('Duplicate logical destination point')
                    destination[key]=value
                for key,value in expected.items():
                    if key in destination and destination[key]!=int(float(value)):raise RuntimeError(f'Destination value conflict: {name}, {window}')
                with db:db.execute('INSERT OR IGNORE INTO settings VALUES(?,?)',(f'authorized:{name}:{window}','1'))
            if expected:
                with native[name]:
                    native[name].execute('INSERT OR IGNORE INTO validation SELECT id,1 FROM batches WHERE start=?',(window,))
            deadline=time.monotonic()+(args.visibility_timeout if wait else 0)
            visible=False
            while True:
                found={key:value for key,timestamp,value in export(configs[name]['url'],window,window+0.001) if timestamp==window*1000}
                visible=all(found.get(key)==int(float(value)) for key,value in expected.items())
                if visible or time.monotonic()>=deadline:break
                time.sleep(1)
            # One not-yet-visible window must not hold other eligible gates.
            # Its immutable reference stays pending for a later raw read.
            if not visible:continue
            digest=hashlib.sha256(payload(expected).encode()).hexdigest()
            with db:
                db.execute('INSERT OR IGNORE INTO audit VALUES(?,?,?,?,?)',(name,window,len(expected),digest,partial))
                for (link,asn,direction,family),value in expected.items():
                    db.execute('INSERT INTO totals VALUES(?,?,?,?,?,1) ON CONFLICT(archive,link,direction,family) DO UPDATE SET bytes=u128_add(bytes,excluded.bytes),points=points+1',(name,link,direction,family,str(value)))
                db.execute('DELETE FROM expected WHERE archive=? AND start=?',(name,window));db.execute('DELETE FROM closed WHERE archive=? AND start=?',(name,window))
    def send_jobs():
        for minute,partial,text in db.execute('SELECT minute,partial,payload FROM jobs ORDER BY minute').fetchall():
            for name in names:stage(native[name],minute,bool(partial),text)
            with db:db.execute('DELETE FROM jobs WHERE minute=?',(minute,))
    def waiting(current,stalled):
        summary('native_stalled' if stalled else 'waiting_native')
        atomic(work/'native-wait.json',dict(target=wait_target,cursors=current,stalled=stalled,utc=dt.datetime.now(dt.timezone.utc).isoformat()))
    summary('running')
    send_jobs()
    verify_closed()
    wait_target=int(db.execute("SELECT value FROM settings WHERE key='next'").fetchone()[0])-180
    wait_native(native,wait_target,args.visibility_timeout,verify_closed,waiting)
    while (minute:=int(db.execute("SELECT value FROM settings WHERE key='next'").fetchone()[0]))<cutoff:
        rows={};partial=not any(begin<=minute and minute+60<=end for begin,end in intervals)
        for key,timestamp,value in export(args.source_url,minute,minute+60):
            if timestamp!=minute*1000:raise RuntimeError('Minute source contains non-minute timestamp')
            if key in rows:raise RuntimeError('Raw export is not deduplicated')
            if len(rows)>=int(global_.get('max_active_keys','100000')):raise RuntimeError('Minute reference key limit reached')
            rows[key]=value
        text=payload(rows)
        if len(text.encode())>int(global_.get('archive_queue_bytes','67108864'))//len(configs):raise RuntimeError('Minute payload exceeds aggregate archive memory budget')
        with db:
            db.execute('INSERT INTO jobs VALUES(?,?,?)',(minute,int(partial),text))
            for name in names:
                window,incomplete=db.execute('SELECT start,partial FROM windows WHERE archive=?',(name,)).fetchone();incomplete=bool(incomplete or partial);i=int(configs[name]['interval_seconds'])
                db.executemany('INSERT INTO active VALUES(?,?,?,?,?,?,?) ON CONFLICT(archive,link,asn,direction,family) DO UPDATE SET bytes=u128_add(bytes,excluded.bytes)',[(name,window,*key,str(value)) for key,value in rows.items()])
                if minute+60==window+i:
                    db.execute('INSERT INTO closed VALUES(?,?,?,?)',(name,window,window+i,int(incomplete)))
                    if not incomplete:db.execute('INSERT INTO expected SELECT * FROM active WHERE archive=?',(name,))
                    db.execute('DELETE FROM active WHERE archive=?',(name,));window+=i;incomplete=False
                db.execute('UPDATE windows SET start=?,partial=? WHERE archive=?',(window,int(incomplete),name))
            db.execute("UPDATE settings SET value=? WHERE key='next'",(str(minute+60),))
        send_jobs()
        # Bound native staging, closed references and HTTP visibility backlog.
        wait_target=minute-120
        wait_native(native,wait_target,args.visibility_timeout,verify_closed,waiting)
        verify_closed()
        if db.execute('SELECT count(*) FROM expected').fetchone()[0]>int(global_.get('archive_outbox_rows','1000000')):
            verify_closed(True)
            if db.execute('SELECT count(*) FROM expected').fetchone()[0]>int(global_.get('archive_outbox_rows','1000000')):raise RuntimeError('Reference outbox limit reached')
        if (minute-start)%300==0:summary('running')
    wait_target=cutoff
    wait_native(native,wait_target,args.visibility_timeout,verify_closed,waiting)
    verify_closed(True)
    if db.execute('SELECT count(*) FROM closed').fetchone()[0]:raise RuntimeError('History verification incomplete; resume with the same command')
    # Crossing windows include native live contributions. Gate them only after a bounded follow-up comparison.
    summary('history_verified_crossing_pending')
    atomic(work/'crossing.json',dict(boundary=cutoff,archives=[dict(name=n,start=db.execute('SELECT start FROM windows WHERE archive=?',(n,)).fetchone()[0],interval=int(configs[n]['interval_seconds'])) for n in names],note='Known restart partial makes crossing windows partial; no full import is allowed. Natural future full windows require the separate bounded live checker.'))
    summary('history_verified')
    print(json.dumps(json.loads((work/'progress.json').read_text()),indent=2))

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--archives',required=True);p.add_argument('--source-url',default='http://127.0.0.1:8428');p.add_argument('--start',type=int,required=True);p.add_argument('--coverage',required=True);p.add_argument('--work',required=True);p.add_argument('--visibility-timeout',type=int,default=90)
    args=p.parse_args()
    if args.start%60:raise ValueError('Start must be minute aligned')
    run(args)
if __name__=='__main__':main()
