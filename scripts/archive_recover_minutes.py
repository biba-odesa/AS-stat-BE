#!/usr/bin/env python3
"""Recover only unprocessed missing minutes from immutable peers or verified raw minute storage."""
import argparse,datetime as dt,hashlib,json,pathlib,sqlite3,time
from archive_backfill import configuration,database,migration_lock,native_cursor,stage,export,payload,atomic

def recover(args):
    global_,archives=configuration(args.config);names=[n for n,a in archives.items() if int(a['interval_seconds'])>60]
    minutes=[a for a in archives.values() if int(a['interval_seconds'])==60]
    if len(minutes)!=1:raise RuntimeError('Recovery requires one authoritative minute endpoint')
    budget=int(global_['archive_state_max_bytes'])//(len(archives)+1);root=pathlib.Path(global_['archive_state_directory']);work=pathlib.Path(args.work)
    if (work/"cancelled.json").exists():raise RuntimeError("Historical recovery cancelled by user; saved state preserved")
    with migration_lock(work):
        ref=database(work/'reference.sqlite',budget)
        ref.executescript('''CREATE TABLE IF NOT EXISTS recovery_inputs(minute INTEGER PRIMARY KEY,partial INTEGER,payload TEXT,digest TEXT,evidence TEXT);
        CREATE TABLE IF NOT EXISTS recovery_acks(archive TEXT,minute INTEGER,digest TEXT,PRIMARY KEY(archive,minute));''')
        native={n:database(root/n/'archives.sqlite',budget) for n in names}
        minute_name=next(n for n,a in archives.items() if int(a['interval_seconds'])==60)
        authority=database(root/minute_name/'archives.sqlite',budget)
        final=json.loads(pathlib.Path(args.final_diagnostic).read_text());final=final.get('diagnostic',final)
        delivery=final['archives'][minute_name]['worker']['delivery'];spool=delivery['spool']
        if final['archives'][minute_name]['lost_minutes'] or delivery['unconfirmed_rows'] or delivery['lost_before_spool_rows'] or delivery['accepted_rows']!=spool['sent_rows'] or spool['pending_batches']:
            raise RuntimeError('Minute delivery before restart is not confirmed; cannot infer raw-export completeness')
        recent={r['window_start']:r for r in final['aggregation']['recent_windows']}
        report=dict(status="running",start=args.start,end=args.end,restored=[],marked_partial=[],existing=[],utc=dt.datetime.now(dt.timezone.utc).isoformat())
        for m in range(args.start,args.end,60):
            targets=[n for n in names if m>=native_cursor(native[n]) and not native[n].execute('SELECT 1 FROM inbox WHERE minute=?',(m,)).fetchone()]
            if not targets:continue
            saved=ref.execute('SELECT partial,payload,digest,evidence FROM recovery_inputs WHERE minute=?',(m,)).fetchone()
            if saved:partial,text,digest,evidence=saved;evidence=json.loads(evidence)
            else:
                donors={n:db.execute('SELECT partial,payload FROM inbox WHERE minute=?',(m,)).fetchone() for n,db in native.items()};donors={n:row for n,row in donors.items() if row}
                if donors:
                    flags={row[0] for row in donors.values()}
                    if len(flags)!=1:raise RuntimeError(f'Peer completeness conflict at {m}')
                    partial=next(iter(flags))
                    texts={row[1] for row in donors.values()}
                    if not partial and len(texts)!=1:raise RuntimeError(f'Peer immutable value conflict at {m}')
                    text=next(iter(texts)) if not partial else ''
                    evidence=dict(method='immutable_peer_inbox',donors=sorted(donors),full=not partial)
                else:
                    if native_cursor(authority)<m+60:raise RuntimeError(f'Minute {m} not yet closed by the authoritative journal')
                    partial=bool(authority.execute('SELECT 1 FROM skipped WHERE start=?',(m,)).fetchone())
                    if partial:text='';evidence=dict(method='authoritative_partial',full=False)
                    else:
                        if m not in recent:
                            count=authority.execute('SELECT count(*) FROM skipped').fetchone()[0]
                            total=authority.execute("SELECT value FROM meta WHERE key='partial_windows'").fetchone()[0]
                            if count!=total or count>=256:raise RuntimeError(f'Completeness history for {m} has been pruned')
                        elif recent[m]['partial']:raise RuntimeError(f'Coverage disagreement for {m}')
                        deadline=time.monotonic()+90;previous=None
                        while True:
                            rows={}
                            for key,stamp,value in export(minutes[0]['url'],m,m+60):
                                if stamp!=m*1000 or key in rows:raise RuntimeError('Invalid or duplicate raw recovery point')
                                rows[key]=value
                                if len(rows)>int(global_['max_active_keys']):raise RuntimeError('Recovery minute exceeds key budget')
                            text=payload(rows);digest=hashlib.sha256(text.encode()).hexdigest()
                            expected=recent.get(m,{}).get('keys')
                            if digest==previous and (expected is None or expected==len(rows)):break
                            if time.monotonic()>deadline:raise RuntimeError(f'Raw minute visibility/completeness timeout: {m}')
                            previous=digest;time.sleep(1)
                        evidence=dict(method='deduplicated_raw_minute_export',full=True,points=len(rows),expected_points=expected,minute_cursor=native_cursor(authority),integer_values_from_storage=True)
                if len(text.encode())>int(global_['archive_queue_bytes'])//len(archives):raise RuntimeError('Recovery minute payload exceeds budget')
                digest=hashlib.sha256(text.encode()).hexdigest()
                with ref:ref.execute('INSERT INTO recovery_inputs VALUES(?,?,?,?,?)',(m,int(partial),text,digest,json.dumps(evidence,sort_keys=True)))
            for name in targets:
                stage(native[name],m,bool(partial),text)
                with ref:ref.execute('INSERT OR IGNORE INTO recovery_acks VALUES(?,?,?)',(name,m,digest))
                entry=dict(archive=name,minute=m,utc=dt.datetime.fromtimestamp(m,dt.timezone.utc).isoformat(),digest=digest,evidence=evidence)
                report['marked_partial' if partial else 'restored'].append(entry)
            atomic(work/'recovery-progress.json',report)
        report['status']='complete'
        atomic(work/'recovery-progress.json',report);print(json.dumps({k:len(v) if isinstance(v,list) else v for k,v in report.items()},indent=2))

def main():
    p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--work',required=True);p.add_argument('--start',type=int,required=True);p.add_argument('--end',type=int,required=True);p.add_argument('--final-diagnostic',required=True);args=p.parse_args()
    if args.start%60 or args.end%60 or args.end<=args.start:raise RuntimeError('Recovery bounds must be ordered UTC minutes')
    recover(args)
if __name__=='__main__':main()
