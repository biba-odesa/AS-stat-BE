#!/usr/bin/env python3
"""Permanent receiver lifecycle and bounded resource monitoring; optional tmpfs state."""
import argparse
import datetime as dt
import fcntl
import json
import logging
from logging.handlers import RotatingFileHandler
import os
import re
from pathlib import Path
import selectors
import signal
import socket
import sqlite3
import subprocess
import time
import urllib.request
import uuid
from zoneinfo import ZoneInfo
import stat

ROOT = Path(os.environ.get("ASSTAT_ROOT", str(Path(__file__).resolve().parents[1])))
STATE = Path(os.environ.get("ASSTAT_MONITOR_DIRECTORY", str(ROOT/"runtime-state")))
CONFIG = Path(os.environ.get("ASSTAT_CONFIG", str(ROOT/"config/asstat.conf")))
BIN = Path(os.environ.get("ASSTAT_BIN", str(ROOT/"build-release")))
SERVICE = os.environ.get("ASSTAT_SERVICE", "asstat-be.service")
MOUNT = os.environ.get("ASSTAT_TMPFS_MOUNT", "")
OPENER = urllib.request.build_opener(urllib.request.ProxyHandler({}))

def atomic(path, value, durable=False):
    temp = path.with_name(path.name+'.'+uuid.uuid4().hex+'.tmp')
    with temp.open('x') as out:
        json.dump(value, out, separators=(',', ':'))
        out.write('\n')
        if durable:
            out.flush();os.fsync(out.fileno())
    if os.getuid() == 0:
        owner = path.parent.stat();os.chown(temp, owner.st_uid, owner.st_gid)
    temp.replace(path)
    if durable:
        fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

def properties(unit):
    keys = ('MainPID', 'ActiveState', 'SubState', 'NRestarts', 'ControlGroup')
    command = ['systemctl', 'show', unit] + [f'--property={k}' for k in keys]
    return dict(line.split('=', 1) for line in subprocess.check_output(command, text=True, timeout=5).splitlines() if '=' in line)

def proc(pid):
    try:
        text = Path(f'/proc/{pid}/stat').read_text()
        parts = text[text.rfind(')')+2:].split()
        return dict(pid=pid, ppid=int(parts[1]), start_ticks=int(parts[19]),
                    cpu_ticks=int(parts[11])+int(parts[12]), rss_bytes=int(parts[21])*os.sysconf('SC_PAGE_SIZE'))
    except (OSError, ValueError, IndexError):
        return None

def runners():
    found = []
    for p in Path('/proc').glob('[0-9]*/cmdline'):
        try:
            first = p.read_bytes().split(b'\0')[0]
            if Path(os.fsdecode(first)).name in ('nf9-receiver', 'nf9-delivery-worker', 'nf9-archive-worker'):
                found.append(int(p.parent.name))
        except OSError:
            pass
    return found

def logger(name, limit=4*1024*1024, backups=4):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    handler = RotatingFileHandler(STATE/(name+'.log'), maxBytes=limit, backupCount=backups)
    handler.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    log.addHandler(handler)
    return log

def run():
    if os.getuid() == 0:
        raise RuntimeError('Receiver wrapper must run as the configured unprivileged user')
    prepare()
    if runners():
        raise RuntimeError("Another receiver/helper is already running")
    log = logger('receiver')
    reports = STATE/'finals';reports.mkdir(mode=0o750, exist_ok=True)
    # Rotate only this launcher's own report namespace, never spool or VM data.
    archives = sorted(reports.glob('final-*.json'))
    for old in archives[:-31]:
        old.unlink()
    run_id = dt.datetime.now(dt.timezone.utc).strftime('%Y%m%dT%H%M%S')+'-'+uuid.uuid4().hex[:12]
    final = reports/f'final-{run_id}.json'
    command = [str(BIN/'nf9-receiver'), '--config', str(CONFIG),
               '--duration', '0', '--output', str(final), '--windows-output', 'none', '--report-interval', '60']
    stopping = False
    child = None
    def stop(sig, frame):
        nonlocal stopping
        stopping = True
        if child and child.poll() is None:
            child.terminate()
    signal.signal(signal.SIGTERM, stop);signal.signal(signal.SIGINT, stop)
    child = subprocess.Popen(command, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    atomic(STATE/'current-run.json', dict(run_id=run_id, wrapper_pid=os.getpid(), receiver_pid=child.pid,
           started_epoch=time.time(), started_utc=dt.datetime.now(dt.timezone.utc).isoformat(), command=command, final=str(final)))
    atomic(STATE/'latest-diagnostic.json', dict(run_id=run_id, receiver_pid=child.pid, captured_epoch=time.time(), diagnostic=None))
    print(f'AS-stat-BE receiver started pid={child.pid} run={run_id}', flush=True)
    selector = selectors.DefaultSelector();selector.register(child.stderr, selectors.EVENT_READ)
    buffer = bytearray();stop_started = None
    while child.poll() is None or selector.get_map():
        if stopping and stop_started is None:
            stop_started = time.monotonic()
        if stop_started is not None and time.monotonic()-stop_started > 75 and child.poll() is None:
            log.error('Receiver exceeded wrapper drain budget; cgroup cleanup is required')
            break
        for key, event in selector.select(timeout=0.5):
            block = os.read(key.fileobj.fileno(), 65536)
            if not block:
                selector.unregister(key.fileobj);continue
            buffer.extend(block)
            if len(buffer) > 512*1024:
                log.error('Oversized diagnostic discarded');buffer.clear();continue
            while b'\n' in buffer:
                line, _, buffer = buffer.partition(b'\n')
                text = line.decode('utf-8', errors='replace')
                log.info(text)
                try:
                    diagnostic = json.loads(text)
                    if 'sources' in diagnostic and isinstance(diagnostic['sources'], list):
                        atomic(STATE/'latest-diagnostic.json', dict(run_id=run_id, receiver_pid=child.pid,
                               captured_epoch=time.time(), diagnostic=diagnostic))
                except (ValueError, TypeError):
                    pass
        if child.poll() is not None and not selector.get_map():
            break
    selector.close();child.stderr.close()
    code = child.poll()
    if code is None:
        return 1
    if final.exists():
        diagnostic = json.loads(final.read_text())
        atomic(STATE/'latest-diagnostic.json', dict(run_id=run_id, receiver_pid=child.pid,
               captured_epoch=time.time(), diagnostic=diagnostic))
    print(f'AS-stat-BE receiver exited code={code} final={final}', flush=True)
    return code if code >= 0 else 1

def disk(path):
    s = os.statvfs(path)
    return dict(path=path, total_bytes=s.f_blocks*s.f_frsize, free_bytes=s.f_bavail*s.f_frsize)

def storage_size(path):
    # Hourly only; no per-minute recursive traversal of VictoriaMetrics storage.
    try:
        p = subprocess.run(['du', '-sb', path], text=True, capture_output=True, timeout=20)
    except subprocess.TimeoutExpired:
        return dict(error='du timeout', measured_epoch=time.time())
    if p.returncode:
        return dict(error=p.stderr[-512:], measured_epoch=time.time())
    result = dict(apparent_bytes=int(p.stdout.split()[0]), measured_epoch=time.time())
    try:
        allocated = subprocess.run(['du', '-s', '-B1', path], text=True,
                                   capture_output=True, timeout=20)
        if allocated.returncode:
            result['allocated_error'] = allocated.stderr[-512:]
        else:
            result['allocated_bytes'] = int(allocated.stdout.split()[0])
    except subprocess.TimeoutExpired:
        result['allocated_error'] = 'du allocated timeout'
    return result

def sample(baseline=False):
    log = logger('monitor-be', 128*1024, 3)
    globals_, configured_archives = configuration(CONFIG)
    spool_path_config = Path(globals_["spool_directory"])
    now = time.time()
    previous_path = STATE/'previous-sample.json'
    previous = json.loads(previous_path.read_text()) if previous_path.exists() else {}
    boot_id = Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    monotonic = time.monotonic()
    same_boot = previous.get('boot_id') == boot_id
    interval = monotonic-previous.get('monotonic', monotonic) if same_boot else 0
    listing=subprocess.check_output(['systemctl','list-units','--type=service','--all','--no-legend','--no-pager'],text=True,timeout=5)
    vm_units=[line.split()[0] for line in listing.splitlines() if line.split() and 'victoriametrics' in line.split()[0] and line.split()[0].endswith('.service')]
    units = {name:properties(name) for name in [SERVICE]+vm_units}
    processes = {p['pid']:p for path in Path('/proc').glob('[0-9]*/stat') if (p:=proc(int(path.parent.name)))}
    main = int(units[SERVICE].get('MainPID', 0))
    ids = {main} if main else set()
    for _ in range(6):
        ids.update(pid for pid, p in processes.items() if p['ppid'] in ids)
    groups = dict(receiver_pipeline=[processes[pid] for pid in sorted(ids) if pid in processes],
                  victoriametrics=[])
    for unit in vm_units:
        label=unit.removesuffix('.service');pid=int(units[unit].get('MainPID',0))
        groups[label]=[processes[pid]] if pid in processes else []
    old = {(p['pid'],p['start_ticks']):p['cpu_ticks'] for group in previous.get('processes', {}).values() for p in group} if same_boot else {}
    cpu = {}
    hz = os.sysconf('SC_CLK_TCK')
    for name, group in groups.items():
        delta = sum(p['cpu_ticks']-old[(p['pid'],p['start_ticks'])] for p in group if (p['pid'],p['start_ticks']) in old)
        new = [p['pid'] for p in group if (p['pid'],p['start_ticks']) not in old]
        departed = [p['pid'] for p in previous.get('processes',{}).get(name,[]) if (p['pid'],p['start_ticks']) not in {(x['pid'],x['start_ticks']) for x in group}] if same_boot else []
        cpu[name] = dict(cpu_seconds_delta=delta/hz if interval>0 else None,
                         percent_one_core=delta/hz/interval*100 if interval>0 else None,
                         rss_bytes=sum(p['rss_bytes'] for p in group), new_pids=new, departed_pids=departed,
                         complete_interval=bool(interval>0 and not new and not departed))
    capture = json.loads((STATE/'latest-diagnostic.json').read_text()) if (STATE/'latest-diagnostic.json').exists() else {}
    diagnostic = capture.get('diagnostic') or {}
    sources = []
    same_run = previous.get('run_id') == capture.get('run_id') and interval>0
    old_sources = {s['name']:s for s in previous.get('sources',[])}
    for source in diagnostic.get('sources',[]):
        item = {k:source[k] for k in ('name','port','last_allowed_received_ns','received','decoded','queue','cache_entries','ifindex','worker_failed')}
        before = old_sources.get(source['name'])
        if same_run and before:
            def rate(group, key):
                return max(0,source[group][key]-before[group][key])/interval
            item['datagrams_per_second_interval'] = rate('received','datagrams')
            item['records_per_second_interval'] = rate('decoded','data_records')
        else:
            item['datagrams_per_second_interval'] = None
            item['records_per_second_interval'] = None
        sources.append(item)
    spool_entries = []
    paths=[spool_path_config]
    for entry in spool_path_config.iterdir():
        if entry.is_dir() and not entry.is_symlink():paths.append(entry)
    for spool_path in paths:
      with os.scandir(spool_path) as entries:
        for entry in entries:
            if not entry.is_file(follow_symlinks=False):continue
            if entry.name == '.lock':
                continue
            try:
                spool_entries.append(entry.stat(follow_symlinks=False).st_size)
            except FileNotFoundError:
                pass
    prior_size = previous.get('vm_storage_size', {})
    size = prior_size
    archive_storage={}
    for name,archive in configured_archives.items():
        prior=previous.get('archive_storage',{}).get(name,{})
        if not baseline and now-prior.get('measured_epoch',0)<3600:
            archive_storage[name]=prior;continue
        try:
            with OPENER.open(archive['url']+'/flags',timeout=3) as response:flags=response.read(65536).decode()
            value=re.search(r'^-storageDataPath=(".*")$',flags,re.M)
            if not value:raise ValueError('Storage path not exposed')
            path=json.loads(value.group(1));archive_storage[name]=dict(path=path,**storage_size(path),filesystem=disk(path))
        except (OSError,ValueError) as error:archive_storage[name]=dict(error=type(error).__name__,measured_epoch=now)
    aggregation = diagnostic.get('aggregation', {}).copy()
    if 'recent_windows' in aggregation:
        aggregation['recent_windows'] = aggregation['recent_windows'][-5:]
    row = dict(timestamp_epoch=now, utc=dt.datetime.fromtimestamp(now,dt.timezone.utc).isoformat(),
               boot_id=boot_id, monotonic=monotonic, interval_seconds=interval, units=units, processes=groups,
               cpu=cpu, run_id=capture.get('run_id'), diagnostic_age_seconds=now-capture.get('captured_epoch',now),
               sources=sources, aggregation=aggregation, delivery=diagnostic.get('delivery',{}),
               diagnostic_output=diagnostic.get('diagnostic_output',{}),
               archives=diagnostic.get('archives',{}),archive_storage=archive_storage,
               spool=dict(files=len(spool_entries),bytes=sum(spool_entries)),
               filesystems=[disk(str(spool_path_config))] + ([disk(MOUNT)] if MOUNT else []), tmpfs=(disk(MOUNT) if MOUNT else None), vm_storage_size=archive_storage.get('minute',size))
    atomic(previous_path, row)
    atomic(STATE/'latest-sample.json', row)
    db = sqlite3.connect(STATE/'measurements.sqlite', timeout=5)
    if os.getuid() == 0:
        owner=STATE.stat();os.chown(STATE/'measurements.sqlite',owner.st_uid,owner.st_gid)
    try:
        db.execute('PRAGMA journal_mode=DELETE');db.execute('PRAGMA max_page_count=32768')
        db.execute('CREATE TABLE IF NOT EXISTS samples(id INTEGER PRIMARY KEY,ts REAL,payload TEXT)')
        db.execute('DELETE FROM samples WHERE id NOT IN (SELECT id FROM samples ORDER BY id DESC LIMIT 10999)')
        db.execute('INSERT INTO samples(ts,payload) VALUES (?,?)',(now,json.dumps(row,separators=(',',':'))));db.commit()
    except sqlite3.Error as error:
        log.error('Measurement database error: %s',error)
        raise
    finally:
        db.close()
    if baseline:
        atomic(STATE/'baseline.json', row, durable=True)
    return 0

def status():
    row = json.loads((STATE/'latest-sample.json').read_text())
    sources = []
    for s in row.get('sources',[]):
        sources.append(dict(port=s['port'], datagrams_per_second=s.get('datagrams_per_second_interval'),
            records_per_second=s.get('records_per_second_interval'), socket_drops=s['received']['socket_drops'],
            queue_drops=s['received']['queue_drops'], unknown_templates=s['decoded']['unknown_template_flowsets'],
            format_errors=sum(s['decoded'][k] for k in ('malformed_datagrams','malformed_data_flowsets','malformed_template_flowsets'))))
    print(json.dumps(dict(mode="permanent", last_sample=row['utc'], age_seconds=time.time()-row['timestamp_epoch'],
        run_id=row.get('run_id'), units_at_last_sample=row['units'], cpu=row['cpu'], sources=sources,
        spool=row['spool'], delivery=row.get('delivery'), archives=row.get('archives',{}), tmpfs=row.get('tmpfs'), filesystems=row['filesystems'], vm_storage_size=row['vm_storage_size']),indent=2))
    return 0


def configuration(path):
    """Read deployment paths after the native parser validates the fixed config."""
    globals_, archives = {}, {};current=globals_
    for raw in Path(path).read_text().splitlines():
        line=raw.strip()
        if not line or line.startswith('#'):continue
        if line.startswith('[archive '):
            name=line[9:-1].strip();current={};archives[name]=current
        else:
            key,value=line.split('=',1);current[key.strip()]=value.strip()
    return globals_,archives


def check_mount(path):
    result=subprocess.run(['findmnt','-M',str(path),'-J','-o','TARGET,FSTYPE,OPTIONS'],capture_output=True,text=True,check=True)
    mounts=json.loads(result.stdout).get('filesystems',[])
    if len(mounts)!=1 or Path(mounts[0]['target']).resolve()!=Path(path).resolve() or mounts[0]['fstype']!='tmpfs' or 'rw' not in mounts[0]['options'].split(','):
        raise RuntimeError('Required mount must be a writable tmpfs at the exact mountpoint')
    owner=Path(path).lstat()
    if stat.S_ISLNK(owner.st_mode) or owner.st_uid!=os.geteuid() or stat.S_IMODE(owner.st_mode)!=0o750:
        raise RuntimeError('tmpfs mountpoint requires service ownership and mode 0750')


def safe_directory(path):
    path=Path(path)
    if not path.exists():
        if not path.parent.exists():safe_directory(path.parent)
        path.mkdir(mode=0o750)
    value=path.lstat()
    if not stat.S_ISDIR(value.st_mode) or value.st_uid!=os.geteuid() or stat.S_IMODE(value.st_mode)!=0o750:
        raise RuntimeError('Unsafe directory ownership/type/mode: '+str(path))


def prepare():
    """Initialize genuinely empty archives; preserve or reject every existing state."""
    if MOUNT:check_mount(MOUNT)
    subprocess.run([str(BIN/'nf9-receiver'),'--config',str(CONFIG),'--check-config'],check=True,stdout=subprocess.DEVNULL,timeout=10)
    globals_,archives=configuration(CONFIG)
    if not archives:raise RuntimeError('Permanent archive runtime requires configured archives')
    state=Path(globals_['archive_state_directory']);spool=Path(globals_['spool_directory'])
    for directory in (state,spool):
        if MOUNT:
            base=Path(MOUNT).resolve()
            if not directory.is_absolute() or not directory.resolve().is_relative_to(base) or directory.resolve()==base:
                raise RuntimeError('Archive state and spool must be inside the required tmpfs')
        safe_directory(directory)
    unknown=[p.name for p in state.iterdir() if p.name not in archives]
    if unknown:raise RuntimeError('Unknown saved archive state is preserved: '+str(unknown))
    first=int(time.time())//60*60
    for name,archive in archives.items():
        directory=state/name;safe_directory(directory);safe_directory(spool/name)
        db_path=directory/'archives.sqlite'
        if db_path.exists() or db_path.is_symlink():
            if db_path.is_symlink() or not db_path.is_file():raise RuntimeError('Unsafe archive DB: '+str(db_path))
            db=sqlite3.connect('file:'+str(db_path)+'?mode=ro',uri=True,timeout=1)
            try:
                if db.execute('PRAGMA quick_check').fetchall()!=[('ok',)]:raise RuntimeError('Archive integrity check failed')
                if db.execute("SELECT value FROM meta WHERE key='version'").fetchone()!=(1,):raise RuntimeError('Incompatible archive schema')
                if db.execute('SELECT name,url,interval FROM archives').fetchall()!=[(name,archive['url'],int(archive['interval_seconds']))]:raise RuntimeError('Saved archive endpoint/interval mismatch')
                cursor=db.execute("SELECT value FROM meta WHERE key='next'").fetchone()
                if not cursor or cursor[0]%60:raise RuntimeError('Invalid saved cursor')
            finally:db.close()
        else:
            if list(directory.iterdir()) or list((spool/name).iterdir()):raise RuntimeError('Nonempty state/spool without DB; manual recovery required')
            subprocess.run([str(BIN/'nf9-archive-worker'),'--init-current',str(CONFIG),name,str(first)],check=True,stdout=subprocess.DEVNULL,timeout=10)
    safe_directory(STATE)


def main():
    os.umask(0o027)
    parser=argparse.ArgumentParser();parser.add_argument('mode',choices=['prepare','run','sample','status']);args=parser.parse_args()
    if args.mode=='prepare':prepare();return 0
    if args.mode=='run':return run()
    if args.mode=='sample':return sample()
    return status()


if __name__=='__main__':
    raise SystemExit(main())
