#!/usr/bin/env python3
"""Bounded follow-up: compare the first naturally full window of each archive with raw minutes."""
import argparse,json,pathlib,time,sqlite3,collections
from archive_backfill import configuration,export,atomic
p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--boundary',type=int,required=True);p.add_argument('--output',required=True);args=p.parse_args()
global_,archives=configuration(args.config);source=next(a['url'] for a in archives.values() if int(a['interval_seconds'])==60)
checks={name:dict(start=(args.boundary//int(a['interval_seconds'])+1)*int(a['interval_seconds']),interval=int(a['interval_seconds']),status='waiting') for name,a in archives.items()}
# Historical catch-up precedes live output; reserve a bounded four-hour catch-up margin.
deadline=max(row['start']+row['interval'] for row in checks.values())+4*3600
prior=pathlib.Path(args.output)
if prior.exists():
    saved=json.loads(prior.read_text())
    if saved.get('boundary')==args.boundary:
        for name,row in checks.items():
            old=saved.get('checks',{}).get(name,{})
            if old.get('start')==row['start'] and old.get('interval')==row['interval'] and old.get('status') in ('verified','partial_excluded'):
                checks[name]=old
if deadline>time.time()+8*3600:raise RuntimeError('Follow-up exceeds eight-hour bound')
while time.time()<deadline and any(row['status']=='waiting' for row in checks.values()):
    for name,row in checks.items():
        if row['status']!='waiting' or time.time()<row['start']+row['interval']+15:continue
        state=pathlib.Path(global_['archive_state_directory'])/name/'archives.sqlite'
        with sqlite3.connect(f'file:{state}?mode=ro',uri=True) as db:
            if db.execute("SELECT value FROM meta WHERE key='next'").fetchone()[0]<row['start']+row['interval']:continue
            if db.execute('SELECT 1 FROM skipped WHERE start=?',(row['start'],)).fetchone():row['status']='partial_excluded';continue
        row.setdefault('visibility_started_epoch',time.time())
        expected=collections.defaultdict(int);points=0
        for minute in range(row['start'],row['start']+row['interval'],60):
            seen=set()
            for key,stamp,value in export(source,minute,minute+60):
                if stamp!=minute*1000 or key in seen:raise RuntimeError('Invalid/duplicate raw minute')
                seen.add(key);expected[key]+=value;points+=1
                if len(expected)>int(global_['archive_max_keys'])//len(archives):raise RuntimeError('Follow-up key limit')
        actual={key:value for key,stamp,value in export(archives[name]['url'],row['start'],row['start']+.001) if stamp==row['start']*1000}
        rounded={key:int(float(value)) for key,value in expected.items()}
        if actual==rounded:row.update(status='verified',source_points=points,points=len(actual),bytes=str(sum(actual.values())))
        elif time.time()>row['visibility_started_epoch']+90:row.update(status='mismatch',expected_points=len(expected),actual_points=len(actual))
    atomic(args.output,dict(boundary=args.boundary,deadline=deadline,checks=checks))
    if any(row['status']=='waiting' for row in checks.values()):time.sleep(15)
for row in checks.values():
    if row['status']=='waiting':row['status']='timeout'
atomic(args.output,dict(boundary=args.boundary,deadline=deadline,checks=checks))
raise SystemExit(0 if all(row['status'] in ('verified','partial_excluded') for row in checks.values()) else 1)
