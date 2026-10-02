#!/usr/bin/env python3
"""Unified-config-only CLI, two routers/ports and independent per-router sampling."""
from config_fixture import write_config
import json
from pathlib import Path
import selectors
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time
binary=str(Path(sys.argv[1]).resolve())
def packet(domain, fid, body):
    return struct.pack('!HHIIII',9,1,1000,2000,1,domain)+struct.pack('!HH',fid,len(body)+4)+body
with tempfile.TemporaryDirectory() as tmp:
    root=Path(tmp)
    held=[socket.socket(socket.AF_INET,socket.SOCK_DGRAM) for _ in range(2)]
    for s in held:s.bind(('127.0.0.1',0))
    ports=[s.getsockname()[1] for s in held]
    conf=root/'asstat.conf'
    write_config(conf,ports,exporters='{'+f'{ports[0]}:127.0.0.1,{ports[1]}:127.0.0.2'+'}',
        samplerate='{127.0.0.1:100,127.0.0.2:125}',
        exported_counters='{127.0.0.1:sampled,127.0.0.2:sampled}',
        windows_output=str(root/'rows.jsonl'),duration=30)
    subprocess.run([binary,'--config',str(conf),'--check-config'],check=True,capture_output=True)
    for s in held:s.close()
    p=subprocess.Popen([binary,'--config',str(conf)],stderr=subprocess.PIPE,stdout=subprocess.DEVNULL)
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(p.stderr,selectors.EVENT_READ)
            assert selector.select(5) and b'ready' in p.stderr.readline()
        for i,port in enumerate(ports):
            with socket.socket(socket.AF_INET,socket.SOCK_DGRAM) as sender:
                sender.bind((f'127.0.0.{i+1}',0))
                for domain in (1,2):
                    fields=[(60,1),(16,4),(17,4),(10,4),(14,4),(1,8)]
                    template=struct.pack('!HH',900,len(fields))+b''.join(struct.pack('!HH',*f) for f in fields)
                    sender.sendto(packet(domain,0,template),('127.0.0.1',port))
                    for family in (4,6):
                        body=struct.pack('!BIIIIQ',family,64512,0,1,1,10)
                        sender.sendto(packet(domain,900,body),('127.0.0.1',port))
        time.sleep(.15)
        p.send_signal(signal.SIGTERM);_,err=p.communicate(timeout=5)
        assert p.returncode==0,err
        final=json.loads((root/'final.json').read_text())
        assert len(final['sources'])==2
        assert [s['decoded']['data_records'] for s in final['sources']]==[4,4]
        assert final['aggregation']['excluded_asn']==8
        totals={}
        for r in map(json.loads,(root/'rows.jsonl').open()):
            assert r['asn']==0 and r['direction']=='out'
            key=(r['link_id'],r['ip_version']);totals[key]=totals.get(key,0)+int(r['bytes'])
        assert totals=={('link',4):2000,('link',6):2000,('second',4):2500,('second',6):2500},totals
    finally:
        if p.poll() is None:p.kill();p.wait()
print('PASS one-config CLI, two listeners, router rates 100/125, both Source IDs/families, private replacement then exclusion')
