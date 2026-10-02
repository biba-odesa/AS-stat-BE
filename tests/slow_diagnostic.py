#!/usr/bin/env python3
"""A full, unread stderr pipe must not prevent UDP reception or signal shutdown."""
from config_fixture import write_config
import os
import json
import socket
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
binary = str(Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory() as tmp:
    root = Path(tmp)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as probe:
        probe.bind(('127.0.0.1', 0))
        port = probe.getsockname()[1]
    write_config(root/'sources',[port],windows_output=str(root/'rows'))
    rd, wr = os.pipe()
    os.set_blocking(wr, False)
    try:
        while True: os.write(wr, b'x'*4096)
    except BlockingIOError: pass
    os.set_blocking(wr, True)
    p = subprocess.Popen([binary, '--config', str(root/'sources'), '--output', str(root/'final'),
        '--duration', '15', '--report-interval', '1'], stderr=wr, stdout=subprocess.DEVNULL)
    try:
        deadline = time.monotonic()+5
        while not (root/'rows').exists():
            assert p.poll() is None and time.monotonic()<deadline
            time.sleep(.02)
        # Version mismatch packets suffice: decoding counters prove the queue is draining.
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as udp:
            for _ in range(30):
                for _ in range(50): udp.sendto(b'\x00\x05', ('127.0.0.1', port))
                time.sleep(.1)
        start = time.monotonic()
        p.send_signal(signal.SIGTERM)
        assert p.wait(timeout=5)==0
        assert time.monotonic()-start<3
        report=json.loads((root/'final').read_text())
        assert report['sources'][0]['decoded']['wrong_version']>1000
        assert report['diagnostic_output']['timed_out']
        assert report['diagnostic_output']['peak_queued']<=8
        assert report['sources'][0]['queue']['current_datagrams']==0
        assert not report['aggregation']['failed']
    finally:
        if p.poll() is None: p.kill(); p.wait()
        os.close(rd); os.close(wr)
print('PASS unread diagnostic sink: continued UDP decoding, bounded queue and shutdown')
