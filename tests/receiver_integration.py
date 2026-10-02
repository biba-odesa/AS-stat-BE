#!/usr/bin/env python3
"""Loopback-only receiver integration; never touches production endpoints."""
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

binary = str(Path(sys.argv[1]).resolve())

def endpoint():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(('127.0.0.1', 0))
    return s

def packet(fid, body, domain=1):
    return struct.pack('!HHIIII', 9, 1, 1000, 2000, 1, domain) + struct.pack('!HH', fid, len(body)+4) + body

def ready(process):
    with selectors.DefaultSelector() as selector:
        selector.register(process.stderr, selectors.EVENT_READ)
        if not selector.select(5):
            raise RuntimeError('Receiver did not become ready')
        line = process.stderr.readline()
        if '"ready"' not in line:
            raise RuntimeError('Startup failed: ' + line)

def run(stop_signal):
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        held = [endpoint(), endpoint()]
        ports = [s.getsockname()[1] for s in held]
        conf = root/'asstat.conf'
        write_config(conf,ports,template_ttl_seconds=1,queue_datagrams=256,queue_memory_bytes=1048576)
        # A busy endpoint must abort the entire startup without starting any receiver.
        busy = subprocess.run([binary, '--config', str(conf), '--output', str(root/'busy.json'), '--duration', '1'],
                              capture_output=True, text=True, timeout=5)
        if busy.returncode == 0 or 'bind ' not in busy.stderr:
            raise RuntimeError('Busy endpoint was accepted')
        for s in held:
            s.close()
        result = root/'final.json'
        process = subprocess.Popen([binary, '--config', str(conf), '--output', str(result), '--duration', '8',
                                    '--warmup', '1', '--report-interval', '1'],
                                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
        try:
            ready(process)
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as allowed, socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as denied:
                allowed.bind(('127.0.0.1', 0)); denied.bind(('127.0.0.2', 0))
                template = struct.pack('!HHHHHH', 900, 2, 60, 1, 10, 4)
                for port in ports:
                    target = ('127.0.0.1', port)
                    denied.sendto(packet(0, template), target)
                    allowed.sendto(packet(0, template), target)
                    allowed.sendto(packet(900, bytes([4, 0, 0, 0, 0])), target)
                    allowed.sendto(packet(900, bytes([6, 0, 0, 0, 0])), target)
                    allowed.sendto(b'\x00\x05', target)
                    allowed.sendto(b'', target)
                time.sleep(1.5)
                for port in ports:
                    allowed.sendto(packet(900, bytes([4, 0, 0, 0, 0])), ('127.0.0.1', port))
                    # Refresh then enqueue a burst; accepted packets must all be drained.
                    allowed.sendto(packet(0, template), ('127.0.0.1', port))
                    for _ in range(200):
                        allowed.sendto(packet(900, bytes([6, 0, 0, 0, 0])), ('127.0.0.1', port))
                time.sleep(0.05)
            process.send_signal(stop_signal)
            _, errors = process.communicate(timeout=5)
            if process.returncode:
                raise RuntimeError(errors)
            data = json.loads(result.read_text())
            assert data['reason'] == signal.Signals(stop_signal).name
            assert len(data['sources']) == 2
            for source in data['sources']:
                rx, dec, q = source['received'], source['decoded'], source['queue']
                assert rx['unexpected_exporters'] == 1
                assert rx['timestamp_fallbacks'] + rx['kernel_timestamps'] == rx['datagrams']
                assert dec['wrong_version'] == 1 and dec['malformed_datagrams'] == 1
                assert dec['template_expiries'] >= 1 and dec['unknown_template_flowsets'] >= 1
                assert dec['ipv4_records'] >= 1 and dec['ipv6_records'] >= 1
                assert q['current_datagrams'] == 0 and q['current_payload_bytes'] == 0
                assert dec['datagrams'] == q['accepted'] == rx['enqueued']
                assert rx['datagrams'] == rx['unexpected_exporters'] + rx['enqueued'] + rx['queue_drops']
                assert source['ifindex']['4']['input_zero'] >= 1 and source['ifindex']['6']['input_zero'] >= 1
                assert source['ifindex']['4']['output_missing'] >= 1 and source['ifindex']['6']['output_missing'] >= 1
                assert source['post_warmup']['decoded']['unknown_template_flowsets'] >= 1
        finally:
            if process.poll() is None:
                process.kill(); process.wait()
        for port in ports:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as check:
                check.bind(('127.0.0.1', port))

run(signal.SIGINT)
run(signal.SIGTERM)
print('PASS loopback allowlist/multiple sources/TTL/ifIndex/signals/drain/port release')
