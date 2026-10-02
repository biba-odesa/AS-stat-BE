#!/usr/bin/env python3
"""Fault tests use isolated directories and a local HTTP endpoint, never VictoriaMetrics."""
import http.server
import json
import pathlib
import socket
import subprocess
import sys
import tempfile
import threading
import time
import unittest

DRIVER = str(pathlib.Path(sys.argv.pop(1)).resolve())

class Server(http.server.ThreadingHTTPServer):
    daemon_threads = True

class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_POST(self):
        body = self.rfile.read(int(self.headers['Content-Length']))
        self.server.bodies.append(body)
        if self.server.mode == 'ambiguous':
            self.server.mode = 'ok'
            self.connection.shutdown(socket.SHUT_RDWR)
            self.connection.close()
            return
        if self.server.mode == 'slow':
            time.sleep(1)
            return
        status = {'ok': 204, 'unavailable': 503, 'permanent': 400}[self.server.mode]
        self.send_response(status)
        self.send_header('Content-Length', '0')
        self.end_headers()

class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = pathlib.Path(self.tmp.name)
        self.server = Server(('127.0.0.1', 0), Handler)
        self.server.mode = 'ok'
        self.server.bodies = []
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.url = f'http://127.0.0.1:{self.server.server_port}'

    def tearDown(self):
        self.path.chmod(0o700)
        self.server.shutdown()
        self.server.server_close()
        self.thread.join()
        self.tmp.cleanup()

    def run_driver(self, mode='normal'):
        p = subprocess.run([DRIVER, str(self.path), self.url, mode], capture_output=True, text=True, timeout=5)
        self.assertTrue(p.stdout, p.stderr)
        return p.returncode, json.loads(p.stdout)

    def test_expired_retained_without_post(self):
        code, stats = self.run_driver('expired')
        self.assertNotEqual(code, 0)
        self.assertEqual(stats['spool']['expired_batches'], 3)
        self.assertEqual(len(list(self.path.glob('*.ready'))), 3)
        self.assertFalse(self.server.bodies)

    def test_file_count_limit(self):
        self.server.mode = 'unavailable'
        code, stats = self.run_driver('files')
        self.assertNotEqual(code, 0)
        self.assertEqual(stats['spool']['lost_rows'], 4)
        self.assertEqual(len(list(self.path.glob('*.ready'))), 1)

    def test_recovery_while_running(self):
        self.server.mode = 'unavailable'
        timer = threading.Timer(0.25, lambda: setattr(self.server, 'mode', 'ok'))
        timer.start()
        try:
            code, stats = self.run_driver('wait')
        finally:
            timer.join()
        self.assertEqual(code, 0)
        self.assertEqual(stats['spool']['sent_rows'], 6)
        self.assertGreater(stats['spool']['http_errors'], 0)
        self.assertEqual(stats['spool']['pending_batches'], 0)

    def test_timeout_and_bounded_shutdown(self):
        self.server.mode = 'slow'
        start = time.monotonic()
        _, stats = self.run_driver()
        self.assertLess(time.monotonic() - start, 2)
        self.assertTrue(list(self.path.glob('*.ready')))
        self.assertGreater(stats['spool']['http_errors'], 0)

    def test_normal(self):
        code, stats = self.run_driver()
        self.assertEqual(code, 0)
        self.assertEqual(stats['spool']['sent_rows'], 6)
        self.assertEqual(stats['partial_rows_skipped'], 1)
        self.assertEqual(stats['spool']['pending_batches'], 0)
        rows = [json.loads(line) for b in self.server.bodies for line in b.splitlines()]
        self.assertEqual(len(rows), 6)
        self.assertEqual(sum(r['values'][0] for r in rows), 615)
        self.assertTrue(all(r['metric']['__name__'] == 'asstat_traffic_bytes' for r in rows))
        self.assertTrue(all(set(r['metric']) == {'__name__', 'link_id', 'asn', 'direction', 'ip_version'} for r in rows))
        self.assertFalse(list(self.path.glob('*.ready')))

    def test_unavailable_and_recovery(self):
        self.server.mode = 'unavailable'
        _, stats = self.run_driver()
        self.assertEqual(stats['spool']['saved_rows'], 6)
        self.assertGreater(stats['spool']['pending_batches'], 0)
        ready = {p.name: p.read_bytes() for p in self.path.glob('*.ready')}
        self.assertTrue(ready)
        self.server.mode = 'ok'
        code, recovered = self.run_driver('recover')
        self.assertEqual(code, 0)
        self.assertEqual(recovered['spool']['sent_rows'], 6)
        self.assertFalse(list(self.path.glob('*.ready')))

    def test_ambiguous_acceptance_retries_identical_body(self):
        self.server.mode = 'ambiguous'
        code, stats = self.run_driver()
        self.assertEqual(code, 0)
        self.assertGreater(stats['spool']['retries'], 0)
        self.assertEqual(self.server.bodies.count(self.server.bodies[0]), 2)
        logical = {}
        for b in self.server.bodies:
            for line in b.splitlines():
                r = json.loads(line)
                k = tuple(sorted(r['metric'].items())), r['timestamps'][0]
                self.assertIn(logical.get(k, r['values'][0]), [r['values'][0]])
                logical[k] = r['values'][0]
        self.assertEqual(len(logical), 6)
        self.assertEqual(sum(logical.values()), 615)

    def test_exclusive_sender(self):
        p = subprocess.Popen([DRIVER, str(self.path), self.url, 'hold'], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        try:
            self.assertEqual(p.stdout.readline().strip(), 'ready')
            q = subprocess.run([DRIVER, str(self.path), self.url, 'recover'], capture_output=True, text=True, timeout=5)
            self.assertNotEqual(q.returncode, 0)
            self.assertIn('locked', q.stderr)
        finally:
            p.communicate('\n', timeout=5)

    def test_corrupt_and_unfinished_preserved(self):
        for name in ('broken.ready', 'interrupted.tmp'):
            (self.path / name).write_text('invalid')
        code, stats = self.run_driver('recover')
        self.assertNotEqual(code, 0)
        self.assertEqual(stats['spool']['corrupt_files'], 1)
        self.assertEqual(stats['spool']['unfinished_files'], 1)
        self.assertTrue((self.path / 'broken.ready').exists())
        self.assertTrue((self.path / 'interrupted.tmp').exists())
        self.assertFalse(self.server.bodies)

    def test_overflow_preserves_previous_batches(self):
        self.server.mode = 'unavailable'
        code, stats = self.run_driver('overflow')
        self.assertNotEqual(code, 0)
        self.assertGreater(stats['spool']['lost_rows'], 0)
        self.assertGreater(stats['spool']['overflow_batches'], 0)
        self.assertTrue(list(self.path.glob('*.ready')))
        self.assertLessEqual(stats['spool']['pending_bytes'], 1024)

    def test_disk_error(self):
        code, stats = self.run_driver('disk')
        self.assertNotEqual(code, 0)
        self.assertEqual(stats['spool']['lost_rows'], 6)
        self.assertGreater(stats['spool']['disk_errors'], 0)

    def test_queue_limit(self):
        code, stats = self.run_driver('queue')
        self.assertNotEqual(code, 0)
        self.assertGreater(stats['lost_before_spool_rows'], 0)
        self.assertLessEqual(stats['peak_queue_records'], 1)

    def test_permanent_error_preserved_without_fast_retry(self):
        self.server.mode = 'permanent'
        code, stats = self.run_driver()
        self.assertNotEqual(code, 0)
        self.assertEqual(stats['spool']['permanent_errors'], 3)
        self.assertEqual(len(self.server.bodies), 3)
        self.assertEqual(len(list(self.path.glob('*.ready'))), 3)

if __name__ == '__main__':
    unittest.main()
