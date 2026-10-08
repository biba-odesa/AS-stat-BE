#!/usr/bin/env python3
"""Permanent runtime guards: no fallback disk writes or silent state reset."""
import contextlib,importlib.util,json,os,pathlib,subprocess,tempfile,unittest
from unittest import mock
ROOT=pathlib.Path(__file__).resolve().parents[1]
spec=importlib.util.spec_from_file_location('runtime',ROOT/'scripts/service_runtime.py');runtime=importlib.util.module_from_spec(spec);spec.loader.exec_module(runtime)
class RuntimeGuards(unittest.TestCase):
    def test_reject_wrong_mount_type_and_read_only(self):
        for kind,options in [('ext4','rw'),('tmpfs','ro')]:
            reply=subprocess.CompletedProcess([],0,json.dumps({'filesystems':[{'target':'/tmp','fstype':kind,'options':options}]}),'')
            with mock.patch.object(runtime.subprocess,'run',return_value=reply):
                with self.assertRaises(RuntimeError):runtime.check_mount('/tmp')
    def test_reject_missing_mount(self):
        with mock.patch.object(runtime.subprocess,'run',side_effect=subprocess.CalledProcessError(1,['findmnt'])):
            with self.assertRaises(subprocess.CalledProcessError):runtime.check_mount('/missing')
    def test_create_directory_and_preserve_contents(self):
        with tempfile.TemporaryDirectory() as t:
            d=pathlib.Path(t)/'state';runtime.safe_directory(d);(d/'keep').write_text('immutable');runtime.safe_directory(d)
            self.assertEqual((d/'keep').read_text(),'immutable');self.assertEqual(d.stat().st_mode&0o777,0o750)
    def test_reject_directory_symlink(self):
        with tempfile.TemporaryDirectory() as t:
            link=pathlib.Path(t)/'link';link.symlink_to(t)
            with self.assertRaises(RuntimeError):runtime.safe_directory(link)
    @unittest.skipUnless(os.environ.get('ASSTAT_TEST_TMPFS'),'requires explicit isolated tmpfs test directory')
    def test_bootstrap_restart_and_corruption(self):
        import sqlite3
        mount=pathlib.Path(os.environ['ASSTAT_TEST_TMPFS'])
        with tempfile.TemporaryDirectory(prefix='runtime-test-',dir=mount) as t:
            p=pathlib.Path(t);p.chmod(0o750);config=p/'asstat.conf'
            known=p/'knownlinks';known.write_text('192.0.2.1 1 link-a Example 336699 1\n')
            text=f"""netflow9_ports = 19997
bind_address = 127.0.0.1
exporters = {{19997:192.0.2.1}}
samplerate = {{192.0.2.1:100}}
exported_counters = {{192.0.2.1:sampled}}
knownlinks_file = {known}
replace_asn = none
private_asn_ranges =
exclude_asn =
delivery_enabled = true
archive_state_directory = {p/'state'}
spool_directory = {p/'spool'}
"""
            for name,port,interval in [('minute',19991,60),('week',19992,300),('month',19993,1800),('year',19994,7200)]:
                text+=f'\n[archive {name}]\nurl = http://127.0.0.1:{port}\ninterval_seconds = {interval}\nretention_days = 370\n'
            config.write_text(text)
            with mock.patch.multiple(runtime,MOUNT=str(mount),CONFIG=config,STATE=p/'monitor',BIN=ROOT/'build-release'):
                runtime.prepare();db=p/'state/year/archives.sqlite'
                def snapshot():
                    with contextlib.closing(sqlite3.connect('file:'+str(db)+'?mode=ro',uri=True)) as d:return d.execute('SELECT * FROM meta ORDER BY key').fetchall()
                before=snapshot();runtime.prepare();self.assertEqual(snapshot(),before)
                for run in range(2):
                    result=subprocess.run([str(ROOT/'build-release/nf9-receiver'),'--config',str(config),'--duration','2','--output',str(p/f'final-{run}.json'),'--windows-output','none'],capture_output=True,text=True,timeout=30)
                    self.assertEqual(result.returncode,0,result.stderr[-1500:])
                    runtime.prepare()
                # Restart preserves the original history boundary and committed cursor.
                after=dict(snapshot());self.assertGreaterEqual(after['next'],dict(before)['next'])
                self.assertEqual(after['history_boundary'],dict(before)['history_boundary'])
                # Existing non-SQLite bytes must never be interpreted as empty state.
                db.write_bytes(b'not sqlite');before=db.read_bytes()
                with self.assertRaises((sqlite3.DatabaseError,subprocess.CalledProcessError)):runtime.prepare()
                self.assertEqual(db.read_bytes(),before)
if __name__=='__main__':unittest.main()
