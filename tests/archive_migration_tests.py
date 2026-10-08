#!/usr/bin/env python3
"""Independent history verification, interrupted resume and explicit bootstrap use synthetic data."""
import http.server,json,pathlib,sqlite3,subprocess,sys,threading,time,unittest,urllib.parse,shutil
import archive_integration as fixture
class Handler(fixture.Handler):
    def do_POST(self):
        if self.path.startswith('/api/v1/export'):
            form=urllib.parse.parse_qs(self.rfile.read(int(self.headers['Content-Length'])).decode())
            start=round(float(form['start'][0])*1000);end=round(float(form['end'][0])*1000)
            rows={}
            if getattr(self.server,'source',False):
                for stamp in range((start//60000)*60000,end+1,60000):
                    if start<=stamp<=end:
                        for link,asn,direction,family,value in [('link-a','0','in','4',100),('link-b','65536','out','6',200)]:
                            labels={'__name__':'asstat_traffic_bytes','link_id':link,'asn':asn,'direction':direction,'ip_version':family};rows[(json.dumps(labels,sort_keys=True),stamp)]=(labels,value)
            else:
                hidden=getattr(self.server,'hide_exports_remaining',0)
                if hidden:self.server.hide_exports_remaining=hidden-1
                for row in ([] if hidden else self.server.rows):
                    for stamp,value in zip(row['timestamps'],row['values']):
                        if start<=stamp<=end:rows[(json.dumps(row['metric'],sort_keys=True),stamp)]=(row['metric'],value)
            self.send_response(200);self.end_headers()
            for (_,stamp),(labels,value) in rows.items():self.wfile.write((json.dumps(dict(metric=labels,values=[value],timestamps=[stamp]))+'\n').encode())
        else:super().do_POST()
class MigrationTests(fixture.Tests):
    # Do not repeat base cases in this migration-specific target.
    test_fingerprint_conflict=None
    test_independent_delivery_and_recovery=None
    def setUp(self):
        super().setUp()
        for s,t in self.servers:s.shutdown();s.server_close();t.join()
        new=[]
        for _ in range(5):
            s=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);s.offline=False;s.rows=[];s.source=len(new)==4
            t=threading.Thread(target=s.serve_forever,daemon=True);t.start();new.append((s,t))
        config=self.config.read_text()
        for (old,_),(s,_) in zip(self.servers,new):config=config.replace(str(old.server_port),str(s.server_port))
        self.servers=new;self.config.write_text(config)
        for name in self.names:
            shutil.rmtree(self.path/'state'/name);(self.path/'state'/name).mkdir(mode=0o700)
        for name in self.names:
            mode='--bootstrap' if name!='custom' else '--init-current'
            result=subprocess.run([str(fixture.DRIVER.parent/'nf9-archive-worker'),mode,str(self.config),name,str(self.first)],capture_output=True,text=True)
            self.assertEqual(result.returncode,0,result.stderr)
        self.process=subprocess.Popen([str(fixture.DRIVER),str(self.config),'serve'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        deadline=time.monotonic()+10
        while time.monotonic()<deadline:
            with sqlite3.connect(self.path/'state'/'short'/'archives.sqlite') as db:
                self.cutoff=db.execute("SELECT value FROM meta WHERE key='history_boundary'").fetchone()[0]
            if self.cutoff<time.time()+60:break
            time.sleep(.05)
        else:self.fail('worker activation timed out')
        self.coverage=self.path/'coverage.json';self.coverage.write_text(json.dumps({'complete_intervals':[[self.first,self.cutoff]]}))
        self.command=[sys.executable,str(fixture.ROOT/'scripts/archive_backfill.py'),'--config',str(self.config),'--archives','short,middle,long','--source-url',f'http://127.0.0.1:{new[4][0].server_port}','--start',str(self.first),'--coverage',str(self.coverage),'--work',str(self.path/'reference'),'--visibility-timeout','10']
    def tearDown(self):
        self.process.terminate();out,err=self.process.communicate(timeout=10)
        super().tearDown()
    def test_resume_and_exact_values(self):
        p=subprocess.Popen(self.command,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        deadline=time.monotonic()+15
        while time.monotonic()<deadline:
            progress=self.path/'reference'/'progress.json'
            if progress.exists() and json.loads(progress.read_text())['next']>self.first+300:break
            if p.poll() is not None:break
            time.sleep(.02)
        if p.poll() is None:p.terminate()
        p.communicate(timeout=5)
        p=subprocess.run(self.command,capture_output=True,text=True,timeout=25)
        self.assertEqual(p.returncode,0,p.stderr)
        before=json.loads((self.path/'reference'/'progress.json').read_text())
        self.assertEqual(before['status'],'history_verified')
        self.assertGreater(before['archives']['short']['verified_points'],0)
        self.assertEqual(before['archives']['long']['verified_points'],2)
        p=subprocess.run(self.command,capture_output=True,text=True,timeout=10);self.assertEqual(p.returncode,0,p.stderr)
        after=json.loads((self.path/'reference'/'progress.json').read_text());self.assertEqual(before,after)
        self.assertEqual(sorted(row['values'][0] for row in self.servers[2][0].rows),[12000,24000])
    def test_gate_release_under_outbox_backpressure(self):
        # Delayed export visibility keeps the oldest reference pending while
        # subsequent closed windows fill the deliberately tiny native outbox.
        self.process.terminate();self.process.communicate(timeout=10)
        self.config.write_text(self.config.read_text().replace('archive_outbox_rows = 1000000','archive_outbox_rows = 16'))
        self.process=subprocess.Popen([str(fixture.DRIVER),str(self.config),'serve'],stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        self.servers[0][0].hide_exports_remaining=80
        p=subprocess.run(self.command,capture_output=True,text=True,timeout=25)
        self.assertEqual(p.returncode,0,p.stderr)
        progress=json.loads((self.path/'reference'/'progress.json').read_text())
        self.assertEqual(progress['status'],'history_verified')
        self.assertGreater(progress['archives']['short']['verified_points'],0)
        with sqlite3.connect(self.path/'state'/'short'/'archives.sqlite') as db:
            self.assertGreaterEqual(db.execute("SELECT value FROM meta WHERE key='next'").fetchone()[0],self.cutoff)

    def test_add_archive_preserves_existing_windows(self):
        def snapshot():
            result={}
            for name in self.names:
                with sqlite3.connect(self.path/'state'/name/'archives.sqlite') as db:
                    result[name]=(db.execute('SELECT * FROM archives').fetchall(),db.execute('SELECT * FROM totals').fetchall(),db.execute("SELECT value FROM meta WHERE key='next'").fetchone())
            return result
        before=snapshot()
        self.config.write_text(self.config.read_text()+f'\n[archive four_years]\nurl = http://127.0.0.1:{self.servers[4][0].server_port}\ninterval_seconds = 14400\nretention_days = 1461\n')
        (self.path/'state'/'four_years').mkdir(mode=0o700)
        result=subprocess.run([str(fixture.DRIVER.parent/'nf9-archive-worker'),'--bootstrap',str(self.config),'four_years',str(self.first)],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr);self.assertEqual(before,snapshot())
        with sqlite3.connect(self.path/'state'/'four_years'/'archives.sqlite') as db:
            self.assertEqual(db.execute("SELECT value FROM meta WHERE key='bootstrap_pending'").fetchone()[0],1)

    def test_bootstrap_cannot_reset_existing_state(self):
        p=subprocess.run([str(fixture.DRIVER.parent/'nf9-archive-worker'),'--bootstrap',str(self.config),'short',str(self.first)],capture_output=True,text=True)
        self.assertNotEqual(p.returncode,0);self.assertIn('Existing archive state is preserved',p.stderr)
if __name__=='__main__':unittest.main()
