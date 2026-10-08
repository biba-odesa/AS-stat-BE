#!/usr/bin/env python3
"""Archive process integration uses synthetic records and isolated local HTTP servers."""
import http.server,json,pathlib,subprocess,sys,tempfile,threading,unittest,time,sqlite3
DRIVER=pathlib.Path(sys.argv.pop(1)).resolve()
ROOT=pathlib.Path(__file__).resolve().parents[1]
class Handler(http.server.BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_GET(self):
        self.send_response(200);self.end_headers();self.wfile.write(b'-retentionPeriod="370d"\n')
    def do_POST(self):
        data=self.rfile.read(int(self.headers['Content-Length']))
        if not self.server.offline:self.server.rows.extend(json.loads(x) for x in data.splitlines())
        self.send_response(503 if self.server.offline else 204);self.send_header('Content-Length','0');self.end_headers()
class Tests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.path=pathlib.Path(self.temp.name)
        self.servers=[]
        for _ in range(4):
            s=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);s.offline=False;s.rows=[]
            t=threading.Thread(target=s.serve_forever,daemon=True);t.start();self.servers.append((s,t))
        base=(ROOT/'config/asstat.conf.example').read_text().split('[archive ',1)[0]
        if 'archive_state_directory =' in base:base='\n'.join('archive_state_directory = '+str(self.path/'state') if line.strip().startswith('archive_state_directory =') else line for line in base.split('\n'))
        else:base+='\narchive_state_directory = '+str(self.path/'state')+'\n'
        (self.path/'state').mkdir(mode=0o700);(self.path/'spool').mkdir(mode=0o700)
        base=base.replace('spool_directory = /var/spool/asstat','spool_directory = '+str(self.path/'spool'))
        base=base.replace('delivery_shutdown_timeout_ms = 10000','delivery_shutdown_timeout_ms = 700')
        base=base.replace('delivery_http_timeout_ms = 3000','delivery_http_timeout_ms = 100')
        self.names=('short','middle','long','custom')
        for name,interval,(s,_) in zip(self.names,(300,1800,7200,420),self.servers):
            (self.path/'state'/name).mkdir(mode=0o700)
            base+=f'\n[archive {name}]\nurl = http://127.0.0.1:{s.server_port}\ninterval_seconds = {interval}\nretention_days = 370\n'
        self.config=self.path/'config.conf';self.config.write_text(base)
        self.first=int(time.time())//7200*7200-7200
        for name in self.names:
            p=subprocess.run([str(DRIVER.parent/'nf9-archive-worker'),'--init-current',str(self.config),name,str(self.first)],capture_output=True,text=True)
            self.assertEqual(p.returncode,0,p.stderr)
    def tearDown(self):
        for s,t in self.servers:s.shutdown();s.server_close();t.join()
        self.temp.cleanup()
    def run_driver(self,mode='run'):
        p=subprocess.run([str(DRIVER),str(self.config),mode],capture_output=True,text=True,timeout=20)
        self.assertEqual(p.returncode,0,p.stderr+'\n'+p.stdout)
        return json.loads(p.stdout)
    def test_independent_delivery_and_recovery(self):
        self.servers[2][0].offline=True
        stats=self.run_driver()
        for name in self.names:self.assertEqual(stats[name]['lost_minutes'],0)
        self.assertEqual(len(self.servers[0][0].rows),48)
        self.assertEqual(len(self.servers[1][0].rows),8)
        self.assertFalse(self.servers[2][0].rows)
        for s,_ in self.servers:
            for row in s.rows:
                expected={'short':500,'middle':3000,'custom':700}.get(next(name for name,(other,_) in zip(self.names,self.servers) if other is s))
                if row['metric']['link_id']=='link-a':self.assertEqual(row['values'],[expected])
        self.assertTrue(list((self.path/'spool'/'long').glob('*.ready')))
        self.servers[2][0].offline=False
        stats=self.run_driver('recover')
        self.assertEqual(len(self.servers[2][0].rows),2)
        self.assertEqual(sorted(row['values'][0] for row in self.servers[2][0].rows),[12000,24000])
        self.assertFalse(list((self.path/'spool'/'long').glob('*.ready')))
        self.assertEqual(stats['long']['worker']['retention_status'],'ok')
    def test_fingerprint_conflict(self):
        self.run_driver()
        self.config.write_text(self.config.read_text().replace('interval_seconds = 300','interval_seconds = 600'))
        p=subprocess.run([str(DRIVER),str(self.config),'recover'],capture_output=True,text=True,timeout=10)
        self.assertNotEqual(p.returncode,0);self.assertIn('conflicts with saved state',p.stderr)
if __name__=='__main__':unittest.main()
