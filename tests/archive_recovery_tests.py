import sys,tempfile,pathlib,sqlite3,json,types,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'scripts'))
import archive_recover_minutes as r
class Recovery(unittest.TestCase):
 def test_cancelled_backfill_preserves_reference(self):
  import archive_backfill as backfill
  with tempfile.TemporaryDirectory() as td:
   work=pathlib.Path(td);(work/'cancelled.json').write_text('{}');(work/'reference.sqlite').write_bytes(b'preserved')
   with self.assertRaisesRegex(RuntimeError,'cancelled'):backfill.run(types.SimpleNamespace(work=work))
   self.assertEqual((work/'reference.sqlite').read_bytes(),b'preserved')
 def test_peer_idempotency_and_conflict(self):
  with tempfile.TemporaryDirectory() as td:
   p=pathlib.Path(td);work=p/'work';work.mkdir();root=p/'state';root.mkdir()
   conf=p/'conf';conf.write_text(f'archive_state_max_bytes = 10000000\narchive_state_directory = {root}\narchive_queue_bytes = 100000\nmax_active_keys = 100\n'+''.join(f'[archive {n}]\ninterval_seconds = {s}\nurl = http://127.0.0.1:9999\n' for n,s in [('minute',60),('week',300),('month',1800),('year',7200)]))
   for n in ['minute','week','month','year']:
    (root/n).mkdir(); db=sqlite3.connect(root/n/'archives.sqlite');db.executescript('CREATE TABLE meta(key TEXT PRIMARY KEY,value INTEGER);CREATE TABLE inbox(minute INTEGER PRIMARY KEY,partial INTEGER,payload TEXT);CREATE TABLE skipped(start INTEGER);CREATE TABLE receipts(minute INTEGER,canonical TEXT);');db.execute('INSERT INTO meta VALUES(?,?)',('next',180 if n=='minute' else 60));db.execute('INSERT INTO meta VALUES(?,?)',('partial_windows',0))
    if n in ['week','month']:db.execute('INSERT INTO inbox VALUES(60,0,?)',('link\t0\t0\t4\t100\n',))
    db.commit();db.close()
   final=p/'final';final.write_text(json.dumps({'archives':{'minute':{'lost_minutes':0,'worker':{'delivery':{'unconfirmed_rows':0,'lost_before_spool_rows':0,'accepted_rows':1,'spool':{'sent_rows':1,'pending_batches':0}}}}},'aggregation':{'recent_windows':[{'window_start':120,'keys':1,'partial':False}]}}))
   args=types.SimpleNamespace(config=conf,work=work,start=60,end=180,final_diagnostic=final)
   with patch.object(r,'export',return_value=[(('link',0,0,4),120000,125)]),patch.object(r.time,'sleep'):
    r.recover(args);r.recover(args)
   db=sqlite3.connect(root/'year/archives.sqlite');self.assertEqual(db.execute('SELECT count(*) FROM inbox').fetchone()[0],2);self.assertIn('100',db.execute('SELECT payload FROM inbox WHERE minute=60').fetchone()[0]);db.execute('DELETE FROM inbox WHERE minute=60');db.commit();db.close()
   ref=sqlite3.connect(work/'reference.sqlite');ref.execute('DELETE FROM recovery_inputs WHERE minute=60');ref.commit();ref.close()
   db=sqlite3.connect(root/'month/archives.sqlite');db.execute('UPDATE inbox SET payload=? WHERE minute=60',('different',));db.commit();db.close()
   with self.assertRaisesRegex(RuntimeError,'conflict'):r.recover(args)
if __name__=='__main__':unittest.main()
