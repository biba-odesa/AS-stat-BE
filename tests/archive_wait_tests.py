#!/usr/bin/env python3
"""Controlled clocks distinguish slow progress, gate backpressure and stalled archives."""
import pathlib,sys,tempfile,unittest
from unittest.mock import patch
sys.path.insert(0,str(pathlib.Path(__file__).resolve().parents[1]/'scripts'))
import archive_backfill as migration
class Clock:
    def __init__(self):self.now=0
    def clock(self):return self.now
    def sleep(self,seconds):self.now+=seconds
class WaitTests(unittest.TestCase):
    def wait(self,native,target,timeout,pump,clock):
        with patch.object(migration,'native_cursor',lambda db:db['cursor']):
            migration.wait_native(native,target,timeout,pump,clock=clock.clock,sleep=clock.sleep)
    def test_slow_advancement_exceeds_total_timeout(self):
        clock=Clock();state={'cursor':0};last=[0]
        def pump():
            if clock.now-last[0]>=.9:state['cursor']+=1;last[0]=clock.now
        self.wait({'slow':state},5,1.5,pump,clock)
        self.assertGreater(clock.now,1.5)
    def test_gate_pumped_while_waiting(self):
        clock=Clock();state={'cursor':0};gate=[False]
        def pump():gate[0]=True;state['cursor']=180
        self.wait({'limited':state},180,1,pump,clock)
        self.assertTrue(gate[0])
    def test_one_archive_cannot_mask_stall(self):
        clock=Clock();active={'cursor':0};stuck={'cursor':0}
        def pump():active['cursor']+=1
        with self.assertRaisesRegex(RuntimeError,'"stuck"'):
            self.wait({'active':active,'stuck':stuck},100,1,pump,clock)
    def test_lock_rejects_concurrent_backfill(self):
        with tempfile.TemporaryDirectory() as directory:
            work=pathlib.Path(directory)
            with migration.migration_lock(work):
                with self.assertRaisesRegex(RuntimeError,'Another backfill'):
                    with migration.migration_lock(work):pass
            with migration.migration_lock(work):pass
if __name__=='__main__':unittest.main()
