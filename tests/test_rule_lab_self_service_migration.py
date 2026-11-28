"""Finite-manifest handoff preserves every original row and fails closed."""
import copy
import importlib.util
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from pc28touzhu.domain.rule_lab import build_experiments,normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabStore,RuleLabError
from pc28touzhu.services.rule_lab_series import _configuration

NOW='2026-11-28T04:00:00Z'


def load_script(name):
    path=Path(__file__).resolve().parents[1]/'scripts'/'maintenance'/(name+'.py')
    spec=importlib.util.spec_from_file_location(name,path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


class SelfServiceMigrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=RuleLabStore(Path(self.tmp.name)/'lab.db');self.store.initialize()
        self.module=load_script('import_rule_lab_queue')
        self.manifest=dict(version=1,user_id=1,request_prefix='finite-test',enable_continuation=False,batches=[])
        for index in range(6):
            config=normalize_batch_payload(dict(name='实验'+str(index),mode='backtest',predictor_id=7,
                metric='big_small',start_date='2026-11-01',end_date='2026-11-26'),now=NOW)
            batch=self.store.create_batch(1,config,'来源',build_experiments('big_small')[:2],now=NOW)
            with self.store._connect() as c:
                frozen=_configuration(c,[self.store._owned(c,1,batch['id'])])
            self.manifest['batches'].append(dict(batch_id=batch['id'],configuration_hash=frozen))
        self.stopped=SimpleNamespace(returncode=0,stdout='LoadState=loaded\nActiveState=inactive\nMainPID=0\n')

    def snapshot(self,*,all_tables=False):
        with sqlite3.connect(self.store.db_path) as c:
            tables=[row[0] for row in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'lab_%' ORDER BY name")]
            return {table:c.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall() for table in tables
                if all_tables or table not in ('lab_queue_items','lab_runtime')}

    def apply(self,manifest=None,now=NOW):
        with patch.object(self.module.subprocess,'run',return_value=self.stopped):
            return self.module.import_queue(self.store.db_path,manifest or self.manifest,apply=True,now=now)

    def test_preview_reads_schema6_without_migrating_or_touching_source(self):
        with sqlite3.connect(self.store.db_path) as c:
            c.execute('DROP TABLE lab_queue_items');c.execute('PRAGMA user_version=6')
        before=self.snapshot(all_tables=True);bytes_before=self.store.db_path.read_bytes()
        with patch.object(self.module.subprocess,'run') as command:
            result=self.module.import_queue(self.store.db_path,self.manifest)
            command.assert_not_called()
        self.assertFalse(result['applied']);self.assertEqual(result['schema'],6)
        self.assertEqual(self.snapshot(all_tables=True),before)
        self.assertEqual(self.store.db_path.read_bytes(),bytes_before)
        with patch.object(self.module.subprocess,'run',return_value=self.stopped):
            with self.assertRaisesRegex(RuleLabError,'schema7'):self.module.import_queue(self.store.db_path,self.manifest,apply=True)
        self.store.initialize();self.store.initialize()
        after=self.snapshot()
        for name,rows in before.items():
            if name not in ('lab_queue_items','lab_runtime'):self.assertEqual(after[name],rows)

    def test_import_preserves_results_pauses_and_repeat_is_exactly_idempotent(self):
        ids=[item['batch_id'] for item in self.manifest['batches']]
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("UPDATE lab_batches SET status='completed',processed_issues=33,started_at=? WHERE id=?",(NOW,ids[0]))
            c.execute("UPDATE lab_batches SET status='running',processed_issues=17,started_at=? WHERE id=?",(NOW,ids[1]))
            c.execute("UPDATE lab_batches SET processed_issues=9,started_at=? WHERE id=?",(NOW,ids[3]))
            c.execute("INSERT INTO lab_object_controls(owner_kind,owner_id,user_id,queue_optout,updated_at) VALUES('batch',?,1,1,?)",(ids[4],NOW))
            c.execute('UPDATE lab_batches SET stake_cents=200 WHERE id=?',(ids[5],))
        old=self.snapshot();first=self.apply()
        self.assertEqual(first['inserted'],6)
        self.assertEqual([row['state'] for row in first['items']],['completed','running','queued','paused','paused','removed'])
        self.assertEqual(self.snapshot(),old)
        all_before=self.snapshot(all_tables=True)
        repeated=self.apply(now='2026-11-28T05:00:00Z')
        self.assertEqual(repeated['inserted'],0);self.assertEqual(repeated['preserved'],6)
        self.assertEqual(self.snapshot(all_tables=True),all_before)
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("UPDATE lab_queue_items SET state='removed',revision=revision+1 WHERE owner_id=?",(ids[2],))
        self.apply();self.assertEqual(self.snapshot(),old)
        with sqlite3.connect(self.store.db_path) as c:
            self.assertEqual(c.execute('SELECT state FROM lab_queue_items WHERE owner_id=?',(ids[2],)).fetchone()[0],'removed')

    def test_live_or_unknown_controller_refuses_before_any_mutation(self):
        before=self.snapshot(all_tables=True)
        for stdout,code in [('LoadState=loaded\nActiveState=active\nMainPID=45\n',0),
                            ('LoadState=error\nActiveState=inactive\nMainPID=0\n',0),
                            ('LoadState=not-found\nActiveState=inactive\nMainPID=45\n',0),('',1)]:
            with self.subTest(state=stdout),patch.object(self.module.subprocess,'run',return_value=SimpleNamespace(returncode=code,stdout=stdout)):
                with self.assertRaises(RuleLabError):self.module.import_queue(self.store.db_path,self.manifest,apply=True)
            self.assertEqual(self.snapshot(all_tables=True),before)

    def test_stopped_transient_controller_can_be_collected_by_systemd(self):
        self.stopped=SimpleNamespace(returncode=0,stdout='LoadState=not-found\nActiveState=inactive\nMainPID=0\n')
        before=self.snapshot()
        result=self.apply()
        self.assertEqual(result['inserted'],6)
        self.assertEqual(self.snapshot(),before)

    def test_second_insert_failure_rolls_back_queue_and_handoff_record(self):
        second=self.manifest['batches'][1]['batch_id']
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("CREATE TRIGGER reject_second BEFORE INSERT ON lab_queue_items WHEN NEW.owner_id="+str(second)+" BEGIN SELECT RAISE(ABORT,'test interruption'); END")
        before=self.snapshot(all_tables=True)
        with self.assertRaises(sqlite3.IntegrityError):self.apply()
        self.assertEqual(self.snapshot(all_tables=True),before)
        with sqlite3.connect(self.store.db_path) as c:c.execute('DROP TRIGGER reject_second')
        self.assertEqual(self.apply()['inserted'],6)

    def test_handoff_stops_only_its_legacy_manifest_and_guards_member_bypass(self):
        self.apply();queue=load_script('run_rule_lab_batch_queue')
        with patch.object(self.store,'start_queued_batch',side_effect=AssertionError('legacy controller must not start')):
            result=queue.queue_step(SimpleNamespace(store=self.store),copy.deepcopy(self.manifest),now=NOW)
        self.assertTrue(result['complete']);self.assertTrue(result['transferred'])
        bid=self.manifest['batches'][0]['batch_id'];frozen=self.manifest['batches'][0]['configuration_hash']
        self.assertTrue(self.store.queue_eligibility(1,bid,frozen)['removed'])
        self.assertTrue(self.store.start_queued_batch(1,bid,frozen,now=NOW)['removed'])
        # A different existing continuation controller is not globally retired.
        other=copy.deepcopy(self.manifest);other['request_prefix']='existing-daily-series';other['batches']=[]
        result=queue.queue_step(SimpleNamespace(store=self.store),other,now=NOW)
        self.assertNotIn('transferred',result)

    def test_bad_or_changed_manifest_cannot_partially_import(self):
        before=self.snapshot(all_tables=True)
        for mutate in (lambda m:m.update(enable_continuation=True),
                       lambda m:m['batches'][0].update(batch_id=True),
                       lambda m:m['batches'].append(copy.deepcopy(m['batches'][0]))):
            bad=copy.deepcopy(self.manifest);mutate(bad)
            with self.assertRaises(RuleLabError):self.apply(bad)
            self.assertEqual(self.snapshot(all_tables=True),before)
        self.apply();before=self.snapshot(all_tables=True)
        bad=copy.deepcopy(self.manifest);bad['batches'][0]['configuration_hash']='0'*64
        with self.assertRaisesRegex(RuleLabError,'拒绝覆盖'):self.apply(bad)
        self.assertEqual(self.snapshot(all_tables=True),before)


if __name__=='__main__':unittest.main()
