"""Session optimization must keep commits visible and never hide concurrent controls."""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabStore, RuleLabError
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_source import RuleLabSource
from tests.test_rule_lab_source import SourceFixture

NOW='2026-11-26T12:00:00Z'


class WorkerConnectionSessionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'lab.db';self.store=RuleLabStore(self.path);self.store.initialize()
        cfg=normalize_batch_payload(dict(name='session',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-26',end_date='2026-11-26'),now=NOW)
        self.batch=self.store.create_batch(1,cfg,'fixture',build_experiments('big_small'),now=NOW)
        self.store.set_batch_state(1,self.batch['id'],'running',now=NOW)

    def session(self):
        self.assertTrue(callable(getattr(self.store,'worker_session',None)),
                        'Worker needs a bounded connection session instead of opening per operation')
        return self.store.worker_session()

    def test_bounded_session_commits_remain_visible_and_external_pause_is_observed(self):
        with self.session():
            before=self.store.get_work_item(self.batch['id'])
            self.store.commit_step(self.batch['id'],before['batch']['revision'],cursor={},now=NOW,
                                   progress={'processed_issues_delta':1})
            independent=RuleLabStore(self.path)
            self.assertEqual(independent.get_batch(1,self.batch['id'])['processed_issues'],1)
            independent.set_batch_state(1,self.batch['id'],'paused',now=NOW)
            fresh=self.store.get_work_item(self.batch['id'])
            self.assertEqual(fresh['batch']['status'],'paused')
            rejected=self.store.commit_step(self.batch['id'],before['batch']['revision'],cursor={},now=NOW,
                                            progress={'processed_issues_delta':10})
            self.assertFalse(rejected['committed'])
            self.assertEqual(self.store.get_batch(1,self.batch['id'])['processed_issues'],1)

    def test_exception_rolls_back_only_current_transaction_and_releases_connection(self):
        with self.session():
            self.store.set_worker_status({'state':'running'},now=NOW)
            with self.assertRaisesRegex(ValueError,'injected'):
                with self.store._connect(write=True) as connection:
                    connection.execute("UPDATE lab_batches SET name='uncommitted' WHERE id=?",(self.batch['id'],))
                    raise ValueError('injected')
            self.assertEqual(self.store.get_batch(1,self.batch['id'])['name'],'session')
            with self.store._connect() as connection:
                connection.execute('BEGIN')
                connection.execute('SELECT id FROM lab_batches').fetchall()
            # An explicit read snapshot must end before another process updates.
            other=RuleLabStore(self.path)
            other.set_batch_state(1,self.batch['id'],'paused',now=NOW)
            self.assertEqual(self.store.get_batch(1,self.batch['id'])['status'],'paused')
        with sqlite3.connect(self.path,timeout=.1) as connection:
            self.assertEqual(connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()[0],0)

    def test_many_reads_and_commits_reuse_one_connection_within_tick_only(self):
        connect=sqlite3.connect
        connections=[]
        def opened(*args,**kwargs):
            connection=connect(*args,**kwargs);connections.append(connection);return connection
        with patch('pc28touzhu.services.rule_lab_store.sqlite3.connect',side_effect=opened):
            with self.session():
                for _ in range(10):
                    work=self.store.get_work_item(self.batch['id'])
                    self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,
                        progress={'processed_issues_delta':1})
            self.assertEqual(len(connections),1)
            self.assertEqual(self.store.get_batch(1,self.batch['id'])['processed_issues'],10)
            self.assertEqual(len(connections),2)
        for connection in connections:
            with self.assertRaises(sqlite3.ProgrammingError):connection.execute('SELECT 1')

    def test_lightweight_work_batch_matches_context_without_loading_experiments(self):
        self.assertTrue(callable(getattr(self.store,'get_work_batch',None)),
                        'Pending settlement needs only fresh batch metadata before deciding if work exists')
        expected=self.store.get_work_item(self.batch['id'])['batch']
        actual=self.store.get_work_batch(self.batch['id'])
        self.assertEqual(actual,expected)
        with self.store._connect(write=True) as connection:
            connection.execute("UPDATE lab_experiments SET settlement_snapshot_json='invalid-json' WHERE batch_id=?",(self.batch['id'],))
        self.assertEqual(self.store.get_work_batch(self.batch['id']),expected)


class WorkerMaintenanceIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=Path(self.tmp.name)
        self.store=RuleLabStore(root/'lab'/'lab.db');self.store.initialize()
        self.fixture=SourceFixture(root/'source.db');self.source=RuleLabSource(self.fixture.path)

    def test_queued_management_progresses_without_any_running_batch(self):
        self.assertTrue(callable(getattr(self.store,'request_delete',None)))
        config=normalize_batch_payload(dict(name='remove',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-26',end_date='2026-11-26'),now=NOW)
        batch=self.store.create_batch(1,config,'fixture',build_experiments('big_small'),now=NOW)
        preview=self.store.delete_preview(1,'batch',batch['id'],now=NOW)
        result=self.store.request_delete(1,'batch',batch['id'],dict(request_id='perf-delete-idle',
            expected_revision=preview['expected_revision'],confirm_name='remove'),now=NOW)
        job_id=result['job']['id']
        worker=RuleLabWorker(self.store,self.source)
        for _ in range(8):
            tick=worker.tick(now=NOW)
            job=self.store.management_job(1,job_id)
            if job['status']=='completed':break
        self.assertEqual(job['status'],'completed',job)
        self.assertIn('maintenance_pending',tick)
        with self.assertRaises(RuleLabError):self.store.get_batch(1,batch['id'])

    def test_removed_batch_during_work_does_not_fail_other_batches(self):
        config=normalize_batch_payload(dict(name='remove',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-26',end_date='2026-11-26'),now=NOW)
        batches=[]
        for _ in range(2):
            batch=self.store.create_batch(1,config,'fixture',build_experiments('big_small'),now=NOW)
            self.store.set_batch_state(1,batch['id'],'running',now=NOW);batches.append(batch)
        class DisappearingWorker(RuleLabWorker):
            def _replay_backtest(worker,batch_id):
                if batch_id==batches[0]['id']:
                    preview=worker.store.delete_preview(1,'batch',batch_id,now=NOW)
                    removed=worker.store.request_delete(1,'batch',batch_id,dict(request_id='perf-delete-race',
                        expected_revision=preview['expected_revision'],confirm_name='remove'),now=NOW)
                    for _ in range(4):
                        worker.store.management_maintenance(now=NOW,limit=500,max_pages=16)
                        if worker.store.management_job(1,removed['job']['id'])['status']=='completed':break
                return super()._replay_backtest(batch_id)
        result=DisappearingWorker(self.store,self.source).tick(now=NOW)
        self.assertEqual(self.store.get_batch(1,batches[1]['id'])['status'],'completed')
        self.assertNotEqual(result['state'],'error',result)
