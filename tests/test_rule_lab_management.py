"""Behavioral management tests use disposable SQLite ledgers, never production."""
import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabStore, RuleLabError
from tests.test_rule_lab_series import completed_baseline, NOW
from tests.test_rule_lab_risk import policy


class ManagementTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(); self.addCleanup(self.tmp.cleanup)
        self.store = RuleLabStore(Path(self.tmp.name)/'lab.db'); self.store.initialize()
        self.batch = self.create()

    def create(self, name='draft', metric='big_small', risk=None):
        config = normalize_batch_payload(dict(name=name,mode='backtest',predictor_id=1,metric=metric,
            start_date='2026-11-26',end_date='2026-11-26',risk_policy=risk),now=NOW)
        return self.store.create_batch(1,config,'source',build_experiments(metric),now=NOW)

    def preview(self, bid=None):
        self.assertTrue(hasattr(self.store,'management_preview'),'Management API is not implemented')
        return self.store.management_preview(1,'batch',bid or self.batch['id'],now=NOW)

    def payload(self, **changes):
        value=dict(request_id='management-request-1',expected_revision=self.preview()['expected_revision'])
        value.update(changes);return value

    def delete(self, bid=None, token='delete-request-1'):
        bid=bid or self.batch['id'];preview=self.store.delete_preview(1,'batch',bid,now=NOW)
        payload=dict(request_id=token,expected_revision=preview['expected_revision'],confirm_name=preview['name'])
        return self.store.request_delete(1,'batch',bid,payload,now=NOW),payload

    def finish(self, job_id, limit=37):
        for _ in range(200):
            result=self.store.management_maintenance(now=NOW,limit=limit,max_pages=16)
            self.assertLessEqual(result['processed_rows'],limit)
            job=self.store.management_job(1,job_id)
            if job['status']=='completed':return job
        self.fail('Bounded job did not finish: '+str(job))

    def test_edit_regenerates_only_pristine_batch_and_opts_out_of_queue(self):
        before=[x['id'] for x in self.store.list_experiments(1,self.batch['id'])]
        config=self.preview()['config'];config.update(name='edited',metric='combo',stake_amount=2)
        normalized=normalize_batch_payload(config,now=NOW)
        result=self.store.edit_batch(1,self.batch['id'],self.payload(config=normalized),predictor_name='source',
                                    experiments=build_experiments('combo'),now=NOW)
        self.assertTrue(result['queue_removed']);self.assertEqual(result['batch']['status'],'paused')
        self.assertEqual(result['batch']['experiment_count'],92)
        ids=[x['id'] for x in self.store.list_experiments(1,self.batch['id'])]
        self.assertGreater(min(ids),max(before))
        self.assertTrue(self.store.queue_eligibility(1,self.batch['id'],'unused')['removed'])
        self.store.set_batch_state(1,self.batch['id'],'running',now=NOW)
        with self.assertRaises(RuleLabError):
            self.store.edit_batch(1,self.batch['id'],self.payload(request_id='edit-after-start',config=normalized),
                                  predictor_name='source',experiments=build_experiments('combo'),now=NOW)

    def test_rename_and_fork_preserve_old_economic_state_and_use_idempotency(self):
        self.store.set_batch_state(1,self.batch['id'],'running',now=NOW)
        original_hash=self.store.continuation_preview(1,'batch',self.batch['id'],now=NOW)['configuration_hash']
        old=self.store.get_work_item(self.batch['id'])
        payload=self.payload(name='new name')
        first=self.store.rename_owner(1,'batch',self.batch['id'],payload,now=NOW)
        self.assertTrue(first['created'])
        self.assertFalse(self.store.rename_owner(1,'batch',self.batch['id'],payload,now=NOW)['created'])
        self.assertEqual(self.store.get_work_item(self.batch['id'])['experiments'],old['experiments'])
        current_hash=self.store.continuation_preview(1,'batch',self.batch['id'],now=NOW)['configuration_hash']
        self.assertEqual(current_hash,original_hash)
        self.assertEqual(self.store.get_batch(1,self.batch['id'])['queue_state'],'running')
        self.assertIn('数据库队列',self.store.queue_eligibility(1,self.batch['id'],current_hash)['reason'])
        config=self.preview()['config'];config.update(name='independent',stake_amount=3)
        fork_payload=self.payload(request_id='fork-request-1',config=normalize_batch_payload(config,now=NOW))
        fork=self.store.fork_batch(1,self.batch['id'],fork_payload,predictor_name='source',experiments=build_experiments('big_small'),now=NOW)
        self.assertEqual(fork['batch']['status'],'paused');self.assertEqual(fork['batch']['processed_issues'],0)
        again=self.store.fork_batch(1,self.batch['id'],fork_payload,predictor_name='source',experiments=build_experiments('big_small'),now=NOW)
        self.assertEqual(fork['batch']['id'],again['batch']['id']);self.assertFalse(again['created'])
        self.assertEqual(self.store.get_work_item(self.batch['id'])['experiments'],old['experiments'])

    def test_delete_running_pending_is_fenced_resumable_and_ids_never_reused(self):
        self.preview();bid=self.batch['id'];self.store.set_batch_state(1,bid,'running',now=NOW)
        work=self.store.get_work_item(bid);eid=work['experiments'][0]['id']
        decision=dict(experiment_id=eid,issue_no='1001',bet_value='大',stake_cents=100,decided_at=NOW,
                      decision_mode='backtest',feature_issue_no='1000',condition_value=1)
        self.store.commit_step(bid,work['batch']['revision'],cursor={},now=NOW,decisions=[decision])
        stale=self.store.get_work_item(bid)
        with self.store._connect(write=True) as c:
            for i in range(1002,1132):
                c.execute('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,stake_cents,decided_at,decision_mode,condition_value_json)
                    VALUES(?,?,?,'大',100,?,'backtest','1')''',(eid,str(i),i,NOW))
        highest_exp=max(e['id'] for e in self.store.list_experiments(1,bid))
        result,payload=self.delete();jid=result['job']['id']
        self.assertEqual(self.store.get_batch(1,bid)['status'],'deleting')
        outcome=dict(experiment_id=eid,issue_no='1001',status='settled',settled_at=NOW,result_type='hit',net_cents=100)
        self.assertFalse(self.store.commit_step(bid,stale['batch']['revision'],cursor={},now=NOW,outcomes=[outcome])['committed'])
        current=self.store.get_work_item(bid)
        self.assertFalse(self.store.commit_step(bid,current['batch']['revision'],cursor={},now=NOW,outcomes=[outcome])['committed'])
        self.store.management_maintenance(now=NOW,limit=37,max_pages=16)
        self.store=RuleLabStore(self.store.db_path);self.store.initialize()
        job=self.finish(jid);self.assertGreaterEqual(job['processed_rows'],180)
        with self.assertRaises(RuleLabError):self.store.get_batch(1,bid)
        self.assertEqual(self.store.request_delete(1,'batch',bid,payload,now=NOW)['job']['id'],jid)
        new=self.create('replacement');self.assertGreater(new['id'],bid)
        self.assertGreater(min(x['id'] for x in self.store.list_experiments(1,new['id'])),highest_exp)
        self.assertTrue(self.store.queue_eligibility(1,bid,'old')['removed'])
        with sqlite3.connect(self.store.db_path) as c:self.assertEqual(c.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_study_dependency_blocks_parent_and_group_delete_keeps_parent(self):
        self.preview();parent=self.create('study parent',risk=policy())
        study=self.store.create_study(1,parent['id'],'study','management-study-1',now=NOW)['study']
        preview=self.store.delete_preview(1,'batch',parent['id'],now=NOW)
        self.assertFalse(preview['eligible']);self.assertIn('dependent_studies',{b['code'] for b in preview['blockers']})
        child=study['children'][0]['batch_id']
        self.assertFalse(self.store.delete_preview(1,'batch',child,now=NOW)['eligible'])
        preview=self.store.delete_preview(1,'study',study['id'],now=NOW)
        result=self.store.request_delete(1,'study',study['id'],dict(request_id='delete-study-1',expected_revision=preview['expected_revision'],confirm_name='study'),now=NOW)
        self.finish(result['job']['id'])
        self.assertEqual(self.store.get_batch(1,parent['id'])['name'],'study parent')
        with self.assertRaises(RuleLabError):self.store.get_batch(1,child)

    def test_series_delete_keeps_shared_source_until_last_consumer_is_deleted(self):
        self.preview();baseline=completed_baseline(self.store)
        preview=self.store.continuation_preview(1,'batch',baseline['id'],now=NOW)
        self.store.set_continuation(1,'batch',baseline['id'],dict(request_id='mgmt-series-enable',expected_revision=preview['expected_revision'],capture_enabled=True,compute_enabled=True,daily_at='00:10',timezone='Asia/Shanghai'),now=NOW)
        result,_=self.delete(baseline['id'],'delete-series-1');self.finish(result['job']['id'])
        with sqlite3.connect(self.store.db_path) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM lab_series').fetchone()[0],0)
            self.assertGreater(c.execute('SELECT count(*) FROM lab_predictions').fetchone()[0],0)
        result,_=self.delete();self.finish(result['job']['id'])
        with sqlite3.connect(self.store.db_path) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM lab_predictions').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM lab_input_versions').fetchone()[0],0)
            self.assertEqual(c.execute('PRAGMA foreign_key_check').fetchall(),[])

    def test_progress_does_not_stale_management_preview_but_rename_does(self):
        self.preview();bid=self.batch['id'];self.store.set_batch_state(1,bid,'running',now=NOW)
        payload=self.payload(name='after progress')
        work=self.store.get_work_item(bid);self.store.commit_step(bid,work['batch']['revision'],cursor={},now=NOW,progress={'processed_issues_delta':1})
        self.assertTrue(self.store.rename_owner(1,'batch',bid,payload,now=NOW)['created'])
        with self.assertRaises(RuleLabError) as e:
            self.store.rename_owner(1,'batch',bid,{**payload,'request_id':'new-stale-request','name':'stale'},now=NOW)
        self.assertEqual(e.exception.code,'stale_revision')
        with self.assertRaises(RuleLabError) as e:self.store.delete_preview(2,'batch',bid,now=NOW)
        self.assertEqual(e.exception.status,404)
