"""No mutations or external sources are needed to reject unsafe continuation states."""
import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pc28touzhu.domain.rule_lab_continuation import checkpoint_readiness
from pc28touzhu.domain.rule_lab import normalize_batch_payload,build_experiments
from pc28touzhu.services.rule_lab_store import RuleLabStore,RuleLabError
from tests.test_rule_lab_risk import policy,NOW


class CheckpointReadinessTests(unittest.TestCase):
    def setUp(self):
        self.study=dict(id=1,total_children=2,status='completed',comparable=True)
        self.batches=[dict(id=i,mode='backtest',status='completed',predictor_id=1,metric='combo',template_pack='basic_v1',
            stake_cents=100,start_date='2026-10-01',end_date='2026-11-26',risk_policy=policy(),
            processed_issues=101,experiment_count=1,revision=4,
            cursor={'last_issue_no':'1100','replay_complete':True,'evidence':{'last_issue':'1100','count':101,'conflict':False}}) for i in (2,3)]
        self.experiments=[dict(id=i,batch_id=i,condition_key='always',condition={'type':'always'},settlement_rule_id='ok',
            settlement_snapshot={'rule':'fixed','odds':6.33},stats={'net_cents':100,'max_loss_streak':7},
            risk_runtime={'active_round':{'round_no':2,'net_cents':100},'last_start_issue':'1099'}) for i in (2,3)]
        self.history=[dict(issue_no=str(i),conflict=False) for i in range(1001,1101)]

    def inspect(self,pending=0):
        return checkpoint_readiness(self.study,self.batches,self.experiments,self.history,pending_count=pending)

    def codes(self):return {b['code'] for b in self.inspect()['blockers']}

    def test_ready_is_not_continuation_permission_and_active_round_is_preserved(self):
        before=copy.deepcopy((self.study,self.batches,self.experiments,self.history))
        value=self.inspect()
        self.assertTrue(value['checkpoint_ready']);self.assertFalse(value['continuation_available'])
        self.assertEqual(value['saved_experiments'],2);self.assertEqual(value['last_issue'],'1100')
        self.assertEqual((self.study,self.batches,self.experiments,self.history),before)
        self.assertEqual(len(value['pending_checks']),3)

    def test_running_group_and_pending_are_explicit_blockers(self):
        self.study['status']='running';self.batches[1]['status']='running'
        result=self.inspect(pending=1)
        self.assertFalse(result['checkpoint_ready'])
        self.assertIn('study_not_completed',{b['code'] for b in result['blockers']})
        self.assertIn('pending_settlements',{b['code'] for b in result['blockers']})

    def test_inconsistent_fingerprint_and_expired_input_are_not_repaired_silently(self):
        self.study['comparable']=False;self.batches[0]['cursor']['input_expired']=True
        self.assertTrue({'input_conflict','incomparable_history'}<=self.codes())

    def test_warm_window_gaps_and_conflicts_are_rejected(self):
        self.history.pop(10)
        self.assertIn('warm_window_missing',self.codes())
        self.history[0]['conflict']=True
        self.assertIn('warm_window_conflict',self.codes())

    def test_same_name_cannot_hide_configuration_change(self):
        original=self.inspect()
        self.experiments[0]['settlement_snapshot']['odds']=5.0
        modified=self.inspect()
        self.assertNotEqual(original['configuration_hash'],modified['configuration_hash'])
        self.assertIn('inconsistent_snapshots',{b['code'] for b in modified['blockers']})
        self.batches[1]['stake_cents']=200
        self.assertIn('inconsistent_configuration',self.codes())

    def test_state_hash_tracks_money_round_and_revision_without_changing_config_hash(self):
        original=self.inspect()
        for mutate in (lambda:self.experiments[0]['stats'].update(net_cents=5),
                       lambda:self.experiments[0]['risk_runtime'].update(last_start_issue='1100'),
                       lambda:self.batches[0].update(revision=5)):
            mutate();modified=self.inspect()
            self.assertNotEqual(original['state_hash'],modified['state_hash'])
            self.assertEqual(original['configuration_hash'],modified['configuration_hash'])

    def test_cursor_mismatch_and_missing_experiment_block_atomic_checkpoint(self):
        self.batches[1]['cursor']['last_issue_no']='1099';self.experiments.pop()
        self.assertTrue({'unequal_cursors','cursor_evidence_mismatch','incomplete_experiments'}<=self.codes())


class CheckpointStoreTests(unittest.TestCase):
    def test_owned_read_only_inspection_leaves_existing_tables_identical(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=RuleLabStore(Path(tmp)/'lab.db');store.initialize()
            cfg=normalize_batch_payload(dict(name='old',mode='backtest',predictor_id=1,metric='combo',template_pack='basic_v1',start_date='2026-10-01',end_date='2026-11-26',risk_policy=policy()),now=NOW)
            batch=store.create_batch(1,cfg,'source',build_experiments('combo',template_pack='basic_v1'),now=NOW)
            study=store.create_study(1,batch['id'],'group','checkpoint-request-1',now=NOW)['study']
            def contents():
                with sqlite3.connect(store.db_path) as c:
                    return {t:c.execute('SELECT * FROM '+t+' ORDER BY rowid').fetchall() for t, in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            before=contents()
            result=store.inspect_continuation_checkpoint(1,study['id'])
            self.assertFalse(result['checkpoint_ready']);self.assertFalse(result['continuation_available'])
            self.assertEqual(result['saved_experiments'],312)
            self.assertEqual(contents(),before)
            for uid,sid in ((2,study['id']),(1,999)):
                with self.assertRaises(RuleLabError) as cm:store.inspect_continuation_checkpoint(uid,sid)
                self.assertEqual(cm.exception.status,404)


if __name__=='__main__':unittest.main()
