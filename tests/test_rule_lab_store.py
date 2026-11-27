"""Independent ledger behavior against temporary SQLite files only."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore

NOW='2026-11-26T04:00:00Z'


class RuleLabStoreTests(unittest.TestCase):
    def test_old_odds_are_labeled_without_changing_saved_results_or_snapshot(self):
        eid=self.store.list_experiments(1,self.batch['id'])[0]['id']
        with sqlite3.connect(self.path) as c:
            snapshot=json.loads(c.execute('SELECT settlement_snapshot_json FROM lab_experiments WHERE id=?',(eid,)).fetchone()[0])
            snapshot['resolved_odds'].update(big_small=1.98,odd_even=1.98,combo={'小单':3.6,'大双':3.6,'大单':4.2,'小双':4.2})
            c.execute('UPDATE lab_experiments SET settlement_snapshot_json=? WHERE id=?',(json.dumps(snapshot),eid))
            before=c.execute('SELECT * FROM lab_experiments WHERE id=?',(eid,)).fetchone()
        batch=self.store.get_batch(1,self.batch['id']);detail=self.store.get_experiment_detail(1,self.batch['id'],eid)
        self.assertTrue(batch['legacy_odds']);self.assertIn('历史成绩未重算',batch['history_notice'])
        self.assertTrue(detail['experiment']['legacy_odds']);self.assertEqual(detail['experiment']['settlement_odds']['combo']['大单'],4.2)
        with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('SELECT * FROM lab_experiments WHERE id=?',(eid,)).fetchone(),before)
        fresh=self.create();self.assertFalse(self.store.get_batch(1,fresh['id'])['legacy_odds'])
        self.assertEqual(self.store.list_experiments(1,fresh['id'])[0]['settlement_odds']['combo']['大单'],4.32)

    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.path=Path(self.tmp.name)/'lab.db'
        self.store=RuleLabStore(self.path)
        self.store.initialize()
        self.batch=self.create()

    def tearDown(self):
        self.tmp.cleanup()

    def create(self, user_id=1, metric='big_small'):
        config=normalize_batch_payload(dict(name='实验',mode='live',predictor_id=1,metric=metric),now=NOW)
        return self.store.create_batch(user_id,config,'公开来源',build_experiments(metric),now=NOW)

    def work(self):
        return self.store.get_work_item(self.batch['id'])

    def start(self):
        self.store.set_batch_state(1,self.batch['id'],'running',now=NOW)
        return self.work()

    def decision(self,work,issue='1001'):
        return dict(experiment_id=work['experiments'][0]['id'],issue_no=issue,bet_value='大',stake_cents=100,
                    decided_at=NOW,decision_mode=work['batch']['mode'],feature_issue_no='1000',condition_value=1)

    def outcome(self,decision,**changes):
        return dict(experiment_id=decision['experiment_id'],issue_no=decision['issue_no'],status='settled',
                    settled_at='2026-11-26T04:05:00Z',result_type='hit',net_cents=98,direction_hit=True,
                    refund_reason='',skip_reason='',**changes)

    def test_initialize_owner_and_default_paused(self):
        self.store.initialize()
        self.assertEqual(self.batch['status'],'paused')
        self.assertEqual(self.batch['experiment_count'],50)
        self.assertEqual(len(self.store.list_experiments(1,self.batch['id'])),50)
        self.assertEqual(self.store.list_batches(2),[])
        with self.assertRaises(RuleLabError) as cm:
            self.store.get_batch(2,self.batch['id'])
        self.assertEqual(cm.exception.status,404)

    def test_global_running_and_total_capacity(self):
        self.start()
        second=self.create(user_id=2)
        self.store.set_batch_state(2,second['id'],'running',now=NOW)
        third=self.create(user_id=2)
        with self.assertRaises(RuleLabError) as cm:
            self.store.set_batch_state(2,third['id'],'running',now=NOW)
        self.assertEqual(cm.exception.code,'capacity_exceeded')
        for _ in range(10):
            self.create()
        with self.assertRaises(RuleLabError):
            self.create()

    def test_atomic_commit_replay_and_daily_totals(self):
        work=self.start(); d=self.decision(work); out=self.outcome(d)
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={'last_issue_no':'1001'},now=NOW,
                                      decisions=[d],outcomes=[out],progress={'processed_issues_delta':1})
        self.assertEqual((result['inserted_decisions'],result['settled']),(1,1))
        work=self.work()
        again=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={'last_issue_no':'1001'},now=NOW,
                                     decisions=[d],outcomes=[out])
        self.assertEqual((again['inserted_decisions'],again['settled']),(0,0))
        detail=self.store.get_experiment_detail(1,self.batch['id'],d['experiment_id'])
        self.assertEqual(detail['experiment']['settled_count'],1)
        self.assertEqual(detail['experiment']['net_profit'],.98)
        self.assertEqual(detail['daily'][0]['net_profit'],.98)
        self.assertEqual(detail['recent'],[])
        self.assertEqual(detail['pending_count'],0)
        self.assertEqual(detail['detail_policy'],'results_only')

    def test_revision_pause_and_pending_settlement(self):
        work=self.start(); d=self.decision(work)
        self.store.set_batch_state(1,self.batch['id'],'paused',now=NOW)
        r=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,decisions=[d])
        self.assertFalse(r['committed'])
        self.assertEqual(r['reason'],'stale_revision')
        work=self.start()
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,decisions=[d])
        self.store.set_batch_state(1,self.batch['id'],'paused',now=NOW)
        self.assertEqual(len(self.store.list_work_batches()),1)
        work=self.work()
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,outcomes=[self.outcome(d)])
        self.assertEqual(self.store.get_batch(1,self.batch['id'])['status'],'paused')
        self.assertEqual(self.store.get_pending_decisions(self.batch['id']),[])

    def test_error_mid_commit_rolls_back_everything(self):
        work=self.start(); d=self.decision(work)
        bad=self.outcome(d);bad['result_type']='invalid'
        with self.assertRaises((ValueError,RuleLabError)):
            self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={'last_issue_no':'1001'},now=NOW,
                                   decisions=[d],outcomes=[bad])
        self.assertEqual(self.store.get_pending_decisions(self.batch['id']),[])
        self.assertEqual(self.work()['batch']['revision'],work['batch']['revision'])

    def test_cache_status_transition_is_not_conflict_but_direction_change_is(self):
        w=self.start()
        draw=dict(issue_no='1000',source_id=1,open_at=NOW,source_created_at=NOW,observed_at=NOW,triplet=[3,6,8],input_hash='draw')
        pred=dict(predictor_id=1,issue_no='1000',source_id=1,created_at=NOW,settled_at=None,observed_at=NOW,
                  status='pending',big_small='大',odd_even='单',combo='大单',input_hash='same')
        self.store.commit_step(self.batch['id'],w['batch']['revision'],cursor={},now=NOW,draws=[draw],predictions=[pred])
        w=self.work(); pred.update(status='settled',settled_at=NOW)
        self.store.commit_step(self.batch['id'],w['batch']['revision'],cursor={},now=NOW,predictions=[pred])
        rows=self.store.get_history(1,before_issue='1001')
        self.assertFalse(rows[0]['conflict'])
        self.assertEqual(rows[0]['prediction_settled_at'],NOW)
        w=self.work();pred.update(big_small='小',input_hash='changed')
        self.store.commit_step(self.batch['id'],w['batch']['revision'],cursor={},now=NOW,predictions=[pred])
        history=self.store.get_history(1,before_issue='1001')[0]
        self.assertTrue(history['conflict'])
        self.assertTrue(history['prediction_conflict'])
        self.assertFalse(history['draw_conflict'])

    def test_cleanup_preserves_statistics_and_prevents_reinsertion(self):
        work=self.start();d=self.decision(work);out=self.outcome(d)
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={'last_issue_no':'1001'},now=NOW,decisions=[d],outcomes=[out])
        self.store.prune(now='2027-01-26T04:00:00Z')
        detail=self.store.get_experiment_detail(1,self.batch['id'],d['experiment_id'])
        self.assertEqual(detail['recent'],[])
        self.assertEqual(detail['experiment']['settled_count'],1)
        work=self.work()
        r=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={'last_issue_no':'1001'},now=NOW,decisions=[d],outcomes=[out])
        self.assertEqual(r['inserted_decisions'],0)
        self.assertEqual(self.store.list_experiments(1,self.batch['id'])[0]['settled_count'],1)

    def test_budget_blocks_growth_but_preserves_read_and_pause(self):
        work=self.start()
        limited=RuleLabStore(self.path,budget_bytes=1024)
        with self.assertRaises(RuleLabError) as cm:
            config=normalize_batch_payload(dict(name='x',mode='live',predictor_id=1,metric='big_small'),now=NOW)
            limited.create_batch(1,config,'source',build_experiments('big_small'),now=NOW)
        self.assertEqual(cm.exception.code,'storage_limit')
        self.assertEqual(len(limited.list_batches(1)),1)
        self.assertEqual(limited.set_batch_state(1,self.batch['id'],'paused',now=NOW)['status'],'paused')

    def test_target_input_conflict_is_skipped_before_counting_profit(self):
        work=self.start();d=self.decision(work)
        pred=dict(predictor_id=1,issue_no='1001',source_id=1,created_at=NOW,settled_at=None,observed_at=NOW,
                  status='pending',big_small='大',odd_even='单',combo='大单',input_hash='original')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,predictions=[pred])
        work=self.work();pred.update(big_small='小',input_hash='changed')
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,predictions=[pred],decisions=[d],outcomes=[self.outcome(d)])
        self.assertEqual(result['settled'],0)
        detail=self.store.get_experiment_detail(1,self.batch['id'],d['experiment_id'])
        self.assertEqual(detail['experiment']['net_profit'],0)
        self.assertEqual(detail['experiment']['skipped_count'],1)
        self.assertEqual(detail['pending_count'],0)
        self.assertEqual(detail['recent'],[])

    def test_long_term_watermark_survives_cleanup_even_without_cursor(self):
        work=self.start();d=self.decision(work)
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,decisions=[d],outcomes=[self.outcome(d)])
        self.store.prune(now='2027-01-26T04:00:00Z')
        work=self.work()
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,decisions=[d],outcomes=[self.outcome(d)])
        self.assertEqual(result['inserted_decisions'],0)
        self.assertEqual(self.store.list_experiments(1,self.batch['id'])[0]['settled_count'],1)

    def test_newly_completed_old_backtest_keeps_results_without_detail_retention(self):
        config=normalize_batch_payload(dict(name='历史',mode='backtest',predictor_id=1,metric='big_small',
                                           start_date='2026-10-01',end_date='2026-10-01'),now=NOW)
        batch=self.store.create_batch(1,config,'公开来源',build_experiments('big_small'),now=NOW)
        self.store.set_batch_state(1,batch['id'],'running',now=NOW)
        work=self.store.get_work_item(batch['id']);d=self.decision(work)
        d['decided_at']='2026-10-01T04:00:00Z'
        out=self.outcome(d);out['settled_at']='2026-10-01T04:05:00Z'
        self.store.commit_step(batch['id'],work['batch']['revision'],cursor={'last_issue_no':'1001'},now=NOW,decisions=[d],outcomes=[out])
        self.store.prune(now=NOW)
        detail=self.store.get_experiment_detail(1,batch['id'],d['experiment_id'])
        self.assertEqual(detail['recent'],[])
        self.assertEqual(detail['experiment']['settled_count'],1)
        self.assertEqual(detail['daily'][0]['date'],'2026-10-01')
        self.store.prune(now='2026-12-27T04:00:00Z')
        self.assertEqual(self.store.get_experiment_detail(1,batch['id'],d['experiment_id'])['recent'],[])

    def test_prediction_revision_does_not_cancel_frozen_live_bet(self):
        work=self.start();d=self.decision(work)
        pred=dict(predictor_id=1,issue_no='1001',source_id=1,created_at=NOW,settled_at=None,observed_at=NOW,
                  status='pending',big_small='大',odd_even='单',combo='大单',input_hash='original')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,predictions=[pred],decisions=[d])
        self.assertEqual(self.store.get_pending_decisions(self.batch['id'])[0]['bet_value'],'大')
        work=self.work();pred.update(big_small='小',input_hash='changed')
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,predictions=[pred],outcomes=[self.outcome(d)])
        self.assertEqual(result['settled'],1)
        detail=self.store.get_experiment_detail(1,self.batch['id'],d['experiment_id'])
        self.assertEqual(detail['experiment']['net_profit'],.98)
        self.assertEqual(detail['experiment']['hit_count'],1)
        self.assertEqual(detail['pending_count'],0)
        self.assertEqual(detail['recent'],[])

    def test_draw_conflict_quarantines_pending_without_changing_balance(self):
        work=self.start();d=self.decision(work)
        draw=dict(issue_no='1001',source_id=1,open_at=NOW,source_created_at=NOW,observed_at=NOW,triplet=[3,6,8],input_hash='original')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,draws=[draw],decisions=[d])
        work=self.work();draw.update(triplet=[1,2,3],input_hash='changed')
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,draws=[draw],outcomes=[self.outcome(d)])
        self.assertEqual(result['settled'],0)
        self.assertEqual(self.store.get_batch(1,self.batch['id'])['status'],'error')
        self.assertEqual(len(self.store.get_pending_decisions(self.batch['id'])),1)
        self.assertEqual(self.store.list_experiments(1,self.batch['id'])[0]['net_profit'],0)

    def test_pending_issue_only_protects_its_inputs_and_recent_window(self):
        work=self.start();d=self.decision(work)
        old='2026-01-01T00:00:00Z'
        draws=[dict(issue_no=str(i),source_id=i,open_at=old,source_created_at=old,observed_at=old,
                    triplet=[3,6,8],input_hash=str(i)) for i in range(1000,1150)]
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,draws=draws,decisions=[d])
        removed=self.store.prune(now=NOW)
        self.assertGreaterEqual(removed['processed_rows'],49)
        with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_draws').fetchone()[0],101)
        self.assertEqual(self.store.get_history(1,before_issue='1002')[0]['issue_no'],'1001')
        self.assertEqual(len(self.store.get_history(1,before_issue='1150')),100)
        self.assertEqual(len(self.store.get_pending_decisions(self.batch['id'])),1)

    def test_earlier_pending_draw_conflict_blocks_later_outcomes_across_commits(self):
        work=self.start();early=self.decision(work,'1001');late=self.decision(work,'1002')
        draw=dict(issue_no='1001',source_id=1,open_at=NOW,source_created_at=NOW,observed_at=NOW,triplet=[3,6,8],input_hash='original')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,draws=[draw],decisions=[early,late])
        work=self.work();draw.update(triplet=[1,2,3],input_hash='changed')
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,draws=[draw],outcomes=[self.outcome(late),self.outcome(early)])
        self.assertEqual(result['settled'],0)
        self.assertEqual(len(self.store.get_pending_decisions(self.batch['id'])),2)
        work=self.work()
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,outcomes=[self.outcome(late)])
        self.assertEqual(result['settled'],0)
        self.assertEqual(self.store.list_experiments(1,self.batch['id'])[0]['net_profit'],0)

    def test_missing_earlier_outcome_waits_and_then_settles_in_order(self):
        work=self.start();early=self.decision(work,'1001');late=self.decision(work,'1002')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,decisions=[early,late])
        work=self.work()
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,outcomes=[self.outcome(late)])
        self.assertEqual(result['settled'],0)
        work=self.work()
        result=self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,outcomes=[self.outcome(late),self.outcome(early)])
        self.assertEqual(result['settled'],2)

    def test_soft_disk_budget_preserves_pending_settlement_reserve(self):
        work=self.start();d=self.decision(work)
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,decisions=[d])
        self.store.set_batch_state(1,self.batch['id'],'paused',now=NOW)
        limited=RuleLabStore(self.path,budget_bytes=1024)
        work=self.work()
        draw=dict(issue_no='1001',source_id=1,open_at=NOW,source_created_at=NOW,observed_at=NOW,triplet=[3,6,8],input_hash='draw')
        result=limited.commit_step(self.batch['id'],work['batch']['revision'],cursor={},now=NOW,draws=[draw],outcomes=[self.outcome(d)])
        self.assertEqual(result['settled'],1)
        self.assertEqual(limited.list_experiments(1,self.batch['id'])[0]['net_profit'],.98)

    def test_busy_is_safe_business_error(self):
        connection=sqlite3.connect(self.path)
        connection.execute('BEGIN IMMEDIATE')
        try:
            with self.assertRaises(RuleLabError) as cm:
                self.store.set_batch_state(1,self.batch['id'],'running',now=NOW)
            self.assertEqual(cm.exception.code,'store_busy')
            self.assertNotIn(str(self.path),str(cm.exception))
        finally:
            connection.rollback();connection.close()

    def test_heartbeat_becomes_stale(self):
        self.assertEqual(self.store.worker_status(now=NOW)['state'],'starting')
        self.store.set_worker_status({'state':'idle','message':'等待','last_tick_ms':1,'processed_issues':0},now=NOW)
        self.assertEqual(self.store.worker_status(now='2026-11-26T04:02:00Z')['state'],'stale')


if __name__=='__main__':
    unittest.main()
