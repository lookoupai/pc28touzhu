"""End-to-end worker checks use local, disposable source and ledger files."""
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.test_rule_lab_source import SourceFixture, UTC, stamp
from tests.test_rule_lab_risk import policy


class RuleLabWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fixture = SourceFixture(Path(self.tmp.name) / 'source.db')
        self.source = RuleLabSource(self.fixture.path)
        self.store = RuleLabStore(Path(self.tmp.name) / 'lab' / 'lab.db')
        self.store.initialize()
        self.opened = datetime(2026, 11, 26, 4, tzinfo=UTC)
        self.now = self.opened + timedelta(seconds=60)
        self.worker = RuleLabWorker(self.store, self.source)

    def tearDown(self):
        self.tmp.cleanup()

    def batch(self, *, mode='live', metric='big_small', start=True):
        payload = dict(name='测试', mode=mode, predictor_id=1, metric=metric)
        if mode == 'backtest':
            payload.update(start_date='2026-11-26', end_date='2026-11-26')
        config = normalize_batch_payload(payload, now=stamp(self.now))
        b = self.store.create_batch(1, config, '公开来源', build_experiments(metric), now=stamp(self.now))
        if start:
            self.store.set_batch_state(1, b['id'], 'running', now=stamp(self.now))
        return b

    def prepare_live(self):
        self.fixture.draw(1000, self.opened)
        self.fixture.prediction(1000, self.opened)
        self.fixture.prediction(1001, self.opened + timedelta(seconds=210), status='pending',
                                created=self.opened + timedelta(seconds=30))

    def settle_target(self, *, balls=(3, 6, 8), opened=None):
        opened = opened or self.opened + timedelta(seconds=210)
        self.fixture.draw(1001, opened, balls)
        self.fixture.execute("UPDATE predictions SET status='settled',settled_at=? WHERE issue_no='1001'",
                             ((opened + timedelta(seconds=15)).strftime('%Y-%m-%d %H:%M:%S'),))
        self.now = opened + timedelta(seconds=30)

    def test_live_freezes_before_draw_and_restarts_without_duplicate_profit(self):
        self.prepare_live()
        batch = self.batch()
        result = self.worker.tick(now=stamp(self.now))
        pending = self.store.get_pending_decisions(batch['id'])
        self.assertGreaterEqual(len(pending), 2)
        self.assertTrue(all(d['issue_no'] == '1001' for d in pending))
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['settled_count'], 0)
        self.assertGreaterEqual(result['inserted_decisions'], 2)
        self.settle_target()
        RuleLabWorker(self.store, self.source).tick(now=stamp(self.now))
        first = self.store.list_experiments(1, batch['id'])[:2]
        self.assertEqual([r['net_profit'] for r in first], [.98, 1.85])
        self.worker.tick(now=stamp(self.now + timedelta(seconds=15)))
        self.assertEqual(self.store.list_experiments(1, batch['id'])[:2], first)

    def test_round_replay_continues_after_entry_condition_breaks_and_keeps_ok_states_independent(self):
        # Three preceding correct big calls open the >=3 hit streak candidate.
        for i in range(-3,5):
            opened=self.opened+timedelta(seconds=210*i)
            self.fixture.draw(1100+i,opened,balls=(3,6,8) if i<0 or i>0 else (1,3,5))
            self.fixture.prediction(1100+i,opened)
        self.now=self.opened+timedelta(hours=1)
        p=policy();p['round'].update(profit_target=1,loss_limit=10);p['day']['enabled']=False
        payload=dict(name='按轮测试',mode='backtest',predictor_id=1,metric='big_small',start_date='2026-11-26',end_date='2026-11-26',risk_policy=p)
        config=normalize_batch_payload(payload,now=stamp(self.now))
        b=self.store.create_batch(1,config,'source',build_experiments('big_small'),now=stamp(self.now))
        self.store.set_batch_state(1,b['id'],'running',now=stamp(self.now))
        for _ in range(20):
            self.worker.tick(now=stamp(self.now))
            if self.store.get_batch(1,b['id'])['status']=='completed':break
        results=self.store.list_experiments(1,b['id'])
        entered=[e for e in results if e['condition']['type']=='hit_streak' and e['condition']['threshold']==3]
        self.assertEqual(len(entered),2)
        for e in entered:
            detail=self.store.get_experiment_detail(1,b['id'],e['id'])
            issues={d['issue_no'] for d in detail['recent']}
            self.assertIn('1100',issues);self.assertIn('1101',issues)  # Loss broke condition, active round continues.
        self.assertNotEqual(entered[0]['risk_state']['completed_rounds'],entered[1]['risk_state']['completed_rounds'])

    def test_round_replay_waits_for_result_publication_before_future_bets(self):
        for i in range(4):
            opened=self.opened+timedelta(seconds=210*i)
            self.fixture.draw(1100+i,opened,available=self.opened+timedelta(seconds=500) if i==0 else None)
            self.fixture.prediction(1100+i,opened)
        self.now=self.opened+timedelta(hours=1)
        p=policy();p['round'].update(profit_target=1,loss_limit=10);p['day']['enabled']=False
        cfg=normalize_batch_payload(dict(name='延迟回测',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-26',end_date='2026-11-26',risk_policy=p),now=stamp(self.now))
        b=self.store.create_batch(1,cfg,'source',build_experiments('big_small'),now=stamp(self.now))
        self.store.set_batch_state(1,b['id'],'running',now=stamp(self.now))
        RuleLabWorker(self.store,self.source,page_size=1).tick(now=stamp(self.now))
        self.store.set_batch_state(1,b['id'],'paused',now=stamp(self.now))
        RuleLabWorker(self.store,self.source,page_size=1).tick(now=stamp(self.now+timedelta(hours=1)))
        before=self.store.list_experiments(1,b['id'])[:2]
        self.assertEqual([e['settled_count'] for e in before],[0,0])
        self.assertEqual([e['risk_state']['stop_reason'] for e in before],['',''])
        self.store.set_batch_state(1,b['id'],'running',now=stamp(self.now))
        for _ in range(20):
            RuleLabWorker(self.store,self.source,page_size=1).tick(now=stamp(self.now))
            if self.store.get_batch(1,b['id'])['status']=='completed':break
        exp=self.store.list_experiments(1,b['id'])[0]
        detail=self.store.get_experiment_detail(1,b['id'],exp['id'])
        self.assertEqual({d['issue_no'] for d in detail['recent']},{'1100','1103'})
        self.assertEqual(exp['settled_count'],2)
        other=self.store.create_batch(1,cfg,'source',build_experiments('big_small'),now=stamp(self.now))
        self.store.set_batch_state(1,other['id'],'running',now=stamp(self.now))
        for _ in range(5):
            RuleLabWorker(self.store,self.source,page_size=200).tick(now=stamp(self.now))
            if self.store.get_batch(1,other['id'])['status']=='completed':break
        other_exp=self.store.list_experiments(1,other['id'])[0]
        other_detail=self.store.get_experiment_detail(1,other['id'],other_exp['id'])
        self.assertEqual(other_exp['net_profit'],exp['net_profit'])
        self.assertEqual(other_exp['risk_state'],exp['risk_state'])
        self.assertEqual({d['issue_no'] for d in other_detail['recent']},{'1100','1103'})

    def test_late_actual_open_is_skipped_instead_of_charged(self):
        self.prepare_live()
        batch = self.batch()
        self.worker.tick(now=stamp(self.now))
        # Early actual open relative to the extrapolated clock invalidates the frozen decision.
        self.settle_target(opened=self.now + timedelta(seconds=20))
        self.worker.tick(now=stamp(self.now))
        result = self.store.list_experiments(1, batch['id'])[0]
        self.assertEqual(result['settled_count'], 0)
        self.assertEqual(result['skipped_count'], 1)
        detail = self.store.get_experiment_detail(1, batch['id'], result['id'])
        self.assertEqual(detail['recent'][0]['skip_reason'], 'late_decision')

    def test_paused_batch_settles_frozen_bets_without_new_decisions(self):
        self.prepare_live()
        batch = self.batch()
        self.worker.tick(now=stamp(self.now))
        self.store.set_batch_state(1, batch['id'], 'paused', now=stamp(self.now))
        self.settle_target()
        self.fixture.prediction(1002, self.opened + timedelta(seconds=420), status='pending')
        self.worker.tick(now=stamp(self.now))
        self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'paused')
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['settled_count'], 1)
        self.assertEqual(self.store.get_pending_decisions(batch['id']), [])

    def test_future_or_settled_prediction_is_never_a_live_decision(self):
        self.prepare_live()
        batch = self.batch()
        self.fixture.execute("UPDATE predictions SET status='settled',settled_at='2026-11-26 04:04:00' WHERE issue_no='1001'")
        self.worker.tick(now=stamp(self.now))
        self.assertEqual(self.store.get_pending_decisions(batch['id']), [])
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['trigger_count'], 0)

    def test_prediction_correction_does_not_cancel_already_frozen_live_loss(self):
        self.prepare_live()
        batch = self.batch()
        self.worker.tick(now=stamp(self.now))
        self.fixture.execute("UPDATE predictions SET prediction_big_small='小' WHERE issue_no='1001'")
        self.settle_target(balls=(1, 3, 5))
        self.worker.tick(now=stamp(self.now))
        self.assertEqual([e['net_profit'] for e in self.store.list_experiments(1, batch['id'])[:2]], [-1, -1])

    def test_revoked_sharing_stops_new_observation_but_existing_bets_settle(self):
        self.prepare_live()
        batch = self.batch()
        self.worker.tick(now=stamp(self.now))
        self.fixture.execute("UPDATE predictors SET share_predictions=0 WHERE id=1")
        self.settle_target()
        self.worker.tick(now=stamp(self.now))
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['settled_count'], 1)
        self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'error')

    def test_historical_replay_is_bounded_and_remains_exact_after_restart(self):
        for i in range(5):
            opened = self.opened + timedelta(seconds=210*i)
            self.fixture.draw(1000+i, opened)
            self.fixture.prediction(1000+i, opened)
        self.now = self.opened + timedelta(hours=1)
        batch = self.batch(mode='backtest')
        worker = RuleLabWorker(self.store, self.source, page_size=2)
        for _ in range(12):
            worker.tick(now=stamp(self.now))
            worker = RuleLabWorker(self.store, self.source, page_size=2)
            if self.store.get_batch(1, batch['id'])['status'] == 'completed':
                break
        b = self.store.get_batch(1, batch['id'])
        self.assertEqual(b['status'], 'completed', b)
        baseline = self.store.list_experiments(1, batch['id'])[0]
        self.assertEqual((baseline['settled_count'], baseline['net_profit']), (5, 4.9))
        detail = self.store.get_experiment_detail(1, batch['id'], baseline['id'])
        self.assertEqual(detail['daily'][0]['date'], '2026-11-26')
        self.assertEqual(detail['daily'][0]['settled_count'], 5)

    def test_late_historical_prediction_cannot_be_used_for_baseline_or_streak(self):
        for i in range(4):
            opened = self.opened + timedelta(seconds=210*i)
            self.fixture.draw(1000+i, opened)
            self.fixture.prediction(1000+i, opened, created=opened + timedelta(seconds=5) if i < 3 else None)
        self.now = self.opened + timedelta(hours=1)
        batch = self.batch(mode='backtest')
        for _ in range(8):
            self.worker.tick(now=stamp(self.now))
            if self.store.get_batch(1, batch['id'])['status'] == 'completed':
                break
        experiments = self.store.list_experiments(1, batch['id'])
        self.assertEqual(experiments[0]['settled_count'], 1)
        hit_streaks = [e for e in experiments if e['condition']['type'] == 'hit_streak']
        self.assertTrue(all(e['trigger_count'] == 0 for e in hit_streaks))

    def test_pause_race_rejects_entire_uncommitted_decision(self):
        self.prepare_live()
        batch = self.batch()
        original = self.store.commit_step
        def pause_then_commit(*args, **kwargs):
            if kwargs.get('decisions'):
                self.store.set_batch_state(1, batch['id'], 'paused', now=stamp(self.now))
            return original(*args, **kwargs)
        with patch.object(self.store, 'commit_step', side_effect=pause_then_commit):
            self.worker.tick(now=stamp(self.now))
        self.assertEqual(self.store.get_pending_decisions(batch['id']), [])
        self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'paused')

    def test_storage_pressure_pauses_growth_and_keeps_statistics_readable(self):
        self.prepare_live()
        batch = self.batch()
        limited = RuleLabStore(self.store.db_path, budget_bytes=1024)
        result = RuleLabWorker(limited, self.source).tick(now=stamp(self.now))
        self.assertEqual(result['state'], 'resource_paused')
        self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'resource_paused')
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['net_profit'], 0)

    def test_empty_and_tiny_time_budget_return_without_work(self):
        self.assertEqual(self.worker.tick(now=stamp(self.now))['state'], 'idle')
        self.prepare_live()
        batch = self.batch()
        result = RuleLabWorker(self.store, self.source, max_tick_seconds=.000001).tick(now=stamp(self.now))
        self.assertEqual(result['processed_issues'], 0)
        self.assertEqual(self.store.get_pending_decisions(batch['id']), [])

    def test_same_source_reads_are_shared_across_batches(self):
        self.prepare_live()
        self.batch()
        self.batch(metric='odd_even')
        with patch.object(self.source, 'latest_draw', wraps=self.source.latest_draw) as latest:
            self.worker.tick(now=stamp(self.now))
        self.assertEqual(latest.call_count, 1)

    def test_late_draw_publication_cannot_create_a_historical_streak(self):
        for i in range(4):
            opened = self.opened + timedelta(seconds=210*i)
            available = self.opened + timedelta(hours=2) if i == 2 else None
            self.fixture.draw(1000+i, opened, available=available)
            self.fixture.prediction(1000+i, opened)
        self.now = self.opened + timedelta(hours=3)
        batch = self.batch(mode='backtest')
        for _ in range(8):
            self.worker.tick(now=stamp(self.now))
            if self.store.get_batch(1, batch['id'])['status'] == 'completed':
                break
        experiments = self.store.list_experiments(1, batch['id'])
        streak = [e for e in experiments if e['condition']['type'] == 'draw_streak'
                  and e['condition']['bet_value'] == '大' and e['condition']['threshold'] == 3]
        self.assertTrue(streak)
        self.assertTrue(all(e['trigger_count'] == 0 for e in streak))

    def test_source_busy_recovers_and_keeps_live_status(self):
        self.prepare_live()
        batch = self.batch()
        c = sqlite3.connect(str(self.fixture.path))
        try:
            c.execute('BEGIN EXCLUSIVE')
            failed = self.worker.tick(now=stamp(self.now))
            self.assertEqual(failed['state'], 'error')
        finally:
            c.rollback()
            c.close()
        self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'running')
        self.worker.tick(now=stamp(self.now + timedelta(seconds=15)))
        self.assertGreaterEqual(len(self.store.get_pending_decisions(batch['id'])), 2)

    def test_pending_settlement_uses_reserve_under_soft_disk_limit(self):
        self.prepare_live()
        batch = self.batch()
        self.worker.tick(now=stamp(self.now))
        self.settle_target()
        limited = RuleLabStore(self.store.db_path, budget_bytes=1024)
        RuleLabWorker(limited, self.source).tick(now=stamp(self.now))
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['settled_count'], 1)
        self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'resource_paused')

    def test_downtime_does_not_backfill_closed_live_issues(self):
        self.prepare_live()
        batch = self.batch()
        self.settle_target()
        self.worker.tick(now=stamp(self.now))
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['trigger_count'], 0)

    def test_backtest_start_seek_skips_old_history_without_importing_all(self):
        old = self.opened - timedelta(days=20)
        for i in range(250):
            self.fixture.draw(1000+i, old + timedelta(seconds=210*i))
        self.fixture.draw(1300, self.opened)
        self.fixture.prediction(1300, self.opened)
        self.now = self.opened + timedelta(hours=1)
        batch = self.batch(mode='backtest')
        self.worker.tick(now=stamp(self.now))
        self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'completed')
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['settled_count'], 1)
        with sqlite3.connect(str(self.store.db_path)) as c:
            self.assertLessEqual(c.execute('SELECT count(*) FROM lab_draws').fetchone()[0], 101)

    def test_live_decision_uses_commit_time_when_real_clock_advances(self):
        self.prepare_live()
        batch = self.batch()
        class AdvancingClock(datetime):
            calls = 0
            @classmethod
            def now(cls, tz=None):
                cls.calls += 1
                # First tick starts within the window, but final commit is after cutoff.
                return self.now if cls.calls == 1 else self.opened + timedelta(seconds=190)
        with patch('pc28touzhu.rule_lab_worker.datetime', AdvancingClock):
            self.worker.tick()
        self.assertEqual(self.store.get_pending_decisions(batch['id']), [])

    def test_resume_after_source_retention_marks_expired_instead_of_complete(self):
        for i in range(8):
            opened = self.opened + timedelta(seconds=210*i)
            self.fixture.draw(1000+i, opened)
            self.fixture.prediction(1000+i, opened)
        self.now = self.opened + timedelta(hours=1)
        batch = self.batch(mode='backtest')
        first = RuleLabWorker(self.store, self.source, page_size=2)
        first.tick(now=stamp(self.now))
        self.assertEqual(self.store.get_batch(1, batch['id'])['processed_issues'], 2)
        self.fixture.execute("DELETE FROM lottery_draws WHERE issue_no<'1006'")
        self.fixture.execute("DELETE FROM predictions WHERE issue_no<'1006'")
        resumed = RuleLabWorker(self.store, self.source, page_size=2)
        resumed.tick(now=stamp(self.now))
        work = self.store.get_work_item(batch['id'])
        self.assertEqual(work['batch']['status'], 'error')
        self.assertTrue(work['batch']['cursor'].get('input_expired'))
        self.assertEqual(self.store.list_experiments(1, batch['id'])[0]['settled_count'], 2)

    def test_two_backtests_take_turns_under_tight_time_budget(self):
        for i in range(6):
            opened = self.opened + timedelta(seconds=210*i)
            self.fixture.draw(1000+i, opened)
            self.fixture.prediction(1000+i, opened)
        self.now = self.opened + timedelta(hours=1)
        first, second = self.batch(mode='backtest'), self.batch(mode='backtest')
        worker = RuleLabWorker(self.store, self.source, page_size=1)
        visited = []
        def consume_budget(batch_id):
            visited.append(batch_id)
            worker._deadline = 0
        with patch.object(worker, '_replay_backtest', side_effect=consume_budget):
            worker.tick(now=stamp(self.now))
            worker.tick(now=stamp(self.now + timedelta(seconds=1)))
        self.assertEqual(visited, [first['id'], second['id']])

    def test_gap_immediately_before_target_resets_trigger_history(self):
        for issue in (1000, 1001, 1002, 1004):
            opened = self.opened + timedelta(seconds=210*(issue-1000))
            self.fixture.draw(issue, opened)
            self.fixture.prediction(issue, opened)
        self.now = self.opened + timedelta(hours=1)
        batch = self.batch(mode='backtest')
        for _ in range(6):
            self.worker.tick(now=stamp(self.now))
            if self.store.get_batch(1, batch['id'])['status'] == 'completed':
                break
        experiments = self.store.list_experiments(1, batch['id'])
        continuous = [e for e in experiments if e['condition']['type'] == 'draw_streak'
                      and e['condition']['bet_value'] == '大' and e['condition']['threshold'] == 3]
        self.assertTrue(all(e['trigger_count'] == 0 for e in continuous))
        self.assertGreater(self.store.get_batch(1, batch['id'])['skip_counts'].get('draw_gap', 0), 0)

    def test_pruning_runs_once_per_five_minutes_in_bounded_batches(self):
        with patch.object(self.store, 'prune', return_value={'decisions': 500, 'predictions': 0, 'draws': 0}) as prune:
            self.worker.tick(now=stamp(self.now))
            self.worker.tick(now=stamp(self.now + timedelta(seconds=15)))
            self.worker.tick(now=stamp(self.now + timedelta(seconds=299)))
            self.assertEqual(prune.call_count, 1)
            self.worker.tick(now=stamp(self.now + timedelta(seconds=300)))
            self.assertEqual(prune.call_count, 2)
            self.assertTrue(all(call.kwargs['limit'] == 500 for call in prune.call_args_list))

    def test_pruning_continues_after_storage_pressure_pauses_batch(self):
        self.prepare_live()
        batch = self.batch()
        limited = RuleLabStore(self.store.db_path, budget_bytes=1024)
        worker = RuleLabWorker(limited, self.source)
        with patch.object(limited, 'prune', wraps=limited.prune) as prune:
            worker.tick(now=stamp(self.now))
            self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'resource_paused')
            worker.tick(now=stamp(self.now + timedelta(seconds=300)))
            self.assertEqual(prune.call_count, 2)
            self.assertTrue(all(call.kwargs['limit'] == 500 for call in prune.call_args_list))


if __name__ == '__main__':
    unittest.main()
