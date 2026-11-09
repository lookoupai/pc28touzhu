from __future__ import annotations

import unittest
import tempfile
from pathlib import Path
from datetime import datetime, timezone, timedelta

from pc28touzhu.domain.win_streak_roll import normalize_win_streak_roll_policy, create_cycle_state, build_period_plan, settle_period
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.platform_service import resolve_pending_pc28_progressions_from_draws
from pc28touzhu.services.pc28_auto_settlement_service import _resolve_pending_win_streak_cycles
from tests.test_cycle_runtime import CycleFixture


class GenericRollEngineTests(unittest.TestCase):
    def policy(self, **kw):
        return normalize_win_streak_roll_policy({'structure': 'single_play', 'play_type': 'big_small',
            'selection_mode': 'follow', 'base_amount': 100, **kw})

    def test_single_size_uses_entire_integer_stake(self):
        policy = self.policy(base_amount=101.75)
        plan = build_period_plan(policy, create_cycle_state(policy), {'bet_type': 'big_small', 'bet_value': '大'})
        self.assertEqual(plan['stake_total'], 101)
        self.assertEqual(plan['main']['bet_type'], 'big_small')
        self.assertNotIn('aux', plan)

    def test_fixed_big_does_not_follow_small_signal(self):
        policy = self.policy(selection_mode='fixed', fixed_value='大')
        plan = build_period_plan(policy, create_cycle_state(policy), {'bet_type': 'big_small', 'bet_value': '小'})
        self.assertEqual(plan['main']['bet_value'], '大')

    def test_kill_group_uses_equal_integer_legs(self):
        policy = self.policy(play_type='kill_group', target_streak=5)
        signal = {'bet_type': 'kill_group', 'bet_value': '小双'}
        plan = build_period_plan(policy, create_cycle_state(policy), signal)
        self.assertEqual(plan['stake_total'], 99)
        self.assertEqual(plan['main']['bet_type'], 'kill_group')
        outcome = settle_period(policy, create_cycle_state(policy), {'result_number': 13}, signal=signal,
                                settlement_rule_id='pc28_fullpay_2_0_regular')
        self.assertEqual(outcome['net_pnl'], -66)
        self.assertEqual(outcome['state']['win_streak'], 0)
        self.assertEqual(outcome['state']['balance'], 34)

    def test_single_bet_can_continue_with_one_unit(self):
        policy = self.policy(base_amount=1)
        plan = build_period_plan(policy, create_cycle_state(policy), {'bet_type': 'big_small', 'bet_value': '大'})
        self.assertEqual(plan['stake_total'], 1)

    def test_invalid_generic_direction_is_rejected(self):
        for kw in ({'play_type': 'bad'}, {'selection_mode': 'fixed', 'fixed_value': '单'},
                   {'play_type': 'kill_group', 'base_amount': 2}, {'play_type': 'number', 'selection_mode': 'fixed', 'fixed_value': '28'}):
            with self.subTest(kw=kw), self.assertRaises(ValueError):
                self.policy(**kw)

    def test_number_zero_is_valid(self):
        policy = self.policy(play_type='number', selection_mode='fixed', fixed_value='0')
        out = settle_period(policy, create_cycle_state(policy), {'result_number': 0},
                            signal={'bet_type': 'number', 'bet_value': '12'})
        self.assertGreater(out['net_pnl'], 0)


class GenericRollDispatchTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.repo = DatabaseRepository(str(Path(tmp.name)/'test.db'))
        self.repo.initialize_database()
        self.uid = self.repo.create_user('roll-user')
        self.source = self.repo.create_source_record(owner_user_id=self.uid, source_type='internal_ai', name='source')
        self.target = self.repo.create_delivery_target_record(user_id=self.uid, executor_type='telegram_group', target_key='-1001', status='active')

    def subscription(self, metric, **kw):
        return self.repo.create_subscription_record(user_id=self.uid, source_id=self.source['id'], strategy={
            'play_filter': {'mode': 'all'}, 'staking_policy': {'mode': 'win_streak_roll',
                'structure': 'single_play', 'play_type': metric, 'selection_mode': 'follow',
                'base_amount': 100, 'target_streak': 3, **kw},
            'settlement_policy': {'rule_source': 'subscription_fixed', 'settlement_rule_id': 'pc28_fullpay_2_0_regular'},
        })

    def signal(self, metric, value, issue='1001', payload=None):
        return self.repo.create_signal_record(source_id=self.source['id'], lottery_type='pc28', issue_no=issue,
            bet_type=metric, bet_value=value, normalized_payload=payload or {})

    def settle(self, job, number, issue='1001'):
        self.repo.report_job_result(str(job['id']), 'offline', 1, 'delivered', 'fake-only', datetime.now(timezone.utc).isoformat(), {}, None)
        result = resolve_pending_pc28_progressions_from_draws(self.repo, user_id=self.uid,
            draw_items=[{'issue_no': issue, 'draw_context': {'result_number': number}}])
        _resolve_pending_win_streak_cycles(self.repo)
        return result

    def test_big_three_gates_is_one_bet_and_stops(self):
        sub = self.subscription('big_small', selection_mode='fixed', fixed_value='大')
        for i in range(3):
            signal = self.signal('big_small', '小', str(1001+i))
            outcome = dispatch_signal(self.repo, signal['id'], subscription_id=sub['id'])
            self.assertEqual(outcome['created_count'], 1)
            self.assertEqual(outcome['jobs'][0]['planned_message_text'], '大'+str(100*2**i))
            self.settle(outcome['jobs'][0], 20, str(1001+i))
        state = self.repo.get_subscription_cycle_state(subscription_id=sub['id'])
        self.assertEqual((state['win_streak'], state['balance'], state['status']), (3, 800, 'stopped'))
        self.assertEqual(dispatch_signal(self.repo, self.signal('big_small','小','1004')['id'], subscription_id=sub['id'])['created_count'], 0)

    def test_kill_five_uses_single_frozen_group_event(self):
        sub = self.subscription('kill_group', target_streak=5)
        signal = self.signal('kill_group', '小双')
        result = dispatch_signal(self.repo, signal['id'], subscription_id=sub['id'])
        self.assertEqual(result['created_count'], 1)
        job = result['jobs'][0]
        self.assertEqual(job['planned_message_text'], '大单33 大双33 小单33')
        self.assertEqual(dispatch_signal(self.repo, signal['id'], subscription_id=sub['id'])['created_count'], 0)
        self.settle(job, 13)
        event = self.repo.get_progression_event(job['progression_event_id'])
        self.assertEqual(event['net_delta'], -66)
        self.assertTrue(event['prediction_hit'])
        state = self.repo.get_subscription_cycle_state(subscription_id=sub['id'])
        self.assertEqual((state['balance'],state['win_streak'],state['target_streak']), (34,0,5))

    def test_two_subscriptions_keep_independent_cycle_balances(self):
        size = self.subscription('big_small')
        group = self.subscription('double_group', target_streak=10)
        a = dispatch_signal(self.repo, self.signal('big_small','大')['id'], subscription_id=size['id'])['jobs'][0]
        b = dispatch_signal(self.repo, self.signal('double_group','大单+小双')['id'], subscription_id=group['id'])['jobs'][0]
        self.settle(a,20)
        self.settle(b,20)
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=size['id'])['balance'],200)
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=group['id'])['balance'],0)

    def test_filter_skips_opposite_without_consuming_capital(self):
        sub = self.subscription('big_small',selection_mode='filter',fixed_value='大')
        outcome = dispatch_signal(self.repo,self.signal('big_small','小')['id'],subscription_id=sub['id'])
        self.assertEqual(outcome['created_count'],0)
        self.assertIsNone(self.repo.get_subscription_cycle_state(subscription_id=sub['id']))

    def test_fixed_group_clears_original_derivation(self):
        sub = self.subscription('kill_group',selection_mode='fixed',fixed_value='大双')
        signal = self.signal('kill_group','小双',payload={'selected_combos':['大单','大双','小单'],
            'excluded_combo':'小双','group_derivation':'opposite_combo_v1','origin_combo':'大单'})
        job = dispatch_signal(self.repo,signal['id'],subscription_id=sub['id'])['jobs'][0]
        self.assertEqual(job['planned_message_text'],'大单33 小单33 小双33')
        self.settle(job,14)
        self.assertEqual(self.repo.get_progression_event(job['progression_event_id'])['net_delta'],-99)

    def test_expired_single_roll_job_cannot_be_retried(self):
        sub=self.subscription('big_small')
        job=dispatch_signal(self.repo,self.signal('big_small','大')['id'],subscription_id=sub['id'])['jobs'][0]
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET status='expired', expire_at='2020-01-01T00:00:00Z' WHERE id=?",(job['id'],))
        with self.assertRaises(ValueError):
            self.repo.retry_execution_job(job_id=job['id'],user_id=self.uid)

    def test_auto_retry_preserves_single_roll_deadline(self):
        sub=self.subscription('big_small')
        job=dispatch_signal(self.repo,self.signal('big_small','大')['id'],subscription_id=sub['id'])['jobs'][0]
        self.repo.report_job_result(str(job['id']),'offline',1,'failed',None,
            (datetime.now(timezone.utc)-timedelta(seconds=20)).isoformat(),{},'failed')
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET execute_after='2020-01-01T00:00:00Z' WHERE id=?",(job['id'],))
        self.repo.requeue_auto_retry_jobs(max_attempts=3,base_delay_seconds=5)
        self.assertEqual(self.repo.get_execution_job(job['id'])['expire_at'],job['expire_at'])

    def test_manual_cycle_keeps_original_play_until_restart(self):
        sub=self.subscription('big_small')
        first=dispatch_signal(self.repo,self.signal('big_small','大')['id'],subscription_id=sub['id'])['jobs'][0]
        self.settle(first,20)
        changed={**sub['strategy_v2'],'staking_policy':{**sub['strategy_v2']['staking_policy'],'play_type':'kill_group'}}
        self.repo.update_subscription_record(subscription_id=sub['id'],user_id=self.uid,source_id=self.source['id'],strategy=changed)
        result=dispatch_signal(self.repo,self.signal('big_small','大','1002')['id'],subscription_id=sub['id'])
        self.assertEqual(result['created_count'],1)
        self.assertEqual(result['jobs'][0]['planned_message_text'],'大200')

    def test_number_selection_supports_fixed_and_martingale(self):
        for mode in ('fixed','martingale'):
            sub=self.repo.create_subscription_record(user_id=self.uid,source_id=self.source['id'],strategy={
                'play_filter':{'mode':'selected','selected_keys':['number:0']},
                'staking_policy':{'mode':mode,'fixed_amount':10,'base_stake':10,'multiplier':2,'max_steps':3},
                'settlement_policy':{'rule_source':'subscription_fixed','settlement_rule_id':'pc28_fullpay_netdisk_regular'}})
            issue='2000' if mode=='fixed' else '2001'
            job=dispatch_signal(self.repo,self.signal('number','0',issue)['id'],subscription_id=sub['id'])['jobs'][0]
            self.settle(job,0,issue)
            self.assertEqual(self.repo.get_progression_event(job['progression_event_id'])['net_delta'],9990)


class GenericAutoRollTests(CycleFixture):
    def test_group_cycle_continues_with_frozen_strategy(self):
        self.strategy['staking_policy'].update(structure='single_play',play_type='kill_group',selection_mode='follow',target_streak=2,base_amount=100)
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='standby')
        rule=self.rule(targets=[self.targets[1]],limit=1)
        signal=self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no=str(self.latest+1),bet_type='kill_group',bet_value='小双',published_at=self.now.isoformat())
        self.signal()  # 最新条是 combo，启动时必须换到同一期 kill_group。
        result=self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'],1,result)
        jobs=self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs),1)
        self.deliver_and_settle(self.latest+1)
        self.assertEqual(self.session(rule)['status'],'running')
        self.advance()
        self.strategy['staking_policy'].update(play_type='big_small',selection_mode='fixed',fixed_value='小')
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='standby')
        signal=self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no=str(self.latest+1),bet_type='kill_group',bet_value='小双',published_at=self.now.isoformat())
        outcome=dispatch_signal(self.repo,signal['id'])
        self.assertEqual(outcome['created_count'],1,outcome)
        self.deliver_and_settle(self.latest+1)
        self.assertEqual(self.session(rule)['status'],'day_closed')


if __name__ == '__main__':
    unittest.main()
