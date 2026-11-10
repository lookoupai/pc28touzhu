"""Configurable per-window start budgets on real SQLite dispatch/settlement."""
from __future__ import annotations

import copy
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pc28touzhu.domain.auto_trigger_cycle import normalize_cycle_policy
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.auto_trigger_cycle_service import resume_rule_cycle
from pc28touzhu.services.auto_trigger_service import list_auto_trigger_rules, update_auto_trigger_rule, update_auto_trigger_rule_status
from pc28touzhu.services.dispatch_service import dispatch_signal
from tests.test_cycle_runtime import CycleFixture


class ScheduleWindowCyclesTests(CycleFixture):
    def setUp(self):
        super().setUp()
        self.now = datetime(2026, 11, 11, 0, 2, tzinfo=timezone.utc)

    def scheduled(self, *, window_limit=1, daily_limit=3, on_target='wait_condition', on_bust='wait_condition', combined=False):
        rule = self.rule(targets=[self.targets[1]], limit=daily_limit, on_target=on_target, on_bust=on_bust)
        payload = {'trigger_mode':'condition_schedule' if combined else 'schedule', 'schedule':{
            'timezone':'Asia/Shanghai', 'weekdays':['Mon','Tue','Wed','Thu','Fri','Sat','Sun'],
            'windows':[{'id':'primary','start':'08:01','end':'08:59'},
                       {'id':'afternoon','start':'14:01','end':'14:59'},
                       {'id':'evening','start':'20:01','end':'20:59'}], 'signal_max_age_seconds':300}}
        if window_limit is not None:
            payload['cycle_policy'] = {'max_starts_per_window':window_limit}
        return update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.user_id, payload=payload)['item']

    def start(self, rule):
        self.signal()
        result=self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'],1,result)
        return self.session(rule)['active_run_id']

    def end(self, *, loss=False):
        # Structure B: 单 + 大双. A small even result loses both legs.
        self.deliver_and_settle(self.latest+1, triplet=(1,3,4) if loss else (5,8,2))

    def next_window(self, hour):
        self.advance(100)
        self.now=self.now.replace(hour=hour-8,minute=2,second=0)

    def assert_window_blocked(self, rule):
        self.advance(2);self.signal()
        result=self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'],0,result)
        self.assertEqual(result['rules'][0]['events'][0]['reason'],'schedule_window_limit')

    def test_three_windows_one_cycle_each_after_success_or_bust(self):
        rule=self.scheduled()
        first=self.start(rule);self.end()
        self.assert_window_blocked(rule)
        self.next_window(14);second=self.start(rule);self.end(loss=True)
        self.assertEqual(self.session(rule)['status'],'waiting')
        self.assert_window_blocked(rule)
        self.next_window(20);third=self.start(rule);self.end()
        self.assertEqual(len({first,second,third}),3)
        self.assertEqual(self.session(rule)['status'],'day_closed')
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,stat_date=self.day)['starts_count'],3)

    def test_per_window_limit_two_allows_second_but_blocks_third(self):
        rule=self.scheduled(window_limit=2)
        self.start(rule);self.end()
        self.advance(2);self.start(rule);self.end()
        self.assert_window_blocked(rule)
        self.next_window(14);self.start(rule)

    def test_unlimited_window_preserves_daily_quota(self):
        rule=self.scheduled(window_limit=0,daily_limit=2)
        self.start(rule);self.end();self.advance(2);self.start(rule);self.end()
        self.assertEqual(self.session(rule)['status'],'day_closed')
        self.advance(2);self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],0)

    def test_immediate_continuation_obeys_window_limit_and_resumes_next_window(self):
        rule=self.scheduled(window_limit=2,on_target='new_cycle')
        self.start(rule);self.end();self.advance();self.start(rule);self.end()
        self.assertEqual(self.session(rule)['status'],'continuation')
        self.assert_window_blocked(rule)
        self.next_window(14);self.start(rule)

    def test_condition_schedule_uses_same_budget(self):
        rule=self.scheduled(combined=True)
        self.start(rule);self.end();self.assert_window_blocked(rule)
        self.next_window(14);self.signal()
        self.assertEqual(self.cycle(rule,matches=False)['summary']['triggered_count'],0)
        self.assertEqual(self.cycle(rule,matches=True)['summary']['triggered_count'],1)

    def test_missing_new_setting_defaults_to_one(self):
        rule=self.scheduled(window_limit=None)
        with self.repo._connect() as conn:
            conn.execute('UPDATE auto_trigger_rules SET cycle_policy_json=? WHERE id=?',
                ('{"mode":"rule","max_starts_per_day":3,"on_target":"wait_condition","on_bust":"wait_condition"}',rule['id']))
        rule=self.repo.get_auto_trigger_rule(rule['id'])
        self.start(rule);self.end();self.assert_window_blocked(rule)

    def test_missing_signal_or_rolled_back_dispatch_does_not_consume_budget(self):
        rule=self.scheduled()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],0)
        self.signal()
        with patch('pc28touzhu.services.dispatch_service.dispatch_signal',return_value={'created_count':0,'jobs':[]}):
            failed=self.cycle(rule)
        self.assertEqual(failed['summary']['triggered_count'],0)
        self.assertEqual(self.repo.list_auto_trigger_rule_runs_for_subscription_date(rule_id=rule['id'],subscription_id=self.sub['id'],stat_date=self.day),[])
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        self.end();self.assert_window_blocked(rule)

    def test_pause_edit_and_repository_reconnect_do_not_erase_used_slot(self):
        rule=self.scheduled();self.start(rule);self.end()
        update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.user_id,status='inactive')
        update_auto_trigger_rule(self.repo,rule_id=rule['id'],user_id=self.user_id,payload={'name':'renamed','schedule':{**rule['schedule'],'windows':[{**w,'end':'09:00'} if w['id']=='primary' else w for w in rule['schedule']['windows']]}})
        update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.user_id,status='active')
        self.repo=DatabaseRepository(self.repo.db_path)
        rule=self.repo.get_auto_trigger_rule(rule['id'])
        self.assert_window_blocked(rule)

    def test_next_day_has_fresh_slot(self):
        rule=self.scheduled();self.start(rule);self.end()
        first_day=self.day
        self.now+=timedelta(days=1);self.latest+=500
        self.start(rule)
        for day in (first_day,self.day):
            self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,stat_date=day)['starts_count'],1)

    def test_partial_loss_restarts_streak_inside_same_cycle_without_new_quota(self):
        self.strategy['staking_policy'].update(target_streak=3)
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='standby')
        rule=self.scheduled();run_id=self.start(rule)
        # After a profit, 27 refunds the main odd leg but loses the auxiliary leg.
        self.deliver_and_settle(self.latest+1,triplet=(5,8,2))
        self.advance();dispatch_signal(self.repo,self.signal()['id'])
        self.deliver_and_settle(self.latest+1,triplet=(9,9,9))
        state=self.repo.get_auto_trigger_cycle_state(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,route_id=rule['routes'][0]['id'])
        self.assertEqual(state['win_streak'],0)
        self.assertGreater(state['cycle_stake'],2)
        self.assertEqual(state['status'],'running')
        self.assertEqual(self.session(rule)['active_run_id'],run_id)
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,stat_date=self.day)['starts_count'],1)
        # In-progress cycles can continue after their start window ends.
        self.advance();self.now=self.now.replace(hour=1,minute=5)
        self.assertEqual(dispatch_signal(self.repo,self.signal()['id'])['created_count'],2)
        self.assertEqual(self.session(rule)['active_run_id'],run_id)

    def test_concurrent_repository_claims_cannot_reuse_completed_window(self):
        rule=self.scheduled();self.start(rule);self.end()
        def attempt(issue):
            return DatabaseRepository(self.repo.db_path).claim_auto_trigger_cycle_start(
                rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,stat_date=self.day,
                started_issue_no=str(issue),max_starts_per_day=3,policy=rule['cycle_policy'],route_ids=[rule['routes'][0]['id']],window_id='primary')
        with ThreadPoolExecutor(max_workers=2) as pool:
            outcomes=list(pool.map(attempt,[self.latest+10,self.latest+11]))
        self.assertTrue(all(not r['claimed'] and r['reason']=='schedule_window_limit' for r in outcomes),outcomes)

    def test_schedule_summary_names_window_for_roll_run_keys(self):
        rule=self.scheduled();self.start(rule);self.end()
        with patch('pc28touzhu.services.auto_trigger_service._schedule_now',return_value=self.now.astimezone(timezone(timedelta(hours=8)))):
            result=list_auto_trigger_rules(self.repo,user_id=self.user_id,stat_date=self.day)['items'][0]
        self.assertIn('08:01',result['schedule_status'])
        runtime=result['cycle_runtime'][0]
        self.assertEqual(runtime['max_starts_per_window'],1)
        self.assertEqual(runtime['window_starts'][0]['starts_count'],1)

    def test_window_budget_validation_and_rename_preserve_value(self):
        for value in (True,-1,1.5,1001,'2'):
            with self.subTest(value=value),self.assertRaises(ValueError):
                normalize_cycle_policy({'mode':'rule','max_starts_per_window':value},creating=True)
        self.assertEqual(normalize_cycle_policy(None,creating=True)['max_starts_per_window'],1)
        rule=self.scheduled(window_limit=2)
        updated=update_auto_trigger_rule(self.repo,rule_id=rule['id'],user_id=self.user_id,payload={'name':'still two'})['item']
        self.assertEqual(updated['cycle_policy']['max_starts_per_window'],2)

    def test_two_subscriptions_and_two_rules_keep_independent_window_budgets(self):
        rule=self.scheduled()
        other_sub=self.repo.create_subscription_record(user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='standby')
        rule=update_auto_trigger_rule(self.repo,rule_id=rule['id'],user_id=self.user_id,
            payload={'subscription_ids':[self.sub['id'],other_sub['id']]})['item']
        self.signal();self.assertEqual(self.cycle(rule)['summary']['triggered_count'],2)
        self.end();self.advance(2);self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],0)
        another=self.scheduled()
        self.assertEqual(self.cycle(another)['summary']['triggered_count'],1)
        for sub_id in (self.sub['id'],other_sub['id']):
            self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=sub_id,user_id=self.user_id,stat_date=self.day)['starts_count'],1)

    def test_raising_window_limit_changes_next_start_without_clearing_history(self):
        rule=self.scheduled();first=self.start(rule);self.end();self.assert_window_blocked(rule)
        rule=update_auto_trigger_rule(self.repo,rule_id=rule['id'],user_id=self.user_id,payload={'cycle_policy':{'max_starts_per_window':2}})['item']
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        self.assertEqual(self.repo.get_auto_trigger_rule_run(first)['status'],'closed')
        self.end();self.assert_window_blocked(rule)
