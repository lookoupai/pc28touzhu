"""条件与时间必须同时满足；只用临时 SQLite 和固定时钟。"""
from datetime import timedelta
from unittest.mock import Mock, patch

from pc28touzhu.services.auto_trigger_service import (
    _rule_time_gate,
    clear_auto_trigger_performance_cache,
    normalize_rule_payload,
    run_auto_trigger_cycle,
    update_auto_trigger_rule,
)
from tests.test_cycle_runtime import CycleFixture


class CombinedTriggerChecks:
    def setUp(self):
        super().setUp()
        clear_auto_trigger_performance_cache()
        self.addCleanup(clear_auto_trigger_performance_cache)
        if self.fixed:
            self.strategy['staking_policy'] = {'mode': 'fixed', 'fixed_amount': 10}
            self.sub = self.repo.update_subscription_record(
                subscription_id=self.sub['id'], user_id=self.user_id,
                source_id=self.source['id'], status='standby', strategy=self.strategy,
            )
        self.schedule = {'timezone': 'Asia/Shanghai', 'weekdays': ['Wed'],
                         'windows': [{'id': 'evening', 'start': '18:00', 'end': '24:00'}],
                         'signal_max_age_seconds': 300}

    def combined_rule(self, **kwargs):
        rule = self.rule(**kwargs)
        return update_auto_trigger_rule(self.repo, user_id=self.user_id, rule_id=rule['id'],
            payload={'trigger_mode': 'condition_schedule', 'schedule': self.schedule})['item']

    def test_before_start_weekday_and_midnight_do_not_fetch_or_start(self):
        rule = self.combined_rule(dispatch=False)
        for offset, reason in [(-1, 'outside_schedule_window'), (6 * 3600, 'schedule_weekday_blocked'), (24 * 3600, 'schedule_weekday_blocked')]:
            with self.subTest(offset=offset):
                fetcher = Mock(side_effect=AssertionError('窗口外不查询条件'))
                result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule['id'],
                    now=self.now + timedelta(seconds=offset), fetcher=fetcher, draw_fetcher=fetcher)
                self.assertEqual(result['summary']['triggered_count'], 0)
                self.assertEqual(result['summary']['failed_count'], 0)
                self.assertEqual(result['rules'][0]['events'][0]['reason'], reason)
                fetcher.assert_not_called()
        self.assertIsNone(self.session(rule))
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])
        self.assertEqual(self.repo.list_active_auto_trigger_rule_runs(rule_id=rule['id'], user_id=self.user_id), [])
        self.assertEqual(self.repo.get_subscription(self.sub['id'])['status'], 'standby')

    def test_start_boundary_still_requires_conditions_then_dispatches(self):
        rule = self.combined_rule()
        self.signal()
        self.assertEqual(self.cycle(rule, matches=False)['summary']['triggered_count'], 0)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])
        clear_auto_trigger_performance_cache()
        result = self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'], 1, result)
        event = result['rules'][0]['events'][0]
        self.assertEqual(event['reason'], 'conditions_matched')
        self.assertEqual(event['snapshot']['window_id'], 'evening')
        self.assertTrue(event['matched_conditions'])
        self.assertTrue(self.repo.list_execution_jobs(user_id=self.user_id))

    def test_guard_groups_are_required_inside_window(self):
        rule = self.combined_rule(dispatch=False)
        rule = update_auto_trigger_rule(self.repo, user_id=self.user_id, rule_id=rule['id'], payload={
            'guard_groups': [{'name': '连挂更高', 'conditions': [
                {'type': 'miss_streak', 'metric': 'combo', 'operator': 'gte', 'threshold': 3}]}],
        })['item']
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])

    def test_signal_before_window_cannot_start_then_fresh_signal_can(self):
        rule = self.combined_rule()
        self.now -= timedelta(seconds=1)
        old = self.signal()
        self.now += timedelta(seconds=1)
        blocked = self.cycle(rule)
        self.assertEqual(blocked['rules'][0]['events'][0]['reason'], 'schedule_signal_before_window')
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])
        with self.repo._connect() as conn:
            conn.execute('UPDATE normalized_signals SET published_at=? WHERE id=?', (self.now.isoformat(), old['id']))
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)

    def test_stale_signal_does_not_start(self):
        rule = self.combined_rule()
        self.signal()
        self.now += timedelta(seconds=301)
        result = self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'], 0)
        self.assertEqual(result['rules'][0]['events'][0]['reason'], 'schedule_signal_stale')
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])

    def test_end_exclusive_and_fetch_crossing_window_rechecks_before_start(self):
        self.schedule['windows'][0]['end'] = '18:01'
        rule = self.combined_rule(dispatch=False)
        before = _rule_time_gate(rule, now=self.now)
        after = _rule_time_gate(rule, now=self.now + timedelta(minutes=1))
        self.assertTrue(before['allowed'])
        self.assertFalse(after['allowed'])
        with patch('pc28touzhu.services.auto_trigger_service._rule_time_gate', side_effect=[before, after]):
            result = self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'], 0)
        self.assertEqual(result['rules'][0]['events'][0]['reason'], 'outside_schedule_window')
        self.assertEqual(self.repo.list_active_auto_trigger_rule_runs(rule_id=rule['id'], user_id=self.user_id), [])


class CombinedFixedTriggerTests(CombinedTriggerChecks, CycleFixture):
    fixed = True


class CombinedRollTriggerTests(CombinedTriggerChecks, CycleFixture):
    fixed = False

    def test_continuation_waits_for_next_allowed_window(self):
        self.schedule['weekdays'] = ['Wed', 'Thu']
        self.schedule['windows'][0]['end'] = '18:01'
        rule = self.combined_rule(on_target='new_cycle')
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        self.deliver_and_settle(self.latest + 1)
        self.assertEqual(self.session(rule)['status'], 'continuation')
        self.advance()
        self.signal()
        result = self.cycle(rule, matches=False)
        self.assertEqual(result['summary']['triggered_count'], 0)
        self.assertEqual(result['rules'][0]['events'][0]['reason'], 'outside_schedule_window')
        self.now = self.now.replace(hour=10, minute=0, second=0) + timedelta(days=1)
        self.latest += 400
        self.signal()
        self.assertEqual(self.cycle(rule, matches=False)['summary']['triggered_count'], 1)

    def test_running_round_keeps_betting_after_window_ends(self):
        from pc28touzhu.services.dispatch_service import dispatch_signal
        self.strategy['staking_policy']['target_streak'] = 3
        self.repo.update_subscription_record(subscription_id=self.sub['id'], user_id=self.user_id,
            source_id=self.source['id'], status='standby', strategy=self.strategy)
        self.schedule['windows'][0]['end'] = '18:01'
        rule = self.combined_rule()
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        self.deliver_and_settle(self.latest + 1)
        self.advance()
        signal = self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        self.assertEqual(dispatch_signal(self.repo, signal['id'])['created_count'], 2)
        self.assertEqual(self.session(rule)['status'], 'running')

    def test_normalization_validates_both_parts_and_midnight(self):
        rule = self.combined_rule(dispatch=False)
        for changes in [{'conditions': []}, {'schedule': {'windows': []}},
                        {'schedule': {'windows': [{'start': '24:00', 'end': '24:00'}]}},
                        {'schedule': {'windows': [{'start': '18:00', 'end': '24:01'}]}},
                        {'schedule': {'windows': [{'start': '18:00', 'end': '17:00'}]}}]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                normalize_rule_payload(changes, current=rule)
        self.assertEqual(rule['schedule']['windows'][0]['end'], '24:00')
        self.assertTrue(_rule_time_gate(rule, now=self.now + timedelta(hours=5, minutes=59, seconds=59))['allowed'])
        changed = update_auto_trigger_rule(self.repo, user_id=self.user_id, rule_id=rule['id'], payload={'name': '改名保留配置'})['item']
        self.assertEqual(changed['schedule'], rule['schedule'])
        self.assertEqual(changed['conditions'], rule['conditions'])
