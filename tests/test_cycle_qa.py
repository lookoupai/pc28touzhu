"""独立 QA：归属、迁移、午夜、恢复与多投递屏障的真实 SQLite 回放。"""
from __future__ import annotations

import copy
import json
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from tests import test_cycle_repository as repository_fixture
from tests import test_cycle_runtime as runtime_fixture
from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.services.auto_trigger_cycle_service import resume_rule_cycle
from pc28touzhu.services.auto_trigger_service import update_auto_trigger_rule, run_auto_trigger_cycle
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.pc28_auto_settlement_service import _resolve_pending_win_streak_cycles
from pc28touzhu.services.platform_service import (
    restart_subscription_cycle,
    reset_subscription_runtime,
    resolve_pending_pc28_progressions_from_draws,
)


class CycleRepositoryQATests(unittest.TestCase):
    setUp = repository_fixture.CycleRepositoryTests.setUp
    make_rule = repository_fixture.CycleRepositoryTests.make_rule
    owner = repository_fixture.CycleRepositoryTests.owner
    session_owner = repository_fixture.CycleRepositoryTests.session_owner
    claim = repository_fixture.CycleRepositoryTests.claim
    seed_old_cycle = repository_fixture.CycleRepositoryTests.seed_old_cycle

    def test_implicit_route_id_cannot_be_modified_through_explicit_routes(self):
        implicit = self.repo.ensure_implicit_auto_trigger_route(
            rule_id=self.rule['id'], user_id=self.user, delivery_target_id=self.target['id'])
        before = self.repo.get_auto_trigger_rule_route(implicit['id'])
        with self.assertRaises(ValueError):
            with self.repo.transaction() as unit:
                with unit._connect() as con:
                    unit._sync_auto_trigger_rule_routes(
                        con, rule_id=self.rule['id'], user_id=self.user,
                        routes=[{'id': implicit['id'], 'delivery_target_id': self.target['id'], 'status': 'inactive'}],
                        updated_at='2026-11-05T00:00:00Z')
        self.assertEqual(self.repo.get_auto_trigger_rule_route(implicit['id']), before)

    def test_completion_barrier_covers_all_participating_routes(self):
        second_target = self.repo.create_delivery_target_record(
            user_id=self.user, executor_type='telegram_group', target_key='-1003', status='active')
        second_route = self.repo.ensure_implicit_auto_trigger_route(
            rule_id=self.rule['id'], user_id=self.user, delivery_target_id=second_target['id'])
        start = self.repo.claim_auto_trigger_cycle_start(
            **self.session_owner(), stat_date=repository_fixture.DAY, started_issue_no='100',
            max_starts_per_day=2, policy=repository_fixture.POLICY,
            route_ids=[self.route['id'], second_route['id']])
        for route in (self.route, second_route):
            self.repo.upsert_auto_trigger_cycle_state(
                **self.owner(route=route), rule_run_id=start['run']['id'],
                state=repository_fixture.state(status='stopped' if route == self.route else 'running'),
                pending_plan={'issue_no': '101'} if route == second_route else None)
        complete = lambda: self.repo.complete_auto_trigger_cycle_session(
            **self.session_owner(), rule_run_id=start['run']['id'],
            completed_issue_no='101', status='waiting')
        self.assertFalse(complete()['completed'])
        self.repo.upsert_auto_trigger_cycle_state(
            **self.owner(route=second_route), rule_run_id=start['run']['id'], state=repository_fixture.state())
        self.assertFalse(complete()['completed'])
        self.repo.upsert_auto_trigger_cycle_state(
            **self.owner(route=second_route), rule_run_id=start['run']['id'], state=repository_fixture.state(status='stopped'))
        self.assertTrue(complete()['completed'])
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(
            **self.session_owner(), stat_date=repository_fixture.DAY)['starts_count'], 1)

    def test_same_rule_subscriptions_have_independent_daily_quota(self):
        second_sub = self.repo.create_subscription_record(
            user_id=self.user, source_id=self.source['id'], strategy=self.sub['strategy'], status='standby')
        with self.repo._connect() as con:
            con.execute('UPDATE auto_trigger_rules SET subscription_ids_json=? WHERE id=?',
                        (json.dumps([self.sub['id'], second_sub['id']]), self.rule['id']))
        self.assertTrue(self.claim(maximum=1)['claimed'])
        second_owner = {**self.session_owner(), 'subscription_id': second_sub['id']}
        result = self.repo.claim_auto_trigger_cycle_start(
            **second_owner, stat_date=repository_fixture.DAY, started_issue_no='100',
            max_starts_per_day=1, policy=repository_fixture.POLICY, route_ids=[self.route['id']])
        self.assertTrue(result['claimed'])
        for owner in (self.session_owner(), second_owner):
            self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(
                **owner, stat_date=repository_fixture.DAY)['starts_count'], 1)

    def test_ambiguous_pending_remains_blocked_after_legs_settle(self):
        ids, _ = self.seed_old_cycle(conflicting=True)
        self.repo.initialize_database()
        before = self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])
        pending = self.repo.list_pending_win_streak_cycles(user_id=self.user)
        self.assertEqual(pending[0]['scope'], 'blocked')
        self.assertEqual(pending[0]['pending_plan']['leg_event_ids'], ids)
        with self.repo._connect() as con:
            con.execute("UPDATE subscription_progression_events SET status='settled', resolved_result_type='miss', net_delta=-stake_amount WHERE id IN (?,?)", ids)
        _resolve_pending_win_streak_cycles(self.repo)
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id']), before)
        migration = self.repo._fetch_one('SELECT original_state_json FROM cycle_state_migrations WHERE subscription_id=?', (self.sub['id'],))
        self.assertEqual(json.loads(json.loads(migration['original_state_json'])['pending_plan_json'])['leg_event_ids'], ids)

    def test_old_auto_migration_does_not_capture_new_manual_state(self):
        self.seed_old_cycle(pending=False, closed=True)
        self.repo.initialize_database()
        automatic = copy.deepcopy(self.repo.get_auto_trigger_cycle_state(**self.owner()))
        self.repo.upsert_subscription_cycle_state(
            subscription_id=self.sub['id'], user_id=self.user, state=repository_fixture.state(balance=27, day_closed=False))
        self.repo.initialize_database()
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])['balance'], 27)
        self.assertEqual(self.repo.get_auto_trigger_cycle_state(**self.owner()), automatic)

    def test_manual_restart_cannot_erase_legacy_no_route_inflight(self):
        self.seed_old_cycle(route=0)
        self.repo.initialize_database()
        before = self.repo.get_auto_trigger_cycle_state(**{**self.owner(), 'route_id': 0})
        events = self.repo._fetch_all('SELECT * FROM subscription_progression_events ORDER BY id', ())
        for operation in (restart_subscription_cycle, reset_subscription_runtime):
            with self.assertRaises(ValueError):
                operation(self.repo, subscription_id=self.sub['id'], user_id=self.user, payload={})
        self.assertEqual(self.repo.get_auto_trigger_cycle_state(**{**self.owner(), 'route_id': 0}), before)
        self.assertEqual(self.repo._fetch_all('SELECT * FROM subscription_progression_events ORDER BY id', ()), events)

    def test_ambiguous_migration_cannot_restart_automatically_on_next_day(self):
        self.seed_old_cycle(pending=False, closed=True, conflicting=True)
        self.repo.initialize_database()
        before = self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])
        self.assertTrue(before['stop_reason'].startswith('migration_'))
        signal = self.repo.create_signal_record(source_id=self.source['id'], lottery_type='pc28',
            issue_no='102', bet_type='combo', bet_value='小单', published_at='2026-11-06T00:00:00Z')
        with patch('pc28touzhu.services.dispatch_service._shanghai_today', return_value='2026-11-06'), \
             patch('pc28touzhu.services.dispatch_service._utc_now', return_value=datetime(2026, 11, 6, tzinfo=timezone.utc)):
            result = dispatch_signal(self.repo, signal['id'], subscription_id=self.sub['id'])
        self.assertEqual(result['created_count'], 0)
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id']), before)
        with self.assertRaises(ValueError):
            restart_subscription_cycle(self.repo, subscription_id=self.sub['id'], user_id=self.user, payload={})
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id']), before)


class CycleRuntimeQATests(runtime_fixture.CycleFixture):
    def auto_state(self, rule, route=None, sub=None):
        return self.repo.get_auto_trigger_cycle_state(
            rule_id=rule['id'], subscription_id=(sub or self.sub)['id'], user_id=self.user_id,
            route_id=(route or rule['routes'][0])['id'])

    def api(self, path, body, *, user_id=None, authenticated=True):
        app = PlatformApiApplication(repository=self.repo, executor_api_token='qa-local', session_secret='qa-secret')
        headers = {}
        if authenticated:
            headers['HTTP_COOKIE'] = '%s=%s' % (SESSION_COOKIE_NAME, build_session_cookie_value(user_id or self.user_id, 'qa-secret'))
        captured = {}
        def response(status, _headers):
            captured['status'] = int(status.split()[0])
        with patch('pc28touzhu.services.auto_trigger_service._today_stat_date', return_value=self.day):
            result = b''.join(app(build_testing_environ(path, method='POST', body=body, headers=headers), response))
        return captured['status'], json.loads(result)

    def test_quota_close_of_one_subscription_does_not_stop_other_subscription(self):
        second_strategy = copy.deepcopy(self.strategy)
        second_strategy['staking_policy']['target_streak'] = 3
        second_sub = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source['id'], strategy=second_strategy, status='standby', name='另一方案')
        rule = self.rule(targets=[self.targets[1]], limit=1, subscriptions=[self.sub['id'], second_sub['id']])
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 2)
        self.deliver_and_settle(self.latest + 1)
        self.assertEqual(self.session(rule)['status'], 'day_closed')
        second_session = self.repo.get_auto_trigger_cycle_session(
            rule_id=rule['id'], subscription_id=second_sub['id'], user_id=self.user_id)
        self.assertEqual(second_session['status'], 'running')
        self.assertEqual(self.auto_state(rule, sub=second_sub)['win_streak'], 1)
        self.assertNotEqual(self.repo.get_auto_trigger_rule_daily_stat(
            rule_id=rule['id'], user_id=self.user_id, stat_date=self.day)['status'], 'stopped')
        self.advance()
        result = dispatch_signal(self.repo, self.signal()['id'], subscription_id=second_sub['id'])
        self.assertEqual(result['created_count'], 2)

    def test_cross_midnight_keeps_running_cycle_and_attributes_new_round_to_new_day(self):
        self.strategy['staking_policy']['target_streak'] = 2
        self.repo.update_subscription_record(subscription_id=self.sub['id'], user_id=self.user_id,
            source_id=self.source['id'], strategy=self.strategy, status='standby')
        self.now = datetime(2026, 11, 4, 15, 59, tzinfo=timezone.utc)
        first_day = self.day
        rule = self.rule(targets=[self.targets[1]], limit=1)
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        # 投注在封盘前送达；只把开奖结算放到午夜后，避免构造逾期回执。
        for job in self.repo.list_execution_jobs(user_id=self.user_id):
            self.repo.report_job_result(str(job['id']), 'qa', 1, 'delivered', 'qa-%s' % job['id'], self.now.isoformat(), {}, None)
        self.advance()
        second_day = self.day
        self.assertNotEqual(first_day, second_day)
        self.deliver_and_settle(self.latest)
        first_state = self.auto_state(rule)
        self.assertEqual(first_state['stat_date'], first_day)
        self.assertEqual(first_state['win_streak'], 1)
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        next_signal = self.signal()
        self.assertEqual(dispatch_signal(self.repo, next_signal['id'], subscription_id=self.sub['id'])['created_count'], 2)
        self.deliver_and_settle(self.latest + 1)
        self.assertEqual(self.auto_state(rule)['win_streak'], 2)
        self.advance(2)
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        for day in (first_day, second_day):
            self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(
                rule_id=rule['id'], subscription_id=self.sub['id'], user_id=self.user_id, stat_date=day)['starts_count'], 1)

    def test_scoped_resume_rejects_foreign_owner_and_does_not_reset_quota(self):
        rule = self.rule(targets=[self.targets[1]], limit=3, on_target='close_day')
        other_rule = self.rule(targets=[self.targets[2]], limit=3, on_target='close_day')
        self.signal()
        self.assertEqual(self.cycle()['summary']['triggered_count'], 2)
        self.deliver_and_settle(self.latest + 1)
        before_other = copy.deepcopy(self.session(other_rule))
        foreign = self.repo.create_user_record(username='foreign-qa', status='active')['id']
        with self.assertRaises(ValueError):
            resume_rule_cycle(self.repo, rule_id=rule['id'], user_id=foreign, subscription_id=self.sub['id'], now=self.now)
        jobs_before = self.repo.list_execution_jobs(user_id=self.user_id)
        result = resume_rule_cycle(self.repo, rule_id=rule['id'], user_id=self.user_id, subscription_id=self.sub['id'], now=self.now)
        self.assertTrue(result['resumed'])
        self.assertEqual(self.session(rule)['status'], 'waiting')
        self.assertEqual(self.session(other_rule), before_other)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), jobs_before)
        self.assertEqual(result['cycle_runtime']['starts_today'], 1)

    def test_increasing_quota_does_not_automatically_unseal_closed_session(self):
        rule = self.rule(targets=[self.targets[1]], limit=1)
        self.signal()
        self.cycle(rule)
        self.deliver_and_settle(self.latest + 1)
        update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.user_id,
            payload={'cycle_policy': {'max_starts_per_day': 3}})
        self.advance(2)
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        self.assertEqual(self.session(rule)['status'], 'day_closed')
        resume_rule_cycle(self.repo, rule_id=rule['id'], user_id=self.user_id, subscription_id=self.sub['id'], now=self.now)
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)

    def test_slow_route_keeps_session_running_until_its_two_legs_finish(self):
        rule = self.rule(targets=self.targets[1:])
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        issue = self.latest + 1
        for route in rule['routes']:
            for job in self.repo.list_execution_jobs(user_id=self.user_id):
                if job['auto_trigger_route_id'] == route['id']:
                    self.repo.report_job_result(str(job['id']), 'qa', 1, 'delivered', 'qa-%s' % job['id'], self.now.isoformat(), {}, None)
            resolve_pending_pc28_progressions_from_draws(self.repo, user_id=self.user_id,
                draw_items=[{'issue_no': str(issue), 'draw_context': {'triplet': [5, 8, 2]}}])
            _resolve_pending_win_streak_cycles(self.repo)
            if route == rule['routes'][0]:
                self.assertEqual(self.session(rule)['status'], 'running')
                self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        self.assertEqual(self.session(rule)['status'], 'waiting')

    def test_scope_change_during_prediction_fetch_cannot_start_removed_subscription(self):
        second_sub = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source['id'], strategy=self.strategy, status='standby', name='新范围方案')
        rule = self.rule(targets=[self.targets[1]])
        self.signal()

        def fetcher(_url):
            update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.user_id,
                payload={'subscription_ids': [second_sub['id']]})
            return {'latest_settled_issue': str(self.latest), 'metrics': {'combo': {'streaks': {'current_miss_streak': 2}}}}

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule['id'],
            now=self.now, draw_clock={'latest_issue_no': str(self.latest),
                'latest_open_time': (self.now-timedelta(seconds=20)).isoformat(),
                'fetched_at': self.now.isoformat(), 'stale': False}, fetcher=fetcher)
        self.assertEqual(result['summary']['triggered_count'], 0)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])

    def test_condition_change_during_prediction_fetch_uses_current_threshold(self):
        rule = self.rule(targets=[self.targets[1]])
        self.signal()

        def fetcher(_url):
            update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.user_id,
                payload={'conditions': [{'type': 'miss_streak', 'metric': 'combo', 'operator': 'gte', 'threshold': 10}]})
            return {'latest_settled_issue': str(self.latest), 'metrics': {'combo': {'streaks': {'current_miss_streak': 2}}}}

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule['id'],
            now=self.now, draw_clock={'latest_issue_no': str(self.latest),
                'latest_open_time': (self.now-timedelta(seconds=20)).isoformat(),
                'fetched_at': self.now.isoformat(), 'stale': False}, fetcher=fetcher)
        self.assertEqual(result['summary']['triggered_count'], 0)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])

    def test_resume_api_denies_anonymous_foreign_and_out_of_scope_without_mutation(self):
        rule = self.rule(targets=[self.targets[1]], limit=3, on_target='close_day')
        self.signal()
        self.cycle(rule)
        self.deliver_and_settle(self.latest + 1)
        before = copy.deepcopy(self.session(rule))
        foreign = self.repo.create_user_record(username='foreign-api', status='active')['id']
        other_sub = self.repo.create_subscription_record(user_id=self.user_id, source_id=self.source['id'],
            strategy=self.strategy, status='standby', name='不在规则中')
        path = '/api/platform/auto-trigger-rules/%s/resume-cycle' % rule['id']
        self.assertEqual(self.api(path, {'subscription_id': self.sub['id']}, authenticated=False)[0], 401)
        self.assertEqual(self.api(path, {'subscription_id': self.sub['id']}, user_id=foreign)[0], 400)
        self.assertEqual(self.api(path, {'subscription_id': other_sub['id']})[0], 400)
        for invalid in (True, 0, 1.5, '1', '1 OR 1=1'):
            self.assertEqual(self.api(path, {'subscription_id': invalid})[0], 400)
        self.assertEqual(self.session(rule), before)
        code, payload = self.api(path, {'subscription_id': self.sub['id']})
        self.assertEqual(code, 200, payload)
        self.assertEqual(payload['cycle_runtime']['starts_today'], 1)
        self.assertEqual(payload['cycle_runtime']['status'], 'waiting')

    def test_resume_api_rejects_running_cycle_and_full_daily_quota(self):
        rule = self.rule(targets=[self.targets[1]], limit=1)
        self.signal()
        self.cycle(rule)
        path = '/api/platform/auto-trigger-rules/%s/resume-cycle' % rule['id']
        code, payload = self.api(path, {'subscription_id': self.sub['id']})
        self.assertEqual(code, 400, payload)
        self.assertEqual(self.session(rule)['status'], 'running')
        self.deliver_and_settle(self.latest + 1)
        before = copy.deepcopy(self.session(rule))
        code, payload = self.api(path, {'subscription_id': self.sub['id']})
        self.assertEqual(code, 400, payload)
        self.assertEqual(self.session(rule), before)

    def test_scheduled_roll_can_restart_inside_window_when_budget_allows(self):
        rule = self.rule(targets=[self.targets[1]], limit=2)
        rule = update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.user_id,
            payload={'cycle_policy': {'max_starts_per_window': 2}, 'trigger_mode': 'schedule', 'schedule': {'timezone': 'Asia/Shanghai',
                'windows': [{'id': 'evening', 'start': '18:00', 'end': '19:00'}],
                'signal_max_age_seconds': 300}})['item']
        self.signal()
        self.assertEqual(self.cycle(rule, matches=False)['summary']['triggered_count'], 1)
        first_id = self.session(rule)['active_run_id']
        self.deliver_and_settle(self.latest + 1)
        self.advance(2)
        self.signal()
        self.assertEqual(self.cycle(rule, matches=False)['summary']['triggered_count'], 1)
        self.assertNotEqual(self.session(rule)['active_run_id'], first_id)
        self.deliver_and_settle(self.latest + 1)
        self.assertEqual(self.session(rule)['status'], 'day_closed')

    def test_scheduled_roll_rejects_signal_published_before_window(self):
        rule = self.rule(targets=[self.targets[1]], limit=2)
        rule = update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.user_id,
            payload={'trigger_mode': 'schedule', 'schedule': {'timezone': 'Asia/Shanghai',
                'windows': [{'id': 'evening', 'start': '18:00', 'end': '19:00'}],
                'signal_max_age_seconds': 300}})['item']
        self.repo.create_signal_record(source_id=self.source['id'], lottery_type='pc28',
            issue_no=str(self.latest + 1), bet_type='combo', bet_value='小单',
            published_at=(self.now - timedelta(seconds=60)).isoformat())
        result = self.cycle(rule, matches=False)
        self.assertEqual(result['summary']['triggered_count'], 0)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])

    def test_scheduled_roll_rejects_outside_window_and_overage_signal(self):
        rule = self.rule(targets=[self.targets[1]], limit=2)
        rule = update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.user_id,
            payload={'trigger_mode': 'schedule', 'schedule': {'timezone': 'Asia/Shanghai',
                'windows': [{'id': 'evening', 'start': '18:00', 'end': '19:00'}],
                'signal_max_age_seconds': 60}})['item']
        self.now = datetime(2026, 11, 4, 10, 10, tzinfo=timezone.utc)
        self.repo.create_signal_record(source_id=self.source['id'], lottery_type='pc28',
            issue_no=str(self.latest + 1), bet_type='combo', bet_value='小单',
            published_at=(self.now - timedelta(seconds=120)).isoformat())
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        self.now = datetime(2026, 11, 4, 11, 1, tzinfo=timezone.utc)
        self.signal(issue=self.latest + 2)
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(
            rule_id=rule['id'], subscription_id=self.sub['id'], user_id=self.user_id, stat_date=self.day)['starts_count'], 0)


if __name__ == '__main__':
    unittest.main()
