"""Independent recovery acceptance checks; temporary SQLite and no external sends."""
from __future__ import annotations

import copy
import json
import sqlite3
from datetime import datetime, timedelta, timezone

from pc28touzhu.domain.win_streak_roll import create_cycle_state
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.pc28_auto_settlement_service import _resolve_pending_win_streak_cycles
from pc28touzhu.services.platform_service import resolve_pending_pc28_progressions_from_draws
from pc28touzhu.services.auto_trigger_service import update_auto_trigger_rule
from tests.test_cycle_recovery import RecoveryFixture
from tests import test_cycle_recovery_api as api_cases


class CycleRecoveryIndependentTests(RecoveryFixture):
    def current_jobs(self, rule):
        run_id = self.session(rule)['active_run_id']
        return [job for job in self.jobs(rule)
                if self.repo.get_progression_event(job['progression_event_id'])['auto_trigger_rule_run_id'] == run_id]

    def unknown_current(self, rule):
        jobs = self.current_jobs(rule)
        leaders = [job for job in jobs if not job.get('send_with_job_id')]
        for job in leaders:
            self.repo.report_job_result(str(job['id']), 'qa-fixture', 1, 'unconfirmed', None,
                                       self.now.isoformat(), {}, 'offline unknown result')
        return jobs

    def next_start(self, rule):
        self.advance()
        self.signal()
        result = self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'], 1, result)
        return self.session(rule)['active_run_id']

    def archive_current(self, rule):
        self.unknown_current(rule)
        self.expire()
        self.assertEqual(self.detach()['detached_count'], 1)
        return self.archives(rule)[0]

    def accepted_old_draw(self, archive, triplet=(2, 3, 5)):
        # An explicit late legacy transport receipt is external evidence; no send occurs.
        event_ids = archive['pending_plan']['leg_event_ids']
        with self.repo._connect() as conn:
            leader = conn.execute('''SELECT id FROM execution_jobs
                WHERE progression_event_id IN (%s) AND send_with_job_id IS NULL''' %
                ','.join('?' for _ in event_ids), event_ids).fetchone()['id']
        self.repo.report_job_result(str(leader), 'qa-fixture', 2, 'delivered', 'qa-late-100',
                                   self.now.isoformat(), {}, None)
        result = resolve_pending_pc28_progressions_from_draws(
            self.repo, user_id=self.user_id,
            draw_items=[{'issue_no': archive['issue_no'], 'draw_context': {'triplet': list(triplet)}}],
        )
        self.assertEqual(result['summary']['resolved_count'], len(event_ids))
        return result

    def test_incident_amounts_use_real_first_period_and_reserve_exactly_once(self):
        rule = self.rule(targets=[self.targets[1]], limit=4)
        route = rule['routes'][0]
        previous = create_cycle_state({'roll_mode': 'chain', 'base_amount': 10, 'target_streak': 3},
                                      stat_date=self.day, account_balance=18.46)
        previous.update(status='stopped', stop_reason='previous_cycle_complete')
        self.repo.upsert_auto_trigger_cycle_state(
            rule_id=rule['id'], subscription_id=self.sub['id'], route_id=route['id'], user_id=self.user_id,
            rule_run_id=None, state=previous, policy={}, pending_plan=None,
        )
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        self.deliver_and_settle(self.latest + 1, triplet=(6, 4, 5))
        won = self.state(rule)
        self.assertEqual((won['balance'], won['cycle_stake'], won['win_streak']), (22.39, 13.93, 1))
        self.advance()
        signal = self.signal()
        dispatched = dispatch_signal(self.repo, signal['id'], subscription_id=self.sub['id'], draw_clock={
            'latest_issue_no': str(self.latest), 'latest_open_time': (self.now-timedelta(seconds=20)).isoformat(),
            'stale': False,
        })
        self.assertEqual(dispatched['created_count'], 2)
        pending = self.state(rule)['pending_plan']
        self.assertEqual(pending['stake_total'], 13)
        newest = next(job for job in self.current_jobs(rule)
                      if job['issue_no'] == str(self.latest+1) and not job.get('send_with_job_id'))
        self.repo.report_job_result(str(newest['id']), 'qa-fixture', 1, 'unconfirmed', None,
                                   self.now.isoformat(), {}, 'offline unknown')
        self.expire()
        self.assertEqual(self.detach()['detached_count'], 1)
        self.assertEqual(self.state(rule)['balance'], 9.39)
        self.assertEqual(self.archives(rule)[0]['reserved_stake'], 13)
        self.assertEqual(self.state(rule)['day_pnl'], 3.93)
        self.assertEqual(self.detach()['detached_count'], 0)
        next_run = self.next_start(rule)
        audit = self.repo.get_auto_trigger_rule_run(next_run)['capital_audit']['routes'][str(route['id'])]
        self.assertEqual((audit['carry_balance'], audit['top_up_amount'], audit['applied_credit']), (9.39, .61, 0))
        self.assertEqual(self.state(rule)['balance'], 10)
        self.assertEqual(self.archives(rule)[0]['reserved_stake'], 13)

    def test_two_old_returns_accumulate_without_touching_current_then_apply_once(self):
        rule = self.start(limit=6)
        first = self.archive_current(rule)
        self.next_start(rule)
        second = self.archive_current(rule)
        self.next_start(rule)
        current = copy.deepcopy(self.state(rule))
        for archive in (second, first):
            self.void_archived(archive)
            self.assertEqual(self.resolve_archives()['resolved_count'], 1)
            self.assertEqual(self.state(rule), current)
        self.assertEqual(self.resolve_archives()['resolved_count'], 0)
        third = self.archive_current(rule)
        new_run = self.next_start(rule)
        audit = self.repo.get_auto_trigger_rule_run(new_run)['capital_audit']['routes'][str(rule['routes'][0]['id'])]
        self.assertEqual(audit['applied_credit'], 20)
        self.assertCountEqual(audit['credit_archive_ids'], [first['id'], second['id']])
        self.assertEqual(self.state(rule)['balance'], 20)
        self.assertEqual(self.state(rule)['cycle_stake'], 10)
        self.assertEqual(sum(a['reserved_stake'] for a in self.archives(rule)), third['initial_reserved_stake'])
        self.assertEqual([a['credit_applied_run_id'] for a in self.archives(rule) if a['id'] in {first['id'], second['id']}],
                         [new_run, new_run])

    def test_chain_all_in_chain_roundtrip_keeps_credit_and_separates_independent_return(self):
        rule = self.start(limit=6)
        chain_archive = self.archive_current(rule)
        self.void_archived(chain_archive)
        self.resolve_archives()
        self.strategy['staking_policy']['roll_mode'] = 'all_in'
        self.repo.update_subscription_record(subscription_id=self.sub['id'], user_id=self.user_id,
                                            source_id=self.source['id'], strategy=self.strategy, status='standby')
        self.next_start(rule)
        self.assertIsNone(self.archives(rule)[0]['credit_applied_run_id'])
        independent_archive = self.archive_current(rule)
        self.void_archived(independent_archive)
        self.resolve_archives()
        self.strategy['staking_policy']['roll_mode'] = 'chain'
        self.repo.update_subscription_record(subscription_id=self.sub['id'], user_id=self.user_id,
                                            source_id=self.source['id'], strategy=self.strategy, status='standby')
        new_run = self.next_start(rule)
        audit = self.repo.get_auto_trigger_rule_run(new_run)['capital_audit']['routes'][str(rule['routes'][0]['id'])]
        self.assertEqual(audit['applied_credit'], 10)
        self.assertEqual(audit['credit_archive_ids'], [chain_archive['id']])
        self.assertEqual(audit['independent_return_archive_ids'], [independent_archive['id']])
        self.assertEqual(self.state(rule)['balance'], 10)

    def risk_rule(self):
        rule = self.rule(targets=[self.targets[1]], limit=4)
        rule = update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.user_id, payload={
            'daily_risk_control': {'enabled': True, 'loss_limit': 1, 'profit_target': 0,
                                   'timezone': 'Asia/Shanghai', 'cancel_pending_jobs': True},
        })['item']
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        return rule

    def test_same_day_late_loss_preserves_real_daily_risk(self):
        rule = self.risk_rule()
        archive = self.archive_current(rule)
        new_run = self.next_start(rule)
        self.accepted_old_draw(archive)
        self.resolve_archives()
        stopped = self.repo.get_auto_trigger_rule_run(new_run)
        self.assertEqual((stopped['status'], stopped['stop_reason']), ('stopped', 'loss_limit_hit'))
        stat = self.repo.get_auto_trigger_rule_daily_stat(rule_id=rule['id'], user_id=self.user_id, stat_date=self.day)
        self.assertEqual((stat['status'], stat['net_profit']), ('stopped', -10))
        self.assertEqual(self.archives(rule)[0]['capital_credit'], 0)

    def test_previous_day_late_loss_does_not_stop_today_or_change_current_cycle(self):
        rule = self.risk_rule()
        archive = self.archive_current(rule)
        self.now += timedelta(days=1)
        self.latest += 500
        new_run = self.next_start(rule)
        before = copy.deepcopy(self.state(rule))
        self.accepted_old_draw(archive)
        self.resolve_archives()
        self.assertEqual(self.repo.get_auto_trigger_rule_run(new_run)['status'], 'active')
        self.assertEqual(self.state(rule), before)
        old_day = self.repo.get_auto_trigger_rule_daily_stat(rule_id=rule['id'], user_id=self.user_id,
                                                           stat_date=archive['stat_date'])
        today = self.repo.get_auto_trigger_rule_daily_stat(rule_id=rule['id'], user_id=self.user_id, stat_date=self.day)
        self.assertEqual((old_day['net_profit'], old_day['status']), (-10, 'stopped'))
        self.assertEqual((today['net_profit'], today['status']), (0, 'active'))

    def test_manual_stop_of_unattempted_jobs_drains_cancelled_and_stays_stopped(self):
        rule = self.start()
        self.repo.stop_auto_trigger_rule_current_run(rule_id=rule['id'], user_id=self.user_id)
        result = _resolve_pending_win_streak_cycles(self.repo)
        self.assertEqual(result['resolved_count'], 1)
        self.assertIsNone(self.state(rule)['pending_plan'])
        self.assertIsNone(self.session(rule)['active_run_id'])
        self.assertEqual((self.session(rule)['status'], self.session(rule)['stop_reason']), ('blocked', 'manual_stop'))
        self.now += timedelta(days=1)
        self.latest += 500
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        self.assertEqual(self.archives(rule), [])

    def test_malformed_archive_rotates_before_next_ready_ledger(self):
        rule = self.start(limit=4)
        first = self.archive_current(rule)
        self.next_start(rule)
        second = self.archive_current(rule)
        self.void_archived(second)
        with self.repo._connect() as conn:
            conn.execute('UPDATE auto_trigger_cycle_archives SET pending_plan_json=? WHERE id=?',
                         ('broken-json', first['id']))
        one = self.repo.resolve_auto_trigger_cycle_archives(user_id=self.user_id, now=self.now, limit=1)
        self.assertEqual(len(one['errors']), 1)
        two = self.repo.resolve_auto_trigger_cycle_archives(user_id=self.user_id, now=self.now, limit=1)
        self.assertEqual(two['resolved_count'], 1)
        ready = next(a for a in self.archives(rule) if a['id'] == second['id'])
        self.assertEqual(ready['capital_credit'], 10)

    def test_inconsistent_cancelled_ledger_does_not_starve_a_ready_credit(self):
        rule = self.start(limit=4)
        first = self.archive_current(rule)
        self.next_start(rule)
        second = self.archive_current(rule)
        self.void_archived(second)
        # Historical cancelled rows with unresolved sends are deliberately NOT terminal.
        # Their failed inspection must rotate, so unrelated finished ledgers still settle.
        with self.repo._connect() as conn:
            for event_id in first['pending_plan']['leg_event_ids']:
                conn.execute("UPDATE subscription_progression_events SET status='cancelled' WHERE id=?", (event_id,))
        self.repo.resolve_auto_trigger_cycle_archives(user_id=self.user_id, now=self.now, limit=1)
        self.now += timedelta(seconds=1)
        self.repo.resolve_auto_trigger_cycle_archives(user_id=self.user_id, now=self.now, limit=1)
        entries = {a['id']: a for a in self.archives(rule)}
        self.assertEqual(entries[first['id']]['reserved_stake'], 10)
        self.assertEqual(entries[second['id']]['status'], 'resolved')
        self.assertEqual(entries[second['id']]['capital_credit'], 10)

    def test_detach_runs_at_original_deadline_boundary_without_extra_period(self):
        rule = self.start()
        jobs = self.unknown_current(rule)
        from datetime import datetime
        deadline = datetime.fromisoformat(jobs[0]['expire_at'].replace('Z', '+00:00'))
        self.now = deadline - timedelta(microseconds=1)
        self.assertEqual(self.detach()['detached_count'], 0)
        self.now = deadline
        self.assertEqual(self.detach()['detached_count'], 1)
        self.assertEqual(self.session(rule)['status'], 'waiting')
        self.assertEqual(self.archives(rule)[0]['reserved_stake'], 10)

    def test_invalid_legacy_leg_identifier_isolated_from_healthy_rule_recovery(self):
        first = self.start(targets=[self.targets[1]])
        self.unknown_current(first)
        second = self.rule(targets=[self.targets[2]], limit=4)
        self.assertEqual(self.cycle(second)['summary']['triggered_count'], 1)
        self.unknown_current(second)
        pending = copy.deepcopy(self.state(first)['pending_plan'])
        pending['leg_event_ids'] = [None]
        with self.repo._connect() as conn:
            conn.execute('UPDATE auto_trigger_cycle_states SET pending_plan_json=? WHERE rule_id=?',
                         (json.dumps(pending), first['id']))
        self.expire()
        result = self.repo.detach_expired_auto_trigger_cycles(now=self.now, user_id=self.user_id, limit=2)
        self.assertEqual(len(result['errors']), 1)
        self.assertEqual(result['detached_count'], 1)
        self.assertEqual(self.session(first)['status'], 'running')
        self.assertEqual(self.session(second)['status'], 'waiting')
        self.assertEqual(self.archives(first), [])
        self.assertEqual(self.archives(second)[0]['reserved_stake'], 10)

    def test_real_partial_settlement_is_counted_once_when_remaining_leg_loses(self):
        rule = self.start()
        jobs = self.current_jobs(rule)
        main = next(job for job in jobs if not job.get('send_with_job_id'))
        aux = next(job for job in jobs if job.get('send_with_job_id'))
        # Historical uncombined legs could receive independent transport outcomes.
        with self.repo._connect() as conn:
            conn.execute('UPDATE execution_jobs SET send_with_job_id=NULL WHERE id=?', (aux['id'],))
        self.repo.report_job_result(str(main['id']), 'qa-fixture', 1, 'delivered', 'qa-main',
                                   self.now.isoformat(), {}, None)
        self.repo.report_job_result(str(aux['id']), 'qa-fixture', 1, 'unconfirmed', None,
                                   self.now.isoformat(), {}, 'unknown auxiliary outcome')
        resolve_pending_pc28_progressions_from_draws(self.repo, user_id=self.user_id,
            draw_items=[{'issue_no': main['issue_no'], 'draw_context': {'triplet': [6, 4, 5]}}])
        main_event = self.repo.get_progression_event(main['progression_event_id'])
        self.assertEqual((main_event['status'], main_event['net_delta']), ('settled', 6.93))
        self.expire()
        self.assertEqual(self.detach()['detached_count'], 1)
        archive = self.archives(rule)[0]
        self.assertEqual((archive['settled_net_at_detach'], archive['reserved_stake']), (6.93, 3))
        self.assertEqual(self.state(rule)['balance'], 13.93)
        self.next_start(rule)
        current = copy.deepcopy(self.state(rule))
        self.repo.report_job_result(str(aux['id']), 'qa-fixture', 2, 'delivered', 'qa-aux',
                                   self.now.isoformat(), {}, None)
        resolve_pending_pc28_progressions_from_draws(self.repo, user_id=self.user_id,
            draw_items=[{'issue_no': aux['issue_no'], 'draw_context': {'triplet': [6, 4, 5]}}])
        self.assertEqual(self.resolve_archives()['resolved_count'], 1)
        self.assertEqual(self.archives(rule)[0]['capital_credit'], 0)
        self.assertEqual(self.state(rule), current)
        old = self.repo.get_auto_trigger_rule_daily_stat(rule_id=rule['id'], user_id=self.user_id,
                                                        stat_date=archive['stat_date'])
        self.assertEqual((old['net_profit'], old['settled_event_count']), (3.93, 2))


class CycleRecoveryApiIndependentTests(RecoveryFixture):
    def setUp(self):
        super().setUp()
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.app = api_cases.PlatformApiApplication(repository=self.repo,
            executor_api_token='recovery-test-token', session_secret='test-secret')
        self.repo.upsert_executor_heartbeat(executor_id='recovery', version='test',
            capabilities={'betting_acceptance_v1': True}, status='online', last_seen_at=self.now.isoformat())

    request = api_cases.CycleRecoveryApiTests.request
    incident = api_cases.CycleRecoveryApiTests.incident
    pull = api_cases.CycleRecoveryApiTests.pull
    evidence = api_cases.CycleRecoveryApiTests.evidence
    reconcile = api_cases.CycleRecoveryApiTests.reconcile

    def financial_rows(self):
        with sqlite3.connect(self.repo.db_path) as conn:
            return {table: conn.execute('SELECT * FROM ' + table).fetchall() for table in (
                'auto_trigger_cycle_archives', 'auto_trigger_cycle_states',
                'subscription_progression_events', 'execution_jobs',
            )}

    def test_admin_read_and_invalid_history_queries_leave_ledgers_unchanged(self):
        rule, _ = self.incident()
        admin = self.repo.create_user_record(username='qa-admin', role='admin')['id']
        path = '/api/platform/auto-trigger-rules/%s/cycle-recoveries' % rule['id']
        before = self.financial_rows()
        status, payload = self.request(path, user_id=admin)
        self.assertEqual(status, 200)
        allowed = {'id', 'rule_id', 'subscription_id', 'route_id', 'rule_run_id', 'stat_date', 'issue_no',
                   'status', 'reason', 'detached_at', 'resolved_at', 'initial_reserved_stake', 'reserved_stake',
                   'capital_credit', 'credit_applied_run_id', 'credit_applied_at', 'recovery_error', 'capital_disposition'}
        self.assertTrue(set(payload['items'][0]).issubset(allowed))
        for query in ('?before_id=-1', '?subscription_id=0', '?limit=abc', '?before_id=1%20OR%201=1'):
            with self.subTest(query=query):
                self.assertEqual(self.request(path + query)[0], 400)
        self.assertEqual(self.request(path, body={})[0], 405)
        self.assertEqual(self.request('/api/platform/auto-trigger-rules/999999/cycle-recoveries')[0], 404)
        self.assertEqual(self.financial_rows(), before)

    def test_expired_or_superseded_reconciliation_lease_cannot_modify_money(self):
        _, _ = self.incident()
        first = self.pull()
        before = self.financial_rows()
        self.now += timedelta(seconds=31)
        self.assertEqual(self.reconcile(first)[0], 400)
        second = self.pull()
        self.assertNotEqual(second['reconciliation_token'], first['reconciliation_token'])
        self.assertEqual(self.reconcile(first)[0], 400)
        self.assertEqual(self.financial_rows(), before)
        self.assertEqual(self.reconcile(second)[0], 200)

    def test_strict_delivery_bad_time_reply_period_and_amount_do_not_apply(self):
        _, _ = self.incident(strict=True)
        item = self.pull()
        baseline = self.financial_rows()
        good = self.evidence(item, strict=True)
        cases = []
        wrong_period = copy.deepcopy(good)
        wrong_period['acceptance_reply']['text'] = wrong_period['acceptance_reply']['text'].replace(item['issue_no'], '4999999')
        cases.append(wrong_period)
        wrong_reply = copy.deepcopy(good)
        wrong_reply['acceptance_reply']['reply_to'] = 999
        cases.append(wrong_reply)
        out_of_window = copy.deepcopy(good)
        out_of_window['telegram_message_date'] = '2000-01-01T00:00:00Z'
        cases.append(out_of_window)
        invalid_message = copy.deepcopy(good)
        invalid_message['message_id'] = True
        cases.append(invalid_message)
        for number, evidence in enumerate(cases):
            with self.subTest(case=number):
                self.assertEqual(self.reconcile(item, outcome='accepted', evidence=evidence)[0], 400)
                self.assertEqual(self.financial_rows(), baseline)
        self.assertEqual(self.reconcile(item, outcome='transport_confirmed', evidence=good)[0], 400)
        self.assertEqual(self.reconcile(item, outcome='not_sent', evidence={})[0], 400)
        self.assertEqual(self.financial_rows(), baseline)

    def test_oversized_and_malformed_evidence_are_rejected_without_side_effect(self):
        self.incident()
        item = self.pull()
        baseline = self.financial_rows()
        self.assertEqual(self.reconcile(item, evidence={'padding': 'x' * 25000})[0], 400)
        self.assertEqual(self.reconcile(item, evidence=['not', 'an', 'object'])[0], 400)
        malformed = self.evidence(item)
        malformed['verified_message'] = ['bad-nested-shape']
        self.assertEqual(self.reconcile(item, evidence=malformed)[0], 400)
        self.assertEqual(self.financial_rows(), baseline)


if __name__ == '__main__':
    import unittest
    unittest.main()
