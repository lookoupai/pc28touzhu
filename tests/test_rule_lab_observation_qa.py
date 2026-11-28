"""Independent adversarial checks for observed list data, using real SQLite/WSGI.

The ledgers below are explicit deterministic report fixtures. No production data,
source adapter or worker is written or mocked into a successful observation.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments, empty_stats, normalize_batch_payload
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.test_api import FakeRepository
from tests.test_rule_lab_risk import policy

NOW = '2026-11-28T04:00:00Z'
PREFIX = '/api/platform/rule-lab'
QUERY = 'observation_months=2&observation_partial=true'


class ForbiddenSource:
    def __getattr__(self, name):
        raise AssertionError('Observation read unexpectedly accessed source: ' + name)


def day(stamp, net, count=1, stake=100):
    return dict(stat_date=stamp, net_cents=net, settled_count=count,
                stake_cents=stake, hit_count=count if net > 0 else 0,
                miss_count=count if net < 0 else 0, refund_count=count if net == 0 else 0)


class ObservationAdversarialTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = RuleLabStore(Path(self.directory.name) / 'ledger' / 'lab.db')
        self.service = RuleLabService(self.store, ForbiddenSource())
        self.repository = FakeRepository()
        self.app = PlatformApiApplication(self.repository, 'qa-executor', 'qa-observation',
                                          rule_lab_service=self.service)

    def fixture(self, days, *, start='2026-10-01', end='2026-11-27', through=None,
                first=None, status='completed', stake_amount=1, count=2):
        risk = policy()
        risk['day']['hit_rate_gate'].update(enabled=True, threshold=25)
        config = normalize_batch_payload(dict(name='QA observation', mode='backtest', predictor_id=7,
            metric='big_small', template_pack='basic_v2', stake_amount=stake_amount,
            start_date=start, end_date=end, risk_policy=risk), now=NOW)
        batch = self.store.create_batch(1, config, 'Synthetic local algorithm',
                                       build_experiments('big_small')[:count], now=NOW)
        self.seed(batch['id'], days, start=start, through=through or end,
                  first=first or start, status=status)
        return batch['id']

    def seed(self, batch_id, days, *, start='2026-10-01', through='2026-11-27',
             first=None, status='completed'):
        frontier = dict(first_open_at=(first or start) + 'T00:00:00+08:00',
                        processed_open_at=through + 'T23:59:00+08:00', replay_cutoff_at=NOW)
        cursor = dict(replay_scheduler_version=2, replay_complete=status == 'completed',
                      retention_v1=frontier, simulation_time=through + 'T23:58:20+08:00',
                      evidence=dict(digest='qa-identical-inputs', count=23316, warm_count=100,
                                    first_issue='3488000', last_issue='3511700'))
        with sqlite3.connect(self.store.db_path) as c:
            c.execute('UPDATE lab_batches SET status=?,started_at=?,processed_issues=23316,cursor_json=? WHERE id=?',
                      (status, start + 'T00:00:00+08:00', json.dumps(cursor), batch_id))
            for eid, in c.execute('SELECT id FROM lab_experiments WHERE batch_id=? ORDER BY id', (batch_id,)).fetchall():
                c.execute('DELETE FROM lab_daily WHERE experiment_id=?', (eid,))
                stats = empty_stats()
                running = peak = worst = 0
                for row in sorted(days, key=lambda item: item['stat_date']):
                    for key in ('net_cents', 'stake_cents', 'settled_count', 'hit_count', 'miss_count', 'refund_count'):
                        stats[key] += row[key]
                    running += row['net_cents']; peak = max(peak, running); worst = max(worst, peak-running)
                    c.execute('''INSERT INTO lab_daily(experiment_id,stat_date,net_cents,stake_cents,
                              settled_count,hit_count,miss_count,refund_count) VALUES(?,?,?,?,?,?,?,?)''',
                              (eid, row['stat_date'], row['net_cents'], row['stake_cents'], row['settled_count'],
                               row['hit_count'], row['miss_count'], row['refund_count']))
                stats.update(max_drawdown_cents=worst, equity_peak_cents=peak)
                c.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?', (json.dumps(stats), eid))

    def observed(self, batch_id, *, months=2, partial=True):
        result = self.service.batch_detail(1, batch_id, observation_months=months,
                                           observation_partial=partial, now=NOW)
        return result['experiments'][0]['observation']

    def request(self, path, query=QUERY):
        environ = build_testing_environ(path, query=query, method='GET', headers={
            'HTTP_COOKIE': SESSION_COOKIE_NAME + '=' + build_session_cookie_value(1, 'qa-observation')})
        captured = {}
        def start_response(status, headers):
            captured.update(status=int(status.split()[0]), headers=dict(headers))
        captured['body'] = json.loads(b''.join(self.app(environ, start_response)))
        return captured

    def eid(self, batch_id):
        with sqlite3.connect(self.store.db_path) as c:
            return c.execute('SELECT id FROM lab_experiments WHERE batch_id=? ORDER BY id LIMIT 1', (batch_id,)).fetchone()[0]

    def test_requested_window_is_a_real_read_only_http_feature(self):
        bid = self.fixture([day('2026-10-01', 599, count=200, stake=20000), day('2026-11-01', 1200, count=200, stake=20000)])
        with sqlite3.connect(self.store.db_path) as c:
            before = '\n'.join(c.iterdump())
        result = self.request(PREFIX + '/batches/' + str(bid))
        self.assertEqual(result['status'], 200, result)
        observed = result['body']['experiments'][0]['observation']
        self.assertEqual(result['headers']['Cache-Control'], 'no-store')
        self.assertEqual(observed['months'][0]['net_cents'], 599)
        self.assertEqual(observed['months'][0]['stake_cents'], 20000)
        self.assertLess(100*observed['months'][0]['net_cents'], 3*observed['months'][0]['stake_cents'])
        with sqlite3.connect(self.store.db_path) as c:
            self.assertEqual('\n'.join(c.iterdump()), before)
        self.assertNotIn('observation', self.request(PREFIX + '/batches/' + str(bid), query='')['body']['experiments'][0])

    def test_zero_trade_months_remain_between_profitable_months(self):
        bid = self.fixture([day('2026-09-05', 100), day('2026-11-05', 100)], start='2026-09-01')
        observed = self.observed(bid, months=3)
        self.assertEqual([m['month'] for m in observed['months']], ['2026-09', '2026-10', '2026-11'])
        empty = observed['months'][1]
        self.assertEqual(empty['status'], 'no_samples')
        self.assertTrue(empty['complete_month'])
        self.assertIsNone(empty['roi_percent'])
        self.assertEqual(observed['window']['available_months'], 2)
        self.assertEqual(observed['window']['complete_months'], 1)
        self.assertEqual(observed['summary']['min_month_count'], 0)
        self.assertEqual(observed['summary']['min_month_days'], 0)
        full = self.observed(bid, months=3, partial=False)
        self.assertEqual([m['month'] for m in full['months']], ['2026-08', '2026-09', '2026-10'])
        self.assertEqual(full['months'][0]['status'], 'missing')
        self.assertEqual(full['window']['available_months'], 1)

    def test_partial_observation_start_makes_fixed_recent_intervals_unavailable(self):
        bid = self.fixture([day('2026-11-21', 200), day('2026-11-25', 100)], start='2026-11-20')
        observed = self.observed(bid, months=1)
        self.assertTrue(observed['window']['period_reached'])
        self.assertEqual(observed['recent']['start_date'], '2026-11-14')
        self.assertEqual(observed['recent']['end_date'], '2026-11-27')
        for key in ('recent', 'previous'):
            self.assertFalse(observed[key]['available'])
            self.assertTrue(all(observed[key][field] is None for field in ('net_cents', 'stake_cents', 'settled_count', 'roi_percent')))
        self.assertFalse(observed['months'][0]['complete_month'])

    def test_full_ledger_reconciliation_includes_history_outside_window(self):
        bid = self.fixture([day('2026-09-01', 100), day('2026-09-02', -100),
                            day('2026-10-01', 300), day('2026-11-01', 400)], start='2026-09-01')
        observed = self.observed(bid)
        self.assertTrue(observed['quality']['history_consistent'])
        self.assertNotIn('history_mismatch', observed['quality']['blockers'])
        self.assertEqual(observed['summary']['settled_count'], 2)
        eid = self.eid(bid)
        # Opposite PnL cancels: a net-only reconciliation would miss both removed rows.
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("DELETE FROM lab_daily WHERE experiment_id=? AND stat_date<'2026-10-01'", (eid,))
        broken = self.observed(bid)
        self.assertEqual(broken['summary']['net_cents'], 700)
        self.assertFalse(broken['quality']['history_consistent'])
        self.assertIn('history_mismatch', broken['quality']['blockers'])

    def test_pending_risk_date_scopes_old_and_future_windows_separately(self):
        bid = self.fixture([day('2026-10-01', 100), day('2026-11-01', 200)])
        eid = self.eid(bid)
        with sqlite3.connect(self.store.db_path) as c:
            # The decision happened in October but is reserved for a November draw.
            c.execute('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,
                       stake_cents,decided_at,decision_mode,condition_value_json,status,risk_date)
                       VALUES(?, '99901', 99901, '大', 100, '2026-10-31T15:59:55Z', 'backtest', '{}', 'pending', '2026-11-01')''', (eid,))
        october = self.observed(bid, months=1, partial=False)
        self.assertEqual(october['window']['end_date'], '2026-10-31')
        self.assertNotIn('window_pending', october['quality']['blockers'])
        self.assertTrue(october['months'][0]['complete_month'])
        self.assertTrue(october['recent']['available'])
        both = self.observed(bid)
        self.assertIn('window_pending', both['quality']['blockers'])
        self.assertEqual(both['months'][0]['pending_count'], 0)
        self.assertEqual(both['months'][1]['pending_count'], 1)
        self.assertTrue(both['recent']['available'])
        self.assertFalse(both['previous']['available'])

    def test_window_recovery_includes_non_betting_and_recovery_days(self):
        days = [day('2026-10-01', 700, stake=250), day('2026-11-01', -100, stake=250),
                day('2026-11-03', 100, stake=250), day('2026-11-20', 100, stake=250),
                day('2026-11-21', -100, stake=250), day('2026-11-24', 100, stake=250),
                day('2026-11-26', -100, stake=250)]
        bid = self.fixture(days, stake_amount=2.5)
        observed = self.observed(bid, months=1)
        self.assertEqual(observed['original_stake_cents'], 250)
        self.assertEqual(observed['summary']['net_cents'], 0)
        self.assertEqual(observed['summary']['best_three_days_cents'], 300)
        self.assertEqual(observed['summary']['net_without_best_three_days_cents'], -300)
        self.assertEqual(observed['summary']['longest_underwater_days'], 4)
        self.assertEqual(observed['summary']['current_underwater_days'], 2)
        self.assertTrue(observed['summary']['recovery_left_censored'])
        self.assertEqual(observed['months'][0]['daily_close_drawdown_cents'], 100)

    def test_incomplete_last_target_day_is_not_a_completed_fourteen_day_interval(self):
        bid = self.fixture([day('2026-10-01', 100), day('2026-11-27', 100)], status='running')
        with sqlite3.connect(self.store.db_path) as c:
            cursor = json.loads(c.execute('SELECT cursor_json FROM lab_batches WHERE id=?', (bid,)).fetchone()[0])
            cursor['retention_v1']['processed_open_at'] = '2026-11-27T12:00:00+08:00'
            cursor['retention_v1'].pop('replay_cutoff_at')
            cursor['simulation_time'] = '2026-11-27T11:59:20+08:00'
            c.execute('UPDATE lab_batches SET cursor_json=? WHERE id=?', (json.dumps(cursor), bid))
        observed = self.observed(bid)
        self.assertEqual(observed['window']['actual_through_date'], '2026-11-27')
        self.assertFalse(observed['window']['period_reached'])
        self.assertFalse(observed['recent']['available'])
        self.assertTrue(observed['previous']['available'])
        self.assertIn('window_not_reached', observed['quality']['warnings'])

    def test_group_rows_use_same_window_and_incomplete_child_never_slides_back(self):
        bid = self.fixture([day('2026-10-01', 100), day('2026-11-01', 200)])
        study = self.service.create_study(1, dict(source_batch_id=bid, name='QA observed study',
            request_id='qa-observation-study-1'), now=NOW)['study']
        ids = [child['batch_id'] for child in study['children']]
        for child in ids:
            self.seed(child, [day('2026-10-01', 100), day('2026-11-01', 200)])
        self.seed(ids[-1], [day('2026-10-01', 100), day('2026-11-01', 200)],
                  through='2026-11-10', status='running')
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("UPDATE lab_studies SET status='running',cutoff_issue='3511700',cutoff_at=? WHERE id=?", (NOW, study['id']))
        result = self.service.study_detail(1, study['id'], observation_months=2, observation_partial=True, now=NOW)
        self.assertFalse(result['study']['comparable'])
        self.assertEqual({(r['observation']['window']['start_date'], r['observation']['window']['end_date']) for r in result['rows']},
                         {('2026-10-01', '2026-11-27')})
        for row in result['rows']:
            self.assertIn('study_not_comparable', row['observation']['quality']['blockers'])
            if row['batch_id'] == ids[-1]:
                self.assertEqual(row['observation']['window']['actual_through_date'], '2026-11-10')
                self.assertFalse(row['observation']['recent']['available'])
                self.assertFalse(row['observation']['previous']['available'])

    def test_parallel_settlement_cannot_mix_original_stats_with_new_daily_rows(self):
        bid = self.fixture([day('2026-10-01', 100), day('2026-11-01', 200)])
        eid = self.eid(bid); fired = []; callback_errors = []
        original = self.store._open_connection
        def traced():
            c = original()
            def on_sql(sql):
                if not fired and 'FROM LAB_DAILY' in sql.upper():
                    fired.append(True)
                    try:
                        self.assertTrue(c.in_transaction)
                        self.assertEqual(c.execute('PRAGMA query_only').fetchone()[0], 1)
                        with sqlite3.connect(self.store.db_path) as writer:
                            stats = json.loads(writer.execute('SELECT stats_json FROM lab_experiments WHERE id=?', (eid,)).fetchone()[0])
                            stats['settled_count'] += 1; stats['stake_cents'] += 100; stats['net_cents'] += 98; stats['hit_count'] += 1
                            writer.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?', (json.dumps(stats), eid))
                            writer.execute('''INSERT INTO lab_daily(experiment_id,stat_date,settled_count,stake_cents,net_cents,hit_count)
                                              VALUES(?, '2026-11-03', 1, 100, 98, 1)''', (eid,))
                    except Exception as error:
                        callback_errors.append(error)
            c.set_trace_callback(on_sql)
            return c
        with patch.object(self.store, '_open_connection', traced):
            result = self.service.batch_detail(1, bid, observation_months=2, observation_partial=True, now=NOW)
        self.assertTrue(fired, 'Concurrency adversary never ran')
        self.assertEqual(callback_errors, [])
        row = result['experiments'][0]
        self.assertEqual(row['net_profit'], 3)
        self.assertEqual(row['observation']['summary']['net_cents'], 300)
        self.assertTrue(row['observation']['quality']['history_consistent'])
        fresh = self.observed(bid)
        self.assertEqual(fresh['summary']['net_cents'], 398)
        self.assertTrue(fresh['quality']['history_consistent'])


if __name__ == '__main__':
    unittest.main()
