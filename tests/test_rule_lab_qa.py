"""Independent adversarial checks; all inputs and writes are temporary fixtures."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from pathlib import Path

from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload, settle_bet
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore
from tests.test_api import FakeRepository
from tests.test_rule_lab_source import SourceFixture, UTC, stamp


class RuleLabIndependentQaTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rule-lab-qa-')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.opened = datetime(2026, 11, 26, 4, tzinfo=UTC)
        self.now = stamp(self.opened + timedelta(hours=3))

    def ledger(self, name='lab'):
        store = RuleLabStore(self.root / name / 'rule_lab.db')
        store.initialize()
        return store

    def batch(self, store, *, user_id=1, metric='big_small', mode='backtest',
              start_date='2026-11-26', end_date='2026-11-26', start=True):
        payload = dict(name='独立验证', predictor_id=1, metric=metric, mode=mode)
        if mode == 'backtest':
            payload.update(start_date=start_date, end_date=end_date)
        config = normalize_batch_payload(payload, now=self.now)
        batch = store.create_batch(user_id, config, '临时公开来源', build_experiments(metric), now=self.now)
        if start:
            store.set_batch_state(user_id, batch['id'], 'running', now=self.now)
        return batch

    def complete(self, store, source, batch, *, page_size=200, restart=False):
        worker = RuleLabWorker(store, source, page_size=page_size)
        for _ in range(250):
            if restart:
                worker = RuleLabWorker(store, source, page_size=page_size)
            worker.tick(now=self.now)
            state = store.get_batch(1, batch['id'])
            self.assertNotIn(state['status'], ('error', 'resource_paused'), state)
            if state['status'] == 'completed':
                return
        self.fail('bounded replay did not complete')

    def test_all_thousand_triplets_follow_independent_ok_payout_oracle(self):
        # Compare to the published rules rather than calling the same production helper twice.
        netdisk_odds = {'big_small': 1.98, 'odd_even': 1.98}
        high_odds = {'big_small': 2.846, 'odd_even': 2.846}
        directions = {'big_small': ('大', '小'), 'odd_even': ('单', '双'),
                      'combo': ('大单', '大双', '小单', '小双')}
        snapshots = {metric: [e['settlement_snapshot'] for e in build_experiments(metric)[:2]]
                     for metric in directions}
        comparisons = 0
        for code in range(1000):
            balls = [code // 100, code // 10 % 10, code % 10]
            total = sum(balls)
            actual = {'big_small': '大' if total >= 14 else '小', 'odd_even': '单' if total % 2 else '双'}
            actual['combo'] = actual['big_small'] + actual['odd_even']
            ordered = sorted(balls)
            straight = (len(set(balls)) == 3 and
                        (ordered[1] == ordered[0] + 1 and ordered[2] == ordered[1] + 1
                         or set(balls) in ({0, 1, 9}, {0, 8, 9})))
            special = total in (13, 14) or len(set(balls)) < 3 or straight
            for metric, values in directions.items():
                for value in values:
                    for high, snapshot in enumerate(snapshots[metric]):
                        hit = actual[metric] == value
                        odds = ((6.78 if value in ('小单', '大双') else 6.33) if high else
                                (3.6 if value in ('小单', '大双') else 4.2)) if metric == 'combo' else (
                                    high_odds if high else netdisk_odds)[metric]
                        expected_type = 'miss' if not hit else 'refund' if high and special else 'hit'
                        expected_cents = -100 if not hit else 0 if high and special else int(round(round(odds - 1, 2) * 100))
                        result = settle_bet(snapshot, metric, value, 100, balls)
                        self.assertEqual((result['result_type'], result['net_cents'], result['direction_hit']),
                                         (expected_type, expected_cents, hit), (balls, metric, value, high))
                        comparisons += 1
        self.assertEqual(comparisons, 16000)

    def test_target_result_and_future_suffix_cannot_change_entry_decisions(self):
        snapshots = []
        for world in range(2):
            fixture = SourceFixture(self.root / ('source-%d.db' % world))
            for i in range(12):
                opened = self.opened + timedelta(seconds=210 * i)
                # Both worlds agree before issue 1008. Its result and all later results differ.
                balls = (3, 6, 8) if world == 0 or i < 8 else (1, 3, 5)
                fixture.draw(1000 + i, opened, balls)
                fixture.prediction(1000 + i, opened)
            store = self.ledger('world-%d' % world)
            batch = self.batch(store)
            self.complete(store, RuleLabSource(fixture.path), batch)
            with sqlite3.connect(store.db_path) as connection:
                snapshots.append(connection.execute('''SELECT e.condition_key,e.settlement_rule_id,d.issue_no,
                    d.bet_value,d.decided_at,d.condition_value_json FROM lab_decisions d
                    JOIN lab_experiments e ON e.id=d.experiment_id WHERE d.issue_key<=1008
                    ORDER BY e.condition_key,e.settlement_rule_id,d.issue_key''').fetchall())
        self.assertTrue(snapshots[0])
        self.assertEqual(snapshots[0], snapshots[1])

    def test_replay_page_size_restart_and_source_file_are_invariant(self):
        fixture = SourceFixture(self.root / 'source.db')
        for i in range(26):
            opened = self.opened + timedelta(seconds=210 * i)
            fixture.draw(1000 + i, opened, (3, 6, 8) if i % 3 else (1, 3, 5))
            fixture.prediction(1000 + i, opened, big_small='大' if i % 5 else '小')
        source = RuleLabSource(fixture.path)
        before = hashlib.sha256(fixture.path.read_bytes()).hexdigest()
        ledgers = []
        for size, restart in [(1, True), (200, False)]:
            store = self.ledger('page-%d' % size)
            batch = self.batch(store)
            self.complete(store, source, batch, page_size=size, restart=restart)
            ledgers.append(store.list_experiments(1, batch['id']))
        self.assertEqual(ledgers[0], ledgers[1])
        self.assertEqual(hashlib.sha256(fixture.path.read_bytes()).hexdigest(), before)

    def test_daily_profit_crosses_beijing_midnight_by_draw_date(self):
        fixture = SourceFixture(self.root / 'midnight-source.db')
        first = datetime(2026, 11, 25, 15, 58, tzinfo=UTC)
        for i in range(4):
            opened = first + timedelta(seconds=210 * i)
            fixture.draw(1000 + i, opened)
            fixture.prediction(1000 + i, opened)
        store = self.ledger()
        batch = self.batch(store, start_date='2026-11-25', end_date='2026-11-26')
        self.complete(store, RuleLabSource(fixture.path), batch)
        baseline = store.list_experiments(1, batch['id'])[0]
        details = store.get_experiment_detail(1, batch['id'], baseline['id'])
        self.assertEqual([(d['date'], d['settled_count']) for d in details['daily']],
                         [('2026-11-25', 1), ('2026-11-26', 3)])
        self.assertAlmostEqual(sum(d['net_profit'] for d in details['daily']), baseline['net_profit'])

    def test_running_limit_is_atomic_when_three_owners_start_together(self):
        store = self.ledger()
        batches = [(uid, self.batch(store, user_id=uid, mode='live', start=False)) for uid in (1, 2, 3)]
        barrier = threading.Barrier(3)
        def start(item):
            uid, batch = item
            barrier.wait(timeout=5)
            try:
                store.set_batch_state(uid, batch['id'], 'running', now=self.now)
                return 'running'
            except RuleLabError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=3) as executor:
            results = list(executor.map(start, batches))
        self.assertEqual(sorted(results), ['capacity_exceeded', 'running', 'running'])
        self.assertEqual(len([b for b in store.list_work_batches() if b['status'] == 'running']), 2)

    def test_total_limit_is_atomic_for_last_two_creations(self):
        store = self.ledger()
        for _ in range(9):
            self.batch(store, mode='live', start=False)
        barrier = threading.Barrier(2)
        def create(uid):
            barrier.wait(timeout=5)
            try:
                self.batch(store, user_id=uid, mode='live', start=False)
                return 'created'
            except RuleLabError as error:
                return error.code
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(create, (2, 3)))
        self.assertEqual(sorted(results), ['capacity_exceeded', 'created'])
        with sqlite3.connect(store.db_path) as connection:
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_experiments').fetchone()[0], 500)

    def test_malformed_json_value_types_are_client_errors_without_partial_batches(self):
        fixture = SourceFixture(self.root / 'source.db')
        store = self.ledger()
        service = RuleLabService(store, RuleLabSource(fixture.path))
        app = PlatformApiApplication(FakeRepository(), 'qa-executor', 'qa-secret', rule_lab_service=service)
        cookie = '%s=%s' % (SESSION_COOKIE_NAME, build_session_cookie_value(1, 'qa-secret'))
        for field in ('name', 'mode', 'predictor_id', 'metric', 'template_pack', 'stake_amount'):
            for value in ([], {}, None, True):
                body = dict(name='QA', mode='live', predictor_id=1, metric='big_small')
                body[field] = value
                captured = {}
                def start_response(status, headers):
                    captured['status'] = int(status.split()[0])
                env = build_testing_environ('/api/platform/rule-lab/batches', method='POST', body=body,
                                            headers={'HTTP_COOKIE': cookie})
                result = json.loads(b''.join(app(env, start_response)))
                self.assertEqual(captured['status'], 400, (field, value, result))
                self.assertEqual(result['code'], 'invalid_input')
        self.assertEqual(store.list_batches(1), [])


if __name__ == '__main__':
    unittest.main()
