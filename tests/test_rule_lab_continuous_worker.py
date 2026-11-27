"""Continuous replay is exercised against disposable sources and real ledgers."""
from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore
from tests.test_rule_lab_results_store import install_settlement_trace, settlement_trace
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_source import SourceFixture, UTC, stamp


class ContinuousWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.fixture = SourceFixture(self.root / 'source.db')
        # A successfully readable empty archive proves that absent recent rows
        # have not already been removed by retention.
        self.fixture.execute('''CREATE TABLE pc28_prediction_daily_summary (
            predictor_id INTEGER, summary_date TEXT, total_predictions INTEGER,
            PRIMARY KEY(predictor_id,summary_date))''')
        self.source = RuleLabSource(self.fixture.path)
        self.baseline_open = datetime(2026, 11, 28, 16, 1, tzinfo=UTC)
        self.now = datetime(2026, 12, 2, 16, 20, tzinfo=UTC)
        self._ticks = 0
        for offset in range(-100, 1):
            opened = self.baseline_open + timedelta(seconds=210 * offset)
            self.fixture.draw(1100 + offset, opened)
            self.fixture.prediction(1100 + offset, opened)
        self.future = [
            (1101, datetime(2026, 11, 30, 15, 54, tzinfo=UTC), (3, 6, 8), None),
            (1102, datetime(2026, 11, 30, 15, 57, 30, tzinfo=UTC), (1, 2, 3), None),
            # The last November result becomes visible after the first December
            # decision. A segment boundary must not give it to that decision.
            (1103, datetime(2026, 11, 30, 15, 59, 30, tzinfo=UTC), (3, 6, 8),
             datetime(2026, 11, 30, 16, 3, tzinfo=UTC)),
            (1104, datetime(2026, 11, 30, 16, 3, tzinfo=UTC), (1, 2, 3), None),
            (1105, datetime(2026, 11, 30, 16, 6, 30, tzinfo=UTC), (3, 6, 8), None),
            (1106, datetime(2026, 12, 1, 15, 59, 30, tzinfo=UTC), (1, 2, 3),
             datetime(2026, 12, 1, 16, 3, tzinfo=UTC)),
            (1107, datetime(2026, 12, 1, 16, 3, tzinfo=UTC), (3, 6, 8), None),
        ]
        for issue, opened, balls, available in self.future:
            self.fixture.draw(issue, opened, balls, available=available)
            self.fixture.prediction(issue, opened)

    def make_baseline(self, name, *, round_mode=True, predictor_id=1, combo=False, store=None, stress_versions=0):
        if store is None:
            store = RuleLabStore(self.root / name / 'lab.db')
            store.initialize()
        install_settlement_trace(store)
        risk = policy(cooldown_issues=2) if round_mode else None
        if risk:
            risk['round'].update(profit_target=2, loss_limit=3)
            risk['day']['enabled'] = False
        metric = 'combo' if combo else 'big_small'
        template = 'combo_long_miss_v1' if combo else 'basic_v2'
        config = normalize_batch_payload(dict(
            name=name, mode='backtest', predictor_id=predictor_id, metric=metric,
            template_pack=template, stake_amount=1,
            start_date='2026-11-29', end_date='2026-11-29', risk_policy=risk,
        ), now=stamp(self.now))
        definitions = build_experiments(metric, template_pack=template)
        if not combo:
            definitions = definitions[:2]
        if stress_versions:
            definitions = [copy.deepcopy(definitions[0]) for _ in range(stress_versions)]
            for index, definition in enumerate(definitions):
                key = 'stress-always-%03d' % index
                definition['condition_key'] = key
                definition['condition']['condition_key'] = key
        batch = store.create_batch(1, config, 'fixture', definitions, now=stamp(self.now))
        store.set_batch_state(1, batch['id'], 'running', now=stamp(self.now))
        worker = RuleLabWorker(store, self.source)
        for _ in range(20):
            worker.tick(now=stamp(self.now))
            if store.get_batch(1, batch['id'])['status'] == 'completed':
                break
        self.assertEqual(store.get_batch(1, batch['id'])['status'], 'completed')
        self.assertEqual(store.get_work_item(batch['id'])['batch']['cursor']['last_issue_no'], '1100')
        return store, batch['id']

    def enable(self, store, batch_id, *, automatic=False):
        self.assertTrue(callable(getattr(store, 'continuation_preview', None)),
                        'The continuation checkpoint API is not implemented')
        preview = store.continuation_preview(1, 'batch', batch_id, now=self.clock())
        self.assertTrue(preview['eligible'], preview)
        return store.set_continuation(1, 'batch', batch_id, dict(
            request_id='configure-manual-series', expected_revision=preview['expected_revision'],
            capture_enabled=True, compute_enabled=automatic,
            daily_at='00:10', timezone='Asia/Shanghai',
        ), now=self.clock())

    def clock(self):
        return stamp(self.now + timedelta(seconds=self._ticks))

    def summary(self, store, batch_id):
        return store.continuation_preview(1, 'batch', batch_id, now=self.clock())['continuation']

    def tick_until(self, store, predicate, *, source=None, max_ticks=80):
        # Recreate the process on every tick: no correctness may depend on RAM.
        for _ in range(max_ticks):
            self._ticks += 1
            result = RuleLabWorker(store, source or self.source).tick(
                now=self.clock())
            if predicate():
                return result
        self.fail('Continuous worker did not reach the requested state: %r' % result)

    def capture(self, store, batch_id, through='1107'):
        self.tick_until(store, lambda: str(self.summary(store, batch_id).get('captured_through_issue')) == through)

    def append(self, store, batch_id, end_date, request_id):
        preview = store.continuation_preview(1, 'batch', batch_id, now=self.clock())
        return store.request_append(1, 'batch', batch_id, dict(
            request_id=request_id, expected_revision=preview['expected_revision'], end_date=end_date,
        ), now=self.clock())

    def finish_append(self, store, batch_id, through, *, source=None):
        self.tick_until(store, lambda: (
            str(self.summary(store, batch_id).get('calculated_through_issue')) == str(through)
            and not self.summary(store, batch_id).get('active_segment_id')
        ), source=source)

    @staticmethod
    def economic_state(store):
        with sqlite3.connect(store.db_path) as connection:
            connection.row_factory = sqlite3.Row
            experiments = [dict(row) for row in connection.execute('''
                SELECT id,condition_key,settlement_rule_id,stats_json,risk_runtime_json,
                       last_decided_issue,last_settled_issue
                FROM lab_experiments ORDER BY id''')]
            for experiment in experiments:
                for key in ('stats_json', 'risk_runtime_json'):
                    experiment[key] = json.loads(experiment[key])
            decisions = [tuple(row) for row in connection.execute('''
                SELECT experiment_id,issue_no,bet_value,stake_cents,decided_at,
                       feature_issue_no,status,skip_reason,settled_at,result_type,
                       net_cents,refund_reason,round_no,risk_date
                FROM lab_decisions ORDER BY experiment_id,issue_key''')]
            days = [tuple(row) for row in connection.execute('SELECT * FROM lab_daily ORDER BY experiment_id,stat_date')]
            risk_days = [(row['experiment_id'], row['stat_date'], json.loads(row['state_json']))
                         for row in connection.execute('SELECT * FROM lab_risk_days ORDER BY experiment_id,stat_date')]
            rounds = [(row['experiment_id'], row['round_no'], row['status'], json.loads(row['summary_json']))
                      for row in connection.execute('SELECT * FROM lab_rounds ORDER BY experiment_id,round_no')]
        return dict(experiments=experiments, decisions=decisions, days=days, risk_days=risk_days, rounds=rounds,
                    settled_trace=settlement_trace(store))

    def test_capture_continues_with_automatic_computation_disabled(self):
        store, batch_id = self.make_baseline('capture')
        before = self.economic_state(store)
        self.enable(store, batch_id)
        self.capture(store, batch_id)
        summary = self.summary(store, batch_id)
        self.assertFalse(summary['compute_enabled'])
        self.assertEqual(str(summary['calculated_through_issue']), '1100')
        self.assertEqual(self.economic_state(store), before)

    def test_round_replay_split_at_month_end_matches_one_segment(self):
        self.assert_segment_equivalence(round_mode=True)

    def test_per_issue_replay_split_at_month_end_matches_one_segment(self):
        self.assert_segment_equivalence(round_mode=False)

    def assert_segment_equivalence(self, *, round_mode):
        full, full_id = self.make_baseline('full', round_mode=round_mode)
        split, split_id = self.make_baseline('split', round_mode=round_mode)
        for store, batch_id in ((full, full_id), (split, split_id)):
            self.enable(store, batch_id)
            self.capture(store, batch_id)
        self.append(full, full_id, '2026-12-01', 'append-full-period')
        self.finish_append(full, full_id, '1106')
        self.append(split, split_id, '2026-11-30', 'append-first-period')
        self.finish_append(split, split_id, '1103')
        # November's last result is not available before December's first decision.
        self.assertIn('1103', {row['issue_no'] for row in split.get_pending_decisions(split_id)})
        first_segment_state = self.economic_state(split)
        for _ in range(3):
            self._ticks += 1
            RuleLabWorker(split, self.source).tick(now=self.clock())
        self.assertEqual(self.economic_state(split), first_segment_state,
                         'The tick-level pending sweep cannot advance an idle segment frontier')
        self.append(split, split_id, '2026-12-01', 'append-second-period')
        self.finish_append(split, split_id, '1106')
        self.assertEqual(self.economic_state(split), self.economic_state(full))
        self.assertEqual(split.get_batch(1, split_id)['start_date'], '2026-11-29')
        self.assertEqual(split.get_batch(1, split_id)['end_date'], '2026-11-29')
        if round_mode:
            decisions = self.economic_state(split)['settled_trace']
            self.assertNotIn('1104', {row['issue_no'] for row in decisions},
                             'A still-pending November result must prevent the next round bet')

    def test_archived_replay_does_not_read_deleted_upstream_inputs(self):
        store, batch_id = self.make_baseline('deleted-source')
        self.enable(store, batch_id)
        self.capture(store, batch_id)
        self.fixture.execute('DELETE FROM predictions')
        self.fixture.execute('DELETE FROM lottery_draws')
        self.append(store, batch_id, '2026-12-01', 'append-archived-period')
        self.finish_append(store, batch_id, '1106')
        state = self.economic_state(store)
        self.assertTrue(any(row['issue_no'] == '1101' for row in state['settled_trace']))
        self.assertTrue(any(row['issue_no'] == '1105' for row in state['settled_trace']))
        self.assertEqual(store.get_batch(1, batch_id)['status'], 'completed')

    def test_a_revoked_source_does_not_stop_another_capture_stream(self):
        self.fixture.execute("INSERT INTO predictors VALUES(2,'第二来源','pc28',1,1,'records','fixture')")
        self.fixture.execute('''INSERT INTO predictions(predictor_id,lottery_type,issue_no,
            prediction_big_small,prediction_odd_even,prediction_combo,status,created_at,settled_at)
            SELECT 2,lottery_type,issue_no,prediction_big_small,prediction_odd_even,
                   prediction_combo,status,created_at,settled_at FROM predictions WHERE predictor_id=1''')
        store, first_id = self.make_baseline('two-sources')
        _, second_id = self.make_baseline('second-source', predictor_id=2, store=store)
        self.enable(store, first_id)
        self.enable(store, second_id)
        self.fixture.execute('UPDATE predictors SET enabled=0 WHERE id=1')
        self.capture(store, second_id)
        first = self.summary(store, first_id)
        second = self.summary(store, second_id)
        self.assertIn(first['state'], ('source_unavailable', 'error'))
        self.assertEqual(str(second['captured_through_issue']), '1107')
        self.assertEqual(str(first['calculated_through_issue']), '1100')

    def test_capture_budget_rotates_after_the_stream_actually_processed(self):
        self.assert_fair_capture_after_restart(fail_first=False)

    def test_capture_failure_rotates_before_the_next_worker_restart(self):
        self.assert_fair_capture_after_restart(fail_first=True)

    def assert_fair_capture_after_restart(self, *, fail_first):
        for predictor_id in (2, 3):
            self.fixture.execute("INSERT INTO predictors VALUES(?,'轮转来源','pc28',1,1,'records','fixture')", (predictor_id,))
            self.fixture.execute('''INSERT INTO predictions(predictor_id,lottery_type,issue_no,
                prediction_big_small,prediction_odd_even,prediction_combo,status,created_at,settled_at)
                SELECT ?,lottery_type,issue_no,prediction_big_small,prediction_odd_even,
                       prediction_combo,status,created_at,settled_at FROM predictions WHERE predictor_id=1''',
                (predictor_id,))
        store, first_id = self.make_baseline('capture-rotation')
        batch_ids = [first_id]
        for predictor_id in (2, 3):
            _, batch_id = self.make_baseline('rotation-%s' % predictor_id, predictor_id=predictor_id, store=store)
            batch_ids.append(batch_id)
        for batch_id in batch_ids:
            self.enable(store, batch_id)
        attempted = []

        class OneCapturePerTick(RuleLabSource):
            worker = None

            def capture_page(source, predictor_id, **kwargs):
                attempted.append(predictor_id)
                source.worker._deadline = 0
                if fail_first and predictor_id == 1:
                    raise RuleLabError('source_unavailable', 'fixture source temporarily unavailable', 503)
                return super().capture_page(predictor_id, **kwargs)

        for _ in range(3):
            source = OneCapturePerTick(self.fixture.path)
            source.worker = RuleLabWorker(store, source)
            self._ticks += 1
            source.worker.tick(now=self.clock())
        self.assertEqual(attempted, [1, 2, 3],
                         'Selecting a page of streams must not skip the ones the tick never attempted')
        for index, batch_id in enumerate(batch_ids):
            summary = self.summary(store, batch_id)
            if index == 0 and fail_first:
                self.assertEqual(str(summary['captured_through_issue']), '1100')
                self.assertEqual(summary['state'], 'source_unavailable')
            else:
                self.assertEqual(str(summary['captured_through_issue']), '1107')

    def test_daily_append_stops_at_yesterday_and_is_restart_idempotent(self):
        # Now is Dec 3 00:20 Beijing: automatic append must finish Dec 2 only.
        current_day = datetime(2026, 12, 2, 16, 5, tzinfo=UTC)
        self.fixture.draw(1108, current_day)
        self.fixture.prediction(1108, current_day)
        self.now -= timedelta(minutes=30)
        store, batch_id = self.make_baseline('daily')
        self.enable(store, batch_id, automatic=True)
        self.now += timedelta(minutes=30)
        self.finish_append(store, batch_id, '1107')
        segments = store.list_segments(1, 'batch', batch_id)['segments']
        self.assertEqual(len(segments), 2)
        before = self.economic_state(store)
        for _ in range(3):
            RuleLabWorker(store, self.source).tick(now=stamp(self.now + timedelta(minutes=1)))
        self.assertEqual(self.economic_state(store), before)
        self.assertEqual(len(store.list_segments(1, 'batch', batch_id)['segments']), 2)
        self.assertNotIn('1108', {row[1] for row in before['decisions']})

    def test_combo_append_keeps_all_original_twenty_six_versions(self):
        store, batch_id = self.make_baseline('combo', combo=True)
        before = {row['id'] for row in store.list_experiments(1, batch_id)}
        self.assertEqual(len(before), 26)
        self.enable(store, batch_id)
        self.capture(store, batch_id)
        self.append(store, batch_id, '2026-12-01', 'append-combo-period')
        self.finish_append(store, batch_id, '1106')
        after = {row['id'] for row in store.list_experiments(1, batch_id)}
        self.assertEqual(after, before)

    def test_pausing_active_segment_preserves_capture_and_does_not_auto_resume(self):
        store, batch_id = self.make_baseline('paused-segment')
        self.enable(store, batch_id, automatic=True)
        self.capture(store, batch_id)
        self.append(store, batch_id, '2026-12-01', 'append-paused-period')
        store.prepare_due_appends(now=self.clock())
        store.set_batch_state(1, batch_id, 'paused', now=self.clock())
        before = self.economic_state(store)
        opened = datetime(2026, 12, 2, 16, 5, tzinfo=UTC)
        self.fixture.draw(1108, opened)
        self.fixture.prediction(1108, opened)
        self.capture(store, batch_id, through='1108')
        self.assertEqual(store.get_batch(1, batch_id)['status'], 'paused')
        self.assertEqual(self.summary(store, batch_id)['state'], 'paused')
        self.assertEqual(self.economic_state(store), before)
        store.set_batch_state(1, batch_id, 'running', now=self.clock())
        self.finish_append(store, batch_id, '1106')

    def test_budget_yield_before_settlement_retries_same_period(self):
        expected, expected_id = self.make_baseline('uninterrupted')
        actual, actual_id = self.make_baseline('budget-yields')
        for store, batch_id in ((expected, expected_id), (actual, actual_id)):
            self.enable(store, batch_id)
            self.capture(store, batch_id)
            self.append(store, batch_id, '2026-12-01', 'append-budget-period')
        self.finish_append(expected, expected_id, '1106')
        interrupted = set()

        class YieldingWorker(RuleLabWorker):
            def _settle_pending(worker, batch_id, *, available_at=None):
                batch = worker.store.get_work_item(batch_id)['batch']
                previous = batch['cursor'].get('simulation_time')
                if (available_at and previous and available_at > previous
                        and available_at not in interrupted
                        and worker.store.get_pending_decisions(batch_id)):
                    interrupted.add(available_at)
                    worker._deadline = 0
                return super()._settle_pending(batch_id, available_at=available_at)

        for _ in range(40):
            self._ticks += 1
            YieldingWorker(actual, self.source).tick(now=self.clock())
            summary = self.summary(actual, actual_id)
            if str(summary.get('calculated_through_issue')) == '1106' and not summary.get('active_segment_id'):
                break
        self.assertTrue(interrupted)
        self.assertEqual(self.economic_state(actual), self.economic_state(expected))

    def test_all_due_pending_pages_settle_before_the_next_decision(self):
        # The supported 100 running versions can accumulate more than the
        # 200-decision read page when several results arrive together.
        release = self.future[5][1] - timedelta(seconds=50)
        self.fixture.execute("UPDATE lottery_draws SET created_at=? WHERE CAST(issue_no AS INTEGER) BETWEEN 1101 AND 1105",
                             (release.strftime('%Y-%m-%d %H:%M:%S'),))
        store, batch_id = self.make_baseline('large-pending', round_mode=False, stress_versions=100)
        self.enable(store, batch_id)
        self.capture(store, batch_id)
        self.append(store, batch_id, '2026-12-01', 'append-many-pending')
        self.finish_append(store, batch_id, '1106')
        with sqlite3.connect(store.db_path) as connection:
            pending = connection.execute("SELECT issue_no,COUNT(*) FROM lab_decisions WHERE status='pending' GROUP BY issue_no ORDER BY issue_no").fetchall()
        self.assertEqual(pending, [('1106', 100)],
                         'All older due pages must settle before advancing to the final decision')

    def test_continuous_clock_never_moves_backwards_for_reordered_draw_times(self):
        store, batch_id = self.make_baseline('time-regression')
        self.fixture.execute("UPDATE lottery_draws SET open_time='2026-11-30 23:00:00' WHERE issue_no='1104'")
        self.enable(store, batch_id)
        self.capture(store, batch_id)
        self.append(store, batch_id, '2026-12-01', 'append-time-regression')
        for _ in range(3):
            self._ticks += 1
            RuleLabWorker(store, self.source).tick(now=self.clock())
        work = store.get_work_item(batch_id)['batch']
        self.assertEqual(work['status'], 'error')
        self.assertEqual(work['cursor']['last_issue_no'], '1103')
        self.assertEqual(work['cursor']['simulation_time'], '2026-11-30T15:58:50Z')

    def test_pause_arriving_before_next_settlement_preserves_current_frontier(self):
        store, batch_id = self.make_baseline('pause-at-settlement')
        self.enable(store, batch_id)
        self.capture(store, batch_id)
        self.append(store, batch_id, '2026-12-01', 'append-pause-at-settlement')
        paused_state = []
        test = self

        class PausingWorker(RuleLabWorker):
            def _settle_pending(worker, current_id, *, available_at=None):
                batch = worker.store.get_work_item(current_id)['batch']
                previous = batch['cursor'].get('simulation_time')
                if (not paused_state and available_at and previous and available_at > previous
                        and worker.store.get_pending_decisions(current_id)):
                    worker.store.set_batch_state(1, current_id, 'paused', now=worker._now)
                    paused_state.append(test.economic_state(worker.store))
                return super()._settle_pending(current_id, available_at=available_at)

        self._ticks += 1
        PausingWorker(store, self.source).tick(now=self.clock())
        self.assertTrue(paused_state)
        self.assertEqual(store.get_batch(1, batch_id)['status'], 'paused')
        self.assertEqual(self.economic_state(store), paused_state[0])

    def test_study_appends_all_children_from_one_frozen_input_manifest(self):
        store, batch_id = self.make_baseline('comparison', combo=True)
        original = self.economic_state(store)
        study = store.create_study(1, batch_id, '续算比较', 'study-before-continuation', now=self.clock())['study']
        study_id = study['id']
        store.set_study_state(1, study_id, 'running', now=self.clock())
        self.tick_until(store, lambda: store.get_study(1, study_id)['status'] == 'completed')
        before_ids = {row['id'] for child in study['children']
                      for row in store.list_experiments(1, child['batch_id'])}
        preview = store.continuation_preview(1, 'study', study_id, now=self.clock())
        self.assertTrue(preview['eligible'], preview)
        store.set_continuation(1, 'study', study_id, dict(
            request_id='configure-comparison-series', expected_revision=preview['expected_revision'],
            capture_enabled=True, compute_enabled=False, daily_at='00:10', timezone='Asia/Shanghai',
        ), now=self.clock())

        def summary():
            return store.continuation_preview(1, 'study', study_id, now=self.clock())['continuation']

        self.tick_until(store, lambda: str(summary().get('captured_through_issue')) == '1107')
        preview = store.continuation_preview(1, 'study', study_id, now=self.clock())
        store.request_append(1, 'study', study_id, dict(
            request_id='append-comparison-period', expected_revision=preview['expected_revision'],
            end_date='2026-12-01',
        ), now=self.clock())
        self.tick_until(store, lambda: str(summary().get('calculated_through_issue')) == '1106'
                        and not summary().get('active_segment_id'))
        evidence = []
        for child in study['children']:
            work = store.get_work_item(child['batch_id'])
            self.assertEqual(work['batch']['cursor']['last_issue_no'], '1106')
            evidence.append(work['batch']['cursor']['evidence'])
        self.assertTrue(all(value == evidence[0] for value in evidence))
        self.assertEqual({row['id'] for child in study['children']
                          for row in store.list_experiments(1, child['batch_id'])}, before_ids)
        self.assertEqual(store.get_batch(1, batch_id)['processed_issues'], 1)
        self.assertEqual(self.economic_state(store)['experiments'][:26], original['experiments'])
        segment = store.list_segments(1, 'study', study_id)['segments'][0]
        self.assertTrue(segment['summary']['comparable'])


if __name__ == '__main__':
    unittest.main()
