"""Independent continuation attacks; production evidence is copied before use."""
from __future__ import annotations

import hashlib
import calendar
import json
import os
import resource
import shutil
import sqlite3
import tempfile
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from tests import test_rule_lab_continuous_worker as worker_fixture
from tests.test_rule_lab_source import SourceFixture, UTC, stamp
from tests.test_rule_lab_risk import policy


def original_rows(path, columns=None, *, exclude_result_metadata=False):
    """Hash every original column and row, ignoring later additive columns/tables."""
    with sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True) as connection:
        connection.execute('PRAGMA query_only=ON')
        if columns is None:
            tables = [row[0] for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
            columns = {table: [row[1] for row in connection.execute('PRAGMA table_info("%s")' % table)]
                       for table in tables}
        result = {}
        for table, names in columns.items():
            quoted = ','.join('"%s"' % name.replace('"', '""') for name in names)
            where=" WHERE key!='result_retention'" if table=='lab_runtime' and exclude_result_metadata else ''
            rows = connection.execute('SELECT %s FROM "%s"%s ORDER BY rowid' % (quoted, table,where)).fetchall()
            digest = hashlib.sha256()
            for row in rows:
                # Original v3 fields are SQLite scalars; preserve both type and value.
                digest.update(repr(tuple(row)).encode('utf-8'))
                digest.update(b'\n')
            result[table] = (len(rows), digest.hexdigest())
        return columns, result


class ContinuationIndependentQaTests(unittest.TestCase):
    def harness(self):
        # Reuse only disposable input construction and operation helpers. These
        # cases assert independent failure modes, not the developer's test suite.
        case = worker_fixture.ContinuousWorkerTests(methodName='runTest')
        case.setUp()
        self.addCleanup(case.doCleanups)
        return case

    def test_concurrent_configuration_from_one_preview_creates_one_baseline(self):
        case = self.harness()
        store, batch_id = case.make_baseline('concurrent-configure')
        before = case.economic_state(store)
        preview = store.continuation_preview(1, 'batch', batch_id, now=case.clock())
        self.assertIsInstance(preview['expected_revision'], str)
        payload = dict(request_id='qa-concurrent-adoption', expected_revision=preview['expected_revision'],
                       capture_enabled=True, compute_enabled=False, daily_at='00:10', timezone='Asia/Shanghai')
        barrier = threading.Barrier(2)

        def configure():
            barrier.wait(timeout=5)
            return RuleLabStore(store.db_path).set_continuation(1, 'batch', batch_id, payload, now=case.clock())

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(lambda _: configure(), range(2)))
        self.assertEqual(sorted(result['created'] for result in results), [False, True])
        self.assertEqual(len(store.list_segments(1, 'batch', batch_id)['segments']), 1)
        self.assertEqual(case.economic_state(store), before)
        with sqlite3.connect(store.db_path) as connection:
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_series').fetchone()[0], 1)
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_continuation_requests').fetchone()[0], 1)

    def test_missing_legacy_warm_prediction_is_not_invented_as_a_known_no_signal(self):
        case = self.harness()
        store, batch_id = case.make_baseline('missing-warm-prediction')
        with sqlite3.connect(store.db_path) as connection:
            connection.execute("DELETE FROM lab_predictions WHERE predictor_id=1 AND issue_no='1099'")
        # The surviving draw does not prove whether a prediction never existed,
        # was pruned, or was not cached. Adoption cannot manufacture that proof.
        preview = store.continuation_preview(1, 'batch', batch_id, now=case.clock())
        self.assertFalse(preview['eligible'])
        self.assertTrue(preview['blockers'])
        with self.assertRaises(RuleLabError):
            store.set_continuation(1, 'batch', batch_id, dict(request_id='qa-missing-warm',
                expected_revision=preview['expected_revision'], capture_enabled=True, compute_enabled=False,
                daily_at='00:10', timezone='Asia/Shanghai'), now=case.clock())

    def test_capture_quota_rejection_does_not_publish_partial_input_or_watermark(self):
        case = self.harness()
        store, batch_id = case.make_baseline('capture-quota')
        case.enable(store, batch_id)
        with sqlite3.connect(store.db_path) as connection:
            connection.row_factory = sqlite3.Row
            stream = dict(connection.execute('SELECT * FROM lab_capture_streams ORDER BY id LIMIT 1').fetchone())
        page = case.source.capture_page(1, after_issue='1100', observed_at=case.clock())
        before_columns, before = original_rows(store.db_path)
        store.budget_bytes = 1  # No write page can fit; no upstream error is mocked.
        with self.assertRaises(RuleLabError) as error:
            store.commit_capture(stream['id'], stream['revision'], now=case.clock(), **{
                key: page[key] for key in ('draws', 'predictions', 'coverage', 'next_issue',
                                          'highest_seen_issue', 'source_identity')
            })
        self.assertEqual(error.exception.code, 'storage_limit')
        self.assertEqual(original_rows(store.db_path, before_columns)[1], before)

    def test_unrecoverable_old_input_does_not_starve_new_capture(self):
        case = self.harness()
        case.now += timedelta(days=3)
        # Issue 1000 is outside the next continuation's 100-period boundary.
        # Its historical absence prevents a full old replay, but cannot consume
        # every capture tick and thereby lose all newly arriving predictions.
        case.fixture.execute("DELETE FROM predictions WHERE issue_no='1000'")
        case.fixture.execute("INSERT INTO pc28_prediction_daily_summary VALUES(1,'2026-11-28',1)")
        start = datetime(2026, 12, 2, 16, 5, tzinfo=UTC)
        for offset in range(400):
            opened = start + timedelta(seconds=210 * offset)
            case.fixture.draw(1108 + offset, opened)
            case.fixture.prediction(1108 + offset, opened)
        store, batch_id = case.make_baseline('old-gap-capture')
        case.enable(store, batch_id)
        case.tick_until(store, lambda: str(case.summary(store, batch_id).get('captured_through_issue')) == '1507', max_ticks=30)
        with sqlite3.connect(store.db_path) as connection:
            self.assertGreater(connection.execute("SELECT count(*) FROM lab_input_versions WHERE kind='prediction' AND issue_key=1507").fetchone()[0], 0)
            category = connection.execute('''SELECT class FROM lab_input_coverage WHERE issue_key=1000
                ORDER BY revision DESC LIMIT 1''').fetchone()
            self.assertIsNone(category)  # Consumed A is no longer re-seeded merely to retain old coverage.

    def test_conflicting_ordinary_cache_does_not_replace_frozen_continuation_inputs(self):
        case = self.harness()
        clean, clean_id = case.make_baseline('cache-clean', round_mode=False)
        attacked, attacked_id = case.make_baseline('cache-poisoned', round_mode=False)
        for store, batch_id in ((clean, clean_id), (attacked, attacked_id)):
            case.enable(store, batch_id)
            case.capture(store, batch_id)
            case.append(store, batch_id, '2026-12-01', 'qa-cache-poison-append')

        # A separate ordinary replay can see a revised upstream record. Its
        # cache conflict must not invalidate or alter a frozen continuous view.
        draw = dict(case.source.get_draw_by_issue('1101', observed_at=case.clock()))
        prediction = dict(case.source.get_predictions(1, ['1101'], observed_at=case.clock())['items'][0])
        draw.update(triplet=[1, 2, 3], input_hash='qa-conflicting-draw')
        prediction.update(big_small='小', input_hash='qa-conflicting-prediction')
        with attacked._connect(write=True) as connection:
            attacked._cache(connection, [draw], [prediction])
            connection.execute("UPDATE lab_draws SET conflict=1 WHERE issue_no='1101'")
            connection.execute("UPDATE lab_predictions SET conflict=1 WHERE predictor_id=1 AND issue_no='1101'")
        for store, batch_id in ((clean, clean_id), (attacked, attacked_id)):
            case.finish_append(store, batch_id, '1106')
        self.assertEqual(case.economic_state(attacked), case.economic_state(clean))

    def test_three_segments_preserve_consumed_input_after_later_source_correction(self):
        case = self.harness()
        # The final requested day is in the past. A later draw proves capture
        # reached beyond that day's close instead of merely reading its prefix.
        lookahead = datetime(2026, 12, 2, 16, 5, tzinfo=UTC)
        case.fixture.draw(1108, lookahead)
        case.fixture.prediction(1108, lookahead)
        clean, clean_id = case.make_baseline('three-clean', round_mode=False)
        attacked, attacked_id = case.make_baseline('three-corrected', round_mode=False)
        for store, batch_id in ((clean, clean_id), (attacked, attacked_id)):
            case.enable(store, batch_id)
            case.capture(store, batch_id, through='1108')
            case.append(store, batch_id, '2026-11-30', 'qa-segment-a')
            case.finish_append(store, batch_id, '1103')

        # Compute the control before any upstream revision. It establishes the
        # same final frontier using only the original frozen input.
        for end_date, through, request_id in (
            ('2026-12-01', '1106', 'qa-segment-b'),
            ('2026-12-02', '1107', 'qa-segment-c'),
        ):
            case.append(clean, clean_id, end_date, request_id)
            case.finish_append(clean, clean_id, through)
        expected = case.economic_state(clean)

        # The record was consumed by A. A new observation is valid archive data,
        # but it cannot revise the carried feature window of B or C.
        case.fixture.execute("UPDATE predictions SET prediction_big_small='小' WHERE issue_no='1101'")
        page = case.source.capture_page(1, after_issue='1100', observed_at=case.clock())
        with sqlite3.connect(attacked.db_path) as connection:
            connection.row_factory = sqlite3.Row
            stream = dict(connection.execute('SELECT * FROM lab_capture_streams ORDER BY id LIMIT 1').fetchone())
        captured = attacked.commit_capture(stream['id'], stream['revision'], now=case.clock(), **{
            key: page[key] for key in ('draws', 'predictions', 'coverage', 'next_issue',
                                      'highest_seen_issue', 'source_identity')
        })
        self.assertTrue(captured['committed'])
        for end_date, through, request_id in (
            ('2026-12-01', '1106', 'qa-segment-b'),
            ('2026-12-02', '1107', 'qa-segment-c'),
        ):
            case.append(attacked, attacked_id, end_date, request_id)
            case.finish_append(attacked, attacked_id, through)
            reader = attacked.archive_source(attacked_id)
            history = reader.get_history(1, before_issue='1108')
            preserved = next(row for row in history if row['issue_no'] == '1101')
            self.assertEqual(preserved['prediction_values']['big_small'], '大', request_id)
        self.assertEqual(case.economic_state(attacked), expected)

    def test_carried_pending_prediction_observes_later_settlement_in_next_segment(self):
        case = self.harness()
        case.fixture.execute("UPDATE predictions SET status='pending',settled_at=NULL WHERE issue_no='1103'")
        store, batch_id = case.make_baseline('pending-prediction-metadata', round_mode=False)
        case.enable(store, batch_id)
        case.capture(store, batch_id)
        case.append(store, batch_id, '2026-11-30', 'qa-pending-metadata-a')
        case.finish_append(store, batch_id, '1103')
        with sqlite3.connect(store.db_path) as connection:
            connection.row_factory = sqlite3.Row
            previous = dict(connection.execute('SELECT id,payload FROM lab_checkpoints ORDER BY id DESC LIMIT 1').fetchone())
            stream = dict(connection.execute('SELECT * FROM lab_capture_streams ORDER BY id LIMIT 1').fetchone())
        case.fixture.execute("UPDATE predictions SET status='settled',settled_at='2026-11-30 16:03:00' WHERE issue_no='1103'")
        page = case.source.capture_page(1, after_issue='1100', observed_at=case.clock())
        store.commit_capture(stream['id'], stream['revision'], now=case.clock(), **{
            key: page[key] for key in ('draws', 'predictions', 'coverage', 'next_issue',
                                      'highest_seen_issue', 'source_identity')
        })
        case.append(store, batch_id, '2026-12-01', 'qa-pending-metadata-b')
        store.prepare_due_appends(now=case.clock())
        history = store.archive_source(batch_id).get_history(1, before_issue='1105')
        previous_prediction = next(row for row in history if row['issue_no'] == '1103')
        self.assertEqual(previous_prediction['prediction_values']['big_small'], '大')
        self.assertEqual(previous_prediction['prediction_settled_at'], '2026-11-30T16:03:00Z',
                         'Permitted settlement metadata must affect B, not wait for C')
        with sqlite3.connect(store.db_path) as connection:
            self.assertEqual(connection.execute('SELECT payload FROM lab_checkpoints WHERE id=?',
                                                (previous['id'],)).fetchone()[0], previous['payload'])

    def test_twelve_monthly_appends_keep_twenty_six_versions_and_original_baseline(self):
        with tempfile.TemporaryDirectory(prefix='rule-lab-year-qa-') as temporary:
            root = Path(temporary)
            fixture = SourceFixture(root / 'source.db')
            fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
            baseline_open = datetime(2025, 11, 29, 16, 1, tzinfo=UTC)
            for offset in range(-100, 1):
                opened = baseline_open + timedelta(seconds=210 * offset)
                fixture.draw(1100 + offset, opened, balls=(1, 3, 5))
                fixture.prediction(1100 + offset, opened)
            targets = []
            for offset in range(12):
                year, month = (2025, 12) if offset == 0 else (2026, offset)
                opened = datetime(year, month, calendar.monthrange(year, month)[1], 4, tzinfo=UTC)
                issue = 1101 + offset
                fixture.draw(issue, opened, balls=(1, 3, 5))
                fixture.prediction(issue, opened)
                targets.append((opened.date().isoformat(), str(issue)))
            fixture.draw(1113, datetime(2026, 12, 1, 4, tzinfo=UTC))
            fixture.prediction(1113, datetime(2026, 12, 1, 4, tzinfo=UTC))
            fixture.draw(1114, datetime(2026, 12, 2, 0, tzinfo=UTC))
            fixture.prediction(1114, datetime(2026, 12, 2, 0, tzinfo=UTC))
            source = RuleLabSource(fixture.path)
            store = RuleLabStore(root / 'lab' / 'lab.db')
            store.initialize()
            old_now = stamp(baseline_open + timedelta(hours=2))
            now = '2026-12-02T04:00:00Z'
            config = normalize_batch_payload(dict(name='十二个月连续追加', mode='backtest', predictor_id=1,
                metric='combo', template_pack='combo_long_miss_v1', start_date='2025-11-30',
                end_date='2025-11-30', risk_policy=policy()), now=old_now)
            batch = store.create_batch(1, config, 'fixture', build_experiments('combo', template_pack='combo_long_miss_v1'), now=old_now)
            batch_id = batch['id']
            store.set_batch_state(1, batch_id, 'running', now=old_now)
            for _ in range(20):
                RuleLabWorker(store, source).tick(now=old_now)
                if store.get_batch(1, batch_id)['status'] == 'completed':
                    break
            self.assertEqual(store.get_batch(1, batch_id)['status'], 'completed')
            original_ids = {item['id'] for item in store.list_experiments(1, batch_id)}
            self.assertEqual(len(original_ids), 26)
            preview = store.continuation_preview(1, 'batch', batch_id, now=now)
            self.assertTrue(preview['eligible'], preview)
            store.set_continuation(1, 'batch', batch_id, dict(request_id='qa-year-adoption',
                expected_revision=preview['expected_revision'], capture_enabled=True, compute_enabled=False,
                daily_at='00:10', timezone='Asia/Shanghai'), now=now)
            with sqlite3.connect(store.db_path) as connection:
                baseline_payload = connection.execute('SELECT payload FROM lab_checkpoints ORDER BY id LIMIT 1').fetchone()[0]
            for _ in range(20):
                RuleLabWorker(store, source).tick(now=now)
                if store.continuation_preview(1, 'batch', batch_id, now=now)['continuation']['captured_through_issue'] == '1114':
                    break
            for index, (end_date, through) in enumerate(targets, 1):
                preview = store.continuation_preview(1, 'batch', batch_id, now=now)
                store.request_append(1, 'batch', batch_id, dict(request_id='qa-year-month-%02d' % index,
                    expected_revision=preview['expected_revision'], end_date=end_date), now=now)
                for _ in range(12):
                    RuleLabWorker(store, source).tick(now=now)
                    summary = store.continuation_preview(1, 'batch', batch_id, now=now)['continuation']
                    if summary['calculated_through_issue'] == through and not summary['active_segment_id']:
                        break
                self.assertEqual(summary['calculated_through_issue'], through, summary)
                self.assertIsNone(summary['active_segment_id'], summary)
                self.assertEqual({item['id'] for item in store.list_experiments(1, batch_id)}, original_ids)
            segments = store.list_segments(1, 'batch', batch_id, limit=50)['segments']
            self.assertEqual(len(segments), 13)
            self.assertEqual(sum(item['kind'] == 'append' for item in segments), 12)
            self.assertEqual(store.get_batch(1, batch_id)['end_date'], '2025-11-30')
            self.assertEqual(store.get_batch(1, batch_id)['processed_issues'], 13)
            with sqlite3.connect(store.db_path) as connection:
                self.assertEqual(connection.execute('SELECT count(*) FROM lab_experiments').fetchone()[0], 26)
                self.assertEqual(connection.execute('SELECT payload FROM lab_checkpoints ORDER BY id LIMIT 1').fetchone()[0], baseline_payload)

            # The same data also exercises a year of downtime: a daily series
            # catches up in bounded chunks after a fresh worker starts.
            automatic = RuleLabStore(root / 'automatic' / 'lab.db')
            automatic.initialize()
            from tests.test_rule_lab_results_store import install_settlement_trace
            install_settlement_trace(automatic)
            auto_batch = automatic.create_batch(1, config, 'fixture',
                build_experiments('combo', template_pack='combo_long_miss_v1'), now=old_now)
            auto_id = auto_batch['id']
            automatic.set_batch_state(1, auto_id, 'running', now=old_now)
            for _ in range(20):
                RuleLabWorker(automatic, source).tick(now=old_now)
                if automatic.get_batch(1, auto_id)['status'] == 'completed':
                    break
            preview = automatic.continuation_preview(1, 'batch', auto_id, now=old_now)
            automatic.set_continuation(1, 'batch', auto_id, dict(request_id='qa-year-auto-adoption',
                expected_revision=preview['expected_revision'], capture_enabled=True, compute_enabled=True,
                daily_at='00:10', timezone='Asia/Shanghai'), now=old_now)
            for _ in range(100):
                RuleLabWorker(automatic, source).tick(now=now)
                summary = automatic.continuation_preview(1, 'batch', auto_id, now=now)['continuation']
                if summary['calculated_through_issue'] == '1113' and not summary['active_segment_id']:
                    break
            self.assertEqual(summary['calculated_through_issue'], '1113', summary)
            self.assertIsNone(summary['active_segment_id'], summary)
            self.assertEqual(len(automatic.list_experiments(1, auto_id)), 26)
            catchup = automatic.list_segments(1, 'batch', auto_id, limit=50)['segments']
            self.assertGreater(len(catchup), 4)
            by_cutoff = {1100: '2025-11-30', **{int(issue): day for day, issue in targets}, 1113: '2026-12-01'}
            ordered = sorted(catchup, key=lambda item: item['sequence'])
            for before, after in zip(ordered, ordered[1:]):
                prior_day = datetime.fromisoformat(by_cutoff[int(before['cutoff_issue'])]).date()
                target_day = datetime.fromisoformat(after['requested_end_date']).date()
                self.assertLessEqual((target_day - prior_day).days, 90)
            stable = worker_fixture.ContinuousWorkerTests.economic_state(automatic)
            RuleLabWorker(automatic, source).tick(now=now)
            self.assertEqual(worker_fixture.ContinuousWorkerTests.economic_state(automatic), stable)
            self.assertEqual(len(automatic.list_segments(1, 'batch', auto_id, limit=50)['segments']), len(catchup))


class ContinuationProductionSnapshotQaTests(unittest.TestCase):
    @unittest.skipUnless(os.environ.get('RULE_LAB_MIGRATION_SNAPSHOT'),
                         'Set RULE_LAB_MIGRATION_SNAPSHOT for the explicit production-copy audit')
    def test_production_copy_migrates_twice_without_changing_any_old_column(self):
        snapshot = Path(os.environ['RULE_LAB_MIGRATION_SNAPSHOT']).resolve()
        source_bytes = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        columns, before = original_rows(snapshot,exclude_result_metadata=True)
        with tempfile.TemporaryDirectory(prefix='rule-lab-production-copy-qa-') as temporary:
            copy_path = Path(temporary) / 'lab.db'
            shutil.copyfile(snapshot, copy_path)
            store = RuleLabStore(copy_path)
            store.initialize()
            self.assertEqual(original_rows(copy_path, columns,exclude_result_metadata=True)[1], before)
            store.initialize()
            self.assertEqual(original_rows(copy_path, columns,exclude_result_metadata=True)[1], before)
            with sqlite3.connect(copy_path) as connection:
                self.assertEqual(connection.execute('PRAGMA user_version').fetchone()[0], 8)
                metadata=json.loads(connection.execute("SELECT value_json FROM lab_runtime WHERE key='result_retention'").fetchone()[0])
                self.assertEqual((metadata['mode'],metadata['removed_rows']),('results_only',0))
                self.assertEqual(connection.execute('PRAGMA quick_check').fetchone()[0], 'ok')
                self.assertEqual(connection.execute('PRAGMA foreign_key_check').fetchall(), [])
                self.assertEqual(connection.execute('SELECT count(*) FROM lab_series').fetchone()[0], 0)
                self.assertEqual(connection.execute('SELECT count(*) FROM lab_experiments WHERE batch_id=8').fetchone()[0], 26)
            preview = store.continuation_preview(1, 'batch', 8, now='2026-11-27T12:00:00Z')
            self.assertTrue(preview['eligible'], preview)
            self.assertEqual(preview['next_issue'], '3511344')
            self.assertEqual(preview['saved_experiments'], 26)
            self.assertFalse(preview['continuation']['enabled'])
            self.assertEqual(original_rows(copy_path, columns,exclude_result_metadata=True)[1], before)
        self.assertEqual(hashlib.sha256(snapshot.read_bytes()).hexdigest(), source_bytes)

    @unittest.skipUnless(os.environ.get('RULE_LAB_MIGRATION_SNAPSHOT') and os.environ.get('RULE_LAB_READONLY_SOURCE_DB'),
                         'Set snapshot and read-only source paths for the explicit production-copy append audit')
    def test_production_copy_append_keeps_twenty_six_ids_and_reconciles_every_cent(self):
        snapshot = Path(os.environ['RULE_LAB_MIGRATION_SNAPSHOT']).resolve()
        snapshot_hash = hashlib.sha256(snapshot.read_bytes()).hexdigest()
        now = stamp(datetime.now(UTC))
        today = datetime.now(UTC).astimezone(timezone(timedelta(hours=8))).date().isoformat()
        source = RuleLabSource(os.environ['RULE_LAB_READONLY_SOURCE_DB'])
        known_latest = source.latest_draw(observed_at=now)
        self.assertIsNotNone(known_latest)
        known_available = max(known_latest['open_at'], known_latest['source_created_at']) <= now
        with tempfile.TemporaryDirectory(prefix='rule-lab-production-append-qa-') as temporary:
            copy_path = Path(temporary) / 'lab.db'
            shutil.copyfile(snapshot, copy_path)
            store = RuleLabStore(copy_path)
            store.initialize()
            from tests.test_rule_lab_results_store import install_settlement_trace
            install_settlement_trace(store)
            observed_outcomes={}
            class AuditedWorker(RuleLabWorker):
                def _outcome(worker,experiment,decision,draw):
                    outcome=super()._outcome(experiment,decision,draw)
                    observed_outcomes[(experiment['id'],str(decision['issue_no']))]={**outcome,'stake_cents':decision['stake_cents']}
                    return outcome
            with sqlite3.connect(copy_path) as connection:
                connection.row_factory = sqlite3.Row
                original = {row['id']: dict(row) for row in connection.execute(
                    'SELECT * FROM lab_experiments WHERE batch_id=8 ORDER BY id')}
                original_batch = dict(connection.execute('SELECT * FROM lab_batches WHERE id=8').fetchone())
                old_daily = [tuple(row) for row in connection.execute(
                    "SELECT * FROM lab_daily WHERE stat_date<='2026-11-26' ORDER BY experiment_id,stat_date")]
            self.assertEqual(len(original), 26)
            actual_last = int(json.loads(original_batch['cursor_json'])['last_issue_no'])
            self.assertEqual(actual_last, 3511343)
            preview = store.continuation_preview(1, 'batch', 8, now=now)
            self.assertTrue(preview['eligible'], preview)
            store.set_continuation(1, 'batch', 8, dict(request_id='qa-copy-adoption',
                expected_revision=preview['expected_revision'], capture_enabled=True, compute_enabled=False,
                daily_at='00:10', timezone='Asia/Shanghai'), now=now)
            configured = store.continuation_preview(1, 'batch', 8, now=now)
            store.request_append(1, 'batch', 8, dict(request_id='qa-copy-append',
                expected_revision=configured['expected_revision'], end_date=today), now=now)
            max_tick_ms = 0.0
            worker = AuditedWorker(store, source)
            for index in range(300):
                tick = worker.tick(now=now)
                max_tick_ms = max(max_tick_ms, tick['elapsed_ms'])
                state = store.continuation_preview(1, 'batch', 8, now=now)
                self.assertNotIn(state['continuation']['state'], ('archive_gap', 'error', 'storage_paused'), state)
                calculated = int(state['continuation'].get('calculated_through_issue') or 0)
                if calculated > actual_last and not state['continuation'].get('active_segment_id'):
                    break
                # A restart is an additional durability check, and also means
                # the test does not wait on a real fifteen-second capture timer.
                worker = AuditedWorker(store, source)
            else:
                self.fail('Production-copy continuation did not complete an append: %r' % state)
            self.assertFalse(state['continuation']['compute_enabled'])
            self.assertGreater(calculated, actual_last)
            if known_available:
                self.assertGreaterEqual(calculated, int(known_latest['issue_no']),
                                        'A current-day append must catch the known source horizon, not just one captured page')
            with sqlite3.connect(copy_path) as connection:
                connection.row_factory = sqlite3.Row
                current = {row['id']: dict(row) for row in connection.execute(
                    'SELECT * FROM lab_experiments WHERE batch_id=8 ORDER BY id')}
                self.assertEqual(set(current), set(original))
                fixed = ('batch_id', 'condition_key', 'name', 'condition_json',
                         'settlement_rule_id', 'settlement_snapshot_json')
                for experiment_id, before in original.items():
                    after = current[experiment_id]
                    self.assertEqual({key: after[key] for key in fixed}, {key: before[key] for key in fixed})
                    before_stats, after_stats = json.loads(before['stats_json']), json.loads(after['stats_json'])
                    committed=connection.execute('''SELECT experiment_id,issue_no FROM test_settlement_trace
                        WHERE experiment_id=? AND issue_key>? ORDER BY issue_key''',(experiment_id,actual_last)).fetchall()
                    keys=[(row['experiment_id'],row['issue_no']) for row in committed]
                    self.assertEqual(len(keys),len(set(keys)),'A pending settlement was applied twice')
                    self.assertTrue(all(key in observed_outcomes for key in keys),'Committed settlement lacks an observed actual outcome')
                    settled=[observed_outcomes[key] for key in keys if observed_outcomes[key]['status']=='settled']
                    self.assertEqual(after_stats['net_cents'] - before_stats['net_cents'],sum(row['net_cents'] for row in settled))
                    self.assertEqual(after_stats['settled_count'] - before_stats['settled_count'],len(settled))
                    self.assertEqual(after_stats['stake_cents'] - before_stats['stake_cents'],sum(row['stake_cents'] for row in settled))
                first_decision = connection.execute('SELECT MIN(issue_key) FROM test_settlement_trace WHERE issue_key>?',
                                                    (actual_last,)).fetchone()[0]
                self.assertEqual(first_decision, 3511344)
                self.assertEqual([tuple(row) for row in connection.execute(
                    "SELECT * FROM lab_daily WHERE stat_date<='2026-11-26' ORDER BY experiment_id,stat_date")], old_daily)
                after_batch = dict(connection.execute('SELECT * FROM lab_batches WHERE id=8').fetchone())
                self.assertEqual((after_batch['start_date'], after_batch['end_date']),
                                 (original_batch['start_date'], original_batch['end_date']))
                self.assertEqual(connection.execute('PRAGMA quick_check').fetchone()[0], 'ok')
                self.assertEqual(connection.execute('PRAGMA foreign_key_check').fetchall(), [])
                decisions = connection.execute('SELECT count(*) FROM test_settlement_trace WHERE issue_key>?', (actual_last,)).fetchone()[0]
                self.assertEqual(connection.execute("SELECT count(*) FROM lab_decisions WHERE issue_key>? AND status!='pending'",(actual_last,)).fetchone()[0],0)
            report = dict(first_issue=3511344, calculated_through_issue=calculated, experiment_count=len(current),
                          known_available_issue=int(known_latest['issue_no']) if known_available else None,
                          new_decisions=decisions, ticks=index + 1, max_tick_ms=max_tick_ms,
                          max_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                          storage_bytes=store.storage_status()['used_bytes'])
            evidence = os.environ.get('RULE_LAB_QA_EVIDENCE_PATH')
            if evidence:
                Path(evidence).write_text(json.dumps(report, ensure_ascii=False, indent=2))
        self.assertEqual(hashlib.sha256(snapshot.read_bytes()).hexdigest(), snapshot_hash)


if __name__ == '__main__':
    unittest.main()
