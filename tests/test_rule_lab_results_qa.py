"""Independent result-only storage attacks on disposable SQLite files."""
from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore


NOW = '2026-11-30T12:00:00Z'


class ResultsIndependentQaTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='rule-lab-results-qa-')
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / 'lab' / 'lab.db'
        self.store = RuleLabStore(self.path)
        self.store.initialize()

    def create(self, *, mode='live', start_date=None, end_date=None):
        payload = dict(name='independent-qa', mode=mode, predictor_id=1, metric='big_small')
        if mode == 'backtest':
            payload.update(start_date=start_date, end_date=end_date)
        config = normalize_batch_payload(payload, now=NOW)
        return self.store.create_batch(1, config, 'fixture', build_experiments('big_small')[:1], now=NOW)

    def work(self, batch_id):
        return self.store.get_work_item(batch_id)

    def decision(self, work, issue):
        return dict(experiment_id=work['experiments'][0]['id'], issue_no=str(issue),
            bet_value='大', stake_cents=100, decided_at=NOW, decision_mode=work['batch']['mode'],
            feature_issue_no=str(issue - 1), condition_value=1)

    @staticmethod
    def outcome(decision):
        return dict(experiment_id=decision['experiment_id'], issue_no=decision['issue_no'],
            status='settled', settled_at=NOW, result_type='hit', net_cents=100,
            direction_hit=True, refund_reason='', skip_reason='')

    def protected_state(self):
        with sqlite3.connect(self.path) as connection:
            return {
                'experiments': connection.execute('''SELECT id,stats_json,risk_runtime_json,
                    last_decided_issue,last_settled_issue FROM lab_experiments ORDER BY id''').fetchall(),
                'daily': connection.execute('SELECT * FROM lab_daily ORDER BY experiment_id,stat_date').fetchall(),
                'pending': connection.execute("SELECT * FROM lab_decisions WHERE status='pending' ORDER BY id").fetchall(),
                'batches': connection.execute('SELECT id,revision,cursor_json,processed_issues,status FROM lab_batches ORDER BY id').fetchall(),
            }

    def maintain_to_idle(self, *, limit=113, attempts=100, now=NOW):
        maintenance = getattr(self.store, 'retention_maintenance', None)
        if not callable(maintenance):
            # Exercise the old public maintenance behavior for RED. Absence of
            # the new helper alone is not evidence for the data boundary cases.
            self.store.prune(now=now, limit=limit)
            return
        for _ in range(attempts):
            result = maintenance(now=now, limit=limit)
            self.assertGreaterEqual(result['processed_rows'], 0)
            self.assertLessEqual(result['processed_rows'], limit)
            self.assertGreaterEqual(result['scanned_rows'], 0)
            self.assertLessEqual(result['scanned_rows'], limit)
            if not result['maintenance_pending']:
                return
        self.fail('Bounded retention did not converge on a finite disposable fixture')

    def test_sqlite_abort_after_pending_delete_rolls_back_results_and_allows_one_retry(self):
        batch = self.create()
        self.store.set_batch_state(1, batch['id'], 'running', now=NOW)
        work = self.work(batch['id'])
        decision = self.decision(work, 1001)
        self.store.commit_step(batch['id'], work['batch']['revision'], cursor={'last_issue_no': '1001'},
                              now=NOW, decisions=[decision])
        before = self.protected_state()
        work = self.work(batch['id'])
        with sqlite3.connect(self.path) as connection:
            connection.execute('''CREATE TRIGGER qa_abort_settlement AFTER DELETE ON lab_decisions
                WHEN OLD.status='pending' BEGIN SELECT RAISE(ABORT,'qa-stop-after-delete'); END''')
        with self.assertRaises((sqlite3.IntegrityError, RuleLabError)):
            self.store.commit_step(batch['id'], work['batch']['revision'], cursor=work['batch']['cursor'],
                                  now=NOW, outcomes=[self.outcome(decision)])
        self.assertEqual(self.protected_state(), before)
        with sqlite3.connect(self.path) as connection:
            connection.execute('DROP TRIGGER qa_abort_settlement')
        for expected in (1, 0):
            work = self.work(batch['id'])
            result = self.store.commit_step(batch['id'], work['batch']['revision'], cursor=work['batch']['cursor'],
                                           now=NOW, outcomes=[self.outcome(decision)])
            self.assertEqual(result['settled'], expected)
        result = self.store.list_experiments(1, batch['id'])[0]
        self.assertEqual((result['settled_count'], result['net_profit']), (1, 1))
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_decisions').fetchone()[0], 0)

    def seed_cache(self):
        """Six finite days: each contains 100 periods, with one shared source."""
        opened = datetime(2026, 11, 23, 16, tzinfo=timezone.utc)
        with self.store._connect(write=True) as connection:
            for start in range(1000, 1600, 100):
                draws, predictions = [], []
                for issue in range(start, start + 100):
                    at = opened + timedelta(days=(issue - 1000) // 100, seconds=210 * ((issue - 1000) % 100))
                    stamp = at.isoformat().replace('+00:00', 'Z')
                    draws.append(dict(issue_no=str(issue), source_id=issue, open_at=stamp,
                        source_created_at=stamp, observed_at=NOW, triplet=[3, 6, 8], input_hash='draw-' + str(issue)))
                    predictions.append(dict(predictor_id=1, issue_no=str(issue), source_id=issue,
                        created_at=stamp, settled_at=stamp, observed_at=NOW, status='settled',
                        big_small='大', odd_even='单', combo='大单', input_hash='prediction-' + str(issue)))
                self.store._cache(connection, draws, predictions)

    def live_old_pending(self):
        batch = self.create()
        self.store.set_batch_state(1, batch['id'], 'running', now=NOW)
        work = self.work(batch['id'])
        decision = self.decision(work, 1001)
        self.store.commit_step(batch['id'], work['batch']['revision'], cursor={'last_issue_no': '1599'},
                              now=NOW, decisions=[decision])
        self.store.set_batch_state(1, batch['id'], 'paused', now=NOW)
        self.seed_cache()
        return batch

    def input_issues(self, table):
        with sqlite3.connect(self.path) as connection:
            return {row[0] for row in connection.execute('SELECT issue_key FROM ' + table)}

    def test_isolated_old_pending_does_not_pin_all_intervening_consumed_input(self):
        batch = self.live_old_pending()
        before = self.store.list_experiments(1, batch['id'])
        self.maintain_to_idle()
        required = {1001} | set(range(1500, 1600))
        self.assertEqual(self.input_issues('lab_draws'), required)
        self.assertEqual(self.input_issues('lab_predictions'), required)
        self.assertEqual(self.store.list_experiments(1, batch['id']), before)
        self.assertEqual([row['issue_no'] for row in self.store.get_pending_decisions(batch['id'])], ['1001'])

    def test_unstarted_same_source_consumer_protects_only_its_finite_dates_and_warmup(self):
        batch = self.live_old_pending()
        draft = self.create(mode='backtest', start_date='2026-11-26', end_date='2026-11-26')
        self.maintain_to_idle()
        # 1200..1299 are the target date and 1100..1199 are its preceding
        # 100-period feature window. The unrelated day 1300..1399 is removable.
        required = {1001} | set(range(1100, 1300)) | set(range(1500, 1600))
        self.assertEqual(self.input_issues('lab_draws'), required)
        self.assertEqual(self.input_issues('lab_predictions'), required)
        self.assertEqual(self.store.get_batch(1, draft['id'])['status'], 'paused')
        self.assertIsNone(self.store.get_batch(1, draft['id'])['started_at'])
        self.assertEqual(len(self.store.get_pending_decisions(batch['id'])), 1)

    def test_unstarted_dates_protect_archive_predictions_with_null_open_time(self):
        from pc28touzhu.services.rule_lab_archive import ensure_stream, _cached_page, _save_page
        self.live_old_pending()
        self.create(mode='backtest', start_date='2026-11-26', end_date='2026-11-26')
        with self.store._connect(write=True) as connection:
            stream = ensure_stream(connection, 1, 1599, NOW, history_start_issue=1000)
            for begin in range(1000, 1600, 200):
                draws, predictions, coverage = _cached_page(connection, stream, begin, begin + 199, limit=200)
                _save_page(connection, stream, draws, predictions, coverage, NOW)
            self.assertIsNone(connection.execute('''SELECT open_at FROM lab_input_versions
                WHERE kind='prediction' AND issue_key=1201 ORDER BY revision DESC LIMIT 1''').fetchone()[0])
        self.maintain_to_idle(attempts=150)
        with sqlite3.connect(self.path) as connection:
            prediction_issues = {row[0] for row in connection.execute(
                "SELECT DISTINCT issue_key FROM lab_input_versions WHERE kind='prediction' AND predictor_id=1")}
        self.assertTrue(set(range(1100, 1300)).issubset(prediction_issues),
                        'Queued target predictions have NULL open_at but still inherit the draw date')
        self.assertNotIn(1301, prediction_issues, 'Unreferenced next day must not be pinned by the queued range')

    def test_legacy_frontier_includes_later_publication_of_an_earlier_settled_issue(self):
        batch = self.create(mode='backtest', start_date='2026-11-24', end_date='2026-11-29')
        experiment = self.store.list_experiments(1, batch['id'])[0]['id']
        self.seed_cache()
        late_visible = '2026-11-30T11:00:00Z'
        with sqlite3.connect(self.path) as connection:
            connection.execute("UPDATE lab_draws SET source_created_at=? WHERE issue_key=1000", (late_visible,))
            connection.execute('''UPDATE lab_batches SET status='completed',started_at=?,processed_issues=600,
                cursor_json=? WHERE id=?''', (NOW, json.dumps(dict(last_issue_no='1599', scan_after_issue='1599',
                    warm_before_issue='1000', replay_complete=True, replay_scheduler_version=2,
                    simulation_time='2026-11-28T21:45:50Z')), batch['id']))
            stats = json.loads(connection.execute('SELECT stats_json FROM lab_experiments WHERE id=?',
                                                  (experiment,)).fetchone()[0])
            stats.update(settled_count=2, hit_count=2, direction_hit_count=2, trigger_count=2,
                         stake_cents=200, net_cents=200, equity_peak_cents=200)
            connection.execute('''UPDATE lab_experiments SET stats_json=?,last_decided_issue=1599,
                last_settled_issue=1599 WHERE id=?''', (json.dumps(stats), experiment))
            connection.executemany('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,
                stake_cents,decided_at,decision_mode,condition_value_json,status,settled_at,finalized_at,
                result_type,net_cents) VALUES(?,?,?,'大',100,?,'backtest','null','settled',?,?,'hit',100)''',
                [(experiment, str(issue), issue, NOW, NOW, NOW) for issue in (1000, 1599)])
        before_stats = self.store.list_experiments(1, batch['id'])
        self.maintain_to_idle()
        saved = self.work(batch['id'])['batch']['cursor']['retention_v1']
        self.assertTrue(saved['frontier_verified'], saved)
        self.assertEqual(saved['settled_available_at'], late_visible,
                         'Checking only the largest settled issue understates the prior visibility frontier')
        self.assertEqual(self.store.list_experiments(1, batch['id']), before_stats)

    def test_retention_can_remove_old_terminal_rows_even_above_new_write_budget(self):
        batch = self.create()
        experiment = self.store.list_experiments(1, batch['id'])[0]['id']
        with sqlite3.connect(self.path) as connection:
            connection.executemany('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,
                stake_cents,decided_at,decision_mode,condition_value_json,status,settled_at,finalized_at,
                result_type,net_cents) VALUES(?,?,?,'大',100,?,'live','null','settled',?,?,'hit',100)''',
                [(experiment, str(issue), issue, NOW, NOW, NOW) for issue in range(1000, 1600)])
            stats = json.loads(connection.execute('SELECT stats_json FROM lab_experiments WHERE id=?',
                                                  (experiment,)).fetchone()[0])
            stats.update(settled_count=600, hit_count=600, direction_hit_count=600, trigger_count=600,
                         stake_cents=60000, net_cents=60000, equity_peak_cents=60000)
            connection.execute('''UPDATE lab_experiments SET stats_json=?,last_decided_issue=1599,
                last_settled_issue=1599 WHERE id=?''', (json.dumps(stats), experiment))
        before = self.store.list_experiments(1, batch['id'])
        self.store.budget_bytes = 1024
        self.maintain_to_idle(limit=37)
        self.assertEqual(self.store.list_experiments(1, batch['id']), before,
                         'Maintenance must never derive new profit from old terminal evidence')
        with sqlite3.connect(self.path) as connection:
            self.assertEqual(connection.execute("SELECT count(*) FROM lab_decisions WHERE status!='pending'").fetchone()[0], 0)

    def test_approved_long_replay_keeps_later_chunks_after_original_consumer_is_deleted(self):
        from pc28touzhu.rule_lab_worker import RuleLabWorker
        from pc28touzhu.services.rule_lab_service import RuleLabService
        from pc28touzhu.services.rule_lab_source import RuleLabSource
        from tests.test_rule_lab_source import SourceFixture, stamp
        fixture = SourceFixture(Path(self.temporary.name) / 'source.db')
        fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        first = datetime(2026, 7, 31, 16, tzinfo=timezone.utc)
        for offset in range(-100, 103):
            opened = first + (timedelta(seconds=210 * offset) if offset <= 0 else timedelta(days=offset))
            fixture.draw(1100 + offset, opened)
            fixture.prediction(1100 + offset, opened)
        source = RuleLabSource(fixture.path)
        service = RuleLabService(self.store, source)
        # Existing finite queued ranges legitimately retain the original interval
        # while the original result is calculated. They are removed later.
        protectors = [self.create(mode='backtest', start_date=begin, end_date=end)
                      for begin, end in [('2026-08-01', '2026-10-29'), ('2026-10-30', '2026-11-10')]]
        parent = self.create(mode='backtest', start_date='2026-08-01', end_date='2026-08-01')
        self.store.set_batch_state(1, parent['id'], 'running', now=NOW)
        ticks = 0
        def run_until(predicate):
            nonlocal ticks
            for _ in range(100):
                ticks += 1
                clock = stamp(datetime.fromisoformat(NOW.replace('Z', '+00:00')) + timedelta(seconds=ticks * 15))
                RuleLabWorker(self.store, source).tick(now=clock)
                if predicate():
                    return
            self.fail('Long replay did not reach the expected state')
        run_until(lambda: self.store.get_batch(1, parent['id'])['status'] == 'completed')
        preview = service.continuation_preview(1, 'batch', parent['id'], now=NOW)
        self.assertTrue(preview['eligible'], preview)
        service.set_continuation(1, 'batch', parent['id'], dict(request_id='qa-long-enable',
            expected_revision=preview['expected_revision'], capture_enabled=True, compute_enabled=False,
            daily_at='00:10', timezone='Asia/Shanghai'), now=NOW)
        run_until(lambda: service.continuation_preview(1, 'batch', parent['id'], now=NOW)
                  ['continuation']['captured_through_issue'] == '1202')
        for end in ('2026-10-30', '2026-11-10'):
            preview = service.continuation_preview(1, 'batch', parent['id'], now=NOW)
            service.request_append(1, 'batch', parent['id'], dict(request_id='qa-long-' + end,
                expected_revision=preview['expected_revision'], end_date=end), now=NOW)
            run_until(lambda: self.store.get_batch(1, parent['id'])['status'] == 'completed' and
                      not service.continuation_preview(1, 'batch', parent['id'], now=NOW)['continuation']['active_segment_id'])
        experiment = self.store.list_experiments(1, parent['id'])[0]
        # This tests an already-approved long local replay, including migrated
        # ones. New adoption intentionally no longer seeds all old A. Populate
        # the fixture's still-protected local view explicitly, without requiring
        # production to recreate that removed retention behavior.
        from pc28touzhu.services.rule_lab_archive import _cached_page, _save_page
        with self.store._connect(write=True) as connection:
            stream = dict(connection.execute('SELECT * FROM lab_capture_streams ORDER BY id LIMIT 1').fetchone())
            for begin in (1000, 1200):
                draws, predictions, coverage = _cached_page(connection, stream, begin, min(begin + 199, 1202), limit=200)
                _save_page(connection, stream, draws, predictions, coverage, NOW)
        replay = service.replay_experiment(1, parent['id'], experiment['id'],
                                          dict(stake_amount=10, risk_amount_mode='keep'), now=NOW)['batch']
        with sqlite3.connect(self.path) as connection:
            target = connection.execute('''SELECT replay_target_issue FROM lab_series
                WHERE owner_kind='batch' AND owner_id=?''', (replay['id'],)).fetchone()[0]
            cutoff = connection.execute('''SELECT cutoff_issue FROM lab_segments s JOIN lab_series r
                ON r.id=s.series_id WHERE r.owner_kind='batch' AND r.owner_id=? ORDER BY s.id DESC LIMIT 1''',
                (replay['id'],)).fetchone()[0]
        self.assertGreater(target, cutoff, 'The fixture must require at least a second approved chunk')
        # Delete every earlier consumer through the real public lifecycle. The
        # new paused replay alone must preserve its already-approved later range.
        for old in protectors + [parent]:
            view = self.store.delete_preview(1, 'batch', old['id'], now=NOW)
            self.store.request_delete(1, 'batch', old['id'], dict(request_id='qa-long-delete-' + str(old['id']),
                expected_revision=view['expected_revision'], confirm_name=view['name']), now=NOW)
        for _ in range(100):
            state = self.store.management_maintenance(now=NOW, limit=500, max_pages=256)
            if not state['maintenance_pending']:
                break
        else:
            self.fail('Temporary old consumers did not finish deleting')
        self.maintain_to_idle(attempts=100)
        with sqlite3.connect(self.path) as connection:
            for kind in ('draw', 'prediction'):
                present = {row[0] for row in connection.execute('''SELECT DISTINCT issue_key FROM lab_input_versions
                    WHERE kind=? AND issue_key>? AND issue_key<=?''', (kind, cutoff, target))}
                self.assertEqual(present, set(range(cutoff + 1, target + 1)), kind)
        fixture.execute('DELETE FROM predictions')
        fixture.execute('DELETE FROM lottery_draws')
        service.set_batch_state(1, replay['id'], {'state': 'running'}, now=NOW)
        run_until(lambda: self.store.get_batch(1, replay['id'])['status'] == 'completed' and
                  int(self.store.get_work_item(replay['id'])['batch']['cursor']['last_issue_no']) == target)
        result = self.store.list_experiments(1, replay['id'])[0]
        # A continuous segment deliberately leaves its final, not-yet-visible
        # result pending instead of fast-forwarding the simulation clock.
        pending = self.store.get_pending_decisions(replay['id'])
        self.assertEqual([row['issue_no'] for row in pending], [str(target)])
        self.assertEqual(result['settled_count'], target - 1100)
        self.assertEqual(result['settled_count'] + len(pending), target - 1100 + 1)
        self.assertAlmostEqual(result['net_profit'], (target - 1100) * 9.98, places=2)

    def test_completed_segment_history_does_not_retain_complete_per_experiment_state(self):
        from tests.test_rule_lab_continuous_worker import ContinuousWorkerTests
        from tests.rule_lab_results_benchmark import economic_result
        from pc28touzhu.services.rule_lab_series import _unpack
        fixture = ContinuousWorkerTests(methodName='runTest')
        fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        fixture.fixture.draw(1108, datetime(2026, 12, 2, 16, 5, tzinfo=timezone.utc))
        fixture.fixture.prediction(1108, datetime(2026, 12, 2, 16, 5, tzinfo=timezone.utc))
        store, batch_id = fixture.make_baseline('bounded-full-state')
        self.store = store
        self.path = store.db_path
        fixture.enable(store, batch_id)
        fixture.capture(store, batch_id, through='1108')
        for index, (end, through) in enumerate((('2026-11-30', '1103'), ('2026-12-01', '1106'), ('2026-12-02', '1107'))):
            fixture.append(store, batch_id, end, 'qa-bounded-segment-' + str(index))
            fixture.finish_append(store, batch_id, through)
        before = economic_result(store.db_path)
        maintenance_now = (fixture.now + timedelta(minutes=5)).isoformat().replace('+00:00', 'Z')
        self.maintain_to_idle(now=maintenance_now)
        self.assertEqual(economic_result(store.db_path), before)
        with sqlite3.connect(store.db_path) as connection:
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_segments').fetchone()[0], 4)
            self.assertLessEqual(connection.execute('SELECT count(*) FROM lab_checkpoints WHERE batch_id=?',
                                                     (batch_id,)).fetchone()[0], 1)
            for payload, summary in connection.execute('''SELECT baseline_json,summary_json
                FROM lab_segment_batches WHERE status='completed' AND batch_id=?''', (batch_id,)):
                value = _unpack(payload)
                self.assertFalse(value.get('checkpoint_retained', True), value)
                self.assertNotIn('experiments', value)
                self.assertNotIn('"cumulative"', summary)
                self.assertNotIn('"risk_runtime"', summary)
            for (summary,) in connection.execute("SELECT summary_json FROM lab_segments WHERE status='completed'"):
                self.assertNotIn('"cumulative"', summary)
                self.assertNotIn('"risk_runtime"', summary)


if __name__ == '__main__':
    unittest.main()
