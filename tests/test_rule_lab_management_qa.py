"""Independent lifecycle attacks against disposable SQLite and copied snapshots."""
from __future__ import annotations

import importlib.util
import io
import json
import sqlite3
import subprocess
import tempfile
import threading
import unittest
from contextlib import redirect_stderr
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_series import completed_baseline, NOW
from tests.test_rule_lab_source import SourceFixture

spec = importlib.util.spec_from_file_location('management_qa_queue',
    Path(__file__).parents[1] / 'scripts/maintenance/run_rule_lab_batch_queue.py')
queue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(queue)
storage_cli_spec = importlib.util.spec_from_file_location('management_qa_storage_cli',
    Path(__file__).parents[1] / 'scripts/maintenance/prepare_rule_lab_storage.py')
storage_cli = importlib.util.module_from_spec(storage_cli_spec)
storage_cli_spec.loader.exec_module(storage_cli)


class ManagementIndependentQaTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.store = RuleLabStore(self.root / 'lab' / 'lab.db')
        self.store.initialize()
        self.fixture = SourceFixture(self.root / 'source.db')
        self.service = RuleLabService(self.store, RuleLabSource(self.fixture.path))

    def create(self, *, owner=1, name='independent', risk=None):
        config = normalize_batch_payload(dict(name=name, mode='backtest', predictor_id=1,
            metric='big_small', start_date='2026-11-26', end_date='2026-11-26', risk_policy=risk), now=NOW)
        return self.store.create_batch(owner, config, 'fixture', build_experiments('big_small'), now=NOW)

    def delete(self, kind, owner_id, request_id, *, user_id=1):
        self.assertTrue(hasattr(self.store, 'delete_preview'), 'Delete lifecycle is not implemented')
        preview = self.store.delete_preview(user_id, kind, owner_id, now=NOW)
        self.assertTrue(preview['eligible'], preview)
        payload = dict(request_id=request_id, expected_revision=preview['expected_revision'],
                       confirm_name=preview['name'])
        return self.store.request_delete(user_id, kind, owner_id, payload, now=NOW), payload

    def finish(self, job_id, *, user_id=1, limit=17):
        for _ in range(500):
            result = self.store.management_maintenance(now=NOW, limit=limit, max_pages=32)
            self.assertLessEqual(result['processed_rows'], limit)
            job = self.store.management_job(user_id, job_id)
            if job['status'] == 'completed':
                return job
            self.assertNotEqual(job['status'], 'error', job)
        self.fail('Deletion did not finish in bounded steps: ' + repr(job))

    def test_two_delete_requests_racing_on_one_preview_cannot_create_two_jobs(self):
        batch = self.create()
        self.assertTrue(hasattr(self.store, 'delete_preview'), 'Delete lifecycle is not implemented')
        preview = self.store.delete_preview(1, 'batch', batch['id'], now=NOW)
        barrier = threading.Barrier(2)
        def run(index):
            local = RuleLabStore(self.store.db_path)
            barrier.wait(timeout=5)
            try:
                result = local.request_delete(1, 'batch', batch['id'], dict(
                    request_id='qa-concurrent-delete-'+str(index),
                    expected_revision=preview['expected_revision'], confirm_name=preview['name']), now=NOW)
                return ('accepted', result['job']['id'])
            except RuleLabError as error:
                return (error.code, error.status)
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(run, (1, 2)))
        accepted = [value for state, value in results if state == 'accepted']
        self.assertTrue(accepted, results)
        self.assertEqual(len(set(accepted)), 1, results)
        self.assertTrue(all(state == 'accepted' or code == 409 for state, code in results), results)
        self.finish(accepted[0])
        self.assertEqual(self.store.capacity()['saved'], 0)
        with sqlite3.connect(self.store.db_path) as connection:
            self.assertEqual(connection.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_deleted_study_and_children_ids_are_not_recycled_after_reinitialize(self):
        parent = self.create(risk=policy())
        first = self.store.create_study(1, parent['id'], 'first group', 'qa-first-group', now=NOW)['study']
        first_child_ids = [child['batch_id'] for child in first['children']]
        first_experiment_ids = [item['id'] for child in first_child_ids for item in self.store.list_experiments(1, child)]
        deletion, payload = self.delete('study', first['id'], 'qa-delete-first-group')
        self.finish(deletion['job']['id'])
        self.store = RuleLabStore(self.store.db_path)
        self.store.initialize()
        second = self.store.create_study(1, parent['id'], 'second group', 'qa-second-group', now=NOW)['study']
        self.assertGreater(second['id'], first['id'])
        self.assertGreater(min(child['batch_id'] for child in second['children']), max(first_child_ids))
        second_experiment_ids = [item['id'] for child in second['children'] for item in self.store.list_experiments(1, child['batch_id'])]
        self.assertGreater(min(second_experiment_ids), max(first_experiment_ids))
        # A valid same-owner receipt remains usable after physical removal;
        # another administrator cannot use the retained receipt to discover it.
        again = self.store.request_delete(1, 'study', first['id'], payload, now=NOW)
        self.assertEqual(again['job']['id'], deletion['job']['id'])
        with self.assertRaises(RuleLabError) as denied:
            self.store.request_delete(2, 'study', first['id'], payload, now=NOW)
        self.assertEqual(denied.exception.status, 404)
        self.assertEqual(self.store.get_batch(1, parent['id'])['name'], 'independent')

    def test_shared_archive_survives_last_series_delete_for_another_owners_draft(self):
        old = completed_baseline(self.store)
        preview = self.store.continuation_preview(1, 'batch', old['id'], now=NOW)
        self.store.set_continuation(1, 'batch', old['id'], dict(request_id='qa-shared-series',
            expected_revision=preview['expected_revision'], capture_enabled=True, compute_enabled=True,
            daily_at='00:10', timezone='Asia/Shanghai'), now=NOW)
        other = self.create(owner=2, name='other owner draft')
        with sqlite3.connect(self.store.db_path) as connection:
            archive_before = connection.execute('SELECT count(*) FROM lab_input_versions').fetchone()[0]
            self.assertGreater(archive_before, 0)
        deletion, _ = self.delete('batch', old['id'], 'qa-delete-shared-series')
        self.finish(deletion['job']['id'])
        with sqlite3.connect(self.store.db_path) as connection:
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_input_versions').fetchone()[0], archive_before)
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_predictions').fetchone()[0], 100)
        self.assertEqual(self.store.get_batch(2, other['id'])['status'], 'paused')
        deletion, _ = self.delete('batch', other['id'], 'qa-delete-last-draft', user_id=2)
        self.finish(deletion['job']['id'], user_id=2)
        with sqlite3.connect(self.store.db_path) as connection:
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_input_versions').fetchone()[0], 0)
            self.assertEqual(connection.execute('SELECT count(*) FROM lab_predictions').fetchone()[0], 0)
            self.assertEqual(connection.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_series_created_during_paged_delete_cannot_reuse_deleted_series_identity(self):
        old = completed_baseline(self.store)
        survivor = completed_baseline(self.store)
        def enable(batch_id, request_id):
            preview = self.store.continuation_preview(1, 'batch', batch_id, now=NOW)
            return self.store.set_continuation(1, 'batch', batch_id, dict(request_id=request_id,
                expected_revision=preview['expected_revision'], capture_enabled=True, compute_enabled=False,
                daily_at='00:10', timezone='Asia/Shanghai'), now=NOW)
        enable(old['id'], 'qa-series-before-delete')
        with sqlite3.connect(self.store.db_path) as connection:
            old_series = connection.execute('SELECT id FROM lab_series WHERE owner_id=?', (old['id'],)).fetchone()[0]
        deletion, _ = self.delete('batch', old['id'], 'qa-series-reuse-delete')
        for _ in range(100):
            self.store.management_maintenance(now=NOW, limit=1, max_pages=1)
            with sqlite3.connect(self.store.db_path) as connection:
                series_exists = connection.execute('SELECT 1 FROM lab_series WHERE id=?', (old_series,)).fetchone()
                old_parent_exists = connection.execute('SELECT 1 FROM lab_batches WHERE id=?', (old['id'],)).fetchone()
            if not series_exists:
                break
        self.assertIsNone(series_exists, 'The old series must be physically removed before interleaving')
        self.assertIsNotNone(old_parent_exists, 'The deletion job must still have owned records to clean')
        enable(survivor['id'], 'qa-series-created-during-delete')
        with sqlite3.connect(self.store.db_path) as connection:
            survivor_series = connection.execute('SELECT id FROM lab_series WHERE owner_id=?', (survivor['id'],)).fetchone()[0]
        self.assertGreater(survivor_series, old_series, 'An in-flight job still retains the old series identity')
        self.finish(deletion['job']['id'], limit=3)
        after = self.store.continuation_preview(1, 'batch', survivor['id'], now=NOW)
        self.assertTrue(after['continuation']['enabled'])
        self.assertTrue(after['continuation']['capture_enabled'])
        self.assertEqual(len(self.store.list_segments(1, 'batch', survivor['id'])['segments']), 1)
        with sqlite3.connect(self.store.db_path) as connection:
            self.assertEqual(connection.execute('PRAGMA foreign_key_check').fetchall(), [])

    def test_cleanup_can_make_progress_when_existing_database_exceeds_budget(self):
        batch = self.create()
        self.assertTrue(hasattr(self.store, 'delete_preview'), 'Delete lifecycle is not implemented')
        self.store.budget_bytes = 1024
        deletion, _ = self.delete('batch', batch['id'], 'qa-overbudget-delete')
        self.finish(deletion['job']['id'], limit=7)
        self.assertEqual(self.store.capacity()['saved'], 0)
        with self.assertRaises(RuleLabError) as missing:
            self.store.get_batch(1, batch['id'])
        self.assertEqual(missing.exception.status, 404)

    def test_queue_cannot_start_a_definition_edited_after_its_read_only_eligibility_check(self):
        batch = self.create(name='approved original')
        self.assertTrue(hasattr(self.store, 'queue_eligibility'), 'Queue edit fencing is not implemented')
        original_preview = self.store.continuation_preview(1, 'batch', batch['id'], now=NOW)
        manifest = dict(user_id=1, request_prefix='qa-edit-queue', daily_at='00:10', batches=[dict(
            batch_id=batch['id'], configuration_hash=original_preview['configuration_hash'])])
        original_eligibility = self.store.queue_eligibility
        edited = False
        def interleaved_eligibility(user_id, batch_id, configuration_hash):
            nonlocal edited
            result = original_eligibility(user_id, batch_id, configuration_hash)
            if not edited:
                edited = True
                view = self.store.management_preview(1, 'batch', batch_id, now=NOW)
                config = dict(view['config'], name='user edited', stake_amount=2)
                normalized = normalize_batch_payload(config, now=NOW)
                self.store.edit_batch(1, batch_id, dict(request_id='qa-edit-between-check-and-start',
                    expected_revision=view['expected_revision'], config=normalized), predictor_name='fixture',
                    experiments=build_experiments('big_small'), now=NOW)
            return result
        self.store.queue_eligibility = interleaved_eligibility
        queue.queue_step(self.service, manifest, now=NOW)
        self.assertTrue(edited, 'The queue must consult the current eligibility before starting')
        current = self.store.get_batch(1, batch['id'])
        self.assertEqual(current['status'], 'paused')
        self.assertIsNone(current['started_at'])
        self.assertEqual(current['stake_amount'], 2)
        self.assertTrue(original_eligibility(1, batch['id'], original_preview['configuration_hash'])['removed'])

    def test_finite_queue_finishes_without_creating_a_daily_schedule(self):
        batch=completed_baseline(self.store)
        preview=self.store.continuation_preview(1,'batch',batch['id'],now=NOW)
        manifest=dict(user_id=1,enable_continuation=False,batches=[dict(
            batch_id=batch['id'],configuration_hash=preview['configuration_hash'])])
        with patch.object(self.service,'set_continuation',side_effect=AssertionError('must not schedule')):
            result=queue.queue_step(self.service,manifest,now=NOW)
            self.assertTrue(result['complete'])
            self.assertEqual(result['batches'][0]['state'],'completed')
            self.assertTrue(queue.queue_step(self.service,manifest,now=NOW)['complete'])
        with sqlite3.connect(self.store.db_path) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_series').fetchone()[0],0)


class StorageCliIndependentQaTests(unittest.TestCase):
    def test_unknown_writer_state_cannot_start_offline_conversion(self):
        # A failed systemd query is not evidence that every writer stopped.
        result = subprocess.CompletedProcess(['systemctl'], 1, stdout='', stderr='Failed to connect to bus')
        with patch.object(storage_cli.subprocess, 'run', return_value=result), \
                patch.object(storage_cli, 'prepare_storage', return_value={}) as convert, \
                patch('sys.argv', ['prepare_rule_lab_storage.py', '--database', '/unused/lab.db',
                                   '--backup', '/unused/backup.db', '--apply']), redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit) as stopped:
                storage_cli.main()
            self.assertEqual(stopped.exception.code, 2)
            convert.assert_not_called()

    def test_interrupted_vacuum_keeps_original_rows_and_complete_external_backup(self):
        from pc28touzhu.services import rule_lab_storage as storage

        class InterruptVacuumConnection(sqlite3.Connection):
            def execute(self, sql, *args, **kwargs):
                if sql == 'VACUUM':
                    self.set_progress_handler(lambda: 1, 100)
                    try:
                        return super().execute(sql, *args, **kwargs)
                    finally:
                        self.set_progress_handler(None, 0)
                return super().execute(sql, *args, **kwargs)

        with tempfile.TemporaryDirectory() as temporary:
            target = Path(temporary) / 'lab' / 'lab.db'
            target.parent.mkdir()
            backup = Path(temporary) / 'external-backup.db'
            expected = [(index, 'preserve-' + str(index)) for index in range(2000)]
            with sqlite3.connect(target) as connection:
                connection.execute('PRAGMA journal_mode=WAL')
                connection.execute('CREATE TABLE original_values(id INTEGER PRIMARY KEY,value TEXT)')
                connection.executemany('INSERT INTO original_values VALUES(?,?)', expected)
            connect = sqlite3.connect

            def interrupt_only_source(path, *args, **kwargs):
                if path == str(target):
                    kwargs['factory'] = InterruptVacuumConnection
                return connect(path, *args, **kwargs)

            with patch.object(storage.sqlite3, 'connect', side_effect=interrupt_only_source):
                with self.assertRaisesRegex(sqlite3.OperationalError, 'interrupted'):
                    storage.prepare_storage(target, apply=True, backup_path=backup)
            self.assertTrue(backup.exists())
            for path in (target, backup):
                with sqlite3.connect(path) as connection:
                    self.assertEqual(connection.execute('SELECT * FROM original_values ORDER BY id').fetchall(), expected)
                    self.assertEqual(connection.execute('PRAGMA auto_vacuum').fetchone()[0], 0)
                    self.assertEqual(connection.execute('PRAGMA quick_check').fetchone()[0], 'ok')
                    self.assertEqual(connection.execute('PRAGMA foreign_key_check').fetchall(), [])


if __name__ == '__main__':
    unittest.main()
