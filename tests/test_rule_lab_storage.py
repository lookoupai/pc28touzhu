"""Physical SQLite space behavior, including old NONE databases and reader locks."""
import importlib.util
import io
import os
import sqlite3
import subprocess
import tempfile
import unittest
from contextlib import redirect_stderr,redirect_stdout
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.services.rule_lab_store import RuleLabStore,RuleLabError

NOW='2026-11-27T04:00:00Z'


class StorageTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'lab'/'lab.db';self.store=RuleLabStore(self.path);self.store.initialize()

    def fill_and_delete(self):
        with sqlite3.connect(self.path) as c:
            c.execute('CREATE TABLE space_fixture(id INTEGER PRIMARY KEY,data BLOB)')
            c.executemany('INSERT INTO space_fixture(data) VALUES(?)',[(os.urandom(4096),) for _ in range(700)])
            c.commit();c.execute('DELETE FROM space_fixture WHERE id<=650');c.commit()
        return self.store.storage_status()

    def test_new_database_supports_bounded_physical_reclaim(self):
        before=self.fill_and_delete()
        self.assertIn('reusable_bytes',before,'Space accounting is not implemented')
        self.assertEqual(before['auto_vacuum'],2)
        self.assertGreater(before['reusable_bytes'],1024*1024)
        original=self.path.stat().st_size
        request={'request_id':'physical-reclaim-test'}
        job=self.store.request_reclaim(1,request,now=NOW)['job']
        for _ in range(100):
            self.store.management_maintenance(now=NOW,limit=100,max_pages=32)
            job=self.store.management_job(1,job['id'])
            if job['status']=='completed':break
        self.assertEqual(job['status'],'completed',job)
        self.assertGreater(job['freed_bytes'],0)
        self.assertLess(self.path.stat().st_size,original)
        self.assertEqual(self.store.request_reclaim(1,request,now=NOW)['job']['id'],job['id'])
        with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('SELECT COUNT(*) FROM space_fixture').fetchone()[0],50)

    def test_busy_reader_waits_without_reporting_success(self):
        self.assertIn('incremental_ready',self.store.storage_status())
        reader=sqlite3.connect(self.path);self.addCleanup(reader.close)
        reader.execute('BEGIN');reader.execute('SELECT COUNT(*) FROM lab_batches').fetchone()
        self.fill_and_delete()
        job=self.store.request_reclaim(1,{'request_id':'reclaim-busy-reader'},now=NOW)['job']
        self.store.management_maintenance(now=NOW,limit=100,max_pages=16)
        result=self.store.management_job(1,job['id'])
        self.assertEqual(result['status'],'waiting')
        self.assertEqual(result['freed_bytes'],0)
        reader.rollback()
        for _ in range(100):
            self.store.management_maintenance(now=NOW,limit=100,max_pages=64)
            if self.store.management_job(1,job['id'])['status']=='completed':break
        self.assertEqual(self.store.management_job(1,job['id'])['status'],'completed')

    def test_existing_none_database_requires_explicit_conversion_and_preserves_data(self):
        from pc28touzhu.services import rule_lab_storage as storage
        legacy=Path(self.tmp.name)/'legacy'/'old.db';legacy.parent.mkdir()
        with sqlite3.connect(legacy) as c:
            c.execute('CREATE TABLE values_to_keep(id INTEGER PRIMARY KEY,value TEXT)')
            c.executemany('INSERT INTO values_to_keep(value) VALUES(?)',[(str(i),) for i in range(30)])
            c.execute('PRAGMA journal_mode=WAL')
        original=legacy.read_bytes()
        preview=storage.prepare_storage(legacy,apply=False)
        self.assertEqual(preview['auto_vacuum'],0);self.assertEqual(legacy.read_bytes(),original)
        with patch.object(storage.shutil,'disk_usage',return_value=type('Disk',(),{'free':1})()):
            with self.assertRaises(RuleLabError):storage.prepare_storage(legacy,apply=True,backup_path=Path(self.tmp.name)/'no-space.db')
        result=storage.prepare_storage(legacy,apply=True,backup_path=Path(self.tmp.name)/'backup.db')
        self.assertEqual(result['auto_vacuum'],2);self.assertTrue(result['old_rows_unchanged'])
        with sqlite3.connect(legacy) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM values_to_keep').fetchone()[0],30)
            self.assertEqual(c.execute('PRAGMA quick_check').fetchone()[0],'ok')


class StorageCliTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        path=Path(__file__).resolve().parents[1]/'scripts/maintenance/prepare_rule_lab_storage.py'
        spec=importlib.util.spec_from_file_location('storage_cli_guard_tests',path)
        cls.cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.cli)

    def invoke(self,apply=True):
        args=['prepare_rule_lab_storage.py','--database','/unused/lab.db']
        if apply:args+=['--apply','--backup','/unused/backup.db']
        with patch('sys.argv',args),redirect_stderr(io.StringIO()),redirect_stdout(io.StringIO()):
            return self.cli.main()

    def test_apply_rejects_every_unconfirmed_writer_state(self):
        for state,code in [('active',0),('activating',0),('reloading',0),('deactivating',3),
                           ('unknown',3),('',1),('inactive',0),('inactive',1),('failed',1)]:
            with self.subTest(state=state,returncode=code), \
                    patch.object(self.cli.subprocess,'run',return_value=subprocess.CompletedProcess([],code,stdout=state)), \
                    patch.object(self.cli,'prepare_storage') as convert:
                with self.assertRaises(SystemExit) as error:self.invoke()
                self.assertEqual(error.exception.code,2);convert.assert_not_called()
        for error in (OSError('systemctl absent'),subprocess.TimeoutExpired('systemctl',10)):
            with self.subTest(error=type(error).__name__),patch.object(self.cli.subprocess,'run',side_effect=error), \
                    patch.object(self.cli,'prepare_storage') as convert:
                with self.assertRaises(SystemExit) as stopped:self.invoke()
                self.assertEqual(stopped.exception.code,2);convert.assert_not_called()

    def test_apply_checks_all_three_stopped_writers_and_preview_skips_checks(self):
        for state in ('inactive','failed'):
            with self.subTest(state=state), \
                    patch.object(self.cli.subprocess,'run',return_value=subprocess.CompletedProcess([],3,stdout=state)) as query, \
                    patch.object(self.cli,'prepare_storage',return_value={}) as convert:
                self.invoke();self.assertEqual(query.call_count,3);convert.assert_called_once()
        with patch.object(self.cli.subprocess,'run') as query, \
                patch.object(self.cli,'prepare_storage',return_value={}) as convert:
            self.invoke(apply=False);query.assert_not_called();convert.assert_called_once()
