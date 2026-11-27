"""Offline result migration validates writer state and preserves economic data."""
import contextlib
import importlib.util
import io
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests import test_rule_lab_retention as fixture_module
NOW=fixture_module.NOW


def cli():
    path=Path(__file__).resolve().parents[1]/'scripts/maintenance/migrate_rule_lab_results.py'
    if not path.exists():return None
    spec=importlib.util.spec_from_file_location('results_migration_cli',path)
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module


class ResultsMigrationTests(unittest.TestCase):
    def fixture(self):
        value=fixture_module.ResultRetentionTests();value.setUp();self.addCleanup(value.tearDown)
        for issue in range(1,42):value.legacy_decision(issue)
        value.legacy_decision(42,'pending')
        return value

    def module(self):
        result=cli();self.assertIsNotNone(result,'offline guarded migration is not implemented');return result

    def test_preview_is_read_only_and_apply_keeps_statistics_and_pending(self):
        fixture=self.fixture();module=self.module();path=fixture.store.db_path
        backup=Path(fixture.tmp.name).parent/(Path(fixture.tmp.name).name+'-results-backup.db')
        self.addCleanup(lambda:backup.unlink(missing_ok=True))
        with sqlite3.connect(path) as c:
            before=c.execute('SELECT * FROM lab_experiments ORDER BY id').fetchall()
            pending=c.execute("SELECT * FROM lab_decisions WHERE status='pending'").fetchall()
        preview=module.migrate(path)
        self.assertFalse(preview['applied']);self.assertFalse(backup.exists())
        result=module.migrate(path,apply=True,backup_path=backup,drain=True,max_steps=100)
        self.assertTrue(result['protected_unchanged']);self.assertTrue(backup.exists())
        with sqlite3.connect(path) as c:
            self.assertEqual(c.execute('SELECT * FROM lab_experiments ORDER BY id').fetchall(),before)
            self.assertEqual(c.execute("SELECT * FROM lab_decisions WHERE status='pending'").fetchall(),pending)
            self.assertEqual(c.execute("SELECT count(*) FROM lab_decisions WHERE status!='pending'").fetchone()[0],0)
        self.assertLessEqual(result['max_step_rows'],500)

    def test_unknown_writer_state_refuses_apply_before_backup(self):
        fixture=self.fixture();module=self.module()
        class Unknown:returncode=1;stdout='';stderr='bus unavailable'
        with patch.object(module.subprocess,'run',return_value=Unknown()),patch('sys.argv',[
            'migrate','--database',str(fixture.store.db_path),'--backup','/tmp/must-not-be-created-result.db','--apply']):
            with contextlib.redirect_stderr(io.StringIO()),self.assertRaises(SystemExit) as error:module.main()
        self.assertEqual(error.exception.code,2)

    def test_bounded_drain_can_resume_without_a_second_backup(self):
        fixture=self.fixture();module=self.module();path=fixture.store.db_path
        backup=Path(fixture.tmp.name).parent/(Path(fixture.tmp.name).name+'-results-backup.db')
        self.addCleanup(lambda:backup.unlink(missing_ok=True))
        first=module.migrate(path,apply=True,backup_path=backup,drain=True,max_steps=1)
        self.assertTrue(first['protected_unchanged'])
        with self.assertRaises(Exception):module.migrate(path,apply=True,backup_path=backup)
