"""Deployment policy and real launcher checks; all runtime data is temporary."""
from __future__ import annotations

import configparser
import fcntl
import json
import os
from pathlib import Path
import shlex
import sqlite3
import subprocess
import sys
import tempfile
import unittest

from pc28touzhu.services.rule_lab_store import RuleLabStore


ROOT = Path(__file__).resolve().parents[1]
UNIT = ROOT / 'deploy/systemd/pc28touzhu-rule-lab.service'
PRODUCTION = '/www/wwwroot/pc28touzhu'


class RuleLabOpsTests(unittest.TestCase):
    def unit(self):
        config = configparser.ConfigParser(interpolation=None)
        self.assertTrue(config.read(UNIT), 'The bounded worker unit must exist')
        return config

    def command(self):
        command = shlex.split(self.unit()['Service']['ExecStart'])
        return [part.replace(PRODUCTION + '/', str(ROOT) + '/', 1) for part in command] + ['--once']

    def environment(self, directory):
        environment = os.environ.copy()
        environment.update(RULE_LAB_DATA_DIR=str(directory / 'lab'),
                           RULE_LAB_SOURCE_DB=str(directory / 'absent-source.db'),
                           PC28_PYTHON_BIN=sys.executable,
                           PYTHONDONTWRITEBYTECODE='1')
        environment.pop('PYTHONPATH', None)
        return environment

    def test_worker_has_hard_resource_and_restart_bounds(self):
        unit = self.unit()
        service = unit['Service']
        self.assertEqual(service['CPUQuota'], '20%')
        self.assertEqual(service['MemoryHigh'], '192M')
        self.assertEqual(service['MemoryMax'], '256M')
        self.assertEqual(service['MemorySwapMax'], '0')
        self.assertGreaterEqual(int(service['Nice']), 10)
        self.assertEqual(service['IOSchedulingClass'], 'idle')
        self.assertEqual(service['Restart'], 'on-failure')
        self.assertEqual(service['RestartSec'], '30s')
        self.assertEqual(unit['Unit']['StartLimitBurst'], '3')
        self.assertEqual(unit['Unit']['StartLimitIntervalSec'], '10min')
        self.assertEqual(service['LogRateLimitIntervalSec'], '1min')
        self.assertLessEqual(int(service['LogRateLimitBurst']), 20)

    def test_worker_cannot_write_production_or_use_network_credentials(self):
        service = self.unit()['Service']
        self.assertEqual(service['User'], 'www')
        self.assertEqual(service['Group'], 'www')
        self.assertEqual(service['ProtectSystem'], 'strict')
        self.assertEqual(shlex.split(service['ReadWritePaths']),
                         [PRODUCTION + '/data/rule_lab', '/www/wwwroot/AITradingSimulator'])
        self.assertEqual(shlex.split(service['ReadOnlyPaths']),
                         ['/www/wwwroot/AITradingSimulator/pc28_predictor.db', PRODUCTION])
        self.assertIn('-/www/wwwroot/AITradingSimulator/.env', shlex.split(service['InaccessiblePaths']))
        self.assertEqual(service['PrivateNetwork'], 'true')
        self.assertEqual(service['RestrictAddressFamilies'], 'AF_UNIX')
        self.assertEqual(service['NoNewPrivileges'], 'true')
        self.assertEqual(service['ProtectHome'], 'true')
        self.assertNotIn('EnvironmentFile', service)
        self.assertNotIn('ExecStartPre', service)
        environment = dict(item.split('=', 1) for item in shlex.split(service['Environment']))
        self.assertEqual(set(environment), {'RULE_LAB_DATA_DIR', 'RULE_LAB_SOURCE_DB', 'SQLITE_TMPDIR', 'PYTHONDONTWRITEBYTECODE'})
        self.assertEqual(environment['RULE_LAB_DATA_DIR'], PRODUCTION + '/data/rule_lab')
        self.assertEqual(environment['RULE_LAB_SOURCE_DB'], '/www/wwwroot/AITradingSimulator/pc28_predictor.db')

    def test_unit_launcher_initializes_only_empty_temporary_lab(self):
        with tempfile.TemporaryDirectory(prefix='rule-lab-ops-') as folder:
            directory = Path(folder)
            result = subprocess.run(self.command(), cwd=ROOT, env=self.environment(directory),
                                    capture_output=True, text=True, timeout=15)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((directory / 'absent-source.db').exists())
            with sqlite3.connect(directory / 'lab/rule_lab.db') as connection:
                self.assertEqual(connection.execute('SELECT COUNT(*) FROM lab_batches').fetchone()[0], 0)
                self.assertEqual(connection.execute('SELECT COUNT(*) FROM lab_decisions').fetchone()[0], 0)
                heartbeat = json.loads(connection.execute("SELECT value_json FROM lab_runtime WHERE key='worker'").fetchone()[0])
                self.assertEqual(heartbeat['state'], 'idle')
            self.assertIn('idle', result.stdout)

    def test_sqlite_can_spill_statement_journals_inside_worker_write_boundary(self):
        service=self.unit()['Service']
        environment=dict(item.split('=',1) for item in shlex.split(service['Environment']))
        self.assertEqual(environment.get('SQLITE_TMPDIR'),environment['RULE_LAB_DATA_DIR'],
                         'Large deletes need SQLite temporary journals in the bounded writable lab directory')
        self.assertIn(environment['SQLITE_TMPDIR'],shlex.split(service['ReadWritePaths']))

    def test_second_worker_exits_without_advancing_the_ledger(self):
        with tempfile.TemporaryDirectory(prefix='rule-lab-ops-lock-') as folder:
            directory = Path(folder)
            lab = directory / 'lab'
            store = RuleLabStore(lab / 'rule_lab.db')
            store.initialize()
            def snapshot():
                with sqlite3.connect(lab / 'rule_lab.db') as connection:
                    tables=[row[0] for row in connection.execute(
                        "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'lab_%' ORDER BY name")]
                    return {table:connection.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall()
                            for table in tables}
            before=snapshot()
            retention=json.loads(dict(before['lab_runtime'])['result_retention'])
            self.assertEqual(retention['mode'],'results_only')
            self.assertIsNone(retention['updated_at'])
            self.assertNotIn('worker',dict(before['lab_runtime']))
            with (lab / 'worker.lock').open('a') as lock:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
                result = subprocess.run(self.command(), cwd=ROOT, env=self.environment(directory),
                                        capture_output=True, text=True, timeout=15)
            self.assertNotEqual(result.returncode, 0)
            self.assertIn('模拟进程已经运行', result.stderr)
            # Reinitialization may find its fixed policy metadata, but a rejected
            # worker cannot advance maintenance, results, cursors or heartbeat.
            self.assertEqual(snapshot(),before)
            self.assertNotIn('worker',dict(snapshot()['lab_runtime']))
            self.assertFalse((directory / 'absent-source.db').exists())


if __name__ == '__main__':
    unittest.main()
