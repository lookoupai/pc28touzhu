"""Read-only UI fixture with real archived runs; no config, worker or messages."""
from __future__ import annotations

import hashlib
import json
import sqlite3
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from wsgiref.simple_server import WSGIRequestHandler, make_server

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT), str(ROOT / 'src')]

from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from tests.test_cycle_recovery import RecoveryFixture


class QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        pass


def fingerprint(db):
    digest = hashlib.sha256()
    with sqlite3.connect(db) as conn:
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
        for (name,) in tables:
            rows = conn.execute('SELECT * FROM "' + name.replace('"', '""') + '" ORDER BY rowid').fetchall()
            digest.update(json.dumps([name, rows], ensure_ascii=False, sort_keys=True).encode())
    return digest.hexdigest()


def main():
    fixture = RecoveryFixture()
    fixture.setUp()
    try:
        fixture.now = datetime.now(timezone.utc) - timedelta(days=1)
        waiting = fixture.start(limit=1000)
        for index in range(21):
            if index:
                fixture.advance()
                fixture.signal()
                fixture.assertEqual(fixture.cycle(waiting)['summary']['triggered_count'], 1)
            fixture.unknown(waiting)
            fixture.expire()
            fixture.assertEqual(fixture.detach()['detached_count'], 1)
        fixture.now = datetime.now(timezone.utc)
        fixture.advance()
        running = fixture.start(targets=[fixture.targets[2]], limit=4)
        fixture.unknown(running)
        fixture.expire()
        fixture.assertEqual(fixture.detach()['detached_count'], 1)
        fixture.advance()
        fixture.signal()
        fixture.assertEqual(fixture.cycle(running)['summary']['triggered_count'], 1)
        with fixture.repo._connect() as conn:
            conn.execute('UPDATE auto_trigger_rules SET name=? WHERE id=?', ('待命规则 · 保留旧账', waiting['id']))
            conn.execute('UPDATE auto_trigger_rules SET name=? WHERE id=?', ('运行规则 · 旧账独立', running['id']))
        uid = fixture.user_id
        app = PlatformApiApplication(repository=fixture.repo, executor_api_token='ui-fixture-only', session_secret='ui-fixture-only')
        expected = fingerprint(fixture.repo.db_path)

        def local_app(environ, start_response):
            path = environ.get('PATH_INFO', '')
            allowed = (path.startswith('/assets/') or path in {
                '/auto-triggers', '/api/auth/me', '/api/health', '/api/platform/sources',
                '/api/platform/subscriptions/summary', '/api/platform/delivery-targets',
                '/api/platform/message-templates', '/api/platform/auto-trigger-rules',
                '/api/platform/auto-trigger-events',
            } or (path.startswith('/api/platform/auto-trigger-rules/') and path.endswith('/cycle-recoveries')))
            if not allowed or environ.get('REQUEST_METHOD') != 'GET':
                start_response('403 Forbidden', [('Content-Type', 'application/json')])
                return [b'{"error":"fixture permits read-only page requests"}']
            return app(environ, start_response)

        with make_server('127.0.0.1', 0, local_app, handler_class=QuietHandler) as server:
            print(json.dumps({
                'url': 'http://127.0.0.1:%s' % server.server_port,
                'cookie': '%s=%s' % (SESSION_COOKIE_NAME, build_session_cookie_value(uid, 'ui-fixture-only')),
                'subscription_id': fixture.sub['id'], 'waiting_rule_id': waiting['id'],
                'running_rule_id': running['id'], 'history_count': 21,
            }), flush=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            sys.stdin.read()
            server.shutdown()
            thread.join(timeout=5)
        if fingerprint(fixture.repo.db_path) != expected:
            raise AssertionError('read-only UI requests changed the fixture database')
    finally:
        fixture.doCleanups()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
