"""Continuous rule lab browser fixture; all writes stay in temporary SQLite files."""
from __future__ import annotations

import json
import os
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from wsgiref.simple_server import WSGIRequestHandler, make_server

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.test_rule_lab_resources import FakeResources
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_source import SourceFixture, stamp

UTC = timezone.utc
BEIJING = timezone(timedelta(hours=8))


class QuietHandler(WSGIRequestHandler):
    def log_message(self, *args):
        pass


def main():
    with tempfile.TemporaryDirectory(prefix="pc28-continuation-ui-") as directory:
        root = Path(directory)
        os.environ["RULE_LAB_DATA_DIR"] = str(root / "lab")
        os.environ["RULE_LAB_SOURCE_DB"] = str(root / "source.db")
        fixture = SourceFixture(root / "source.db")
        with sqlite3.connect(fixture.path) as c:
            c.execute("CREATE TABLE pc28_prediction_daily_summary (predictor_id INTEGER, summary_date TEXT, total_predictions INTEGER)")
        source = RuleLabSource(fixture.path)
        store = RuleLabStore(root / "lab" / "rule_lab.db")
        store.initialize()
        now = datetime.now(UTC).replace(microsecond=0)
        today = now.astimezone(BEIJING).date()
        baseline_day = today - timedelta(days=1)
        opened = datetime.combine(baseline_day, datetime.min.time(), tzinfo=BEIJING).astimezone(UTC) + timedelta(seconds=150)
        for offset in range(-100, 4):
            at = opened + timedelta(seconds=210 * offset)
            fixture.draw(1100 + offset, at)
            fixture.prediction(1100 + offset, at)
        repo = DatabaseRepository(str(root / "platform.db"))
        repo.initialize_database()
        admin = repo.create_user_record(username="continuation-ui-admin", role="admin")
        secret = "continuation-ui-local-only"
        risk = policy()
        risk['round']['max_bets'] = 4
        risk['day']['enabled'] = False
        config = normalize_batch_payload(dict(
            name="连续观察 · 原26个版本", mode="backtest", predictor_id=1,
            metric="combo", template_pack="combo_long_miss_v1", stake_amount=1,
            start_date=baseline_day.isoformat(), end_date=baseline_day.isoformat(), risk_policy=risk,
        ), now=stamp(now))
        batch = store.create_batch(admin['id'], config, "公开来源", build_experiments(
            'combo', template_pack='combo_long_miss_v1'), now=stamp(now))
        store.set_batch_state(admin['id'], batch['id'], 'running', now=stamp(now))
        for _ in range(100):
            RuleLabWorker(store, source).tick(now=stamp(now))
            if store.get_batch(admin['id'], batch['id'])['status'] == 'completed':
                break
        assert store.get_batch(admin['id'], batch['id'])['status'] == 'completed', 'Baseline fixture must complete'
        assert not store.get_pending_decisions(batch['id']), 'Baseline fixture must have no pending'
        original_ids = [item['id'] for item in store.list_experiments(admin['id'], batch['id'])]
        service = RuleLabService(store, source, resources=FakeResources())
        app = PlatformApiApplication(repo, "offline-only", secret, rule_lab_service=service)
        appended = False

        def reply(start_response, payload, status="200 OK"):
            body = json.dumps(payload, ensure_ascii=False).encode()
            start_response(status, [('Content-Type', 'application/json'), ('Content-Length', str(len(body)))])
            return [body]

        def local_app(environ, start_response):
            nonlocal appended
            path = environ.get('PATH_INFO', '')
            method = environ.get('REQUEST_METHOD', 'GET')
            if path == '/__fixture/append-source' and method == 'POST':
                if not appended:
                    first = datetime.combine(today, datetime.min.time(), tzinfo=BEIJING).astimezone(UTC) + timedelta(seconds=150)
                    for offset in range(8):
                        at = first + timedelta(seconds=210 * offset)
                        fixture.draw(1104 + offset, at, balls=(1, 1, 3) if offset % 3 == 0 else (3, 6, 8))
                        fixture.prediction(1104 + offset, at)
                    appended = True
                return reply(start_response, {'ok': True, 'last_issue': '1111'})
            if path == '/__fixture/tick' and method == 'POST':
                length = int(environ.get('CONTENT_LENGTH') or 0)
                params = json.loads(environ['wsgi.input'].read(length)) if length else {}
                result = RuleLabWorker(store, source, max_tick_seconds=params.get('budget', 2),
                                       page_size=params.get('page_size', 200)).tick()
                return reply(start_response, result)
            if path == '/__fixture/create-study' and method == 'POST':
                result = store.create_study(admin['id'], batch['id'], '续算比较组 · 本地验证',
                                            'continuation-ui-study-001', now=stamp(datetime.now(UTC)))
                study_id = result['study']['id']
                store.set_study_state(admin['id'], study_id, 'running', now=stamp(datetime.now(UTC)))
                for _ in range(60):
                    RuleLabWorker(store, source).tick()
                    study = store.get_study(admin['id'], study_id)
                    if study['status'] == 'completed':
                        break
                assert study['status'] == 'completed', study
                return reply(start_response, {'study_id': study_id, 'child_id': study['children'][0]['batch_id']})
            if path == '/__fixture/monthly-revisions' and method == 'POST':
                # Persisted report fixtures exercise the real read API and version view;
                # engine-generated monthly revision correctness is covered by backend tests.
                experiment_id = original_ids[0]
                report = store.get_experiment_detail(admin['id'], batch['id'], experiment_id)['monthly']
                month = (today.replace(day=1) - timedelta(days=1)).strftime('%Y-%m')
                with store._connect(write=True) as c:
                    series_id = c.execute("SELECT id FROM lab_series WHERE owner_kind='batch' AND owner_id=?", (batch['id'],)).fetchone()[0]
                    for revision, profit in ((1, 12.34), (2, 13.34)):
                        row = dict(report['rows'][0], month=month, net_profit=profit, status='complete',
                                   time_complete=True, complete_month=True, pending_count=0)
                        c.execute('''INSERT OR IGNORE INTO lab_monthly_snapshots
                            (series_id,batch_id,experiment_id,month,revision,payload_json,reason,created_at)
                            VALUES(?,?,?,?,?,?,?,?)''', (series_id,batch['id'],experiment_id,month,revision,
                            json.dumps(row), 'period_closed' if revision == 1 else 'late_settlement', stamp(datetime.now(UTC))))
                return reply(start_response, {'month': month})
            if path == '/__fixture/state' and method == 'GET':
                with store._connect() as c:
                    tables = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")]
                    counts = {name: c.execute('SELECT COUNT(*) FROM ' + name).fetchone()[0]
                              for name in tables if name in ('lab_series', 'lab_segments', 'lab_continuation_requests')}
                return reply(start_response, {
                    'batch': store.get_batch(admin['id'], batch['id']),
                    'experiments': store.list_experiments(admin['id'], batch['id']),
                    'original_ids': original_ids, 'counts': counts,
                    'pending_count': len(store.get_pending_decisions(batch['id'])),
                })
            if method != 'GET' and not path.startswith('/api/platform/rule-lab/'):
                return reply(start_response, {'error': 'fixture permits rule lab writes only'}, '403 Forbidden')
            return app(environ, start_response)

        with make_server('127.0.0.1', 0, local_app, handler_class=QuietHandler) as server:
            print(json.dumps({
                'url': 'http://127.0.0.1:%s' % server.server_port,
                'cookie_name': SESSION_COOKIE_NAME,
                'cookie': build_session_cookie_value(admin['id'], secret),
                'batch_id': batch['id'], 'baseline_day': baseline_day.isoformat(), 'today': today.isoformat(),
            }), flush=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            sys.stdin.read()
            server.shutdown()


if __name__ == '__main__':
    main()
