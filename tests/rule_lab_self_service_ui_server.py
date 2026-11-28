"""自助实验室界面测试：临时真实 WSGI、SQLite 和预测来源。"""
from __future__ import annotations
import json
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from wsgiref.simple_server import make_server
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.rule_lab_ui_server import QuietHandler
from tests.test_rule_lab_resources import FakeResources
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_source import SourceFixture


def main():
    with tempfile.TemporaryDirectory(prefix='pc28-self-service-ui-') as directory:
        root = Path(directory)
        source = SourceFixture(root / 'source.db')
        source.execute("UPDATE predictors SET name='jnd28 测试来源一' WHERE id=1")
        source.execute("INSERT INTO predictors VALUES(2,'jnd28 测试来源二','pc28',1,1,'records','not-used')")
        opened = datetime(2026, 11, 26, 4, tzinfo=timezone.utc)
        for i in range(-100, 8):
            moment = opened + timedelta(seconds=210*i)
            source.draw(4000+i, moment, balls=(3,6,8) if i%3 else (1,2,5))
            for pid in (1,2):
                source.prediction(4000+i, moment, predictor_id=pid)
        store = RuleLabStore(root/'lab'/'rule_lab.db')
        service = RuleLabService(store, RuleLabSource(source.path), resources=FakeResources())
        risk = policy()
        risk['day']['hit_rate_gate'].update(enabled=True, threshold=25, min_settled_count=20)
        risk['cooldown_issues'] = 10
        config = normalize_batch_payload(dict(name='来源条件基准', mode='backtest', predictor_id=1,
            metric='big_small', start_date='2026-11-26', end_date='2026-11-26', risk_policy=risk), now='2026-11-28T04:00:00Z')
        batch = store.create_batch(1, config, 'jnd28 测试来源一', build_experiments('big_small'), now='2026-11-28T04:00:00Z')
        repo = DatabaseRepository(str(root/'main.db'))
        repo.initialize_database()
        admin = repo.create_user_record(username='self-service-admin', role='admin')
        secret = 'self-service-temporary-fixture'
        app = PlatformApiApplication(repo, 'offline-only', secret, rule_lab_service=service)
        def routed(environ, start_response):
            if environ.get('PATH_INFO') == '/__fixture/state' and environ.get('REQUEST_METHOD') == 'GET':
                with sqlite3.connect(store.db_path) as c:
                    data = {t:c.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in ('lab_batches','lab_experiments','lab_studies','lab_series')}
                start_response('200 OK', [('Content-Type','application/json')])
                return [json.dumps(data).encode()]
            return app(environ, start_response)
        with make_server('127.0.0.1', 0, routed, handler_class=QuietHandler) as server:
            print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port),cookie_name=SESSION_COOKIE_NAME,
                cookie=build_session_cookie_value(admin['id'],secret),batch_id=batch['id'])),flush=True)
            threading.Thread(target=server.serve_forever,daemon=True).start()
            sys.stdin.read()
            server.shutdown()

if __name__=='__main__':main()
