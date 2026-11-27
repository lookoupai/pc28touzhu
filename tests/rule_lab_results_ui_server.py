"""Disposable real WSGI/source/ledger for results-only DOM integration."""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from wsgiref.simple_server import make_server

sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME,build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments,normalize_batch_payload
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.rule_lab_ui_server import QuietHandler
from tests.test_rule_lab_resources import FakeResources
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_source import SourceFixture,stamp


def main():
    with tempfile.TemporaryDirectory(prefix='pc28-results-ui-') as directory:
        root=Path(directory);fixture=SourceFixture(root/'source.db')
        now=datetime.now(timezone.utc).replace(microsecond=0)
        zone=timezone(timedelta(hours=8));day=(now.astimezone(zone)-timedelta(days=1)).date()
        first=(datetime.combine(day,datetime.min.time(),tzinfo=zone)+timedelta(minutes=2)).astimezone(timezone.utc)
        for offset in range(-100,6):
            opened=first+timedelta(seconds=210*offset)
            fixture.draw(1100+offset,opened)
            fixture.prediction(1100+offset,opened,big_small='小' if offset%3==1 else '大')
        boundary=first+timedelta(days=1)
        fixture.draw(1106,boundary);fixture.prediction(1106,boundary)
        fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        store=RuleLabStore(root/'lab'/'rule_lab.db');source=RuleLabSource(fixture.path)
        service=RuleLabService(store,source,resources=FakeResources())
        risk=policy(cooldown_issues=0);risk['round']['max_bets']=2
        risk['additional_day_guards']=[{**risk['day'],'profit_target':40,'loss_limit':40}]
        config=normalize_batch_payload(dict(name='结果观察 · 原方案',mode='backtest',predictor_id=1,metric='big_small',
            start_date=day.isoformat(),end_date=day.isoformat(),risk_policy=risk),now=stamp(now))
        batch=store.create_batch(1,config,'本地模拟来源',build_experiments('big_small')[:2],now=stamp(now))
        store.set_batch_state(1,batch['id'],'running',now=stamp(now))
        worker=RuleLabWorker(store,source)
        for _ in range(40):
            worker.tick(now=stamp(now))
            if store.get_batch(1,batch['id'])['status']=='completed':break
        assert store.get_batch(1,batch['id'])['status']=='completed'
        eid=store.list_experiments(1,batch['id'])[0]['id']
        repo=DatabaseRepository(str(root/'main.db'));repo.initialize_database()
        admin=repo.create_user_record(username='results-admin',role='admin')
        secret='results-dom-local-fixture'
        app=PlatformApiApplication(repo,'offline-only',secret,rule_lab_service=service)
        def local_app(environ,start_response):
            route=environ.get('PATH_INFO');method=environ.get('REQUEST_METHOD')
            if route.startswith('/__fixture/') and method=='POST':
                if route=='/__fixture/tick':
                    for _ in range(60):
                        result=worker.tick(now=stamp(now))
                        if not any(item['status']=='running' for item in store.list_work_batches()):break
                    value=result
                elif route=='/__fixture/remove-source-history':
                    with sqlite3.connect(fixture.path) as c:
                        c.execute('DELETE FROM predictions WHERE issue_no>=? AND issue_no<=?',('1100','1105'))
                        c.execute('INSERT INTO pc28_prediction_daily_summary VALUES(1,?)',(day.isoformat(),))
                    value={'removed':True}
                else:
                    start_response('404 Not Found',[('Content-Type','application/json')]);return [b'{}']
                start_response('200 OK',[('Content-Type','application/json')]);return [json.dumps(value).encode()]
            if method!='GET' and not route.startswith('/api/platform/rule-lab/'):
                start_response('403 Forbidden',[('Content-Type','application/json')]);return [b'{}']
            return app(environ,start_response)
        with make_server('127.0.0.1',0,local_app,handler_class=QuietHandler) as server:
            print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port),cookie_name=SESSION_COOKIE_NAME,
                cookie=build_session_cookie_value(admin['id'],secret),batch_id=batch['id'],experiment_id=eid,
                date=day.isoformat())),flush=True)
            threading.Thread(target=server.serve_forever,daemon=True).start();sys.stdin.read();server.shutdown()


if __name__=='__main__':main()
