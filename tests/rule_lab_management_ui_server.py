"""Disposable management UI server. The only mutable state is under TemporaryDirectory."""
from __future__ import annotations
import json
import os
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime,timedelta,timezone
from pathlib import Path
from wsgiref.simple_server import make_server,WSGIRequestHandler
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME,build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments,normalize_batch_payload
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from pc28touzhu.services import rule_lab_store
from pc28touzhu.rule_lab_worker import RuleLabWorker
from tests.test_rule_lab_resources import FakeResources
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_source import SourceFixture,stamp

class Quiet(WSGIRequestHandler):
    def log_message(self,*args):pass

def main():
    with tempfile.TemporaryDirectory(prefix='pc28-management-ui-') as directory:
        root=Path(directory);fixture=SourceFixture(root/'source.db')
        os.environ['RULE_LAB_DATA_DIR']=str(root/'lab');os.environ['RULE_LAB_SOURCE_DB']=str(fixture.path)
        now=datetime.now(timezone.utc);day=(now+timedelta(hours=8)).date()-timedelta(days=1)
        start=datetime.combine(day,datetime.min.time(),tzinfo=timezone(timedelta(hours=8))).astimezone(timezone.utc)+timedelta(seconds=150)
        for offset in range(-100,5):
            at=start+timedelta(seconds=210*offset);fixture.draw(1100+offset,at);fixture.prediction(1100+offset,at)
        source=RuleLabSource(fixture.path);store=RuleLabStore(root/'lab'/'lab.db');store.initialize()
        service=RuleLabService(store,source,resources=FakeResources())
        repo=DatabaseRepository(str(root/'platform.db'));repo.initialize_database()
        admin=repo.create_user_record(username='management-ui-admin',role='admin');secret='management-local-fixture'
        payload=dict(name='已有成绩 · 原实验',mode='backtest',predictor_id=1,metric='big_small',template_pack='basic_v2',
            stake_amount=1,start_date=day.isoformat(),end_date=day.isoformat(),risk_policy=policy())
        config=normalize_batch_payload(payload,now=stamp(now))
        existing=store.create_batch(admin['id'],config,'公开来源',build_experiments('big_small')[:2],now=stamp(now))
        store.set_batch_state(admin['id'],existing['id'],'running',now=stamp(now))
        for _ in range(30):
            RuleLabWorker(store,source).tick(now=stamp(now))
            if store.get_batch(admin['id'],existing['id'])['status']=='completed':break
        assert store.get_batch(admin['id'],existing['id'])['status']=='completed'
        pristine=service.create_batch(admin['id'],{**payload,'name':'等待编辑 · 尚未启动'},now=stamp(now))['batch']
        before=store.list_experiments(admin['id'],existing['id'])
        app=PlatformApiApplication(repo,'offline',secret,rule_lab_service=service)
        def reply(response,value):
            body=json.dumps(value,ensure_ascii=False).encode();response('200 OK',[('Content-Type','application/json'),('Content-Length',str(len(body)))]);return [body]
        def local(environ,response):
            path=environ.get('PATH_INFO');method=environ.get('REQUEST_METHOD')
            if path=='/__fixture/maintenance' and method=='POST':
                values=[]
                for _ in range(30):values.append(store.management_maintenance(now=stamp(datetime.now(timezone.utc)),limit=500,max_pages=256))
                return reply(response,{'ok':True,'last':values[-1]})
            if path=='/__fixture/state' and method=='GET':
                return reply(response,{'batches':store.list_batches(admin['id']),'original':store.list_experiments(admin['id'],existing['id']),
                    'before':before,'storage':store.storage_status()})
            if path=='/__fixture/create-study' and method=='POST':
                result=store.create_study(admin['id'],existing['id'],'比较组管理样本','management-ui-study',now=stamp(now))
                return reply(response,result)
            if path in ('/__fixture/capacity/full','/__fixture/capacity/normal') and method=='POST':
                rule_lab_store.MAX_TOTAL_EXPERIMENTS=1 if path.endswith('/full') else 650
                return reply(response,{'ok':True})
            if method!='GET' and not path.startswith('/api/platform/rule-lab/'):
                response('403 Forbidden',[('Content-Length','0')]);return [b'']
            return app(environ,response)
        with make_server('127.0.0.1',0,local,handler_class=Quiet) as server:
            print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port),cookie_name=SESSION_COOKIE_NAME,
                cookie=build_session_cookie_value(admin['id'],secret),existing_id=existing['id'],pristine_id=pristine['id'],day=day.isoformat())),flush=True)
            threading.Thread(target=server.serve_forever,daemon=True).start();sys.stdin.read();server.shutdown()
if __name__=='__main__':main()
