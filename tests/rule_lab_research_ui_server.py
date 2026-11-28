"""Real research UI fixture; all mutations stay in temporary SQLite databases."""
import json,sqlite3,sys,tempfile,threading
from datetime import datetime,timedelta,timezone
from pathlib import Path
from wsgiref.simple_server import make_server
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'src'))
from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME,build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments,normalize_batch_payload,empty_stats
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_store import RuleLabStore
from pc28touzhu.services.rule_lab_source import RuleLabSource
from tests.test_rule_lab_source import SourceFixture
from tests.test_rule_lab_resources import FakeResources
from tests.rule_lab_ui_server import QuietHandler
from tests.test_rule_lab_series import completed_baseline
NOW='2026-11-28T04:00:00Z'
def main():
 with tempfile.TemporaryDirectory(prefix='pc28-research-ui-') as tmp:
  root=Path(tmp);fixture=SourceFixture(root/'source.db');fixture.execute("INSERT INTO predictors VALUES(2,'第二来源','pc28',1,1,'records','secret')")
  fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT,total_predictions INTEGER)')
  start=datetime(2026,11,25,16,tzinfo=timezone.utc)
  for i in range(-1,406):
   opened=start+timedelta(seconds=210*i+60);fixture.draw(2000+i,opened)
   for pid in (1,2):fixture.prediction(2000+i,opened,predictor_id=pid)
  store=RuleLabStore(root/'lab.db');service=RuleLabService(store,RuleLabSource(fixture.path),resources=FakeResources());ids=[]
  for pid,profit in ((1,100),(2,200)):
   config=normalize_batch_payload(dict(name='研究来源'+str(pid),mode='backtest',predictor_id=pid,metric='big_small',start_date='2026-10-01',end_date='2026-10-31'),now=NOW)
   batch=store.create_batch(1,config,'来源'+str(pid),build_experiments('big_small')[:2],now=NOW);ids.append(batch['id'])
   cursor=dict(replay_scheduler_version=2,simulation_time='2026-10-31T15:59:00Z',retention_v1=dict(first_open_at='2026-10-01T00:01:00+08:00',processed_open_at='2026-10-31T15:59:00Z',replay_cutoff_at='2026-11-01T00:00:00Z'))
   with sqlite3.connect(store.db_path) as c:
    c.execute("UPDATE lab_batches SET status='completed',started_at=?,processed_issues=12000,cursor_json=? WHERE id=?",(NOW,json.dumps(cursor),batch['id']))
    for (eid,) in c.execute('SELECT id FROM lab_experiments WHERE batch_id=?',(batch['id'],)).fetchall():
     stats=empty_stats();stats.update(settled_count=100,stake_cents=10000,net_cents=profit,hit_count=60,miss_count=40)
     c.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?',(json.dumps(stats),eid))
     c.execute('INSERT INTO lab_daily(experiment_id,stat_date,settled_count,stake_cents,net_cents,hit_count,miss_count) VALUES(?,?,100,10000,?,60,40)',(eid,'2026-10-15',profit))
  append_batch=completed_baseline(store)
  repo=DatabaseRepository(str(root/'main.db'));repo.initialize_database();admin=repo.create_user_record(username='research-ui',role='admin');secret='temporary-research-test'
  app=PlatformApiApplication(repo,'offline',secret,rule_lab_service=service)
  def serve(environ,start_response):
   if environ.get('PATH_INFO')=='/__fixture/state':
    with sqlite3.connect(store.db_path) as c:data={key:c.execute('SELECT COUNT(*) FROM '+key).fetchone()[0] for key in ('lab_favorites','lab_batches','lab_series','lab_continuation_requests')}
    start_response('200 OK',[('Content-Type','application/json')]);return [json.dumps(data).encode()]
   if environ.get('PATH_INFO')=='/__fixture/drain':
    for _ in range(80):store.management_maintenance(now=NOW,limit=500)
    start_response('200 OK',[('Content-Type','application/json')]);return [b'{"ok":true}']
   return app(environ,start_response)
  with make_server('127.0.0.1',0,serve,handler_class=QuietHandler) as server:
   print(json.dumps(dict(url='http://127.0.0.1:'+str(server.server_port),cookie_name=SESSION_COOKIE_NAME,cookie=build_session_cookie_value(admin['id'],secret),batch_ids=ids,append_id=append_batch['id'])),flush=True)
   threading.Thread(target=server.serve_forever,daemon=True).start();sys.stdin.read();server.shutdown()
if __name__=='__main__':main()
