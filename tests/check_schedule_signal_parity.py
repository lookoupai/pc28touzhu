"""Optional cross-repo regression using temporary execution DB, never production data.
PYTHONPATH=src python3 tests/check_schedule_signal_parity.py --simulator-src /path/AITradingSimulator
"""
import argparse
import copy
import json
import sys
import tempfile
from datetime import datetime,timedelta,timezone
from pathlib import Path
from unittest.mock import patch

parser=argparse.ArgumentParser();parser.add_argument('--simulator-src',required=True);args=parser.parse_args()
sys.path.insert(0,args.simulator_src)
from services.profit_simulator import ProfitSimulator
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.platform_service import create_subscription,resolve_pending_pc28_progressions_from_draws
from pc28touzhu.services.auto_trigger_service import create_auto_trigger_rule,update_auto_trigger_rule_status,run_auto_trigger_cycle
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.pc28_auto_settlement_service import _resolve_pending_win_streak_cycles

published=['2026-11-10T15:59:50Z','2026-11-10T16:03:00Z','2026-11-10T16:06:11Z','2026-11-10T16:10:23Z']
created=['2026-11-10T16:00:13Z','2026-11-10T16:04:06Z','2026-11-10T16:06:36Z','2026-11-10T16:11:13Z']
triplets=[(3,7,8),(5,4,7),(0,5,4),(6,6,7)]
class MemoryHistory:
    def __init__(self):
        self.predictions=[];self.draws={}
        for i,triple in enumerate(triplets):
            issue=str(3504912+i);number=sum(triple);combo=('大' if number>=14 else '小')+('单' if number%2 else '双')
            self.predictions.append({'issue_no':issue,'status':'settled','prediction_combo':'小双','created_at':created[i],'external_fetched_at':published[i]})
            self.draws[issue]={'issue_no':issue,'open_time':(datetime(2026,11,11,0,2,30)+timedelta(seconds=210*i)).isoformat(),
                'result_number':number,'combo':combo,'source_payload':json.dumps({'number':'+'.join(map(str,triple))})}
    def get_pc28_simulation_predictions(self,*a):return self.predictions
    def get_draws_by_issues(self,*a):return self.draws

for allow in (False,True):
 with tempfile.TemporaryDirectory() as tmp:
    repo=DatabaseRepository(str(Path(tmp)/'test.db'));repo.initialize_database()
    uid=repo.create_user('local');source=repo.create_source_record(owner_user_id=uid,source_type='internal_ai',name='offline')
    target=repo.create_delivery_target_record(user_id=uid,executor_type='telegram_group',target_key='offline',status='active')
    sub=create_subscription(repo,{'user_id':uid,'source_id':source['id'],'strategy_v2':{
        'staking_policy':{'mode':'win_streak_roll','structure':'single_play','play_type':'kill_group','selection_mode':'follow','base_amount':10,'target_streak':3,'roll_mode':'chain'},
        'settlement_policy':{'rule_source':'subscription_fixed','settlement_rule_id':'pc28_fullpay_2_8_regular'},'dispatch':{'delivery_target_ids':[target['id']]}}})['item']
    schedule={'timezone':'Asia/Shanghai','windows':[{'id':'zero','start':'00:00','end':'00:59'}], 'allow_pre_window_signal':allow,'signal_max_age_seconds':300}
    rule=create_auto_trigger_rule(repo,user_id=uid,payload={'name':'offline','subscription_ids':[sub['id']], 'trigger_mode':'schedule','schedule':schedule,
       'cycle_policy':{'mode':'rule','max_starts_per_day':1,'on_target':'close_day','on_bust':'close_day'},'cooldown_issues':0})['item']
    update_auto_trigger_rule_status(repo,rule_id=rule['id'],user_id=uid,status='active')
    actual=[]
    for i,triple in enumerate(triplets):
        issue=str(3504912+i);now=datetime.fromisoformat(created[i].replace('Z','+00:00'))+timedelta(seconds=23)
        drawtime=datetime(2026,11,10,16,2,30,tzinfo=timezone.utc)+timedelta(seconds=210*i)
        clock={'latest_issue_no':str(int(issue)-1),'latest_open_time':(drawtime-timedelta(seconds=210)).isoformat(),'fetched_at':now.isoformat(),'stale':False}
        with patch('pc28touzhu.executor.db_repository._utc_now_iso',return_value=now.isoformat()),patch('pc28touzhu.services.dispatch_service._utc_now',return_value=now),patch('pc28touzhu.services.dispatch_service._shanghai_today',return_value='2026-11-11'):
            signal=repo.create_signal_record(source_id=source['id'],lottery_type='pc28',issue_no=issue,bet_type='kill_group',bet_value='大单',published_at=published[i])
            run_auto_trigger_cycle(repo,user_id=uid,rule_id=rule['id'],now=now,draw_clock=clock)
            dispatch_signal(repo,signal['id'],draw_clock=clock)
            for job in repo.list_execution_jobs(user_id=uid):
                if job['issue_no']!=issue or job['status']!='pending':continue
                repo.report_job_result(str(job['id']),'offline',1,'delivered','offline-only',now.isoformat(),{},None)
                resolve_pending_pc28_progressions_from_draws(repo,user_id=uid,draw_items=[{'issue_no':issue,'draw_context':{'triplet':list(triple)}}])
                _resolve_pending_win_streak_cycles(repo)
                event=repo.get_progression_event(job['progression_event_id'])
                state=repo.get_auto_trigger_cycle_state(rule_id=rule['id'],subscription_id=sub['id'],user_id=uid,route_id=job['auto_trigger_route_id'])
                actual.append((issue,event['stake_amount'],event['net_delta'],state['balance'],state['win_streak']))
    config={'structure':'single_play','play_type':'kill_group','selection_mode':'follow','base_amount':10,'target_streak':3,
            'schedule':{k:v for k,v in schedule.items() if k!='timezone'}|{'receive_delay_seconds':23}}
    with patch('services.profit_simulator.get_current_beijing_time',return_value=datetime(2026,11,11,0,20)):
        result=ProfitSimulator(MemoryHistory())._build_pc28_roll_simulation({'id':1,'lottery_type':'pc28','prediction_targets':['combo']},'pc28_fullpay_2_8','regular',config,True)
    simulated=[(r['issue_no'],r['stake_amount'],r['net_profit'],r['balance_after'],r['win_streak']) for r in result['records']]
    assert actual==simulated,(allow,actual,simulated)
    print(json.dumps({'allow_pre_window_signal':allow,'matched':actual},ensure_ascii=False))
