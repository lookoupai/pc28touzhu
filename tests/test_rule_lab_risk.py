"""Round and daily risk contracts: pure state, real temporary ledger and migration."""
import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.domain.rule_lab_risk import (
    normalize_risk_policy, empty_risk_runtime, empty_risk_day,
    admit_risk, settle_risk,
)
from pc28touzhu.services.rule_lab_store import RuleLabStore, RuleLabError

NOW='2026-11-26T04:00:00Z'


def policy(**changes):
    p=dict(version=1,mode='round',round=dict(enabled=True,profit_target=10,loss_limit=10,max_bets=0),
           day=dict(enabled=True,profit_target=20,loss_limit=20,max_bets=0,max_rounds=0,
                    hit_rate_gate=dict(enabled=False,threshold=0,min_settled_count=20)),
           additional_day_guards=[],cooldown_issues=0,source={})
    p.update(changes)
    return normalize_risk_policy(p)


class RiskDomainTests(unittest.TestCase):
    def test_policy_no_exit_and_invalid_types_are_rejected(self):
        self.assertEqual(normalize_risk_policy(None)['mode'],'legacy')
        p=policy()
        for changes in [dict(cooldown_issues=True),dict(day=[]),dict(mode=[]),dict(secret='bad')]:
            with self.assertRaises(ValueError): normalize_risk_policy({**p,**changes})
        p['round'].update(profit_target=0,loss_limit=0)
        p['day'].update(profit_target=0,loss_limit=0,max_rounds=1)
        with self.assertRaisesRegex(ValueError,'退出'): normalize_risk_policy(p)

    def test_round_continues_without_rematching_and_cooldown_is_from_start(self):
        p=policy(cooldown_issues=10)
        p['round'].update(profit_target=1,loss_limit=10)
        r,d=empty_risk_runtime(),empty_risk_day('2026-11-26')
        a=admit_risk(p,r,d,issue_no='1000',at=NOW,matched=True,pending_count=0)
        self.assertTrue(a['allowed']);r,d=a['runtime'],a['day']
        s=settle_risk(p,r,d,dict(status='settled',net_cents=-100,result_type='miss',settled_at=NOW,issue_no='1000'))
        r,d=s['runtime'],s['day']
        a=admit_risk(p,r,d,issue_no='1001',at=NOW,matched=False,pending_count=0)
        self.assertTrue(a['allowed'])
        s=settle_risk(p,a['runtime'],a['day'],dict(status='settled',net_cents=300,result_type='hit',settled_at=NOW,issue_no='1001'))
        self.assertEqual(s['closed_round']['net_cents'],200)
        self.assertEqual(s['closed_round']['stop_reason'],'round_profit_target')
        self.assertFalse(admit_risk(p,s['runtime'],s['day'],issue_no='1009',at=NOW,matched=True,pending_count=0)['allowed'])
        self.assertTrue(admit_risk(p,s['runtime'],s['day'],issue_no='1010',at=NOW,matched=True,pending_count=0)['allowed'])

    def test_day_gate_ignores_refunds_is_strict_and_sticky(self):
        p=policy();p['day']['hit_rate_gate']=dict(enabled=True,threshold=50,min_settled_count=2)
        p['round']['profit_target']=100
        r,d=empty_risk_runtime(),empty_risk_day('2026-11-26')
        for index,(kind,net) in enumerate([('refund',0),('hit',100),('miss',-100),('miss',-100)]):
            a=admit_risk(p,r,d,issue_no=str(1000+index),at=NOW,matched=True,pending_count=0)
            self.assertTrue(a['allowed'])
            s=settle_risk(p,a['runtime'],a['day'],dict(status='settled',net_cents=net,result_type=kind,settled_at=NOW,issue_no=str(1000+index)))
            r,d=s['runtime'],s['day']
            if index<3:self.assertEqual(d['stop_reason'],'')
        self.assertEqual(d['stop_reason'],'daily_hit_rate_low')
        self.assertFalse(admit_risk(p,r,d,issue_no='1004',at=NOW,matched=True,pending_count=0)['allowed'])
        self.assertTrue(admit_risk(p,r,empty_risk_day('2026-11-27'),issue_no='1400',at=NOW,matched=True,pending_count=0)['allowed'])

    def test_max_rounds_does_not_interrupt_active_round_and_pending_reserves_bets(self):
        p=policy();p['day'].update(max_rounds=1,max_bets=2)
        r,d=empty_risk_runtime(),empty_risk_day('2026-11-26')
        a=admit_risk(p,r,d,issue_no='1000',at=NOW,matched=True,pending_count=0)
        self.assertFalse(admit_risk(p,a['runtime'],a['day'],issue_no='1001',at=NOW,matched=True,pending_count=1)['allowed'])
        s=settle_risk(p,a['runtime'],a['day'],dict(status='settled',net_cents=98,result_type='hit',settled_at=NOW,issue_no='1000'))
        a=admit_risk(p,s['runtime'],s['day'],issue_no='1001',at=NOW,matched=False,pending_count=0)
        self.assertTrue(a['allowed'])
        s=settle_risk(p,a['runtime'],a['day'],dict(status='settled',net_cents=98,result_type='hit',settled_at=NOW,issue_no='1001'))
        self.assertEqual(s['day']['stop_reason'],'day_max_bets')
        self.assertIsNone(s['runtime']['active_round'])

    def test_round_loss_limit_preserves_overshoot_and_round_survives_midnight(self):
        p=policy();p['round'].update(loss_limit=1.5,profit_target=100)
        r,d=empty_risk_runtime(),empty_risk_day('2026-11-25')
        a=admit_risk(p,r,d,issue_no='1000',at=NOW,matched=True,pending_count=0)
        s=settle_risk(p,a['runtime'],a['day'],dict(status='settled',net_cents=-100,result_type='miss',settled_at=NOW,issue_no='1000'))
        a=admit_risk(p,s['runtime'],empty_risk_day('2026-11-26'),issue_no='1001',at=NOW,matched=False,pending_count=0)
        self.assertTrue(a['allowed']);self.assertEqual(a['runtime']['round_seq'],1)
        s=settle_risk(p,a['runtime'],a['day'],dict(status='settled',net_cents=-100,result_type='miss',settled_at=NOW,issue_no='1001'))
        self.assertEqual(s['closed_round']['net_cents'],-200)
        self.assertEqual(s['closed_round']['stop_reason'],'round_loss_limit')
        self.assertEqual(s['day']['net_cents'],-100)

    def test_invalid_round_is_not_counted_as_success_or_flat_completion(self):
        p=policy();a=admit_risk(p,empty_risk_runtime(),empty_risk_day('2026-11-26'),issue_no='1000',at=NOW,matched=True,pending_count=0)
        s=settle_risk(p,a['runtime'],a['day'],dict(status='skipped',net_cents=None,result_type=None,settled_at=NOW,issue_no='1000'))
        self.assertEqual(s['runtime']['invalid_rounds'],1)
        self.assertEqual(s['runtime']['completed_rounds'],0)
        self.assertEqual(s['runtime']['flat_rounds'],0)


class RiskStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'risk.db';self.store=RuleLabStore(self.path);self.store.initialize()

    def make(self,p=None):
        config=normalize_batch_payload(dict(name='按轮',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-25',end_date='2026-11-26',risk_policy=p or policy()),now=NOW)
        return self.store.create_batch(1,config,'source',build_experiments('big_small'),now=NOW)

    def step(self,bid,issue,net=98,*,matched=True,stamp=NOW,risk_date='2026-11-26',settle=True):
        work=self.store.get_work_item(bid);eid=work['experiments'][0]['id']
        d=dict(experiment_id=eid,issue_no=str(issue),bet_value='大',stake_cents=100,decided_at=stamp,decision_mode='backtest',
               feature_issue_no=str(issue-1),condition_value=1,condition_matched=matched,risk_date=risk_date)
        out=dict(experiment_id=eid,issue_no=str(issue),status='settled',settled_at=stamp,result_type='hit' if net>0 else 'miss' if net<0 else 'refund',
                 net_cents=net,direction_hit=net>=0,refund_reason='',skip_reason='')
        result=self.store.commit_step(bid,work['batch']['revision'],cursor={'last_issue_no':str(issue)},now=NOW,decisions=[d],outcomes=[out] if settle else [])
        return result,eid,out

    def test_round_tp_blocks_new_bets_and_restarts_after_cooldown(self):
        p=policy(cooldown_issues=10);p['round']['profit_target']=1
        b=self.make(p);self.store.set_batch_state(1,b['id'],'running',now=NOW)
        self.step(b['id'],1000)
        self.step(b['id'],1001,matched=False)
        self.assertEqual(self.step(b['id'],1002)[0]['inserted_decisions'],0)
        self.assertEqual(self.step(b['id'],1010)[0]['inserted_decisions'],1)
        e=self.store.list_experiments(1,b['id'])[0]
        self.assertEqual(e['settled_count'],3)
        self.assertEqual(e['risk_state']['completed_rounds'],1)
        detail=self.store.get_experiment_detail(1,b['id'],e['id'])
        self.assertEqual(detail['recent_rounds'][-1]['net_profit'],1.96)

    def test_unstarted_edit_is_atomic_and_started_is_immutable(self):
        b=self.make();old=self.store.get_work_item(b['id'])
        p=policy();p['round']['profit_target']=5
        self.store.set_risk_policy(1,b['id'],p,now=NOW)
        self.assertEqual(self.store.get_batch(1,b['id'])['risk_policy']['round']['profit_target'],5)
        result=self.store.commit_step(b['id'],old['batch']['revision'],cursor={},now=NOW)
        self.assertFalse(result['committed'])
        with self.assertRaises(RuleLabError):self.store.set_risk_policy(2,b['id'],p,now=NOW)
        self.store.set_batch_state(1,b['id'],'running',now=NOW)
        self.store.set_batch_state(1,b['id'],'paused',now=NOW)
        with self.assertRaises(RuleLabError) as cm:self.store.set_risk_policy(1,b['id'],p,now=NOW)
        self.assertEqual(cm.exception.code,'risk_policy_locked')

    def test_round_cleanup_keeps_cumulative_counts_and_replay_remains_idempotent(self):
        p=policy();p['round']['max_bets']=1
        b=self.make(p);self.store.set_batch_state(1,b['id'],'running',now=NOW)
        result,eid,out=self.step(b['id'],1000)
        work=self.store.get_work_item(b['id'])
        self.store.commit_step(b['id'],work['batch']['revision'],cursor=work['batch']['cursor'],now=NOW,outcomes=[out])
        self.assertEqual(self.store.list_experiments(1,b['id'])[0]['risk_state']['completed_rounds'],1)
        self.store.prune(now='2027-01-26T04:00:00Z')
        detail=self.store.get_experiment_detail(1,b['id'],eid)
        self.assertEqual(detail['recent_rounds'],[])
        self.assertEqual(detail['experiment']['risk_state']['completed_rounds'],1)

    def test_day_round_limit_is_visible_after_round_closes(self):
        p=policy();p['round']['max_bets']=1;p['day']['max_rounds']=1
        b=self.make(p);self.store.set_batch_state(1,b['id'],'running',now=NOW)
        self.step(b['id'],1000)
        state=self.store.list_experiments(1,b['id'])[0]['risk_state']
        self.assertEqual(state['state'],'day_stopped');self.assertEqual(state['stop_reason'],'day_max_rounds')
        self.assertEqual(self.step(b['id'],1001)[0]['inserted_decisions'],0)

    def test_cross_day_pending_is_accounted_on_actual_day_without_rewinding(self):
        p=policy();p['day']['profit_target']=1
        b=self.make(p);self.store.set_batch_state(1,b['id'],'running',now=NOW)
        result,eid,out=self.step(b['id'],1000,net=185,stamp='2026-11-25T15:59:30Z',risk_date='2026-11-25',settle=False)
        out['settled_at']='2026-11-25T16:00:10Z'
        w=self.store.get_work_item(b['id'])
        self.store.commit_step(b['id'],w['batch']['revision'],cursor=w['batch']['cursor'],now=NOW,outcomes=[out])
        e=self.store.list_experiments(1,b['id'])[0]
        self.assertEqual(e['risk_state']['day_date'],'2026-11-26')
        self.assertEqual(e['risk_state']['day_net_profit'],1.85)
        self.assertEqual(e['risk_state']['stop_reason'],'day_profit_target')
        self.assertEqual(self.step(b['id'],1001)[0]['inserted_decisions'],0)

    def test_v1_migration_keeps_all_old_columns_and_is_repeatable(self):
        from pc28touzhu.domain.rule_lab import empty_stats,apply_outcome
        old_path=Path(self.tmp.name)/'v1.db'
        with sqlite3.connect(old_path) as c:
            c.executescript((Path(__file__).parent/'fixtures'/'rule_lab_v1.sql').read_text())
            c.execute('''INSERT INTO lab_batches(id,user_id,name,mode,predictor_id,predictor_name,metric,template_pack,stake_cents,
                status,created_at,started_at,updated_at,cursor_json,revision,processed_issues) VALUES(1,1,'legacy','live',1,'source',
                'big_small','basic_v1',100,'running',?,?,?, ?,4,2)''',(NOW,NOW,NOW,json.dumps({'last_issue_no':'1002'})))
            definition=build_experiments('big_small')[0]
            stats=apply_outcome(empty_stats(),dict(result_type='hit',net_cents=98,direction_hit=True),stake_cents=100);stats['trigger_count']=2
            c.execute('''INSERT INTO lab_experiments(id,batch_id,condition_key,name,condition_json,settlement_rule_id,
                settlement_snapshot_json,stats_json,last_settled_issue,last_decided_issue) VALUES(1,1,?,?,?,?,?,?,1001,1002)''',
                (definition['condition_key'],definition['name'],json.dumps(definition['condition']),definition['settlement_rule_id'],
                 json.dumps(definition['settlement_snapshot']),json.dumps(stats)))
            c.execute('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,stake_cents,decided_at,decision_mode,
                condition_value_json,status,settled_at,finalized_at,result_type,net_cents) VALUES(1,'1001',1001,'大',100,?,'live','1','settled',?,?,'hit',98)''',(NOW,NOW,NOW))
            c.execute('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,stake_cents,decided_at,decision_mode,
                condition_value_json) VALUES(1,'1002',1002,'大',100,?,'live','1')''',(NOW,))
            c.execute("INSERT INTO lab_daily(experiment_id,stat_date,settled_count,hit_count,direction_hit_count,stake_cents,net_cents,trigger_count) VALUES(1,'2026-11-26',1,1,1,100,98,2)")
        old_columns={};before={}
        with sqlite3.connect(old_path) as c:
            for (table,) in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'lab_%'"):
                columns=[r[1] for r in c.execute('PRAGMA table_info('+table+')')];old_columns[table]=columns
                before[table]=c.execute('SELECT '+','.join(columns)+' FROM '+table+' ORDER BY rowid').fetchall()
        migrated=RuleLabStore(old_path);migrated.initialize();migrated.initialize()
        with sqlite3.connect(old_path) as c:
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],5)
            for table,columns in old_columns.items():
                self.assertEqual(c.execute('SELECT '+','.join(columns)+' FROM '+table+' ORDER BY rowid').fetchall(),before[table],table)
        self.assertEqual(migrated.get_batch(1,1)['risk_policy']['mode'],'legacy')
        self.assertEqual(migrated.list_experiments(1,1)[0]['net_profit'],.98)
        self.assertEqual(len(migrated.get_pending_decisions(1)),1)
