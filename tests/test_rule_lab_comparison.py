"""Risk studies are fresh, atomic, bounded and comparable only on identical inputs."""
import copy
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from datetime import datetime,timedelta,timezone
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from pc28touzhu.domain.rule_lab import build_experiments,normalize_batch_payload
from pc28touzhu.domain.rule_lab_comparison import risk_variants,evidence_step
from pc28touzhu.services.rule_lab_store import RuleLabStore,RuleLabError
from tests.test_rule_lab_risk import policy,NOW
from tests.test_rule_lab_source import SourceFixture,stamp,UTC
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.rule_lab_worker import RuleLabWorker


class ComparisonDomainTests(unittest.TestCase):
    def test_five_variants_change_only_named_factor_and_do_not_mutate_original(self):
        original=policy();original['source']={'rule_id':6,'rule_name':'旧快照'};before=copy.deepcopy(original)
        variants=risk_variants(original,metric='combo')
        self.assertEqual([v['key'] for v in variants[:5]],['baseline','round_tp_half','round_sl_half','day_sl_half','gate_off'])
        self.assertFalse(variants[4]['available'])
        original['day']['hit_rate_gate']['enabled']=True;original['day']['hit_rate_gate']['threshold']=25
        variants=risk_variants(original,metric='combo')
        self.assertEqual(sum(v['available'] for v in variants),7)
        self.assertEqual(variants[1]['policy']['round']['profit_target'],5)
        self.assertEqual(variants[1]['policy']['round']['loss_limit'],10)
        self.assertEqual(variants[3]['policy']['day']['loss_limit'],10)
        self.assertFalse(variants[4]['policy']['day']['hit_rate_gate']['enabled'])
        self.assertEqual(before['round'],original['round'])

    def test_half_below_cent_and_disabled_are_unavailable_without_duplicates(self):
        p=policy();p['round']['profit_target']=.01;p['round']['enabled']=False;p['day']['loss_limit']=.01
        result=risk_variants(p,metric='combo')
        self.assertFalse(result[1]['available']);self.assertFalse(result[2]['available']);self.assertFalse(result[3]['available'])
        self.assertTrue(all(v['reason'] for v in result if not v['available']))

    def test_binary_grid_freezes_other_controls_and_deduplicates_baseline(self):
        p=policy();p['day']['hit_rate_gate'].update(enabled=True,threshold=25)
        p['source']={'rule_id':6,'rule_name':'风控来源'}
        before=copy.deepcopy(p)
        for metric in ('big_small','odd_even'):
            variants={v['key']:v for v in risk_variants(p,metric=metric)}
            self.assertEqual(sum(v['available'] for v in variants.values()),8)
            self.assertFalse(variants['gate_25_20']['available'])
            for threshold,minimum in ((25,50),(50,20),(50,50)):
                actual=variants[f'gate_{threshold}_{minimum}']['policy']
                expected=copy.deepcopy(p)
                expected['day']['hit_rate_gate'].update(threshold=threshold,min_settled_count=minimum)
                expected['source']['modified']=True
                self.assertEqual(actual,expected)
        self.assertEqual(p,before)

    def test_combo_grid_and_additional_guards_have_no_hidden_gate(self):
        p=policy();p['day']['hit_rate_gate'].update(enabled=True,threshold=25)
        extra=copy.deepcopy(p['day']);extra['hit_rate_gate'].update(threshold=50,min_settled_count=50)
        p['additional_day_guards']=[extra]
        variants={v['key']:v for v in risk_variants(p,metric='combo')}
        self.assertNotIn('gate_50_50',variants)
        for threshold in (20,25,30):
            candidate=variants[f'gate_{threshold}_20']['policy']
            for control in [candidate['day']]+candidate['additional_day_guards']:
                self.assertEqual(control['hit_rate_gate'],dict(enabled=True,threshold=threshold,min_settled_count=20))
        p['day']['enabled']=False;p['additional_day_guards'][0]['enabled']=False
        disabled=risk_variants(p,metric='combo')
        self.assertTrue(all(not v['available'] for v in disabled if v['key'].startswith('gate_')))
        with self.assertRaises(ValueError):risk_variants(p,metric='number')

    def test_fingerprint_excludes_observation_time_but_keeps_inputs(self):
        first=evidence_step(None,{'issue_no':'1001','triplet':[3,6,8],'observed_at':'a','prediction':None})
        second=evidence_step(None,{'issue_no':'1001','triplet':[3,6,8],'observed_at':'b','prediction':None})
        changed=evidence_step(None,{'issue_no':'1001','triplet':[1,2,3],'observed_at':'a','prediction':None})
        self.assertEqual(first,second);self.assertNotEqual(first,changed)


class ComparisonStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=RuleLabStore(Path(self.tmp.name)/'lab.db');self.store.initialize()
        self.p=policy();self.p['day']['hit_rate_gate'].update(enabled=True,threshold=25)
        config=normalize_batch_payload(dict(name='原始组合',mode='backtest',predictor_id=7,metric='combo',template_pack='basic_v1',stake_amount=1,
            start_date='2026-10-01',end_date='2026-11-26',risk_policy=self.p),now=NOW)
        self.original=self.store.create_batch(1,config,'频次趋势',build_experiments('combo',template_pack='basic_v1'),now=NOW)

    def create(self,token='study-request-0001',name='五组比较'):
        return self.store.create_study(1,self.original['id'],name,token,now=NOW)

    def test_fresh_grid_atomic_copy_keeps_original_and_frozen_snapshots(self):
        before=self.store.get_work_item(self.original['id'])
        created=self.create();study=created['study']
        self.assertEqual(study['status'],'paused');self.assertEqual(len(study['children']),7)
        self.assertEqual(self.store.capacity()['saved'],624)
        self.assertEqual(self.store.get_work_item(self.original['id']),before)
        for child in study['children']:
            work=self.store.get_work_item(child['batch_id'])
            self.assertEqual(work['batch']['status'],'paused')
            self.assertEqual(work['batch']['predictor_id'],7)
            self.assertEqual(work['batch']['stake_cents'],100)
            self.assertEqual([e['settlement_snapshot'] for e in work['experiments']],[e['settlement_snapshot'] for e in before['experiments']])
            self.assertTrue(all(e['stats']['settled_count']==0 for e in work['experiments']))

    def test_idempotent_replay_and_capacity_failure_leave_no_partial_children(self):
        first=self.create();again=self.create()
        self.assertEqual(first['study']['id'],again['study']['id']);self.assertFalse(again['created'])
        with self.assertRaises(RuleLabError):self.create(name='different')
        with patch('pc28touzhu.services.rule_lab_store.MAX_TOTAL_EXPERIMENTS',1000):
            with self.assertRaises(RuleLabError) as cm:self.create(token='study-request-0002')
        self.assertEqual(cm.exception.code,'capacity_exceeded')
        self.assertEqual(self.store.capacity()['saved'],624)
        self.assertEqual(len(self.store.list_studies(1)),1)

    def test_parent_queue_owns_children_and_waits_for_existing_batch_capacity(self):
        study=self.create()['study'];sid=study['id']
        self.store.set_batch_state(1,self.original['id'],'running',now=NOW)
        self.store.set_study_state(1,sid,'running',now=NOW)
        self.store.advance_studies(now=NOW,cutoff={'issue_no':'2000','at':NOW})
        self.assertEqual(self.store.get_study(1,sid)['status'],'queued')
        child=study['children'][0]['batch_id']
        with self.assertRaises(RuleLabError) as cm:self.store.set_batch_state(1,child,'running',now=NOW)
        self.assertEqual(cm.exception.code,'study_managed')
        with self.assertRaises(RuleLabError):self.store.set_risk_policy(1,child,self.p,now=NOW)
        self.store.set_batch_state(1,self.original['id'],'paused',now=NOW)
        self.store.advance_studies(now=NOW,cutoff={'issue_no':'2001','at':NOW})
        current=self.store.get_study(1,sid)
        self.assertEqual(current['status'],'running');self.assertEqual(self.store.capacity()['running'],78)
        self.assertEqual(current['cutoff_issue'],'2001')
        self.store.set_study_state(1,sid,'paused',now=NOW)
        self.assertEqual(self.store.capacity()['running'],0)
        self.store.set_study_state(1,sid,'running',now=NOW)
        self.store.advance_studies(now=NOW,cutoff={'issue_no':'9999','at':NOW})
        self.assertEqual(self.store.get_study(1,sid)['cutoff_issue'],'2001')

    def test_failure_stops_group_and_does_not_skip_remaining(self):
        study=self.create()['study'];sid=study['id']
        self.store.set_study_state(1,sid,'running',now=NOW);self.store.advance_studies(now=NOW,cutoff={'issue_no':'2000','at':NOW})
        child=study['children'][0]['batch_id'];work=self.store.get_work_item(child)
        self.store.commit_step(child,work['batch']['revision'],cursor={},now=NOW,progress={'status':'error','error':'坏数据'})
        self.store.advance_studies(now=NOW,cutoff={'issue_no':'9000','at':NOW})
        current=self.store.get_study(1,sid);self.assertEqual(current['status'],'error')
        self.assertTrue(all(c['status']=='paused' for c in current['children'][1:]))
        with self.assertRaises(RuleLabError):self.store.get_study(2,sid)

    def test_fifo_moves_completed_child_to_back_of_queue_and_resume_keeps_progress(self):
        # Two 200-version groups (gate already off); 50 versions per active child.
        tmp=Path(self.tmp.name)/'fifo.db';store=RuleLabStore(tmp);store.initialize()
        p=policy();cfg=normalize_batch_payload(dict(name='s',mode='backtest',predictor_id=1,metric='big_small',start_date='2026-11-01',end_date='2026-11-26',risk_policy=p),now=NOW)
        original=store.create_batch(1,cfg,'s',build_experiments('big_small'),now=NOW)
        a=store.create_study(1,original['id'],'a','fifo-request-a',now=NOW)['study'];b=store.create_study(1,original['id'],'b','fifo-request-b',now=NOW)['study']
        store.set_study_state(1,a['id'],'running',now=NOW);store.set_study_state(1,b['id'],'running',now=NOW)
        store.advance_studies(now=NOW,cutoff={'issue_no':'2000','at':NOW})
        self.assertEqual(store.capacity()['running'],100)
        child=a['children'][0]['batch_id'];work=store.get_work_item(child)
        store.commit_step(child,work['batch']['revision'],cursor={'last_issue_no':'1001'},now=NOW,progress={'processed_issues_delta':1})
        stale=store.get_work_item(child)
        store.set_study_state(1,a['id'],'paused',now=NOW)
        result=store.commit_step(child,stale['batch']['revision'],cursor=stale['batch']['cursor'],now=NOW)
        self.assertFalse(result['committed'])
        store.set_study_state(1,a['id'],'running',now=NOW);store.advance_studies(now=NOW,cutoff={'issue_no':'9000','at':NOW})
        self.assertEqual(store.get_work_item(child)['batch']['processed_issues'],1)
        self.assertEqual(store.get_study(1,a['id'])['cutoff_issue'],'2000')

    def test_fifo_waiting_group_runs_before_previous_group_next_child(self):
        store=RuleLabStore(Path(self.tmp.name)/'order.db');store.initialize()
        cfg=normalize_batch_payload(dict(name='s',mode='backtest',predictor_id=1,metric='big_small',start_date='2026-11-01',end_date='2026-11-26',risk_policy=policy()),now=NOW)
        original=store.create_batch(1,cfg,'s',build_experiments('big_small'),now=NOW)
        a=store.create_study(1,original['id'],'a','order-request-a',now=NOW)['study'];b=store.create_study(1,original['id'],'b','order-request-b',now=NOW)['study']
        store.set_batch_state(1,original['id'],'running',now=NOW)  # Leaves one 50-version slot.
        for study in (a,b):store.set_study_state(1,study['id'],'running',now=NOW)
        store.advance_studies(now=NOW,cutoff={'issue_no':'2000','at':NOW})
        self.assertEqual(store.get_study(1,a['id'])['status'],'running')
        self.assertEqual(store.get_study(1,b['id'])['status'],'queued')
        child=a['children'][0]['batch_id'];work=store.get_work_item(child)
        store.commit_step(child,work['batch']['revision'],cursor={},now=NOW,progress={'status':'completed'})
        store.advance_studies(now=NOW,cutoff={'issue_no':'2000','at':NOW})
        self.assertEqual(store.get_study(1,b['id'])['status'],'running')
        self.assertEqual(store.get_study(1,a['id'])['status'],'queued')
        self.assertEqual(store.capacity()['running'],100)

    def test_complete_comparison_requires_identical_evidence(self):
        study=self.create()['study'];sid=study['id']
        self.store.set_study_state(1,sid,'running',now=NOW)
        for index,child in enumerate(study['children']):
            self.store.advance_studies(now=NOW,cutoff={'issue_no':'2000','at':NOW})
            work=self.store.get_work_item(child['batch_id'])
            evidence=dict(digest='same',count=2,warm_count=100,first_issue='1001',last_issue='1002',conflict=False)
            if index==4:evidence['digest']='changed'
            self.store.commit_step(child['batch_id'],work['batch']['revision'],cursor={'evidence':evidence},now=NOW,
                progress={'status':'completed','processed_issues_delta':2})
        self.store.advance_studies(now=NOW,cutoff={'issue_no':'2000','at':NOW})
        comparison=self.store.study_comparison(1,sid)
        self.assertEqual(comparison['study']['status'],'completed')
        self.assertFalse(comparison['study']['comparable'])
        self.assertTrue(all(r['delta_net_profit'] is None for r in comparison['rows']))

    def test_fault_during_group_creation_rolls_back_all_children(self):
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("CREATE TRIGGER fail_study_insert BEFORE INSERT ON lab_study_batches WHEN NEW.position=2 BEGIN SELECT RAISE(ABORT,'injected'); END")
        with self.assertRaises(RuleLabError):self.create()
        self.assertEqual(self.store.capacity()['saved'],78)
        self.assertEqual(self.store.list_studies(1),[])

    def test_concurrent_creation_same_key_is_single_atomic_group(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:self.create(),range(2)))
        self.assertEqual(len({r['study']['id'] for r in results}),1)
        self.assertEqual(sum(r['created'] for r in results),1)
        self.assertEqual(self.store.capacity()['saved'],624)

    def test_v2_schema_migration_preserves_rows_and_all_old_columns(self):
        path=Path(self.tmp.name)/'v2.db'
        with sqlite3.connect(path) as c:
            c.executescript((Path(__file__).parent/'fixtures'/'rule_lab_v2.sql').read_text())
            c.execute("INSERT INTO lab_batches(id,user_id,name,mode,predictor_id,predictor_name,metric,template_pack,stake_cents,created_at,updated_at,risk_policy_json) VALUES(1,1,'old','backtest',7,'source','combo','basic_v1',100,?,?,?)",(NOW,NOW,json.dumps(self.p)))
            c.execute("INSERT INTO lab_runtime(key,value_json) VALUES('worker','{}')")
            columns={table:[r[1] for r in c.execute('PRAGMA table_info('+table+')')] for table, in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            before={table:c.execute('SELECT '+','.join(cols)+' FROM '+table+' ORDER BY rowid').fetchall() for table,cols in columns.items()}
        new=RuleLabStore(path);new.initialize();new.initialize()
        with sqlite3.connect(path) as c:
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],8)
            for table,cols in columns.items():
                condition=" WHERE key!='result_retention'" if table=='lab_runtime' else ''
                self.assertEqual(c.execute('SELECT '+','.join(cols)+' FROM '+table+condition+' ORDER BY rowid').fetchall(),before[table])
            self.assertEqual(c.execute("SELECT count(*) FROM lab_runtime WHERE key='result_retention'").fetchone()[0],1)


class ComparisonWorkerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.fixture=SourceFixture(Path(self.tmp.name)/'source.db');self.source=RuleLabSource(self.fixture.path)
        self.store=RuleLabStore(Path(self.tmp.name)/'lab'/'lab.db');self.store.initialize()
        self.opened=datetime(2026,11,26,4,tzinfo=UTC);self.now=stamp(self.opened+timedelta(hours=2))
        for i in range(-5,12):
            opened=self.opened+timedelta(seconds=210*i)
            self.fixture.draw(1100+i,opened,balls=(3,6,8) if i%3 else (1,2,5))
            self.fixture.prediction(1100+i,opened)
        p=policy();p['round'].update(profit_target=2,loss_limit=2);p['day'].update(profit_target=5,loss_limit=4)
        p['day']['hit_rate_gate'].update(enabled=True,threshold=25,min_settled_count=3)
        cfg=normalize_batch_payload(dict(name='source',mode='backtest',predictor_id=1,metric='big_small',start_date='2026-11-26',end_date='2026-11-26',risk_policy=p),now=self.now)
        self.original=self.store.create_batch(1,cfg,'source',build_experiments('big_small'),now=self.now)
        self.study=self.store.create_study(1,self.original['id'],'compare','worker-study-0001',now=self.now)['study']
        self.store.set_study_state(1,self.study['id'],'running',now=self.now)

    def run_to_end(self,page_size=3):
        for _ in range(100):
            size=page_size if page_size else (1 if _%2 else 200)
            RuleLabWorker(self.store,self.source,page_size=size).tick(now=self.now)
            state=self.store.get_study(1,self.study['id'])
            if state['status'] in ('completed','error','resource_paused'):return state
        self.fail('study did not complete')

    def test_all_grid_variants_complete_with_frozen_cutoff_and_matching_input_evidence(self):
        RuleLabWorker(self.store,self.source,page_size=200).tick(now=self.now)
        frozen=self.store.get_study(1,self.study['id'])['cutoff_issue']
        later=self.opened+timedelta(hours=3)
        self.fixture.draw(1200,later);self.fixture.prediction(1200,later)
        self.now=stamp(later+timedelta(hours=1))
        state=self.run_to_end(0)
        self.assertEqual(state['status'],'completed',state)
        self.assertEqual(state['cutoff_issue'],frozen)
        self.assertTrue(state['comparable'],state)
        self.assertEqual(len({c['digest'] for c in state['actual_coverage']}),1)
        rows=self.store.study_comparison(1,state['id'])['rows']
        self.assertEqual(len(rows),450)
        base=next(r for r in rows if r['variant_key']=='baseline' and r['condition']['type']=='always')
        self.assertEqual(base['delta_net_profit'],0)
        self.assertGreater(base['betting_days'],0)

    def test_source_revision_after_baseline_marks_comparison_incomparable(self):
        for _ in range(30):
            RuleLabWorker(self.store,self.source,page_size=200).tick(now=self.now)
            state=self.store.get_study(1,self.study['id'])
            if state['children'][0]['status']=='completed':break
        self.fixture.execute("UPDATE predictions SET prediction_big_small='小' WHERE issue_no='1105'")
        state=self.run_to_end(200)
        self.assertEqual(state['status'],'completed',state)
        self.assertFalse(state['comparable'])
        self.assertTrue(all(r['delta_net_profit'] is None for r in self.store.study_comparison(1,state['id'])['rows']))

    def test_old_scheduler_results_remain_visible_but_require_review(self):
        state=self.run_to_end(200);self.assertTrue(state['comparable'])
        bid=state['children'][0]['batch_id']
        before=self.store.list_experiments(1,bid)
        with sqlite3.connect(self.store.db_path) as c:
            cursor=json.loads(c.execute('SELECT cursor_json FROM lab_batches WHERE id=?',(bid,)).fetchone()[0])
            cursor.pop('replay_scheduler_version')
            c.execute('UPDATE lab_batches SET cursor_json=? WHERE id=?',(json.dumps(cursor),bid))
        current=self.store.study_comparison(1,state['id'])
        self.assertTrue(current['study']['review_required']);self.assertFalse(current['study']['comparable'])
        self.assertIn('需复核',self.store.get_batch(1,bid)['history_notice'])
        self.assertTrue(all(row['delta_net_profit'] is None for row in current['rows']))
        self.assertEqual(self.store.list_experiments(1,bid),before)

    def test_pending_input_moved_after_cutoff_errors_instead_of_waiting_forever(self):
        RuleLabWorker(self.store,self.source,page_size=1).tick(now=self.now)
        child=self.store.get_study(1,self.study['id'])['children'][0]['batch_id']
        pending=self.store.get_pending_decisions(child);self.assertTrue(pending)
        self.fixture.execute("UPDATE lottery_draws SET created_at='2026-11-28 00:00:00' WHERE issue_no=?",(pending[0]['issue_no'],))
        state=self.run_to_end(1)
        self.assertEqual(state['status'],'error');self.assertIn('截止之后',state['error'])
