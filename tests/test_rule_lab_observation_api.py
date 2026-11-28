"""Real WSGI and SQLite contracts for optional read-only list observations."""
from __future__ import annotations

import json
import sqlite3
import unittest
from unittest.mock import patch

from pc28touzhu.domain.rule_lab import build_experiments, empty_stats, normalize_batch_payload
from tests import test_rule_lab_api as api_fixtures
PREFIX=api_fixtures.PREFIX
from tests.test_rule_lab_risk import policy

NOW='2026-11-28T04:00:00Z'
QUERY='observation_months=2&observation_partial=true'


class ObservationApiTests(unittest.TestCase):
    setUp=api_fixtures.RuleLabApiTests.setUp
    request=api_fixtures.RuleLabApiTests.request

    def create_fixture(self,count=2):
        risk=policy();risk['day']['hit_rate_gate'].update(enabled=True,threshold=25,min_settled_count=20)
        config=normalize_batch_payload(dict(name='observation',mode='backtest',predictor_id=7,metric='big_small',
            start_date='2026-10-01',end_date='2026-11-27',risk_policy=risk),now=NOW)
        batch=self.store.create_batch(1,config,'local source',build_experiments('big_small')[:count],now=NOW)
        self.seed([batch['id']])
        return batch

    def seed(self,batch_ids):
        with sqlite3.connect(self.store.db_path) as c:
            for bid in batch_ids:
                cursor=dict(replay_scheduler_version=2,simulation_time='2026-11-27T15:58:00Z',
                    retention_v1=dict(first_open_at='2026-10-01T00:00:01+08:00',processed_open_at='2026-11-27T15:59:59Z',replay_cutoff_at=NOW),
                    evidence=dict(digest='fixed-input',count=23316,warm_count=100,first_issue='3488000',last_issue='3511343'))
                c.execute("UPDATE lab_batches SET status='completed',started_at=?,processed_issues=23316,cursor_json=? WHERE id=?",(NOW,json.dumps(cursor),bid))
                for eid, in c.execute('SELECT id FROM lab_experiments WHERE batch_id=?',(bid,)).fetchall():
                    stats=empty_stats();stats.update(settled_count=400,stake_cents=40000,net_cents=1799,hit_count=200,miss_count=200)
                    c.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?',(json.dumps(stats),eid))
                    c.execute('INSERT INTO lab_daily(experiment_id,stat_date,settled_count,stake_cents,net_cents,hit_count,miss_count) VALUES(?,?,200,20000,599,100,100)',(eid,'2026-10-02'))
                    c.execute('INSERT INTO lab_daily(experiment_id,stat_date,settled_count,stake_cents,net_cents,hit_count,miss_count) VALUES(?,?,200,20000,1200,100,100)',(eid,'2026-11-02'))

    def test_explicit_batch_query_is_read_only_source_independent_and_legacy_unchanged(self):
        batch=self.create_fixture();path=PREFIX+'/batches/'+str(batch['id'])
        before=self.request(path)['json'];self.assertNotIn('observation',before['experiments'][0])
        with sqlite3.connect(self.store.db_path) as c:dump_before='\n'.join(c.iterdump())
        self.source.available=False
        result=self.request(path,query=QUERY)
        self.assertEqual(result['status_code'],200,result)
        self.assertEqual(result['headers']['Cache-Control'],'no-store')
        observed=result['json']['experiments'][0]['observation']
        self.assertEqual(observed['version'],1)
        self.assertEqual(observed['months'][0]['net_cents'],599)
        self.assertEqual(observed['months'][0]['stake_cents'],20000)
        self.assertEqual(observed['quality']['blockers'],[])
        self.assertTrue(observed['window']['period_reached'])
        without=result['json']
        for row in without['experiments']:row.pop('observation')
        self.assertEqual(without,before)
        with sqlite3.connect(self.store.db_path) as c:self.assertEqual('\n'.join(c.iterdump()),dump_before)
        self.assertEqual(self.request(path)['json'],before)

    def test_query_pair_validation_and_route_whitelist(self):
        batch=self.create_fixture();path=PREFIX+'/batches/'+str(batch['id'])
        for query in ('observation_months=2','observation_partial=true','observation_months=02&observation_partial=true',
                      'observation_months=0&observation_partial=true','observation_months=13&observation_partial=true',
                      'observation_months=2&observation_partial=1','observation_months=2&observation_partial=True',
                      'observation_months=2&observation_partial=','observation_months=2&observation_partial=true&observation_months=3',
                      'observation_months=2&observation_partial=true&other=1'):
            with self.subTest(query=query):
                result=self.request(path,query=query);self.assertEqual(result['status_code'],400,result)
                self.assertEqual(result['json']['code'],'invalid_input')
        eid=self.store.list_experiments(1,batch['id'])[0]['id']
        for route in (PREFIX,PREFIX+'/options',PREFIX+'/studies',path+'/export.csv',path+'/experiments/'+str(eid),path+'/comparison-preview'):
            self.assertEqual(self.request(route,query=QUERY)['status_code'],400,route)
        self.assertEqual(self.request(path+'/state',{'state':'running'},query=QUERY)['status_code'],400)

    def test_owner_and_admin_permissions_apply_to_observation_reads(self):
        batch=self.create_fixture();path=PREFIX+'/batches/'+str(batch['id'])
        for uid,code in ((None,401),(self.regular['id'],403),(self.other['id'],404)):
            self.assertEqual(self.request(path,uid=uid,query=QUERY)['status_code'],code)
        self.assertEqual(self.request(PREFIX+'/batches/99999',query=QUERY)['status_code'],404)

    def test_study_rows_share_window_and_keep_frozen_risk_results(self):
        batch=self.create_fixture()
        result=self.service.create_study(1,dict(source_batch_id=batch['id'],name='risk observation',request_id='observations-study'),now=NOW)
        study=result['study'];ids=[x['batch_id'] for x in study['children']];self.seed(ids)
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("UPDATE lab_studies SET status='completed',cutoff_issue='3511343',cutoff_at=? WHERE id=?",(NOW,study['id']))
        path=PREFIX+'/studies/'+str(study['id']);before=self.request(path)['json']
        result=self.request(path,query=QUERY)
        self.assertEqual(result['status_code'],200,result)
        self.assertEqual(len(result['json']['rows']),10)
        windows={(r['observation']['window']['start_date'],r['observation']['window']['end_date']) for r in result['json']['rows']}
        self.assertEqual(windows,{('2026-10-01','2026-11-27')})
        for row in result['json']['rows']:
            self.assertEqual(row['observation']['quality']['blockers'],[])
            row.pop('observation')
        self.assertEqual(result['json'],before)
        self.assertEqual(self.request(path,uid=self.other['id'],query=QUERY)['status_code'],404)

    def test_deleting_study_with_removed_children_remains_readable(self):
        batch=self.create_fixture()
        study=self.service.create_study(1,dict(source_batch_id=batch['id'],name='deleting study',request_id='observations-delete'),now=NOW)['study']
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("UPDATE lab_studies SET status='deleting' WHERE id=?",(study['id'],))
            c.execute('DELETE FROM lab_study_batches WHERE study_id=?',(study['id'],))
        path=PREFIX+'/studies/'+str(study['id'])
        for query in ('',QUERY):
            result=self.request(path,query=query)
            self.assertEqual(result['status_code'],200,result)
            self.assertEqual(result['json']['rows'],[])
            self.assertEqual(result['json']['study']['status'],'deleting')
        self.assertEqual(self.request(path,uid=self.other['id'],query=QUERY)['status_code'],404)

    def test_observation_queries_batch_daily_and_do_not_load_full_reports(self):
        batch=self.create_fixture(50);path=PREFIX+'/batches/'+str(batch['id']);queries=[]
        original=self.store._open_connection
        def traced():
            c=original();c.set_trace_callback(queries.append);return c
        with patch.object(self.store,'_open_connection',traced),patch.object(self.store,'_monthly_report',side_effect=AssertionError('Do not load per-experiment full reports')):
            result=self.request(path,query=QUERY)
        self.assertEqual(result['status_code'],200,result)
        daily=[q for q in queries if 'FROM LAB_DAILY' in q.upper()]
        self.assertLessEqual(len(daily),3,daily)
        self.assertNotIn('equity',result['json']['experiments'][0]['observation'])


if __name__=='__main__':unittest.main()
