"""Result retention behavior against disposable SQLite ledgers."""
import importlib
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabStore

NOW='2026-11-27T12:00:00Z'


class ResultRetentionTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory()
        self.store=RuleLabStore(Path(self.tmp.name)/'lab.db');self.store.initialize()
        cfg=normalize_batch_payload(dict(name='retention',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-26',end_date='2026-11-26'),now=NOW)
        self.batch=self.store.create_batch(1,cfg,'source',build_experiments('big_small')[:1],now=NOW)
        self.eid=self.store.list_experiments(1,self.batch['id'])[0]['id']

    def tearDown(self):self.tmp.cleanup()

    def maintain(self,limit=500):
        # Run the old public cleanup path until the new policy module exists,
        # so RED demonstrates retained data rather than an import failure.
        path=Path(__file__).resolve().parents[1]/'src/pc28touzhu/services/rule_lab_retention.py'
        if not path.exists():return self.store.prune(now=NOW,limit=limit)
        module=importlib.import_module('pc28touzhu.services.rule_lab_retention')
        return module.maintain(self.store,now=NOW,limit=limit)

    def legacy_decision(self,issue,status='settled'):
        with self.store._connect(write=True) as c:
            c.execute('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,stake_cents,
                decided_at,decision_mode,condition_value_json,status,settled_at,finalized_at,result_type,net_cents)
                VALUES(?,?,?,'大',100,?,'backtest','null',?,?,?,?,?)''',
                (self.eid,str(issue),issue,NOW,status,NOW if status!='pending' else None,
                 NOW if status!='pending' else None,'hit' if status!='pending' else None,100 if status!='pending' else None))

    def count(self,table,where='1=1'):
        with self.store._connect() as c:return c.execute('SELECT count(*) FROM '+table+' WHERE '+where).fetchone()[0]

    def test_recent_legacy_terminals_are_removed_without_changing_results(self):
        for issue in range(1,8):self.legacy_decision(issue)
        self.legacy_decision(8,'pending')
        with self.store._connect() as c:before=tuple(c.execute('SELECT stats_json,last_decided_issue,last_settled_issue FROM lab_experiments').fetchone())
        for _ in range(80):self.maintain(limit=3)
        self.assertEqual(self.count('lab_decisions',"status!='pending'"),0)
        self.assertEqual(self.count('lab_decisions',"status='pending'"),1)
        with self.store._connect() as c:self.assertEqual(tuple(c.execute('SELECT stats_json,last_decided_issue,last_settled_issue FROM lab_experiments').fetchone()),before)

    def test_completed_batch_warm_and_isolated_pending_do_not_pin_intervening_history(self):
        draws=[];predictions=[]
        for issue in range(1,501):
            draws.append(dict(issue_no=str(issue),source_id=issue,open_at=NOW,source_created_at=NOW,
                observed_at=NOW,triplet=[3,6,8],input_hash='d'+str(issue)))
            predictions.append(dict(predictor_id=1,issue_no=str(issue),source_id=issue,created_at=NOW,
                settled_at=NOW,observed_at=NOW,status='settled',big_small='大',odd_even='单',combo='大单',input_hash='p'+str(issue)))
        with self.store._connect(write=True) as c:
            self.store._cache(c,draws,predictions)
            c.execute("UPDATE lab_batches SET status='completed',started_at=?,processed_issues=500,cursor_json=? WHERE id=?",
                (NOW,json.dumps(dict(last_issue_no='500',warm_before_issue='1',replay_complete=True,
                 retention_v1=dict(first_open_at=NOW,processed_open_at=NOW,settled_available_at=NOW,frontier_verified=True))),self.batch['id']))
        self.legacy_decision(12,'pending')
        for _ in range(100):self.maintain(limit=100)
        with self.store._connect() as c:
            issues={r[0] for r in c.execute('SELECT issue_key FROM lab_draws')}
        self.assertEqual(issues,set(range(401,501))|{12})

    def test_cleanup_pages_bound_scans_and_deletions(self):
        for issue in range(1,26):self.legacy_decision(issue)
        for _ in range(80):
            result=self.maintain(limit=4)
            self.assertLessEqual(result['processed_rows'] if 'processed_rows' in result else sum(result.values()),4)
            self.assertLessEqual(result.get('scanned_rows',0),4)
        self.assertEqual(self.count('lab_decisions'),0)

    def test_frontier_ignores_unconsumed_future_page(self):
        path=Path(__file__).resolve().parents[1]/'src/pc28touzhu/services/rule_lab_retention.py'
        if not path.exists():self.fail('processed input frontiers are not persisted')
        module=importlib.import_module('pc28touzhu.services.rule_lab_retention')
        with self.store._connect(write=True) as c:
            row=c.execute('SELECT * FROM lab_batches WHERE id=?',(self.batch['id'],)).fetchone()
            cursor=module.update_frontier(c,row,dict(last_issue_no='10',warm_before_issue='10'),draws=[
                dict(issue_no='10',open_at='2026-11-26T01:00:00Z',source_created_at='2026-11-26T01:00:20Z'),
                dict(issue_no='99',open_at='2026-12-01T01:00:00Z',source_created_at='2026-12-01T01:00:20Z')])
        self.assertEqual(cursor['retention_v1']['processed_open_at'],'2026-11-26T01:00:00Z')
        self.assertEqual(cursor['retention_v1']['first_open_at'],'2026-11-26T01:00:00Z')
        self.assertIsNone(cursor['retention_v1'].get('settled_available_at'))

    def test_migration_keeps_late_visibility_of_an_earlier_consumed_draw(self):
        with self.store._connect(write=True) as c:
            self.store._cache(c,[dict(issue_no=str(issue),source_id=issue,open_at='2026-11-26T01:00:00Z',
                source_created_at=visible,observed_at=NOW,triplet=[3,6,8],input_hash='d'+str(issue))
                for issue,visible in ((1,'2026-11-26T03:00:00Z'),(2,'2026-11-26T02:00:00Z'))],[])
            c.execute('UPDATE lab_experiments SET last_settled_issue=2 WHERE id=?',(self.eid,))
            c.execute("UPDATE lab_batches SET status='completed',cursor_json=? WHERE id=?",
                (json.dumps(dict(last_issue_no='2',warm_before_issue='1',replay_complete=True)),self.batch['id']))
        self.maintain()
        with self.store._connect() as c:cursor=json.loads(c.execute('SELECT cursor_json FROM lab_batches').fetchone()[0])
        self.assertEqual(cursor['retention_v1']['settled_available_at'],'2026-11-26T03:00:00Z')

    def test_queued_dates_protect_archive_predictions_with_null_open_time(self):
        from pc28touzhu.services.rule_lab_archive import ensure_stream,_save_page
        with self.store._connect(write=True) as c:
            stream=ensure_stream(c,1,0,NOW)
            draws=[];predictions=[]
            for issue,at in ((10,'2026-11-24T01:00:00Z'),(1000,'2026-11-26T01:00:00Z'),(1100,'2026-11-28T01:00:00Z')):
                draws.append(dict(issue_no=str(issue),open_at=at,source_created_at=at,observed_at=NOW,triplet=[3,6,8],input_hash='d'+str(issue)))
                predictions.append(dict(predictor_id=1,issue_no=str(issue),created_at=at,settled_at=at,observed_at=NOW,
                    status='settled',big_small='大',odd_even='单',combo='大单',input_hash='p'+str(issue)))
            _save_page(c,stream,draws,predictions,[dict(issue_no=d['issue_no'],**{'class':'present'}) for d in draws],NOW)
        for _ in range(30):self.maintain(limit=20)
        with self.store._connect() as c:
            kept=[tuple(r) for r in c.execute('SELECT kind,issue_key FROM lab_input_versions ORDER BY kind,issue_key')]
        self.assertEqual(kept,[('draw',1000),('prediction',1000)])

    def test_housekeeping_reuses_one_reclaim_job_across_sweeps(self):
        self.legacy_decision(1)
        self.maintain()
        with self.store._connect(write=True) as c:
            c.execute("UPDATE lab_management_jobs SET status='completed'")
            c.execute("UPDATE lab_runtime SET value_json=? WHERE key='result_retention'",
                (json.dumps(dict(mode='results_only',version=1,phase='decisions',cursor=0,removed_rows=0)),))
        self.legacy_decision(2)
        self.maintain()
        self.assertEqual(self.count('lab_management_jobs'),1)

    def test_compacting_old_summary_preserves_incremental_money(self):
        from pc28touzhu.services.rule_lab_retention import _small_summary
        old=dict(batch_id=2,pending_count=1,experiments=[dict(incremental=dict(net_cents=123,settled_count=2),cumulative=dict(net_cents=900)),
            dict(incremental=dict(net_cents=-50,settled_count=1),risk_runtime=dict(active_round={'huge':1}))])
        result=_small_summary(old)
        self.assertEqual(result,dict(batch_id=2,pending_count=1,incremental=dict(net_cents=73,settled_count=3)))
        self.assertEqual(_small_summary(result),result)

    def test_storage_reports_result_policy_without_scanning_history(self):
        value=self.store.storage_status()
        self.assertEqual(value.get('retention',{}).get('mode'),'results_only')
        self.assertEqual(value['detail_retention_days'],0)
