"""Atomic multi-owner changes run against real temporary WSGI and SQLite."""
import sqlite3,unittest
from tests import test_rule_lab_api as base
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_series import completed_baseline
PREFIX=base.PREFIX;NOW='2026-11-28T04:00:00Z'
class BulkManagementTests(unittest.TestCase):
    setUp=base.RuleLabApiTests.setUp
    request=base.RuleLabApiTests.request
    def call(self,path,body=None,code=200):
        response=self.request(PREFIX+path,body);self.assertEqual(response['status_code'],code,response);return response['json']
    def batch(self):
        return self.call('/batches',dict(name='批量原始',mode='backtest',predictor_id=7,metric='odd_even',start_date='2026-11-01',end_date='2026-11-26',risk_policy=policy()),201)['batch']
    def payload(self,action,owners):return dict(action=action,owners=owners,names=None,end_date=None)
    def submit(self,payload,token):
        preview=self.call('/management/bulk-preview',payload);self.assertTrue(preview['available'],preview)
        body=dict(payload,request_id=token,preview_revision=preview['preview_revision'],confirm_text=preview['confirmation_text'])
        result=self.call('/management/bulk',body,202 if payload['action'] in ('delete','append') else 200)
        return result,body
    def test_group_and_source_deletion_checks_dependency_closure_then_keeps_favorites(self):
        source=self.batch();eid=self.store.list_experiments(1,source['id'])[0]['id']
        self.call('/favorites',dict(request_id='bulk-delete-favorite',batch_id=source['id'],experiment_id=eid,name='保留候选',notes=''),201)
        study=self.call('/studies',dict(source_batch_id=source['id'],name='依赖组',request_id='bulk-dependency-study'),201)['study']
        owner=dict(owner_kind='batch',owner_id=source['id'])
        blocked=self.call('/management/bulk-preview',self.payload('delete',[owner]));self.assertFalse(blocked['available'])
        selected=[owner,dict(owner_kind='study',owner_id=study['id'])]
        result,body=self.submit(self.payload('delete',selected),'bulk-delete-closure')
        self.assertEqual(result['affected_owners'],2);self.assertEqual(result['jobs'][0]['owner_kind'],'study')
        for _ in range(80):self.store.management_maintenance(now=NOW,limit=500)
        self.assertFalse(self.call('/management/bulk',body)['created'])
        self.assertEqual(self.call('/favorites')['total'],1)
        with sqlite3.connect(self.store.db_path) as c:self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_batches').fetchone()[0],0)
    def test_append_is_manual_atomic_and_keeps_existing_daily_settings(self):
        self.source.list_predictors=lambda:[dict(id=1,name='共享来源')]
        one=completed_baseline(self.store);two=completed_baseline(self.store)
        preview=self.store.continuation_preview(1,'batch',one['id'],now=NOW)
        self.store.set_continuation(1,'batch',one['id'],dict(request_id='already-scheduled',expected_revision=preview['expected_revision'],capture_enabled=True,compute_enabled=True,daily_at='03:25',timezone='Asia/Shanghai'),now=NOW)
        owners=[dict(owner_kind='batch',owner_id=b['id']) for b in (one,two)]
        payload=dict(action='append',owners=owners,names=None,end_date='2026-11-27')
        before={b['id']:[e['id'] for e in self.store.list_experiments(1,b['id'])] for b in (one,two)}
        result,body=self.submit(payload,'batch-manual-append');self.assertEqual(len(result['items']),2)
        with sqlite3.connect(self.store.db_path) as c:
            rows=c.execute('SELECT owner_id,compute_enabled,daily_at FROM lab_series ORDER BY owner_id').fetchall()
        self.assertEqual(rows[0],(one['id'],1,'03:25'));self.assertEqual(rows[1][1],0)
        self.assertFalse(self.call('/management/bulk',body)['created'])
        for bid,eids in before.items():self.assertEqual([e['id'] for e in self.store.list_experiments(1,bid)],eids)
        blocked=self.call('/management/bulk-preview',payload);self.assertFalse(blocked['available'])
    def test_confirm_phrase_and_duplicate_selection_never_delete_partially(self):
        a=self.batch();b=self.batch();owners=[dict(owner_kind='batch',owner_id=x['id']) for x in (a,b)]
        payload=self.payload('delete',owners);preview=self.call('/management/bulk-preview',payload)
        body=dict(payload,request_id='bulk-delete-confirm',preview_revision=preview['preview_revision'],confirm_text='wrong')
        self.call('/management/bulk',body,409)
        self.assertEqual([r['status'] for r in self.store.list_batches(1)],['paused','paused'])
if __name__=='__main__':unittest.main()
