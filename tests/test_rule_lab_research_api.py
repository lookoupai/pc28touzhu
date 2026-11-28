"""Real WSGI and temporary SQLite for saved research candidates."""
import json,sqlite3,unittest
from tests import test_rule_lab_api as base
from pc28touzhu.domain.rule_lab import build_experiments,normalize_batch_payload

PREFIX=base.PREFIX
NOW='2026-11-28T04:00:00Z'
class ResearchApiTests(unittest.TestCase):
    setUp=base.RuleLabApiTests.setUp
    request=base.RuleLabApiTests.request
    def call(self,path,payload=None,code=200,**kw):
        result=self.request(PREFIX+path,payload,**kw);self.assertEqual(result['status_code'],code,result);return result['json']
    def batch(self):
        config=normalize_batch_payload(dict(name='收藏原始批次',mode='backtest',predictor_id=7,metric='odd_even',start_date='2026-10-01',end_date='2026-10-31'),now=NOW)
        batch=self.store.create_batch(1,config,'来源',build_experiments('odd_even')[:2],now=NOW)
        return batch,self.store.list_experiments(1,batch['id'])[0]
    def favorite(self):
        batch,exp=self.batch();payload=dict(request_id='research-save-001',batch_id=batch['id'],experiment_id=exp['id'],name='我的候选',notes='保留历史赔率')
        return batch,exp,payload,self.call('/favorites',payload,201)['favorite']
    def test_favorite_snapshot_survives_source_delete_and_retries(self):
        batch,exp,payload,fav=self.favorite()
        saved=fav['snapshot'];self.assertEqual(saved['condition'],exp['condition'])
        self.assertEqual(self.call('/favorites',payload)['favorite']['id'],fav['id'])
        preview=self.service.delete_preview(1,'batch',batch['id'],now=NOW)
        self.service.request_delete(1,'batch',batch['id'],dict(request_id='remove-research-source',expected_revision=preview['expected_revision'],confirm_name=batch['name']),now=NOW)
        for _ in range(20):self.store.management_maintenance(now=NOW,limit=500)
        item=self.call('/favorites')['items'][0]
        self.assertFalse(item['source']['available']);self.assertIsNone(item['current_result'])
        self.assertEqual(item['snapshot'],saved)
        self.assertEqual(self.call('/favorites',payload)['favorite']['id'],fav['id'])
        created=self.call('/favorites/'+str(fav['id'])+'/create',dict(request_id='saved-candidate-create',expected_revision=1,name='按快照重算',start_date='2026-11-01',end_date='2026-11-26'),201)
        self.assertEqual(created['generated_count'],1)
        frozen=self.store.list_experiments(1,created['batch']['id'])[0]
        self.assertEqual(frozen['condition'],exp['condition']);self.assertEqual(frozen['settlement_odds'],exp['settlement_odds']);self.assertEqual(frozen['settled_count'],0)
    def test_favorite_revision_ownership_paging_and_validation(self):
        _,_,_,fav=self.favorite();path='/favorites/'+str(fav['id'])
        edited=self.call(path+'/edit',dict(request_id='research-edit-001',expected_revision=1,name='改名',notes='说明'))['favorite']
        self.assertEqual(edited['revision'],2)
        self.call(path+'/edit',dict(request_id='research-edit-stale',expected_revision=1,name='失败',notes=''),409)
        self.call(path+'/delete',dict(request_id='research-other-delete',expected_revision=2),404,uid=self.other['id'])
        for uid,status in ((None,401),(self.regular['id'],403)):self.call('/favorites',code=status,uid=uid)
        self.call('/favorites',code=400,query='limit=01');self.call('/favorites',code=400,query='limit=5&limit=5')
        self.call(path+'/delete',dict(request_id='research-delete-001',expected_revision=2))
        self.assertEqual(self.call('/favorites')['items'],[])
        self.assertFalse(self.call(path+'/delete',dict(request_id='research-delete-001',expected_revision=2))['created'])

if __name__=='__main__':unittest.main()
