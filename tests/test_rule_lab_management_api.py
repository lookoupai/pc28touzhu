"""管理接口使用真实WSGI和临时SQLite；不访问生产状态。"""
from __future__ import annotations

import copy
import json
import sqlite3
import unittest
from unittest.mock import patch

from tests import test_rule_lab_continuous_api as fixture

PREFIX = fixture.PREFIX
NOW = fixture.NOW


class ManagementApiTests(unittest.TestCase):
    setUp = fixture.ContinuousApiTests.setUp
    request = fixture.ContinuousApiTests.request
    contents = fixture.ContinuousApiTests.contents

    def preview(self, path=None):
        result = self.request((path or self.path)+'/management')
        self.assertEqual(result['status_code'], 200, result)
        return result['json']

    def config(self, **changes):
        config = dict(name='未启动编辑样本',mode='backtest',predictor_id=1,metric='big_small',
            template_pack='basic_v2',stake_amount=1,start_date='2026-11-25',end_date='2026-11-25',
            risk_policy=fixture.api_fixture.policy())
        config.update(changes)
        return config

    def pristine(self):
        result=self.request(PREFIX+'/batches',self.config())
        self.assertEqual(result['status_code'],201,result)
        return result['json']['batch'], PREFIX+'/batches/'+str(result['json']['batch']['id'])

    def payload(self, action, preview, **changes):
        value=dict(request_id='management-api-'+action,expected_revision=preview['expected_revision'])
        value.update(changes)
        return value

    def drain(self, job_id):
        for _ in range(100):
            self.store.management_maintenance(now=NOW,limit=500,max_pages=256)
            value=self.request(PREFIX+'/management-jobs/'+str(job_id))
            self.assertEqual(value['status_code'],200,value)
            if value['json']['job']['status']=='completed':return value['json']['job']
        self.fail('管理任务未完成: '+str(value))

    def test_management_reads_are_owned_read_only_and_storage_is_truthful(self):
        before=self.contents()
        value=self.preview();self.assertFalse(value['editable']);self.assertTrue(value['can_fork'])
        self.assertIsInstance(value['expected_revision'],str)
        deletion=self.request(self.path+'/delete-preview');self.assertEqual(deletion['status_code'],200,deletion)
        storage=self.request(PREFIX+'/storage');self.assertEqual(storage['status_code'],200,storage)
        for key in ('database_bytes','wal_bytes','shm_bytes','used_bytes','reusable_bytes','reclaimable_bytes','incremental_ready'):
            self.assertIn(key,storage['json'])
        self.assertEqual(storage['headers']['Cache-Control'],'no-store')
        self.assertEqual(self.contents(),before)

    def test_auth_owner_methods_and_query_boundaries(self):
        for suffix,payload in (('management',None),('delete-preview',None),('rename',{}),('edit',{}),('fork',{}),('delete',{})):
            for uid,status in ((None,401),(self.regular['id'],403),(self.other['id'],404)):
                response=self.request(self.path+'/'+suffix,payload,uid=uid)
                self.assertEqual(response['status_code'],status,(suffix,uid,response))
        for route in (self.path+'/management',self.path+'/delete-preview',PREFIX+'/storage'):
            self.assertEqual(self.request(route,query='limit=1')['status_code'],400)
        self.assertEqual(self.request(self.path+'/delete')['status_code'],405)
        self.assertEqual(self.request(PREFIX+'/storage',{})['status_code'],405)
        self.assertEqual(self.request(PREFIX+'/storage',uid=self.regular['id'])['status_code'],403)

    def test_rename_preserves_history_and_retries_original_request(self):
        before=self.store.list_experiments(1,self.bid)
        preview=self.preview();body=self.payload('rename',preview,name='改名 <img onerror=alert(1)>')
        first=self.request(self.path+'/rename',body);self.assertEqual(first['status_code'],200,first)
        self.assertEqual(self.store.get_batch(1,self.bid)['name'],body['name'])
        second=self.request(self.path+'/rename',body);self.assertEqual(second['status_code'],200,second)
        self.assertFalse(second['json']['created'])
        self.assertEqual(self.store.list_experiments(1,self.bid),before)
        self.assertEqual(self.request(self.path+'/rename',{**body,'name':'different'})['status_code'],409)

    def test_pristine_edit_rebuilds_configuration_stays_paused_and_exits_queue(self):
        batch,path=self.pristine();preview=self.preview(path)
        self.assertTrue(preview['editable'])
        body=self.payload('edit',preview,config=self.config(metric='combo',stake_amount=2))
        result=self.request(path+'/edit',body);self.assertEqual(result['status_code'],200,result)
        changed=result['json']['batch'];self.assertEqual(changed['id'],batch['id'])
        self.assertEqual(changed['metric'],'combo');self.assertEqual(changed['experiment_count'],92)
        self.assertEqual(changed['stake_amount'],2);self.assertEqual(changed['status'],'paused')
        self.assertTrue(result['json']['queue_removed'])
        self.assertTrue(all(item['settled_count']==0 for item in self.store.list_experiments(1,batch['id'])))
        self.assertFalse(self.request(path+'/edit',body)['json']['created'])

    def test_started_batch_cannot_be_edited_and_fork_keeps_original(self):
        before=self.contents();preview=self.preview()
        edit=self.request(self.path+'/edit',self.payload('edit-locked',preview,config=self.config()))
        self.assertEqual(edit['status_code'],409,edit);self.assertEqual(self.contents(),before)
        body=self.payload('fork',preview,config=self.config(name='新策略',stake_amount=3))
        result=self.request(self.path+'/fork',body);self.assertEqual(result['status_code'],201,result)
        self.assertNotEqual(result['json']['batch']['id'],self.bid)
        self.assertEqual(result['json']['batch']['status'],'paused')
        self.assertEqual(self.store.get_batch(1,self.bid)['name'],'续算基线')
        self.assertEqual(self.request(self.path+'/fork',body)['status_code'],200)
        self.assertEqual(len(self.store.list_batches(1)),2)

    def test_delete_runs_as_job_and_receipt_survives_parent_removal(self):
        spare,path=self.pristine()
        response=self.request(self.path+'/delete-preview');self.assertEqual(response['status_code'],200,response)
        preview=response['json']
        body=self.payload('delete',preview,confirm_name=preview['name'])
        result=self.request(self.path+'/delete',body);self.assertEqual(result['status_code'],202,result)
        job=result['json']['job'];self.assertIn(job['status'],('queued','running'))
        finished=self.drain(job['id']);self.assertEqual(finished['status'],'completed')
        self.assertEqual(self.request(self.path)['status_code'],404)
        self.assertEqual(self.request(path)['status_code'],200)
        retry=self.request(self.path+'/delete',body);self.assertEqual(retry['status_code'],200,retry)
        self.assertEqual(retry['json']['job']['id'],job['id'])
        self.assertEqual(self.request(PREFIX+'/management-jobs/'+str(job['id']),uid=self.other['id'])['status_code'],404)
        new,path2=self.pristine();self.assertGreater(new['id'],max(self.bid,spare['id']))

    def test_validation_and_capacity_do_not_create_partial_mutations(self):
        preview=self.preview();before=self.contents()
        for body in ({},[],self.payload('bad-rename',preview,name=''),self.payload('bad-rename',preview,name='x'*81),
                     {**self.payload('bad-rename',preview,name='ok'),'extra':1},
                     {**self.payload('bad-rename',preview,name='ok'),'expected_revision':123}):
            self.assertEqual(self.request(self.path+'/rename',body)['status_code'],400)
        mismatch=self.request(self.path+'/delete',self.payload('bad-delete',preview,confirm_name='wrong'))
        self.assertEqual(mismatch['status_code'],409,mismatch)
        self.assertEqual(mismatch['json']['code'],'confirmation_mismatch')
        oversized=self.request(self.path+'/rename',self.payload('oversized',preview,name='x'*17000))
        self.assertEqual(oversized['status_code'],400)
        with patch('pc28touzhu.services.rule_lab_store.MAX_TOTAL_EXPERIMENTS',2):
            failed=self.request(self.path+'/fork',self.payload('full-fork',preview,config=self.config()))
        self.assertEqual(failed['status_code'],409,failed)
        self.assertEqual(self.contents(),before)

    def test_study_rename_and_explicit_dependency_delete(self):
        result=self.store.create_study(1,self.bid,'五方案管理样本','management-study-create',now=NOW)
        sid=result['study']['id'];path=PREFIX+'/studies/'+str(sid)
        child=result['study']['children'][0]['batch_id']
        child_preview=self.request(PREFIX+'/batches/'+str(child)+'/delete-preview')
        self.assertEqual(child_preview['status_code'],200,child_preview)
        self.assertFalse(child_preview['json']['eligible'])
        self.assertIn('group_action_required',{item['code'] for item in child_preview['json']['blockers']})
        dependent=self.request(self.path+'/delete-preview');self.assertEqual(dependent['status_code'],200,dependent)
        self.assertFalse(dependent['json']['eligible'])
        self.assertIn('dependent_studies',{row['code'] for row in dependent['json']['blockers']})
        preview=self.preview(path);body=self.payload('study-rename',preview,name='比较组新名称')
        self.assertEqual(self.request(path+'/rename',body)['status_code'],200)
        preview=self.request(path+'/delete-preview')['json']
        deleted=self.request(path+'/delete',self.payload('study-delete',preview,confirm_name=preview['name']))
        self.assertEqual(deleted['status_code'],202,deleted)
        self.drain(deleted['json']['job']['id'])
        self.assertEqual(self.request(path)['status_code'],404)
        self.assertEqual(self.request(self.path)['status_code'],200)

    def test_reclaim_is_a_durable_job_and_never_runs_inline_full_vacuum(self):
        result=self.request(PREFIX+'/storage/reclaim',dict(request_id='management-reclaim'))
        self.assertEqual(result['status_code'],202,result)
        again=self.request(PREFIX+'/storage/reclaim',dict(request_id='management-reclaim'))
        self.assertEqual(again['status_code'],200,again)
        self.assertEqual(result['json']['job']['id'],again['json']['job']['id'])
        self.assertEqual(self.request(PREFIX+'/storage/reclaim',{'request_id':'management-reclaim','pages':0})['status_code'],400)


if __name__=='__main__':unittest.main()
