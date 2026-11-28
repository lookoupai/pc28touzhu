"""Persistent queue control, fairness and legacy-state coherence on temporary ledgers."""
import sqlite3
import unittest
from unittest.mock import patch
from tests import test_rule_lab_api as fixtures

PREFIX=fixtures.PREFIX
NOW=fixtures.NOW


class QueueTests(unittest.TestCase):
    setUp=fixtures.RuleLabApiTests.setUp
    request=fixtures.RuleLabApiTests.request

    def batch(self,name='队列实验'):
        result=self.request(PREFIX+'/batches',dict(name=name,mode='backtest',predictor_id=7,metric='big_small',start_date='2026-11-01',end_date='2026-11-26'))
        self.assertEqual(result['status_code'],201,result)
        return result['json']['batch']

    def enqueue(self,bid,token):
        revision=self.request(PREFIX+'/batches/'+str(bid)+'/management')['json']['expected_revision']
        result=self.request(PREFIX+'/queue/enqueue',dict(request_id=token,owner_kind='batch',owner_id=bid,expected_revision=revision))
        self.assertEqual(result['status_code'],200,result)
        return result['json']['item']

    def small_batch(self,name='小批次',risk=None):
        from pc28touzhu.domain.rule_lab import build_experiments,normalize_batch_payload
        config=normalize_batch_payload(dict(name=name,mode='backtest',predictor_id=7,metric='big_small',
            start_date='2026-11-01',end_date='2026-11-25',risk_policy=risk),now=NOW)
        return self.store.create_batch(1,config,'测试',build_experiments('big_small')[:2],now=NOW)

    def test_saved_draft_and_persistent_queue_are_distinct(self):
        batch=self.batch()
        queue=self.request(PREFIX+'/queue')
        self.assertEqual(queue['status_code'],200,queue)
        self.assertEqual([d['owner_id'] for d in queue['json']['drafts']],[batch['id']])
        item=self.enqueue(batch['id'],'queue-enqueue-001')
        self.assertEqual(item['state'],'queued')
        self.assertEqual(self.service.batch_detail(1,batch['id'])['batch']['display_status'],'queued')
        self.store.initialize()
        self.assertEqual(self.request(PREFIX+'/queue')['json']['items'][0]['id'],item['id'])

    def test_global_capacity_fairness_and_legacy_pause(self):
        first=self.batch('第一组');middle=self.batch('中间组');second=self.batch('第二组')
        self.enqueue(first['id'],'queue-first-001');self.enqueue(middle['id'],'queue-middle-001');self.enqueue(second['id'],'queue-second-001')
        self.store.advance_studies(now=NOW)
        self.assertEqual(self.store.get_batch(1,first['id'])['status'],'running')
        self.assertEqual(self.store.get_batch(1,second['id'])['display_status'],'queued')
        self.assertLessEqual(self.store.capacity()['running'],100)
        self.request(PREFIX+'/batches/'+str(second['id'])+'/state',dict(state='paused'))
        self.request(PREFIX+'/batches/'+str(first['id'])+'/state',dict(state='paused'))
        self.store.advance_studies(now=NOW)
        self.assertEqual(self.store.get_batch(1,second['id'])['status'],'paused')
        self.assertEqual(self.store.get_batch(1,second['id'])['queue_state'],'paused')

    def test_remove_then_explicit_enqueue_preserves_cursor(self):
        batch=self.batch();q=self.enqueue(batch['id'],'queue-remove-001')
        self.store.advance_studies(now=NOW)
        q=self.request(PREFIX+'/queue')['json']['items'][0]
        with sqlite3.connect(self.store.db_path) as c:
            c.execute('UPDATE lab_batches SET processed_issues=7,cursor_json=? WHERE id=?',('{"last_issue_no":"100"}',batch['id']))
        response=self.request(PREFIX+'/queue/'+str(q['id'])+'/state',dict(request_id='queue-remove-action',expected_revision=q['expected_revision'],action='remove'))
        self.assertEqual(response['status_code'],200,response)
        removed=response['json']['item']
        result=self.request(PREFIX+'/queue/enqueue',dict(request_id='queue-explicit-readd',owner_kind='batch',owner_id=batch['id'],expected_revision=removed['expected_revision']))
        self.assertEqual(result['status_code'],200,result)
        self.store.advance_studies(now=NOW)
        self.assertEqual(self.store.get_batch(1,batch['id'])['processed_issues'],7)
        self.assertEqual(self.store.get_batch(1,batch['id'])['status'],'running')

    def test_stale_revision_and_risk_edit_exit_queue(self):
        from tests.test_rule_lab_risk import policy
        batch=self.batch();q=self.enqueue(batch['id'],'queue-stale-001')
        response=self.request(PREFIX+'/queue/'+str(q['id'])+'/state',dict(request_id='queue-pause-001',expected_revision=q['expected_revision'],action='pause'))
        self.assertEqual(response['status_code'],200,response)
        stale=self.request(PREFIX+'/queue/'+str(q['id'])+'/state',dict(request_id='queue-stale-resume',expected_revision=q['expected_revision'],action='resume'))
        self.assertEqual(stale['status_code'],409,stale)
        edited=self.request(PREFIX+'/batches/'+str(batch['id'])+'/risk-policy',dict(risk_policy=policy()))
        self.assertEqual(edited['status_code'],200,edited)
        self.store.advance_studies(now=NOW)
        self.assertEqual(self.store.get_batch(1,batch['id'])['queue_state'],'removed')
        self.assertEqual(self.store.capacity()['running'],0)

    def test_more_than_twenty_small_batches_admit_and_reconcile_without_starvation(self):
        ids=[]
        for index in range(25):
            batch=self.small_batch('小批次'+str(index));ids.append(batch['id'])
            self.enqueue(batch['id'],'queue-many-'+str(index))
        for _ in range(4):self.store.advance_studies(now=NOW)
        self.assertEqual(self.store.capacity()['running'],50)
        self.assertEqual(len(self.store.list_work_batches()),25)
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("UPDATE lab_batches SET status='completed' WHERE id=?",(ids[20],))
            c.execute("UPDATE lab_batches SET status='error',error='来源不可用' WHERE id=?",(ids[21],))
        for _ in range(3):self.store.advance_studies(now=NOW)
        items={item['owner_id']:item for item in self.request(PREFIX+'/queue',query='state=all')['json']['items']}
        self.assertEqual(items[ids[20]]['state'],'completed')
        self.assertEqual(items[ids[21]]['state'],'error')
        self.assertEqual(items[ids[21]]['error'],'来源不可用')

    def test_study_returns_to_tail_after_child_and_respects_error_retry(self):
        from tests.test_rule_lab_risk import policy
        source=self.small_batch('比较来源',policy())
        study=self.store.create_study(1,source['id'],'轮转对比','queue-fair-study',now=NOW)['study']
        first=study['children'][0]['batch_id'];second=study['children'][1]['batch_id']
        self.store.set_study_state(1,study['id'],'running',now=NOW)
        later=self.small_batch('后续任务');self.enqueue(later['id'],'queue-after-study')
        cutoff=dict(issue_no='3000',at=NOW)
        with patch('pc28touzhu.services.rule_lab_store.MAX_RUNNING_EXPERIMENTS',2):
            self.store.advance_studies(now=NOW,cutoff=cutoff)
            self.assertEqual(self.store.get_batch(1,first)['status'],'running')
            self.assertEqual(self.store.get_batch(1,later['id'])['display_status'],'queued')
            with sqlite3.connect(self.store.db_path) as c:c.execute("UPDATE lab_batches SET status='completed' WHERE id=?",(first,))
            self.store.advance_studies(now=NOW,cutoff=cutoff)
            self.assertEqual(self.store.get_batch(1,later['id'])['status'],'running')
            self.assertEqual(self.store.get_batch(1,second)['status'],'paused')
            with sqlite3.connect(self.store.db_path) as c:c.execute("UPDATE lab_batches SET status='completed' WHERE id=?",(later['id'],))
            self.store.advance_studies(now=NOW,cutoff=cutoff)
            self.assertEqual(self.store.get_batch(1,second)['status'],'running')
            with sqlite3.connect(self.store.db_path) as c:c.execute("UPDATE lab_batches SET status='error',error='可修复' WHERE id=?",(second,))
            self.store.advance_studies(now=NOW,cutoff=cutoff)
            queue=next(q for q in self.request(PREFIX+'/queue')['json']['items'] if q['owner_kind']=='study')
            self.assertEqual(queue['state'],'error')
            self.assertIn('retry',queue['actions'])
            result=self.request(PREFIX+'/queue/'+str(queue['id'])+'/state',dict(request_id='queue-study-retry',
                expected_revision=queue['expected_revision'],action='retry'))
            self.assertEqual(result['status_code'],200,result)
            self.store.advance_studies(now=NOW,cutoff=cutoff)
            self.assertEqual(self.store.get_batch(1,second)['status'],'running')


if __name__=='__main__':unittest.main()
