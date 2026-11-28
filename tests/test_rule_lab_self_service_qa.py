"""独立自助实验室攻击：真实API/SQLite，临时来源隔离，绝不打开生产库。"""
from __future__ import annotations

import copy
import json
import sqlite3
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from tests import test_rule_lab_api as base
from tests.test_rule_lab_risk import policy


class SelfServiceIndependentQA(unittest.TestCase):
    setUp = base.RuleLabApiTests.setUp
    request = base.RuleLabApiTests.request

    def call(self, path, payload=None, *, code=200, **kwargs):
        result = self.request(base.PREFIX + path, payload, **kwargs)
        self.assertEqual(result['status_code'], code, result.get('json', result))
        return result['json']

    def condition_body(self, token='qa-template-create'):
        return dict(request_id=token,kind='condition',name='QA 单双精选',description='',
                    definition=dict(metric='odd_even',conditions=[dict(type='always'),
                        dict(type='miss_streak',operator='gte',threshold=7)]))

    def template(self):
        body=self.condition_body()
        return self.call('/templates',body,code=201)['template'],body

    def bulk_config(self,t):
        return dict(mode='backtest',sources=[7],start_date='2026-11-01',end_date='2026-11-26',stake_amount=1,
            metric_configs=[dict(metric='odd_even',condition_selection=dict(template_ref=t['ref'],expected_revision=t['revision']),
                risk_selection=dict(policy=policy()))],name_pattern='{source} · {metric} · {template}',names=[])

    def bulk(self,t,*,enqueue=False,token='qa-bulk-create'):
        cfg=self.bulk_config(t);preview=self.call('/batches/preview',cfg)
        payload=dict(cfg,request_id=token,preview_revision=preview['preview_revision'],enqueue=enqueue)
        return self.call('/batches/bulk',payload,code=201),payload

    def counts(self):
        with sqlite3.connect(self.store.db_path) as c:
            tables=['lab_batches','lab_experiments','lab_queue_items','lab_command_receipts','lab_series']
            return {t:c.execute('SELECT COUNT(*) FROM '+t).fetchone()[0] for t in tables}

    def delete_template(self,t,token='qa-template-delete'):
        return self.call('/templates/%s/delete'%t['id'],dict(request_id=token,expected_revision=t['revision']))

    def test_template_receipt_after_edit_delete_is_not_a_second_mutation(self):
        t,body=self.template()
        edit=dict(request_id='qa-template-edit',expected_revision=t['revision'],name=t['name'],description='changed',
                  definition=dict(metric='odd_even',conditions=[dict(type='always'),dict(type='miss_streak',operator='gte',threshold=9)]))
        edited=self.call('/templates/%s/edit'%t['id'],edit)['template']
        deletion=dict(request_id='qa-template-delete',expected_revision=edited['revision'])
        self.call('/templates/%s/delete'%t['id'],deletion)
        before=self.counts()
        retry=self.call('/templates',body)
        self.assertFalse(retry['created'])
        self.assertEqual(retry['template']['id'],t['id'])
        self.assertFalse(self.call('/templates/%s/edit'%t['id'],edit)['created'])
        self.assertFalse(self.call('/templates/%s/delete'%t['id'],deletion)['created'])
        self.call('/templates/%s'%t['id'],code=404)
        self.assertEqual(self.counts(),before)
        with sqlite3.connect(self.store.db_path) as c:
            revision,deleted_at,definition=c.execute('SELECT revision,deleted_at,definition_json FROM lab_templates WHERE id=?',(t['id'],)).fetchone()
        self.assertEqual(revision,edited['revision']+1)
        self.assertIsNotNone(deleted_at)
        self.assertEqual(json.loads(definition)['conditions'][1]['threshold'],9)
        self.call('/templates/%s/delete'%t['id'],deletion,uid=self.other['id'],code=404)
        self.call('/templates/%s/edit'%t['id'],edit,uid=self.other['id'],code=404)
        other=self.call('/templates',body,uid=self.other['id'],code=201)['template']
        self.assertNotEqual(other['id'],t['id'])
        self.assertEqual(other['revision'],1)
        self.call('/templates/%s'%other['id'],code=404)

    def test_template_receipt_ignores_json_object_member_order(self):
        t,body=self.template()
        def reordered(value):
            if isinstance(value,dict):return {k:reordered(v) for k,v in reversed(list(value.items()))}
            if isinstance(value,list):return [reordered(v) for v in value]
            return value
        retry=self.call('/templates',reordered(body))
        self.assertFalse(retry['created'])
        self.assertEqual(retry['template']['id'],t['id'])
        with sqlite3.connect(self.store.db_path) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_templates').fetchone()[0],1)

    def test_batch_snapshot_rejects_boolean_id_and_pending_deletion(self):
        t,_=self.template()
        source=self.call('/batches',dict(name='QA snapshot source',mode='live',predictor_id=7,metric='odd_even',
            condition_selection=dict(template_ref=t['ref'],expected_revision=t['revision'])),code=201)['batch']
        revision=self.call('/batches/%s/continuation-preview'%source['id'])['configuration_hash']
        for scenario in ('boolean_id','deleting_source'):
            if scenario=='deleting_source':
                preview=self.call('/batches/%s/delete-preview'%source['id'])
                self.call('/batches/%s/delete'%source['id'],dict(request_id='qa-snapshot-delete',
                    expected_revision=preview['expected_revision'],confirm_name=preview['name']),code=202)
            with self.subTest(scenario=scenario):
                with sqlite3.connect(self.store.db_path) as c:
                    before=c.execute('SELECT COUNT(*) FROM lab_batches').fetchone()[0]
                payload=dict(name='QA invalid snapshot',mode='live',predictor_id=7,metric='odd_even',
                    condition_selection=dict(template_ref=dict(kind='batch_snapshot',
                        batch_id=True if scenario=='boolean_id' else source['id']),expected_revision=revision))
                response=self.request(base.PREFIX+'/batches',payload)
                self.assertIn(response['status_code'],(400,404,409),response['json'])
                with sqlite3.connect(self.store.db_path) as c:
                    self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_batches').fetchone()[0],before)

    def test_bulk_lost_response_retry_survives_template_delete(self):
        t,_=self.template();created,payload=self.bulk(t)
        self.delete_template(t)
        before=self.counts()
        retry=self.call('/batches/bulk',payload)
        self.assertFalse(retry['created'])
        self.assertEqual([b['id'] for b in retry['batches']],[b['id'] for b in created['batches']])
        self.assertEqual(self.counts(),before)
        altered=copy.deepcopy(payload);altered['stake_amount']=2
        self.call('/batches/bulk',altered,code=409)
        self.assertEqual(self.counts(),before)

    def test_template_edit_invalidates_bulk_preview_without_partial_rows(self):
        t,_=self.template();cfg=self.bulk_config(t);preview=self.call('/batches/preview',cfg)
        self.call('/templates/%s/edit'%t['id'],dict(request_id='qa-stale-template-edit',expected_revision=t['revision'],
            name=t['name'],description='',definition=dict(metric='odd_even',conditions=[dict(type='always'),
                dict(type='miss_streak',operator='gte',threshold=8)])))
        before=self.counts()
        self.call('/batches/bulk',dict(cfg,request_id='qa-stale-bulk-submit',preview_revision=preview['preview_revision'],enqueue=True),code=409)
        self.assertEqual(self.counts(),before)

    def test_concurrent_bulk_submissions_recheck_capacity_in_write_transaction(self):
        t,_=self.template();cfg=self.bulk_config(t)
        with patch('pc28touzhu.services.rule_lab_store.MAX_TOTAL_EXPERIMENTS',6):
            preview=self.call('/batches/preview',cfg)
            self.assertEqual(preview['new_experiments'],4)
            barrier=threading.Barrier(2)
            def submit(index):
                payload=dict(cfg,request_id='qa-capacity-race-'+str(index),preview_revision=preview['preview_revision'],enqueue=False)
                barrier.wait(timeout=5)
                return self.request(base.PREFIX+'/batches/bulk',payload)
            with ThreadPoolExecutor(max_workers=2) as pool:responses=list(pool.map(submit,range(2)))
            self.assertEqual(sorted(r['status_code'] for r in responses),[201,409],responses)
            rejected=next(r for r in responses if r['status_code']==409)
            self.assertEqual(rejected['json']['code'],'capacity_exceeded')
            with sqlite3.connect(self.store.db_path) as c:
                self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_batches').fetchone()[0],1)
                self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_experiments').fetchone()[0],4)
                self.assertEqual(c.execute("SELECT COUNT(*) FROM lab_command_receipts WHERE action='bulk.create'").fetchone()[0],1)

    def test_concurrent_same_bulk_receipt_creates_one_batch(self):
        t,_=self.template();cfg=self.bulk_config(t);preview=self.call('/batches/preview',cfg)
        payload=dict(cfg,request_id='qa-same-command-race',preview_revision=preview['preview_revision'],enqueue=False)
        barrier=threading.Barrier(2)
        def submit(_):
            barrier.wait(timeout=5)
            return self.request(base.PREFIX+'/batches/bulk',payload)
        with ThreadPoolExecutor(max_workers=2) as pool:responses=list(pool.map(submit,range(2)))
        self.assertEqual(sorted(r['status_code'] for r in responses),[200,201],responses)
        self.assertEqual([b['id'] for b in responses[0]['json']['batches']],
                         [b['id'] for b in responses[1]['json']['batches']])
        with sqlite3.connect(self.store.db_path) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_batches').fetchone()[0],1)
            self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_experiments').fetchone()[0],4)

    def test_deleted_template_fork_retains_original_conditions_and_odds(self):
        t,_=self.template();created,_=self.bulk(t);bid=created['batches'][0]['id']
        with sqlite3.connect(self.store.db_path) as c:
            # 合法历史快照：模拟旧赔率目录创建的批次，而非从新目录重建。
            for eid,raw in c.execute('SELECT id,settlement_snapshot_json FROM lab_experiments WHERE batch_id=?',(bid,)).fetchall():
                snapshot=json.loads(raw)
                if snapshot['settlement_rule_id']=='pc28_netdisk_regular':snapshot['resolved_odds']['odd_even']=1.98
                c.execute('UPDATE lab_experiments SET settlement_snapshot_json=? WHERE id=?',(json.dumps(snapshot),eid))
        original=self.call('/batches/%s'%bid)
        before_hash=self.call('/batches/%s/continuation-preview'%bid)['configuration_hash']
        self.delete_template(t)
        self.assertEqual(self.call('/batches/%s/continuation-preview'%bid)['configuration_hash'],before_hash)
        management=self.call('/batches/%s/management'%bid)
        cfg=dict(management['config'],name='QA frozen fork')
        fork=self.call('/batches/%s/fork'%bid,dict(request_id='qa-frozen-fork',expected_revision=management['expected_revision'],config=cfg),code=201)['batch']
        with sqlite3.connect(self.store.db_path) as c:
            fields='condition_json,settlement_rule_id,settlement_snapshot_json'
            src=c.execute('SELECT '+fields+' FROM lab_experiments WHERE batch_id=? ORDER BY condition_key,settlement_rule_id',(bid,)).fetchall()
            dest=c.execute('SELECT '+fields+' FROM lab_experiments WHERE batch_id=? ORDER BY condition_key,settlement_rule_id',(fork['id'],)).fetchall()
        self.assertEqual([(json.loads(condition),rule,json.loads(snapshot)) for condition,rule,snapshot in src],
                         [(json.loads(condition),rule,json.loads(snapshot)) for condition,rule,snapshot in dest])
        self.assertEqual(self.call('/batches/%s'%bid)['experiments'],original['experiments'])

    def test_bulk_failure_on_second_source_rolls_back_every_new_record(self):
        t,_=self.template()
        self.source.list_predictors=lambda:[dict(id=7,name='first'),dict(id=8,name='second')]
        cfg=self.bulk_config(t);cfg['sources']=[7,8]
        preview=self.call('/batches/preview',cfg);before=self.counts()
        with sqlite3.connect(self.store.db_path) as c:
            c.execute("CREATE TRIGGER qa_fail_second BEFORE INSERT ON lab_batches WHEN NEW.predictor_id=8 BEGIN SELECT RAISE(ABORT,'injected last-item write failure'); END")
        self.call('/batches/bulk',dict(cfg,request_id='qa-atomic-last-item',preview_revision=preview['preview_revision'],enqueue=True),code=503)
        self.assertEqual(self.counts(),before)
        with sqlite3.connect(self.store.db_path) as c:c.execute('DROP TRIGGER qa_fail_second')
        result=self.call('/batches/bulk',dict(cfg,request_id='qa-atomic-last-item',preview_revision=preview['preview_revision'],enqueue=True),code=201)
        self.assertEqual(len(result['batches']),2)
        self.assertEqual(self.counts()['lab_series'],before['lab_series'])

    def test_selective_study_uses_only_requested_condition_and_single_odds(self):
        t,_=self.template();created,_=self.bulk(t);bid=created['batches'][0]['id']
        original=self.call('/batches/%s'%bid)
        cond=next(e['condition_key'] for e in original['experiments'] if e['condition']['type']=='miss_streak')
        cfg=dict(source_batch_id=bid,condition_keys=[cond],settlement_rule_ids=['pc28_high_regular'],
                 candidates=[dict(kind='preset',key='round_sl_half')])
        preview=self.call('/studies/preview',cfg)
        self.assertEqual(preview['new_experiments'],2)
        payload=dict(cfg,name='QA 单条件单赔率',request_id='qa-selective-study',preview_revision=preview['preview_revision'],enqueue=False)
        study=self.call('/studies',payload,code=201)['study'];result=self.call('/studies/%s'%study['id'])
        self.assertEqual(len(result['rows']),2)
        self.assertEqual({r['condition_key'] for r in result['rows']},{cond})
        self.assertEqual({r['settlement_rule_id'] for r in result['rows']},{'pc28_high_regular'})
        self.assertEqual({r['variant_key'] for r in result['rows']},{'baseline','round_sl_half'})
        self.assertEqual(self.call('/batches/%s'%bid)['experiments'],original['experiments'])

    def test_queue_resume_receipt_replay_cannot_undo_a_later_user_pause(self):
        t,_=self.template();created,_=self.bulk(t,enqueue=True);bid=created['batches'][0]['id']
        def item():return next(x for x in self.call('/queue')['items'] if x['owner_kind']=='batch' and x['owner_id']==bid)
        q=item();paused=self.call('/queue/%s/state'%q['id'],dict(request_id='qa-first-pause',expected_revision=q['expected_revision'],action='pause'))['item']
        resume=dict(request_id='qa-then-resume',expected_revision=paused['expected_revision'],action='resume')
        self.call('/queue/%s/state'%q['id'],resume)
        self.call('/batches/%s/state'%bid,dict(state='paused'))
        self.assertEqual(item()['state'],'paused')
        self.call('/queue/%s/state'%q['id'],resume)
        self.store.advance_studies(now=base.NOW,cutoff=dict(issue_no='2000',at=base.NOW))
        self.assertEqual(item()['state'],'paused')
        self.assertEqual(self.call('/batches/%s'%bid)['batch']['status'],'paused')

    def test_removed_queue_entry_requires_explicit_new_enqueue_intent(self):
        t,_=self.template();created,_=self.bulk(t,enqueue=True);bid=created['batches'][0]['id']
        q=next(x for x in self.call('/queue')['items'] if x['owner_id']==bid)
        removed=self.call('/queue/%s/state'%q['id'],dict(request_id='qa-remove-entry',expected_revision=q['expected_revision'],action='remove'))['item']
        for _ in range(3):self.store.advance_studies(now=base.NOW,cutoff=dict(issue_no='2000',at=base.NOW))
        self.assertEqual(self.call('/batches/%s'%bid)['batch']['status'],'paused')
        result=self.call('/queue/enqueue',dict(request_id='qa-rejoin-explicit',owner_kind='batch',owner_id=bid,expected_revision=removed['expected_revision']))
        self.assertIn(result['item']['state'],['queued','running'])
        self.assertEqual(result['item']['id'],q['id'])
        self.assertEqual(self.counts()['lab_queue_items'],1)


if __name__=='__main__':unittest.main()
