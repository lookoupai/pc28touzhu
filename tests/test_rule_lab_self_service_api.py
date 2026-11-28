"""自助配置真实WSGI流程：临时账本、权限、快照和幂等。"""
import copy
import sqlite3
import unittest
from unittest.mock import patch
from tests import test_rule_lab_api as fixtures
PREFIX=fixtures.PREFIX


class SelfServiceApiTests(unittest.TestCase):
    setUp = fixtures.RuleLabApiTests.setUp
    request = fixtures.RuleLabApiTests.request

    def template_payload(self, token='template-create-001'):
        return dict(request_id=token,kind='condition',name='单双精选',description='',
            definition=dict(metric='odd_even',conditions=[dict(type='always'),
                dict(type='miss_streak',threshold=7,operator='gte')]))

    def make_template(self):
        result=self.request(PREFIX+'/templates',self.template_payload())
        self.assertEqual(result['status_code'],201,result)
        return result['json']['template']

    def test_template_crud_is_owned_idempotent_and_revision_checked(self):
        created=self.make_template();tid=created['id']
        self.assertEqual(created['condition_count'],2)
        replay=self.request(PREFIX+'/templates',self.template_payload())
        self.assertEqual(replay['status_code'],200,replay)
        self.assertEqual(replay['json']['template']['id'],tid)
        path=PREFIX+'/templates/'+str(tid)
        self.assertEqual(self.request(path,uid=self.other['id'])['status_code'],404)
        for uid,status in [(None,401),(self.regular['id'],403)]:
            self.assertEqual(self.request(PREFIX+'/templates',uid=uid)['status_code'],status)
        changed=self.template_payload('template-edit-001');changed.pop('kind')
        changed['expected_revision']=1;changed['definition']['conditions'][1]['threshold']=8
        edited=self.request(path+'/edit',changed)
        self.assertEqual(edited['status_code'],200,edited)
        self.assertEqual(edited['json']['template']['revision'],2)
        stale={**changed,'request_id':'template-edit-stale'}
        self.assertEqual(self.request(path+'/edit',stale)['status_code'],409)
        deleted=dict(request_id='template-delete-001',expected_revision=2)
        self.assertEqual(self.request(path+'/delete',deleted)['status_code'],200)
        self.assertEqual(self.request(path+'/delete',deleted)['json']['created'],False)
        self.assertEqual(self.request(path)['status_code'],404)
        self.assertEqual(self.request(path+'/delete',deleted,uid=self.other['id'])['status_code'],404)

    def test_template_preview_deduplicates_conditions_and_rejects_unknown_fields(self):
        payload=self.template_payload();payload.pop('request_id')
        payload['definition']['conditions'] += [dict(type='miss_streak',threshold=7,operator='gte')]
        response=self.request(PREFIX+'/templates/preview',payload)
        self.assertEqual(response['status_code'],200,response)
        self.assertEqual(response['json']['condition_count'],2)
        self.assertEqual(response['json']['duplicates_removed'],1)
        bad=copy.deepcopy(payload);bad['definition']['conditions'][1]['threshold']=True
        self.assertEqual(self.request(PREFIX+'/templates/preview',bad)['status_code'],400)
        self.assertEqual(self.request(PREFIX+'/templates',query='kind=condition&metric=odd_even')['status_code'],200)
        self.assertEqual(self.request(PREFIX+'/templates',query='kind=condition&kind=risk')['status_code'],400)

    def test_template_edit_delete_and_fork_keep_experiment_snapshots(self):
        template=self.make_template()
        selection=dict(template_ref=template['ref'],expected_revision=template['revision'])
        response=self.request(PREFIX+'/batches',dict(name='冻结条件',mode='live',predictor_id=7,metric='odd_even',
            condition_selection=selection))
        self.assertEqual(response['status_code'],201,response)
        batch=response['json']['batch'];bid=batch['id']
        original=self.service.batch_detail(1,bid)
        self.assertEqual(len(original['experiments']),4)
        delete=dict(request_id='template-delete-freeze',expected_revision=1)
        self.assertEqual(self.request(PREFIX+'/templates/'+str(template['id'])+'/delete',delete)['status_code'],200)
        self.assertEqual(self.service.batch_detail(1,bid),original)
        preview=self.request(PREFIX+'/batches/'+str(bid)+'/management')['json']
        config=dict(preview['config'],name='冻结条件副本')
        fork=self.request(PREFIX+'/batches/'+str(bid)+'/fork',dict(request_id='freeze-fork-001',expected_revision=preview['expected_revision'],config=config))
        self.assertEqual(fork['status_code'],201,fork)
        other=self.service.batch_detail(1,fork['json']['batch']['id'])
        self.assertEqual([e['condition'] for e in other['experiments']],[e['condition'] for e in original['experiments']])
        self.assertEqual([e['settlement_odds'] for e in other['experiments']],[e['settlement_odds'] for e in original['experiments']])

    def bulk_payload(self,template):
        from tests.test_rule_lab_risk import policy
        return dict(mode='backtest',sources=[7],start_date='2026-11-01',end_date='2026-11-26',stake_amount=1,
            metric_configs=[dict(metric='odd_even',condition_selection=dict(template_ref=template['ref'],expected_revision=template['revision']),
                risk_selection=dict(policy=policy()))],name_pattern='{source} · {metric} · {template}',names=[])

    def test_bulk_preview_and_idempotent_transaction(self):
        template=self.make_template();cfg=self.bulk_payload(template)
        preview=self.request(PREFIX+'/batches/preview',cfg)
        self.assertEqual(preview['status_code'],200,preview)
        self.assertEqual(preview['json']['new_experiments'],4)
        payload=dict(cfg,request_id='bulk-create-001',preview_revision=preview['json']['preview_revision'],enqueue=False)
        created=self.request(PREFIX+'/batches/bulk',payload)
        self.assertEqual(created['status_code'],201,created)
        repeated=self.request(PREFIX+'/batches/bulk',payload)
        self.assertEqual(repeated['status_code'],200,repeated)
        self.assertFalse(repeated['json']['created'])
        self.assertEqual([b['id'] for b in created['json']['batches']],[b['id'] for b in repeated['json']['batches']])
        self.assertEqual(self.request(PREFIX+'/batches/bulk',dict(payload,stake_amount=2))['status_code'],409)

    def test_selected_study_only_creates_requested_pairs(self):
        from tests.test_rule_lab_risk import policy
        batch=self.request(PREFIX+'/batches',dict(name='比较来源',mode='backtest',predictor_id=7,metric='odd_even',
            start_date='2026-11-01',end_date='2026-11-26',risk_policy=policy()))['json']['batch']
        source=self.service.batch_detail(1,batch['id']);key=source['experiments'][1]['condition']['condition_key']
        cfg=dict(source_batch_id=batch['id'],condition_keys=[key],settlement_rule_ids=['pc28_high_regular'],
            candidates=[dict(kind='preset',key='round_sl_half')])
        preview=self.request(PREFIX+'/studies/preview',cfg)
        self.assertEqual(preview['status_code'],200,preview)
        self.assertEqual(preview['json']['new_experiments'],2)
        result=self.request(PREFIX+'/studies',dict(cfg,name='单条件对照',request_id='selective-study-001',
            preview_revision=preview['json']['preview_revision'],enqueue=False))
        self.assertEqual(result['status_code'],201,result)
        self.assertEqual(sum(c['experiment_count'] for c in result['json']['study']['children']),2)

    def test_selected_risk_templates_keep_precise_frozen_provenance(self):
        from tests.test_rule_lab_risk import policy
        templates=[]
        for index,amount in enumerate((10,5)):
            risk=policy();risk['round']['loss_limit']=amount
            response=self.request(PREFIX+'/templates',dict(request_id='risk-origin-'+str(index),kind='risk',
                name='风险模板'+str(index),description='',definition=dict(metrics=['odd_even'],policy=risk)))
            self.assertEqual(response['status_code'],201,response)
            templates.append(response['json']['template'])
        baseline,candidate=templates
        response=self.request(PREFIX+'/batches',dict(name='有风控出处的来源',mode='backtest',predictor_id=7,
            metric='odd_even',start_date='2026-11-01',end_date='2026-11-26',
            risk_selection=dict(template_ref=baseline['ref'],expected_revision=1)))
        self.assertEqual(response['status_code'],201,response);source=response['json']['batch']
        key=self.service.batch_detail(1,source['id'])['experiments'][0]['condition']['condition_key']
        selection=dict(source_batch_id=source['id'],condition_keys=[key],settlement_rule_ids=['pc28_high_regular'],
            candidates=[dict(kind='risk_template',template_id=candidate['id'],expected_revision=1),
                        dict(kind='preset',key='round_tp_half')])
        preview=self.request(PREFIX+'/studies/preview',selection)
        self.assertEqual(preview['status_code'],200,preview)
        created=self.request(PREFIX+'/studies',dict(selection,name='有出处对照',request_id='risk-origin-study',
            preview_revision=preview['json']['preview_revision'],enqueue=False))
        self.assertEqual(created['status_code'],201,created)
        by_key={child['variant_key']:self.store.get_batch(1,child['batch_id'])
                for child in created['json']['study']['children']}
        self.assertEqual(by_key['baseline']['risk_template_snapshot'],source['risk_template_snapshot'])
        snapshot=by_key['template_'+str(candidate['id'])]['risk_template_snapshot']
        self.assertEqual(snapshot,dict(ref=candidate['ref'],name=candidate['name'],revision=1))
        self.assertEqual(by_key['round_tp_half']['risk_template_snapshot'],{})
        self.assertEqual(self.request(PREFIX+'/templates/'+str(candidate['id'])+'/delete',
            dict(request_id='risk-origin-delete',expected_revision=1))['status_code'],200)
        self.assertEqual(self.store.get_batch(1,by_key['template_'+str(candidate['id'])]['id'])['risk_template_snapshot'],snapshot)

    def test_selected_gate_preview_explains_values_in_every_changed_day_layer(self):
        from tests.test_rule_lab_risk import policy
        risk=policy();risk['day']['hit_rate_gate'].update(enabled=True,threshold=25,min_settled_count=20)
        risk['additional_day_guards']=[copy.deepcopy(risk['day'])]
        created=self.request(PREFIX+'/batches',dict(name='双日层来源',mode='backtest',predictor_id=7,metric='odd_even',
            start_date='2026-11-01',end_date='2026-11-26',risk_policy=risk))
        self.assertEqual(created['status_code'],201,created);source=created['json']['batch']
        key=self.service.batch_detail(1,source['id'])['experiments'][0]['condition']['condition_key']
        preview=self.request(PREFIX+'/studies/preview',dict(source_batch_id=source['id'],condition_keys=[key],
            settlement_rule_ids=['pc28_high_regular'],candidates=[dict(kind='gate',threshold=40,min_settled_count=30)]))
        self.assertEqual(preview['status_code'],200,preview)
        changes='\n'.join(preview['json']['variants'][1]['changes'])
        self.assertIn('主日层',changes);self.assertIn('附加日层',changes)
        self.assertIn('25% → 40%',changes);self.assertIn('20 → 30',changes)

    def test_manual_candidate_uses_owned_real_rule_names_and_legacy_id_conflicts(self):
        from tests.test_rule_lab_risk import policy
        original=policy()
        source=self.request(PREFIX+'/batches',dict(name='引用对照',mode='backtest',predictor_id=7,metric='odd_even',
            start_date='2026-11-01',end_date='2026-11-26',risk_policy=original))['json']['batch']
        reference=policy();reference['round']['loss_limit']=5
        reference['source']=dict(rule_id=6,route_id=9,subscription_id=7,rule_name='实际规则',route_name='实际路由',subscription_name='实际方案')
        submitted=copy.deepcopy(reference);submitted['source']['rule_name']='客户端伪造名称'
        key=self.service.batch_detail(1,source['id'])['experiments'][0]['condition']['condition_key']
        selection=dict(source_batch_id=source['id'],condition_keys=[key],settlement_rule_ids=['pc28_high_regular'],
            candidates=[dict(kind='policy',label='单次风控',policy=submitted)])
        with patch.object(self.service,'_risk_presets',return_value=[dict(metric='odd_even',policy=reference)]):
            preview=self.request(PREFIX+'/studies/preview',selection)
            self.assertEqual(preview['status_code'],200,preview)
            self.assertEqual(preview['json']['variants'][1]['policy']['source']['rule_name'],'实际规则')
            self.assertFalse(preview['json']['variants'][1]['policy']['source']['modified'])
            legacy=self.request(PREFIX+'/studies',dict(source_batch_id=source['id'],name='旧载荷对照',request_id='shared-study-request'))
            self.assertEqual(legacy['status_code'],201,legacy)
            conflict=self.request(PREFIX+'/studies',dict(selection,name='新载荷对照',request_id='shared-study-request',
                preview_revision=preview['json']['preview_revision'],enqueue=False))
            self.assertEqual(conflict['status_code'],409,conflict)


if __name__=='__main__':unittest.main()
