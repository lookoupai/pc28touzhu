"""Cookie认证、真实WSGI及SQLite的新周期配置契约。"""
from __future__ import annotations

import json
from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from tests.test_cycle_runtime import CycleFixture


class CycleApiTests(CycleFixture):
    def setUp(self):
        super().setUp()
        self.app=PlatformApiApplication(repository=self.repo,executor_api_token='test',session_secret='test-secret')

    def request(self,path,body=None,*,user_id=None,authenticated=True):
        headers={}
        if authenticated:
            headers['HTTP_COOKIE']='%s=%s'%(SESSION_COOKIE_NAME,build_session_cookie_value(user_id or self.user_id,'test-secret'))
        captured={}
        def start_response(status,headers): captured['status']=status
        payload=json.loads(b''.join(self.app(build_testing_environ(path,method='GET' if body is None else 'POST',body=body,headers=headers),start_response)).decode())
        return int(captured['status'].split()[0]),payload

    def payload(self):
        return {'name':'周期规则','scope_mode':'selected_subscriptions','subscription_ids':[self.sub['id']], 'conditions':[{'type':'miss_streak','metric':'combo','operator':'gte','threshold':2}], 'action':{'dispatch_latest_signal':True}}

    def test_new_rule_defaults_and_edit_round_trip(self):
        code,result=self.request('/api/platform/auto-trigger-rules',self.payload())
        self.assertEqual(code,200,result)
        rule=result['item']
        self.assertEqual(rule.get('cycle_policy'),{'mode':'rule','max_starts_per_day':1,'max_starts_per_window':1,'on_target':'wait_condition','on_bust':'wait_condition'})
        policy={'mode':'rule','max_starts_per_day':3,'max_starts_per_window':2,'on_target':'new_cycle','on_bust':'close_day'}
        code,result=self.request('/api/platform/auto-trigger-rules/%s'%rule['id'],{'cycle_policy':policy})
        self.assertEqual(code,200,result)
        code,result=self.request('/api/platform/auto-trigger-rules')
        self.assertEqual(result['items'][0]['cycle_policy'],policy)
        self.assertIn('cycle_runtime',result['items'][0])

    def test_invalid_quota_and_legacy_creation_rejected(self):
        for value in [True,0,-1,1.5,1001,'nan']:
            payload=self.payload()
            payload['cycle_policy']={'mode':'rule','max_starts_per_day':value,'on_target':'wait_condition','on_bust':'wait_condition'}
            code,result=self.request('/api/platform/auto-trigger-rules',payload)
            self.assertEqual(code,400,(value,result))
        payload=self.payload()
        payload['cycle_policy']={'mode':'legacy'}
        self.assertEqual(self.request('/api/platform/auto-trigger-rules',payload)[0],400)

    def test_combined_trigger_round_trip_and_validation(self):
        payload = self.payload()
        payload.update(trigger_mode='condition_schedule', schedule={
            'timezone': 'Asia/Shanghai', 'weekdays': ['Wed'],
            'windows': [{'id': 'evening', 'start': '18:00', 'end': '24:00'}],
        }, guard_groups=[{'name': '附加条件', 'conditions': payload['conditions']}])
        code, result = self.request('/api/platform/auto-trigger-rules', payload)
        self.assertEqual(code, 200, result)
        rule = result['item']
        self.assertTrue(rule['conditions'])
        self.assertTrue(rule['guard_groups'])
        code, result = self.request('/api/platform/auto-trigger-rules/%s' % rule['id'], {'name': '组合改名'})
        self.assertEqual(code, 200, result)
        self.assertEqual(result['item']['trigger_mode'], 'condition_schedule')
        self.assertEqual(result['item']['schedule']['windows'], payload['schedule']['windows'])
        self.assertEqual(result['item']['conditions'], rule['conditions'])
        self.assertEqual(result['item']['guard_groups'], rule['guard_groups'])
        for changes in [{'conditions': []}, {'schedule': {'windows': []}}]:
            code, _ = self.request('/api/platform/auto-trigger-rules/%s' % rule['id'], changes)
            self.assertEqual(code, 400)

    def test_summary_returns_manual_scope_and_resume_requires_auth(self):
        code,result=self.request('/api/platform/subscriptions/summary')
        self.assertEqual(code,200,result)
        self.assertIn('manual_cycle_state',result['items'][0])
        self.assertIsNone(result['items'][0]['manual_cycle_state'])
        rule=self.rule()
        self.assertEqual(self.request('/api/platform/auto-trigger-rules/%s/resume-cycle'%rule['id'],{'subscription_id':self.sub['id']},authenticated=False)[0],401)

    def test_legacy_closed_rule_switch_and_scoped_resume_preserve_other_sub(self):
        rule=self.rule(targets=[self.targets[1]],limit=1)
        self.signal()
        self.cycle(rule)
        self.deliver_and_settle(self.latest+1)
        other=self.repo.create_subscription_record(user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='standby')
        with self.repo._connect() as conn:
            conn.execute("UPDATE auto_trigger_rules SET cycle_policy_json='{}',subscription_ids_json=? WHERE id=?",(json.dumps([self.sub['id'],other['id']]),rule['id']))
        self.repo.stop_auto_trigger_rule_day_for_cycle(rule_id=rule['id'],user_id=self.user_id,stat_date=self.day,reason='cycle_balance_depleted')
        code,result=self.request('/api/platform/auto-trigger-rules/%s'%rule['id'],{'cycle_policy':{'mode':'rule','max_starts_per_day':3,'on_target':'wait_condition','on_bust':'wait_condition'}})
        self.assertEqual(code,200,result)
        self.assertEqual(self.repo.get_auto_trigger_rule_daily_stat(rule_id=rule['id'],user_id=self.user_id,stat_date=self.day)['status'],'stopped')
        from pc28touzhu.services.auto_trigger_cycle_service import resume_rule_cycle
        result=resume_rule_cycle(self.repo,rule_id=rule['id'],user_id=self.user_id,subscription_id=self.sub['id'],now=self.now)
        self.assertTrue(result['resumed'])
        self.assertEqual(self.session(rule)['status'],'waiting')
        self.assertEqual(self.repo.get_auto_trigger_cycle_session(rule_id=rule['id'],subscription_id=other['id'],user_id=self.user_id)['status'],'day_closed')
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,stat_date=self.day)['starts_count'],1)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)),2)
