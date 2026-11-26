"""Rule lab HTTP workflows against a temporary simulation ledger, without workers."""
from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore
from tests.test_api import FakeRepository
from tests.test_rule_lab_risk import policy

PREFIX = '/api/platform/rule-lab'
NOW = '2026-11-26T04:00:00Z'


class PublicSource:
    def __init__(self):
        self.available = True
        self.shared = True

    def list_predictors(self):
        if not self.available:
            raise OSError('/private/source.db cannot be opened')
        return [{'id': 7, 'name': '公开预测器'}] if self.shared else []

    def get_predictor(self, predictor_id):
        return next((p for p in self.list_predictors() if p['id'] == predictor_id), None)


class RuleLabApiTests(unittest.TestCase):
    def setUp(self):
        from pc28touzhu.services.rule_lab_service import RuleLabService
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.store = RuleLabStore(Path(self.tmp.name) / 'lab' / 'rule_lab.db')
        self.source = PublicSource()
        self.service = RuleLabService(self.store, self.source)
        self.repo = FakeRepository()
        self.other = self.repo.create_user_record(username='second-admin', role='admin', status='active')
        self.regular = self.repo.create_user_record(username='regular', role='user', status='active')
        self.app = PlatformApiApplication(self.repo, 'executor-fixture', 'lab-fixture',
                                          rule_lab_service=self.service)

    def request(self, path=PREFIX, payload=None, *, uid=1, method=None, query='', raw=None):
        headers = {}
        if uid is not None:
            headers['HTTP_COOKIE'] = '%s=%s' % (SESSION_COOKIE_NAME, build_session_cookie_value(uid, 'lab-fixture'))
        env = build_testing_environ(path, method=method or ('GET' if payload is None else 'POST'),
                                    body=payload, headers=headers, query=query)
        if raw is not None:
            env['wsgi.input'] = io.BytesIO(raw)
            env['CONTENT_LENGTH'] = str(len(raw))
        captured = {}
        def start_response(status, response_headers):
            captured.update(status=status, headers=dict(response_headers))
        body = b''.join(self.app(env, start_response))
        captured['body'] = body
        captured['status_code'] = int(captured['status'].split()[0])
        if captured['headers'].get('Content-Type', '').startswith('application/json'):
            captured['json'] = json.loads(body)
        return captured

    @staticmethod
    def payload(**changes):
        return dict(name='规则比较', mode='live', predictor_id=7, metric='big_small', **changes)

    def create(self, *, metric='big_small', uid=1):
        payload = self.payload()
        payload['metric'] = metric
        result = self.request(PREFIX + '/batches', payload, uid=uid)
        self.assertEqual(result['status'], '201 Created', result)
        return result['json']['batch']

    def test_options_only_two_ok_presets_with_full_odds(self):
        result = self.request(PREFIX + '/options')
        self.assertEqual(result['status_code'], 200)
        data = result['json']
        self.assertEqual(data['predictors'], [{'id': 7, 'name': '公开预测器'}])
        self.assertEqual([r['id'] for r in data['settlement_rules']], ['pc28_netdisk_regular', 'pc28_high_regular'])
        self.assertEqual(data['settlement_rules'][1]['big_small_odds'], 2.846)
        self.assertEqual(data['limits']['max_running_experiments'], 100)
        self.assertTrue(data['source_available'])

    def test_risk_policy_edit_is_owned_unstarted_and_updates_no_source_fields(self):
        batch=self.create();path=PREFIX+'/batches/'+str(batch['id'])
        result=self.request(path+'/risk-policy',{'risk_policy':policy()})
        self.assertEqual(result['status_code'],200,result)
        self.assertEqual(result['json']['batch']['predictor_id'],7)
        self.assertEqual(result['json']['batch']['stake_amount'],1)
        self.assertEqual(result['json']['batch']['risk_policy']['mode'],'round')
        self.assertEqual(self.request(path+'/risk-policy',{'risk_policy':policy()},uid=self.other['id'])['status_code'],404)
        self.request(path+'/state',{'state':'running'});self.request(path+'/state',{'state':'paused'})
        self.assertEqual(self.request(path+'/risk-policy',{'risk_policy':policy()})['status_code'],409)

    def test_presets_keep_route_and_rule_layers_and_only_owners_safe_fields(self):
        class Repo:
            def list_auto_trigger_rules(self,*,user_id):
                self.uid=user_id
                return [dict(id=6,name='参考规则',user_id=user_id,scope_mode='selected_subscriptions',subscription_ids=[7],cooldown_issues=10,
                    daily_risk_control=dict(enabled=True,profit_target=20,loss_limit=20,hit_rate_gate=dict(enabled=True,threshold=25,min_settled_count=20)),
                    cycle_policy={'mode':'legacy'},routes=[dict(id=9,name='路由A',route_risk_mode='override',
                        route_risk_control=dict(enabled=True,profit_target=30,loss_limit=10),subscription_risk_mode='override',
                        subscription_risk_control=dict(enabled=True,profit_target=10,loss_limit=10)),
                        dict(id=10,name='路由B',route_risk_mode='disabled',subscription_risk_mode='disabled')])]
            def list_subscriptions(self,user_id):
                return [dict(id=7,user_id=user_id,name='方案',source_id=99,config_json='SECRET',strategy_v2=dict(
                    play_filter=dict(mode='selected',selected_keys=['combo:大单']),staking_policy=dict(mode='fixed',fixed_amount=9),
                    risk_control=dict(enabled=True,profit_target=5,loss_limit=7)))]
        repo=Repo();self.service.platform_repository=repo
        data=self.request(PREFIX+'/options')['json'];presets=data['risk_presets']
        self.assertEqual(repo.uid,1);self.assertEqual(len(presets),2)
        self.assertNotIn('SECRET',json.dumps(data))
        first=next(p for p in presets if p['policy']['source']['route_id']==9)['policy']
        self.assertEqual(first['round']['profit_target'],10)
        self.assertEqual(first['day']['profit_target'],20)
        self.assertEqual(first['additional_day_guards'][0]['loss_limit'],10)
        self.assertEqual(first['day']['max_rounds'],0)
        second=next(p for p in presets if p['policy']['source']['route_id']==10)['policy']
        self.assertFalse(second['round']['enabled'])
        self.assertTrue(second['day']['enabled'])

    def test_disabled_daily_targets_are_not_revived_by_cycle_quota(self):
        class Repo:
            def list_auto_trigger_rules(self,*,user_id):
                return [dict(id=6,user_id=user_id,name='轮数规则',subscription_ids=[7],cooldown_issues=0,cycle_policy=dict(mode='rule',max_starts_per_day=3),
                    daily_risk_control=dict(enabled=False,profit_target=10,loss_limit=8,hit_rate_gate=dict(enabled=True,threshold=40,min_settled_count=2)),
                    routes=[dict(id=9,name='路由',route_risk_mode='inherit_rule',subscription_risk_mode='override',subscription_risk_control=dict(enabled=True,profit_target=1,loss_limit=1))])]
            def list_subscriptions(self,user_id):
                return [dict(id=7,user_id=user_id,name='方案',strategy_v2=dict(play_filter=dict(mode='selected',selected_keys=['odd_even:单']),
                    staking_policy=dict(mode='fixed',fixed_amount=1),risk_control=dict(enabled=False,profit_target=0,loss_limit=0)))]
        self.service.platform_repository=Repo()
        p=self.request(PREFIX+'/options')['json']['risk_presets'][0]['policy']
        self.assertTrue(p['day']['enabled']);self.assertEqual(p['day']['max_rounds'],3)
        self.assertEqual(p['day']['profit_target'],0);self.assertEqual(p['day']['loss_limit'],0)
        self.assertFalse(p['day']['hit_rate_gate']['enabled'])

    def test_inherited_play_limits_and_unsupported_roll_are_explicitly_not_mixed(self):
        class Repo:
            def list_auto_trigger_rules(self,*,user_id):
                return [dict(id=6,user_id=user_id,name='继承',subscription_ids=[7,8],daily_risk_control=dict(enabled=False),
                    routes=[dict(id=9,name='路由',route_risk_mode='disabled',subscription_risk_mode='inherit_subscription')])]
            def list_subscriptions(self,user_id):
                return [dict(id=7,user_id=user_id,name='固定',strategy_v2=dict(play_filter=dict(mode='selected',selected_keys=['combo:大单']),staking_policy=dict(mode='fixed',fixed_amount=9),
                    risk_control=dict(enabled=True,profit_target=4,loss_limit=6,play_limits=dict(combo=dict(profit_target=8,loss_limit=0))))),
                    dict(id=8,user_id=user_id,name='过关不可复制',strategy_v2=dict(play_filter=dict(mode='all'),staking_policy=dict(mode='win_streak_roll',initial_amount=10)))]
        self.service.platform_repository=Repo()
        result=self.request(PREFIX+'/options')
        self.assertEqual(result['status_code'],200,result)
        presets=result['json']['risk_presets'];self.assertEqual(len(presets),1)
        self.assertEqual(presets[0]['policy']['round']['profit_target'],8)
        self.assertEqual(presets[0]['policy']['round']['loss_limit'],6)

    def test_comparison_create_queue_permissions_idempotence_and_csv(self):
        p=policy();p['day']['hit_rate_gate'].update(enabled=True,threshold=25)
        result=self.request(PREFIX+'/batches',dict(name='历史风险',mode='backtest',predictor_id=7,metric='big_small',
            start_date='2026-11-01',end_date='2026-11-26',risk_policy=p))
        self.assertEqual(result['status_code'],201,result);bid=result['json']['batch']['id']
        preview=self.request(PREFIX+'/batches/'+str(bid)+'/comparison-preview')
        self.assertEqual(preview['json']['new_experiments'],250)
        payload={'source_batch_id':bid,'name':'=危险名字只作文本','request_id':'api-study-request-001'}
        created=self.request(PREFIX+'/studies',payload);self.assertEqual(created['status_code'],201,created)
        sid=created['json']['study']['id'];self.assertEqual(self.request(PREFIX+'/studies',payload)['status_code'],200)
        path=PREFIX+'/studies/'+str(sid)
        for endpoint,body in [(path,None),(path+'/state',{'state':'running'}),(path+'/export.csv',None)]:
            self.assertEqual(self.request(endpoint,body,uid=None)['status_code'],401)
            self.assertEqual(self.request(endpoint,body,uid=self.regular['id'])['status_code'],403)
            self.assertEqual(self.request(endpoint,body,uid=self.other['id'])['status_code'],404)
        self.assertEqual(self.request(path+'/state',{'state':'running'})['json']['study']['status'],'queued')
        self.assertEqual(self.request(path+'/state',{'state':'paused'})['json']['study']['status'],'paused')
        detail=self.request(path)['json'];self.assertEqual(len(detail['rows']),250);self.assertFalse(detail['study']['comparable'])
        child=detail['study']['children'][0]['batch_id']
        self.assertEqual(self.request(PREFIX+'/batches/'+str(child)+'/state',{'state':'running'})['json']['code'],'study_managed')
        exported=self.request(path+'/export.csv');self.assertEqual(exported['status_code'],200)
        self.assertIn('comparison_reason',exported['body'].decode('utf-8-sig'))

    def test_empty_overview_has_bounded_storage_and_worker_status(self):
        data = self.request()['json']
        self.assertEqual(data['batches'], [])
        self.assertEqual(data['worker']['state'], 'starting')
        self.assertGreater(data['storage']['used_bytes'], 0)
        self.assertEqual(data['storage']['detail_retention_days'], 30)

    def test_monthly_detail_is_owned_read_only_and_compatible(self):
        batch=self.create();bid=batch['id'];eid=self.store.list_experiments(1,bid)[0]['id']
        path=PREFIX+'/batches/'+str(bid)+'/experiments/'+str(eid)
        result=self.request(path)
        self.assertEqual(result['status_code'],200,result)
        self.assertEqual(set(result['json']),{'experiment','daily','recent','recent_rounds','monthly'})
        report=result['json']['monthly']
        self.assertEqual(report['timezone'],'Asia/Shanghai')
        self.assertFalse(report['capital']['available'])
        self.assertIsNone(report['summary']['profitable_month_ratio'])
        for uid,expected in [(None,401),(self.regular['id'],403),(self.other['id'],404)]:
            self.assertEqual(self.request(path,uid=uid)['status_code'],expected)
        self.assertEqual(self.request(path,method='POST',payload={})['status_code'],405)
        self.assertEqual(self.request(path,query='month=2026-10')['status_code'],400)
        self.assertEqual(self.store.get_batch(1,bid)['status'],'paused')
        self.assertEqual(result['headers']['Cache-Control'],'no-store')

    def test_create_start_pause_detail_and_export_workflow(self):
        batch = self.create()
        self.assertEqual(batch['status'], 'paused')
        self.assertEqual(batch['experiment_count'], 50)
        path = PREFIX + '/batches/' + str(batch['id'])
        self.assertEqual(len(self.request(path)['json']['experiments']), 50)
        for state in ('running', 'paused', 'paused'):
            result = self.request(path + '/state', {'state': state})
            self.assertEqual(result['status_code'], 200, result)
            self.assertEqual(result['json']['batch']['status'], state)
        experiments = self.request(path)['json']['experiments']
        self.assertEqual({e['settlement_rule_id'] for e in experiments}, {'pc28_netdisk_regular', 'pc28_high_regular'})
        detail = self.request(path + '/experiments/' + str(experiments[0]['id']))['json']
        self.assertEqual(detail['daily'], [])
        self.assertEqual(detail['recent'], [])
        self.assertIsNone(detail['experiment']['roi_percent'])
        export = self.request(path + '/export.csv')
        self.assertTrue(export['body'].startswith(b'\xef\xbb\xbf'))
        self.assertIn('attachment;', export['headers']['Content-Disposition'])
        rows = list(csv.DictReader(io.StringIO(export['body'].decode('utf-8-sig'))))
        self.assertEqual(len(rows), 50)
        self.assertEqual(rows[0]['settled_count'], '0')
        self.assertEqual(len(self.store.list_batches(1)), 1)
        self.assertEqual(self.repo.jobs, [])
        self.assertEqual(self.repo.subscriptions, [])

    def test_combo_creates_78_versions(self):
        self.assertEqual(self.create(metric='combo')['experiment_count'], 78)

    def test_long_combo_pack_freezes_26_versions_and_five_risk_variants(self):
        options=self.request(PREFIX+'/options')['json']
        pack=next(p for p in options['template_packs'] if p['id']=='combo_long_miss_v1')
        self.assertEqual(pack['condition_counts']['combo'],13)
        p=policy();p['day']['hit_rate_gate'].update(enabled=True,threshold=25)
        result=self.request(PREFIX+'/batches',dict(name='组合长连错',mode='backtest',predictor_id=7,metric='combo',
            start_date='2026-11-01',end_date='2026-11-26',risk_policy=p,template_pack=pack['id']))
        self.assertEqual(result['status_code'],201,result)
        batch=result['json']['batch'];self.assertEqual(batch['experiment_count'],26)
        before=self.service.batch_detail(1,batch['id'])
        study=self.request(PREFIX+'/studies',dict(source_batch_id=batch['id'],name='long variants',request_id='long-variants-01'))
        self.assertEqual(study['status_code'],201,study)
        self.assertEqual(self.store.capacity()['saved'],156)
        for child in study['json']['study']['children']:
            detail=self.service.batch_detail(1,child['batch_id'])
            self.assertEqual(detail['batch']['template_pack'],pack['id'])
            self.assertEqual(len(detail['experiments']),26)
            self.assertEqual(sorted({e['condition']['threshold'] for e in detail['experiments'] if e['condition']['type']=='miss_streak'}),[7,8,9,10,11,12,13,14,15,20,25,30])
            self.assertEqual(max(e['condition']['threshold'] or 0 for e in detail['experiments']),30)
            self.assertEqual(detail['batch']['status'],'paused')
        self.assertEqual(self.service.batch_detail(1,batch['id']),before)

    def test_anonymous_and_regular_users_cannot_access_any_lab_api(self):
        endpoints = [(PREFIX, None), (PREFIX + '/options', None), (PREFIX + '/batches', self.payload()),
                     (PREFIX + '/batches/1', None), (PREFIX + '/batches/1/state', {'state': 'running'}),
                     (PREFIX + '/batches/1/experiments/1', None), (PREFIX + '/batches/1/export.csv', None)]
        for uid, status, code in [(None, 401, 'unauthorized'), (self.regular['id'], 403, 'forbidden')]:
            for path, payload in endpoints:
                with self.subTest(uid=uid, path=path):
                    result = self.request(path, payload, uid=uid)
                    self.assertEqual(result['status_code'], status)
                    self.assertEqual(result['json']['code'], code)
        self.assertEqual(self.store.list_batches(1), [])

    def test_admins_cannot_read_or_change_another_owners_batch(self):
        batch = self.create()
        path = PREFIX + '/batches/' + str(batch['id'])
        self.assertEqual(self.request(uid=self.other['id'])['json']['batches'], [])
        for suffix, payload in [('', None), ('/state', {'state': 'running'}), ('/experiments/1', None), ('/export.csv', None)]:
            result = self.request(path + suffix, payload, uid=self.other['id'])
            self.assertEqual(result['status_code'], 404, result)
            self.assertEqual(result['json']['code'], 'not_found')
        self.assertEqual(self.store.get_batch(1, batch['id'])['status'], 'paused')

    def test_invalid_create_fields_are_rejected_without_creating_any_batch(self):
        variants = [dict(user_id=9), dict(source_path='/secret.db'), dict(settlement_rule_id='pc28_fullpay_2_8_regular'),
                    dict(predictor_id=True), dict(predictor_id=7.0), dict(predictor_id=999), dict(mode=[]), dict(metric={}),
                    dict(stake_amount=True), dict(stake_amount=float('nan')), dict(stake_amount=float('inf')),
                    dict(stake_amount=.001), dict(stake_amount=0), dict(start_date='2026-11-01'),
                    dict(name=' '), dict(name='x' * 81), dict(template_pack='other')]
        for changes in variants:
            with self.subTest(changes=changes):
                payload = self.payload(); payload.update(changes)
                result = self.request(PREFIX + '/batches', payload)
                self.assertEqual(result['status_code'], 400, result)
                self.assertEqual(result['json']['code'], 'invalid_input')
        self.assertEqual(self.store.list_batches(1), [])

    def test_backtest_dates_and_history_label(self):
        payload = self.payload(); payload.update(mode='backtest', start_date='2026-11-01', end_date='2026-11-02')
        result = self.service.create_batch(1, payload, now=NOW)
        self.assertIn('历史', result['batch']['history_notice'])
        for changes in [dict(start_date='2026-11-03'), dict(end_date='2026-11-27'), dict(start_date='2026-01-01'), dict(end_date=None)]:
            invalid = dict(payload, **changes)
            with self.subTest(changes=changes), self.assertRaises(RuleLabError) as error:
                self.service.create_batch(1, invalid, now=NOW)
            self.assertEqual(error.exception.code, 'invalid_input')

    def test_global_running_limit_returns_conflict_and_can_recover(self):
        first, second, third = self.create(), self.create(uid=self.other['id']), self.create()
        for batch, uid in [(first, 1), (second, self.other['id'])]:
            self.assertEqual(self.request(PREFIX + '/batches/%s/state' % batch['id'], {'state': 'running'}, uid=uid)['status_code'], 200)
        path = PREFIX + '/batches/%s/state' % third['id']
        limited = self.request(path, {'state': 'running'})
        self.assertEqual(limited['status'], '409 Conflict')
        self.assertEqual(limited['json']['code'], 'capacity_exceeded')
        self.request(PREFIX + '/batches/%s/state' % first['id'], {'state': 'paused'})
        self.assertEqual(self.request(path, {'state': 'running'})['status_code'], 200)

    def test_unshared_source_cannot_be_created_or_started_but_can_pause(self):
        batch = self.create()
        self.source.shared = False
        self.assertEqual(self.request(PREFIX + '/batches', self.payload())['status_code'], 400)
        path = PREFIX + '/batches/%s/state' % batch['id']
        self.assertEqual(self.request(path, {'state': 'running'})['status_code'], 400)
        self.assertEqual(self.request(path, {'state': 'paused'})['status_code'], 200)

    def test_source_failure_is_safe_and_existing_scores_remain_readable(self):
        batch = self.create()
        self.source.available = False
        options = self.request(PREFIX + '/options')
        self.assertFalse(options['json']['source_available'])
        self.assertNotIn('/private', options['body'].decode())
        create = self.request(PREFIX + '/batches', self.payload())
        self.assertEqual(create['status'], '503 Service Unavailable')
        self.assertEqual(create['json']['code'], 'source_unavailable')
        self.assertNotIn('/private', create['body'].decode())
        self.assertEqual(self.request(PREFIX + '/batches/%s' % batch['id'])['status_code'], 200)

    def test_invalid_methods_routes_json_and_status_are_safe(self):
        batch = self.create(); path = PREFIX + '/batches/%s/state' % batch['id']
        for payload in [{'state': 'completed'}, {'state': 'running', 'user_id': 5}, {'state': []}, {}]:
            self.assertEqual(self.request(path, payload)['status_code'], 400)
        self.assertEqual(self.request(PREFIX + '/batches', method='GET')['status_code'], 405)
        self.assertEqual(self.request(PREFIX + '/batches/1', method='DELETE')['status_code'], 405)
        self.assertEqual(self.request(PREFIX + '/batches/not-id')['status_code'], 404)
        self.assertEqual(self.request(PREFIX + '/batches/999999999999999999999999')['status_code'], 404)
        self.assertEqual(self.request(PREFIX + '/batches', method='POST', raw=b'[')['status_code'], 400)
        oversized = json.dumps(self.payload()).encode() + b' ' * 16385
        self.assertEqual(self.request(PREFIX + '/batches', method='POST', raw=oversized)['status_code'], 400)
        self.assertEqual(self.request(PREFIX, query='user_id=2')['status_code'], 400)

    def test_detail_and_csv_reflect_settled_ledger_without_recalculation(self):
        batch = self.create(); bid = batch['id']
        self.service.set_batch_state(1, bid, {'state': 'running'}, now=NOW)
        work = self.store.get_work_item(bid); eid = work['experiments'][0]['id']
        decision = dict(experiment_id=eid, issue_no='1001', bet_value='大', stake_cents=100,
                        decided_at=NOW, decision_mode='live', feature_issue_no='1000', condition_value=1)
        outcome = dict(experiment_id=eid, issue_no='1001', status='settled', settled_at='2026-11-26T04:05:00Z',
                       result_type='hit', net_cents=98, direction_hit=True, refund_reason='', skip_reason='')
        self.store.commit_step(bid, work['batch']['revision'], cursor={'last_issue_no': '1001'}, now=NOW,
                               decisions=[decision], outcomes=[outcome])
        detail = self.request(PREFIX + '/batches/%s/experiments/%s' % (bid, eid))['json']
        self.assertEqual(detail['experiment']['net_profit'], .98)
        self.assertEqual(detail['daily'][0]['date'], '2026-11-26')
        self.assertEqual(detail['recent'][0]['result_type'], 'hit')
        csv_body = self.request(PREFIX + '/batches/%s/export.csv' % bid)['body'].decode('utf-8-sig')
        self.assertEqual(next(csv.DictReader(io.StringIO(csv_body)))['net_profit'], '0.98')

    def test_csv_quotes_and_neutralizes_formula_strings(self):
        batch = self.create()
        real_rows = self.store.list_experiments(1, batch['id'])
        values = ['=HYPERLINK("https://example.invalid")', '+SUM(1,2)', '-1+1', '@x', '\tformula', '\nformula', '安全,"文本"']
        rows = [dict(real_rows[0], name=value) for value in values]
        with patch.object(self.store, 'list_experiments', return_value=rows):
            output = self.service.export_csv(1, batch['id']).decode('utf-8-sig')
        names = [r['name'] for r in csv.DictReader(io.StringIO(output))]
        self.assertEqual(names[:-1], ["'" + v for v in values[:-1]])
        self.assertEqual(names[-1], values[-1])


class RuleLabLazyIntegrationTests(unittest.TestCase):
    def test_existing_routes_do_not_initialize_lab(self):
        app = PlatformApiApplication(FakeRepository(), 'test', 'test')
        with patch.object(RuleLabStore, 'initialize', side_effect=AssertionError('unexpected lab initialization')):
            captured = {}
            body = b''.join(app(build_testing_environ('/api/health'), lambda status, headers: captured.update(status=status)))
        self.assertEqual(captured['status'], '200 OK')
        self.assertEqual(json.loads(body)['status'], 'ok')


if __name__ == '__main__':
    unittest.main()
