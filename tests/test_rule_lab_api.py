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

    def test_empty_overview_has_bounded_storage_and_worker_status(self):
        data = self.request()['json']
        self.assertEqual(data['batches'], [])
        self.assertEqual(data['worker']['state'], 'starting')
        self.assertGreater(data['storage']['used_bytes'], 0)
        self.assertEqual(data['storage']['detail_retention_days'], 30)

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
