"""续算 HTTP 契约使用真实 WSGI、临时来源和独立实验账本。"""
from __future__ import annotations

import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests import test_rule_lab_api as api_fixture
from tests.test_api import FakeRepository
from tests.test_rule_lab_resources import FakeResources
from tests.test_rule_lab_source import SourceFixture, UTC, stamp

PREFIX = api_fixture.PREFIX
NOW = '2026-11-27T04:00:00Z'


class ContinuousApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        root = Path(self.tmp.name)
        self.fixture = SourceFixture(root / 'source.db')
        self.source = RuleLabSource(self.fixture.path)
        self.store = RuleLabStore(root / 'lab' / 'rule_lab.db')
        self.service = RuleLabService(self.store, self.source, resources=FakeResources())
        self.repo = FakeRepository()
        self.other = self.repo.create_user_record(username='other-admin', role='admin', status='active')
        self.regular = self.repo.create_user_record(username='member', role='user', status='active')
        self.app = PlatformApiApplication(self.repo, 'executor-fixture', 'lab-fixture',
                                          rule_lab_service=self.service)
        first = datetime(2026, 11, 25, 15, 50, tzinfo=UTC)
        for offset in range(-100, 6):
            opened = first + timedelta(seconds=210 * offset)
            self.fixture.draw(1100 + offset, opened)
            self.fixture.prediction(1100 + offset, opened)
        config = normalize_batch_payload(dict(name='续算基线', mode='backtest', predictor_id=1,
            metric='big_small', start_date='2026-11-25', end_date='2026-11-25',
            risk_policy=api_fixture.policy()), now=NOW)
        self.batch = self.store.create_batch(1, config, '公开来源', build_experiments('big_small')[:2], now=NOW)
        self.bid = self.batch['id']
        self.path = PREFIX + '/batches/' + str(self.bid)
        self.store.set_batch_state(1, self.bid, 'running', now=NOW)
        for _ in range(20):
            RuleLabWorker(self.store, self.source).tick(now=NOW)
            if self.store.get_batch(1, self.bid)['status'] == 'completed':
                break
        self.assertEqual(self.store.get_batch(1, self.bid)['status'], 'completed')

    def request(self, *args, **kwargs):
        with patch('pc28touzhu.services.rule_lab_service._now', return_value=NOW):
            return api_fixture.RuleLabApiTests.request(self, *args, **kwargs)

    def contents(self):
        with sqlite3.connect(self.store.db_path) as connection:
            tables = [row[0] for row in connection.execute("SELECT name FROM sqlite_master WHERE type='table'")]
            return {name: connection.execute('SELECT * FROM ' + name + ' ORDER BY rowid').fetchall()
                    for name in tables}

    def preview(self):
        response = self.request(self.path + '/continuation-preview')
        self.assertEqual(response['status_code'], 200, response)
        return response['json']

    def configure(self, **changes):
        payload = dict(request_id='configure-continuation-001', expected_revision=self.preview()['expected_revision'],
                       capture_enabled=True, compute_enabled=False, daily_at='00:10', timezone='Asia/Shanghai')
        payload.update(changes)
        response = self.request(self.path + '/continuation', payload)
        self.assertIn(response['status_code'], (200, 201), response)
        return payload, response

    def test_preview_and_empty_segment_reads_do_not_create_a_series(self):
        before = self.contents()
        result = self.preview()
        self.assertTrue(result['eligible'], result)
        self.assertEqual(result['next_issue'], '1103')
        self.assertEqual(result['saved_experiments'], 2)
        self.assertIsInstance(result['expected_revision'], str)
        self.assertFalse(result['continuation']['enabled'])
        for suffix in ('segments', 'input-coverage'):
            response = self.request(self.path + '/' + suffix)
            self.assertEqual(response['status_code'], 200, response)
            self.assertEqual(response['headers']['Cache-Control'], 'no-store')
        self.assertEqual(self.contents(), before)

    def test_new_routes_preserve_session_admin_and_owner_boundaries(self):
        routes = [('continuation-preview', None), ('segments', None), ('input-coverage', None),
                  ('continuation', {}), ('append', {})]
        before = self.contents()
        for suffix, payload in routes:
            for uid, code in ((None, 401), (self.regular['id'], 403), (self.other['id'], 404)):
                with self.subTest(suffix=suffix, uid=uid):
                    result = self.request(self.path + '/' + suffix, payload, uid=uid)
                    self.assertEqual(result['status_code'], code, result)
                    self.assertEqual(result['headers']['Cache-Control'], 'no-store')
        self.assertEqual(self.contents(), before)

    def test_configuration_retries_return_one_baseline_and_keep_original_experiments(self):
        original = self.store.get_batch(1, self.bid)
        experiments = self.store.list_experiments(1, self.bid)
        payload, response = self.configure()
        self.assertEqual(response['status_code'], 201, response)
        retry = self.request(self.path + '/continuation', payload)
        self.assertEqual(retry['status_code'], 200, retry)
        self.assertEqual(retry['json']['continuation']['revision'], response['json']['continuation']['revision'])
        self.assertFalse(retry['json']['continuation']['compute_enabled'])
        current = self.store.get_batch(1, self.bid)
        for key in ('start_date', 'end_date', 'processed_issues', 'experiment_count', 'risk_policy'):
            self.assertEqual(current[key], original[key])
        self.assertEqual(self.store.list_experiments(1, self.bid), experiments)
        segments = self.request(self.path + '/segments')['json']['segments']
        self.assertEqual(len(segments), 1)
        self.assertEqual(segments[0]['kind'], 'baseline')
        changed = dict(payload, daily_at='00:20')
        self.assertEqual(self.request(self.path + '/continuation', changed)['status_code'], 409)
        stale = dict(payload, request_id='configure-continuation-002')
        self.assertEqual(self.request(self.path + '/continuation', stale)['status_code'], 409)

    def test_manual_append_persists_a_receipt_while_waiting_for_inputs(self):
        self.configure()
        payload = dict(request_id='append-continuation-001', expected_revision=self.preview()['expected_revision'],
                       end_date='2026-11-26')
        queued = self.request(self.path + '/append', payload)
        self.assertEqual(queued['status_code'], 202, queued)
        self.assertTrue(queued['json']['queued'])
        self.assertFalse(queued['json']['continuation']['compute_enabled'])
        retry = self.request(self.path + '/append', payload)
        self.assertEqual(retry['status_code'], 200, retry)
        recovered = self.preview()['latest_request']
        self.assertEqual(recovered['request_id'], payload['request_id'])
        self.assertEqual(recovered['end_date'], payload['end_date'])
        self.assertEqual(len(self.request(self.path + '/segments')['json']['segments']), 1)
        self.assertEqual(self.store.get_batch(1, self.bid)['experiment_count'], 2)

    def test_only_segment_routes_allow_bounded_explicit_pagination(self):
        response = self.request(self.path + '/segments', query='limit=1&before_id=999999')
        self.assertEqual(response['status_code'], 200, response)
        for query in ('limit=0', 'limit=51', 'limit=true', 'limit=1&limit=2', 'before_id=-1',
                      'before_id=0', 'before_id=1.5', 'unknown=1', 'limit=', 'limit=1&ignored='):
            with self.subTest(query=query):
                self.assertEqual(self.request(self.path + '/segments', query=query)['status_code'], 400)
        for suffix in ('', '/continuation-preview', '/input-coverage'):
            self.assertEqual(self.request(self.path + suffix, query='limit=1')['status_code'], 400)

    def test_configuration_rejects_unknown_fields_types_dates_and_oversized_payloads(self):
        revision = self.preview()['expected_revision']
        good = dict(request_id='validation-request-001', expected_revision=revision, capture_enabled=True,
                    compute_enabled=False, daily_at='00:10', timezone='Asia/Shanghai')
        before = self.contents()
        invalid = [dict(good, capture_enabled=1), dict(good, compute_enabled='false'),
                   dict(good, capture_enabled=False, compute_enabled=True), dict(good, daily_at='24:00'),
                   dict(good, daily_at='0:10'), dict(good, timezone='UTC'), dict(good, expected_revision=1),
                   dict(good, request_id='x'), dict(good, stake_amount=100)]
        for payload in invalid:
            with self.subTest(payload=payload):
                self.assertEqual(self.request(self.path + '/continuation', payload)['status_code'], 400)
        for date_text in ('2026-11-24', '2026-02-30', '2026-11-28', 'not-a-date'):
            payload = dict(request_id='validation-append-001', expected_revision=revision, end_date=date_text)
            self.assertEqual(self.request(self.path + '/append', payload)['status_code'], 400)
        self.assertEqual(self.request(self.path + '/continuation', raw=b' ' * 16385, method='POST')['status_code'], 400)
        self.assertEqual(self.request(self.path + '/append', method='DELETE')['status_code'], 405)
        self.assertEqual(self.contents(), before)

    def test_revoked_source_blocks_new_capture_without_blocking_disable(self):
        payload, _ = self.configure()
        self.fixture.execute('UPDATE predictors SET share_predictions=0 WHERE id=1')
        revision = self.preview()['expected_revision']
        append = self.request(self.path + '/append', dict(request_id='revoked-append-001',
                              expected_revision=revision, end_date='2026-11-26'))
        self.assertEqual(append['status_code'], 403, append)
        disabled = self.request(self.path + '/continuation', dict(payload,
            request_id='disable-continuation-001', expected_revision=revision,
            capture_enabled=False, compute_enabled=False))
        self.assertEqual(disabled['status_code'], 200, disabled)
        self.assertFalse(disabled['json']['continuation']['capture_enabled'])

    def test_saved_receipts_can_be_recovered_after_source_access_is_revoked(self):
        configured, _ = self.configure()
        append = dict(request_id='receipt-after-revoke-001', expected_revision=self.preview()['expected_revision'],
                      end_date='2026-11-26')
        self.assertEqual(self.request(self.path + '/append', append)['status_code'], 202)
        self.fixture.execute('UPDATE predictors SET share_predictions=0 WHERE id=1')
        before = self.contents()
        for suffix, payload in (('continuation', configured), ('append', append)):
            result = self.request(self.path + '/' + suffix, payload)
            self.assertEqual(result['status_code'], 200, result)
            self.assertFalse(result['json']['created'])
        self.assertEqual(self.contents(), before)

    def test_api_append_runs_after_capture_and_preserves_baseline_report(self):
        next_day=datetime(2026,11,26,16,1,tzinfo=UTC)
        self.fixture.draw(1106,next_day)
        self.fixture.prediction(1106,next_day)
        self.configure()
        baseline = copy.deepcopy(self.request(self.path + '/segments')['json']['segments'][0])
        original = self.store.get_batch(1, self.bid)
        append = dict(request_id='actual-api-append-001', expected_revision=self.preview()['expected_revision'],
                      end_date='2026-11-26')
        self.assertEqual(self.request(self.path + '/append', append)['status_code'], 202)
        for offset in range(40):
            now = stamp(datetime.fromisoformat(NOW.replace('Z', '+00:00')) + timedelta(seconds=15 * offset))
            RuleLabWorker(self.store, self.source).tick(now=now)
            state = self.preview()['continuation']
            if state['calculated_through_issue'] == '1105' and not state['active_segment_id']:
                break
        self.assertEqual(state['calculated_through_issue'], '1105', state)
        self.assertFalse(state['compute_enabled'])
        self.assertFalse(state['active_segment_id'])
        segments = self.request(self.path + '/segments')['json']['segments']
        self.assertEqual(segments[-1], baseline)
        self.assertEqual(len(segments), 2)
        self.assertEqual(segments[0]['summary']['batches'][0]['processed_issues'], 3)
        self.assertEqual(self.store.get_batch(1, self.bid)['end_date'], original['end_date'])
        self.assertEqual(self.store.get_batch(1, self.bid)['experiment_count'], 2)
        self.assertEqual(self.request(self.path + '/append', append)['status_code'], 200)
        later_day=datetime(2026,11,27,1,tzinfo=UTC)
        self.fixture.draw(1107,later_day)
        self.fixture.prediction(1107,later_day)
        later=dict(request_id='actual-api-append-002',expected_revision=self.preview()['expected_revision'],
                   end_date='2026-11-27')
        self.assertEqual(self.request(self.path+'/append',later)['status_code'],202)
        for offset in range(40):
            now=stamp(datetime.fromisoformat(NOW.replace('Z','+00:00'))+timedelta(seconds=15*(offset+50)))
            RuleLabWorker(self.store,self.source).tick(now=now)
            state=self.preview()['continuation']
            if state['calculated_through_issue']=='1107' and not state['active_segment_id']:break
        self.assertEqual(state['calculated_through_date'],'2026-11-27',state)
        before_retry=self.contents()
        # 后续已有更晚段时，旧请求仍找回原回执，不能按当前水位判为日期倒退。
        self.assertEqual(self.request(self.path+'/append',append)['status_code'],200)
        self.assertEqual(self.contents(),before_retry)

    def test_study_children_reject_independent_continuation(self):
        study = self.store.create_study(1, self.bid, '对等续算', 'continuous-study-001', now=NOW)['study']
        child = study['children'][0]['batch_id']
        child_path = PREFIX + '/batches/' + str(child)
        response = self.request(child_path + '/continuation-preview')
        self.assertEqual(response['status_code'], 409, response)
        self.assertEqual(response['json']['code'], 'group_action_required')
        study_path = PREFIX + '/studies/' + str(study['id'])
        response = self.request(study_path + '/continuation-preview')
        self.assertEqual(response['status_code'], 200, response)
        self.assertFalse(response['json']['eligible'])
        self.assertEqual(self.request(study_path + '/segments')['status_code'], 200)
        self.assertEqual(self.request(study_path + '/input-coverage')['status_code'], 200)


if __name__ == '__main__':
    unittest.main()
