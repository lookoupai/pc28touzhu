"""结果型页面接口：真实 WSGI/SQLite，来源覆盖故障独立隔离。"""
from __future__ import annotations

import copy
import unittest
from datetime import datetime,timedelta
from unittest.mock import patch

from pc28touzhu.services.rule_lab_store import RuleLabError
from tests import test_rule_lab_continuous_api as fixture

PREFIX, NOW = fixture.PREFIX, fixture.NOW


class ResultsApiTests(unittest.TestCase):
    def setUp(self):
        self.tmp=fixture.tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        root=fixture.Path(self.tmp.name)
        self.fixture=fixture.SourceFixture(root/'source.db')
        self.fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        first=datetime(2026,11,24,16,tzinfo=fixture.UTC)
        for offset in range(-100,6):
            at=first+timedelta(seconds=210*offset)
            self.fixture.draw(1100+offset,at);self.fixture.prediction(1100+offset,at)
        self.fixture.draw(1106,first+timedelta(days=1));self.fixture.prediction(1106,first+timedelta(days=1))
        self.source=fixture.RuleLabSource(self.fixture.path)
        self.store=fixture.RuleLabStore(root/'lab'/'rule_lab.db')
        self.service=fixture.RuleLabService(self.store,self.source,resources=fixture.FakeResources())
        self.repo=fixture.FakeRepository()
        self.other=self.repo.create_user_record(username='other-admin',role='admin',status='active')
        self.regular=self.repo.create_user_record(username='member',role='user',status='active')
        self.app=fixture.PlatformApiApplication(self.repo,'executor-fixture','lab-fixture',rule_lab_service=self.service)
        config=fixture.normalize_batch_payload(dict(name='结果接口基线',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-25',end_date='2026-11-25',risk_policy=fixture.api_fixture.policy()),now=NOW)
        self.batch=self.store.create_batch(1,config,'真实来源',fixture.build_experiments('big_small')[:2],now=NOW)
        self.bid=self.batch['id'];self.path=PREFIX+'/batches/'+str(self.bid)
        self.store.set_batch_state(1,self.bid,'running',now=NOW)
        for _ in range(20):
            fixture.RuleLabWorker(self.store,self.source).tick(now=NOW)
            if self.store.get_batch(1,self.bid)['status']=='completed':break
        self.assertEqual(self.store.get_batch(1,self.bid)['status'],'completed')
    request = fixture.ContinuousApiTests.request
    contents = fixture.ContinuousApiTests.contents

    def experiment_path(self):
        eid=self.store.list_experiments(1,self.bid)[0]['id']
        return self.path+'/experiments/'+str(eid)

    def availability(self, *, available=True, basis='current_source', reason=''):
        dates={'start_date':'2026-11-25','end_date':'2026-11-25'}
        return dict(available=available,reason=reason,requested_range=dates,
                    verified_range=copy.deepcopy(dates) if available else None,input_basis=basis)

    def test_replay_preview_is_owned_read_only_and_proves_current_source(self):
        path=self.experiment_path()+'/replay-preview';before=self.contents()
        response=self.request(path)
        self.assertEqual(response['status_code'],200,response)
        self.assertEqual(response['headers']['Cache-Control'],'no-store')
        self.assertTrue(response['json']['available'],response)
        self.assertEqual(response['json']['input_basis'],'current_source')
        self.assertEqual(response['json']['verified_range'],{'start_date':'2026-11-25','end_date':'2026-11-25'})
        self.assertEqual(self.contents(),before)

    def test_replay_preview_preserves_auth_method_and_query_boundaries(self):
        path=self.experiment_path()+'/replay-preview';before=self.contents()
        for uid,expected in ((None,401),(self.regular['id'],403),(self.other['id'],404)):
            self.assertEqual(self.request(path,uid=uid)['status_code'],expected)
        self.assertEqual(self.request(path,query='start_date=2026-01-01')['status_code'],400)
        self.assertEqual(self.request(path,{})['status_code'],405)
        self.assertEqual(self.request(path.replace('/experiments/1/','/experiments/999999/'))['status_code'],404)
        self.assertEqual(self.contents(),before)

    def test_result_detail_keeps_profit_and_daily_data_without_history(self):
        before=self.store.list_experiments(1,self.bid)[0]
        response=self.request(self.experiment_path());self.assertEqual(response['status_code'],200,response)
        detail=response['json']
        self.assertEqual(detail['detail_policy'],'results_only')
        self.assertEqual(detail['retention_version'],1)
        self.assertEqual(detail['recent'],[]);self.assertEqual(detail['recent_rounds'],[])
        self.assertEqual(detail['pending_count'],0)
        self.assertEqual(detail['experiment']['net_profit'],before['net_profit'])
        self.assertEqual(sum(row['settled_count'] for row in detail['daily']),before['settled_count'])
        self.assertEqual(detail['monthly']['capital']['net_profit'],before['net_profit'])

    def test_preview_failure_does_not_create_or_rewrite_original(self):
        path=self.experiment_path();before=self.contents()
        unavailable=self.availability(available=False,basis='unavailable',reason='所选区间输入已清理')
        with patch.object(self.source,'replay_availability',return_value=unavailable,create=True):
            preview=self.request(path+'/replay-preview');self.assertEqual(preview['status_code'],200,preview)
            self.assertFalse(preview['json']['available']);self.assertEqual(preview['json']['input_basis'],'unavailable')
            result=self.request(path+'/replay',{'stake_amount':10,'risk_amount_mode':'keep'})
            self.assertEqual(result['status_code'],409,result)
            self.assertEqual(result['json']['code'],'replay_input_unavailable')
        self.assertEqual(self.contents(),before)

    def test_post_rechecks_input_after_a_successful_preview(self):
        path=self.experiment_path();before=self.store.list_experiments(1,self.bid)
        good=self.availability();bad=self.availability(available=False,basis='unavailable',reason='输入在预览后已清理')
        with patch.object(self.source,'replay_availability',side_effect=[good,bad],create=True) as verify:
            self.assertTrue(self.request(path+'/replay-preview')['json']['available'])
            result=self.request(path+'/replay',{'stake_amount':10,'risk_amount_mode':'scale'})
            self.assertEqual(result['status_code'],409,result)
            self.assertEqual(verify.call_count,2)
        self.assertEqual(len(self.store.list_batches(1)),1)
        self.assertEqual(self.store.list_experiments(1,self.bid),before)

    def test_source_timeout_is_retryable_and_does_not_claim_unavailable(self):
        before=self.contents()
        with patch.object(self.source,'replay_availability',side_effect=RuleLabError('source_unavailable','暂时无法核对输入',503),create=True):
            response=self.request(self.experiment_path()+'/replay-preview')
            self.assertEqual(response['status_code'],503,response)
            self.assertEqual(response['json']['code'],'source_unavailable')
        self.assertEqual(self.contents(),before)

    def test_active_continuation_does_not_fall_back_to_current_source(self):
        self.fixture.draw(1107,datetime(2026,11,26,16,tzinfo=fixture.UTC))
        self.fixture.prediction(1107,datetime(2026,11,26,16,tzinfo=fixture.UTC))
        preview=self.service.continuation_preview(1,'batch',self.bid,now=NOW)
        self.service.set_continuation(1,'batch',self.bid,dict(request_id='results-active-enable',
            expected_revision=preview['expected_revision'],capture_enabled=True,compute_enabled=False,
            daily_at='00:10',timezone='Asia/Shanghai'),now=NOW)
        for _ in range(10):
            fixture.RuleLabWorker(self.store,self.source).tick(now=NOW)
            preview=self.service.continuation_preview(1,'batch',self.bid,now=NOW)
            if preview['continuation']['captured_through_issue']=='1107':break
        self.service.request_append(1,'batch',self.bid,dict(request_id='results-active-append',
            expected_revision=preview['expected_revision'],end_date='2026-11-26'),now=NOW)
        self.store.prepare_due_appends(now=NOW)
        self.assertIsNotNone(self.store.get_batch(1,self.bid)['continuation']['active_segment_id'])
        before=self.contents()
        with patch.object(self.source,'replay_availability',wraps=self.source.replay_availability) as source_check:
            result=self.request(self.experiment_path()+'/replay-preview')
            self.assertEqual(result['status_code'],200,result)
            self.assertFalse(result['json']['available'])
            self.assertIn('当前追加',result['json']['reason'])
            self.assertEqual(source_check.call_count,0)
        self.assertEqual(self.contents(),before)

    def test_current_source_replay_keeps_frozen_price_and_marks_input_basis(self):
        path=self.experiment_path();before=self.store.list_experiments(1,self.bid)[0]
        response=self.request(path+'/replay',{'stake_amount':10,'risk_amount_mode':'keep'})
        self.assertEqual(response['status_code'],201,response)
        child=response['json']['batch']
        self.assertEqual(response['json']['input_basis'],'current_source')
        self.assertEqual(child['status'],'paused');self.assertEqual(child['stake_amount'],10)
        saved=self.store.list_experiments(1,child['id'])[0]
        self.assertEqual(saved['settlement_odds'],before['settlement_odds'])
        self.assertEqual(saved['settled_count'],0)
        cursor=self.store.get_work_item(child['id'])['batch']['cursor']
        self.assertEqual(cursor['replay_input_basis'],'current_source')
        self.assertTrue(cursor['replay_require_coverage'])
        self.assertEqual(self.store.list_experiments(1,self.bid)[0],before)

    def test_options_and_storage_describe_result_retention_without_read_side_effects(self):
        before=self.contents()
        options=self.request(PREFIX+'/options')['json'];self.assertEqual(options['limits']['detail_retention_days'],0)
        self.assertEqual(options['limits']['detail_policy'],'results_only')
        storage=self.request(PREFIX+'/storage');self.assertEqual(storage['status_code'],200,storage)
        self.assertEqual(storage['json']['retention']['mode'],'results_only')
        self.assertEqual(self.contents(),before)


if __name__=='__main__':unittest.main()
