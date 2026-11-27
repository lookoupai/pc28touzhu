"""长期区间、时间完成与未结算/输入质量必须分别表达。"""
import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.domain.rule_lab import empty_stats, build_experiments, normalize_batch_payload
from pc28touzhu.domain.rule_lab_monthly import monthly_report, report_window
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore
from tests.test_rule_lab_monthly import day
from tests.test_rule_lab_source import SourceFixture, stamp


class ContinuousMonthlyTests(unittest.TestCase):
    def batch(self, **changes):
        batch = dict(mode='backtest', start_date='2026-10-01', end_date='2026-11-26',
                     status='completed', processed_issues=300, stake_cents=100,
                     created_at='2026-11-27T04:00:00Z', started_at='2026-11-27T04:00:00Z',
                     skip_counts={}, continuation=dict(enabled=True, effective_end_date='2026-12-31',
                     calculated_through_date='2026-12-31', coverage_quality='complete', comparable=True))
        batch.update(changes)
        return batch

    def test_continuous_report_extends_beyond_frozen_original_end_date(self):
        batch = self.batch()
        before = copy.deepcopy(batch)
        rows = [day('2026-10-31', 100), day('2026-11-30', -200), day('2026-12-31', 300)]
        stats = empty_stats()
        stats.update(net_cents=200, settled_count=3, max_drawdown_cents=200)
        result = monthly_report(batch, stats, rows, now='2027-01-01T04:00:00Z',
                                observed_at='2027-01-01T00:01:00Z', cutoff_at='2027-01-01T00:01:00Z')
        self.assertEqual(result['window']['end'], '2026-12-31')
        self.assertEqual([row['month'] for row in result['rows']], ['2026-10', '2026-11', '2026-12'])
        self.assertEqual(sum(row['net_profit'] for row in result['rows']), 2)
        self.assertEqual(result['capital']['max_drawdown'], 2)
        self.assertEqual(batch, before)

    def test_processed_month_with_pending_is_time_complete_but_not_settlement_complete(self):
        batch = self.batch()
        batch['continuation']['pending_counts'] = {'2026-11': 2}
        result = monthly_report(batch, empty_stats(), [day('2026-11-30', 100)],
            now='2027-01-01T04:00:00Z', observed_at='2027-01-01T00:01:00Z',
            cutoff_at='2027-01-01T00:01:00Z', pending_since='2026-11-30T15:59:00Z')
        november = next(row for row in result['rows'] if row['month'] == '2026-11')
        self.assertTrue(november.get('time_complete'), november)
        self.assertEqual(november.get('pending_count'), 2)
        self.assertFalse(november['complete_month'])
        self.assertEqual(november['status'], 'pending')

    def test_known_skips_are_not_reported_as_complete_input_coverage(self):
        batch = self.batch(skip_counts={'missing_prediction': 1, 'late_prediction': 2})
        batch['continuation'].update(coverage_quality='known_skips', comparable=False)
        result = monthly_report(batch, empty_stats(), [], now='2027-01-01T04:00:00Z',
                                observed_at='2027-01-01T00:01:00Z', cutoff_at='2027-01-01T00:01:00Z')
        self.assertEqual(result.get('coverage_quality'), 'known_skips')
        self.assertFalse(result.get('comparable', True))
        self.assertEqual(result['input_warnings']['missing_prediction'], 1)

    def test_manual_current_day_append_does_not_claim_month_complete(self):
        batch = self.batch()
        batch['continuation'].update(effective_end_date='2026-12-31', calculated_through_date='2026-12-31')
        result = monthly_report(batch, empty_stats(), [day('2026-12-31', 100)],
            now='2026-12-31T04:00:00Z', observed_at='2026-12-31T03:59:00Z', cutoff_at='2026-12-31T04:00:00Z')
        self.assertIn('2026-12', [row['month'] for row in result['rows']])
        december = next(row for row in result['rows'] if row['month'] == '2026-12')
        self.assertFalse(december.get('time_complete', True))
        self.assertEqual(december['status'], 'partial')

    def test_requested_month_end_without_coverage_proof_is_not_complete(self):
        batch=self.batch()
        batch['continuation']['completed_through_date']='2026-11-26'
        result=monthly_report(batch,empty_stats(),[day('2026-12-01',100)],now='2027-01-02T04:00:00Z',
                              observed_at='2026-12-01T04:00:00Z',cutoff_at='2027-01-02T04:00:00Z')
        december=next(row for row in result['rows'] if row['month']=='2026-12')
        self.assertFalse(december['time_complete'])
        self.assertFalse(december['complete_month'])
        self.assertFalse(result['time_complete'])

    def test_ordinary_report_uses_its_original_window(self):
        batch = self.batch()
        batch.pop('continuation')
        self.assertEqual(report_window(batch, now='2027-01-01T04:00:00Z')[1].isoformat(), '2026-11-26')


class ContinuousMoneyReplayTests(unittest.TestCase):
    def test_long_history_results_survive_cleanup_without_promising_full_replay(self):
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp)
            fixture=SourceFixture(root/'source.db')
            fixture.execute('''CREATE TABLE pc28_prediction_daily_summary (
                predictor_id INTEGER,summary_date TEXT,total_predictions INTEGER,
                PRIMARY KEY(predictor_id,summary_date))''')
            source=RuleLabSource(fixture.path)
            store=RuleLabStore(root/'lab'/'lab.db')
            service=RuleLabService(store,source)
            # 北京时间8月1日零点，前100期都在原回测日期之前，构成真实暖机窗口。
            first=datetime(2026,7,31,16,0,tzinfo=timezone.utc)
            for offset in range(-100,1):
                opened=first+timedelta(seconds=210*offset)
                fixture.draw(1100+offset,opened)
                fixture.prediction(1100+offset,opened)
            for offset in range(1,103):
                opened=first+timedelta(days=offset)
                fixture.draw(1100+offset,opened)
                fixture.prediction(1100+offset,opened)
            now='2026-11-12T04:00:00Z'
            config=normalize_batch_payload(dict(name='长期原版',mode='backtest',predictor_id=1,metric='big_small',
                stake_amount=1,start_date='2026-08-01',end_date='2026-08-01'),now=now)
            parent=store.create_batch(1,config,'公开来源',build_experiments('big_small')[:1],now=now)
            bid=parent['id']
            def run_until(predicate):
                for tick in range(80):
                    clock=stamp(datetime.fromisoformat(now.replace('Z','+00:00'))+timedelta(seconds=15*tick))
                    RuleLabWorker(store,source).tick(now=clock)
                    if predicate():return
                self.fail('长期测试未到达预期状态')
            store.set_batch_state(1,bid,'running',now=now)
            run_until(lambda:store.get_batch(1,bid)['status']=='completed')
            preview=service.continuation_preview(1,'batch',bid,now=now)
            self.assertTrue(preview['eligible'],preview)
            service.set_continuation(1,'batch',bid,dict(request_id='long-replay-configure',
                expected_revision=preview['expected_revision'],capture_enabled=True,compute_enabled=False,
                daily_at='00:10',timezone='Asia/Shanghai'),now=now)
            run_until(lambda:service.continuation_preview(1,'batch',bid,now=now)['continuation']['captured_through_issue']=='1202')
            historical_months=[]
            for end in ('2026-10-30','2026-11-10'):
                preview=service.continuation_preview(1,'batch',bid,now=now)
                service.request_append(1,'batch',bid,dict(request_id='long-replay-'+end,
                    expected_revision=preview['expected_revision'],end_date=end),now=now)
                run_until(lambda:store.get_batch(1,bid)['status']=='completed' and
                          not service.continuation_preview(1,'batch',bid,now=now)['continuation']['active_segment_id'] and
                          service.continuation_preview(1,'batch',bid,now=now)['latest_request']['status']=='completed')
                eid=store.list_experiments(1,bid)[0]['id']
                months=service.experiment_detail(1,bid,eid)['monthly']['rows']
                if not historical_months:
                    historical_months=[row for row in months if row['month'] in ('2026-08','2026-09')]
                    self.assertEqual(len(historical_months),2)
                else:
                    self.assertEqual([row for row in months if row['month'] in ('2026-08','2026-09')],historical_months)
            original=store.list_experiments(1,bid)[0]
            eid=original['id']
            monthly=service.experiment_detail(1,bid,eid)['monthly']
            self.assertEqual(monthly['window']['end'],'2026-11-10')
            self.assertIn('2026-10',[row['month'] for row in monthly['rows']])
            with sqlite3.connect(store.db_path) as connection:
                before=connection.execute('SELECT stats_json,risk_runtime_json FROM lab_experiments WHERE id=?',(eid,)).fetchone()
            # 已消费A按新策略不再永久保留；旧成绩可读，但不能承诺原区间重测。
            fixture.execute('DELETE FROM predictions')
            fixture.execute('DELETE FROM lottery_draws')
            available=service.replay_preview(1,bid,eid,now=now)
            self.assertFalse(available['available']);self.assertEqual(available['input_basis'],'unavailable')
            with self.assertRaises(RuleLabError) as denied:
                service.replay_experiment(1,bid,eid,dict(stake_amount=10,risk_amount_mode='keep'),now=now)
            self.assertEqual(denied.exception.code,'replay_input_unavailable')
            self.assertEqual(len(store.list_batches(1)),1)
            after_monthly=service.experiment_detail(1,bid,eid)['monthly']
            self.assertEqual(after_monthly['window']['end'],'2026-11-10')
            self.assertEqual(after_monthly['rows'],monthly['rows'])
            self.assertEqual(after_monthly['capital']['net_profit'],original['net_profit'])
            with sqlite3.connect(store.db_path) as connection:
                after=connection.execute('SELECT stats_json,risk_runtime_json FROM lab_experiments WHERE id=?',(eid,)).fetchone()
            self.assertEqual(before,after)


if __name__ == '__main__':
    unittest.main()
