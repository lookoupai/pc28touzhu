"""Observation windows and integer-money summaries, independent of HTTP or storage."""
from __future__ import annotations

import copy
import importlib
import unittest
from datetime import date, timedelta

from pc28touzhu.domain.rule_lab import empty_stats
from pc28touzhu.domain.rule_lab_monthly import monthly_report

NOW = '2026-11-28T04:00:00Z'


def day(stamp, net, stake=100, count=1):
    return dict(stat_date=stamp, net_cents=net, stake_cents=stake, settled_count=count,
                hit_count=int(net > 0), refund_count=int(net == 0), miss_count=int(net < 0))


class ObservationDomainTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        try:
            cls.module = importlib.import_module('pc28touzhu.domain.rule_lab_observation')
        except ModuleNotFoundError:
            cls.module = None

    def setUp(self):
        self.assertIsNotNone(self.module, 'Observation statistics have not been implemented')

    def report(self, daily, *, months=2, include_partial=True, batch=None, **changes):
        config = dict(mode='backtest', start_date='2026-10-01', end_date='2026-11-27', status='completed',
                      processed_issues=23316, stake_cents=100, created_at=NOW, started_at=NOW,
                      skip_counts={}, cursor={}, continuation={'enabled': False})
        config.update(batch or {})
        stats=empty_stats()
        for row in daily:
            for key in ('net_cents','stake_cents','settled_count'):
                stats[key] += row[key]
        totals={key:stats[key] for key in ('net_cents','stake_cents','settled_count')}
        options=dict(observed_at='2026-11-27T15:59:00Z',cutoff_at=NOW,
                     first_observed_at='2026-10-01T00:05:00+08:00',history_totals=totals)
        options.update(changes)
        window=self.module.observation_window(config['end_date'], months=months,
                                             include_partial=include_partial, now=NOW)
        return self.module.build_observation(config, stats, daily, window=window, now=NOW, **options)

    def test_contiguous_calendar_months_and_complete_only_do_not_skip_missing_month(self):
        rows=[day('2026-10-02',100),day('2026-11-02',200)]
        report=self.report(rows,include_partial=False)
        self.assertEqual([r['month'] for r in report['months']],['2026-09','2026-10'])
        self.assertEqual([r['status'] for r in report['months']],['missing','complete'])
        self.assertEqual(report['window']['available_months'],1)
        self.assertEqual(report['window']['complete_months'],1)
        self.assertEqual(report['window']['requested_months'],2)
        self.assertIsNone(report['months'][0]['roi_percent'])

    def test_raw_cents_weighted_roi_and_daily_drawdown(self):
        rows=[day('2026-10-02',100,100),day('2026-10-03',-300,500),day('2026-10-04',100,200),
              day('2026-11-02',599,20000,200)]
        report=self.report(rows)
        october,november=report['months']
        self.assertEqual(october['net_cents'],-100)
        self.assertEqual(october['stake_cents'],800)
        self.assertEqual(october['roi_percent'],-12.5)
        self.assertEqual(october['daily_close_drawdown_cents'],300)
        self.assertEqual(november['net_cents'],599)
        self.assertEqual(november['stake_cents'],20000)
        self.assertLess(november['net_cents']*100,3*november['stake_cents'])
        self.assertEqual(report['window']['complete_months'],1)
        self.assertTrue(report['window']['has_partial'])

    def test_monthly_completeness_matches_existing_report(self):
        rows=[day('2026-10-02',100),day('2026-11-02',100)]
        for status,observed,cutoff,end in [
            ('completed','2026-10-31T04:00:00Z','2026-10-31T04:05:00Z','2026-10-31'),
            ('completed','2026-10-31T15:55:00Z','2026-11-01T00:00:00Z','2026-10-31'),
            ('running','2026-11-15T04:00:00Z',NOW,'2026-11-27')]:
            with self.subTest(status=status,observed=observed):
                batch=dict(mode='backtest',start_date='2026-10-01',end_date=end,status=status,
                           processed_issues=100,stake_cents=100,created_at=NOW,started_at=NOW,skip_counts={})
                legacy=monthly_report(batch,empty_stats(),rows,now=NOW,observed_at=observed,cutoff_at=cutoff)
                result=self.report(rows,batch=batch,observed_at=observed,cutoff_at=cutoff)
                actual={r['month']:r for r in result['months']}
                for expected in legacy['rows']:
                    self.assertEqual(actual[expected['month']]['complete_month'],expected['complete_month'])

    def test_unprocessed_window_and_recent_periods_are_not_future_zeroes(self):
        result=self.report([day('2026-10-02',100)],batch={'status':'running'},
                           observed_at='2026-10-15T02:00:00Z',cutoff_at=None)
        self.assertEqual(result['months'][-1]['status'],'not_replayed')
        self.assertFalse(result['window']['period_reached'])
        self.assertEqual(result['window']['actual_through_date'],'2026-10-15')
        for key in ('recent','previous'):
            self.assertFalse(result[key]['available'])
            self.assertIsNone(result[key]['settled_count'])
            self.assertIsNone(result[key]['net_cents'])
            self.assertIsNone(result[key]['roi_percent'])

    def test_delayed_first_observation_midday_does_not_cover_first_recent_day(self):
        result=self.report([day('2026-11-14',100),day('2026-11-27',200)],months=1,
                           batch={'start_date':'2026-11-01'},
                           first_observed_at='2026-11-14T12:00:00+08:00')
        self.assertEqual(result['recent']['start_date'],'2026-11-14')
        self.assertTrue(result['window']['period_reached'])
        self.assertFalse(result['recent']['available'])
        for key in ('settled_count','stake_cents','net_cents','roi_percent'):
            self.assertIsNone(result['recent'][key])

    def test_first_scheduled_draw_on_configured_start_day_preserves_recent_coverage(self):
        result=self.report([day('2026-11-14',100),day('2026-11-27',200)],months=1,
                           batch={'start_date':'2026-11-14'},
                           first_observed_at='2026-11-14T00:05:00+08:00')
        self.assertTrue(result['recent']['available'])
        self.assertEqual(result['recent']['settled_count'],2)
        self.assertEqual(result['recent']['net_cents'],300)

    def test_pending_is_scoped_to_exact_window_and_recent_period(self):
        rows=[day('2026-10-02',100),day('2026-11-02',200)]
        outside=self.report(rows,pending_days={'2026-09-30':1,'2026-12-01':1})
        self.assertNotIn('window_pending',outside['quality']['blockers'])
        self.assertTrue(outside['recent']['available'])
        pending=self.report(rows,pending_days={'2026-11-20':1})
        self.assertIn('window_pending',pending['quality']['blockers'])
        self.assertFalse(pending['recent']['available'])
        self.assertTrue(pending['previous']['available'])
        self.assertEqual(pending['months'][-1]['pending_count'],1)

    def test_recovery_counts_calendar_days_and_current_streak(self):
        rows=[day('2026-10-01',-100),day('2026-10-03',100),day('2026-11-26',-200)]
        result=self.report(rows)
        self.assertEqual(result['summary']['longest_underwater_days'],3)
        self.assertEqual(result['summary']['current_underwater_days'],2)
        self.assertFalse(result['summary']['recovery_left_censored'])
        later=self.report(rows,months=1)
        self.assertTrue(later['summary']['recovery_left_censored'])

    def test_best_three_days_only_excludes_positive_days(self):
        rows=[day('2026-10-01',1000),day('2026-10-02',-800),day('2026-10-03',500),
              day('2026-11-01',100),day('2026-11-02',-50)]
        result=self.report(rows)
        self.assertEqual(result['summary']['net_cents'],750)
        self.assertEqual(result['summary']['best_three_days_cents'],1600)
        self.assertEqual(result['summary']['net_without_best_three_days_cents'],-850)
        empty=self.report([])
        self.assertIsNone(empty['summary']['longest_underwater_days'])
        self.assertIsNone(empty['summary']['min_month_drawdown_cents'])

    def test_quality_blocks_bad_history_but_known_skips_only_warn(self):
        rows=[day('2026-10-01',100)]
        base=self.report(rows,batch={'skip_counts':{'insufficient_history':100}})
        self.assertEqual(base['quality']['blockers'],[])
        self.assertIn('known_skips',base['quality']['warnings'])
        broken=self.report(rows,history_totals={'net_cents':100,'stake_cents':0,'settled_count':0},
                           legacy_engine=True,study_comparable=False,
                           batch={'cursor':{'input_expired':True,'evidence':{'conflict':True}}})
        self.assertTrue(set(['legacy_engine','input_conflict','archive_gap','history_mismatch','study_not_comparable']).issubset(broken['quality']['blockers']))

    def test_cross_year_and_leap_month_windows_are_bounded(self):
        window=self.module.observation_window('2024-03-15',months=3,include_partial=False,now='2024-03-16T00:00:00Z')
        self.assertEqual((window['start_month'],window['end_month'],window['end_date']),('2023-12','2024-02','2024-02-29'))
        for months in (0,13,True,2.5):
            with self.assertRaises(ValueError):self.module.observation_window('2026-11-27',months=months,include_partial=True,now=NOW)
        with self.assertRaises(ValueError):self.module.observation_window('2026-11-27',months=2,include_partial=1,now=NOW)

    def test_no_input_mutation(self):
        rows=[day('2026-10-02',0),day('2026-11-02',100)]
        original=copy.deepcopy(rows)
        result=self.report(rows)
        self.assertEqual(rows,original)
        self.assertEqual(result['months'][0]['roi_percent'],0)
        self.assertEqual(result['months'][0]['daily_close_drawdown_cents'],0)


if __name__=='__main__':unittest.main()
