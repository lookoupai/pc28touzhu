"""Calendar coverage, durable monthly totals and owner isolation."""
import copy
import json
import sqlite3
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from pc28touzhu.domain.rule_lab import empty_stats, build_experiments, normalize_batch_payload
from pc28touzhu.domain.rule_lab_monthly import monthly_report
from pc28touzhu.services.rule_lab_store import RuleLabStore, RuleLabError

NOW='2026-11-26T15:00:00Z'


def day(stamp,net,stake=100,count=1):
    return dict(stat_date=stamp,net_cents=net,stake_cents=stake,settled_count=count,
                hit_count=int(net>0),miss_count=int(net<0),refund_count=int(net==0))


class MonthlyStatisticsTests(unittest.TestCase):
    def report(self,rows,**changes):
        batch=dict(mode='backtest',start_date='2026-09-01',end_date='2026-11-26',status='running',
                   processed_issues=100,stake_cents=100,created_at=NOW,started_at=NOW,skip_counts={})
        batch.update(changes.pop('batch',{}))
        return monthly_report(batch,empty_stats(),rows,now=NOW,
            observed_at=changes.pop('observed_at','2026-11-25T12:00:00Z'),**changes)

    def test_calendar_pnl_weighted_roi_and_daily_close_drawdown(self):
        rows=[day('2026-09-02',100,100),day('2026-09-03',-300,500),day('2026-09-04',100,200),
              day('2026-10-02',-50),day('2026-11-02',500)]
        result=self.report(rows)
        september=result['rows'][0]
        self.assertEqual(september['net_profit'],-1)
        self.assertEqual(september['roi_percent'],-12.5)
        self.assertEqual(september['daily_close_drawdown'],3)
        self.assertEqual(september['worst_day_profit'],-3)
        self.assertEqual((september['profitable_days'],september['losing_days']),(2,1))
        summary=result['summary']
        self.assertEqual((summary['eligible_months'],summary['losing_months'],summary['longest_losing_months']),(2,2,2))
        self.assertEqual(summary['best_month_profit'],-.5)
        self.assertEqual(summary['profitable_month_ratio'],0)
        self.assertEqual(result['rows'][2]['status'],'partial')

    def test_partial_start_and_current_month_excluded_even_if_profitable(self):
        result=self.report([day('2026-09-20',500),day('2026-10-01',98),day('2026-11-02',100)],
            batch={'start_date':'2026-09-15'})
        self.assertEqual([r['eligible'] for r in result['rows']],[False,True,False])
        self.assertEqual(result['summary']['profitable_month_ratio'],100)
        self.assertEqual(result['summary']['eligible_months'],1)

    def test_no_samples_and_future_replay_months_are_not_losses_or_flat_months(self):
        result=self.report([],observed_at='2026-10-10T04:00:00Z')
        self.assertEqual([r['status'] for r in result['rows']],['no_samples','partial','not_replayed'])
        self.assertEqual(result['summary']['flat_months'],0)
        self.assertIsNone(result['summary']['profitable_month_ratio'])
        self.assertFalse(result['capital']['available'])

    def test_last_day_midday_does_not_make_complete_month(self):
        result=self.report([day('2026-10-31',100)],batch={'status':'completed','end_date':'2026-10-31'},
                           observed_at='2026-10-31T04:00:00Z',cutoff_at='2026-10-31T04:05:00Z')
        self.assertFalse(result['rows'][-1]['eligible'])
        finished=self.report([day('2026-10-31',100)],batch={'status':'completed','end_date':'2026-10-31'},
                             observed_at='2026-10-31T15:55:00Z',cutoff_at='2026-11-01T00:00:00Z')
        self.assertTrue(finished['rows'][-1]['eligible'])

    def test_pending_settlement_and_missing_start_history_prevent_full_label(self):
        rows=[day('2026-09-10',100),day('2026-10-01',100)]
        missing=self.report(rows,first_observed_at='2026-09-10T00:00:00Z')
        self.assertFalse(missing['rows'][0]['eligible'])
        self.assertTrue(missing['rows'][1]['eligible'])
        pending=self.report(rows,pending_since='2026-10-31T12:00:00Z')
        self.assertTrue(pending['rows'][0]['eligible'])
        self.assertEqual(pending['rows'][1]['status'],'pending')

    def test_leap_year_year_boundary_and_loss_streak_gap(self):
        result=self.report([day('2023-12-31',-1),day('2024-02-29',-1)],
            batch={'start_date':'2023-12-01','end_date':'2024-03-02'},observed_at='2024-03-02T00:00:00Z')
        self.assertEqual(result['rows'][2]['calendar_days'],29)
        self.assertEqual(result['summary']['longest_losing_months'],1)
        self.assertEqual(result['summary']['worst_month_profit'],-.01)

    def test_live_window_is_bounded_and_started_midday_is_partial(self):
        result=self.report([],batch={'mode':'live','created_at':'2000-01-01T00:00:00Z','started_at':'2000-01-01T00:00:00Z'})
        self.assertEqual(len(result['rows']),120)
        self.assertTrue(result['window']['truncated'])
        self.assertEqual(result['rows'][0]['month'],'2016-12')
        result=self.report([day('2026-10-02',100)],batch={'mode':'live','started_at':'2026-10-01T00:00:00Z'})
        self.assertFalse(result['rows'][0]['eligible'])

    def test_months_with_refunds_only_are_flat_and_inputs_are_not_mutated(self):
        rows=[day('2026-09-02',0),day('2026-10-02',100)]
        original=copy.deepcopy(rows)
        result=self.report(rows)
        self.assertEqual(result['summary']['flat_months'],1)
        self.assertEqual(result['summary']['profitable_month_ratio'],50)
        self.assertEqual(rows,original)

    def test_equity_uses_all_daily_totals_in_date_order_and_reconciles(self):
        batch=dict(mode='backtest',start_date='2026-10-01',end_date='2026-11-26',status='completed',
                   processed_issues=100,stake_cents=100,created_at=NOW,started_at=NOW,skip_counts={})
        stats=empty_stats();stats.update(settled_count=3,net_cents=50)
        report=monthly_report(batch,stats,[day('2026-11-02',-250),day('2026-10-01',200),day('2026-10-05',100)],now=NOW)
        self.assertEqual([p['cumulative_net_cents'] for p in report['equity']['points']],[200,300,50])
        self.assertEqual(report['equity']['opening_net_cents'],0)
        self.assertTrue(report['equity']['complete_history'])
        partial=monthly_report(batch,stats,[day('2026-11-02',-250)],now=NOW)
        self.assertEqual(partial['equity']['opening_net_cents'],300)
        self.assertEqual(partial['equity']['points'][-1]['cumulative_net_cents'],50)
        self.assertFalse(partial['equity']['complete_history'])

    def test_truncated_curve_does_not_claim_whole_history_even_when_missing_net_is_zero(self):
        batch=dict(mode='live',status='running',processed_issues=100,stake_cents=100,
                   created_at='2000-01-01T00:00:00Z',started_at='2000-01-01T00:00:00Z',skip_counts={})
        stats=empty_stats();stats.update(settled_count=1,net_cents=100)
        report=monthly_report(batch,stats,[day('2026-11-01',100)],now=NOW)
        self.assertFalse(report['equity']['complete_history'])


class MonthlyLedgerTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.store=RuleLabStore(Path(self.tmp.name)/'lab.db');self.store.initialize()
        config=normalize_batch_payload(dict(name='monthly',mode='live',predictor_id=1,metric='big_small'),now='2026-01-01T00:00:00Z')
        self.batch=self.store.create_batch(1,config,'source',build_experiments('big_small'),now='2026-01-01T00:00:00Z')
        self.eid=self.store.list_experiments(1,self.batch['id'])[0]['id']

    def test_daily_ledger_beyond_ninety_rows_is_used_and_survives_pruning(self):
        stats=empty_stats();stats.update(settled_count=150,stake_cents=15000,net_cents=150,max_drawdown_cents=400)
        with sqlite3.connect(self.store.db_path) as c:
            for offset in range(150):
                stamp=(date(2026,1,1)+timedelta(days=offset)).isoformat()
                c.execute('INSERT INTO lab_daily(experiment_id,stat_date,settled_count,stake_cents,net_cents,hit_count) VALUES(?,?,1,100,1,1)',(self.eid,stamp))
            c.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?',(json.dumps(stats),self.eid))
            c.execute('UPDATE lab_batches SET processed_issues=150,cursor_json=? WHERE id=?',
                (json.dumps({'simulation_time':'2026-06-01T00:00:00Z'}),self.batch['id']))
        detail=self.store.get_experiment_detail(1,self.batch['id'],self.eid)
        self.assertEqual(len(detail['daily']),90)
        self.assertEqual(sum(r['settled_count'] for r in detail['monthly']['rows']),150)
        self.assertEqual(detail['monthly']['rows'][0]['net_profit'],.31)
        self.assertEqual(detail['monthly']['capital']['max_drawdown'],4)
        self.assertTrue(detail['monthly']['capital']['available'])
        self.store.prune(now='2027-01-01T00:00:00Z')
        self.assertEqual(self.store.get_experiment_detail(1,self.batch['id'],self.eid)['monthly'],detail['monthly'])
        for uid,bid,eid in [(2,self.batch['id'],self.eid),(1,self.batch['id'],9999)]:
            with self.assertRaises(RuleLabError) as cm:self.store.get_experiment_detail(uid,bid,eid)
            self.assertEqual(cm.exception.status,404)

    def test_settlement_month_is_beijing_date_and_snapshot_is_query_only(self):
        self.store.set_batch_state(1,self.batch['id'],'running',now='2026-10-01T00:00:00Z')
        work=self.store.get_work_item(self.batch['id'])
        decision=dict(experiment_id=self.eid,issue_no='1001',bet_value='大',stake_cents=100,
                      decided_at='2026-10-31T15:59:00Z',decision_mode='live')
        outcome=dict(experiment_id=self.eid,issue_no='1001',status='settled',settled_at='2026-10-31T16:01:00Z',
                     result_type='miss',net_cents=-100,direction_hit=False,refund_reason='',skip_reason='')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor={'last_issue_no':'1001'},now=NOW,
                               decisions=[decision],outcomes=[outcome],progress={'processed_issues_delta':1})
        result=self.store.get_experiment_detail(1,self.batch['id'],self.eid)
        months={r['month']:r for r in result['monthly']['rows']}
        self.assertEqual(months['2026-10']['settled_count'],0)
        self.assertEqual(months['2026-11']['net_profit'],-1)
        self.assertEqual(months['2026-11']['daily_close_drawdown'],1)

    def test_concurrent_commit_cannot_mix_monthly_and_experiment_snapshots(self):
        original=RuleLabStore._monthly_report
        def after_reader_started(c,batch,experiment):
            self.assertTrue(c.in_transaction)
            with self.assertRaises(sqlite3.OperationalError):
                c.execute('UPDATE lab_experiments SET name=name WHERE 0')
            with sqlite3.connect(self.store.db_path) as writer:
                stats=empty_stats();stats.update(settled_count=1,net_cents=98,stake_cents=100)
                writer.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?',(json.dumps(stats),self.eid))
                writer.execute('INSERT INTO lab_daily(experiment_id,stat_date,settled_count,net_cents,stake_cents) VALUES(?,?,1,98,100)',(self.eid,'2026-10-01'))
            return original(c,batch,experiment)
        with patch.object(RuleLabStore,'_monthly_report',staticmethod(after_reader_started)):
            detail=self.store.get_experiment_detail(1,self.batch['id'],self.eid)
        self.assertEqual(detail['experiment']['net_profit'],0)
        self.assertEqual(sum(r['net_profit'] for r in detail['monthly']['rows']),0)
        fresh=self.store.get_experiment_detail(1,self.batch['id'],self.eid)
        self.assertEqual(fresh['experiment']['net_profit'],.98)
        self.assertEqual(sum(r['net_profit'] for r in fresh['monthly']['rows']),.98)


if __name__=='__main__':unittest.main()
