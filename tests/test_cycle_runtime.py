"""真实SQLite派单/结算链上的手动与自动周期隔离。"""
from __future__ import annotations

import copy
import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from tests.launch_fixtures import create_legacy_rule
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.auto_trigger_service import run_auto_trigger_cycle, update_auto_trigger_rule
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.pc28_auto_settlement_service import _resolve_pending_win_streak_cycles
from pc28touzhu.services.platform_service import resolve_pending_pc28_progressions_from_draws, restart_subscription_cycle


class CycleFixture(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = DatabaseRepository(os.path.join(self.tmp.name, 'cycle.db'))
        self.repo.initialize_database()
        self.user_id = self.repo.create_user_record(username='cycle-owner', status='active')['id']
        self.source = self.repo.create_source_record(owner_user_id=self.user_id, source_type='ai_trading_simulator_export', name='来源', status='active', config={'fetch': {'url': 'https://example.test/api/export/predictors/1/signals'}})
        self.targets = [self.repo.create_delivery_target_record(user_id=self.user_id, executor_type='telegram_group', target_key='-100%s' % n, target_name='群%s' % n, status='active') for n in (1, 2, 3)]
        self.strategy = {'play_filter': {'mode': 'all'}, 'staking_policy': {'mode': 'win_streak_roll', 'structure': 'B', 'base_amount': 10, 'target_streak': 1, 'roll_mode': 'chain', 'stop_policy': {'on_target': 'close_day', 'on_bust': 'close_day', 'max_cycles_per_day': 1}}, 'settlement_policy': {'rule_source': 'subscription_fixed', 'settlement_rule_id': 'pc28_fullpay_netdisk_regular'}, 'dispatch': {'delivery_target_ids': [self.targets[0]['id']]}}
        self.sub = self.repo.create_subscription_record(user_id=self.user_id, source_id=self.source['id'], strategy=self.strategy, status='standby', name='过关方案')
        self.now = datetime(2026, 11, 4, 10, 0, tzinfo=timezone.utc)
        self.latest = 4000000
        for target, func in [('pc28touzhu.services.dispatch_service._utc_now', lambda: self.now), ('pc28touzhu.services.dispatch_service._shanghai_today', lambda: self.day), ('pc28touzhu.executor.db_repository._utc_now_iso', lambda: self.now.isoformat().replace('+00:00', 'Z'))]:
            p = patch(target, side_effect=func)
            p.start()
            self.addCleanup(p.stop)

    @property
    def day(self):
        return self.now.astimezone(timezone(timedelta(hours=8))).strftime('%Y-%m-%d')

    def signal(self, issue=None):
        return self.repo.create_signal_record(source_id=self.source['id'], lottery_type='pc28', issue_no=str(issue or self.latest + 1), bet_type='combo', bet_value='小单', published_at=self.now.isoformat())

    def rule(self, *, targets=None, limit=2, on_target='wait_condition', on_bust='wait_condition', dispatch=True, cooldown=0, subscriptions=None):
        return create_legacy_rule(self.repo, user_id=self.user_id, payload={'name': '条件过关', 'scope_mode': 'selected_subscriptions', 'subscription_ids': subscriptions or [self.sub['id']], 'conditions': [{'type': 'miss_streak', 'metric': 'combo', 'operator': 'gte', 'threshold': 2}], 'cooldown_issues': cooldown, 'action': {'dispatch_latest_signal': dispatch, 'play_filter_action': 'keep'}, 'routes': [{'delivery_target_id': target['id']} for target in (targets or [])], 'cycle_policy': {'mode': 'rule', 'max_starts_per_day': limit, 'on_target': on_target, 'on_bust': on_bust}})['item']

    def cycle(self, rule=None, *, matches=True):
        performance = {'latest_settled_issue': str(self.latest), 'metrics': {'combo': {'streaks': {'current_miss_streak': 2 if matches else 0}}}}
        return run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule['id'] if rule else None, now=self.now, draw_clock={'latest_issue_no': str(self.latest), 'latest_open_time': (self.now-timedelta(seconds=20)).isoformat(), 'fetched_at': self.now.isoformat(), 'stale': False}, fetcher=lambda _: copy.deepcopy(performance))

    def deliver_and_settle(self, issue, triplet=(5, 8, 2)):
        for job in self.repo.list_execution_jobs(user_id=self.user_id):
            if job['issue_no'] == str(issue) and job['status'] == 'pending':
                self.repo.report_job_result(str(job['id']), 'fixture', 1, 'delivered', 'message-%s' % job['id'], self.now.isoformat(), {}, None)
        resolve_pending_pc28_progressions_from_draws(self.repo, user_id=self.user_id, draw_items=[{'issue_no': str(issue), 'draw_context': {'triplet': list(triplet)}}])
        return _resolve_pending_win_streak_cycles(self.repo)

    def session(self, rule):
        return self.repo.get_auto_trigger_cycle_session(rule_id=rule['id'], subscription_id=self.sub['id'], user_id=self.user_id)

    def advance(self, count=1):
        self.latest += count
        self.now += timedelta(seconds=210 * count)


class CycleRuntimeTests(CycleFixture):
    def test_manual_and_two_rule_states_are_independent(self):
        self.repo.update_subscription_status(subscription_id=self.sub['id'], user_id=self.user_id, status='active')
        signal = self.signal()
        manual = dispatch_signal(self.repo, signal['id'], subscription_id=self.sub['id'])
        self.assertEqual(manual['created_count'], 2)
        before = self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])
        a = self.rule(targets=[self.targets[1]])
        b = self.rule(targets=[self.targets[2]])
        self.assertEqual(self.cycle()['summary']['triggered_count'], 2)
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id']), before)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 6)
        self.deliver_and_settle(self.latest+1)
        self.assertTrue(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])['day_closed'])
        self.assertEqual(self.session(a)['status'], 'waiting')
        self.assertEqual(self.session(b)['status'], 'waiting')

    def test_new_round_waits_for_new_evidence_and_daily_quota(self):
        rule = self.rule(targets=[self.targets[1]], limit=2)
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        self.deliver_and_settle(self.latest+1)
        self.assertEqual(self.session(rule)['status'], 'waiting')
        self.advance()
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        self.advance()
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        self.deliver_and_settle(self.latest+1)
        self.assertEqual(self.session(rule)['status'], 'day_closed')
        self.advance(2)
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 0)
        quota = self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'], subscription_id=self.sub['id'], user_id=self.user_id, stat_date=self.day)
        self.assertEqual(quota['starts_count'], 2)

    def test_new_cycle_bypasses_condition_and_trigger_cooldown(self):
        rule = self.rule(targets=[self.targets[1]], on_target='new_cycle', cooldown=100)
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        self.deliver_and_settle(self.latest+1)
        self.assertEqual(self.session(rule)['status'], 'continuation')
        self.advance()
        self.signal()
        self.assertEqual(self.cycle(rule, matches=False)['summary']['triggered_count'], 1)
        jobs = [j for j in self.repo.list_execution_jobs(user_id=self.user_id) if j['issue_no'] == str(self.latest+1)]
        self.assertEqual(sum(j['stake_plan']['amount'] for j in jobs), 10)

    def test_two_routes_start_together_but_count_once(self):
        rule = self.rule(targets=self.targets[1:])
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 4)
        messages = self.repo.pull_ready_jobs('test-executor')
        self.assertEqual(len(messages), 2)
        self.assertEqual({j['message_text'] for j in messages}, {'单7 大双3'})
        self.assertEqual({j['target']['key'] for j in messages}, {t['target_key'] for t in self.targets[1:]})
        first = messages[0]
        self.repo.report_job_result(first['job_id'], 'test-executor', 1, 'delivered', 'shared-message', self.now.isoformat(), {}, None)
        remaining = self.repo.pull_ready_jobs('test-executor', limit=1)
        self.assertEqual(len(remaining), 1)
        self.assertNotEqual(first['target']['key'], remaining[0]['target']['key'])
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'], subscription_id=self.sub['id'], user_id=self.user_id, stat_date=self.day)['starts_count'], 1)

    def test_no_route_uses_auto_ledger_and_leaves_manual_standby(self):
        rule = self.rule()
        self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'], 1)
        self.assertEqual(self.repo.get_subscription(self.sub['id'])['status'], 'standby')
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 2)
        self.assertTrue(all(j['auto_trigger_route_id'] for j in jobs))
        self.assertEqual([j['message_text'] for j in self.repo.pull_ready_jobs('test-executor', limit=1)], ['单7 大双3'])
        self.assertIsNone(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id']))
        self.assertEqual(self.repo.get_auto_trigger_rule(rule['id'])['routes'], [])

    def test_target_conflict_does_not_consume_quota_or_claim_run(self):
        self.repo.update_subscription_status(subscription_id=self.sub['id'], user_id=self.user_id, status='active')
        signal=self.signal()
        self.assertEqual(dispatch_signal(self.repo,signal['id'],subscription_id=self.sub['id'])['created_count'],2)
        rule=self.rule(targets=[self.targets[0]])
        result=self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'],0)
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,stat_date=self.day)['starts_count'],0)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)),2)

    def test_manual_restart_does_not_resume_stopped_auto_rule(self):
        rule=self.rule(targets=[self.targets[1]], limit=1)
        self.signal()
        self.cycle(rule)
        self.deliver_and_settle(self.latest+1)
        before=copy.deepcopy(self.session(rule))
        restart_subscription_cycle(self.repo,subscription_id=self.sub['id'],user_id=self.user_id,payload={})
        self.assertEqual(self.session(rule),before)
        self.assertEqual(before['status'],'day_closed')

    def test_running_round_keeps_strategy_snapshot_when_manual_settings_change(self):
        self.strategy['staking_policy']['target_streak'] = 3
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='standby')
        rule=self.rule(targets=[self.targets[1]])
        self.signal()
        self.cycle(rule)
        self.deliver_and_settle(self.latest+1)
        changed=copy.deepcopy(self.strategy)
        changed['staking_policy']={'mode':'fixed','fixed_amount':999}
        changed['play_filter']={'mode':'selected','selected_keys':['big_small:大']}
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,source_id=self.source['id'],strategy=changed,status='standby')
        self.advance()
        signal=self.signal()
        result=dispatch_signal(self.repo,signal['id'])
        self.assertEqual(result['created_count'],2)
        jobs=[j for j in result['jobs'] if j.get('subscription_id')==self.sub['id']]
        self.assertEqual({j['stake_plan']['mode'] for j in jobs},{'win_streak_roll'})
        self.assertEqual(sum(j['stake_plan']['amount'] for j in jobs),13)

    def test_start_without_dispatch_continues_on_first_signal(self):
        rule=self.rule(targets=[self.targets[1]],dispatch=False)
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id),[])
        signal=self.signal()
        result=dispatch_signal(self.repo,signal['id'])
        self.assertEqual(result['created_count'],2)
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,stat_date=self.day)['starts_count'],1)

    def test_partial_route_conflict_does_not_join_later(self):
        self.repo.update_subscription_status(subscription_id=self.sub['id'],user_id=self.user_id,status='active')
        signal=self.signal()
        dispatch_signal(self.repo,signal['id'],subscription_id=self.sub['id'])
        self.repo.update_subscription_status(subscription_id=self.sub['id'],user_id=self.user_id,status='standby')
        self.strategy['staking_policy']['target_streak']=3
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='standby')
        rule=self.rule(targets=self.targets[:2])
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        participating=self.session(rule)['participating_route_ids']
        self.assertEqual(len(participating),1)
        self.deliver_and_settle(self.latest+1)
        self.advance()
        signal=self.signal()
        result=dispatch_signal(self.repo,signal['id'])
        self.assertEqual(result['created_count'],2)
        self.assertEqual({j['auto_trigger_route_id'] for j in result['jobs']},set(participating))

    def test_two_routes_wait_for_slowest_and_close_day_stops_new_periods(self):
        rule=self.rule(targets=self.targets[1:],on_target='close_day',limit=3)
        self.signal()
        self.cycle(rule)
        jobs=self.repo.list_execution_jobs(user_id=self.user_id)
        first_route=rule['routes'][0]['id']
        for job in jobs:
            self.repo.report_job_result(str(job['id']),'fixture',1,'delivered','m'+str(job['id']),self.now.isoformat(),{},None)
            if job['auto_trigger_route_id']==first_route:
                self.repo.settle_progression_event(subscription_id=self.sub['id'],user_id=self.user_id,progression_event_id=job['progression_event_id'],result_type='hit' if job['stake_plan']['leg']=='main' else 'miss')
        _resolve_pending_win_streak_cycles(self.repo)
        self.assertEqual(self.session(rule)['status'],'day_closed')
        self.assertIsNotNone(self.session(rule)['active_run_id'])
        self.advance()
        signal=self.signal()
        self.assertEqual(dispatch_signal(self.repo,signal['id'])['created_count'],0)
        self.deliver_and_settle(self.latest)
        self.assertIsNone(self.session(rule)['active_run_id'])

    def test_draw_streak_requires_evidence_after_completed_period(self):
        rule=self.rule(targets=[self.targets[1]])
        rule=update_auto_trigger_rule(self.repo,rule_id=rule['id'],user_id=self.user_id,payload={'conditions':[{'type':'draw_streak','metric':'combo','bet_value':'大双','operator':'gte','threshold':2}]})['item']
        def draw_items():
            return {'source':'test','items':[{'issue_no':str(self.latest-i),'result_number':16 if i<2 else 9} for i in range(3)]}
        with patch('pc28touzhu.services.auto_trigger_service.fetch_pc28_recent_draws_deep',side_effect=lambda **_:draw_items()):
            self.signal()
            self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
            self.deliver_and_settle(self.latest+1)
            self.advance()
            self.signal()
            self.assertEqual(self.cycle(rule)['summary']['triggered_count'],0)
            self.advance()
            self.signal()
            self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)

    def test_old_route_zero_round_drains_without_resetting_progress(self):
        """旧无路由自动轮在原ledger走完整轮，手动重置不得擦掉两期之间的进度。"""
        from pc28touzhu.domain.win_streak_roll import create_cycle_state, normalize_win_streak_roll_policy
        self.strategy['staking_policy']['target_streak']=2
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='active')
        rule=self.repo.create_auto_trigger_rule_record(user_id=self.user_id,name='旧无路由',subscription_ids=[self.sub['id']])
        run=self.repo.ensure_auto_trigger_rule_run(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,stat_date=self.day)
        owner={'rule_id':rule['id'],'subscription_id':self.sub['id'],'user_id':self.user_id}
        policy=normalize_win_streak_roll_policy(self.strategy['staking_policy'])
        self.repo.upsert_auto_trigger_cycle_session(**owner,values={'status':'running','active_run_id':run['id'],'start_stat_date':self.day,'policy':{'mode':'legacy'},'participating_route_ids':[0]})
        self.repo.upsert_auto_trigger_cycle_state(**owner,route_id=0,rule_run_id=run['id'],state=create_cycle_state(policy,stat_date=self.day),policy=policy)
        signal=self.signal()
        result=dispatch_signal(self.repo,signal['id'],subscription_id=self.sub['id'],auto_trigger_context={'rule_id':rule['id'],'rule_run_id':run['id'],'stat_date':self.day})
        self.assertEqual(result['created_count'],2)
        self.deliver_and_settle(self.latest+1)
        state=self.repo.get_auto_trigger_cycle_state(**owner,route_id=0)
        self.assertEqual(state['win_streak'],1)
        self.assertIsNone(state['pending_plan'])
        with self.assertRaises(ValueError):
            restart_subscription_cycle(self.repo,subscription_id=self.sub['id'],user_id=self.user_id,payload={})
        self.advance()
        next_signal = self.signal()
        self.repo.update_auto_trigger_rule_status(rule_id=rule['id'],user_id=self.user_id,status='inactive')
        self.assertEqual(dispatch_signal(self.repo,next_signal['id'],subscription_id=self.sub['id'])['created_count'],0)
        self.repo.update_auto_trigger_rule_status(rule_id=rule['id'],user_id=self.user_id,status='active')
        with patch('pc28touzhu.services.dispatch_service._shanghai_today',return_value='2026-11-05'):
            self.repo.stop_auto_trigger_rule_day(rule_id=rule['id'],user_id=self.user_id,stat_date='2026-11-05',reason='profit_target_hit')
            self.assertEqual(dispatch_signal(self.repo,next_signal['id'],subscription_id=self.sub['id'])['created_count'],0)
        self.assertEqual(dispatch_signal(self.repo,next_signal['id'],subscription_id=self.sub['id'])['created_count'],2)
        self.deliver_and_settle(self.latest+1)
        self.assertEqual(self.repo.get_auto_trigger_cycle_state(**owner,route_id=0)['win_streak'],2)
        self.assertEqual(self.session(rule)['status'],'day_closed')
        self.assertIsNone(self.repo.get_subscription_runtime_rule_run(subscription_id=self.sub['id'],user_id=self.user_id))

    def test_migration_blocked_manual_never_reopens_by_date_or_restart(self):
        from pc28touzhu.domain.win_streak_roll import create_cycle_state, normalize_win_streak_roll_policy
        policy=normalize_win_streak_roll_policy(self.strategy['staking_policy'])
        state=create_cycle_state(policy,stat_date='2026-11-03')
        state.update(status='stopped',day_closed=True,stop_reason='migration_owner_ambiguous')
        self.repo.upsert_subscription_cycle_state(subscription_id=self.sub['id'],user_id=self.user_id,state=state,policy=policy)
        self.repo.update_subscription_status(subscription_id=self.sub['id'],user_id=self.user_id,status='active')
        self.assertEqual(dispatch_signal(self.repo,self.signal()['id'],subscription_id=self.sub['id'])['created_count'],0)
        with self.assertRaisesRegex(ValueError,'迁移'):
            restart_subscription_cycle(self.repo,subscription_id=self.sub['id'],user_id=self.user_id,payload={})
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])['stop_reason'],'migration_owner_ambiguous')

    def test_completion_during_fetch_rechecks_latest_completed_issue_in_transaction(self):
        rule=self.rule(targets=[self.targets[1]],limit=3)
        self.signal()
        self.cycle(rule)
        self.deliver_and_settle(self.latest+1)
        self.advance(2)
        self.signal()
        def fetch_with_new_completion(_):
            self.repo.upsert_auto_trigger_cycle_session(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,
                values={'last_completed_issue':str(self.latest)})
            return {'latest_settled_issue':str(self.latest),'metrics':{'combo':{'streaks':{'current_miss_streak':2}}}}
        result=run_auto_trigger_cycle(self.repo,user_id=self.user_id,rule_id=rule['id'],now=self.now,
            draw_clock={'latest_issue_no':str(self.latest),'latest_open_time':(self.now-timedelta(seconds=20)).isoformat()},fetcher=fetch_with_new_completion)
        self.assertEqual(result['summary']['triggered_count'],0)
        self.assertEqual(result['rules'][0]['events'][0]['reason'],'cycle_waiting_new_issue')

    def test_last_completed_evidence_keeps_newest_route_when_older_route_finishes_last(self):
        from pc28touzhu.services.auto_trigger_cycle_service import finish_roll_session
        rule=self.rule(targets=self.targets[1:],limit=3)
        self.signal()
        self.cycle(rule)
        owner={'rule_id':rule['id'],'subscription_id':self.sub['id'],'user_id':self.user_id}
        states=[self.repo.get_auto_trigger_cycle_state(**owner,route_id=r['id']) for r in rule['routes']]
        for state in states:
            state.update(status='stopped',stop_reason='win_streak_target',last_cycle_stop_reason='cycle_target_hit')
        self.repo.upsert_auto_trigger_cycle_state(**owner,route_id=states[0]['route_id'],rule_run_id=states[0]['rule_run_id'],state=states[0],policy=states[0]['policy'])
        self.assertFalse(finish_roll_session(self.repo,states[0],completed_issue_no=str(self.latest+5))['completed'])
        self.repo.upsert_auto_trigger_cycle_state(**owner,route_id=states[1]['route_id'],rule_run_id=states[1]['rule_run_id'],state=states[1],policy=states[1]['policy'])
        self.assertTrue(finish_roll_session(self.repo,states[1],completed_issue_no=str(self.latest+3))['completed'])
        self.assertEqual(self.session(rule)['last_completed_issue'],str(self.latest+5))
