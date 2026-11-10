from __future__ import annotations

import tempfile
import copy
import json
import sqlite3
import unittest
from pathlib import Path
from datetime import datetime, timezone, timedelta
from unittest.mock import patch

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.platform_service import create_subscription, update_subscription_status, update_subscription, list_subscriptions, restart_subscription_cycle, resolve_pending_pc28_progressions_from_draws
from pc28touzhu.services.auto_trigger_service import create_auto_trigger_rule, update_auto_trigger_rule_status, update_auto_trigger_rule, run_auto_trigger_cycle
from pc28touzhu.services.dispatch_service import dispatch_signal


class ExplicitStartTests(unittest.TestCase):
    def setUp(self):
        tmp=tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        self.repo=DatabaseRepository(str(Path(tmp.name)/'start.db')); self.repo.initialize_database()
        self.uid=self.repo.create_user('starter')
        self.source=self.repo.create_source_record(owner_user_id=self.uid,source_type='ai_trading_simulator_export',name='source',
            config={'fetch':{'url':'https://example.test/api/export/predictors/1/signals'}})
        self.target=self.repo.create_delivery_target_record(user_id=self.uid,executor_type='telegram_group',target_key='-1001',status='active')
        self.strategy={'play_filter':{'mode':'selected','selected_keys':['big_small:大','big_small:小']},
            'staking_policy':{'mode':'fixed','fixed_amount':10},'dispatch':{'delivery_target_ids':[self.target['id']]}}

    def sub(self, **extra):
        return create_subscription(self.repo,payload={'user_id':self.uid,'source_id':self.source['id'],'strategy_v2':self.strategy,**extra})['item']

    def signal(self, issue='1001'):
        return self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no=issue,bet_type='big_small',bet_value='大')

    def rule(self, sub, **extra):
        return create_auto_trigger_rule(self.repo,user_id=self.uid,payload={'name':'test rule','subscription_ids':[sub['id']],
            'conditions':[{'type':'miss_streak','metric':'big_small','operator':'gte','threshold':2}],
            'cooldown_issues':0,**extra})['item']

    def test_saved_subscription_never_dispatches(self):
        sub=self.sub()
        self.assertEqual(sub['status'],'inactive')
        self.assertEqual(dispatch_signal(self.repo,self.signal()['id'])['created_count'],0)

    def test_saved_rule_is_not_enabled(self):
        rule=self.rule(self.sub())
        self.assertEqual(rule['status'],'inactive')

    def test_explicit_direct_start_only_uses_new_signal(self):
        sub=self.sub(); old=self.signal()
        update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='active')
        self.assertEqual(dispatch_signal(self.repo,old['id'])['created_count'],0)
        self.assertEqual(dispatch_signal(self.repo,self.signal('1002')['id'])['created_count'],1)

    def test_enabling_rule_arms_subscription_atomically(self):
        sub=self.sub(); rule=self.rule(sub)
        update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='active')
        current=self.repo.get_subscription(sub['id'])
        self.assertEqual(current['status'],'standby')
        self.assertEqual(current['launch_mode'],'automatic')
        self.assertEqual(dispatch_signal(self.repo,self.signal()['id'])['created_count'],0)

    def test_save_config_keeps_launch_state(self):
        sub=self.sub(); update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='standby')
        update_subscription(self.repo,subscription_id=sub['id'],user_id=self.uid,
            payload={'source_id':self.source['id'],'strategy_v2':self.strategy,'status':'active'})
        self.assertEqual(self.repo.get_subscription(sub['id'])['status'],'standby')

    def test_rule_activation_rolls_back_on_foreign_subscription(self):
        sub=self.sub(); foreign=self.repo.create_user('other')
        other=self.repo.create_subscription_record(user_id=foreign,source_id=self.source['id'])
        rule=self.rule(sub,subscription_ids=[sub['id'],other['id']])
        with self.assertRaises(ValueError):
            update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='active')
        self.assertEqual(self.repo.get_subscription(sub['id'])['status'],'inactive')
        self.assertEqual(self.repo.get_auto_trigger_rule(rule['id'])['status'],'inactive')

    def test_manual_pending_prevents_rule_activation_without_touching_job(self):
        sub=self.sub(); update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='active')
        job=dispatch_signal(self.repo,self.signal()['id'])['jobs'][0]
        rule=self.rule(sub)
        with self.assertRaisesRegex(ValueError,'待发送或待结算'):
            update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='active')
        self.assertEqual(self.repo.get_execution_job(job['id'])['status'],'pending')
        self.assertEqual(self.repo.get_subscription(sub['id'])['status'],'active')

    def test_automatic_flat_keeps_standby_and_uses_isolated_route(self):
        sub=self.sub(); rule=self.rule(sub)
        update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='active')
        signal=self.signal()
        now=datetime.now(timezone.utc)
        outcome=run_auto_trigger_cycle(self.repo,user_id=self.uid,rule_id=rule['id'],now=now,
            draw_clock={'latest_issue_no':'1000','latest_open_time':(now-timedelta(seconds=10)).isoformat(),'fetched_at':now.isoformat(),'stale':False},
            fetcher=lambda _: {'latest_settled_issue':'1000','metrics':{'big_small':{'streaks':{'current_miss_streak':2}}}})
        self.assertEqual(outcome['summary']['triggered_count'],1,outcome)
        self.assertEqual(self.repo.get_subscription(sub['id'])['status'],'standby')
        jobs=self.repo.list_execution_jobs(user_id=self.uid)
        self.assertEqual(len(jobs),1)
        self.assertIsNotNone(jobs[0]['auto_trigger_route_id'])
        update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='inactive')
        self.assertEqual(dispatch_signal(self.repo,self.signal('1002')['id'])['created_count'],0)
        self.assertEqual(self.repo.get_execution_job(jobs[0]['id'])['status'],'pending')

    def test_all_scope_rule_does_not_arm_future_drafts(self):
        first=self.sub(); rule=self.rule(first,scope_mode='all_subscriptions',subscription_ids=[])
        update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='active')
        second=self.sub()
        self.assertEqual(self.repo.get_subscription(second['id'])['status'],'inactive')

    def test_migration_preserves_existing_status_and_is_idempotent(self):
        legacy=self.repo.create_subscription_record(user_id=self.uid,source_id=self.source['id'],status='active')
        self.repo.initialize_database();self.repo.initialize_database()
        self.assertEqual(self.repo.get_subscription(legacy['id'])['status'],'active')
        self.assertEqual(self.repo.get_subscription(legacy['id'])['launch_mode'],'legacy')

    def enable(self, sub, **extra):
        rule = self.rule(sub, **extra)
        return update_auto_trigger_rule_status(self.repo, rule_id=rule['id'], user_id=self.uid, status='active')['item']

    def run_rule(self, rule, latest='1000', now=None):
        now = now or datetime.now(timezone.utc)
        return run_auto_trigger_cycle(self.repo, user_id=self.uid, rule_id=rule['id'], now=now,
            draw_clock={'latest_issue_no':latest,'latest_open_time':(now-timedelta(seconds=10)).isoformat(), 'fetched_at':now.isoformat(),'stale':False},
            fetcher=lambda _: {'latest_settled_issue':latest,'metrics':{'big_small':{'streaks':{'current_miss_streak':2}}}})

    def test_create_ignores_requested_active_status(self):
        sub = self.sub(status='active')
        rule = self.rule(sub, status='active')
        self.assertEqual((sub['status'], sub['launch_mode'], rule['status']), ('inactive','unstarted','inactive'))
        self.signal()
        self.assertEqual(self.run_rule(rule)['summary']['triggered_count'], 0)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.uid), [])

    def test_edit_rule_cannot_enable_or_disable_it(self):
        sub = self.sub(); rule = self.rule(sub)
        def save(status):
            return update_auto_trigger_rule(self.repo, rule_id=rule['id'], user_id=self.uid,
                payload={'name':'renamed', 'status':status})['item']
        self.assertEqual(save('active')['status'], 'inactive')
        update_auto_trigger_rule_status(self.repo, rule_id=rule['id'], user_id=self.uid, status='active')
        self.assertEqual(save('inactive')['status'], 'active')
        self.assertEqual(self.repo.get_subscription(sub['id'])['status'], 'standby')

    def test_atomic_activation_rolls_back_after_partial_write_failure(self):
        first=self.sub(); second=self.sub(); rule=self.rule(first,subscription_ids=[first['id'],second['id']])
        original=DatabaseRepository.set_subscription_launch_mode
        def fail_on_second(repo, **kwargs):
            if kwargs['subscription_id'] == second['id']: raise RuntimeError('write failed')
            return original(repo, **kwargs)
        with patch.object(DatabaseRepository, 'set_subscription_launch_mode', fail_on_second):
            with self.assertRaisesRegex(RuntimeError, 'write failed'):
                update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='active')
        self.assertEqual(self.repo.get_subscription(first['id'])['launch_mode'], 'unstarted')
        self.assertEqual(self.repo.get_subscription(first['id'])['status'], 'inactive')
        self.assertEqual(self.repo.get_auto_trigger_rule(rule['id'])['status'], 'inactive')

    def test_martingale_automatic_route_continues_without_direct_dispatch(self):
        self.strategy['staking_policy']={'mode':'martingale','base_stake':10,'multiplier':2,'max_steps':5}
        sub=self.sub(); rule=self.enable(sub)
        self.signal(); self.assertEqual(self.run_rule(rule)['summary']['triggered_count'],1)
        first=self.repo.list_execution_jobs(user_id=self.uid)[0]
        self.repo.report_job_result(str(first['id']),'offline',1,'delivered','offline-only',datetime.now(timezone.utc).isoformat(),{},None)
        resolve_pending_pc28_progressions_from_draws(self.repo,user_id=self.uid,
            draw_items=[{'issue_no':'1001','draw_context':{'result_number':5}}])
        self.signal('1002'); self.run_rule(rule,latest='1001')
        jobs=self.repo.list_execution_jobs(user_id=self.uid)
        self.assertEqual(len(jobs),2)
        second=next(job for job in jobs if job['issue_no']=='1002')
        self.assertEqual(second['stake_plan']['amount'],20)
        self.assertTrue(all(job['auto_trigger_route_id'] for job in jobs))
        self.assertEqual(self.repo.get_subscription(sub['id'])['status'],'standby')

    def test_schedule_automatic_flat_keeps_standby(self):
        sub=self.sub()
        rule=self.enable(sub,trigger_mode='schedule',schedule={'timezone':'Asia/Shanghai',
            'weekdays':['Mon','Tue','Wed','Thu','Fri','Sat','Sun'],
            'windows':[{'id':'all','start':'00:00','end':'24:00'}]})
        self.signal(); self.assertEqual(self.run_rule(rule)['summary']['triggered_count'],1)
        self.assertEqual(self.repo.get_subscription(sub['id'])['status'],'standby')
        self.assertIsNotNone(self.repo.list_execution_jobs(user_id=self.uid)[0]['auto_trigger_route_id'])

    def test_roll_wait_condition_label_and_pause_preserve_settlement(self):
        from pc28touzhu.services.pc28_auto_settlement_service import _resolve_pending_win_streak_cycles
        self.strategy['staking_policy']={'mode':'win_streak_roll','structure':'single_play','play_type':'big_small',
            'selection_mode':'follow','base_amount':10,'target_streak':1,'roll_mode':'chain'}
        self.strategy['settlement_policy']={'rule_source':'subscription_fixed','settlement_rule_id':'pc28_fullpay_2_8_regular'}
        sub=self.sub(); rule=self.enable(sub,cycle_policy={'mode':'rule','max_starts_per_day':3,'on_target':'wait_condition','on_bust':'wait_condition'})
        self.signal(); self.assertEqual(self.run_rule(rule)['summary']['triggered_count'],1)
        job=self.repo.list_execution_jobs(user_id=self.uid)[0]
        before=self.repo.get_progression_event(job['progression_event_id'])
        self.assertEqual(list_subscriptions(self.repo,self.uid)['items'][0]['launch_view']['status_label'],'自动规则运行中')
        update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='inactive')
        changed=copy.deepcopy(self.strategy);changed['settlement_policy']['settlement_rule_id']='pc28_fullpay_2_0_regular'
        update_subscription(self.repo,subscription_id=sub['id'],user_id=self.uid,payload={'source_id':self.source['id'],'strategy_v2':changed})
        self.assertEqual(self.repo.get_progression_event(job['progression_event_id']),before)
        self.repo.report_job_result(str(job['id']),'offline',1,'delivered','offline-only',datetime.now(timezone.utc).isoformat(),{},None)
        resolve_pending_pc28_progressions_from_draws(self.repo,user_id=self.uid,draw_items=[{'issue_no':'1001','draw_context':{'result_number':20}}])
        _resolve_pending_win_streak_cycles(self.repo)
        settled=self.repo.get_progression_event(job['progression_event_id'])
        self.assertEqual(settled['net_delta'],18.4)  # 满赔2.8的大小实际赔率为2.84
        update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='active')
        self.assertEqual(list_subscriptions(self.repo,self.uid)['items'][0]['launch_view']['status_label'],'等待时间窗口／条件')
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=sub['id'],user_id=self.uid,
            stat_date=datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d'))['starts_count'],1)

    def test_automatic_plan_cannot_direct_start_or_restart(self):
        sub=self.sub(); self.enable(sub)
        with self.assertRaisesRegex(ValueError,'自动规则'):
            update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='active')
        with self.assertRaisesRegex(ValueError,'自动规则'):
            restart_subscription_cycle(self.repo,subscription_id=sub['id'],user_id=self.uid,payload={})

    def test_real_old_schema_migration_preserves_legacy_dispatch(self):
        legacy=self.repo.create_subscription_record(user_id=self.uid,source_id=self.source['id'],status='active',strategy=self.strategy)
        with sqlite3.connect(self.repo.db_path) as conn:
            conn.execute('ALTER TABLE user_subscriptions DROP COLUMN launch_mode')
            conn.execute('ALTER TABLE user_subscriptions DROP COLUMN direct_after_signal_id')
        self.repo.initialize_database(); self.repo.initialize_database()
        self.assertEqual(self.repo.get_subscription(legacy['id'])['launch_mode'],'legacy')
        self.assertEqual(dispatch_signal(self.repo,self.signal()['id'])['created_count'],1)

    def test_foreign_user_cannot_start_or_arm_owned_plan(self):
        sub=self.sub();rule=self.rule(sub);foreign=self.repo.create_user('foreign')
        with self.assertRaises(ValueError):
            update_subscription_status(self.repo,subscription_id=sub['id'],user_id=foreign,status='active')
        with self.assertRaises(ValueError):
            update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=foreign,status='active')
        self.assertEqual(self.repo.get_subscription(sub['id'])['launch_mode'],'unstarted')

    def test_edited_draft_roll_uses_latest_config_on_first_start(self):
        self.strategy['staking_policy']={'mode':'win_streak_roll','structure':'single_play','play_type':'big_small',
            'selection_mode':'follow','base_amount':10,'target_streak':1,'roll_mode':'chain'}
        sub=self.sub()
        changed=copy.deepcopy(self.strategy)
        changed['staking_policy'].update(base_amount=30,target_streak=5)
        update_subscription(self.repo,subscription_id=sub['id'],user_id=self.uid,payload={'source_id':self.source['id'],'strategy_v2':changed})
        update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='active')
        job=dispatch_signal(self.repo,self.signal()['id'])['jobs'][0]
        self.assertEqual(job['stake_plan']['amount'],30)
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=sub['id'])['target_streak'],5)

    def test_direct_start_is_idempotent_and_explicitly_adopts_legacy_plan(self):
        legacy=self.repo.create_subscription_record(user_id=self.uid,source_id=self.source['id'],status='active',strategy=self.strategy)
        old=self.signal()
        update_subscription_status(self.repo,subscription_id=legacy['id'],user_id=self.uid,status='active')
        self.assertEqual(self.repo.get_subscription(legacy['id'])['launch_mode'],'direct')
        fresh=self.signal('1002')
        update_subscription_status(self.repo,subscription_id=legacy['id'],user_id=self.uid,status='active')
        self.assertEqual(dispatch_signal(self.repo,old['id'])['created_count'],0)
        self.assertEqual(dispatch_signal(self.repo,fresh['id'])['created_count'],1)

    def test_launch_view_distinguishes_rule_override_from_subscription_default(self):
        self.strategy['settlement_policy']={'rule_source':'subscription_fixed','settlement_rule_id':'pc28_fullpay_2_8_regular'}
        sub=self.sub()
        rule=self.enable(sub,routes=[{'delivery_target_id':self.target['id'],'settlement_mode':'override',
            'settlement_policy':{'rule_source':'subscription_fixed','settlement_rule_id':'pc28_fullpay_2_0_regular'}}])
        payload=list_subscriptions(self.repo,self.uid)
        self.assertEqual(payload['launch_workflow'],'explicit_start_v1')
        view=payload['items'][0]['launch_view']
        self.assertEqual(view['settlement_policy']['settlement_rule_id'],'pc28_fullpay_2_8_regular')
        self.assertEqual(view['automatic_settlements'][0]['source'],'route_override')
        self.assertEqual(view['automatic_settlements'][0]['policy']['settlement_rule_id'],'pc28_fullpay_2_0_regular')

    def test_archived_subscription_rejects_rule_enable(self):
        sub=self.sub();rule=self.rule(sub)
        update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='archived')
        with self.assertRaisesRegex(ValueError,'已归档'):
            update_auto_trigger_rule_status(self.repo,rule_id=rule['id'],user_id=self.uid,status='active')
        self.assertEqual(self.repo.get_auto_trigger_rule(rule['id'])['status'],'inactive')

    def test_manual_pending_blocks_standby_switch(self):
        sub=self.sub();update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='active')
        dispatch_signal(self.repo,self.signal()['id'])
        with self.assertRaisesRegex(ValueError,'待发送或待结算'):
            update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='standby')
        self.assertEqual(self.repo.get_subscription(sub['id'])['launch_mode'],'direct')

    def test_stale_auto_route_candidate_cannot_dispatch_direct_plan(self):
        sub=self.sub();rule=self.rule(sub,routes=[{'delivery_target_id':self.target['id']}])
        self.repo.update_auto_trigger_rule_status(rule_id=rule['id'],user_id=self.uid,status='active')
        update_subscription_status(self.repo,subscription_id=sub['id'],user_id=self.uid,status='active')
        result=dispatch_signal(self.repo,self.signal()['id'],subscription_id=sub['id'],
            auto_trigger_context={'rule_id':rule['id'],'routes':rule['routes']})
        self.assertEqual(result['created_count'],0)
        self.assertEqual(result['skipped_routes'][0]['reason'],'subscription_not_automatic')
