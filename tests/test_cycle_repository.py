from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from pc28touzhu.executor.db_repository import DatabaseRepository


POLICY = {"mode": "rule", "max_starts_per_day": 2, "on_target": "wait_condition", "on_bust": "wait_condition"}
DAY = "2026-11-05"


def state(**changes):
    return {"stat_date": DAY, "balance": 10, "cycle_stake": 10, "day_pnl": 0, "win_streak": 0, "target_streak": 3,
            "status": "running", "periods": 0, "cycles_today": 0, "day_closed": False, **changes}


class CycleRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = os.path.join(self.temp.name, 'cycle.db')
        self.repo = DatabaseRepository(self.path)
        self.repo.initialize_database()
        self.user = self.repo.create_user('owner')
        self.other = self.repo.create_user('other')
        self.source = self.repo.create_source_record(owner_user_id=self.user, source_type='internal_ai', name='source')
        self.sub = self.repo.create_subscription_record(user_id=self.user, source_id=self.source['id'], strategy={
            'staking_policy': {'mode':'win_streak_roll','base_amount':10,'structure':'B','target_streak':3,
                               'stop_policy': {'on_target':'close_day','on_bust':'close_day','max_cycles_per_day':1}},
            'play_filter': {'mode':'all'},
        })
        self.target = self.repo.create_delivery_target_record(user_id=self.user, executor_type='telegram_group', target_key='-1001', status='active')
        self.rule = self.make_rule('A')
        self.rule_b = self.make_rule('B')
        self.route = self.rule['routes'][0]
        self.route_b = self.rule_b['routes'][0]

    def make_rule(self, name):
        return self.repo.create_auto_trigger_rule_record(user_id=self.user, name=name, subscription_ids=[self.sub['id']],
            conditions=[{'type':'draw_streak','metric':'combo','bet_value':'大双','operator':'gte','threshold':2}],
            routes=[{'delivery_target_id':self.target['id']}])

    def owner(self, rule=None, route=None):
        return {'rule_id': (rule or self.rule)['id'], 'subscription_id': self.sub['id'], 'route_id': (route or self.route)['id'], 'user_id': self.user}

    def session_owner(self, rule=None):
        return {'rule_id': (rule or self.rule)['id'], 'subscription_id': self.sub['id'], 'user_id': self.user}

    def claim(self, repo=None, issue='100', day=DAY, rule=None, route=None, maximum=2):
        return (repo or self.repo).claim_auto_trigger_cycle_start(**self.session_owner(rule), stat_date=day, started_issue_no=issue,
            max_starts_per_day=maximum, policy=POLICY, route_ids=[(route or self.route)['id']])

    def test_manual_and_each_rule_keep_separate_state(self):
        self.repo.upsert_subscription_cycle_state(subscription_id=self.sub['id'], user_id=self.user, state=state(balance=17))
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(), rule_run_id=None, state=state(balance=23))
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(self.rule_b, self.route_b), rule_run_id=None, state=state(balance=31))
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])['balance'], 17)
        self.assertEqual(self.repo.get_auto_trigger_cycle_state(**self.owner())['balance'], 23)
        self.assertEqual(self.repo.get_auto_trigger_cycle_state(**self.owner(self.rule_b,self.route_b))['balance'], 31)

    def test_pending_enumeration_has_explicit_owner(self):
        self.repo.upsert_subscription_cycle_state(subscription_id=self.sub['id'], user_id=self.user, state=state(), pending_plan={'issue_no':'101'})
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(), rule_run_id=None, state=state(), pending_plan={'issue_no':'102'})
        rows=self.repo.list_pending_win_streak_cycles(user_id=self.user)
        self.assertEqual({r['scope'] for r in rows}, {'manual','auto'})
        self.assertEqual(next(r for r in rows if r['scope']=='auto')['rule_id'], self.rule['id'])
        self.assertEqual(self.repo.list_pending_win_streak_cycles(user_id=self.other), [])

    def test_wrong_owner_and_route_are_rejected(self):
        wrong={**self.owner(),'user_id':self.other}
        with self.assertRaises(ValueError):
            self.repo.upsert_auto_trigger_cycle_state(**wrong, rule_run_id=None, state=state())
        with self.assertRaises(ValueError):
            self.repo.upsert_auto_trigger_cycle_state(**{**self.owner(),'route_id':self.route_b['id']},rule_run_id=None,state=state())
        with self.assertRaises(ValueError):
            self.repo.claim_auto_trigger_cycle_start(**{**self.session_owner(),'user_id':self.other},stat_date=DAY,
                started_issue_no='100',max_starts_per_day=2,policy=POLICY,route_ids=[self.route['id']])

    def test_claim_and_state_rollback_with_outer_transaction(self):
        with self.assertRaisesRegex(RuntimeError,'no delivery'):
            with self.repo.transaction() as unit:
                result=self.claim(unit)
                self.assertTrue(result['claimed'])
                unit.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=result['run']['id'],state=state())
                raise RuntimeError('no delivery')
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(**self.session_owner(),stat_date=DAY)['starts_count'],0)
        self.assertIsNone(self.repo.get_auto_trigger_cycle_session(**self.session_owner()))
        self.assertIsNone(self.repo.get_auto_trigger_cycle_state(**self.owner()))

    def test_concurrent_claim_has_one_winner(self):
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _: self.claim(DatabaseRepository(self.path)), range(2)))
        self.assertEqual(sum(r['claimed'] for r in results),1)
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(**self.session_owner(),stat_date=DAY)['starts_count'],1)

    def test_completion_waits_for_routes_and_new_run_preserves_previous(self):
        result=self.claim()
        run=result['run']
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=run['id'],state=state(),pending_plan={'issue_no':'101'})
        done=self.repo.complete_auto_trigger_cycle_session(**self.session_owner(),rule_run_id=run['id'],completed_issue_no='101',status='waiting')
        self.assertFalse(done['completed'])
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=run['id'],state=state(status='stopped'))
        done=self.repo.complete_auto_trigger_cycle_session(**self.session_owner(),rule_run_id=run['id'],completed_issue_no='101',status='waiting')
        self.assertTrue(done['completed'])
        self.assertEqual(self.repo.get_auto_trigger_rule_run(run['id'])['status'],'closed')
        repeated=self.claim(issue='100')
        self.assertFalse(repeated['claimed'])
        next_run=self.claim(issue='102')['run']
        self.assertNotEqual(run['id'],next_run['id'])
        self.assertEqual(self.repo.get_auto_trigger_rule_run(run['id'])['started_issue_no'],'100')
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(**self.session_owner(),stat_date=DAY)['starts_count'],2)

    def test_daily_quota_and_other_rule_do_not_interfere(self):
        first=self.claim(maximum=1)
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=first['run']['id'],state=state(status='stopped'))
        self.repo.complete_auto_trigger_cycle_session(**self.session_owner(),rule_run_id=first['run']['id'],completed_issue_no='101',status='waiting')
        self.assertFalse(self.claim(issue='102', maximum=1)['claimed'])
        self.assertTrue(self.claim(issue='102',rule=self.rule_b,route=self.route_b,maximum=1)['claimed'])
        self.assertTrue(self.claim(issue='103',day='2026-11-06',maximum=1)['claimed'])
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(**self.session_owner(),stat_date=DAY)['starts_count'],1)

    def test_running_session_blocks_new_day(self):
        self.assertTrue(self.claim()['claimed'])
        self.assertFalse(self.claim(day='2026-11-06',issue='200')['claimed'])
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(**self.session_owner(),stat_date='2026-11-06')['starts_count'],0)

    def test_claim_only_runtime_is_discoverable_by_later_source_signal(self):
        claimed=self.claim()
        runtime=self.repo.ensure_auto_trigger_cycle_route_runtime(**self.owner(),rule_run_id=claimed['run']['id'],
            strategy=self.sub['strategy_v2'],started_issue_no='100')
        self.assertIsNone(runtime['started_signal_id'])
        signal=self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no='101',bet_type='combo',bet_value='大双')
        candidates=self.repo.list_active_auto_trigger_route_dispatch_candidates(signal['id'])
        self.assertEqual(len(candidates),1)
        self.assertEqual(candidates[0]['rule_run_id'],claimed['run']['id'])
        self.assertEqual(candidates[0]['runtime_started_signal_id'], signal['id'])
        self.assertIn(signal['id'],self.repo.list_pending_auto_trigger_route_signals(created_after='2026-01-01T00:00:00Z'))
        old=self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no='100',bet_type='combo',bet_value='大双')
        self.assertEqual(self.repo.list_active_auto_trigger_route_dispatch_candidates(old['id']), [])
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(**self.session_owner(),stat_date=DAY)['starts_count'],1)

    def test_discarded_empty_route_cannot_continue_sending(self):
        claimed=self.claim()
        self.repo.ensure_auto_trigger_cycle_route_runtime(**self.owner(),rule_run_id=claimed['run']['id'],
            strategy=self.sub['strategy_v2'],started_issue_no='100')
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=claimed['run']['id'],state=state(balance=17))
        result=self.repo.discard_unstarted_auto_trigger_cycle_routes(**self.session_owner(),rule_run_id=claimed['run']['id'],route_ids=[self.route['id']])
        self.assertEqual(result['discarded_route_ids'],[self.route['id']])
        signal=self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no='101',bet_type='combo',bet_value='大双')
        self.assertEqual(self.repo.list_active_auto_trigger_route_dispatch_candidates(signal['id']),[])
        saved=self.repo.get_auto_trigger_cycle_state(**self.owner())
        self.assertEqual(saved['status'],'stopped')
        self.assertEqual(saved['balance'],17)

    def test_discard_does_not_close_route_with_created_leg(self):
        claimed=self.claim()
        runtime=self.repo.ensure_auto_trigger_cycle_route_runtime(**self.owner(),rule_run_id=claimed['run']['id'],
            strategy=self.sub['strategy_v2'],started_issue_no='100')
        signal=self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no='101',bet_type='combo',bet_value='大双')
        with self.repo._connect() as con:
            con.execute('''INSERT INTO subscription_progression_events(subscription_id,user_id,signal_id,issue_no,leg_key,
                auto_trigger_rule_id,auto_trigger_rule_run_id,auto_trigger_route_id) VALUES(?,?,?,?,?,?,?,?)''',
                (self.sub['id'],self.user,signal['id'],'101','main',self.rule['id'],claimed['run']['id'],self.route['id']))
        result=self.repo.discard_unstarted_auto_trigger_cycle_routes(**self.session_owner(),rule_run_id=claimed['run']['id'],route_ids=[self.route['id']])
        self.assertEqual(result['discarded_route_ids'],[])
        row=self.repo._fetch_one('SELECT status FROM auto_trigger_route_subscription_runtime_runs WHERE id=?',(runtime['id'],))
        self.assertEqual(row['status'],'active')

    def test_deleting_archived_rule_cleans_new_state_without_foreign_key_error(self):
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=None,state=state(status='stopped'))
        self.repo.upsert_auto_trigger_cycle_session(**self.session_owner(),values={'status':'waiting'})
        self.repo.update_auto_trigger_rule_status(rule_id=self.rule['id'],user_id=self.user,status='archived')
        self.assertTrue(self.repo.delete_auto_trigger_rule_record(rule_id=self.rule['id'],user_id=self.user))
        self.assertEqual(self.repo._fetch_one('SELECT COUNT(*) AS n FROM auto_trigger_cycle_states',())['n'],0)

    def test_deleting_archived_subscription_cleans_new_state(self):
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=None,state=state(status='stopped'))
        self.repo.upsert_auto_trigger_cycle_session(**self.session_owner(),values={'status':'waiting'})
        self.repo.upsert_subscription_cycle_state(subscription_id=self.sub['id'],user_id=self.user,state=state())
        self.repo.update_subscription_status(subscription_id=self.sub['id'],user_id=self.user,status='archived')
        self.assertTrue(self.repo.delete_subscription_record(subscription_id=self.sub['id'],user_id=self.user))
        self.assertEqual(self.repo._fetch_one('SELECT COUNT(*) AS n FROM auto_trigger_cycle_states',())['n'],0)

    def test_implicit_route_is_hidden_and_survives_explicit_sync(self):
        rule=self.repo.create_auto_trigger_rule_record(user_id=self.user,name='implicit',subscription_ids=[self.sub['id']])
        route=self.repo.ensure_implicit_auto_trigger_route(rule_id=rule['id'],user_id=self.user,delivery_target_id=self.target['id'])
        self.assertTrue(route['is_implicit'])
        self.assertEqual(self.repo.get_auto_trigger_rule(rule['id'])['routes'],[])
        again=self.repo.ensure_implicit_auto_trigger_route(rule_id=rule['id'],user_id=self.user,delivery_target_id=self.target['id'])
        self.assertEqual(route['id'],again['id'])
        with self.repo._connect() as con:
            self.repo._sync_auto_trigger_rule_routes(con,rule_id=rule['id'],user_id=self.user,routes=[],updated_at='2026-11-05T00:00:00Z')
        self.assertEqual(self.repo.get_auto_trigger_rule_route(route['id'])['status'],'active')
        self.assertEqual(next(r for r in self.repo.list_auto_trigger_rules(user_id=self.user) if r['id']==rule['id'])['routes'],[])

    def test_implicit_route_rejects_foreign_or_direct_only_target(self):
        target=self.repo.create_delivery_target_record(user_id=self.other,executor_type='telegram_group',target_key='-1002', status='active')
        with self.assertRaises(ValueError):
            self.repo.ensure_implicit_auto_trigger_route(rule_id=self.rule['id'],user_id=self.user,delivery_target_id=target['id'])
        with self.repo._connect() as con:
            con.execute("UPDATE delivery_targets SET dispatch_mode='direct_only' WHERE id=?",(self.target['id'],))
        with self.assertRaises(ValueError):
            self.repo.ensure_implicit_auto_trigger_route(rule_id=self.rule['id'],user_id=self.user,delivery_target_id=self.target['id'])

    def seed_old_cycle(self, *, pending=True, closed=False, route=None, conflicting=False):
        run=self.repo.ensure_auto_trigger_rule_run(**self.session_owner(),stat_date=DAY,started_issue_no='100')
        ids=[]
        for index,leg in enumerate(('main','aux')):
            signal=self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no='101',bet_type='combo',bet_value='大双')
            with self.repo._connect() as con:
                cur=con.execute('''INSERT INTO subscription_progression_events(subscription_id,user_id,signal_id,issue_no,stake_amount,status,leg_key,
                    auto_trigger_rule_id,auto_trigger_rule_run_id,auto_trigger_route_id,auto_trigger_stat_date,result_context_json,created_at,updated_at)
                    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''', (self.sub['id'],self.user,signal['id'],'101',7 if leg=='main' else 3,
                    'placed' if pending else 'settled',leg,self.rule_b['id'] if conflicting and index else self.rule['id'],run['id'],
                    (route if route is not None else self.route['id']) or None,DAY,json.dumps({'cycle':{'stat_date':DAY}}), '2026-11-05T00:00:00Z','2026-11-05T00:01:00Z'))
                ids.append(cur.lastrowid)
        plan={'rule_id':self.rule['id'],'route_id':self.route['id'] if route is None else route,'rule_run_id':run['id'],'issue_no':'101','leg_event_ids':ids} if pending else {}
        with self.repo._connect() as con:
            # 直接写旧版格式，避免新手动 CRUD 正确标记的 scope 信息。
            con.execute('''INSERT OR REPLACE INTO subscription_cycle_states(subscription_id,user_id,stat_date,balance,cycle_stake,day_pnl,
                win_streak,target_streak,status,periods,pending_plan_json,policy_json,day_closed,stop_reason,last_cycle_stop_reason,updated_at)
                VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',(self.sub['id'],self.user,DAY,2.35 if closed else 10,0 if closed else 10,-7.65 if closed else 0,
                0,3,'stopped' if closed else 'running',7 if closed else 0,json.dumps(plan),json.dumps(self.sub['strategy_v2']['staking_policy']),int(closed),
                'balance_depleted' if closed else None,'cycle_balance_depleted' if closed else None,'2026-11-05T00:02:00Z'))
        return ids,run

    def test_old_pending_migrates_once_and_keeps_ledger_identity(self):
        ids,run=self.seed_old_cycle()
        before=self.repo._fetch_all('SELECT * FROM subscription_progression_events ORDER BY id')
        self.repo.initialize_database()
        self.repo.initialize_database()
        migrated=self.repo.get_auto_trigger_cycle_state(**self.owner())
        self.assertEqual(migrated['pending_plan']['leg_event_ids'],ids)
        self.assertEqual(migrated['rule_run_id'],run['id'])
        self.assertIsNone(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id']))
        self.assertEqual(self.repo._fetch_all('SELECT * FROM subscription_progression_events ORDER BY id'),before)
        self.assertEqual(self.repo._fetch_one('SELECT COUNT(*) AS n FROM cycle_state_migrations', ())['n'],1)
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(**self.session_owner(),stat_date=DAY)['starts_count'],1)

    def test_old_closed_cycle_remains_stopped_and_keeps_balance(self):
        self.seed_old_cycle(pending=False,closed=True)
        self.repo.initialize_database()
        migrated=self.repo.get_auto_trigger_cycle_state(**self.owner())
        self.assertEqual(migrated['balance'],2.35)
        self.assertTrue(migrated['day_closed'])
        self.assertEqual(self.repo.get_auto_trigger_cycle_session(**self.session_owner())['status'],'day_closed')
        self.assertFalse(self.claim(issue='102')['claimed'])

    def test_old_no_route_pending_keeps_route_zero_until_settlement(self):
        ids,_=self.seed_old_cycle(route=0)
        self.repo.initialize_database()
        migrated=self.repo.get_auto_trigger_cycle_state(**{**self.owner(),'route_id':0})
        self.assertEqual(migrated['pending_plan']['leg_event_ids'],ids)
        self.assertFalse(self.claim(issue='102')['claimed'])

    def test_conflicting_migration_blocks_instead_of_copying_money(self):
        self.seed_old_cycle(conflicting=True)
        self.repo.initialize_database()
        migration=self.repo._fetch_one('SELECT * FROM cycle_state_migrations WHERE subscription_id=?',(self.sub['id'],))
        self.assertEqual(migration['status'],'blocked')
        self.assertEqual(self.repo.get_auto_trigger_cycle_session(**self.session_owner())['status'],'blocked')
        self.assertFalse(self.claim(issue='102')['claimed'])

    def test_new_manual_cycle_is_not_remigrated_on_restart(self):
        self.repo.upsert_subscription_cycle_state(subscription_id=self.sub['id'],user_id=self.user,state=state(balance=27))
        self.repo.initialize_database()
        self.assertEqual(self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])['balance'],27)
        self.assertIsNone(self.repo.get_auto_trigger_cycle_state(**self.owner()))

    def test_old_settled_manual_cycle_uses_unique_manual_runtime_evidence(self):
        ids,_=self.seed_old_cycle(pending=False,closed=True)
        with self.repo._connect() as con:
            con.execute('''UPDATE subscription_progression_events SET auto_trigger_rule_id=NULL,
                auto_trigger_rule_run_id=NULL,auto_trigger_route_id=NULL,auto_trigger_stat_date='',
                result_context_json='{}',settled_at='2026-11-05T00:01:00Z' WHERE id IN (?,?)''',ids)
            con.execute('''INSERT INTO subscription_runtime_runs(subscription_id,user_id,status,started_at,ended_at)
                VALUES(?,?,'closed','2026-11-04T23:59:00Z','2026-11-05T00:02:00Z')''',(self.sub['id'],self.user))
        self.repo.initialize_database()
        saved=self.repo.get_subscription_cycle_state(subscription_id=self.sub['id'])
        self.assertEqual(saved['balance'],2.35)
        self.assertEqual(saved['stop_reason'],'balance_depleted')
        self.assertTrue(saved['day_closed'])
        self.assertIsNone(self.repo.get_auto_trigger_cycle_state(**self.owner()))
        migration=self.repo._fetch_one('SELECT status FROM cycle_state_migrations WHERE subscription_id=?',(self.sub['id'],))
        self.assertEqual(migration['status'],'manual')

    def test_rule_cycle_policy_round_trip_and_old_default(self):
        self.assertEqual(self.rule['cycle_policy']['mode'],'legacy')
        rule=self.repo.create_auto_trigger_rule_record(user_id=self.user,name='independent',subscription_ids=[self.sub['id']],cycle_policy=POLICY)
        self.assertEqual(rule['cycle_policy'],POLICY)

    def test_runtime_projection_uses_requested_date_and_correct_scope(self):
        first=self.claim()
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=first['run']['id'],state=state(balance=23,win_streak=2),pending_plan={'issue_no':'101'})
        self.repo.upsert_subscription_cycle_state(subscription_id=self.sub['id'],user_id=self.user,state=state(balance=999))
        item=self.repo.list_auto_trigger_cycle_runtime(rule_id=self.rule['id'],user_id=self.user,stat_date=DAY)[0]
        self.assertEqual(item['starts_today'],1)
        self.assertEqual(item['routes'][0]['balance'],23)
        self.assertEqual(item['routes'][0]['pending_issue'],'101')
        next_day=self.repo.list_auto_trigger_cycle_runtime(rule_id=self.rule['id'],user_id=self.user,stat_date='2026-11-06')[0]
        self.assertEqual(next_day['starts_today'],0)
        self.assertEqual(next_day['start_stat_date'],DAY)

    def test_runtime_projection_retains_active_cycle_after_strategy_or_scope_change(self):
        claimed=self.claim()
        self.repo.upsert_auto_trigger_cycle_state(**self.owner(),rule_run_id=claimed['run']['id'],state=state(balance=23))
        with self.repo._connect() as con:
            con.execute('UPDATE user_subscriptions SET strategy_json=? WHERE id=?',
                        (json.dumps({'staking_policy': {'mode':'fixed','fixed_amount':10}}),self.sub['id']))
            con.execute("UPDATE auto_trigger_rules SET subscription_ids_json='[]' WHERE id=?",(self.rule['id'],))
        item=self.repo.list_auto_trigger_cycle_runtime(rule_id=self.rule['id'],user_id=self.user,stat_date=DAY)[0]
        self.assertEqual(item['status'],'running')
        self.assertEqual(item['routes'][0]['balance'],23)
        self.assertTrue(item['outside_scope'])

    def test_explicit_resume_converts_global_cycle_stop_to_other_session_stops(self):
        second=self.repo.create_subscription_record(user_id=self.user,source_id=self.source['id'],strategy=self.sub['strategy_v2'])
        with self.repo._connect() as con:
            con.execute('UPDATE auto_trigger_rules SET cycle_policy_json=?,subscription_ids_json=? WHERE id=?',
                        (json.dumps(POLICY),json.dumps([self.sub['id'],second['id']]),self.rule['id']))
            con.execute('''INSERT INTO auto_trigger_rule_daily_stats(rule_id,user_id,stat_date,net_profit,status,stopped_reason)
                VALUES(?,?,?,-7.65,'stopped','cycle_balance_depleted')''',(self.rule['id'],self.user,DAY))
        self.repo.upsert_auto_trigger_cycle_session(**self.session_owner(),values={'status':'day_closed','start_stat_date':DAY})
        result=self.repo.resume_auto_trigger_cycle_day_scope(**self.session_owner(),stat_date=DAY)
        self.assertEqual(result['protected_subscription_ids'],[second['id']])
        self.assertEqual(self.repo.get_auto_trigger_cycle_session(**self.session_owner())['status'],'waiting')
        other=self.repo.get_auto_trigger_cycle_session(rule_id=self.rule['id'],subscription_id=second['id'],user_id=self.user)
        self.assertEqual(other['status'],'day_closed')
        day=self.repo.get_auto_trigger_rule_daily_stat(rule_id=self.rule['id'],user_id=self.user,stat_date=DAY)
        self.assertEqual(day['status'],'active')
        self.assertEqual(day['net_profit'],-7.65)

    def test_explicit_resume_never_clears_daily_risk_stop(self):
        with self.repo._connect() as con:
            con.execute('UPDATE auto_trigger_rules SET cycle_policy_json=? WHERE id=?',(json.dumps(POLICY),self.rule['id']))
            con.execute('''INSERT INTO auto_trigger_rule_daily_stats(rule_id,user_id,stat_date,status,stopped_reason)
                VALUES(?,?,?,'stopped','loss_limit_hit')''',(self.rule['id'],self.user,DAY))
        with self.assertRaises(ValueError):
            self.repo.resume_auto_trigger_cycle_day_scope(**self.session_owner(),stat_date=DAY)
        self.assertEqual(self.repo.get_auto_trigger_rule_daily_stat(rule_id=self.rule['id'],user_id=self.user,stat_date=DAY)['status'],'stopped')

    def test_bound_legacy_owner_survives_rule_disabled_and_routes_added(self):
        rule=self.repo.create_auto_trigger_rule_record(user_id=self.user,name='old no-route',subscription_ids=[self.sub['id']])
        run=self.repo.ensure_auto_trigger_rule_run(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user,
            stat_date=DAY,started_issue_no='100')
        with self.repo._connect() as con:
            con.execute('''INSERT INTO subscription_runtime_runs(subscription_id,user_id,status,started_at,auto_trigger_rule_run_id)
                VALUES(?,?,'active','2026-11-05T00:00:00Z',?)''',(self.sub['id'],self.user,run['id']))
            con.execute("UPDATE auto_trigger_rules SET status='inactive' WHERE id=?",(rule['id'],))
            self.repo._sync_auto_trigger_rule_routes(con,rule_id=rule['id'],user_id=self.user,
                routes=[{'delivery_target_id':self.target['id']}],updated_at='2026-11-05T00:01:00Z')
        owner=self.repo.get_subscription_runtime_rule_run(subscription_id=self.sub['id'],user_id=self.user)
        self.assertEqual(owner['id'],run['id'])
        self.assertEqual(owner['rule_id'],rule['id'])
        self.assertIsNone(self.repo.get_subscription_runtime_rule_run(subscription_id=self.sub['id'],user_id=self.other))

    def test_unbound_manual_runtime_never_guesses_historical_rule_owner(self):
        self.repo.ensure_auto_trigger_rule_run(**self.session_owner(),stat_date=DAY,started_issue_no='100')
        with self.repo._connect() as con:
            con.execute('''INSERT INTO subscription_runtime_runs(subscription_id,user_id,status,started_at)
                VALUES(?,?,'active','2026-11-05T00:00:00Z')''',(self.sub['id'],self.user))
        self.assertIsNone(self.repo.get_subscription_runtime_rule_run(subscription_id=self.sub['id'],user_id=self.user))


if __name__ == '__main__':
    unittest.main()
