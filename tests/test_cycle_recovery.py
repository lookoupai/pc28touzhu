"""Run-scoped recovery uses temporary databases, never live execution."""
from __future__ import annotations

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.platform_service import resolve_pending_pc28_progressions_from_draws
from pc28touzhu.services.auto_trigger_service import update_auto_trigger_rule

from tests.test_cycle_runtime import CycleFixture


class CycleRecoveryMigrationTests(CycleFixture):
    def _tables(self):
        with sqlite3.connect(self.repo.db_path) as conn:
            return {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}

    def _archive(self, run, route):
        with self.repo._connect() as conn:
            conn.execute("""INSERT INTO auto_trigger_cycle_archives
                (user_id,rule_id,subscription_id,route_id,rule_run_id,stat_date,issue_no,
                 state_json,policy_json,pending_plan_json,detached_at,updated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (self.user_id,run['rule_id'],self.sub['id'],route['id'],run['id'],self.day,
                 str(self.latest+1),'{}','{}','{}',self.now.isoformat(),self.now.isoformat()))

    def test_additive_archive_schema_preserves_original_records(self):
        self.assertIn('auto_trigger_cycle_archives', self._tables(), 'missing run-scoped archive')
        rule = self.rule(targets=[self.targets[1]])
        self.signal()
        self.cycle(rule)
        tables = ['auto_trigger_rules','auto_trigger_rule_runs','auto_trigger_cycle_states',
                  'auto_trigger_cycle_sessions','auto_trigger_cycle_daily_starts',
                  'subscription_progression_events','execution_jobs']
        with sqlite3.connect(self.repo.db_path) as conn:
            before = {table:conn.execute('SELECT * FROM '+table).fetchall() for table in tables}
            conn.execute('DROP TABLE auto_trigger_cycle_archives')
        self.repo.initialize_database()
        self.repo.initialize_database()
        with sqlite3.connect(self.repo.db_path) as conn:
            after = {table:conn.execute('SELECT * FROM '+table).fetchall() for table in tables}
            self.assertEqual(conn.execute('PRAGMA foreign_key_check').fetchall(), [])
        self.assertEqual(after, before)

    def test_archive_is_unique_for_original_run_and_route(self):
        self.assertIn('auto_trigger_cycle_archives', self._tables(), 'missing run-scoped archive')
        rule = self.rule(targets=[self.targets[1]])
        self.signal();self.cycle(rule)
        run = self.repo.get_auto_trigger_rule_run(self.session(rule)['active_run_id'])
        self._archive(run, rule['routes'][0])
        with self.assertRaises(sqlite3.IntegrityError):
            self._archive(run, rule['routes'][0])

    def test_retention_preserves_archived_runs(self):
        self.assertIn('auto_trigger_cycle_archives', self._tables(), 'missing run-scoped archive')
        rule = self.rule(targets=[self.targets[1]])
        self.signal();self.cycle(rule)
        run = self.repo.get_auto_trigger_rule_run(self.session(rule)['active_run_id'])
        self._archive(run, rule['routes'][0])
        self.repo.prune_auto_trigger_rule_runtime_data(runs_cutoff='2099-01-01',stats_cutoff='2000-01-01')
        self.assertIsNotNone(self.repo.get_auto_trigger_rule_run(run['id']))


class RecoveryFixture(CycleFixture):
    def setUp(self):
        super().setUp()
        self.strategy['staking_policy']['target_streak'] = 3
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,
            source_id=self.source['id'],strategy=self.strategy,status='standby')

    def start(self, *, targets=None, limit=4):
        rule=self.rule(targets=targets or [self.targets[1]],limit=limit)
        self.signal();self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        return rule

    def jobs(self, rule):
        return [j for j in self.repo.list_execution_jobs(user_id=self.user_id)
                if j['auto_trigger_route_id'] in [r['id'] for r in rule['routes']]]

    def unknown(self, rule, *, route_id=None):
        jobs=[j for j in self.jobs(rule) if route_id is None or j['auto_trigger_route_id']==route_id]
        leader=next(j for j in jobs if not j.get('send_with_job_id'))
        self.repo.report_job_result(str(leader['id']),'fixture',1,'unconfirmed',None,
            self.now.isoformat(),{},'connection outcome unknown')
        return jobs

    def detach(self, *, repository=None, user_id=None):
        repository=repository or self.repo
        self.assertTrue(hasattr(repository,'detach_expired_auto_trigger_cycles'),
                        'missing expired uncertain cycle recovery')
        return repository.detach_expired_auto_trigger_cycles(now=self.now,
            user_id=self.user_id if user_id is None else user_id)

    def archives(self, rule):
        self.assertTrue(hasattr(self.repo,'list_auto_trigger_cycle_archives'), 'missing archive reads')
        return self.repo.list_auto_trigger_cycle_archives(user_id=self.user_id,rule_id=rule['id'])

    def resolve_archives(self):
        self.assertTrue(hasattr(self.repo,'resolve_auto_trigger_cycle_archives'), 'missing archive settlement')
        return self.repo.resolve_auto_trigger_cycle_archives(user_id=self.user_id,now=self.now)

    def state(self, rule, route=None):
        return self.repo.get_auto_trigger_cycle_state(user_id=self.user_id,rule_id=rule['id'],
            subscription_id=self.sub['id'],route_id=(route or rule['routes'][0])['id'])

    def expire(self):
        self.now += timedelta(seconds=180)

    def void_archived(self, archive):
        # Simulate an authoritative no-bet determination, not a timeout inference.
        with self.repo._connect() as conn:
            for event_id in archive['pending_plan']['leg_event_ids']:
                conn.execute("UPDATE execution_jobs SET status='rejected' WHERE progression_event_id=?",(event_id,))
                conn.execute("UPDATE subscription_progression_events SET status='void',result_context_json=? WHERE id=?",
                    (json.dumps({'void_reason':'verified_rejection'}),event_id))


class CycleRecoveryDetachTests(RecoveryFixture):
    def test_unknown_expired_detaches_with_original_evidence(self):
        rule=self.start();jobs=self.unknown(rule);before=self.state(rule)
        self.expire();result=self.detach()
        self.assertEqual(result['detached_count'],1)
        archive=self.archives(rule)[0]
        self.assertEqual(archive['pending_plan'],before['pending_plan'])
        self.assertEqual(archive['initial_reserved_stake'],10)
        self.assertEqual(archive['reserved_stake'],10)
        self.assertEqual(archive['state']['balance'],10)
        self.assertEqual(self.state(rule)['balance'],0)
        self.assertIsNone(self.state(rule)['pending_plan'])
        self.assertEqual(self.session(rule)['status'],'waiting')
        self.assertIsNone(self.session(rule)['active_run_id'])
        self.assertEqual([self.repo.get_execution_job(j['id'])['status'] for j in jobs],['unconfirmed']*2)
        self.assertEqual([self.repo.get_progression_event(j['progression_event_id'])['status'] for j in jobs],['pending']*2)

    def test_unknown_before_original_deadline_keeps_current_run(self):
        rule=self.start();self.unknown(rule)
        self.assertEqual(self.detach()['detached_count'],0)
        self.assertEqual(self.session(rule)['status'],'running')
        self.assertEqual(self.archives(rule),[])

    def test_placed_awaiting_draw_is_not_an_expiry_fault(self):
        rule=self.start();leader=next(j for j in self.jobs(rule) if not j.get('send_with_job_id'))
        self.repo.report_job_result(str(leader['id']),'fixture',1,'delivered','100',self.now.isoformat(),{},None)
        self.now+=timedelta(days=1)
        self.assertEqual(self.detach()['detached_count'],0)
        self.assertEqual(self.session(rule)['status'],'running')

    def test_all_routes_archive_together_including_placed(self):
        rule=self.start(targets=self.targets[1:]);self.unknown(rule,route_id=rule['routes'][0]['id'])
        leader=next(j for j in self.jobs(rule) if j['auto_trigger_route_id']==rule['routes'][1]['id'] and not j.get('send_with_job_id'))
        self.repo.report_job_result(str(leader['id']),'fixture',1,'delivered','101',self.now.isoformat(),{},None)
        self.expire();self.assertEqual(self.detach()['detached_count'],1)
        archives=self.archives(rule)
        self.assertEqual(len(archives),2)
        self.assertEqual({a['status'] for a in archives},{'awaiting_reconciliation','awaiting_settlement'})
        self.assertEqual(sum(a['reserved_stake'] for a in archives),20)
        for route in rule['routes']:
            self.assertIsNone(self.state(rule,route)['pending_plan'])

    def test_manual_stop_is_not_reenabled_by_detachment(self):
        rule=self.start();self.unknown(rule)
        self.repo.stop_auto_trigger_rule_current_run(rule_id=rule['id'],user_id=self.user_id)
        self.expire();self.detach()
        self.assertEqual(self.session(rule)['status'],'blocked')
        self.assertEqual(self.session(rule)['stop_reason'],'manual_stop')
        self.assertIsNone(self.session(rule)['active_run_id'])

    def test_repeated_and_concurrent_detachment_reserves_once(self):
        rule=self.start();self.unknown(rule);self.expire()
        self.assertTrue(hasattr(self.repo,'detach_expired_auto_trigger_cycles'),'missing expired uncertain cycle recovery')
        with ThreadPoolExecutor(max_workers=2) as pool:
            results=list(pool.map(lambda _:DatabaseRepository(self.repo.db_path).detach_expired_auto_trigger_cycles(
                now=self.now,user_id=self.user_id),range(2)))
        self.assertEqual(sum(r['detached_count'] for r in results),1)
        self.assertEqual(self.detach()['detached_count'],0)
        self.assertEqual(len(self.archives(rule)),1)
        self.assertEqual(self.state(rule)['balance'],0)

    def test_wrong_user_scan_does_not_detach(self):
        rule=self.start();self.unknown(rule);self.expire()
        other=self.repo.create_user('unrelated-recovery-user')
        self.assertEqual(self.detach(user_id=other)['detached_count'],0)
        self.assertEqual(self.session(rule)['status'],'running')

    def test_pending_retry_on_sibling_route_is_retired(self):
        rule=self.start(targets=self.targets[1:]);self.unknown(rule,route_id=rule['routes'][0]['id'])
        sibling=[j for j in self.jobs(rule) if j['auto_trigger_route_id']==rule['routes'][1]['id']]
        leader=next(j for j in sibling if not j.get('send_with_job_id'))
        self.repo.report_job_result(str(leader['id']),'fixture',1,'failed',None,self.now.isoformat(),
            {'failure_stage':'connect','retryable':True},'connect failed')
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET status='pending' WHERE auto_trigger_route_id=?",(rule['routes'][1]['id'],))
            conn.execute("UPDATE subscription_progression_events SET status='pending' WHERE auto_trigger_route_id=?",(rule['routes'][1]['id'],))
        self.expire();self.detach()
        self.assertTrue(all(self.repo.get_execution_job(j['id'])['status']=='expired' for j in sibling))
        self.assertFalse(self.repo.auto_trigger_route_has_open_run(route_id=rule['routes'][1]['id'],
            subscription_id=self.sub['id'],user_id=self.user_id)['has_open_run'])


class CycleRecoverySettlementTests(RecoveryFixture):
    def test_corrupt_archive_does_not_stop_new_scheduled_work(self):
        rule=self.start();self.unknown(rule);self.expire();self.detach()
        with self.repo._connect() as conn:
            conn.execute("UPDATE auto_trigger_cycle_archives SET pending_plan_json=?",('broken-json',))
        self.advance();self.signal()
        result=self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'],1)
        self.assertEqual(len(result['archive_resolution']['errors']),1)
        self.assertEqual(self.archives(rule)[0]['reserved_stake'],10)

    def test_missing_reserved_legs_cannot_release_money(self):
        rule=self.start();self.unknown(rule);self.expire();self.detach()
        with self.repo._connect() as conn:
            conn.execute("UPDATE auto_trigger_cycle_archives SET pending_plan_json='{}'")
        result=self.resolve_archives()
        self.assertEqual(result['resolved_count'],0)
        self.assertEqual(len(result['errors']),1)
        self.assertEqual(self.archives(rule)[0]['reserved_stake'],10)

    def test_late_no_bet_releases_credit_without_mutating_new_run(self):
        rule=self.start();self.unknown(rule);self.expire();self.detach()
        archive=self.archives(rule)[0]
        self.advance();self.signal();self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        new_state=self.state(rule)
        self.void_archived(archive);self.resolve_archives()
        self.assertEqual(self.state(rule),new_state)
        resolved=self.archives(rule)[0]
        self.assertEqual(resolved['status'],'resolved')
        self.assertEqual(resolved['reserved_stake'],0)
        self.assertEqual(resolved['capital_credit'],10)
        self.assertIsNone(resolved['credit_applied_run_id'])
        self.resolve_archives();self.assertEqual(self.archives(rule)[0]['capital_credit'],10)

    def test_late_accepted_settles_original_ledger_only(self):
        rule=self.start();jobs=self.unknown(rule);old_run=self.session(rule)['active_run_id']
        self.expire();self.detach();archive=self.archives(rule)[0]
        self.advance();self.signal();self.cycle(rule);before=self.state(rule)
        with self.repo._connect() as conn:
            for job in jobs:
                conn.execute("UPDATE execution_jobs SET status='delivered' WHERE id=?",(job['id'],))
                unit=self.repo
                unit._sync_progression_event_delivery_status(conn,progression_event_id=job['progression_event_id'],now=self.now.isoformat())
        resolve_pending_pc28_progressions_from_draws(self.repo,user_id=self.user_id,
            draw_items=[{'issue_no':archive['issue_no'],'draw_context':{'triplet':[5,8,2]}}])
        self.resolve_archives()
        self.assertEqual(self.state(rule),before)
        events=[self.repo.get_progression_event(j['progression_event_id']) for j in jobs]
        self.assertTrue(all(e['status']=='settled' and e['auto_trigger_rule_run_id']==old_run for e in events))
        self.assertAlmostEqual(self.archives(rule)[0]['capital_credit'],10+sum(e['net_delta'] for e in events),places=2)


class CycleRecoveryExposureTests(RecoveryFixture):
    def test_legacy_route_zero_carry_and_credit_follow_unique_original_target(self):
        rule=self.start();jobs=self.unknown(rule);route=rule['routes'][0]
        state=self.state(rule);run_id=self.session(rule)['active_run_id']
        self.strategy['dispatch']['delivery_target_ids']=[self.targets[1]['id']]
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,
            source_id=self.source['id'],strategy=self.strategy,status='standby')
        state['balance']=22.39;pending=state['pending_plan'];pending['state']['balance']=22.39;pending['route_id']=0
        with self.repo._connect() as conn:
            conn.execute('UPDATE auto_trigger_rule_routes SET is_implicit=1 WHERE id=?',(route['id'],))
            conn.execute('UPDATE subscription_progression_events SET auto_trigger_route_id=NULL WHERE auto_trigger_rule_run_id=?',(run_id,))
            conn.execute("UPDATE auto_trigger_cycle_sessions SET participating_route_ids_json='[0]' WHERE rule_id=?",(rule['id'],))
            conn.execute('DELETE FROM auto_trigger_cycle_states WHERE rule_id=?',(rule['id'],))
        self.repo.upsert_auto_trigger_cycle_state(rule_id=rule['id'],subscription_id=self.sub['id'],user_id=self.user_id,
            route_id=0,rule_run_id=run_id,state=state,policy=state['policy'],pending_plan=pending)
        self.expire();self.detach();archive=self.archives(rule)[0]
        self.advance();self.signal();self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        self.assertEqual(self.state(rule)['balance'],12.39)
        self.void_archived(archive);self.resolve_archives()
        self.unknown(rule);self.expire();self.detach()
        self.advance();self.signal();self.cycle(rule)
        self.assertEqual(self.state(rule)['balance'],12.39)
        old=next(a for a in self.archives(rule) if a['id']==archive['id'])
        self.assertEqual(old['capital_route_id'],route['id'])
        self.assertIsNotNone(old['credit_applied_run_id'])

    def test_delete_cannot_lose_resolved_unconsumed_credit(self):
        rule=self.start();self.unknown(rule);self.expire();self.detach();self.void_archived(self.archives(rule)[0]);self.resolve_archives()
        self.repo.update_auto_trigger_rule_status(rule_id=rule['id'],user_id=self.user_id,status='archived')
        self.repo.update_subscription_status(subscription_id=self.sub['id'],user_id=self.user_id,status='archived')
        with self.assertRaisesRegex(ValueError,'历史'):
            self.repo.delete_auto_trigger_rule_record(rule_id=rule['id'],user_id=self.user_id)
        with self.assertRaisesRegex(ValueError,'历史'):
            self.repo.delete_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id)
        self.assertEqual(self.archives(rule)[0]['capital_credit'],10)

    def test_switch_to_all_in_preserves_chain_credit_for_future_chain(self):
        rule=self.start();self.unknown(rule);self.expire();self.detach();self.void_archived(self.archives(rule)[0]);self.resolve_archives()
        self.strategy['staking_policy']['roll_mode']='all_in'
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,
            source_id=self.source['id'],strategy=self.strategy,status='standby')
        self.advance();self.signal();self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        self.assertIsNone(self.archives(rule)[0]['credit_applied_run_id'])
        self.assertEqual(self.state(rule)['balance'],10)

    def test_rolled_back_start_does_not_consume_old_credit(self):
        rule=self.start();self.unknown(rule);self.expire();self.detach();archive=self.archives(rule)[0]
        self.void_archived(archive);self.resolve_archives();before=self.state(rule)
        self.advance();self.signal()
        with patch('pc28touzhu.services.dispatch_service.dispatch_signal',return_value={'created_count':0,'jobs':[]}):
            self.assertEqual(self.cycle(rule)['summary']['triggered_count'],0)
        self.assertIsNone(self.archives(rule)[0]['credit_applied_run_id'])
        self.assertEqual(self.state(rule),before)

    def test_unstarted_route_retains_credit_and_original_carry(self):
        rule=self.start(targets=self.targets[1:]);self.unknown(rule,route_id=rule['routes'][0]['id'])
        self.unknown(rule,route_id=rule['routes'][1]['id']);self.expire();self.detach()
        for archive in self.archives(rule):self.void_archived(archive)
        self.resolve_archives();skipped_route=rule['routes'][1]
        old_state=self.state(rule,skipped_route)
        self.advance();self.signal()
        real_dispatch=dispatch_signal
        def partial(repository, signal_id, **kwargs):
            kwargs['auto_trigger_context']={**kwargs['auto_trigger_context'],
                'routes':[r for r in kwargs['auto_trigger_context']['routes'] if r['id']!=skipped_route['id']]}
            return real_dispatch(repository,signal_id,**kwargs)
        with patch('pc28touzhu.services.dispatch_service.dispatch_signal',side_effect=partial):
            self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        missing=next(a for a in self.archives(rule) if a['route_id']==skipped_route['id'])
        self.assertIsNone(missing['credit_applied_run_id'])
        self.assertEqual(self.state(rule,skipped_route)['balance'],old_state['balance'])

    def test_credit_is_consumed_only_at_next_start_and_top_up_is_audited(self):
        rule=self.start();self.unknown(rule);self.expire();self.detach();archive=self.archives(rule)[0]
        self.advance();self.signal();self.cycle(rule)
        second=self.session(rule)['active_run_id']
        audit=self.repo.get_auto_trigger_rule_run(second)['capital_audit']
        self.assertEqual(audit['routes'][str(rule['routes'][0]['id'])]['top_up_amount'],10)
        self.void_archived(archive);self.resolve_archives()
        # Interrupt the second run too, then enter another fresh start.
        self.unknown(rule);self.expire();self.detach()
        self.advance();self.signal();self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        third=self.session(rule)['active_run_id']
        self.assertEqual(self.archives(rule)[-1]['credit_applied_run_id'],third)
        self.assertEqual(self.state(rule)['balance'],10)
        audit=self.repo.get_auto_trigger_rule_run(third)['capital_audit']['routes'][str(rule['routes'][0]['id'])]
        self.assertEqual(audit['applied_credit'],10)
        self.assertEqual(audit['top_up_amount'],0)

    def test_mixed_settled_and_unknown_reserves_only_unknown_principal(self):
        rule=self.start();self.unknown(rule);before=self.state(rule)
        event_id=before['pending_plan']['leg_event_ids'][0]
        with self.repo._connect() as conn:
            conn.execute("UPDATE subscription_progression_events SET status='settled',net_delta=6.93 WHERE id=?",(event_id,))
            conn.execute("UPDATE execution_jobs SET status='delivered' WHERE progression_event_id=?",(event_id,))
        self.expire();self.detach();archive=self.archives(rule)[0]
        self.assertEqual(archive['settled_net_at_detach'],6.93)
        self.assertEqual(archive['initial_reserved_stake'],3)
        self.assertEqual(self.state(rule)['balance'],13.93)


class CycleRecoverySchedulingTests(RecoveryFixture):
    def scheduled(self):
        self.now=datetime(2026,11,25,18,2,tzinfo=timezone.utc)
        rule=self.rule(targets=[self.targets[1]],limit=4)
        return update_auto_trigger_rule(self.repo,rule_id=rule['id'],user_id=self.user_id,payload={
            'trigger_mode':'schedule','schedule':{'timezone':'Asia/Shanghai','weekdays':['Mon','Tue','Wed','Thu','Fri','Sat','Sun'],
                'windows':[{'id':'night','start':'02:01','end':'02:59'},{'id':'morning','start':'08:01','end':'08:59'},
                           {'id':'afternoon','start':'14:01','end':'14:59'},{'id':'evening','start':'20:01','end':'20:59'}],
                'signal_max_age_seconds':300},'cycle_policy':{'max_starts_per_window':1}})['item']

    def test_four_window_schedule_continues_without_reopening_used_window(self):
        rule=self.scheduled();self.signal();self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        old_run=self.session(rule)['active_run_id'];old_day=self.day;old_jobs=self.unknown(rule)
        self.expire();self.advance();self.signal()
        same=self.cycle(rule)
        self.assertEqual(same['summary']['triggered_count'],0)
        self.assertEqual(same['rules'][0]['events'][0]['reason'],'schedule_window_limit')
        self.now=datetime(2026,11,27,0,2,tzinfo=timezone.utc);self.latest+=500;self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        new_run=self.session(rule)['active_run_id'];self.assertNotEqual(old_run,new_run)
        new_jobs=[j for j in self.jobs(rule) if j['status']=='pending']
        self.assertEqual(len(new_jobs),2)
        for job in new_jobs:
            self.assertEqual(self.repo.get_progression_event(job['progression_event_id'])['auto_trigger_rule_run_id'],new_run)
        self.repo.upsert_executor_heartbeat(executor_id='recovery-test',version='test',capabilities={'betting_acceptance_v1':True},
            status='online',last_seen_at=self.now.isoformat())
        claimed=self.repo.claim_ready_jobs('recovery-test',100)
        self.assertFalse({str(j['id']) for j in old_jobs}.intersection({j['job_id'] for j in claimed}))
        for day in (old_day,self.day):
            self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],subscription_id=self.sub['id'],
                user_id=self.user_id,stat_date=day)['starts_count'],1)

    def test_next_day_starts_while_old_unknown_stays_archived(self):
        rule=self.start();self.unknown(rule);old_day=self.day
        self.now+=timedelta(days=1);self.latest+=500;self.signal()
        result=self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'],1,'expired unknown must not lock next day')
        archive=self.archives(rule)[0]
        self.assertEqual(archive['status'],'awaiting_reconciliation')
        self.assertEqual(archive['stat_date'],old_day)
        for day in (old_day,self.day):
            self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],
                subscription_id=self.sub['id'],user_id=self.user_id,stat_date=day)['starts_count'],1)

    def test_same_day_quota_is_not_refunded_by_interruption(self):
        rule=self.start(limit=1);self.unknown(rule);self.expire();self.detach()
        self.advance();self.signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],0)
        self.assertEqual(self.repo.get_auto_trigger_cycle_daily_starts(rule_id=rule['id'],
            subscription_id=self.sub['id'],user_id=self.user_id,stat_date=self.day)['starts_count'],1)
