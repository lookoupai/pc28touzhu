import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime,timedelta,timezone
from pc28touzhu.executor.db_repository import DatabaseRepository


def stamp(value):
    return value.isoformat().replace('+00:00','Z')


class ExecutionAcceptanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=os.path.join(self.tmp.name,'db.sqlite');self.repo=DatabaseRepository(self.path);self.repo.initialize_database()
        self.user=self.repo.create_user('acceptance')
        self.source=self.repo.create_source('internal_ai','test',owner_user_id=self.user)
        self.signal=self.repo.create_signal(source_id=self.source,lottery_type='pc28',issue_no='3510182',bet_type='combo',bet_value='大单')
        self.target=self.repo.create_delivery_target_record(user_id=self.user,executor_type='telegram_group',target_key='test',status='active',betting_protocol={'mode':'announcement','bot_id':123})['id']
        self.subscription=self.repo.create_subscription_record(user_id=self.user,source_id=self.source,status='active',strategy={})['id']
        self.repo.upsert_executor_heartbeat(executor_id='test',version='v1',capabilities={'betting_acceptance_v1':True},status='online',last_seen_at=stamp(datetime.now(timezone.utc)))

    def job(self):
        now=datetime.now(timezone.utc).replace(microsecond=0)
        event=self.repo.create_progression_event_record(subscription_id=self.subscription,user_id=self.user,signal_id=self.signal,issue_no='3510182',progression_step=1,stake_amount=1,base_stake=1,multiplier=1,max_steps=1,refund_action='hold',cap_action='reset',status='pending')
        job=self.repo.create_execution_job(user_id=self.user,signal_id=self.signal,delivery_target_id=self.target,subscription_id=self.subscription,progression_event_id=event['id'],executor_type='telegram_group',idempotency_key='test-'+str(event['id']),planned_message_text='大单1',stake_plan={'mode':'follow'},execute_after=stamp(now-timedelta(seconds=1)),expire_at=stamp(now+timedelta(seconds=120)))
        return job,event['id']

    def claim(self):
        jobs=self.repo.claim_ready_jobs('test',10);self.assertEqual(len(jobs),1);return jobs[0]

    def report(self, claim, status, **raw):
        return self.repo.report_job_result(job_id=claim['job_id'],executor_id='test',attempt_no=claim['attempt_no'],delivery_status=status,
            remote_message_id='132' if status in ('delivered','rejected','unconfirmed') else None,
            executed_at=stamp(datetime.now(timezone.utc)-timedelta(seconds=40)),raw_result={'attempt_token':claim['attempt_token'],**raw},error_message=None)

    def test_telegram_delivery_without_acceptance_never_settles(self):
        job,event=self.job();claim=self.claim();self.report(claim,'delivered')
        self.assertEqual(self.repo.get_execution_job(job)['status'],'unconfirmed')
        self.assertEqual(self.repo.get_progression_event(event)['status'],'pending')
        self.assertEqual(self.repo.claim_ready_jobs('test',10),[])
        self.assertEqual(self.repo.requeue_auto_retry_jobs(max_attempts=3,base_delay_seconds=5),[])

    def test_explicit_rejection_voids_without_profit(self):
        job,event=self.job();claim=self.claim();self.report(claim,'rejected',acceptance={'state':'rejected','reply_to':'132','reply_message_id':133,'target_round_id':'abcdef1234567890abcdef1234567890'})
        record=self.repo.get_progression_event(event)
        self.assertEqual(record['status'],'void');self.assertEqual(record['net_delta'],0)

    def test_verified_acceptance_and_duplicate_report_apply_once(self):
        job,event=self.job();claim=self.claim();self.report(claim,'delivered',acceptance={'state':'accepted','issue_no':'3510182','reply_to':'132','reply_message_id':133,'target_round_id':'abcdef1234567890abcdef1234567890'})
        self.report(claim,'delivered',acceptance={'state':'accepted','issue_no':'3510182','reply_to':'132','reply_message_id':133,'target_round_id':'abcdef1234567890abcdef1234567890'})
        self.report(claim,'failed')
        self.assertEqual(self.repo.get_progression_event(event)['status'],'placed')
        with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('SELECT COUNT(*) FROM execution_attempts WHERE job_id=?',(job,)).fetchone()[0],1)

    def test_claim_is_exclusive_and_wait_does_not_consume_attempt(self):
        job,event=self.job();claim=self.claim();deadline=claim['expire_at']
        self.assertEqual(self.repo.claim_ready_jobs('test',10),[])
        self.report(claim,'pending',deferred=True)
        self.report(claim,'pending',deferred=True)
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM execution_attempts').fetchone()[0],0)
            c.execute('UPDATE execution_jobs SET execute_after=? WHERE id=?',(stamp(datetime.now(timezone.utc)-timedelta(seconds=1)),job))
        second=self.claim();self.assertEqual(second['attempt_no'],1);self.assertEqual(second['expire_at'],deadline)
        self.assertEqual(second['telegram_random_id'],claim['telegram_random_id'])

    def test_exhausted_history_does_not_starve_recent_transient_error(self):
        # Old ineligible rows precede the new eligible error even with limit=1.
        for i in range(60):
            with sqlite3.connect(self.path) as c:
                c.execute("INSERT INTO execution_jobs(user_id,signal_id,delivery_target_id,executor_type,idempotency_key,execute_after,expire_at,status) VALUES (?,?,?,?,?,?,?,'failed')",
                    (self.user,self.signal,self.target,'telegram_group',f'old-{i}','2020-01-01T00:00:00Z','2020-01-01T00:01:00Z'))
        job,event=self.job();claim=self.claim();self.report(claim,'failed',retryable=True,failure_stage='connect')
        self.assertEqual(self.repo.get_progression_event(event)['status'],'pending')
        requeued=self.repo.requeue_auto_retry_jobs(max_attempts=3,base_delay_seconds=5,limit=1)
        self.assertEqual([r['job_id'] for r in requeued],[job]);self.assertEqual(requeued[0]['expire_at'],claim['expire_at'])
        second=self.claim();self.assertEqual(second['telegram_random_id'],claim['telegram_random_id']);self.assertEqual(second['attempt_no'],2)

    def test_expired_and_stopped_jobs_cannot_be_retried(self):
        job,event=self.job();claim=self.claim();self.report(claim,'failed',retryable=True,failure_stage='connect')
        with sqlite3.connect(self.path) as c:c.execute('UPDATE execution_jobs SET expire_at=? WHERE id=?',(stamp(datetime.now(timezone.utc)-timedelta(seconds=1)),job))
        self.assertEqual(self.repo.requeue_auto_retry_jobs(max_attempts=3,base_delay_seconds=5),[])
        with self.assertRaises(ValueError):self.repo.retry_execution_job(job_id=job,user_id=self.user)

    def test_claim_expiry_is_unknown_not_requeued(self):
        job,event=self.job();self.claim()
        with sqlite3.connect(self.path) as c:c.execute('UPDATE execution_jobs SET claim_until=? WHERE id=?',(stamp(datetime.now(timezone.utc)-timedelta(seconds=1)),job))
        self.assertEqual(self.repo.claim_ready_jobs('test',10),[])
        self.assertEqual(self.repo.get_execution_job(job)['status'],'unconfirmed')
        self.assertEqual(self.repo.get_progression_event(event)['status'],'pending')

    def test_stale_deferred_report_after_new_claim_is_idempotent(self):
        job,event=self.job();claim=self.claim();self.report(claim,'pending',deferred=True)
        with sqlite3.connect(self.path) as c:c.execute("UPDATE execution_jobs SET execute_after='2000-01-01T00:00:00Z' WHERE id=?",(job,))
        second=self.claim();self.report(claim,'pending',deferred=True)
        self.assertTrue(self.repo.validate_execution_claim(job,'test',second['attempt_token'])['allowed'])
        self.assertEqual(self.repo.get_execution_job(job)['status'],'sending')

    def test_target_stop_after_claim_blocks_send_and_unknown_blocks_manual_settlement(self):
        job,event=self.job();claim=self.claim()
        with sqlite3.connect(self.path) as c:c.execute("UPDATE delivery_targets SET status='inactive' WHERE id=?",(self.target,))
        self.assertFalse(self.repo.validate_execution_claim(job,'test',claim['attempt_token'])['allowed'])
        self.report(claim,'unconfirmed')
        with self.assertRaisesRegex(ValueError,'尚未确认'):
            self.repo.settle_progression_event(subscription_id=self.subscription,user_id=self.user,progression_event_id=event,result_type='miss')
        self.assertEqual(self.repo.get_progression_event(event)['net_delta'],0)

    def test_wrong_period_acceptance_stays_unknown(self):
        job,event=self.job();claim=self.claim()
        self.report(claim,'delivered',acceptance={'state':'accepted','issue_no':'3510181','reply_to':'132','reply_message_id':133,'target_round_id':'abcdef1234567890abcdef1234567890'})
        self.assertEqual(self.repo.get_execution_job(job)['status'],'unconfirmed')

    def test_http_contract_and_durable_report_replay_never_resend(self):
        from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
        from pc28touzhu.executor.runtime import run_executor_cycle
        from pc28touzhu.executor.state import ExecutorStateStore
        job,event=self.job();sent=[]
        app=PlatformApiApplication(repository=self.repo,executor_api_token='test-secret',session_secret='test')
        owner=self
        class Api:
            lost_report=True
            def request(self,path,method='GET',payload=None):
                captured=[]
                environ=build_testing_environ(path,method=method,body=payload,headers={'HTTP_AUTHORIZATION':'Bearer test-secret','HTTP_X_EXECUTOR_ID':'test'})
                body=b''.join(app(environ,lambda status,headers:captured.append(status)))
                response=json.loads(body)
                owner.assertEqual(captured,['200 OK'],response)
                return response
            def heartbeat(self,**kwargs):return self.request('/api/executor/heartbeat','POST',kwargs)
            def pull_jobs(self,limit):return self.request('/api/executor/jobs/pull')['items']
            def report_job(self,job_id,payload):
                result=self.request(f'/api/executor/jobs/{job_id}/report','POST',payload)
                if self.lost_report:
                    self.lost_report=False
                    raise ConnectionError('response lost after commit')
                return result
        api=Api()
        class Sender:
            def send_text(self,j):
                owner.assertEqual(j.target.metadata['betting_protocol']['bot_id'],123)
                owner.assertTrue(j.telegram_random_id);owner.assertTrue(j.attempt_token)
                verdict=api.request(f'/api/executor/jobs/{j.job_id}/validate','POST',{'attempt_token':j.attempt_token})
                owner.assertTrue(verdict['allowed'])
                sent.append(j.idempotency_key)
                return {'message_id':132,'acceptance':{'state':'accepted','issue_no':j.issue_no,'reply_to':132,'reply_message_id':133,'target_round_id':'abcdef1234567890abcdef1234567890'}}
        state_path=os.path.join(self.tmp.name,'executor.sqlite')
        args=dict(api_client=api,message_sender=Sender(),executor_id='test',limit=1,version='test',capabilities={'betting_acceptance_v1':True})
        with self.assertRaisesRegex(ConnectionError,'response lost'):
            run_executor_cycle(**args,state_store=ExecutorStateStore(state_path))
        run_executor_cycle(**args,state_store=ExecutorStateStore(state_path))
        self.assertEqual(len(sent),1)
        self.assertEqual(self.repo.get_progression_event(event)['status'],'placed')
        with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('SELECT COUNT(*) FROM execution_attempts').fetchone()[0],1)

    def test_process_crash_after_prepare_never_resends(self):
        from pc28touzhu.executor.models import ExecutorJob
        from pc28touzhu.executor.runtime import _process_job
        from pc28touzhu.executor.state import ExecutorStateStore
        job,event=self.job();claim=self.claim();state_path=os.path.join(self.tmp.name,'crash.sqlite')
        state=ExecutorStateStore(state_path);state.prepare(ExecutorJob.from_payload(claim),'test',1)
        owner=self
        class Api:
            def report_job(self,job_id,payload):
                from pc28touzhu.services.job_service import report_job
                return report_job(owner.repo,job_id,payload)
        class Sender:
            def send_text(self,j):raise AssertionError('must not send')
        _process_job(raw=claim,api_client=Api(),state_store=ExecutorStateStore(state_path),executor_id='test',message_sender=Sender())
        self.assertEqual(self.repo.get_execution_job(job)['status'],'unconfirmed')

    def test_target_change_after_claim_cannot_reuse_old_protocol(self):
        job,event=self.job();claim=self.claim()
        with sqlite3.connect(self.path) as c:c.execute("UPDATE delivery_targets SET target_key='different-chat' WHERE id=?",(self.target,))
        self.assertFalse(self.repo.validate_execution_claim(job,'test',claim['attempt_token'])['allowed'])

    def test_shared_event_can_send_initial_message_to_each_enabled_target(self):
        job,event=self.job();claim=self.claim()
        other=self.repo.create_delivery_target_record(user_id=self.user,executor_type='telegram_group',target_key='other',status='active')['id']
        second=self.repo.create_execution_job(user_id=self.user,signal_id=self.signal,delivery_target_id=other,subscription_id=self.subscription,progression_event_id=event,executor_type='telegram_group',idempotency_key='different-chat',planned_message_text='大单1',stake_plan={'mode':'follow'},execute_after=claim['execute_after'],expire_at=claim['expire_at'])
        self.report(claim,'delivered',acceptance={'state':'accepted','issue_no':'3510182','reply_to':'132','reply_message_id':133,'target_round_id':'abcdef1234567890abcdef1234567890'})
        next_claim=self.claim()
        self.assertEqual(int(next_claim['job_id']),second)
        self.assertTrue(self.repo.validate_execution_claim(second,'test',next_claim['attempt_token'])['allowed'])
