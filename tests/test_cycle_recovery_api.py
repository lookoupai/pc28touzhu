"""Authenticated WSGI recovery contracts and genuine SQLite ledger transitions."""
from __future__ import annotations

import json
import sqlite3
from urllib.parse import urlsplit
from datetime import datetime, timedelta, timezone

from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from tests.test_cycle_recovery import RecoveryFixture
from pc28touzhu.services.platform_service import list_platform_alerts


class CycleRecoveryApiTests(RecoveryFixture):
    def setUp(self):
        super().setUp()
        self.now=datetime.now(timezone.utc).replace(microsecond=0)
        self.app=PlatformApiApplication(repository=self.repo,executor_api_token='recovery-test-token',session_secret='test-secret')
        self.repo.upsert_executor_heartbeat(executor_id='recovery',version='test',capabilities={'betting_acceptance_v1':True},
            status='online',last_seen_at=self.now.isoformat())

    def request(self,path,body=None,*,user_id=None,executor=None,authenticated=True):
        headers={}
        if authenticated and executor:
            headers={'HTTP_AUTHORIZATION':'Bearer recovery-test-token','HTTP_X_EXECUTOR_ID':executor}
        elif authenticated:
            headers={'HTTP_COOKIE':'%s=%s'%(SESSION_COOKIE_NAME,build_session_cookie_value(user_id or self.user_id,'test-secret'))}
        status=[]
        parsed=urlsplit(path)
        environ=build_testing_environ(parsed.path,method='GET' if body is None else 'POST',body=body,headers=headers)
        environ['QUERY_STRING']=parsed.query
        payload=json.loads(b''.join(self.app(environ,lambda s,h:status.append(s))).decode())
        return int(status[0].split()[0]),payload

    def incident(self, *, strict=False):
        if strict:
            with self.repo._connect() as conn:
                conn.execute('UPDATE delivery_targets SET betting_protocol_json=? WHERE id=?',
                    (json.dumps({'mode':'receipt_only','bot_id':123}),self.targets[1]['id']))
        rule=self.start();claim=self.repo.claim_ready_jobs('recovery',10)[0]
        self.repo.report_job_result(claim['job_id'],'recovery',claim['attempt_no'],'unconfirmed',None,
            self.now.isoformat(),{'attempt_token':claim['attempt_token']},'unknown')
        self.expire();self.detach()
        return rule,claim

    def pull(self):
        status,payload=self.request('/api/executor/reconciliations/pull?limit=1',executor='recovery')
        self.assertEqual(status,200,payload)
        self.assertEqual(len(payload['items']),1)
        return payload['items'][0]

    def evidence(self, item, *, strict=False):
        sent=item['execute_after']
        ev={'message_id':300,'target_key':item['target']['key'],
            'telegram_account_id':(item.get('telegram_account') or {}).get('id'),
            'text':item['message_text'],'issue_no':item['issue_no'],'telegram_message_date':sent,
            'send_started_at':sent,'send_finished_at':sent,'provenance':'remote_history',
            'verified_message':{'id':300,'text':item['message_text'],'out':True,'date':sent}}
        if strict:
            # Structure B defaults to 单7 大双3.
            reply={'id':301,'sender_id':123,'reply_to':300,'date':sent,
                'text':'期号：%s\n单-7 投注成功\n大双-3 投注成功'%item['issue_no']}
            ev['acceptance_reply']=reply
        return ev

    def reconcile(self,item,*,outcome='transport_confirmed',evidence=None,executor='recovery'):
        return self.request('/api/executor/jobs/%s/reconcile'%item['job_id'],{
            'reconciliation_token':item['reconciliation_token'],
            'original_attempt_token':item['original_attempt']['token'],
            'outcome':outcome,'evidence':self.evidence(item) if evidence is None else evidence,'reason':'fixture'},executor=executor)

    def test_history_is_read_only_and_owner_scoped(self):
        rule,claim=self.incident()
        path='/api/platform/auto-trigger-rules/%s/cycle-recoveries?limit=1'%rule['id']
        with sqlite3.connect(self.repo.db_path) as conn:before=conn.execute('SELECT * FROM auto_trigger_cycle_archives').fetchall()
        status,payload=self.request(path)
        self.assertEqual(status,200,payload)
        self.assertEqual(payload['items'][0]['reserved_stake'],10)
        self.assertNotIn('policy',payload['items'][0]);self.assertNotIn('pending_plan',payload['items'][0])
        self.assertIn('next_cursor',payload)
        with sqlite3.connect(self.repo.db_path) as conn:self.assertEqual(before,conn.execute('SELECT * FROM auto_trigger_cycle_archives').fetchall())
        other=self.repo.create_user('other')
        self.assertEqual(self.request(path,user_id=other)[0],403)
        self.assertEqual(self.request(path,authenticated=False)[0],401)

    def test_runtime_summary_counts_runs_not_routes(self):
        rule=self.start(targets=self.targets[1:]);self.unknown(rule,route_id=rule['routes'][0]['id']);self.expire();self.detach()
        status,payload=self.request('/api/platform/auto-trigger-rules')
        self.assertEqual(status,200,payload)
        runtime=next(r for r in payload['items'] if r['id']==rule['id'])['cycle_runtime'][0]
        self.assertEqual(runtime.get('unresolved_run_count'),1)
        self.assertEqual(runtime['status'],'waiting')
        self.assertEqual(runtime['reserved_stake'],10)
        self.assertTrue(runtime['last_interrupted_run_id'])

    def test_archived_unknown_has_nonblocking_history_alert(self):
        rule,claim=self.incident()
        items=list_platform_alerts(self.repo,user_id=self.user_id)['items']
        alerts=[a for a in items if a['alert_type']=='cycle_history_unresolved']
        self.assertEqual(len(alerts),1)
        self.assertIn('后续按原计划',alerts[0]['message'])
        other=self.repo.create_user('alert-other')
        self.assertFalse(any(a['alert_type']=='cycle_history_unresolved' for a in list_platform_alerts(self.repo,user_id=other)['items']))

    def test_reconciliation_pull_is_not_a_send_claim(self):
        rule,claim=self.incident();item=self.pull()
        self.assertNotIn('attempt_token',item)
        self.assertEqual(item['original_attempt']['token'],claim['attempt_token'])
        self.assertEqual(item['message_text'],claim['message_text'])
        self.assertEqual(self.repo.get_execution_job(int(item['job_id']))['status'],'unconfirmed')
        self.assertEqual(self.request('/api/executor/reconciliations/pull',authenticated=False)[0],401)
        status,payload=self.request('/api/executor/reconciliations/pull',executor='different-executor')
        self.assertIn(status,(200,400));self.assertFalse(payload.get('items'))

    def test_evidence_upgrades_original_attempt_without_overwriting_audit(self):
        rule,claim=self.incident();item=self.pull()
        status,result=self.reconcile(item)
        self.assertEqual(status,200,result)
        self.assertEqual(result['delivery_status'],'delivered')
        self.assertEqual(self.reconcile(item)[0],200)
        with sqlite3.connect(self.repo.db_path) as conn:
            rows=conn.execute('SELECT delivery_status FROM execution_attempts WHERE job_id=?',(int(item['job_id']),)).fetchall()
            self.assertEqual(rows,[('unconfirmed',)])
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM execution_reconciliation_audits').fetchone()[0],1)
        archive=self.archives(rule)[0]
        self.assertEqual(archive['status'],'awaiting_settlement')
        self.assertTrue(all(self.repo.get_progression_event(e)['status']=='placed' for e in archive['pending_plan']['leg_event_ids']))
        self.assertEqual(self.session(rule)['status'],'waiting')

    def test_unknown_has_no_financial_side_effect(self):
        rule,claim=self.incident();item=self.pull();before=self.archives(rule)[0]
        status,result=self.reconcile(item,outcome='unknown',evidence={})
        self.assertEqual(status,200,result)
        self.assertEqual(self.archives(rule)[0],before)
        self.assertEqual(self.repo.get_execution_job(int(item['job_id']))['status'],'unconfirmed')

    def test_wrong_account_target_or_original_token_cannot_apply(self):
        rule,claim=self.incident();item=self.pull()
        evidence=self.evidence(item);evidence['target_key']='another-target'
        self.assertEqual(self.reconcile(item,evidence=evidence)[0],400)
        self.assertEqual(self.reconcile(item,executor='other')[0],400)
        self.assertEqual(self.repo.get_execution_job(int(item['job_id']))['status'],'unconfirmed')

    def test_strict_acceptance_requires_trusted_reply_and_exact_bets(self):
        rule,claim=self.incident(strict=True);item=self.pull();ev=self.evidence(item,strict=True)
        bad=json.loads(json.dumps(ev));bad['acceptance_reply']['sender_id']=999
        self.assertEqual(self.reconcile(item,outcome='accepted',evidence=bad)[0],400)
        bad=json.loads(json.dumps(ev));bad['acceptance_reply']['text']=bad['acceptance_reply']['text'].replace('单-7','单-8')
        self.assertEqual(self.reconcile(item,outcome='accepted',evidence=bad)[0],400)
        status,result=self.reconcile(item,outcome='accepted',evidence=ev)
        self.assertEqual(status,200,result)
        self.assertEqual(result['delivery_status'],'delivered')

    def test_remote_opaque_acceptance_never_uses_old_opening(self):
        rule,claim=self.incident(strict=True);item=self.pull();ev=self.evidence(item,strict=True)
        ev['acceptance_reply']['text']=ev['acceptance_reply']['text'].replace(item['issue_no'],'abcdef1234567890abcdef1234567890')
        ev['opening_evidence']=ev['current_opening_evidence']={'state':'ready','announcement_id':1,'issue_no':item['issue_no']}
        self.assertEqual(self.reconcile(item,outcome='accepted',evidence=ev)[0],400)
        self.assertEqual(self.repo.get_execution_job(int(item['job_id']))['status'],'unconfirmed')

    def test_late_unknown_report_cannot_undo_verified_reconciliation(self):
        rule=self.start();claim=self.repo.claim_ready_jobs('recovery',10)[0]
        self.expire();self.detach();item=self.pull()
        self.assertEqual(self.reconcile(item)[0],200)
        status,result=self.request('/api/executor/jobs/%s/report'%claim['job_id'],{
            'executor_id':'recovery','attempt_no':claim['attempt_no'],'delivery_status':'unconfirmed',
            'executed_at':self.now.isoformat(),'raw_result':{'attempt_token':claim['attempt_token']}},executor='recovery')
        self.assertEqual(status,200,result)
        self.assertEqual(self.repo.get_execution_job(int(claim['job_id']))['status'],'delivered')

    def test_late_delivered_without_ack_cannot_undo_verified_acceptance(self):
        with self.repo._connect() as conn:
            conn.execute('UPDATE delivery_targets SET betting_protocol_json=? WHERE id=?',
                (json.dumps({'mode':'receipt_only','bot_id':123}),self.targets[1]['id']))
        rule=self.start();claim=self.repo.claim_ready_jobs('recovery',10)[0]
        self.expire();self.detach();item=self.pull()
        self.assertEqual(self.reconcile(item,outcome='accepted',evidence=self.evidence(item,strict=True))[0],200)
        status,result=self.request('/api/executor/jobs/%s/report'%claim['job_id'],{
            'executor_id':'recovery','attempt_no':claim['attempt_no'],'delivery_status':'delivered',
            'remote_message_id':'300','executed_at':self.now.isoformat(),
            'raw_result':{'attempt_token':claim['attempt_token']}},executor='recovery')
        self.assertEqual(status,200,result)
        self.assertEqual(self.repo.get_execution_job(int(claim['job_id']))['status'],'delivered')
        archive=self.archives(rule)[0]
        self.assertTrue(all(self.repo.get_progression_event(e)['status']=='placed' for e in archive['pending_plan']['leg_event_ids']))

    def test_late_delivered_cannot_poison_verified_rejection_or_duplicate_audit(self):
        with self.repo._connect() as conn:
            conn.execute('UPDATE delivery_targets SET betting_protocol_json=? WHERE id=?',
                (json.dumps({'mode':'receipt_only','bot_id':123}),self.targets[1]['id']))
        rule=self.start();claim=self.repo.claim_ready_jobs('recovery',10)[0]
        self.expire();self.detach();item=self.pull()
        evidence=self.evidence(item,strict=True)
        evidence['acceptance_reply']['text']='已经封盘，投注失败'
        self.assertEqual(self.reconcile(item,outcome='rejected',evidence=evidence)[0],200)
        body={'executor_id':'recovery','attempt_no':claim['attempt_no'],'delivery_status':'delivered',
              'remote_message_id':'300','executed_at':self.now.isoformat(),
              'raw_result':{'attempt_token':claim['attempt_token']}}
        path='/api/executor/jobs/%s/report'%claim['job_id']
        for _ in range(2):
            status,result=self.request(path,body,executor='recovery')
            self.assertEqual(status,200,result)
            self.assertEqual(result['delivery_status'],'rejected')
        resolved=self.resolve_archives()
        self.assertEqual(resolved['resolved_count'],1)
        archive=self.archives(rule)[0]
        self.assertEqual(archive['capital_credit'],10)
        self.assertEqual(archive['reserved_stake'],0)
        with self.repo._connect() as conn:
            for event_id in archive['pending_plan']['leg_event_ids']:
                self.repo._sync_progression_event_delivery_status(conn,progression_event_id=event_id,now=self.now.isoformat())
        self.assertTrue(all(self.repo.get_progression_event(e)['status']=='void' for e in archive['pending_plan']['leg_event_ids']))
        with sqlite3.connect(self.repo.db_path) as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM execution_attempts WHERE delivery_status='delivered'").fetchone()[0],0)
            self.assertEqual(conn.execute('SELECT COUNT(*) FROM execution_ignored_reports').fetchone()[0],1)
