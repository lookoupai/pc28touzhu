"""Recovery tests exercise real runtime decisions with offline transports."""
from __future__ import annotations

import asyncio
import copy
import os
import tempfile
import threading
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pc28touzhu.executor.models import ExecutorJob
from pc28touzhu.executor.runtime import run_executor_cycle
from pc28touzhu.executor import runtime
from pc28touzhu.executor.api_client import ExecutorApiClient
from pc28touzhu.executor.state import ExecutorStateStore
from pc28touzhu.executor.telethon_sender import TelethonMessageSender, TelethonSenderPool


def job_payload():
    now = datetime.now(timezone.utc)
    return {
        'job_id': '7', 'signal_id': '8', 'lottery_type': 'pc28', 'issue_no': '3510978',
        'bet_type': 'odd_even', 'bet_value': '双', 'message_text': '双9 大单4',
        'stake_plan': {'amount': 13},
        'target': {'type': 'telegram_group', 'key': '-100123', 'metadata': {'betting_protocol': {'mode': 'legacy'}}},
        'idempotency_key': 'recovery-7', 'attempt_token': 'offline-attempt', 'attempt_no': 1,
        'execute_after': (now - timedelta(seconds=2)).isoformat(),
        'expire_at': (now + timedelta(seconds=120)).isoformat(),
    }


class RecoveryApi:
    def __init__(self, raw=None):
        self.raw = raw or job_payload()
        self.reports = []

    def heartbeat(self, **kwargs):
        return {}

    def pull_jobs(self, **kwargs):
        return [self.raw]

    def report_job(self, *, job_id, payload):
        self.reports.append(payload)
        return {'ok': True}


class TransportClassificationTests(unittest.TestCase):
    def run_with_sender(self, sender):
        api = RecoveryApi()
        result = run_executor_cycle(
            api_client=api, message_sender=sender, state_store=ExecutorStateStore(),
            executor_id='offline', limit=1, version='test', capabilities={},
        )
        return api.reports[0], result

    def test_eof_during_connect_is_retryable_without_a_send(self):
        for failure in (EOFError('closed before sending'), asyncio.IncompleteReadError(b'', 8)):
            with self.subTest(error=type(failure).__name__):
                sent = []

                class Sender:
                    def __init__(self, **kwargs): pass
                    def connect(self): raise failure
                    def disconnect(self): pass
                    def send_text(self, *args, **kwargs): sent.append(True)

                with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Sender):
                    report, result = self.run_with_sender(TelethonSenderPool(api_id=1, api_hash='offline'))
                self.assertEqual(sent, [])
                self.assertEqual(report['delivery_status'], 'failed')
                self.assertTrue(report['raw_result']['retryable'])
                self.assertEqual(report['raw_result']['failure_stage'], 'connect')
                self.assertEqual(report['raw_result']['exception_type'], type(failure).__name__)
                self.assertEqual(result['failed_count'], 1)

    def test_eof_resolving_target_is_retryable_before_rpc(self):
        sent = []

        class Client:
            def get_input_entity(self, target): raise asyncio.IncompleteReadError(b'', 8)
            def get_dialogs(self): raise asyncio.IncompleteReadError(b'', 8)
            def send_message(self, *args): sent.append(True)

        sender = TelethonMessageSender(api_id=1, api_hash='offline', phone='', session='unused')
        sender._client = Client()

        class Adapter:
            def send_text(self, job):
                return sender.send_text(job.target.key, job.message_text)

        report, _ = self.run_with_sender(Adapter())
        self.assertEqual(sent, [])
        self.assertEqual(report['delivery_status'], 'failed')
        self.assertEqual(report['raw_result']['preparation_stage'], 'resolve_entity')
        self.assertEqual(report['raw_result']['failure_stage'], 'connect')

    def test_eof_after_rpc_starts_stays_unknown_without_resend(self):
        sent = []

        class Client:
            def get_input_entity(self, target): return target
            def send_message(self, *args):
                sent.append(args)
                raise asyncio.IncompleteReadError(b'', 8)

        sender = TelethonMessageSender(api_id=1, api_hash='offline', phone='', session='unused')
        sender._client = Client()

        class Adapter:
            def send_text(self, job):
                return sender.send_text(job.target.key, job.message_text)

        report, result = self.run_with_sender(Adapter())
        self.assertEqual(len(sent), 1)
        self.assertEqual(report['delivery_status'], 'unconfirmed')
        self.assertEqual(report['raw_result'].get('stage'), 'send_message')
        self.assertIn('send_started_at', report['raw_result'])
        self.assertNotIn('retryable', report['raw_result'])
        self.assertEqual(result['unconfirmed_count'], 1)


def reconciliation_payload(raw=None, evidence=None):
    item = copy.deepcopy(raw or job_payload())
    item['original_attempt'] = {'token': item.pop('attempt_token'), 'no': item.pop('attempt_no'),
                                'executor_id': 'offline'}
    item['reconciliation_token'] = 'offline-reconciliation'
    item['delivery_evidence'] = evidence or {}
    return item


def transport_evidence(raw, message_id=131):
    now = datetime.now(timezone.utc).isoformat()
    return {'message_id': message_id, 'target_key': raw['target']['key'],
            'telegram_account_id': (raw.get('telegram_account') or {}).get('id'),
            'sender_session_path': (raw.get('telegram_account') or {}).get('session_path', 'telegram-session'),
            'text': raw['message_text'], 'issue_no': raw['issue_no'],
            'send_started_at': now, 'send_finished_at': now, 'telegram_message_date': now}


class ReconciliationApi(RecoveryApi):
    def __init__(self, item):
        super().__init__()
        self.item = item
        self.reconciled = []
        self.pulled = []
        self.jobs = []

    def pull_jobs(self, **kwargs):
        return self.jobs

    def pull_reconciliation_jobs(self, *, limit):
        self.pulled.append(limit)
        return [self.item]

    def reconcile_job(self, *, job_id, payload):
        self.reconciled.append((job_id, payload))
        return {'outcome': payload['outcome'], 'updated': payload['outcome'] != 'unknown'}


class TransportReconciliationTests(unittest.TestCase):
    def run_reconciliation(self, api, sender, state):
        operation = getattr(runtime, 'run_reconciliation_cycle', None)
        self.assertTrue(callable(operation), 'runtime must expose bounded read-only reconciliation')
        return operation(api_client=api, message_sender=sender, state_store=state, executor_id='offline')

    def test_transport_evidence_is_durable_before_acceptance_wait(self):
        raw = job_payload()
        raw['target']['metadata']['betting_protocol'] = {'mode': 'receipt_only', 'bot_id': 123}
        evidence = transport_evidence(raw)
        sent = []

        class Sender:
            def __init__(self, **kwargs): pass
            def connect(self): pass
            def disconnect(self): pass
            def send_text(self, *args, **kwargs):
                sent.append(args)
                return dict(evidence)
            def read_target_messages(self, *args, **kwargs): raise KeyboardInterrupt('offline crash during ack')

        with tempfile.TemporaryDirectory() as directory:
            path = os.path.join(directory, 'state.sqlite')
            api = RecoveryApi(raw)
            with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Sender):
                pool = TelethonSenderPool(api_id=1, api_hash='offline')
                with self.assertRaises(KeyboardInterrupt):
                    run_executor_cycle(api_client=api, message_sender=pool, state_store=ExecutorStateStore(path),
                                       executor_id='offline', limit=1, version='test', capabilities={})
                reloaded = ExecutorStateStore(path)
                saved = reloaded.get_record(raw['idempotency_key'])
                self.assertEqual((saved.get('transport_evidence') or {}).get('message_id'), 131)
                run_executor_cycle(api_client=api, message_sender=pool, state_store=reloaded,
                                   executor_id='offline', limit=1, version='test', capabilities={})
            self.assertEqual(len(sent), 1)
            self.assertEqual(api.reports[-1]['delivery_status'], 'unconfirmed')
            self.assertEqual(api.reports[-1]['raw_result'].get('message_id'), 131)

    def test_persisted_original_success_is_reconciled_without_a_remote_read(self):
        raw = job_payload()
        state = ExecutorStateStore()
        job = ExecutorJob.from_payload(raw)
        state.prepare(job, 'offline', 1)
        state.store_result(job, {'executor_id': 'offline', 'attempt_no': 1, 'delivery_status': 'delivered',
            'remote_message_id': '131', 'raw_result': {**transport_evidence(raw), 'attempt_token': job.attempt_token}})
        state.reported(job.idempotency_key)
        api = ReconciliationApi(reconciliation_payload(raw))

        class NoRemote:
            def reconcile_text(self, *args, **kwargs): raise AssertionError('local evidence should suffice')
            def send_text(self, *args, **kwargs): raise AssertionError('reconciliation never sends')

        self.run_reconciliation(api, NoRemote(), state)
        self.assertEqual(api.pulled, [1])
        self.assertEqual(api.reconciled[0][1]['outcome'], 'transport_confirmed')
        self.assertEqual(api.reconciled[0][1]['evidence'].get('provenance'), 'local_result')
        self.assertEqual(api.reconciled[0][1]['original_attempt_token'], 'offline-attempt')

    def test_legacy_local_acceptance_without_raw_reply_is_rechecked(self):
        raw = job_payload()
        raw['target']['metadata']['betting_protocol'] = {'mode': 'receipt_only', 'bot_id': 123}
        state = ExecutorStateStore()
        job = ExecutorJob.from_payload(raw)
        state.prepare(job, 'offline', 1)
        state.store_result(job, {'executor_id': 'offline', 'attempt_no': 1, 'delivery_status': 'delivered',
            'raw_result': {**transport_evidence(raw), 'attempt_token': job.attempt_token,
                           'acceptance': {'state': 'accepted', 'reply_message_id': 132}}})
        api = ReconciliationApi(reconciliation_payload(raw))
        reads = []

        class Reader:
            def reconcile_text(self, job, evidence):
                reads.append(evidence['message_id'])
                return {'outcome': 'unknown', 'evidence': evidence, 'reason': 'raw_reply_unavailable'}

        self.run_reconciliation(api, Reader(), state)
        self.assertEqual(reads, [131])
        self.assertEqual(api.reconciled[0][1]['outcome'], 'unknown')

    def test_changed_original_identity_cannot_reuse_local_evidence(self):
        raw = job_payload()
        state = ExecutorStateStore()
        job = ExecutorJob.from_payload(raw)
        state.prepare(job, 'offline', 1)
        state.store_result(job, {'executor_id': 'offline', 'attempt_no': 1, 'delivery_status': 'delivered',
            'raw_result': {**transport_evidence(raw), 'attempt_token': job.attempt_token}})
        altered = reconciliation_payload(raw)
        altered['target']['key'] = '-100999'
        api = ReconciliationApi(altered)

        class NoRemote:
            def reconcile_text(self, *args, **kwargs): raise AssertionError('identity mismatch must not query another target')

        self.run_reconciliation(api, NoRemote(), state)
        self.assertEqual(api.reconciled[0][1]['outcome'], 'unknown')
        self.assertEqual(api.reconciled[0][1]['reason'], 'local_identity_mismatch')

    def test_another_attempt_cannot_reuse_persisted_success(self):
        raw = job_payload()
        state = ExecutorStateStore()
        job = ExecutorJob.from_payload(raw)
        state.prepare(job, 'offline', 1)
        state.store_result(job, {'executor_id': 'offline', 'attempt_no': 1, 'delivery_status': 'delivered',
            'raw_result': {**transport_evidence(raw), 'attempt_token': job.attempt_token}})
        item = reconciliation_payload(raw)
        item['original_attempt']['token'] = 'different-attempt'
        api = ReconciliationApi(item)

        class NoRemote:
            def reconcile_text(self, *args, **kwargs): raise AssertionError('wrong attempt cannot read')

        self.run_reconciliation(api, NoRemote(), state)
        self.assertEqual(api.reconciled[0][1]['outcome'], 'unknown')
        self.assertEqual(api.reconciled[0][1]['reason'], 'local_attempt_mismatch')

    def test_no_message_identity_stays_unknown_without_network_or_send(self):
        api = ReconciliationApi(reconciliation_payload())

        class NoRemote:
            def reconcile_text(self, *args, **kwargs): raise AssertionError('missing message id is not searchable proof')
            def send_text(self, *args, **kwargs): raise AssertionError('reconciliation never sends')

        self.run_reconciliation(api, NoRemote(), ExecutorStateStore())
        self.assertEqual(api.reconciled[0][1]['outcome'], 'unknown')
        self.assertEqual(api.reconciled[0][1]['reason'], 'message_id_unavailable')

    def test_known_id_reads_original_message_without_sending(self):
        raw = job_payload()
        item = reconciliation_payload(raw, transport_evidence(raw))
        calls = []

        class Reader:
            def __init__(self, **kwargs): pass
            def connect(self): calls.append('connect')
            def disconnect(self): calls.append('disconnect')
            def send_text(self, *args, **kwargs): raise AssertionError('read-only')
            def read_message(self, target, message_id):
                calls.append((target, message_id))
                return {'id': 131, 'text': raw['message_text'], 'out': True,
                        'date': item['delivery_evidence']['telegram_message_date']}

        with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Reader):
            pool = TelethonSenderPool(api_id=1, api_hash='offline')
            operation = getattr(pool, 'reconcile_text', None)
            self.assertTrue(callable(operation))
            result = operation(ExecutorJob.from_payload(item), item['delivery_evidence'])
        self.assertEqual(result['outcome'], 'transport_confirmed')
        self.assertEqual(result['evidence'].get('provenance'), 'remote_history')
        self.assertIn(('-100123', 131), calls)

    def test_wrong_remote_message_never_confirms_transport(self):
        raw = job_payload()
        item = reconciliation_payload(raw, transport_evidence(raw))

        class Reader:
            def __init__(self, **kwargs): pass
            def connect(self): pass
            def disconnect(self): pass
            def read_message(self, *args):
                return {'id': 131, 'text': '双90 大单40', 'out': True,
                        'date': item['delivery_evidence']['telegram_message_date']}

        with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Reader):
            pool = TelethonSenderPool(api_id=1, api_hash='offline')
            operation = getattr(pool, 'reconcile_text', None)
            self.assertTrue(callable(operation))
            result = operation(ExecutorJob.from_payload(item), item['delivery_evidence'])
        self.assertEqual(result['outcome'], 'unknown')

    def test_strict_receipt_is_parsed_from_original_linked_reply(self):
        raw = job_payload()
        raw['target']['metadata']['betting_protocol'] = {'mode': 'receipt_only', 'bot_id': 123}
        evidence = transport_evidence(raw)
        for sender_id, issue_no, expected in ((123, raw['issue_no'], 'accepted'),
                                              (999, raw['issue_no'], 'unknown'),
                                              (123, '3510979', 'unknown')):
            with self.subTest(sender_id=sender_id, issue=issue_no):
                class Reader:
                    def __init__(self, **kwargs): pass
                    def connect(self): pass
                    def disconnect(self): pass
                    def read_message(self, *args):
                        return {'id': 131, 'text': raw['message_text'], 'out': True,
                                'date': evidence['telegram_message_date']}
                    def read_reconciliation_messages(self, *args, **kwargs):
                        return [{'id': 132, 'sender_id': sender_id, 'reply_to': 131,
                                 'date': evidence['telegram_message_date'],
                                 'text': '期号：' + issue_no + '\n双 - 9 投注成功\n大单 - 4 投注成功'}]
                with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Reader):
                    result = TelethonSenderPool(api_id=1, api_hash='offline').reconcile_text(
                        ExecutorJob.from_payload(raw), evidence)
                self.assertEqual(result['outcome'], expected)

    def test_strict_verified_reply_preserves_sender_identity_for_server_validation(self):
        raw = job_payload()
        raw['target']['metadata']['betting_protocol'] = {'mode': 'receipt_only', 'bot_id': 123}
        evidence = transport_evidence(raw)

        class Reader:
            def __init__(self, **kwargs): pass
            def connect(self): pass
            def disconnect(self): pass
            def read_message(self, *args):
                return {'id': 131, 'text': raw['message_text'], 'out': True,
                        'date': evidence['telegram_message_date']}
            def read_reconciliation_messages(self, *args, **kwargs):
                return [{'id': 132, 'sender_id': 123, 'reply_to': 131,
                         'date': evidence['telegram_message_date'],
                         'text': '期号：3510978\n双 - 9 投注成功\n大单 - 4 投注成功'}]

        with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Reader):
            result = TelethonSenderPool(api_id=1, api_hash='offline').reconcile_text(
                ExecutorJob.from_payload(raw), evidence)
        self.assertEqual(result['outcome'], 'accepted')
        self.assertEqual((result['evidence'].get('acceptance_reply') or {}).get('sender_id'), 123)
        self.assertEqual(result['evidence']['acceptance_reply']['reply_to'], 131)

    def test_background_read_budget_cancels_slow_remote_query(self):
        loop = asyncio.new_event_loop()
        self.addCleanup(loop.close)
        raw = job_payload()
        evidence = transport_evidence(raw)
        cancelled = []

        class Client:
            def __init__(self): self.loop = loop
            def get_input_entity(self, target): return target
            async def get_messages(self, *args, **kwargs):
                try:
                    await asyncio.sleep(1)
                except asyncio.CancelledError:
                    cancelled.append(True)
                    raise
            def disconnect(self): pass

        sender = TelethonMessageSender(api_id=1, api_hash='offline', phone='', session='unused')
        sender._client = Client()
        pool = TelethonSenderPool(api_id=1, api_hash='offline', default_session='unused')
        evidence['sender_session_path'] = 'unused'
        pool._senders['default'] = sender
        with patch('pc28touzhu.executor.telethon_sender.RECONCILIATION_READ_BUDGET_SECONDS', .02, create=True):
            before = time.monotonic()
            result = pool.reconcile_text(ExecutorJob.from_payload(raw), evidence)
        self.assertEqual(result['outcome'], 'unknown')
        self.assertLess(time.monotonic() - before, .2)
        self.assertEqual(cancelled, [True])

    def test_original_session_mismatch_never_queries_cached_sender(self):
        raw = job_payload()
        raw['telegram_account'] = {'id': 3, 'label': 'test', 'phone': '', 'session_path': 'original'}
        evidence = transport_evidence(raw)
        pool = TelethonSenderPool(api_id=1, api_hash='offline')

        class WrongSession:
            session = 'changed-session'
            def connect(self): raise AssertionError('must not read using another session')
            def disconnect(self): pass

        pool._senders['account:3'] = WrongSession()
        result = pool.reconcile_text(ExecutorJob.from_payload(raw), evidence)
        self.assertEqual(result['reason'], 'original_session_unavailable')

    def test_default_account_without_frozen_session_identity_is_not_queried(self):
        raw = job_payload()
        pool = TelethonSenderPool(api_id=1, api_hash='offline', default_session='new-default')

        class UnverifiableDefault:
            def connect(self): raise AssertionError('cannot prove historical default account')
            def disconnect(self): pass

        pool._senders['default'] = UnverifiableDefault()
        evidence = transport_evidence(raw)
        evidence.pop('sender_session_path')
        result = pool.reconcile_text(ExecutorJob.from_payload(raw), evidence)
        self.assertEqual(result['reason'], 'original_session_unavailable')

    def test_foreground_waiter_prevents_background_read(self):
        raw = job_payload()
        pool = TelethonSenderPool(api_id=1, api_hash='offline')
        gate = getattr(pool, '_foreground_send', None)
        self.assertTrue(callable(gate), 'foreground sends need priority over new background reads')
        lock = threading.RLock()
        pool._account_locks['default'] = lock
        with gate('default', lock):
            result = pool.reconcile_text(ExecutorJob.from_payload(raw), transport_evidence(raw))
        self.assertEqual(result['reason'], 'account_busy')

    def test_busy_account_skips_reconciliation_without_waiting(self):
        raw = job_payload()
        pool = TelethonSenderPool(api_id=1, api_hash='offline')
        lock = threading.Lock()
        pool._account_locks['default'] = lock
        lock.acquire()
        self.addCleanup(lock.release)
        started = time.monotonic()
        result = pool.reconcile_text(ExecutorJob.from_payload(raw), transport_evidence(raw))
        self.assertEqual(result['reason'], 'account_busy')
        self.assertLess(time.monotonic() - started, .1)

    def test_strict_opaque_round_without_historical_mapping_stays_unknown(self):
        raw = job_payload()
        raw['target']['metadata']['betting_protocol'] = {'mode': 'announcement', 'bot_id': 123}
        evidence = transport_evidence(raw)

        class Reader:
            def __init__(self, **kwargs): pass
            def connect(self): pass
            def disconnect(self): pass
            def read_message(self, *args):
                return {'id': 131, 'text': raw['message_text'], 'out': True,
                        'date': evidence['telegram_message_date']}
            def read_reconciliation_messages(self, *args, **kwargs):
                return [{'id': 132, 'sender_id': 123, 'reply_to': 131,
                         'date': evidence['telegram_message_date'],
                         'text': '期号：abcdef1234567890abcdef1234567890\n双 - 9 投注成功\n大单 - 4 投注成功'}]
            def read_target_messages(self, *args, **kwargs): raise AssertionError('must not use current latest announcement')

        with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Reader):
            result = TelethonSenderPool(api_id=1, api_hash='offline').reconcile_text(
                ExecutorJob.from_payload(raw), evidence)
        self.assertEqual(result['outcome'], 'unknown')
        self.assertEqual(result['reason'], 'acceptance_issue_mismatch')

    def test_remote_late_opaque_reply_cannot_reuse_old_opening_snapshots(self):
        raw = job_payload()
        raw['target']['metadata']['betting_protocol'] = {'mode': 'announcement', 'bot_id': 123}
        evidence = transport_evidence(raw)
        opening = {'state': 'ready', 'announcement_id': 100, 'issue_no': raw['issue_no'],
                   'observed_at': evidence['send_started_at']}
        evidence.update(opening_evidence=opening, current_opening_evidence=opening)

        class Reader:
            def __init__(self, **kwargs): pass
            def connect(self): pass
            def disconnect(self): pass
            def read_message(self, *args):
                return {'id': 131, 'text': raw['message_text'], 'out': True,
                        'date': evidence['telegram_message_date']}
            def read_reconciliation_messages(self, *args, **kwargs):
                return [{'id': 132, 'sender_id': 123, 'reply_to': 131,
                         'date': (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
                         'text': '期号：abcdef1234567890abcdef1234567890\n双 - 9 投注成功\n大单 - 4 投注成功'}]

        with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Reader):
            result = TelethonSenderPool(api_id=1, api_hash='offline').reconcile_text(
                ExecutorJob.from_payload(raw), evidence)
        self.assertEqual(result['outcome'], 'unknown')
        self.assertEqual(result['reason'], 'historical_round_mapping_unavailable')

    def test_local_opaque_acceptance_requires_contemporaneous_reply_time(self):
        raw = job_payload()
        raw['target']['metadata']['betting_protocol'] = {'mode': 'announcement', 'bot_id': 123}
        evidence = transport_evidence(raw)
        opening = {'state': 'ready', 'announcement_id': 100, 'issue_no': raw['issue_no'],
                   'observed_at': evidence['send_started_at']}
        evidence.update(opening_evidence=opening, current_opening_evidence=opening,
            acceptance={'state': 'accepted', 'reply_message_id': 132},
            acceptance_reply={'id': 132, 'sender_id': 123, 'reply_to': 131,
                'date': (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
                'text': '期号：abcdef1234567890abcdef1234567890\n双 - 9 投注成功\n大单 - 4 投注成功'})
        state = ExecutorStateStore()
        job = ExecutorJob.from_payload(raw)
        state.prepare(job, 'offline', 1)
        state.store_result(job, {'executor_id': 'offline', 'attempt_no': 1, 'delivery_status': 'delivered',
            'raw_result': {**evidence, 'attempt_token': job.attempt_token}})
        api = ReconciliationApi(reconciliation_payload(raw))

        class Reader:
            def reconcile_text(self, *args):
                return {'outcome': 'unknown', 'evidence': {}, 'reason': 'historical_round_mapping_unavailable'}

        self.run_reconciliation(api, Reader(), state)
        self.assertEqual(api.reconciled[0][1]['outcome'], 'unknown')

    def test_remote_missing_message_never_becomes_not_sent(self):
        raw = job_payload()

        class Reader:
            def __init__(self, **kwargs): pass
            def connect(self): pass
            def disconnect(self): pass
            def read_message(self, *args): return None

        with patch('pc28touzhu.executor.telethon_sender.TelethonMessageSender', Reader):
            result = TelethonSenderPool(api_id=1, api_hash='offline').reconcile_text(
                ExecutorJob.from_payload(raw), transport_evidence(raw))
        self.assertEqual(result['outcome'], 'unknown')

    def test_slow_reconciliation_does_not_block_next_normal_send(self):
        raw = job_payload()
        api = ReconciliationApi(reconciliation_payload(raw, transport_evidence(raw)))
        started, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)
        sent = []

        class Sender:
            def reconcile_text(self, *args, **kwargs):
                started.set()
                release.wait(3)
                return {'outcome': 'unknown', 'evidence': {}, 'reason': 'offline'}
            def send_text(self, job):
                sent.append(job.job_id)
                return {'message_id': 222}

        state = ExecutorStateStore()
        args = dict(api_client=api, message_sender=Sender(), state_store=state,
                    executor_id='offline', limit=1, version='test', capabilities={})
        run_executor_cycle(**args)
        self.assertTrue(started.wait(.5), 'idle executor should start background reconciliation')
        fresh = job_payload()
        fresh.update(job_id='9', idempotency_key='fresh-9')
        api.jobs = [fresh]
        before = time.monotonic()
        result = run_executor_cycle(**args)
        self.assertLess(time.monotonic() - before, .5)
        self.assertEqual(result['delivered_count'], 1)
        self.assertEqual(sent, ['9'])
        self.assertEqual(api.pulled, [1])
        release.set()

    def test_client_uses_dedicated_reconciliation_endpoints(self):
        client = ExecutorApiClient(base_url='http://offline', token='test', executor_id='offline')
        pull = getattr(client, 'pull_reconciliation_jobs', None)
        report = getattr(client, 'reconcile_job', None)
        self.assertTrue(callable(pull) and callable(report))
        calls = []
        client._request = lambda method, path, payload=None: calls.append((method, path, payload)) or {'items': []}
        pull(limit=1)
        report(job_id='7', payload={'outcome': 'unknown'})
        self.assertEqual(calls[0][:2], ('GET', '/api/executor/reconciliations/pull?limit=1'))
        self.assertEqual(calls[1][:2], ('POST', '/api/executor/jobs/7/reconcile'))


class TransportApiIntegrationTests(unittest.TestCase):
    def test_persisted_results_reconcile_through_real_wsgi_into_original_ledger(self):
        from tests.test_cycle_recovery_api import CycleRecoveryApiTests

        for strict in (False, True):
            with self.subTest(strict=strict):
                fixture = CycleRecoveryApiTests()
                fixture.setUp()
                try:
                    rule, claim = fixture.incident(strict=strict)
                    state = ExecutorStateStore()
                    job = ExecutorJob.from_payload(claim)
                    state.prepare(job, 'recovery', claim['attempt_no'])
                    evidence = transport_evidence(claim, message_id=300)
                    evidence.update(send_started_at=claim['execute_after'], send_finished_at=claim['execute_after'],
                                    telegram_message_date=claim['execute_after'])
                    if strict:
                        evidence['acceptance_reply'] = {
                            'id': 301, 'sender_id': 123, 'reply_to': 300, 'date': claim['execute_after'],
                            'text': '期号：%s\n单-7 投注成功\n大双-3 投注成功' % claim['issue_no'],
                        }
                    state.store_result(job, {'executor_id': 'recovery', 'attempt_no': claim['attempt_no'],
                        'delivery_status': 'delivered', 'remote_message_id': '300',
                        'raw_result': {**evidence, 'attempt_token': claim['attempt_token']}})
                    state.reported(job.idempotency_key)
                    client = ExecutorApiClient(base_url='http://offline', token='offline', executor_id='recovery')

                    def request(method, path, payload=None):
                        code, response = fixture.request(path, body=payload, executor='recovery')
                        self.assertEqual(code, 200, response)
                        return response

                    client._request = request

                    class NoNetwork:
                        def reconcile_text(self, *args): raise AssertionError('complete local evidence needs no remote read')
                        def send_text(self, *args): raise AssertionError('history must never resend')

                    result = runtime.run_reconciliation_cycle(api_client=client, message_sender=NoNetwork(),
                        state_store=state, executor_id='recovery')
                    self.assertEqual(result['outcomes'], ['accepted' if strict else 'transport_confirmed'])
                    archive = fixture.archives(rule)[0]
                    self.assertTrue(all(fixture.repo.get_progression_event(event_id)['status'] == 'placed'
                                        for event_id in archive['pending_plan']['leg_event_ids']))
                    self.assertEqual(fixture.session(rule)['status'], 'waiting')
                finally:
                    fixture.doCleanups()


if __name__ == '__main__':
    unittest.main()
