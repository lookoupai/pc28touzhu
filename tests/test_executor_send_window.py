from __future__ import annotations

import asyncio
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from pc28touzhu.executor.models import ExecutorJob, JobSendBlocked, JobSendUnconfirmed
from pc28touzhu.executor.runtime import run_executor_cycle
from pc28touzhu.executor.state import ExecutorStateStore
from pc28touzhu.executor.telethon_sender import TelethonMessageSender, TelethonSenderPool
from tests.test_executor_runtime import FakeApiClient, FakeSender


class ExecutorSendWindowTests(unittest.TestCase):
    def _job(self):
        payload = FakeApiClient().pull_jobs(limit=1)[0]
        payload.update(issue_no="3478696", execute_after="2026-09-06T10:26:57Z", expire_at="2026-09-06T10:28:57Z")
        return ExecutorJob.from_payload(payload)

    def _clock(self):
        return {"latest_issue_no": "3478695", "latest_open_time": "2026-09-06T10:25:00Z"}

    def test_connection_delay_past_deadline_never_sends(self):
        times = [datetime(2026, 9, 6, 10, 26, 58, tzinfo=timezone.utc)]
        sent = []

        class Clock:
            @staticmethod
            def now(tz=None):
                return times[0]

            fromisoformat = datetime.fromisoformat

        class SlowConnectSender:
            def __init__(self, **kwargs):
                pass

            def connect(self):
                times[0] = datetime(2026, 9, 6, 10, 28, 31, tzinfo=timezone.utc)

            def disconnect(self):
                pass

            def send_text(self, *args, **kwargs):
                sent.append(args)
                return {"message_id": 1}

        with patch("pc28touzhu.executor.telethon_sender.datetime", Clock), \
             patch("pc28touzhu.executor.telethon_sender.TelethonMessageSender", SlowConnectSender):
            pool = TelethonSenderPool(api_id=1, api_hash="test", draw_clock_provider=self._clock)
            with self.assertRaises(JobSendBlocked):
                pool.send_text(self._job())
        self.assertEqual(sent, [])

    def test_missing_clock_blocks_final_send(self):
        pool = TelethonSenderPool(api_id=1, api_hash="test", draw_clock_provider=lambda: None)
        job = ExecutorJob.from_payload(FakeApiClient().pull_jobs(limit=1)[0])
        with self.assertRaises(JobSendBlocked) as caught:
            pool._ensure_send_window(job)
        self.assertEqual(caught.exception.details["issue_window"]["reason"], "clock_unavailable")

    def test_entity_resolution_rechecks_before_rpc(self):
        calls = []

        class Client:
            def get_input_entity(self, value):
                calls.append("resolve")
                return value

            def send_message(self, *args):
                calls.append("send")

        sender = TelethonMessageSender(api_id=1, api_hash="test", phone="", session="unused")
        sender._client = Client()

        def blocked():
            raise JobSendBlocked("封盘")

        with self.assertRaises(JobSendBlocked):
            sender.send_text("-100123", "单1", before_send=blocked)
        self.assertEqual(calls, ["resolve"])

    def test_rpc_timeout_cancels_coroutine_without_automatic_retry(self):
        sent = []
        cancelled = []
        loop = asyncio.new_event_loop()
        self.addCleanup(loop.close)

        class Client:
            def __init__(self):
                self.loop = loop

            def get_input_entity(self, value):
                return value

            async def send_message(self, *args):
                try:
                    await asyncio.sleep(1)
                    sent.append(args)
                except asyncio.CancelledError:
                    cancelled.append(True)
                    raise

        sender = TelethonMessageSender(api_id=1, api_hash="test", phone="", session="unused")
        sender._client = Client()
        with patch("pc28touzhu.executor.telethon_sender.TELEGRAM_OPERATION_TIMEOUT_SECONDS", 0.01):
            with self.assertRaises(JobSendUnconfirmed):
                sender.send_text("-100123", "单1")
        self.assertEqual(sent, [])
        self.assertEqual(cancelled, [True])

    def test_unconfirmed_send_reports_skipped_not_retryable_failed(self):
        class Sender:
            def send_text(self, job):
                raise JobSendUnconfirmed("回执未知", details={"reason": "send_unconfirmed"})

        api = FakeApiClient()
        result = run_executor_cycle(api_client=api, message_sender=Sender(), state_store=ExecutorStateStore(),
                                    executor_id="test", limit=1, version="test", capabilities={})
        self.assertEqual(result["skipped_count"], 1)
        self.assertEqual(api.reported[0][1]["delivery_status"], "skipped")
        self.assertEqual(api.reported[0][1]["raw_result"]["reason"], "send_unconfirmed")

    def test_blocked_send_reports_expired(self):
        class Sender:
            def send_text(self, job):
                raise JobSendBlocked("封盘", details={"reason": "issue_window_blocked"})

        api = FakeApiClient()
        result = run_executor_cycle(api_client=api, message_sender=Sender(), state_store=ExecutorStateStore(),
                                    executor_id="test", limit=1, version="test", capabilities={})
        self.assertEqual(result["expired_count"], 1)
        self.assertEqual(api.reported[0][1]["delivery_status"], "expired")

    def test_delivered_local_record_replays_even_after_expiry(self):
        api = FakeApiClient()
        raw = api.pull_jobs(limit=1)[0]
        raw["expire_at"] = "2026-04-07T15:01:00Z"
        api.pull_jobs = lambda **kwargs: [raw]
        state = ExecutorStateStore()
        state.record_attempt(idempotency_key="idemp-001", delivery_status="delivered", executor_id="test",
                             attempt_no=1, remote_message_id="sent", error_message=None)
        sender = FakeSender()
        result = run_executor_cycle(api_client=api, message_sender=sender, state_store=state,
                                    executor_id="test", limit=1, version="test", capabilities={})
        self.assertEqual(result["replayed_count"], 1)
        self.assertEqual(sender.sent, [])
        self.assertEqual(api.reported[0][1]["delivery_status"], "delivered")


if __name__ == "__main__":
    unittest.main()
