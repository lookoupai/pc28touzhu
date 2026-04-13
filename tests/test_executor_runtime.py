from __future__ import annotations

import sys
import tempfile
import types
import unittest
from unittest.mock import patch
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.executor import ExecutorStateStore, run_executor_cycle
from pc28touzhu.executor.models import ExecutorJob
from pc28touzhu.executor.telethon_sender import TelethonMessageSender, TelethonSenderPool, _coerce_entity


class FakeApiClient:
    def __init__(self):
        self.reported = []

    def heartbeat(self, *, version, capabilities):
        return {"version": version, "capabilities": capabilities, "status": "online"}

    def pull_jobs(self, *, limit):
        return [
            {
                "job_id": "job_001",
                "signal_id": "sig_001",
                "lottery_type": "pc28",
                "issue_no": "20260407001",
                "bet_type": "big_small",
                "bet_value": "大",
                "message_text": "大10",
                "stake_plan": {"mode": "flat", "amount": 10},
                "target": {"type": "telegram_group", "key": "-1001234567890", "name": "测试群"},
                "telegram_account": {
                    "id": 99,
                    "label": "主账号",
                    "phone": "+12019362923",
                    "session_path": "/data/user_1/main",
                },
                "idempotency_key": "idemp-001",
                "execute_after": "2026-04-07T15:00:00Z",
                "expire_at": "2099-04-07T15:01:00Z",
            }
        ][:limit]

    def report_job(self, *, job_id, payload):
        self.reported.append((job_id, payload))
        return {"ok": True}


class FakeSender:
    def __init__(self):
        self.sent = []

    def send_text(self, job):
        self.sent.append((job.telegram_account.id if job.telegram_account else None, job.target.key, job.message_text))
        return {"message_id": "m001", "target_key": job.target.key}


class ExecutorRuntimeTests(unittest.TestCase):
    def test_run_executor_cycle_reports_delivered_job(self):
        api_client = FakeApiClient()
        sender = FakeSender()
        state = ExecutorStateStore()

        result = run_executor_cycle(
            api_client=api_client,
            message_sender=sender,
            state_store=state,
            executor_id="exec-1",
            limit=10,
            version="test/0.1.0",
            capabilities={"send": True},
        )

        self.assertEqual(result["pulled_count"], 1)
        self.assertEqual(result["delivered_count"], 1)
        self.assertEqual(result["failed_count"], 0)
        self.assertEqual(result["replayed_count"], 0)
        self.assertEqual(sender.sent[0][0], 99)
        self.assertEqual(sender.sent[0][1], "-1001234567890")
        self.assertEqual(api_client.reported[0][0], "job_001")
        self.assertTrue(state.has_delivered("idemp-001"))

    def test_run_executor_cycle_reports_failed_job_when_sender_raises(self):
        class ExplodingSender:
            def send_text(self, job):
                raise RuntimeError("telethon timeout")

        api_client = FakeApiClient()
        state = ExecutorStateStore()

        result = run_executor_cycle(
            api_client=api_client,
            message_sender=ExplodingSender(),
            state_store=state,
            executor_id="exec-1",
            limit=10,
            version="test/0.1.0",
            capabilities={"send": True},
        )

        self.assertEqual(result["failed_count"], 1)
        self.assertEqual(api_client.reported[0][1]["delivery_status"], "failed")
        self.assertEqual(api_client.reported[0][1]["error_message"], "telethon timeout")
        self.assertEqual(state.get_record("idemp-001")["delivery_status"], "failed")

    def test_run_executor_cycle_replays_delivered_result_without_resend(self):
        api_client = FakeApiClient()
        sender = FakeSender()
        state = ExecutorStateStore()
        state.record_attempt(
            idempotency_key="idemp-001",
            delivery_status="delivered",
            executor_id="exec-1",
            attempt_no=1,
            remote_message_id="m001",
            error_message=None,
        )

        result = run_executor_cycle(
            api_client=api_client,
            message_sender=sender,
            state_store=state,
            executor_id="exec-1",
            limit=10,
            version="test/0.1.0",
            capabilities={"send": True},
        )

        self.assertEqual(result["replayed_count"], 1)
        self.assertEqual(sender.sent, [])
        self.assertEqual(api_client.reported[0][1]["delivery_status"], "delivered")
        self.assertTrue(api_client.reported[0][1]["raw_result"]["replayed_from_local_state"])

    def test_coerce_entity_supports_numeric_and_username(self):
        self.assertEqual(_coerce_entity("-1001234567890"), -1001234567890)
        self.assertEqual(_coerce_entity("my_channel"), "my_channel")

    def test_telethon_sender_uses_existing_authorized_session_without_phone(self):
        calls = []

        class FakeTelegramClient:
            def __init__(self, session, api_id, api_hash):
                calls.append(("init", session, api_id, api_hash))

            def connect(self):
                calls.append(("connect",))

            def is_user_authorized(self):
                calls.append(("is_user_authorized",))
                return True

            def disconnect(self):
                calls.append(("disconnect",))

        fake_sync = types.ModuleType("telethon.sync")
        fake_sync.TelegramClient = FakeTelegramClient
        fake_telethon = types.ModuleType("telethon")

        with patch.dict(sys.modules, {"telethon": fake_telethon, "telethon.sync": fake_sync}):
            sender = TelethonMessageSender(
                api_id=123456,
                api_hash="hash",
                phone="",
                session="imported-session",
            )
            sender.connect()
            sender.disconnect()

        self.assertIn(("connect",), calls)
        self.assertIn(("is_user_authorized",), calls)
        self.assertNotIn(("start",), calls)

    def test_telethon_sender_rejects_unauthorized_session(self):
        calls = []

        class FakeTelegramClient:
            def __init__(self, session, api_id, api_hash):
                calls.append(("init", session, api_id, api_hash))

            def connect(self):
                calls.append(("connect",))

            def is_user_authorized(self):
                calls.append(("is_user_authorized",))
                return False

        fake_sync = types.ModuleType("telethon.sync")
        fake_sync.TelegramClient = FakeTelegramClient
        fake_telethon = types.ModuleType("telethon")

        with patch.dict(sys.modules, {"telethon": fake_telethon, "telethon.sync": fake_sync}):
            sender = TelethonMessageSender(
                api_id=123456,
                api_hash="hash",
                phone="+12018880000",
                session="fresh-session",
            )
            with self.assertRaises(ValueError) as ctx:
                sender.connect()
        self.assertIn("未授权", str(ctx.exception))

    def test_telethon_sender_creates_session_parent_directory(self):
        calls = []

        class FakeTelegramClient:
            def __init__(self, session, api_id, api_hash):
                calls.append(("init", session, api_id, api_hash))

            def connect(self):
                calls.append(("connect",))

            def is_user_authorized(self):
                return True

            def disconnect(self):
                calls.append(("disconnect",))

        fake_sync = types.ModuleType("telethon.sync")
        fake_sync.TelegramClient = FakeTelegramClient
        fake_telethon = types.ModuleType("telethon")

        with tempfile.TemporaryDirectory() as tmpdir:
            session_path = str(Path(tmpdir) / "nested" / "account_main")
            with patch.dict(sys.modules, {"telethon": fake_telethon, "telethon.sync": fake_sync}):
                sender = TelethonMessageSender(
                    api_id=123456,
                    api_hash="hash",
                    phone="",
                    session=session_path,
                )
                sender.connect()
                sender.disconnect()

            self.assertTrue((Path(tmpdir) / "nested").exists())
            self.assertIn(("connect",), calls)

    def test_telethon_sender_pool_reuses_sender_by_account_id(self):
        class FakeSingleSender:
            instances = []

            def __init__(self, *, api_id, api_hash, phone, session):
                self.api_id = api_id
                self.api_hash = api_hash
                self.phone = phone
                self.session = session
                self.connected = False
                FakeSingleSender.instances.append(self)

            def connect(self):
                self.connected = True

            def disconnect(self):
                self.connected = False

            def send_text(self, target_key, message_text):
                return {"message_id": "m001", "target_key": target_key, "text": message_text}

        job = ExecutorJob.from_payload(
            {
                "job_id": "job_001",
                "signal_id": "sig_001",
                "lottery_type": "pc28",
                "issue_no": "20260407001",
                "bet_type": "big_small",
                "bet_value": "大",
                "message_text": "大10",
                "stake_plan": {"mode": "flat", "amount": 10},
                "target": {"type": "telegram_group", "key": "-1001234567890"},
                "telegram_account": {
                    "id": 7,
                    "label": "账号A",
                    "phone": "+12010000000",
                    "session_path": "/data/a/main",
                },
                "idempotency_key": "idemp-001",
                "execute_after": "2026-04-07T15:00:00Z",
                "expire_at": "2099-04-07T15:01:00Z",
            }
        )

        with patch("pc28touzhu.executor.telethon_sender.TelethonMessageSender", FakeSingleSender):
            pool = TelethonSenderPool(api_id=1, api_hash="hash", default_phone="", default_session="default")
            pool.send_text(job)
            pool.send_text(job)
            pool.disconnect()

        self.assertEqual(len(FakeSingleSender.instances), 1)
        self.assertEqual(FakeSingleSender.instances[0].session, "/data/a/main")

    def test_telethon_sender_resolves_channel_id_via_dialogs_cache(self):
        class FakeDialog:
            def __init__(self, dialog_id, entity):
                self.id = dialog_id
                self.entity = entity

        class FakeEntity:
            def __init__(self, entity_id):
                self.id = entity_id

        class FakeMessage:
            id = 123
            chat_id = -1005041409209

        class FakeTelegramClient:
            def __init__(self, session, api_id, api_hash):
                self.entity = FakeEntity(5041409209)

            def connect(self):
                return None

            def is_user_authorized(self):
                return True

            def disconnect(self):
                return None

            def get_input_entity(self, candidate):
                if candidate == self.entity:
                    return self.entity
                raise ValueError("not cached")

            def get_dialogs(self):
                return [FakeDialog(-1005041409209, self.entity)]

            def send_message(self, entity, message_text):
                if entity is not self.entity:
                    raise AssertionError("解析到的实体不正确")
                return FakeMessage()

        fake_sync = types.ModuleType("telethon.sync")
        fake_sync.TelegramClient = FakeTelegramClient
        fake_telethon = types.ModuleType("telethon")

        with patch.dict(sys.modules, {"telethon": fake_telethon, "telethon.sync": fake_sync}):
            sender = TelethonMessageSender(
                api_id=123456,
                api_hash="hash",
                phone="",
                session="cached-session",
            )
            sender.connect()
            result = sender.send_text("-1005041409209", "小10")
            sender.disconnect()

        self.assertEqual(result["message_id"], 123)
        self.assertEqual(result["target_key"], "-1005041409209")

    def test_telethon_sender_raises_clear_error_when_entity_missing(self):
        class FakeTelegramClient:
            def __init__(self, session, api_id, api_hash):
                pass

            def connect(self):
                return None

            def is_user_authorized(self):
                return True

            def disconnect(self):
                return None

            def get_input_entity(self, candidate):
                raise ValueError("not cached")

            def get_dialogs(self):
                return []

        fake_sync = types.ModuleType("telethon.sync")
        fake_sync.TelegramClient = FakeTelegramClient
        fake_telethon = types.ModuleType("telethon")

        with patch.dict(sys.modules, {"telethon": fake_telethon, "telethon.sync": fake_sync}):
            sender = TelethonMessageSender(
                api_id=123456,
                api_hash="hash",
                phone="",
                session="missing-session",
            )
            sender.connect()
            with self.assertRaises(ValueError) as ctx:
                sender.send_text("-1005041409209", "小10")
            sender.disconnect()

        self.assertIn("无法解析目标群组实体", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
