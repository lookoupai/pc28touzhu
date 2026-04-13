import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.executor import (
    ExecutorJob,
    ExecutorResult,
    ExecutorStateStore,
    should_send_job,
)


class ExecutorJobTests(unittest.TestCase):
    def test_from_payload_parses_fields(self):
        payload = {
            "job_id": "job_001",
            "signal_id": "sig_001",
            "lottery_type": "pc28",
            "issue_no": "20260407001",
            "bet_type": "big_small",
            "bet_value": "大",
            "message_text": "大10",
            "stake_plan": {
                "mode": "flat",
                "amount": 10,
                "base_stake": 10,
                "multiplier": 2,
                "max_steps": 6,
                "refund_action": "hold",
                "cap_action": "reset",
            },
            "target": {"type": "telegram_group", "key": "-1001234567890"},
            "idempotency_key": "exec-user1-20260407001-big",
            "execute_after": "2026-04-07T15:00:00Z",
            "expire_at": "2026-04-07T15:01:00Z",
        }
        job = ExecutorJob.from_payload(payload)
        self.assertEqual(job.job_id, "job_001")
        self.assertEqual(job.stake_plan.amount, 10)
        self.assertEqual(job.stake_plan.base_stake, 10)
        self.assertEqual(job.stake_plan.multiplier, 2)
        self.assertEqual(job.stake_plan.max_steps, 6)
        self.assertEqual(job.target.key, "-1001234567890")
        self.assertEqual(job.execute_after.tzinfo, timezone.utc)


class StateStoreTests(unittest.TestCase):
    def test_idempotency_prevents_redelivery(self):
        store = ExecutorStateStore()
        key = "dup-key"
        self.assertFalse(store.has_delivered(key))
        store.record_attempt(
            idempotency_key=key,
            delivery_status="delivered",
            executor_id="exec",
            attempt_no=1,
            remote_message_id="123",
            error_message=None,
        )
        self.assertTrue(store.has_delivered(key))
        self.assertEqual(store.next_attempt_no(key), 2)

    def test_records_attempt_metadata(self):
        store = ExecutorStateStore()
        key = "retry-key"
        now = datetime.now(timezone.utc)
        store.record_attempt(
            idempotency_key=key,
            delivery_status="failed",
            executor_id="node-1",
            attempt_no=1,
            remote_message_id=None,
            error_message="network",
            executed_at=now,
        )
        record = store.get_record(key)
        self.assertIsNotNone(record)
        self.assertEqual(record["delivery_status"], "failed")
        self.assertEqual(record["executor_id"], "node-1")


class ShouldSendJobTests(unittest.TestCase):
    def test_due_job_can_send(self):
        now = datetime(2026, 4, 7, 15, 0, tzinfo=timezone.utc)
        job = ExecutorJob(
            job_id="job",
            signal_id="sig",
            lottery_type="pc28",
            issue_no="issue",
            bet_type="big_small",
            bet_value="大",
            message_text="大10",
            stake_plan=None,  # type: ignore[arg-type]
            target=None,  # type: ignore[arg-type]
            telegram_account=None,
            idempotency_key="key",
            execute_after=now - timedelta(seconds=30),
            expire_at=now + timedelta(minutes=1),
        )
        self.assertTrue(should_send_job(job, reference_time=now))

    def test_early_job_not_send(self):
        now = datetime(2026, 4, 7, 14, tzinfo=timezone.utc)
        job = ExecutorJob(
            job_id="job",
            signal_id="sig",
            lottery_type="pc28",
            issue_no="issue",
            bet_type="big_small",
            bet_value="大",
            message_text="大10",
            stake_plan=None,  # type: ignore[arg-type]
            target=None,  # type: ignore[arg-type]
            telegram_account=None,
            idempotency_key="key",
            execute_after=now + timedelta(minutes=5),
            expire_at=now + timedelta(minutes=10),
        )
        self.assertFalse(should_send_job(job, reference_time=now))

    def test_expired_job_not_send(self):
        now = datetime(2026, 4, 7, 16, tzinfo=timezone.utc)
        job = ExecutorJob(
            job_id="job",
            signal_id="sig",
            lottery_type="pc28",
            issue_no="issue",
            bet_type="big_small",
            bet_value="大",
            message_text="大10",
            stake_plan=None,  # type: ignore[arg-type]
            target=None,  # type: ignore[arg-type]
            telegram_account=None,
            idempotency_key="key",
            execute_after=now - timedelta(minutes=5),
            expire_at=now,
        )
        self.assertFalse(should_send_job(job, reference_time=now))


class ExecutorResultTests(unittest.TestCase):
    def test_to_payload_includes_optional_fields(self):
        result = ExecutorResult(
            job_id="job",
            executor_id="exec-1",
            attempt_no=2,
            delivery_status="delivered",
            executed_at=datetime(2026, 4, 7, 15, tzinfo=timezone.utc),
            remote_message_id="msg-123",
            raw_result={"chat_id": "-100"},
            error_message=None,
        )
        payload = result.to_payload()
        self.assertEqual(payload["executor_id"], "exec-1")
        self.assertIn("remote_message_id", payload)
        self.assertIn("raw_result", payload)


if __name__ == "__main__":
    unittest.main()
