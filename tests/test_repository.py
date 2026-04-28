from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal


def utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _parse_dt(value: str) -> datetime:
    text = str(value).replace("Z", "+00:00")
    return datetime.fromisoformat(text)


class DatabaseRepositoryTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_initialize_database_creates_tables(self):
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
            names = {row[0] for row in rows}
        self.assertIn("execution_jobs", names)
        self.assertIn("executor_instances", names)
        self.assertIn("platform_alert_records", names)
        self.assertIn("normalized_signals", names)
        self.assertIn("user_subscriptions", names)
        self.assertIn("telegram_accounts", names)
        self.assertIn("message_templates", names)
        self.assertIn("subscription_financial_state", names)

    def test_pull_ready_jobs_shapes_payload(self):
        user_id = self.repo.create_user("u1")
        source_id = self.repo.create_source("internal_ai", "src", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407001",
            bet_type="big_small",
            bet_value="大",
        )
        target_id = self.repo.create_delivery_target_record(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100123",
            target_name="测试群",
            status="active",
        )["id"]
        now = datetime.now(timezone.utc).replace(microsecond=0)
        execute_after = (now - timedelta(seconds=5)).isoformat().replace("+00:00", "Z")
        expire_at = (now + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="idemp-001",
            planned_message_text="大10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=execute_after,
            expire_at=expire_at,
        )

        items = self.repo.pull_ready_jobs(executor_id="exec-1", limit=10)
        self.assertEqual(len(items), 1)
        item = items[0]
        self.assertEqual(item["job_id"], str(job_id))
        self.assertEqual(item["signal_id"], str(signal_id))
        self.assertEqual(item["bet_value"], "大")
        self.assertEqual(item["stake_plan"]["amount"], 10)
        self.assertEqual(item["stake_plan"].get("base_stake"), None if "base_stake" not in item["stake_plan"] else item["stake_plan"]["base_stake"])
        self.assertEqual(item["target"]["key"], "-100123")
        self.assertEqual(item["execute_after"], execute_after)

    def test_pull_ready_jobs_excludes_inactive_account(self):
        user_id = self.repo.create_user("u1-inactive-account")
        source_id = self.repo.create_source("internal_ai", "src-inactive-account", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407001",
            bet_type="big_small",
            bet_value="大",
        )
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="暂停账号",
            phone="+12018881111",
            session_path="/data/inactive-account/main",
            status="inactive",
        )["id"]
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-1001234",
            target_name="测试群",
        )
        now = datetime.now(timezone.utc).replace(microsecond=0)
        self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            idempotency_key="idemp-inactive-account",
            planned_message_text="大10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=(now - timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            expire_at=(now + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        )

        items = self.repo.pull_ready_jobs(executor_id="exec-1", limit=10)
        self.assertEqual(items, [])

    def test_pull_ready_jobs_excludes_expired_or_non_pending(self):
        user_id = self.repo.create_user("u2")
        source_id = self.repo.create_source("internal_ai", "src2", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407002",
            bet_type="big_small",
            bet_value="小",
        )
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100456",
        )
        now = datetime.now(timezone.utc).replace(microsecond=0)
        # expired
        self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="idemp-expired",
            planned_message_text="小10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=(now - timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
            expire_at=(now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        )
        # delivered (non-pending)
        self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="idemp-delivered",
            planned_message_text="小10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=(now - timedelta(seconds=5)).isoformat().replace("+00:00", "Z"),
            expire_at=(now + timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            status="delivered",
        )

        items = self.repo.pull_ready_jobs(executor_id="exec-1", limit=10)
        self.assertEqual(items, [])

    def test_pull_ready_jobs_marks_expired_pending_jobs(self):
        user_id = self.repo.create_user("u2-expired")
        source_id = self.repo.create_source("internal_ai", "src-expired", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407099",
            bet_type="big_small",
            bet_value="小",
        )
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100457",
        )
        now = datetime.now(timezone.utc).replace(microsecond=0)
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="idemp-expired-state",
            planned_message_text="小10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=(now - timedelta(minutes=3)).isoformat().replace("+00:00", "Z"),
            expire_at=(now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        )

        self.assertEqual(self.repo.pull_ready_jobs(executor_id="exec-1", limit=10), [])
        job = self.repo.get_execution_job(job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job["status"], "expired")
        self.assertEqual(job["error_message"], "任务已过期")

    def test_report_job_result_updates_job_and_inserts_attempt(self):
        user_id = self.repo.create_user("u3")
        source_id = self.repo.create_source("internal_ai", "src3", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407003",
            bet_type="big_small",
            bet_value="大",
        )
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100789",
        )
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        now = now_dt.isoformat().replace("+00:00", "Z")
        expire_at = (now_dt + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="idemp-report",
            planned_message_text="大10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=now,
            expire_at=expire_at,
        )

        payload = self.repo.report_job_result(
            job_id=str(job_id),
            executor_id="exec-1",
            attempt_no=1,
            delivery_status="delivered",
            remote_message_id="m123",
            executed_at=now,
            raw_result={"chat_id": "-100789"},
            error_message=None,
        )
        self.assertEqual(payload["job_id"], str(job_id))
        self.assertEqual(payload["delivery_status"], "delivered")

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            job_row = conn.execute(
                "SELECT status, error_message FROM execution_jobs WHERE id = ?",
                (job_id,),
            ).fetchone()
            self.assertIsNotNone(job_row)
            self.assertEqual(job_row["status"], "delivered")

            attempt_row = conn.execute(
                "SELECT * FROM execution_attempts WHERE job_id = ?",
                (job_id,),
            ).fetchone()
            self.assertIsNotNone(attempt_row)
            self.assertEqual(attempt_row["attempt_no"], 1)
            self.assertEqual(attempt_row["remote_message_id"], "m123")
            self.assertEqual(json.loads(attempt_row["raw_result"])["chat_id"], "-100789")

    def test_list_execution_jobs_includes_last_attempt_summary(self):
        user_id = self.repo.create_user("job-list-user")
        source_id = self.repo.create_source("internal_ai", "src-job-list", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407123",
            bet_type="big_small",
            bet_value="大",
        )
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="任务账号",
            phone="+12015550000",
            session_path="/data/job-list/main",
        )["id"]
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-100555",
            target_name="任务群",
        )
        now = utc_now_iso()
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            idempotency_key="idemp-job-list",
            planned_message_text="大10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=now,
            expire_at=utc_now_iso(),
            status="failed",
        )
        self.repo.report_job_result(
            job_id=str(job_id),
            executor_id="exec-job-list",
            attempt_no=1,
            delivery_status="failed",
            remote_message_id=None,
            executed_at=now,
            raw_result={"exception_type": "RuntimeError"},
            error_message="network down",
        )

        items = self.repo.list_execution_jobs(user_id=user_id, status="failed")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["id"], job_id)
        self.assertEqual(items[0]["attempt_count"], 1)
        self.assertEqual(items[0]["last_attempt_no"], 1)
        self.assertEqual(items[0]["last_error_message"], "network down")
        self.assertEqual(items[0]["target_key"], "-100555")
        self.assertEqual(items[0]["telegram_account_label"], "任务账号")

    def test_retry_execution_job_resets_failed_job_to_pending(self):
        user_id = self.repo.create_user("job-retry-user")
        source_id = self.repo.create_source("internal_ai", "src-job-retry", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407124",
            bet_type="big_small",
            bet_value="大",
        )
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100556",
        )
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        execute_after = (now_dt - timedelta(minutes=2)).isoformat().replace("+00:00", "Z")
        expire_at = (now_dt + timedelta(minutes=1)).isoformat().replace("+00:00", "Z")
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="idemp-job-retry",
            planned_message_text="大10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=execute_after,
            expire_at=expire_at,
            status="failed",
        )
        self.repo.report_job_result(
            job_id=str(job_id),
            executor_id="exec-retry",
            attempt_no=1,
            delivery_status="failed",
            remote_message_id=None,
            executed_at=utc_now_iso(),
            raw_result={"exception_type": "RuntimeError"},
            error_message="telethon timeout",
        )

        retried = self.repo.retry_execution_job(job_id=job_id, user_id=user_id)
        self.assertEqual(retried["status"], "pending")
        self.assertIsNone(retried["error_message"])
        self.assertGreaterEqual(_parse_dt(retried["expire_at"]), _parse_dt(retried["execute_after"]))
        self.assertGreaterEqual(_parse_dt(retried["execute_after"]), now_dt)

    def test_requeue_auto_retry_jobs_reschedules_failed_job_after_backoff(self):
        user_id = self.repo.create_user("auto-retry-user")
        source_id = self.repo.create_source("internal_ai", "auto-retry-src", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407125",
            bet_type="big_small",
            bet_value="大",
        )
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100557",
        )
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="idemp-auto-retry",
            planned_message_text="大10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after=(now_dt - timedelta(minutes=3)).isoformat().replace("+00:00", "Z"),
            expire_at=(now_dt - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
            status="failed",
        )
        self.repo.report_job_result(
            job_id=str(job_id),
            executor_id="exec-auto-retry",
            attempt_no=1,
            delivery_status="failed",
            remote_message_id=None,
            executed_at=(now_dt - timedelta(seconds=90)).isoformat().replace("+00:00", "Z"),
            raw_result={"exception_type": "RuntimeError"},
            error_message="network glitch",
        )

        requeued = self.repo.requeue_auto_retry_jobs(max_attempts=3, base_delay_seconds=30, limit=10)
        self.assertEqual(len(requeued), 1)
        job = self.repo.get_execution_job(job_id)
        self.assertIsNotNone(job)
        self.assertEqual(job["status"], "pending")
        self.assertIsNone(job["error_message"])

    def test_upsert_executor_heartbeat_is_idempotent(self):
        now = utc_now_iso()
        payload1 = self.repo.upsert_executor_heartbeat(
            executor_id="exec-1",
            version="0.1.0",
            capabilities={"send": True},
            status="online",
            last_seen_at=now,
        )
        self.assertEqual(payload1["executor_id"], "exec-1")

        payload2 = self.repo.upsert_executor_heartbeat(
            executor_id="exec-1",
            version="0.2.0",
            capabilities={"send": True, "pull": True},
            status="online",
            last_seen_at=now,
        )
        self.assertEqual(payload2["version"], "0.2.0")

        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                "SELECT version, capabilities FROM executor_instances WHERE executor_id = ?",
                ("exec-1",),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row[0], "0.2.0")
            self.assertIn("pull", json.loads(row[1]))

    def test_list_executor_instances_includes_attempt_aggregates(self):
        user_id = self.repo.create_user("exec-summary-user")
        source_id = self.repo.create_source("internal_ai", "exec-summary-src", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407188",
            bet_type="big_small",
            bet_value="大",
        )
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100888",
        )
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="exec-summary-job",
            planned_message_text="大18",
            stake_plan={"mode": "flat", "amount": 18},
            execute_after=utc_now_iso(),
            expire_at=utc_now_iso(),
        )
        self.repo.upsert_executor_heartbeat(
            executor_id="exec-summary",
            version="0.3.0",
            capabilities={"send": True, "provider": "telethon"},
            status="online",
            last_seen_at=utc_now_iso(),
        )
        self.repo.report_job_result(
            job_id=str(job_id),
            executor_id="exec-summary",
            attempt_no=1,
            delivery_status="failed",
            remote_message_id=None,
            executed_at=utc_now_iso(),
            raw_result={"exception_type": "RuntimeError"},
            error_message="flood wait",
        )

        items = self.repo.list_executor_instances(limit=10)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["executor_id"], "exec-summary")
        self.assertEqual(items[0]["failed_attempt_count"], 1)
        self.assertEqual(items[0]["delivered_attempt_count"], 0)
        self.assertEqual(items[0]["last_failure_error_message"], "flood wait")
        self.assertTrue(items[0]["capabilities"]["send"])

    def test_list_recent_execution_failures_returns_unresolved_failed_jobs(self):
        user_id = self.repo.create_user("failure-list-user")
        source_id = self.repo.create_source("internal_ai", "failure-list-src", owner_user_id=user_id)
        signal_id = self.repo.create_signal(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407189",
            bet_type="big_small",
            bet_value="小",
        )
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="失败账号",
            phone="+12016660000",
            session_path="/data/failure-list/main",
        )["id"]
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-100889",
            target_name="失败群",
        )
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            idempotency_key="failure-list-job",
            planned_message_text="小18",
            stake_plan={"mode": "flat", "amount": 18},
            execute_after=utc_now_iso(),
            expire_at=utc_now_iso(),
            status="failed",
        )
        self.repo.report_job_result(
            job_id=str(job_id),
            executor_id="exec-failure-list",
            attempt_no=1,
            delivery_status="failed",
            remote_message_id=None,
            executed_at=utc_now_iso(),
            raw_result={"exception_type": "RuntimeError"},
            error_message="session revoked",
        )

        items = self.repo.list_recent_execution_failures(user_id=user_id, limit=10)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["job_id"], job_id)
        self.assertEqual(items[0]["delivery_status"], "failed")
        self.assertEqual(items[0]["executor_instance_id"], "exec-failure-list")
        self.assertEqual(items[0]["target_key"], "-100889")
        self.assertEqual(items[0]["telegram_account_label"], "失败账号")

    def test_sync_platform_alert_records_tracks_active_and_resolved_alerts(self):
        active_items = self.repo.sync_platform_alert_records(
            [
                {
                    "alert_key": "executor_offline:test",
                    "alert_type": "executor_offline",
                    "severity": "critical",
                    "title": "执行器离线",
                    "message": "executor-1 offline",
                    "metadata": {"executor_id": "executor-1"},
                }
            ],
            repeat_interval_seconds=900,
        )
        self.assertEqual(len(active_items), 1)
        record = self.repo.get_platform_alert_record("executor_offline:test")
        self.assertIsNotNone(record)
        self.assertEqual(record["status"], "active")

        resolved_items = self.repo.sync_platform_alert_records([], repeat_interval_seconds=900)
        self.assertEqual(len(resolved_items), 1)
        self.assertEqual(resolved_items[0]["notification_event"], "resolved")
        resolved_record = self.repo.get_platform_alert_record("executor_offline:test")
        self.assertIsNotNone(resolved_record)
        self.assertEqual(resolved_record["status"], "resolved")

    def test_mark_platform_alert_sent_updates_delivery_state(self):
        self.repo.sync_platform_alert_records(
            [
                {
                    "alert_key": "job_retry_exhausted:test",
                    "alert_type": "job_retry_exhausted",
                    "severity": "warning",
                    "title": "任务重试耗尽",
                    "message": "job #1 exhausted",
                    "metadata": {"job_id": 1},
                }
            ],
            repeat_interval_seconds=900,
        )
        sent_record = self.repo.mark_platform_alert_sent(alert_key="job_retry_exhausted:test")
        self.assertIsNotNone(sent_record)
        self.assertEqual(sent_record["send_count"], 1)
        self.assertIsNotNone(sent_record["last_sent_at"])
        failed_record = self.repo.mark_platform_alert_sent(alert_key="job_retry_exhausted:test", error="send failed")
        self.assertEqual(failed_record["last_error"], "send failed")

    def test_create_and_list_sources(self):
        owner_user_id = self.repo.create_user("source-owner")
        self.repo.create_source_record(
            owner_user_id=owner_user_id,
            source_type="telegram_channel",
            name="channel-a",
            visibility="public",
            config={"chat_id": "@demo"},
        )
        items = self.repo.list_sources(owner_user_id=owner_user_id)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["source_type"], "telegram_channel")
        self.assertEqual(items[0]["config"]["chat_id"], "@demo")

    def test_create_and_list_subscriptions(self):
        user_id = self.repo.create_user("sub-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-1",
        )["id"]
        created = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"mode": "follow"},
        )
        items = self.repo.list_subscriptions(user_id=user_id)
        self.assertEqual(created["source_id"], source_id)
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["strategy"]["mode"], "follow")

    def test_update_subscription_status(self):
        user_id = self.repo.create_user("sub-status-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-status",
        )["id"]
        created = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"mode": "follow"},
            status="active",
        )
        updated = self.repo.update_subscription_status(
            subscription_id=created["id"],
            user_id=user_id,
            status="inactive",
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "inactive")
        standby = self.repo.update_subscription_status(
            subscription_id=created["id"],
            user_id=user_id,
            status="standby",
        )
        self.assertIsNotNone(standby)
        self.assertEqual(standby["status"], "standby")

    def test_update_subscription_record(self):
        user_id = self.repo.create_user("sub-edit-user")
        source_a = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-a",
        )["id"]
        source_b = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-b",
        )["id"]
        created = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_a,
            strategy={"mode": "follow", "stake_amount": 10},
        )
        updated = self.repo.update_subscription_record(
            subscription_id=created["id"],
            user_id=user_id,
            source_id=source_b,
            strategy={"mode": "follow", "stake_amount": 20},
            status="active",
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated["source_id"], source_b)
        self.assertEqual(updated["strategy"]["stake_amount"], 20)

    def test_progression_event_settlement_updates_subscription_state(self):
        user_id = self.repo.create_user("sub-progression-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-progression",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"mode": "martingale", "base_stake": 10, "multiplier": 2, "max_steps": 3},
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407009",
            bet_type="big_small",
            bet_value="大",
        )
        event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=signal["id"],
            issue_no="20260407009",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=2,
            max_steps=3,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        settled = self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="miss",
            progression_event_id=event["id"],
        )
        self.assertEqual(settled["event"]["status"], "settled")
        self.assertEqual(settled["event"]["resolved_result_type"], "miss")
        self.assertEqual(settled["state"]["current_step"], 2)
        self.assertEqual(settled["financial"]["realized_loss"], 10)
        self.assertEqual(settled["financial"]["net_profit"], -10)

    def test_progression_event_settlement_updates_daily_stats(self):
        user_id = self.repo.create_user("sub-daily-stat-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-daily",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "mode": "follow",
                "stake_amount": 10,
                "risk_control": {"enabled": True, "win_profit_ratio": 1.5},
            },
        )
        hit_signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407011",
            bet_type="edge",
            bet_value="边",
        )
        miss_signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407012",
            bet_type="edge",
            bet_value="中",
        )
        hit_event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=hit_signal["id"],
            issue_no="20260407011",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=2,
            max_steps=3,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        miss_event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=miss_signal["id"],
            issue_no="20260407012",
            progression_step=1,
            stake_amount=8,
            base_stake=8,
            multiplier=2,
            max_steps=3,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )

        self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="hit",
            progression_event_id=hit_event["id"],
        )
        self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="miss",
            progression_event_id=miss_event["id"],
        )

        stat_date = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        stats = self.repo.list_user_daily_subscription_stats(user_id=user_id, stat_date=stat_date)
        self.assertEqual(len(stats), 1)
        self.assertEqual(stats[0]["source_name"], "model-daily")
        self.assertEqual(stats[0]["profit_amount"], 15)
        self.assertEqual(stats[0]["loss_amount"], 8)
        self.assertEqual(stats[0]["net_profit"], 7)
        self.assertEqual(stats[0]["settled_event_count"], 2)
        summary = self.repo.get_user_daily_profit_summary(user_id=user_id, stat_date=stat_date)
        self.assertEqual(summary["plan_count"], 1)
        self.assertEqual(summary["net_profit"], 7)
        history = self.repo.list_subscription_daily_stats(
            subscription_id=subscription["id"],
            user_id=user_id,
            limit=7,
        )
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["stat_date"], stat_date)
        self.assertEqual(history[0]["net_profit"], 7)

    def test_progression_event_settlement_uses_pc28_rule_for_daxiao(self):
        user_id = self.repo.create_user("sub-pc28-basic-profit-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-basic-profit",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "mode": "follow",
                "stake_amount": 10,
                "risk_control": {"enabled": True, "win_profit_ratio": 5.0},
            },
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407013",
            bet_type="big_small",
            bet_value="大",
            normalized_payload={"profit_rule_id": "pc28_netdisk", "odds_profile": "regular"},
        )
        event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=signal["id"],
            issue_no="20260407013",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )

        settled = self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )

        self.assertEqual(settled["financial"]["realized_profit"], 9.8)
        self.assertEqual(settled["financial"]["net_profit"], 9.8)

    def test_progression_event_settlement_uses_pc28_rule_for_combo(self):
        user_id = self.repo.create_user("sub-pc28-combo-profit-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-combo-profit",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "mode": "follow",
                "stake_amount": 10,
                "risk_control": {"enabled": True, "win_profit_ratio": 1.0},
            },
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407014",
            bet_type="combo",
            bet_value="大单",
            normalized_payload={"profit_rule_id": "pc28_high", "odds_profile": "regular"},
        )
        event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=signal["id"],
            issue_no="20260407014",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )

        settled = self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )

        self.assertEqual(settled["financial"]["realized_profit"], 53.3)
        self.assertEqual(settled["financial"]["net_profit"], 53.3)

    def test_progression_event_settlement_prefers_subscription_fixed_rule(self):
        user_id = self.repo.create_user("sub-fixed-settlement-rule-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-fixed-settlement-rule",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "play_filter": {"mode": "all", "selected_keys": []},
                "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                "settlement_policy": {
                    "rule_source": "subscription_fixed",
                    "settlement_rule_id": "pc28_high_regular",
                    "fallback_profit_ratio": 1.0,
                },
                "risk_control": {"enabled": False, "profit_target": 0, "loss_limit": 0},
                "dispatch": {"expire_after_seconds": 120},
            },
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407015",
            bet_type="big_small",
            bet_value="大",
            normalized_payload={"profit_rule_id": "pc28_netdisk", "odds_profile": "regular"},
        )
        event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=signal["id"],
            issue_no="20260407015",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )

        settled = self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )

        self.assertEqual(settled["financial"]["realized_profit"], 18.46)
        self.assertEqual(settled["financial"]["net_profit"], 18.46)
        self.assertEqual(settled["event"]["settlement_rule_id"], "pc28_high_regular")
        self.assertEqual(settled["event"]["profit_delta"], 18.46)
        self.assertEqual(settled["event"]["loss_delta"], 0)
        self.assertEqual(settled["event"]["net_delta"], 18.46)
        self.assertEqual(settled["event"]["settlement_snapshot"]["settlement_rule_id"], "pc28_high_regular")
        self.assertEqual(settled["event"]["settlement_snapshot"]["rule_source"], "subscription_fixed")
        self.assertEqual(settled["event"]["result_context"]["result_type"], "hit")

    def test_subscription_serialization_projects_strategy_v1_and_v2(self):
        user_id = self.repo.create_user("sub-strategy-v2-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-strategy-v2",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "play_filter": {"mode": "selected", "selected_keys": ["big_small:大"]},
                "staking_policy": {"mode": "fixed", "fixed_amount": 12},
                "settlement_policy": {
                    "rule_source": "subscription_fixed",
                    "settlement_rule_id": "pc28_high_regular",
                    "fallback_profit_ratio": 1.2,
                },
                "risk_control": {"enabled": True, "profit_target": 66, "loss_limit": 33},
                "dispatch": {"expire_after_seconds": 90},
            },
        )

        self.assertEqual(subscription["strategy_schema_version"], 2)
        self.assertEqual(subscription["strategy"]["bet_filter"]["selected_keys"], ["big_small:大"])
        self.assertEqual(subscription["strategy"]["stake_amount"], 12)
        self.assertEqual(subscription["strategy"]["risk_control"]["win_profit_ratio"], 1.2)
        self.assertEqual(subscription["strategy_v2"]["staking_policy"]["mode"], "fixed")
        self.assertEqual(subscription["strategy_v2"]["dispatch"]["expire_after_seconds"], 90)

    def test_user_telegram_binding_token_flow(self):
        user = self.repo.create_user_record(username="binding-user", email="", role="user", status="active")
        token_state = self.repo.set_user_telegram_bind_token(
            user_id=user["id"],
            bind_token="ABC123TOKEN",
            expire_at="2026-04-17T13:00:00Z",
        )
        self.assertTrue(token_state["has_active_bind_token"])
        self.assertEqual(token_state["bind_token"], "ABC123TOKEN")
        self.assertEqual(self.repo.get_user_by_telegram_bind_token("ABC123TOKEN")["id"], user["id"])

        bound = self.repo.update_user_telegram_binding(
            user_id=user["id"],
            telegram_user_id=778899,
            telegram_chat_id="778899",
            telegram_username="tg_binding_user",
            telegram_bound_at="2026-04-17T12:30:00Z",
        )
        self.assertTrue(bound["is_bound"])
        self.assertEqual(bound["telegram_user_id"], 778899)
        self.assertEqual(bound["telegram_chat_id"], "778899")
        self.assertEqual(self.repo.get_user_by_telegram_user_id(778899)["id"], user["id"])
        self.assertEqual(bound["bind_token"], "")

        cleared = self.repo.clear_user_telegram_binding(user_id=user["id"])
        self.assertFalse(cleared["is_bound"])
        self.assertEqual(cleared["telegram_chat_id"], "")

    def test_mark_telegram_daily_report_sent_and_runtime_state(self):
        first = self.repo.mark_telegram_daily_report_sent(
            report_key="daily:2026-04-16:-1001",
            stat_date="2026-04-16",
            target_chat_id="-1001",
            report_type="daily_profit_loss",
            sent_at="2026-04-17T01:00:00Z",
        )
        second = self.repo.mark_telegram_daily_report_sent(
            report_key="daily:2026-04-16:-1001",
            stat_date="2026-04-16",
            target_chat_id="-1001",
            report_type="daily_profit_loss",
            sent_at="2026-04-17T01:01:00Z",
        )
        self.assertEqual(first["send_count"], 1)
        self.assertEqual(second["send_count"], 2)
        state = self.repo.update_telegram_bot_runtime_state(bot_name="profit-query-bot", last_update_id=123)
        self.assertEqual(state["last_update_id"], 123)

    def test_platform_runtime_setting_round_trip(self):
        stored = self.repo.upsert_platform_runtime_setting(
            setting_key="telegram_runtime_settings",
            value={"bot": {"enabled": True, "poll_interval_seconds": 8}},
        )
        self.assertEqual(stored["setting_key"], "telegram_runtime_settings")
        self.assertEqual(stored["value"]["bot"]["poll_interval_seconds"], 8)
        loaded = self.repo.get_platform_runtime_setting("telegram_runtime_settings")
        self.assertEqual(loaded["value"]["bot"]["enabled"], True)

    def test_progression_event_settlement_can_auto_stop_subscription(self):
        user_id = self.repo.create_user("sub-risk-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-risk",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "mode": "follow",
                "stake_amount": 10,
                "risk_control": {
                    "enabled": True,
                    "loss_limit": 10,
                    "profit_target": 0,
                    "win_profit_ratio": 1.0,
                },
            },
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407010",
            bet_type="big_small",
            bet_value="大",
        )
        event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=signal["id"],
            issue_no="20260407010",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )

        settled = self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="miss",
            progression_event_id=event["id"],
        )

        self.assertEqual(settled["financial"]["threshold_status"], "loss_limit_hit")
        self.assertEqual(self.repo.get_subscription(subscription["id"])["status"], "active")
        runtime_history = self.repo.list_subscription_runtime_runs(
            subscription_id=subscription["id"],
            user_id=user_id,
            limit=5,
        )
        self.assertEqual(runtime_history[0]["status"], "closed")
        self.assertEqual(runtime_history[0]["end_reason"], "loss_limit_hit")
        self.assertEqual(runtime_history[0]["ended_at"], settled["financial"]["last_settled_at"])

    def test_dispatch_candidates_skip_risk_blocked_subscription(self):
        user_id = self.repo.create_user("dispatch-risk-blocked-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="risk-blocked-source",
        )["id"]
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="执行号",
            phone="+12017770111",
            session_path="/data/risk-blocked-user/main",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "mode": "follow",
                "stake_amount": 10,
                "risk_control": {
                    "enabled": True,
                    "loss_limit": 10,
                    "profit_target": 0,
                    "win_profit_ratio": 1.0,
                },
            },
        )
        self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-100778899",
            status="active",
        )
        settled_signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407013",
            bet_type="big_small",
            bet_value="大",
        )
        settled_event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=settled_signal["id"],
            issue_no="20260407013",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="miss",
            progression_event_id=settled_event["id"],
        )

        next_signal_id = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407014",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={},
        )["id"]

        self.assertEqual(self.repo.get_subscription(subscription["id"])["status"], "active")
        self.assertEqual(self.repo.list_dispatch_candidates(next_signal_id), [])

    def test_reset_subscription_runtime_clears_current_round_state(self):
        user_id = self.repo.create_user("sub-reset-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-reset",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "mode": "follow",
                "stake_amount": 10,
                "risk_control": {
                    "enabled": True,
                    "profit_target": 20,
                    "loss_limit": 10,
                    "win_profit_ratio": 1.0,
                },
            },
        )
        target_id = self.repo.create_delivery_target_record(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100998",
            status="active",
        )["id"]
        settled_signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407011",
            bet_type="big_small",
            bet_value="小",
        )
        settled_event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=settled_signal["id"],
            issue_no="20260407011",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="miss",
            progression_event_id=settled_event["id"],
        )

        pending_signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407012",
            bet_type="big_small",
            bet_value="大",
        )
        pending_event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=pending_signal["id"],
            issue_no="20260407012",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        pending_job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=pending_signal["id"],
            subscription_id=subscription["id"],
            progression_event_id=pending_event["id"],
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="reset-job-2",
            planned_message_text="大10",
            stake_plan={"mode": "follow", "amount": 10},
            execute_after="2026-04-07T15:02:00Z",
            expire_at="2026-04-07T15:03:00Z",
        )

        result = self.repo.reset_subscription_runtime(
            subscription_id=subscription["id"],
            user_id=user_id,
            note="手动重置",
        )

        self.assertEqual(result["financial"]["net_profit"], 0)
        self.assertEqual(result["progression"]["current_step"], 1)
        self.assertEqual(result["voided_event_ids"], [pending_event["id"]])
        self.assertEqual(self.repo.get_progression_event(pending_event["id"])["status"], "reset")
        self.assertEqual(self.repo.get_execution_job(pending_job_id)["status"], "skipped")
        runtime_history = self.repo.list_subscription_runtime_runs(
            subscription_id=subscription["id"],
            user_id=user_id,
            limit=5,
        )
        self.assertEqual(len(runtime_history), 2)
        self.assertTrue(all(item["status"] == "closed" for item in runtime_history))
        loss_limit_run = next(item for item in runtime_history if item["end_reason"] == "loss_limit_hit")
        self.assertEqual(loss_limit_run["status"], "closed")
        self.assertEqual(loss_limit_run["settled_event_count"], 1)
        self.assertEqual(loss_limit_run["net_profit"], -10)

    def test_list_subscription_runtime_runs_reconciles_threshold_closed_runs(self):
        user_id = self.repo.create_user("sub-runtime-reconcile-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-runtime-reconcile",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"mode": "follow", "stake_amount": 10},
        )
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_runtime_runs(
                    subscription_id, user_id, status, started_issue_no, started_at, start_reason,
                    ended_at, end_reason, last_issue_no, last_result_type,
                    realized_profit, realized_loss, net_profit,
                    settled_event_count, hit_count, miss_count, refund_count,
                    baseline_reset_at, baseline_reset_note, created_at, updated_at
                ) VALUES (?, ?, 'active', ?, ?, ?, NULL, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL, '', ?, ?)
                """,
                (
                    int(subscription["id"]),
                    int(user_id),
                    "20260428001",
                    "2026-04-28T06:00:00Z",
                    "auto_started",
                    "20260428010",
                    "hit",
                    20.67,
                    10.0,
                    10.67,
                    10,
                    4,
                    5,
                    1,
                    "2026-04-28T06:00:00Z",
                    "2026-04-28T06:30:00Z",
                ),
            )
            conn.execute(
                """
                INSERT INTO subscription_financial_state(
                    subscription_id, user_id, realized_profit, realized_loss, net_profit,
                    threshold_status, stopped_reason, baseline_reset_at, baseline_reset_note,
                    last_settled_event_id, last_settled_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, '', 10, ?, ?)
                """,
                (
                    int(subscription["id"]),
                    int(user_id),
                    20.67,
                    10.0,
                    10.67,
                    "profit_target_hit",
                    "达到止盈阈值，当前轮次已停止",
                    "2026-04-28T06:44:35Z",
                    "2026-04-28T06:44:35Z",
                ),
            )

        runtime_history = self.repo.list_subscription_runtime_runs(
            subscription_id=subscription["id"],
            user_id=user_id,
            limit=5,
        )

        self.assertEqual(len(runtime_history), 1)
        self.assertEqual(runtime_history[0]["status"], "closed")
        self.assertEqual(runtime_history[0]["end_reason"], "profit_target_hit")
        self.assertEqual(runtime_history[0]["ended_at"], "2026-04-28T06:44:35Z")

    def test_report_job_result_marks_progression_event_placed(self):
        user_id = self.repo.create_user("sub-progression-job-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-progression-job",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"mode": "martingale", "base_stake": 10, "multiplier": 2, "max_steps": 3},
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407010",
            bet_type="big_small",
            bet_value="小",
        )
        target_id = self.repo.create_delivery_target(
            user_id=user_id,
            executor_type="telegram_group",
            target_key="-100123456",
        )
        progression_event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            signal_id=signal["id"],
            issue_no="20260407010",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=2,
            max_steps=3,
            refund_action="hold",
            cap_action="reset",
            status="pending",
        )
        job_id = self.repo.create_execution_job(
            user_id=user_id,
            signal_id=signal["id"],
            subscription_id=subscription["id"],
            progression_event_id=progression_event["id"],
            delivery_target_id=target_id,
            executor_type="telegram_group",
            idempotency_key="progression-job-1",
            planned_message_text="小10",
            stake_plan={"mode": "martingale", "amount": 10},
            execute_after="2026-04-07T15:00:00Z",
            expire_at="2026-04-07T15:01:00Z",
        )
        self.repo.report_job_result(
            str(job_id),
            executor_id="exec-1",
            attempt_no=1,
            delivery_status="delivered",
            remote_message_id="m001",
            executed_at="2026-04-07T15:00:10Z",
            raw_result={},
            error_message=None,
        )
        refreshed = self.repo.get_progression_event(progression_event["id"])
        self.assertEqual(refreshed["status"], "placed")

    def test_delete_subscription_record(self):
        user_id = self.repo.create_user("sub-delete-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="model-delete",
        )["id"]
        created = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"mode": "follow"},
            status="archived",
        )
        deleted = self.repo.delete_subscription_record(
            subscription_id=created["id"],
            user_id=user_id,
        )
        self.assertTrue(deleted)
        self.assertIsNone(self.repo.get_subscription(created["id"]))

    def test_create_and_list_delivery_targets(self):
        user_id = self.repo.create_user("target-user")
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="主号",
            phone="+12019990000",
            session_path="/data/target-user/main",
        )["id"]
        created = self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-100111",
            target_name="投注群",
        )
        items = self.repo.list_delivery_targets(user_id=user_id)
        self.assertEqual(created["target_key"], "-100111")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["target_name"], "投注群")
        self.assertEqual(items[0]["telegram_account_id"], account_id)

    def test_update_delivery_target_status(self):
        user_id = self.repo.create_user("target-status-user")
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="状态号",
            phone="+12017770000",
            session_path="/data/target-status-user/main",
        )["id"]
        created = self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-100333",
            target_name="状态群",
            status="active",
        )
        updated = self.repo.update_delivery_target_status(
            delivery_target_id=created["id"],
            user_id=user_id,
            status="inactive",
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "inactive")

    def test_update_delivery_target_record(self):
        user_id = self.repo.create_user("target-edit-user")
        account_a = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="账号A",
            phone="+12017770010",
            session_path="/data/target-edit/a",
        )["id"]
        account_b = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="账号B",
            phone="+12017770011",
            session_path="/data/target-edit/b",
        )["id"]
        created = self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_a,
            executor_type="telegram_group",
            target_key="-100400",
            target_name="旧群",
        )
        updated = self.repo.update_delivery_target_record(
            delivery_target_id=created["id"],
            user_id=user_id,
            telegram_account_id=account_b,
            executor_type="telegram_group",
            target_key="-100401",
            target_name="新群",
            template_id=None,
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated["telegram_account_id"], account_b)
        self.assertEqual(updated["target_key"], "-100401")
        self.assertEqual(updated["target_name"], "新群")

    def test_create_and_update_message_template_record(self):
        user_id = self.repo.create_user("template-user")
        created = self.repo.create_message_template_record(
            user_id=user_id,
            name="PC28 模板",
            lottery_type="pc28",
            bet_type="*",
            template_text="{{bet_value}}{{amount}}",
            config={"bet_rules": {"number_sum": {"format": "{{bet_value}}/{{amount}}"}}},
        )
        self.assertEqual(created["name"], "PC28 模板")
        self.assertEqual(created["config"]["bet_rules"]["number_sum"]["format"], "{{bet_value}}/{{amount}}")

        updated = self.repo.update_message_template_record(
            template_id=created["id"],
            user_id=user_id,
            name="PC28 模板 v2",
            lottery_type="pc28",
            bet_type="*",
            template_text="{{bet_value}}/{{amount}}",
            config={"bet_rules": {"big_small": {"format": "{{bet_value}}{{amount}}"}}},
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated["name"], "PC28 模板 v2")
        self.assertEqual(updated["template_text"], "{{bet_value}}/{{amount}}")

    def test_delete_delivery_target_record(self):
        user_id = self.repo.create_user("target-delete-user")
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="账号删除",
            phone="+12017770012",
            session_path="/data/target-delete/account",
        )["id"]
        created = self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-100402",
            target_name="待删群",
            status="archived",
        )
        deleted = self.repo.delete_delivery_target_record(
            delivery_target_id=created["id"],
            user_id=user_id,
        )
        self.assertTrue(deleted)
        self.assertIsNone(self.repo.get_delivery_target(created["id"]))

    def test_create_and_list_telegram_accounts(self):
        user_id = self.repo.create_user("account-user")
        created = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="账号A",
            phone="+12018880000",
            session_path="/data/account-user/a",
            meta={"provider": "manual"},
        )
        items = self.repo.list_telegram_accounts(user_id=user_id)
        self.assertEqual(created["label"], "账号A")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["session_path"], "/data/account-user/a")

    def test_update_telegram_account_status(self):
        user_id = self.repo.create_user("account-status-user")
        created = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="账号状态",
            phone="+12018880001",
            session_path="/data/account-status/a",
        )
        updated = self.repo.update_telegram_account_status(
            telegram_account_id=created["id"],
            user_id=user_id,
            status="inactive",
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated["status"], "inactive")

    def test_update_telegram_account_record(self):
        user_id = self.repo.create_user("account-edit-user")
        created = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="账号旧",
            phone="+12018880002",
            session_path="/data/account-edit/old",
        )
        updated = self.repo.update_telegram_account_record(
            telegram_account_id=created["id"],
            user_id=user_id,
            label="账号新",
            phone="+12018880003",
            session_path="/data/account-edit/new",
            meta={"note": "edited"},
        )
        self.assertIsNotNone(updated)
        self.assertEqual(updated["label"], "账号新")
        self.assertEqual(updated["session_path"], "/data/account-edit/new")
        self.assertEqual(updated["meta"]["note"], "edited")

    def test_delete_telegram_account_record_and_reference_counts(self):
        user_id = self.repo.create_user("account-delete-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="account-delete-source",
        )["id"]
        signal_id = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260409001",
            bet_type="combo",
            bet_value="大单",
            normalized_payload={"message_text": "大单20"},
        )["id"]
        referenced_account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="账号删",
            phone="+12018880004",
            session_path="/data/account-delete/main",
            status="archived",
        )["id"]
        target_id = self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=referenced_account_id,
            executor_type="telegram_group",
            target_key="-100403",
            target_name="关联群",
            status="archived",
        )["id"]
        self.repo.create_execution_job_record(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            telegram_account_id=referenced_account_id,
            executor_type="telegram_group",
            idempotency_key="account-delete-job",
            planned_message_text="大单20",
            stake_plan={"mode": "follow", "amount": 20},
            execute_after="2026-04-09T00:00:00Z",
            expire_at="2026-04-09T00:02:00Z",
        )
        self.assertEqual(self.repo.count_delivery_targets_by_telegram_account(referenced_account_id, user_id=user_id), 1)
        self.assertEqual(self.repo.count_execution_jobs_by_telegram_account(referenced_account_id, user_id=user_id), 1)

        deletable_account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="可删除账号",
            phone="+12018880005",
            session_path="/data/account-delete/free",
            status="archived",
        )["id"]
        deleted = self.repo.delete_telegram_account_record(
            telegram_account_id=deletable_account_id,
            user_id=user_id,
        )
        self.assertTrue(deleted)
        self.assertIsNone(self.repo.get_telegram_account(deletable_account_id))

    def test_create_and_list_signals(self):
        user_id = self.repo.create_user("signal-owner")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="strategy-x",
        )["id"]
        created = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407009",
            bet_type="combo",
            bet_value="大单",
            normalized_payload={"message_text": "大单20"},
        )
        items = self.repo.list_signals(source_id=source_id)
        self.assertEqual(created["bet_type"], "combo")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["normalized_payload"]["message_text"], "大单20")

    def test_create_and_list_raw_items(self):
        user_id = self.repo.create_user("raw-owner")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="telegram_channel",
            name="raw-source",
        )["id"]
        created = self.repo.create_raw_item_record(
            source_id=source_id,
            external_item_id="ext-001",
            issue_no="20260407011",
            raw_payload={"signals": [{"bet_type": "big_small", "bet_value": "大"}]},
        )
        items = self.repo.list_raw_items(source_id=source_id)
        self.assertEqual(created["external_item_id"], "ext-001")
        self.assertEqual(len(items), 1)
        self.assertEqual(items[0]["parse_status"], "pending")

    def test_update_raw_item_parse_result(self):
        user_id = self.repo.create_user("raw-update-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="website_feed",
            name="web-a",
        )["id"]
        raw_item_id = self.repo.create_raw_item_record(
            source_id=source_id,
            issue_no="20260407012",
            raw_payload={"foo": "bar"},
        )["id"]
        updated = self.repo.update_raw_item_parse_result(
            raw_item_id,
            parse_status="failed",
            parse_error="invalid payload",
        )
        self.assertEqual(updated["parse_status"], "failed")
        self.assertEqual(updated["parse_error"], "invalid payload")

    def test_dispatch_candidates_and_idempotent_job_creation(self):
        user_id = self.repo.create_user("dispatch-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="dispatch-source",
        )["id"]
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="执行号",
            phone="+12017770000",
            session_path="/data/dispatch-user/main",
        )["id"]
        signal_id = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407010",
            bet_type="big_small",
            bet_value="大",
            normalized_payload={},
        )["id"]
        self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"stake_amount": 15},
        )
        target_id = self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-100333",
            target_name="实盘群",
            status="active",
        )["id"]

        candidates = self.repo.list_dispatch_candidates(signal_id)
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["delivery_target_id"], target_id)
        self.assertEqual(candidates[0]["telegram_account_id"], account_id)

        created = self.repo.create_execution_job_record(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            idempotency_key="signal:%s:target:%s" % (signal_id, target_id),
            planned_message_text="大15",
            stake_plan={"mode": "flat", "amount": 15},
            execute_after=utc_now_iso(),
            expire_at=utc_now_iso(),
        )
        duplicated = self.repo.create_execution_job_record(
            user_id=user_id,
            signal_id=signal_id,
            delivery_target_id=target_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            idempotency_key="signal:%s:target:%s" % (signal_id, target_id),
            planned_message_text="大15",
            stake_plan={"mode": "flat", "amount": 15},
            execute_after=utc_now_iso(),
            expire_at=utc_now_iso(),
        )
        self.assertTrue(created["created"])
        self.assertFalse(duplicated["created"])
        jobs = self.repo.list_execution_jobs(signal_id=signal_id)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["telegram_account_id"], account_id)

    def test_dispatch_signal_creates_settlement_event_for_follow_mode(self):
        user_id = self.repo.create_user("dispatch-follow-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="dispatch-follow",
        )["id"]
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="执行号",
            phone="+12017771234",
            session_path="/data/dispatch-follow/main",
            status="active",
        )["id"]
        self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"mode": "follow", "stake_amount": 12},
        )
        self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-1004455",
            target_name="跟随群",
            status="active",
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407012",
            bet_type="big_small",
            bet_value="单",
            normalized_payload={},
        )

        result = dispatch_signal(self.repo, signal["id"])

        self.assertEqual(result["created_count"], 1)
        self.assertIsNotNone(result["jobs"][0]["progression_event_id"])
        event = self.repo.get_progression_event(result["jobs"][0]["progression_event_id"])
        self.assertIsNotNone(event)
        self.assertEqual(event["issue_no"], "20260407012")
        self.assertEqual(event["status"], "pending")

    def test_dispatch_signal_skips_unselected_play_filter(self):
        user_id = self.repo.create_user("dispatch-play-filter-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="dispatch-play-filter",
        )["id"]
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="执行号",
            phone="+12017771235",
            session_path="/data/dispatch-play-filter/main",
            status="active",
        )["id"]
        self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "mode": "follow",
                "stake_amount": 12,
                "bet_filter": {"mode": "selected", "selected_keys": ["big_small:大"]},
            },
        )
        self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-1004456",
            target_name="跟随群",
            status="active",
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407013",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={},
        )

        result = dispatch_signal(self.repo, signal["id"])

        self.assertEqual(result["candidate_count"], 0)
        self.assertEqual(result["created_count"], 0)
        self.assertEqual(self.repo.list_execution_jobs(), [])

    def test_dispatch_signal_matches_danshuang_filter_for_big_small_signal(self):
        user_id = self.repo.create_user("dispatch-odd-even-filter-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="dispatch-odd-even-filter",
        )["id"]
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="执行号",
            phone="+12017771236",
            session_path="/data/dispatch-odd-even-filter/main",
            status="active",
        )["id"]
        self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "mode": "follow",
                "stake_amount": 12,
                "bet_filter": {"mode": "selected", "selected_keys": ["odd_even:单"]},
            },
        )
        self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-1004457",
            target_name="跟随群",
            status="active",
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407014",
            bet_type="big_small",
            bet_value="单",
            normalized_payload={},
        )

        result = dispatch_signal(self.repo, signal["id"])

        self.assertEqual(result["candidate_count"], 1)
        self.assertEqual(result["created_count"], 1)
        self.assertEqual(result["jobs"][0]["planned_message_text"], "单12")

    def test_dispatch_signal_freezes_settlement_rule_on_progression_event(self):
        user_id = self.repo.create_user("dispatch-settlement-freeze-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="dispatch-settlement-freeze",
        )["id"]
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="执行号",
            phone="+12017771237",
            session_path="/data/dispatch-settlement-freeze/main",
            status="active",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "play_filter": {"mode": "all", "selected_keys": []},
                "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                "settlement_policy": {
                    "rule_source": "subscription_fixed",
                    "settlement_rule_id": "pc28_high_regular",
                    "fallback_profit_ratio": 1.0,
                },
                "risk_control": {"enabled": False, "profit_target": 0, "loss_limit": 0},
                "dispatch": {"expire_after_seconds": 120},
            },
        )
        self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-1004458",
            target_name="冻结结算群",
            status="active",
        )
        signal = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407015",
            bet_type="big_small",
            bet_value="大",
            normalized_payload={"profit_rule_id": "pc28_netdisk", "odds_profile": "regular"},
        )

        result = dispatch_signal(self.repo, signal["id"])

        event = self.repo.get_progression_event(result["jobs"][0]["progression_event_id"])
        self.assertEqual(event["settlement_rule_id"], "pc28_high_regular")
        self.assertEqual(event["settlement_snapshot"]["rule_source"], "subscription_fixed")

        self.repo.update_subscription_record(
            subscription_id=subscription["id"],
            user_id=user_id,
            source_id=source_id,
            strategy={
                "play_filter": {"mode": "all", "selected_keys": []},
                "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                "settlement_policy": {
                    "rule_source": "subscription_fixed",
                    "settlement_rule_id": "pc28_netdisk_regular",
                    "fallback_profit_ratio": 1.0,
                },
                "risk_control": {"enabled": False, "profit_target": 0, "loss_limit": 0},
                "dispatch": {"expire_after_seconds": 120},
            },
            status="active",
        )
        self.repo.update_progression_event_status(progression_event_id=event["id"], status="placed")

        settled = self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )

        self.assertEqual(settled["event"]["settlement_rule_id"], "pc28_high_regular")
        self.assertEqual(settled["financial"]["realized_profit"], 18.46)

    def test_dispatch_candidates_excludes_inactive_account(self):
        user_id = self.repo.create_user("dispatch-inactive-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="dispatch-source-inactive",
        )["id"]
        account_id = self.repo.create_telegram_account_record(
            user_id=user_id,
            label="暂停账号",
            phone="+12017770001",
            session_path="/data/dispatch-inactive/main",
            status="inactive",
        )["id"]
        signal_id = self.repo.create_signal_record(
            source_id=source_id,
            lottery_type="pc28",
            issue_no="20260407011",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={},
        )["id"]
        self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={"stake_amount": 12},
        )
        self.repo.create_delivery_target_record(
            user_id=user_id,
            telegram_account_id=account_id,
            executor_type="telegram_group",
            target_key="-100334",
            target_name="暂停群",
        )

        candidates = self.repo.list_dispatch_candidates(signal_id)
        self.assertEqual(candidates, [])


if __name__ == "__main__":
    unittest.main()
