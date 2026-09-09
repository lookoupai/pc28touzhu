from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
from unittest.mock import patch

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.auto_trigger_service import (
    create_auto_trigger_rule,
    list_auto_trigger_rules,
    run_auto_trigger_cycle,
    stop_auto_trigger_rule_current_run,
    update_auto_trigger_rule,
)
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.platform_service import list_subscription_runtime_runs


class DispatchIsolationTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = os.path.join(self.tmpdir.name, "isolation.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()
        self.now = datetime(2026, 9, 2, 10, 5, tzinfo=timezone.utc)
        clock = patch("pc28touzhu.executor.db_repository._utc_now_iso", side_effect=self._iso_now)
        clock.start()
        self.addCleanup(clock.stop)
        dispatch_clock = patch("pc28touzhu.services.dispatch_service._utc_now", side_effect=lambda: self.now)
        dispatch_clock.start()
        self.addCleanup(dispatch_clock.stop)
        self.user_id = self.repo.create_user("isolation-user")
        self.source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="internal_ai", name="测试信号",
        )
        self.subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"],
            strategy={"mode": "follow", "stake_amount": 10},
        )
        self.target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100600001", status="active",
        )
        self.signal_no = 0

    def _iso_now(self):
        return self.now.isoformat().replace("+00:00", "Z")

    def _signal(self, *, source=None, issue_no=None):
        self.signal_no += 1
        return self.repo.create_signal_record(
            source_id=(source or self.source)["id"], lottery_type="pc28",
            issue_no=issue_no or str(20260902000 + self.signal_no),
            bet_type="big_small", bet_value="大", published_at=self._iso_now(),
        )

    def _rule(self, *, target=None, subscriptions=None, risk=None):
        return create_auto_trigger_rule(
            self.repo, user_id=self.user_id,
            payload={
                "name": "独立路由",
                "cooldown_issues": 0,
                "scope_mode": "selected_subscriptions",
                "subscription_ids": subscriptions or [self.subscription["id"]],
                "trigger_mode": "schedule",
                "schedule": {
                    "timezone": "Asia/Shanghai", "weekdays": ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"],
                    "windows": [{"id": "primary", "start": "18:00", "end": "19:00"}],
                },
                "daily_risk_control": risk or {"enabled": False},
                "routes": [{
                    "delivery_target_id": (target or self.target)["id"],
                    "route_risk_mode": "disabled", "subscription_risk_mode": "disabled",
                }],
            },
        )["item"]

    def _context(self, rule, *, subscription=None, stat_date=None):
        run = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule["id"], user_id=self.user_id,
            subscription_id=(subscription or self.subscription)["id"],
            stat_date=stat_date or self.now.astimezone(timezone(timedelta(hours=8))).date().isoformat(),
        )
        return {"rule_id": rule["id"], "rule_run_id": run["id"], "stat_date": run["stat_date"], "routes": rule["routes"]}

    def _route_dispatch(self, rule, signal, *, context=None, subscription=None):
        subscription = subscription or self.subscription
        return dispatch_signal(
            self.repo, signal["id"], subscription_id=subscription["id"],
            auto_trigger_context=context or self._context(rule, subscription=subscription),
        )

    def _settle(self, job, result="miss"):
        return self.repo.settle_progression_event(
            subscription_id=job["subscription_id"], user_id=self.user_id,
            progression_event_id=job["progression_event_id"], result_type=result,
        )

    def test_route_dispatch_preserves_issue_send_deadline(self):
        rule = self._rule()
        signal = self._signal(issue_no="3478696")
        clock = {
            "latest_issue_no": "3478695",
            "latest_open_time": (self.now - timedelta(seconds=117)).isoformat(),
            "fetched_at": self.now.isoformat(),
        }
        result = dispatch_signal(
            self.repo, signal["id"], subscription_id=self.subscription["id"],
            auto_trigger_context=self._context(rule), draw_clock=clock,
        )
        expected = (self.now + timedelta(seconds=53)).isoformat().replace("+00:00", "Z")
        self.assertEqual(result["jobs"][0]["expire_at"], expected)
        self.assertEqual(result["jobs"][0]["stake_plan"]["meta"]["issue_window"]["send_before"], expected)

    def test_expired_route_job_releases_next_signal_without_resetting_run(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"], user_id=self.user_id, status="standby",
        )
        rule = self._rule()
        first = self._route_dispatch(rule, self._signal())["jobs"][0]
        self.repo.report_job_result(
            str(first["id"]), "test-executor", 1, "delivered", "message-1", self._iso_now(), {}, None,
        )
        self._settle(first)
        job = self._route_dispatch(rule, self._signal())["jobs"][0]
        event = self.repo.get_progression_event(job["progression_event_id"])
        financial = self._rows("SELECT * FROM auto_trigger_route_subscription_financial_state")
        runs = self._rows("SELECT * FROM auto_trigger_route_subscription_runtime_runs")
        self.now += timedelta(minutes=4)

        self.assertEqual(self.repo.pull_ready_jobs(executor_id="test-executor"), [])
        self.assertEqual(self.repo.get_execution_job(job["id"])["status"], "expired")
        self.assertEqual(self.repo.get_progression_event(event["id"])["status"], "void")
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_route_subscription_financial_state"), financial)
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_route_subscription_runtime_runs"), runs)
        self.assertEqual(self.repo.expire_due_jobs(), 0)

        next_result = dispatch_signal(self.repo, self._signal()["id"])
        self.assertEqual(next_result["created_count"], 1)
        next_event = self.repo.get_progression_event(next_result["jobs"][0]["progression_event_id"])
        self.assertEqual(next_event["auto_trigger_rule_run_id"], event["auto_trigger_rule_run_id"])
        self.assertEqual(next_event["auto_trigger_runtime_run_id"], event["auto_trigger_runtime_run_id"])

    def test_expiry_keeps_event_pending_while_another_target_can_send(self):
        jobs = dispatch_signal(self.repo, self._signal()["id"])["jobs"]
        jobs.append(self._additional_target_job(jobs[0]))
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET expire_at=? WHERE id=?", (
                (self.now + timedelta(minutes=10)).isoformat().replace("+00:00", "Z"), jobs[1]["id"],
            ))
        self.now += timedelta(minutes=4)

        self.assertEqual(self.repo.expire_due_jobs(), 1)
        self.assertEqual(self.repo.get_progression_event(jobs[0]["progression_event_id"])["status"], "pending")
        self.assertEqual(self.repo.get_execution_job(jobs[1]["id"])["status"], "pending")

    def test_expiry_keeps_delivered_event_for_settlement(self):
        jobs = dispatch_signal(self.repo, self._signal()["id"])["jobs"]
        jobs.append(self._additional_target_job(jobs[0]))
        self.repo.report_job_result(
            str(jobs[0]["id"]), "test-executor", 1, "delivered", "message-1", self._iso_now(), {}, None,
        )
        self.now += timedelta(minutes=4)

        self.assertEqual(self.repo.expire_due_jobs(), 1)
        self.assertEqual(self.repo.get_progression_event(jobs[0]["progression_event_id"])["status"], "placed")
        self.assertEqual(self.repo.get_execution_job(jobs[0]["id"])["status"], "delivered")

    def test_expiry_voids_event_when_other_target_has_failed(self):
        jobs = dispatch_signal(self.repo, self._signal()["id"])["jobs"]
        jobs.append(self._additional_target_job(jobs[0]))
        self.repo.report_job_result(
            str(jobs[0]["id"]), "test-executor", 1, "failed", None, self._iso_now(), {}, "连接失败",
        )
        self.assertEqual(self.repo.get_progression_event(jobs[0]["progression_event_id"])["status"], "pending")
        self.now += timedelta(minutes=4)

        self.assertEqual(self.repo.expire_due_jobs(), 1)
        self.assertEqual(self.repo.get_progression_event(jobs[0]["progression_event_id"])["status"], "void")

    def test_late_delivery_report_restores_auto_expired_event_for_settlement(self):
        job = self._route_dispatch(self._rule(), self._signal())["jobs"][0]
        sent_at = self._iso_now()
        self.now += timedelta(minutes=4)
        self.repo.expire_due_jobs()
        self.assertEqual(self.repo.get_progression_event(job["progression_event_id"])["status"], "void")

        self.repo.report_job_result(
            str(job["id"]), "test-executor", 1, "delivered", "message-1", sent_at, {}, None,
        )
        self.assertEqual(self.repo.get_progression_event(job["progression_event_id"])["status"], "placed")
        self._settle(job)
        before = self._rows("SELECT * FROM auto_trigger_route_subscription_financial_state")
        self.repo.report_job_result(
            str(job["id"]), "test-executor", 1, "delivered", "message-1", sent_at, {}, None,
        )
        self.assertEqual(self.repo.get_progression_event(job["progression_event_id"])["status"], "settled")
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_route_subscription_financial_state"), before)

    def test_expiry_and_event_update_roll_back_together(self):
        job = self._route_dispatch(self._rule(), self._signal())["jobs"][0]
        self.now += timedelta(minutes=4)
        with patch.object(self.repo, "_sync_progression_event_delivery_status", side_effect=RuntimeError("写入失败")):
            with self.assertRaisesRegex(RuntimeError, "写入失败"):
                self.repo.expire_due_jobs()
        self.assertEqual(self._rows("SELECT status FROM execution_jobs WHERE id=?", (job["id"],)), [{"status": "pending"}])
        self.assertEqual(self.repo.get_progression_event(job["progression_event_id"])["status"], "pending")

    def test_scheduled_route_preserves_standby_and_stops_without_direct_fallback(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"], user_id=self.user_id, status="standby",
        )
        other_target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100600002", status="active",
        )
        rule = self._rule()
        self._signal()

        result = run_auto_trigger_cycle(self.repo, rule_id=rule["id"], user_id=self.user_id, now=self.now)

        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.assertEqual(self.repo.get_subscription(self.subscription["id"])["status"], "standby")
        first = self.repo.list_execution_jobs(user_id=self.user_id)[0]
        self.assertEqual(first["auto_trigger_route_id"], rule["routes"][0]["id"])
        self.assertEqual(first["delivery_target_id"], self.target["id"])
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET status='delivered' WHERE id=?", (first["id"],))
        self._settle(first)

        self.now += timedelta(minutes=4)
        second = dispatch_signal(self.repo, self._signal()["id"])
        self.assertEqual(second["created_count"], 1)
        self.assertEqual(second["jobs"][0]["auto_trigger_route_id"], rule["routes"][0]["id"])
        self.assertNotEqual(second["jobs"][0]["delivery_target_id"], other_target["id"])

        self.repo.update_auto_trigger_rule_status(rule_id=rule["id"], user_id=self.user_id, status="inactive")
        self.now += timedelta(minutes=4)
        stopped = dispatch_signal(self.repo, self._signal()["id"])
        self.assertEqual(stopped["created_count"], 0)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 2)

    def test_explicit_route_cannot_send_after_subscription_is_disabled(self):
        rule = self._rule()
        signal = self._signal()
        context = self._context(rule)
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"], user_id=self.user_id, status="inactive",
        )

        result = self._route_dispatch(rule, signal, context=context)

        self.assertEqual(result["created_count"], 0)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])

    def _rows(self, sql, params=()):
        with self.repo._connect() as conn:
            return [dict(row) for row in conn.execute(sql, params)]

    def _additional_target_job(self, job):
        target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100600002",
        )
        job_id = self.repo.create_execution_job(
            user_id=self.user_id, signal_id=job["signal_id"],
            subscription_id=job["subscription_id"], progression_event_id=job["progression_event_id"],
            delivery_target_id=target["id"], executor_type="telegram_group",
            planned_message_text="大10", stake_plan={"amount": 10},
            idempotency_key="second-target", execute_after=job["execute_after"], expire_at=job["expire_at"],
        )
        return self.repo.get_execution_job(job_id)

    def test_concurrent_direct_and_two_routes_create_one_job_and_event(self):
        signal = self._signal()
        contexts = [self._context(self._rule()), self._context(self._rule()), None]
        barrier = Barrier(len(contexts))

        def send(context):
            repo = DatabaseRepository(self.db_path)
            barrier.wait(timeout=5)
            return dispatch_signal(
                repo, signal["id"], subscription_id=self.subscription["id"], auto_trigger_context=context,
            )

        with ThreadPoolExecutor(max_workers=len(contexts)) as pool:
            results = list(pool.map(send, contexts))
        self.assertEqual(sum(item["created_count"] for item in results), 1)
        self.assertEqual(len(self._rows("SELECT * FROM execution_jobs")), 1)
        self.assertEqual(len(self._rows("SELECT * FROM subscription_progression_events")), 1)
        self.assertEqual(
            len(self._rows("SELECT id FROM subscription_runtime_runs"))
            + len(self._rows("SELECT id FROM auto_trigger_route_subscription_runtime_runs")), 1,
        )

    def test_two_routes_sequentially_share_delivery_claim(self):
        signal = self._signal()
        first, second = self._rule(), self._rule()
        self.assertEqual(self._route_dispatch(first, signal)["created_count"], 1)
        self.assertEqual(self._route_dispatch(second, signal)["created_count"], 0)
        self.assertEqual(len(self._rows("SELECT * FROM subscription_progression_events")), 1)

    def test_dispatch_failure_rolls_back_runtime_and_event(self):
        signal = self._signal()
        context = self._context(self._rule())
        with patch.object(DatabaseRepository, "create_execution_job_record", side_effect=RuntimeError("写入失败")):
            with self.assertRaisesRegex(RuntimeError, "写入失败"):
                dispatch_signal(
                    self.repo, signal["id"], subscription_id=self.subscription["id"], auto_trigger_context=context,
                )
        self.assertEqual(self._rows("SELECT id FROM subscription_progression_events"), [])
        self.assertEqual(self._rows("SELECT id FROM auto_trigger_route_subscription_runtime_runs"), [])

    def test_schedule_dispatch_failure_rolls_back_start_and_financial_reset(self):
        rule = self._rule()
        job = self._route_dispatch(rule, self._signal())["jobs"][0]
        self._settle(job)
        stop_auto_trigger_rule_current_run(self.repo, rule_id=rule["id"], user_id=self.user_id)
        before = self._rows("SELECT * FROM auto_trigger_route_subscription_financial_state")
        self.now += timedelta(days=1)
        self._signal()
        with patch.object(DatabaseRepository, "create_execution_job_record", side_effect=RuntimeError("写入失败")):
            result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(result["rules"][0]["summary"]["failed_count"], 1)
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_route_subscription_financial_state"), before)
        self.assertEqual(self._rows("SELECT id FROM auto_trigger_rule_runs WHERE stat_date='2026-09-03'"), [])
        self.assertEqual(len(self._rows("SELECT id FROM subscription_progression_events")), 1)

    def test_database_rejects_job_with_a_different_path_key(self):
        signal = self._signal()
        dispatch_signal(self.repo, signal["id"])
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.create_execution_job(
                user_id=self.user_id, signal_id=signal["id"], subscription_id=self.subscription["id"],
                delivery_target_id=self.target["id"], executor_type="telegram_group", idempotency_key="different-key",
                planned_message_text="大10", stake_plan={}, execute_after=self._iso_now(), expire_at="2026-09-02T10:07:00Z",
            )

    def test_direct_event_ignores_active_and_stopped_route_rules(self):
        for stop in (False, True):
            with self.subTest(stopped=stop):
                rule = self._rule()
                self._context(rule)
                if stop:
                    stop_auto_trigger_rule_current_run(self.repo, rule_id=rule["id"], user_id=self.user_id)
                result = dispatch_signal(self.repo, self._signal()["id"])
                self.assertEqual(result["created_count"], 1)
                event = self.repo.get_progression_event(result["jobs"][0]["progression_event_id"])
                self.assertIsNone(event["auto_trigger_rule_id"])
                self.assertIsNone(event["auto_trigger_rule_run_id"])
                self.assertEqual(event["auto_trigger_stat_date"], "")

    def test_auto_trigger_threshold_scope_does_not_block_direct_dispatch(self):
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_financial_state(
                    subscription_id, user_id, threshold_status, threshold_scope, stopped_reason
                ) VALUES (?, ?, 'loss_limit_hit', 'auto_trigger_rule', '自动规则已停止')
                ON CONFLICT(subscription_id) DO UPDATE SET
                    threshold_status=excluded.threshold_status,
                    threshold_scope=excluded.threshold_scope,
                    stopped_reason=excluded.stopped_reason
                """,
                (self.subscription["id"], self.user_id),
            )
        result = dispatch_signal(self.repo, self._signal()["id"])
        self.assertEqual(result["created_count"], 1)
        event = self.repo.get_progression_event(result["jobs"][0]["progression_event_id"])
        self.assertIsNone(event["auto_trigger_rule_id"])

    def test_subscription_threshold_scope_still_blocks_direct_dispatch(self):
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_financial_state(
                    subscription_id, user_id, threshold_status, threshold_scope, stopped_reason
                ) VALUES (?, ?, 'loss_limit_hit', 'subscription', '订阅已停止')
                ON CONFLICT(subscription_id) DO UPDATE SET
                    threshold_status=excluded.threshold_status,
                    threshold_scope=excluded.threshold_scope,
                    stopped_reason=excluded.stopped_reason
                """,
                (self.subscription["id"], self.user_id),
            )
        result = dispatch_signal(self.repo, self._signal()["id"])
        self.assertEqual(result["created_count"], 0)

    def test_rule_stop_does_not_set_direct_subscription_threshold(self):
        rule = self._rule(risk={"enabled": True, "loss_limit": 5})
        job = self._route_dispatch(rule, self._signal())["jobs"][0]
        result = self._settle(job)
        self.assertTrue(result["auto_trigger_daily_risk"]["stopped"])
        financial = self.repo.get_subscription_financial_state(self.subscription["id"])
        self.assertEqual(financial["threshold_status"], "")
        self.assertEqual(financial["net_profit"], 0)
        self.assertEqual(self._rows("SELECT id FROM auto_trigger_route_subscription_runtime_runs WHERE status='active'"), [])

    def test_direct_loss_limit_does_not_close_route_rule(self):
        self.repo.update_subscription_record(
            subscription_id=self.subscription["id"], user_id=self.user_id, source_id=self.source["id"],
            strategy={"mode": "follow", "stake_amount": 10, "risk_control": {"enabled": True, "loss_limit": 10}},
        )
        rule = self._rule()
        context = self._context(rule)
        direct = dispatch_signal(self.repo, self._signal()["id"])["jobs"][0]
        self._settle(direct)
        self.assertEqual(self.repo.get_subscription_financial_state(self.subscription["id"])["threshold_status"], "loss_limit_hit")
        self.assertEqual(self.repo.get_auto_trigger_rule_run(context["rule_run_id"])["status"], "active")

    def test_scheduled_route_ignores_direct_runtime_on_another_target(self):
        dispatch_signal(self.repo, self._signal()["id"])
        other_target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100600002", status="active",
        )
        rule = self._rule(target=other_target)
        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(result["rules"][0]["summary"]["triggered_count"], 1)

    def test_route_closure_releases_next_day_without_reading_ui(self):
        rule = self._rule()
        rule = update_auto_trigger_rule(self.repo, rule_id=rule["id"], user_id=self.user_id, payload={
            "routes": [{**rule["routes"][0], "subscription_risk_mode": "override", "subscription_risk_control": {"enabled": True, "loss_limit": 10}}],
        })["item"]
        signal = self._signal()
        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(first["rules"][0]["summary"]["triggered_count"], 1)
        job = self.repo.list_execution_jobs(user_id=self.user_id, signal_id=signal["id"])[0]
        self._settle(job)
        self.assertEqual(self._rows("SELECT status FROM auto_trigger_rule_runs")[0]["status"], "closed")
        self.now += timedelta(days=1)
        self._signal()
        second = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(second["rules"][0]["summary"]["triggered_count"], 1)

    def _start_overnight_schedule(self, *, route_stop=False):
        self.now = datetime(2026, 9, 7, 10, 2, tzinfo=timezone.utc)
        rule = self._rule(risk={"enabled": True, "profit_target": 5} if not route_stop else None)
        payload = {"schedule": {**rule["schedule"], "windows": [
            {"id": "primary", "start": "18:01", "end": "18:30"},
            {"id": "fallback", "start": "20:00", "end": "20:30"},
        ]}}
        if route_stop:
            payload["routes"] = [{
                **rule["routes"][0], "subscription_risk_mode": "override",
                "subscription_risk_control": {"enabled": True, "profit_target": 5},
            }]
        rule = update_auto_trigger_rule(
            self.repo, rule_id=rule["id"], user_id=self.user_id, payload=payload,
        )["item"]
        signal = self._signal()
        started = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(started["summary"]["triggered_count"], 1)
        job = self.repo.list_execution_jobs(user_id=self.user_id, signal_id=signal["id"])[0]
        self.repo.report_job_result(
            str(job["id"]), "test-executor", 1, "delivered", "overnight-message", self._iso_now(), {}, None,
        )
        return rule, job

    def _legacy_schedule_block(self, rule):
        stat_date = self.now.astimezone(timezone(timedelta(hours=8))).date().isoformat()
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO auto_trigger_rule_runs(
                    rule_id, user_id, subscription_id, stat_date, status, started_at
                ) VALUES (?, ?, ?, ?, 'blocked', ?)
                """,
                (rule["id"], self.user_id, self.subscription["id"], stat_date, self._iso_now()),
            )
        return self.repo.get_auto_trigger_rule_run_for_subscription_date(
            rule_id=rule["id"], subscription_id=self.subscription["id"], stat_date=stat_date,
        )

    def test_schedule_overnight_rule_stop_allows_evening_start(self):
        rule, job = self._start_overnight_schedule()
        self.now = datetime(2026, 9, 7, 16, 9, 38, tzinfo=timezone.utc)
        self._settle(job, "hit")
        old_run = self._rows("SELECT * FROM auto_trigger_rule_runs")[0]
        self.assertEqual(old_run["status"], "stopped")
        self.assertEqual(old_run["stat_date"], "2026-09-07")
        old_stats = self._rows("SELECT * FROM auto_trigger_rule_daily_stats")

        self.now = datetime(2026, 9, 8, 10, 1, 30, tzinfo=timezone.utc)
        self._signal()
        started = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(started["summary"]["triggered_count"], 1)
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_rule_daily_stats"), old_stats)
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_rule_runs WHERE id=?", (old_run["id"],))[0], old_run)
        listed = list_auto_trigger_rules(self.repo, user_id=self.user_id, stat_date="2026-09-08")["items"][0]
        self.assertEqual(listed["schedule_status"], "主窗口已启动")

        self.now = datetime(2026, 9, 8, 12, 0, 30, tzinfo=timezone.utc)
        self._signal()
        duplicate = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(duplicate["summary"]["triggered_count"], 0)
        self.assertEqual(duplicate["rules"][0]["events"][0]["reason"], "schedule_day_already_started")

    def test_schedule_overnight_route_stop_reuses_legacy_block(self):
        rule, job = self._start_overnight_schedule(route_stop=True)
        self.now = datetime(2026, 9, 7, 16, 13, 10, tzinfo=timezone.utc)
        self._settle(job, "hit")
        old_run = self._rows("SELECT * FROM auto_trigger_rule_runs")[0]
        self.assertEqual(old_run["status"], "closed")
        self.now = datetime(2026, 9, 8, 10, 1, 2, tzinfo=timezone.utc)
        blocked = self._legacy_schedule_block(rule)
        listed = list_auto_trigger_rules(self.repo, user_id=self.user_id, stat_date="2026-09-08")["items"][0]
        self.assertNotIn("已启动", listed["schedule_status"])
        self._signal()
        started = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(started["summary"]["triggered_count"], 1)
        current = self.repo.get_auto_trigger_rule_run(blocked["id"])
        self.assertEqual(current["status"], "active")
        self.assertEqual(current["stat_date"], "2026-09-08")
        self.assertTrue(current["started_issue_no"])
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_rule_runs WHERE id=?", (old_run["id"],))[0], old_run)

    def test_schedule_waits_for_previous_round_without_consuming_fallback(self):
        rule, job = self._start_overnight_schedule(route_stop=True)
        self.now = datetime(2026, 9, 8, 10, 1, 30, tzinfo=timezone.utc)
        self._signal()
        waiting = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(waiting["summary"]["triggered_count"], 0)
        self.assertEqual(waiting["rules"][0]["events"][0]["reason"], "schedule_previous_run_active")
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_rule_runs WHERE stat_date='2026-09-08'"), [])
        listed = list_auto_trigger_rules(self.repo, user_id=self.user_id, stat_date="2026-09-08")["items"][0]
        self.assertEqual(listed["schedule_status"], "上一轮仍在运行，等待结束")

        self.now = datetime(2026, 9, 8, 11, 55, tzinfo=timezone.utc)
        self._settle(job, "hit")
        self.now = datetime(2026, 9, 8, 12, 0, 30, tzinfo=timezone.utc)
        self._signal()
        started = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(started["summary"]["triggered_count"], 1)
        self.assertEqual(started["rules"][0]["events"][0]["snapshot"]["window_id"], "fallback")

    def test_legacy_schedule_block_waits_for_fresh_window_signal(self):
        rule, job = self._start_overnight_schedule(route_stop=True)
        self.now = datetime(2026, 9, 7, 16, 13, tzinfo=timezone.utc)
        self._settle(job, "hit")
        self.now = datetime(2026, 9, 8, 10, 1, 2, tzinfo=timezone.utc)
        blocked = self._legacy_schedule_block(rule)
        stale = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(stale["summary"]["triggered_count"], 0)
        self.assertEqual(stale["rules"][0]["events"][0]["reason"], "schedule_signal_before_window")
        self.assertEqual(self.repo.get_auto_trigger_rule_run(blocked["id"]), blocked)

        self.now = datetime(2026, 9, 8, 11, 0, tzinfo=timezone.utc)
        self._signal()
        outside = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(outside["summary"]["triggered_count"], 0)
        self.assertEqual(outside["rules"][0]["events"][0]["reason"], "outside_schedule_window")
        self.assertEqual(self.repo.get_auto_trigger_rule_run(blocked["id"]), blocked)

        self.now = datetime(2026, 9, 8, 12, 0, 30, tzinfo=timezone.utc)
        self._signal()
        started = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(started["summary"]["triggered_count"], 1)
        self.assertEqual(self.repo.get_auto_trigger_rule_run(blocked["id"])["started_at"], self._iso_now())

    def test_legacy_schedule_block_dispatch_failure_can_retry(self):
        rule, job = self._start_overnight_schedule(route_stop=True)
        self.now = datetime(2026, 9, 7, 16, 13, tzinfo=timezone.utc)
        self._settle(job, "hit")
        self.now = datetime(2026, 9, 8, 10, 1, 30, tzinfo=timezone.utc)
        blocked = self._legacy_schedule_block(rule)
        financial = self._rows("SELECT * FROM auto_trigger_route_subscription_financial_state")
        self._signal()
        with patch.object(DatabaseRepository, "create_execution_job_record", side_effect=RuntimeError("写入失败")):
            failed = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(failed["summary"]["failed_count"], 1)
        self.assertEqual(self.repo.get_auto_trigger_rule_run(blocked["id"]), blocked)
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_route_subscription_financial_state"), financial)
        retry = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(retry["summary"]["triggered_count"], 1)

    def test_legacy_schedule_block_concurrent_start_creates_one_job(self):
        rule, job = self._start_overnight_schedule(route_stop=True)
        self.now = datetime(2026, 9, 7, 16, 13, tzinfo=timezone.utc)
        self._settle(job, "hit")
        self.now = datetime(2026, 9, 8, 10, 1, 30, tzinfo=timezone.utc)
        blocked = self._legacy_schedule_block(rule)
        signal = self._signal()
        barrier = Barrier(2)

        def start(_):
            repo = DatabaseRepository(self.db_path)
            barrier.wait(timeout=5)
            return run_auto_trigger_cycle(repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(start, range(2)))
        self.assertEqual(sum(item["summary"]["triggered_count"] for item in results), 1)
        self.assertEqual(sum(item["summary"]["failed_count"] for item in results), 0)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id, signal_id=signal["id"])), 1)
        self.assertEqual(self.repo.get_auto_trigger_rule_run(blocked["id"])["status"], "active")

    def test_legacy_schedule_block_with_execution_history_cannot_be_reused(self):
        rule, job = self._start_overnight_schedule(route_stop=True)
        old_event = self.repo.get_progression_event(job["progression_event_id"])
        self.now = datetime(2026, 9, 8, 10, 1, 30, tzinfo=timezone.utc)
        blocked = self._legacy_schedule_block(rule)
        for table in ("subscription_progression_events", "auto_trigger_route_subscription_runtime_runs"):
            with self.subTest(table=table):
                with self.repo._connect() as conn:
                    conn.execute(
                        f"UPDATE {table} SET auto_trigger_rule_run_id=? WHERE auto_trigger_rule_run_id=?",
                        (blocked["id"], old_event["auto_trigger_rule_run_id"]),
                    )
                claim = self.repo.claim_auto_trigger_daily_start(
                    rule_id=rule["id"], user_id=self.user_id, subscription_id=self.subscription["id"],
                    stat_date="2026-09-08", started_issue_no="new",
                )
                self.assertFalse(claim["claimed"])
                self.assertEqual(self.repo.get_auto_trigger_rule_run(blocked["id"]), blocked)
                with self.repo._connect() as conn:
                    conn.execute(
                        f"UPDATE {table} SET auto_trigger_rule_run_id=? WHERE auto_trigger_rule_run_id=?",
                        (old_event["auto_trigger_rule_run_id"], blocked["id"]),
                    )

    def test_manual_stop_preserves_balance_and_late_settlement_stays_in_old_run(self):
        rule = self._rule()
        context = self._context(rule)
        first = self._route_dispatch(rule, self._signal(), context=context)["jobs"][0]
        self._settle(first, "hit")
        self.now += timedelta(minutes=3)
        second = self._route_dispatch(rule, self._signal(), context=context)["jobs"][0]
        self.repo.update_progression_event_status(progression_event_id=second["progression_event_id"], status="placed")
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET status='delivered' WHERE id=?", (second["id"],))
        before = self.repo.get_auto_trigger_route_subscription_financial_state(
            route_id=rule["routes"][0]["id"], subscription_id=self.subscription["id"], user_id=self.user_id,
        )
        self.now += timedelta(minutes=1)
        stopped = stop_auto_trigger_rule_current_run(self.repo, rule_id=rule["id"], user_id=self.user_id)
        self.assertEqual(stopped["pending_settlement_count"], 1)
        after = self.repo.get_auto_trigger_route_subscription_financial_state(
            route_id=rule["routes"][0]["id"], subscription_id=self.subscription["id"], user_id=self.user_id,
        )
        self.assertEqual(after["net_profit"], before["net_profit"])
        self.assertEqual(after["baseline_reset_at"], before["baseline_reset_at"])
        self.now += timedelta(minutes=1)
        self._settle(second)
        runs = self._rows("SELECT * FROM auto_trigger_route_subscription_runtime_runs")
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "closed")
        self.assertEqual(runs[0]["end_reason"], "manual_stop")
        self.assertEqual(runs[0]["net_profit"], before["net_profit"] - 10)
        self.assertEqual(runs[0]["settled_event_count"], 2)

    def test_stale_route_context_cannot_dispatch_after_manual_stop(self):
        rule = self._rule()
        context = self._context(rule)
        self._route_dispatch(rule, self._signal(), context=context)
        stop_auto_trigger_rule_current_run(self.repo, rule_id=rule["id"], user_id=self.user_id)
        self.now += timedelta(minutes=3)
        result = self._route_dispatch(rule, self._signal(), context=context)
        self.assertEqual(result["created_count"], 0)
        self.assertEqual(self._rows("SELECT id FROM auto_trigger_route_subscription_runtime_runs WHERE status='active'"), [])

    def test_target_rule_only_blocks_direct_after_rule_stops(self):
        self.repo.update_delivery_target_record(
            delivery_target_id=self.target["id"], user_id=self.user_id, telegram_account_id=None,
            executor_type="telegram_group", target_key=self.target["target_key"], dispatch_mode="rule_only",
        )
        signal = self._signal()
        self.assertEqual(dispatch_signal(self.repo, signal["id"])["created_count"], 0)
        rule = self._rule()
        self.assertEqual(self._route_dispatch(rule, signal)["created_count"], 1)
        stop_auto_trigger_rule_current_run(self.repo, rule_id=rule["id"], user_id=self.user_id)
        self.assertEqual(dispatch_signal(self.repo, self._signal()["id"])["created_count"], 0)

    def test_target_direct_only_blocks_stale_rule_context(self):
        rule = self._rule()
        context = self._context(rule)
        self.repo.update_delivery_target_record(
            delivery_target_id=self.target["id"], user_id=self.user_id, telegram_account_id=None,
            executor_type="telegram_group", target_key=self.target["target_key"], dispatch_mode="direct_only",
        )
        signal = self._signal()
        self.assertEqual(self._route_dispatch(rule, signal, context=context)["created_count"], 0)
        self.assertEqual(dispatch_signal(self.repo, signal["id"])["created_count"], 1)

    def test_stopping_one_subscription_keeps_other_subscription_jobs(self):
        source = self.repo.create_source_record(owner_user_id=self.user_id, source_type="internal_ai", name="第二信号")
        other = self.repo.create_subscription_record(user_id=self.user_id, source_id=source["id"], strategy={"stake_amount": 10})
        rule = self._rule(subscriptions=[self.subscription["id"], other["id"]])
        self._route_dispatch(rule, self._signal())
        other_job = self._route_dispatch(rule, self._signal(source=source), subscription=other)["jobs"][0]
        stop_auto_trigger_rule_current_run(
            self.repo, rule_id=rule["id"], user_id=self.user_id, payload={"subscription_id": self.subscription["id"]},
        )
        self.assertEqual(self.repo.get_execution_job(other_job["id"])["status"], "pending")
        self.assertEqual(self.repo.get_progression_event(other_job["progression_event_id"])["status"], "pending")

    def test_concurrent_settlement_counts_the_event_once(self):
        rule = self._rule()
        job = self._route_dispatch(rule, self._signal())["jobs"][0]
        barrier = Barrier(2)

        def settle(_):
            repo = DatabaseRepository(self.db_path)
            barrier.wait(timeout=5)
            return repo.settle_progression_event(
                subscription_id=self.subscription["id"], user_id=self.user_id,
                progression_event_id=job["progression_event_id"], result_type="miss",
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(settle, range(2)))
        self.assertEqual(self._rows("SELECT net_profit,settled_event_count FROM auto_trigger_route_subscription_runtime_runs"), [{"net_profit": -10.0, "settled_event_count": 1}])
        self.assertEqual(self._rows("SELECT net_profit,settled_event_count FROM auto_trigger_rule_daily_stats"), [{"net_profit": -10.0, "settled_event_count": 1}])

    def test_nested_transaction_failure_rolls_back_only_its_own_writes(self):
        with self.repo.transaction() as unit:
            first = unit.create_user("outer-first")
            with self.assertRaisesRegex(RuntimeError, "内层失败"):
                with unit.transaction() as nested:
                    nested.create_user("inner-rollback")
                    raise RuntimeError("内层失败")
            second = unit.create_user("outer-second")
        self.assertIsNotNone(self.repo.get_user(first))
        self.assertIsNotNone(self.repo.get_user(second))
        self.assertEqual(self._rows("SELECT id FROM users WHERE username='inner-rollback'"), [])

    def test_subscription_history_includes_direct_and_route_runtime(self):
        direct = dispatch_signal(self.repo, self._signal()["id"])["jobs"][0]
        self._settle(direct)
        self.now += timedelta(minutes=3)
        self._route_dispatch(self._rule(), self._signal())
        history = list_subscription_runtime_runs(
            self.repo, subscription_id=self.subscription["id"], user_id=self.user_id,
        )["items"]
        self.assertEqual(len(history), 2)
        self.assertEqual({item["scope"] for item in history}, {"subscription", "route"})
        direct_runtime = next(item for item in history if item["scope"] == "subscription")
        self.assertEqual(direct_runtime["net_profit"], -10)

    def test_passive_dispatch_keeps_explicit_parent_when_a_newer_parent_exists(self):
        rule = self._rule()
        old_context = self._context(rule)
        first = self._route_dispatch(rule, self._signal(), context=old_context)["jobs"][0]
        self._settle(first)
        self.now += timedelta(days=1)
        newer_context = self._context(rule)
        self.assertNotEqual(newer_context["rule_run_id"], old_context["rule_run_id"])
        signal = self._signal()
        candidates = self.repo.list_active_auto_trigger_route_dispatch_candidates(signal["id"])
        self.assertEqual(len(candidates), 1)
        self.assertEqual(candidates[0]["rule_run_id"], old_context["rule_run_id"])
        dispatched = dispatch_signal(self.repo, signal["id"])
        event = self.repo.get_progression_event(dispatched["jobs"][0]["progression_event_id"])
        self.assertEqual(event["auto_trigger_rule_run_id"], old_context["rule_run_id"])
        self.assertEqual(event["auto_trigger_stat_date"], old_context["stat_date"])

    def test_late_old_event_does_not_change_new_runtime_or_progression(self):
        rule = self._rule()
        route_id = rule["routes"][0]["id"]
        old_context = self._context(rule)
        old_job = self._route_dispatch(rule, self._signal(), context=old_context)["jobs"][0]
        self.repo.update_progression_event_status(progression_event_id=old_job["progression_event_id"], status="placed")
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET status='delivered' WHERE id=?", (old_job["id"],))
        stop_auto_trigger_rule_current_run(self.repo, rule_id=rule["id"], user_id=self.user_id)
        old_runtime = self._rows("SELECT * FROM auto_trigger_route_subscription_runtime_runs")[0]
        self.now += timedelta(days=1)
        new_context = self._context(rule)
        self.repo.reset_auto_trigger_route_subscription_runtime(
            route_id=route_id, rule_id=rule["id"], subscription_id=self.subscription["id"], user_id=self.user_id,
        )
        # 模拟历史版本在旧单未回执时已经开出新轮的数据库，验证升级后的延迟结算归属。
        signal = self._signal()
        new_event = self.repo.create_progression_event_record(
            subscription_id=self.subscription["id"], user_id=self.user_id, signal_id=signal["id"],
            issue_no=signal["issue_no"], progression_step=1, stake_amount=10,
            base_stake=10, multiplier=2, max_steps=5, refund_action="hold", cap_action="reset",
            auto_trigger_rule_id=rule["id"], auto_trigger_rule_run_id=new_context["rule_run_id"],
            auto_trigger_route_id=route_id, auto_trigger_stat_date=new_context["stat_date"],
        )
        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"], user_id=self.user_id,
            progression_event_id=new_event["id"], result_type="miss",
        )
        new_runtime_before = self._rows("SELECT * FROM auto_trigger_route_subscription_runtime_runs ORDER BY id DESC")[0]
        financial_before = self.repo.get_auto_trigger_route_subscription_financial_state(
            route_id=route_id, subscription_id=self.subscription["id"], user_id=self.user_id,
        )
        progression_before = self.repo.get_auto_trigger_route_progression_state(
            route_id=route_id, subscription_id=self.subscription["id"], user_id=self.user_id,
        )
        self.now += timedelta(minutes=1)
        self._settle(old_job, "hit")
        self.assertEqual(self._rows("SELECT * FROM auto_trigger_route_subscription_runtime_runs ORDER BY id DESC")[0], new_runtime_before)
        self.assertEqual(self.repo.get_auto_trigger_route_subscription_financial_state(
            route_id=route_id, subscription_id=self.subscription["id"], user_id=self.user_id,
        ), financial_before)
        self.assertEqual(self.repo.get_auto_trigger_route_progression_state(
            route_id=route_id, subscription_id=self.subscription["id"], user_id=self.user_id,
        ), progression_before)
        settled_old = self._rows("SELECT * FROM auto_trigger_route_subscription_runtime_runs WHERE id=?", (old_runtime["id"],))[0]
        self.assertEqual(settled_old["settled_event_count"], 1)
        self.assertEqual(settled_old["end_reason"], "manual_stop")
        self.assertGreater(settled_old["net_profit"], 0)

    def test_legacy_event_without_runtime_id_settles_in_its_closed_runtime(self):
        rule = self._rule()
        job = self._route_dispatch(rule, self._signal())["jobs"][0]
        self.repo.update_progression_event_status(progression_event_id=job["progression_event_id"], status="placed")
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET status='delivered' WHERE id=?", (job["id"],))
            conn.execute("UPDATE subscription_progression_events SET auto_trigger_runtime_run_id=NULL WHERE id=?", (job["progression_event_id"],))
            conn.execute("UPDATE auto_trigger_route_subscription_runtime_runs SET auto_trigger_rule_run_id=NULL")
        self.now += timedelta(minutes=1)
        stop_auto_trigger_rule_current_run(self.repo, rule_id=rule["id"], user_id=self.user_id)
        self.now += timedelta(minutes=1)
        self._settle(job)
        self.assertEqual(self._rows("SELECT status,end_reason,net_profit FROM auto_trigger_route_subscription_runtime_runs"), [
            {"status": "closed", "end_reason": "manual_stop", "net_profit": -10.0},
        ])

    def test_new_route_cycle_resets_progression_but_manual_stop_does_not(self):
        rule = self._rule()
        route_id = rule["routes"][0]["id"]
        self._settle(self._route_dispatch(rule, self._signal())["jobs"][0])
        with self.repo._connect() as conn:
            self.repo.upsert_auto_trigger_route_progression_state(
                conn, route_id=route_id, rule_id=rule["id"], subscription_id=self.subscription["id"],
                user_id=self.user_id, current_step=3, last_signal_id=None, last_issue_no="old-issue",
                last_result_type="miss", updated_at=self._iso_now(),
            )
        stop_auto_trigger_rule_current_run(self.repo, rule_id=rule["id"], user_id=self.user_id)
        self.assertEqual(self.repo.get_auto_trigger_route_progression_state(
            route_id=route_id, subscription_id=self.subscription["id"], user_id=self.user_id,
        )["current_step"], 3)
        self.now += timedelta(days=1)
        self._signal()
        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, rule_id=rule["id"], now=self.now)
        self.assertEqual(result["rules"][0]["summary"]["triggered_count"], 1)
        self.assertEqual(self.repo.get_auto_trigger_route_progression_state(
            route_id=route_id, subscription_id=self.subscription["id"], user_id=self.user_id,
        )["current_step"], 1)

    def test_schema_upgrade_preserves_financial_amounts_and_thresholds(self):
        rule = self._rule()
        self._settle(self._route_dispatch(rule, self._signal())["jobs"][0])
        self.repo.update_subscription_record(
            subscription_id=self.subscription["id"], user_id=self.user_id, source_id=self.source["id"],
            strategy={"mode": "follow", "stake_amount": 10, "risk_control": {"enabled": True, "loss_limit": 10}},
        )
        direct_job = dispatch_signal(self.repo, self._signal()["id"], subscription_id=self.subscription["id"], auto_trigger_context={})["jobs"][0]
        self._settle(direct_job)
        with self.repo._connect() as conn:
            conn.execute("DROP INDEX idx_execution_jobs_signal_subscription_target")
            conn.execute("ALTER TABLE delivery_targets DROP COLUMN dispatch_mode")
            conn.execute("ALTER TABLE subscription_progression_events DROP COLUMN auto_trigger_runtime_run_id")
            conn.execute("ALTER TABLE auto_trigger_route_subscription_runtime_runs DROP COLUMN auto_trigger_rule_run_id")
            conn.execute("ALTER TABLE subscription_runtime_runs DROP COLUMN auto_trigger_rule_run_id")
        tables = ("subscription_financial_state", "auto_trigger_route_subscription_financial_state", "execution_jobs")
        before = {table: self._rows(f"SELECT * FROM {table}") for table in tables}
        self.assertEqual(before["subscription_financial_state"][0]["threshold_status"], "loss_limit_hit")
        self.repo.initialize_database()
        self.repo.initialize_database()
        for table in tables:
            self.assertEqual(self._rows(f"SELECT * FROM {table}"), before[table])
        self.assertEqual(self.repo.get_delivery_target(self.target["id"])["dispatch_mode"], "shared")
        self.assertEqual(self._rows("PRAGMA foreign_key_check"), [])
        self.assertEqual(len(self._rows("SELECT name FROM sqlite_master WHERE name='idx_execution_jobs_signal_subscription_target'")), 1)


if __name__ == "__main__":
    unittest.main()
