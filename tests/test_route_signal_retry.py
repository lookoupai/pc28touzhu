from __future__ import annotations

import os
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
from unittest.mock import patch

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.auto_trigger_service import create_auto_trigger_rule, stop_auto_trigger_rule_current_run
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.source_sync_service import run_source_sync_cycle


class RouteSignalRetryTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = os.path.join(self.tmpdir.name, "retry.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()
        self.now = datetime.now(timezone.utc).replace(microsecond=0) - timedelta(seconds=210)
        for name, value in (
            ("pc28touzhu.executor.db_repository._utc_now_iso", self._iso_now),
            ("pc28touzhu.services.dispatch_service._utc_now", lambda: self.now),
        ):
            mocked = patch(name, side_effect=value)
            mocked.start()
            self.addCleanup(mocked.stop)
        self.user_id = self.repo.create_user("route-retry-user")
        self.source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="ai_trading_simulator_export", name="共识信号",
            config={"fetch": {"url": "https://example.invalid/signals"}},
        )
        self.sub = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"], status="standby", strategy={"stake_amount": 10},
        )
        self.target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100100001", status="active",
        )
        self.rule = self._rule(self.target)
        self.previous = self._signal("3479173")
        self.first = self._start(self.rule, self.previous)
        self._delivered(self.first)
        self.now += timedelta(seconds=210)
        self.draw_clock = {
            "latest_issue_no": "3479173",
            "latest_open_time": (self.now - timedelta(seconds=10)).isoformat(),
            "stale": False,
        }
        self.payload = {"items": [{
            "signal_id": "consensus-3479174", "issue_no": "3479174", "lottery_type": "pc28",
            "published_at": self._iso_now(), "signals": [{"bet_type": "odd_even", "bet_value": "双"}],
        }]}

    def _iso_now(self):
        return self.now.isoformat().replace("+00:00", "Z")

    def _rule(self, target):
        return create_auto_trigger_rule(self.repo, user_id=self.user_id, payload={
            "name": "多数票定时跟单", "trigger_mode": "schedule",
            "scope_mode": "selected_subscriptions", "subscription_ids": [self.sub["id"]],
            "daily_risk_control": {"enabled": False},
            "routes": [{
                "delivery_target_id": target["id"],
                "route_risk_mode": "disabled", "subscription_risk_mode": "disabled",
            }],
        })["item"]

    def _signal(self, issue_no):
        return self.repo.create_signal_record(
            source_id=self.source["id"], lottery_type="pc28", issue_no=issue_no,
            bet_type="odd_even", bet_value="双", published_at=self._iso_now(),
        )

    def _start(self, rule, signal):
        run = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule["id"], subscription_id=self.sub["id"], user_id=self.user_id,
            stat_date=self.now.astimezone(timezone(timedelta(hours=8))).date().isoformat(),
        )
        return dispatch_signal(self.repo, signal["id"], subscription_id=self.sub["id"], auto_trigger_context={
            "rule_id": rule["id"], "rule_run_id": run["id"], "stat_date": run["stat_date"], "routes": rule["routes"],
        })["jobs"][0]

    def _delivered(self, job):
        self.repo.report_job_result(str(job["id"]), "test", 1, "delivered", str(job["id"]), self._iso_now(), {}, None)

    def _settle(self, job=None, *, result="miss"):
        return self.repo.settle_progression_event(
            subscription_id=self.sub["id"], user_id=self.user_id,
            progression_event_id=(job or self.first)["progression_event_id"], result_type=result,
        )

    def _sync(self, **overrides):
        args = {"fetcher": lambda *a, **k: self.payload, "draw_clock": self.draw_clock}
        args.update(overrides)
        return run_source_sync_cycle(self.repo, **args)

    def _receive_while_waiting(self):
        result = self._sync()
        self.assertEqual(result["summary"]["created_job_count"], 0)
        self.assertEqual(result["summary"]["dispatch_candidate_count"], 1)
        signal = self.repo.list_signals(source_id=self.source["id"])[0]
        self.assertEqual(signal["issue_no"], "3479174")
        self.assertIsNone(signal["dispatch_blocked_at"])
        return signal

    def _retry_ids(self):
        return self.repo.list_pending_auto_trigger_route_signals(created_after="2000-01-01T00:00:00Z")

    def _jobs(self):
        return self.repo.list_execution_jobs(user_id=self.user_id)

    def test_signal_is_retried_after_settlement_despite_source_deduplication(self):
        signal = self._receive_while_waiting()
        waiting = self._sync()
        self.assertEqual(waiting["summary"]["route_retry_checked_count"], 1)
        self.assertEqual(waiting["summary"]["route_retry_created_job_count"], 0)
        self.now += timedelta(seconds=8)
        self._settle()
        original = self.repo.get_progression_event(self.first["progression_event_id"])

        retried = self._sync()

        self.assertEqual(retried["summary"]["skipped_duplicate_count"], 1)
        self.assertEqual(retried["summary"]["route_retry_created_job_count"], 1)
        jobs = self._jobs()
        self.assertEqual(len(jobs), 2)
        retry_job = next(item for item in jobs if item["signal_id"] == signal["id"])
        event = self.repo.get_progression_event(retry_job["progression_event_id"])
        self.assertEqual(event["auto_trigger_rule_run_id"], original["auto_trigger_rule_run_id"])
        self.assertEqual(event["auto_trigger_runtime_run_id"], original["auto_trigger_runtime_run_id"])
        self.assertLessEqual(retry_job["expire_at"], retry_job["stake_plan"]["meta"]["issue_window"]["send_before"])
        for _ in range(3):
            self.assertEqual(self._sync()["summary"]["route_retry_created_job_count"], 0)
        self.assertEqual(len(self._jobs()), 2)

    def test_retry_does_not_depend_on_upstream_returning_the_same_signal(self):
        self._receive_while_waiting()
        self._settle()

        result = self._sync(fetcher=lambda *a, **k: {"items": []})

        self.assertEqual(result["summary"]["skipped_no_signal_count"], 1)
        self.assertEqual(result["summary"]["route_retry_created_job_count"], 1)
        self.assertEqual(len(self._jobs()), 2)

    def test_consecutive_issues_retry_after_each_previous_settlement(self):
        cycle_start = self.now
        previous = self.first
        for index, delay in enumerate((8, 4, 14, 14, 17)):
            self.now = cycle_start + timedelta(seconds=210 * index)
            issue_no = str(3479174 + index)
            self.draw_clock = {
                "latest_issue_no": str(int(issue_no) - 1),
                "latest_open_time": (self.now - timedelta(seconds=10)).isoformat(),
            }
            self.payload = {"items": [{
                "signal_id": "consensus-" + issue_no, "issue_no": issue_no, "lottery_type": "pc28",
                "published_at": self._iso_now(), "signals": [{"bet_type": "odd_even", "bet_value": "双"}],
            }]}
            with self.subTest(issue_no=issue_no, delay=delay):
                self.assertEqual(self._sync()["summary"]["created_job_count"], 0)
                self.now += timedelta(seconds=delay)
                self._settle(previous)
                self.assertEqual(self._sync()["summary"]["route_retry_created_job_count"], 1)
                previous = self._jobs()[0]
                self._delivered(previous)
                self.assertEqual(self._sync()["summary"]["route_retry_created_job_count"], 0)
        self.assertEqual(len(self._jobs()), 6)

    def test_retry_still_runs_if_upstream_request_fails(self):
        self._receive_while_waiting()
        self._settle()
        with patch("pc28touzhu.services.source_sync_service.fetch_source_to_raw_item", side_effect=RuntimeError("上游超时")):
            result = self._sync()
        self.assertEqual(result["summary"]["failed_count"], 1)
        self.assertEqual(result["summary"]["route_retry_created_job_count"], 1)
        self.assertEqual(len(self._jobs()), 2)

    def test_retry_rechecks_source_status_after_scan(self):
        signal = self._receive_while_waiting()
        self._settle()
        self.assertEqual(self._retry_ids(), [signal["id"]])
        with self.repo._connect() as conn:
            conn.execute("UPDATE signal_sources SET status='inactive' WHERE id=?", (self.source["id"],))

        result = dispatch_signal(self.repo, signal["id"], draw_clock=self.draw_clock, retry_active_routes_only=True)

        self.assertEqual(result["created_count"], 0)
        self.assertEqual(self._retry_ids(), [])
        self.assertEqual(len(self._jobs()), 1)

    def test_retry_does_not_fall_back_to_direct_dispatch_after_manual_stop(self):
        signal = self._receive_while_waiting()
        self._settle()
        stop_auto_trigger_rule_current_run(self.repo, rule_id=self.rule["id"], user_id=self.user_id)
        self.repo.update_subscription_status(subscription_id=self.sub["id"], user_id=self.user_id, status="active")
        self.assertEqual(self._retry_ids(), [])

        result = dispatch_signal(self.repo, signal["id"], draw_clock=self.draw_clock, retry_active_routes_only=True)

        self.assertEqual(result["created_count"], 0)
        self.assertEqual(self._sync()["summary"]["route_retry_created_job_count"], 0)
        self.assertEqual(len(self._jobs()), 1)

    def _assert_settlement_stops_retry(self, result):
        with self.repo._connect() as conn:
            conn.execute("UPDATE auto_trigger_rule_routes SET subscription_risk_mode='override', subscription_risk_control_json=? WHERE id=?", (
                '{"enabled":true,"profit_target":5,"loss_limit":5}', self.rule["routes"][0]["id"],
            ))
        self._receive_while_waiting()
        self._settle(result=result)
        self.assertEqual(self._retry_ids(), [])
        self.assertEqual(self._sync()["summary"]["route_retry_created_job_count"], 0)
        self.assertEqual(len(self._jobs()), 1)

    def test_retry_obeys_profit_target_reached_during_settlement(self):
        self._assert_settlement_stops_retry("hit")

    def test_retry_obeys_loss_limit_reached_during_settlement(self):
        self._assert_settlement_stops_retry("miss")

    def test_retry_rejects_missing_clock_or_closed_window(self):
        self._receive_while_waiting()
        self._settle()
        for clock in (
            None,
            {"latest_issue_no": "3479173"},
            {"latest_issue_no": "3479174"},
            {"latest_issue_no": "3479173", "latest_open_time": (self.now - timedelta(seconds=180)).isoformat()},
        ):
            with self.subTest(clock=clock):
                result = self._sync(draw_clock=clock)
                self.assertEqual(result["summary"]["route_retry_created_job_count"], 0)
                self.assertEqual(len(self._jobs()), 1)

    def test_each_route_retries_independently_when_one_target_already_received_signal(self):
        other_target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100100002", status="active",
        )
        other_rule = self._rule(other_target)
        other_first = self._start(other_rule, self.previous)
        self._delivered(other_first)
        self._settle()
        self.assertEqual(self._sync()["summary"]["created_job_count"], 1)
        signal = self.repo.list_signals(source_id=self.source["id"])[0]
        self.assertEqual(self._retry_ids(), [signal["id"]])
        self._settle(other_first)

        self.assertEqual(self._sync()["summary"]["route_retry_created_job_count"], 1)
        jobs = [item for item in self._jobs() if item["signal_id"] == signal["id"]]
        self.assertEqual(len(jobs), 2)
        self.assertEqual({item["delivery_target_id"] for item in jobs}, {self.target["id"], other_target["id"]})
        self.assertEqual(self._retry_ids(), [])

    def test_old_signal_cannot_be_retried_into_a_new_run(self):
        signal = self._receive_while_waiting()
        self._settle()
        stop_auto_trigger_rule_current_run(self.repo, rule_id=self.rule["id"], user_id=self.user_id)
        latest = self._signal("3479175")
        next_job = self._start(self._rule(self.target), latest)
        self._delivered(next_job)
        self._settle(next_job)

        result = dispatch_signal(self.repo, signal["id"], draw_clock=self.draw_clock, retry_active_routes_only=True)

        self.assertEqual(result["created_count"], 0)
        self.assertEqual(self._retry_ids(), [])
        self.assertEqual(len(self._jobs()), 2)

    def test_only_latest_signal_per_play_is_considered_for_retry(self):
        old = self._receive_while_waiting()
        latest = self._signal("3479175")

        self.assertEqual(self._retry_ids(), [latest["id"]])
        self.assertNotIn(old["id"], self._retry_ids())

    def test_concurrent_retries_create_one_job(self):
        signal = self._receive_while_waiting()
        self._settle()
        barrier = Barrier(2)

        def retry(_):
            repo = DatabaseRepository(self.db_path)
            barrier.wait(timeout=5)
            return dispatch_signal(repo, signal["id"], draw_clock=self.draw_clock, retry_active_routes_only=True)

        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(retry, range(2)))
        self.assertEqual(sum(item["created_count"] for item in results), 1)
        self.assertEqual(len(self._jobs()), 2)


if __name__ == "__main__":
    unittest.main()
