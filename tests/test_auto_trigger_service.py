from __future__ import annotations

import os
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.auto_trigger_service import (
    clear_auto_trigger_performance_cache,
    create_auto_trigger_rule,
    list_auto_trigger_rules,
    resume_auto_trigger_rule_day,
    run_auto_trigger_cycle,
    update_auto_trigger_rule,
)


class AutoTriggerServiceTests(unittest.TestCase):
    def setUp(self):
        clear_auto_trigger_performance_cache()
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()
        self.user_id = self.repo.create_user("auto-trigger-user")
        self.source = self.repo.create_source_record(
            owner_user_id=self.user_id,
            source_type="ai_trading_simulator_export",
            name="AI 方案 #5",
            status="active",
            config={
                "fetch": {
                    "url": "https://example.com/api/export/predictors/5/signals?view=execution",
                    "headers": {"Accept": "application/json"},
                    "timeout": 10,
                }
            },
        )
        self.subscription = self.repo.create_subscription_record(
            user_id=self.user_id,
            source_id=self.source["id"],
            status="active",
            strategy={
                "play_filter": {"mode": "selected", "selected_keys": ["big_small:大", "big_small:小"]},
                "staking_policy": {"mode": "fixed", "fixed_amount": 10},
            },
        )
        self.repo.create_delivery_target_record(
            user_id=self.user_id,
            executor_type="telegram_group",
            target_key="-100123456",
            target_name="测试群",
            status="active",
        )
        self.signal = self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418001",
            bet_type="big_small",
            bet_value="大",
            normalized_payload={"message_text": "大10"},
            published_at="2026-04-18T09:30:00Z",
        )

    def tearDown(self):
        clear_auto_trigger_performance_cache()
        self.tmpdir.cleanup()

    def _performance_payload(
        self,
        issue_no="20260418000",
        big_small_rate=38.0,
        big_small_recent_20_rate=None,
        combo_rate=19.0,
        odd_even_rate=50.0,
        odd_even_recent_20_rate=None,
        big_small_miss_streak=0,
        odd_even_miss_streak=0,
        combo_miss_streak=0,
    ):
        big_small_metric = {
            "label": "大小",
            "recent_100": {"hit_rate": big_small_rate, "sample_count": 100, "hit_count": int(big_small_rate)},
            "streaks": {"current_miss_streak": big_small_miss_streak},
        }
        if big_small_recent_20_rate is not None:
            big_small_metric["recent_20"] = {
                "hit_rate": big_small_recent_20_rate,
                "sample_count": 20,
                "hit_count": int(big_small_recent_20_rate * 20 / 100),
            }
        odd_even_metric = {
            "label": "单双",
            "recent_100": {"hit_rate": odd_even_rate, "sample_count": 100, "hit_count": int(odd_even_rate)},
            "streaks": {"current_miss_streak": odd_even_miss_streak},
        }
        if odd_even_recent_20_rate is not None:
            odd_even_metric["recent_20"] = {
                "hit_rate": odd_even_recent_20_rate,
                "sample_count": 20,
                "hit_count": int(odd_even_recent_20_rate * 20 / 100),
            }
        return {
            "schema_version": "1.0",
            "predictor_id": 5,
            "predictor_name": "AI 方案 #5",
            "lottery_type": "pc28",
            "latest_settled_issue": issue_no,
            "metrics": {
                "big_small": big_small_metric,
                "odd_even": odd_even_metric,
                "combo": {
                    "label": "组合",
                    "recent_100": {"hit_rate": combo_rate, "sample_count": 100, "hit_count": int(combo_rate)},
                    "streaks": {"current_miss_streak": combo_miss_streak},
                },
            },
        }

    def test_default_performance_fetcher_reuses_successful_cache(self):
        for name in ["规则 A", "规则 B"]:
            create_auto_trigger_rule(
                self.repo,
                user_id=self.user_id,
                payload={
                    "name": name,
                    "scope_mode": "selected_subscriptions",
                    "subscription_ids": [self.subscription["id"]],
                    "cooldown_issues": 0,
                    "conditions": [
                        {"metric": "big_small", "operator": "lt", "threshold": 1, "min_sample_count": 100}
                    ],
                    "action": {"dispatch_latest_signal": False},
                },
            )

        with patch(
            "pc28touzhu.services.auto_trigger_service._http_json_fetch",
            return_value=self._performance_payload(big_small_rate=50.0),
        ) as fetch:
            first = run_auto_trigger_cycle(self.repo, user_id=self.user_id)
            second = run_auto_trigger_cycle(self.repo, user_id=self.user_id)

        self.assertEqual(first["summary"]["checked_count"], 2)
        self.assertEqual(second["summary"]["checked_count"], 2)
        self.assertEqual(fetch.call_count, 1)
        self.assertIn("/api/export/predictors/5/performance", fetch.call_args[0][0])

    def test_default_performance_fetcher_retries_transient_failure(self):
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "临时失败重试",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "gt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )

        with patch("pc28touzhu.services.auto_trigger_service.time.sleep") as sleep:
            with patch(
                "pc28touzhu.services.auto_trigger_service._http_json_fetch",
                side_effect=[TimeoutError("timed out"), self._performance_payload(big_small_rate=50.0)],
            ) as fetch:
                result = run_auto_trigger_cycle(self.repo, user_id=self.user_id)

        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.assertEqual(fetch.call_count, 2)
        sleep.assert_called_once()
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["snapshot"]["performance_fetch"]["attempts"], 2)
        self.assertEqual(event["snapshot"]["performance_fetch"]["source"], "network")

    def test_default_performance_fetcher_uses_recent_success_when_fetch_fails(self):
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "最近成功兜底",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "gt", "threshold": 60, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )

        with patch(
            "pc28touzhu.services.auto_trigger_service._http_json_fetch",
            return_value=self._performance_payload(big_small_rate=50.0),
        ):
            with patch("pc28touzhu.services.auto_trigger_service.PERFORMANCE_CACHE_TTL_SECONDS", 0):
                first = run_auto_trigger_cycle(self.repo, user_id=self.user_id)
        rule = self.repo.list_auto_trigger_rules(user_id=self.user_id)[0]
        update_auto_trigger_rule(
            self.repo,
            rule_id=rule["id"],
            user_id=self.user_id,
            payload={
                **rule,
                "conditions": [
                    {"metric": "big_small", "operator": "gt", "threshold": 40, "min_sample_count": 100}
                ],
            },
        )
        with patch("pc28touzhu.services.auto_trigger_service.time.sleep"):
            with patch(
                "pc28touzhu.services.auto_trigger_service._http_json_fetch",
                side_effect=TimeoutError("timed out"),
            ) as fetch:
                second = run_auto_trigger_cycle(self.repo, user_id=self.user_id)

        self.assertEqual(first["summary"]["triggered_count"], 0)
        self.assertEqual(second["summary"]["triggered_count"], 1)
        self.assertEqual(fetch.call_count, 2)
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["snapshot"]["performance_fetch"]["source"], "stale_cache")
        self.assertTrue(event["snapshot"]["performance_fetch"]["stale"])
        self.assertEqual(event["snapshot"]["performance_fetch"]["error"], "timed out")

    def test_default_performance_fetcher_skips_network_during_failure_cooldown(self):
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "失败冷却",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "gt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )

        with patch("pc28touzhu.services.auto_trigger_service.time.sleep"):
            with patch(
                "pc28touzhu.services.auto_trigger_service._http_json_fetch",
                side_effect=TimeoutError("timed out"),
            ) as fetch:
                first = run_auto_trigger_cycle(self.repo, user_id=self.user_id)
                second = run_auto_trigger_cycle(self.repo, user_id=self.user_id)

        self.assertEqual(first["summary"]["failed_count"], 1)
        self.assertEqual(second["summary"]["failed_count"], 1)
        self.assertEqual(fetch.call_count, 2)
        failed_events = self.repo.list_auto_trigger_events(user_id=self.user_id, status="failed", limit=10)
        self.assertEqual(len(failed_events), 1)
        self.assertEqual(failed_events[0]["reason"], "timed out")

    def test_rule_restarts_subscription_and_dispatches_latest_signal(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "低命中自动跟单",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 10,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
            },
        )["item"]

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.assertEqual(result["rules"][0]["events"][0]["status"], "triggered")
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["subscription_id"], self.subscription["id"])
        updated_rule = self.repo.get_auto_trigger_rule(rule["id"])
        self.assertEqual(updated_rule["last_triggered_issue_no"], "20260418000")

    def test_hit_rate_condition_uses_configured_recent_window(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "近20期高命中自动跟单",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {
                        "metric": "odd_even",
                        "operator": "gt",
                        "threshold": 55,
                        "window_size": 20,
                        "min_sample_count": 20,
                    }
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(odd_even_rate=40.0, odd_even_recent_20_rate=60.0),
        )

        event = result["rules"][0]["events"][0]
        self.assertEqual(event["status"], "triggered")
        self.assertEqual(event["matched_conditions"][0]["window"], "recent_20")
        self.assertEqual(event["matched_conditions"][0]["window_size"], 20)
        self.assertEqual(event["matched_conditions"][0]["actual_hit_rate"], 60.0)

    def test_hit_rate_condition_does_not_fallback_to_recent_100_when_window_missing(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "缺少近20期不触发",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {
                        "metric": "odd_even",
                        "operator": "gt",
                        "threshold": 55,
                        "window_size": 20,
                        "min_sample_count": 20,
                    }
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(odd_even_rate=80.0),
        )

        self.assertEqual(result["summary"]["triggered_count"], 0)
        self.assertEqual(result["rules"][0]["events"], [])

    def test_route_trigger_dispatches_signal_for_matched_metric(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        target = self.repo.list_delivery_targets(self.user_id)[0]
        self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418002",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={"message_text": "小10"},
            published_at="2026-04-18T09:35:00Z",
        )
        self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418002",
            bet_type="combo",
            bet_value="小双",
            normalized_payload={"message_text": "小双10"},
            published_at="2026-04-18T09:35:00Z",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "匹配玩法发单",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True, "play_filter_action": "matched_metric"},
                "routes": [{"delivery_target_id": target["id"], "name": "测试路由"}],
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418001"),
        )

        self.assertEqual(result["summary"]["triggered_count"], 1)
        event = result["rules"][0]["events"][0]
        self.assertEqual(event["snapshot"]["dispatch_result"]["created_count"], 1)
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 1)
        signal = self.repo.get_signal(jobs[0]["signal_id"])
        self.assertEqual(signal["bet_type"], "big_small")
        self.assertEqual(jobs[0]["planned_message_text"], "小10")

    def test_daily_profit_target_stops_rule_and_blocks_following_dispatch(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "日止盈自动跟单",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "daily_risk_control": {"enabled": True, "profit_target": 9, "loss_limit": 0},
            },
        )["item"]

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        self.assertEqual(first["summary"]["triggered_count"], 1)
        job = self.repo.list_execution_jobs(user_id=self.user_id)[0]
        event = self.repo.get_progression_event(job["progression_event_id"])
        self.assertEqual(event["auto_trigger_rule_id"], rule["id"])
        self.assertTrue(event["auto_trigger_rule_run_id"])
        self.assertTrue(event["auto_trigger_stat_date"])

        settled = self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )
        self.assertTrue(settled["auto_trigger_daily_risk"]["stopped"])
        stat = self.repo.get_auto_trigger_rule_daily_stat(
            rule_id=rule["id"],
            user_id=self.user_id,
            stat_date=event["auto_trigger_stat_date"],
        )
        self.assertEqual(stat["status"], "stopped")
        self.assertEqual(stat["stopped_reason"], "profit_target_hit")

        second = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418002"),
        )
        self.assertEqual(second["summary"]["triggered_count"], 0)
        self.assertEqual(second["summary"]["skipped_count"], 1)
        self.assertEqual(second["rules"][0]["events"][0]["reason"], "daily_risk_stopped")

        next_signal = self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418003",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={"message_text": "小10"},
            published_at="2026-04-18T09:35:00Z",
        )
        dispatch_result = dispatch_signal(self.repo, next_signal["id"])
        self.assertEqual(dispatch_result["candidate_count"], 0)
        self.assertEqual(dispatch_result["skipped_count"], 0)
        self.assertEqual(dispatch_result["created_count"], 0)
        subscription = self.repo.get_subscription(self.subscription["id"])
        self.assertEqual(subscription["financial"]["threshold_status"], "profit_target_hit")
        self.assertEqual(subscription["financial"]["stopped_reason"], "自动触发达到日止盈阈值，当前轮次已停止")
        runtime_history = self.repo.list_subscription_runtime_runs(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            limit=5,
        )
        self.assertEqual(runtime_history[0]["status"], "closed")
        self.assertEqual(runtime_history[0]["end_reason"], "profit_target_hit")

    def test_update_profit_target_resumes_stopped_rule_day_when_threshold_allows(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "提高止盈继续触发",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "daily_risk_control": {"enabled": True, "profit_target": 9, "loss_limit": 0},
            },
        )["item"]

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        job = self.repo.list_execution_jobs(user_id=self.user_id)[0]
        event = self.repo.get_progression_event(job["progression_event_id"])
        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )
        self.repo.report_job_result(
            job_id=str(job["id"]),
            executor_id="test-executor",
            attempt_no=1,
            delivery_status="sent",
            remote_message_id="remote-1",
            executed_at="2026-04-18T09:40:00Z",
            raw_result={},
            error_message=None,
        )
        self.assertEqual(first["summary"]["triggered_count"], 1)

        updated = update_auto_trigger_rule(
            self.repo,
            rule_id=rule["id"],
            user_id=self.user_id,
            payload={
                **rule,
                "daily_risk_control": {"enabled": True, "profit_target": 40, "loss_limit": 0},
            },
        )

        self.assertTrue(updated["daily_risk_resume"]["resumed"])
        stat = self.repo.get_auto_trigger_rule_daily_stat(
            rule_id=rule["id"],
            user_id=self.user_id,
            stat_date=event["auto_trigger_stat_date"],
        )
        self.assertEqual(stat["status"], "active")
        self.assertEqual(stat["stopped_reason"], "")

        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418003",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={"message_text": "小10"},
            published_at="2026-04-18T09:35:00Z",
        )
        second = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418002"),
        )
        self.assertEqual(second["summary"]["triggered_count"], 1)

    def test_manual_resume_rejects_when_current_threshold_still_hit(self):
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "仍达阈值不可继续",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "daily_risk_control": {"enabled": True, "profit_target": 10, "loss_limit": 0},
            },
        )["item"]
        today = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        with self.repo._connect() as conn:
            self.repo.upsert_auto_trigger_rule_daily_stat(
                conn,
                rule_id=rule["id"],
                user_id=self.user_id,
                stat_date=today,
                profit_delta=10,
                loss_delta=0,
                net_delta=10,
                result_type="hit",
                updated_at="2026-04-18T09:00:00Z",
            )
        self.repo.stop_auto_trigger_rule_day(
            rule_id=rule["id"],
            user_id=self.user_id,
            stat_date=today,
            reason="profit_target_hit",
        )

        with self.assertRaises(ValueError):
            resume_auto_trigger_rule_day(self.repo, rule_id=rule["id"], user_id=self.user_id, stat_date=today)

    def test_auto_trigger_profit_uses_event_stat_date_when_settled_after_midnight(self):
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "跨天统计",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": False},
                "daily_risk_control": {"enabled": True, "profit_target": 100, "loss_limit": 0},
            },
        )["item"]
        run = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule["id"],
            user_id=self.user_id,
            subscription_id=self.subscription["id"],
            stat_date="2026-04-18",
            started_issue_no="20260418099",
        )
        event = self.repo.create_progression_event_record(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            signal_id=self.signal["id"],
            issue_no="20260418099",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            auto_trigger_rule_id=rule["id"],
            auto_trigger_rule_run_id=run["id"],
            auto_trigger_stat_date="2026-04-18",
            status="pending",
        )

        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )

        previous_day = self.repo.get_auto_trigger_rule_daily_stat(
            rule_id=rule["id"],
            user_id=self.user_id,
            stat_date="2026-04-18",
        )
        self.assertEqual(previous_day["settled_event_count"], 1)
        self.assertEqual(previous_day["net_profit"], 10)

    def test_list_auto_trigger_rules_supports_selected_stat_date(self):
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "今日盈亏展示",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )["item"]
        stat_date = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        run = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule["id"],
            user_id=self.user_id,
            subscription_id=self.subscription["id"],
            stat_date=stat_date,
            started_issue_no="20260418001",
        )
        event = self.repo.create_progression_event_record(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            signal_id=self.signal["id"],
            issue_no="20260418001",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            auto_trigger_rule_id=rule["id"],
            auto_trigger_rule_run_id=run["id"],
            auto_trigger_stat_date=stat_date,
            status="pending",
        )

        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )

        payload = list_auto_trigger_rules(self.repo, user_id=self.user_id, stat_date=stat_date)

        current = next(item for item in payload["items"] if item["id"] == rule["id"])
        self.assertEqual(current["stat_date"], stat_date)
        self.assertEqual(current["daily_stat"]["stat_date"], stat_date)
        self.assertEqual(current["daily_stat"]["settled_event_count"], 1)
        self.assertEqual(current["daily_stat"]["net_profit"], 10)

    def test_rule_activates_standby_subscription_when_conditions_match(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "待命自动跟单",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 10,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
            },
        )

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.assertEqual(self.repo.get_subscription(self.subscription["id"])["status"], "active")
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["subscription_id"], self.subscription["id"])

    def test_standby_subscription_does_not_join_normal_dispatch_candidates(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )

        candidates = self.repo.list_dispatch_candidates(self.signal["id"])

        self.assertEqual(candidates, [])

    def test_rule_triggers_when_metric_miss_streak_reaches_threshold(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "大小连挂触发",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"type": "miss_streak", "metric": "big_small", "operator": "gte", "threshold": 5}
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(big_small_rate=80.0, combo_rate=80.0, big_small_miss_streak=5),
        )

        self.assertEqual(result["summary"]["triggered_count"], 1)
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["status"], "triggered")
        self.assertEqual(event["matched_conditions"][0]["type"], "miss_streak")
        self.assertEqual(event["matched_conditions"][0]["actual_miss_streak"], 5)

    def test_guard_group_pairs_same_metric_before_cross_metric_conditions(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "同玩法附加条件优先",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "guard_groups": [
                    {
                        "conditions": [
                            {"type": "miss_streak", "metric": "big_small", "operator": "gte", "threshold": 5},
                            {"type": "miss_streak", "metric": "combo", "operator": "gte", "threshold": 5},
                        ]
                    }
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(big_small_rate=38.0, combo_miss_streak=5),
        )

        self.assertEqual(result["summary"]["triggered_count"], 0)
        self.assertEqual(self.repo.list_auto_trigger_events(user_id=self.user_id, limit=10), [])

    def test_guard_group_uses_cross_metric_when_same_metric_is_absent(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "跨玩法附加条件",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "guard_groups": [
                    {
                        "conditions": [
                            {"type": "miss_streak", "metric": "combo", "operator": "gte", "threshold": 5}
                        ]
                    }
                ],
                "action": {
                    "dispatch_latest_signal": False,
                    "play_filter_action": "matched_metric",
                },
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(big_small_rate=38.0, combo_miss_streak=5),
        )

        self.assertEqual(result["summary"]["triggered_count"], 1)
        updated = self.repo.get_subscription(self.subscription["id"])
        self.assertEqual(updated["strategy_v2"]["play_filter"]["selected_keys"], ["big_small:大", "big_small:小"])
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual([condition["metric"] for condition in event["matched_conditions"]], ["big_small", "combo"])
        self.assertEqual(event["snapshot"]["matched_primary_condition"]["metric"], "big_small")

    def test_all_guard_groups_must_pass(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "多个附加条件区",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "guard_groups": [
                    {"conditions": [{"type": "miss_streak", "metric": "big_small", "operator": "gte", "threshold": 5}]},
                    {"conditions": [{"type": "miss_streak", "metric": "combo", "operator": "gte", "threshold": 3}]},
                ],
                "action": {"dispatch_latest_signal": False},
            },
        )

        failed = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(big_small_rate=38.0, big_small_miss_streak=5, combo_miss_streak=2),
        )
        passed = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418001", big_small_rate=38.0, big_small_miss_streak=5, combo_miss_streak=3),
        )

        self.assertEqual(failed["summary"]["triggered_count"], 0)
        self.assertEqual(passed["summary"]["triggered_count"], 1)

    def test_first_successful_primary_path_controls_matched_metric_action(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "第一条成功路径",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100},
                    {"metric": "combo", "operator": "lt", "threshold": 20, "min_sample_count": 100},
                ],
                "guard_groups": [
                    {
                        "conditions": [
                            {"type": "miss_streak", "metric": "big_small", "operator": "gte", "threshold": 5},
                            {"type": "miss_streak", "metric": "combo", "operator": "gte", "threshold": 5},
                        ]
                    }
                ],
                "action": {
                    "dispatch_latest_signal": False,
                    "play_filter_action": "matched_metric",
                },
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(big_small_rate=38.0, combo_rate=19.0, big_small_miss_streak=0, combo_miss_streak=5),
        )

        self.assertEqual(result["summary"]["triggered_count"], 1)
        updated = self.repo.get_subscription(self.subscription["id"])
        self.assertEqual(
            updated["strategy_v2"]["play_filter"]["selected_keys"],
            ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
        )
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["snapshot"]["matched_primary_condition"]["metric"], "combo")

    def test_rule_skips_when_multiple_distinct_metrics_match_if_enabled(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "多指标保护",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100},
                    {"metric": "combo", "operator": "lt", "threshold": 20, "min_sample_count": 100},
                ],
                "action": {
                    "dispatch_latest_signal": False,
                    "skip_multiple_metrics_matched": True,
                },
            },
        )["item"]

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(result["summary"]["triggered_count"], 0)
        self.assertEqual(result["summary"]["skipped_count"], 1)
        self.assertEqual(self.repo.get_subscription(self.subscription["id"])["status"], "standby")
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])
        self.assertEqual(self.repo.get_auto_trigger_rule(rule["id"])["last_triggered_issue_no"], "")
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["status"], "skipped")
        self.assertEqual(event["reason"], "multiple_metrics_matched")
        self.assertEqual({condition["metric"] for condition in event["matched_conditions"]}, {"big_small", "combo"})

    def test_rule_does_not_skip_when_duplicate_conditions_match_same_metric(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "同指标重复条件",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100},
                    {"metric": "big_small", "operator": "lt", "threshold": 45, "min_sample_count": 100},
                ],
                "action": {
                    "dispatch_latest_signal": False,
                    "skip_multiple_metrics_matched": True,
                },
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(combo_rate=99.0),
        )

        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.assertEqual(result["summary"]["skipped_count"], 0)
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["status"], "triggered")
        self.assertEqual(event["reason"], "conditions_matched")
        self.assertEqual([condition["metric"] for condition in event["matched_conditions"]], ["big_small", "big_small"])

    def test_cooldown_prevents_repeated_trigger_for_same_subscription(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "组合低命中",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 10,
                "conditions": [
                    {"metric": "combo", "operator": "lt", "threshold": 20, "min_sample_count": 100}
                ],
            },
        )

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        second = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(first["summary"]["triggered_count"], 1)
        self.assertEqual(second["summary"]["triggered_count"], 0)
        self.assertEqual(second["summary"]["skipped_count"], 1)
        events = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=10)
        self.assertEqual(events[0]["reason"], "subscription_has_open_run")
        skipped_events = self.repo.list_auto_trigger_events(user_id=self.user_id, status="skipped", limit=10)
        self.assertEqual(len(skipped_events), 1)
        self.assertEqual(skipped_events[0]["reason"], "subscription_has_open_run")

    def test_repeated_skipped_event_is_deduplicated(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "跳过去重",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 10,
                "conditions": [
                    {"metric": "combo", "operator": "lt", "threshold": 20, "min_sample_count": 100}
                ],
            },
        )

        run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        second = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        third = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(second["summary"]["skipped_count"], 1)
        self.assertEqual(third["summary"]["skipped_count"], 1)
        skipped_events = self.repo.list_auto_trigger_events(user_id=self.user_id, status="skipped", limit=10)
        self.assertEqual(len(skipped_events), 1)
        self.assertEqual(skipped_events[0]["reason"], "subscription_has_open_run")

    def test_repeated_failed_event_is_deduplicated(self):
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "失败去重",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "combo", "operator": "lt", "threshold": 20, "min_sample_count": 100}
                ],
            },
        )

        def failing_fetcher(_url):
            raise RuntimeError("performance_unavailable")

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=failing_fetcher)
        second = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=failing_fetcher)

        self.assertEqual(first["summary"]["failed_count"], 1)
        self.assertEqual(second["summary"]["failed_count"], 1)
        failed_events = self.repo.list_auto_trigger_events(user_id=self.user_id, status="failed", limit=10)
        self.assertEqual(len(failed_events), 1)
        self.assertEqual(failed_events[0]["reason"], "performance_unavailable")

    def test_auto_trigger_cycle_prunes_expired_events(self):
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "记录保留",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "combo", "operator": "lt", "threshold": 1, "min_sample_count": 100}
                ],
            },
        )["item"]
        old_skipped = self.repo.record_auto_trigger_event(
            rule_id=rule["id"],
            user_id=self.user_id,
            subscription_id=self.subscription["id"],
            source_id=self.source["id"],
            status="skipped",
            reason="cooldown",
        )
        old_triggered = self.repo.record_auto_trigger_event(
            rule_id=rule["id"],
            user_id=self.user_id,
            subscription_id=self.subscription["id"],
            source_id=self.source["id"],
            status="triggered",
            reason="conditions_matched",
        )
        old_failed = self.repo.record_auto_trigger_event(
            rule_id=rule["id"],
            user_id=self.user_id,
            subscription_id=self.subscription["id"],
            source_id=self.source["id"],
            status="failed",
            reason="fetch_failed",
        )
        recent_skipped = self.repo.record_auto_trigger_event(
            rule_id=rule["id"],
            user_id=self.user_id,
            subscription_id=self.subscription["id"],
            source_id=self.source["id"],
            status="skipped",
            reason="source_not_active",
        )
        with self.repo._connect() as conn:
            for item in [old_skipped, old_triggered, old_failed]:
                conn.execute(
                    "UPDATE auto_trigger_events SET created_at = ? WHERE id = ?",
                    ("2000-01-01T00:00:00Z", int(item["id"])),
                )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(combo_rate=99.0, big_small_rate=99.0),
        )

        self.assertEqual(result["cleanup"]["deleted_count"], 3)
        events = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=20)
        event_ids = {item["id"] for item in events}
        self.assertNotIn(old_skipped["id"], event_ids)
        self.assertNotIn(old_triggered["id"], event_ids)
        self.assertNotIn(old_failed["id"], event_ids)
        self.assertIn(recent_skipped["id"], event_ids)

    def test_matched_metric_action_uses_condition_order_as_priority(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "按优先级切换玩法",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "combo", "operator": "lt", "threshold": 20, "min_sample_count": 100},
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100},
                ],
                "action": {
                    "dispatch_latest_signal": False,
                    "play_filter_action": "matched_metric",
                },
            },
        )

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(result["summary"]["triggered_count"], 1)
        updated = self.repo.get_subscription(self.subscription["id"])
        self.assertEqual(updated["strategy_v2"]["play_filter"]["mode"], "selected")
        self.assertEqual(
            updated["strategy_v2"]["play_filter"]["selected_keys"],
            ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
        )
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["snapshot"]["play_filter_result"]["target_metric"], "combo")

    def test_fixed_metric_action_switches_to_configured_metric(self):
        self.repo.update_subscription_status(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            status="standby",
        )
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "固定切到单双",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "combo", "operator": "lt", "threshold": 20, "min_sample_count": 100},
                ],
                "action": {
                    "dispatch_latest_signal": False,
                    "play_filter_action": "fixed_metric",
                    "fixed_metric": "odd_even",
                },
            },
        )

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(result["summary"]["triggered_count"], 1)
        updated = self.repo.get_subscription(self.subscription["id"])
        self.assertEqual(updated["strategy_v2"]["play_filter"]["selected_keys"], ["odd_even:单", "odd_even:双"])

    def test_active_subscription_without_open_run_can_trigger(self):
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "运行中空闲可触发",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
            },
        )

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.assertEqual(result["summary"]["skipped_count"], 0)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 1)
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["status"], "triggered")
        self.assertEqual(event["reason"], "conditions_matched")

    def test_running_round_below_threshold_is_not_restarted_by_rule(self):
        """活跃订阅当前轮次仍在跑（有 active runtime_run、未达止盈止损），即使命中条件也不应被自动开新一轮。"""
        # 模拟"轮次还在跑"：直接植入一条 active 的 subscription_runtime_runs，financial 中 threshold_status 留空
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_runtime_runs(
                    subscription_id, user_id, status, started_issue_no, started_at, start_reason,
                    ended_at, end_reason, last_issue_no, last_result_type,
                    realized_profit, realized_loss, net_profit,
                    settled_event_count, hit_count, miss_count, refund_count,
                    baseline_reset_at, baseline_reset_note, created_at, updated_at
                ) VALUES (?, ?, 'active', ?, ?, 'auto_started', NULL, '', ?, 'miss', 0, 5, -5, 5, 2, 3, 0, NULL, '', ?, ?)
                """,
                (
                    int(self.subscription["id"]),
                    int(self.user_id),
                    "20260418000",
                    "2026-04-18T08:00:00Z",
                    "20260418004",
                    "2026-04-18T08:00:00Z",
                    "2026-04-18T09:00:00Z",
                ),
            )
            conn.execute(
                """
                INSERT INTO subscription_financial_state(
                    subscription_id, user_id, realized_profit, realized_loss, net_profit,
                    threshold_status, stopped_reason, baseline_reset_at, baseline_reset_note,
                    last_settled_event_id, last_settled_at, updated_at
                ) VALUES (?, ?, 0, 5, -5, '', '', NULL, '', NULL, ?, ?)
                """,
                (
                    int(self.subscription["id"]),
                    int(self.user_id),
                    "2026-04-18T09:00:00Z",
                    "2026-04-18T09:00:00Z",
                ),
            )

        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "未达阈值不应打断在跑轮次",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
            },
        )

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(),
        )

        # 应被 subscription_has_open_run 拦截，轮次不被打断、信号不下发
        self.assertEqual(result["summary"]["triggered_count"], 0)
        self.assertEqual(result["summary"]["skipped_count"], 1)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 0)
        skipped_events = self.repo.list_auto_trigger_events(user_id=self.user_id, status="skipped", limit=10)
        self.assertEqual(len(skipped_events), 1)
        self.assertEqual(skipped_events[0]["reason"], "subscription_has_open_run")

        # 当前 active runtime_run 应保持不动
        runtime_history = self.repo.list_subscription_runtime_runs(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            limit=5,
        )
        self.assertEqual(runtime_history[0]["status"], "active")

    def test_rule_routes_dispatch_independent_jobs_and_settlement_rules(self):
        second_target = self.repo.create_delivery_target_record(
            user_id=self.user_id,
            executor_type="telegram_group",
            target_key="-100654321",
            target_name="正式群",
            status="active",
        )
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "多路由自动投注",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "daily_risk_control": {"enabled": True, "profit_target": 20, "loss_limit": 0},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "测试群路由",
                        "risk_mode": "inherit",
                        "settlement_mode": "override",
                        "settlement_policy": {"settlement_rule_id": "pc28_netdisk_regular"},
                    },
                    {
                        "delivery_target_id": second_target["id"],
                        "name": "正式群路由",
                        "risk_mode": "inherit",
                        "settlement_mode": "override",
                        "settlement_policy": {"settlement_rule_id": "pc28_high_regular"},
                    },
                ],
            },
        )["item"]

        result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(result["summary"]["triggered_count"], 1)
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 2)
        route_ids = {route["id"] for route in rule["routes"]}
        self.assertEqual({job["auto_trigger_route_id"] for job in jobs}, route_ids)
        events = [self.repo.get_progression_event(job["progression_event_id"]) for job in jobs]
        self.assertEqual({event["auto_trigger_route_id"] for event in events}, route_ids)
        self.assertEqual(
            {event["settlement_rule_id"] for event in events},
            {"pc28_netdisk_regular", "pc28_high_regular"},
        )
        for event in events:
            self.assertEqual(event["auto_trigger_rule_id"], rule["id"])
            self.assertTrue(event["auto_trigger_rule_run_id"])
            self.assertTrue(event["auto_trigger_stat_date"])

    def test_route_dispatch_allows_signal_matching_performance_issue(self):
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "同期期号路由自动投注",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "测试群路由",
                        "risk_mode": "inherit",
                    },
                ],
            },
        )["item"]

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418001"),
        )

        self.assertEqual(result["summary"]["triggered_count"], 1)
        event = result["rules"][0]["events"][0]
        self.assertEqual(event["snapshot"]["dispatch_result"]["created_count"], 1)
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["auto_trigger_route_id"], rule["routes"][0]["id"])
        progression_event = self.repo.get_progression_event(jobs[0]["progression_event_id"])
        self.assertEqual(progression_event["auto_trigger_route_id"], rule["routes"][0]["id"])

    def test_route_open_run_skip_uses_specific_reason_and_deduplicates(self):
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "路由在跑时跳过",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "测试群路由",
                        "risk_mode": "inherit",
                    },
                ],
            },
        )

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        second = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        third = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())

        self.assertEqual(first["summary"]["triggered_count"], 1)
        self.assertEqual(second["summary"]["skipped_count"], 1)
        self.assertEqual(third["summary"]["skipped_count"], 1)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 1)
        skipped_events = self.repo.list_auto_trigger_events(user_id=self.user_id, status="skipped", limit=10)
        self.assertEqual(len(skipped_events), 1)
        self.assertEqual(skipped_events[0]["reason"], "route_has_open_run")
        skipped_routes = skipped_events[0]["snapshot"]["play_filter_result"]["skipped_routes"]
        self.assertEqual(skipped_routes[0]["reason"], "route_has_open_run")

    def test_active_route_continues_when_global_subscription_threshold_is_stopped(self):
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "路由续单不受全局订阅旧风控影响",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "测试群路由",
                        "risk_mode": "inherit",
                    },
                ],
            },
        )["item"]

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        self.assertEqual(first["summary"]["triggered_count"], 1)
        route_id = rule["routes"][0]["id"]
        first_job = self.repo.list_execution_jobs(user_id=self.user_id)[0]
        first_event = self.repo.get_progression_event(first_job["progression_event_id"])
        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="refund",
            progression_event_id=first_event["id"],
        )
        self.repo.report_job_result(
            job_id=str(first_job["id"]),
            executor_id="test-executor",
            attempt_no=1,
            delivery_status="sent",
            remote_message_id="remote-route-1",
            executed_at="2026-04-18T09:32:00Z",
            raw_result={},
            error_message=None,
        )
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_financial_state(
                    subscription_id, user_id, realized_profit, realized_loss, net_profit,
                    threshold_status, stopped_reason, baseline_reset_at, baseline_reset_note,
                    last_settled_event_id, last_settled_at, updated_at
                ) VALUES (?, ?, 0, 10, -10, 'loss_limit_hit', '旧订阅止损状态', NULL, '', NULL, ?, ?)
                ON CONFLICT(subscription_id) DO UPDATE SET
                    threshold_status = excluded.threshold_status,
                    stopped_reason = excluded.stopped_reason,
                    updated_at = excluded.updated_at
                """,
                (self.subscription["id"], self.user_id, "2026-04-18T09:33:00Z", "2026-04-18T09:33:00Z"),
            )

        next_signal = self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418002",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={"message_text": "小10"},
            published_at="2026-04-18T09:35:00Z",
        )
        second = dispatch_signal(self.repo, next_signal["id"])

        self.assertEqual(second["created_count"], 1)
        self.assertEqual(second["jobs"][0]["auto_trigger_route_id"], route_id)
        second_event = self.repo.get_progression_event(second["jobs"][0]["progression_event_id"])
        self.assertEqual(second_event["auto_trigger_route_id"], route_id)
        self.assertEqual(self.repo.get_subscription_financial_state(self.subscription["id"])["threshold_status"], "loss_limit_hit")

    def test_route_runtime_play_filter_uses_matched_metric_strategy(self):
        current_subscription = self.repo.get_subscription(self.subscription["id"])
        strategy = dict(current_subscription["strategy_v2"])
        strategy["play_filter"] = {
            "mode": "selected",
            "selected_keys": ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
        }
        self.repo.update_subscription_record(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            source_id=self.subscription["source_id"],
            strategy=strategy,
        )
        self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418002",
            bet_type="odd_even",
            bet_value="单",
            normalized_payload={"message_text": "单10"},
            published_at="2026-04-18T09:35:00Z",
        )
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "命中玩法写入路由运行态",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "odd_even", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True, "play_filter_action": "matched_metric"},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "测试群路由",
                        "risk_mode": "inherit",
                    },
                ],
            },
        )["item"]

        result = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(
                big_small_rate=50,
                odd_even_rate=38,
                combo_rate=50,
            ),
        )

        self.assertEqual(result["summary"]["triggered_count"], 1)
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["auto_trigger_route_id"], rule["routes"][0]["id"])
        signal = self.repo.get_signal(jobs[0]["signal_id"])
        self.assertEqual(signal["bet_type"], "odd_even")
        with self.repo._connect() as conn:
            row = conn.execute(
                """
                SELECT play_filter_json
                FROM auto_trigger_route_subscription_runtime_runs
                WHERE route_id = ? AND subscription_id = ? AND status = 'active'
                ORDER BY id DESC
                LIMIT 1
                """,
                (rule["routes"][0]["id"], self.subscription["id"]),
            ).fetchone()
        self.assertIsNotNone(row)
        self.assertEqual(
            json.loads(row["play_filter_json"]),
            {"mode": "selected", "selected_keys": ["odd_even:单", "odd_even:双"]},
        )

    def test_route_profit_target_stops_only_that_route(self):
        second_target = self.repo.create_delivery_target_record(
            user_id=self.user_id,
            executor_type="telegram_group",
            target_key="-100654322",
            target_name="正式群 B",
            status="active",
        )
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "路由独立止盈",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "daily_risk_control": {"enabled": True, "profit_target": 9, "loss_limit": 0},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "A 路由",
                        "route_risk_mode": "inherit_rule",
                        "subscription_risk_mode": "disabled",
                    },
                    {
                        "delivery_target_id": second_target["id"],
                        "name": "B 路由",
                        "route_risk_mode": "inherit_rule",
                        "subscription_risk_mode": "disabled",
                    },
                ],
            },
        )["item"]

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        self.assertEqual(first["summary"]["triggered_count"], 1)
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 2)
        route_a_id = rule["routes"][0]["id"]
        route_b_id = rule["routes"][1]["id"]
        job_a = next(job for job in jobs if job["auto_trigger_route_id"] == route_a_id)
        event_a = self.repo.get_progression_event(job_a["progression_event_id"])

        settled = self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="hit",
            progression_event_id=event_a["id"],
        )

        self.assertTrue(settled["auto_trigger_daily_risk"]["stopped"])
        self.assertEqual(settled["auto_trigger_daily_risk"]["scope"], "route")
        stat_a = self.repo.get_auto_trigger_route_daily_stat(
            route_id=route_a_id,
            user_id=self.user_id,
            stat_date=event_a["auto_trigger_stat_date"],
        )
        stat_b = self.repo.get_auto_trigger_route_daily_stat(
            route_id=route_b_id,
            user_id=self.user_id,
            stat_date=event_a["auto_trigger_stat_date"],
        )
        self.assertEqual(stat_a["status"], "stopped")
        self.assertEqual(stat_a["stopped_reason"], "profit_target_hit")
        self.assertEqual(stat_b["status"], "active")
        subscription = self.repo.get_subscription(self.subscription["id"])
        self.assertEqual(subscription["financial"]["threshold_status"], "")
        job_b = next(job for job in jobs if job["auto_trigger_route_id"] == route_b_id)
        event_b = self.repo.get_progression_event(job_b["progression_event_id"])
        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="miss",
            progression_event_id=event_b["id"],
        )
        self.repo.report_job_result(
            job_id=str(job_b["id"]),
            executor_id="test-executor",
            attempt_no=1,
            delivery_status="sent",
            remote_message_id="remote-b-1",
            executed_at="2026-04-18T09:40:00Z",
            raw_result={},
            error_message=None,
        )

        self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418003",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={"message_text": "小10"},
            published_at="2026-04-18T09:35:00Z",
        )
        second = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418002"),
        )

        self.assertEqual(second["summary"]["triggered_count"], 1)
        all_jobs = self.repo.list_execution_jobs(user_id=self.user_id, limit=10)
        route_b_jobs = [job for job in all_jobs if job["auto_trigger_route_id"] == route_b_id]
        route_a_jobs = [job for job in all_jobs if job["auto_trigger_route_id"] == route_a_id]
        self.assertEqual(len(route_b_jobs), 2)
        self.assertEqual(len(route_a_jobs), 1)

    def test_route_subscription_risk_stops_only_that_route_subscription(self):
        current_subscription = self.repo.get_subscription(self.subscription["id"])
        strategy = dict(current_subscription["strategy_v2"])
        strategy["risk_control"] = {"enabled": True, "profit_target": 0, "loss_limit": 10}
        self.repo.update_subscription_record(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            source_id=self.subscription["source_id"],
            strategy=strategy,
        )
        second_target = self.repo.create_delivery_target_record(
            user_id=self.user_id,
            executor_type="telegram_group",
            target_key="-100654323",
            target_name="正式群 C",
            status="active",
        )
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "路由方案风控",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "daily_risk_control": {"enabled": True, "profit_target": 20, "loss_limit": 20},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "A 路由",
                        "route_risk_mode": "inherit_rule",
                        "subscription_risk_mode": "inherit_subscription",
                    },
                    {
                        "delivery_target_id": second_target["id"],
                        "name": "B 路由",
                        "route_risk_mode": "disabled",
                        "subscription_risk_mode": "disabled",
                    },
                ],
            },
        )["item"]

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        self.assertEqual(first["summary"]["triggered_count"], 1)
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 2)
        route_a_id = rule["routes"][0]["id"]
        route_b_id = rule["routes"][1]["id"]
        event_a = self.repo.get_progression_event(
            next(job for job in jobs if job["auto_trigger_route_id"] == route_a_id)["progression_event_id"]
        )

        settled = self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="miss",
            progression_event_id=event_a["id"],
        )

        self.assertTrue(settled["auto_trigger_daily_risk"]["stopped"])
        self.assertEqual(settled["auto_trigger_daily_risk"]["reason"], "loss_limit_hit")
        self.assertEqual(settled["auto_trigger_daily_risk"]["subscription_risk_mode"], "inherit_subscription")
        route_financial = self.repo.get_auto_trigger_route_subscription_financial_state(
            route_id=route_a_id,
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
        )
        self.assertEqual(route_financial["threshold_status"], "loss_limit_hit")
        self.assertEqual(route_financial["net_profit"], -10.0)
        subscription = self.repo.get_subscription(self.subscription["id"])
        self.assertEqual(subscription["financial"]["threshold_status"], "")
        rule_stat = self.repo.get_auto_trigger_rule_daily_stat(
            rule_id=rule["id"],
            user_id=self.user_id,
            stat_date=event_a["auto_trigger_stat_date"],
        )
        self.assertEqual(rule_stat["status"], "active")
        route_stat = self.repo.get_auto_trigger_route_daily_stat(
            route_id=route_a_id,
            user_id=self.user_id,
            stat_date=event_a["auto_trigger_stat_date"],
        )
        self.assertEqual(route_stat["status"], "active")
        job_b = next(job for job in jobs if job["auto_trigger_route_id"] == route_b_id)
        event_b = self.repo.get_progression_event(job_b["progression_event_id"])
        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="refund",
            progression_event_id=event_b["id"],
        )
        self.repo.report_job_result(
            job_id=str(job_b["id"]),
            executor_id="test-executor",
            attempt_no=1,
            delivery_status="sent",
            remote_message_id="remote-b-1",
            executed_at="2026-04-18T09:40:00Z",
            raw_result={},
            error_message=None,
        )

        self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418003",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={"message_text": "小10"},
            published_at="2026-04-18T09:35:00Z",
        )
        second = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418002"),
        )

        self.assertEqual(second["summary"]["triggered_count"], 1)
        all_jobs = self.repo.list_execution_jobs(user_id=self.user_id, limit=10)
        route_b_jobs = [job for job in all_jobs if job["auto_trigger_route_id"] == route_b_id]
        route_a_jobs = [job for job in all_jobs if job["auto_trigger_route_id"] == route_a_id]
        self.assertEqual(len(route_b_jobs), 2)
        self.assertEqual(len(route_a_jobs), 1)


    def test_route_runtime_run_blocks_early_restart_until_threshold_hit(self):
        # 跨天延续保护：路由级轮次仍在 active 且未达止盈/止损时，
        # 新一次自动触发不应提前 reset 旧轮、开新一轮（修复"净盈亏为正却被 auto_trigger_restart 打断"）。
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "未达阈值禁止提前重开",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "daily_risk_control": {"enabled": True, "profit_target": 20, "loss_limit": 20},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "测试群路由",
                        "risk_mode": "inherit",
                    },
                ],
            },
        )["item"]
        route_id = rule["routes"][0]["id"]

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        self.assertEqual(first["summary"]["triggered_count"], 1)
        first_job = self.repo.list_execution_jobs(user_id=self.user_id)[0]
        first_event = self.repo.get_progression_event(first_job["progression_event_id"])
        # 命中结算：净盈亏为正，远未到止盈 20，旧轮必须保持 active。
        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="hit",
            progression_event_id=first_event["id"],
        )
        self.repo.report_job_result(
            job_id=str(first_job["id"]),
            executor_id="test-executor",
            attempt_no=1,
            delivery_status="sent",
            remote_message_id="remote-route-keep",
            executed_at="2026-04-18T09:32:00Z",
            raw_result={},
            error_message=None,
        )
        route_financial = self.repo.get_auto_trigger_route_subscription_financial_state(
            route_id=route_id,
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
        )
        self.assertEqual(route_financial["threshold_status"], "")
        self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418003",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={"message_text": "小10"},
            published_at="2026-04-18T09:35:00Z",
        )
        # 第二次触发条件仍满足，旧轮未达阈值 -> 不开新轮（reset 被跨天延续保护挡住），
        # 但允许复用旧轮继续派单（triggered=1、不新增 runtime_run）。
        second = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418002"),
        )
        self.assertEqual(second["summary"]["triggered_count"], 1)
        # 路由级 runtime_run 仍只有一条，且保持 active（未被 auto_trigger_restart 提前关闭）。
        runs = self.repo.list_auto_trigger_route_subscription_runtime_runs(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            limit=5,
        )
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["status"], "active")
        self.assertEqual(runs[0]["end_reason"], "")
        # 财务未被清零，旧轮的净盈亏延续保留。
        route_financial_after = self.repo.get_auto_trigger_route_subscription_financial_state(
            route_id=route_id,
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
        )
        self.assertEqual(route_financial_after["threshold_status"], "")
        self.assertGreater(route_financial_after["net_profit"], 0)

    def test_active_route_cycle_keeps_started_stat_date_after_midnight(self):
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "跨日轮次归属日",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "daily_risk_control": {
                    "enabled": True,
                    "profit_target": 20,
                    "loss_limit": 20,
                    "timezone": "Asia/Shanghai",
                },
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "测试群路由",
                        "route_risk_mode": "inherit_rule",
                        "subscription_risk_mode": "inherit_subscription",
                    },
                ],
            },
        )["item"]
        route_id = rule["routes"][0]["id"]

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        self.assertEqual(first["summary"]["triggered_count"], 1)
        first_job = self.repo.list_execution_jobs(user_id=self.user_id)[0]
        first_event = self.repo.get_progression_event(first_job["progression_event_id"])
        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="refund",
            progression_event_id=first_event["id"],
        )
        self.repo.report_job_result(
            job_id=str(first_job["id"]),
            executor_id="test-executor",
            attempt_no=1,
            delivery_status="sent",
            remote_message_id="remote-cross-day-1",
            executed_at="2026-06-27T15:35:00Z",
            raw_result={},
            error_message=None,
        )
        with self.repo._connect() as conn:
            conn.execute(
                """
                UPDATE auto_trigger_route_subscription_runtime_runs
                SET started_at = ?, created_at = ?, updated_at = ?
                WHERE route_id = ? AND subscription_id = ? AND user_id = ? AND status = 'active'
                """,
                (
                    "2026-06-27T15:27:52Z",
                    "2026-06-27T15:27:52Z",
                    "2026-06-27T15:35:00Z",
                    route_id,
                    self.subscription["id"],
                    self.user_id,
                ),
            )

        second_signal = self.repo.create_signal_record(
            source_id=self.source["id"],
            lottery_type="pc28",
            issue_no="20260418002",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={"message_text": "小10"},
            published_at="2026-06-28T00:05:00Z",
        )
        second = run_auto_trigger_cycle(
            self.repo,
            user_id=self.user_id,
            fetcher=lambda url: self._performance_payload(issue_no="20260418001"),
        )

        self.assertEqual(second["summary"]["triggered_count"], 1)
        second_event = self.repo.get_progression_event_by_signal(
            subscription_id=self.subscription["id"],
            signal_id=second_signal["id"],
            auto_trigger_route_id=route_id,
        )
        self.assertIsNotNone(second_event)
        self.assertEqual(second_event["auto_trigger_stat_date"], "2026-06-27")
        rule_run = self.repo.get_auto_trigger_rule_run(second_event["auto_trigger_rule_run_id"])
        self.assertEqual(rule_run["stat_date"], "2026-06-27")

        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="miss",
            progression_event_id=second_event["id"],
        )
        stat = self.repo.get_auto_trigger_rule_daily_stat(
            rule_id=rule["id"],
            user_id=self.user_id,
            stat_date="2026-06-27",
        )
        self.assertEqual(stat["settled_event_count"], 1)
        self.assertEqual(stat["loss_amount"], 10.0)

    def test_route_runtime_history_lists_route_scoped_runs(self):
        # 方案挂了路由时，轮次历史应读取路由级 runtime_run，并带上 route_name / scope=route。
        first_target = self.repo.list_delivery_targets(self.user_id)[0]
        rule = create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "轮次历史读取路由级",
                "scope_mode": "selected_subscriptions",
                "subscription_ids": [self.subscription["id"]],
                "cooldown_issues": 0,
                "conditions": [
                    {"metric": "big_small", "operator": "lt", "threshold": 40, "min_sample_count": 100}
                ],
                "action": {"dispatch_latest_signal": True},
                "daily_risk_control": {"enabled": True, "profit_target": 9, "loss_limit": 0},
                "routes": [
                    {
                        "delivery_target_id": first_target["id"],
                        "name": "测试群路由",
                        "risk_mode": "inherit",
                    },
                ],
            },
        )["item"]
        route_id = rule["routes"][0]["id"]

        first = run_auto_trigger_cycle(self.repo, user_id=self.user_id, fetcher=lambda url: self._performance_payload())
        self.assertEqual(first["summary"]["triggered_count"], 1)
        job = self.repo.list_execution_jobs(user_id=self.user_id)[0]
        event = self.repo.get_progression_event(job["progression_event_id"])
        self.repo.settle_progression_event(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            result_type="hit",
            progression_event_id=event["id"],
        )

        runs = self.repo.list_auto_trigger_route_subscription_runtime_runs(
            subscription_id=self.subscription["id"],
            user_id=self.user_id,
            limit=5,
        )
        self.assertEqual(len(runs), 1)
        self.assertEqual(runs[0]["scope"], "route")
        self.assertEqual(runs[0]["route_id"], route_id)
        self.assertEqual(runs[0]["route_name"], "测试群路由")
        self.assertEqual(runs[0]["status"], "closed")
        self.assertEqual(runs[0]["end_reason"], "profit_target_hit")


if __name__ == "__main__":
    unittest.main()
