from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.auto_trigger_service import (
    create_auto_trigger_rule,
    list_auto_trigger_rules,
    resume_auto_trigger_rule_day,
    run_auto_trigger_cycle,
    update_auto_trigger_rule,
)


class AutoTriggerServiceTests(unittest.TestCase):
    def setUp(self):
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
        self.tmpdir.cleanup()

    def _performance_payload(
        self,
        issue_no="20260418000",
        big_small_rate=38.0,
        combo_rate=19.0,
        big_small_miss_streak=0,
        odd_even_miss_streak=0,
        combo_miss_streak=0,
    ):
        return {
            "schema_version": "1.0",
            "predictor_id": 5,
            "predictor_name": "AI 方案 #5",
            "lottery_type": "pc28",
            "latest_settled_issue": issue_no,
            "metrics": {
                "big_small": {
                    "label": "大小",
                    "recent_100": {"hit_rate": big_small_rate, "sample_count": 100, "hit_count": int(big_small_rate)},
                    "streaks": {"current_miss_streak": big_small_miss_streak},
                },
                "odd_even": {
                    "label": "单双",
                    "recent_100": {"hit_rate": 50.0, "sample_count": 100, "hit_count": 50},
                    "streaks": {"current_miss_streak": odd_even_miss_streak},
                },
                "combo": {
                    "label": "组合",
                    "recent_100": {"hit_rate": combo_rate, "sample_count": 100, "hit_count": int(combo_rate)},
                    "streaks": {"current_miss_streak": combo_miss_streak},
                },
            },
        }

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
        self.assertEqual(dispatch_result["candidate_count"], 1)
        self.assertEqual(dispatch_result["skipped_count"], 1)
        self.assertEqual(dispatch_result["created_count"], 0)

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

    def test_active_subscription_without_threshold_status_is_skipped(self):
        create_auto_trigger_rule(
            self.repo,
            user_id=self.user_id,
            payload={
                "name": "运行中不强开新轮",
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

        self.assertEqual(result["summary"]["triggered_count"], 0)
        self.assertEqual(result["summary"]["skipped_count"], 1)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["status"], "skipped")
        self.assertEqual(event["reason"], "subscription_not_ready_for_restart")


if __name__ == "__main__":
    unittest.main()
