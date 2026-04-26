from __future__ import annotations

import os
import tempfile
import unittest

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.auto_trigger_service import create_auto_trigger_rule, run_auto_trigger_cycle


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

    def _performance_payload(self, issue_no="20260418000", big_small_rate=38.0, combo_rate=19.0):
        return {
            "schema_version": "1.0",
            "predictor_id": 5,
            "predictor_name": "AI 方案 #5",
            "lottery_type": "pc28",
            "latest_settled_issue": issue_no,
            "metrics": {
                "big_small": {"label": "大小", "recent_100": {"hit_rate": big_small_rate, "sample_count": 100, "hit_count": int(big_small_rate)}},
                "odd_even": {"label": "单双", "recent_100": {"hit_rate": 50.0, "sample_count": 100, "hit_count": 50}},
                "combo": {"label": "组合", "recent_100": {"hit_rate": combo_rate, "sample_count": 100, "hit_count": int(combo_rate)}},
            },
        }

    def test_rule_restarts_subscription_and_dispatches_latest_signal(self):
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

    def test_cooldown_prevents_repeated_trigger_for_same_subscription(self):
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


if __name__ == "__main__":
    unittest.main()
