from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pc28touzhu.domain.draw_streak import build_draw_streak_snapshot
from pc28touzhu.domain.prediction_streak import build_prediction_direction_streaks
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.auto_trigger_service import (
    _normalize_conditions, clear_auto_trigger_performance_cache, create_auto_trigger_rule, run_auto_trigger_cycle,
)
from pc28touzhu.services.platform_service import resolve_subscription_progression
from pc28touzhu.services.telegram_bot_service import handle_telegram_command


class GroupTriggerTests(unittest.TestCase):
    def setUp(self):
        clear_auto_trigger_performance_cache()
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = DatabaseRepository(str(Path(self.tmp.name) / "trigger.db"))
        self.repo.initialize_database()
        self.user = self.repo.create_user("group-trigger-user")
        self.source = self.repo.create_source_record(owner_user_id=self.user, source_type="ai_trading_simulator_export", name="来源", config={"fetch": {"url": "https://example.com/api/export/predictors/5/signals"}})
        self.sub = self.repo.create_subscription_record(user_id=self.user, source_id=self.source["id"], status="standby", strategy={
            "play_filter": {"mode": "all"}, "staking_policy": {"mode": "martingale", "base_stake": 10, "multiplier": 2, "max_steps": 5},
            "settlement_policy": {"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_fullpay_2_0_regular"},
        })
        self.target = self.repo.create_delivery_target_record(user_id=self.user, executor_type="telegram_group", target_key="-1001", status="active")

    def test_conditions_accept_group_metrics_and_canonical_directions(self):
        conditions = _normalize_conditions([
            {"type": "hit_rate", "metric": "kill_group", "operator": "gte", "threshold": 75, "window_size": 20},
            {"type": "hit_streak", "metric": "double_group", "prediction_value": "小双+大单", "operator": "gte", "threshold": 3},
        ])
        self.assertEqual(conditions[1]["prediction_value"], "大单+小双")

    def test_draw_streak_counts_coverage_and_exclusion_without_single_combo_assumption(self):
        # 最新三期都不是小双；其中最后两期属于大单/大双双组。
        draws = [{"issue_no": str(100 - i), "result_number": number} for i, number in enumerate((15, 16, 11, 12))]
        snapshot = build_draw_streak_snapshot(draws, latest_issue_no="100")
        self.assertEqual(snapshot["metrics"]["kill_group"]["小双"]["count"], 3)
        self.assertEqual(snapshot["metrics"]["double_group"]["大单+大双"]["count"], 2)
        self.assertEqual(snapshot["absent_metrics"]["kill_group"]["小双"]["count"], 0)

    def test_historical_combo_signals_supply_derived_direction_but_explicit_wins(self):
        rows = [{"issue_no": str(issue), "bet_type": "combo", "bet_value": "大单"} for issue in (100, 99, 98)]
        metrics = {"kill_group": {"streaks": {"current_hit_streak": 3}}}
        result = build_prediction_direction_streaks(rows, latest_issue_no="100", metrics=metrics)
        self.assertEqual(result["kill_group"]["小双"]["hit_streak"]["count"], 3)
        rows.append({"issue_no": "99", "bet_type": "kill_group", "bet_value": "大双"})
        result = build_prediction_direction_streaks(rows, latest_issue_no="100", metrics=metrics)
        self.assertEqual(result["kill_group"]["小双"]["hit_streak"]["count"], 1)

    def test_fixed_group_route_dispatches_and_settles_once(self):
        for metric, value in (("kill_group", "小双"), ("combo", "大单")):
            self.repo.create_signal_record(source_id=self.source["id"], lottery_type="pc28", issue_no="1002", bet_type=metric, bet_value=value)
        rule = create_auto_trigger_rule(self.repo, user_id=self.user, payload={
            "name": "杀组路由", "scope_mode": "selected_subscriptions", "subscription_ids": [self.sub["id"]], "cooldown_issues": 0,
            "conditions": [{"type": "hit_rate", "metric": "kill_group", "operator": "gte", "threshold": 60, "min_sample_count": 20, "window_size": 20}],
            "action": {"dispatch_latest_signal": True, "play_filter_action": "keep"},
            "routes": [{"delivery_target_id": self.target["id"], "name": "杀组", "play_filter_mode": "fixed_metric", "play_filter": {"fixed_metric": "kill_group"}}],
        })['item']
        result = run_auto_trigger_cycle(self.repo, user_id=self.user, fetcher=lambda *args, **kwargs: {
            "latest_settled_issue": "1001", "metrics": {"kill_group": {"recent_20": {"hit_rate": 75, "sample_count": 20, "hit_count": 15}}},
        })
        self.assertEqual(result["summary"]["triggered_count"], 1, result)
        jobs = self.repo.list_execution_jobs(user_id=self.user)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["planned_message_text"], "大单10 大双10 小单10")
        job = jobs[0]
        self.repo.update_progression_event_status(progression_event_id=job["progression_event_id"], status="placed")
        resolved = resolve_subscription_progression(self.repo, subscription_id=self.sub["id"], user_id=self.user, payload={"progression_event_id": job["progression_event_id"], "draw_context": {"result_number": 13}})
        event = resolved["progression"]["event"]
        self.assertEqual(event["net_delta"], -20)
        with self.repo._connect() as conn:
            report = dict(conn.execute("SELECT * FROM profit_report_daily_stats WHERE rule_id = ?", (rule["id"],)).fetchone())
            daily = dict(conn.execute("SELECT * FROM auto_trigger_route_daily_stats WHERE route_id = ?", (job["auto_trigger_route_id"],)).fetchone())
        self.assertEqual((report["hit_count"], report["miss_count"], report["net_profit"]), (1, 0, -20))
        self.assertEqual((daily["settled_event_count"], daily["hit_count"], daily["net_profit"]), (1, 1, -20))

    def test_bot_can_select_both_group_play_presets_without_sending_messages(self):
        self.repo.update_user_telegram_binding(user_id=self.user, telegram_user_id=123, telegram_chat_id="123", telegram_username="", telegram_bound_at=datetime.now(timezone.utc).isoformat())
        for label, metric, expected_count in (("双组", "double_group", 6), ("杀组", "kill_group", 4)):
            text = handle_telegram_command(self.repo, telegram_user_id=123, telegram_chat_id="123", telegram_username="", text=f'/play {self.sub["id"]} {label}')
            self.assertIn(label, text)
            keys = self.repo.get_subscription(self.sub["id"])["strategy_v2"]["play_filter"]["selected_keys"]
            self.assertEqual(len(keys), expected_count)
            self.assertTrue(all(key.startswith(metric + ":") for key in keys))


if __name__ == "__main__":
    unittest.main()
