from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.pc28_auto_settlement_service import _resolve_pending_win_streak_cycles
from pc28touzhu.services.platform_service import resolve_pending_pc28_progressions_from_draws

STRATEGY = {
    "play_filter": {"mode": "all"},
    "staking_policy": {
        "mode": "win_streak_roll",
        "base_amount": 100.0,
        "target_streak": 3,
        "structure": "A",
        "roll_mode": "chain",
    },
    "settlement_policy": {
        "rule_source": "subscription_fixed",
        "settlement_rule_id": "pc28_fullpay_2_8_regular",
    },
}


class WinStreakRollDispatchTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = os.path.join(self.tmpdir.name, "roll.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()
        self.user_id = self.repo.create_user("roll-user")
        self.source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="internal_ai", name="滚仓来源",
        )
        self.subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"], strategy=STRATEGY,
        )
        self.target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100700001", status="active",
        )
        self.signal_no = 0

    def _signal_for(self, source):
        return self._signal(source=source)

    def _signal(self, *, bet_type="big_small", bet_value="大", issue_no=None, source=None):
        self.signal_no += 1
        return self.repo.create_signal_record(
            source_id=(source or self.source)["id"], lottery_type="pc28",
            issue_no=issue_no or str(20261103000 + self.signal_no),
            bet_type=bet_type, bet_value=bet_value,
            published_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        )

    def _draw_items(self, issue_no, triplet):
        return [{"issue_no": str(issue_no), "draw_context": {"triplet": list(triplet)}}]

    def _iso_now(self):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _deliver_and_settle(self, result, *, triplet):
        """模拟执行器把两腿消息送达（事件进入 placed），再用真实开奖结算并推进玩法状态。"""
        issue_no = str(20261103000 + self.signal_no)
        for job in result.get("jobs") or []:
            self.repo.report_job_result(
                str(job["id"]), "test-executor", 1, "delivered", "message-1", self._iso_now(), {}, None,
            )
        resolved = resolve_pending_pc28_progressions_from_draws(
            self.repo, user_id=self.user_id,
            draw_items=self._draw_items(issue_no, triplet),
        )
        cycle = _resolve_pending_win_streak_cycles(self.repo)
        return resolved, cycle

    def test_dispatch_creates_two_legs_with_policy_stakes(self):
        signal = self._signal(bet_type="big_small", bet_value="大")
        result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
        self.assertEqual(result["created_count"], 2)
        legs = sorted(job["stake_plan"]["leg"] for job in result["jobs"])
        self.assertEqual(legs, ["aux", "main"])
        stakes = {job["stake_plan"]["leg"]: job["stake_plan"]["amount"] for job in result["jobs"]}
        self.assertAlmostEqual(stakes["main"], 70.0)
        self.assertAlmostEqual(stakes["aux"], 30.0)

        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertIsNotNone(state)
        self.assertAlmostEqual(state["balance"], 100.0)
        self.assertAlmostEqual(state["cycle_stake"], 100.0)
        self.assertIsNotNone(state["pending_plan"])
        self.assertEqual(len(state["pending_plan"]["leg_event_ids"]), 2)

    def test_settlement_updates_cycle_state(self):
        signal = self._signal()
        result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
        resolved, cycle = self._deliver_and_settle(result, triplet=[5, 8, 2])  # 15 大单：主注单中
        self.assertEqual(resolved["summary"]["resolved_count"], 2)
        self.assertEqual(cycle["resolved_count"], 1)
        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertIsNone(state["pending_plan"])
        self.assertAlmostEqual(state["balance"], 198.80)  # 70 * 2.84
        self.assertAlmostEqual(state["cycle_stake"], 198.80)
        self.assertEqual(state["win_streak"], 1)
        self.assertEqual(state["status"], "running")

    def test_three_wins_stop_and_block_further_dispatch(self):
        for triplet in ([5, 8, 2], [1, 2, 7], [5, 8, 2]):
            signal = self._signal()
            result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
            self._deliver_and_settle(result, triplet=triplet)
        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(state["stop_reason"], "win_streak_target")

        blocked = dispatch_signal(self.repo, self._signal()["id"], subscription_id=self.subscription["id"])
        self.assertEqual(blocked["created_count"], 0)
        self.assertEqual(blocked["skipped_count"], 1)

    def test_wipe_stops_with_exact_daily_loss(self):
        signal = self._signal()
        result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
        resolved, cycle = self._deliver_and_settle(result, triplet=[5, 5, 4])  # 14 大双：两腿全 miss
        self.assertEqual(resolved["summary"]["miss_count"], 2)
        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertAlmostEqual(state["day_pnl"], -100.0)
        self.assertAlmostEqual(state["balance"], 0.0)
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(state["stop_reason"], "balance_depleted")

    def test_same_source_can_host_two_plans_dispatching_independently(self):
        """同一来源建两个方案（普通跟单 + 过关滚仓）：都能创建、同一条信号各自独立派单、各记各的。"""
        roll_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"],
            strategy=dict(STRATEGY), name="组合马尔可夫 过三关",
        )
        fixed_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"],
            strategy={"play_filter": {"mode": "all"}, "staking_policy": {"mode": "fixed", "fixed_amount": 10.0}},
            name="组合马尔可夫 普通跟单",
        )
        self.assertNotEqual(roll_subscription["id"], fixed_subscription["id"])

        signal = self._signal(bet_type="combo", bet_value="大单")
        roll_result = dispatch_signal(self.repo, signal["id"], subscription_id=roll_subscription["id"])
        fixed_result = dispatch_signal(self.repo, signal["id"], subscription_id=fixed_subscription["id"])
        self.assertEqual(roll_result["created_count"], 2)   # 过关滚仓两腿
        self.assertEqual(fixed_result["created_count"], 1)  # 普通跟单单注
        self.assertAlmostEqual(fixed_result["jobs"][0]["stake_plan"]["amount"], 10.0)

    def test_route_rules_same_group_same_issue_dispatch_independently(self):
        """两条规则、两个方案、同一个群、同一期：各自派单、各自记账，互不影响。"""
        from pc28touzhu.services.auto_trigger_service import create_auto_trigger_rule

        fixed_source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="internal_ai", name="另一来源",
        )
        fixed_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=fixed_source["id"],
            strategy={"play_filter": {"mode": "all"}, "staking_policy": {"mode": "fixed", "fixed_amount": 10.0}},
            name="普通跟单方案",
        )
        issue_no = "20261103500"

        rule_a = create_auto_trigger_rule(
            self.repo, user_id=self.user_id,
            payload={
                "name": "过三关规则", "cooldown_issues": 0,
                "scope_mode": "selected_subscriptions", "subscription_ids": [self.subscription["id"]],
                "trigger_mode": "condition",
                "conditions": [{"type": "miss_streak", "metric": "odd_even", "window": "current",
                                 "operator": "gte", "threshold": 5, "min_sample_count": 100}],
                "daily_risk_control": {"enabled": False},
                "routes": [{"delivery_target_id": self.target["id"],
                             "route_risk_mode": "disabled", "subscription_risk_mode": "disabled"}],
            },
        )["item"]
        rule_b = create_auto_trigger_rule(
            self.repo, user_id=self.user_id,
            payload={
                "name": "普通规则", "cooldown_issues": 0,
                "scope_mode": "selected_subscriptions", "subscription_ids": [fixed_subscription["id"]],
                "trigger_mode": "condition",
                "conditions": [{"type": "miss_streak", "metric": "odd_even", "window": "current",
                                 "operator": "gte", "threshold": 5, "min_sample_count": 100}],
                "daily_risk_control": {"enabled": False},
                "routes": [{"delivery_target_id": self.target["id"],
                             "route_risk_mode": "disabled", "subscription_risk_mode": "disabled"}],
            },
        )["item"]

        issue_signal_a = self._signal(bet_type="combo", bet_value="大单", issue_no=issue_no)
        issue_signal_b = self.repo.create_signal_record(
            source_id=fixed_source["id"], lottery_type="pc28", issue_no=issue_no,
            bet_type="big_small", bet_value="大",
            published_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        )
        run_a = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule_a["id"], user_id=self.user_id, subscription_id=self.subscription["id"], stat_date="2026-11-03",
        )
        run_b = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule_b["id"], user_id=self.user_id, subscription_id=fixed_subscription["id"], stat_date="2026-11-03",
        )
        result_a = dispatch_signal(self.repo, issue_signal_a["id"], subscription_id=self.subscription["id"],
                                    auto_trigger_context={"rule_id": rule_a["id"], "rule_run_id": run_a["id"],
                                                           "stat_date": run_a["stat_date"], "routes": rule_a["routes"]})
        result_b = dispatch_signal(self.repo, issue_signal_b["id"], subscription_id=fixed_subscription["id"],
                                    auto_trigger_context={"rule_id": rule_b["id"], "rule_run_id": run_b["id"],
                                                           "stat_date": run_b["stat_date"], "routes": rule_b["routes"]})
        self.assertEqual(result_a["created_count"], 2)  # 过关滚仓两腿
        self.assertEqual(result_b["created_count"], 1)  # 普通跟单单注
    def test_combo_signal_prices_each_leg_with_its_own_play(self):
        """组合信号触发的过关期：两条腿各自按自己的玩法算钱，不能按信号原始组合赔率算。

        满赔2.8 规则：单双 2.84，小单/大双 6.79，大单/小双 6.33。
        跟随信号取 小单 -> 结构 A（主注单 + 辅助小双）；15 大单 时主注中、辅助未中：
        主注应为 70 × (2.84 - 1) = 128.80，而不是 70 × (6.79 - 1) = 405.30。
        """
        strategy = {
            "play_filter": {
                "mode": "selected",
                "selected_keys": ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
            },
            "staking_policy": {**STRATEGY["staking_policy"], "structure": "follow_signal"},
            "settlement_policy": {
                "rule_source": "subscription_fixed",
                "settlement_rule_id": "pc28_fullpay_2_8_regular",
            },
        }
        subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"], strategy=strategy, name="过三关 跟随信号",
        )
        signal = self._signal(bet_type="combo", bet_value="小单")
        result = dispatch_signal(self.repo, signal["id"], subscription_id=subscription["id"])
        self.assertEqual(result["created_count"], 2)
        self.assertEqual({job["stake_plan"]["structure"] for job in result["jobs"]}, {"A"})

        self._deliver_and_settle(result, triplet=[5, 8, 2])
        legs = {}
        for job in result["jobs"]:
            legs[job["stake_plan"]["leg"]] = self.repo.get_progression_event(int(job["progression_event_id"]))
        self.assertEqual(legs["main"]["resolved_result_type"], "hit")
        self.assertEqual(legs["aux"]["resolved_result_type"], "miss")
        self.assertAlmostEqual(legs["main"]["net_delta"], 128.80)
        self.assertAlmostEqual(legs["aux"]["net_delta"], -30.0)
        state = self.repo.get_subscription_cycle_state(subscription_id=subscription["id"])
        self.assertAlmostEqual(state["balance"], 198.80)

    def test_old_fixed_mode_still_single_job(self):
        fixed_strategy = {
            "play_filter": {"mode": "all"},
            "staking_policy": {"mode": "fixed", "fixed_amount": 10.0},
        }
        fixed_source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="internal_ai", name="普通跟单来源",
        )
        fixed_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=fixed_source["id"], strategy=fixed_strategy,
        )
        fixed_signal = self._signal_for(fixed_source)
        result = dispatch_signal(self.repo, fixed_signal["id"], subscription_id=fixed_subscription["id"])
        self.assertEqual(result["created_count"], 1)
        self.assertAlmostEqual(result["jobs"][0]["stake_plan"]["amount"], 10.0)


if __name__ == "__main__":
    unittest.main()
