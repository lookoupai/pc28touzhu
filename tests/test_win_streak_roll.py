from __future__ import annotations

import unittest

from pc28touzhu.domain.subscription_strategy import (
    normalize_subscription_strategy_input,
    resolve_staking_runtime_policy,
)
from pc28touzhu.domain.win_streak_roll import (
    COMBO_TO_STRUCTURE,
    ensure_daily_state,
    build_period_plan,
    create_cycle_state,
    is_stopped,
    normalize_win_streak_roll_policy,
    settle_period,
    structure_for_period,
)
from pc28touzhu.domain.win_streak_stop import (
    STOP_REASON_BALANCE_DEPLETED,
    STOP_REASON_WIN_STREAK,
)


def _policy(**overrides):
    payload = {"mode": "win_streak_roll", "base_amount": 100.0, "structure": "A", "roll_mode": "all_in"}
    payload.update(overrides)
    return normalize_win_streak_roll_policy(payload)


class PolicyTests(unittest.TestCase):
    def test_defaults(self):
        policy = normalize_win_streak_roll_policy({"mode": "win_streak_roll"})
        self.assertAlmostEqual(policy["base_amount"], 100.0)
        self.assertAlmostEqual(policy["main_share"], 0.70)
        self.assertAlmostEqual(policy["aux_share"], 0.30)
        self.assertEqual(policy["target_streak"], 3)
        self.assertEqual(policy["structure"], "A")
        self.assertEqual(policy["roll_mode"], "chain")
        self.assertEqual(policy["reset_scope"], "daily")

    def test_invalid_values_rejected(self):
        with self.assertRaises(ValueError):
            normalize_win_streak_roll_policy({"base_amount": 0})
        with self.assertRaises(ValueError):
            normalize_win_streak_roll_policy({"main_share": 1.5})
        with self.assertRaises(ValueError):
            normalize_win_streak_roll_policy({"roll_mode": "martingale2"})
        with self.assertRaises(ValueError):
            normalize_win_streak_roll_policy({"structure": "E"})
        with self.assertRaises(ValueError):
            normalize_win_streak_roll_policy({"target_streak": 0})

    def test_structure_follow_signal_accepted(self):
        policy = normalize_win_streak_roll_policy({"structure": "follow_signal"})
        self.assertEqual(policy["structure"], "follow_signal")

    def test_subscription_strategy_accepts_mode(self):
        normalized = normalize_subscription_strategy_input({
            "play_filter": {"mode": "all"},
            "staking_policy": {
                "mode": "win_streak_roll",
                "base_amount": 100.0,
                "target_streak": 3,
                "structure": "A",
                "roll_mode": "chain",
            },
        })
        staking = normalized["staking_policy"]
        self.assertEqual(staking["mode"], "win_streak_roll")
        self.assertAlmostEqual(staking["base_amount"], 100.0)
        self.assertEqual(staking["roll_mode"], "chain")

    def test_subscription_strategy_rejects_bad_roll_mode(self):
        with self.assertRaises(ValueError):
            normalize_subscription_strategy_input({
                "play_filter": {"mode": "all"},
                "staking_policy": {"mode": "win_streak_roll", "roll_mode": "nope"},
            })

    def test_runtime_policy_branch(self):
        runtime = resolve_staking_runtime_policy({
            "staking_policy": {"mode": "win_streak_roll", "base_amount": 100.0, "structure": "C"},
        }, {})
        self.assertEqual(runtime["mode"], "win_streak_roll")
        self.assertEqual(runtime["structure"], "C")
        self.assertAlmostEqual(runtime["base_amount"], 100.0)


class StructureFromSignalTests(unittest.TestCase):
    def test_combo_signal_maps_to_structure(self):
        for combo, expected in COMBO_TO_STRUCTURE.items():
            signal = {"bet_type": "combo", "bet_value": combo}
            self.assertEqual(structure_for_period({"structure": "follow_signal"}, signal), expected)

    def test_odd_even_signal_rejected_for_follow(self):
        with self.assertRaises(ValueError):
            structure_for_period({"structure": "follow_signal"}, {"bet_type": "odd_even", "bet_value": "单"})

    def test_fixed_structure_ignores_signal(self):
        self.assertEqual(structure_for_period({"structure": "D"}, None), "D")


class AllInCycleTests(unittest.TestCase):
    def test_three_wins_stop_the_day(self):
        policy = _policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        balances = []
        for _ in range(3):
            result = settle_period(policy, state, {"triplet": [5, 8, 2]})  # 15 大单，主注单中
            state = result["state"]
            balances.append(state["balance"])
        # 整数注码 + 未押出的尾差留仓：100 -> 139.30 -> 押 139（主 97）-> 193.33
        #                                        -> 押 193（主 135）-> 268.98
        self.assertAlmostEqual(balances[0], 139.30)
        self.assertAlmostEqual(balances[1], 193.33)
        self.assertAlmostEqual(balances[2], 268.98)
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(state["stop_reason"], STOP_REASON_WIN_STREAK)

    def test_total_miss_busts(self):
        policy = _policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        result = settle_period(policy, state, {"triplet": [5, 5, 4]})  # 14 大双，两腿全 miss
        self.assertAlmostEqual(result["state"]["balance"], 0.0)
        self.assertEqual(result["state"]["status"], "stopped")
        self.assertEqual(result["state"]["stop_reason"], STOP_REASON_BALANCE_DEPLETED)

    def test_stopped_state_blocks_new_period(self):
        policy = _policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        result = settle_period(policy, state, {"triplet": [5, 5, 4]})
        with self.assertRaises(ValueError):
            build_period_plan(policy, result["state"], stat_date="2026-11-03")

    def test_next_day_resets_balance(self):
        policy = _policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        state = settle_period(policy, state, {"triplet": [5, 8, 2]})["state"]
        next_day = ensure_daily_state(policy, state, "2026-11-04")
        self.assertAlmostEqual(next_day["balance"], 100.0)
        self.assertEqual(next_day["win_streak"], 0)
        self.assertEqual(next_day["stat_date"], "2026-11-04")

    def test_same_day_keeps_state(self):
        policy = _policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        state = settle_period(policy, state, {"triplet": [5, 8, 2]})["state"]
        self.assertEqual(ensure_daily_state(policy, state, "2026-11-03"), state)


class ChainCycleTests(unittest.TestCase):
    """链条口径按雨实盘的满赔2.8 结算（单双 2.84 / 组合 6.79、6.33）。"""

    RULE = "pc28_fullpay_2_8_regular"

    def _chain_policy(self):
        return _policy(roll_mode="chain")

    def test_chain_stake_rolls_with_payout(self):
        policy = self._chain_policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        self.assertAlmostEqual(state["cycle_stake"], 100.0)
        result = settle_period(policy, state, {"triplet": [5, 8, 2]}, settlement_rule_id=self.RULE)  # 主注单中
        self.assertAlmostEqual(result["payout_total"], 198.80)  # 70 * 2.84
        self.assertAlmostEqual(result["state"]["cycle_stake"], 198.80)
        self.assertAlmostEqual(result["state"]["balance"], 198.80)  # 账户=起注+盈利
        self.assertAlmostEqual(result["state"]["day_pnl"], 98.80)

    def test_refund_period_shrinks_stake_and_resets_streak(self):
        policy = self._chain_policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        state = settle_period(policy, state, {"triplet": [5, 8, 2]}, settlement_rule_id=self.RULE)["state"]
        result = settle_period(policy, state, {"triplet": [2, 3, 4]}, settlement_rule_id=self.RULE)  # 9 单中遇顺子退本
        self.assertEqual(result["main_result"]["result_type"], "refund")
        # 整数注码：198.80 -> 总额 198（主 138 / 辅助 60）；主注遇顺子退本，辅助注亏 60
        self.assertAlmostEqual(result["net_pnl"], -60.0)
        self.assertAlmostEqual(result["state"]["cycle_stake"], 138.0)
        self.assertEqual(result["state"]["win_streak"], 0)

    def test_wipe_stops_day_at_exact_daily_loss(self):
        policy = self._chain_policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        state = settle_period(policy, state, {"triplet": [5, 8, 2]}, settlement_rule_id=self.RULE)["state"]
        result = settle_period(policy, state, {"triplet": [5, 5, 4]}, settlement_rule_id=self.RULE)  # 全 miss
        self.assertAlmostEqual(result["payout_total"], 0.0)
        self.assertAlmostEqual(result["state"]["cycle_stake"], 0.0)
        # 整数注码下 198.80 只能押 198，尾差 0.80 留仓，所以当天净亏 99.20
        self.assertAlmostEqual(result["state"]["day_pnl"], -99.20)
        self.assertEqual(result["state"]["status"], "stopped")
        self.assertEqual(result["state"]["stop_reason"], STOP_REASON_BALANCE_DEPLETED)

    def test_account_carries_into_next_day(self):
        policy = self._chain_policy()
        state = create_cycle_state(policy, stat_date="2026-11-03", account_balance=1000.0)
        state = settle_period(policy, state, {"triplet": [5, 8, 2]}, settlement_rule_id=self.RULE)["state"]
        next_day = ensure_daily_state(policy, state, "2026-11-04")
        self.assertAlmostEqual(next_day["balance"], 1098.80)  # 账户跨日结转
        self.assertAlmostEqual(next_day["cycle_stake"], 100.0)  # 次日起注重置
        self.assertEqual(next_day["day_pnl"], 0.0)

    def test_special_sum_refunds_only_main(self):
        policy = self._chain_policy()
        state = create_cycle_state(policy, stat_date="2026-11-03")
        result = settle_period(policy, state, {"triplet": [0, 0, 0]})  # 和值0：单亏、小双照赔（网盘口径）
        self.assertEqual(result["main_result"]["result_type"], "miss")
        self.assertEqual(result["aux_result"]["result_type"], "hit")
        self.assertAlmostEqual(result["payout_total"], 129.60)


class FollowSignalCycleTests(unittest.TestCase):
    def test_combo_signal_drives_structure(self):
        policy = _policy(structure="follow_signal")
        signal = {"bet_type": "combo", "bet_value": "小单"}
        plan = build_period_plan(policy, create_cycle_state(policy, stat_date="2026-11-03"), signal)
        self.assertEqual(plan["structure"], "A")
        self.assertEqual(plan["main"]["bet_value"], "单")
        self.assertEqual(plan["aux"]["bet_value"], "小双")


if __name__ == "__main__":
    unittest.main()
