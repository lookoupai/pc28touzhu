from __future__ import annotations

import unittest

from pc28touzhu.domain.split_roll_strategy import (
    DEFAULT_SETTLEMENT_RULE_ID,
    STRUCTURES,
    build_period_bets,
    create_day,
    is_stopped,
    normalize_structure,
    reset_day,
    settle_leg,
    settle_period,
    split_stakes,
    structure_options,
)
from pc28touzhu.domain.win_streak_stop import (
    STATUS_RUNNING,
    STOP_REASON_BALANCE_DEPLETED,
    STOP_REASON_WIN_STREAK,
    apply_period_result,
    create_tracker,
    evaluate_stop,
    update_after_period,
)


class StructureTests(unittest.TestCase):
    def test_four_structures_match_spec(self):
        self.assertEqual(STRUCTURES["A"]["main_value"], "单")
        self.assertEqual(STRUCTURES["A"]["aux_value"], "小双")
        self.assertEqual(STRUCTURES["B"]["main_value"], "单")
        self.assertEqual(STRUCTURES["B"]["aux_value"], "大双")
        self.assertEqual(STRUCTURES["C"]["main_value"], "双")
        self.assertEqual(STRUCTURES["C"]["aux_value"], "小单")
        self.assertEqual(STRUCTURES["D"]["main_value"], "双")
        self.assertEqual(STRUCTURES["D"]["aux_value"], "大单")

    def test_normalize_structure_accepts_key_and_label(self):
        self.assertEqual(normalize_structure("A"), "A")
        self.assertEqual(normalize_structure(" b "), "B")
        self.assertEqual(normalize_structure("双+小单"), "C")
        with self.assertRaises(ValueError):
            normalize_structure("E")
        with self.assertRaises(ValueError):
            normalize_structure("")

    def test_structure_options_is_copy(self):
        options = structure_options()
        options[0]["label"] = "changed"
        self.assertEqual(STRUCTURES["A"]["label"], "单+小双")


class StakeTests(unittest.TestCase):
    def test_split_is_seventy_thirty_and_sums_to_balance(self):
        stakes = split_stakes(100)
        self.assertAlmostEqual(stakes["main"], 70.0)
        self.assertAlmostEqual(stakes["aux"], 30.0)
        self.assertAlmostEqual(stakes["total"], 100.0)

    def test_split_rounds_to_two_decimals_without_losing_cents(self):
        stakes = split_stakes(139.31)
        self.assertAlmostEqual(stakes["main"], 97.52)
        self.assertAlmostEqual(stakes["aux"], 41.79)
        self.assertAlmostEqual(stakes["total"], 139.31)

    def test_build_bets_uses_current_balance(self):
        state = create_day(100.0)
        bets = build_period_bets(state, "A")
        self.assertEqual(bets["main"]["bet_type"], "odd_even")
        self.assertEqual(bets["main"]["bet_value"], "单")
        self.assertEqual(bets["aux"]["bet_type"], "combo")
        self.assertEqual(bets["aux"]["bet_value"], "小双")
        self.assertAlmostEqual(bets["total_stake"], 100.0)


class SettlementTests(unittest.TestCase):
    def _state(self):
        return create_day(100.0)

    def test_main_hit_pays_199_with_principal(self):
        result = settle_period(self._state(), structure="A", draw_context={"triplet": [5, 8, 2]})  # 15 大单
        self.assertEqual(result["main_result"]["result_type"], "hit")
        self.assertAlmostEqual(result["main_result"]["payout"], 139.30)
        self.assertEqual(result["aux_result"]["result_type"], "miss")
        self.assertAlmostEqual(result["new_balance"], 139.30)
        self.assertAlmostEqual(result["net_pnl"], 39.30)
        self.assertEqual(result["win_streak"], 1)

    def test_aux_hit_pays_432_for_small_even(self):
        result = settle_period(self._state(), structure="A", draw_context={"triplet": [1, 2, 7]})  # 10 小双
        self.assertEqual(result["main_result"]["result_type"], "miss")
        self.assertEqual(result["aux_result"]["result_type"], "hit")
        self.assertAlmostEqual(result["aux_result"]["payout"], 129.60)  # 30 * 4.32
        self.assertAlmostEqual(result["new_balance"], 129.60)

    def test_aux_big_even_pays_371(self):
        result = settle_period(self._state(), structure="B", draw_context={"triplet": [5, 5, 4]})  # 14 大双
        self.assertEqual(result["aux_result"]["result_type"], "hit")
        self.assertAlmostEqual(result["aux_result"]["payout"], 111.30)  # 30 * 3.71

    def test_sum_zero_main_miss_but_combo_still_pays(self):
        """和值 0：主注「单」未中亏损；组合「小双」按 4.32 正常赔付，整期不退款。"""
        result = settle_period(self._state(), structure="A", draw_context={"triplet": [0, 0, 0]})
        self.assertEqual(result["main_result"]["result_type"], "miss")
        self.assertAlmostEqual(result["main_result"]["payout"], 0.0)
        self.assertEqual(result["aux_result"]["result_type"], "hit")
        self.assertAlmostEqual(result["aux_result"]["payout"], 129.60)
        self.assertAlmostEqual(result["new_balance"], 129.60)

    def test_sum_zero_refunds_even_main_only(self):
        """和值 0：主注「双」退本 70；辅助「小单」未中亏损 30。"""
        result = settle_period(self._state(), structure="C", draw_context={"triplet": [0, 0, 0]})
        self.assertEqual(result["main_result"]["result_type"], "refund")
        self.assertAlmostEqual(result["main_result"]["payout"], 70.0)
        self.assertEqual(result["aux_result"]["result_type"], "miss")
        self.assertAlmostEqual(result["new_balance"], 70.0)
        self.assertAlmostEqual(result["net_pnl"], -30.0)
        self.assertEqual(result["win_streak"], 0)

    def test_sum_27_refunds_odd_main_only(self):
        """和值 27：主注「单」退本 70；辅助「小双」未中亏损 30。"""
        result = settle_period(self._state(), structure="A", draw_context={"triplet": [9, 9, 9]})
        self.assertEqual(result["main_result"]["result_type"], "refund")
        self.assertAlmostEqual(result["main_result"]["payout"], 70.0)
        self.assertEqual(result["aux_result"]["result_type"], "miss")
        self.assertAlmostEqual(result["new_balance"], 70.0)

    def test_draw_context_can_be_sum_only(self):
        result = settle_period(self._state(), structure="A", draw_context={"result_number": 27})
        self.assertEqual(result["main_result"]["result_type"], "refund")
        self.assertAlmostEqual(result["new_balance"], 70.0)

    def test_both_legs_miss_wipes_balance_and_stops(self):
        result = settle_period(self._state(), structure="A", draw_context={"triplet": [5, 5, 4]})  # 14 大双
        self.assertEqual(result["main_result"]["result_type"], "miss")
        self.assertEqual(result["aux_result"]["result_type"], "miss")
        self.assertAlmostEqual(result["new_balance"], 0.0)
        self.assertAlmostEqual(result["net_pnl"], -100.0)
        self.assertEqual(result["status"], "stopped")
        self.assertEqual(result["stop_reason"], STOP_REASON_BALANCE_DEPLETED)
        self.assertTrue(is_stopped(result["state"]))

    def test_settle_leg_rejects_negative_stake(self):
        with self.assertRaises(ValueError):
            settle_leg(stake=-1, bet_type="odd_even", bet_value="单",
                       draw_context={"result_number": 15})


class StopTests(unittest.TestCase):
    def test_three_consecutive_wins_stop_the_day(self):
        state = create_day(100.0)
        for _ in range(2):
            result = settle_period(state, structure="A", draw_context={"triplet": [5, 8, 2]})
            state = result["state"]
            self.assertEqual(state["status"], STATUS_RUNNING)
        result = settle_period(state, structure="A", draw_context={"triplet": [5, 8, 2]})
        state = result["state"]
        self.assertEqual(state["win_streak"], 3)
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(state["stop_reason"], STOP_REASON_WIN_STREAK)

    def test_records_match_three_win_growth(self):
        state = create_day(100.0)
        balances = []
        for _ in range(3):
            result = settle_period(state, structure="A", draw_context={"triplet": [5, 8, 2]})
            balances.append(result["new_balance"])
            state = result["state"]
        self.assertAlmostEqual(balances[0], 139.30)
        self.assertAlmostEqual(balances[1], 194.04)
        self.assertAlmostEqual(balances[2], 270.30)

    def test_stopped_day_cannot_place_more_bets(self):
        state = create_day(100.0)
        for _ in range(3):
            state = settle_period(state, structure="A", draw_context={"triplet": [5, 8, 2]})["state"]
        with self.assertRaises(ValueError):
            build_period_bets(state, "A")
        with self.assertRaises(ValueError):
            settle_period(state, structure="A", draw_context={"triplet": [5, 8, 2]})

    def test_reset_day_restarts_fresh(self):
        state = create_day(100.0)
        for _ in range(3):
            state = settle_period(state, structure="A", draw_context={"triplet": [5, 8, 2]})["state"]
        state = reset_day(balance=100.0)
        self.assertAlmostEqual(state["balance"], 100.0)
        self.assertEqual(state["win_streak"], 0)
        self.assertEqual(state["status"], STATUS_RUNNING)
        self.assertIsNone(state["stop_reason"])

    def test_loss_resets_streak_but_keeps_running(self):
        state = create_day(100.0)
        state = settle_period(state, structure="A", draw_context={"triplet": [5, 8, 2]})["state"]
        self.assertEqual(state["win_streak"], 1)
        state = settle_period(state, structure="A", draw_context={"triplet": [5, 5, 4]})["state"]
        self.assertEqual(state["win_streak"], 0)
        self.assertAlmostEqual(state["balance"], 0.0)
        self.assertEqual(state["status"], "stopped")


class WinStreakTrackerTests(unittest.TestCase):
    def test_zero_net_pnl_keeps_streak(self):
        tracker = create_tracker(3)
        tracker = apply_period_result(tracker, 5.0)
        tracker = apply_period_result(tracker, 0.0)
        self.assertEqual(tracker["win_streak"], 1)
        tracker = apply_period_result(tracker, -0.01)
        self.assertEqual(tracker["win_streak"], 0)

    def test_target_streak_is_configurable(self):
        tracker = create_tracker(2)
        tracker = update_after_period(tracker, 1.0, balance=110.0)
        self.assertEqual(tracker["status"], STATUS_RUNNING)
        tracker = update_after_period(tracker, 1.0, balance=120.0)
        self.assertEqual(tracker["status"], "stopped")
        self.assertEqual(tracker["stop_reason"], STOP_REASON_WIN_STREAK)

    def test_balance_stop_only_when_not_already_stopped(self):
        tracker = create_tracker(3)
        tracker = evaluate_stop(tracker, balance=0.0)
        self.assertEqual(tracker["stop_reason"], STOP_REASON_BALANCE_DEPLETED)
        tracker = evaluate_stop(tracker, balance=100.0)
        self.assertEqual(tracker["stop_reason"], STOP_REASON_BALANCE_DEPLETED)

    def test_invalid_target_streak(self):
        with self.assertRaises(ValueError):
            create_tracker(0)


class DefaultsTests(unittest.TestCase):
    def test_default_rule_and_balance(self):
        state = create_day()
        self.assertAlmostEqual(state["balance"], 100.0)
        self.assertEqual(state["settlement_rule_id"], DEFAULT_SETTLEMENT_RULE_ID)
        self.assertEqual(state["target_streak"], 3)

    def test_unknown_settlement_rule_rejected(self):
        with self.assertRaises(ValueError):
            create_day(100.0, settlement_rule_id="not_a_rule")


if __name__ == "__main__":
    unittest.main()
