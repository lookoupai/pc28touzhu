from __future__ import annotations

import itertools
import unittest

from pc28touzhu.domain.pc28_group_play import (
    COMBO_VALUES, derive_group_selections, normalize_group_selection,
)
from pc28touzhu.domain.pc28_play_filter import strategy_matches_signal
from pc28touzhu.domain.settlement_rules import (
    build_settlement_snapshot, resolve_pc28_group_settlement,
)


class GroupPlayTests(unittest.TestCase):
    def test_every_pair_and_exclusion_has_canonical_unique_combos(self):
        for pair in itertools.combinations(COMBO_VALUES, 2):
            selection = normalize_group_selection("double_group", "+".join(reversed(pair)))
            self.assertEqual(selection["selected_combos"], list(pair))
            self.assertEqual(selection["bet_value"], "+".join(pair))
        for excluded in COMBO_VALUES:
            selection = normalize_group_selection("kill_group", "杀" + excluded)
            self.assertEqual(selection["bet_value"], excluded)
            self.assertEqual(set(selection["selected_combos"]), set(COMBO_VALUES) - {excluded})
            self.assertEqual(selection["excluded_combo"], excluded)

    def test_invalid_groups_and_conflicting_metadata_are_rejected(self):
        for metric, value, payload in (
            ("double_group", "大单+大单", {}),
            ("double_group", "大单", {}),
            ("double_group", "大单+大双+小单", {}),
            ("double_group", "大单+小双", {"selected_combos": ["大单", "大双"]}),
            ("kill_group", "大单", {"excluded_combo": "小双"}),
            ("kill_group", "大单", {"selected_combos": ["大单", "大双", "小单"]}),
            ("kill_group", "大", {}),
        ):
            with self.subTest(metric=metric, value=value, payload=payload), self.assertRaises(ValueError):
                normalize_group_selection(metric, value, payload)

    def test_legacy_labels_and_derivation_keep_existing_meaning(self):
        self.assertEqual(normalize_group_selection("double_group", "单组")["selected_combos"], ["大单", "小双"])
        self.assertEqual(normalize_group_selection("double_group", "双组")["selected_combos"], ["大双", "小单"])
        expected = {"大单": "小双", "大双": "小单", "小单": "大双", "小双": "大单"}
        for combo, opposite in expected.items():
            pair, kill = derive_group_selections(combo)
            self.assertEqual(set(pair["selected_combos"]), {combo, opposite})
            self.assertEqual(kill["excluded_combo"], opposite)
            self.assertNotIn(opposite, kill["selected_combos"])

    def test_legacy_all_and_empty_filters_do_not_enable_groups(self):
        group = {"bet_type": "kill_group", "bet_value": "小双"}
        for strategy in ({}, {"play_filter": {"mode": "all"}}, {"bet_filter": {"mode": "selected", "selected_keys": []}}):
            self.assertFalse(strategy_matches_signal(strategy, group))
            self.assertTrue(strategy_matches_signal(strategy, {"bet_type": "combo", "bet_value": "大单"}))
        self.assertTrue(strategy_matches_signal({"play_filter": {"mode": "selected", "selected_keys": ["kill_group:小双"]}}, group))

    def settle(self, metric, value, number, *, rule="pc28_fullpay_2_0_regular", overrides=None, snapshot=None):
        signal = {"lottery_type": "pc28", "issue_no": "1001", "bet_type": metric, "bet_value": value}
        amount = 20 if metric == "double_group" else 30
        if snapshot is None:
            snapshot = build_settlement_snapshot(
                rule_source="subscription_fixed", settlement_rule_id=rule,
                fallback_profit_ratio=1, resolved_from="subscription", signal=signal,
                odds_overrides=overrides, stake_amount=amount,
            )
        return resolve_pc28_group_settlement(
            signal=signal, stake_amount=amount, settlement_rule_id=rule,
            draw_context={"result_number": number}, snapshot=snapshot,
        )

    def test_multiple_bets_use_winning_combo_odds_and_deduct_other_bets(self):
        for metric, value, expected in (("double_group", "大单+小双", 23.2), ("kill_group", "小双", 13.2)):
            result = self.settle(metric, value, 15)
            self.assertTrue(result["prediction_hit"])
            self.assertEqual(result["result_type"], "hit")
            self.assertEqual(result["group_settlement"]["net_delta"], expected)
            self.assertEqual(sum(item["stake_amount"] for item in result["group_settlement"]["legs"]), result["group_settlement"]["stake_amount"])

    def test_partial_refund_is_covered_prediction_but_financial_loss(self):
        result = self.settle("kill_group", "小双", 13)
        self.assertTrue(result["prediction_hit"])
        self.assertEqual(result["result_type"], "miss")
        self.assertEqual(result["group_settlement"]["net_delta"], -20)
        self.assertEqual([leg["result_type"] for leg in result["group_settlement"]["legs"]], ["miss", "miss", "refund"])

    def test_excluded_result_loses_whole_group(self):
        result = self.settle("kill_group", "小双", 12)
        self.assertFalse(result["prediction_hit"])
        self.assertEqual(result["group_settlement"]["net_delta"], -30)

    def test_frozen_selection_and_odds_survive_source_changes(self):
        signal = {"bet_type": "kill_group", "bet_value": "小双", "lottery_type": "pc28"}
        snapshot = build_settlement_snapshot(
            rule_source="subscription_fixed", settlement_rule_id="pc28_fullpay_2_0_regular",
            fallback_profit_ratio=1, resolved_from="subscription", signal=signal, stake_amount=30,
        )
        result = resolve_pc28_group_settlement(
            signal={**signal, "bet_value": "大单"}, stake_amount=30,
            settlement_rule_id="pc28_netdisk_regular", draw_context={"result_number": 15}, snapshot=snapshot,
        )
        self.assertEqual(result["predicted_value"], "小双")
        self.assertEqual(result["group_settlement"]["net_delta"], 13.2)

    def test_invalid_total_stake_never_rounds_into_unaccounted_money(self):
        for amount in (0, -1, float("nan"), 10.01):
            with self.subTest(amount=amount), self.assertRaises(ValueError):
                resolve_pc28_group_settlement(
                    signal={"bet_type": "kill_group", "bet_value": "小双"},
                    stake_amount=amount, settlement_rule_id="pc28_fullpay_2_0_regular",
                    draw_context={"result_number": 15},
                )

    def test_roll_and_group_combination_is_rejected_before_strategy_repair(self):
        from pc28touzhu.domain.subscription_strategy import normalize_subscription_strategy_input
        from pc28touzhu.services.dispatch_service import _route_effective_strategy, _staking_policy_mode
        strategy = {"staking_policy": {"mode": "win_streak_roll", "structure": "A", "base_amount": 100, "target_streak": 3}, "play_filter": {"mode": "selected", "selected_keys": ["kill_group:小双"]}}
        with self.assertRaisesRegex(ValueError, "过关"):
            normalize_subscription_strategy_input(strategy)
        with self.assertRaisesRegex(ValueError, "过关"):
            _staking_policy_mode(strategy, {"bet_type": "kill_group", "bet_value": "小双"})
        with self.assertRaisesRegex(ValueError, "过关"):
            _route_effective_strategy({**strategy, "play_filter": {"mode": "all"}}, {"play_filter_mode": "fixed_metric", "play_filter": {"fixed_metric": "kill_group"}}, {})


if __name__ == "__main__":
    unittest.main()
