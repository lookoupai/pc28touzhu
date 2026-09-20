from __future__ import annotations

import unittest

from pc28touzhu.domain.settlement_rules import (
    LEGACY_TO_SETTLEMENT_RULE_ID,
    SETTLEMENT_RULE_CATALOG,
    build_settlement_snapshot,
    resolve_pc28_result_for_signal,
    resolve_pc28_settled_hit_profit,
    settlement_rule_id_from_legacy,
)


class SettlementRuleTests(unittest.TestCase):
    def test_high_rule_refunds_special_sum(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "大"},
            settlement_rule_id="pc28_high_regular",
            draw_context={"result_number": 14, "triplet": [4, 4, 6]},
        )
        self.assertEqual(result["result_type"], "refund")
        self.assertEqual(result["refund_reason"], "13/14 退本金")

    def test_high_rule_refunds_baozi(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "小"},
            settlement_rule_id="pc28_high_regular",
            draw_context={"triplet": [2, 2, 2]},
        )
        self.assertEqual(result["result_type"], "refund")
        self.assertEqual(result["refund_reason"], "豹子退本金")

    def test_high_rule_refunds_straight(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "小"},
            settlement_rule_id="pc28_high_regular",
            draw_context={"triplet": [1, 2, 3]},
        )
        self.assertEqual(result["result_type"], "refund")
        self.assertEqual(result["refund_reason"], "顺子退本金")

    def test_high_rule_refunds_wraparound_straight(self):
        for triplet, winning_value in (([8, 9, 0], "单"), ([1, 9, 0], "双")):
            result = resolve_pc28_result_for_signal(
                signal={"lottery_type": "pc28", "bet_type": "odd_even", "bet_value": winning_value},
                settlement_rule_id="pc28_high_regular",
                draw_context={"triplet": triplet},
            )
            self.assertEqual(result["result_type"], "refund", triplet)
            self.assertEqual(result["refund_reason"], "顺子退本金", triplet)

    def test_high_rule_keeps_miss_on_wraparound_straight(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "odd_even", "bet_value": "单"},
            settlement_rule_id="pc28_high_regular",
            draw_context={"triplet": [1, 9, 0]},
        )
        self.assertEqual(result["actual_value"], "双")
        self.assertEqual(result["result_type"], "miss")
        self.assertIsNone(result["refund_reason"])

    def test_high_rule_rejects_non_straight_gap(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "odd_even", "bet_value": "单"},
            settlement_rule_id="pc28_high_regular",
            draw_context={"triplet": [0, 2, 9]},
        )
        self.assertEqual(result["result_type"], "hit")
        self.assertIsNone(result["refund_reason"])

    def test_netdisk_rule_keeps_normal_hit(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "combo", "bet_value": "大单"},
            settlement_rule_id="pc28_netdisk_regular",
            draw_context={"triplet": [6, 3, 8]},
        )
        self.assertEqual(result["result_type"], "hit")
        self.assertIsNone(result["refund_reason"])

    def test_legacy_mapping_stays_on_old_four_rules(self):
        self.assertEqual(
            set(LEGACY_TO_SETTLEMENT_RULE_ID.values()),
            {"pc28_netdisk_regular", "pc28_netdisk_abc", "pc28_high_regular", "pc28_high_abc"},
        )
        self.assertEqual(settlement_rule_id_from_legacy("pc28_fullpay_2_8", "regular"), "pc28_netdisk_regular")

    def test_cai28_misses_extreme_sum_when_wrong_side(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "大"},
            settlement_rule_id="pc28_fullpay_netdisk_regular",
            draw_context={"triplet": [0, 0, 0]},
        )
        self.assertEqual(result["actual_value"], "小")
        self.assertEqual(result["result_type"], "miss")
        self.assertIsNone(result["refund_reason"])

    def test_cai28_refunds_extreme_sum_on_hit_only(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "小"},
            settlement_rule_id="pc28_fullpay_netdisk_regular",
            draw_context={"triplet": [0, 0, 0]},
        )
        self.assertEqual(result["result_type"], "refund")
        self.assertEqual(result["refund_reason"], "0/27 退本金")

    def test_fullpay_netdisk_combo_keeps_extreme_hit(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "combo", "bet_value": "小双"},
            settlement_rule_id="pc28_fullpay_netdisk_regular",
            draw_context={"triplet": [0, 0, 0]},
        )
        self.assertEqual(result["result_type"], "hit")
        self.assertIsNone(result["refund_reason"])

    def test_fullpay_2_0_combo_refunds_special_sum_on_hit(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "combo", "bet_value": "小单"},
            settlement_rule_id="pc28_fullpay_2_0_regular",
            draw_context={"triplet": [4, 4, 5]},
        )
        self.assertEqual(result["actual_value"], "小单")
        self.assertEqual(result["result_type"], "refund")
        self.assertEqual(result["refund_reason"], "13/14 退本金")

    def test_fullpay_2_0_big_small_keeps_hit_on_special_sum(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "小"},
            settlement_rule_id="pc28_fullpay_2_0_regular",
            draw_context={"triplet": [4, 4, 5]},
        )
        self.assertEqual(result["result_type"], "hit")
        self.assertEqual(
            resolve_pc28_settled_hit_profit(
                stake_amount=2001,
                signal={"bet_type": "big_small", "bet_value": "小"},
                settlement_rule_id="pc28_fullpay_2_0_regular",
                draw_snapshot=result["draw_snapshot"],
            ),
            2001.0,
        )
        self.assertEqual(
            resolve_pc28_settled_hit_profit(
                stake_amount=2002,
                signal={"bet_type": "big_small", "bet_value": "小"},
                settlement_rule_id="pc28_fullpay_2_0_regular",
                draw_snapshot=result["draw_snapshot"],
            ),
            1961.96,
        )

    def test_fullpay_2_8_profit_uses_2_84_not_old_high(self):
        self.assertEqual(
            resolve_pc28_settled_hit_profit(
                stake_amount=10,
                signal={"bet_type": "big_small", "bet_value": "大"},
                settlement_rule_id="pc28_fullpay_2_8_regular",
            ),
            18.4,
        )
        self.assertEqual(
            resolve_pc28_settled_hit_profit(
                stake_amount=10,
                signal={"bet_type": "big_small", "bet_value": "大"},
                settlement_rule_id="pc28_high_regular",
            ),
            18.46,
        )

    def test_fullpay_2_8_refunds_wraparound_straight_on_hit(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "odd_even", "bet_value": "单"},
            settlement_rule_id="pc28_fullpay_2_8_regular",
            draw_context={"triplet": [8, 9, 0]},
        )
        self.assertEqual(result["result_type"], "refund")
        self.assertEqual(result["refund_reason"], "顺子退本金")

    def test_fullpay_2_0_does_not_treat_wraparound_as_straight(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "odd_even", "bet_value": "单"},
            settlement_rule_id="pc28_fullpay_2_0_regular",
            draw_context={"triplet": [8, 9, 0]},
        )
        self.assertEqual(result["actual_value"], "单")
        self.assertEqual(result["result_type"], "hit")
        self.assertTrue(result["draw_snapshot"]["is_wraparound_straight"])
        self.assertFalse(result["draw_snapshot"]["special_flags"]["is_straight"])

    def test_fullpay_3_2_refunds_abc_zero_or_nine_on_hit_only(self):
        hit = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "小"},
            settlement_rule_id="pc28_fullpay_3_2_regular",
            draw_context={"triplet": [0, 1, 2]},
        )
        self.assertEqual(hit["result_type"], "refund")
        self.assertEqual(hit["refund_reason"], "ABC 含 0/9 退本金")
        miss = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "大"},
            settlement_rule_id="pc28_fullpay_3_2_regular",
            draw_context={"triplet": [0, 1, 2]},
        )
        self.assertEqual(miss["result_type"], "miss")

    def test_fullpay_3_2_does_not_refund_pair(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "小"},
            settlement_rule_id="pc28_fullpay_3_2_regular",
            draw_context={"triplet": [2, 2, 5]},
        )
        self.assertEqual(result["result_type"], "hit")
        self.assertTrue(result["draw_snapshot"]["special_flags"]["is_pair"])

    def test_odds_overlay_only_applies_to_that_snapshot(self):
        overlay_profit = resolve_pc28_settled_hit_profit(
            stake_amount=10,
            signal={"bet_type": "big_small", "bet_value": "大"},
            settlement_rule_id="pc28_fullpay_2_8_regular",
            odds_overrides={"big_small": 2.9, "odd_even": 2.9},
        )
        default_profit = resolve_pc28_settled_hit_profit(
            stake_amount=10,
            signal={"bet_type": "big_small", "bet_value": "大"},
            settlement_rule_id="pc28_fullpay_2_8_regular",
        )
        self.assertEqual(overlay_profit, 19.0)
        self.assertEqual(default_profit, 18.4)

    def test_snapshot_freeze_ignores_later_catalog_change(self):
        snapshot = build_settlement_snapshot(
            rule_source="subscription_fixed",
            settlement_rule_id="pc28_fullpay_2_8_regular",
            fallback_profit_ratio=1.0,
            resolved_from="subscription",
            signal={"bet_type": "big_small", "bet_value": "大"},
        )
        self.assertEqual(snapshot["implemented_odds"]["big_small"], 2.84)
        original = SETTLEMENT_RULE_CATALOG["pc28_fullpay_2_8_regular"]["implemented_odds"]["big_small"]
        SETTLEMENT_RULE_CATALOG["pc28_fullpay_2_8_regular"]["implemented_odds"]["big_small"] = 9.99
        try:
            profit = resolve_pc28_settled_hit_profit(
                stake_amount=10,
                signal={"bet_type": "big_small", "bet_value": "大"},
                settlement_rule_id="pc28_fullpay_2_8_regular",
                snapshot=snapshot,
            )
            self.assertEqual(profit, 18.4)
        finally:
            SETTLEMENT_RULE_CATALOG["pc28_fullpay_2_8_regular"]["implemented_odds"]["big_small"] = original

    def test_old_snapshot_without_metric_settlement_still_refunds_high_special(self):
        result = resolve_pc28_result_for_signal(
            signal={"lottery_type": "pc28", "bet_type": "big_small", "bet_value": "大"},
            settlement_rule_id="pc28_high_regular",
            draw_context={"result_number": 14, "triplet": [4, 4, 6]},
            rule={
                "id": "pc28_high_regular",
                "refund_policy_by_metric": {
                    "big_small": "special_on_hit",
                    "odd_even": "special_on_hit",
                    "combo": "special_on_hit",
                    "number": "none",
                },
            },
        )
        self.assertEqual(result["result_type"], "refund")
        self.assertEqual(result["refund_reason"], "13/14 退本金")


if __name__ == "__main__":
    unittest.main()
