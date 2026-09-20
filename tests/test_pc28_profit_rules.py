from __future__ import annotations

import unittest

from pc28touzhu.domain.pc28_profit_rules import (
    MAX_CUSTOM_ODDS,
    PC28_PROFIT_RULES,
    catalog_implemented_odds,
    coerce_odds_overrides,
    drop_matching_catalog_odds,
    merge_implemented_odds,
    normalize_odds_overrides,
    normalize_profit_rule_id,
    resolve_pc28_hit_profit,
    resolve_pc28_odds,
    resolve_pc28_odds_detail,
)


class Pc28ProfitRuleTests(unittest.TestCase):
    def test_old_high_catalog_numbers_stay_unchanged(self):
        self.assertEqual(PC28_PROFIT_RULES["pc28_high"]["big_small"]["regular"]["odds"], 2.846)
        self.assertEqual(
            PC28_PROFIT_RULES["pc28_high"]["combo"]["regular"]["group_odds"]["小单 / 大双"],
            6.78,
        )
        self.assertEqual(
            PC28_PROFIT_RULES["pc28_high"]["combo"]["regular"]["group_odds"]["大单 / 小双"],
            6.33,
        )
        self.assertEqual(
            resolve_pc28_hit_profit(
                stake_amount=10,
                bet_type="big_small",
                bet_value="大",
                profit_rule_id="pc28_high",
                odds_profile="regular",
            ),
            18.46,
        )

    def test_fullpay_catalog_uses_house_numbers_not_old_high(self):
        implemented = catalog_implemented_odds("pc28_fullpay_2_8", "regular")
        self.assertEqual(implemented["big_small"], 2.84)
        self.assertEqual(implemented["combo"]["小单"], 6.79)
        self.assertEqual(implemented["combo"]["大单"], 6.33)
        self.assertEqual(
            resolve_pc28_hit_profit(
                stake_amount=10,
                bet_type="big_small",
                bet_value="大",
                profit_rule_id="pc28_fullpay_2_8",
            ),
            18.4,
        )

    def test_unknown_profit_rule_id_falls_back_to_netdisk_not_fullpay(self):
        self.assertEqual(normalize_profit_rule_id("pc28_unknown"), "pc28_netdisk")
        self.assertEqual(
            resolve_pc28_odds(
                bet_type="big_small",
                bet_value="大",
                profit_rule_id="not-a-rule",
            ),
            1.98,
        )

    def test_nested_combo_overrides_and_flat_keys(self):
        nested = normalize_odds_overrides(
            {
                "big_small": 2.9,
                "combo": {"小单": 6.9, "大双": 6.9, "大单": 6.4, "小双": 6.4},
            }
        )
        self.assertEqual(nested["big_small"], 2.9)
        self.assertEqual(nested["combo"]["小单"], 6.9)
        self.assertEqual(nested["combo"]["大单"], 6.4)
        flat = coerce_odds_overrides({"combo_small_odd_big_even": 7.1, "combo_big_odd_small_even": 6.2})
        self.assertEqual(flat["combo"]["小单"], 7.1)
        self.assertEqual(flat["combo"]["大双"], 7.1)
        self.assertEqual(flat["combo"]["大单"], 6.2)
        self.assertEqual(flat["combo"]["小双"], 6.2)

    def test_custom_odds_max_is_1000(self):
        self.assertEqual(MAX_CUSTOM_ODDS, 1000.0)
        self.assertEqual(normalize_odds_overrides({"big_small": 1000})["big_small"], 1000.0)
        with self.assertRaises(ValueError):
            normalize_odds_overrides({"big_small": 1000.0001})
        with self.assertRaises(ValueError):
            normalize_odds_overrides({"big_small": 1})
        self.assertEqual(coerce_odds_overrides({"big_small": 1001}), {})

    def test_drop_matching_catalog_odds_keeps_only_diffs(self):
        implemented = catalog_implemented_odds("pc28_fullpay_2_8", "regular")
        compacted = drop_matching_catalog_odds(
            {
                "big_small": 2.84,
                "odd_even": 2.9,
                "combo": {"小单": 6.79, "大双": 6.79, "大单": 6.4, "小双": 6.4},
            },
            implemented,
        )
        self.assertNotIn("big_small", compacted)
        self.assertEqual(compacted["odd_even"], 2.9)
        self.assertEqual(compacted["combo"], {"大单": 6.4, "小双": 6.4})

    def test_overlay_only_changes_that_metric(self):
        overlay_profit = resolve_pc28_hit_profit(
            stake_amount=10,
            bet_type="big_small",
            bet_value="大",
            profit_rule_id="pc28_fullpay_2_8",
            odds_overrides={"big_small": 2.9, "odd_even": 2.9},
        )
        combo_profit = resolve_pc28_hit_profit(
            stake_amount=10,
            bet_type="combo",
            bet_value="小单",
            profit_rule_id="pc28_fullpay_2_8",
            odds_overrides={"big_small": 2.9, "odd_even": 2.9},
        )
        self.assertEqual(overlay_profit, 19.0)
        self.assertEqual(combo_profit, 57.9)

    def test_fullpay_2_0_stake_tier_is_hit_odds_not_refund(self):
        tiers = [{"sum_values": [13, 14], "stake_gt": 2001, "odds": 1.98}]
        under = resolve_pc28_odds_detail(
            bet_type="big_small",
            bet_value="小",
            profit_rule_id="pc28_fullpay_2_0",
            odds_tiers=tiers,
            sum_value=13,
            stake_amount=2001,
        )
        over = resolve_pc28_odds_detail(
            bet_type="big_small",
            bet_value="小",
            profit_rule_id="pc28_fullpay_2_0",
            odds_tiers=tiers,
            sum_value=13,
            stake_amount=2002,
        )
        self.assertEqual(under["odds"], 2.0)
        self.assertIsNone(under["adjustment"])
        self.assertEqual(over["odds"], 1.98)
        self.assertEqual(over["adjustment"]["from"], 2.0)
        self.assertEqual(over["adjustment"]["to"], 1.98)
        self.assertEqual(
            resolve_pc28_hit_profit(
                stake_amount=2002,
                bet_type="big_small",
                bet_value="小",
                profit_rule_id="pc28_fullpay_2_0",
                odds_tiers=tiers,
                sum_value=13,
            ),
            1961.96,
        )

    def test_frozen_implemented_odds_win_over_catalog(self):
        profit = resolve_pc28_hit_profit(
            stake_amount=10,
            bet_type="big_small",
            bet_value="大",
            profit_rule_id="pc28_fullpay_2_8",
            implemented_odds={"big_small": 2.5, "odd_even": 2.5, "combo": {}},
        )
        self.assertEqual(profit, 15.0)

    def test_merge_copies_big_small_overlay_onto_odd_even(self):
        merged = merge_implemented_odds(
            {"big_small": 2.84, "odd_even": 2.84, "combo": {"小单": 6.79}},
            {"big_small": 2.9},
        )
        self.assertEqual(merged["big_small"], 2.9)
        self.assertEqual(merged["odd_even"], 2.9)
        self.assertEqual(merged["combo"]["小单"], 6.79)


if __name__ == "__main__":
    unittest.main()
