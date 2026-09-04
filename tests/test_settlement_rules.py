from __future__ import annotations

import unittest

from pc28touzhu.domain.settlement_rules import resolve_pc28_result_for_signal


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


if __name__ == "__main__":
    unittest.main()
