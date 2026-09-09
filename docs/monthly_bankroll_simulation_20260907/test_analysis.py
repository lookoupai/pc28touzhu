"""验证退款、提前停手和单注资金约束，防止回测虚构盈利。"""
import unittest
from collections import Counter
from datetime import datetime, timedelta, timezone

from analyze import RULES, evaluate, net_cents, prediction_streams, simulate
from target100 import select_on_earlier_months


class BankrollTests(unittest.TestCase):
    def test_bankrupt_path_never_borrows_or_recovers(self):
        result = simulate([(str(i), -100) for i in range(100)] + [("later", 99900)])
        self.assertEqual(result["bets"], 100)
        self.assertEqual(result["ending_bankroll"], 0)
        self.assertEqual(result["stop_reason"], "bankrupt")

    def test_fractional_balance_cannot_fund_a_full_unit(self):
        bets = [("win", 185)] + [(str(i), -100) for i in range(103)]
        result = simulate(bets)
        self.assertEqual(result["ending_bankroll"], 0.85)
        self.assertEqual(result["stop_reason"], "insufficient_for_one_unit")

    def test_profit_target_stops_before_next_loss(self):
        result = simulate([("first", 185), ("later", -100)], target=100, loss_limit=2000)
        self.assertEqual(result["bets"], 1)
        self.assertEqual(result["net"], 1.85)

    def test_loss_limit_is_checked_before_staking(self):
        result = simulate([("win", 185)] + [(str(i), -100) for i in range(30)], loss_limit=2000)
        self.assertEqual(result["net"], -19.15)
        self.assertEqual(result["stop_reason"], "loss_limit")

    def test_drawdown_is_different_from_principal_loss(self):
        result = simulate([("1", 578), ("2", -100), ("3", -100)])
        self.assertEqual(result["max_drawdown"], 2)
        self.assertEqual(result["max_principal_loss"], 0)

    def test_special_hit_refunds_but_special_miss_loses(self):
        draw = {"triplet": [0, 3, 3]}
        self.assertEqual(net_cents("odd_even", "双", RULES[0], draw), 0)
        self.assertEqual(net_cents("odd_even", "单", RULES[0], draw), -100)
        self.assertEqual(net_cents("odd_even", "双", RULES[1], draw), 98)
        self.assertEqual(net_cents("number", 6, RULES[0], draw), 2700)

    def test_empty_period_is_not_a_profitable_month(self):
        result = simulate([])
        self.assertEqual(result["net"], 0)
        self.assertEqual(result["stop_reason"], "no_signal")

    def test_first_profit_policy_accepts_less_than_one_unit(self):
        result = simulate([("1", -100), ("2", 185), ("3", -100)], target=1, loss_limit=9900)
        self.assertEqual(result["bets"], 2)
        self.assertEqual(result["net"], 0.85)
        self.assertEqual(result["stop_reason"], "profit_target")

    def test_ninety_nine_loss_limit_keeps_one_unit(self):
        result = simulate([(str(i), -100) for i in range(100)], target=1, loss_limit=9900)
        self.assertEqual(result["bets"], 99)
        self.assertEqual(result["ending_bankroll"], 1)
        self.assertEqual(result["stop_reason"], "loss_limit")

    def test_target_one_hundred_does_not_stop_at_small_profit(self):
        result = simulate([(str(i), 185) for i in range(60)], target=10000)
        self.assertEqual(result["bets"], 55)
        self.assertEqual(result["net"], 101.75)
        self.assertEqual(result["stop_reason"], "profit_target")

    def test_ninety_nine_point_nine_is_below_target(self):
        streams = {"test": [("2026-08", str(i), 185) for i in range(54)]}
        result = evaluate(streams, "test", {"target100_bankroll": (10000, None)})[0]
        self.assertEqual(result["net"], 99.90)
        self.assertEqual(result["stop_reason"], "sample_end")

    def test_monthly_target_cannot_be_combined_across_months(self):
        streams = {"test": [(month, str(index * 100 + i), 185)
                             for index,month in enumerate(("2026-07", "2026-08"))
                             for i in range(30)]}
        rows = evaluate(streams, "test", {"target100_bankroll": (10000, None)})
        self.assertEqual([r["net"] for r in rows], [55.50, 55.50])
        self.assertTrue(all(r["stop_reason"] == "sample_end" for r in rows))

    def test_selection_does_not_reward_overshoot_above_target(self):
        common = {"family":"predictions", "policy":"target100_bankroll", "month":"2026-07",
                  "available_bets":1000, "bets":100, "max_drawdown":90}
        rows = [{**common, "strategy":"overshoot", "net":150, "max_principal_loss":90},
                {**common, "strategy":"less_loss", "net":101, "max_principal_loss":5}]
        selection = select_on_earlier_months(rows, "predictions", ["2026-07"], [])
        self.assertEqual(selection["selected"], "less_loss")

    def test_trigger_cannot_use_results_settled_in_the_future(self):
        start = datetime(2026, 7, 1, tzinfo=timezone.utc)
        rows, draws = [], {}
        for i in range(102):
            issue = str(1000 + i)
            opened = start + timedelta(days=1, minutes=i)
            draws[issue] = {"month":"2026-07", "opened":opened,
                           "snapshot":{"sum_value":15,"triplet":[3,5,7],
                                       "big_small":"大","odd_even":"单","combo":"大单"}}
            rows.append({"id":i,"predictor_id":7,"issue_no":issue,"actual_number":15,
                         "prediction_big_small":"大","prediction_odd_even":"单",
                         "prediction_combo":"大单","prediction_number":None,
                         "created_at":(start+timedelta(minutes=i)).isoformat(),
                         "settled_at":(opened+timedelta(seconds=10)).isoformat()})
        streams = prediction_streams({7:"测试"}, draws, rows, Counter())
        self.assertFalse(any(":high100:" in key for key in streams))
        rows[-1]["created_at"] = (start+timedelta(days=1,minutes=100,seconds=30)).isoformat()
        streams = prediction_streams({7:"测试"}, draws, rows, Counter())
        hits = streams["predictor7:测试:odd_even:high100:pc28_high_regular"]
        self.assertEqual(len(hits), 1)
        self.assertEqual(hits[0][1], "1101")


if __name__ == "__main__":
    unittest.main()
