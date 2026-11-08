from __future__ import annotations

import unittest

from pc28touzhu.domain.draw_streak import DRAW_VALUES_BY_METRIC, build_draw_streak_snapshot
from pc28touzhu.domain.prediction_streak import build_prediction_direction_streaks
from pc28touzhu.services.auto_trigger_service import _match_condition, _normalize_conditions


def condition(kind, value="", metric="big_small", threshold=3, operator="gte"):
    return {"type": kind, "metric": metric, "threshold": threshold, "operator": operator,
            ("bet_value" if kind in {"draw_streak", "draw_absence"} else "prediction_value"): value}


def prediction_performance(values, *, kind="hit_streak", count=None, metric="big_small"):
    items = [{"issue_no": str(100 - i), "bet_type": metric, "bet_value": value} for i, value in enumerate(values)]
    metrics = {metric: {"streaks": {"current_" + kind: len(values) if count is None else count}}}
    directions = build_prediction_direction_streaks(items, latest_issue_no="100", metrics=metrics)
    metrics[metric]["direction_streaks"] = directions[metric]
    return {"metrics": metrics}


class StreakConditionTests(unittest.TestCase):
    def test_new_types_round_trip_and_old_miss_defaults_to_unrestricted(self):
        for kind in ("hit_streak", "miss_streak"):
            old = {"type": kind, "metric": "big_small", "threshold": 3, "operator": "gte"}
            self.assertNotIn("prediction_value", _normalize_conditions([old])[0])
            for metric, values in DRAW_VALUES_BY_METRIC.items():
                for value in values:
                    with self.subTest(kind=kind, metric=metric, value=value):
                        item = _normalize_conditions([condition(kind, value, metric)])[0]
                        self.assertEqual(item["prediction_value"], value)
        self.assertEqual(_normalize_conditions([condition("draw_absence", "大")])[0]["bet_value"], "大")

    def test_invalid_directions_and_thresholds_are_rejected(self):
        invalid = [condition("draw_absence"), condition("draw_absence", "单"), condition("miss_streak", "大双")]
        for kind in ("hit_streak", "miss_streak", "draw_absence"):
            invalid.extend(condition(kind, "大", threshold=value) for value in (-1, 1.5, "nan", "inf"))
        invalid.extend(condition("draw_absence", "大", threshold=value) for value in (0, 101))
        for item in invalid:
            with self.subTest(item=item), self.assertRaises(ValueError):
                _normalize_conditions([item])

    def test_unrestricted_prediction_streak_allows_direction_changes(self):
        for kind in ("hit_streak", "miss_streak"):
            performance = prediction_performance(["大", "小", "大"], kind=kind)
            self.assertEqual(_match_condition(condition(kind), performance)["actual_" + kind], 3)
            self.assertIsNone(_match_condition(condition(kind, "大"), performance))
            self.assertEqual(_match_condition(condition(kind, "大", threshold=1), performance)["actual_" + kind], 1)

    def test_selected_direction_supports_all_values_and_stops_at_outcome_change(self):
        for kind in ("hit_streak", "miss_streak"):
            for metric, values in DRAW_VALUES_BY_METRIC.items():
                for value in values:
                    with self.subTest(kind=kind, value=value):
                        performance = prediction_performance([value] * 4, kind=kind, count=2, metric=metric)
                        self.assertIsNone(_match_condition(condition(kind, value, metric), performance))
                        match = _match_condition(condition(kind, value, metric, threshold=2), performance)
                        self.assertEqual(match["actual_" + kind], 2)
                        self.assertTrue(match["streak_complete"])
                        self.assertEqual(match["prediction_issues"], ["100", "99"])

    def test_missing_prediction_history_is_a_lower_bound(self):
        for kind in ("hit_streak", "miss_streak"):
            performance = prediction_performance(["大"], kind=kind, count=4)
            for operator, expected in (("gte", True), ("gt", False), ("lte", False), ("lt", False)):
                with self.subTest(kind=kind, operator=operator):
                    match = _match_condition(condition(kind, "大", threshold=1, operator=operator), performance)
                    self.assertEqual(match is not None, expected)
            empty = prediction_performance([], kind=kind, count=4)
            self.assertIsNone(_match_condition(condition(kind, "大", operator="lt"), empty))

    def test_missing_invalid_and_conflicting_predictions_do_not_get_skipped(self):
        metrics = {"big_small": {"streaks": {"current_miss_streak": 4}}}
        latest = {"issue_no": "100", "bet_type": "big_small", "bet_value": "大"}
        older = {**latest, "issue_no": "98"}
        for middle in ([], [{**latest, "issue_no": "99", "bet_value": "?"}],
                       [{**latest, "issue_no": "99"}, {**latest, "issue_no": "99", "bet_value": "小"}, {**latest, "issue_no": "99"}]):
            values = build_prediction_direction_streaks([latest, latest, older, *middle], latest_issue_no="100", metrics=metrics)
            self.assertEqual(values["big_small"]["大"]["miss_streak"], {"count": 1, "complete": False, "issues": ["100"]})

    def test_opposite_direction_and_zero_overall_streak_are_exact_zero(self):
        for performance in (prediction_performance(["小"] * 3), prediction_performance([], count=0)):
            match = _match_condition(condition("hit_streak", "大", threshold=0, operator="lte"), performance)
            self.assertEqual(match["actual_hit_streak"], 0)
            self.assertTrue(match["streak_complete"])

    def test_missing_or_invalid_source_streak_is_not_zero(self):
        for value in (None, -1, "bad", "nan", "inf", 1.5):
            performance = {"metrics": {"big_small": {"streaks": {"current_hit_streak": value}}}}
            self.assertIsNone(_match_condition(condition("hit_streak", operator="lt"), performance))

    def test_absence_allows_other_combinations_to_alternate_and_resets_on_target(self):
        # 最新到最旧：小单、小双、大单、小单、大双。
        items = [{"issue_no": str(100 - i), "result_number": number} for i, number in enumerate((11, 0, 17, 9, 16))]
        snapshot = build_draw_streak_snapshot(items, latest_issue_no="100")
        match = _match_condition(condition("draw_absence", "大双", "combo", threshold=4), {"draw_streak": snapshot})
        self.assertEqual(match["actual_draw_absence"], 4)
        self.assertTrue(match["streak_complete"])
        self.assertIsNone(_match_condition(condition("draw_streak", "小单", "combo"), {"draw_streak": snapshot}))
        self.assertEqual(snapshot["absent_metrics"]["combo"]["小单"]["count"], 0)

    def test_binary_absence_equals_opposite_draw_streak(self):
        items = [{"issue_no": str(100 - i), "result_number": number} for i, number in enumerate((16, 18, 0, 9))]
        snapshot = build_draw_streak_snapshot(items, latest_issue_no="100")
        for metric in ("big_small", "odd_even"):
            first, second = DRAW_VALUES_BY_METRIC[metric]
            self.assertEqual(snapshot["metrics"][metric][first], snapshot["absent_metrics"][metric][second])
            self.assertEqual(snapshot["metrics"][metric][second], snapshot["absent_metrics"][metric][first])

    def test_absence_missing_period_is_not_an_exact_count(self):
        snapshot = build_draw_streak_snapshot([
            {"issue_no": "100", "result_number": 9}, {"issue_no": "98", "result_number": 11},
        ], latest_issue_no="100")
        performance = {"draw_streak": snapshot}
        self.assertIsNotNone(_match_condition(condition("draw_absence", "大", threshold=1), performance))
        for operator in ("lt", "lte"):
            self.assertIsNone(_match_condition(condition("draw_absence", "大", threshold=2, operator=operator), performance))


if __name__ == "__main__":
    unittest.main()
