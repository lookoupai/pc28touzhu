from __future__ import annotations

import os
import json
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.domain.draw_streak import DRAW_VALUES_BY_METRIC, build_draw_streak_snapshot
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.auto_trigger_service import (
    _match_condition,
    _normalize_conditions,
    create_auto_trigger_rule,
    run_auto_trigger_cycle,
    update_auto_trigger_rule,
)


LATEST = 4000000


def draws(*numbers):
    return [{"issue_no": str(LATEST - offset), "result_number": number} for offset, number in enumerate(numbers)]


def condition(value="大双", metric="combo", threshold=2, operator="gte"):
    return {"type": "draw_streak", "metric": metric, "bet_value": value, "threshold": threshold, "operator": operator}


class DrawStreakTests(unittest.TestCase):
    def test_all_eight_results_and_special_numbers_use_raw_draws(self):
        examples = {"大": 14, "小": 13, "单": 27, "双": 0, "大单": 27, "大双": 14, "小单": 13, "小双": 0}
        for metric, values in DRAW_VALUES_BY_METRIC.items():
            for value in values:
                with self.subTest(value=value):
                    c = _normalize_conditions([condition(value, metric)])[0]
                    snapshot = build_draw_streak_snapshot(draws(examples[value], examples[value]), latest_issue_no=str(LATEST))
                    match = _match_condition(c, {"draw_streak": snapshot})
                    self.assertEqual(match["actual_draw_streak"], 2)

    def test_interruption_resets_instead_of_counting_occurrences(self):
        snapshot = build_draw_streak_snapshot(draws(16, 9, 18, 20), latest_issue_no=str(LATEST))
        self.assertIsNone(_match_condition(condition(), {"draw_streak": snapshot}))
        self.assertEqual(snapshot["metrics"]["combo"]["大双"]["count"], 1)

    def test_sorting_and_duplicate_issues_do_not_inflate_streak(self):
        items = draws(16, 18, 9)
        snapshot = build_draw_streak_snapshot(list(reversed(items)) + [items[0]], latest_issue_no=str(LATEST))
        streak = snapshot["metrics"]["combo"]["大双"]
        self.assertEqual(streak, {"count": 2, "complete": True, "issues": [str(LATEST), str(LATEST - 1)]})

    def test_missing_invalid_or_conflicting_period_cannot_be_crossed(self):
        for items in [
            [draws(16)[0], draws(16, 18, 20)[2]],
            draws(16, 99, 18),
            draws(16, 18, 20) + [{"issue_no": str(LATEST - 1), "result_number": 9}],
        ]:
            with self.subTest(items=items):
                snapshot = build_draw_streak_snapshot(items, latest_issue_no=str(LATEST))
                self.assertIsNone(_match_condition(condition(), {"draw_streak": snapshot}))
                self.assertFalse(snapshot["metrics"]["combo"]["大双"]["complete"])

    def test_truncated_history_is_a_lower_bound_not_an_exact_count(self):
        snapshot = build_draw_streak_snapshot(draws(16, 18), latest_issue_no=str(LATEST))
        for operator, expected in [("gte", True), ("gt", False), ("lt", False), ("lte", False)]:
            with self.subTest(operator=operator):
                result = _match_condition(condition(operator=operator), {"draw_streak": snapshot})
                self.assertEqual(result is not None, expected)
        complete = build_draw_streak_snapshot(draws(16, 18, 9), latest_issue_no=str(LATEST))
        self.assertIsNotNone(_match_condition(condition(operator="lte"), {"draw_streak": complete}))

    def test_latest_period_must_match_clock_and_have_valid_data(self):
        for items in [[], draws(99), draws(16)[1:], [{"issue_no": str(LATEST - 1), "result_number": 16}], draws(16) + [{"issue_no": str(LATEST + 1), "result_number": 18}], draws(16) + draws(9)]:
            with self.subTest(items=items), self.assertRaisesRegex(ValueError, "draw_history_not_current"):
                build_draw_streak_snapshot(items, latest_issue_no=str(LATEST))

    def test_invalid_values_and_fractional_or_unbounded_thresholds_are_rejected(self):
        for payload in [condition(value=""), condition(value="单"), condition(metric="odd_even"), *[condition(threshold=value) for value in [0, -1, 1.5, 101, "nan", "inf"]]]:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                _normalize_conditions([payload])


class DrawStreakTriggerTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.repo = DatabaseRepository(os.path.join(self.tmpdir.name, "test.db"))
        self.repo.initialize_database()
        self.user_id = self.repo.create_user_record(username="draw-trigger-user", status="active")["id"]
        # 固定过关结构只需要来源发布期号信号；开奖条件不依赖来源 performance 接口。
        self.source = self.repo.create_source_record(owner_user_id=self.user_id, source_type="internal_ai", name="测试来源", status="active")
        self.strategy = {
            "play_filter": {"mode": "all"},
            "staking_policy": {"mode": "win_streak_roll", "structure": "B", "base_amount": 100, "target_streak": 3, "roll_mode": "chain"},
            "settlement_policy": {"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_fullpay_netdisk_regular"},
        }
        self.subscription = self.repo.create_subscription_record(user_id=self.user_id, source_id=self.source["id"], strategy=self.strategy, status="standby")
        self.target = self.repo.create_delivery_target_record(user_id=self.user_id, executor_type="telegram_group", target_key="-100123456", status="active")
        self.now = datetime.now(timezone.utc).replace(microsecond=0)
        self.clock = {"latest_issue_no": str(LATEST), "latest_open_time": (self.now - timedelta(seconds=20)).isoformat(), "fetched_at": self.now.isoformat(), "stale": False}
        self.prediction_fetcher = Mock(side_effect=AssertionError("纯开奖条件不应请求预测统计"))
        draw_patch = patch("pc28touzhu.services.auto_trigger_service.fetch_pc28_recent_draws_deep", return_value={"items": draws(16, 18, 9), "source": "test"})
        self.draw_fetch = draw_patch.start()
        self.addCleanup(draw_patch.stop)

    def create_rule(self, *, conditions=None, dispatch=True, routes=None, guard_groups=None):
        return create_auto_trigger_rule(self.repo, user_id=self.user_id, payload={
            "name": "连续开奖启动", "scope_mode": "selected_subscriptions", "subscription_ids": [self.subscription["id"]],
            "cooldown_issues": 0, "conditions": conditions or [condition()],
            "guard_groups": guard_groups or [], "action": {"dispatch_latest_signal": dispatch, "play_filter_action": "keep"},
            "routes": routes or [],
        })["item"]

    def signal(self, issue=LATEST + 1):
        return self.repo.create_signal_record(source_id=self.source["id"], lottery_type="pc28", issue_no=str(issue), bet_type="big_small", bet_value="小", published_at=self.now.isoformat())

    def run_cycle(self, **kwargs):
        return run_auto_trigger_cycle(self.repo, user_id=self.user_id, now=self.now, draw_clock=self.clock, fetcher=self.prediction_fetcher, **kwargs)

    def auto_cycle(self):
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        route_id = jobs[0]["auto_trigger_route_id"]
        route = self.repo.get_auto_trigger_rule_route(route_id)
        return self.repo.get_auto_trigger_cycle_state(rule_id=route["rule_id"], subscription_id=self.subscription["id"], user_id=self.user_id, route_id=route_id)

    def test_draw_trigger_starts_chosen_structure_not_observed_result(self):
        self.signal()
        self.create_rule()
        result = self.run_cycle()
        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.prediction_fetcher.assert_not_called()
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs), 2)
        plan = self.auto_cycle()["pending_plan"]
        self.assertEqual(plan["structure"], "B")
        self.assertEqual(plan["stake_total"], 100)
        self.assertEqual({job["stake_plan"]["leg"]: job["stake_plan"]["amount"] for job in jobs}, {"main": 70, "aux": 30})
        self.assertEqual({job["planned_message_text"] for job in jobs}, {"单70", "大双30"})
        self.assertEqual({job["issue_no"] for job in jobs}, {str(LATEST + 1)})
        event = self.repo.get_latest_auto_trigger_event(rule_id=result["rules"][0]["rule_id"])
        self.assertEqual(event["latest_issue_no"], str(LATEST))
        self.assertEqual(event["matched_conditions"][0]["actual_draw_streak"], 2)
        self.assertEqual(event["matched_conditions"][0]["draw_issues"], [str(LATEST), str(LATEST - 1)])

    def test_inherited_route_uses_subscription_structure(self):
        self.signal()
        self.create_rule(routes=[{"delivery_target_id": self.target["id"], "staking_mode": "inherit", "play_filter_mode": "keep"}])
        result = self.run_cycle()
        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 2)
        self.assertEqual(self.auto_cycle()["pending_plan"]["structure"], "B")
        again = self.run_cycle()
        self.assertEqual(again["summary"]["triggered_count"], 0)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 2)

    def test_different_trigger_value_does_not_change_structure(self):
        self.signal()
        self.draw_fetch.return_value["items"] = draws(9, 11, 16)
        self.create_rule(conditions=[condition("小单")])
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)
        plan = self.auto_cycle()["pending_plan"]
        self.assertEqual(plan["structure"], "B")

    def test_one_period_and_interrupted_streak_do_not_start(self):
        self.signal()
        self.create_rule()
        for items in [draws(16, 9), draws(16, 9, 18), draws(9, 16, 18)]:
            self.draw_fetch.return_value["items"] = items
            self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)
            self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])

    def test_drawn_signal_waits_for_next_signal_without_consuming_trigger(self):
        self.signal(issue=LATEST)
        rule = self.create_rule()
        first = self.run_cycle()
        self.assertEqual(first["summary"]["triggered_count"], 0)
        self.assertEqual(first["rules"][0]["events"][0]["reason"], "issue_already_drawn")
        self.assertIsNone(self.repo.get_latest_auto_trigger_event(rule_id=rule["id"]))
        self.assertEqual(self.repo.get_subscription(self.subscription["id"])["status"], "standby")
        self.signal()
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)

    def test_same_draw_cannot_restart_even_with_zero_cooldown_and_no_jobs(self):
        self.create_rule(dispatch=False)
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)
        # 没有派单时没有待结算任务，仍须按开奖期号防重复。
        self.repo.update_subscription_status(subscription_id=self.subscription["id"], user_id=self.user_id, status="standby")
        second = self.run_cycle()
        self.assertEqual(second["summary"]["triggered_count"], 0)
        self.assertEqual(second["rules"][0]["events"][0]["reason"], "cycle_running")

    def test_stale_clock_and_old_draw_history_do_not_start(self):
        self.create_rule(dispatch=False)
        self.clock["stale"] = True
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)
        self.draw_fetch.assert_not_called()
        self.clock["stale"] = False
        self.draw_fetch.return_value["items"] = draws(16, 18, 20)[1:]
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)
        self.assertEqual(self.repo.list_execution_jobs(user_id=self.user_id), [])

    def test_draw_fetch_failure_does_not_reuse_previous_success(self):
        self.create_rule(dispatch=False)
        self.draw_fetch.return_value["items"] = draws(16, 9)
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)
        self.draw_fetch.side_effect = RuntimeError("draw_unavailable")
        result = self.run_cycle()
        self.assertEqual(result["summary"]["triggered_count"], 0)
        self.assertEqual(result["summary"]["failed_count"], 1)

    def test_rule_round_trip_and_guard_conditions_keep_selected_value(self):
        rule = self.create_rule(dispatch=False, guard_groups=[{"conditions": [condition("单", "odd_even")]}])
        updated = update_auto_trigger_rule(self.repo, rule_id=rule["id"], user_id=self.user_id, payload={"name": "改名"})["item"]
        self.assertEqual(updated["conditions"][0]["bet_value"], "大双")
        self.assertEqual(updated["guard_groups"][0]["conditions"][0]["bet_value"], "单")
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)
        update_auto_trigger_rule(self.repo, rule_id=rule["id"], user_id=self.user_id, payload={"guard_groups": [{"conditions": [condition("双", "odd_even")]}]})
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)

    def test_draw_fetch_is_shared_across_rules(self):
        self.create_rule(dispatch=False, conditions=[condition("小单")])
        self.create_rule(dispatch=False, conditions=[condition("小双")])
        self.run_cycle()
        self.assertEqual(self.draw_fetch.call_count, 1)

    def test_strict_greater_than_max_threshold_fetches_extra_period(self):
        self.create_rule(dispatch=False, conditions=[condition(threshold=100, operator="gt")])
        self.draw_fetch.return_value["items"] = draws(*([16] * 101))
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)
        self.assertEqual(self.draw_fetch.call_args.kwargs["limit"], 101)

    def test_draw_only_rule_fetches_clock_when_caller_does_not_supply_one(self):
        self.signal(issue=LATEST)
        self.create_rule()
        with patch("pc28touzhu.services.auto_trigger_service.get_pc28_draw_clock", return_value=self.clock) as clock_fetch:
            result = run_auto_trigger_cycle(self.repo, user_id=self.user_id, now=self.now, fetcher=self.prediction_fetcher)
        clock_fetch.assert_called_once()
        self.assertEqual(result["summary"]["triggered_count"], 0)
        self.assertEqual(result["rules"][0]["events"][0]["reason"], "issue_already_drawn")

    def test_prediction_only_rule_does_not_fetch_draw_history(self):
        self.create_rule(dispatch=False, conditions=[{"type": "miss_streak", "metric": "combo", "threshold": 2, "operator": "gte"}])
        with patch("pc28touzhu.services.auto_trigger_service._performance_url_from_source", return_value="https://example.com/performance"):
            self.prediction_fetcher.side_effect = None
            self.prediction_fetcher.return_value = {"latest_settled_issue": str(LATEST), "metrics": {"combo": {"streaks": {"current_miss_streak": 2}}}}
            self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)
        self.draw_fetch.assert_not_called()

    def test_absence_triggers_roll_with_alternating_combinations_without_prediction_api(self):
        self.signal()
        self.draw_fetch.return_value["items"] = draws(11, 0, 17, 9, 16)
        self.create_rule(conditions=[{**condition(threshold=4), "type": "draw_absence"}])
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)
        self.prediction_fetcher.assert_not_called()
        self.assertEqual(self.auto_cycle()["pending_plan"]["structure"], "B")
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["matched_conditions"][0]["actual_draw_absence"], 4)
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)

    def test_selected_prediction_direction_interrupts_roll_trigger(self):
        for offset, value in enumerate(("大", "小", "大")):
            self.repo.create_signal_record(source_id=self.source["id"], lottery_type="pc28", issue_no=str(LATEST - offset),
                                           bet_type="big_small", bet_value=value, published_at=self.now.isoformat())
        self.signal()
        selected = {"type": "miss_streak", "metric": "big_small", "prediction_value": "大", "threshold": 3, "operator": "gte"}
        rule = self.create_rule(conditions=[selected])
        self.prediction_fetcher.side_effect = None
        self.prediction_fetcher.return_value = {"latest_settled_issue": str(LATEST), "metrics": {"big_small": {"streaks": {"current_miss_streak": 3}}}}
        with patch("pc28touzhu.services.auto_trigger_service._performance_url_from_source", return_value="https://example.com/performance"):
            self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)
            update_auto_trigger_rule(self.repo, rule_id=rule["id"], user_id=self.user_id, payload={"conditions": [{**selected, "threshold": 1}]})
            self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual(event["matched_conditions"][0]["actual_miss_streak"], 1)
        self.assertEqual(event["matched_conditions"][0]["prediction_issues"], [str(LATEST)])
        self.draw_fetch.assert_not_called()

    def test_hit_streak_roll_and_absence_guard_require_same_period(self):
        for offset in range(3):
            self.repo.create_signal_record(source_id=self.source["id"], lottery_type="pc28", issue_no=str(LATEST - offset),
                                           bet_type="big_small", bet_value="大", published_at=self.now.isoformat())
        self.signal()
        self.create_rule(conditions=[{"type": "hit_streak", "metric": "big_small", "prediction_value": "大", "threshold": 2, "operator": "gte"}],
                         guard_groups=[{"conditions": [{**condition("小单", threshold=2), "type": "draw_absence"}]}])
        self.prediction_fetcher.side_effect = None
        payload = {"latest_settled_issue": str(LATEST - 1), "metrics": {"big_small": {"streaks": {"current_hit_streak": 2}}}}
        self.prediction_fetcher.return_value = payload
        with patch("pc28touzhu.services.auto_trigger_service._performance_url_from_source", return_value="https://example.com/performance"):
            self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)
            payload["latest_settled_issue"] = str(LATEST)
            self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)
        event = self.repo.list_auto_trigger_events(user_id=self.user_id, limit=1)[0]
        self.assertEqual({item["type"] for item in event["matched_conditions"]}, {"hit_streak", "draw_absence"})

    def test_mixed_guard_requires_prediction_and_draws_from_same_period(self):
        self.create_rule(dispatch=False, guard_groups=[{"conditions": [{"type": "miss_streak", "metric": "combo", "threshold": 2, "operator": "gte"}]}])
        with patch("pc28touzhu.services.auto_trigger_service._performance_url_from_source", return_value="https://example.com/performance"):
            self.prediction_fetcher.side_effect = None
            self.prediction_fetcher.return_value = {"latest_settled_issue": str(LATEST - 1), "metrics": {"combo": {"streaks": {"current_miss_streak": 2}}}}
            self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 0)
            self.prediction_fetcher.return_value["latest_settled_issue"] = str(LATEST)
            self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)

    def test_draw_alternative_can_match_without_prediction_api(self):
        self.create_rule(dispatch=False, conditions=[{"type": "miss_streak", "metric": "combo", "threshold": 2, "operator": "gte"}, condition()])
        self.assertEqual(self.run_cycle()["summary"]["triggered_count"], 1)

    def test_api_create_list_edit_and_run_draw_rule(self):
        app = PlatformApiApplication(repository=self.repo, executor_api_token="test-token", session_secret="test-secret")
        cookie = build_session_cookie_value(self.user_id, "test-secret")

        def request(path, body=None):
            captured = {}
            def start_response(status, headers):
                captured["status"] = status
            environ = build_testing_environ(
                path, method="GET" if body is None else "POST", body=body,
                headers={"HTTP_COOKIE": "%s=%s" % (SESSION_COOKIE_NAME, cookie)},
            )
            payload = json.loads(b"".join(app(environ, start_response)).decode())
            self.assertEqual(captured["status"], "200 OK", payload)
            return payload

        rule = request("/api/platform/auto-trigger-rules", {
            "name": "连续开奖", "scope_mode": "selected_subscriptions", "subscription_ids": [self.subscription["id"]],
            "conditions": [condition("小单")], "action": {"dispatch_latest_signal": True},
        })["item"]
        self.assertEqual(request("/api/platform/auto-trigger-rules")["items"][0]["conditions"][0]["bet_value"], "小单")
        request("/api/platform/auto-trigger-rules/%s" % rule["id"], {
            "conditions": [{"type": "hit_streak", "metric": "big_small", "prediction_value": "大", "operator": "gte", "threshold": 3},
                           {**condition("小双"), "type": "draw_absence"}],
            "guard_groups": [{"conditions": [{"type": "miss_streak", "metric": "odd_even", "prediction_value": "单", "operator": "gte", "threshold": 2}]}],
        })
        saved = request("/api/platform/auto-trigger-rules")["items"][0]
        self.assertEqual(saved["conditions"][0]["prediction_value"], "大")
        self.assertEqual(saved["conditions"][1]["type"], "draw_absence")
        self.assertEqual(saved["conditions"][1]["bet_value"], "小双")
        self.assertEqual(saved["guard_groups"][0]["conditions"][0]["prediction_value"], "单")
        request("/api/platform/auto-trigger-rules/%s" % rule["id"], {"conditions": [condition("大双")], "guard_groups": []})
        self.signal()
        with patch("pc28touzhu.api.app.get_pc28_draw_clock", return_value=self.clock):
            result = request("/api/platform/auto-trigger-rules/run-once", {"rule_id": rule["id"]})
        self.assertEqual(result["summary"]["triggered_count"], 1)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 2)

