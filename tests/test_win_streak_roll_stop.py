"""过关玩法结账时机与收工开关（stop_policy）的回归测试。

覆盖两件事：
1. 周期只在两条腿都到终态（settled / void）后才结账——早期版本在 placed 阶段就结账，
   净盈亏恒按 0 记，导致滚仓、止盈、止损全部失效。
2. stop_policy 决定达标 / 爆仓后是当天收工、当天重开一轮，还是继续滚。
"""
from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from tests.launch_fixtures import create_legacy_rule
from pc28touzhu.domain.win_streak_roll import (
    CYCLE_STOP_REASON_BUST,
    CYCLE_STOP_REASON_TARGET,
    normalize_stop_policy,
)
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.auto_trigger_service import update_auto_trigger_rule
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.pc28_auto_settlement_service import (
    TERMINAL_CYCLE_LEG_STATUSES,
    _resolve_pending_win_streak_cycles,
)
from pc28touzhu.services.platform_service import (
    reset_subscription_runtime,
    resolve_pending_pc28_progressions_from_draws,
    restart_subscription_cycle,
)


def _strategy(**policy_overrides):
    policy = {
        "mode": "win_streak_roll",
        "base_amount": 100.0,
        "main_share": 0.70,
        "aux_share": 0.30,
        "target_streak": 3,
        "structure": "A",
        "roll_mode": "chain",
        "reset_scope": "daily",
    }
    policy.update(policy_overrides)
    return {
        "play_filter": {"mode": "all"},
        "staking_policy": policy,
        "settlement_policy": {
            "rule_source": "subscription_fixed",
            "settlement_rule_id": "pc28_fullpay_2_8_regular",
        },
    }


STAT_DATE = "2026-11-04"


class StopPolicyNormalizationTests(unittest.TestCase):
    def test_defaults_keep_original_semantics(self):
        policy = normalize_stop_policy(None)
        self.assertEqual(policy["on_target"], "close_day")
        self.assertEqual(policy["on_bust"], "close_day")
        self.assertEqual(policy["close_scope"], "rule")
        self.assertEqual(policy["max_cycles_per_day"], 1)

    def test_accepts_multi_cycle_configuration(self):
        policy = normalize_stop_policy(
            {"on_target": "new_cycle", "on_bust": "new_cycle", "close_scope": "route", "max_cycles_per_day": 4}
        )
        self.assertEqual(policy["on_target"], "new_cycle")
        self.assertEqual(policy["max_cycles_per_day"], 4)

    def test_rejects_unknown_values(self):
        with self.assertRaises(ValueError):
            normalize_stop_policy({"on_target": "whatever"})
        with self.assertRaises(ValueError):
            normalize_stop_policy({"on_bust": "continue"})
        with self.assertRaises(ValueError):
            normalize_stop_policy({"close_scope": "subscription"})
        with self.assertRaises(ValueError):
            normalize_stop_policy({"max_cycles_per_day": 0})


class CycleSettlementTimingTests(unittest.TestCase):
    """周期结账必须等开奖结果，不能把 placed 当成已结算。"""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.repo = DatabaseRepository(os.path.join(self.tmpdir.name, "stop.db"))
        self.repo.initialize_database()
        clock = patch("pc28touzhu.services.dispatch_service._shanghai_today", return_value=STAT_DATE)
        clock.start()
        self.addCleanup(clock.stop)
        self.user_id = self.repo.create_user("stop-user")
        self.source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="internal_ai", name="过关来源",
        )
        self.subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"], strategy=_strategy(),
        )
        self.target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100700901", status="active",
        )
        self.signal_no = 0

    def _signal(self, *, bet_type="combo", bet_value="小单", issue_no=None, source=None):
        self.signal_no += 1
        return self.repo.create_signal_record(
            source_id=(source or self.source)["id"], lottery_type="pc28",
            issue_no=issue_no or str(20261104000 + self.signal_no),
            bet_type=bet_type, bet_value=bet_value,
            published_at=self._iso_now(),
        )

    def _iso_now(self):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _deliver(self, result):
        for job in result.get("jobs") or []:
            self.repo.report_job_result(
                str(job["id"]), "test-executor", 1, "delivered", "message-1", self._iso_now(), {}, None,
            )

    def _settle_draws(self, issue_no, triplet):
        return resolve_pending_pc28_progressions_from_draws(
            self.repo, user_id=self.user_id,
            draw_items=[{"issue_no": str(issue_no), "draw_context": {"triplet": list(triplet)}}],
        )

    def test_cycle_state_write_inside_transaction_keeps_savepoint(self):
        """过关周期状态写入不能提交外层事务，否则嵌套 savepoint 会提前失效。

        规则评估是在 transaction() 里开 savepoint 后触发派单的；派单内部如果显式
        commit，外层 RELEASE pc28_unit 就会报 no such savepoint（今天线上出现过两次）。
        """
        from pc28touzhu.domain.win_streak_roll import create_cycle_state, normalize_win_streak_roll_policy

        policy = normalize_win_streak_roll_policy({"mode": "win_streak_roll", "base_amount": 100.0})
        state = create_cycle_state(policy, stat_date="2026-11-04")
        with self.repo.transaction() as unit:
            with unit.transaction() as inner:
                inner.upsert_subscription_cycle_state(
                    subscription_id=int(self.subscription["id"]),
                    user_id=self.user_id,
                    state=state,
                    policy=policy,
                    pending_plan=None,
                )
        saved = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertIsNotNone(saved)
        self.assertEqual(saved["status"], "running")

    def test_placed_legs_do_not_advance_the_cycle(self):
        signal = self._signal()
        result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
        self.assertEqual(result["created_count"], 2)
        self._deliver(result)

        # 消息已送达但还没开奖：周期必须继续等待，不能按 0 盈亏结账
        waiting = _resolve_pending_win_streak_cycles(self.repo)
        self.assertEqual(waiting["resolved_count"], 0)
        self.assertEqual(waiting["waiting_count"], 1)
        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertIsNotNone(state["pending_plan"])
        self.assertAlmostEqual(state["balance"], 100.0)
        self.assertEqual(state["periods"], 0)

        # 开奖结算之后才推进周期
        self._settle_draws(signal["issue_no"], [5, 8, 2])
        resolved = _resolve_pending_win_streak_cycles(self.repo)
        self.assertEqual(resolved["resolved_count"], 1)
        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertIsNone(state["pending_plan"])
        self.assertEqual(state["periods"], 1)
        self.assertNotAlmostEqual(state["balance"], 100.0)

    def test_void_leg_counts_as_terminal(self):
        self.assertIn("void", TERMINAL_CYCLE_LEG_STATUSES)
        self.assertIn("settled", TERMINAL_CYCLE_LEG_STATUSES)


class CycleStopPolicyTests(CycleSettlementTimingTests):
    """带自动触发规则的过关玩法：收工要写到规则当日统计。"""

    def _create_rule(self, *, stop_policy=None, name="过三关规则"):
        strategy = _strategy(**(stop_policy or {}))
        roll_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"], strategy=strategy, name=name,
        )
        rule = create_legacy_rule(
            self.repo, user_id=self.user_id,
            payload={
                "name": name, "cooldown_issues": 0,
                "scope_mode": "selected_subscriptions", "subscription_ids": [roll_subscription["id"]],
                "trigger_mode": "condition",
                "conditions": [{"type": "miss_streak", "metric": "odd_even", "window": "current",
                                 "operator": "gte", "threshold": 5, "min_sample_count": 100}],
                "daily_risk_control": {"enabled": False},
                "routes": [{"delivery_target_id": self.target["id"],
                             "route_risk_mode": "disabled", "subscription_risk_mode": "disabled"}],
            },
        )["item"]
        with self.repo._connect() as conn:
            conn.execute("UPDATE auto_trigger_rules SET cycle_policy_json='{}' WHERE id=?", (rule["id"],))
        rule = self.repo.get_auto_trigger_rule(rule["id"])
        run = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule["id"], user_id=self.user_id,
            subscription_id=roll_subscription["id"], stat_date=STAT_DATE,
        )
        return rule, run, roll_subscription

    def _dispatch_period(self, rule, run, subscription, *, issue_no, triplet):
        signal = self._signal(issue_no=issue_no)
        result = dispatch_signal(
            self.repo, signal["id"], subscription_id=subscription["id"],
            auto_trigger_context={
                "rule_id": rule["id"], "rule_run_id": run["id"],
                "stat_date": run["stat_date"], "routes": rule["routes"],
            },
        )
        self._deliver(result)
        self._settle_draws(issue_no, triplet)
        return _resolve_pending_win_streak_cycles(self.repo)

    def _auto_state(self, rule, subscription):
        return self.repo.get_auto_trigger_cycle_state(rule_id=rule["id"], subscription_id=subscription["id"],
            route_id=rule["routes"][0]["id"], user_id=self.user_id)

    def _rule_day(self, rule, stat_date=STAT_DATE):
        return self.repo.get_auto_trigger_rule_daily_stat(
            rule_id=int(rule["id"]), user_id=self.user_id, stat_date=stat_date,
        )

    def test_three_wins_close_the_rule_day(self):
        rule, run, subscription = self._create_rule()
        # 主注「单」中：15 大单命中单；辅助「小双」未中
        for index, issue_no in enumerate(["20261104501", "20261104502", "20261104503"], start=1):
            self._dispatch_period(rule, run, subscription, issue_no=issue_no, triplet=[5, 8, 2])
            state = self._auto_state(rule, subscription)
            self.assertEqual(state["win_streak"], index)

        state = self._auto_state(rule, subscription)
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(state["stop_reason"], "win_streak_target")
        self.assertTrue(state["day_closed"])

        day = self._rule_day(rule)
        self.assertEqual(day["status"], "stopped")
        self.assertEqual(day["stopped_reason"], CYCLE_STOP_REASON_TARGET)
        self.assertEqual(self.repo.get_auto_trigger_rule_run(run["id"])["status"], "stopped")

        # 收工之后不再派单
        blocked = dispatch_signal(
            self.repo, self._signal(issue_no="20261104504")["id"], subscription_id=subscription["id"],
            auto_trigger_context={"rule_id": rule["id"], "rule_run_id": run["id"], "stat_date": STAT_DATE, "routes": rule["routes"]},
        )
        self.assertEqual(blocked["created_count"], 0)

    def test_cycle_stop_survives_rule_update(self):
        rule, run, subscription = self._create_rule()
        for issue_no in ["20261104511", "20261104512", "20261104513"]:
            self._dispatch_period(rule, run, subscription, issue_no=issue_no, triplet=[5, 8, 2])
        self.assertEqual(self._rule_day(rule)["status"], "stopped")

        updated = update_auto_trigger_rule(
            self.repo, rule_id=rule["id"], user_id=self.user_id,
            payload={
                "name": rule["name"], "cooldown_issues": 0, "status": "active",
                "scope_mode": "selected_subscriptions", "subscription_ids": [subscription["id"]],
                "trigger_mode": "condition",
                "conditions": [{"type": "miss_streak", "metric": "odd_even", "window": "current",
                                 "operator": "gte", "threshold": 5, "min_sample_count": 100}],
                "daily_risk_control": {"enabled": False},
                "routes": [{"delivery_target_id": self.target["id"],
                             "route_risk_mode": "disabled", "subscription_risk_mode": "disabled"}],
            },
        )
        self.assertFalse(updated["daily_risk_resume"]["resumed"])
        self.assertEqual(self._rule_day(rule)["status"], "stopped")

    def test_route_scope_stops_only_that_route(self):
        rule, run, subscription = self._create_rule(
            stop_policy={"stop_policy": {"close_scope": "route"}},
        )
        for issue_no in ["20261104521", "20261104522", "20261104523"]:
            self._dispatch_period(rule, run, subscription, issue_no=issue_no, triplet=[5, 8, 2])
        route_id = int(rule["routes"][0]["id"])
        route_day = self.repo.get_auto_trigger_route_daily_stat(
            route_id=route_id, user_id=self.user_id, stat_date=STAT_DATE,
        )
        self.assertEqual(route_day["status"], "stopped")
        self.assertEqual(route_day["stopped_reason"], CYCLE_STOP_REASON_TARGET)
        self.assertEqual(self._rule_day(rule)["status"], "active")

    def test_target_new_cycle_runs_second_round_then_closes(self):
        rule, run, subscription = self._create_rule(
            stop_policy={"stop_policy": {"on_target": "new_cycle", "max_cycles_per_day": 2}},
        )
        for issue_no in ["20261104531", "20261104532", "20261104533"]:
            self._dispatch_period(rule, run, subscription, issue_no=issue_no, triplet=[5, 8, 2])

        state = self._auto_state(rule, subscription)
        self.assertEqual(state["status"], "running")
        self.assertEqual(state["win_streak"], 0)
        self.assertEqual(state["cycles_today"], 1)
        self.assertAlmostEqual(state["cycle_stake"], 100.0)
        self.assertEqual(self._rule_day(rule)["status"], "active")

        # 第二轮同样连赢三次：轮数用满，转为当天收工
        for issue_no in ["20261104534", "20261104535", "20261104536"]:
            self._dispatch_period(rule, run, subscription, issue_no=issue_no, triplet=[5, 8, 2])
        state = self._auto_state(rule, subscription)
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(self._rule_day(rule)["status"], "stopped")

    def test_on_target_continue_keeps_rolling(self):
        rule, run, subscription = self._create_rule(
            stop_policy={"stop_policy": {"on_target": "continue", "max_cycles_per_day": 1}},
        )
        for issue_no in ["20261104541", "20261104542", "20261104543"]:
            self._dispatch_period(rule, run, subscription, issue_no=issue_no, triplet=[5, 8, 2])

        state = self._auto_state(rule, subscription)
        self.assertEqual(state["status"], "running")
        self.assertEqual(state["win_streak"], 0)
        self.assertEqual(state["cycles_today"], 0)
        self.assertEqual(self._rule_day(rule)["status"], "active")

    def test_manual_restart_keeps_auto_stopped_run(self):
        """手动重开仅重置手动进度，不移动/解封自动规则窗口。"""
        rule, run, subscription = self._create_rule()
        self.repo.stop_auto_trigger_rule_current_run(rule_id=rule["id"], user_id=self.user_id, subscription_id=subscription["id"])
        before = self.repo.get_auto_trigger_rule_run(run["id"])
        result = restart_subscription_cycle(self.repo, subscription_id=subscription["id"], user_id=self.user_id, payload={})
        self.assertEqual(result["rule_runs_reopened"], [])
        self.assertEqual(self.repo.get_auto_trigger_rule_run(run["id"]), before)

    def test_manual_restart_does_not_reopen_risk_stopped_run(self):
        """风控停轮不能被开始新一轮解封：止盈止损/熔断停掉的轮次保持 stopped。"""
        rule, run, subscription = self._create_rule()
        self.repo.stop_auto_trigger_rule_day(
            rule_id=int(rule["id"]), user_id=self.user_id, stat_date=STAT_DATE, reason="daily_hit_rate_low",
        )
        self.assertEqual(self.repo.get_auto_trigger_rule_run(int(run["id"]))["status"], "stopped")

        result = restart_subscription_cycle(
            self.repo, subscription_id=subscription["id"], user_id=self.user_id,
            payload={"note": "测试风控停轮不解封"},
        )
        self.assertEqual(result["rule_runs_reopened"], [])
        self.assertEqual(self.repo.get_auto_trigger_rule_run(int(run["id"]))["status"], "stopped")

    def test_manual_restart_preserves_auto_cycle_stop(self):
        rule, run, subscription = self._create_rule()
        for issue_no in ["20261104601", "20261104602", "20261104603"]:
            self._dispatch_period(rule, run, subscription, issue_no=issue_no, triplet=[5, 8, 2])
        before = self._auto_state(rule, subscription)
        self.assertEqual(self._rule_day(rule)["status"], "stopped")
        result = restart_subscription_cycle(self.repo, subscription_id=subscription["id"], user_id=self.user_id, payload={})
        self.assertEqual(result["cycle_reset"]["resumed_rule_ids"], [])
        self.assertEqual(self._auto_state(rule, subscription), before)
        self.assertEqual(self._rule_day(rule)["status"], "stopped")
        manual = self.repo.get_subscription_cycle_state(subscription_id=subscription["id"])
        self.assertEqual(manual["status"], "running")
        self.assertEqual(manual["cycle_stake"], 100)

    def test_manual_profit_reset_keeps_the_day_closed(self):
        """重置盈亏只清周期基线，不解封玩法当日收工。"""
        rule, run, subscription = self._create_rule()
        for issue_no in ["20261104611", "20261104612", "20261104613"]:
            self._dispatch_period(rule, run, subscription, issue_no=issue_no, triplet=[5, 8, 2])
        self.assertEqual(self._rule_day(rule)["status"], "stopped")

        result = reset_subscription_runtime(
            self.repo, subscription_id=subscription["id"], user_id=self.user_id,
            payload={"note": "测试重置盈亏"},
        )
        self.assertTrue(result["cycle_reset"]["applied"])
        self.assertEqual(result["cycle_reset"]["resumed_rule_ids"], [])
        self.assertEqual(self._rule_day(rule)["status"], "stopped")

    def test_wipe_closes_the_day_with_bust_reason(self):
        rule, run, subscription = self._create_rule()
        # 14 大双：主注「单」与辅助「小双」同时未中，整期返还 0
        self._dispatch_period(rule, run, subscription, issue_no="20261104551", triplet=[5, 5, 4])
        state = self._auto_state(rule, subscription)
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(state["stop_reason"], "balance_depleted")
        self.assertAlmostEqual(state["balance"], 0.0)
        self.assertEqual(self._rule_day(rule)["stopped_reason"], CYCLE_STOP_REASON_BUST)


if __name__ == "__main__":
    unittest.main()
