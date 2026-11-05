from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.executor import ExecutorStateStore, run_executor_cycle_concurrent
from pc28touzhu.services.job_service import pull_jobs, report_job
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.pc28_auto_settlement_service import _resolve_pending_win_streak_cycles
from pc28touzhu.services.platform_service import resolve_pending_pc28_progressions_from_draws

STRATEGY = {
    "play_filter": {"mode": "all"},
    "staking_policy": {
        "mode": "win_streak_roll",
        "base_amount": 100.0,
        "target_streak": 3,
        "structure": "A",
        "roll_mode": "chain",
    },
    "settlement_policy": {
        "rule_source": "subscription_fixed",
        "settlement_rule_id": "pc28_fullpay_2_8_regular",
    },
}


class WinStreakRollDispatchTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.db_path = os.path.join(self.tmpdir.name, "roll.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()
        self.user_id = self.repo.create_user("roll-user")
        self.source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="internal_ai", name="滚仓来源",
        )
        self.subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"], strategy=STRATEGY,
        )
        self.target = self.repo.create_delivery_target_record(
            user_id=self.user_id, executor_type="telegram_group", target_key="-100700001", status="active",
        )
        self.signal_no = 0

    def _signal_for(self, source):
        return self._signal(source=source)

    def _signal(self, *, bet_type="big_small", bet_value="大", issue_no=None, source=None):
        self.signal_no += 1
        return self.repo.create_signal_record(
            source_id=(source or self.source)["id"], lottery_type="pc28",
            issue_no=issue_no or str(20261103000 + self.signal_no),
            bet_type=bet_type, bet_value=bet_value,
            published_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        )

    def _draw_items(self, issue_no, triplet):
        return [{"issue_no": str(issue_no), "draw_context": {"triplet": list(triplet)}}]

    def _iso_now(self):
        return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")

    def _deliver_and_settle(self, result, *, triplet):
        """一次合并发送回执令两腿进入 placed，再用真实开奖结算。"""
        issue_no = str(20261103000 + self.signal_no)
        for job in self.repo.pull_ready_jobs(executor_id="test-executor"):
            self.repo.report_job_result(
                str(job["job_id"]), "test-executor", 1, "delivered", "message-1", self._iso_now(), {}, None,
            )
        resolved = resolve_pending_pc28_progressions_from_draws(
            self.repo, user_id=self.user_id,
            draw_items=self._draw_items(issue_no, triplet),
        )
        cycle = _resolve_pending_win_streak_cycles(self.repo)
        return resolved, cycle

    def _dispatch(self):
        return dispatch_signal(self.repo, self._signal()["id"], subscription_id=self.subscription["id"])

    def _report(self, job, status, *, executed_at=None):
        return self.repo.report_job_result(str(job["id"]), "test-executor", 1, status,
            "message-1" if status == "delivered" else None, executed_at or self._iso_now(), {}, None)

    def test_all_structures_combine_correct_stakes_even_with_source_text_and_limit_one(self):
        for structure, expected in {"A": "单70 小双30", "B": "单70 大双30", "C": "双70 小单30", "D": "双70 大单30"}.items():
            with self.subTest(structure=structure):
                strategy = {**STRATEGY, "staking_policy": {**STRATEGY["staking_policy"], "structure": structure}}
                sub = self.repo.create_subscription_record(user_id=self.user_id, source_id=self.source["id"], strategy=strategy)
                signal = self.repo.create_signal_record(source_id=self.source["id"], lottery_type="pc28", issue_no="4000001",
                    bet_type="combo", bet_value="小单", normalized_payload={"message_text": "小单999"}, published_at=self._iso_now())
                result = dispatch_signal(self.repo, signal["id"], subscription_id=sub["id"])
                items = pull_jobs(self.repo, executor_id="test-executor", limit=1)["items"]
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["message_text"], expected)
                self.assertEqual(items[0]["stake_plan"]["amount"], 100)
                self._report(result["jobs"][0], "delivered")
                self.assertEqual(self.repo.pull_ready_jobs("test-executor"), [])
                self.assertEqual({self.repo.get_progression_event(j["progression_event_id"])["status"] for j in result["jobs"]}, {"placed"})

    def test_executor_replays_lost_report_without_resending_combined_message(self):
        result = self._dispatch()
        repository = self.repo
        sent = []

        class Api:
            lost_report = True

            def heartbeat(self, **kwargs):
                return {}

            def pull_jobs(self, *, limit):
                return pull_jobs(repository, executor_id="test-executor", limit=limit)["items"]

            def report_job(self, *, job_id, payload):
                if self.lost_report:
                    self.lost_report = False
                    raise RuntimeError("report unavailable")
                return report_job(repository, job_id, payload)

        class Sender:
            def send_text(self, job):
                sent.append(job.message_text)
                return {"message_id": "shared-123"}

        args = dict(api_client=Api(), message_sender=Sender(), state_store=ExecutorStateStore(),
            executor_id="test-executor", limit=1, version="test", capabilities={})
        with self.assertRaisesRegex(RuntimeError, "report unavailable"):
            run_executor_cycle_concurrent(**args)
        replay = run_executor_cycle_concurrent(**args)
        self.assertEqual(replay["replayed_count"], 1)
        self.assertEqual(sent, ["单70 小双30"])
        self.assertEqual(self.repo.pull_ready_jobs("test-executor"), [])
        jobs = self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual({j["last_remote_message_id"] for j in jobs}, {"shared-123"})
        self.assertEqual({self.repo.get_progression_event(j["progression_event_id"])["status"] for j in result["jobs"]}, {"placed"})

    def test_report_updates_both_legs_atomically(self):
        result = self._dispatch()
        original = self.repo._sync_progression_event_delivery_status
        calls = []

        def fail_second(conn, **kwargs):
            calls.append(kwargs["progression_event_id"])
            if len(calls) == 2:
                raise RuntimeError("second leg failed")
            original(conn, **kwargs)

        with patch.object(self.repo, "_sync_progression_event_delivery_status", side_effect=fail_second):
            with self.assertRaisesRegex(RuntimeError, "second leg failed"):
                self._report(result["jobs"][0], "delivered")
        self.assertEqual({j["status"] for j in self.repo.list_execution_jobs(user_id=self.user_id)}, {"pending"})
        self.assertEqual({self.repo.get_progression_event(j["progression_event_id"])["status"] for j in result["jobs"]}, {"pending"})
        self.assertEqual({j["attempt_count"] for j in self.repo.list_execution_jobs(user_id=self.user_id)}, {0})

    def test_failed_combined_send_retries_once_and_keeps_original_deadline(self):
        result = self._dispatch()
        jobs = result["jobs"]
        past = (datetime.now(timezone.utc) - timedelta(seconds=40)).isoformat()
        self._report(jobs[0], "failed", executed_at=past)
        self.assertEqual({self.repo.get_progression_event(j["progression_event_id"])["status"] for j in jobs}, {"pending"})
        self.assertEqual(_resolve_pending_win_streak_cycles(self.repo)["resolved_count"], 0)
        retried = self.repo.requeue_auto_retry_jobs(max_attempts=3, base_delay_seconds=5, limit=1)
        self.assertEqual(len(retried), 1)
        self.assertEqual({self.repo.get_execution_job(j["id"])["expire_at"] for j in jobs}, {jobs[0]["expire_at"]})
        self.assertEqual([j["message_text"] for j in self.repo.pull_ready_jobs("test-executor", limit=1)], ["单70 小双30"])
        self._report(jobs[0], "delivered")
        self._report(jobs[0], "failed")
        self.assertEqual(self.repo.requeue_auto_retry_jobs(max_attempts=3, base_delay_seconds=5), [])
        self.assertEqual({self.repo.get_execution_job(j["id"])["status"] for j in jobs}, {"delivered"})
        self.assertEqual({self.repo.get_progression_event(j["progression_event_id"])["status"] for j in jobs}, {"placed"})

    def test_manual_retry_from_auxiliary_requeues_the_complete_message(self):
        jobs = self._dispatch()["jobs"]
        self._report(jobs[0], "failed")
        self.repo.retry_execution_job(job_id=jobs[1]["id"], user_id=self.user_id)
        self.assertEqual([j["message_text"] for j in self.repo.pull_ready_jobs("test-executor")], ["单70 小双30"])
        self.assertEqual({self.repo.get_execution_job(j["id"])["expire_at"] for j in jobs}, {jobs[0]["expire_at"]})

    def test_template_rules_render_each_leg_before_combining(self):
        template = self.repo.create_message_template_record(user_id=self.user_id, name="自定义群模板",
            lottery_type="pc28", bet_type="*", template_text="{{bet_value}}={{amount}}",
            config={"bet_rules": {"odd_even": {"value_map": {"单": "单数"}}, "combo": {"format": "{{bet_value}}/{{amount}}"}}})
        self.repo.update_delivery_target_record(delivery_target_id=self.target["id"], user_id=self.user_id,
            telegram_account_id=None, executor_type="telegram_group", target_key=self.target["target_key"],
            target_name="测试群", template_id=template["id"])
        self._dispatch()
        self.assertEqual([j["message_text"] for j in self.repo.pull_ready_jobs("test-executor")], ["单数=70 小双/30"])

    def test_expired_or_unconfirmed_combined_message_cannot_be_manually_resent(self):
        jobs = self._dispatch()["jobs"]
        for status in ("expired", "skipped"):
            with self.subTest(status=status):
                self._report(jobs[0], status)
                with self.assertRaisesRegex(ValueError, "合并投注只能"):
                    self.repo.retry_execution_job(job_id=jobs[1]["id"], user_id=self.user_id)
                self.assertEqual(self.repo.pull_ready_jobs("test-executor"), [])

    def test_late_success_receipt_restores_both_expired_legs(self):
        jobs = self._dispatch()["jobs"]
        with patch("pc28touzhu.executor.db_repository._utc_now_iso", return_value=jobs[0]["expire_at"]):
            self.assertEqual(self.repo.expire_due_jobs(), 2)
            self._report(jobs[0], "delivered", executed_at=jobs[0]["execute_after"])
        self.assertEqual({self.repo.get_progression_event(j["progression_event_id"])["status"] for j in jobs}, {"placed"})
        self.assertEqual({self.repo.get_execution_job(j["id"])["status"] for j in jobs}, {"delivered"})

    def test_failed_combined_send_expires_both_legs_without_extending_window(self):
        jobs = self._dispatch()["jobs"]
        self._report(jobs[0], "failed")
        with patch("pc28touzhu.executor.db_repository._utc_now_iso", return_value=jobs[0]["expire_at"]):
            self.assertEqual(self.repo.expire_due_jobs(), 2)
            self.assertEqual(self.repo.pull_ready_jobs("test-executor"), [])
            self.assertEqual(self.repo.requeue_auto_retry_jobs(max_attempts=3, base_delay_seconds=5), [])
        self.assertEqual({self.repo.get_progression_event(j["progression_event_id"])["status"] for j in jobs}, {"void"})

    def test_failed_message_cannot_retry_after_its_round_is_reset(self):
        jobs = self._dispatch()["jobs"]
        past = (datetime.now(timezone.utc) - timedelta(seconds=40)).isoformat()
        self._report(jobs[0], "failed", executed_at=past)
        self.repo.reset_subscription_runtime(subscription_id=self.subscription["id"], user_id=self.user_id)
        self.assertEqual(self.repo.requeue_auto_retry_jobs(max_attempts=3, base_delay_seconds=5), [])
        with self.assertRaisesRegex(ValueError, "合并投注只能"):
            self.repo.retry_execution_job(job_id=jobs[1]["id"], user_id=self.user_id)
        self.assertEqual(self.repo.pull_ready_jobs("test-executor"), [])

    def test_old_separate_jobs_remain_separate_after_reinitialization(self):
        jobs = self._dispatch()["jobs"]
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET send_with_job_id = NULL")
            conn.execute("DROP INDEX idx_execution_jobs_send_with")
            conn.execute("ALTER TABLE execution_jobs DROP COLUMN send_with_job_id")
        self.repo.initialize_database()
        self.repo.initialize_database()
        self.assertEqual([j["message_text"] for j in self.repo.pull_ready_jobs("test-executor")], ["单70", "小双30"])
        self._report(jobs[0], "delivered")
        self.assertEqual(self.repo.get_execution_job(jobs[1]["id"])["status"], "pending")

    def test_dispatch_creates_two_legs_with_policy_stakes(self):
        signal = self._signal(bet_type="big_small", bet_value="大")
        result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
        self.assertEqual(result["created_count"], 2)
        legs = sorted(job["stake_plan"]["leg"] for job in result["jobs"])
        self.assertEqual(legs, ["aux", "main"])
        stakes = {job["stake_plan"]["leg"]: job["stake_plan"]["amount"] for job in result["jobs"]}
        self.assertAlmostEqual(stakes["main"], 70.0)
        self.assertAlmostEqual(stakes["aux"], 30.0)

        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertIsNotNone(state)
        self.assertAlmostEqual(state["balance"], 100.0)
        self.assertAlmostEqual(state["cycle_stake"], 100.0)
        self.assertIsNotNone(state["pending_plan"])
        self.assertEqual(len(state["pending_plan"]["leg_event_ids"]), 2)

    def test_settlement_updates_cycle_state(self):
        signal = self._signal()
        result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
        resolved, cycle = self._deliver_and_settle(result, triplet=[5, 8, 2])  # 15 大单：主注单中
        self.assertEqual(resolved["summary"]["resolved_count"], 2)
        self.assertEqual(cycle["resolved_count"], 1)
        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertIsNone(state["pending_plan"])
        self.assertAlmostEqual(state["balance"], 198.80)  # 70 * 2.84
        self.assertAlmostEqual(state["cycle_stake"], 198.80)
        self.assertEqual(state["win_streak"], 1)
        self.assertEqual(state["status"], "running")

    def test_three_wins_stop_and_block_further_dispatch(self):
        for triplet in ([5, 8, 2], [1, 2, 7], [5, 8, 2]):
            signal = self._signal()
            result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
            self._deliver_and_settle(result, triplet=triplet)
        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(state["stop_reason"], "win_streak_target")

        blocked = dispatch_signal(self.repo, self._signal()["id"], subscription_id=self.subscription["id"])
        self.assertEqual(blocked["created_count"], 0)
        self.assertEqual(blocked["skipped_count"], 1)

    def test_wipe_stops_with_exact_daily_loss(self):
        signal = self._signal()
        result = dispatch_signal(self.repo, signal["id"], subscription_id=self.subscription["id"])
        resolved, cycle = self._deliver_and_settle(result, triplet=[5, 5, 4])  # 14 大双：两腿全 miss
        self.assertEqual(resolved["summary"]["miss_count"], 2)
        state = self.repo.get_subscription_cycle_state(subscription_id=self.subscription["id"])
        self.assertAlmostEqual(state["day_pnl"], -100.0)
        self.assertAlmostEqual(state["balance"], 0.0)
        self.assertEqual(state["status"], "stopped")
        self.assertEqual(state["stop_reason"], "balance_depleted")

    def test_same_source_can_host_two_plans_dispatching_independently(self):
        """同一来源建两个方案（普通跟单 + 过关滚仓）：都能创建、同一条信号各自独立派单、各记各的。"""
        roll_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"],
            strategy=dict(STRATEGY), name="组合马尔可夫 过三关",
        )
        fixed_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"],
            strategy={"play_filter": {"mode": "all"}, "staking_policy": {"mode": "fixed", "fixed_amount": 10.0}},
            name="组合马尔可夫 普通跟单",
        )
        self.assertNotEqual(roll_subscription["id"], fixed_subscription["id"])

        signal = self._signal(bet_type="combo", bet_value="大单")
        roll_result = dispatch_signal(self.repo, signal["id"], subscription_id=roll_subscription["id"])
        fixed_result = dispatch_signal(self.repo, signal["id"], subscription_id=fixed_subscription["id"])
        self.assertEqual(roll_result["created_count"], 2)   # 过关滚仓两腿
        self.assertEqual(fixed_result["created_count"], 1)  # 普通跟单单注
        self.assertAlmostEqual(fixed_result["jobs"][0]["stake_plan"]["amount"], 10.0)

    def test_route_rules_same_group_same_issue_dispatch_independently(self):
        """两条规则、两个方案、同一个群、同一期：各自派单、各自记账，互不影响。"""
        from pc28touzhu.services.auto_trigger_service import create_auto_trigger_rule

        fixed_source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="internal_ai", name="另一来源",
        )
        fixed_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=fixed_source["id"],
            strategy={"play_filter": {"mode": "all"}, "staking_policy": {"mode": "fixed", "fixed_amount": 10.0}},
            name="普通跟单方案",
        )
        issue_no = "20261103500"

        rule_a = create_auto_trigger_rule(
            self.repo, user_id=self.user_id,
            payload={
                "name": "过三关规则", "cooldown_issues": 0,
                "scope_mode": "selected_subscriptions", "subscription_ids": [self.subscription["id"]],
                "trigger_mode": "condition",
                "conditions": [{"type": "miss_streak", "metric": "odd_even", "window": "current",
                                 "operator": "gte", "threshold": 5, "min_sample_count": 100}],
                "daily_risk_control": {"enabled": False},
                "routes": [{"delivery_target_id": self.target["id"],
                             "route_risk_mode": "disabled", "subscription_risk_mode": "disabled"}],
            },
        )["item"]
        rule_b = create_auto_trigger_rule(
            self.repo, user_id=self.user_id,
            payload={
                "name": "普通规则", "cooldown_issues": 0,
                "scope_mode": "selected_subscriptions", "subscription_ids": [fixed_subscription["id"]],
                "trigger_mode": "condition",
                "conditions": [{"type": "miss_streak", "metric": "odd_even", "window": "current",
                                 "operator": "gte", "threshold": 5, "min_sample_count": 100}],
                "daily_risk_control": {"enabled": False},
                "routes": [{"delivery_target_id": self.target["id"],
                             "route_risk_mode": "disabled", "subscription_risk_mode": "disabled"}],
            },
        )["item"]

        issue_signal_a = self._signal(bet_type="combo", bet_value="大单", issue_no=issue_no)
        issue_signal_b = self.repo.create_signal_record(
            source_id=fixed_source["id"], lottery_type="pc28", issue_no=issue_no,
            bet_type="big_small", bet_value="大",
            published_at=datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        )
        run_a = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule_a["id"], user_id=self.user_id, subscription_id=self.subscription["id"], stat_date="2026-11-03",
        )
        run_b = self.repo.ensure_auto_trigger_rule_run(
            rule_id=rule_b["id"], user_id=self.user_id, subscription_id=fixed_subscription["id"], stat_date="2026-11-03",
        )
        result_a = dispatch_signal(self.repo, issue_signal_a["id"], subscription_id=self.subscription["id"],
                                    auto_trigger_context={"rule_id": rule_a["id"], "rule_run_id": run_a["id"],
                                                           "stat_date": run_a["stat_date"], "routes": rule_a["routes"]})
        result_b = dispatch_signal(self.repo, issue_signal_b["id"], subscription_id=fixed_subscription["id"],
                                    auto_trigger_context={"rule_id": rule_b["id"], "rule_run_id": run_b["id"],
                                                           "stat_date": run_b["stat_date"], "routes": rule_b["routes"]})
        self.assertEqual(result_a["created_count"], 2)  # 过关滚仓两腿
        self.assertEqual(result_b["created_count"], 1)  # 普通跟单单注
    def test_combo_signal_prices_each_leg_with_its_own_play(self):
        """组合信号触发的过关期：两条腿各自按自己的玩法算钱，不能按信号原始组合赔率算。

        满赔2.8 规则：单双 2.84，小单/大双 6.79，大单/小双 6.33。
        跟随信号取 小单 -> 结构 A（主注单 + 辅助小双）；15 大单 时主注中、辅助未中：
        主注应为 70 × (2.84 - 1) = 128.80，而不是 70 × (6.79 - 1) = 405.30。
        """
        strategy = {
            "play_filter": {
                "mode": "selected",
                "selected_keys": ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
            },
            "staking_policy": {**STRATEGY["staking_policy"], "structure": "follow_signal"},
            "settlement_policy": {
                "rule_source": "subscription_fixed",
                "settlement_rule_id": "pc28_fullpay_2_8_regular",
            },
        }
        subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=self.source["id"], strategy=strategy, name="过三关 跟随信号",
        )
        signal = self._signal(bet_type="combo", bet_value="小单")
        result = dispatch_signal(self.repo, signal["id"], subscription_id=subscription["id"])
        self.assertEqual(result["created_count"], 2)
        self.assertEqual({job["stake_plan"]["structure"] for job in result["jobs"]}, {"A"})

        self._deliver_and_settle(result, triplet=[5, 8, 2])
        legs = {}
        for job in result["jobs"]:
            legs[job["stake_plan"]["leg"]] = self.repo.get_progression_event(int(job["progression_event_id"]))
        self.assertEqual(legs["main"]["resolved_result_type"], "hit")
        self.assertEqual(legs["aux"]["resolved_result_type"], "miss")
        self.assertAlmostEqual(legs["main"]["net_delta"], 128.80)
        self.assertAlmostEqual(legs["aux"]["net_delta"], -30.0)
        state = self.repo.get_subscription_cycle_state(subscription_id=subscription["id"])
        self.assertAlmostEqual(state["balance"], 198.80)

    def test_old_fixed_mode_still_single_job(self):
        fixed_strategy = {
            "play_filter": {"mode": "all"},
            "staking_policy": {"mode": "fixed", "fixed_amount": 10.0},
        }
        fixed_source = self.repo.create_source_record(
            owner_user_id=self.user_id, source_type="internal_ai", name="普通跟单来源",
        )
        fixed_subscription = self.repo.create_subscription_record(
            user_id=self.user_id, source_id=fixed_source["id"], strategy=fixed_strategy,
        )
        fixed_signal = self._signal_for(fixed_source)
        result = dispatch_signal(self.repo, fixed_signal["id"], subscription_id=fixed_subscription["id"])
        self.assertEqual(result["created_count"], 1)
        self.assertAlmostEqual(result["jobs"][0]["stake_plan"]["amount"], 10.0)


if __name__ == "__main__":
    unittest.main()
