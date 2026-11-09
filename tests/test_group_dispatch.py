from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.platform_service import resolve_subscription_progression
from pc28touzhu.services.platform_service import list_execution_jobs, list_recent_execution_failures, resolve_pending_pc28_progressions_from_draws


class GroupDispatchTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = DatabaseRepository(str(Path(self.tmp.name) / "groups.db"))
        self.repo.initialize_database()
        self.user = self.repo.create_user("group-user")
        self.source = self.repo.create_source_record(owner_user_id=self.user, source_type="internal_ai", name="组合来源")
        self.target = self.repo.create_delivery_target_record(user_id=self.user, executor_type="telegram_group", target_key="-1001", status="active")
        self.strategy = {
            "play_filter": {"mode": "selected", "selected_keys": ["kill_group:小双", "double_group:大单+小双"]},
            "staking_policy": {"mode": "martingale", "base_stake": 10, "multiplier": 2, "max_steps": 5},
            "settlement_policy": {"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_fullpay_2_0_regular"},
            "risk_control": {"enabled": False},
        }
        self.sub = self.repo.create_subscription_record(user_id=self.user, source_id=self.source["id"], strategy=self.strategy)

    def signal(self, metric="kill_group", value="小双", issue="1001"):
        return self.repo.create_signal_record(
            source_id=self.source["id"], lottery_type="pc28", issue_no=issue,
            bet_type=metric, bet_value=value,
            normalized_payload={"message_text": "来源旧单注不应发送10"},
        )

    def dispatch(self, **kwargs):
        signal = self.signal(**kwargs)
        result = dispatch_signal(self.repo, signal["id"])
        self.assertEqual(result["created_count"], 1)
        return signal, result["jobs"][0]

    def deliver(self, job):
        self.repo.report_job_result(
            job_id=str(job["id"]), executor_id="offline", attempt_no=1, delivery_status="delivered",
            remote_message_id="local-only", executed_at=datetime.now(timezone.utc).isoformat(), raw_result={}, error_message=None,
        )

    def resolve(self, job, number):
        return resolve_subscription_progression(
            self.repo, subscription_id=self.sub["id"], user_id=self.user,
            payload={"progression_event_id": job["progression_event_id"], "draw_context": {"result_number": number}},
        )

    def test_group_is_one_complete_delivery_and_one_frozen_event(self):
        signal, job = self.dispatch()
        self.assertEqual(job["planned_message_text"], "大单10 大双10 小单10")
        self.assertEqual(job["stake_plan"]["amount"], 30)
        self.assertEqual(job["stake_plan"]["per_combo_amount"], 10)
        self.assertEqual(len(job["stake_plan"]["legs"]), 3)
        event = self.repo.get_progression_event(job["progression_event_id"])
        self.assertEqual(event["stake_amount"], 30)
        self.assertEqual(event["settlement_snapshot"]["group"]["legs"], job["stake_plan"]["legs"])
        repeat = dispatch_signal(self.repo, signal["id"])
        self.assertEqual(repeat["created_count"], 0)
        pulled = self.repo.pull_ready_jobs(executor_id="offline")
        self.assertEqual(len(pulled), 1)
        self.assertEqual(pulled[0]["message_text"], "大单10 大双10 小单10")
        self.assertEqual(pulled[0]["stake_plan"]["amount"], 30)

    def test_covered_partial_refund_advances_once_but_counts_prediction_hit(self):
        _, job = self.dispatch()
        self.deliver(job)
        result = self.resolve(job, 13)
        event = result["progression"]["event"]
        self.assertEqual(event["net_delta"], -20)
        self.assertTrue(event["prediction_hit"])
        self.assertEqual(event["resolved_result_type"], "miss")
        self.assertEqual(result["progression"]["state"]["current_step"], 2)
        self.assertEqual(self.repo.get_subscription_financial_state(self.sub["id"])["net_profit"], -20)
        with self.repo._connect() as conn:
            report = dict(conn.execute("SELECT * FROM profit_report_daily_stats WHERE subscription_id = ?", (self.sub["id"],)).fetchone())
        self.assertEqual((report["settled_event_count"], report["hit_count"], report["miss_count"], report["net_profit"]), (1, 1, 0, -20))
        self.resolve(job, 13)
        self.repo.initialize_database()
        self.resolve(job, 13)
        self.assertEqual(self.repo.get_subscription_financial_state(self.sub["id"])["net_profit"], -20)
        _, next_job = self.dispatch(issue="1002")
        self.assertEqual(next_job["planned_message_text"], "大单20 大双20 小单20")
        self.assertEqual(next_job["stake_plan"]["amount"], 60)

    def test_normal_win_books_all_legs_once_and_resets_progression(self):
        _, job = self.dispatch()
        self.deliver(job)
        event = self.resolve(job, 15)["progression"]["event"]
        self.assertEqual((event["profit_delta"], event["loss_delta"], event["net_delta"]), (33.2, 20, 13.2))
        self.assertEqual(self.repo.get_subscription_progression_state(self.sub["id"])["current_step"], 1)
        self.assertEqual(len(event["result_context"]["group_settlement"]["legs"]), 3)

    def test_miss_books_total_stake_and_double_group_sends_two_bets(self):
        _, job = self.dispatch(metric="double_group", value="小双+大单")
        self.assertEqual(job["planned_message_text"], "大单10 小双10")
        self.deliver(job)
        event = self.resolve(job, 14)["progression"]["event"]
        self.assertEqual(event["net_delta"], -20)
        self.assertFalse(event["prediction_hit"])

    def test_manual_unqualified_hit_is_rejected_without_financial_mutation(self):
        _, job = self.dispatch()
        self.deliver(job)
        with self.assertRaisesRegex(ValueError, "开奖"):
            self.repo.settle_progression_event(
                subscription_id=self.sub["id"], user_id=self.user,
                progression_event_id=job["progression_event_id"], result_type="hit",
            )
        self.assertEqual(self.repo.get_progression_event(job["progression_event_id"])["status"], "placed")
        self.assertEqual(self.repo.get_subscription_financial_state(self.sub["id"])["net_profit"], 0)

    def test_frozen_group_settles_after_source_and_subscription_change(self):
        signal, job = self.dispatch()
        self.deliver(job)
        with self.repo._connect() as conn:
            conn.execute("UPDATE normalized_signals SET bet_value = '大单' WHERE id = ?", (signal["id"],))
        event = self.resolve(job, 15)["progression"]["event"]
        self.assertEqual(event["net_delta"], 13.2)
        self.assertEqual(event["result_context"]["group_settlement"]["excluded_combo"], "小双")

    def test_legacy_all_does_not_create_additional_bets(self):
        self.repo.update_subscription_record(subscription_id=self.sub["id"], user_id=self.user, source_id=self.source["id"], strategy={
            **self.strategy, "play_filter": {"mode": "all", "selected_keys": []},
        })
        result = dispatch_signal(self.repo, self.signal()["id"])
        self.assertEqual(result["created_count"], 0)

    def test_group_failed_send_retries_whole_message_without_new_event(self):
        _, job = self.dispatch()
        self.repo.report_job_result(
            job_id=str(job["id"]), executor_id="offline", attempt_no=1, delivery_status="failed",
            remote_message_id=None, executed_at=datetime.now(timezone.utc).isoformat(), raw_result={}, error_message="offline failure",
        )
        self.repo.retry_execution_job(job_id=job["id"], user_id=self.user)
        pulled = self.repo.pull_ready_jobs(executor_id="offline")
        self.assertEqual(len(pulled), 1)
        self.assertEqual(pulled[0]["message_text"], "大单10 大双10 小单10")
        self.assertEqual(self.repo.get_execution_job(job["id"])["progression_event_id"], job["progression_event_id"])

    def test_unknown_send_and_expiry_are_not_retryable(self):
        _, job = self.dispatch()
        self.repo.report_job_result(
            job_id=str(job["id"]), executor_id="offline", attempt_no=1, delivery_status="skipped",
            remote_message_id=None, executed_at=datetime.now(timezone.utc).isoformat(), raw_result={"reason": "send_unconfirmed"}, error_message="unknown",
        )
        with self.assertRaises(ValueError):
            self.repo.retry_execution_job(job_id=job["id"], user_id=self.user)
        _, failed = self.dispatch(issue="1002")
        self.repo.report_job_result(
            job_id=str(failed["id"]), executor_id="offline", attempt_no=1, delivery_status="failed",
            remote_message_id=None, executed_at=datetime.now(timezone.utc).isoformat(), raw_result={}, error_message="failure",
        )
        self.assertEqual(self.repo.get_progression_event(failed["progression_event_id"])["status"], "pending")
        with self.repo._connect() as conn:
            conn.execute("UPDATE execution_jobs SET expire_at = '2000-01-01T00:00:00Z' WHERE id = ?", (failed["id"],))
        self.repo.expire_due_jobs()
        self.assertEqual(self.repo.get_progression_event(failed["progression_event_id"])["status"], "void")
        with self.assertRaises(ValueError):
            self.repo.retry_execution_job(job_id=failed["id"], user_id=self.user)

    def test_late_failure_cannot_make_delivered_group_retryable(self):
        _, job = self.dispatch()
        self.deliver(job)
        self.repo.report_job_result(
            job_id=str(job["id"]), executor_id="offline", attempt_no=2, delivery_status="failed",
            remote_message_id=None, executed_at=datetime.now(timezone.utc).isoformat(), raw_result={}, error_message="late failure",
        )
        self.assertEqual(self.repo.get_execution_job(job["id"])["status"], "delivered")
        with self.assertRaises(ValueError):
            self.repo.retry_execution_job(job_id=job["id"], user_id=self.user)

    def test_auto_retry_preserves_deadline_and_waits_for_unsettled_group(self):
        _, job = self.dispatch()
        self.repo.report_job_result(
            job_id=str(job["id"]), executor_id="offline", attempt_no=1, delivery_status="failed",
            remote_message_id=None, executed_at=(datetime.now(timezone.utc) - timedelta(seconds=40)).isoformat(), raw_result={}, error_message="failure",
        )
        self.assertTrue(list_execution_jobs(self.repo, user_id=self.user)["items"][0]["can_retry"])
        self.assertTrue(list_recent_execution_failures(self.repo, user_id=self.user)["items"][0]["can_retry"])
        result = self.repo.requeue_auto_retry_jobs(max_attempts=3, base_delay_seconds=5)
        self.assertEqual(len(result), 1)
        self.assertEqual(result[0]["expire_at"], job["expire_at"])

    def test_financial_stop_uses_group_net_profit_not_winning_leg_profit(self):
        self.repo.update_subscription_record(subscription_id=self.sub["id"], user_id=self.user, source_id=self.source["id"], strategy={
            **self.strategy, "risk_control": {"enabled": True, "profit_target": 20, "loss_limit": 50},
        })
        _, job = self.dispatch()
        self.deliver(job)
        self.resolve(job, 15)
        financial = self.repo.get_subscription_financial_state(self.sub["id"])
        self.assertEqual(financial["net_profit"], 13.2)
        self.assertEqual(financial["threshold_status"], "")

    def test_group_specific_loss_limit_and_batch_stats_use_different_outcomes(self):
        self.repo.update_subscription_record(subscription_id=self.sub["id"], user_id=self.user, source_id=self.source["id"], strategy={
            **self.strategy, "risk_control": {"enabled": True, "loss_limit": 100, "play_limits": {"kill_group": {"loss_limit": 15}}},
        })
        _, job = self.dispatch()
        self.deliver(job)
        result = resolve_pending_pc28_progressions_from_draws(self.repo, user_id=self.user, draw_items=[{"issue_no": "1001", "draw_context": {"result_number": 13}}])
        self.assertEqual((result["summary"]["hit_count"], result["summary"]["miss_count"]), (1, 0))
        self.assertEqual(result["items"][0]["financial_result"], "miss")
        self.assertEqual(self.repo.get_subscription_financial_state(self.sub["id"])["threshold_status"], "loss_limit_hit")

    def test_group_cannot_be_settled_by_other_user_or_forged_profit(self):
        _, job = self.dispatch()
        self.deliver(job)
        other = self.repo.create_user("other-user")
        with self.assertRaises(ValueError):
            self.repo.settle_progression_event(subscription_id=self.sub["id"], user_id=other,
                progression_event_id=job["progression_event_id"], result_type="hit",
                result_context={"draw_snapshot": {"result_number": 13}, "group_settlement": {"net_delta": 1000}})
        settled = self.repo.settle_progression_event(subscription_id=self.sub["id"], user_id=self.user,
            progression_event_id=job["progression_event_id"], result_type="hit",
            result_context={"draw_snapshot": {"result_number": 13}, "prediction_hit": False, "group_settlement": {"net_delta": 1000}})
        self.assertEqual(settled["event"]["net_delta"], -20)
        self.assertTrue(settled["event"]["prediction_hit"])

    def test_group_settlement_preserves_special_flags_without_triplet(self):
        self.repo.update_subscription_record(subscription_id=self.sub["id"], user_id=self.user, source_id=self.source["id"], strategy={
            **self.strategy, "settlement_policy": {"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_high_regular"},
        })
        _, job = self.dispatch()
        self.deliver(job)
        result = resolve_subscription_progression(self.repo, subscription_id=self.sub["id"], user_id=self.user,
            payload={"progression_event_id": job["progression_event_id"], "draw_context": {"result_number": 15, "is_pair": True}})
        self.assertEqual(result["progression"]["event"]["net_delta"], -20)


if __name__ == "__main__":
    unittest.main()
