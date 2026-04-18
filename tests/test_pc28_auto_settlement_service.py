from __future__ import annotations

import os
import tempfile
import unittest

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.pc28_auto_settlement_service import run_pc28_auto_settlement_cycle


class PC28AutoSettlementServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_run_cycle_skips_when_no_pending_progressions(self):
        result = run_pc28_auto_settlement_cycle(self.repo, draw_limit=20, fetcher=lambda *args, **kwargs: {})
        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "no_pending_progressions")

    def test_run_cycle_resolves_pending_progressions_for_multiple_users(self):
        for index, issue_no in enumerate(("20260418001", "20260418002"), start=1):
            user_id = self.repo.create_user("auto-user-%s" % index)
            source_id = self.repo.create_source_record(
                owner_user_id=user_id,
                source_type="internal_ai",
                name="auto-source-%s" % index,
            )["id"]
            account_id = self.repo.create_telegram_account_record(
                user_id=user_id,
                label="执行号-%s" % index,
                phone="+1201888%s" % index,
                session_path="/data/auto-worker/%s" % index,
                status="active",
            )["id"]
            self.repo.create_subscription_record(
                user_id=user_id,
                source_id=source_id,
                strategy={
                    "play_filter": {"mode": "all", "selected_keys": []},
                    "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                    "settlement_policy": {
                        "rule_source": "subscription_fixed",
                        "settlement_rule_id": "pc28_high_regular",
                        "fallback_profit_ratio": 1.0,
                    },
                    "risk_control": {"enabled": False, "profit_target": 0, "loss_limit": 0},
                    "dispatch": {"expire_after_seconds": 120},
                },
            )
            self.repo.create_delivery_target_record(
                user_id=user_id,
                telegram_account_id=account_id,
                executor_type="telegram_group",
                target_key="-10055%s" % index,
                target_name="自动结算群-%s" % index,
                status="active",
            )
            signal = self.repo.create_signal_record(
                source_id=source_id,
                lottery_type="pc28",
                issue_no=issue_no,
                bet_type="big_small",
                bet_value="大" if index == 1 else "小",
                normalized_payload={"profit_rule_id": "pc28_high", "odds_profile": "regular"},
            )
            dispatch_result = dispatch_signal(self.repo, signal["id"])
            self.assertEqual(dispatch_result["created_count"], 1)
            event = self.repo.get_progression_event(dispatch_result["jobs"][0]["progression_event_id"])
            self.repo.update_progression_event_status(progression_event_id=event["id"], status="placed")

        fetch_count = {"value": 0}

        def fake_fetcher(url, params=None, headers=None, timeout=10):
            fetch_count["value"] += 1
            return {
                "message": "success",
                "data": [
                    {"nbr": "20260418001", "num": "14", "number": "4+4+6"},
                    {"nbr": "20260418002", "num": "11", "number": "2+4+5"},
                ],
            }

        result = run_pc28_auto_settlement_cycle(self.repo, draw_limit=20, fetcher=fake_fetcher)
        self.assertFalse(result["skipped"])
        self.assertGreaterEqual(fetch_count["value"], 1)
        self.assertEqual(result["summary"]["user_count"], 2)
        self.assertEqual(result["summary"]["resolved_count"], 2)
        self.assertEqual(result["summary"]["refund_count"], 1)
        self.assertEqual(result["summary"]["hit_count"], 1)

    def test_run_cycle_backfills_all_placed_events_for_same_subscription(self):
        user_id = self.repo.create_user("auto-backfill-user")
        source_id = self.repo.create_source_record(
            owner_user_id=user_id,
            source_type="internal_ai",
            name="auto-backfill-source",
        )["id"]
        subscription = self.repo.create_subscription_record(
            user_id=user_id,
            source_id=source_id,
            strategy={
                "play_filter": {"mode": "all", "selected_keys": []},
                "staking_policy": {"mode": "fixed", "fixed_amount": 1},
                "settlement_policy": {
                    "rule_source": "subscription_fixed",
                    "settlement_rule_id": "pc28_high_regular",
                    "fallback_profit_ratio": 1.0,
                },
                "risk_control": {"enabled": False, "profit_target": 0, "loss_limit": 0},
                "dispatch": {"expire_after_seconds": 120},
            },
        )

        for issue_no, bet_value in (("20260418001", "大"), ("20260418002", "大"), ("20260418003", "小")):
            signal = self.repo.create_signal_record(
                source_id=source_id,
                lottery_type="pc28",
                issue_no=issue_no,
                bet_type="big_small",
                bet_value=bet_value,
                normalized_payload={"profit_rule_id": "pc28_high", "odds_profile": "regular"},
            )
            self.repo.create_progression_event_record(
                subscription_id=subscription["id"],
                user_id=user_id,
                signal_id=signal["id"],
                issue_no=issue_no,
                progression_step=1,
                stake_amount=1,
                base_stake=1,
                multiplier=2,
                max_steps=3,
                refund_action="hold",
                cap_action="reset",
                status="placed",
            )

        def fake_fetcher(url, params=None, headers=None, timeout=10):
            if "pc28.help" in url:
                return {
                    "message": "success",
                    "data": [
                        {"nbr": "20260418003", "num": "13", "number": "1+5+7"},
                        {"nbr": "20260418002", "num": "11", "number": "2+4+5"},
                        {"nbr": "20260418001", "num": "15", "number": "1+5+9"},
                    ],
                }
            raise AssertionError("不应请求其他来源")

        result = run_pc28_auto_settlement_cycle(self.repo, draw_limit=20, fetcher=fake_fetcher)
        self.assertFalse(result["skipped"])
        self.assertEqual(result["summary"]["pending_count"], 3)
        self.assertEqual(result["summary"]["resolved_count"], 3)
        self.assertEqual(result["summary"]["hit_count"], 1)
        self.assertEqual(result["summary"]["refund_count"], 1)
        self.assertEqual(result["summary"]["miss_count"], 1)

        settled_rows = self.repo.list_open_progression_events(user_id=user_id, statuses=["placed"])
        self.assertEqual(settled_rows, [])
        subscription_state = self.repo.get_subscription(subscription["id"])
        self.assertEqual(subscription_state["financial"]["realized_profit"], 1.85)
        self.assertEqual(subscription_state["financial"]["realized_loss"], 1)
        self.assertEqual(subscription_state["financial"]["net_profit"], 0.85)


if __name__ == "__main__":
    unittest.main()
