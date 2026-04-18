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
        self.assertEqual(fetch_count["value"], 1)
        self.assertEqual(result["summary"]["user_count"], 2)
        self.assertEqual(result["summary"]["resolved_count"], 2)
        self.assertEqual(result["summary"]["refund_count"], 1)
        self.assertEqual(result["summary"]["hit_count"], 1)


if __name__ == "__main__":
    unittest.main()
