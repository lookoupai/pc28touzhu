from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.telegram_report_service import (
    build_daily_profit_rankings,
    build_daily_profit_report_text,
    deliver_daily_profit_report,
    run_daily_report_cycle,
)


class FakeSender:
    def __init__(self):
        self.sent = []

    def send_text(self, target_chat_id: str, message_text: str):
        self.sent.append((target_chat_id, message_text))
        return {"target_chat_id": target_chat_id, "text": message_text}


class TelegramReportServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.repo = DatabaseRepository(os.path.join(self.tmpdir.name, "report.db"))
        self.repo.initialize_database()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _seed_stat(self, *, username: str, source_name: str, result_type: str, stake_amount: float):
        user = self.repo.create_user_record(username=username, email="", role="user", status="active")
        source = self.repo.create_source_record(
            owner_user_id=user["id"],
            source_type="internal_ai",
            name=source_name,
        )
        subscription = self.repo.create_subscription_record(
            user_id=user["id"],
            source_id=source["id"],
            strategy={
                "mode": "follow",
                "stake_amount": stake_amount,
                "risk_control": {"enabled": True, "win_profit_ratio": 1.0},
            },
        )
        signal = self.repo.create_signal_record(
            source_id=source["id"],
            lottery_type="pc28",
            issue_no="20260417001",
            bet_type="big_small",
            bet_value="大",
        )
        event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user["id"],
            signal_id=signal["id"],
            issue_no="20260417001",
            progression_step=1,
            stake_amount=stake_amount,
            base_stake=stake_amount,
            multiplier=2,
            max_steps=3,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user["id"],
            result_type=result_type,
            progression_event_id=event["id"],
        )

    def test_build_and_deliver_daily_profit_report(self):
        self._seed_stat(username="winner_user", source_name="方案A", result_type="hit", stake_amount=18)
        self._seed_stat(username="loser_user", source_name="方案B", result_type="miss", stake_amount=9)
        stat_date = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")

        ranking = build_daily_profit_rankings(self.repo, stat_date=stat_date, top_n=10)
        self.assertEqual(ranking["summary"]["settled_user_count"], 2)
        self.assertEqual(ranking["profit_ranking"][0]["username"], "winner_user")
        self.assertEqual(ranking["loss_ranking"][0]["username"], "loser_user")

        text = build_daily_profit_report_text(
            stat_date=stat_date,
            summary=ranking["summary"],
            profit_ranking=ranking["profit_ranking"],
            loss_ranking=ranking["loss_ranking"],
        )
        self.assertIn("盈利榜", text)
        self.assertIn("亏损榜", text)
        self.assertIn("w***", text)
        self.assertIn("l***", text)

        sender = FakeSender()
        first = deliver_daily_profit_report(
            self.repo,
            sender=sender,
            target_chat_id="-100report",
            stat_date=stat_date,
            top_n=10,
        )
        self.assertEqual(first["delivery_status"], "sent")
        self.assertEqual(len(sender.sent), 1)

        second = deliver_daily_profit_report(
            self.repo,
            sender=sender,
            target_chat_id="-100report",
            stat_date=stat_date,
            top_n=10,
        )
        self.assertTrue(second["skipped"])
        self.assertEqual(second["reason"], "already_sent")

    def test_run_daily_report_cycle_skips_before_schedule(self):
        sender = FakeSender()
        result = run_daily_report_cycle(
            self.repo,
            sender=sender,
            target_chat_id="-100report",
            send_hour=23,
            send_minute=59,
            top_n=10,
            timezone_name="Asia/Shanghai",
            reference_time=datetime(2026, 4, 17, 10, 0, tzinfo=timezone.utc),
        )
        self.assertTrue(result["skipped"])
        self.assertEqual(result["reason"], "before_schedule")


if __name__ == "__main__":
    unittest.main()
