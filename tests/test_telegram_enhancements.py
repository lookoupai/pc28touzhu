from __future__ import annotations

import os
import tempfile
import unittest

from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.telegram_bot_service import handle_telegram_command
from pc28touzhu.services.telegram_report_service import (
    build_monthly_profit_rankings,
    deliver_monthly_profit_report,
    run_daily_report_cycle,
)


class FakeSender:
    def __init__(self):
        self.sent = []

    def send_text(self, target_chat_id, message_text):
        self.sent.append((target_chat_id, message_text))
        return {"ok": True}


class TelegramEnhancementTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.repo = DatabaseRepository(os.path.join(self.tmpdir.name, "telegram.db"))
        self.repo.initialize_database()
        self.user = self.repo.create_user_record(username="monthly-user", email="", role="user", status="active")
        self.source = self.repo.create_source_record(owner_user_id=self.user["id"], source_type="internal_ai", name="手动方案")
        self.subscription = self.repo.create_subscription_record(user_id=self.user["id"], source_id=self.source["id"], strategy={})
        self.repo.update_user_telegram_binding(
            user_id=self.user["id"], telegram_user_id=777, telegram_chat_id="777",
            telegram_username="monthly_user", telegram_bound_at="2026-09-01T00:00:00Z",
        )
        self.sequence = 0

    def tearDown(self):
        self.tmpdir.cleanup()

    def _insert_manual_stat(self, stat_date, net_profit):
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_daily_stats(
                    stat_date, user_id, subscription_id, source_id,
                    profit_amount, loss_amount, net_profit, settled_event_count,
                    hit_count, miss_count, refund_count
                ) VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, 0)
                """,
                (stat_date, self.user["id"], self.subscription["id"], self.source["id"],
                 max(0, net_profit), max(0, -net_profit), net_profit,
                 1 if net_profit > 0 else 0, 1 if net_profit < 0 else 0),
            )
        self._insert_settlement(stat_date, net_profit)

    def _insert_auto_stat(self, rule_id, stat_date, net_profit):
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO auto_trigger_rule_daily_stats(
                    rule_id, user_id, stat_date, profit_amount, loss_amount,
                    net_profit, settled_event_count, hit_count, miss_count, refund_count
                ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?, 0)
                """,
                (rule_id, self.user["id"], stat_date, max(0, net_profit), max(0, -net_profit), net_profit,
                 1 if net_profit > 0 else 0, 1 if net_profit < 0 else 0),
            )
        self._insert_settlement(stat_date, net_profit, rule_id=rule_id)

    def _insert_settlement(self, stat_date, net_profit, *, rule_id=None, business_date=None):
        self.sequence += 1
        signal = self.repo.create_signal_record(
            source_id=self.source["id"], lottery_type="pc28", issue_no=str(self.sequence),
            bet_type="big_small", bet_value="大",
        )
        with self.repo._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_progression_events(
                    subscription_id, user_id, signal_id, status, resolved_result_type,
                    profit_delta, loss_delta, net_delta, settled_at,
                    auto_trigger_rule_id, auto_trigger_stat_date
                ) VALUES (?, ?, ?, 'settled', ?, ?, ?, ?, ?, ?, ?)
                """,
                (self.subscription["id"], self.user["id"], signal["id"],
                 "hit" if net_profit > 0 else "miss", max(0, net_profit), max(0, -net_profit), net_profit,
                 stat_date + "T04:00:00Z", rule_id, (business_date or stat_date) if rule_id else ""),
            )

    def test_monthly_queries_keep_manual_and_auto_separate(self):
        self._insert_manual_stat("2026-08-01", 12)
        self._insert_manual_stat("2026-08-15", -2)
        with self.repo._connect() as conn:
            rule_id = conn.execute(
                "INSERT INTO auto_trigger_rules(user_id, name) VALUES (?, ?)",
                (self.user["id"], "规则A"),
            ).lastrowid
        self._insert_auto_stat(rule_id, "2026-08-05", 30)

        manual = handle_telegram_command(
            self.repo, telegram_user_id=777, telegram_chat_id="777", telegram_username="monthly_user",
            text="/profit 上个月", reference_time=datetime(2026, 9, 9, tzinfo=timezone.utc),
        )
        auto = handle_telegram_command(
            self.repo, telegram_user_id=777, telegram_chat_id="777", telegram_username="monthly_user",
            text="/profit auto 上个月", reference_time=datetime(2026, 9, 9, tzinfo=timezone.utc),
        )
        plan = handle_telegram_command(
            self.repo, telegram_user_id=777, telegram_chat_id="777", telegram_username="monthly_user",
            text="/plan 上个月", reference_time=datetime(2026, 9, 9, tzinfo=timezone.utc),
        )
        rule = handle_telegram_command(
            self.repo, telegram_user_id=777, telegram_chat_id="777", telegram_username="monthly_user",
            text="/rule 上个月", reference_time=datetime(2026, 9, 9, tzinfo=timezone.utc),
        )
        self.assertIn("净利润: +10.00", manual)
        self.assertIn("净利润: +30.00", auto)
        self.assertIn("手动方案", plan)
        self.assertIn("规则A", rule)
        ranking = build_monthly_profit_rankings(self.repo, stat_month="2026-08")
        self.assertEqual(ranking["summary"]["total_net_profit"], 40)
        self.assertEqual(ranking["profit_ranking"][0]["manual_net_profit"], 10)
        self.assertEqual(ranking["profit_ranking"][0]["auto_net_profit"], 30)

    def test_auto_report_uses_settlement_month_and_ignores_mixed_legacy_stats(self):
        with self.repo._connect() as conn:
            rule_id = conn.execute(
                "INSERT INTO auto_trigger_rules(user_id, name) VALUES (?, ?)",
                (self.user["id"], "跨月规则"),
            ).lastrowid
            conn.execute(
                """
                INSERT INTO subscription_daily_stats(
                    stat_date, user_id, subscription_id, source_id,
                    profit_amount, net_profit, settled_event_count, hit_count
                ) VALUES ('2026-08-01', ?, ?, ?, 7, 7, 1, 1)
                """,
                (self.user["id"], self.subscription["id"], self.source["id"]),
            )
        self._insert_settlement("2026-08-01", 7, rule_id=rule_id, business_date="2026-07-31")

        self.assertEqual(self.repo.get_user_monthly_profit_summary(user_id=self.user["id"], stat_month="2026-08")["net_profit"], 0)
        self.assertEqual(self.repo.get_user_daily_profit_summary(user_id=self.user["id"], stat_date="2026-08-01")["settled_event_count"], 0)
        self.assertEqual(self.repo.get_user_monthly_auto_trigger_profit_summary(user_id=self.user["id"], stat_month="2026-07")["net_profit"], 0)
        self.assertEqual(self.repo.get_user_monthly_auto_trigger_profit_summary(user_id=self.user["id"], stat_month="2026-08")["net_profit"], 7)
        self.assertEqual(self.repo.get_user_daily_auto_trigger_profit_summary(user_id=self.user["id"], stat_date="2026-08-01")["net_profit"], 7)

        text = handle_telegram_command(
            self.repo, telegram_user_id=777, telegram_chat_id="777", telegram_username="monthly_user",
            text="/rule #%s 2026-08" % rule_id,
        )
        self.assertIn("跨月规则", text)
        self.assertIn("净利润: +7.00", text)

    def test_monthly_report_is_idempotent_and_runs_on_month_start(self):
        self._insert_manual_stat("2026-08-01", 8)
        sender = FakeSender()
        result = run_daily_report_cycle(
            self.repo, sender=sender, target_chat_id="-100", send_hour=9, send_minute=0, top_n=10,
            timezone_name="Asia/Shanghai", reference_time=datetime(2026, 9, 1, 2, 0, tzinfo=timezone.utc),
        )
        self.assertEqual(result["monthly"]["delivery_status"], "sent")
        self.assertEqual(len(sender.sent), 1)

        sender = FakeSender()
        first = deliver_monthly_profit_report(
            self.repo, sender=sender, target_chat_id="-100", stat_month="2026-08", top_n=10,
        )
        second = deliver_monthly_profit_report(
            self.repo, sender=sender, target_chat_id="-100", stat_month="2026-08", top_n=10,
        )
        self.assertTrue(second["skipped"])
        self.assertTrue(first["skipped"])
        self.assertEqual(len(sender.sent), 0)


if __name__ == "__main__":
    unittest.main()
