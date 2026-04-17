from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.telegram_bot_service import (
    create_telegram_bind_token,
    handle_telegram_command,
    process_telegram_bot_cycle,
)


class FakeBotClient:
    def __init__(self, updates=None):
        self.updates = list(updates or [])
        self.sent = []

    def send_text(self, target_chat_id: str, message_text: str):
        self.sent.append((target_chat_id, message_text))
        return {"target_chat_id": target_chat_id, "text": message_text}

    def get_updates(self, *, offset=None, timeout_seconds=10, limit=100):
        if offset is None:
            return list(self.updates[:limit])
        return [item for item in self.updates if int(item.get("update_id") or 0) >= int(offset)][:limit]


class TelegramBotServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.repo = DatabaseRepository(os.path.join(self.tmpdir.name, "bot.db"))
        self.repo.initialize_database()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _seed_daily_stat(self, *, username: str = "profit-user", stake_amount: float = 10.0):
        user = self.repo.create_user_record(username=username, email="", role="user", status="active")
        source = self.repo.create_source_record(
            owner_user_id=user["id"],
            source_type="internal_ai",
            name="方案A",
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
            result_type="hit",
            progression_event_id=event["id"],
        )
        stat_date = datetime.now(timezone.utc).astimezone(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        return user, stat_date

    def test_bind_command_and_profit_queries(self):
        user, stat_date = self._seed_daily_stat()
        token_result = create_telegram_bind_token(
            self.repo,
            user_id=user["id"],
            ttl_seconds=600,
            reference_time=datetime(2026, 4, 17, 0, 0, tzinfo=timezone.utc),
        )
        bind_token = token_result["item"]["bind_token"]

        bind_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9001,
            telegram_chat_id="9001",
            telegram_username="query_user",
            text="/bind %s" % bind_token,
            reference_time=datetime(2026, 4, 17, 0, 1, tzinfo=timezone.utc),
        )
        self.assertIn("绑定成功", bind_text)

        summary_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9001,
            telegram_chat_id="9001",
            telegram_username="query_user",
            text="/profit %s" % stat_date,
        )
        self.assertIn("跟单汇总", summary_text)
        self.assertIn("净利润: +10.00", summary_text)

        list_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9001,
            telegram_chat_id="9001",
            telegram_username="query_user",
            text="/plan %s" % stat_date,
        )
        self.assertIn("方案A", list_text)

        detail_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9001,
            telegram_chat_id="9001",
            telegram_username="query_user",
            text="/plan 方案A %s" % stat_date,
        )
        self.assertIn("方案收益", detail_text)
        self.assertIn("净利润: +10.00", detail_text)

    def test_process_cycle_replies_and_updates_runtime_state(self):
        user = self.repo.create_user_record(username="cycle-user", email="", role="user", status="active")
        self.repo.update_user_telegram_binding(
            user_id=user["id"],
            telegram_user_id=7001,
            telegram_chat_id="7001",
            telegram_username="cycle_user",
            telegram_bound_at="2026-04-17T00:00:00Z",
        )
        client = FakeBotClient(
            updates=[
                {
                    "update_id": 10,
                    "message": {
                        "text": "/start",
                        "chat": {"id": 7001, "type": "private"},
                        "from": {"id": 7001, "username": "cycle_user"},
                    },
                },
                {
                    "update_id": 11,
                    "message": {
                        "text": "/profit 2026-04-17",
                        "chat": {"id": -1001, "type": "group"},
                        "from": {"id": 7001, "username": "cycle_user"},
                    },
                },
            ]
        )

        result = process_telegram_bot_cycle(
            self.repo,
            bot_client=client,
            poll_timeout_seconds=1,
        )

        self.assertEqual(result["update_count"], 2)
        self.assertEqual(result["handled_count"], 1)
        self.assertEqual(result["replied_count"], 1)
        self.assertEqual(result["ignored_count"], 1)
        self.assertEqual(self.repo.get_telegram_bot_runtime_state(bot_name="profit-query-bot")["last_update_id"], 11)
        self.assertIn("/bind <绑定码>", client.sent[0][1])


if __name__ == "__main__":
    unittest.main()
