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
    get_telegram_bot_commands,
    handle_telegram_command,
    process_telegram_bot_cycle,
    sync_telegram_bot_commands,
)


class FakeBotClient:
    def __init__(self, updates=None):
        self.updates = list(updates or [])
        self.sent = []
        self.commands = []

    def send_text(self, target_chat_id: str, message_text: str):
        self.sent.append((target_chat_id, message_text))
        return {"target_chat_id": target_chat_id, "text": message_text}

    def get_updates(self, *, offset=None, timeout_seconds=10, limit=100):
        if offset is None:
            return list(self.updates[:limit])
        return [item for item in self.updates if int(item.get("update_id") or 0) >= int(offset)][:limit]

    def set_my_commands(self, commands, *, scope=None, language_code=None):
        self.commands.append(
            {
                "commands": [dict(item) for item in commands],
                "scope": dict(scope or {}),
                "language_code": language_code,
            }
        )
        return {"ok": True}


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

    def test_start_and_help_return_help_text(self):
        help_text = handle_telegram_command(
            self.repo,
            telegram_user_id=1,
            telegram_chat_id="1",
            telegram_username="help_user",
            text="/start",
        )
        alias_text = handle_telegram_command(
            self.repo,
            telegram_user_id=1,
            telegram_chat_id="1",
            telegram_username="help_user",
            text="/help",
        )

        self.assertIn("/subs 查看跟单方案列表", help_text)
        self.assertIn("/restart <订阅ID> 开始新一轮", help_text)
        self.assertEqual(help_text, alias_text)

    def test_profit_and_plan_without_date_prefer_today_data(self):
        user, today_stat_date = self._seed_daily_stat()
        self.repo.update_user_telegram_binding(
            user_id=user["id"],
            telegram_user_id=9002,
            telegram_chat_id="9002",
            telegram_username="fallback_user",
            telegram_bound_at="2026-04-18T00:00:00Z",
        )
        reference_time = (
            datetime.strptime(today_stat_date, "%Y-%m-%d").replace(tzinfo=timezone(timedelta(hours=8)), hour=12)
        ).astimezone(timezone.utc)

        summary_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9002,
            telegram_chat_id="9002",
            telegram_username="fallback_user",
            text="/profit",
            reference_time=reference_time,
        )
        list_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9002,
            telegram_chat_id="9002",
            telegram_username="fallback_user",
            text="/plan",
            reference_time=reference_time,
        )
        detail_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9002,
            telegram_chat_id="9002",
            telegram_username="fallback_user",
            text="/plan 方案A",
            reference_time=reference_time,
        )

        self.assertIn(today_stat_date, summary_text)
        self.assertIn("净利润: +10.00", summary_text)
        self.assertIn(today_stat_date, list_text)
        self.assertIn("方案A", list_text)
        self.assertIn(today_stat_date, detail_text)
        self.assertIn("净利润: +10.00", detail_text)

    def test_profit_and_plan_without_date_fallback_to_yesterday_data(self):
        user, stat_date = self._seed_daily_stat()
        self.repo.update_user_telegram_binding(
            user_id=user["id"],
            telegram_user_id=9003,
            telegram_chat_id="9003",
            telegram_username="yesterday_user",
            telegram_bound_at="2026-04-18T00:00:00Z",
        )
        reference_time = (
            datetime.strptime(stat_date, "%Y-%m-%d").replace(tzinfo=timezone(timedelta(hours=8)), hour=12)
            + timedelta(days=1)
        ).astimezone(timezone.utc)

        summary_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9003,
            telegram_chat_id="9003",
            telegram_username="yesterday_user",
            text="/profit",
            reference_time=reference_time,
        )
        list_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9003,
            telegram_chat_id="9003",
            telegram_username="yesterday_user",
            text="/plan",
            reference_time=reference_time,
        )

        self.assertIn(stat_date, summary_text)
        self.assertIn("净利润: +10.00", summary_text)
        self.assertIn(stat_date, list_text)
        self.assertIn("方案A", list_text)

    def test_sync_telegram_bot_commands_uses_private_scope(self):
        client = FakeBotClient()

        result = sync_telegram_bot_commands(client)

        self.assertTrue(result["ok"])
        self.assertEqual(client.commands[0]["scope"], {})
        self.assertEqual(client.commands[1]["scope"]["type"], "all_private_chats")
        self.assertEqual(client.commands[0]["commands"], get_telegram_bot_commands())
        self.assertEqual(client.commands[1]["commands"], get_telegram_bot_commands())

    def test_status_command_returns_summary(self):
        user, _ = self._seed_daily_stat()
        self.repo.update_user_telegram_binding(
            user_id=user["id"],
            telegram_user_id=9010,
            telegram_chat_id="9010",
            telegram_username="status_user",
            telegram_bound_at="2026-04-19T00:00:00Z",
        )

        text = handle_telegram_command(
            self.repo,
            telegram_user_id=9010,
            telegram_chat_id="9010",
            telegram_username="status_user",
            text="/status",
        )

        self.assertIn("当前跟单状态", text)
        self.assertIn("策略总数: 1", text)
        self.assertIn("运行中: 1", text)
        self.assertIn("手动停用: 0", text)
        self.assertIn("待结算: 0", text)
        self.assertIn("本轮已实现净盈亏合计: +10.00", text)
        self.assertIn("待结算金额合计: 0.00", text)

    def test_status_command_returns_plan_detail(self):
        user = self.repo.create_user_record(username="detail-user", email="", role="user", status="active")
        source = self.repo.create_source_record(
            owner_user_id=user["id"],
            source_type="internal_ai",
            name="方案B",
        )
        subscription = self.repo.create_subscription_record(
            user_id=user["id"],
            source_id=source["id"],
            strategy={
                "mode": "martingale",
                "base_stake": 10,
                "multiplier": 2,
                "max_steps": 3,
            },
        )
        signal = self.repo.create_signal_record(
            source_id=source["id"],
            lottery_type="pc28",
            issue_no="20260418001",
            bet_type="big_small",
            bet_value="小",
        )
        event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user["id"],
            signal_id=signal["id"],
            issue_no="20260418001",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=2,
            max_steps=3,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        self.repo.update_user_telegram_binding(
            user_id=user["id"],
            telegram_user_id=9011,
            telegram_chat_id="9011",
            telegram_username="status_detail_user",
            telegram_bound_at="2026-04-19T00:00:00Z",
        )

        text = handle_telegram_command(
            self.repo,
            telegram_user_id=9011,
            telegram_chat_id="9011",
            telegram_username="status_detail_user",
            text="/status 方案B",
        )

        self.assertIn("当前方案状态", text)
        self.assertIn("方案: 方案B", text)
        self.assertIn("待结算期号: 20260418001", text)
        self.assertIn("待结算状态: placed", text)
        self.assertIn("当前待结算金额: 10.00", text)
        self.assertIn("当前手数: 1", text)
        self.assertIn("本轮已实现净盈亏: 0.00", text)

    def test_status_command_handles_empty_subscriptions(self):
        user = self.repo.create_user_record(username="empty-user", email="", role="user", status="active")
        self.repo.update_user_telegram_binding(
            user_id=user["id"],
            telegram_user_id=9012,
            telegram_chat_id="9012",
            telegram_username="empty_status_user",
            telegram_bound_at="2026-04-19T00:00:00Z",
        )

        text = handle_telegram_command(
            self.repo,
            telegram_user_id=9012,
            telegram_chat_id="9012",
            telegram_username="empty_status_user",
            text="/status",
        )

        self.assertEqual("当前没有跟单策略。", text)

    def test_subscription_commands_list_toggle_play_and_restart(self):
        user = self.repo.create_user_record(username="ops-user", email="", role="user", status="active")
        source = self.repo.create_source_record(
            owner_user_id=user["id"],
            source_type="internal_ai",
            name="方案C",
        )
        subscription = self.repo.create_subscription_record(
            user_id=user["id"],
            source_id=source["id"],
            status="inactive",
            strategy={
                "play_filter": {"mode": "selected", "selected_keys": ["big_small:大", "big_small:小"]},
                "staking_policy": {"mode": "fixed", "fixed_amount": 20},
                "settlement_policy": {"rule_source": "follow_signal", "fallback_profit_ratio": 1.0},
                "risk_control": {"enabled": True, "profit_target": 0, "loss_limit": 20},
                "dispatch": {"expire_after_seconds": 120},
            },
        )
        self.repo.update_user_telegram_binding(
            user_id=user["id"],
            telegram_user_id=9013,
            telegram_chat_id="9013",
            telegram_username="ops_user",
            telegram_bound_at="2026-04-19T00:00:00Z",
        )

        subs_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9013,
            telegram_chat_id="9013",
            telegram_username="ops_user",
            text="/subs",
        )
        self.assertIn("方案C", subs_text)
        self.assertIn("玩法：大小", subs_text)

        enable_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9013,
            telegram_chat_id="9013",
            telegram_username="ops_user",
            text="/enable %s" % subscription["id"],
        )
        self.assertIn("已启动跟单方案", enable_text)
        self.assertEqual(self.repo.get_subscription(subscription["id"])["status"], "active")

        play_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9013,
            telegram_chat_id="9013",
            telegram_username="ops_user",
            text="/play %s 单双" % subscription["id"],
        )
        self.assertIn("玩法已切换", play_text)
        self.assertEqual(
            self.repo.get_subscription(subscription["id"])["strategy_v2"]["play_filter"]["selected_keys"],
            ["odd_even:单", "odd_even:双"],
        )

        signal = self.repo.create_signal_record(
            source_id=source["id"],
            lottery_type="pc28",
            issue_no="20260419001",
            bet_type="big_small",
            bet_value="大",
        )
        event = self.repo.create_progression_event_record(
            subscription_id=subscription["id"],
            user_id=user["id"],
            signal_id=signal["id"],
            issue_no="20260419001",
            progression_step=1,
            stake_amount=20,
            base_stake=20,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        self.repo.settle_progression_event(
            subscription_id=subscription["id"],
            user_id=user["id"],
            result_type="miss",
            progression_event_id=event["id"],
        )

        blocked_enable_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9013,
            telegram_chat_id="9013",
            telegram_username="ops_user",
            text="/enable %s" % subscription["id"],
        )
        self.assertIn("/restart %s" % subscription["id"], blocked_enable_text)

        restart_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9013,
            telegram_chat_id="9013",
            telegram_username="ops_user",
            text="/restart %s" % subscription["id"],
        )
        self.assertIn("已开始新一轮", restart_text)
        restarted = self.repo.get_subscription(subscription["id"])
        self.assertEqual(restarted["status"], "active")
        self.assertEqual(restarted["financial"]["net_profit"], 0.0)
        self.assertEqual(restarted["financial"]["threshold_status"], "")

        disable_text = handle_telegram_command(
            self.repo,
            telegram_user_id=9013,
            telegram_chat_id="9013",
            telegram_username="ops_user",
            text="/disable %s" % subscription["id"],
        )
        self.assertIn("已关闭跟单方案", disable_text)
        self.assertEqual(self.repo.get_subscription(subscription["id"])["status"], "inactive")

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
