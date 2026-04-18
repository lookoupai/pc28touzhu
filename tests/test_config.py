from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from pc28touzhu.config import get_runtime_config


class RuntimeConfigTests(unittest.TestCase):
    def test_env_file_populates_all_sections(self):
        keys = [
            "HOST",
            "PORT",
            "DATABASE_PATH",
            "EXECUTOR_API_TOKEN",
            "EXECUTOR_STALE_AFTER_SECONDS",
            "EXECUTOR_OFFLINE_AFTER_SECONDS",
            "AUTO_RETRY_MAX_ATTEMPTS",
            "AUTO_RETRY_BASE_DELAY_SECONDS",
            "ALERT_FAILURE_STREAK_THRESHOLD",
            "ALERT_TELEGRAM_ENABLED",
            "ALERT_TELEGRAM_BOT_TOKEN",
            "ALERT_TELEGRAM_TARGET_CHAT_ID",
            "ALERT_NOTIFY_REPEAT_SECONDS",
            "ALERT_NOTIFIER_INTERVAL_SECONDS",
            "ALERT_NOTIFIER_ONCE",
            "TG_BOT_ENABLED",
            "TG_BOT_TOKEN",
            "TG_BOT_POLL_INTERVAL_SECONDS",
            "TG_BOT_BIND_TOKEN_TTL_SECONDS",
            "TG_BOT_ONCE",
            "TG_REPORT_ENABLED",
            "TG_REPORT_TARGET_CHAT_ID",
            "TG_REPORT_INTERVAL_SECONDS",
            "TG_REPORT_ONCE",
            "TG_REPORT_SEND_HOUR",
            "TG_REPORT_SEND_MINUTE",
            "TG_REPORT_TOP_N",
            "TG_REPORT_TIMEZONE",
            "SOURCE_SYNC_ENABLED",
            "SOURCE_SYNC_INTERVAL_SECONDS",
            "SOURCE_SYNC_ONCE",
            "PLATFORM_BASE_URL",
            "EXECUTOR_ID",
            "PULL_LIMIT",
            "ONCE",
            "ISSUE_NO",
            "BET_TYPE",
            "BET_VALUE",
            "TARGET_KEY",
            "IDEMPOTENCY_KEY",
            "MESSAGE_TEXT",
            "STAKE_AMOUNT",
        ]
        original_env = {key: os.environ.get(key) for key in keys}

        with tempfile.TemporaryDirectory() as tmpdir:
            env_path = Path(tmpdir) / ".env"
            env_path.write_text(
                "\n".join(
                    [
                        "HOST=127.0.0.1",
                        "PORT=35200",
                        "DATABASE_PATH=test.db",
                        "EXECUTOR_API_TOKEN=test-token",
                        "EXECUTOR_STALE_AFTER_SECONDS=45",
                        "EXECUTOR_OFFLINE_AFTER_SECONDS=240",
                        "AUTO_RETRY_MAX_ATTEMPTS=4",
                        "AUTO_RETRY_BASE_DELAY_SECONDS=20",
                        "ALERT_FAILURE_STREAK_THRESHOLD=5",
                        "ALERT_TELEGRAM_ENABLED=true",
                        "ALERT_TELEGRAM_BOT_TOKEN=bot-token-123",
                        "ALERT_TELEGRAM_TARGET_CHAT_ID=-100alert",
                        "ALERT_NOTIFY_REPEAT_SECONDS=900",
                        "ALERT_NOTIFIER_INTERVAL_SECONDS=15",
                        "ALERT_NOTIFIER_ONCE=false",
                        "TG_BOT_ENABLED=true",
                        "TG_BOT_TOKEN=query-bot-token",
                        "TG_BOT_POLL_INTERVAL_SECONDS=7",
                        "TG_BOT_BIND_TOKEN_TTL_SECONDS=1200",
                        "TG_BOT_ONCE=false",
                        "TG_REPORT_ENABLED=true",
                        "TG_REPORT_TARGET_CHAT_ID=-100report",
                        "TG_REPORT_INTERVAL_SECONDS=45",
                        "TG_REPORT_ONCE=false",
                        "TG_REPORT_SEND_HOUR=11",
                        "TG_REPORT_SEND_MINUTE=35",
                        "TG_REPORT_TOP_N=12",
                        "TG_REPORT_TIMEZONE=Asia/Shanghai",
                        "SOURCE_SYNC_ENABLED=true",
                        "SOURCE_SYNC_INTERVAL_SECONDS=25",
                        "SOURCE_SYNC_ONCE=false",
                        "PLATFORM_BASE_URL=http://127.0.0.1:35200",
                        "EXECUTOR_ID=test-exec",
                        "PULL_LIMIT=25",
                        "ONCE=false",
                        "ISSUE_NO=20260407088",
                        "BET_TYPE=combo",
                        "BET_VALUE=大单",
                        "TARGET_KEY=-100888",
                        "IDEMPOTENCY_KEY=test-idemp",
                        "MESSAGE_TEXT=大单88",
                        "STAKE_AMOUNT=88.5",
                    ]
                ),
                encoding="utf-8",
            )

            try:
                for key in keys:
                    os.environ.pop(key, None)

                config = get_runtime_config(env_path)
                self.assertEqual(config.platform.host, "127.0.0.1")
                self.assertEqual(config.platform.port, 35200)
                self.assertEqual(config.platform.database_path, str((Path(tmpdir) / "test.db").resolve()))
                self.assertEqual(config.platform.executor_api_token, "test-token")
                self.assertEqual(config.platform.executor_stale_after_seconds, 45)
                self.assertEqual(config.platform.executor_offline_after_seconds, 240)
                self.assertEqual(config.platform.auto_retry_max_attempts, 4)
                self.assertEqual(config.platform.auto_retry_base_delay_seconds, 20)
                self.assertEqual(config.platform.alert_failure_streak_threshold, 5)
                self.assertEqual(config.executor.platform_base_url, "http://127.0.0.1:35200")
                self.assertEqual(config.executor.executor_id, "test-exec")
                self.assertEqual(config.executor.pull_limit, 25)
                self.assertFalse(config.executor.once)
                self.assertTrue(config.executor.telegram_session.endswith("telegram-session"))
                self.assertEqual(config.demo_seed.issue_no, "20260407088")
                self.assertEqual(config.demo_seed.bet_type, "combo")
                self.assertEqual(config.demo_seed.bet_value, "大单")
                self.assertEqual(config.demo_seed.target_key, "-100888")
                self.assertEqual(config.demo_seed.idempotency_key, "test-idemp")
                self.assertEqual(config.demo_seed.message_text, "大单88")
                self.assertEqual(config.demo_seed.stake_amount, 88.5)
                self.assertTrue(config.alert_notifier.enabled)
                self.assertEqual(config.alert_notifier.bot_token, "bot-token-123")
                self.assertEqual(config.alert_notifier.target_chat_id, "-100alert")
                self.assertEqual(config.alert_notifier.repeat_interval_seconds, 900)
                self.assertEqual(config.alert_notifier.interval_seconds, 15)
                self.assertFalse(config.alert_notifier.once)
                self.assertTrue(config.telegram_bot.enabled)
                self.assertEqual(config.telegram_bot.bot_token, "query-bot-token")
                self.assertEqual(config.telegram_bot.poll_interval_seconds, 7)
                self.assertEqual(config.telegram_bot.bind_token_ttl_seconds, 1200)
                self.assertFalse(config.telegram_bot.once)
                self.assertTrue(config.telegram_report.enabled)
                self.assertEqual(config.telegram_report.target_chat_id, "-100report")
                self.assertEqual(config.telegram_report.interval_seconds, 45)
                self.assertFalse(config.telegram_report.once)
                self.assertEqual(config.telegram_report.send_hour, 11)
                self.assertEqual(config.telegram_report.send_minute, 35)
                self.assertEqual(config.telegram_report.top_n, 12)
                self.assertEqual(config.telegram_report.timezone, "Asia/Shanghai")
                self.assertTrue(config.source_sync.enabled)
                self.assertEqual(config.source_sync.interval_seconds, 25)
                self.assertFalse(config.source_sync.once)
            finally:
                for key, value in original_env.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value


if __name__ == "__main__":
    unittest.main()
