from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.telegram_runtime_settings_service import (
    get_effective_telegram_runtime_settings,
    get_telegram_runtime_settings_for_admin,
    update_pc28_auto_settlement_runtime_state,
    update_telegram_runtime_settings,
)


def build_runtime_stub():
    return SimpleNamespace(
        alert_notifier=SimpleNamespace(
            enabled=False,
            bot_token="env-alert-token",
            target_chat_id="-100env-alert",
            repeat_interval_seconds=1800,
            interval_seconds=30,
            once=True,
        ),
        telegram_bot=SimpleNamespace(
            enabled=False,
            bot_token="env-query-token",
            poll_interval_seconds=5,
            bind_token_ttl_seconds=600,
            once=True,
        ),
        telegram_report=SimpleNamespace(
            enabled=False,
            target_chat_id="-100env-report",
            interval_seconds=30,
            once=True,
            send_hour=9,
            send_minute=0,
            top_n=10,
            timezone="Asia/Shanghai",
        ),
        pc28_auto_settlement=SimpleNamespace(
            enabled=False,
            interval_seconds=30,
            once=True,
            draw_limit=60,
        ),
    )


class TelegramRuntimeSettingsServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.repo = DatabaseRepository(os.path.join(self.tmpdir.name, "settings.db"))
        self.repo.initialize_database()
        self.runtime = build_runtime_stub()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_effective_settings_fall_back_to_env_defaults(self):
        payload = get_effective_telegram_runtime_settings(self.repo, runtime_config=self.runtime)
        self.assertEqual(payload["source"], "env_default")
        self.assertEqual(payload["item"]["alert"]["bot_token"], "env-alert-token")
        self.assertEqual(payload["item"]["bot"]["bot_token"], "env-query-token")
        self.assertEqual(payload["item"]["report"]["target_chat_id"], "-100env-report")
        self.assertEqual(payload["item"]["auto_settlement"]["draw_limit"], 60)

    def test_update_settings_persists_real_values_and_returns_masked_values(self):
        payload = update_telegram_runtime_settings(
            self.repo,
            runtime_config=self.runtime,
            payload={
                "alert": {
                    "enabled": True,
                    "bot_token": "new-alert-token-123",
                    "target_chat_id": "-100alert",
                    "repeat_interval_seconds": 900,
                    "interval_seconds": 15,
                },
                "bot": {
                    "enabled": True,
                    "bot_token": "new-query-token-456",
                    "poll_interval_seconds": 7,
                    "bind_token_ttl_seconds": 1200,
                },
                "report": {
                    "enabled": True,
                    "target_chat_id": "-100report",
                    "interval_seconds": 45,
                    "send_hour": 11,
                    "send_minute": 35,
                    "top_n": 12,
                    "timezone": "Asia/Shanghai",
                },
                "auto_settlement": {
                    "enabled": True,
                    "interval_seconds": 20,
                    "draw_limit": 80,
                },
            },
        )
        self.assertTrue(payload["item"]["alert"]["has_bot_token"])
        self.assertNotIn("new-alert-token-123", payload["item"]["alert"]["bot_token_masked"])
        self.assertEqual(payload["item"]["report"]["top_n"], 12)
        self.assertTrue(payload["item"]["auto_settlement"]["enabled"])

        stored = get_effective_telegram_runtime_settings(self.repo, runtime_config=self.runtime)
        self.assertEqual(stored["source"], "database")
        self.assertEqual(stored["item"]["alert"]["bot_token"], "new-alert-token-123")
        self.assertEqual(stored["item"]["bot"]["bot_token"], "new-query-token-456")
        self.assertEqual(stored["item"]["report"]["send_minute"], 35)
        self.assertEqual(stored["item"]["auto_settlement"]["draw_limit"], 80)

    def test_blank_token_keeps_current_effective_token(self):
        update_telegram_runtime_settings(
            self.repo,
            runtime_config=self.runtime,
            payload={
                "alert": {
                    "enabled": True,
                    "bot_token": "",
                    "target_chat_id": "-100alert",
                    "repeat_interval_seconds": 900,
                    "interval_seconds": 15,
                },
                "bot": {
                    "enabled": True,
                    "bot_token": "",
                    "poll_interval_seconds": 8,
                    "bind_token_ttl_seconds": 900,
                },
                "report": {
                    "enabled": True,
                    "target_chat_id": "-100report",
                    "interval_seconds": 50,
                    "send_hour": 10,
                    "send_minute": 5,
                    "top_n": 8,
                    "timezone": "Asia/Shanghai",
                },
                "auto_settlement": {
                    "enabled": False,
                    "interval_seconds": 30,
                    "draw_limit": 60,
                },
            },
        )
        admin_view = get_telegram_runtime_settings_for_admin(self.repo, runtime_config=self.runtime)
        self.assertTrue(admin_view["item"]["alert"]["has_bot_token"])
        effective = get_effective_telegram_runtime_settings(self.repo, runtime_config=self.runtime)
        self.assertEqual(effective["item"]["alert"]["bot_token"], "env-alert-token")
        self.assertEqual(effective["item"]["bot"]["bot_token"], "env-query-token")

    def test_admin_view_includes_auto_settlement_runtime_state(self):
        update_pc28_auto_settlement_runtime_state(
            self.repo,
            last_run_at="2026-04-18T06:00:00Z",
            last_status="success",
            last_summary={"resolved_count": 3},
            last_error="",
        )
        admin_view = get_telegram_runtime_settings_for_admin(self.repo, runtime_config=self.runtime)
        runtime_state = admin_view["item"]["auto_settlement"]["runtime_state"]
        self.assertEqual(runtime_state["last_status"], "success")
        self.assertEqual(runtime_state["last_summary"]["resolved_count"], 3)


if __name__ == "__main__":
    unittest.main()
