from __future__ import annotations

import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.alert_notification_service import (
    build_alert_notification_text,
    deliver_platform_alerts,
)


class FakeAlertSender:
    def __init__(self):
        self.sent = []

    def send_text(self, target_chat_id: str, message_text: str):
        self.sent.append((target_chat_id, message_text))
        return {"message_id": "alert-001", "target_chat_id": target_chat_id, "text": message_text}


class AlertNotificationServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "alerts.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_build_alert_notification_text_includes_core_fields(self):
        text = build_alert_notification_text(
            {
                "notification_event": "firing",
                "title": "执行器离线",
                "message": "executor-1 offline",
                "alert_type": "executor_offline",
                "severity": "critical",
                "metadata": {"executor_id": "executor-1"},
            }
        )
        self.assertIn("告警触发", text)
        self.assertIn("执行器离线", text)
        self.assertIn("executor-1", text)

    def test_deliver_platform_alerts_marks_record_as_sent(self):
        sender = FakeAlertSender()
        result = deliver_platform_alerts(
            self.repo,
            alerts=[
                {
                    "alert_key": "executor_offline:test",
                    "alert_type": "executor_offline",
                    "severity": "critical",
                    "title": "执行器离线",
                    "message": "executor-1 offline",
                    "metadata": {"executor_id": "executor-1"},
                }
            ],
            sender=sender,
            target_chat_id="-100alert",
            repeat_interval_seconds=900,
        )
        self.assertEqual(result["pending_count"], 1)
        self.assertEqual(result["sent_count"], 1)
        self.assertEqual(sender.sent[0][0], "-100alert")
        record = self.repo.get_platform_alert_record("executor_offline:test")
        self.assertIsNotNone(record)
        self.assertEqual(record["send_count"], 1)
        self.assertIsNotNone(record["last_sent_at"])


if __name__ == "__main__":
    unittest.main()
