from __future__ import annotations

import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.telegram_bot_sender import TelegramBotSender


class FakeResponse:
    def __init__(self, payload: dict):
        self._payload = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    def read(self) -> bytes:
        return self._payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return None


class TelegramBotSenderTests(unittest.TestCase):
    def test_send_text_calls_bot_api(self):
        captured = {}

        def fake_urlopen(request, timeout=0):
            captured["url"] = request.full_url
            captured["body"] = json.loads(request.data.decode("utf-8"))
            captured["timeout"] = timeout
            return FakeResponse(
                {
                    "ok": True,
                    "result": {
                        "message_id": 123,
                        "chat": {"id": -1009001},
                    },
                }
            )

        with patch("urllib.request.urlopen", fake_urlopen):
            sender = TelegramBotSender(bot_token="bot-token")
            result = sender.send_text("-1009001", "告警测试")

        self.assertIn("/botbot-token/sendMessage", captured["url"])
        self.assertEqual(captured["body"]["chat_id"], "-1009001")
        self.assertEqual(captured["body"]["text"], "告警测试")
        self.assertEqual(result["message_id"], 123)
        self.assertEqual(result["chat_id"], "-1009001")

    def test_send_text_supports_reply_markup(self):
        captured = {}

        def fake_urlopen(request, timeout=0):
            captured["body"] = json.loads(request.data.decode("utf-8"))
            return FakeResponse({"ok": True, "result": {"message_id": 1, "chat": {"id": 9001}}})

        with patch("urllib.request.urlopen", fake_urlopen):
            sender = TelegramBotSender(bot_token="bot-token")
            sender.send_text(
                "9001",
                "按钮测试",
                reply_markup={"inline_keyboard": [[{"text": "打开", "callback_data": "sub:1:1"}]]},
            )

        self.assertIn("reply_markup", captured["body"])
        self.assertEqual(captured["body"]["reply_markup"]["inline_keyboard"][0][0]["callback_data"], "sub:1:1")

    def test_send_text_raises_on_bot_api_error(self):
        def fake_urlopen(request, timeout=0):
            return FakeResponse({"ok": False, "description": "chat not found"})

        with patch("urllib.request.urlopen", fake_urlopen):
            sender = TelegramBotSender(bot_token="bot-token")
            with self.assertRaises(RuntimeError) as ctx:
                sender.send_text("-1009001", "告警测试")

        self.assertIn("chat not found", str(ctx.exception))

    def test_get_updates_calls_bot_api(self):
        captured = {}

        def fake_urlopen(request, timeout=0):
            captured["url"] = request.full_url
            captured["body"] = json.loads(request.data.decode("utf-8"))
            captured["timeout"] = timeout
            return FakeResponse(
                {
                    "ok": True,
                    "result": [
                        {
                            "update_id": 99,
                            "message": {"message_id": 10, "text": "/start"},
                        }
                    ],
                }
            )

        with patch("urllib.request.urlopen", fake_urlopen):
            sender = TelegramBotSender(bot_token="bot-token")
            result = sender.get_updates(offset=50, timeout_seconds=7, limit=20)

        self.assertIn("/botbot-token/getUpdates", captured["url"])
        self.assertEqual(captured["body"]["offset"], 50)
        self.assertEqual(captured["body"]["timeout"], 7)
        self.assertEqual(captured["body"]["limit"], 20)
        self.assertEqual(captured["body"]["allowed_updates"], ["message", "callback_query"])
        self.assertEqual(result[0]["update_id"], 99)

    def test_edit_text_calls_bot_api(self):
        captured = {}

        def fake_urlopen(request, timeout=0):
            captured["url"] = request.full_url
            captured["body"] = json.loads(request.data.decode("utf-8"))
            return FakeResponse({"ok": True, "result": {"message_id": 321, "chat": {"id": 9001}}})

        with patch("urllib.request.urlopen", fake_urlopen):
            sender = TelegramBotSender(bot_token="bot-token")
            result = sender.edit_text(
                "9001",
                321,
                "更新后的内容",
                reply_markup={"inline_keyboard": [[{"text": "返回", "callback_data": "subs:1"}]]},
            )

        self.assertIn("/botbot-token/editMessageText", captured["url"])
        self.assertEqual(captured["body"]["message_id"], 321)
        self.assertEqual(captured["body"]["reply_markup"]["inline_keyboard"][0][0]["callback_data"], "subs:1")
        self.assertEqual(result["message_id"], 321)

    def test_answer_callback_query_calls_bot_api(self):
        captured = {}

        def fake_urlopen(request, timeout=0):
            captured["url"] = request.full_url
            captured["body"] = json.loads(request.data.decode("utf-8"))
            return FakeResponse({"ok": True, "result": True})

        with patch("urllib.request.urlopen", fake_urlopen):
            sender = TelegramBotSender(bot_token="bot-token")
            result = sender.answer_callback_query("cbq-1", text="已刷新")

        self.assertIn("/botbot-token/answerCallbackQuery", captured["url"])
        self.assertEqual(captured["body"]["callback_query_id"], "cbq-1")
        self.assertEqual(captured["body"]["text"], "已刷新")
        self.assertTrue(result["ok"])

    def test_set_my_commands_calls_bot_api(self):
        captured = {}

        def fake_urlopen(request, timeout=0):
            captured["url"] = request.full_url
            captured["body"] = json.loads(request.data.decode("utf-8"))
            captured["timeout"] = timeout
            return FakeResponse({"ok": True, "result": True})

        with patch("urllib.request.urlopen", fake_urlopen):
            sender = TelegramBotSender(bot_token="bot-token")
            result = sender.set_my_commands(
                [
                    {"command": "start", "description": "查看帮助"},
                    {"command": "profit", "description": "查询汇总"},
                ],
                scope={"type": "all_private_chats"},
            )

        self.assertIn("/botbot-token/setMyCommands", captured["url"])
        self.assertEqual(captured["body"]["commands"][0]["command"], "start")
        self.assertEqual(captured["body"]["scope"]["type"], "all_private_chats")
        self.assertTrue(result["ok"])


if __name__ == "__main__":
    unittest.main()
