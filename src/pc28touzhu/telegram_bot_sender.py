from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict


class TelegramBotSender:
    def __init__(self, *, bot_token: str):
        self.bot_token = str(bot_token or "").strip()

    def send_text(self, target_chat_id: str, message_text: str) -> Dict[str, Any]:
        if not self.bot_token:
            raise ValueError("ALERT_TELEGRAM_BOT_TOKEN 未配置")
        chat_id = str(target_chat_id or "").strip()
        if not chat_id:
            raise ValueError("target_chat_id 不能为空")

        payload = json.dumps(
            {
                "chat_id": chat_id,
                "text": str(message_text or ""),
                "disable_web_page_preview": True,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        request = urllib.request.Request(
            "https://api.telegram.org/bot%s/sendMessage" % self.bot_token,
            data=payload,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read().decode("utf-8")
        result = json.loads(raw) if raw else {}
        if not isinstance(result, dict) or not result.get("ok"):
            description = result.get("description") if isinstance(result, dict) else None
            raise RuntimeError(str(description or "Telegram Bot API 请求失败"))
        message = result.get("result") or {}
        return {
            "message_id": message.get("message_id"),
            "chat_id": str((message.get("chat") or {}).get("id") or chat_id),
            "text": message_text,
            "target_chat_id": chat_id,
        }
