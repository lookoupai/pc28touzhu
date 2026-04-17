from __future__ import annotations

import json
import urllib.request
from typing import Any, Dict, List, Optional


class TelegramBotSender:
    def __init__(self, *, bot_token: str):
        self.bot_token = str(bot_token or "").strip()

    def _require_bot_token(self) -> None:
        if not self.bot_token:
            raise ValueError("TG_BOT_TOKEN 未配置")

    def _request(self, method: str, *, payload: Optional[Dict[str, Any]] = None, timeout: int = 10) -> Dict[str, Any]:
        self._require_bot_token()
        body = None
        if payload is not None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = urllib.request.Request(
            "https://api.telegram.org/bot%s/%s" % (self.bot_token, method),
            data=body,
            method="POST",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8")
        result = json.loads(raw) if raw else {}
        if not isinstance(result, dict) or not result.get("ok"):
            description = result.get("description") if isinstance(result, dict) else None
            raise RuntimeError(str(description or "Telegram Bot API 请求失败"))
        return result

    def send_text(self, target_chat_id: str, message_text: str) -> Dict[str, Any]:
        chat_id = str(target_chat_id or "").strip()
        if not chat_id:
            raise ValueError("target_chat_id 不能为空")

        result = self._request(
            "sendMessage",
            payload={
                "chat_id": chat_id,
                "text": str(message_text or ""),
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        message = result.get("result") or {}
        return {
            "message_id": message.get("message_id"),
            "chat_id": str((message.get("chat") or {}).get("id") or chat_id),
            "text": message_text,
            "target_chat_id": chat_id,
        }

    def get_updates(
        self,
        *,
        offset: Optional[int] = None,
        timeout_seconds: int = 10,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {
            "timeout": max(0, int(timeout_seconds or 0)),
            "limit": max(1, min(int(limit or 100), 100)),
            "allowed_updates": ["message"],
        }
        if offset is not None:
            payload["offset"] = int(offset)
        result = self._request(
            "getUpdates",
            payload=payload,
            timeout=max(10, int(timeout_seconds or 0) + 5),
        )
        items = result.get("result") or []
        return [dict(item) for item in items if isinstance(item, dict)]
