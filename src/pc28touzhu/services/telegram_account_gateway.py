from __future__ import annotations

from pathlib import Path
from typing import Any, Dict


class TelethonAccountGateway:
    def __init__(self, *, api_id: int, api_hash: str):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash or "").strip()

    def _load_client_class(self):
        if self.api_id <= 0:
            raise ValueError("TELEGRAM_API_ID 未配置")
        if not self.api_hash:
            raise ValueError("TELEGRAM_API_HASH 未配置")
        try:
            from telethon.sync import TelegramClient
        except ImportError as exc:
            raise RuntimeError("未安装 Telethon，请先安装 `Telethon>=1.42,<2`") from exc
        return TelegramClient

    def _connect_client(self, session_path: str):
        client_class = self._load_client_class()
        normalized_path = Path(str(session_path or "").strip()).expanduser()
        normalized_path.parent.mkdir(parents=True, exist_ok=True)
        client = client_class(str(normalized_path), self.api_id, self.api_hash)
        client.connect()
        return client

    def inspect_session(self, session_path: str) -> Dict[str, Any]:
        client = self._connect_client(session_path)
        try:
            authorized = bool(client.is_user_authorized())
            result: Dict[str, Any] = {"authorized": authorized}
            if not authorized:
                return result
            me = client.get_me()
            result["phone"] = str(getattr(me, "phone", "") or "")
            first_name = str(getattr(me, "first_name", "") or "").strip()
            last_name = str(getattr(me, "last_name", "") or "").strip()
            username = str(getattr(me, "username", "") or "").strip()
            display_name = " ".join(part for part in [first_name, last_name] if part).strip()
            result["display_name"] = display_name or username or result["phone"] or ""
            return result
        finally:
            client.disconnect()

    def send_login_code(self, session_path: str, phone: str) -> Dict[str, Any]:
        client = self._connect_client(session_path)
        try:
            sent = client.send_code_request(str(phone or "").strip())
            return {
                "phone_code_hash": str(getattr(sent, "phone_code_hash", "") or ""),
            }
        finally:
            client.disconnect()

    def verify_code(self, session_path: str, *, phone: str, code: str, phone_code_hash: str) -> Dict[str, Any]:
        client = self._connect_client(session_path)
        try:
            try:
                client.sign_in(
                    phone=str(phone or "").strip(),
                    code=str(code or "").strip(),
                    phone_code_hash=str(phone_code_hash or "").strip(),
                )
            except Exception as exc:
                exc_name = exc.__class__.__name__
                if exc_name == "SessionPasswordNeededError":
                    return {"authorized": False, "password_required": True}
                raise

            return self.inspect_session(session_path)
        finally:
            client.disconnect()

    def verify_password(self, session_path: str, *, password: str) -> Dict[str, Any]:
        client = self._connect_client(session_path)
        try:
            client.sign_in(password=str(password or ""))
            return self.inspect_session(session_path)
        finally:
            client.disconnect()
