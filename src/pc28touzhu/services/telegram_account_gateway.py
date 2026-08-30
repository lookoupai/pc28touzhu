from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict

from pc28touzhu.runtime_environment import (
    build_telethon_missing_message,
    ensure_telethon_session_writable,
    reset_telethon_session_file,
)


def _extract_flood_wait_seconds(exc: Exception) -> int:
    seconds = getattr(exc, "seconds", None)
    try:
        value = int(seconds)
        if value > 0:
            return value
    except (TypeError, ValueError):
        pass
    match = re.search(r"wait of\s+(\d+)\s+seconds", str(exc or ""), re.I)
    if match:
        return int(match.group(1))
    return 0


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
            raise RuntimeError(build_telethon_missing_message()) from exc
        return TelegramClient

    def _connect_client(self, session_path: str):
        client_class = self._load_client_class()
        normalized_path = ensure_telethon_session_writable(session_path)
        normalized_path.parent.mkdir(parents=True, exist_ok=True)
        client = client_class(str(normalized_path), self.api_id, self.api_hash)
        client.connect()
        return client

    def _build_authorization_result(self, client) -> Dict[str, Any]:
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

    def inspect_session(self, session_path: str) -> Dict[str, Any]:
        client = self._connect_client(session_path)
        try:
            return self._build_authorization_result(client)
        finally:
            client.disconnect()

    def send_login_code(self, session_path: str, phone: str) -> Dict[str, Any]:
        last_error: Exception | None = None
        restart_errors = {
            "AuthRestartError",
            "AuthKeyUnregisteredError",
            "AuthKeyDuplicatedError",
            "SessionRevokedError",
            "SessionExpiredError",
        }
        for attempt in range(2):
            reset_telethon_session_file(session_path)
            client = self._connect_client(session_path)
            try:
                sent = client.send_code_request(str(phone or "").strip())
                return {
                    "phone_code_hash": str(getattr(sent, "phone_code_hash", "") or ""),
                    "session_reset": True,
                }
            except Exception as exc:
                last_error = exc
                if attempt == 0 and exc.__class__.__name__ in restart_errors:
                    continue
                raise
            finally:
                client.disconnect()
        if last_error is not None:
            raise last_error
        raise RuntimeError("发送验证码失败")

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
                if exc_name in {"PhoneCodeExpiredError", "PhoneCodeInvalidError"}:
                    return {"authorized": False, "code_invalid": exc_name == "PhoneCodeInvalidError", "login_expired": exc_name == "PhoneCodeExpiredError"}
                if exc_name in {"AuthRestartError", "SrpIdInvalidError", "AuthKeyUnregisteredError"}:
                    return {"authorized": False, "login_expired": True}
                raise

            return self._build_authorization_result(client)
        finally:
            client.disconnect()

    def verify_password(self, session_path: str, *, password: str) -> Dict[str, Any]:
        client = self._connect_client(session_path)
        try:
            try:
                client.sign_in(password=str(password or ""))
            except Exception as exc:
                exc_name = exc.__class__.__name__
                if exc_name == "PasswordHashInvalidError":
                    return {"authorized": False, "password_invalid": True}
                if exc_name == "PasswordEmptyError":
                    return {"authorized": False, "password_empty": True}
                if exc_name == "FloodWaitError" or "wait of" in str(exc).lower():
                    return {
                        "authorized": False,
                        "password_flood": True,
                        "wait_seconds": _extract_flood_wait_seconds(exc),
                    }
                if exc_name in {"SrpIdInvalidError", "AuthRestartError"}:
                    return {"authorized": False, "login_expired": True}
                raise
            return self._build_authorization_result(client)
        finally:
            client.disconnect()
