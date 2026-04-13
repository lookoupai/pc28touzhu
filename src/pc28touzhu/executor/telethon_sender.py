from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from .models import ExecutorJob


def _coerce_entity(value: str) -> Any:
    text = str(value).strip()
    if not text:
        raise ValueError("target_key 不能为空")
    if text.lstrip("-").isdigit():
        return int(text)
    return text


def _build_numeric_candidates(value: str) -> list[int]:
    text = str(value).strip()
    if not text.lstrip("-").isdigit():
        return []
    number = int(text)
    candidates = [number]
    if text.startswith("-100") and text[4:].isdigit():
        candidates.append(int(text[4:]))
    elif number > 0:
        candidates.append(int("-100%s" % number))
    return list(dict.fromkeys(candidates))


class TelethonMessageSender:
    def __init__(self, *, api_id: int, api_hash: str, phone: str, session: str):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash or "").strip()
        self.phone = str(phone or "").strip()
        self.session = str(session or "").strip() or "telegram-session"
        self._client = None

    def connect(self) -> None:
        if self.api_id <= 0:
            raise ValueError("TELEGRAM_API_ID 未配置")
        if not self.api_hash:
            raise ValueError("TELEGRAM_API_HASH 未配置")

        try:
            from telethon.sync import TelegramClient
        except ImportError as exc:
            raise RuntimeError("未安装 Telethon，请先安装 `Telethon>=1.42,<2`") from exc

        session_path = Path(self.session).expanduser()
        session_path.parent.mkdir(parents=True, exist_ok=True)
        client = TelegramClient(self.session, self.api_id, self.api_hash)
        client.connect()
        if not client.is_user_authorized():
            disconnect = getattr(client, "disconnect", None)
            if callable(disconnect):
                disconnect()
            raise ValueError("当前 session 未授权，请先在账号管理中完成登录或导入有效 Session")
        self._client = client

    def disconnect(self) -> None:
        if self._client is not None:
            self._client.disconnect()
            self._client = None

    def _resolve_entity(self, target_key: str) -> Any:
        if self._client is None:
            raise RuntimeError("Telethon client 尚未连接")

        candidates = [_coerce_entity(target_key)]
        candidates.extend(_build_numeric_candidates(target_key))

        for candidate in candidates:
            try:
                return self._client.get_input_entity(candidate)
            except Exception:
                continue

        try:
            dialogs = list(self._client.get_dialogs())
        except Exception:
            dialogs = []

        for candidate in candidates:
            try:
                return self._client.get_input_entity(candidate)
            except Exception:
                continue

        text_key = str(target_key).strip()
        string_candidates = {text_key}
        string_candidates.update(str(item) for item in _build_numeric_candidates(target_key))

        for dialog in dialogs:
            dialog_candidates = set()
            dialog_id = getattr(dialog, "id", None)
            if dialog_id is not None:
                dialog_candidates.add(str(dialog_id))
            entity = getattr(dialog, "entity", None)
            entity_id = getattr(entity, "id", None)
            if entity_id is not None:
                dialog_candidates.add(str(entity_id))
                dialog_candidates.add("-100%s" % entity_id)

            if dialog_candidates.intersection(string_candidates):
                return entity if entity is not None else dialog

        raise ValueError(
            "无法解析目标群组实体，请确认账号已加入该群，并优先使用 @username、邀请链接或已加入群的有效 ID"
        )

    def send_text(self, target_key: str, message_text: str) -> Dict[str, Any]:
        if self._client is None:
            raise RuntimeError("Telethon client 尚未连接")
        entity = self._resolve_entity(target_key)
        message = self._client.send_message(entity, message_text)
        return {
            "message_id": getattr(message, "id", None),
            "chat_id": getattr(message, "chat_id", None),
            "text": message_text,
            "target_key": target_key,
        }


class TelethonSenderPool:
    def __init__(self, *, api_id: int, api_hash: str, default_phone: str = "", default_session: str = "telegram-session"):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash or "").strip()
        self.default_phone = str(default_phone or "").strip()
        self.default_session = str(default_session or "").strip() or "telegram-session"
        self._senders: Dict[str, TelethonMessageSender] = {}

    def _account_key(self, job: ExecutorJob) -> str:
        if job.telegram_account and job.telegram_account.id is not None:
            return "account:%s" % job.telegram_account.id
        return "default"

    def _build_sender(self, job: ExecutorJob) -> TelethonMessageSender:
        account = job.telegram_account
        session = account.session_path if account and account.session_path else self.default_session
        phone = account.phone if account and account.phone else self.default_phone
        return TelethonMessageSender(
            api_id=self.api_id,
            api_hash=self.api_hash,
            phone=phone,
            session=session,
        )

    def send_text(self, job: ExecutorJob) -> Dict[str, Any]:
        account_key = self._account_key(job)
        sender = self._senders.get(account_key)
        if sender is None:
            sender = self._build_sender(job)
            sender.connect()
            self._senders[account_key] = sender

        result = sender.send_text(job.target.key, job.message_text)
        result["telegram_account_id"] = job.telegram_account.id if job.telegram_account else None
        return result

    def disconnect(self) -> None:
        for sender in self._senders.values():
            sender.disconnect()
        self._senders.clear()
