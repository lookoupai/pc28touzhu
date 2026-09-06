"""Telethon senders with account-scoped, process-safe session locking."""
from __future__ import annotations

import asyncio
import inspect
import os
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterator

from .models import ExecutorJob, JobSendBlocked, JobSendUnconfirmed
from ..domain.pc28_issue_window import evaluate_pc28_issue_dispatch_window
from ..runtime_environment import (
    build_telethon_missing_message,
    ensure_telethon_session_writable,
    resolve_telethon_session_file,
)

try:
    import fcntl
except ImportError:  # pragma: no cover - production runs on Linux
    fcntl = None  # type: ignore[assignment]


SESSION_LOCK_TIMEOUT_SECONDS = 30.0
SESSION_LOCK_POLL_SECONDS = 0.05
TELEGRAM_OPERATION_TIMEOUT_SECONDS = 10.0


def _telegram_call(client: Any, callback: Callable[[], Any], *, timeout: float = TELEGRAM_OPERATION_TIMEOUT_SECONDS) -> Any:
    loop = getattr(client, "loop", None)
    if loop is None:
        return callback()

    async def invoke():
        result = callback()
        return await result if inspect.isawaitable(result) else result

    # 必须在事件循环中调用 sync 包装的方法，否则会先阻塞再进入 wait_for。
    return loop.run_until_complete(asyncio.wait_for(invoke(), timeout=max(0.001, timeout)))


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


@contextmanager
def _session_file_lock(session: str, *, timeout: float = SESSION_LOCK_TIMEOUT_SECONDS) -> Iterator[None]:
    """Serialize access to a Telethon SQLite session across processes."""
    session_file = resolve_telethon_session_file(session)
    lock_path = session_file.with_name(session_file.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    if fcntl is None:
        yield
        return

    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o664)
    acquired = False
    deadline = time.monotonic() + max(0.0, float(timeout))
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                acquired = True
                break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Telegram session 正在被其他发送任务使用，请稍后重试")
                time.sleep(SESSION_LOCK_POLL_SECONDS)
        yield
    finally:
        if acquired:
            try:
                fcntl.flock(fd, fcntl.LOCK_UN)
            except OSError:
                pass
        os.close(fd)


class TelethonMessageSender:
    def __init__(self, *, api_id: int, api_hash: str, phone: str, session: str):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash or "").strip()
        self.phone = str(phone or "").strip()
        self.session = str(session or "").strip() or "telegram-session"
        self._client = None
        self._session_lock: Any = None

    def connect(self) -> None:
        if self._client is not None:
            return
        if self.api_id <= 0:
            raise ValueError("TELEGRAM_API_ID 未配置")
        if not self.api_hash:
            raise ValueError("TELEGRAM_API_HASH 未配置")

        try:
            from telethon.sync import TelegramClient
        except ImportError as exc:
            raise RuntimeError(build_telethon_missing_message()) from exc

        session_path = ensure_telethon_session_writable(self.session)
        session_path.parent.mkdir(parents=True, exist_ok=True)
        session_lock = _session_file_lock(self.session)
        session_lock.__enter__()
        self._session_lock = session_lock
        client = None
        try:
            client = TelegramClient(
                self.session, self.api_id, self.api_hash,
                timeout=TELEGRAM_OPERATION_TIMEOUT_SECONDS,
                request_retries=0, connection_retries=0, retry_delay=0,
                auto_reconnect=False, flood_sleep_threshold=0, raise_last_call_error=True,
            )
            _telegram_call(client, client.connect)
            if not _telegram_call(client, client.is_user_authorized):
                raise ValueError("当前 session 未授权，请先在账号管理中完成登录或导入有效 Session")
            self._client = client
        except Exception:
            if client is not None:
                try:
                    disconnect = getattr(client, "disconnect", None)
                    if callable(disconnect):
                        _telegram_call(client, disconnect)
                except Exception:
                    pass
            self._session_lock = None
            session_lock.__exit__(None, None, None)
            raise

    def disconnect(self) -> None:
        client = self._client
        self._client = None
        try:
            if client is not None:
                _telegram_call(client, client.disconnect)
        except Exception:
            pass
        finally:
            session_lock = self._session_lock
            self._session_lock = None
            if session_lock is not None:
                session_lock.__exit__(None, None, None)

    def _resolve_entity(self, target_key: str) -> Any:
        if self._client is None:
            raise RuntimeError("Telethon client 尚未连接")

        candidates = [_coerce_entity(target_key)]
        candidates.extend(_build_numeric_candidates(target_key))
        for candidate in candidates:
            try:
                return _telegram_call(self._client, lambda: self._client.get_input_entity(candidate))
            except Exception:
                continue

        try:
            dialogs = list(_telegram_call(self._client, self._client.get_dialogs))
        except Exception:
            dialogs = []

        text_key = str(target_key).strip()
        string_candidates = {text_key}
        string_candidates.update(str(item) for item in _build_numeric_candidates(target_key))
        for dialog in dialogs:
            dialog_candidates = set()
            dialog_id = getattr(dialog, "id", None)
            if dialog_id is not None:
                dialog_candidates.add(str(dialog_id))
                dialog_candidates.add("-100%s" % dialog_id)
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

    def send_text(
        self, target_key: str, message_text: str, *,
        before_send: Callable[[], None] | None = None,
        deadline: datetime | None = None,
    ) -> Dict[str, Any]:
        if self._client is None:
            raise RuntimeError("Telethon client 尚未连接")
        entity = self._resolve_entity(target_key)
        if before_send is not None:
            before_send()
        started = datetime.now(timezone.utc)
        timeout = TELEGRAM_OPERATION_TIMEOUT_SECONDS
        if deadline is not None:
            remaining = (deadline - started).total_seconds()
            if remaining <= 0:
                raise JobSendBlocked("任务已超过发送截止时间", details={"reason": "expired_before_rpc"})
            timeout = min(timeout, remaining)
        try:
            message = _telegram_call(
                self._client, lambda: self._client.send_message(entity, message_text), timeout=timeout,
            )
        except (TimeoutError, ConnectionError, OSError) as exc:
            # 请求可能已送达，回执不明时不得自动重发，避免重复消息或跨期补发。
            raise JobSendUnconfirmed(
                "Telegram 发送回执未确认，已停止自动重试，请核对群消息",
                details={"reason": "send_unconfirmed", "stage": "send_message",
                         "cause": str(exc) or exc.__class__.__name__, "send_started_at": started.isoformat()},
            ) from exc
        return {
            "message_id": getattr(message, "id", None),
            "chat_id": getattr(message, "chat_id", None),
            "text": message_text,
            "target_key": target_key,
            "send_started_at": started.isoformat(),
            "send_finished_at": datetime.now(timezone.utc).isoformat(),
            "telegram_message_date": (
                message.date.isoformat() if isinstance(getattr(message, "date", None), datetime) else None
            ),
        }


class TelethonSenderPool:
    """Reuse sender objects while opening a session only for one send operation."""

    def __init__(
        self, *, api_id: int, api_hash: str, default_phone: str = "", default_session: str = "telegram-session",
        draw_clock_provider: Callable[[], Dict[str, Any] | None] | None = None,
    ):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash or "").strip()
        self.default_phone = str(default_phone or "").strip()
        self.default_session = str(default_session or "").strip() or "telegram-session"
        self.draw_clock_provider = draw_clock_provider
        self._senders: Dict[str, TelethonMessageSender] = {}
        self._account_locks: Dict[str, threading.RLock] = {}
        self._pool_lock = threading.Lock()

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

    def _get_sender_and_lock(self, job: ExecutorJob) -> tuple[TelethonMessageSender, threading.RLock]:
        account_key = self._account_key(job)
        with self._pool_lock:
            sender = self._senders.get(account_key)
            if sender is None:
                sender = self._build_sender(job)
                self._senders[account_key] = sender
            account_lock = self._account_locks.setdefault(account_key, threading.RLock())
        return sender, account_lock

    def _ensure_send_window(self, job: ExecutorJob) -> Dict[str, Any]:
        if datetime.now(timezone.utc) >= job.expire_at:
            raise JobSendBlocked("任务已超过发送截止时间", details={"reason": "expired_before_send"})
        if job.lottery_type != "pc28" or self.draw_clock_provider is None:
            return {}
        clock = self.draw_clock_provider()
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no=job.issue_no, draw_clock=clock, now=datetime.now(timezone.utc),
        )
        # 最后发送阶段必须有可核对的时钟，不能按派单阶段的 fail-open 放行。
        if not verdict["allowed"] or verdict.get("remaining_seconds") is None:
            raise JobSendBlocked("发送前封盘检查未通过", details={"reason": "issue_window_blocked", "issue_window": verdict})
        if verdict.get("send_before"):
            job.expire_at = min(job.expire_at, datetime.fromisoformat(verdict["send_before"].replace("Z", "+00:00")))
        if datetime.now(timezone.utc) >= job.expire_at:
            raise JobSendBlocked("任务已超过发送截止时间", details={"reason": "expired_after_clock_check", "issue_window": verdict})
        return verdict

    def send_text(self, job: ExecutorJob) -> Dict[str, Any]:
        sender, account_lock = self._get_sender_and_lock(job)
        window_snapshot: Dict[str, Any] = {}

        def before_send() -> None:
            window_snapshot.update(self._ensure_send_window(job))

        with account_lock:
            try:
                before_send()
                preparation_started = datetime.now(timezone.utc)
                sender.connect()
                connection_ready = datetime.now(timezone.utc)
                before_send()
                if self.draw_clock_provider is not None:
                    result = sender.send_text(job.target.key, job.message_text, before_send=before_send, deadline=job.expire_at)
                else:
                    result = sender.send_text(job.target.key, job.message_text)
                result["telegram_account_id"] = job.telegram_account.id if job.telegram_account else None
                result["preparation_started_at"] = preparation_started.isoformat()
                result["connection_ready_at"] = connection_ready.isoformat()
                if window_snapshot:
                    result["issue_window"] = window_snapshot
                return result
            finally:
                # Never keep a SQLite-backed Telethon client open between jobs.
                sender.disconnect()

    def disconnect(self) -> None:
        with self._pool_lock:
            senders = list(self._senders.items())
        for account_key, sender in senders:
            account_lock = self._account_locks[account_key]
            with account_lock:
                sender.disconnect()
        with self._pool_lock:
            self._senders.clear()
            self._account_locks.clear()
