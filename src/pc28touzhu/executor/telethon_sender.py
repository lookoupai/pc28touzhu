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

from .models import ExecutorJob, JobSendBlocked, JobSendUnconfirmed, JobSendDeferred, JobSendRetryable
from ..domain.betting_protocol import normalize_protocol, target_readiness, parse_acceptance
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
PRE_SEND_CONNECTION_ERRORS = (TimeoutError, ConnectionError, OSError, EOFError)
RECONCILIATION_READ_BUDGET_SECONDS = 5.0


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
        self._read_deadline = None
        self._read_cancelled = None

    def _operation_timeout(self, maximum=TELEGRAM_OPERATION_TIMEOUT_SECONDS):
        if self._read_deadline is None:
            return maximum
        if self._read_cancelled and self._read_cancelled():
            raise TimeoutError('foreground send takes priority')
        remaining = self._read_deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('reconciliation read budget exhausted')
        return min(maximum, remaining)

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
        session_lock = _session_file_lock(self.session, timeout=self._operation_timeout(SESSION_LOCK_TIMEOUT_SECONDS))
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
            _telegram_call(client, client.connect, timeout=self._operation_timeout())
            if not _telegram_call(client, client.is_user_authorized, timeout=self._operation_timeout()):
                raise ValueError("当前 session 未授权，请先在账号管理中完成登录或导入有效 Session")
            self._client = client
        except Exception:
            if client is not None:
                try:
                    disconnect = getattr(client, "disconnect", None)
                    if callable(disconnect):
                        _telegram_call(client, disconnect, timeout=1 if self._read_deadline is not None else TELEGRAM_OPERATION_TIMEOUT_SECONDS)
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
                _telegram_call(client, client.disconnect, timeout=1 if self._read_deadline is not None else TELEGRAM_OPERATION_TIMEOUT_SECONDS)
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
                return _telegram_call(self._client, lambda: self._client.get_input_entity(candidate), timeout=self._operation_timeout())
            except PRE_SEND_CONNECTION_ERRORS:
                raise
            except Exception:
                continue

        try:
            dialogs = list(_telegram_call(self._client, self._client.get_dialogs, timeout=self._operation_timeout()))
        except PRE_SEND_CONNECTION_ERRORS:
            raise
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
        random_id: int | None = None,
    ) -> Dict[str, Any]:
        if self._client is None:
            raise RuntimeError("Telethon client 尚未连接")
        try:
            entity = self._resolve_entity(target_key)
        except PRE_SEND_CONNECTION_ERRORS as exc:
            # Entity lookup precedes the send RPC, so this is a known unsent failure.
            raise JobSendRetryable(str(exc) or type(exc).__name__, details={
                'retryable': True, 'failure_stage': 'connect', 'preparation_stage': 'resolve_entity',
                'exception_type': type(exc).__name__,
            }) from exc
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
            if random_id is None:
                message = _telegram_call(self._client, lambda: self._client.send_message(entity, message_text), timeout=timeout)
            else:
                from telethon.tl.functions.messages import SendMessageRequest
                from telethon.tl.types import UpdateShortSentMessage
                request = SendMessageRequest(peer=entity, message=message_text, random_id=random_id, no_webpage=True)
                response = _telegram_call(self._client, lambda: self._client(request), timeout=timeout)
                message = response if isinstance(response, UpdateShortSentMessage) else self._client._get_response_message(request, response, entity)
                if not message or not getattr(message, 'id', None):
                    raise JobSendUnconfirmed('Telegram 回执缺少消息标识，需核对', details={'reason':'send_unconfirmed'})
        except PRE_SEND_CONNECTION_ERRORS as exc:
            # 请求可能已送达，回执不明时不得自动重发，避免重复消息或跨期补发。
            raise JobSendUnconfirmed(
                "Telegram 发送回执未确认，已停止自动重试，请核对群消息",
                details={"reason": "send_unconfirmed", "stage": "send_message",
                         "cause": str(exc) or exc.__class__.__name__, "send_started_at": started.isoformat(),
                         "cause_type": type(exc).__name__},
            ) from exc
        except Exception as exc:
            if isinstance(exc, JobSendUnconfirmed):
                raise
            from telethon.errors import RpcCallFailError, ServerError
            if random_id is not None and isinstance(exc, (RpcCallFailError, ServerError)):
                raise JobSendRetryable(str(exc), details={'retryable':True, 'failure_stage':'send_rpc',
                    'exception_type':exc.__class__.__name__, 'telegram_random_id':str(random_id)}) from exc
            raise
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

    def read_target_messages(self, target_key, *, after_id=0):
        entity = self._resolve_entity(target_key)
        messages = _telegram_call(self._client, lambda: self._client.get_messages(entity, limit=100, min_id=after_id), timeout=3)
        return [{'id':m.id, 'sender_id':m.sender_id, 'text':m.raw_text or '',
                 'reply_to':getattr(m, 'reply_to_msg_id', None), 'date':m.date.isoformat()}
                for m in messages]

    def read_message(self, target_key, message_id):
        entity = self._resolve_entity(target_key)
        message = _telegram_call(self._client,
            lambda: self._client.get_messages(entity, ids=int(message_id)), timeout=self._operation_timeout(3))
        if not message:
            return None
        return {'id': message.id, 'sender_id': message.sender_id, 'text': message.raw_text or '',
                'reply_to': getattr(message, 'reply_to_msg_id', None), 'date': message.date.isoformat(),
                'out': bool(getattr(message, 'out', False))}

    def read_reconciliation_messages(self, target_key, *, after_id):
        """Bounded historical slice after the original message, never latest-only evidence."""
        entity = self._resolve_entity(target_key)
        messages = _telegram_call(self._client,
            lambda: self._client.get_messages(entity, limit=100, min_id=int(after_id), reverse=True), timeout=self._operation_timeout(3))
        return [{'id': m.id, 'sender_id': m.sender_id, 'text': m.raw_text or '',
                 'reply_to': getattr(m, 'reply_to_msg_id', None), 'date': m.date.isoformat()}
                for m in messages]


class TelethonSenderPool:
    """Reuse sender objects while opening a session only for one send operation."""

    def __init__(
        self, *, api_id: int, api_hash: str, default_phone: str = "", default_session: str = "telegram-session",
        draw_clock_provider: Callable[[], Dict[str, Any] | None] | None = None,
        job_validator=None,
    ):
        self.api_id = int(api_id)
        self.api_hash = str(api_hash or "").strip()
        self.default_phone = str(default_phone or "").strip()
        self.default_session = str(default_session or "").strip() or "telegram-session"
        self.draw_clock_provider = draw_clock_provider
        self.job_validator = job_validator
        self._senders: Dict[str, TelethonMessageSender] = {}
        self._account_locks: Dict[str, threading.RLock] = {}
        self._foreground_waiters: Dict[str, int] = {}
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

    @contextmanager
    def _foreground_send(self, account_key, account_lock):
        with self._pool_lock:
            self._foreground_waiters[account_key] = self._foreground_waiters.get(account_key, 0) + 1
        try:
            with account_lock:
                yield
        finally:
            with self._pool_lock:
                self._foreground_waiters[account_key] -= 1

    def _foreground_waiting(self, account_key):
        with self._pool_lock:
            return self._foreground_waiters.get(account_key, 0) > 0

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
        protocol = normalize_protocol((job.target.metadata or {}).get('betting_protocol'))
        opening = {}

        def before_send() -> None:
            if self.job_validator and job.attempt_token:
                try:
                    verdict=self.job_validator(job)
                except Exception as exc:
                    raise JobSendDeferred('等待任务资格复核',details={'reason':'claim_validation_unavailable'}) from exc
                if not verdict.get('allowed'):
                    raise JobSendBlocked('任务已停止或领取失效',details=verdict)
            window_snapshot.update(self._ensure_send_window(job))

        with self._foreground_send(self._account_key(job), account_lock):
            try:
                before_send()
                preparation_started = datetime.now(timezone.utc)
                try:
                    sender.connect()
                except PRE_SEND_CONNECTION_ERRORS as exc:
                    raise JobSendRetryable(str(exc) or type(exc).__name__, details={
                        'retryable': True, 'failure_stage': 'connect', 'preparation_stage': 'connect',
                        'exception_type': type(exc).__name__,
                    }) from exc
                connection_ready = datetime.now(timezone.utc)
                before_send()
                if protocol['mode'] != 'legacy':
                    if protocol['mode'] == 'delay':
                        clock = self.draw_clock_provider() if self.draw_clock_provider else None
                        if not clock or not clock.get('latest_open_time'):
                            raise JobSendDeferred('等待可信开奖时钟', details={'reason':'clock_unavailable'})
                        previous = datetime.fromisoformat(clock['latest_open_time'].replace('Z','+00:00'))
                        offset = int(job.issue_no) - int(clock['latest_issue_no']) - 1
                        earliest = previous.timestamp() + offset * 210 + protocol['opening_delay_seconds']
                        if datetime.now(timezone.utc).timestamp() < earliest:
                            raise JobSendDeferred('等待目标配置的发送时间', details={'reason':'opening_delay'})
                    try:
                        messages = sender.read_target_messages(job.target.key) if protocol['mode'] == 'announcement' else []
                    except Exception as exc:
                        raise JobSendDeferred('暂时无法读取开盘状态', details={'reason':'target_status_unavailable'}) from exc
                    opening = target_readiness(messages, protocol, job.issue_no, datetime.now(timezone.utc), job.expire_at)
                    if opening['state'] == 'wait':
                        raise JobSendDeferred('等待目标开放本期投注', details=opening)
                    if opening['state'] == 'blocked':
                        raise JobSendBlocked('本期发送窗口不足', details=opening)
                    if opening.get('send_before'):
                        job.expire_at = min(job.expire_at, datetime.fromisoformat(opening['send_before']))
                    before_send()
                if self.draw_clock_provider is not None or job.attempt_token:
                    options = {'before_send':before_send, 'deadline':job.expire_at}
                    if job.telegram_random_id is not None:
                        options['random_id'] = job.telegram_random_id
                    result = sender.send_text(job.target.key, job.message_text, **options)
                else:
                    result = sender.send_text(job.target.key, job.message_text)
                result.update(telegram_account_id=job.telegram_account.id if job.telegram_account else None,
                              issue_no=job.issue_no, opening_evidence=dict(opening),
                              sender_session_path=(job.telegram_account.session_path if job.telegram_account
                                                   and job.telegram_account.session_path else self.default_session))
                if job.transport_evidence_callback:
                    # A crash during business acknowledgement must retain the RPC's message ID.
                    job.transport_evidence_callback(dict(result))
                if protocol['mode'] != 'legacy':
                    acceptance = {'state':'unconfirmed','reason':'acceptance_reply_missing'}
                    ack_deadline = time.monotonic() + protocol['ack_timeout_seconds']
                    while time.monotonic() < ack_deadline:
                        try:
                            replies = sender.read_target_messages(job.target.key, after_id=int(result['message_id']))
                            current = target_readiness(sender.read_target_messages(job.target.key), protocol, job.issue_no,
                                datetime.now(timezone.utc), job.expire_at) if protocol['mode'] == 'announcement' else {}
                            result['current_opening_evidence'] = current
                            result['acceptance_checked_at'] = datetime.now(timezone.utc).isoformat()
                            acceptance = parse_acceptance(replies, protocol, result['message_id'], job.issue_no, job.message_text, opening, current)
                            if acceptance.get('reply_message_id'):
                                result['acceptance_reply'] = next((dict(reply) for reply in replies
                                    if str(reply.get('id')) == str(acceptance['reply_message_id'])), {})
                            if acceptance['state'] != 'unconfirmed' or acceptance['reason'] != 'acceptance_reply_missing':
                                break
                        except Exception:
                            acceptance = {'state':'unconfirmed','reason':'acceptance_read_failed'}
                            break
                        time.sleep(.5)
                    result['acceptance'] = acceptance
                result["telegram_account_id"] = job.telegram_account.id if job.telegram_account else None
                result["preparation_started_at"] = preparation_started.isoformat()
                result["connection_ready_at"] = connection_ready.isoformat()
                if window_snapshot:
                    result["issue_window"] = window_snapshot
                return result
            finally:
                # Never keep a SQLite-backed Telethon client open between jobs.
                sender.disconnect()

    def reconcile_text(self, job: ExecutorJob, evidence: Dict[str, Any]) -> Dict[str, Any]:
        """Only read an original message and its replies; never submit a bet."""
        unknown = {'outcome': 'unknown', 'evidence': {}, 'reason': 'message_id_unavailable'}
        message_id = evidence.get('message_id')
        if isinstance(message_id, bool) or not str(message_id or '').isdigit() or int(message_id) <= 0:
            return unknown
        account_id = job.telegram_account.id if job.telegram_account else None
        if any(evidence.get(key) not in (None, expected) for key, expected in (
            ('target_key', job.target.key), ('telegram_account_id', account_id),
            ('text', job.message_text), ('issue_no', job.issue_no),
        )):
            return {**unknown, 'reason': 'original_identity_mismatch'}
        explicit_session = job.telegram_account.session_path if job.telegram_account else None
        session = explicit_session or self.default_session
        if not explicit_session and evidence.get('sender_session_path') != session:
            return {**unknown, 'reason': 'original_session_unavailable'}
        sender, account_lock = self._get_sender_and_lock(job)
        if getattr(sender, 'session', session) != session:
            return {**unknown, 'reason': 'original_session_unavailable'}
        account_key = self._account_key(job)
        if self._foreground_waiting(account_key):
            return {**unknown, 'reason': 'account_busy'}
        if not account_lock.acquire(blocking=False):
            return {**unknown, 'reason': 'account_busy'}
        sender._read_deadline = time.monotonic() + RECONCILIATION_READ_BUDGET_SECONDS
        sender._read_cancelled = lambda: self._foreground_waiting(account_key)
        try:
            sender.connect()
            message = sender.read_message(job.target.key, int(message_id))
            if (not message or str(message.get('id')) != str(message_id)
                    or not message.get('out') or message.get('text') != job.message_text):
                return {**unknown, 'reason': 'original_message_not_verified'}
            sent_at = datetime.fromisoformat(str(message['date']).replace('Z', '+00:00'))
            if sent_at.tzinfo is None:
                sent_at = sent_at.replace(tzinfo=timezone.utc)
            if not job.execute_after.timestamp() - 2 <= sent_at.timestamp() <= job.expire_at.timestamp() + 2:
                return {**unknown, 'reason': 'original_message_outside_window'}
            verified = {**evidence, 'message_id': int(message_id), 'target_key': job.target.key,
                        'telegram_account_id': account_id, 'text': job.message_text, 'issue_no': job.issue_no,
                        'telegram_message_date': sent_at.isoformat(), 'verified_message': message,
                        'provenance': 'remote_history'}
            protocol = normalize_protocol((job.target.metadata or {}).get('betting_protocol'))
            if protocol['mode'] == 'legacy':
                return {'outcome': 'transport_confirmed', 'evidence': verified, 'reason': 'original_message_verified'}
            replies = sender.read_reconciliation_messages(job.target.key, after_id=int(message_id))
            acceptance = parse_acceptance(replies, protocol, int(message_id), job.issue_no, job.message_text,
                evidence.get('opening_evidence') or {}, evidence.get('current_opening_evidence') or {})
            verified['acceptance'] = acceptance
            if acceptance.get('reply_message_id'):
                verified['acceptance_reply'] = next((dict(reply) for reply in replies
                    if str(reply.get('id')) == str(acceptance['reply_message_id'])), {})
            if acceptance.get('state') in ('accepted', 'rejected'):
                round_id = str(acceptance.get('target_round_id') or '')
                if acceptance['state'] == 'accepted' and not (round_id.isdigit() and len(round_id) < 20):
                    # Saved opening snapshots describe the original acknowledgement poll,
                    # not the period associated with a newly fetched historical reply.
                    return {'outcome': 'unknown', 'evidence': verified, 'reason': 'historical_round_mapping_unavailable'}
                return {'outcome': acceptance['state'], 'evidence': verified, 'reason': acceptance.get('reason', '')}
            return {'outcome': 'unknown', 'evidence': verified, 'reason': acceptance.get('reason') or 'acceptance_unverified'}
        except Exception as exc:
            return {**unknown, 'reason': 'reconciliation_read_failed:' + type(exc).__name__}
        finally:
            sender.disconnect()
            sender._read_deadline = None
            sender._read_cancelled = None
            account_lock.release()

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
