"""Platform-side management helpers for sources, subscriptions and delivery targets."""
from __future__ import annotations

import base64
import binascii
import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from collections import Counter, defaultdict
from typing import Any, Dict, Optional
from uuid import uuid4

from pc28touzhu.config import get_runtime_config
from pc28touzhu.domain.pc28_play_filter import normalize_play_filter_keys, normalize_play_filter_mode
from pc28touzhu.domain.settlement_rules import resolve_pc28_result_for_signal
from pc28touzhu.domain.subscription_strategy import (
    normalize_subscription_strategy_input,
    present_subscription_item,
    resolve_settlement_runtime_policy,
)
from pc28touzhu.executor.telethon_sender import TelethonMessageSender
from pc28touzhu.services.dispatch_service import dispatch_signal as dispatch_signal_jobs
from pc28touzhu.services.normalize_service import normalize_raw_item as normalize_raw_item_to_signals
from pc28touzhu.services.pc28_draw_service import fetch_pc28_recent_draws, fetch_pc28_recent_draws_deep
from pc28touzhu.services.source_fetch_service import fetch_source_to_raw_item
from pc28touzhu.services.telegram_account_gateway import TelethonAccountGateway
from pc28touzhu.services.telegram_target_key import normalize_telegram_target_key


ALLOWED_EXECUTION_JOB_STATUSES = {"pending", "delivered", "failed", "expired", "skipped"}
RETRYABLE_EXECUTION_JOB_STATUSES = {"failed", "expired", "skipped"}
FAILED_DELIVERY_STATUSES = {"failed", "expired", "skipped"}
ALLOWED_ENTITY_STATUSES = {"active", "inactive", "archived"}
ALLOWED_SUBSCRIPTION_STATUSES = ALLOWED_ENTITY_STATUSES | {"standby"}
ALLOWED_TELEGRAM_AUTH_MODES = {"phone_login", "session_import"}
AUTHORIZED_TELEGRAM_AUTH_STATE = "authorized"
SHANGHAI_TZ = timezone(timedelta(hours=8))


class ActionableValueError(ValueError):
    def __init__(self, error: str, *, reason_code: str = "", why: str = "", next_step: str = ""):
        super().__init__(error)
        self.payload = {
            "error": error,
            "reason_code": reason_code,
            "why": why,
            "next_step": next_step,
        }


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today_stat_date() -> str:
    return datetime.now(timezone.utc).astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d")


def _normalize_stat_date(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return _today_stat_date()
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        raise ValueError("stat_date 必须为 YYYY-MM-DD")
    return parsed.strftime("%Y-%m-%d")


def _parse_iso8601(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _heartbeat_state(
    last_seen_at: Any,
    *,
    stale_after_seconds: int = 60,
    offline_after_seconds: int = 300,
) -> tuple[str, Optional[int]]:
    last_seen = _parse_iso8601(last_seen_at)
    if last_seen is None:
        return ("offline", None)
    age_seconds = max(0, int((datetime.now(timezone.utc) - last_seen).total_seconds()))
    if age_seconds <= max(1, int(stale_after_seconds)):
        return ("online", age_seconds)
    if age_seconds <= max(stale_after_seconds + 1, int(offline_after_seconds)):
        return ("stale", age_seconds)
    return ("offline", age_seconds)


def _failure_streak(attempts: list[Dict[str, Any]]) -> int:
    streak = 0
    for attempt in attempts:
        if str(attempt.get("delivery_status") or "") == "delivered":
            break
        streak += 1
    return streak


def _auto_retry_policy(
    *,
    job_status: Any,
    attempt_count: Any,
    executed_at: Any,
    max_attempts: int,
    base_delay_seconds: int,
) -> Dict[str, Any]:
    normalized_attempt_count = max(0, int(attempt_count or 0))
    if str(job_status or "") != "failed":
        return {
            "auto_retry_enabled": False,
            "auto_retry_state": "manual_only",
            "next_retry_at": None,
            "attempts_remaining": max(0, int(max_attempts) - normalized_attempt_count),
        }

    if normalized_attempt_count >= int(max_attempts):
        return {
            "auto_retry_enabled": True,
            "auto_retry_state": "exhausted",
            "next_retry_at": None,
            "attempts_remaining": 0,
        }

    executed_dt = _parse_iso8601(executed_at)
    if executed_dt is None:
        return {
            "auto_retry_enabled": True,
            "auto_retry_state": "scheduled",
            "next_retry_at": None,
            "attempts_remaining": max(0, int(max_attempts) - normalized_attempt_count),
        }

    backoff_seconds = max(5, int(base_delay_seconds or 5)) * (2 ** max(0, normalized_attempt_count - 1))
    next_retry_at = executed_dt + timedelta(seconds=backoff_seconds)
    now = datetime.now(timezone.utc)
    return {
        "auto_retry_enabled": True,
        "auto_retry_state": "due" if next_retry_at <= now else "scheduled",
        "next_retry_at": next_retry_at.replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "attempts_remaining": max(0, int(max_attempts) - normalized_attempt_count),
        "backoff_seconds": backoff_seconds,
    }


def _alert_item(
    *,
    severity: str,
    alert_type: str,
    title: str,
    message: str,
    metadata: Optional[Dict[str, Any]] = None,
    key_parts: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    stable_payload = {
        "alert_type": alert_type,
        "key_parts": key_parts or metadata or {},
    }
    alert_key = "%s:%s" % (
        alert_type,
        hashlib.sha1(json.dumps(stable_payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16],
    )
    return {
        "alert_key": alert_key,
        "severity": severity,
        "alert_type": alert_type,
        "title": title,
        "message": message,
        "metadata": metadata or {},
    }


def _to_positive_int(value: Any, field_name: str, *, allow_none: bool = False) -> Optional[int]:
    if value is None:
        if allow_none:
            return None
        raise ValueError("%s 不能为空" % field_name)
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        raise ValueError("%s 必须为整数" % field_name)
    if normalized <= 0:
        raise ValueError("%s 必须大于 0" % field_name)
    return normalized


def _to_non_empty_str(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("%s 不能为空" % field_name)
    return text


def _to_object(value: Any, field_name: str) -> Dict[str, Any]:
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ValueError("%s 必须为对象" % field_name)
    return dict(value)


def _to_optional_non_negative_float(value: Any, field_name: str) -> Optional[float]:
    if value in {None, ""}:
        return None
    try:
        normalized = round(float(value), 4)
    except (TypeError, ValueError):
        raise ValueError("%s 必须为数字" % field_name)
    if normalized < 0:
        raise ValueError("%s 不能小于 0" % field_name)
    return normalized


def _normalize_subscription_bet_filter(value: Any) -> Dict[str, Any]:
    payload = value if isinstance(value, dict) else {}
    mode = normalize_play_filter_mode(payload.get("mode"))
    selected_keys = normalize_play_filter_keys(payload.get("selected_keys"))
    if mode == "selected" and not selected_keys:
        raise ValueError("选择自定义玩法时，至少要勾选一个玩法")
    return {
        "mode": mode,
        "selected_keys": selected_keys,
    }


def _normalize_subscription_strategy(value: Any) -> Dict[str, Any]:
    return normalize_subscription_strategy_input(value)


def _subscription_strategy_input_from_payload(payload: Dict[str, Any]) -> Any:
    if isinstance(payload.get("strategy_v2"), dict):
        return payload.get("strategy_v2")
    return payload.get("strategy")


def _present_subscription_items(items: list[Dict[str, Any]]) -> list[Dict[str, Any]]:
    return [present_subscription_item(item) for item in items]


def _default_subscription_daily_stat(subscription: Dict[str, Any], *, stat_date: str) -> Dict[str, Any]:
    return {
        "id": None,
        "stat_date": stat_date,
        "user_id": int(subscription["user_id"]),
        "subscription_id": int(subscription["id"]),
        "source_id": int(subscription["source_id"]),
        "source_name": str(subscription.get("source_name") or ""),
        "profit_amount": 0.0,
        "loss_amount": 0.0,
        "net_profit": 0.0,
        "settled_event_count": 0,
        "hit_count": 0,
        "miss_count": 0,
        "refund_count": 0,
        "updated_at": None,
    }


def _normalize_entity_status(value: Any, field_name: str = "status") -> str:
    text = str(value or "").strip()
    if text not in ALLOWED_ENTITY_STATUSES:
        raise ValueError("%s 仅支持 active、inactive 或 archived" % field_name)
    return text


def _normalize_subscription_status(value: Any, field_name: str = "status") -> str:
    text = str(value or "").strip()
    if text not in ALLOWED_SUBSCRIPTION_STATUSES:
        raise ValueError("%s 仅支持 active、standby、inactive 或 archived" % field_name)
    return text


def _raise_actionable(error: str, *, reason_code: str, why: str, next_step: str) -> None:
    raise ActionableValueError(error, reason_code=reason_code, why=why, next_step=next_step)


def _target_test_feedback_from_exception(error: Exception) -> Dict[str, str]:
    error_name = type(error).__name__
    message = str(error or "").strip()
    normalized = message.lower()

    if "未安装 Telethon" in message:
        return {
            "error": "测试发送失败：当前平台运行进程缺少 Telethon 依赖。",
            "reason_code": "telethon_missing",
            "why": message,
            "next_step": "如果依赖装在虚拟环境，请让 platform / executor / bot 服务切到同一个解释器后重启；否则请直接在当前 Python 解释器安装 `Telethon>=1.42,<2` 后重试。",
        }
    if "Telethon session 文件不可写" in message or "Telethon session 目录不可写" in message or "readonly database" in normalized:
        return {
            "error": "测试发送失败：托管账号 Session 文件不可写。",
            "reason_code": "session_readonly",
            "why": message or "Telethon 无法写入当前账号的 session 数据库。",
            "next_step": "把该账号 session 文件及其目录的属主/权限改到当前运行服务的用户后重试；当前部署通常应为 `www:www` 且文件至少可写。",
        }
    if error_name in {"UserNotParticipantError"} or "无法解析目标群组实体" in message:
        return {
            "error": "测试发送失败：托管账号还没有进入这个群组。",
            "reason_code": "target_not_joined",
            "why": "系统找不到这个群组实体，通常是因为当前托管账号没有加入目标群组。",
            "next_step": "先把该托管账号拉进目标群组，再重新点击测试发送。",
        }
    if error_name in {"ChatWriteForbiddenError", "ChatRestrictedError", "ChatAdminRequiredError"}:
        return {
            "error": "测试发送失败：当前账号没有发言权限。",
            "reason_code": "target_no_write_permission",
            "why": "目标群组禁止当前账号发言，或者该账号被禁言/缺少必要权限。",
            "next_step": "检查群权限或改用有发言权限的托管账号后重新测试。",
        }
    if error_name in {"ChannelPrivateError", "PeerIdInvalidError", "ChatIdInvalidError"}:
        return {
            "error": "测试发送失败：群组标识不可用。",
            "reason_code": "target_unreachable",
            "why": "目标可能是私有群、ID 无效，或当前填写的 target_key 与实际群组不匹配。",
            "next_step": "确认群组 ID 或 @username 是否正确，必要时用 @userinfobot 重新获取 Chat ID。",
        }
    if error_name in {"FloodWaitError"} or "flood" in normalized:
        return {
            "error": "测试发送失败：Telegram 暂时限制了发送频率。",
            "reason_code": "telegram_rate_limited",
            "why": "当前账号触发了 Telegram 频率限制，短时间内不能继续发送消息。",
            "next_step": "等待一段时间后重试，或切换到其他托管账号。",
        }
    if "session 未授权" in message or "尚未完成授权" in message:
        return {
            "error": "测试发送失败：托管账号还没有完成授权。",
            "reason_code": "account_unauthorized",
            "why": "当前 session 已失效或账号登录流程没有完成。",
            "next_step": "回到托管账号区域重新授权，再重新测试发送。",
        }
    return {
        "error": "测试发送失败：系统未能把消息发到目标群组。",
        "reason_code": "target_test_failed",
        "why": message or "发送过程中出现了未分类异常。",
        "next_step": "先核对群组标识和账号状态，若仍失败再查看执行日志定位具体异常。",
    }


def _ensure_user_exists(repository: Any, user_id: Optional[int], field_name: str = "user_id") -> None:
    if user_id is None:
        return
    if not repository.get_user(int(user_id)):
        raise ValueError("%s 对应的用户不存在" % field_name)


def _ensure_source_exists(repository: Any, source_id: int) -> None:
    if not repository.get_source(int(source_id)):
        raise ValueError("source_id 对应的来源不存在")


def _ensure_telegram_account_exists(repository: Any, telegram_account_id: Optional[int]) -> None:
    if telegram_account_id is None:
        return
    if not repository.get_telegram_account(int(telegram_account_id)):
        raise ValueError("telegram_account_id 对应的账号不存在")


def _ensure_owned_telegram_account(repository: Any, *, telegram_account_id: Optional[int], user_id: int) -> None:
    if telegram_account_id is None:
        return
    account = repository.get_telegram_account(int(telegram_account_id))
    if not account:
        raise ValueError("telegram_account_id 对应的账号不存在")
    if int(account.get("user_id") or 0) != int(user_id):
        raise ValueError("telegram_account_id 不属于当前用户")


def _ensure_message_template_exists(repository: Any, template_id: Optional[int]) -> None:
    if template_id is None:
        return
    if not repository.get_message_template(int(template_id)):
        raise ValueError("template_id 对应的下注格式模板不存在")


def _ensure_owned_message_template(repository: Any, *, template_id: Optional[int], user_id: int) -> None:
    if template_id is None:
        return
    item = repository.get_message_template(int(template_id))
    if not item:
        raise ValueError("template_id 对应的下注格式模板不存在")
    if int(item.get("user_id") or 0) != int(user_id):
        raise ValueError("template_id 不属于当前用户")


def _normalize_template_config(value: Any) -> Dict[str, Any]:
    config = _to_object(value, "config")
    bet_rules = config.get("bet_rules")
    if bet_rules is not None and not isinstance(bet_rules, dict):
        raise ValueError("config.bet_rules 必须为对象")
    for bet_type, rule in (bet_rules or {}).items():
        if not isinstance(rule, dict):
            raise ValueError("config.bet_rules.%s 必须为对象" % bet_type)
        if "format" in rule and not str(rule.get("format") or "").strip():
            raise ValueError("config.bet_rules.%s.format 不能为空" % bet_type)
        value_map = rule.get("value_map")
        if value_map is not None and not isinstance(value_map, dict):
            raise ValueError("config.bet_rules.%s.value_map 必须为对象" % bet_type)
    return config


def _normalize_telegram_auth_mode(value: Any) -> str:
    text = str(value or "").strip()
    if text not in ALLOWED_TELEGRAM_AUTH_MODES:
        raise ValueError("auth_mode 仅支持 phone_login 或 session_import")
    return text


def _managed_accounts_root() -> Path:
    database_parent = Path(get_runtime_config().platform.database_path).expanduser().resolve().parent
    data_dir = database_parent if database_parent.name == "data" else (database_parent / "data")
    return data_dir / "accounts"


def _build_managed_session_path(user_id: int) -> str:
    account_slug = "account-%s" % uuid4().hex[:10]
    return str(_managed_accounts_root() / ("u%s" % int(user_id)) / account_slug / "main")


def _session_file_path(session_path: str) -> Path:
    return Path("%s.session" % str(session_path or "").strip()).expanduser()


def _normalize_account_meta(meta: Any) -> Dict[str, Any]:
    return dict(meta) if isinstance(meta, dict) else {}


def _decorated_auth_state(item: Dict[str, Any]) -> str:
    meta = _normalize_account_meta(item.get("meta"))
    auth_state = str(meta.get("auth_state") or "").strip()
    if auth_state:
        return auth_state
    if str(item.get("session_path") or "").strip():
        return AUTHORIZED_TELEGRAM_AUTH_STATE
    return "pending"


def _decorate_telegram_account(item: Dict[str, Any]) -> Dict[str, Any]:
    payload = dict(item or {})
    meta = _normalize_account_meta(payload.get("meta"))
    auth_mode = str(meta.get("auth_mode") or "").strip()
    auth_state = _decorated_auth_state(payload)
    payload["meta"] = meta
    payload["auth_mode"] = auth_mode or ("phone_login" if str(payload.get("phone") or "").strip() else "session_import")
    payload["auth_state"] = auth_state
    payload["is_authorized"] = auth_state == AUTHORIZED_TELEGRAM_AUTH_STATE
    return payload


def _get_owned_telegram_account(repository: Any, *, telegram_account_id: int, user_id: int) -> Dict[str, Any]:
    current = repository.get_telegram_account(int(telegram_account_id))
    if not current or int(current.get("user_id") or 0) != int(user_id):
        raise ValueError("telegram_account_id 对应的账号不存在")
    return current


def _store_telegram_account(
    repository: Any,
    *,
    current: Dict[str, Any],
    label: Optional[str] = None,
    phone: Optional[str] = None,
    session_path: Optional[str] = None,
    meta: Optional[Dict[str, Any]] = None,
    status: Optional[str] = None,
) -> Dict[str, Any]:
    updated = repository.update_telegram_account_record(
        telegram_account_id=int(current["id"]),
        user_id=int(current["user_id"]),
        label=label if label is not None else str(current.get("label") or ""),
        session_path=session_path if session_path is not None else str(current.get("session_path") or ""),
        phone=phone if phone is not None else str(current.get("phone") or ""),
        meta=meta if meta is not None else _normalize_account_meta(current.get("meta")),
    )
    if not updated:
        raise ValueError("telegram_account_id 对应的账号不存在")
    if status is not None and str(updated.get("status") or "") != str(status):
        updated = repository.update_telegram_account_status(
            telegram_account_id=int(current["id"]),
            user_id=int(current["user_id"]),
            status=str(status),
        )
    if not updated:
        raise ValueError("telegram_account_id 对应的账号不存在")
    return updated


def _clear_pending_login_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    cleaned = dict(meta)
    for key in ("phone_code_hash", "auth_requested_at", "pending_phone"):
        cleaned.pop(key, None)
    return cleaned


def _build_account_gateway() -> TelethonAccountGateway:
    executor = get_runtime_config().executor
    return TelethonAccountGateway(api_id=executor.telegram_api_id, api_hash=executor.telegram_api_hash)


def _resolve_telegram_auth_mode(payload: Dict[str, Any]) -> str:
    if payload.get("auth_mode") is not None:
        return _normalize_telegram_auth_mode(payload.get("auth_mode"))
    meta = _normalize_account_meta(payload.get("meta"))
    if meta.get("auth_mode") is not None:
        return _normalize_telegram_auth_mode(meta.get("auth_mode"))
    if str(payload.get("phone") or "").strip():
        return "phone_login"
    return "session_import"


def _write_session_bytes(session_path: str, content: bytes) -> None:
    session_file = _session_file_path(session_path)
    session_file.parent.mkdir(parents=True, exist_ok=True)
    session_file.write_bytes(content)


def list_users(repository: Any) -> Dict[str, Any]:
    return {"items": repository.list_users()}


def list_support_snapshot(repository: Any, *, user_id: Any = None) -> Dict[str, Any]:
    normalized_user_id = (
        _to_positive_int(user_id, "user_id", allow_none=True)
        if user_id is not None and str(user_id).strip() != ""
        else None
    )
    users = repository.list_users()
    if normalized_user_id is not None:
        users = [item for item in users if int(item.get("id") or 0) == normalized_user_id]

    sources: list[Dict[str, Any]] = []
    accounts: list[Dict[str, Any]] = []
    subscriptions: list[Dict[str, Any]] = []
    targets: list[Dict[str, Any]] = []
    templates: list[Dict[str, Any]] = []

    for user in users:
        current_user_id = int(user["id"])
        user_meta = {
            "user_id": current_user_id,
            "user_username": str(user.get("username") or ""),
            "user_email": str(user.get("email") or ""),
            "user_role": str(user.get("role") or ""),
            "user_status": str(user.get("status") or ""),
        }
        user_sources = list_sources(repository, owner_user_id=current_user_id)["items"]
        source_map = {int(item["id"]): item for item in user_sources if item.get("id") is not None}
        sources.extend([{**item, **user_meta} for item in user_sources])

        user_accounts = list_telegram_accounts(repository, user_id=current_user_id)["items"]
        accounts.extend([{**item, **user_meta} for item in user_accounts])

        user_subscriptions = list_subscriptions(repository, user_id=current_user_id)["items"]
        for item in user_subscriptions:
            source = source_map.get(int(item["source_id"])) if item.get("source_id") is not None else None
            subscriptions.append(
                {
                    **item,
                    **user_meta,
                    "source_name": (source or {}).get("name") or ("#" + str(item.get("source_id") or "--")),
                    "source_type": (source or {}).get("source_type") or "",
                }
            )

        user_targets = list_delivery_targets(repository, user_id=current_user_id)["items"]
        targets.extend([{**item, **user_meta} for item in user_targets])

        user_templates = list_message_templates(repository, user_id=current_user_id)["items"]
        templates.extend([{**item, **user_meta} for item in user_templates])

    return {
        "users": users,
        "sources": sources,
        "accounts": accounts,
        "subscriptions": subscriptions,
        "targets": targets,
        "templates": templates,
    }


def create_user(repository: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    item = repository.create_user_record(
        username=_to_non_empty_str(payload.get("username"), "username"),
        email=str(payload.get("email") or "").strip(),
        password_hash=str(payload.get("password_hash") or "").strip(),
        role=str(payload.get("role") or "user").strip() or "user",
        status=str(payload.get("status") or "active").strip() or "active",
    )
    return {"item": item}


def list_sources(repository: Any, owner_user_id: Optional[Any] = None) -> Dict[str, Any]:
    normalized_owner_id = (
        _to_positive_int(owner_user_id, "owner_user_id", allow_none=True)
        if owner_user_id is not None and str(owner_user_id).strip() != ""
        else None
    )
    return {"items": repository.list_sources(owner_user_id=normalized_owner_id)}


def create_source(repository: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    owner_user_id = _to_positive_int(payload.get("owner_user_id"), "owner_user_id", allow_none=True)
    _ensure_user_exists(repository, owner_user_id, field_name="owner_user_id")
    item = repository.create_source_record(
        owner_user_id=owner_user_id,
        source_type=_to_non_empty_str(payload.get("source_type"), "source_type"),
        name=_to_non_empty_str(payload.get("name"), "name"),
        status=str(payload.get("status") or "active").strip() or "active",
        visibility=str(payload.get("visibility") or "private").strip() or "private",
        config=_to_object(payload.get("config"), "config"),
    )
    return {"item": item}


def update_source(repository: Any, *, source_id: Any, owner_user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_source_id = _to_positive_int(source_id, "source_id")
    normalized_owner_user_id = _to_positive_int(owner_user_id, "owner_user_id")
    current = repository.get_source(normalized_source_id)
    if not current or int(current.get("owner_user_id") or 0) != normalized_owner_user_id:
        raise ValueError("source_id 对应的来源不存在")
    item = repository.update_source_record(
        source_id=normalized_source_id,
        owner_user_id=normalized_owner_user_id,
        name=_to_non_empty_str(payload.get("name"), "name"),
        visibility=str(payload.get("visibility") or current.get("visibility") or "private").strip() or "private",
        status=str(payload.get("status") or current.get("status") or "active").strip() or "active",
        config=_to_object(payload.get("config"), "config"),
    )
    if not item:
        raise ValueError("source_id 对应的来源不存在")
    return {"item": item}


def update_source_status(repository: Any, *, source_id: Any, owner_user_id: Any, status: Any) -> Dict[str, Any]:
    normalized_source_id = _to_positive_int(source_id, "source_id")
    normalized_owner_user_id = _to_positive_int(owner_user_id, "owner_user_id")
    normalized_status = _normalize_entity_status(status)
    current = repository.get_source(normalized_source_id)
    if not current or int(current.get("owner_user_id") or 0) != normalized_owner_user_id:
        raise ValueError("source_id 对应的来源不存在")
    item = repository.update_source_status(
        source_id=normalized_source_id,
        owner_user_id=normalized_owner_user_id,
        status=normalized_status,
    )
    if not item:
        raise ValueError("source_id 对应的来源不存在")
    return {"item": item}


def delete_source(repository: Any, *, source_id: Any, owner_user_id: Any) -> Dict[str, Any]:
    normalized_source_id = _to_positive_int(source_id, "source_id")
    normalized_owner_user_id = _to_positive_int(owner_user_id, "owner_user_id")
    current = repository.get_source(normalized_source_id)
    if not current or int(current.get("owner_user_id") or 0) != normalized_owner_user_id:
        raise ValueError("source_id 对应的来源不存在")
    if str(current.get("status") or "") != "archived":
        raise ValueError("请先归档来源，再执行删除")
    if int(repository.count_subscriptions_by_source(normalized_source_id, user_id=normalized_owner_user_id) or 0) > 0:
        raise ValueError("该来源已有跟单策略引用，暂不支持删除")
    if int(repository.count_raw_items_by_source(normalized_source_id) or 0) > 0:
        raise ValueError("该来源已有抓取记录，暂不支持删除")
    if int(repository.count_signals_by_source(normalized_source_id) or 0) > 0:
        raise ValueError("该来源已生成标准信号，暂不支持删除")
    deleted = repository.delete_source_record(source_id=normalized_source_id, owner_user_id=normalized_owner_user_id)
    if not deleted:
        raise ValueError("source_id 对应的来源不存在")
    return {"deleted": True, "id": normalized_source_id}


def list_telegram_accounts(repository: Any, user_id: Any) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    items = repository.list_telegram_accounts(user_id=normalized_user_id)
    return {"items": [_decorate_telegram_account(item) for item in items]}


def create_telegram_account(repository: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    user_id = _to_positive_int(payload.get("user_id"), "user_id")
    _ensure_user_exists(repository, user_id)
    raw_meta = _to_object(payload.get("meta"), "meta")
    auth_mode = _resolve_telegram_auth_mode(payload)
    explicit_session_path = str(payload.get("session_path") or "").strip()
    session_path = explicit_session_path or _build_managed_session_path(user_id)
    auth_state = str(raw_meta.get("auth_state") or "").strip()
    if not auth_state:
        auth_state = AUTHORIZED_TELEGRAM_AUTH_STATE if explicit_session_path else ("new" if auth_mode == "phone_login" else "pending_import")
    meta = {
        **raw_meta,
        "auth_mode": auth_mode,
        "auth_state": auth_state,
        "managed_session": not bool(explicit_session_path),
    }
    status = str(payload.get("status") or "").strip() or ("active" if auth_state == AUTHORIZED_TELEGRAM_AUTH_STATE else "inactive")
    item = repository.create_telegram_account_record(
        user_id=user_id,
        label=_to_non_empty_str(payload.get("label"), "label"),
        session_path=session_path,
        phone=str(payload.get("phone") or "").strip(),
        status=status,
        meta=meta,
    )
    return {"item": _decorate_telegram_account(item)}


def update_telegram_account(repository: Any, *, telegram_account_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_account_id = _to_positive_int(telegram_account_id, "telegram_account_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = _get_owned_telegram_account(repository, telegram_account_id=normalized_account_id, user_id=normalized_user_id)
    current_meta = _normalize_account_meta(current.get("meta"))
    raw_meta = _to_object(payload.get("meta"), "meta")
    explicit_session_path = payload.get("session_path")
    session_path = (
        _to_non_empty_str(explicit_session_path, "session_path")
        if explicit_session_path is not None and str(explicit_session_path).strip() != ""
        else str(current.get("session_path") or "")
    )
    auth_mode = (
        _resolve_telegram_auth_mode(payload)
        if payload.get("auth_mode") is not None or current_meta.get("auth_mode") is not None or str(payload.get("phone") or "").strip()
        else str(current_meta.get("auth_mode") or "session_import")
    )
    meta = {
        **current_meta,
        **raw_meta,
        "auth_mode": auth_mode,
        "managed_session": bool(current_meta.get("managed_session")) if explicit_session_path is None else False,
    }
    item = repository.update_telegram_account_record(
        telegram_account_id=normalized_account_id,
        user_id=normalized_user_id,
        label=_to_non_empty_str(payload.get("label"), "label"),
        session_path=session_path,
        phone=str(payload.get("phone") or "").strip(),
        meta=meta,
    )
    if not item:
        raise ValueError("telegram_account_id 对应的账号不存在")
    return {"item": _decorate_telegram_account(item)}


def update_telegram_account_status(repository: Any, *, telegram_account_id: Any, user_id: Any, status: Any) -> Dict[str, Any]:
    normalized_account_id = _to_positive_int(telegram_account_id, "telegram_account_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    normalized_status = _normalize_entity_status(status)
    current = _get_owned_telegram_account(repository, telegram_account_id=normalized_account_id, user_id=normalized_user_id)
    if normalized_status == "active" and not _decorate_telegram_account(current).get("is_authorized"):
        raise ValueError("账号尚未完成授权，不能激活")
    item = repository.update_telegram_account_status(
        telegram_account_id=normalized_account_id,
        user_id=normalized_user_id,
        status=normalized_status,
    )
    if not item:
        raise ValueError("telegram_account_id 对应的账号不存在")
    return {"item": _decorate_telegram_account(item)}


def import_telegram_account_session(repository: Any, *, telegram_account_id: Any, user_id: Any, payload: Dict[str, Any], auth_gateway=None) -> Dict[str, Any]:
    normalized_account_id = _to_positive_int(telegram_account_id, "telegram_account_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = _get_owned_telegram_account(repository, telegram_account_id=normalized_account_id, user_id=normalized_user_id)
    gateway = auth_gateway or _build_account_gateway()

    encoded = _to_non_empty_str(
        payload.get("session_file_base64") or payload.get("file_base64"),
        "session_file_base64",
    )
    try:
        content = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        raise ValueError("Session 文件内容无效")
    if not content:
        raise ValueError("Session 文件不能为空")

    temp_base = str(_session_file_path(str(current.get("session_path") or "")).with_name("import-%s" % uuid4().hex))
    if temp_base.endswith(".session"):
        temp_base = temp_base[:-len(".session")]
    _write_session_bytes(temp_base, content)
    temp_session_file = _session_file_path(temp_base)
    try:
        result = gateway.inspect_session(temp_base)
        if not result.get("authorized"):
            raise ValueError("导入的 Session 尚未授权，不能直接使用")

        final_session_file = _session_file_path(str(current.get("session_path") or ""))
        final_session_file.parent.mkdir(parents=True, exist_ok=True)
        temp_session_file.replace(final_session_file)
    finally:
        if temp_session_file.exists():
            temp_session_file.unlink()

    meta = _clear_pending_login_meta(_normalize_account_meta(current.get("meta")))
    meta.update(
        {
            "auth_mode": "session_import",
            "auth_state": AUTHORIZED_TELEGRAM_AUTH_STATE,
            "managed_session": bool(meta.get("managed_session", True)),
            "session_source_filename": str(payload.get("file_name") or "").strip(),
            "session_validated_at": _utc_now_iso(),
            "last_auth_error": "",
        }
    )
    phone = str(current.get("phone") or "").strip() or str(result.get("phone") or "").strip()
    item = _store_telegram_account(
        repository,
        current=current,
        phone=phone,
        meta=meta,
        status="archived" if str(current.get("status") or "") == "archived" else "active",
    )
    return {"item": _decorate_telegram_account(item)}


def begin_telegram_account_login(repository: Any, *, telegram_account_id: Any, user_id: Any, payload: Dict[str, Any], auth_gateway=None) -> Dict[str, Any]:
    normalized_account_id = _to_positive_int(telegram_account_id, "telegram_account_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = _get_owned_telegram_account(repository, telegram_account_id=normalized_account_id, user_id=normalized_user_id)
    gateway = auth_gateway or _build_account_gateway()

    phone = str(payload.get("phone") or current.get("phone") or "").strip()
    if not phone:
        raise ValueError("手机号不能为空")

    result = gateway.send_login_code(str(current.get("session_path") or ""), phone)
    meta = _clear_pending_login_meta(_normalize_account_meta(current.get("meta")))
    meta.update(
        {
            "auth_mode": "phone_login",
            "auth_state": "code_sent",
            "phone_code_hash": str(result.get("phone_code_hash") or ""),
            "auth_requested_at": _utc_now_iso(),
            "pending_phone": phone,
            "last_auth_error": "",
        }
    )
    item = _store_telegram_account(
        repository,
        current=current,
        phone=phone,
        meta=meta,
        status="archived" if str(current.get("status") or "") == "archived" else "inactive",
    )
    return {"item": _decorate_telegram_account(item)}


def verify_telegram_account_login_code(repository: Any, *, telegram_account_id: Any, user_id: Any, payload: Dict[str, Any], auth_gateway=None) -> Dict[str, Any]:
    normalized_account_id = _to_positive_int(telegram_account_id, "telegram_account_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = _get_owned_telegram_account(repository, telegram_account_id=normalized_account_id, user_id=normalized_user_id)
    gateway = auth_gateway or _build_account_gateway()

    meta = _normalize_account_meta(current.get("meta"))
    phone = str(meta.get("pending_phone") or current.get("phone") or "").strip()
    phone_code_hash = str(meta.get("phone_code_hash") or "").strip()
    if not phone or not phone_code_hash:
        raise ValueError("当前账号没有待验证的登录请求，请先发送验证码")

    result = gateway.verify_code(
        str(current.get("session_path") or ""),
        phone=phone,
        code=_to_non_empty_str(payload.get("code"), "code"),
        phone_code_hash=phone_code_hash,
    )

    next_meta = dict(meta)
    if result.get("password_required"):
        next_meta.update(
            {
                "auth_mode": "phone_login",
                "auth_state": "password_required",
                "last_auth_error": "",
            }
        )
        item = _store_telegram_account(repository, current=current, meta=next_meta)
        return {"item": _decorate_telegram_account(item)}

    next_meta = _clear_pending_login_meta(next_meta)
    next_meta.update(
        {
            "auth_mode": "phone_login",
            "auth_state": AUTHORIZED_TELEGRAM_AUTH_STATE,
            "session_validated_at": _utc_now_iso(),
            "last_auth_error": "",
        }
    )
    phone = str(current.get("phone") or "").strip() or str(result.get("phone") or "").strip() or phone
    item = _store_telegram_account(
        repository,
        current=current,
        phone=phone,
        meta=next_meta,
        status="archived" if str(current.get("status") or "") == "archived" else "active",
    )
    return {"item": _decorate_telegram_account(item)}


def verify_telegram_account_login_password(repository: Any, *, telegram_account_id: Any, user_id: Any, payload: Dict[str, Any], auth_gateway=None) -> Dict[str, Any]:
    normalized_account_id = _to_positive_int(telegram_account_id, "telegram_account_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = _get_owned_telegram_account(repository, telegram_account_id=normalized_account_id, user_id=normalized_user_id)
    gateway = auth_gateway or _build_account_gateway()

    meta = _normalize_account_meta(current.get("meta"))
    result = gateway.verify_password(
        str(current.get("session_path") or ""),
        password=_to_non_empty_str(payload.get("password"), "password"),
    )
    next_meta = _clear_pending_login_meta(meta)
    next_meta.update(
        {
            "auth_mode": "phone_login",
            "auth_state": AUTHORIZED_TELEGRAM_AUTH_STATE,
            "session_validated_at": _utc_now_iso(),
            "last_auth_error": "",
        }
    )
    phone = str(current.get("phone") or "").strip() or str(result.get("phone") or "").strip()
    item = _store_telegram_account(
        repository,
        current=current,
        phone=phone,
        meta=next_meta,
        status="archived" if str(current.get("status") or "") == "archived" else "active",
    )
    return {"item": _decorate_telegram_account(item)}


def delete_telegram_account(repository: Any, *, telegram_account_id: Any, user_id: Any) -> Dict[str, Any]:
    normalized_account_id = _to_positive_int(telegram_account_id, "telegram_account_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = repository.get_telegram_account(normalized_account_id)
    if not current or int(current["user_id"]) != normalized_user_id:
        raise ValueError("telegram_account_id 对应的账号不存在")
    if str(current.get("status") or "") != "archived":
        raise ValueError("请先归档托管账号，再执行删除")
    if int(repository.count_delivery_targets_by_telegram_account(normalized_account_id, user_id=normalized_user_id) or 0) > 0:
        raise ValueError("该托管账号仍被投递群组引用，请先处理关联群组后再删除")
    if int(repository.count_execution_jobs_by_telegram_account(normalized_account_id, user_id=normalized_user_id) or 0) > 0:
        raise ValueError("该托管账号已有执行记录，暂不支持删除")
    deleted = repository.delete_telegram_account_record(telegram_account_id=normalized_account_id, user_id=normalized_user_id)
    if not deleted:
        raise ValueError("telegram_account_id 对应的账号不存在")
    return {"deleted": True, "id": normalized_account_id}


def fetch_source(repository: Any, source_id: Any, fetcher=None) -> Dict[str, Any]:
    normalized_source_id = _to_positive_int(source_id, "source_id")
    return fetch_source_to_raw_item(repository, source_id=normalized_source_id, fetcher=fetcher)


def list_raw_items(repository: Any, source_id: Optional[Any] = None) -> Dict[str, Any]:
    normalized_source_id = (
        _to_positive_int(source_id, "source_id", allow_none=True)
        if source_id is not None and str(source_id).strip() != ""
        else None
    )
    return {"items": repository.list_raw_items(source_id=normalized_source_id)}


def create_raw_item(repository: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    source_id = _to_positive_int(payload.get("source_id"), "source_id")
    _ensure_source_exists(repository, source_id)
    item = repository.create_raw_item_record(
        source_id=source_id,
        external_item_id=str(payload.get("external_item_id") or "").strip() or None,
        issue_no=str(payload.get("issue_no") or "").strip(),
        published_at=str(payload.get("published_at") or "").strip() or None,
        raw_payload=_to_object(payload.get("raw_payload"), "raw_payload"),
        parse_status=str(payload.get("parse_status") or "pending").strip() or "pending",
        parse_error=str(payload.get("parse_error") or "").strip() or None,
    )
    return {"item": item}


def list_signals(repository: Any, source_id: Optional[Any] = None) -> Dict[str, Any]:
    normalized_source_id = (
        _to_positive_int(source_id, "source_id", allow_none=True)
        if source_id is not None and str(source_id).strip() != ""
        else None
    )
    return {"items": repository.list_signals(source_id=normalized_source_id)}


def create_signal(repository: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    source_id = _to_positive_int(payload.get("source_id"), "source_id")
    _ensure_source_exists(repository, source_id)
    item = repository.create_signal_record(
        source_id=source_id,
        lottery_type=_to_non_empty_str(payload.get("lottery_type"), "lottery_type"),
        issue_no=_to_non_empty_str(payload.get("issue_no"), "issue_no"),
        bet_type=_to_non_empty_str(payload.get("bet_type"), "bet_type"),
        bet_value=_to_non_empty_str(payload.get("bet_value"), "bet_value"),
        confidence=float(payload["confidence"]) if payload.get("confidence") not in {None, ""} else None,
        normalized_payload=_to_object(payload.get("normalized_payload"), "normalized_payload"),
        status=str(payload.get("status") or "ready").strip() or "ready",
    )
    return {"item": item}


def normalize_raw_item(repository: Any, raw_item_id: Any) -> Dict[str, Any]:
    normalized_raw_item_id = _to_positive_int(raw_item_id, "raw_item_id")
    return normalize_raw_item_to_signals(repository, raw_item_id=normalized_raw_item_id)


def list_subscriptions(repository: Any, user_id: Any, stat_date: Any = None) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    resolved_stat_date = _normalize_stat_date(stat_date)
    items = _present_subscription_items(repository.list_subscriptions(user_id=normalized_user_id))
    daily_stats = repository.list_user_daily_subscription_stats(user_id=normalized_user_id, stat_date=resolved_stat_date)
    daily_stats_by_subscription = {
        int(item["subscription_id"]): item
        for item in daily_stats
        if item.get("subscription_id") is not None
    }
    enriched_items = [
        {
            **item,
            "stat_date": resolved_stat_date,
            "daily_stat": daily_stats_by_subscription.get(int(item["id"])) or _default_subscription_daily_stat(item, stat_date=resolved_stat_date),
            "daily_history": repository.list_subscription_daily_stats(
                subscription_id=int(item["id"]),
                user_id=normalized_user_id,
                limit=7,
            ),
            "runtime_history": repository.list_subscription_runtime_runs(
                subscription_id=int(item["id"]),
                user_id=normalized_user_id,
                limit=5,
            ),
        }
        for item in items
    ]
    return {
        "items": enriched_items,
        "stat_date": resolved_stat_date,
        "daily_summary": repository.get_user_daily_profit_summary(user_id=normalized_user_id, stat_date=resolved_stat_date),
    }


def create_subscription(repository: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    user_id = _to_positive_int(payload.get("user_id"), "user_id")
    source_id = _to_positive_int(payload.get("source_id"), "source_id")
    _ensure_user_exists(repository, user_id)
    _ensure_source_exists(repository, source_id)
    item = repository.create_subscription_record(
        user_id=user_id,
        source_id=source_id,
        status=_normalize_subscription_status(payload.get("status") or "active"),
        strategy=_normalize_subscription_strategy(_subscription_strategy_input_from_payload(payload)),
    )
    return {"item": present_subscription_item(item)}


def update_subscription(repository: Any, *, subscription_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_subscription_id = _to_positive_int(subscription_id, "subscription_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    source_id = _to_positive_int(payload.get("source_id"), "source_id")
    _ensure_source_exists(repository, source_id)
    current = repository.get_subscription(normalized_subscription_id)
    if not current or int(current["user_id"]) != normalized_user_id:
        raise ValueError("subscription_id 对应的订阅不存在")
    item = repository.update_subscription_record(
        subscription_id=normalized_subscription_id,
        user_id=normalized_user_id,
        source_id=source_id,
        strategy=_normalize_subscription_strategy(_subscription_strategy_input_from_payload(payload)),
        status=current["status"],
    )
    if not item:
        raise ValueError("subscription_id 对应的订阅不存在")
    return {"item": present_subscription_item(item)}


def update_subscription_status(repository: Any, *, subscription_id: Any, user_id: Any, status: Any) -> Dict[str, Any]:
    normalized_subscription_id = _to_positive_int(subscription_id, "subscription_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    normalized_status = _normalize_subscription_status(status)
    item = repository.update_subscription_status(
        subscription_id=normalized_subscription_id,
        user_id=normalized_user_id,
        status=normalized_status,
    )
    if not item:
        raise ValueError("subscription_id 对应的订阅不存在")
    return {"item": present_subscription_item(item)}


def delete_subscription(repository: Any, *, subscription_id: Any, user_id: Any) -> Dict[str, Any]:
    normalized_subscription_id = _to_positive_int(subscription_id, "subscription_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = repository.get_subscription(normalized_subscription_id)
    if not current or int(current["user_id"]) != normalized_user_id:
        raise ValueError("subscription_id 对应的订阅不存在")
    if str(current.get("status") or "") != "archived":
        raise ValueError("请先归档跟单策略，再执行删除")
    deleted = repository.delete_subscription_record(subscription_id=normalized_subscription_id, user_id=normalized_user_id)
    if not deleted:
        raise ValueError("subscription_id 对应的订阅不存在")
    return {"deleted": True, "id": normalized_subscription_id}


def settle_subscription_progression(repository: Any, *, subscription_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_subscription_id = _to_positive_int(subscription_id, "subscription_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    result_type = _to_non_empty_str(payload.get("result_type"), "result_type").strip().lower()
    progression_event_id = payload.get("progression_event_id")
    if progression_event_id not in {None, ""}:
        progression_event_id = _to_positive_int(progression_event_id, "progression_event_id")
    result = repository.settle_progression_event(
        subscription_id=normalized_subscription_id,
        user_id=normalized_user_id,
        result_type=result_type,
        progression_event_id=progression_event_id,
    )
    item = present_subscription_item(result.get("subscription") or repository.get_subscription(normalized_subscription_id))
    return {
        "item": item,
        "progression": result,
    }


def resolve_subscription_progression(repository: Any, *, subscription_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_subscription_id = _to_positive_int(subscription_id, "subscription_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    progression_event_id = payload.get("progression_event_id")
    if progression_event_id not in {None, ""}:
        progression_event_id = _to_positive_int(progression_event_id, "progression_event_id")
    draw_context = _to_object(payload.get("draw_context"), "draw_context")

    current = repository.get_subscription(normalized_subscription_id)
    if not current or int(current.get("user_id") or 0) != normalized_user_id:
        raise ValueError("subscription_id 对应的订阅不存在")
    current_event = (
        repository.get_progression_event(int(progression_event_id))
        if progression_event_id is not None
        else repository.get_latest_pending_progression_event(subscription_id=normalized_subscription_id)
    )
    if not current_event or int(current_event.get("subscription_id") or 0) != normalized_subscription_id:
        raise ValueError("当前没有待结算的倍投状态")
    if int(current_event.get("user_id") or 0) != normalized_user_id:
        raise ValueError("无权结算该倍投状态")

    signal = repository.get_signal(int(current_event.get("signal_id") or 0))
    if not signal:
        raise ValueError("待结算记录对应的信号不存在")
    if str(signal.get("lottery_type") or "").strip().lower() != "pc28":
        raise ValueError("当前只支持 PC28 自动结算")

    settlement_policy = resolve_settlement_runtime_policy(current.get("strategy_v2") or current.get("strategy"), signal.get("normalized_payload"))
    settlement_rule_id = str(current_event.get("settlement_rule_id") or settlement_policy.get("settlement_rule_id") or "").strip()
    resolved = resolve_pc28_result_for_signal(
        signal=signal,
        settlement_rule_id=settlement_rule_id,
        draw_context=draw_context,
    )
    result = repository.settle_progression_event(
        subscription_id=normalized_subscription_id,
        user_id=normalized_user_id,
        result_type=resolved["result_type"],
        progression_event_id=progression_event_id,
        result_context={
            "resolution_mode": "auto",
            "refund_reason": resolved.get("refund_reason"),
            "metric": resolved.get("metric"),
            "predicted_value": resolved.get("predicted_value"),
            "actual_value": resolved.get("actual_value"),
            "draw_snapshot": resolved.get("draw_snapshot"),
        },
    )
    item = present_subscription_item(result.get("subscription") or repository.get_subscription(normalized_subscription_id))
    return {
        "item": item,
        "progression": result,
        "resolved": resolved,
    }


def _empty_batch_resolution_result() -> Dict[str, Any]:
    return {
        "summary": {
            "pending_count": 0,
            "matched_count": 0,
            "resolved_count": 0,
            "hit_count": 0,
            "refund_count": 0,
            "miss_count": 0,
            "unmatched_count": 0,
        },
        "items": [],
        "unmatched": [],
        "draw_source": "",
    }


def _issue_sort_key(value: Any) -> tuple[int, str]:
    text = str(value or "").strip()
    if text.isdigit():
        return (int(text), text)
    return (0, text)


def estimate_pc28_draw_fetch_limit(pending_entries: list[Dict[str, Any]], *, base_limit: int = 60) -> int:
    normalized_base_limit = max(10, min(int(base_limit or 60), 2000))
    issue_numbers = [str(((item.get("event") or {}).get("issue_no")) or "").strip() for item in pending_entries or []]
    numeric_issues = [int(item) for item in issue_numbers if item.isdigit()]
    if not numeric_issues:
        return max(normalized_base_limit, len(issue_numbers) + 10)
    issue_span = max(numeric_issues) - min(numeric_issues) + 1
    return max(normalized_base_limit, issue_span + 10, len(numeric_issues) + 10)


def collect_pending_pc28_progressions(repository: Any, *, user_id: int) -> list[Dict[str, Any]]:
    open_events = repository.list_open_progression_events(user_id=int(user_id), statuses=["placed"], limit=5000)
    subscription_cache: Dict[int, Dict[str, Any]] = {}
    signal_cache: Dict[int, Dict[str, Any]] = {}
    pending_entries = []
    for event in open_events:
        signal_id = int(event.get("signal_id") or 0)
        if signal_id <= 0:
            continue
        signal = signal_cache.get(signal_id)
        if signal is None:
            signal = repository.get_signal(signal_id) or {}
            signal_cache[signal_id] = signal
        if not signal or str(signal.get("lottery_type") or "").strip().lower() != "pc28":
            continue
        subscription_id = int(event.get("subscription_id") or 0)
        if subscription_id <= 0:
            continue
        subscription = subscription_cache.get(subscription_id)
        if subscription is None:
            subscription = repository.get_subscription(subscription_id) or {}
            subscription_cache[subscription_id] = subscription
        if not subscription or int(subscription.get("user_id") or 0) != int(user_id):
            continue
        pending_entries.append(
            {
                "subscription": subscription,
                "event": event,
                "signal": signal,
            }
        )
    pending_entries.sort(
        key=lambda item: (
            int(item["subscription"]["id"]),
            _issue_sort_key((item.get("event") or {}).get("issue_no")),
            int((item.get("event") or {}).get("id") or 0),
        )
    )
    return pending_entries


def resolve_pending_pc28_progressions_from_draws(
    repository: Any,
    *,
    user_id: int,
    draw_items: list[Dict[str, Any]],
    draw_source: str = "",
) -> Dict[str, Any]:
    pending_entries = collect_pending_pc28_progressions(repository, user_id=int(user_id))
    if not pending_entries:
        result = _empty_batch_resolution_result()
        result["draw_source"] = str(draw_source or "")
        return result

    draw_map = {
        str(item.get("issue_no") or "").strip(): item
        for item in draw_items or []
        if str(item.get("issue_no") or "").strip()
    }
    resolved_items = []
    unmatched = []
    hit_count = 0
    refund_count = 0
    miss_count = 0
    for entry in pending_entries:
        issue_no = str((entry.get("event") or {}).get("issue_no") or "").strip()
        draw = draw_map.get(issue_no)
        if not draw:
            unmatched.append(
                {
                    "subscription_id": int(entry["subscription"]["id"]),
                    "progression_event_id": int(entry["event"]["id"]),
                    "issue_no": issue_no,
                }
            )
            continue
        resolved = resolve_subscription_progression(
            repository,
            subscription_id=entry["subscription"]["id"],
            user_id=int(user_id),
            payload={
                "progression_event_id": entry["event"]["id"],
                "draw_context": dict(draw.get("draw_context") or {}),
            },
        )
        result_type = str(((resolved.get("resolved") or {}).get("result_type")) or "").strip().lower()
        if result_type == "hit":
            hit_count += 1
        elif result_type == "refund":
            refund_count += 1
        elif result_type == "miss":
            miss_count += 1
        resolved_items.append(
            {
                "subscription_id": int(entry["subscription"]["id"]),
                "progression_event_id": int(entry["event"]["id"]),
                "issue_no": issue_no,
                "result_type": result_type,
                "refund_reason": (resolved.get("resolved") or {}).get("refund_reason"),
            }
        )

    return {
        "summary": {
            "pending_count": len(pending_entries),
            "matched_count": len(pending_entries) - len(unmatched),
            "resolved_count": len(resolved_items),
            "hit_count": hit_count,
            "refund_count": refund_count,
            "miss_count": miss_count,
            "unmatched_count": len(unmatched),
        },
        "items": resolved_items,
        "unmatched": unmatched,
        "draw_source": str(draw_source or ""),
    }


def resolve_pending_subscription_progressions(repository: Any, *, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    draw_limit = max(10, min(int(payload.get("draw_limit") or 60), 2000))
    pending_entries = collect_pending_pc28_progressions(repository, user_id=normalized_user_id)
    if not pending_entries:
        return _empty_batch_resolution_result()
    fetch_result = fetch_pc28_recent_draws_deep(
        limit=estimate_pc28_draw_fetch_limit(pending_entries, base_limit=draw_limit)
    )
    return resolve_pending_pc28_progressions_from_draws(
        repository,
        user_id=normalized_user_id,
        draw_items=list(fetch_result.get("items") or []),
        draw_source=str(fetch_result.get("source") or ""),
    )


def reset_subscription_runtime(repository: Any, *, subscription_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_subscription_id = _to_positive_int(subscription_id, "subscription_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    result = repository.reset_subscription_runtime(
        subscription_id=normalized_subscription_id,
        user_id=normalized_user_id,
        note=str(payload.get("note") or "").strip(),
    )
    if isinstance(result.get("item"), dict):
        result["item"] = present_subscription_item(result.get("item"))
    return result


def restart_subscription_cycle(repository: Any, *, subscription_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_subscription_id = _to_positive_int(subscription_id, "subscription_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = repository.get_subscription(normalized_subscription_id)
    if not current or int(current.get("user_id") or 0) != normalized_user_id:
        raise ValueError("subscription_id 对应的订阅不存在")
    if str(current.get("status") or "") == "archived":
        raise ValueError("已归档的跟单策略不能直接开始新一轮，请先恢复后再试")

    result = repository.reset_subscription_runtime(
        subscription_id=normalized_subscription_id,
        user_id=normalized_user_id,
        note=str(payload.get("note") or "").strip(),
    )
    item = repository.update_subscription_status(
        subscription_id=normalized_subscription_id,
        user_id=normalized_user_id,
        status="active",
    )
    if not item:
        raise ValueError("subscription_id 对应的订阅不存在")
    result["item"] = present_subscription_item(item)
    result["restarted"] = True
    return result


def _group_targets_by_template(targets: list[Dict[str, Any]]) -> Dict[int, list[Dict[str, Any]]]:
    grouped: Dict[int, list[Dict[str, Any]]] = defaultdict(list)
    for target in targets:
        if target.get("template_id") is None:
            continue
        try:
            template_id = int(target["template_id"])
        except (TypeError, ValueError):
            continue
        grouped[template_id].append(target)
    return grouped


def list_message_templates(repository: Any, user_id: Any) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    templates = repository.list_message_templates(user_id=normalized_user_id)
    targets = repository.list_delivery_targets(user_id=normalized_user_id)
    template_bindings = _group_targets_by_template(targets)
    for template in templates:
        if template.get("id") is None:
            continue
        bindings = template_bindings.get(int(template["id"])) or []
        constructed_targets = []
        active_count = 0
        for target in bindings:
            constructed_targets.append(
                {
                    "id": target.get("id"),
                    "target_name": target.get("target_name"),
                    "target_key": target.get("target_key"),
                    "status": target.get("status"),
                }
            )
            if target.get("status") == "active":
                active_count += 1
        if template.get("usage_count") is None:
            template["usage_count"] = len(constructed_targets)
        if template.get("bound_targets") is None:
            template["bound_targets"] = constructed_targets
        if template.get("active_target_count") is None:
            template["active_target_count"] = active_count
    return {"items": templates}


def create_message_template(repository: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    user_id = _to_positive_int(payload.get("user_id"), "user_id")
    _ensure_user_exists(repository, user_id)
    item = repository.create_message_template_record(
        user_id=user_id,
        name=_to_non_empty_str(payload.get("name"), "name"),
        lottery_type=_to_non_empty_str(payload.get("lottery_type"), "lottery_type"),
        bet_type=str(payload.get("bet_type") or "*").strip() or "*",
        template_text=_to_non_empty_str(payload.get("template_text"), "template_text"),
        config=_normalize_template_config(payload.get("config")),
        status=str(payload.get("status") or "active").strip() or "active",
    )
    return {"item": item}


def update_message_template(repository: Any, *, template_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_template_id = _to_positive_int(template_id, "template_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = repository.get_message_template(normalized_template_id)
    if not current or int(current.get("user_id") or 0) != normalized_user_id:
        raise ValueError("template_id 对应的下注格式模板不存在")
    item = repository.update_message_template_record(
        template_id=normalized_template_id,
        user_id=normalized_user_id,
        name=_to_non_empty_str(payload.get("name"), "name"),
        lottery_type=_to_non_empty_str(payload.get("lottery_type"), "lottery_type"),
        bet_type=str(payload.get("bet_type") or "*").strip() or "*",
        template_text=_to_non_empty_str(payload.get("template_text"), "template_text"),
        config=_normalize_template_config(payload.get("config")),
    )
    if not item:
        raise ValueError("template_id 对应的下注格式模板不存在")
    return {"item": item}


def update_message_template_status(repository: Any, *, template_id: Any, user_id: Any, status: Any) -> Dict[str, Any]:
    normalized_template_id = _to_positive_int(template_id, "template_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    normalized_status = _normalize_entity_status(status)
    item = repository.update_message_template_status(
        template_id=normalized_template_id,
        user_id=normalized_user_id,
        status=normalized_status,
    )
    if not item:
        raise ValueError("template_id 对应的下注格式模板不存在")
    return {"item": item}


def _failure_digest_for_target(failures: list[Dict[str, Any]]) -> Dict[str, Any]:
    if not failures:
        return {}
    reason_counter: Counter[str] = Counter()
    for failure in failures:
        reason = str(failure.get("error_message") or failure.get("delivery_status") or "执行失败").strip()
        if not reason:
            reason = "执行失败"
        reason_counter[reason] += 1
    lines = []
    for reason, count in reason_counter.most_common(3):
        lines.append("%s %s次" % (reason, count))
    top_reason = lines[0] if lines else ""
    return {
        "count": len(failures),
        "top_reason": top_reason,
        "details": lines,
        "last_failure_at": failures[0].get("executed_at"),
    }


def list_delivery_targets(repository: Any, user_id: Any) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    targets = repository.list_delivery_targets(user_id=normalized_user_id)
    subscriptions = repository.list_subscriptions(user_id=normalized_user_id)
    sources = repository.list_sources(owner_user_id=normalized_user_id)
    source_map = {int(source["id"]): source for source in sources if source.get("id") is not None}

    subscription_refs = []
    for subscription in subscriptions:
        source_info = source_map.get(int(subscription["source_id"])) if subscription.get("source_id") is not None else None
        subscription_refs.append(
            {
                "id": subscription.get("id"),
                "source_id": subscription.get("source_id"),
                "source_name": (source_info or {}).get("name") or ("#" + str(subscription.get("source_id") or "--")),
                "status": subscription.get("status"),
            }
        )

    recent_failures = repository.list_recent_execution_failures(user_id=normalized_user_id, limit=max(1, min(len(targets) * 3 or 1, 200)))
    failures_by_target: Dict[int, list[Dict[str, Any]]] = defaultdict(list)
    for failure in recent_failures:
        target_id = failure.get("delivery_target_id")
        if target_id is None:
            continue
        failures_by_target[int(target_id)].append(failure)

    recent_jobs = repository.list_execution_jobs(user_id=normalized_user_id, limit=max(1, min(len(targets) * 3 or 1, 200)))
    latest_job_by_target: Dict[int, Dict[str, Any]] = {}
    for job in recent_jobs:
        target_id = job.get("delivery_target_id")
        if target_id is None:
            continue
        if int(target_id) in latest_job_by_target:
            continue
        latest_job_by_target[int(target_id)] = job

    for target in targets:
        target_id = target.get("id")
        if target_id is None:
            continue
        key = int(target_id)
        target_failures = failures_by_target.get(key, [])
        target["recent_failure_summary"] = _failure_digest_for_target(target_failures)
        job_info = latest_job_by_target.get(key)
        if job_info:
            target["recent_execution_at"] = job_info.get("last_executed_at")
            target["recent_execution_status"] = job_info.get("last_delivery_status")
            target["recent_execution_attempt_count"] = job_info.get("attempt_count")
        target["subscription_refs"] = list(subscription_refs)
        active_sub_count = sum(1 for entry in subscription_refs if entry.get("status") == "active")
        target["matched_subscription_count"] = len(subscription_refs)
        target["active_matched_subscription_count"] = active_sub_count
    return {"items": targets}


def create_delivery_target(repository: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    user_id = _to_positive_int(payload.get("user_id"), "user_id")
    telegram_account_id = _to_positive_int(
        payload.get("telegram_account_id"),
        "telegram_account_id",
        allow_none=True,
    )
    template_id = _to_positive_int(payload.get("template_id"), "template_id", allow_none=True)
    _ensure_user_exists(repository, user_id)
    _ensure_telegram_account_exists(repository, telegram_account_id)
    _ensure_owned_telegram_account(repository, telegram_account_id=telegram_account_id, user_id=int(user_id))
    _ensure_message_template_exists(repository, template_id)
    _ensure_owned_message_template(repository, template_id=template_id, user_id=int(user_id))
    item = repository.create_delivery_target_record(
        user_id=user_id,
        telegram_account_id=telegram_account_id,
        executor_type=_to_non_empty_str(payload.get("executor_type"), "executor_type"),
        target_key=normalize_telegram_target_key(_to_non_empty_str(payload.get("target_key"), "target_key")),
        target_name=str(payload.get("target_name") or "").strip(),
        template_id=template_id,
        status="inactive",
    )
    return {"item": item}


def update_delivery_target(repository: Any, *, delivery_target_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_target_id = _to_positive_int(delivery_target_id, "delivery_target_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    telegram_account_id = _to_positive_int(payload.get("telegram_account_id"), "telegram_account_id", allow_none=True)
    template_id = _to_positive_int(payload.get("template_id"), "template_id", allow_none=True)
    _ensure_telegram_account_exists(repository, telegram_account_id)
    _ensure_owned_telegram_account(repository, telegram_account_id=telegram_account_id, user_id=int(normalized_user_id))
    _ensure_message_template_exists(repository, template_id)
    _ensure_owned_message_template(repository, template_id=template_id, user_id=int(normalized_user_id))
    current = repository.get_delivery_target(normalized_target_id)
    if not current or int(current["user_id"]) != normalized_user_id:
        raise ValueError("delivery_target_id 对应的投递目标不存在")
    item = repository.update_delivery_target_record(
        delivery_target_id=normalized_target_id,
        user_id=normalized_user_id,
        telegram_account_id=telegram_account_id,
        executor_type=_to_non_empty_str(payload.get("executor_type"), "executor_type"),
        target_key=normalize_telegram_target_key(_to_non_empty_str(payload.get("target_key"), "target_key")),
        target_name=str(payload.get("target_name") or "").strip(),
        template_id=template_id,
    )
    if not item:
        raise ValueError("delivery_target_id 对应的投递目标不存在")
    return {"item": item}


def test_delivery_target_send(repository: Any, *, delivery_target_id: Any, user_id: Any, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    normalized_target_id = _to_positive_int(delivery_target_id, "delivery_target_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = repository.get_delivery_target(normalized_target_id)
    if not current or int(current.get("user_id") or 0) != normalized_user_id:
        raise ValueError("delivery_target_id 对应的投递目标不存在")
    if str(current.get("status") or "") == "archived":
        _raise_actionable(
            "测试发送失败：该投递群组已归档。",
            reason_code="target_archived",
            why="已归档的投递群组不会参与发送，也不允许再做可达性测试。",
            next_step="先取消归档到 inactive，再重新测试发送。",
        )
    account_id = current.get("telegram_account_id")
    if account_id is None:
        _raise_actionable(
            "测试发送失败：该投递群组还没有绑定托管账号。",
            reason_code="target_account_missing",
            why="没有绑定账号时，系统不知道该用哪个 Telegram session 发送测试消息。",
            next_step="先选择一个已授权的托管账号保存群组，再重新测试发送。",
        )

    account = repository.get_telegram_account(int(account_id))
    if not account or int(account.get("user_id") or 0) != normalized_user_id:
        _raise_actionable(
            "测试发送失败：绑定的托管账号不可用。",
            reason_code="target_account_invalid",
            why="该群组绑定的账号不存在，或不属于当前用户。",
            next_step="重新选择一个属于当前用户的托管账号后再测试发送。",
        )
    decorated = _decorate_telegram_account(account)
    if not decorated.get("is_authorized"):
        _raise_actionable(
            "测试发送失败：托管账号尚未完成授权。",
            reason_code="account_unauthorized",
            why="当前账号还不能正常发送 Telegram 消息。",
            next_step="先完成验证码或二次密码授权，再重新测试发送。",
        )
    if str(account.get("status") or "") == "archived":
        _raise_actionable(
            "测试发送失败：托管账号已归档。",
            reason_code="account_archived",
            why="已归档账号不会参与消息发送。",
            next_step="先恢复该账号，或切换到其他已授权账号后重新测试。",
        )

    target_key = normalize_telegram_target_key(str(current.get("target_key") or ""))
    message_text = str((payload or {}).get("message_text") or "").strip()
    if not message_text:
        message_text = "【pc28touzhu】测试发送成功。"

    executor = get_runtime_config().executor
    sender = TelethonMessageSender(
        api_id=executor.telegram_api_id,
        api_hash=executor.telegram_api_hash,
        phone=str(account.get("phone") or ""),
        session=str(account.get("session_path") or ""),
    )
    try:
        sender.connect()
        result = sender.send_text(target_key, message_text)
        item = repository.update_delivery_target_test_result(
            delivery_target_id=normalized_target_id,
            user_id=normalized_user_id,
            last_test_status="success",
            last_test_error_code="",
            last_test_message="测试发送成功，目标群组可正常接收消息。",
            last_tested_at=_utc_now_iso(),
        )
    except Exception as exc:
        feedback = _target_test_feedback_from_exception(exc)
        repository.update_delivery_target_test_result(
            delivery_target_id=normalized_target_id,
            user_id=normalized_user_id,
            last_test_status="failed",
            last_test_error_code=feedback["reason_code"],
            last_test_message=feedback["why"],
            last_tested_at=_utc_now_iso(),
        )
        raise ActionableValueError(
            feedback["error"],
            reason_code=feedback["reason_code"],
            why=feedback["why"],
            next_step=feedback["next_step"],
        )
    finally:
        sender.disconnect()
    return {
        "delivery_target_id": int(current["id"]),
        "telegram_account_id": int(account["id"]),
        "target_key": target_key,
        "message_text": message_text,
        "item": item or repository.get_delivery_target(normalized_target_id),
        "test_status": "success",
        "result": result,
    }


def update_delivery_target_status(repository: Any, *, delivery_target_id: Any, user_id: Any, status: Any) -> Dict[str, Any]:
    normalized_target_id = _to_positive_int(delivery_target_id, "delivery_target_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    normalized_status = _normalize_entity_status(status)
    current = repository.get_delivery_target(normalized_target_id)
    if not current or int(current.get("user_id") or 0) != normalized_user_id:
        raise ValueError("delivery_target_id 对应的投递目标不存在")
    if normalized_status == "active":
        if str(current.get("last_test_status") or "") != "success":
            _raise_actionable(
                "启用失败：该投递群组还没有通过测试发送。",
                reason_code="target_test_required",
                why="创建后的群组默认是 inactive，且最近一次测试发送没有成功。",
                next_step="先点击测试发送，确认消息发送成功后再切换到 active。",
            )
        account_id = current.get("telegram_account_id")
        if account_id is None:
            _raise_actionable(
                "启用失败：该投递群组没有绑定托管账号。",
                reason_code="target_account_missing",
                why="没有账号就无法实际发送消息。",
                next_step="先给群组绑定一个已授权账号，再重新启用。",
            )
        account = repository.get_telegram_account(int(account_id))
        if not account or int(account.get("user_id") or 0) != normalized_user_id:
            _raise_actionable(
                "启用失败：绑定的托管账号不可用。",
                reason_code="target_account_invalid",
                why="该账号不存在，或者不属于当前用户。",
                next_step="重新绑定一个有效托管账号后再启用。",
            )
        if not _decorate_telegram_account(account).get("is_authorized"):
            _raise_actionable(
                "启用失败：绑定账号尚未完成授权。",
                reason_code="account_unauthorized",
                why="账号当前不能正常发消息，启用后也无法执行。",
                next_step="先完成账号授权，再重新启用群组。",
            )
        if str(account.get("status") or "") == "archived":
            _raise_actionable(
                "启用失败：绑定账号已归档。",
                reason_code="account_archived",
                why="已归档账号不会参与发送任务。",
                next_step="恢复账号或切换到其他已授权账号后再启用。",
            )
    item = repository.update_delivery_target_status(
        delivery_target_id=normalized_target_id,
        user_id=normalized_user_id,
        status=normalized_status,
    )
    if not item:
        raise ValueError("delivery_target_id 对应的投递目标不存在")
    return {"item": item}


def delete_delivery_target(repository: Any, *, delivery_target_id: Any, user_id: Any) -> Dict[str, Any]:
    normalized_target_id = _to_positive_int(delivery_target_id, "delivery_target_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = repository.get_delivery_target(normalized_target_id)
    if not current or int(current["user_id"]) != normalized_user_id:
        raise ValueError("delivery_target_id 对应的投递目标不存在")
    if str(current.get("status") or "") != "archived":
        raise ValueError("请先归档投递群组，再执行删除")
    if int(repository.count_execution_jobs_by_delivery_target(normalized_target_id, user_id=normalized_user_id) or 0) > 0:
        raise ValueError("该投递群组已有执行记录，暂不支持删除")
    deleted = repository.delete_delivery_target_record(delivery_target_id=normalized_target_id, user_id=normalized_user_id)
    if not deleted:
        raise ValueError("delivery_target_id 对应的投递目标不存在")
    return {"deleted": True, "id": normalized_target_id}


def list_execution_jobs(
    repository: Any,
    *,
    user_id: Any = None,
    signal_id: Optional[Any] = None,
    status: Optional[Any] = None,
    limit: Any = 100,
) -> Dict[str, Any]:
    normalized_user_id = (
        _to_positive_int(user_id, "user_id", allow_none=True)
        if user_id is not None and str(user_id).strip() != ""
        else None
    )
    normalized_signal_id = (
        _to_positive_int(signal_id, "signal_id", allow_none=True)
        if signal_id is not None and str(signal_id).strip() != ""
        else None
    )
    normalized_status = str(status or "").strip()
    if normalized_status and normalized_status not in ALLOWED_EXECUTION_JOB_STATUSES:
        raise ValueError("status 不合法")

    items = repository.list_execution_jobs(
        user_id=normalized_user_id,
        signal_id=normalized_signal_id,
        status=normalized_status or None,
        limit=max(1, min(int(limit or 100), 200)),
    )
    for item in items:
        item["can_retry"] = item.get("status") in RETRYABLE_EXECUTION_JOB_STATUSES
    return {"items": items}


def retry_execution_job(repository: Any, *, job_id: Any, user_id: Any) -> Dict[str, Any]:
    normalized_job_id = _to_positive_int(job_id, "job_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    item = repository.retry_execution_job(job_id=normalized_job_id, user_id=normalized_user_id)
    item["can_retry"] = item.get("status") in RETRYABLE_EXECUTION_JOB_STATUSES
    return {"item": item}


def list_executor_instances(
    repository: Any,
    *,
    limit: Any = 20,
    stale_after_seconds: int = 60,
    offline_after_seconds: int = 300,
    failure_streak_threshold: int = 3,
) -> Dict[str, Any]:
    items = repository.list_executor_instances(limit=max(1, min(int(limit or 20), 100)))
    for item in items:
        heartbeat_status, heartbeat_age_seconds = _heartbeat_state(
            item.get("last_seen_at"),
            stale_after_seconds=stale_after_seconds,
            offline_after_seconds=offline_after_seconds,
        )
        item["heartbeat_status"] = heartbeat_status
        item["heartbeat_age_seconds"] = heartbeat_age_seconds
        item["is_online"] = heartbeat_status == "online"
        attempts = repository.list_executor_attempts(
            executor_id=item["executor_id"],
            limit=max(10, int(failure_streak_threshold) + 2),
        )
        item["recent_failure_streak"] = _failure_streak(attempts)
    return {"items": items}


def list_recent_execution_failures(
    repository: Any,
    *,
    user_id: Any = None,
    limit: Any = 20,
    auto_retry_max_attempts: int = 3,
    auto_retry_base_delay_seconds: int = 30,
) -> Dict[str, Any]:
    normalized_user_id = (
        _to_positive_int(user_id, "user_id", allow_none=True)
        if user_id is not None and str(user_id).strip() != ""
        else None
    )
    items = repository.list_recent_execution_failures(
        user_id=normalized_user_id,
        limit=max(1, min(int(limit or 20), 100)),
    )
    for item in items:
        item["can_retry"] = item.get("job_status") in RETRYABLE_EXECUTION_JOB_STATUSES
        item.update(
            _auto_retry_policy(
                job_status=item.get("job_status"),
                attempt_count=item.get("attempt_count"),
                executed_at=item.get("executed_at"),
                max_attempts=auto_retry_max_attempts,
                base_delay_seconds=auto_retry_base_delay_seconds,
            )
        )
    return {"items": items}


def list_platform_alerts(
    repository: Any,
    *,
    user_id: Any = None,
    limit: Any = 50,
    stale_after_seconds: int = 60,
    offline_after_seconds: int = 300,
    auto_retry_max_attempts: int = 3,
    auto_retry_base_delay_seconds: int = 30,
    failure_streak_threshold: int = 3,
) -> Dict[str, Any]:
    alerts = []
    bounded_limit = max(1, min(int(limit or 50), 200))

    executor_items = list_executor_instances(
        repository,
        limit=bounded_limit,
        stale_after_seconds=stale_after_seconds,
        offline_after_seconds=offline_after_seconds,
        failure_streak_threshold=failure_streak_threshold,
    )["items"]
    for item in executor_items:
        if item["heartbeat_status"] == "offline":
            alerts.append(
                _alert_item(
                    severity="critical",
                    alert_type="executor_offline",
                    title="执行器离线",
                    message="%s 已超过 %s 秒未上报心跳" % (item["executor_id"], item.get("heartbeat_age_seconds") or 0),
                    metadata={"executor_id": item["executor_id"]},
                    key_parts={"executor_id": item["executor_id"]},
                )
            )
        elif item["heartbeat_status"] == "stale":
            alerts.append(
                _alert_item(
                    severity="warning",
                    alert_type="executor_stale",
                    title="执行器心跳延迟",
                    message="%s 心跳延迟 %s 秒" % (item["executor_id"], item.get("heartbeat_age_seconds") or 0),
                    metadata={"executor_id": item["executor_id"]},
                    key_parts={"executor_id": item["executor_id"]},
                )
            )

        if int(item.get("recent_failure_streak") or 0) >= int(failure_streak_threshold):
            alerts.append(
                _alert_item(
                    severity="critical",
                    alert_type="executor_failure_streak",
                    title="执行器连续失败过多",
                    message="%s 最近连续失败 %s 次" % (
                        item["executor_id"],
                        item["recent_failure_streak"],
                    ),
                    metadata={
                        "executor_id": item["executor_id"],
                        "recent_failure_streak": int(item["recent_failure_streak"]),
                    },
                    key_parts={"executor_id": item["executor_id"]},
                )
            )

    failure_items = list_recent_execution_failures(
        repository,
        user_id=user_id,
        limit=bounded_limit,
        auto_retry_max_attempts=auto_retry_max_attempts,
        auto_retry_base_delay_seconds=auto_retry_base_delay_seconds,
    )["items"]
    for item in failure_items:
        if item["auto_retry_state"] == "exhausted":
            alerts.append(
                _alert_item(
                    severity="warning",
                    alert_type="job_retry_exhausted",
                    title="任务已达到自动重试上限",
                    message="任务 #%s 已失败 %s 次，需要人工处理" % (
                        item["job_id"],
                        item["attempt_count"],
                    ),
                    metadata={
                        "job_id": item["job_id"],
                        "signal_id": item["signal_id"],
                        "user_id": item.get("user_id"),
                    },
                    key_parts={"job_id": item["job_id"]},
                )
            )

    severity_order = {"critical": 0, "warning": 1, "info": 2}
    alerts.sort(key=lambda item: (severity_order.get(item["severity"], 99), item["alert_type"], item["title"]))
    alerts = alerts[:bounded_limit]
    records_map = repository.list_platform_alert_records_by_keys([str(item["alert_key"]) for item in alerts])
    for item in alerts:
        record = records_map.get(str(item["alert_key"]))
        item["notification"] = record
        if record is None:
            item["notification_status"] = "pending"
        elif record.get("last_error"):
            item["notification_status"] = "failed"
        elif record.get("last_sent_at"):
            item["notification_status"] = "sent"
        else:
            item["notification_status"] = "pending"
    return {"items": alerts}


def dispatch_signal(repository: Any, signal_id: Any) -> Dict[str, Any]:
    normalized_signal_id = _to_positive_int(signal_id, "signal_id")
    return dispatch_signal_jobs(repository, signal_id=normalized_signal_id)
