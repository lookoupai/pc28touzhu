"""Telegram Bot binding and profit query services."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Protocol
from uuid import uuid4


SHANGHAI_TZ = timezone(timedelta(hours=8))
DEFAULT_BOT_STATE_KEY = "profit-query-bot"


class TelegramBotClient(Protocol):
    def send_text(self, target_chat_id: str, message_text: str) -> Dict[str, Any]:
        ...

    def get_updates(
        self,
        *,
        offset: Optional[int] = None,
        timeout_seconds: int = 10,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_now_iso() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _format_iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _parse_iso8601(value: Optional[str]) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _normalize_stat_date(value: Optional[str], *, reference_time: Optional[datetime] = None) -> str:
    text = str(value or "").strip()
    if not text:
        local_now = (reference_time or _utc_now()).astimezone(SHANGHAI_TZ)
        return (local_now.date() - timedelta(days=1)).isoformat()
    try:
        return datetime.strptime(text, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        raise ValueError("日期格式必须为 YYYY-MM-DD") from exc


def _signed_money(value: Any) -> str:
    amount = round(float(value or 0), 2)
    if amount > 0:
        return "+%.2f" % amount
    return "%.2f" % amount


def _mask_username(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return "用户"
    if len(text) <= 1:
        return text + "*"
    if len(text) == 2:
        return text[:1] + "*"
    return text[:1] + ("*" * min(3, len(text) - 1))


def _build_help_text() -> str:
    return "\n".join(
        [
            "可用命令：",
            "/bind <绑定码> 绑定平台账号",
            "/profit 查询昨天全部方案汇总",
            "/profit YYYY-MM-DD 查询指定日期汇总",
            "/plan YYYY-MM-DD 查询指定日期单方案列表",
            "/plan <方案名> YYYY-MM-DD 查询指定方案盈亏",
        ]
    )


def get_telegram_binding_status(repository: Any, *, user_id: Any) -> Dict[str, Any]:
    normalized_user_id = int(user_id)
    if not repository.get_user(normalized_user_id):
        raise ValueError("user_id 对应的用户不存在")
    return {"item": repository.get_user_telegram_binding(normalized_user_id)}


def create_telegram_bind_token(
    repository: Any,
    *,
    user_id: Any,
    ttl_seconds: int = 600,
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    normalized_user_id = int(user_id)
    if not repository.get_user(normalized_user_id):
        raise ValueError("user_id 对应的用户不存在")
    now = reference_time or _utc_now()
    expire_at = _format_iso8601(now + timedelta(seconds=max(60, int(ttl_seconds or 600))))
    token = uuid4().hex[:12].upper()
    item = repository.set_user_telegram_bind_token(
        user_id=normalized_user_id,
        bind_token=token,
        expire_at=expire_at,
    )
    return {"item": item}


def clear_telegram_binding(repository: Any, *, user_id: Any) -> Dict[str, Any]:
    normalized_user_id = int(user_id)
    if not repository.get_user(normalized_user_id):
        raise ValueError("user_id 对应的用户不存在")
    return {"item": repository.clear_user_telegram_binding(user_id=normalized_user_id)}


def bind_telegram_user(
    repository: Any,
    *,
    bind_token: str,
    telegram_user_id: int,
    telegram_chat_id: str,
    telegram_username: str = "",
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    normalized_token = str(bind_token or "").strip().upper()
    if not normalized_token:
        raise ValueError("绑定码不能为空")
    user = repository.get_user_by_telegram_bind_token(normalized_token)
    if not user:
        raise ValueError("绑定码无效或已失效")

    binding = repository.get_user_telegram_binding(int(user["id"]))
    expire_at = _parse_iso8601(binding.get("bind_token_expire_at"))
    now = reference_time or _utc_now()
    if expire_at is None or now > expire_at:
        repository.clear_user_telegram_bind_token(user_id=int(user["id"]))
        raise ValueError("绑定码已过期，请回到平台重新生成")

    item = repository.update_user_telegram_binding(
        user_id=int(user["id"]),
        telegram_user_id=int(telegram_user_id),
        telegram_chat_id=str(telegram_chat_id or "").strip(),
        telegram_username=str(telegram_username or "").strip(),
        telegram_bound_at=_format_iso8601(now),
    )
    return {"user": repository.get_user(int(user["id"])) or {}, "item": item}


def _resolve_bound_user(repository: Any, *, telegram_user_id: int) -> Dict[str, Any]:
    user = repository.get_user_by_telegram_user_id(int(telegram_user_id))
    if not user:
        raise ValueError("当前 Telegram 账号尚未绑定平台用户，请先在平台生成绑定码后发送 /bind <绑定码>")
    return user


def _build_profit_summary_text(user: Dict[str, Any], summary: Dict[str, Any]) -> str:
    stat_date = str(summary.get("stat_date") or "")
    if int(summary.get("settled_event_count") or 0) <= 0:
        return "%s 暂无已结算跟单数据。" % stat_date
    return "\n".join(
        [
            "【%s 跟单汇总】" % stat_date,
            "账号: %s" % str(user.get("username") or ""),
            "方案数: %s" % int(summary.get("plan_count") or 0),
            "已结算: %s" % int(summary.get("settled_event_count") or 0),
            "盈利: %.2f" % round(float(summary.get("profit_amount") or 0), 2),
            "亏损: %.2f" % round(float(summary.get("loss_amount") or 0), 2),
            "净利润: %s" % _signed_money(summary.get("net_profit")),
            "胜/负/退: %s/%s/%s"
            % (
                int(summary.get("hit_count") or 0),
                int(summary.get("miss_count") or 0),
                int(summary.get("refund_count") or 0),
            ),
            "发送 /plan %s 查看单方案明细" % stat_date,
        ]
    )


def _build_plan_list_text(stat_date: str, items: List[Dict[str, Any]]) -> str:
    if not items:
        return "%s 暂无单方案已结算数据。" % stat_date
    lines = ["【%s 单方案明细】" % stat_date]
    for index, item in enumerate(items, start=1):
        lines.append(
            "%s. %s %s | 盈 %.2f 亏 %.2f | %s 笔"
            % (
                index,
                str(item.get("source_name") or "未命名方案"),
                _signed_money(item.get("net_profit")),
                round(float(item.get("profit_amount") or 0), 2),
                round(float(item.get("loss_amount") or 0), 2),
                int(item.get("settled_event_count") or 0),
            )
        )
    return "\n".join(lines)


def _build_plan_detail_text(item: Dict[str, Any]) -> str:
    return "\n".join(
        [
            "【%s 方案收益】" % str(item.get("stat_date") or ""),
            "方案: %s" % str(item.get("source_name") or "未命名方案"),
            "盈利: %.2f" % round(float(item.get("profit_amount") or 0), 2),
            "亏损: %.2f" % round(float(item.get("loss_amount") or 0), 2),
            "净利润: %s" % _signed_money(item.get("net_profit")),
            "已结算: %s" % int(item.get("settled_event_count") or 0),
            "胜/负/退: %s/%s/%s"
            % (
                int(item.get("hit_count") or 0),
                int(item.get("miss_count") or 0),
                int(item.get("refund_count") or 0),
            ),
        ]
    )


def _build_candidate_text(stat_date: str, source_names: List[str]) -> str:
    if not source_names:
        return "%s 暂无可查询的方案数据。" % stat_date
    return "\n".join(
        [
            "未找到对应方案，请确认方案名称。",
            "可用方案：",
            *["- %s" % name for name in source_names[:10]],
        ]
    )


def handle_telegram_command(
    repository: Any,
    *,
    telegram_user_id: int,
    telegram_chat_id: str,
    telegram_username: str,
    text: str,
    reference_time: Optional[datetime] = None,
) -> str:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return ""

    parts = normalized_text.split()
    command = parts[0].split("@", 1)[0].lower()
    args = parts[1:]

    if command == "/start":
        return _build_help_text()

    if command == "/bind":
        if not args:
            return "绑定命令格式：/bind <绑定码>"
        result = bind_telegram_user(
            repository,
            bind_token=args[0],
            telegram_user_id=int(telegram_user_id),
            telegram_chat_id=str(telegram_chat_id or "").strip(),
            telegram_username=str(telegram_username or "").strip(),
            reference_time=reference_time,
        )
        return "绑定成功，当前平台账号：%s" % str((result.get("user") or {}).get("username") or "")

    user = _resolve_bound_user(repository, telegram_user_id=int(telegram_user_id))

    if command in {"/profit", "/profitall"}:
        stat_date = _normalize_stat_date(args[0] if args else "", reference_time=reference_time)
        summary = repository.get_user_daily_profit_summary(user_id=int(user["id"]), stat_date=stat_date)
        return _build_profit_summary_text(user, summary)

    if command == "/plan":
        if not args:
            stat_date = _normalize_stat_date("", reference_time=reference_time)
            items = repository.list_user_daily_subscription_stats(user_id=int(user["id"]), stat_date=stat_date)
            return _build_plan_list_text(stat_date, items)

        plan_name = ""
        if len(args) == 1:
            try:
                stat_date = _normalize_stat_date(args[0], reference_time=reference_time)
            except ValueError:
                stat_date = _normalize_stat_date("", reference_time=reference_time)
                plan_name = args[0]
        else:
            try:
                stat_date = _normalize_stat_date(args[-1], reference_time=reference_time)
                plan_name = " ".join(args[:-1]).strip()
            except ValueError:
                stat_date = _normalize_stat_date("", reference_time=reference_time)
                plan_name = " ".join(args).strip()

        items = repository.list_user_daily_subscription_stats(user_id=int(user["id"]), stat_date=stat_date)
        if not plan_name:
            return _build_plan_list_text(stat_date, items)

        matched = [item for item in items if str(item.get("source_name") or "") == plan_name]
        if len(matched) == 1:
            return _build_plan_detail_text(matched[0])
        if len(matched) > 1:
            return "存在重名方案，请先发送 /plan %s 查看方案列表后确认名称。" % stat_date
        source_names = repository.list_user_subscription_source_names(user_id=int(user["id"]))
        return _build_candidate_text(stat_date, source_names)

    return _build_help_text()


def process_telegram_bot_cycle(
    repository: Any,
    *,
    bot_client: TelegramBotClient,
    bot_name: str = DEFAULT_BOT_STATE_KEY,
    poll_timeout_seconds: int = 5,
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    state = repository.get_telegram_bot_runtime_state(bot_name=bot_name)
    last_update_id = int(state.get("last_update_id") or 0)
    offset = last_update_id + 1 if last_update_id > 0 else None
    updates = bot_client.get_updates(
        offset=offset,
        timeout_seconds=max(1, int(poll_timeout_seconds or 1)),
        limit=100,
    )
    handled_count = 0
    replied_count = 0
    ignored_count = 0

    for update in updates:
        update_id = int(update.get("update_id") or 0)
        message = update.get("message") if isinstance(update, dict) else None
        chat = (message or {}).get("chat") or {}
        chat_type = str(chat.get("type") or "").strip()
        chat_id = str(chat.get("id") or "").strip()
        text = str((message or {}).get("text") or "").strip()
        from_user = (message or {}).get("from") or {}
        telegram_user_id = from_user.get("id")
        telegram_username = str(from_user.get("username") or "").strip()

        if not message or not text or not chat_id or telegram_user_id is None or chat_type != "private":
            ignored_count += 1
            if update_id > 0:
                repository.update_telegram_bot_runtime_state(bot_name=bot_name, last_update_id=update_id)
            continue

        try:
            response_text = handle_telegram_command(
                repository,
                telegram_user_id=int(telegram_user_id),
                telegram_chat_id=chat_id,
                telegram_username=telegram_username,
                text=text,
                reference_time=reference_time,
            )
        except Exception as exc:
            response_text = str(exc) or "命令处理失败"
        handled_count += 1
        if response_text:
            bot_client.send_text(chat_id, response_text)
            replied_count += 1
        if update_id > 0:
            repository.update_telegram_bot_runtime_state(bot_name=bot_name, last_update_id=update_id)

    return {
        "update_count": len(updates),
        "handled_count": handled_count,
        "replied_count": replied_count,
        "ignored_count": ignored_count,
        "last_update_id": int(repository.get_telegram_bot_runtime_state(bot_name=bot_name).get("last_update_id") or 0),
    }
