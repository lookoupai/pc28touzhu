"""Telegram Bot binding and profit query services."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Protocol, Tuple
from uuid import uuid4

from pc28touzhu.domain.subscription_strategy import upgrade_subscription_strategy
from pc28touzhu.services.platform_service import (
    restart_subscription_cycle,
    update_subscription,
    update_subscription_status,
)


SHANGHAI_TZ = timezone(timedelta(hours=8))
DEFAULT_BOT_STATE_KEY = "profit-query-bot"
TELEGRAM_BOT_COMMANDS = (
    {"command": "start", "description": "查看帮助"},
    {"command": "help", "description": "查看帮助"},
    {"command": "bind", "description": "绑定平台账号"},
    {"command": "subs", "description": "查看跟单方案列表"},
    {"command": "enable", "description": "启动跟单方案"},
    {"command": "disable", "description": "暂停跟单方案"},
    {"command": "play", "description": "切换跟单玩法"},
    {"command": "restart", "description": "开始新一轮"},
    {"command": "profit", "description": "查询跟单汇总"},
    {"command": "plan", "description": "查询单方案盈亏"},
    {"command": "status", "description": "查询当前跟单状态"},
)
SUBSCRIPTION_PLAY_FILTER_PRESETS = {
    "大小": {"mode": "selected", "selected_keys": ["big_small:大", "big_small:小"]},
    "单双": {"mode": "selected", "selected_keys": ["odd_even:单", "odd_even:双"]},
    "组合": {"mode": "selected", "selected_keys": ["combo:大单", "combo:大双", "combo:小单", "combo:小双"]},
    "全部": {"mode": "all", "selected_keys": []},
}
SUBSCRIPTION_PLAY_FILTER_ALIASES = {
    "big_small": "大小",
    "odd_even": "单双",
    "combo": "组合",
    "all": "全部",
    "大小": "大小",
    "单双": "单双",
    "组合": "组合",
    "全部": "全部",
}
SUBSCRIPTION_PLAY_FILTER_CALLBACKS = {
    "bs": "大小",
    "oe": "单双",
    "cb": "组合",
    "all": "全部",
}
SUBSCRIPTION_PLAY_FILTER_LABELS = {
    "big_small:大": "大",
    "big_small:小": "小",
    "odd_even:单": "单",
    "odd_even:双": "双",
    "combo:大单": "大单",
    "combo:大双": "大双",
    "combo:小单": "小单",
    "combo:小双": "小双",
}
SUBSCRIPTIONS_PAGE_SIZE = 5


class TelegramBotClient(Protocol):
    def send_text(
        self,
        target_chat_id: str,
        message_text: str,
        *,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ...

    def edit_text(
        self,
        target_chat_id: str,
        message_id: int,
        message_text: str,
        *,
        reply_markup: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        ...

    def answer_callback_query(
        self,
        callback_query_id: str,
        *,
        text: str = "",
        show_alert: bool = False,
    ) -> Dict[str, Any]:
        ...

    def get_updates(
        self,
        *,
        offset: Optional[int] = None,
        timeout_seconds: int = 10,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        ...

    def set_my_commands(
        self,
        commands: List[Dict[str, Any]],
        *,
        scope: Optional[Dict[str, Any]] = None,
        language_code: Optional[str] = None,
    ) -> Dict[str, Any]:
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


def _default_stat_date_candidates(*, reference_time: Optional[datetime] = None) -> List[str]:
    local_now = (reference_time or _utc_now()).astimezone(SHANGHAI_TZ)
    today = local_now.date().isoformat()
    yesterday = (local_now.date() - timedelta(days=1)).isoformat()
    return [today] if today == yesterday else [today, yesterday]


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
            "/start 查看帮助",
            "/help 查看帮助",
            "/bind <绑定码> 绑定平台账号",
            "/subs 查看跟单方案列表",
            "/enable <订阅ID> 启动跟单方案",
            "/disable <订阅ID> 暂停跟单方案",
            "/play <订阅ID> <大小|单双|组合|全部> 切换玩法",
            "/restart <订阅ID> 开始新一轮",
            "/status 查询当前跟单状态、待结算金额",
            "/status <方案名> 查询指定方案当前状态",
            "/profit 查询最近有已结算数据的汇总",
            "/profit YYYY-MM-DD 查询指定日期汇总",
            "/plan 查询最近有已结算数据的单方案列表",
            "/plan YYYY-MM-DD 查询指定日期单方案列表",
            "/plan <方案名> 查询最近有已结算数据的指定方案盈亏",
            "/plan <方案名> YYYY-MM-DD 查询指定方案盈亏",
            "说明：/profit 与 /plan 仅展示已结算数据；/status 展示当前状态与待结算金额；/restart 会清空当前轮次运行态并立即开始新一轮。",
        ]
    )


def get_telegram_bot_commands() -> List[Dict[str, str]]:
    return [dict(item) for item in TELEGRAM_BOT_COMMANDS]


def sync_telegram_bot_commands(bot_client: Any) -> Dict[str, Any]:
    setter = getattr(bot_client, "set_my_commands", None)
    if not callable(setter):
        raise ValueError("bot_client 不支持 set_my_commands")
    commands = get_telegram_bot_commands()
    results = [
        setter(commands),
        setter(
            commands,
            scope={"type": "all_private_chats"},
        ),
    ]
    return {
        "ok": all(bool((item or {}).get("ok", True)) for item in results),
        "results": results,
    }


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


def _manual_subscription_status_text(status: Any) -> str:
    normalized = str(status or "").strip()
    if normalized == "active":
        return "已启用"
    if normalized == "archived":
        return "已归档"
    return "已停用"


def _subscription_threshold_status(item: Dict[str, Any]) -> str:
    financial = item.get("financial") if isinstance(item.get("financial"), dict) else {}
    return str(financial.get("threshold_status") or "").strip()


def _subscription_is_risk_blocked(item: Dict[str, Any]) -> bool:
    return _subscription_threshold_status(item) in {"profit_target_hit", "loss_limit_hit"}


def _subscription_is_dispatching(item: Dict[str, Any]) -> bool:
    return str(item.get("status") or "").strip() == "active" and not _subscription_is_risk_blocked(item)


def _subscription_runtime_status_text(item: Dict[str, Any]) -> str:
    manual_status = str(item.get("status") or "").strip()
    threshold_status = _subscription_threshold_status(item)
    if manual_status == "archived":
        return "已归档"
    if manual_status != "active":
        return "已停用"
    if threshold_status == "profit_target_hit":
        return "风控止盈阻塞"
    if threshold_status == "loss_limit_hit":
        return "风控止损阻塞"
    return "运行中"


def _subscription_strategy_v2(item: Dict[str, Any]) -> Dict[str, Any]:
    strategy_v2 = item.get("strategy_v2") if isinstance(item.get("strategy_v2"), dict) else item.get("strategy")
    return upgrade_subscription_strategy(strategy_v2)


def _subscription_play_filter_label(item: Dict[str, Any]) -> str:
    strategy_v2 = _subscription_strategy_v2(item)
    play_filter = strategy_v2.get("play_filter") if isinstance(strategy_v2.get("play_filter"), dict) else {}
    mode = str(play_filter.get("mode") or "all").strip() or "all"
    selected_keys = list(play_filter.get("selected_keys") or [])
    if mode != "selected" or not selected_keys:
        return "全部"
    for label, preset in SUBSCRIPTION_PLAY_FILTER_PRESETS.items():
        if mode == str(preset.get("mode") or "") and selected_keys == list(preset.get("selected_keys") or []):
            return label
    labels = [SUBSCRIPTION_PLAY_FILTER_LABELS.get(key) for key in selected_keys if SUBSCRIPTION_PLAY_FILTER_LABELS.get(key)]
    return " / ".join(labels) if labels else "全部"


def _parse_subscription_id_arg(raw_value: Any) -> int:
    try:
        subscription_id = int(str(raw_value or "").strip())
    except (TypeError, ValueError):
        raise ValueError("订阅ID 格式不正确")
    if subscription_id <= 0:
        raise ValueError("订阅ID 格式不正确")
    return subscription_id


def _resolve_user_subscription_by_id(repository: Any, *, user_id: int, subscription_id: Any) -> Dict[str, Any]:
    normalized_subscription_id = _parse_subscription_id_arg(subscription_id)
    item = repository.get_subscription(normalized_subscription_id)
    if not item or int(item.get("user_id") or 0) != int(user_id):
        raise ValueError("未找到该跟单方案")
    return item


def _normalize_play_preset_arg(raw_value: Any) -> str:
    normalized = SUBSCRIPTION_PLAY_FILTER_ALIASES.get(str(raw_value or "").strip())
    if not normalized:
        raise ValueError("玩法仅支持：大小、单双、组合、全部")
    return normalized


def _build_subscriptions_list_text(subscriptions: List[Dict[str, Any]], source_names: Dict[int, str]) -> str:
    if not subscriptions:
        return "当前没有跟单策略。"
    lines = ["【跟单方案列表】"]
    for item in subscriptions[:10]:
        subscription_id = int(item.get("id") or 0)
        source_name = source_names.get(int(item.get("source_id") or 0), "未命名方案")
        runtime_status = _subscription_runtime_status_text(item)
        play_filter = _subscription_play_filter_label(item)
        lines.append("%s | %s | %s | 玩法：%s" % (subscription_id, source_name, runtime_status, play_filter))
    if len(subscriptions) > 10:
        lines.append("仅展示前 10 条，请到网页端查看完整列表。")
    lines.extend(
        [
            "",
            "使用示例：",
            "/play 12 单双",
            "/enable 12",
            "/disable 12",
            "/restart 12",
        ]
    )
    return "\n".join(lines)


def _bot_action(
    *,
    text: str,
    reply_markup: Optional[Dict[str, Any]] = None,
    callback_text: str = "",
    callback_alert: bool = False,
) -> Dict[str, Any]:
    return {
        "text": str(text or ""),
        "reply_markup": reply_markup if isinstance(reply_markup, dict) and reply_markup else None,
        "callback_text": str(callback_text or ""),
        "callback_alert": bool(callback_alert),
    }


def _inline_button(text: str, callback_data: str) -> Dict[str, str]:
    return {
        "text": str(text or "").strip(),
        "callback_data": str(callback_data or "").strip(),
    }


def _inline_keyboard(rows: List[List[Dict[str, str]]]) -> Dict[str, Any]:
    keyboard = []
    for row in rows:
        normalized_row = []
        for item in row:
            if not isinstance(item, dict):
                continue
            text = str(item.get("text") or "").strip()
            callback_data = str(item.get("callback_data") or "").strip()
            if not text or not callback_data:
                continue
            normalized_row.append({"text": text, "callback_data": callback_data})
        if normalized_row:
            keyboard.append(normalized_row)
    return {"inline_keyboard": keyboard} if keyboard else {}


def _to_positive_int_or_default(value: Any, default: int) -> int:
    try:
        normalized = int(str(value or "").strip())
    except (TypeError, ValueError):
        return int(default)
    return normalized if normalized > 0 else int(default)


def _subscriptions_page(subscriptions: List[Dict[str, Any]], page: int) -> Tuple[List[Dict[str, Any]], int, int]:
    total_count = len(subscriptions)
    if total_count <= 0:
        return ([], 1, 1)
    total_pages = max(1, (total_count + SUBSCRIPTIONS_PAGE_SIZE - 1) // SUBSCRIPTIONS_PAGE_SIZE)
    current_page = min(max(1, int(page or 1)), total_pages)
    start_index = (current_page - 1) * SUBSCRIPTIONS_PAGE_SIZE
    end_index = start_index + SUBSCRIPTIONS_PAGE_SIZE
    return (subscriptions[start_index:end_index], current_page, total_pages)


def _subscription_summary_line(item: Dict[str, Any], source_name: str) -> str:
    return "#%s %s | %s | %s | %s" % (
        int(item.get("id") or 0),
        source_name,
        _subscription_runtime_status_text(item),
        _subscription_play_filter_label(item),
        _signed_money((item.get("financial") or {}).get("net_profit")),
    )


def _render_subscription_list_page(repository: Any, *, user_id: int, page: int = 1) -> Dict[str, Any]:
    subscriptions = repository.list_subscriptions(user_id=int(user_id))
    source_names = _resolve_subscription_source_names(repository, subscriptions)
    page_items, current_page, total_pages = _subscriptions_page(subscriptions, page)
    if not subscriptions:
        return _bot_action(text="当前没有跟单策略。")

    lines = ["【跟单方案列表】", "第 %s / %s 页" % (current_page, total_pages), ""]
    for item in page_items:
        source_name = source_names.get(int(item.get("source_id") or 0), "未命名方案")
        lines.append(_subscription_summary_line(item, source_name))
    text = "\n".join(lines)

    button_rows = [[_inline_button(_subscription_summary_line(item, source_names.get(int(item.get("source_id") or 0), "未命名方案")), "sub:%s:%s" % (int(item.get("id") or 0), current_page))] for item in page_items]
    button_rows.append(
        [
            _inline_button("上一页", "subs:%s" % max(1, current_page - 1)),
            _inline_button("刷新", "rfsubs:%s" % current_page),
            _inline_button("下一页", "subs:%s" % min(total_pages, current_page + 1)),
        ]
    )
    return _bot_action(text=text, reply_markup=_inline_keyboard(button_rows))


def _render_subscription_error_page(message: str, *, page: int = 1) -> Dict[str, Any]:
    return _bot_action(
        text=str(message or "操作失败"),
        reply_markup=_inline_keyboard([[_inline_button("返回列表", "subs:%s" % max(1, int(page or 1)))]]),
        callback_text=str(message or "操作失败"),
        callback_alert=True,
    )


def _build_subscription_detail_text(item: Dict[str, Any], source_name: str, pending_event: Optional[Dict[str, Any]] = None) -> str:
    progression = item.get("progression") if isinstance(item.get("progression"), dict) else {}
    financial = item.get("financial") if isinstance(item.get("financial"), dict) else {}
    lines = [
        "【跟单方案详情】",
        "方案：%s" % source_name,
        "订阅ID：%s" % int(item.get("id") or 0),
        "",
        "用户状态：%s" % _manual_subscription_status_text(item.get("status")),
        "运行状态：%s" % _subscription_runtime_status_text(item),
        "当前玩法：%s" % _subscription_play_filter_label(item),
        "本轮净盈亏：%s" % _signed_money(financial.get("net_profit")),
        "当前手数：%s" % int(progression.get("current_step") or 1),
    ]
    if progression.get("pending_event_id"):
        lines.append("待结算状态：%s" % str(progression.get("pending_status") or "pending"))
        lines.append("待结算期号：%s" % str(progression.get("pending_issue_no") or "--"))
        lines.append("当前待结算金额：%.2f" % round(float((pending_event or {}).get("stake_amount") or 0), 2))
    else:
        lines.append("待结算状态：无")
    if _subscription_is_risk_blocked(item):
        lines.extend(["", "说明：", "当前轮次已停止。", "如需继续，请开始新一轮。"])
    else:
        lines.extend(
            [
                "",
                "说明：",
                "启动/暂停会继续当前轮次。",
                "开始新一轮会从 0 重新开始。",
                "切换玩法只影响后续新信号。",
            ]
        )
    return "\n".join(lines)


def _build_subscription_detail_keyboard(item: Dict[str, Any], *, page: int) -> Dict[str, Any]:
    subscription_id = int(item.get("id") or 0)
    rows: List[List[Dict[str, str]]] = []
    status = str(item.get("status") or "").strip()
    risk_blocked = _subscription_is_risk_blocked(item)
    if status != "archived":
        if risk_blocked:
            rows.append([_inline_button("开始新一轮", "rstc:%s:%s" % (subscription_id, page))])
        else:
            rows.append([
                _inline_button(
                    "暂停" if status == "active" else "启动",
                    ("dis" if status == "active" else "en") + ":%s:%s" % (subscription_id, page),
                )
            ])
            rows.append([_inline_button("开始新一轮", "rstc:%s:%s" % (subscription_id, page))])
        rows.append(
            [
                _inline_button("大小", "play:%s:bs:%s" % (subscription_id, page)),
                _inline_button("单双", "play:%s:oe:%s" % (subscription_id, page)),
            ]
        )
        rows.append(
            [
                _inline_button("组合", "play:%s:cb:%s" % (subscription_id, page)),
                _inline_button("全部", "play:%s:all:%s" % (subscription_id, page)),
            ]
        )
    rows.append(
        [
            _inline_button("返回列表", "subs:%s" % page),
            _inline_button("刷新", "rfsub:%s:%s" % (subscription_id, page)),
        ]
    )
    return _inline_keyboard(rows)


def _render_subscription_detail_page(repository: Any, *, user_id: int, subscription_id: int, page: int = 1) -> Dict[str, Any]:
    item = _resolve_user_subscription_by_id(repository, user_id=int(user_id), subscription_id=subscription_id)
    source_names = _resolve_subscription_source_names(repository, [item])
    pending_events = _resolve_pending_event_snapshots(repository, [item])
    progression = item.get("progression") if isinstance(item.get("progression"), dict) else {}
    pending_event = pending_events.get(int(progression.get("pending_event_id") or 0))
    source_name = source_names.get(int(item.get("source_id") or 0), "未命名方案")
    return _bot_action(
        text=_build_subscription_detail_text(item, source_name, pending_event),
        reply_markup=_build_subscription_detail_keyboard(item, page=page),
    )


def _render_restart_confirm_page(repository: Any, *, user_id: int, subscription_id: int, page: int = 1) -> Dict[str, Any]:
    item = _resolve_user_subscription_by_id(repository, user_id=int(user_id), subscription_id=subscription_id)
    source_names = _resolve_subscription_source_names(repository, [item])
    source_name = source_names.get(int(item.get("source_id") or 0), "未命名方案")
    text = "\n".join(
        [
            "【确认开始新一轮】",
            "方案：%s" % source_name,
            "订阅ID：%s" % int(item.get("id") or 0),
            "",
            "执行后将：",
            "1. 本轮净盈亏重置为 0",
            "2. 当前手数回到第 1 手",
            "3. 旧轮次未执行任务被跳过",
            "",
            "是否继续？",
        ]
    )
    return _bot_action(
        text=text,
        reply_markup=_inline_keyboard(
            [
                [_inline_button("确认开始新一轮", "rsta:%s:%s" % (subscription_id, page))],
                [_inline_button("取消", "rstx:%s:%s" % (subscription_id, page))],
            ]
        ),
    )


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


def _progression_result_text(value: Any) -> str:
    mapping = {
        "hit": "命中",
        "refund": "回本",
        "miss": "未中",
        "reset": "已重置",
    }
    text = str(value or "").strip()
    return mapping.get(text, text or "--")


def _resolve_subscription_source_names(repository: Any, subscriptions: List[Dict[str, Any]]) -> Dict[int, str]:
    source_ids = []
    seen: set[int] = set()
    for item in subscriptions:
        source_id = int(item.get("source_id") or 0)
        if source_id <= 0 or source_id in seen:
            continue
        source_ids.append(source_id)
        seen.add(source_id)

    result: Dict[int, str] = {}
    for source_id in source_ids:
        source = repository.get_source(source_id) if hasattr(repository, "get_source") else None
        result[source_id] = str((source or {}).get("name") or "未命名方案")
    return result


def _resolve_pending_event_snapshots(repository: Any, subscriptions: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    pending_events: Dict[int, Dict[str, Any]] = {}
    if not hasattr(repository, "get_progression_event"):
        return pending_events
    for item in subscriptions:
        progression = item.get("progression") if isinstance(item.get("progression"), dict) else {}
        pending_event_id = int(progression.get("pending_event_id") or 0)
        if pending_event_id <= 0 or pending_event_id in pending_events:
            continue
        event = repository.get_progression_event(pending_event_id)
        if isinstance(event, dict) and event:
            pending_events[pending_event_id] = event
    return pending_events


def _build_status_summary_text(
    user: Dict[str, Any],
    subscriptions: List[Dict[str, Any]],
    source_names: Dict[int, str],
    pending_events: Dict[int, Dict[str, Any]],
) -> str:
    if not subscriptions:
        return "当前没有跟单策略。"

    total_count = len(subscriptions)
    dispatching_count = len([item for item in subscriptions if _subscription_is_dispatching(item)])
    paused_count = len([item for item in subscriptions if str(item.get("status") or "") == "inactive"])
    archived_count = len([item for item in subscriptions if str(item.get("status") or "") == "archived"])
    pending_items = []
    risk_blocked_count = 0
    total_realized_net_profit = 0.0
    total_pending_stake = 0.0
    for item in subscriptions:
        progression = item.get("progression") if isinstance(item.get("progression"), dict) else {}
        financial = item.get("financial") if isinstance(item.get("financial"), dict) else {}
        if progression.get("pending_event_id") and str(progression.get("pending_status") or "") == "placed":
            pending_items.append(item)
            pending_event = pending_events.get(int(progression.get("pending_event_id") or 0)) or {}
            total_pending_stake += round(float(pending_event.get("stake_amount") or 0), 2)
        if str(financial.get("threshold_status") or "") in {"profit_target_hit", "loss_limit_hit"}:
            risk_blocked_count += 1
        total_realized_net_profit += round(float(financial.get("net_profit") or 0), 2)

    lines = [
        "【当前跟单状态】",
        "账号: %s" % str(user.get("username") or ""),
        "策略总数: %s" % total_count,
        "运行中: %s" % dispatching_count,
        "手动停用: %s" % paused_count,
        "待结算: %s" % len(pending_items),
        "本轮已实现净盈亏合计: %s" % _signed_money(total_realized_net_profit),
        "待结算金额合计: %.2f" % round(total_pending_stake, 2),
    ]
    if risk_blocked_count > 0:
        lines.append("风控阻塞: %s" % risk_blocked_count)
    if archived_count > 0:
        lines.append("已归档: %s" % archived_count)
    if pending_items:
        lines.append("待结算方案：")
        for index, item in enumerate(pending_items[:5], start=1):
            progression = item.get("progression") if isinstance(item.get("progression"), dict) else {}
            financial = item.get("financial") if isinstance(item.get("financial"), dict) else {}
            pending_event = pending_events.get(int(progression.get("pending_event_id") or 0)) or {}
            source_name = source_names.get(int(item.get("source_id") or 0), "未命名方案")
            lines.append(
                "%s. %s | 期号 %s | 待结算 %.2f | 已实现 %s"
                % (
                    index,
                    source_name,
                    str(progression.get("pending_issue_no") or "--"),
                    round(float(pending_event.get("stake_amount") or 0), 2),
                    _signed_money(financial.get("net_profit")),
                )
            )
        if len(pending_items) > 5:
            lines.append("更多方案请发送 /status <方案名> 查看。")
    else:
        lines.append("当前没有待结算记录。")
    return "\n".join(lines)


def _build_status_detail_text(item: Dict[str, Any], source_name: str, pending_event: Optional[Dict[str, Any]] = None) -> str:
    progression = item.get("progression") if isinstance(item.get("progression"), dict) else {}
    financial = item.get("financial") if isinstance(item.get("financial"), dict) else {}
    lines = [
        "【当前方案状态】",
        "方案: %s" % source_name,
        "状态: %s" % _subscription_runtime_status_text(item),
        "用户设置: %s" % _manual_subscription_status_text(item.get("status")),
        "当前手数: %s" % int(progression.get("current_step") or 1),
        "本轮已实现净盈亏: %s" % _signed_money(financial.get("net_profit")),
        "累计盈利: %.2f" % round(float(financial.get("realized_profit") or 0), 2),
        "累计亏损: %.2f" % round(float(financial.get("realized_loss") or 0), 2),
        "当前玩法: %s" % _subscription_play_filter_label(item),
    ]
    if progression.get("last_result_type"):
        lines.append("最近结果: %s" % _progression_result_text(progression.get("last_result_type")))
    if progression.get("pending_event_id"):
        lines.append("待结算期号: %s" % str(progression.get("pending_issue_no") or "--"))
        lines.append("待结算状态: %s" % str(progression.get("pending_status") or "pending"))
        lines.append("当前待结算金额: %.2f" % round(float((pending_event or {}).get("stake_amount") or 0), 2))
    else:
        lines.append("待结算状态: 无")

    threshold_status = str(financial.get("threshold_status") or "")
    if threshold_status == "profit_target_hit":
        lines.append("风控状态: 已触发止盈")
    elif threshold_status == "loss_limit_hit":
        lines.append("风控状态: 已触发止损")
    if str(financial.get("stopped_reason") or "").strip():
        lines.append("停用原因: %s" % str(financial.get("stopped_reason") or "").strip())
    if financial.get("last_settled_at"):
        lines.append("最近结算时间: %s" % str(financial.get("last_settled_at") or ""))
    if financial.get("baseline_reset_at"):
        lines.append("最近重置时间: %s" % str(financial.get("baseline_reset_at") or ""))
    return "\n".join(lines)


def _resolve_default_profit_summary(
    repository: Any,
    *,
    user_id: int,
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    fallback: Optional[Dict[str, Any]] = None
    for index, stat_date in enumerate(_default_stat_date_candidates(reference_time=reference_time)):
        summary = repository.get_user_daily_profit_summary(user_id=int(user_id), stat_date=stat_date)
        if index == 0:
            fallback = summary
        if int(summary.get("settled_event_count") or 0) > 0:
            return summary
    return fallback or repository.get_user_daily_profit_summary(
        user_id=int(user_id),
        stat_date=_default_stat_date_candidates(reference_time=reference_time)[0],
    )


def _resolve_default_plan_items(
    repository: Any,
    *,
    user_id: int,
    reference_time: Optional[datetime] = None,
) -> Tuple[str, List[Dict[str, Any]]]:
    fallback_stat_date = _default_stat_date_candidates(reference_time=reference_time)[0]
    fallback_items: List[Dict[str, Any]] = []
    for index, stat_date in enumerate(_default_stat_date_candidates(reference_time=reference_time)):
        items = repository.list_user_daily_subscription_stats(user_id=int(user_id), stat_date=stat_date)
        if index == 0:
            fallback_stat_date = stat_date
            fallback_items = items
        if items:
            return stat_date, items
    return fallback_stat_date, fallback_items


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

    if command in {"/start", "/help"}:
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

    if command == "/subs":
        subscriptions = repository.list_subscriptions(user_id=int(user["id"]))
        source_names = _resolve_subscription_source_names(repository, subscriptions)
        return _build_subscriptions_list_text(subscriptions, source_names)

    if command == "/enable":
        if not args:
            return "命令格式：/enable <订阅ID>"
        item = _resolve_user_subscription_by_id(repository, user_id=int(user["id"]), subscription_id=args[0])
        if str(item.get("status") or "") == "archived":
            return "该方案当前已归档，请先到网页端恢复后再启动。"
        if _subscription_is_risk_blocked(item):
            return "该方案当前处于风控阻塞，请发送 /restart %s 开始新一轮。" % int(item["id"])
        if str(item.get("status") or "") == "active":
            return "该方案当前已经是已启用状态。"
        result = update_subscription_status(
            repository,
            subscription_id=int(item["id"]),
            user_id=int(user["id"]),
            status="active",
        )
        next_item = result.get("item") if isinstance(result.get("item"), dict) else item
        source_name = _resolve_subscription_source_names(repository, [next_item]).get(int(next_item.get("source_id") or 0), "未命名方案")
        return "\n".join(
            [
                "已启动跟单方案：%s（#%s）" % (source_name, int(next_item["id"])),
                "当前玩法：%s" % _subscription_play_filter_label(next_item),
            ]
        )

    if command == "/disable":
        if not args:
            return "命令格式：/disable <订阅ID>"
        item = _resolve_user_subscription_by_id(repository, user_id=int(user["id"]), subscription_id=args[0])
        if str(item.get("status") or "") == "archived":
            return "该方案当前已归档，请先到网页端恢复后再操作。"
        if str(item.get("status") or "") == "inactive":
            return "该方案当前已经是已停用状态。"
        result = update_subscription_status(
            repository,
            subscription_id=int(item["id"]),
            user_id=int(user["id"]),
            status="inactive",
        )
        next_item = result.get("item") if isinstance(result.get("item"), dict) else item
        source_name = _resolve_subscription_source_names(repository, [next_item]).get(int(next_item.get("source_id") or 0), "未命名方案")
        lines = ["已关闭跟单方案：%s（#%s）" % (source_name, int(next_item["id"]))]
        progression = next_item.get("progression") if isinstance(next_item.get("progression"), dict) else {}
        if progression.get("pending_event_id"):
            lines.append("仅停止后续新信号，当前待结算记录保持不变。")
        return "\n".join(lines)

    if command == "/play":
        if len(args) < 2:
            return "命令格式：/play <订阅ID> <大小|单双|组合|全部>"
        item = _resolve_user_subscription_by_id(repository, user_id=int(user["id"]), subscription_id=args[0])
        if str(item.get("status") or "") == "archived":
            return "该方案当前已归档，请先到网页端恢复后再切换玩法。"
        play_label = _normalize_play_preset_arg(args[1])
        strategy_v2 = _subscription_strategy_v2(item)
        strategy_v2["play_filter"] = dict(SUBSCRIPTION_PLAY_FILTER_PRESETS[play_label])
        result = update_subscription(
            repository,
            subscription_id=int(item["id"]),
            user_id=int(user["id"]),
            payload={
                "source_id": int(item["source_id"]),
                "strategy_v2": strategy_v2,
            },
        )
        next_item = result.get("item") if isinstance(result.get("item"), dict) else item
        source_name = _resolve_subscription_source_names(repository, [next_item]).get(int(next_item.get("source_id") or 0), "未命名方案")
        return "\n".join(
            [
                "玩法已切换：%s（#%s）" % (source_name, int(next_item["id"])),
                "当前玩法：%s" % _subscription_play_filter_label(next_item),
                "其他策略设置保持不变。",
                "仅影响后续新信号，已生成或待结算记录不会变。",
            ]
        )

    if command == "/restart":
        if not args:
            return "命令格式：/restart <订阅ID>"
        item = _resolve_user_subscription_by_id(repository, user_id=int(user["id"]), subscription_id=args[0])
        if str(item.get("status") or "") == "archived":
            return "该方案当前已归档，请先到网页端恢复后再开始新一轮。"
        result = restart_subscription_cycle(
            repository,
            subscription_id=int(item["id"]),
            user_id=int(user["id"]),
            payload={"note": "Telegram /restart"},
        )
        next_item = result.get("item") if isinstance(result.get("item"), dict) else item
        source_name = _resolve_subscription_source_names(repository, [next_item]).get(int(next_item.get("source_id") or 0), "未命名方案")
        return "\n".join(
            [
                "已开始新一轮：%s（#%s）" % (source_name, int(next_item["id"])),
                "当前玩法：%s" % _subscription_play_filter_label(next_item),
                "本轮净盈亏已重置为 0，旧轮次未执行任务已跳过。",
            ]
        )

    if command in {"/profit", "/profitall"}:
        if args:
            stat_date = _normalize_stat_date(args[0], reference_time=reference_time)
            summary = repository.get_user_daily_profit_summary(user_id=int(user["id"]), stat_date=stat_date)
        else:
            summary = _resolve_default_profit_summary(
                repository,
                user_id=int(user["id"]),
                reference_time=reference_time,
            )
        return _build_profit_summary_text(user, summary)

    if command == "/status":
        subscriptions = repository.list_subscriptions(user_id=int(user["id"]))
        source_names = _resolve_subscription_source_names(repository, subscriptions)
        pending_events = _resolve_pending_event_snapshots(repository, subscriptions)
        if not args:
            return _build_status_summary_text(user, subscriptions, source_names, pending_events)

        plan_name = " ".join(args).strip()
        matched = [
            item
            for item in subscriptions
            if source_names.get(int(item.get("source_id") or 0), "未命名方案") == plan_name
        ]
        if len(matched) == 1:
            item = matched[0]
            progression = item.get("progression") if isinstance(item.get("progression"), dict) else {}
            return _build_status_detail_text(
                item,
                source_names.get(int(item.get("source_id") or 0), "未命名方案"),
                pending_events.get(int(progression.get("pending_event_id") or 0)),
            )
        if len(matched) > 1:
            return "存在重名方案，请先发送 /status 查看待结算方案列表后确认名称。"
        source_name_list = sorted(set(source_names.values()))
        if not source_name_list:
            return "当前没有跟单策略。"
        return "\n".join(
            [
                "未找到对应方案，请确认方案名称。",
                "可用方案：",
                *["- %s" % name for name in source_name_list[:10]],
            ]
        )

    if command == "/plan":
        if not args:
            stat_date, items = _resolve_default_plan_items(
                repository,
                user_id=int(user["id"]),
                reference_time=reference_time,
            )
            return _build_plan_list_text(stat_date, items)

        stat_date = ""
        plan_name = ""
        if len(args) == 1:
            try:
                stat_date = _normalize_stat_date(args[0], reference_time=reference_time)
            except ValueError:
                plan_name = args[0]
        else:
            try:
                stat_date = _normalize_stat_date(args[-1], reference_time=reference_time)
                plan_name = " ".join(args[:-1]).strip()
            except ValueError:
                plan_name = " ".join(args).strip()

        if stat_date:
            items = repository.list_user_daily_subscription_stats(user_id=int(user["id"]), stat_date=stat_date)
        else:
            stat_date, items = _resolve_default_plan_items(
                repository,
                user_id=int(user["id"]),
                reference_time=reference_time,
            )
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


def _handle_telegram_command_action(
    repository: Any,
    *,
    telegram_user_id: int,
    telegram_chat_id: str,
    telegram_username: str,
    text: str,
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    normalized_text = str(text or "").strip()
    if not normalized_text:
        return _bot_action(text="")
    parts = normalized_text.split()
    command = parts[0].split("@", 1)[0].lower()
    if command == "/subs":
        user = _resolve_bound_user(repository, telegram_user_id=int(telegram_user_id))
        return _render_subscription_list_page(repository, user_id=int(user["id"]), page=1)
    return _bot_action(
        text=handle_telegram_command(
            repository,
            telegram_user_id=int(telegram_user_id),
            telegram_chat_id=telegram_chat_id,
            telegram_username=telegram_username,
            text=text,
            reference_time=reference_time,
        )
    )


def _handle_telegram_callback_action(
    repository: Any,
    *,
    telegram_user_id: int,
    data: str,
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    del reference_time
    parts = [str(item or "").strip() for item in str(data or "").split(":")]
    action = parts[0] if parts else ""
    user = _resolve_bound_user(repository, telegram_user_id=int(telegram_user_id))
    user_id = int(user["id"])

    try:
        if action in {"subs", "rfsubs", "back"}:
            page = _to_positive_int_or_default(parts[1] if len(parts) > 1 else 1, 1)
            result = _render_subscription_list_page(repository, user_id=user_id, page=page)
            if action == "rfsubs":
                result["callback_text"] = "列表已刷新"
            return result

        if action in {"sub", "rfsub", "en", "dis", "rstc", "rsta", "rstx"}:
            subscription_id = _parse_subscription_id_arg(parts[1] if len(parts) > 1 else "")
            page = _to_positive_int_or_default(parts[2] if len(parts) > 2 else 1, 1)
        elif action == "play":
            subscription_id = _parse_subscription_id_arg(parts[1] if len(parts) > 1 else "")
            play_code = str(parts[2] if len(parts) > 2 else "").strip()
            page = _to_positive_int_or_default(parts[3] if len(parts) > 3 else 1, 1)
        else:
            return _render_subscription_error_page("未识别的按钮操作，请重新发送 /subs", page=1)

        if action == "sub":
            return _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)

        if action == "rfsub":
            result = _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)
            result["callback_text"] = "详情已刷新"
            return result

        if action == "rstc":
            return _render_restart_confirm_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)

        if action == "rstx":
            result = _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)
            result["callback_text"] = "已取消"
            return result

        item = _resolve_user_subscription_by_id(repository, user_id=user_id, subscription_id=subscription_id)
        if str(item.get("status") or "") == "archived":
            return _render_subscription_error_page("该方案当前已归档，请先到网页端恢复。", page=page)

        if action == "en":
            if _subscription_is_risk_blocked(item):
                result = _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)
                result["callback_text"] = "该方案当前处于风控阻塞，请开始新一轮"
                result["callback_alert"] = True
                return result
            if str(item.get("status") or "") != "active":
                update_subscription_status(
                    repository,
                    subscription_id=subscription_id,
                    user_id=user_id,
                    status="active",
                )
            result = _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)
            result["callback_text"] = "已启动"
            return result

        if action == "dis":
            if str(item.get("status") or "") != "inactive":
                update_subscription_status(
                    repository,
                    subscription_id=subscription_id,
                    user_id=user_id,
                    status="inactive",
                )
            result = _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)
            result["callback_text"] = "已暂停"
            return result

        if action == "play":
            play_label = SUBSCRIPTION_PLAY_FILTER_CALLBACKS.get(play_code)
            if not play_label:
                return _render_subscription_error_page("玩法按钮已失效，请重新发送 /subs。", page=page)
            current_play = _subscription_play_filter_label(item)
            if current_play == play_label:
                result = _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)
                result["callback_text"] = "当前已经是“%s”玩法" % play_label
                return result
            strategy_v2 = _subscription_strategy_v2(item)
            strategy_v2["play_filter"] = dict(SUBSCRIPTION_PLAY_FILTER_PRESETS[play_label])
            update_subscription(
                repository,
                subscription_id=subscription_id,
                user_id=user_id,
                payload={"source_id": int(item["source_id"]), "strategy_v2": strategy_v2},
            )
            result = _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)
            result["callback_text"] = "已切换为%s" % play_label
            return result

        if action == "rsta":
            restart_subscription_cycle(
                repository,
                subscription_id=subscription_id,
                user_id=user_id,
                payload={"note": "Telegram callback restart"},
            )
            result = _render_subscription_detail_page(repository, user_id=user_id, subscription_id=subscription_id, page=page)
            result["callback_text"] = "已开始新一轮"
            return result

    except ValueError as exc:
        return _render_subscription_error_page(str(exc) or "操作失败", page=_to_positive_int_or_default(parts[-1] if parts else 1, 1))

    return _render_subscription_error_page("未识别的按钮操作，请重新发送 /subs", page=1)


def _apply_bot_action_to_message(
    bot_client: TelegramBotClient,
    *,
    chat_id: str,
    message_id: Optional[int],
    action: Dict[str, Any],
) -> bool:
    text = str(action.get("text") or "")
    reply_markup = action.get("reply_markup") if isinstance(action.get("reply_markup"), dict) else None
    if message_id is None or int(message_id or 0) <= 0:
        if not text:
            return False
        bot_client.send_text(chat_id, text, reply_markup=reply_markup)
        return True
    try:
        bot_client.edit_text(chat_id, int(message_id), text, reply_markup=reply_markup)
    except Exception as exc:
        if "message is not modified" not in str(exc).lower():
            raise
    return bool(text)


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
        callback_query = update.get("callback_query") if isinstance(update, dict) else None

        if isinstance(callback_query, dict):
            callback_id = str(callback_query.get("id") or "").strip()
            callback_data = str(callback_query.get("data") or "").strip()
            message_obj = callback_query.get("message") if isinstance(callback_query.get("message"), dict) else {}
            chat = message_obj.get("chat") if isinstance(message_obj.get("chat"), dict) else {}
            chat_type = str(chat.get("type") or "").strip()
            chat_id = str(chat.get("id") or "").strip()
            message_id = int(message_obj.get("message_id") or 0)
            from_user = callback_query.get("from") if isinstance(callback_query.get("from"), dict) else {}
            telegram_user_id = from_user.get("id")
            if not callback_id or not callback_data or not chat_id or telegram_user_id is None or chat_type != "private":
                ignored_count += 1
                if update_id > 0:
                    repository.update_telegram_bot_runtime_state(bot_name=bot_name, last_update_id=update_id)
                continue
            try:
                action = _handle_telegram_callback_action(
                    repository,
                    telegram_user_id=int(telegram_user_id),
                    data=callback_data,
                    reference_time=reference_time,
                )
            except Exception as exc:
                action = _bot_action(text=str(exc) or "按钮处理失败", callback_text=str(exc) or "按钮处理失败", callback_alert=True)
            handled_count += 1
            bot_client.answer_callback_query(
                callback_id,
                text=str(action.get("callback_text") or ""),
                show_alert=bool(action.get("callback_alert")),
            )
            if _apply_bot_action_to_message(
                bot_client,
                chat_id=chat_id,
                message_id=message_id,
                action=action,
            ):
                replied_count += 1
            if update_id > 0:
                repository.update_telegram_bot_runtime_state(bot_name=bot_name, last_update_id=update_id)
            continue

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
            action = _handle_telegram_command_action(
                repository,
                telegram_user_id=int(telegram_user_id),
                telegram_chat_id=chat_id,
                telegram_username=telegram_username,
                text=text,
                reference_time=reference_time,
            )
        except Exception as exc:
            action = _bot_action(text=str(exc) or "命令处理失败")
        handled_count += 1
        if str(action.get("text") or "").strip():
            bot_client.send_text(
                chat_id,
                str(action.get("text") or ""),
                reply_markup=action.get("reply_markup") if isinstance(action.get("reply_markup"), dict) else None,
            )
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
