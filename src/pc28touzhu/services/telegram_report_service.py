"""Telegram daily ranking report services."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Protocol


DEFAULT_REPORT_TYPE = "daily_profit_loss"
SHANGHAI_TZ = timezone(timedelta(hours=8))


class TelegramTextSender(Protocol):
    def send_text(self, target_chat_id: str, message_text: str) -> Dict[str, Any]:
        ...


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _utc_now_iso() -> str:
    return _utc_now().isoformat().replace("+00:00", "Z")


def _resolve_timezone(timezone_name: str) -> timezone:
    if str(timezone_name or "").strip() == "Asia/Shanghai":
        return SHANGHAI_TZ
    return timezone.utc


def _local_now(*, reference_time: Optional[datetime] = None, timezone_name: str = "Asia/Shanghai") -> datetime:
    return (reference_time or _utc_now()).astimezone(_resolve_timezone(timezone_name))


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


def build_daily_profit_rankings(repository: Any, *, stat_date: str, top_n: int = 10) -> Dict[str, Any]:
    items = repository.list_daily_user_profit_rankings(stat_date=str(stat_date or "").strip())
    winners = [item for item in items if round(float(item.get("net_profit") or 0), 2) > 0]
    losers = [item for item in items if round(float(item.get("net_profit") or 0), 2) < 0]
    winners = sorted(winners, key=lambda item: (-float(item.get("net_profit") or 0), int(item.get("user_id") or 0)))[:top_n]
    losers = sorted(losers, key=lambda item: (float(item.get("net_profit") or 0), int(item.get("user_id") or 0)))[:top_n]
    summary = {
        "stat_date": str(stat_date or "").strip(),
        "settled_user_count": len(items),
        "profit_user_count": len([item for item in items if round(float(item.get("net_profit") or 0), 2) > 0]),
        "loss_user_count": len([item for item in items if round(float(item.get("net_profit") or 0), 2) < 0]),
        "total_net_profit": round(sum(float(item.get("net_profit") or 0) for item in items), 2),
    }
    return {
        "summary": summary,
        "profit_ranking": winners,
        "loss_ranking": losers,
    }


def build_daily_profit_report_text(
    *,
    stat_date: str,
    summary: Dict[str, Any],
    profit_ranking: List[Dict[str, Any]],
    loss_ranking: List[Dict[str, Any]],
) -> str:
    lines = [
        "【%s 跟单收益战报】" % str(stat_date or ""),
        "参与用户: %s" % int(summary.get("settled_user_count") or 0),
        "盈利人数: %s" % int(summary.get("profit_user_count") or 0),
        "亏损人数: %s" % int(summary.get("loss_user_count") or 0),
        "总净利润: %s" % _signed_money(summary.get("total_net_profit")),
        "",
        "盈利榜",
    ]
    if profit_ranking:
        for index, item in enumerate(profit_ranking, start=1):
            lines.append("%s. %s %s" % (index, _mask_username(item.get("username")), _signed_money(item.get("net_profit"))))
    else:
        lines.append("暂无上榜用户")

    lines.extend(["", "亏损榜"])
    if loss_ranking:
        for index, item in enumerate(loss_ranking, start=1):
            lines.append("%s. %s %s" % (index, _mask_username(item.get("username")), _signed_money(item.get("net_profit"))))
    else:
        lines.append("暂无上榜用户")

    lines.extend(["", "数据口径：平台昨日已结算跟单结果"])
    return "\n".join(lines)


def build_daily_report_key(*, stat_date: str, target_chat_id: str, report_type: str = DEFAULT_REPORT_TYPE) -> str:
    return "%s:%s:%s" % (str(report_type or "").strip(), str(stat_date or "").strip(), str(target_chat_id or "").strip())


def deliver_daily_profit_report(
    repository: Any,
    *,
    sender: TelegramTextSender,
    target_chat_id: str,
    stat_date: str,
    top_n: int = 10,
    report_type: str = DEFAULT_REPORT_TYPE,
) -> Dict[str, Any]:
    normalized_target_chat_id = str(target_chat_id or "").strip()
    if not normalized_target_chat_id:
        raise ValueError("target_chat_id 不能为空")

    report_key = build_daily_report_key(
        stat_date=str(stat_date or "").strip(),
        target_chat_id=normalized_target_chat_id,
        report_type=report_type,
    )
    current = repository.get_telegram_daily_report_record(report_key)
    if current and str(current.get("status") or "") == "sent":
        return {"skipped": True, "reason": "already_sent", "report_key": report_key, "record": current}

    ranking = build_daily_profit_rankings(repository, stat_date=str(stat_date or "").strip(), top_n=max(1, int(top_n or 1)))
    summary = ranking["summary"]
    if int(summary.get("settled_user_count") or 0) <= 0:
        return {"skipped": True, "reason": "empty_data", "report_key": report_key, "record": current}

    message_text = build_daily_profit_report_text(
        stat_date=str(stat_date or "").strip(),
        summary=summary,
        profit_ranking=ranking["profit_ranking"],
        loss_ranking=ranking["loss_ranking"],
    )
    try:
        send_result = sender.send_text(normalized_target_chat_id, message_text)
        record = repository.mark_telegram_daily_report_sent(
            report_key=report_key,
            stat_date=str(stat_date or "").strip(),
            target_chat_id=normalized_target_chat_id,
            report_type=report_type,
            sent_at=_utc_now_iso(),
        )
        return {
            "skipped": False,
            "report_key": report_key,
            "delivery_status": "sent",
            "summary": summary,
            "send_result": send_result,
            "record": record,
        }
    except Exception as exc:
        record = repository.mark_telegram_daily_report_failed(
            report_key=report_key,
            stat_date=str(stat_date or "").strip(),
            target_chat_id=normalized_target_chat_id,
            report_type=report_type,
            error_message=str(exc),
            failed_at=_utc_now_iso(),
        )
        return {
            "skipped": False,
            "report_key": report_key,
            "delivery_status": "failed",
            "summary": summary,
            "error_message": str(exc) or exc.__class__.__name__,
            "record": record,
        }


def run_daily_report_cycle(
    repository: Any,
    *,
    sender: TelegramTextSender,
    target_chat_id: str,
    send_hour: int,
    send_minute: int,
    top_n: int,
    timezone_name: str = "Asia/Shanghai",
    reference_time: Optional[datetime] = None,
) -> Dict[str, Any]:
    local_now = _local_now(reference_time=reference_time, timezone_name=timezone_name)
    scheduled = local_now.replace(hour=max(0, min(int(send_hour or 0), 23)), minute=max(0, min(int(send_minute or 0), 59)), second=0, microsecond=0)
    if local_now < scheduled:
        return {"skipped": True, "reason": "before_schedule", "now": local_now.isoformat()}

    stat_date = (local_now.date() - timedelta(days=1)).isoformat()
    result = deliver_daily_profit_report(
        repository,
        sender=sender,
        target_chat_id=target_chat_id,
        stat_date=stat_date,
        top_n=top_n,
    )
    result["stat_date"] = stat_date
    result["now"] = local_now.isoformat()
    return result
