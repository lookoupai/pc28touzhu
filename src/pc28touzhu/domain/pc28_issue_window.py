"""PC28 期号投注窗口判定。

投注群在开奖前 20 秒封盘，封盘后收到的投注消息不会作废，而是被顺延记账到下一期，
系统却仍按原期号结算，导致明细与实际错位一整期。因此派单前必须确认：
目标期号尚未开奖，且距该期开奖仍留有足够余量。

本模块只做纯计算，开奖时钟由调用方注入（见 services/pc28_draw_service.get_pc28_draw_clock）。
"""
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, Optional


# PC28 期距严格 210 秒；停机只会让开奖更晚，故用它外推恒为下界（偏保守，安全方向）。
PC28_ISSUE_INTERVAL_SECONDS = 210

# 距开奖的最小余量：群封盘线在开奖前 20 秒，另留 20 秒投递缓冲。
PC28_ISSUE_WINDOW_MIN_SECONDS = max(0, int(os.getenv("PC28_ISSUE_WINDOW_MIN_SECONDS", "40")))


def _parse_iso_z(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        return datetime.fromisoformat(text).astimezone(timezone.utc)
    except ValueError:
        return None


def _parse_issue_no(value: Any) -> Optional[int]:
    text = str(value or "").strip()
    return int(text) if text.isdigit() else None


def evaluate_pc28_issue_dispatch_window(
    *,
    issue_no: Any,
    draw_clock: Any,
    now: Optional[datetime] = None,
    min_remaining_seconds: Optional[int] = None,
) -> Dict[str, Any]:
    """判断目标期号是否仍可投注。

    时钟缺失或字段不可用时一律放行（fail-open）：接口故障时全面停派单的代价，
    远大于错位的基率。判定所依据的数据一并返回，便于事后核查。
    """
    threshold = (
        PC28_ISSUE_WINDOW_MIN_SECONDS
        if min_remaining_seconds is None
        else max(0, int(min_remaining_seconds))
    )
    verdict: Dict[str, Any] = {
        "allowed": True,
        "reason": "clock_unavailable",
        "issue_no": str(issue_no or ""),
        "latest_issue_no": "",
        "issue_offset": None,
        "remaining_seconds": None,
        "min_remaining_seconds": threshold,
        "clock_stale": False,
    }
    clock = draw_clock if isinstance(draw_clock, dict) else None
    if not clock:
        return verdict

    verdict["latest_issue_no"] = str(clock.get("latest_issue_no") or "")
    verdict["clock_stale"] = bool(clock.get("stale"))
    latest_issue_no = _parse_issue_no(clock.get("latest_issue_no"))
    target_issue_no = _parse_issue_no(issue_no)
    if latest_issue_no is None or target_issue_no is None:
        verdict["reason"] = "issue_not_comparable"
        return verdict

    offset = target_issue_no - latest_issue_no
    verdict["issue_offset"] = offset
    if offset <= 0:
        verdict["allowed"] = False
        verdict["reason"] = "issue_already_drawn"
        return verdict

    reference = now if isinstance(now, datetime) else datetime.now(timezone.utc)

    # 余量优先用 data[0] 的开奖时刻直接外推：开奖时刻随开奖结果一起发布，可靠且与时钟陈旧度无关；
    # countdown 是服务器侧计时器，接口异常时会停留在开奖前的旧值（实测曾把 177 秒余量算成 0 误拦派单）。
    # 期距 210 秒外推在停机断档时恒为下界，保守方向不变。
    remaining: Optional[int] = None
    latest_open_time = _parse_iso_z(clock.get("latest_open_time"))
    if latest_open_time is not None:
        remaining = int(latest_open_time.timestamp() + PC28_ISSUE_INTERVAL_SECONDS * offset - reference.timestamp())
        verdict["remaining_source"] = "open_time"
    else:
        countdown_seconds = clock.get("countdown_seconds")
        fetched_at = _parse_iso_z(clock.get("fetched_at"))
        if not isinstance(countdown_seconds, (int, float)) or fetched_at is None:
            verdict["reason"] = "countdown_unavailable"
            return verdict
        # countdown 是抓取瞬间距下一期开奖的秒数，必须扣掉已流逝时间才是当前余量；
        # 这一步同时抵消了 latest_issue_no 的陈旧误差（每晚一期就补 210 秒）。
        elapsed = max(0.0, (reference - fetched_at).total_seconds())
        remaining = int(
            float(countdown_seconds) - elapsed + PC28_ISSUE_INTERVAL_SECONDS * (offset - 1)
        )
        verdict["remaining_source"] = "countdown"
    verdict["remaining_seconds"] = remaining

    if remaining <= 0:
        verdict["allowed"] = False
        verdict["reason"] = "issue_already_drawn"
        return verdict
    if remaining < threshold:
        verdict["allowed"] = False
        verdict["reason"] = "window_too_short"
        return verdict
    verdict["reason"] = "ok"
    return verdict
