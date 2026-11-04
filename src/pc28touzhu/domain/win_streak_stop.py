"""连续盈利止盈（过三关）通用组件。

只负责「连赢计数 + 停止判定」，不关心投注结构、金额与玩法，任何策略都可以复用：

    tracker = create_tracker(target_streak=3)
    tracker = apply_period_result(tracker, net_pnl)
    tracker = evaluate_stop(tracker, balance=balance)

计数口径（与业务约定一致）：
- 整期净盈亏 > 0：win_streak + 1
- 整期净盈亏 < 0：win_streak 归零
- 整期净盈亏 == 0：计数保持不变（回本不算赢，也不算输）

停止口径：
- win_streak >= target_streak：止盈停止（原因 ``win_streak_target``）
- balance <= 0：本金亏光停止（原因 ``balance_depleted``）
"""
from __future__ import annotations

from typing import Any, Dict, Optional


DEFAULT_TARGET_STREAK = 3
STATUS_RUNNING = "running"
STATUS_STOPPED = "stopped"
STOP_REASON_WIN_STREAK = "win_streak_target"
STOP_REASON_BALANCE_DEPLETED = "balance_depleted"
ALLOWED_STOP_REASONS = (STOP_REASON_WIN_STREAK, STOP_REASON_BALANCE_DEPLETED)


def normalize_target_streak(value: Any, field_name: str = "target_streak") -> int:
    """归一化目标连赢次数（>= 1）。"""
    if value in (None, ""):
        return DEFAULT_TARGET_STREAK
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        raise ValueError("%s 必须为整数" % field_name)
    if normalized < 1:
        raise ValueError("%s 必须 >= 1" % field_name)
    return normalized


def create_tracker(target_streak: Any = DEFAULT_TARGET_STREAK) -> Dict[str, Any]:
    """创建当天连赢计数器。"""
    return {
        "win_streak": 0,
        "target_streak": normalize_target_streak(target_streak),
        "status": STATUS_RUNNING,
        "stop_reason": None,
    }


def apply_period_result(tracker: Dict[str, Any], net_pnl: Any) -> Dict[str, Any]:
    """按一整期净盈亏更新连赢计数；已停止时不改变计数。"""
    state = dict(tracker or {})
    if state.get("status") == STATUS_STOPPED:
        return state
    try:
        value = float(net_pnl or 0)
    except (TypeError, ValueError):
        raise ValueError("net_pnl 必须为数字")
    streak = int(state.get("win_streak") or 0)
    if value > 0:
        streak += 1
    elif value < 0:
        streak = 0
    # value == 0：保持 win_streak 不变
    state["win_streak"] = streak
    return state


def evaluate_stop(tracker: Dict[str, Any], *, balance: Any) -> Dict[str, Any]:
    """检查停止条件；满足时写入 status/stop_reason。"""
    state = dict(tracker or {})
    state.setdefault("status", STATUS_RUNNING)
    state.setdefault("stop_reason", None)
    if state.get("status") == STATUS_STOPPED:
        return state
    try:
        current_balance = float(balance)
    except (TypeError, ValueError):
        raise ValueError("balance 必须为数字")
    if int(state.get("win_streak") or 0) >= int(state.get("target_streak") or DEFAULT_TARGET_STREAK):
        state["status"] = STATUS_STOPPED
        state["stop_reason"] = STOP_REASON_WIN_STREAK
    elif current_balance <= 0:
        state["status"] = STATUS_STOPPED
        state["stop_reason"] = STOP_REASON_BALANCE_DEPLETED
    return state


def update_after_period(
    tracker: Dict[str, Any],
    net_pnl: Any,
    *,
    balance: Any,
    target_streak: Optional[Any] = None,
) -> Dict[str, Any]:
    """一步完成：更新计数 + 检查停止。"""
    merged = dict(tracker or {})
    if target_streak is not None:
        merged["target_streak"] = normalize_target_streak(target_streak)
    return evaluate_stop(apply_period_result(merged, net_pnl), balance=balance)


def is_stopped(tracker: Dict[str, Any]) -> bool:
    return str((tracker or {}).get("status") or "") == STATUS_STOPPED
