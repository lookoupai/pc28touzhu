"""PC28 自动结算后台轮询服务。"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pc28touzhu.domain.win_streak_roll import apply_period_outcome, resolve_day_close_action
from pc28touzhu.services.pc28_draw_service import Fetcher, fetch_pc28_recent_draws_deep
from pc28touzhu.services.platform_service import (
    collect_pending_pc28_progressions,
    estimate_pc28_draw_fetch_limit,
    resolve_pending_pc28_progressions_from_draws,
)


# 一条腿只有到了终态，才能拿它的净盈亏推进玩法周期。
# 关键：placed 表示消息已发出、开奖还没到（此时 net_delta 还是 0），
# 早期版本在这里只排除 pending，导致周期在开奖前就结账，净盈亏恒按 0 记，
# 连赢计数、滚仓注码、止盈止损全部失效。
TERMINAL_CYCLE_LEG_STATUSES = ("settled", "void")


def _first_leg_value(leg_events: List[Dict[str, Any]], key: str) -> Any:
    for event in leg_events:
        value = (event or {}).get(key)
        if value not in (None, "", 0):
            return value
    return None


def _close_cycle_day(repository: Any, *, action: Dict[str, Any], leg_events: List[Dict[str, Any]], pending: Dict[str, Any]) -> Dict[str, Any]:
    """过关达标 / 爆仓收工：把停止写到规则（或路由）当日统计，复用既有派单与评估闸门。"""
    rule_id = int(pending.get("rule_id") or 0) or int(_first_leg_value(leg_events, "auto_trigger_rule_id") or 0)
    route_id = int(pending.get("route_id") or 0) or int(_first_leg_value(leg_events, "auto_trigger_route_id") or 0)
    rule_stat_date = str(pending.get("rule_stat_date") or "") or str(_first_leg_value(leg_events, "auto_trigger_stat_date") or "")
    user_id = int(pending.get("user_id") or 0) or int(_first_leg_value(leg_events, "user_id") or 0)
    reason = str(action.get("reason") or "cycle_stopped")
    outcome = {"closed": False, "scope": str(action.get("scope") or "rule"), "reason": reason}
    if not rule_stat_date:
        outcome["skipped_reason"] = "no_stat_date"
        return outcome
    try:
        if str(action.get("scope") or "rule") == "route" and route_id and hasattr(repository, "stop_auto_trigger_route_day"):
            repository.stop_auto_trigger_route_day(
                route_id=int(route_id), user_id=user_id, stat_date=rule_stat_date, reason=reason,
            )
            outcome.update({"closed": True, "route_id": int(route_id), "stat_date": rule_stat_date})
            return outcome
        if rule_id and hasattr(repository, "stop_auto_trigger_rule_day_for_cycle"):
            repository.stop_auto_trigger_rule_day_for_cycle(
                rule_id=int(rule_id), user_id=user_id, stat_date=rule_stat_date, reason=reason,
            )
            outcome.update({"closed": True, "rule_id": int(rule_id), "stat_date": rule_stat_date})
            return outcome
        outcome["skipped_reason"] = "no_rule_day_target"
    except Exception as exc:  # 收工失败不能影响周期状态推进
        outcome["error"] = str(exc) or exc.__class__.__name__
    return outcome


def _resolve_pending_win_streak_cycles(repository: Any) -> Dict[str, Any]:
    """过关滚仓玩法：两腿都到终态（settled / void）后，推进玩法周期状态（余额/连赢/止盈止损）。

    只有全部腿终态才结账，并带上规则 / 路由信息，收工时按 stop_policy 决定是否当天收口。
    """
    if not hasattr(repository, "list_pending_win_streak_cycles"):
        return {"resolved_count": 0, "waiting_count": 0, "closed_day_count": 0, "items": []}
    items: List[Dict[str, Any]] = []
    waiting_count = 0
    closed_day_count = 0
    for row in repository.list_pending_win_streak_cycles():
        pending = row.get("pending_plan") or {}
        leg_event_ids = [int(i) for i in (pending.get("leg_event_ids") or [])]
        if not leg_event_ids:
            continue
        leg_events = [repository.get_progression_event(event_id) for event_id in leg_event_ids]
        if any(event is None for event in leg_events):
            waiting_count += 1
            continue
        if any(str(event.get("status") or "pending") not in TERMINAL_CYCLE_LEG_STATUSES for event in leg_events):
            waiting_count += 1
            continue
        policy = row.get("policy") or {}
        snapshot_state = pending.get("state") or {}
        stake_total = float(pending.get("stake_total") or 0)
        # net_delta 对 hit/miss/refund 都满足：返还额 = 注码 + 净盈亏；
        # void（消息过期未发出）净盈亏为 0，等于该腿退本、没有真正上注。
        payout_total = round(sum(
            float(event.get("stake_amount") or 0) + float(event.get("net_delta") or 0)
            for event in leg_events
        ), 2)
        new_state = apply_period_outcome(policy, snapshot_state, stake_total=stake_total, payout_total=payout_total)
        day_close = resolve_day_close_action(policy, new_state)
        close_result: Dict[str, Any] = {"closed": False}
        if day_close.get("close_day"):
            payload = dict(pending)
            payload.setdefault("user_id", int(row["user_id"]))
            close_result = _close_cycle_day(
                repository, action=day_close, leg_events=leg_events, pending=payload,
            )
            if close_result.get("closed"):
                closed_day_count += 1
        repository.upsert_subscription_cycle_state(
            subscription_id=int(row["subscription_id"]),
            user_id=int(row["user_id"]),
            state=new_state,
            policy=policy,
            pending_plan=None,
        )
        items.append({
            "subscription_id": int(row["subscription_id"]),
            "issue_no": str(pending.get("issue_no") or ""),
            "net_pnl": round(payout_total - stake_total, 2),
            "balance": new_state.get("balance"),
            "win_streak": new_state.get("win_streak"),
            "status": new_state.get("status"),
            "stop_reason": new_state.get("stop_reason"),
            "cycles_today": new_state.get("cycles_today"),
            "day_closed": bool(new_state.get("day_closed")),
            "day_close": close_result,
        })
    return {
        "resolved_count": len(items),
        "waiting_count": waiting_count,
        "closed_day_count": closed_day_count,
        "items": items,
    }


def _pending_progression_entries(repository: Any, *, user_id: int) -> List[Dict[str, Any]]:
    return collect_pending_pc28_progressions(repository, user_id=int(user_id))


def run_pc28_auto_settlement_cycle(
    repository: Any,
    *,
    draw_limit: int = 60,
    fetcher: Optional[Fetcher] = None,
) -> Dict[str, Any]:
    users = repository.list_users() if hasattr(repository, "list_users") else []
    pending_users: List[Dict[str, Any]] = []
    pending_count = 0
    pending_entries_all: List[Dict[str, Any]] = []
    for user in users:
        user_id = int(user.get("id") or 0)
        if user_id <= 0:
            continue
        user_pending_entries = _pending_progression_entries(repository, user_id=user_id)
        user_pending_count = len(user_pending_entries)
        if user_pending_count <= 0:
            continue
        pending_users.append({"id": user_id, "pending_count": user_pending_count, "username": str(user.get("username") or "")})
        pending_count += user_pending_count
        pending_entries_all.extend(user_pending_entries)

    cycle_resolution = _resolve_pending_win_streak_cycles(repository)
    if pending_count <= 0:
        return {
            "skipped": True,
            "reason": "no_pending_progressions",
            "summary": {
                "user_count": 0,
                "pending_count": 0,
                "resolved_count": 0,
                "hit_count": 0,
                "refund_count": 0,
                "miss_count": 0,
                "unmatched_count": 0,
                "cycle_resolved_count": cycle_resolution.get("resolved_count", 0),
                "cycle_waiting_count": cycle_resolution.get("waiting_count", 0),
                "cycle_closed_day_count": cycle_resolution.get("closed_day_count", 0),
            },
            "users": [],
            "cycle_items": list(cycle_resolution.get("items") or []),
            "draw_source": "",
        }

    fetch_result = fetch_pc28_recent_draws_deep(
        limit=estimate_pc28_draw_fetch_limit(pending_entries_all, base_limit=max(10, int(draw_limit or 60))),
        fetcher=fetcher,
    )
    draw_items = list(fetch_result.get("items") or [])
    draw_source = str(fetch_result.get("source") or "")
    results = []
    summary = {
        "user_count": len(pending_users),
        "pending_count": pending_count,
        "resolved_count": 0,
        "hit_count": 0,
        "refund_count": 0,
        "miss_count": 0,
        "unmatched_count": 0,
        "cycle_resolved_count": int(cycle_resolution.get("resolved_count") or 0),
        "cycle_waiting_count": int(cycle_resolution.get("waiting_count") or 0),
        "cycle_closed_day_count": int(cycle_resolution.get("closed_day_count") or 0),
    }
    for user in pending_users:
        resolved = resolve_pending_pc28_progressions_from_draws(
            repository,
            user_id=int(user["id"]),
            draw_items=draw_items,
            draw_source=draw_source,
        )
        result_summary = resolved.get("summary") if isinstance(resolved.get("summary"), dict) else {}
        summary["resolved_count"] += int(result_summary.get("resolved_count") or 0)
        summary["hit_count"] += int(result_summary.get("hit_count") or 0)
        summary["refund_count"] += int(result_summary.get("refund_count") or 0)
        summary["miss_count"] += int(result_summary.get("miss_count") or 0)
        summary["unmatched_count"] += int(result_summary.get("unmatched_count") or 0)
        results.append(
            {
                "user_id": int(user["id"]),
                "username": str(user.get("username") or ""),
                "summary": result_summary,
                "items": list(resolved.get("items") or []),
                "unmatched": list(resolved.get("unmatched") or []),
            }
        )
    return {
        "skipped": False,
        "reason": "",
        "summary": summary,
        "users": results,
        "cycle_items": list(cycle_resolution.get("items") or []),
        "draw_source": draw_source,
    }
