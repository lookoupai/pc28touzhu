"""PC28 自动结算后台轮询服务。"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pc28touzhu.services.pc28_draw_service import Fetcher, fetch_pc28_recent_draws
from pc28touzhu.services.platform_service import resolve_pending_pc28_progressions_from_draws


def _pending_subscription_count(repository: Any, *, user_id: int) -> int:
    count = 0
    for item in repository.list_subscriptions(user_id=int(user_id)):
        progression = item.get("progression") if isinstance(item.get("progression"), dict) else {}
        if progression and progression.get("pending_event_id") and str(progression.get("pending_status") or "") == "placed":
            count += 1
    return count


def run_pc28_auto_settlement_cycle(
    repository: Any,
    *,
    draw_limit: int = 60,
    fetcher: Optional[Fetcher] = None,
) -> Dict[str, Any]:
    users = repository.list_users() if hasattr(repository, "list_users") else []
    pending_users: List[Dict[str, Any]] = []
    pending_count = 0
    for user in users:
        user_id = int(user.get("id") or 0)
        if user_id <= 0:
            continue
        user_pending_count = _pending_subscription_count(repository, user_id=user_id)
        if user_pending_count <= 0:
            continue
        pending_users.append({"id": user_id, "pending_count": user_pending_count, "username": str(user.get("username") or "")})
        pending_count += user_pending_count

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
            },
            "users": [],
            "draw_source": "",
        }

    fetch_result = fetch_pc28_recent_draws(limit=max(10, int(draw_limit or 60)), fetcher=fetcher)
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
        "draw_source": draw_source,
    }
