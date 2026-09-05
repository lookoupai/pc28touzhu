"""Automatic source fetch -> normalize -> dispatch service."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.normalize_service import normalize_raw_item
from pc28touzhu.services.source_fetch_service import fetch_source_to_raw_item

# 被闸拦截信号的补派窗口：信号发布后在该时限内仍会随同步周期重试；
# 超时后目标期必然已开奖（期距 210 秒），继续重试无意义，标记留作核查凭据。
PC28_BLOCKED_SIGNAL_RETRY_MAX_AGE_SECONDS = max(
    0, int(os.getenv("PC28_BLOCKED_SIGNAL_RETRY_MAX_AGE_SECONDS", "900"))
)


def collect_active_source_ids(repository: Any) -> List[int]:
    source_ids: List[int] = []
    seen: set[int] = set()
    for user in repository.list_users():
        subscriptions = repository.list_subscriptions(user_id=int(user["id"]))
        for item in subscriptions:
            if str(item.get("status") or "").strip() != "active":
                continue
            source_id = int(item.get("source_id") or 0)
            if source_id <= 0 or source_id in seen:
                continue
            source = repository.get_source(source_id)
            if not source or str(source.get("status") or "").strip() != "active":
                continue
            seen.add(source_id)
            source_ids.append(source_id)
    return source_ids


def redispatch_gate_blocked_signals(
    repository: Any,
    *,
    draw_clock: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """补派曾被封盘闸拦截、至今未产生任何任务的信号。

    典型场景：开奖接口 countdown 字段在开奖后短暂未跳新值，余量被低估而误拦。
    信号仍在投注窗口内时随同步周期重派即可补上；目标期已真正开奖的重派会被闸
    再次拦下并刷新标记（连同 verdict 快照留作核查凭据），超过补派窗口后不再重试。
    """
    summary = {
        "checked_count": 0,
        "blocked_count": 0,
        "created_job_count": 0,
        "existing_job_count": 0,
    }
    if not hasattr(repository, "list_blocked_dispatch_signals"):
        return summary
    cutoff = (
        datetime.now(timezone.utc) - timedelta(seconds=PC28_BLOCKED_SIGNAL_RETRY_MAX_AGE_SECONDS)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")
    signal_ids = repository.list_blocked_dispatch_signals(published_after=cutoff, limit=50)
    for signal_id in signal_ids:
        summary["checked_count"] += 1
        result = dispatch_signal(repository, signal_id=int(signal_id), draw_clock=draw_clock)
        if result.get("blocked"):
            summary["blocked_count"] += 1
        else:
            summary["created_job_count"] += int(result.get("created_count") or 0)
            summary["existing_job_count"] += int(result.get("existing_count") or 0)
    return summary


def run_source_sync_cycle(repository: Any, *, fetcher=None, draw_clock: Dict[str, Any] | None = None) -> Dict[str, Any]:
    source_ids = collect_active_source_ids(repository)
    summary = {
        "source_count": len(source_ids),
        "processed_count": 0,
        "skipped_duplicate_count": 0,
        "skipped_no_signal_count": 0,
        "fetched_count": 0,
        "normalized_signal_count": 0,
        "dispatch_candidate_count": 0,
        "created_job_count": 0,
        "existing_job_count": 0,
        "failed_count": 0,
        "blocked_retry_checked_count": 0,
        "blocked_retry_created_job_count": 0,
    }
    source_results: List[Dict[str, Any]] = []

    retry_result = redispatch_gate_blocked_signals(repository, draw_clock=draw_clock)
    summary["blocked_retry_checked_count"] = int(retry_result.get("checked_count") or 0)
    summary["blocked_retry_created_job_count"] = int(retry_result.get("created_job_count") or 0)

    for source_id in source_ids:
        source = repository.get_source(source_id)
        if not source:
            continue
        result: Dict[str, Any] = {
            "source_id": source_id,
            "source_name": str(source.get("name") or ""),
            "status": "success",
            "raw_item_id": None,
            "created": False,
            "normalized_signal_count": 0,
            "dispatch_candidate_count": 0,
            "created_job_count": 0,
            "existing_job_count": 0,
            "error_message": "",
            "skipped_reason": "",
        }
        try:
            fetch_result = fetch_source_to_raw_item(repository, source_id=source_id, fetcher=fetcher)
            if fetch_result.get("skipped"):
                result["status"] = "skipped"
                result["skipped_reason"] = str(fetch_result.get("skipped_reason") or "upstream_no_signal")
                summary["skipped_no_signal_count"] += 1
                source_results.append(result)
                continue
            raw_item = fetch_result.get("raw_item") or {}
            result["raw_item_id"] = raw_item.get("id")
            result["created"] = bool(fetch_result.get("created"))
            if result["created"]:
                summary["fetched_count"] += 1
            if not result["created"] and str(raw_item.get("parse_status") or "") == "parsed":
                result["status"] = "skipped"
                result["skipped_reason"] = "duplicate_raw_item"
                summary["skipped_duplicate_count"] += 1
                source_results.append(result)
                continue

            normalized = normalize_raw_item(repository, raw_item_id=int(raw_item["id"]))
            result["normalized_signal_count"] = int(normalized.get("created_count") or 0)
            summary["normalized_signal_count"] += result["normalized_signal_count"]

            signals = normalized.get("items") or []
            for signal in signals:
                dispatch_result = dispatch_signal(repository, signal_id=int(signal["id"]), draw_clock=draw_clock)
                result["dispatch_candidate_count"] += int(dispatch_result.get("candidate_count") or 0)
                result["created_job_count"] += int(dispatch_result.get("created_count") or 0)
                result["existing_job_count"] += int(dispatch_result.get("existing_count") or 0)

            summary["dispatch_candidate_count"] += result["dispatch_candidate_count"]
            summary["created_job_count"] += result["created_job_count"]
            summary["existing_job_count"] += result["existing_job_count"]
            summary["processed_count"] += 1
        except Exception as exc:
            result["status"] = "failed"
            result["error_message"] = str(exc) or exc.__class__.__name__
            summary["failed_count"] += 1
        source_results.append(result)

    return {
        "summary": summary,
        "sources": source_results,
    }
