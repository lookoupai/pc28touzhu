"""Automatic source fetch -> normalize -> dispatch service."""
from __future__ import annotations

from typing import Any, Dict, List

from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.normalize_service import normalize_raw_item
from pc28touzhu.services.source_fetch_service import fetch_source_to_raw_item


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


def run_source_sync_cycle(repository: Any, *, fetcher=None) -> Dict[str, Any]:
    source_ids = collect_active_source_ids(repository)
    summary = {
        "source_count": len(source_ids),
        "processed_count": 0,
        "skipped_duplicate_count": 0,
        "fetched_count": 0,
        "normalized_signal_count": 0,
        "dispatch_candidate_count": 0,
        "created_job_count": 0,
        "existing_job_count": 0,
        "failed_count": 0,
    }
    source_results: List[Dict[str, Any]] = []

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
                dispatch_result = dispatch_signal(repository, signal_id=int(signal["id"]))
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
