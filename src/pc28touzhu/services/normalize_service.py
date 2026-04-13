"""Convert raw source items into normalized signals."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _as_object(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_signal_entries(raw_item: Dict[str, Any]) -> List[Dict[str, Any]]:
    payload = _as_object(raw_item.get("raw_payload"))
    items = payload.get("items")
    if isinstance(items, list):
        flattened = []
        for item in items:
            if not isinstance(item, dict):
                continue
            base_issue_no = str(item.get("issue_no") or "").strip()
            base_lottery_type = str(item.get("lottery_type") or "pc28").strip() or "pc28"
            source_ref = _as_object(item.get("source_ref"))
            for signal in item.get("signals") or []:
                if not isinstance(signal, dict):
                    continue
                merged = dict(signal)
                if "issue_no" not in merged and base_issue_no:
                    merged["issue_no"] = base_issue_no
                if "lottery_type" not in merged and base_lottery_type:
                    merged["lottery_type"] = base_lottery_type
                merged.setdefault("normalized_payload", {})
                if isinstance(merged["normalized_payload"], dict):
                    if item.get("signal_id") and "signal_id" not in merged["normalized_payload"]:
                        merged["normalized_payload"]["signal_id"] = item.get("signal_id")
                    if source_ref and "source_ref" not in merged["normalized_payload"]:
                        merged["normalized_payload"]["source_ref"] = source_ref
                flattened.append(merged)
        if flattened:
            return flattened

    signals = payload.get("signals")
    if isinstance(signals, list):
        return [item for item in signals if isinstance(item, dict)]

    if payload.get("bet_type") and payload.get("bet_value"):
        return [payload]

    return []


def normalize_raw_item(repository: Any, raw_item_id: int) -> Dict[str, Any]:
    raw_item = repository.get_raw_item(raw_item_id)
    if not raw_item:
        raise ValueError("raw_item 不存在")

    entries = _as_signal_entries(raw_item)
    if not entries:
        repository.update_raw_item_parse_result(
            raw_item_id,
            parse_status="failed",
            parse_error="raw_payload 中未找到可标准化的 signals 或 bet_type/bet_value",
        )
        raise ValueError("raw_payload 中未找到可标准化的 signals 或 bet_type/bet_value")

    created_items = []
    for entry in entries:
        issue_no = str(entry.get("issue_no") or raw_item.get("issue_no") or "").strip()
        lottery_type = str(entry.get("lottery_type") or "pc28").strip() or "pc28"
        bet_type = str(entry.get("bet_type") or "").strip()
        bet_value = str(entry.get("bet_value") or "").strip()
        if not issue_no or not bet_type or not bet_value:
            continue

        normalized_payload = _as_object(entry.get("normalized_payload"))
        for key in (
            "message_text",
            "stake_amount",
            "base_stake",
            "multiplier",
            "max_steps",
            "refund_action",
            "cap_action",
            "primary_metric",
            "share_level",
            "source_note",
        ):
            if key in entry and key not in normalized_payload:
                normalized_payload[key] = entry[key]

        created_items.append(
            repository.create_signal_record(
                source_id=int(raw_item["source_id"]),
                source_raw_item_id=int(raw_item_id),
                lottery_type=lottery_type,
                issue_no=issue_no,
                bet_type=bet_type,
                bet_value=bet_value,
                confidence=float(entry["confidence"]) if entry.get("confidence") not in {None, ""} else None,
                normalized_payload=normalized_payload,
                published_at=raw_item.get("published_at") or _utc_now_iso(),
                status="ready",
            )
        )

    if not created_items:
        repository.update_raw_item_parse_result(
            raw_item_id,
            parse_status="failed",
            parse_error="signals 缺少 issue_no / bet_type / bet_value",
        )
        raise ValueError("signals 缺少 issue_no / bet_type / bet_value")

    updated_raw_item = repository.update_raw_item_parse_result(raw_item_id, parse_status="parsed", parse_error=None)
    return {
        "raw_item": updated_raw_item,
        "created_count": len(created_items),
        "items": created_items,
    }
