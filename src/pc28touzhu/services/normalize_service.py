"""Convert raw source items into normalized signals."""
from __future__ import annotations

from datetime import datetime, timezone
from contextlib import nullcontext
from typing import Any, Dict, List

from pc28touzhu.domain.pc28_group_play import (
    COMBO_VALUES, GROUP_PLAY_TYPES, derive_group_selections, normalize_group_selection,
)
from pc28touzhu.domain.subscription_strategy import enrich_signal_payload_source_hints


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


def _prepare_group_entries(entries: List[dict], raw_item: dict) -> List[dict]:
    """先校验整批，再派生缺失玩法；显式信号优先，不从冲突组合猜方向。"""
    prepared = []
    explicit = {}
    combo_entries = {}
    group_fields = ("selected_combos", "excluded_combo", "group_derivation", "origin_combo")
    for original in entries:
        entry = {**original, "normalized_payload": dict(_as_object(original.get("normalized_payload")))}
        lottery = str(entry.get("lottery_type") or "pc28").strip()
        issue = str(entry.get("issue_no") or raw_item.get("issue_no") or "").strip()
        metric = str(entry.get("bet_type") or "").strip()
        metadata = entry["normalized_payload"]
        source_ref = _as_object(metadata.get("source_ref"))
        scope = (lottery, issue, str(source_ref.get("predictor_id") or metadata.get("predictor_id") or ""))
        if lottery == "pc28" and metric in GROUP_PLAY_TYPES:
            for field in group_fields:
                if field in entry:
                    if field in metadata and metadata[field] != entry[field]:
                        raise ValueError("组信号顶层和 normalized_payload 的 %s 不一致" % field)
                    metadata[field] = entry[field]
            group = normalize_group_selection(metric, entry.get("bet_value"), metadata)
            entry["bet_value"] = group["bet_value"]
            metadata.update(group)
            metadata.pop("message_text", None)
            entry.pop("message_text", None)
            key = scope + (metric,)
            if key in explicit:
                if explicit[key]["bet_value"] != entry["bet_value"]:
                    raise ValueError("同一期同一来源的组玩法方向冲突")
                continue
            explicit[key] = entry
        elif lottery == "pc28" and metric == "combo" and entry.get("bet_value") in COMBO_VALUES:
            combo_entries.setdefault(scope, []).append(entry)
        prepared.append(entry)
    derived = []
    for scope, candidates in combo_entries.items():
        values = {candidate["bet_value"] for candidate in candidates}
        if len(values) != 1:
            continue
        source = candidates[0]
        for group in derive_group_selections(source["bet_value"]):
            if scope + (group["bet_type"],) in explicit:
                continue
            metadata = {**source["normalized_payload"], **group}
            metadata.pop("message_text", None)
            if metadata.get("group_settlement_rule_id"):
                hints = dict(_as_object(metadata.get("source_hints")))
                hints["settlement"] = {**_as_object(hints.get("settlement")), "settlement_rule_id": metadata["group_settlement_rule_id"]}
                metadata["source_hints"] = hints
            derived.append({
                **source, "bet_type": group["bet_type"], "bet_value": group["bet_value"],
                "normalized_payload": metadata, "confidence": None, "_derived_locally": True,
            })
            derived[-1].pop("message_text", None)
    # 保持旧来源的“最新一条”仍为原玩法，避免 keep 路由被新派生信号抢走。
    return derived + [entry for entry in prepared if entry.get("bet_type") in GROUP_PLAY_TYPES] + [
        entry for entry in prepared if entry.get("bet_type") not in GROUP_PLAY_TYPES
    ]


def normalize_raw_item(repository: Any, raw_item_id: int) -> Dict[str, Any]:
    try:
        with repository.transaction() if hasattr(repository, "transaction") else nullcontext(repository) as unit:
            return _normalize_raw_item(unit, raw_item_id)
    except (ValueError, TypeError) as exc:
        if repository.get_raw_item(raw_item_id):
            repository.update_raw_item_parse_result(raw_item_id, parse_status="failed", parse_error=str(exc))
        raise


def _normalize_raw_item(repository: Any, raw_item_id: int) -> Dict[str, Any]:
    raw_item = repository.get_raw_item(raw_item_id)
    if not raw_item:
        raise ValueError("raw_item 不存在")

    existing_items = repository.list_signals_by_raw_item(int(raw_item_id))
    if str(raw_item.get("parse_status") or "") == "parsed" and existing_items:
        return {
            "raw_item": raw_item,
            "created_count": 0,
            "items": existing_items,
        }

    try:
        entries = _prepare_group_entries(_as_signal_entries(raw_item), raw_item)
    except ValueError as exc:
        repository.update_raw_item_parse_result(raw_item_id, parse_status="failed", parse_error=str(exc))
        raise
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

        if entry.get("_derived_locally") and hasattr(repository, "get_latest_ready_signal_for_source"):
            frozen = repository.get_latest_ready_signal_for_source(source_id=int(raw_item["source_id"]), issue_no=issue_no, bet_type=bet_type)
            if frozen and frozen.get("bet_value") != bet_value:
                # 同期来源修订不能给已冻结组单换方向，也不阻断原有单组合信号。
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
            "profit_rule_id",
            "odds_profile",
            "primary_metric",
            "share_level",
            "source_note",
        ):
            if key in entry and key not in normalized_payload:
                normalized_payload[key] = entry[key]
        normalized_payload = enrich_signal_payload_source_hints(normalized_payload)

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
        "created_count": sum(item.get("source_raw_item_id") == int(raw_item_id) for item in created_items),
        "items": created_items,
    }
