"""结算规则目录。"""
from __future__ import annotations

import re
from typing import Any, Dict, Optional

from pc28touzhu.domain.pc28_profit_rules import normalize_odds_profile, normalize_profit_rule_id


DEFAULT_SETTLEMENT_RULE_ID = "pc28_netdisk_regular"

SETTLEMENT_RULE_CATALOG: Dict[str, Dict[str, Any]] = {
    "pc28_netdisk_regular": {
        "id": "pc28_netdisk_regular",
        "name": "PC28 网盘常规",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_netdisk",
        "odds_profile": "regular",
        "special_refund_sum_values": [],
        "refund_policy_by_metric": {"big_small": "none", "odd_even": "none", "combo": "none", "number": "none"},
    },
    "pc28_netdisk_abc": {
        "id": "pc28_netdisk_abc",
        "name": "PC28 网盘 ABC",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_netdisk",
        "odds_profile": "abc",
        "special_refund_sum_values": [],
        "refund_policy_by_metric": {"big_small": "none", "odd_even": "none", "combo": "none", "number": "none"},
    },
    "pc28_high_regular": {
        "id": "pc28_high_regular",
        "name": "PC28 高赔常规",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_high",
        "odds_profile": "regular",
        "special_refund_sum_values": [13, 14],
        "refund_policy_by_metric": {"big_small": "special_on_hit", "odd_even": "special_on_hit", "combo": "special_on_hit", "number": "none"},
    },
    "pc28_high_abc": {
        "id": "pc28_high_abc",
        "name": "PC28 高赔 ABC",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_high",
        "odds_profile": "abc",
        "special_refund_sum_values": [13, 14],
        "refund_policy_by_metric": {"big_small": "special_on_hit", "odd_even": "special_on_hit", "combo": "special_on_hit", "number": "none"},
    },
}

ALLOWED_SETTLEMENT_RULE_IDS = set(SETTLEMENT_RULE_CATALOG.keys())

LEGACY_TO_SETTLEMENT_RULE_ID = {
    (rule["profit_rule_id"], rule["odds_profile"]): rule_id
    for rule_id, rule in SETTLEMENT_RULE_CATALOG.items()
}


def normalize_settlement_rule_id(value: Any, *, allow_empty: bool = True) -> Optional[str]:
    text = str(value or "").strip().lower()
    if not text:
        if allow_empty:
            return None
        raise ValueError("settlement_rule_id 不能为空")
    if text not in ALLOWED_SETTLEMENT_RULE_IDS:
        raise ValueError("不支持的 settlement_rule_id")
    return text


def get_settlement_rule(rule_id: Any) -> Optional[Dict[str, Any]]:
    normalized = normalize_settlement_rule_id(rule_id, allow_empty=True)
    if not normalized:
        return None
    rule = SETTLEMENT_RULE_CATALOG.get(normalized)
    return dict(rule) if rule else None


def settlement_rule_id_from_legacy(profit_rule_id: Any, odds_profile: Any) -> str:
    key = (
        normalize_profit_rule_id(profit_rule_id),
        normalize_odds_profile(odds_profile),
    )
    return LEGACY_TO_SETTLEMENT_RULE_ID.get(key, DEFAULT_SETTLEMENT_RULE_ID)


def legacy_profit_rule_args_from_settlement_rule_id(rule_id: Any) -> Dict[str, str]:
    rule = get_settlement_rule(rule_id)
    if not rule:
        return {}
    return {
        "profit_rule_id": str(rule.get("profit_rule_id") or ""),
        "odds_profile": str(rule.get("odds_profile") or ""),
    }


def build_settlement_snapshot(
    *,
    rule_source: str,
    settlement_rule_id: Optional[str],
    fallback_profit_ratio: float,
    resolved_from: str,
    signal: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    signal_payload = signal if isinstance(signal, dict) else {}
    normalized_rule_id = normalize_settlement_rule_id(settlement_rule_id, allow_empty=True)
    rule = get_settlement_rule(normalized_rule_id)
    return {
        "rule_source": str(rule_source or ""),
        "settlement_rule_id": normalized_rule_id,
        "fallback_profit_ratio": round(float(fallback_profit_ratio or 1.0), 4),
        "resolved_from": str(resolved_from or ""),
        "rule": rule,
        "signal": {
            "lottery_type": str(signal_payload.get("lottery_type") or ""),
            "issue_no": str(signal_payload.get("issue_no") or ""),
            "bet_type": str(signal_payload.get("bet_type") or ""),
            "bet_value": str(signal_payload.get("bet_value") or ""),
        },
    }


def _to_object(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _parse_optional_int(value: Any) -> Optional[int]:
    if value in {None, ""}:
        return None
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _parse_triplet(value: Any) -> Optional[tuple[int, int, int]]:
    if isinstance(value, (list, tuple)) and len(value) == 3:
        numbers = []
        for item in value:
            parsed = _parse_optional_int(item)
            if parsed is None:
                return None
            numbers.append(parsed)
        return tuple(numbers)
    text = str(value or "").strip()
    if not text:
        return None
    numbers = [int(item) for item in re.findall(r"\d+", text)]
    if len(numbers) == 3:
        return tuple(numbers[:3])
    if len(numbers) >= 4 and sum(numbers[:3]) == numbers[3]:
        return tuple(numbers[:3])
    return None


def _is_pc28_baozi(triplet: Optional[tuple[int, int, int]]) -> bool:
    return bool(triplet) and len(set(triplet)) == 1


def _is_pc28_pair(triplet: Optional[tuple[int, int, int]]) -> bool:
    return bool(triplet) and len(set(triplet)) == 2


def _is_pc28_straight(triplet: Optional[tuple[int, int, int]]) -> bool:
    if not triplet or len(set(triplet)) != 3:
        return False
    ordered = sorted(triplet)
    return ordered[0] + 1 == ordered[1] and ordered[1] + 1 == ordered[2]


def _resolve_metric_from_signal(signal: Dict[str, Any]) -> Optional[str]:
    bet_type = str(signal.get("bet_type") or "").strip().lower()
    bet_value = str(signal.get("bet_value") or "").strip()
    if bet_type == "combo" and bet_value in {"大单", "大双", "小单", "小双"}:
        return "combo"
    if bet_type == "odd_even" and bet_value in {"单", "双"}:
        return "odd_even"
    if bet_type == "big_small":
        if bet_value in {"大", "小"}:
            return "big_small"
        if bet_value in {"单", "双"}:
            return "odd_even"
    return None


def derive_pc28_draw_snapshot(draw_context: Any) -> Dict[str, Any]:
    payload = _to_object(draw_context)
    triplet = _parse_triplet(payload.get("triplet") or payload.get("open_numbers") or payload.get("open_code"))
    sum_value = _parse_optional_int(payload.get("sum_value"))
    if sum_value is None:
        sum_value = _parse_optional_int(payload.get("result_number"))
    if sum_value is None:
        open_code_number = _parse_optional_int(payload.get("open_code"))
        if open_code_number is not None:
            sum_value = open_code_number
    if sum_value is None and triplet:
        sum_value = sum(triplet)
    big_small = str(payload.get("big_small") or "").strip()
    if not big_small and sum_value is not None:
        big_small = "小" if int(sum_value) <= 13 else "大"
    odd_even = str(payload.get("odd_even") or "").strip()
    if not odd_even and sum_value is not None:
        odd_even = "双" if int(sum_value) % 2 == 0 else "单"
    combo = str(payload.get("combo") or "").strip()
    if not combo and big_small and odd_even:
        combo = big_small + odd_even
    is_special_sum = bool(payload.get("is_special_sum")) if "is_special_sum" in payload else (sum_value in {13, 14})
    is_baozi = bool(payload.get("is_baozi")) if "is_baozi" in payload else _is_pc28_baozi(triplet)
    is_straight = bool(payload.get("is_straight")) if "is_straight" in payload else _is_pc28_straight(triplet)
    is_pair = bool(payload.get("is_pair")) if "is_pair" in payload else _is_pc28_pair(triplet)
    return {
        "sum_value": int(sum_value) if sum_value is not None else None,
        "result_number": int(sum_value) if sum_value is not None else None,
        "triplet": list(triplet) if triplet else None,
        "big_small": big_small or None,
        "odd_even": odd_even or None,
        "combo": combo or None,
        "special_flags": {
            "is_special_sum": bool(is_special_sum),
            "is_baozi": bool(is_baozi),
            "is_straight": bool(is_straight),
            "is_pair": bool(is_pair),
        },
    }


def _resolve_refund_aware_result(
    *,
    hit: bool,
    special_flags: Dict[str, Any],
    refund_policy: str,
) -> tuple[str, Optional[str]]:
    if not hit:
        return ("miss", None)
    if str(refund_policy or "none") != "special_on_hit":
        return ("hit", None)
    if special_flags.get("is_special_sum"):
        return ("refund", "13/14 退本金")
    if special_flags.get("is_baozi"):
        return ("refund", "豹子退本金")
    if special_flags.get("is_straight"):
        return ("refund", "顺子退本金")
    if special_flags.get("is_pair"):
        return ("refund", "对子退本金")
    return ("hit", None)


def resolve_pc28_result_for_signal(
    *,
    signal: Dict[str, Any],
    settlement_rule_id: Any,
    draw_context: Any,
) -> Dict[str, Any]:
    rule = get_settlement_rule(settlement_rule_id) or {
        "id": "",
        "lottery_type": "pc28",
        "refund_policy_by_metric": {"big_small": "none", "odd_even": "none", "combo": "none", "number": "none"},
    }
    snapshot = derive_pc28_draw_snapshot(draw_context)
    metric = _resolve_metric_from_signal(signal)
    if metric is None:
        raise ValueError("当前信号玩法暂不支持自动结算")
    predicted_value = str(signal.get("bet_value") or "").strip()
    actual_value = snapshot.get(metric)
    if actual_value in {None, ""}:
        raise ValueError("draw_context 缺少可用于自动结算的开奖结果")
    actual_value = str(actual_value)
    refund_policy = str((_to_object(rule.get("refund_policy_by_metric"))).get(metric) or "none")
    result_type, refund_reason = _resolve_refund_aware_result(
        hit=predicted_value == actual_value,
        special_flags=_to_object(snapshot.get("special_flags")),
        refund_policy=refund_policy,
    )
    return {
        "lottery_type": "pc28",
        "metric": metric,
        "predicted_value": predicted_value,
        "actual_value": actual_value,
        "result_type": result_type,
        "refund_reason": refund_reason,
        "settlement_rule_id": str(rule.get("id") or ""),
        "draw_snapshot": snapshot,
    }
