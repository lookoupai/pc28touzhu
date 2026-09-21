"""结算规则目录。"""
from __future__ import annotations

import copy
import re
from typing import Any, Dict, Optional

from pc28touzhu.domain.pc28_profit_rules import (
    HOUSE_NUMBER_ODDS,
    HOUSE_REFERENCE_ODDS,
    catalog_implemented_odds,
    coerce_odds_overrides,
    drop_matching_catalog_odds,
    merge_implemented_odds,
    normalize_odds_profile,
    normalize_profit_rule_id,
    resolve_pc28_odds_detail,
)


DEFAULT_SETTLEMENT_RULE_ID = "pc28_netdisk_regular"


def _metric_settlement(
    *,
    on_hit_refund_sums: tuple[int, ...] = (),
    refund_pair: bool = False,
    refund_straight: bool = False,
    refund_baozi: bool = False,
    refund_abc_zero_or_nine: bool = False,
    odds_tiers: Optional[list] = None,
    legacy_policy: str = "none",
) -> Dict[str, Any]:
    return {
        "on_hit_refund_sum_values": list(on_hit_refund_sums),
        "on_hit_refund_pair": bool(refund_pair),
        "on_hit_refund_straight": bool(refund_straight),
        "on_hit_refund_baozi": bool(refund_baozi),
        "on_hit_refund_abc_zero_or_nine": bool(refund_abc_zero_or_nine),
        "on_hit_odds_tiers": list(odds_tiers or []),
        "legacy_policy": str(legacy_policy or "none"),
    }


_NONE_METRIC = _metric_settlement()
_HIGH_SPECIAL_ON_HIT = _metric_settlement(
    on_hit_refund_sums=(13, 14),
    refund_pair=True,
    refund_straight=True,
    refund_baozi=True,
    legacy_policy="special_on_hit",
)
_ON_HIT_0_27 = _metric_settlement(on_hit_refund_sums=(0, 27), legacy_policy="special_sum_on_hit")
_FULLPAY_2_0_BIG_SMALL = _metric_settlement(
    on_hit_refund_sums=(0, 27),
    odds_tiers=[{"sum_values": [13, 14], "stake_gt": 2001, "odds": 1.98}],
    legacy_policy="special_sum_on_hit",
)
_FULLPAY_2_0_COMBO = _metric_settlement(
    on_hit_refund_sums=(13, 14),
    legacy_policy="special_sum_on_hit",
)
_FULLPAY_2_8 = _metric_settlement(
    on_hit_refund_sums=(13, 14),
    refund_pair=True,
    refund_straight=True,
    refund_baozi=True,
    legacy_policy="special_on_hit",
)
_FULLPAY_3_2 = _metric_settlement(
    on_hit_refund_sums=(13, 14),
    refund_abc_zero_or_nine=True,
    legacy_policy="special_sum_or_edge_on_hit",
)


SETTLEMENT_RULE_CATALOG: Dict[str, Dict[str, Any]] = {
    "pc28_netdisk_regular": {
        "id": "pc28_netdisk_regular",
        "name": "OK游戏网盘",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_netdisk",
        "odds_profile": "regular",
        "straight_wraparound": False,
        "special_refund_sum_values": [],
        "refund_policy_by_metric": {"big_small": "none", "odd_even": "none", "combo": "none", "number": "none"},
        "metric_settlement": {"big_small": _NONE_METRIC, "odd_even": _NONE_METRIC, "combo": _NONE_METRIC, "number": _NONE_METRIC},
    },
    "pc28_netdisk_abc": {
        "id": "pc28_netdisk_abc",
        "name": "OK游戏网盘 ABC",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_netdisk",
        "odds_profile": "abc",
        "straight_wraparound": False,
        "special_refund_sum_values": [],
        "refund_policy_by_metric": {"big_small": "none", "odd_even": "none", "combo": "none", "number": "none"},
        "metric_settlement": {"big_small": _NONE_METRIC, "odd_even": _NONE_METRIC, "combo": _NONE_METRIC, "number": _NONE_METRIC},
    },
    "pc28_high_regular": {
        "id": "pc28_high_regular",
        "name": "OK游戏高赔常规",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_high",
        "odds_profile": "regular",
        "straight_wraparound": True,
        "special_refund_sum_values": [13, 14],
        "refund_policy_by_metric": {"big_small": "special_on_hit", "odd_even": "special_on_hit", "combo": "special_on_hit", "number": "none"},
        "metric_settlement": {
            "big_small": _HIGH_SPECIAL_ON_HIT,
            "odd_even": _HIGH_SPECIAL_ON_HIT,
            "combo": _HIGH_SPECIAL_ON_HIT,
            "number": _NONE_METRIC,
        },
    },
    "pc28_high_abc": {
        "id": "pc28_high_abc",
        "name": "OK游戏高赔 ABC",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_high",
        "odds_profile": "abc",
        "straight_wraparound": True,
        "special_refund_sum_values": [13, 14],
        "refund_policy_by_metric": {"big_small": "special_on_hit", "odd_even": "special_on_hit", "combo": "special_on_hit", "number": "none"},
        "metric_settlement": {
            "big_small": _HIGH_SPECIAL_ON_HIT,
            "odd_even": _HIGH_SPECIAL_ON_HIT,
            "combo": _HIGH_SPECIAL_ON_HIT,
            "number": _NONE_METRIC,
        },
    },
    "pc28_fullpay_netdisk_regular": {
        "id": "pc28_fullpay_netdisk_regular",
        "name": "彩28网盘",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_fullpay_netdisk",
        "odds_profile": "regular",
        "straight_wraparound": False,
        "special_refund_sum_values": [0, 27],
        "refund_policy_by_metric": {"big_small": "special_sum_on_hit", "odd_even": "special_sum_on_hit", "combo": "none", "number": "none"},
        "metric_settlement": {
            "big_small": _ON_HIT_0_27,
            "odd_even": _ON_HIT_0_27,
            "combo": _NONE_METRIC,
            "number": _NONE_METRIC,
        },
    },
    "pc28_fullpay_2_0_regular": {
        "id": "pc28_fullpay_2_0_regular",
        "name": "彩28满赔2.0",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_fullpay_2_0",
        "odds_profile": "regular",
        "straight_wraparound": False,
        "special_refund_sum_values": [13, 14],
        "refund_policy_by_metric": {
            "big_small": "special_sum_on_hit",
            "odd_even": "special_sum_on_hit",
            "combo": "special_sum_on_hit",
            "number": "none",
        },
        "metric_settlement": {
            "big_small": _FULLPAY_2_0_BIG_SMALL,
            "odd_even": _FULLPAY_2_0_BIG_SMALL,
            "combo": _FULLPAY_2_0_COMBO,
            "number": _NONE_METRIC,
        },
    },
    "pc28_fullpay_2_8_regular": {
        "id": "pc28_fullpay_2_8_regular",
        "name": "彩28满赔2.8",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_fullpay_2_8",
        "odds_profile": "regular",
        "straight_wraparound": True,
        "special_refund_sum_values": [13, 14],
        "refund_policy_by_metric": {"big_small": "special_on_hit", "odd_even": "special_on_hit", "combo": "special_on_hit", "number": "none"},
        "metric_settlement": {
            "big_small": _FULLPAY_2_8,
            "odd_even": _FULLPAY_2_8,
            "combo": _FULLPAY_2_8,
            "number": _NONE_METRIC,
        },
    },
    "pc28_fullpay_3_2_regular": {
        "id": "pc28_fullpay_3_2_regular",
        "name": "彩28满赔3.2",
        "lottery_type": "pc28",
        "profit_rule_id": "pc28_fullpay_3_2",
        "odds_profile": "regular",
        "straight_wraparound": True,
        "special_refund_sum_values": [13, 14],
        "refund_policy_by_metric": {
            "big_small": "special_sum_or_edge_on_hit",
            "odd_even": "special_sum_or_edge_on_hit",
            "combo": "special_sum_or_edge_on_hit",
            "number": "none",
        },
        "metric_settlement": {
            "big_small": _FULLPAY_3_2,
            "odd_even": _FULLPAY_3_2,
            "combo": _FULLPAY_3_2,
            "number": _NONE_METRIC,
        },
    },
}

ALLOWED_SETTLEMENT_RULE_IDS = set(SETTLEMENT_RULE_CATALOG.keys())

LEGACY_TO_SETTLEMENT_RULE_ID = {
    ("pc28_netdisk", "regular"): "pc28_netdisk_regular",
    ("pc28_netdisk", "abc"): "pc28_netdisk_abc",
    ("pc28_high", "regular"): "pc28_high_regular",
    ("pc28_high", "abc"): "pc28_high_abc",
}


def _enrich_settlement_catalog() -> None:
    for rule in SETTLEMENT_RULE_CATALOG.values():
        profit_rule_id = str(rule.get("profit_rule_id") or "")
        odds_profile = str(rule.get("odds_profile") or "regular")
        rule["implemented_odds"] = catalog_implemented_odds(profit_rule_id, odds_profile)
        wraparound = bool(rule.get("straight_wraparound", True))
        rule["straight_wraparound"] = wraparound
        rule["wraparound_straight"] = wraparound
        reference = HOUSE_REFERENCE_ODDS.get(profit_rule_id)
        if reference:
            house_odds = dict(reference)
            house_odds["number"] = dict(HOUSE_NUMBER_ODDS)
            rule["house_odds"] = house_odds


_enrich_settlement_catalog()


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
    odds_overrides: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    signal_payload = signal if isinstance(signal, dict) else {}
    normalized_rule_id = normalize_settlement_rule_id(settlement_rule_id, allow_empty=True)
    rule = copy.deepcopy(get_settlement_rule(normalized_rule_id)) if normalized_rule_id else None
    implemented_odds = copy.deepcopy((rule or {}).get("implemented_odds") or {})
    if not implemented_odds and rule:
        implemented_odds = catalog_implemented_odds(rule.get("profit_rule_id"), rule.get("odds_profile"))
    normalized_overrides = drop_matching_catalog_odds(coerce_odds_overrides(odds_overrides), implemented_odds)
    resolved_odds = merge_implemented_odds(implemented_odds, normalized_overrides) if implemented_odds else {}
    return {
        "rule_source": str(rule_source or ""),
        "settlement_rule_id": normalized_rule_id,
        "fallback_profit_ratio": round(float(fallback_profit_ratio or 1.0), 4),
        "resolved_from": str(resolved_from or ""),
        "odds_overrides": normalized_overrides,
        "rule": rule,
        "implemented_odds": implemented_odds,
        "resolved_odds": resolved_odds,
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


def _is_pc28_straight(triplet: Optional[tuple[int, int, int]], *, wraparound: bool = True) -> bool:
    # 三球在 0~9 的环上首尾相接也算顺子：890（8,9,0）与 190（9,0,1）。
    if not triplet or len(set(triplet)) != 3:
        return False
    ordered = sorted(triplet)
    if ordered[0] + 1 == ordered[1] and ordered[1] + 1 == ordered[2]:
        return True
    if not wraparound:
        return False
    return set(ordered) in ({0, 8, 9}, {0, 1, 9})


def _has_abc_zero_or_nine(triplet: Optional[tuple[int, int, int]]) -> bool:
    return bool(triplet) and any(int(item) in {0, 9} for item in triplet)


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


def _rule_straight_wraparound(rule: Optional[Dict[str, Any]]) -> bool:
    if not rule:
        return True
    if "wraparound_straight" in rule:
        return bool(rule.get("wraparound_straight"))
    if "straight_wraparound" in rule:
        return bool(rule.get("straight_wraparound"))
    return True


def derive_pc28_draw_snapshot(draw_context: Any, *, straight_wraparound: bool = True) -> Dict[str, Any]:
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
    if triplet is not None:
        is_straight = _is_pc28_straight(triplet, wraparound=straight_wraparound)
        has_abc_zero_or_nine = _has_abc_zero_or_nine(triplet)
    else:
        is_straight = bool(payload.get("is_straight")) if "is_straight" in payload else False
        has_abc_zero_or_nine = bool(payload.get("has_abc_zero_or_nine")) if "has_abc_zero_or_nine" in payload else False
    is_pair = bool(payload.get("is_pair")) if "is_pair" in payload else _is_pc28_pair(triplet)
    is_extreme_sum = bool(payload.get("is_extreme_sum")) if "is_extreme_sum" in payload else (sum_value in {0, 27})
    is_wraparound_straight = False
    if triplet is not None:
        is_wraparound_straight = _is_pc28_straight(triplet, wraparound=True) and not _is_pc28_straight(triplet, wraparound=False)
    elif "is_wraparound_straight" in payload:
        is_wraparound_straight = bool(payload.get("is_wraparound_straight"))
    open_time = str(payload.get("open_time") or "").strip() or None
    return {
        "sum_value": int(sum_value) if sum_value is not None else None,
        "result_number": int(sum_value) if sum_value is not None else None,
        "triplet": list(triplet) if triplet else None,
        "big_small": big_small or None,
        "odd_even": odd_even or None,
        "combo": combo or None,
        "open_time": open_time,
        "is_extreme_sum": bool(is_extreme_sum),
        "has_ball_0_or_9": bool(has_abc_zero_or_nine),
        "is_wraparound_straight": bool(is_wraparound_straight),
        "special_flags": {
            "is_special_sum": bool(is_special_sum),
            "is_baozi": bool(is_baozi),
            "is_straight": bool(is_straight),
            "is_pair": bool(is_pair),
            "has_abc_zero_or_nine": bool(has_abc_zero_or_nine),
            "is_extreme_sum": bool(is_extreme_sum),
            "is_wraparound_straight": bool(is_wraparound_straight),
        },
    }


def _sum_refund_reason(values: Any) -> str:
    numbers = []
    for item in values or []:
        parsed = _parse_optional_int(item)
        if parsed is not None:
            numbers.append(str(parsed))
    if not numbers:
        return "退本金"
    return "/".join(numbers) + " 退本金"


def _metric_spec_for_rule(rule: Dict[str, Any], metric: str) -> Dict[str, Any]:
    specs = _to_object(rule.get("metric_settlement"))
    spec = specs.get(metric)
    if isinstance(spec, dict) and spec:
        return spec
    policy = str((_to_object(rule.get("refund_policy_by_metric"))).get(metric) or "none")
    if policy == "special_on_hit":
        return _HIGH_SPECIAL_ON_HIT
    return _NONE_METRIC


def _resolve_refund_aware_result(
    *,
    hit: bool,
    sum_value: Optional[int],
    special_flags: Dict[str, Any],
    spec: Dict[str, Any],
) -> tuple[str, Optional[str]]:
    if not hit:
        return ("miss", None)
    on_hit_sums = {
        int(item)
        for item in (spec.get("on_hit_refund_sum_values") or [])
        if _parse_optional_int(item) is not None
    }
    if sum_value is not None and int(sum_value) in on_hit_sums:
        return ("refund", _sum_refund_reason(sorted(on_hit_sums)))
    if spec.get("on_hit_refund_baozi") and special_flags.get("is_baozi"):
        return ("refund", "豹子退本金")
    if spec.get("on_hit_refund_straight") and special_flags.get("is_straight"):
        return ("refund", "顺子退本金")
    if spec.get("on_hit_refund_pair") and special_flags.get("is_pair"):
        return ("refund", "对子退本金")
    if spec.get("on_hit_refund_abc_zero_or_nine") and special_flags.get("has_abc_zero_or_nine"):
        return ("refund", "ABC 含 0/9 退本金")
    return ("hit", None)


def resolve_pc28_result_for_signal(
    *,
    signal: Dict[str, Any],
    settlement_rule_id: Any,
    draw_context: Any,
    rule: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    frozen_rule = rule if isinstance(rule, dict) and rule else None
    catalog_rule = get_settlement_rule(settlement_rule_id)
    resolved_rule = frozen_rule or catalog_rule or {
        "id": "",
        "lottery_type": "pc28",
        "refund_policy_by_metric": {"big_small": "none", "odd_even": "none", "combo": "none", "number": "none"},
        "metric_settlement": {},
        "straight_wraparound": True,
        "wraparound_straight": True,
    }
    snapshot = derive_pc28_draw_snapshot(
        draw_context,
        straight_wraparound=_rule_straight_wraparound(resolved_rule),
    )
    metric = _resolve_metric_from_signal(signal)
    if metric is None:
        raise ValueError("当前信号玩法暂不支持自动结算")
    predicted_value = str(signal.get("bet_value") or "").strip()
    actual_value = snapshot.get(metric)
    if actual_value in {None, ""}:
        raise ValueError("draw_context 缺少可用于自动结算的开奖结果")
    actual_value = str(actual_value)
    spec = _metric_spec_for_rule(resolved_rule, metric)
    result_type, refund_reason = _resolve_refund_aware_result(
        hit=predicted_value == actual_value,
        sum_value=snapshot.get("sum_value"),
        special_flags=_to_object(snapshot.get("special_flags")),
        spec=spec,
    )
    return {
        "lottery_type": "pc28",
        "metric": metric,
        "predicted_value": predicted_value,
        "actual_value": actual_value,
        "result_type": result_type,
        "refund_reason": refund_reason,
        "settlement_rule_id": str(resolved_rule.get("id") or settlement_rule_id or ""),
        "draw_snapshot": snapshot,
    }


def _frozen_resolved_odds_map(snapshot: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    resolved = snapshot.get("resolved_odds")
    if not isinstance(resolved, dict) or not resolved:
        return None
    return {key: value for key, value in resolved.items() if key != "adjustment"}


def resolve_pc28_settled_hit_detail(
    *,
    stake_amount: float,
    signal: Optional[Dict[str, Any]],
    settlement_rule_id: Any,
    odds_overrides: Optional[Dict[str, Any]] = None,
    draw_snapshot: Optional[Dict[str, Any]] = None,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    payload = signal if isinstance(signal, dict) else {}
    frozen_snapshot = snapshot if isinstance(snapshot, dict) else {}
    frozen_rule = frozen_snapshot.get("rule") if isinstance(frozen_snapshot.get("rule"), dict) else None
    rule = frozen_rule or get_settlement_rule(settlement_rule_id) or {}
    if not rule:
        return None
    metric = _resolve_metric_from_signal(payload)
    spec = _metric_spec_for_rule(rule, metric or "")
    draw = draw_snapshot if isinstance(draw_snapshot, dict) else {}
    resolved_odds = _frozen_resolved_odds_map(frozen_snapshot)
    implemented_odds = (
        resolved_odds
        if resolved_odds is not None
        else (
            frozen_snapshot.get("implemented_odds")
            if isinstance(frozen_snapshot.get("implemented_odds"), dict)
            else rule.get("implemented_odds")
        )
    )
    overlay = None if resolved_odds is not None else (
        odds_overrides if odds_overrides is not None else frozen_snapshot.get("odds_overrides")
    )
    detail = resolve_pc28_odds_detail(
        bet_type=str(payload.get("bet_type") or ""),
        bet_value=str(payload.get("bet_value") or ""),
        profit_rule_id=str(rule.get("profit_rule_id") or ""),
        odds_profile=str(rule.get("odds_profile") or ""),
        odds_overrides=overlay,
        implemented_odds=implemented_odds if isinstance(implemented_odds, dict) else None,
        odds_tiers=spec.get("on_hit_odds_tiers"),
        sum_value=draw.get("sum_value"),
        stake_amount=float(stake_amount or 0),
    )
    if not detail:
        return None
    profit = round(float(stake_amount or 0) * max(0.0, float(detail["odds"]) - 1.0), 2)
    detail["profit"] = profit
    return detail


def resolve_pc28_settled_hit_profit(
    *,
    stake_amount: float,
    signal: Optional[Dict[str, Any]],
    settlement_rule_id: Any,
    odds_overrides: Optional[Dict[str, Any]] = None,
    draw_snapshot: Optional[Dict[str, Any]] = None,
    snapshot: Optional[Dict[str, Any]] = None,
) -> Optional[float]:
    detail = resolve_pc28_settled_hit_detail(
        stake_amount=stake_amount,
        signal=signal,
        settlement_rule_id=settlement_rule_id,
        odds_overrides=odds_overrides,
        draw_snapshot=draw_snapshot,
        snapshot=snapshot,
    )
    if not detail:
        return None
    return float(detail["profit"])
