"""PC28 盈亏规则：内置目录 + 订阅/路由赔率覆盖。"""
from __future__ import annotations

from typing import Any, Dict, Optional


DEFAULT_PROFIT_RULE_ID = "pc28_netdisk"
DEFAULT_ODDS_PROFILE = "regular"
ALLOWED_PROFIT_RULES = {
    "pc28_netdisk",
    "pc28_high",
    "pc28_fullpay_netdisk",
    "pc28_fullpay_2_0",
    "pc28_fullpay_2_8",
    "pc28_fullpay_3_2",
}
ALLOWED_ODDS_PROFILES = {"regular", "abc"}
COMBO_GROUP_SMALL_ODD_BIG_EVEN = "小单 / 大双"
COMBO_GROUP_BIG_ODD_SMALL_EVEN = "大单 / 小双"
COMBO_VALUES_SMALL_ODD_BIG_EVEN = ("小单", "大双")
COMBO_VALUES_BIG_ODD_SMALL_EVEN = ("大单", "小双")
COMBO_VALUES = COMBO_VALUES_SMALL_ODD_BIG_EVEN + COMBO_VALUES_BIG_ODD_SMALL_EVEN
MAX_CUSTOM_ODDS = 1000.0


def _regular_play_odds(big_small_odd_even: float, small_odd_big_even: float, big_odd_small_even: float) -> Dict[str, Any]:
    return {
        "big_small": {"regular": {"odds": big_small_odd_even}},
        "odd_even": {"regular": {"odds": big_small_odd_even}},
        "combo": {
            "regular": {
                "group_odds": {
                    COMBO_GROUP_SMALL_ODD_BIG_EVEN: small_odd_big_even,
                    COMBO_GROUP_BIG_ODD_SMALL_EVEN: big_odd_small_even,
                }
            }
        },
    }


PC28_PROFIT_RULES = {
    "pc28_netdisk": {
        "big_small": {
            "regular": {"odds": 1.98},
            "abc": {"odds": 1.98},
        },
        "odd_even": {
            "regular": {"odds": 1.98},
            "abc": {"odds": 1.98},
        },
        "combo": {
            "regular": {
                "group_odds": {
                    COMBO_GROUP_SMALL_ODD_BIG_EVEN: 3.6,
                    COMBO_GROUP_BIG_ODD_SMALL_EVEN: 4.2,
                }
            },
            "abc": {
                "group_odds": {
                    COMBO_GROUP_SMALL_ODD_BIG_EVEN: 4.8,
                    COMBO_GROUP_BIG_ODD_SMALL_EVEN: 3.1,
                }
            },
        },
    },
    "pc28_high": {
        "big_small": {
            "regular": {"odds": 2.846},
            "abc": {"odds": 1.98},
        },
        "odd_even": {
            "regular": {"odds": 2.846},
            "abc": {"odds": 1.98},
        },
        "combo": {
            "regular": {
                "group_odds": {
                    COMBO_GROUP_SMALL_ODD_BIG_EVEN: 6.78,
                    COMBO_GROUP_BIG_ODD_SMALL_EVEN: 6.33,
                }
            },
            "abc": {
                "group_odds": {
                    COMBO_GROUP_SMALL_ODD_BIG_EVEN: 4.9,
                    COMBO_GROUP_BIG_ODD_SMALL_EVEN: 3.1,
                }
            },
        },
    },
    "pc28_fullpay_netdisk": _regular_play_odds(1.99, 3.71, 4.32),
    "pc28_fullpay_2_0": _regular_play_odds(2.0, 4.76, 4.32),
    "pc28_fullpay_2_8": _regular_play_odds(2.84, 6.79, 6.33),
    "pc28_fullpay_3_2": _regular_play_odds(3.2, 7.0, 6.5),
}

HOUSE_REFERENCE_ODDS = {
    "pc28_fullpay_netdisk": {
        "extreme": 17.8,
        "pair": 3.69,
        "straight": 20.6,
        "baozi": 99,
        "dragon_tiger": 2.2,
        "tie": 9.98,
        "edge": 2.269,
        "big_small_edge": 4.538,
        "middle": 1.783,
        "abc_number": 9.98,
        "abc_big_small_odd_even": 1.99,
        "abc_big_even_small_odd": 4.98,
        "abc_small_even_big_odd": 3.32,
        "sanjun": (3.3, 6.6, 15),
    },
    "pc28_fullpay_2_0": {
        "extreme": 17.8,
        "pair": 3.69,
        "straight": 20.6,
        "baozi": 99,
        "dragon_tiger": 2.2,
        "tie": 9.98,
        "edge": 2.269,
        "big_small_edge": 4.538,
        "middle": 1.783,
        "abc_number": 9.98,
        "abc_big_small_odd_even": 1.99,
        "abc_big_even_small_odd": 4.98,
        "abc_small_even_big_odd": 3.32,
        "sanjun": (3.3, 6.6, 15),
    },
    "pc28_fullpay_2_8": {
        "extreme": 17.8,
        "pair": 3.69,
        "straight": 16,
        "baozi": 99,
        "dragon_tiger": 2.2,
        "tie": 9.98,
        "edge": 2.269,
        "big_small_edge": 4.538,
        "middle": 1.783,
        "abc_number": 9.96,
        "abc_big_small_odd_even": 1.99,
        "abc_big_even_small_odd": 4.98,
        "abc_small_even_big_odd": 3.32,
        "sanjun": (3.3, 6.6, 15),
    },
    "pc28_fullpay_3_2": {
        "extreme": 17.8,
        "pair": 3.69,
        "straight": 16,
        "baozi": 99,
        "dragon_tiger": 2.2,
        "tie": 9.98,
        "edge": 2.269,
        "big_small_edge": 4.538,
        "middle": 1.783,
        "abc_number": 9.98,
        "abc_big_small_odd_even": 1.99,
        "abc_big_even_small_odd": 4.98,
        "abc_small_even_big_odd": 3.32,
        "sanjun": (3.3, 6.6, 15),
    },
}

HOUSE_NUMBER_ODDS = {
    0: 1000,
    27: 1000,
    1: 320,
    26: 320,
    2: 160,
    25: 160,
    3: 97,
    24: 97,
    4: 64.5,
    23: 64.5,
    5: 46,
    22: 46,
    6: 35,
    21: 35,
    7: 27.2,
    20: 27.2,
    8: 22,
    19: 22,
    9: 18,
    18: 18,
    10: 15.8,
    17: 15.8,
    11: 14.4,
    16: 14.4,
    12: 13.6,
    15: 13.6,
    13: 13.3,
    14: 13.3,
}


def normalize_profit_rule_id(value: Optional[str]) -> str:
    text = str(value or "").strip().lower()
    if text in ALLOWED_PROFIT_RULES:
        return text
    return DEFAULT_PROFIT_RULE_ID


def normalize_odds_profile(value: Optional[str]) -> str:
    text = str(value or "").strip().lower()
    if text in ALLOWED_ODDS_PROFILES:
        return text
    return DEFAULT_ODDS_PROFILE


def _parse_odds_number(raw: Any, *, strict: bool, field_name: str) -> Optional[float]:
    if raw in {None, ""}:
        return None
    try:
        odds = round(float(raw), 4)
    except (TypeError, ValueError):
        if strict:
            raise ValueError("赔率 %s 必须为数字" % field_name)
        return None
    if odds <= 1:
        if strict:
            raise ValueError("赔率 %s 必须大于 1" % field_name)
        return None
    if odds > MAX_CUSTOM_ODDS:
        if strict:
            raise ValueError("赔率 %s 超出允许范围" % field_name)
        return None
    return odds


def _odds_equal(left: Any, right: Any) -> bool:
    try:
        return round(float(left), 4) == round(float(right), 4)
    except (TypeError, ValueError):
        return False


def _extract_combo_overrides(payload: Dict[str, Any], *, strict: bool) -> Dict[str, float]:
    combo: Dict[str, float] = {}
    nested = payload.get("combo")
    if isinstance(nested, dict):
        for name in COMBO_VALUES:
            parsed = _parse_odds_number(nested.get(name), strict=strict, field_name="combo.%s" % name)
            if parsed is not None:
                combo[name] = parsed
    for flat_key, names in (
        ("combo_small_odd_big_even", COMBO_VALUES_SMALL_ODD_BIG_EVEN),
        ("combo_big_odd_small_even", COMBO_VALUES_BIG_ODD_SMALL_EVEN),
    ):
        parsed = _parse_odds_number(payload.get(flat_key), strict=strict, field_name=flat_key)
        if parsed is None:
            continue
        for name in names:
            combo.setdefault(name, parsed)
    return combo


def _normalize_odds_overrides_payload(value: Any, *, strict: bool) -> Dict[str, Any]:
    payload = value if isinstance(value, dict) else {}
    normalized: Dict[str, Any] = {}
    for key in ("big_small", "odd_even"):
        parsed = _parse_odds_number(payload.get(key), strict=strict, field_name=key)
        if parsed is not None:
            normalized[key] = parsed
    combo = _extract_combo_overrides(payload, strict=strict)
    if combo:
        normalized["combo"] = combo
    return normalized


def coerce_odds_overrides(value: Any) -> Dict[str, Any]:
    return _normalize_odds_overrides_payload(value, strict=False)


def normalize_odds_overrides(value: Any) -> Dict[str, Any]:
    return _normalize_odds_overrides_payload(value, strict=True)


def drop_matching_catalog_odds(overrides: Any, implemented_odds: Any) -> Dict[str, Any]:
    payload = coerce_odds_overrides(overrides)
    implemented = implemented_odds if isinstance(implemented_odds, dict) else {}
    compacted: Dict[str, Any] = {}
    for key in ("big_small", "odd_even"):
        if key in payload and not _odds_equal(payload[key], implemented.get(key)):
            compacted[key] = payload[key]
    combo_over = payload.get("combo") if isinstance(payload.get("combo"), dict) else {}
    combo_impl = implemented.get("combo") if isinstance(implemented.get("combo"), dict) else {}
    combo: Dict[str, float] = {}
    for name, odds in combo_over.items():
        if not _odds_equal(odds, combo_impl.get(name)):
            combo[name] = odds
    if combo:
        compacted["combo"] = combo
    return compacted


def merge_implemented_odds(implemented_odds: Any, overrides: Any) -> Dict[str, Any]:
    implemented = implemented_odds if isinstance(implemented_odds, dict) else {}
    merged: Dict[str, Any] = {
        "big_small": implemented.get("big_small"),
        "odd_even": implemented.get("odd_even", implemented.get("big_small")),
        "combo": dict(implemented.get("combo") or {}) if isinstance(implemented.get("combo"), dict) else {},
    }
    overlay = coerce_odds_overrides(overrides)
    if "big_small" in overlay:
        merged["big_small"] = overlay["big_small"]
        if "odd_even" not in overlay:
            merged["odd_even"] = overlay["big_small"]
    if "odd_even" in overlay:
        merged["odd_even"] = overlay["odd_even"]
        if "big_small" not in overlay:
            merged["big_small"] = overlay["odd_even"]
    combo_over = overlay.get("combo") if isinstance(overlay.get("combo"), dict) else {}
    merged["combo"].update(combo_over)
    return merged


def _resolve_metric_from_bet(bet_type: str, bet_value: str) -> Optional[str]:
    normalized_type = str(bet_type or "").strip().lower()
    normalized_value = str(bet_value or "").strip()
    if normalized_type == "combo" and normalized_value in {"大单", "大双", "小单", "小双"}:
        return "combo"
    if normalized_type == "odd_even" and normalized_value in {"单", "双"}:
        return "odd_even"
    if normalized_type == "big_small":
        if normalized_value in {"大", "小"}:
            return "big_small"
        if normalized_value in {"单", "双"}:
            return "odd_even"
    return None


def _resolve_combo_group(bet_value: str) -> Optional[str]:
    if bet_value in {"小单", "大双"}:
        return COMBO_GROUP_SMALL_ODD_BIG_EVEN
    if bet_value in {"大单", "小双"}:
        return COMBO_GROUP_BIG_ODD_SMALL_EVEN
    return None


def _metric_profile(profit_rule_id: str, metric: str, odds_profile: str) -> Dict[str, Any]:
    rule_id = normalize_profit_rule_id(profit_rule_id)
    profile_key = normalize_odds_profile(odds_profile)
    metric_profiles = ((PC28_PROFIT_RULES.get(rule_id) or {}).get(metric) or {})
    profile = metric_profiles.get(profile_key)
    if isinstance(profile, dict) and profile:
        return profile
    fallback = metric_profiles.get(DEFAULT_ODDS_PROFILE)
    return fallback if isinstance(fallback, dict) else {}


def catalog_odds_for_bet(
    *,
    bet_type: str,
    bet_value: str,
    profit_rule_id: Optional[str] = None,
    odds_profile: Optional[str] = None,
) -> Optional[float]:
    metric = _resolve_metric_from_bet(bet_type, bet_value)
    if metric is None:
        return None
    metric_profile = _metric_profile(str(profit_rule_id or ""), metric, str(odds_profile or ""))
    if metric == "combo":
        combo_group = _resolve_combo_group(str(bet_value or "").strip())
        if not combo_group:
            return None
        odds = float((metric_profile.get("group_odds") or {}).get(combo_group) or 0)
    else:
        odds = float(metric_profile.get("odds") or 0)
    if odds <= 0:
        return None
    return odds


def catalog_implemented_odds(profit_rule_id: Optional[str], odds_profile: Optional[str] = None) -> Dict[str, Any]:
    profile_key = normalize_odds_profile(odds_profile)
    return {
        "big_small": float(
            catalog_odds_for_bet(
                bet_type="big_small",
                bet_value="大",
                profit_rule_id=profit_rule_id,
                odds_profile=profile_key,
            )
            or 0
        ),
        "odd_even": float(
            catalog_odds_for_bet(
                bet_type="odd_even",
                bet_value="单",
                profit_rule_id=profit_rule_id,
                odds_profile=profile_key,
            )
            or 0
        ),
        "combo": {
            "小单": float(
                catalog_odds_for_bet(
                    bet_type="combo",
                    bet_value="小单",
                    profit_rule_id=profit_rule_id,
                    odds_profile=profile_key,
                )
                or 0
            ),
            "大双": float(
                catalog_odds_for_bet(
                    bet_type="combo",
                    bet_value="大双",
                    profit_rule_id=profit_rule_id,
                    odds_profile=profile_key,
                )
                or 0
            ),
            "大单": float(
                catalog_odds_for_bet(
                    bet_type="combo",
                    bet_value="大单",
                    profit_rule_id=profit_rule_id,
                    odds_profile=profile_key,
                )
                or 0
            ),
            "小双": float(
                catalog_odds_for_bet(
                    bet_type="combo",
                    bet_value="小双",
                    profit_rule_id=profit_rule_id,
                    odds_profile=profile_key,
                )
                or 0
            ),
        },
    }


def _lookup_odds_map(odds_map: Any, *, metric: str, bet_value: str) -> Optional[float]:
    payload = odds_map if isinstance(odds_map, dict) else {}
    if metric == "combo":
        combo = payload.get("combo") if isinstance(payload.get("combo"), dict) else {}
        raw = combo.get(bet_value)
        if raw in {None, ""}:
            if bet_value in COMBO_VALUES_SMALL_ODD_BIG_EVEN:
                raw = payload.get("combo_small_odd_big_even")
            elif bet_value in COMBO_VALUES_BIG_ODD_SMALL_EVEN:
                raw = payload.get("combo_big_odd_small_even")
        try:
            odds = float(raw or 0)
        except (TypeError, ValueError):
            return None
        return odds if odds > 0 else None
    if metric == "odd_even":
        raw = payload.get("odd_even", payload.get("big_small"))
    else:
        raw = payload.get("big_small", payload.get("odd_even"))
    try:
        odds = float(raw or 0)
    except (TypeError, ValueError):
        return None
    return odds if odds > 0 else None


def _apply_odds_tiers(
    odds: float,
    *,
    odds_tiers: Optional[list],
    sum_value: Optional[int],
    stake_amount: float,
) -> float:
    if not odds_tiers or sum_value is None:
        return odds
    try:
        current_sum = int(sum_value)
    except (TypeError, ValueError):
        return odds
    stake = float(stake_amount or 0)
    for tier in odds_tiers:
        if not isinstance(tier, dict):
            continue
        sums = {int(item) for item in (tier.get("sum_values") or []) if str(item).strip() != ""}
        if current_sum not in sums:
            continue
        stake_gt = tier.get("stake_gt")
        if stake_gt not in {None, ""} and stake <= float(stake_gt):
            continue
        stake_lte = tier.get("stake_lte")
        if stake_lte not in {None, ""} and stake > float(stake_lte):
            continue
        tier_odds = float(tier.get("odds") or 0)
        if tier_odds > 0:
            return tier_odds
    return odds


def resolve_pc28_odds_detail(
    *,
    bet_type: str,
    bet_value: str,
    profit_rule_id: Optional[str] = None,
    odds_profile: Optional[str] = None,
    odds_overrides: Optional[Dict[str, Any]] = None,
    implemented_odds: Optional[Dict[str, Any]] = None,
    odds_tiers: Optional[list] = None,
    sum_value: Optional[int] = None,
    stake_amount: float = 0,
) -> Optional[Dict[str, Any]]:
    metric = _resolve_metric_from_bet(bet_type, bet_value)
    if metric is None:
        return None
    catalog = _lookup_odds_map(implemented_odds, metric=metric, bet_value=str(bet_value or "").strip())
    if catalog is None:
        catalog = catalog_odds_for_bet(
            bet_type=bet_type,
            bet_value=bet_value,
            profit_rule_id=profit_rule_id,
            odds_profile=odds_profile,
        )
    overridden = _lookup_odds_map(coerce_odds_overrides(odds_overrides), metric=metric, bet_value=str(bet_value or "").strip())
    base = overridden if overridden is not None else catalog
    if base is None or base <= 0:
        return None
    adjusted = _apply_odds_tiers(
        float(base),
        odds_tiers=odds_tiers,
        sum_value=sum_value,
        stake_amount=stake_amount,
    )
    adjustment = None
    if round(float(adjusted), 4) != round(float(base), 4):
        adjustment = {
            "from": round(float(base), 4),
            "to": round(float(adjusted), 4),
            "reason": "hit_odds_adjustment",
            "sum_value": sum_value,
            "stake_amount": round(float(stake_amount or 0), 2),
        }
    return {
        "odds": round(float(adjusted), 4),
        "base_odds": round(float(base), 4),
        "catalog_odds": round(float(catalog), 4) if catalog else None,
        "override_applied": overridden is not None,
        "adjustment": adjustment,
        "metric": metric,
    }


def resolve_pc28_odds(
    *,
    bet_type: str,
    bet_value: str,
    profit_rule_id: Optional[str] = None,
    odds_profile: Optional[str] = None,
    odds_overrides: Optional[Dict[str, Any]] = None,
    implemented_odds: Optional[Dict[str, Any]] = None,
    odds_tiers: Optional[list] = None,
    sum_value: Optional[int] = None,
    stake_amount: float = 0,
) -> Optional[float]:
    detail = resolve_pc28_odds_detail(
        bet_type=bet_type,
        bet_value=bet_value,
        profit_rule_id=profit_rule_id,
        odds_profile=odds_profile,
        odds_overrides=odds_overrides,
        implemented_odds=implemented_odds,
        odds_tiers=odds_tiers,
        sum_value=sum_value,
        stake_amount=stake_amount,
    )
    if not detail:
        return None
    return float(detail["odds"])


def resolve_pc28_hit_profit(
    *,
    stake_amount: float,
    bet_type: str,
    bet_value: str,
    profit_rule_id: Optional[str] = None,
    odds_profile: Optional[str] = None,
    odds_overrides: Optional[Dict[str, Any]] = None,
    implemented_odds: Optional[Dict[str, Any]] = None,
    odds_tiers: Optional[list] = None,
    sum_value: Optional[int] = None,
) -> Optional[float]:
    odds = resolve_pc28_odds(
        bet_type=bet_type,
        bet_value=bet_value,
        profit_rule_id=profit_rule_id,
        odds_profile=odds_profile,
        odds_overrides=odds_overrides,
        implemented_odds=implemented_odds,
        odds_tiers=odds_tiers,
        sum_value=sum_value,
        stake_amount=stake_amount,
    )
    if odds is None or odds <= 0:
        return None
    return round(float(stake_amount) * max(0.0, odds - 1.0), 2)
