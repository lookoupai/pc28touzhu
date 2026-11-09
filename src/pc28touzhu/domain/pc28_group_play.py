"""双组、杀组的下注集合；金额始终按独立 combo 子注核算。"""
from __future__ import annotations

from decimal import Decimal, InvalidOperation
from itertools import combinations
from typing import Any, Optional


COMBO_VALUES = ("大单", "大双", "小单", "小双")
GROUP_PLAY_TYPES = ("double_group", "kill_group")
DOUBLE_GROUP_VALUES = tuple("+".join(pair) for pair in combinations(COMBO_VALUES, 2))
OPPOSITE_COMBOS = {"大单": "小双", "小双": "大单", "大双": "小单", "小单": "大双"}
LEGACY_DOUBLE_GROUPS = {"单组": "大单+小双", "双组": "大双+小单"}
GROUP_DERIVATION = "opposite_combo_v1"


def _combo_list(values: Any, count: int) -> list[str]:
    if not isinstance(values, (list, tuple)) or len(values) != count:
        raise ValueError("玩法必须包含 %s 个不同的组合" % count)
    if any(not isinstance(value, str) or value.strip() not in COMBO_VALUES for value in values):
        raise ValueError("组合仅支持大单、大双、小单、小双")
    normalized = [value.strip() for value in values]
    if len(set(normalized)) != count:
        raise ValueError("同一组内不能重复下注相同组合")
    return [combo for combo in COMBO_VALUES if combo in normalized]


def normalize_group_selection(bet_type: str, bet_value: Any, payload: Optional[dict] = None) -> dict:
    """在边界校验集合与排除项，旧组别仅作为输入别名。"""
    metric = str(bet_type or "").strip().lower()
    value = str(bet_value or "").strip()
    metadata = payload if isinstance(payload, dict) else {}
    if metric == "double_group":
        value = LEGACY_DOUBLE_GROUPS.get(value, value)
        for separator in ("＋", "/", "、", ",", "，"):
            value = value.replace(separator, "+")
        selected = _combo_list(value.split("+"), 2)
        canonical_value = "+".join(selected)
        excluded = None
    elif metric == "kill_group":
        if value.startswith("杀"):
            value = value[1:].strip()
        if value not in COMBO_VALUES:
            raise ValueError("杀组必须指定排除的大单、大双、小单或小双")
        excluded = value
        selected = [combo for combo in COMBO_VALUES if combo != excluded]
        canonical_value = excluded
    else:
        raise ValueError("当前玩法不是双组或杀组")

    if "selected_combos" in metadata and _combo_list(metadata["selected_combos"], len(selected)) != selected:
        raise ValueError("selected_combos 与玩法方向不一致")
    if metadata.get("excluded_combo") not in (None, "", excluded):
        raise ValueError("excluded_combo 与玩法方向不一致")
    if metric == "double_group" and metadata.get("excluded_combo") not in (None, ""):
        raise ValueError("双组不能指定单个排除项")
    selection = {
        "bet_type": metric,
        "bet_value": canonical_value,
        "selected_combos": selected,
        "excluded_combo": excluded,
        "unit_count": len(selected),
    }
    derivation = str(metadata.get("group_derivation") or "").strip()
    origin = str(metadata.get("origin_combo") or "").strip()
    if derivation:
        if derivation != GROUP_DERIVATION or origin not in COMBO_VALUES:
            raise ValueError("派生玩法必须携带有效的原始组合及派生版本")
        expected = {origin, OPPOSITE_COMBOS[origin]} if metric == "double_group" else set(COMBO_VALUES) - {OPPOSITE_COMBOS[origin]}
        if set(selected) != expected:
            raise ValueError("下注集合与原始组合的派生规则不一致")
        selection.update(group_derivation=derivation, origin_combo=origin)
    return selection


def group_selection_for_signal(signal: Optional[dict]) -> Optional[dict]:
    data = signal if isinstance(signal, dict) else {}
    metric = str(data.get("bet_type") or "").strip().lower()
    if metric not in GROUP_PLAY_TYPES:
        return None
    return normalize_group_selection(metric, data.get("bet_value"), data.get("normalized_payload"))


def derive_group_selections(combo: str) -> list[dict]:
    if combo not in COMBO_VALUES:
        raise ValueError("无法从无效组合派生双组或杀组")
    metadata = {"group_derivation": GROUP_DERIVATION, "origin_combo": combo}
    opposite = OPPOSITE_COMBOS[combo]
    return [
        normalize_group_selection("double_group", combo + "+" + opposite, metadata),
        normalize_group_selection("kill_group", opposite, metadata),
    ]


def group_stake_legs(selection: dict, stake_amount: Any) -> list[dict]:
    """总额须能精确拆成等额、至少一分的子注，禁止静默吞掉余数。"""
    try:
        total = Decimal(str(stake_amount))
        if not total.is_finite() or total <= 0 or total != total.quantize(Decimal("0.01")):
            raise ValueError("整组金额必须为正数且最多两位小数")
        cents = total * 100 / len(selection["selected_combos"])
        if cents < 1 or cents != cents.to_integral_value():
            raise ValueError("整组金额必须能精确等分到每个组合，且每注至少 0.01")
    except (InvalidOperation, TypeError, ArithmeticError) as exc:
        raise ValueError("整组金额无效") from exc
    amount = float(cents / 100)
    return [{"bet_type": "combo", "bet_value": combo, "stake_amount": amount} for combo in selection["selected_combos"]]
