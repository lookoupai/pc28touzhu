"""PC28 跟单玩法筛选规则。"""
from __future__ import annotations

from typing import Iterable, Optional

from pc28touzhu.domain.pc28_group_play import (
    COMBO_VALUES, DOUBLE_GROUP_VALUES, GROUP_PLAY_TYPES, normalize_group_selection,
)
from pc28touzhu.domain.roll_play import SINGLE_PLAY, selected_play_signal


PLAY_FILTER_LABELS = {
    "big_small:大": "大",
    "big_small:小": "小",
    "odd_even:单": "单",
    "odd_even:双": "双",
    "combo:大单": "大单",
    "combo:大双": "大双",
    "combo:小单": "小单",
    "combo:小双": "小双",
}

PLAY_FILTER_LABELS.update({"double_group:" + value: value.replace("+", "＋") for value in DOUBLE_GROUP_VALUES})
PLAY_FILTER_LABELS.update({"kill_group:" + value: "杀" + value for value in COMBO_VALUES})
PLAY_FILTER_LABELS.update({'number:' + str(value): str(value) for value in range(28)})
PLAY_FILTER_KEYS_BY_METRIC = {
    metric: [key for key in PLAY_FILTER_LABELS if key.startswith(metric + ":")]
    for metric in ("big_small", "odd_even", "combo", "number", *GROUP_PLAY_TYPES)
}

ALLOWED_PLAY_FILTER_KEYS = tuple(PLAY_FILTER_LABELS.keys())


def normalize_play_filter_mode(value: Optional[str]) -> str:
    text = str(value or "").strip().lower()
    if text == "selected":
        return "selected"
    return "all"


def normalize_play_filter_keys(values: Optional[Iterable[str]]) -> list[str]:
    normalized: list[str] = []
    for value in values or []:
        text = str(value or "").strip()
        metric, separator, direction = text.partition(":")
        if separator and metric in GROUP_PLAY_TYPES:
            try:
                text = metric + ":" + normalize_group_selection(metric, direction)["bet_value"]
            except ValueError:
                continue
        if text in PLAY_FILTER_LABELS and text not in normalized:
            normalized.append(text)
    return normalized


def resolve_signal_play_filter_key(*, bet_type: str, bet_value: str) -> Optional[str]:
    normalized_type = str(bet_type or "").strip().lower()
    normalized_value = str(bet_value or "").strip()
    if normalized_type == 'number' and normalized_value.isdigit() and 0 <= int(normalized_value) <= 27:
        return 'number:' + str(int(normalized_value))
    if normalized_type in GROUP_PLAY_TYPES:
        try:
            return normalized_type + ":" + normalize_group_selection(normalized_type, normalized_value)["bet_value"]
        except ValueError:
            return None
    if normalized_type == "combo" and normalized_value in {"大单", "大双", "小单", "小双"}:
        return "combo:" + normalized_value
    if normalized_type == "big_small" and normalized_value in {"大", "小"}:
        return "big_small:" + normalized_value
    if normalized_type in {"big_small", "odd_even"} and normalized_value in {"单", "双"}:
        return "odd_even:" + normalized_value
    return None


def strategy_matches_signal(strategy: Optional[dict], signal: Optional[dict]) -> bool:
    payload = strategy if isinstance(strategy, dict) else {}
    staking = payload.get('staking_policy') or payload.get('win_streak_roll') or {}
    if staking.get('structure') == SINGLE_PLAY and staking.get('mode', 'win_streak_roll') == 'win_streak_roll':
        try:
            selected_play_signal(staking, signal)
            return True
        except ValueError:
            return False
    bet_filter = payload.get("bet_filter") if isinstance(payload.get("bet_filter"), dict) else {}
    if not bet_filter and isinstance(payload.get("play_filter"), dict):
        bet_filter = payload.get("play_filter") or {}
    mode = normalize_play_filter_mode(bet_filter.get("mode"))
    selected_keys = normalize_play_filter_keys(bet_filter.get("selected_keys"))
    if mode != "selected" or not selected_keys:
        # 历史 all/空筛选保持原有范围；新玩法必须明确勾选。
        return str((signal or {}).get("bet_type") or "").strip().lower() not in GROUP_PLAY_TYPES
    signal_key = resolve_signal_play_filter_key(
        bet_type=str((signal or {}).get("bet_type") or ""),
        bet_value=str((signal or {}).get("bet_value") or ""),
    )
    if signal_key is None:
        return False
    return signal_key in selected_keys
