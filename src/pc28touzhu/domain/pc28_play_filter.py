"""PC28 跟单玩法筛选规则。"""
from __future__ import annotations

from typing import Iterable, Optional


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
        if text in PLAY_FILTER_LABELS and text not in normalized:
            normalized.append(text)
    return normalized


def resolve_signal_play_filter_key(*, bet_type: str, bet_value: str) -> Optional[str]:
    normalized_type = str(bet_type or "").strip().lower()
    normalized_value = str(bet_value or "").strip()
    if normalized_type == "combo" and normalized_value in {"大单", "大双", "小单", "小双"}:
        return "combo:" + normalized_value
    if normalized_type == "big_small" and normalized_value in {"大", "小"}:
        return "big_small:" + normalized_value
    if normalized_type in {"big_small", "odd_even"} and normalized_value in {"单", "双"}:
        return "odd_even:" + normalized_value
    return None


def strategy_matches_signal(strategy: Optional[dict], signal: Optional[dict]) -> bool:
    payload = strategy if isinstance(strategy, dict) else {}
    bet_filter = payload.get("bet_filter") if isinstance(payload.get("bet_filter"), dict) else {}
    mode = normalize_play_filter_mode(bet_filter.get("mode"))
    selected_keys = normalize_play_filter_keys(bet_filter.get("selected_keys"))
    if mode != "selected" or not selected_keys:
        return True
    signal_key = resolve_signal_play_filter_key(
        bet_type=str((signal or {}).get("bet_type") or ""),
        bet_value=str((signal or {}).get("bet_value") or ""),
    )
    if signal_key is None:
        return False
    return signal_key in selected_keys
