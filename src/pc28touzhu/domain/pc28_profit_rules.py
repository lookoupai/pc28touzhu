"""PC28 盈亏规则，当前只覆盖已落地的两类玩法。"""
from __future__ import annotations

from typing import Optional


DEFAULT_PROFIT_RULE_ID = "pc28_netdisk"
DEFAULT_ODDS_PROFILE = "regular"
ALLOWED_PROFIT_RULES = {"pc28_netdisk", "pc28_high"}
ALLOWED_ODDS_PROFILES = {"regular", "abc"}


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
                    "小单 / 大双": 3.6,
                    "大单 / 小双": 4.2,
                }
            },
            "abc": {
                "group_odds": {
                    "小单 / 大双": 4.8,
                    "大单 / 小双": 3.1,
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
                    "小单 / 大双": 6.78,
                    "大单 / 小双": 6.33,
                }
            },
            "abc": {
                "group_odds": {
                    "小单 / 大双": 4.9,
                    "大单 / 小双": 3.1,
                }
            },
        },
    },
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
        return "小单 / 大双"
    if bet_value in {"大单", "小双"}:
        return "大单 / 小双"
    return None


def resolve_pc28_hit_profit(
    *,
    stake_amount: float,
    bet_type: str,
    bet_value: str,
    profit_rule_id: Optional[str] = None,
    odds_profile: Optional[str] = None,
) -> Optional[float]:
    metric = _resolve_metric_from_bet(bet_type, bet_value)
    if metric is None:
        return None
    rule_id = normalize_profit_rule_id(profit_rule_id)
    profile_key = normalize_odds_profile(odds_profile)
    metric_profile = (((PC28_PROFIT_RULES.get(rule_id) or {}).get(metric) or {}).get(profile_key) or {})
    if metric == "combo":
        combo_group = _resolve_combo_group(str(bet_value or "").strip())
        if not combo_group:
            return None
        odds = float((metric_profile.get("group_odds") or {}).get(combo_group) or 0)
    else:
        odds = float(metric_profile.get("odds") or 0)
    if odds <= 0:
        return None
    return round(float(stake_amount) * max(0.0, odds - 1.0), 2)
