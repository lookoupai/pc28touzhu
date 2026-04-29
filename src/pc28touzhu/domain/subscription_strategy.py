"""订阅策略兼容层：统一旧版 strategy 与新版 strategy_v2。"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pc28touzhu.domain.pc28_play_filter import normalize_play_filter_keys, normalize_play_filter_mode
from pc28touzhu.domain.settlement_rules import (
    ALLOWED_SETTLEMENT_RULE_IDS,
    DEFAULT_SETTLEMENT_RULE_ID,
    legacy_profit_rule_args_from_settlement_rule_id as _legacy_profit_rule_args_from_settlement_rule_id,
    normalize_settlement_rule_id,
    settlement_rule_id_from_legacy as _settlement_rule_id_from_legacy,
)


ALLOWED_STAKING_POLICY_MODES = {"fixed", "follow_source", "martingale"}
ALLOWED_SETTLEMENT_RULE_SOURCES = {"follow_signal", "subscription_fixed"}
DEFAULT_EXPIRE_AFTER_SECONDS = 120
DEFAULT_FALLBACK_PROFIT_RATIO = 1.0


def _to_object(value: Any) -> Dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _to_optional_non_negative_float(value: Any, field_name: str) -> Optional[float]:
    if value in {None, ""}:
        return None
    try:
        normalized = round(float(value), 4)
    except (TypeError, ValueError):
        raise ValueError("%s 必须为数字" % field_name)
    if normalized < 0:
        raise ValueError("%s 不能小于 0" % field_name)
    return normalized


def _to_optional_positive_float(value: Any, field_name: str) -> Optional[float]:
    normalized = _to_optional_non_negative_float(value, field_name)
    if normalized is None:
        return None
    if normalized <= 0:
        raise ValueError("%s 必须大于 0" % field_name)
    return normalized


def _normalize_positive_int_list(value: Any, field_name: str) -> list[int]:
    if value is None or value == "":
        return []
    if not isinstance(value, list):
        raise ValueError("%s 必须为数组" % field_name)
    normalized: list[int] = []
    seen: set[int] = set()
    for index, item in enumerate(value, start=1):
        try:
            item_id = int(item)
        except (TypeError, ValueError):
            raise ValueError("%s[%s] 必须为整数" % (field_name, index))
        if item_id <= 0:
            raise ValueError("%s[%s] 必须大于 0" % (field_name, index))
        if item_id not in seen:
            seen.add(item_id)
            normalized.append(item_id)
    return normalized


def _normalize_ratio(value: Any, field_name: str, default: float = DEFAULT_FALLBACK_PROFIT_RATIO) -> float:
    normalized = _to_optional_positive_float(value, field_name)
    if normalized is None:
        return round(float(default), 4)
    return round(float(normalized), 4)


def _normalize_refund_action(value: Any, field_name: str) -> str:
    text = str(value or "hold").strip() or "hold"
    if text not in {"hold", "reset"}:
        raise ValueError("%s 仅支持 hold 或 reset" % field_name)
    return text


def _normalize_cap_action(value: Any, field_name: str) -> str:
    text = str(value or "reset").strip() or "reset"
    if text not in {"hold", "reset"}:
        raise ValueError("%s 仅支持 hold 或 reset" % field_name)
    return text


def _normalize_settlement_rule_id(value: Any, field_name: str, *, allow_empty: bool = True) -> Optional[str]:
    try:
        return normalize_settlement_rule_id(value, allow_empty=allow_empty)
    except ValueError:
        raise ValueError("%s 不支持当前结算规则" % field_name)


def is_subscription_strategy_v2(value: Any) -> bool:
    payload = _to_object(value)
    return any(
        key in payload
        for key in ("play_filter", "staking_policy", "settlement_policy", "dispatch")
    )


def settlement_rule_id_from_legacy(profit_rule_id: Any, odds_profile: Any) -> str:
    return _settlement_rule_id_from_legacy(profit_rule_id, odds_profile)


def legacy_profit_rule_args_from_settlement_rule_id(settlement_rule_id: Any) -> Dict[str, str]:
    return _legacy_profit_rule_args_from_settlement_rule_id(settlement_rule_id)


def _normalize_play_filter_v2(payload: Any) -> Dict[str, Any]:
    data = _to_object(payload)
    mode = normalize_play_filter_mode(data.get("mode"))
    selected_keys = normalize_play_filter_keys(data.get("selected_keys"))
    if mode == "selected" and not selected_keys:
        raise ValueError("选择自定义玩法时，至少要勾选一个玩法")
    return {
        "mode": mode,
        "selected_keys": selected_keys,
    }


def _normalize_staking_policy_v2(payload: Any) -> Dict[str, Any]:
    data = _to_object(payload)
    mode = str(data.get("mode") or "follow_source").strip() or "follow_source"
    if mode not in ALLOWED_STAKING_POLICY_MODES:
        raise ValueError("strategy.staking_policy.mode 仅支持 fixed、follow_source 或 martingale")

    fixed_amount = _to_optional_positive_float(data.get("fixed_amount"), "strategy.staking_policy.fixed_amount")
    base_stake = _to_optional_positive_float(data.get("base_stake"), "strategy.staking_policy.base_stake")
    multiplier = _to_optional_non_negative_float(data.get("multiplier"), "strategy.staking_policy.multiplier")
    max_steps_raw = data.get("max_steps")
    max_steps = None
    if max_steps_raw not in {None, ""}:
        try:
            max_steps = int(max_steps_raw)
        except (TypeError, ValueError):
            raise ValueError("strategy.staking_policy.max_steps 必须为整数")
        if max_steps < 1:
            raise ValueError("strategy.staking_policy.max_steps 必须大于 0")

    if mode == "fixed":
        if fixed_amount is None:
            raise ValueError("strategy.staking_policy.fixed_amount 必须大于 0")
        normalized_fixed_amount = round(float(fixed_amount), 2)
        normalized_base_stake = None
        normalized_multiplier = 2.0
        normalized_max_steps = 1
    elif mode == "martingale":
        if base_stake is None:
            raise ValueError("strategy.staking_policy.base_stake 必须大于 0")
        normalized_fixed_amount = None
        normalized_base_stake = round(float(base_stake), 2)
        normalized_multiplier = round(float(multiplier or 2.0), 4)
        if normalized_multiplier <= 1:
            raise ValueError("strategy.staking_policy.multiplier 必须大于 1")
        normalized_max_steps = int(max_steps or 1)
    else:
        normalized_fixed_amount = None
        normalized_base_stake = None
        normalized_multiplier = 2.0
        normalized_max_steps = 1

    return {
        "mode": mode,
        "fixed_amount": normalized_fixed_amount,
        "base_stake": normalized_base_stake,
        "multiplier": normalized_multiplier,
        "max_steps": normalized_max_steps,
        "refund_action": _normalize_refund_action(data.get("refund_action"), "strategy.staking_policy.refund_action"),
        "cap_action": _normalize_cap_action(data.get("cap_action"), "strategy.staking_policy.cap_action"),
    }


def _normalize_settlement_policy_v2(payload: Any) -> Dict[str, Any]:
    data = _to_object(payload)
    rule_source = str(data.get("rule_source") or "follow_signal").strip() or "follow_signal"
    if rule_source not in ALLOWED_SETTLEMENT_RULE_SOURCES:
        raise ValueError("strategy.settlement_policy.rule_source 仅支持 follow_signal 或 subscription_fixed")
    settlement_rule_id = _normalize_settlement_rule_id(
        data.get("settlement_rule_id"),
        "strategy.settlement_policy.settlement_rule_id",
        allow_empty=True,
    )
    if rule_source == "subscription_fixed" and not settlement_rule_id:
        raise ValueError("固定结算规则时，strategy.settlement_policy.settlement_rule_id 不能为空")
    return {
        "rule_source": rule_source,
        "settlement_rule_id": settlement_rule_id,
        "fallback_profit_ratio": _normalize_ratio(
            data.get("fallback_profit_ratio"),
            "strategy.settlement_policy.fallback_profit_ratio",
        ),
    }


def _normalize_risk_control_v2(payload: Any) -> Dict[str, Any]:
    data = _to_object(payload)
    enabled = bool(data.get("enabled"))
    profit_target = round(float(_to_optional_non_negative_float(data.get("profit_target"), "strategy.risk_control.profit_target") or 0), 2)
    loss_limit = round(float(_to_optional_non_negative_float(data.get("loss_limit"), "strategy.risk_control.loss_limit") or 0), 2)
    if enabled and profit_target <= 0 and loss_limit <= 0:
        raise ValueError("启用止盈止损时，止盈或止损至少要设置一个大于 0 的阈值")
    return {
        "enabled": enabled,
        "profit_target": profit_target,
        "loss_limit": loss_limit,
    }


def _normalize_dispatch_v2(payload: Any) -> Dict[str, Any]:
    data = _to_object(payload)
    expire_after_seconds = data.get("expire_after_seconds")
    if expire_after_seconds in {None, ""}:
        normalized_expire_after_seconds = DEFAULT_EXPIRE_AFTER_SECONDS
    else:
        try:
            normalized_expire_after_seconds = int(expire_after_seconds)
        except (TypeError, ValueError):
            raise ValueError("strategy.dispatch.expire_after_seconds 必须为整数")
        normalized_expire_after_seconds = max(30, normalized_expire_after_seconds)
    return {
        "expire_after_seconds": normalized_expire_after_seconds,
        "delivery_target_ids": _normalize_positive_int_list(
            data.get("delivery_target_ids"),
            "strategy.dispatch.delivery_target_ids",
        ),
    }


def normalize_subscription_strategy_input(value: Any) -> Dict[str, Any]:
    payload = _to_object(value)
    if is_subscription_strategy_v2(payload):
        return {
            "play_filter": _normalize_play_filter_v2(payload.get("play_filter")),
            "staking_policy": _normalize_staking_policy_v2(payload.get("staking_policy")),
            "settlement_policy": _normalize_settlement_policy_v2(payload.get("settlement_policy")),
            "risk_control": _normalize_risk_control_v2(payload.get("risk_control")),
            "dispatch": _normalize_dispatch_v2(payload.get("dispatch")),
        }
    return _normalize_legacy_subscription_strategy(payload)


def _normalize_legacy_subscription_strategy(payload: Dict[str, Any]) -> Dict[str, Any]:
    mode = str(payload.get("mode") or "follow").strip() or "follow"
    if mode not in {"follow", "martingale"}:
        raise ValueError("strategy.mode 仅支持 follow 或 martingale")

    staking_mode = "follow_source"
    if mode == "martingale":
        staking_mode = "martingale"
    elif payload.get("stake_amount") not in {None, ""}:
        staking_mode = "fixed"

    base_stake = payload.get("base_stake")
    if base_stake in {None, ""} and payload.get("stake_amount") not in {None, ""} and mode == "martingale":
        base_stake = payload.get("stake_amount")

    return normalize_subscription_strategy_input(
        {
            "play_filter": payload.get("bet_filter"),
            "staking_policy": {
                "mode": staking_mode,
                "fixed_amount": payload.get("stake_amount") if staking_mode == "fixed" else None,
                "base_stake": base_stake,
                "multiplier": payload.get("multiplier"),
                "max_steps": payload.get("max_steps"),
                "refund_action": payload.get("refund_action"),
                "cap_action": payload.get("cap_action"),
            },
            "settlement_policy": {
                "rule_source": "follow_signal",
                "settlement_rule_id": None,
                "fallback_profit_ratio": _to_object(payload.get("risk_control")).get("win_profit_ratio"),
            },
            "risk_control": {
                "enabled": _to_object(payload.get("risk_control")).get("enabled"),
                "profit_target": _to_object(payload.get("risk_control")).get("profit_target"),
                "loss_limit": _to_object(payload.get("risk_control")).get("loss_limit"),
            },
            "dispatch": {
                "expire_after_seconds": payload.get("expire_after_seconds"),
                "delivery_target_ids": payload.get("delivery_target_ids"),
            },
        }
    )


def upgrade_subscription_strategy(value: Any) -> Dict[str, Any]:
    payload = _to_object(value)
    try:
        return normalize_subscription_strategy_input(payload)
    except Exception:
        repaired = dict(payload)
        risk_control = _to_object(repaired.get("risk_control"))
        if risk_control:
            repaired_risk_control = dict(risk_control)
            repaired_risk_control["enabled"] = False
            repaired["risk_control"] = repaired_risk_control
        if is_subscription_strategy_v2(repaired):
            settlement_policy = _to_object(repaired.get("settlement_policy"))
            if str(settlement_policy.get("rule_source") or "").strip() == "subscription_fixed" and settlement_policy.get("settlement_rule_id") in {None, ""}:
                repaired_settlement_policy = dict(settlement_policy)
                repaired_settlement_policy["rule_source"] = "follow_signal"
                repaired["settlement_policy"] = repaired_settlement_policy
        try:
            return normalize_subscription_strategy_input(repaired)
        except Exception:
            return normalize_subscription_strategy_input({})


def project_subscription_strategy_v1(value: Any) -> Dict[str, Any]:
    strategy_v2 = upgrade_subscription_strategy(value)
    play_filter = dict(strategy_v2.get("play_filter") or {})
    staking_policy = _to_object(strategy_v2.get("staking_policy"))
    settlement_policy = _to_object(strategy_v2.get("settlement_policy"))
    risk_control = _to_object(strategy_v2.get("risk_control"))
    dispatch = _to_object(strategy_v2.get("dispatch"))

    projected = {
        "mode": "martingale" if str(staking_policy.get("mode") or "") == "martingale" else "follow",
        "bet_filter": {
            "mode": normalize_play_filter_mode(play_filter.get("mode")),
            "selected_keys": normalize_play_filter_keys(play_filter.get("selected_keys")),
        },
        "expire_after_seconds": int(dispatch.get("expire_after_seconds") or DEFAULT_EXPIRE_AFTER_SECONDS),
        "delivery_target_ids": _normalize_positive_int_list(
            dispatch.get("delivery_target_ids"),
            "strategy.dispatch.delivery_target_ids",
        ),
        "risk_control": {
            "enabled": bool(risk_control.get("enabled")),
            "profit_target": round(float(risk_control.get("profit_target") or 0), 2),
            "loss_limit": round(float(risk_control.get("loss_limit") or 0), 2),
            "win_profit_ratio": round(float(settlement_policy.get("fallback_profit_ratio") or DEFAULT_FALLBACK_PROFIT_RATIO), 4),
        },
    }
    if str(staking_policy.get("mode") or "") == "fixed":
        projected["stake_amount"] = round(float(staking_policy.get("fixed_amount") or 0), 2)
    if str(staking_policy.get("mode") or "") == "martingale":
        projected["base_stake"] = round(float(staking_policy.get("base_stake") or 0), 2)
        projected["multiplier"] = round(float(staking_policy.get("multiplier") or 2.0), 4)
        projected["max_steps"] = int(staking_policy.get("max_steps") or 1)
        projected["refund_action"] = _normalize_refund_action(
            staking_policy.get("refund_action"),
            "strategy.refund_action",
        )
        projected["cap_action"] = _normalize_cap_action(
            staking_policy.get("cap_action"),
            "strategy.cap_action",
        )
    return projected


def present_subscription_item(item: Any) -> Any:
    if not isinstance(item, dict):
        return item
    raw_strategy = item.get("strategy_v2") if isinstance(item.get("strategy_v2"), dict) else item.get("strategy")
    strategy_v2 = upgrade_subscription_strategy(raw_strategy)
    presented = dict(item)
    presented["strategy_v2"] = strategy_v2
    presented["strategy"] = project_subscription_strategy_v1(strategy_v2)
    presented["strategy_schema_version"] = 2
    return presented


def resolve_signal_stake_hints(payload: Any) -> Dict[str, Any]:
    data = _to_object(payload)
    source_hints = _to_object(data.get("source_hints"))
    stake = _to_object(source_hints.get("stake"))
    result: Dict[str, Any] = {}

    def _copy_float(key: str) -> None:
        value = stake.get(key, data.get(key))
        if value in {None, ""}:
            return
        try:
            normalized = round(float(value), 4)
        except (TypeError, ValueError):
            return
        if normalized <= 0:
            return
        result[key] = normalized

    _copy_float("stake_amount")
    _copy_float("base_stake")
    _copy_float("multiplier")

    max_steps = stake.get("max_steps", data.get("max_steps"))
    if max_steps not in {None, ""}:
        try:
            normalized_steps = int(max_steps)
        except (TypeError, ValueError):
            normalized_steps = None
        if normalized_steps and normalized_steps > 0:
            result["max_steps"] = normalized_steps

    for key, default in (("refund_action", "hold"), ("cap_action", "reset")):
        value = stake.get(key, data.get(key))
        if value in {None, ""}:
            continue
        try:
            result[key] = _normalize_refund_action(value, key) if key == "refund_action" else _normalize_cap_action(value, key)
        except ValueError:
            result[key] = default
    return result


def resolve_signal_settlement_rule_id(payload: Any) -> Optional[str]:
    data = _to_object(payload)
    source_hints = _to_object(data.get("source_hints"))
    settlement = _to_object(source_hints.get("settlement"))
    if settlement.get("settlement_rule_id") not in {None, ""}:
        try:
            return _normalize_settlement_rule_id(
                settlement.get("settlement_rule_id"),
                "source_hints.settlement.settlement_rule_id",
                allow_empty=False,
            )
        except ValueError:
            return None
    if data.get("settlement_rule_id") not in {None, ""}:
        try:
            return _normalize_settlement_rule_id(
                data.get("settlement_rule_id"),
                "normalized_payload.settlement_rule_id",
                allow_empty=False,
            )
        except ValueError:
            return None
    source_ref = _to_object(data.get("source_ref"))
    should_use_legacy_pc28_rule = (
        data.get("profit_rule_id") not in {None, ""}
        or data.get("odds_profile") not in {None, ""}
        or str(source_ref.get("platform") or "").strip() == "AITradingSimulator"
    )
    if not should_use_legacy_pc28_rule:
        return None
    return settlement_rule_id_from_legacy(data.get("profit_rule_id"), data.get("odds_profile"))


def enrich_signal_payload_source_hints(payload: Any) -> Dict[str, Any]:
    data = dict(_to_object(payload))
    source_hints = _to_object(data.get("source_hints"))
    stake_hints = resolve_signal_stake_hints(data)
    settlement_rule_id = resolve_signal_settlement_rule_id(data)
    if stake_hints:
        next_stake = _to_object(source_hints.get("stake"))
        next_stake.update(stake_hints)
        source_hints["stake"] = next_stake
    if settlement_rule_id:
        next_settlement = _to_object(source_hints.get("settlement"))
        next_settlement["settlement_rule_id"] = settlement_rule_id
        source_hints["settlement"] = next_settlement
    if source_hints:
        data["source_hints"] = source_hints
    return data


def resolve_staking_runtime_policy(strategy: Any, signal_payload: Any) -> Dict[str, Any]:
    strategy_v2 = upgrade_subscription_strategy(strategy)
    policy = _to_object(strategy_v2.get("staking_policy"))
    signal_hints = resolve_signal_stake_hints(signal_payload)
    mode = str(policy.get("mode") or "follow_source")
    if mode == "fixed":
        fixed_amount = round(float(policy.get("fixed_amount") or 0), 2)
        return {
            "mode": "fixed",
            "base_stake": fixed_amount,
            "fixed_amount": fixed_amount,
            "multiplier": 2.0,
            "max_steps": 1,
            "refund_action": _normalize_refund_action(policy.get("refund_action"), "strategy.staking_policy.refund_action"),
            "cap_action": _normalize_cap_action(policy.get("cap_action"), "strategy.staking_policy.cap_action"),
        }
    if mode == "martingale":
        return {
            "mode": "martingale",
            "base_stake": round(float(policy.get("base_stake") or 0), 2),
            "fixed_amount": None,
            "multiplier": round(float(policy.get("multiplier") or 2.0), 4),
            "max_steps": int(policy.get("max_steps") or 1),
            "refund_action": _normalize_refund_action(policy.get("refund_action"), "strategy.staking_policy.refund_action"),
            "cap_action": _normalize_cap_action(policy.get("cap_action"), "strategy.staking_policy.cap_action"),
        }

    fixed_amount = signal_hints.get("stake_amount")
    base_stake = signal_hints.get("base_stake")
    if fixed_amount is None and base_stake is None:
        fixed_amount = 10.0
        base_stake = 10.0
    elif fixed_amount is None:
        fixed_amount = round(float(base_stake or 10.0), 2)
    elif base_stake is None:
        base_stake = round(float(fixed_amount or 10.0), 2)

    return {
        "mode": "follow_source",
        "base_stake": round(float(base_stake or 10.0), 2),
        "fixed_amount": round(float(fixed_amount or 10.0), 2),
        "multiplier": round(float(signal_hints.get("multiplier") or 2.0), 4),
        "max_steps": int(signal_hints.get("max_steps") or 1),
        "refund_action": _normalize_refund_action(signal_hints.get("refund_action"), "source_hints.stake.refund_action"),
        "cap_action": _normalize_cap_action(signal_hints.get("cap_action"), "source_hints.stake.cap_action"),
    }


def resolve_settlement_runtime_policy(strategy: Any, signal_payload: Any) -> Dict[str, Any]:
    strategy_v2 = upgrade_subscription_strategy(strategy)
    settlement_policy = _to_object(strategy_v2.get("settlement_policy"))
    fallback_profit_ratio = round(
        float(settlement_policy.get("fallback_profit_ratio") or DEFAULT_FALLBACK_PROFIT_RATIO),
        4,
    )
    configured_rule_id = _normalize_settlement_rule_id(
        settlement_policy.get("settlement_rule_id"),
        "strategy.settlement_policy.settlement_rule_id",
        allow_empty=True,
    )
    if str(settlement_policy.get("rule_source") or "follow_signal") == "subscription_fixed" and configured_rule_id:
        return {
            "rule_source": "subscription_fixed",
            "settlement_rule_id": configured_rule_id,
            "fallback_profit_ratio": fallback_profit_ratio,
            "resolved_from": "subscription",
        }
    signal_rule_id = resolve_signal_settlement_rule_id(signal_payload)
    if signal_rule_id:
        return {
            "rule_source": "follow_signal",
            "settlement_rule_id": signal_rule_id,
            "fallback_profit_ratio": fallback_profit_ratio,
            "resolved_from": "signal_hint",
        }
    return {
        "rule_source": str(settlement_policy.get("rule_source") or "follow_signal"),
        "settlement_rule_id": None,
        "fallback_profit_ratio": fallback_profit_ratio,
        "resolved_from": "fallback_ratio",
    }


def resolve_risk_control_policy(strategy: Any) -> Dict[str, Any]:
    strategy_v2 = upgrade_subscription_strategy(strategy)
    return dict(_to_object(strategy_v2.get("risk_control")))


def resolve_dispatch_policy(strategy: Any) -> Dict[str, Any]:
    strategy_v2 = upgrade_subscription_strategy(strategy)
    return dict(_to_object(strategy_v2.get("dispatch")))
