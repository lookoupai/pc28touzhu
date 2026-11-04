"""主注 70% + 辅助注 30% 全仓滚「过三关」策略（纯逻辑，无 IO / 无调度 / 无预测）。

规则（用户 spec，2026-11-03）：
- 每日初始化：balance = 100U，win_streak = 0，状态 running
- 每期总投注 = 当前 balance，主注 70% / 辅助注 30%
- 结构 A/B/C/D 由外部参数指定，不做任何自动选择：
      A 主注单 + 辅助小双      B 主注单 + 辅助大双
      C 主注双 + 辅助小单      D 主注双 + 辅助大单
- 两笔独立结算；和值 0/27 只让命中的单双主注退本，组合注照常判定
- 整期净利 > 0 计 1 关；< 0 清零；= 0 保持
- win_streak >= 3 止盈停止；balance <= 0 止损停止；停止后当日不再产生投注
- 次日调用 reset_day() 重新初始化

结算与赔率完全复用现网接口：
    resolve_pc28_result_for_signal   判定 hit / refund / miss（含 0/27 退本、组合不退）
    resolve_pc28_settled_hit_detail  取该笔的赔率与盈利
    默认结算规则 ``pc28_fullpay_netdisk_regular``（1.99 / 3.71 / 4.32，含本金）
金额沿用程序现有精度：round(value, 2)，结算后余额下限钳制为 0。

用法：
    state = create_day(100.0)
    bets = build_period_bets(state, "A")
    result = settle_period(state, structure="A", draw_context={"triplet": [5, 8, 2]})
    state = result["state"]
    if is_stopped(state):
        state = reset_day(balance=100.0)
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from pc28touzhu.domain.settlement_rules import (
    get_settlement_rule,
    resolve_pc28_result_for_signal,
    resolve_pc28_settled_hit_detail,
)
from pc28touzhu.domain.win_streak_stop import (
    DEFAULT_TARGET_STREAK,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STOP_REASON_BALANCE_DEPLETED,
    STOP_REASON_WIN_STREAK,
    create_tracker,
    is_stopped as _tracker_is_stopped,
    normalize_target_streak,
    update_after_period,
)


DEFAULT_DAY_BALANCE = 100.0
# 整数下注下凑不出两注（< 2）就无法继续这一期，等同于本金耗尽
MIN_PLACEABLE_TOTAL = 2.0
DEFAULT_SETTLEMENT_RULE_ID = "pc28_fullpay_netdisk_regular"
MAIN_SHARE = 0.70
AUX_SHARE = 0.30

STRUCTURES = {
    "A": {"key": "A", "label": "单+小双", "main_value": "单", "aux_value": "小双"},
    "B": {"key": "B", "label": "单+大双", "main_value": "单", "aux_value": "大双"},
    "C": {"key": "C", "label": "双+小单", "main_value": "双", "aux_value": "小单"},
    "D": {"key": "D", "label": "双+大单", "main_value": "双", "aux_value": "大单"},
}

__all__ = [
    "DEFAULT_DAY_BALANCE",
    "MIN_PLACEABLE_TOTAL",
    "DEFAULT_SETTLEMENT_RULE_ID",
    "MAIN_SHARE",
    "AUX_SHARE",
    "STRUCTURES",
    "STATUS_RUNNING",
    "STATUS_STOPPED",
    "STOP_REASON_WIN_STREAK",
    "STOP_REASON_BALANCE_DEPLETED",
    "structure_options",
    "normalize_structure",
    "create_day",
    "reset_day",
    "split_stakes",
    "split_integer_stakes",
    "build_period_bets",
    "settle_leg",
    "settle_period",
    "is_stopped",
]


def _round_money(value: Any) -> float:
    """金额精度：与程序现有口径一致，保留 2 位小数。"""
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def structure_options() -> List[Dict[str, str]]:
    """返回结构列表，便于 UI / 配置展示。"""
    return [dict(config) for config in STRUCTURES.values()]


def normalize_structure(value: Any) -> str:
    """把外部传入的结构参数归一化为 A/B/C/D。

    支持 ``"A"`` / ``"a"`` / ``"单+小双"`` 三种写法；其他值直接报错，不做猜测。
    """
    text = str(value or "").strip()
    if not text:
        raise ValueError("structure 不能为空，必须显式传入 A/B/C/D")
    key = text.upper()
    if key in STRUCTURES:
        return key
    for config in STRUCTURES.values():
        if text == config["label"]:
            return config["key"]
    raise ValueError("未知投注结构: %s（可选 A/B/C/D 或 %s）" % (
        value, "/".join(config["label"] for config in STRUCTURES.values())))


def _resolve_settlement_rule_id(value: Any = None) -> str:
    rule_id = str(value or DEFAULT_SETTLEMENT_RULE_ID).strip() or DEFAULT_SETTLEMENT_RULE_ID
    if not get_settlement_rule(rule_id):
        raise ValueError("未知结算规则: %s" % rule_id)
    return rule_id


def create_day(
    balance: Any = DEFAULT_DAY_BALANCE,
    *,
    target_streak: Any = DEFAULT_TARGET_STREAK,
    settlement_rule_id: Any = DEFAULT_SETTLEMENT_RULE_ID,
) -> Dict[str, Any]:
    """每日初始化。"""
    try:
        normalized_balance = round(float(balance), 2)
    except (TypeError, ValueError):
        raise ValueError("balance 必须为数字")
    if normalized_balance < 0:
        raise ValueError("balance 不能为负数")
    tracker = create_tracker(target_streak)
    return {
        "balance": normalized_balance,
        "win_streak": tracker["win_streak"],
        "target_streak": tracker["target_streak"],
        "status": tracker["status"],
        "stop_reason": None,
        "settlement_rule_id": _resolve_settlement_rule_id(settlement_rule_id),
        "periods": 0,
    }


def reset_day(
    balance: Any = DEFAULT_DAY_BALANCE,
    *,
    target_streak: Any = DEFAULT_TARGET_STREAK,
    settlement_rule_id: Any = DEFAULT_SETTLEMENT_RULE_ID,
) -> Dict[str, Any]:
    """次日重新初始化（等价于 create_day，语义更直观）。"""
    return create_day(balance, target_streak=target_streak, settlement_rule_id=settlement_rule_id)


def split_integer_stakes(stake_total: Any, main_share: Any = MAIN_SHARE) -> Dict[str, float]:
    """两腿整数注：总额向下取整，主注按占比向下取整，余数全部给辅助注。

    投注按整数下（平台最小注 1），不允许出现 0.1 这类带小数的单子，未押出去的
    尾差（不足 1 的部分）留在仓里继续滚：
        189.90 -> 总额 189，主注 132，辅助 57（余 0.90 留仓）
        19.88  -> 总额 19，主注 13，辅助 6（余 0.88 留仓）
    资金不足以开出两注（总额 < 2）时返回全 0，由调用方决定跳过还是收口。
    """
    try:
        amount = round(float(stake_total), 2)
    except (TypeError, ValueError):
        raise ValueError("stake_total 必须为数字")
    if amount < 0:
        raise ValueError("stake_total 不能为负数")
    share = MAIN_SHARE if main_share in (None, "") else float(main_share)
    total = int(amount + 1e-9)  # 向下取整，1e-9 抵消浮点误差
    if total < 2:
        return {"main": 0.0, "aux": 0.0, "total": 0.0}
    main_stake = int(total * share + 1e-9)
    if main_stake < 1:
        return {"main": 0.0, "aux": 0.0, "total": 0.0}
    aux_stake = total - main_stake
    if aux_stake < 1:
        main_stake, aux_stake = total - 1, 1
    return {"main": float(main_stake), "aux": float(aux_stake), "total": float(total)}


def split_stakes(balance: Any) -> Dict[str, float]:
    """按 70/30 拆分当期注额；两腿都是整数注，主注 + 辅助注 == 总额（整数）。"""
    return split_integer_stakes(balance, MAIN_SHARE)


def build_period_bets(state: Dict[str, Any], structure: Any) -> Dict[str, Any]:
    """生成本期两笔投注（尚未结算）。已停止时不允许再产生投注。"""
    if not isinstance(state, dict):
        raise ValueError("state 必须为策略状态")
    if _tracker_is_stopped(state):
        raise ValueError("当天策略已停止，不能再产生投注")
    key = normalize_structure(structure)
    config = STRUCTURES[key]
    stakes = split_stakes(state.get("balance"))
    if stakes["total"] <= 0:
        raise ValueError("当期资金不足以下注（整数下注至少需要两注）")
    return {
        "structure": key,
        "label": config["label"],
        "main": {
            "bet_type": "odd_even",
            "bet_value": config["main_value"],
            "stake": stakes["main"],
        },
        "aux": {
            "bet_type": "combo",
            "bet_value": config["aux_value"],
            "stake": stakes["aux"],
        },
        "total_stake": stakes["total"],
    }


def settle_leg(
    *,
    stake: Any,
    bet_type: str,
    bet_value: str,
    draw_context: Dict[str, Any],
    settlement_rule_id: Any = DEFAULT_SETTLEMENT_RULE_ID,
) -> Dict[str, Any]:
    """结算单笔投注：复用现网结算接口，返回实际返还金额。"""
    rule_id = _resolve_settlement_rule_id(settlement_rule_id)
    amount = _round_money(stake)
    if amount < 0:
        raise ValueError("stake 不能为负数")
    signal = {
        "lottery_type": "pc28",
        "bet_type": str(bet_type or "").strip(),
        "bet_value": str(bet_value or "").strip(),
    }
    result = resolve_pc28_result_for_signal(
        signal=signal,
        settlement_rule_id=rule_id,
        draw_context=draw_context,
    )
    result_type = str(result.get("result_type") or "")
    detail = resolve_pc28_settled_hit_detail(
        stake_amount=amount,
        signal=signal,
        settlement_rule_id=rule_id,
        draw_snapshot=result.get("draw_snapshot"),
    )
    odds = float(detail["odds"]) if detail else None

    if result_type == "hit":
        payout = _round_money(amount + float(detail["profit"])) if detail else _round_money(amount * (odds or 0))
    elif result_type == "refund":
        payout = amount
    else:
        payout = 0.0

    return {
        "bet_type": signal["bet_type"],
        "bet_value": signal["bet_value"],
        "stake": amount,
        "result_type": result_type,
        "refund_reason": result.get("refund_reason"),
        "actual_value": result.get("actual_value"),
        "odds": odds,
        "payout": _round_money(payout),
    }


def settle_period(
    state: Dict[str, Any],
    *,
    structure: Any,
    draw_context: Dict[str, Any],
) -> Dict[str, Any]:
    """结算一整期：两笔独立结算 → 余额更新 → 连赢计数 → 停止检查。

    返回结果里 ``state`` 是更新后的策略状态，其余字段是本期明细。
    """
    if not isinstance(state, dict):
        raise ValueError("state 必须为策略状态")
    if _tracker_is_stopped(state):
        raise ValueError("当天策略已停止，不能再结算新的投注")
    bets = build_period_bets(state, structure)
    rule_id = state.get("settlement_rule_id") or DEFAULT_SETTLEMENT_RULE_ID
    balance_before = float(state.get("balance") or 0)

    main_result = settle_leg(
        stake=bets["main"]["stake"],
        bet_type=bets["main"]["bet_type"],
        bet_value=bets["main"]["bet_value"],
        draw_context=draw_context,
        settlement_rule_id=rule_id,
    )
    aux_result = settle_leg(
        stake=bets["aux"]["stake"],
        bet_type=bets["aux"]["bet_type"],
        bet_value=bets["aux"]["bet_value"],
        draw_context=draw_context,
        settlement_rule_id=rule_id,
    )

    # 当期总投注 = 取整后的总额；未押出去的尾差留在余额里（spec 第 5 条 + 整数下注）
    staked_total = _round_money(bets["total_stake"])
    new_balance = _round_money(
        main_result["payout"] + aux_result["payout"] + max(0.0, balance_before - staked_total)
    )
    if new_balance < 0:  # 防御：四舍五入不得产生负余额
        new_balance = 0.0
    net_pnl = _round_money(new_balance - balance_before)

    tracker = update_after_period(
        {
            "win_streak": state.get("win_streak") or 0,
            "target_streak": state.get("target_streak") or DEFAULT_TARGET_STREAK,
            "status": state.get("status") or STATUS_RUNNING,
            "stop_reason": state.get("stop_reason"),
        },
        net_pnl,
        balance=new_balance if new_balance >= MIN_PLACEABLE_TOTAL else 0.0,
    )

    new_state = {
        **state,
        "balance": new_balance,
        "win_streak": tracker["win_streak"],
        "status": tracker["status"],
        "stop_reason": tracker["stop_reason"],
        "periods": int(state.get("periods") or 0) + 1,
    }
    return {
        "state": new_state,
        "structure": bets["structure"],
        "label": bets["label"],
        "bets": bets,
        "main_result": main_result,
        "aux_result": aux_result,
        "balance_before": balance_before,
        "new_balance": new_balance,
        "net_pnl": net_pnl,
        "win_streak": new_state["win_streak"],
        "status": new_state["status"],
        "stop_reason": new_state["stop_reason"],
    }


def is_stopped(state: Dict[str, Any]) -> bool:
    """当天策略是否已停止（止盈或止损）。"""
    return str((state or {}).get("status") or "") == STATUS_STOPPED
