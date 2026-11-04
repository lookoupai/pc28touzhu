"""可配置「过三关 + 滚仓」玩法引擎（订阅 / 自动触发路由共用）。

与 ``split_roll_strategy``（单玩法内核）的区别：这里把两个口径做成配置项，
供不同玩法自由选择，并可被订阅策略与路由策略同时引用。

配置字段（strategy.staking_policy / routes[].staking_policy）：
    mode:          "win_streak_roll"
    base_amount:   每日起注 / 全仓口径的每日本金，默认 100
    main_share:    主注占比，默认 0.70
    aux_share:     辅助注占比，默认 0.30（不传则自动取 1-main_share）
    target_streak: 连续有效盈利几关止盈，默认 3
    structure:     "A" | "B" | "C" | "D" | "follow_signal"，默认 "A"
    roll_mode:     "chain"（链条：下一注=上期返还，账户跨日结转）
                   "all_in"（全仓：每期押全部余额，每日重置）
    reset_scope:   "daily"（当前仅支持按日重置）
    stop_policy:   达标 / 爆仓之后的收工开关（见下）

收工开关（stop_policy，随 staking_policy 一起配置）：
    on_target: close_day（默认）达标即当天收工
               new_cycle 达标后当天再开一轮（受 max_cycles_per_day 限制）
               continue  达标不处理，连赢计数清零后继续滚
    on_bust:   close_day（默认）爆仓即当天收工
               new_cycle 爆仓后补足每天起始注再开一轮
    close_scope: rule（默认）停整条规则的当日 / route 只停该路由
    max_cycles_per_day: 一个交易日最多开几轮，默认 1

收工时把原因（cycle_target_hit / cycle_balance_depleted）写进规则或路由的当日统计，
派单与触发评估的既有闸门立刻生效；这两个原因属于当天粘性停止，保存一次配置不会把它解掉。

状态字段（每个订阅 / 路由各一份）：
    stat_date     状态所属日期（YYYY-MM-DD）
    balance       all_in=当日可用余额；chain=账户余额（跨日结转）
    cycle_stake   chain=本期注码（初始 base_amount，之后=上期返还）；all_in=None
    day_pnl       chain=当日累计盈亏；all_in=None
    win_streak / target_streak / status / stop_reason / periods
    cycles_today 今日已走完的轮数；day_closed/day_close_scope 当日是否已收工及收工范围

口径要点：
- 一次「期」= 两笔独立结算（主注 odd_even + 辅助注 combo），70/30 拆注
- 整期净利 >0 计 1 关、<0 清零、=0 保持
- 止盈：连赢达 target_streak；止损：all_in 余额归零 / chain 当期注码归零（= 当日风险金耗尽）
- 特殊号 0/27 只作用于命中的单双主注，组合照常结算（复用现网结算接口）
"""
from __future__ import annotations

from typing import Any, Dict, Optional

from pc28touzhu.domain.settlement_rules import (
    get_settlement_rule,
    resolve_pc28_result_for_signal,
    resolve_pc28_settled_hit_detail,
)
from pc28touzhu.domain.split_roll_strategy import (
    DEFAULT_SETTLEMENT_RULE_ID,
    STRUCTURES,
    normalize_structure,
)
from pc28touzhu.domain.win_streak_stop import (
    DEFAULT_TARGET_STREAK,
    STATUS_RUNNING,
    STATUS_STOPPED,
    STOP_REASON_BALANCE_DEPLETED,
    STOP_REASON_WIN_STREAK,
    normalize_target_streak,
    update_after_period,
)


MODE_WIN_STREAK_ROLL = "win_streak_roll"
ALLOWED_ROLL_MODES = ("chain", "all_in")
ALLOWED_RESET_SCOPES = ("daily",)
STRUCTURE_FOLLOW_SIGNAL = "follow_signal"
DEFAULT_ROLL_MODE = "chain"
DEFAULT_MAIN_SHARE = 0.70
DEFAULT_BASE_AMOUNT = 100.0

# 收工开关（stop_policy）：达标 / 爆仓之后是否当天收工、一天最多开几轮。
# 默认值等于最初口径：达标或爆仓都当天收工、停到规则层、一天一轮。
ALLOWED_ON_TARGET = ("close_day", "new_cycle", "continue")
ALLOWED_ON_BUST = ("close_day", "new_cycle")
ALLOWED_CLOSE_SCOPES = ("rule", "route")
DEFAULT_ON_TARGET = "close_day"
DEFAULT_ON_BUST = "close_day"
DEFAULT_CLOSE_SCOPE = "rule"
DEFAULT_MAX_CYCLES_PER_DAY = 1

# 写进规则 / 路由当日统计的停止原因；恢复闸门必须认这几个原因，否则保存一次配置就会解封。
CYCLE_STOP_REASON_TARGET = "cycle_target_hit"
CYCLE_STOP_REASON_BUST = "cycle_balance_depleted"
CYCLE_DAY_STOP_REASONS = (CYCLE_STOP_REASON_TARGET, CYCLE_STOP_REASON_BUST)
_WIN_STREAK_TO_CYCLE_REASON = {
    STOP_REASON_WIN_STREAK: CYCLE_STOP_REASON_TARGET,
    STOP_REASON_BALANCE_DEPLETED: CYCLE_STOP_REASON_BUST,
}

# 结构跟随信号：预测组合 -> 结构（主注=组合的单双，辅助=翻转单双，放弃=对角组）
COMBO_TO_STRUCTURE = {"小单": "A", "大单": "B", "小双": "C", "大双": "D"}

__all__ = [
    "MODE_WIN_STREAK_ROLL",
    "apply_period_outcome",
    "ALLOWED_ROLL_MODES",
    "ALLOWED_RESET_SCOPES",
    "STRUCTURE_FOLLOW_SIGNAL",
    "DEFAULT_ROLL_MODE",
    "COMBO_TO_STRUCTURE",
    "normalize_win_streak_roll_policy",
    "normalize_stop_policy",
    "resolve_day_close_action",
    "ALLOWED_ON_TARGET",
    "ALLOWED_ON_BUST",
    "ALLOWED_CLOSE_SCOPES",
    "CYCLE_STOP_REASON_TARGET",
    "CYCLE_STOP_REASON_BUST",
    "CYCLE_DAY_STOP_REASONS",
    "structure_for_period",
    "create_cycle_state",
    "ensure_daily_state",
    "build_period_plan",
    "settle_period",
    "is_stopped",
]


def _round_money(value: Any) -> float:
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _to_positive_float(value: Any, field: str, default: float) -> float:
    if value in (None, ""):
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        raise ValueError("%s 必须为数字" % field)
    if parsed <= 0:
        raise ValueError("%s 必须大于 0" % field)
    return parsed


def normalize_stop_policy(payload: Any, field_prefix: str = "stop_policy") -> Dict[str, Any]:
    """归一化过关玩法的收工开关。

    on_target: close_day 达标即当天收工（默认）
               new_cycle 达标后当天再开一轮（受 max_cycles_per_day 限制）
               continue  达标不处理，连赢计数清零后继续滚
    on_bust:   close_day 爆仓即当天收工（默认）/ new_cycle 爆仓后补足起始注再开一轮
    close_scope: 收工时停到哪一层：rule 整条规则（默认）/ route 只停该路由
    max_cycles_per_day: 一个交易日最多开几轮（含第一轮）
    """
    data = payload if isinstance(payload, dict) else {}
    on_target = str(data.get("on_target") or DEFAULT_ON_TARGET).strip() or DEFAULT_ON_TARGET
    if on_target not in ALLOWED_ON_TARGET:
        raise ValueError("%s.on_target 仅支持 close_day、new_cycle 或 continue" % field_prefix)
    on_bust = str(data.get("on_bust") or DEFAULT_ON_BUST).strip() or DEFAULT_ON_BUST
    if on_bust not in ALLOWED_ON_BUST:
        raise ValueError("%s.on_bust 仅支持 close_day 或 new_cycle" % field_prefix)
    close_scope = str(data.get("close_scope") or DEFAULT_CLOSE_SCOPE).strip() or DEFAULT_CLOSE_SCOPE
    if close_scope not in ALLOWED_CLOSE_SCOPES:
        raise ValueError("%s.close_scope 仅支持 rule 或 route" % field_prefix)
    raw_cycles = data.get("max_cycles_per_day")
    if raw_cycles in (None, ""):
        max_cycles = DEFAULT_MAX_CYCLES_PER_DAY
    else:
        try:
            max_cycles = int(raw_cycles)
        except (TypeError, ValueError):
            raise ValueError("%s.max_cycles_per_day 必须为整数" % field_prefix)
        if max_cycles < 1:
            raise ValueError("%s.max_cycles_per_day 必须 >= 1" % field_prefix)
    return {
        "on_target": on_target,
        "on_bust": on_bust,
        "close_scope": close_scope,
        "max_cycles_per_day": max_cycles,
    }


def normalize_win_streak_roll_policy(payload: Any, field_prefix: str = "staking_policy") -> Dict[str, Any]:
    """校验并归一化 win_streak_roll 玩法配置（订阅 / 路由共用）。"""
    data = payload if isinstance(payload, dict) else {}
    base_amount = _to_positive_float(data.get("base_amount"), "%s.base_amount" % field_prefix, DEFAULT_BASE_AMOUNT)
    main_share = _to_positive_float(data.get("main_share"), "%s.main_share" % field_prefix, DEFAULT_MAIN_SHARE)
    if main_share >= 1:
        raise ValueError("%s.main_share 必须小于 1" % field_prefix)
    aux_share = _to_positive_float(data.get("aux_share"), "%s.aux_share" % field_prefix, round(1 - main_share, 4))
    if abs(main_share + aux_share - 1.0) > 1e-6:
        raise ValueError("%s.main_share + aux_share 必须等于 1" % field_prefix)

    structure = str(data.get("structure") or "A").strip()
    if structure == STRUCTURE_FOLLOW_SIGNAL:
        normalized_structure = STRUCTURE_FOLLOW_SIGNAL
    else:
        try:
            normalized_structure = normalize_structure(structure)
        except ValueError as exc:
            raise ValueError("%s.structure 无效：%s" % (field_prefix, exc)) from exc

    roll_mode = str(data.get("roll_mode") or DEFAULT_ROLL_MODE).strip()
    if roll_mode not in ALLOWED_ROLL_MODES:
        raise ValueError("%s.roll_mode 仅支持 chain 或 all_in" % field_prefix)

    reset_scope = str(data.get("reset_scope") or "daily").strip()
    if reset_scope not in ALLOWED_RESET_SCOPES:
        raise ValueError("%s.reset_scope 仅支持 daily" % field_prefix)

    return {
        "base_amount": round(base_amount, 2),
        "main_share": round(main_share, 4),
        "aux_share": round(aux_share, 4),
        "target_streak": normalize_target_streak(
            data.get("target_streak") if data.get("target_streak") not in (None, "") else DEFAULT_TARGET_STREAK,
            "%s.target_streak" % field_prefix,
        ),
        "structure": normalized_structure,
        "roll_mode": roll_mode,
        "reset_scope": reset_scope,
        "stop_policy": normalize_stop_policy(
            data.get("stop_policy"), field_prefix="%s.stop_policy" % field_prefix
        ),
    }


def structure_for_period(policy: Dict[str, Any], signal: Optional[Dict[str, Any]] = None) -> str:
    """确定本期结构：固定 A/B/C/D，或由 combo 信号推导（follow_signal）。"""
    structure = str((policy or {}).get("structure") or "A")
    if structure != STRUCTURE_FOLLOW_SIGNAL:
        return normalize_structure(structure)
    payload = signal if isinstance(signal, dict) else {}
    normalized = payload.get("normalized_payload")
    merged = dict(normalized) if isinstance(normalized, dict) else {}
    for key in ("bet_type", "bet_value"):
        if key not in merged and key in payload:
            merged[key] = payload[key]
    bet_type = str(merged.get("bet_type") or "").strip()
    bet_value = str(merged.get("bet_value") or "").strip()
    if bet_type == "combo" and bet_value in COMBO_TO_STRUCTURE:
        return COMBO_TO_STRUCTURE[bet_value]
    raise ValueError(
        "structure=follow_signal 需要 combo 类型信号（小单/大单/小双/大双），当前信号: %s/%s"
        % (bet_type or "-", bet_value or "-")
    )


def create_cycle_state(
    policy: Dict[str, Any],
    *,
    stat_date: Optional[str] = None,
    account_balance: Any = None,
) -> Dict[str, Any]:
    """初始化玩法状态。chain 可传入跨日账户余额，all_in 直接使用 base_amount。"""
    roll_mode = str(policy.get("roll_mode") or DEFAULT_ROLL_MODE)
    base_amount = _round_money(policy.get("base_amount") or DEFAULT_BASE_AMOUNT)
    if roll_mode == "chain":
        balance = _round_money(account_balance) if account_balance not in (None, "") else base_amount
        cycle_stake = base_amount
        day_pnl = 0.0
    else:
        balance = base_amount
        cycle_stake = None
        day_pnl = None
    return {
        "stat_date": stat_date,
        "balance": balance,
        "cycle_stake": cycle_stake,
        "day_pnl": day_pnl,
        "win_streak": 0,
        "target_streak": int(policy.get("target_streak") or DEFAULT_TARGET_STREAK),
        "status": STATUS_RUNNING,
        "stop_reason": None,
        "periods": 0,
        # 当天已经走完的轮数（用于 max_cycles_per_day）与收口标记
        "cycles_today": 0,
        "day_closed": False,
        "day_close_scope": None,
        "last_cycle_stop_reason": None,
    }


def ensure_daily_state(policy: Dict[str, Any], state: Optional[Dict[str, Any]], stat_date: str) -> Dict[str, Any]:
    """跨日自动重置：交易日切换时重开一天（chain 保留账户余额，all_in 重置本金）。"""
    current = dict(state) if isinstance(state, dict) else None
    if current and str(current.get("stat_date") or "") == str(stat_date):
        return current
    return create_cycle_state(
        policy,
        stat_date=stat_date,
        account_balance=current.get("balance") if current and str(policy.get("roll_mode") or DEFAULT_ROLL_MODE) == "chain" else None,
    )


def build_period_plan(
    policy: Dict[str, Any],
    state: Dict[str, Any],
    signal: Optional[Dict[str, Any]] = None,
    *,
    stat_date: Optional[str] = None,
) -> Dict[str, Any]:
    """生成本期两笔投注计划（不结算）。"""
    if not isinstance(state, dict):
        raise ValueError("state 必须为玩法状态")
    if stat_date is not None:
        state = ensure_daily_state(policy, state, stat_date)
    if str(state.get("status") or "") == STATUS_STOPPED:
        raise ValueError("当天玩法已停止，不能再产生投注")

    roll_mode = str(policy.get("roll_mode") or DEFAULT_ROLL_MODE)
    stake_total = _round_money(state.get("balance") if roll_mode == "all_in" else state.get("cycle_stake"))
    if stake_total <= 0:
        raise ValueError("当期无可投注余额")
    main_share = float(policy.get("main_share") or DEFAULT_MAIN_SHARE)
    main_stake = _round_money(stake_total * main_share)
    aux_stake = _round_money(stake_total - main_stake)
    if aux_stake < 0:
        main_stake, aux_stake = stake_total, 0.0

    structure = structure_for_period(policy, signal)
    config = STRUCTURES[structure]
    return {
        "state": state,
        "roll_mode": roll_mode,
        "structure": structure,
        "label": config["label"],
        "stake_total": stake_total,
        "main": {
            "bet_type": "odd_even",
            "bet_value": config["main_value"],
            "stake": main_stake,
        },
        "aux": {
            "bet_type": "combo",
            "bet_value": config["aux_value"],
            "stake": aux_stake,
        },
    }


def _settle_leg(
    *,
    stake: float,
    bet_type: str,
    bet_value: str,
    draw_context: Dict[str, Any],
    settlement_rule_id: str,
) -> Dict[str, Any]:
    rule_id = str(settlement_rule_id or DEFAULT_SETTLEMENT_RULE_ID)
    if not get_settlement_rule(rule_id):
        raise ValueError("未知结算规则: %s" % rule_id)
    signal = {"lottery_type": "pc28", "bet_type": bet_type, "bet_value": bet_value}
    result = resolve_pc28_result_for_signal(
        signal=signal,
        settlement_rule_id=rule_id,
        draw_context=draw_context,
    )
    detail = resolve_pc28_settled_hit_detail(
        stake_amount=stake,
        signal=signal,
        settlement_rule_id=rule_id,
        draw_snapshot=result.get("draw_snapshot"),
    )
    result_type = str(result.get("result_type") or "")
    if result_type == "hit":
        payout = _round_money(stake + float(detail["profit"])) if detail else 0.0
    elif result_type == "refund":
        payout = stake
    else:
        payout = 0.0
    return {
        "bet_type": bet_type,
        "bet_value": bet_value,
        "stake": stake,
        "result_type": result_type,
        "refund_reason": result.get("refund_reason"),
        "odds": float(detail["odds"]) if detail else None,
        "payout": payout,
    }


def apply_period_outcome(
    policy: Dict[str, Any],
    state: Dict[str, Any],
    *,
    stake_total: Any,
    payout_total: Any,
) -> Dict[str, Any]:
    """按一整期的实际返还推进玩法状态（派单结算与引擎共用此唯一入口）。

    - chain：账户余额 += 净盈亏；当日累计盈亏累加；下一注 = 本期返还额
    - all_in：余额 = 本期返还额
    - 连赢计数与停止判定复用 win_streak_stop（>0 加关、<0 清零、=0 保持）
    """
    roll_mode = str(policy.get("roll_mode") or DEFAULT_ROLL_MODE)
    stake_amount = _round_money(stake_total)
    payout = _round_money(payout_total)
    net_pnl = _round_money(payout - stake_amount)

    if roll_mode == "chain":
        balance = _round_money(float(state.get("balance") or 0) + net_pnl)
        day_pnl: Optional[float] = _round_money(float(state.get("day_pnl") or 0) + net_pnl)
        cycle_stake: Optional[float] = payout
        risk_capital = cycle_stake
    else:
        balance = payout
        day_pnl = None
        cycle_stake = None
        risk_capital = balance
    if balance < 0:
        balance = 0.0

    tracker = update_after_period(
        {
            "win_streak": state.get("win_streak") or 0,
            "target_streak": state.get("target_streak") or policy.get("target_streak") or DEFAULT_TARGET_STREAK,
            "status": state.get("status") or STATUS_RUNNING,
            "stop_reason": state.get("stop_reason"),
        },
        net_pnl,
        balance=risk_capital,
    )
    next_state = {
        **state,
        "balance": balance,
        "cycle_stake": cycle_stake,
        "day_pnl": day_pnl,
        "win_streak": tracker["win_streak"],
        "status": tracker["status"],
        "stop_reason": tracker["stop_reason"],
        "periods": int(state.get("periods") or 0) + 1,
    }
    if tracker["status"] != STATUS_STOPPED:
        return next_state

    # 周期收口：按 stop_policy 决定当天收工 / 重开一轮 / 继续滚
    stop_policy = normalize_stop_policy((policy or {}).get("stop_policy"))
    cycle_reason = _WIN_STREAK_TO_CYCLE_REASON.get(str(tracker["stop_reason"])) or str(tracker["stop_reason"] or "")
    next_state["last_cycle_stop_reason"] = cycle_reason
    decision = (
        stop_policy["on_target"]
        if str(tracker["stop_reason"]) == STOP_REASON_WIN_STREAK
        else stop_policy["on_bust"]
    )
    cycles_today = int(state.get("cycles_today") or 0)
    if decision == "continue":
        next_state.update({"win_streak": 0, "status": STATUS_RUNNING, "stop_reason": None})
        return next_state
    if decision == "new_cycle" and cycles_today + 1 < int(stop_policy["max_cycles_per_day"]):
        base_amount = _round_money(policy.get("base_amount") or DEFAULT_BASE_AMOUNT)
        restart_balance = balance if roll_mode == "chain" else base_amount
        if restart_balance < base_amount:
            # 爆仓后重开一轮：补足到每天起始注，账户余额口径保持可继续下注
            restart_balance = base_amount
        next_state.update({
            "win_streak": 0,
            "status": STATUS_RUNNING,
            "stop_reason": None,
            "balance": restart_balance,
            "cycle_stake": base_amount if roll_mode == "chain" else None,
            "cycles_today": cycles_today + 1,
        })
        return next_state
    next_state.update({"day_closed": True, "day_close_scope": stop_policy["close_scope"]})
    return next_state


def settle_period(
    policy: Dict[str, Any],
    state: Dict[str, Any],
    draw_context: Dict[str, Any],
    *,
    signal: Optional[Dict[str, Any]] = None,
    settlement_rule_id: str = DEFAULT_SETTLEMENT_RULE_ID,
    stat_date: Optional[str] = None,
) -> Dict[str, Any]:
    """结算一整期并推进玩法状态。"""
    plan = build_period_plan(policy, state, signal, stat_date=stat_date)
    working = plan["state"]
    main_result = _settle_leg(
        stake=plan["main"]["stake"],
        bet_type=plan["main"]["bet_type"],
        bet_value=plan["main"]["bet_value"],
        draw_context=draw_context,
        settlement_rule_id=settlement_rule_id,
    )
    aux_result = _settle_leg(
        stake=plan["aux"]["stake"],
        bet_type=plan["aux"]["bet_type"],
        bet_value=plan["aux"]["bet_value"],
        draw_context=draw_context,
        settlement_rule_id=settlement_rule_id,
    )
    payout_total = _round_money(main_result["payout"] + aux_result["payout"])
    new_state = apply_period_outcome(policy, working, stake_total=plan["stake_total"], payout_total=payout_total)
    net_pnl = _round_money(payout_total - plan["stake_total"])
    return {
        "state": new_state,
        "plan": {key: plan[key] for key in ("structure", "label", "roll_mode", "stake_total", "main", "aux")},
        "main_result": main_result,
        "aux_result": aux_result,
        "payout_total": payout_total,
        "net_pnl": net_pnl,
        "win_streak": new_state["win_streak"],
        "status": new_state["status"],
        "stop_reason": new_state["stop_reason"],
    }


def resolve_day_close_action(policy: Dict[str, Any], state: Dict[str, Any]) -> Dict[str, Any]:
    """周期结算后是否需要收口当日（供结算服务写规则 / 路由当日停止）。"""
    data = state if isinstance(state, dict) else {}
    stop_policy = normalize_stop_policy((policy or {}).get("stop_policy"))
    closed = bool(data.get("day_closed"))
    return {
        "close_day": closed,
        "reason": str(data.get("last_cycle_stop_reason") or "") if closed else "",
        "scope": str(data.get("day_close_scope") or stop_policy["close_scope"]),
        "cycles_today": int(data.get("cycles_today") or 0),
        "max_cycles_per_day": int(stop_policy["max_cycles_per_day"]),
    }


def is_stopped(state: Dict[str, Any]) -> bool:
    return str((state or {}).get("status") or "") == STATUS_STOPPED
