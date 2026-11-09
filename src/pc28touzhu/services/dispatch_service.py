"""Dispatch planning from signals to execution jobs."""
from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from pc28touzhu.domain.auto_trigger_cycle import auto_roll_policy
from pc28touzhu.domain.pc28_play_filter import PLAY_FILTER_KEYS_BY_METRIC, strategy_matches_signal
from pc28touzhu.domain.pc28_group_play import group_selection_for_signal, group_stake_legs
from pc28touzhu.domain.roll_play import SINGLE_PLAY
from pc28touzhu.domain.pc28_issue_window import evaluate_pc28_issue_dispatch_window
from pc28touzhu.domain.settlement_rules import build_settlement_snapshot
from pc28touzhu.domain.win_streak_roll import (
    MODE_WIN_STREAK_ROLL,
    build_period_plan,
    ensure_daily_state,
)
from pc28touzhu.domain.subscription_strategy import (
    hit_rate_gate_stop_reason,
    resolve_dispatch_policy,
    resolve_risk_control_policy,
    resolve_settlement_runtime_policy,
    resolve_staking_runtime_policy,
    upgrade_subscription_strategy,
)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _shanghai_today() -> str:
    """过关滚仓按北京时间自然日重置。"""
    return datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")


def _staking_policy_mode(strategy: Dict[str, Any], signal: Dict[str, Any]) -> str:
    if (strategy.get("staking_policy") or {}).get("mode") == MODE_WIN_STREAK_ROLL and (strategy.get('staking_policy') or {}).get('structure') != SINGLE_PLAY and group_selection_for_signal(signal):
        raise ValueError("双组、杀组不能使用过关结构下注")
    runtime = resolve_staking_runtime_policy(strategy, signal.get("normalized_payload"))
    return str(runtime.get("mode") or "follow_source")


def _parse_iso_z(value: str | None) -> datetime:
    if not value:
        return _utc_now()
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _job_expire_at(
    execute_after: datetime,
    strategy: Dict[str, Any],
    stake_plan: Dict[str, Any],
    issue_window: Dict[str, Any] | None,
) -> datetime:
    seconds = int(resolve_dispatch_policy(strategy).get("expire_after_seconds") or 120)
    expire_at = execute_after + timedelta(seconds=max(30, seconds))
    if issue_window:
        # 任务必须携带固定的投注截止时间，不能因排队或网络阻塞一直有效到开奖后。
        stake_plan.setdefault("meta", {})["issue_window"] = dict(issue_window)
        if issue_window.get("send_before"):
            expire_at = min(expire_at, _parse_iso_z(issue_window["send_before"]))
    return expire_at


def _default_message_text(signal: Dict[str, Any], amount: float) -> str:
    payload = signal.get("normalized_payload") or {}
    custom = str(payload.get("message_text") or "").strip()
    if custom:
        return custom
    amount_text = str(int(amount)) if float(amount).is_integer() else str(amount)
    return "%s%s" % (signal["bet_value"], amount_text)


def _amount_text(amount: float) -> str:
    return str(int(amount)) if float(amount).is_integer() else str(amount)


def _render_format(template_text: str, context: Dict[str, Any]) -> str:
    output = str(template_text or "")
    for key, value in context.items():
        output = output.replace("{{" + str(key) + "}}", str(value))
    return output.strip()


def _message_text_from_template(signal: Dict[str, Any], amount: float, template: Dict[str, Any] | None) -> str:
    group = group_selection_for_signal(signal)
    if group:
        # 来源建议文案对应原始单注；整组逐注用用户的 combo 模板重新生成。
        payload = dict(signal.get("normalized_payload") or {})
        payload.pop("message_text", None)
        return " ".join(
            _message_text_from_template(
                {**signal, "bet_type": "combo", "bet_value": leg["bet_value"], "normalized_payload": payload},
                leg["stake_amount"], template,
            )
            for leg in group_stake_legs(group, amount)
        )
    if not template or str(template.get("status") or "active") != "active":
        return _default_message_text(signal, amount)

    payload = signal.get("normalized_payload") or {}
    config = template.get("config") or {}
    bet_rules = config.get("bet_rules") if isinstance(config.get("bet_rules"), dict) else {}
    rule = bet_rules.get(str(signal.get("bet_type") or "")) or bet_rules.get("*") or {}
    template_text = str(rule.get("format") or template.get("template_text") or "").strip()
    if not template_text:
        return _default_message_text(signal, amount)

    raw_value = str(signal.get("bet_value") or "")
    value_map = rule.get("value_map") if isinstance(rule.get("value_map"), dict) else {}
    rendered_value = str(value_map.get(raw_value, raw_value))
    context = {
        "bet_value": rendered_value,
        "raw_bet_value": raw_value,
        "bet_type": str(signal.get("bet_type") or ""),
        "lottery_type": str(signal.get("lottery_type") or ""),
        "issue_no": str(signal.get("issue_no") or ""),
        "amount": _amount_text(amount),
    }
    for key, value in payload.items():
        if key not in context and value not in (None, ""):
            context[str(key)] = value
    rendered = _render_format(template_text, context)
    return rendered or _default_message_text(signal, amount)


def _resolve_auto_trigger_context(repository: Any, candidate: Dict[str, Any], explicit_context: Dict[str, Any] | None) -> Dict[str, Any] | None:
    context = explicit_context if isinstance(explicit_context, dict) else None
    # 无 route 的兼容模式延续当前 runtime 的明确归属，手动轮次不反查历史规则。
    if not context and hasattr(repository, "get_subscription_runtime_rule_run"):
        run = repository.get_subscription_runtime_rule_run(
            subscription_id=int(candidate["subscription_id"]), user_id=int(candidate["user_id"]),
        )
        if run:
            context = {"rule_id": run["rule_id"], "rule_run_id": run["id"], "stat_date": run["stat_date"]}
            if run.get("status") != "active":
                return {**context, "stopped": True, "stopped_reason": run.get("stop_reason") or "rule_run_stopped"}
    if not context or int(context.get("rule_id") or 0) <= 0 or not str(context.get("stat_date") or "").strip():
        return None
    if hasattr(repository, "get_auto_trigger_rule"):
        owning_rule = repository.get_auto_trigger_rule(int(context["rule_id"]))
        if not owning_rule or owning_rule.get("status") != "active":
            return {**context, "stopped": True, "stopped_reason": "rule_not_active"}
    stat = repository.get_auto_trigger_rule_daily_stat(
        rule_id=int(context["rule_id"]),
        user_id=int(candidate["user_id"]),
        stat_date=str(context["stat_date"]),
    )
    if str(stat.get("status") or "") == "stopped":
        return {**context, "stopped": True, "stopped_reason": str(stat.get("stopped_reason") or "daily_risk_stopped")}
    return {**context, "stopped": False}


def _target_allows_dispatch(repository: Any, target_id: int, *, from_rule: bool) -> bool:
    target = repository.get_delivery_target(target_id)
    if not target or target.get("status") != "active":
        return False
    mode = str(target.get("dispatch_mode") or "shared")
    return mode == "shared" or mode == ("rule_only" if from_rule else "direct_only")


def _subscription_hit_rate_gate_blocked(
    repository: Any,
    *,
    strategy: Dict[str, Any],
    subscription_id: int,
    user_id: int,
) -> bool:
    """直派订阅的当日命中率熔断：当日结算满 N 单且命中率低于阈值时暂停继续直派。

    每次派单前按当日直派账本实时判定，无状态、次日自然恢复；
    未启用或样本不足时放行。
    """
    risk_control = resolve_risk_control_policy(strategy)
    gate = risk_control.get("hit_rate_gate") if isinstance(risk_control.get("hit_rate_gate"), dict) else {}
    if not bool(gate.get("enabled")):
        return False
    if not hasattr(repository, "get_subscription_daily_stat_counts"):
        return False
    stat = repository.get_subscription_daily_stat_counts(
        subscription_id=subscription_id,
        user_id=user_id,
    )
    return bool(
        hit_rate_gate_stop_reason(
            {"hit_rate_gate": gate},
            hit_count=stat.get("hit_count"),
            miss_count=stat.get("miss_count"),
        )
    )


def _frozen_group_signal(signal: Dict[str, Any], event: Dict[str, Any] | None) -> Dict[str, Any]:
    group = ((event or {}).get("settlement_snapshot") or {}).get("group")
    if not isinstance(group, dict) or not group:
        return signal
    return {
        **signal, "bet_type": group["bet_type"], "bet_value": group["bet_value"],
        "normalized_payload": dict(group),
    }


def _build_stake_plan(
    strategy: Dict[str, Any], signal: Dict[str, Any], current_step: int = 1,
    progression_event: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    signal = _frozen_group_signal(signal, progression_event)
    payload = signal.get("normalized_payload") or {}
    runtime_policy = resolve_staking_runtime_policy(strategy, payload)
    raw_mode = str(runtime_policy.get("mode") or "follow_source")
    mode = "martingale" if raw_mode == "martingale" else "follow"
    base_stake = float(runtime_policy.get("base_stake") or runtime_policy.get("fixed_amount") or 10.0)
    multiplier = float(runtime_policy.get("multiplier") or 2.0)
    max_steps = int(runtime_policy.get("max_steps") or 1)
    refund_action = str(runtime_policy.get("refund_action") or "hold").strip() or "hold"
    cap_action = str(runtime_policy.get("cap_action") or "reset").strip() or "reset"
    if raw_mode == "martingale":
        amount = round(base_stake * (multiplier ** max(0, int(current_step or 1) - 1)), 2)
    else:
        amount = float(runtime_policy.get("fixed_amount") or base_stake)
    stake_plan = {
        "mode": mode,
        "amount": amount,
        "base_stake": base_stake,
        "multiplier": multiplier,
        "max_steps": max_steps,
        "refund_action": refund_action,
        "cap_action": cap_action,
        "current_step": max(1, int(current_step or 1)),
    }
    meta = {
        "legacy_stake_amount": float(runtime_policy.get("fixed_amount") or 0) if runtime_policy.get("fixed_amount") is not None else None,
        "staking_policy_mode": raw_mode,
    }
    stake_plan["meta"] = {key: value for key, value in meta.items() if value is not None}
    group = group_selection_for_signal(signal)
    if group:
        total = round(amount * group["unit_count"], 2)
        stake_plan["base_stake"] = round(base_stake * group["unit_count"], 2)
        if progression_event:
            total = float(progression_event["stake_amount"])
            for key in ("base_stake", "multiplier", "max_steps", "refund_action", "cap_action"):
                stake_plan[key] = progression_event[key]
        legs = group_stake_legs(group, total)
        stake_plan.update(amount=total, per_combo_amount=legs[0]["stake_amount"], legs=legs, group_play=group)
    return stake_plan


def _route_effective_strategy(
    base_strategy: Dict[str, Any],
    route: Dict[str, Any],
    auto_trigger_context: Dict[str, Any] | None,
) -> Dict[str, Any]:
    strategy = upgrade_subscription_strategy(base_strategy or {})

    if str(route.get("staking_mode") or "inherit") == "override" and isinstance(route.get("staking_policy"), dict):
        strategy["staking_policy"] = dict(route.get("staking_policy") or {})

    if str(route.get("settlement_mode") or "inherit") == "override" and isinstance(route.get("settlement_policy"), dict):
        strategy["settlement_policy"] = dict(route.get("settlement_policy") or {})

    play_filter_mode = str(route.get("play_filter_mode") or "inherit").strip() or "inherit"
    if play_filter_mode == "inherit":
        rule_action = (auto_trigger_context or {}).get("rule_action")
        rule_action = rule_action if isinstance(rule_action, dict) else {}
        play_filter_mode = str(rule_action.get("play_filter_action") or "keep").strip() or "keep"
        if play_filter_mode == "fixed_metric":
            fixed_metric = str(rule_action.get("fixed_metric") or "").strip()
        else:
            fixed_metric = ""
    else:
        route_play_filter = route.get("play_filter") if isinstance(route.get("play_filter"), dict) else {}
        fixed_metric = str(route_play_filter.get("fixed_metric") or route_play_filter.get("metric") or "").strip()

    target_metric = ""
    if play_filter_mode == "fixed_metric":
        target_metric = fixed_metric
    elif play_filter_mode == "matched_metric":
        matched_conditions = (auto_trigger_context or {}).get("matched_conditions")
        if isinstance(matched_conditions, list) and matched_conditions:
            target_metric = str((matched_conditions[0] or {}).get("metric") or "").strip()

    if target_metric in PLAY_FILTER_KEYS_BY_METRIC:
        if target_metric in {"double_group", "kill_group"} and (strategy.get("staking_policy") or {}).get("mode") == MODE_WIN_STREAK_ROLL and (strategy.get('staking_policy') or {}).get('structure') != SINGLE_PLAY:
            raise ValueError("过关方案不能切换为双组、杀组下注，请使用普通跟单方案")
        if (strategy.get('staking_policy') or {}).get('structure') == SINGLE_PLAY and strategy['staking_policy'].get('play_type') != target_metric:
            strategy['staking_policy'] = {**strategy['staking_policy'], 'play_type': target_metric, 'selection_mode':'follow', 'fixed_value':None}
        strategy["play_filter"] = {
            "mode": "selected",
            "selected_keys": list(PLAY_FILTER_KEYS_BY_METRIC[target_metric]),
        }

    return strategy


def _route_template_id(route: Dict[str, Any]) -> int | None:
    if str(route.get("template_mode") or "target_default") == "override" and route.get("template_id") is not None:
        return int(route["template_id"])
    if route.get("target_template_id") is not None:
        return int(route["target_template_id"])
    return None


def _route_is_stopped(repository: Any, *, route: Dict[str, Any], user_id: int, stat_date: str) -> bool:
    stat = repository.get_auto_trigger_route_daily_stat(
        route_id=int(route["id"]),
        user_id=int(user_id),
        stat_date=stat_date,
    )
    return str(stat.get("status") or "") == "stopped"


def _route_subscription_is_stopped(
    repository: Any,
    *,
    route: Dict[str, Any],
    subscription_id: int,
    user_id: int,
) -> bool:
    if not hasattr(repository, "get_auto_trigger_route_subscription_financial_state"):
        return False
    financial = repository.get_auto_trigger_route_subscription_financial_state(
        route_id=int(route["id"]),
        subscription_id=int(subscription_id),
        user_id=int(user_id),
    )
    return str(financial.get("threshold_status") or "") in {"profit_target_hit", "loss_limit_hit"}


def _auto_trigger_rule_day_is_stopped(
    repository: Any,
    *,
    rule_id: int,
    user_id: int,
    stat_date: str,
) -> bool:
    normalized_stat_date = str(stat_date or "").strip()
    if not normalized_stat_date or not hasattr(repository, "get_auto_trigger_rule_daily_stat"):
        return False
    stat = repository.get_auto_trigger_rule_daily_stat(
        rule_id=int(rule_id),
        user_id=int(user_id),
        stat_date=normalized_stat_date,
    )
    return str(stat.get("status") or "") == "stopped"


def _route_play_filter_mode(route: Dict[str, Any], rule_action: Dict[str, Any] | None) -> str:
    mode = str(route.get("play_filter_mode") or "inherit").strip() or "inherit"
    if mode != "inherit":
        return mode
    action = rule_action if isinstance(rule_action, dict) else {}
    return str(action.get("play_filter_action") or "keep").strip() or "keep"


def _route_continuation_strategy(
    base_strategy: Dict[str, Any],
    route: Dict[str, Any],
    candidate: Dict[str, Any],
) -> Dict[str, Any]:
    rule_action = candidate.get("rule_action") if isinstance(candidate.get("rule_action"), dict) else {}
    started_metric = str(candidate.get("started_signal_bet_type") or "").strip()
    context: Dict[str, Any] = {"rule_action": rule_action}
    if started_metric in PLAY_FILTER_KEYS_BY_METRIC:
        context["matched_conditions"] = [{"metric": started_metric}]
    strategy = _route_effective_strategy(base_strategy, route, context)

    play_filter_mode = _route_play_filter_mode(route, rule_action)
    runtime_play_filter = candidate.get("runtime_play_filter")
    if play_filter_mode not in {"matched_metric", "fixed_metric"} and isinstance(runtime_play_filter, dict):
        mode = str(runtime_play_filter.get("mode") or "all").strip()
        selected_keys = list(runtime_play_filter.get("selected_keys") or [])
        strategy["play_filter"] = {
            "mode": "selected" if mode == "selected" and selected_keys else "all",
            "selected_keys": selected_keys if mode == "selected" and selected_keys else [],
        }
    return strategy


def _route_play_filter_fixed_metric(route: Dict[str, Any]) -> str:
    play_filter = route.get("play_filter") if isinstance(route.get("play_filter"), dict) else {}
    if str(route.get("play_filter_mode") or "inherit").strip() != "fixed_metric":
        return ""
    fixed_metric = str(play_filter.get("fixed_metric") or "").strip()
    return fixed_metric if fixed_metric in PLAY_FILTER_KEYS_BY_METRIC else ""


def _dispatch_signal_for_auto_trigger_routes(
    repository: Any,
    signal: Dict[str, Any],
    *,
    subscription_id: int,
    auto_trigger_context: Dict[str, Any],
    issue_window: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    subscription = repository.get_subscription(int(subscription_id))
    if not subscription:
        raise ValueError("subscription_id 对应的订阅不存在")
    routes = [
        item for item in (auto_trigger_context.get("routes") or [])
        if isinstance(item, dict) and str(item.get("status") or "active") == "active"
    ]
    user_id = int(subscription["user_id"])
    stat_date = str(auto_trigger_context.get("stat_date") or "").strip()

    now = _utc_now()
    base_execute_after = max(_parse_iso_z(signal.get("published_at")), now)
    created_count = 0
    existing_count = 0
    skipped_count = 0
    skipped_routes: list[Dict[str, Any]] = []
    jobs = []

    def _skip_route(route: Dict[str, Any], reason: str, **detail: Any) -> None:
        nonlocal skipped_count
        skipped_count += 1
        entry: Dict[str, Any] = {"route_id": route.get("id"), "reason": reason}
        entry.update(detail)
        skipped_routes.append(entry)

    for route in routes:
        if str(subscription.get("status") or "") not in {"active", "standby"}:
            _skip_route(route, "subscription_not_active", subscription_status=str(subscription.get("status") or ""))
            continue
        route_id = int(route["id"])
        if hasattr(repository, "get_auto_trigger_rule_route"):
            current_route = repository.get_auto_trigger_rule_route(route_id)
            if not current_route or current_route.get("status") != "active":
                _skip_route(route, "route_not_active", route_status=str((current_route or {}).get("status") or ""))
                continue
            route = {**route, **current_route}
        route_stat_date = str(route.get("_auto_trigger_stat_date") or stat_date).strip()
        route_rule_run_id = route.get("_auto_trigger_rule_run_id") or auto_trigger_context.get("rule_run_id")
        delivery_target_id = int(route["delivery_target_id"])
        if not _target_allows_dispatch(repository, delivery_target_id, from_rule=True):
            _skip_route(route, "target_not_dispatchable", delivery_target_id=delivery_target_id)
            continue
        # 候选可能是在用户停轮之前读取的，事务内必须重新核对所属规则轮。
        if route_rule_run_id and hasattr(repository, "get_auto_trigger_rule_run"):
            owning_run = repository.get_auto_trigger_rule_run(int(route_rule_run_id))
            owning_rule = repository.get_auto_trigger_rule(int(auto_trigger_context["rule_id"]))
            if (
                not owning_run or owning_run.get("status") != "active"
                or int(owning_run.get("subscription_id") or 0) != int(subscription_id)
                or int(owning_run.get("user_id") or 0) != user_id
                or int(owning_run.get("rule_id") or 0) != int(auto_trigger_context.get("rule_id") or 0)
                or not owning_rule or owning_rule.get("status") != "active"
            ):
                _skip_route(
                    route,
                    "rule_run_not_active",
                    rule_run_id=int(route_rule_run_id),
                    run_status=str((owning_run or {}).get("status") or ""),
                )
                continue
        if auto_trigger_context.get("rule_id") and _auto_trigger_rule_day_is_stopped(
            repository,
            rule_id=int(auto_trigger_context["rule_id"]),
            user_id=user_id,
            stat_date=route_stat_date or stat_date,
        ):
            _skip_route(route, "rule_day_stopped", stat_date=route_stat_date or stat_date)
            continue
        if auto_trigger_context.get("rule_id") and route_stat_date != _shanghai_today() and _auto_trigger_rule_day_is_stopped(
            repository, rule_id=int(auto_trigger_context["rule_id"]), user_id=user_id, stat_date=_shanghai_today(),
        ):
            _skip_route(route, "rule_day_stopped", stat_date=_shanghai_today())
            continue
        if str(route.get("target_status") or "active") != "active":
            _skip_route(route, "target_not_active", target_status=str(route.get("target_status") or ""))
            continue
        account_status = str(route.get("telegram_account_status") or "active")
        if route.get("telegram_account_id") is not None and account_status != "active":
            _skip_route(route, "telegram_account_not_active", account_status=account_status)
            continue
        if route_stat_date and _route_is_stopped(
            repository,
            route=route,
            user_id=user_id,
            stat_date=route_stat_date,
        ):
            _skip_route(route, "route_daily_risk_stopped", stat_date=route_stat_date)
            continue
        if _route_subscription_is_stopped(
            repository,
            route=route,
            subscription_id=int(subscription_id),
            user_id=user_id,
        ):
            _skip_route(route, "route_subscription_risk_stopped")
            continue
        if repository.auto_trigger_route_has_open_run(
            route_id=route_id,
            subscription_id=int(subscription_id),
            user_id=user_id,
        ).get("has_open_run"):
            _skip_route(route, "route_has_open_run")
            continue

        strategy = (
            route["_runtime_strategy"]
            if isinstance(route.get("_runtime_strategy"), dict)
            else _route_effective_strategy(
                subscription.get("strategy_v2") or subscription.get("strategy") or {},
                route,
                auto_trigger_context,
            )
        )
        if hasattr(repository, "get_auto_trigger_cycle_state"):
            roll_state = repository.get_auto_trigger_cycle_state(rule_id=int(auto_trigger_context["rule_id"]),
                subscription_id=int(subscription_id), user_id=user_id, route_id=route_id)
            if roll_state and int(roll_state.get("rule_run_id") or 0) == int(route_rule_run_id or 0):
                strategy = dict((roll_state.get("policy") or {}).get("_strategy_snapshot") or strategy)
        # 玩法不一致时按路由固定玩法在同期号内换选信号：触发选单取的是“最新一条”，
        # 逐期发多种玩法的源会拿到非路由玩法（如最新是组合、路由只收单双），
        # 同期里通常有匹配玩法的信号，换选即可派单；换不到才按玩法不匹配跳过。
        route_signal = signal
        signal_substitution: Dict[str, Any] | None = None
        if not strategy_matches_signal(strategy, signal):
            route_metric = _route_play_filter_fixed_metric(route)
            if (strategy.get('staking_policy') or {}).get('structure') == SINGLE_PLAY:
                route_metric = strategy['staking_policy']['play_type']
            if not route_metric:
                rule_action = (
                    auto_trigger_context.get("rule_action")
                    if isinstance(auto_trigger_context.get("rule_action"), dict)
                    else {}
                )
                if str(rule_action.get("play_filter_action") or "keep").strip() == "fixed_metric":
                    metric = str(rule_action.get("fixed_metric") or "").strip()
                    route_metric = metric if metric in PLAY_FILTER_KEYS_BY_METRIC else ""
            if (
                route_metric
                and str(signal.get("bet_type") or "") != route_metric
                and hasattr(repository, "get_latest_ready_signal_for_source")
            ):
                candidate_signal = repository.get_latest_ready_signal_for_source(
                    source_id=int(subscription["source_id"]),
                    issue_no=str(signal.get("issue_no") or ""),
                    bet_type=route_metric,
                )
                if candidate_signal and strategy_matches_signal(strategy, candidate_signal):
                    route_signal = candidate_signal
                    signal_substitution = {
                        "bet_type": route_metric,
                        "signal_id": int(candidate_signal["id"]),
                        "bet_value": str(candidate_signal.get("bet_value") or ""),
                    }
        if not strategy_matches_signal(strategy, route_signal):
            _skip_route(
                route,
                "play_filter_mismatch",
                signal_bet_type=str(route_signal.get("bet_type") or ""),
                signal_bet_value=str(route_signal.get("bet_value") or ""),
                play_filter=(strategy.get("play_filter") if isinstance(strategy.get("play_filter"), dict) else {}),
                signal_substitution=signal_substitution,
            )
            continue
        if hasattr(repository, "execution_job_exists_for_target") and repository.execution_job_exists_for_target(
            signal_id=int(route_signal["id"]),
            subscription_id=int(subscription_id),
            delivery_target_id=delivery_target_id,
            from_route=False,
        ):
            # 同一信号已由其他 route 或直派占用该目标。
            _skip_route(route, "target_already_dispatched", signal_id=int(route_signal["id"]))
            continue

        if _staking_policy_mode(strategy, route_signal) == MODE_WIN_STREAK_ROLL:
            # 过关滚仓玩法：同期派两腿（主注+辅助），注码由玩法周期状态决定。
            template = None
            template_id = _route_template_id(route)
            if template_id is not None and hasattr(repository, "get_message_template"):
                template = repository.get_message_template(template_id)
            outcome = _dispatch_win_streak_roll_legs(
                repository,
                subscription_id=int(subscription_id),
                user_id=user_id,
                delivery_target_id=delivery_target_id,
                telegram_account_id=(
                    int(route["telegram_account_id"]) if route.get("telegram_account_id") is not None else None
                ),
                executor_type=str(route.get("executor_type") or "telegram_group"),
                strategy=strategy,
                signal=route_signal,
                template=template,
                issue_window=issue_window,
                auto_trigger_context=auto_trigger_context,
                route_id=route_id,
                route_rule_id=int(auto_trigger_context.get("rule_id") or 0) or None,
                route_rule_run_id=route_rule_run_id,
                route_stat_date=route_stat_date,
            )
            created_count += int(outcome.get("created_count") or 0)
            existing_count += int(outcome.get("existing_count") or 0)
            if int(outcome.get("skipped_count") or 0):
                _skip_route(route, str(outcome.get("skip_reason") or "cycle_skipped"))
            skipped_count += int(outcome.get("skipped_count") or 0)
            jobs.extend(outcome.get("jobs") or [])
            continue

        progression_event = repository.get_progression_event_by_signal(
            subscription_id=int(subscription_id),
            signal_id=int(route_signal["id"]),
            auto_trigger_route_id=route_id,
        )
        if not progression_event:
            progression_state = repository.get_auto_trigger_route_progression_state(
                route_id=route_id,
                subscription_id=int(subscription_id),
                user_id=user_id,
            )
            stake_plan_preview = _build_stake_plan(
                strategy,
                route_signal,
                current_step=int(progression_state.get("current_step") or 1),
            )
            settlement_policy = resolve_settlement_runtime_policy(strategy, route_signal.get("normalized_payload"))
            progression_event = repository.create_progression_event_record(
                subscription_id=int(subscription_id),
                user_id=user_id,
                signal_id=int(route_signal["id"]),
                issue_no=str(route_signal.get("issue_no") or ""),
                progression_step=int(stake_plan_preview["current_step"]),
                stake_amount=float(stake_plan_preview["amount"]),
                base_stake=float(stake_plan_preview["base_stake"]),
                multiplier=float(stake_plan_preview["multiplier"]),
                max_steps=int(stake_plan_preview["max_steps"]),
                refund_action=str(stake_plan_preview["refund_action"]),
                cap_action=str(stake_plan_preview["cap_action"]),
                settlement_rule_id=str(settlement_policy.get("settlement_rule_id") or ""),
                settlement_snapshot=build_settlement_snapshot(
                    rule_source=str(settlement_policy.get("rule_source") or ""),
                    settlement_rule_id=settlement_policy.get("settlement_rule_id"),
                    fallback_profit_ratio=float(settlement_policy.get("fallback_profit_ratio") or 1.0),
                    resolved_from=str(settlement_policy.get("resolved_from") or ""),
                    signal=route_signal,
                    odds_overrides=settlement_policy.get("odds_overrides"),
                    stake_amount=float(stake_plan_preview["amount"]),
                ),
                auto_trigger_rule_id=(
                    int(auto_trigger_context["rule_id"]) if auto_trigger_context.get("rule_id") else None
                ),
                auto_trigger_rule_run_id=(
                    int(route_rule_run_id) if route_rule_run_id else None
                ),
                auto_trigger_route_id=route_id,
                auto_trigger_stat_date=route_stat_date,
                runtime_strategy=strategy,
                status="pending",
            )

        current_step = int((progression_event or {}).get("progression_step") or 1)
        stake_plan = _build_stake_plan(strategy, route_signal, current_step=current_step, progression_event=progression_event)
        stake_plan.setdefault("meta", {})
        stake_plan["meta"].update({
            "progression_event_id": int(progression_event["id"]),
            "progression_step": current_step,
            "auto_trigger_route_id": route_id,
        })
        if signal_substitution is not None:
            stake_plan["meta"]["signal_substitution"] = signal_substitution

        template = None
        template_id = _route_template_id(route)
        if template_id is not None and hasattr(repository, "get_message_template"):
            template = repository.get_message_template(template_id)
        planned_message_text = _message_text_from_template(_frozen_group_signal(route_signal, progression_event), float(stake_plan["amount"]), template)
        execute_after = base_execute_after
        expire_at = _job_expire_at(execute_after, strategy, stake_plan, issue_window)
        idempotency_key = "signal:%s:rule:%s:route:%s:target:%s" % (
            route_signal["id"],
            auto_trigger_context.get("rule_id") or "",
            route_id,
            delivery_target_id,
        )

        created = repository.create_execution_job_record(
            user_id=user_id,
            signal_id=int(route_signal["id"]),
            subscription_id=int(subscription_id),
            progression_event_id=int(progression_event["id"]),
            auto_trigger_route_id=route_id,
            delivery_target_id=delivery_target_id,
            telegram_account_id=(
                int(route["telegram_account_id"]) if route.get("telegram_account_id") is not None else None
            ),
            executor_type=str(route.get("executor_type") or "telegram_group"),
            idempotency_key=idempotency_key,
            planned_message_text=planned_message_text,
            stake_plan=stake_plan,
            execute_after=_iso_z(execute_after),
            expire_at=_iso_z(expire_at),
            leg_key="group" if stake_plan.get("group_play") else "",
        )
        jobs.append(created["job"])
        if created["created"]:
            created_count += 1
        else:
            existing_count += 1

    return {
        "signal_id": int(signal["id"]),
        "subscription_id": int(subscription_id),
        "candidate_count": len(routes),
        "created_count": created_count,
        "existing_count": existing_count,
        "skipped_count": skipped_count,
        "skipped_routes": skipped_routes,
        "jobs": jobs,
    }


def _dispatch_signal_for_active_auto_trigger_routes(
    repository: Any, signal: Dict[str, Any], *, issue_window: Dict[str, Any] | None = None,
    retry_only: bool = False,
) -> Dict[str, Any]:
    if not hasattr(repository, "list_active_auto_trigger_route_dispatch_candidates"):
        return {
            "signal_id": int(signal["id"]),
            "candidate_count": 0,
            "created_count": 0,
            "existing_count": 0,
            "skipped_count": 0,
            "jobs": [],
        }

    candidates = repository.list_active_auto_trigger_route_dispatch_candidates(int(signal["id"]))
    created_count = 0
    existing_count = 0
    skipped_count = 0
    skipped_routes: list[Dict[str, Any]] = []
    jobs = []

    for candidate in candidates:
        route_id_hint = int((candidate.get("route") or {}).get("id") or 0)

        def _skip(reason: str = "", **detail: Any) -> None:
            nonlocal skipped_count
            skipped_count += 1
            if reason:
                skipped_routes.append({"route_id": route_id_hint, "reason": reason, **detail})

        if retry_only and (
            not candidate.get("runtime_started_signal_id")
            or int(signal["id"]) < int(candidate["runtime_started_signal_id"])
        ):
            # 重试时重新核对轮次，不能把旧轮次的信号补进新开轮次。
            _skip("runtime_run_stale")
            continue
        route = candidate.get("route") if isinstance(candidate.get("route"), dict) else {}
        if not route:
            _skip("route_missing")
            continue
        if not candidate.get("rule_run_id"):
            _skip("rule_run_missing")
            continue
        if _auto_trigger_rule_day_is_stopped(
            repository,
            rule_id=int(route["rule_id"]),
            user_id=int(candidate["user_id"]),
            stat_date=str(candidate.get("stat_date") or ""),
        ):
            _skip("rule_day_stopped", stat_date=str(candidate.get("stat_date") or ""))
            continue
        strategy = _route_continuation_strategy(
            candidate.get("strategy_json") if isinstance(candidate.get("strategy_json"), dict) else {},
            route,
            candidate,
        )
        if hasattr(repository, "get_auto_trigger_cycle_state"):
            roll_state = repository.get_auto_trigger_cycle_state(rule_id=int(route["rule_id"]),
                subscription_id=int(candidate["subscription_id"]), user_id=int(candidate["user_id"]), route_id=int(route["id"]))
            if roll_state and int(roll_state.get("rule_run_id") or 0) == int(candidate.get("rule_run_id") or 0):
                strategy = dict((roll_state.get("policy") or {}).get("_strategy_snapshot") or strategy)
        if not strategy_matches_signal(strategy, signal):
            _skip(
                "play_filter_mismatch",
                signal_bet_type=str(signal.get("bet_type") or ""),
                signal_bet_value=str(signal.get("bet_value") or ""),
                play_filter=(strategy.get("play_filter") if isinstance(strategy.get("play_filter"), dict) else {}),
            )
            continue
        route_with_strategy = {**route, "_runtime_strategy": strategy}
        context = {
            "rule_id": int(route["rule_id"]),
            "rule_run_id": candidate.get("rule_run_id"),
            "stat_date": str(candidate.get("stat_date") or ""),
            "routes": [route_with_strategy],
            "rule_action": candidate.get("rule_action") if isinstance(candidate.get("rule_action"), dict) else {},
        }
        started_metric = str(candidate.get("started_signal_bet_type") or "").strip()
        if started_metric in PLAY_FILTER_KEYS_BY_METRIC:
            context["matched_conditions"] = [{"metric": started_metric}]

        result = _dispatch_signal_for_auto_trigger_routes(
            repository,
            signal,
            subscription_id=int(candidate["subscription_id"]),
            auto_trigger_context=context,
            issue_window=issue_window,
        )
        created_count += int(result.get("created_count") or 0)
        existing_count += int(result.get("existing_count") or 0)
        skipped_count += int(result.get("skipped_count") or 0)
        skipped_routes.extend(result.get("skipped_routes") or [])
        jobs.extend(result.get("jobs") or [])

    return {
        "signal_id": int(signal["id"]),
        "candidate_count": len(candidates),
        "created_count": created_count,
        "existing_count": existing_count,
        "skipped_count": skipped_count,
        "skipped_routes": skipped_routes,
        "jobs": jobs,
    }


def _candidate_matches_subscription_targets(candidate: Dict[str, Any]) -> bool:
    strategy = candidate.get("strategy_json")
    if isinstance(strategy, str):
        strategy = {}
    dispatch = strategy.get("dispatch") if isinstance(strategy, dict) and isinstance(strategy.get("dispatch"), dict) else {}
    target_ids = dispatch.get("delivery_target_ids")
    if not isinstance(target_ids, list) or not target_ids:
        return True
    try:
        delivery_target_id = int(candidate.get("delivery_target_id"))
    except (TypeError, ValueError):
        return False
    normalized_ids = set()
    for target_id in target_ids:
        try:
            normalized_ids.add(int(target_id))
        except (TypeError, ValueError):
            continue
    return delivery_target_id in normalized_ids


def _dispatch_win_streak_roll_legs(
    repository: Any,
    *,
    subscription_id: int,
    user_id: int,
    delivery_target_id: int,
    telegram_account_id: int | None,
    executor_type: str,
    strategy: Dict[str, Any],
    signal: Dict[str, Any],
    template: Dict[str, Any] | None,
    issue_window: Dict[str, Any] | None = None,
    auto_trigger_context: Dict[str, Any] | None = None,
    route_id: int | None = None,
    route_rule_id: int | None = None,
    route_rule_run_id: int | None = None,
    route_stat_date: str = "",
) -> Dict[str, Any]:
    """过关滚仓玩法派单：同期派两腿（主注单双 70% + 辅助组合 30%），共享一个玩法周期状态。

    - 每个订阅每天一个周期；止盈/止损后当天跳过（skip_reason=cycle_stopped）
    - 上一期未结算前不再派新周期（skip_reason=cycle_pending_settlement），防止同订阅并发双周期
    - 两腿各建一条 progression_event（leg_key 区分），结算完全复用现有 progression 财务/日报链路
    """
    automatic_rule_id = int((auto_trigger_context or {}).get("rule_id") or route_rule_id or 0)
    automatic = automatic_rule_id > 0 and hasattr(repository, "get_auto_trigger_cycle_state")
    cycle_owner = {"rule_id": automatic_rule_id, "subscription_id": subscription_id, "user_id": user_id, "route_id": int(route_id or 0)}
    today = str(route_stat_date or (auto_trigger_context or {}).get("stat_date") or _shanghai_today()) if automatic else _shanghai_today()
    state_row = (repository.get_auto_trigger_cycle_state(**cycle_owner) if automatic else
        repository.get_subscription_cycle_state(subscription_id=subscription_id)
        if hasattr(repository, "get_subscription_cycle_state") else None)
    if not automatic and str((state_row or {}).get("stop_reason") or "").startswith("migration_"):
        return {"created_count": 0, "existing_count": 0, "skipped_count": 1, "skip_reason": "migration_owner_blocked", "jobs": []}
    if automatic:
        owning_rule = repository.get_auto_trigger_rule(automatic_rule_id) or {}
        current_run_id = int(route_rule_run_id or (auto_trigger_context or {}).get("rule_run_id") or 0)
        owning_run = repository.get_auto_trigger_rule_run(current_run_id) if current_run_id else None
        if owning_rule.get("status") != "active" or not owning_run or owning_run.get("status") != "active":
            return {"created_count": 0, "existing_count": 0, "skipped_count": 1, "skip_reason": "rule_run_not_active", "jobs": []}
        if _auto_trigger_rule_day_is_stopped(repository, rule_id=automatic_rule_id, user_id=user_id, stat_date=_shanghai_today()):
            return {"created_count": 0, "existing_count": 0, "skipped_count": 1, "skip_reason": "rule_day_stopped", "jobs": []}
        session = repository.get_auto_trigger_cycle_session(rule_id=automatic_rule_id, subscription_id=subscription_id, user_id=user_id) or {}
        if session and session.get("status") != "running":
            return {"created_count": 0, "existing_count": 0, "skipped_count": 1, "skip_reason": "cycle_not_running", "jobs": []}
        current_run_id = int(route_rule_run_id or (auto_trigger_context or {}).get("rule_run_id") or 0)
        if session and (int(session.get("active_run_id") or 0) != current_run_id or int(route_id or 0) not in (session.get("participating_route_ids") or [])):
            return {"created_count": 0, "existing_count": 0, "skipped_count": 1, "skip_reason": "cycle_run_stale", "jobs": []}
        if state_row:
            today = str(state_row.get("stat_date") or today)
            strategy = dict((state_row.get("policy") or {}).get("_strategy_snapshot") or strategy)
    # 任意归属已占用该期该群时不能为另一归属留下孤儿event或消耗余额。
    if hasattr(repository, "execution_job_exists_for_target") and repository.execution_job_exists_for_target(
        signal_id=int(signal["id"]), subscription_id=subscription_id, delivery_target_id=delivery_target_id, from_route=automatic,
    ):
        return {"created_count": 0, "existing_count": 0, "skipped_count": 1, "skip_reason": "target_already_dispatched", "jobs": []}
    policy = dict((state_row or {}).get("policy") or {})
    if not policy and automatic:
        owning_rule = repository.get_auto_trigger_rule(automatic_rule_id) or {}
        policy = auto_roll_policy(strategy, owning_rule.get("cycle_policy") or {"mode": "legacy"}, session.get("legacy_policy"))
    if not policy:
        runtime = resolve_staking_runtime_policy(strategy, signal.get("normalized_payload"))
        policy = {
            key: runtime[key]
            for key in ("base_amount", "main_share", "aux_share", "target_streak", "structure", "roll_mode", "reset_scope", "stop_policy", 'play_type', 'selection_mode', 'fixed_value')
            if key in runtime
        }
    previous_state = None
    if state_row:
        previous_state = {
            key: state_row[key]
            for key in (
                "stat_date", "balance", "cycle_stake", "day_pnl", "win_streak", "target_streak",
                "status", "stop_reason", "periods",
                "cycles_today", "day_closed", "day_close_scope", "last_cycle_stop_reason",
            )
        }
    state = ensure_daily_state(policy, previous_state, today)
    if str(state.get("status") or "running") == "stopped":
        return {"created_count": 0, "existing_count": 0, "skipped_count": 1, "skip_reason": "cycle_stopped", "jobs": []}
    pending = (state_row or {}).get("pending_plan")
    if pending:
        return {"created_count": 0, "existing_count": 0, "skipped_count": 1, "skip_reason": "cycle_pending_settlement", "jobs": []}

    try:
        plan = build_period_plan(policy, state, signal, stat_date=today)
    except ValueError as exc:
        # 结算时已按资金下限收口，这里只做兜底：资金凑不出两注就不出单，别让评估记 failed
        if "资金不足" not in str(exc) and "无可投注余额" not in str(exc):
            raise
        return {
            "created_count": 0, "existing_count": 0, "skipped_count": 1,
            "skip_reason": "cycle_insufficient_stake", "jobs": [],
        }
    settlement_policy = resolve_settlement_runtime_policy(strategy, signal.get("normalized_payload"))
    settlement_rule_id = str(settlement_policy.get("settlement_rule_id") or "")

    now = _utc_now()
    base_execute_after = max(_parse_iso_z(signal.get("published_at")), now)
    execute_after = base_execute_after

    legs = [{**plan[key], 'leg_key': key} for key in ('main', 'aux') if key in plan]
    leg_event_ids: list[int] = []
    jobs = []
    created_count = 0
    existing_count = 0
    for leg in legs:
        leg_signal = dict(signal)
        leg_signal["bet_type"] = leg["bet_type"]
        leg_signal["bet_value"] = leg["bet_value"]
        leg_signal["normalized_payload"] = {
            **(leg.get('normalized_payload') if 'normalized_payload' in leg else (signal.get('normalized_payload') or {})),
            "bet_type": leg["bet_type"],
            "bet_value": leg["bet_value"],
        }
        # 来源文案对应原始信号，不能覆盖过关重新计算出的玩法和注码。
        leg_signal["normalized_payload"].pop("message_text", None)
        settlement_snapshot = build_settlement_snapshot(
            rule_source=str(settlement_policy.get("rule_source") or ""),
            settlement_rule_id=settlement_policy.get("settlement_rule_id"),
            fallback_profit_ratio=float(settlement_policy.get("fallback_profit_ratio") or 1.0),
            resolved_from=str(settlement_policy.get("resolved_from") or ""),
            signal=leg_signal,
            odds_overrides=settlement_policy.get("odds_overrides"),
            stake_amount=float(leg['stake']),
        )
        progression_event = repository.create_progression_event_record(
            subscription_id=subscription_id,
            user_id=user_id,
            signal_id=int(signal["id"]),
            issue_no=str(signal.get("issue_no") or ""),
            progression_step=1,
            stake_amount=float(leg["stake"]),
            base_stake=float(leg["stake"]),
            multiplier=2.0,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            settlement_rule_id=settlement_rule_id,
            settlement_snapshot=settlement_snapshot,
            auto_trigger_rule_id=(
                int(auto_trigger_context["rule_id"])
                if auto_trigger_context and auto_trigger_context.get("rule_id")
                else route_rule_id
            ),
            auto_trigger_rule_run_id=(
                int(auto_trigger_context["rule_run_id"])
                if auto_trigger_context and auto_trigger_context.get("rule_run_id")
                else route_rule_run_id
            ),
            auto_trigger_route_id=route_id,
            auto_trigger_stat_date=route_stat_date or "",
            runtime_strategy=strategy,
            status="pending",
            leg_key=leg["leg_key"],
            result_context={
                "leg": {
                    "bet_type": leg["bet_type"],
                    "bet_value": leg["bet_value"],
                    "stake": float(leg["stake"]),
                },
                "cycle": {
                    "stat_date": today,
                    "base_amount": policy.get("base_amount"),
                    "target_streak": policy.get("target_streak"),
                    "structure": plan["structure"],
                    "roll_mode": plan["roll_mode"],
                },
            },
        )
        leg_event_ids.append(int(progression_event["id"]))
        stake_plan = {
            "mode": MODE_WIN_STREAK_ROLL,
            "leg": leg["leg_key"],
            "amount": float(leg["stake"]),
            "base_amount": policy.get("base_amount"),
            "target_streak": policy.get("target_streak"),
            "structure": plan["structure"],
            "roll_mode": plan["roll_mode"],
            "meta": {
                "progression_event_id": int(progression_event["id"]),
                "cycle_stat_date": today,
            },
        }
        if settlement_snapshot.get('group'):
            group = settlement_snapshot['group']
            stake_plan.update(group_play=group, legs=group['legs'], unit_count=group['unit_count'],
                              per_combo_amount=group['legs'][0]['stake_amount'], stake_basis='per_combo')
        if route_id is not None:
            stake_plan["meta"]["auto_trigger_route_id"] = int(route_id)
        planned_message_text = _message_text_from_template(leg_signal, float(leg["stake"]), template)
        if route_id is not None:
            idempotency_key = "signal:%s:subscription:%s:rule:%s:route:%s:target:%s:leg:%s" % (
                signal["id"], subscription_id, auto_trigger_context.get("rule_id") or "", route_id, delivery_target_id, leg["leg_key"],
            )
        else:
            idempotency_key = "signal:%s:subscription:%s:target:%s:leg:%s" % (signal["id"], subscription_id, delivery_target_id, leg["leg_key"])
        expire_at = _job_expire_at(execute_after, strategy, stake_plan, issue_window)
        created = repository.create_execution_job_record(
            user_id=user_id,
            signal_id=int(signal["id"]),
            subscription_id=subscription_id,
            progression_event_id=int(progression_event["id"]),
            auto_trigger_route_id=route_id,
            delivery_target_id=delivery_target_id,
            telegram_account_id=telegram_account_id,
            executor_type=executor_type,
            idempotency_key=idempotency_key,
            planned_message_text=planned_message_text,
            stake_plan=stake_plan,
            execute_after=_iso_z(execute_after),
            expire_at=_iso_z(expire_at),
            leg_key=leg["leg_key"],
        )
        jobs.append(created["job"])
        if created["created"]:
            created_count += 1
        else:
            existing_count += 1

    # 两笔账保持独立；执行器只领取主注，并携带完整的合并文案。
    if len(jobs) == 2:
        repository.combine_roll_execution_jobs([int(job["id"]) for job in jobs])
        jobs[1]["send_with_job_id"] = int(jobs[0]["id"])

    pending_plan = {
        "signal_id": int(signal["id"]),
        "issue_no": str(signal.get("issue_no") or ""),
        "stat_date": today,
        "stake_total": float(plan["stake_total"]),
        "structure": plan["structure"],
        "roll_mode": plan["roll_mode"],
        "settlement_rule_id": settlement_rule_id,
        "leg_event_ids": leg_event_ids,
        "state": state,
        "created_at": _iso_z(now),
        # 收工时要写哪条规则的当日统计，这里先记下来，
        # 免得结算进程再去反查（两条规则共用一个订阅时反查容易串）。
        "rule_id": int(auto_trigger_context.get("rule_id") or 0) if auto_trigger_context else int(route_rule_id or 0),
        "route_id": int(route_id) if route_id is not None else 0,
        "rule_run_id": int(
            route_rule_run_id
            or (auto_trigger_context.get("rule_run_id") if auto_trigger_context else 0)
            or 0
        ) or None,
        "rule_stat_date": str(route_stat_date or (auto_trigger_context.get("stat_date") if auto_trigger_context else "") or ""),
        "user_id": int(user_id),
    }
    if automatic:
        repository.upsert_auto_trigger_cycle_state(**cycle_owner,
            rule_run_id=int(route_rule_run_id or (auto_trigger_context or {}).get("rule_run_id") or 0) or None,
            state=state, policy=policy, pending_plan=pending_plan)
    elif hasattr(repository, "upsert_subscription_cycle_state"):
        repository.upsert_subscription_cycle_state(
            subscription_id=subscription_id,
            user_id=user_id,
            state=state,
            policy=policy,
            pending_plan=pending_plan,
        )
    return {
        "created_count": created_count,
        "existing_count": existing_count,
        "skipped_count": 0,
        "jobs": jobs,
    }


def dispatch_signal(
    repository: Any,
    signal_id: int,
    *,
    subscription_id: int | None = None,
    auto_trigger_context: Dict[str, Any] | None = None,
    draw_clock: Dict[str, Any] | None = None,
    retry_active_routes_only: bool = False,
) -> Dict[str, Any]:
    transaction = repository.transaction() if hasattr(repository, "transaction") else nullcontext(repository)
    with transaction as unit:
        return _dispatch_signal(
            unit, signal_id, subscription_id=subscription_id,
            auto_trigger_context=auto_trigger_context, draw_clock=draw_clock,
            retry_active_routes_only=retry_active_routes_only,
        )


def _dispatch_signal(
    repository: Any,
    signal_id: int,
    *,
    subscription_id: int | None = None,
    auto_trigger_context: Dict[str, Any] | None = None,
    draw_clock: Dict[str, Any] | None = None,
    retry_active_routes_only: bool = False,
) -> Dict[str, Any]:
    signal = repository.get_signal(signal_id)
    if not signal:
        raise ValueError("signal 不存在")

    # 封盘闸：目标期已开奖或余量不足时整单不派，避免被投注群顺延记账到下一期。
    # draw_clock 由调用方注入，未注入即不启用。
    issue_window = evaluate_pc28_issue_dispatch_window(
        issue_no=signal.get("issue_no"),
        draw_clock=draw_clock,
        now=_utc_now(),
    )
    if not issue_window["allowed"] or (retry_active_routes_only and issue_window["reason"] != "ok"):
        # 拦截落标记供重试扫描补派（典型场景：开奖接口 countdown 字段在开奖后短暂未跳新值，
        # 余量被低估）；verdict 快照一并落库留作核查凭据
        if not retry_active_routes_only and hasattr(repository, "mark_signal_dispatch_blocked"):
            repository.mark_signal_dispatch_blocked(
                int(signal["id"]),
                reason=str(issue_window.get("reason") or ""),
                verdict=issue_window,
            )
        return {
            "signal_id": int(signal["id"]),
            "subscription_id": int(subscription_id) if subscription_id is not None else None,
            "candidate_count": 0,
            "created_count": 0,
            "existing_count": 0,
            "skipped_count": 0,
            "jobs": [],
            "blocked": True,
            "block_reason": str(issue_window["reason"]),
            "issue_window": issue_window,
        }
    if retry_active_routes_only:
        source = repository.get_source(int(signal["source_id"]))
        if not source or source.get("status") != "active" or signal.get("status") != "ready":
            return {"signal_id": int(signal_id), "created_count": 0, "existing_count": 0, "skipped_count": 1, "jobs": []}
        # 重试限定在当前自动规则路由，避免停轮后回退到订阅直派。
        result = _dispatch_signal_for_active_auto_trigger_routes(
            repository, signal, issue_window=issue_window, retry_only=True,
        )
        if result["created_count"] and hasattr(repository, "clear_signal_dispatch_blocked"):
            repository.clear_signal_dispatch_blocked(int(signal_id))
        return result
    if hasattr(repository, "clear_signal_dispatch_blocked"):
        repository.clear_signal_dispatch_blocked(int(signal["id"]))

    if (
        subscription_id is not None
        and isinstance(auto_trigger_context, dict)
        and isinstance(auto_trigger_context.get("routes"), list)
    ):
        return _dispatch_signal_for_auto_trigger_routes(
            repository,
            signal,
            subscription_id=int(subscription_id),
            auto_trigger_context=auto_trigger_context,
            issue_window=issue_window,
        )

    active_route_result = _dispatch_signal_for_active_auto_trigger_routes(repository, signal, issue_window=issue_window)
    raw_candidates = (
        repository.list_dispatch_candidates_for_subscription(signal_id, subscription_id=subscription_id)
        if subscription_id is not None and hasattr(repository, "list_dispatch_candidates_for_subscription")
        else repository.list_dispatch_candidates(signal_id)
    )
    for item in raw_candidates:
        current_strategy = item.get('strategy_json') or {}
        if (current_strategy.get('staking_policy') or {}).get('mode') == MODE_WIN_STREAK_ROLL and hasattr(repository, 'get_subscription_cycle_state'):
            manual_state = repository.get_subscription_cycle_state(subscription_id=int(item['subscription_id'])) or {}
            frozen_policy = manual_state.get('policy') or {}
            if frozen_policy.get('structure') == SINGLE_PLAY:
                item['strategy_json'] = {**current_strategy, 'staking_policy': {**current_strategy.get('staking_policy', {}), **frozen_policy}}
    candidates = [
        item for item in raw_candidates
        if strategy_matches_signal(item.get("strategy_json"), signal)
        and _candidate_matches_subscription_targets(item)
    ]
    now = _utc_now()
    base_execute_after = max(_parse_iso_z(signal.get("published_at")), now)
    base_expire_at = base_execute_after + timedelta(minutes=2)

    created_count = 0
    existing_count = 0
    skipped_count = 0
    jobs = []
    progression_events: dict[int, Dict[str, Any] | None] = {}

    for candidate in candidates:
        strategy = candidate.get("strategy_json")
        if isinstance(strategy, str):
            # defensive; repository currently returns dict rows only
            strategy = {}
        strategy = strategy or {}
        subscription_id = int(candidate["subscription_id"])
        resolved_auto_trigger_context = _resolve_auto_trigger_context(repository, candidate, auto_trigger_context)
        if not _target_allows_dispatch(
            repository, int(candidate["delivery_target_id"]), from_rule=bool(resolved_auto_trigger_context),
        ):
            skipped_count += 1
            continue
        if resolved_auto_trigger_context and resolved_auto_trigger_context.get("stopped"):
            skipped_count += 1
            continue
        if not resolved_auto_trigger_context and _subscription_hit_rate_gate_blocked(
            repository, strategy=strategy, subscription_id=subscription_id, user_id=int(candidate["user_id"]),
        ):
            # 直派订阅当日命中率熔断：路由单有自己的规则/路由风控，不走这条闸。
            skipped_count += 1
            continue
        if _staking_policy_mode(strategy, signal) == MODE_WIN_STREAK_ROLL:
            # 过关滚仓玩法：同期派两腿（主注+辅助），注码由玩法周期状态决定。
            template = None
            if candidate.get("template_id") is not None and hasattr(repository, "get_message_template"):
                template = repository.get_message_template(int(candidate["template_id"]))
            outcome = _dispatch_win_streak_roll_legs(
                repository,
                subscription_id=subscription_id,
                user_id=int(candidate["user_id"]),
                delivery_target_id=int(candidate["delivery_target_id"]),
                telegram_account_id=(
                    int(candidate["telegram_account_id"]) if candidate.get("telegram_account_id") is not None else None
                ),
                executor_type=str(candidate["executor_type"]),
                strategy=strategy,
                signal=signal,
                template=template,
                issue_window=issue_window,
                auto_trigger_context=resolved_auto_trigger_context,
            )
            created_count += int(outcome.get("created_count") or 0)
            existing_count += int(outcome.get("existing_count") or 0)
            skipped_count += int(outcome.get("skipped_count") or 0)
            jobs.extend(outcome.get("jobs") or [])
            continue
        if hasattr(repository, "execution_job_exists_for_target") and repository.execution_job_exists_for_target(
            signal_id=int(signal_id),
            subscription_id=subscription_id,
            delivery_target_id=int(candidate["delivery_target_id"]),
            from_route=True,
        ):
            # 该期该群已被 route 跟单占位：直派让位，避免同一期同一群双注。
            # 手动开单与自动触发规则因此可以并存——规则没派的期由直派补上。
            skipped_count += 1
            continue
        progression_event = progression_events.get(subscription_id)
        if progression_event is None:
            existing_event = repository.get_progression_event_by_signal(
                subscription_id=subscription_id,
                signal_id=int(signal_id)
            )
            if existing_event:
                progression_event = existing_event
            else:
                progression_state = repository.get_subscription_progression_state(subscription_id)
                stake_plan_preview = _build_stake_plan(strategy, signal, current_step=int(progression_state.get("current_step") or 1))
                settlement_policy = resolve_settlement_runtime_policy(strategy, signal.get("normalized_payload"))
                progression_event = repository.create_progression_event_record(
                    subscription_id=subscription_id,
                    user_id=int(candidate["user_id"]),
                    signal_id=int(signal_id),
                    issue_no=str(signal.get("issue_no") or ""),
                    progression_step=int(stake_plan_preview["current_step"]),
                    stake_amount=float(stake_plan_preview["amount"]),
                    base_stake=float(stake_plan_preview["base_stake"]),
                    multiplier=float(stake_plan_preview["multiplier"]),
                    max_steps=int(stake_plan_preview["max_steps"]),
                    refund_action=str(stake_plan_preview["refund_action"]),
                    cap_action=str(stake_plan_preview["cap_action"]),
                    settlement_rule_id=str(settlement_policy.get("settlement_rule_id") or ""),
                    settlement_snapshot=build_settlement_snapshot(
                        rule_source=str(settlement_policy.get("rule_source") or ""),
                        settlement_rule_id=settlement_policy.get("settlement_rule_id"),
                        fallback_profit_ratio=float(settlement_policy.get("fallback_profit_ratio") or 1.0),
                        resolved_from=str(settlement_policy.get("resolved_from") or ""),
                        signal=signal,
                        odds_overrides=settlement_policy.get("odds_overrides"),
                        stake_amount=float(stake_plan_preview["amount"]),
                    ),
                    auto_trigger_rule_id=(
                        int(resolved_auto_trigger_context["rule_id"])
                        if resolved_auto_trigger_context and resolved_auto_trigger_context.get("rule_id")
                        else None
                    ),
                    auto_trigger_rule_run_id=(
                        int(resolved_auto_trigger_context["rule_run_id"])
                        if resolved_auto_trigger_context and resolved_auto_trigger_context.get("rule_run_id")
                        else None
                    ),
                    auto_trigger_stat_date=(
                        str(resolved_auto_trigger_context.get("stat_date") or "")
                        if resolved_auto_trigger_context
                        else ""
                    ),
                    status="pending",
                )
            progression_events[subscription_id] = progression_event
        current_step = int((progression_event or {}).get("progression_step") or 1)
        stake_plan = _build_stake_plan(strategy, signal, current_step=current_step, progression_event=progression_event)
        if progression_event:
            stake_plan.setdefault("meta", {})
            stake_plan["meta"].update({
                "progression_event_id": int(progression_event["id"]),
                "progression_step": current_step,
            })
        template = None
        if candidate.get("template_id") is not None and hasattr(repository, "get_message_template"):
            template = repository.get_message_template(int(candidate["template_id"]))
        planned_message_text = _message_text_from_template(_frozen_group_signal(signal, progression_event), float(stake_plan["amount"]), template)
        execute_after = base_execute_after
        expire_at = _job_expire_at(execute_after, strategy, stake_plan, issue_window)
        idempotency_key = "signal:%s:target:%s" % (signal_id, candidate["delivery_target_id"])

        created = repository.create_execution_job_record(
            user_id=int(candidate["user_id"]),
            signal_id=int(signal_id),
            subscription_id=subscription_id,
            progression_event_id=int(progression_event["id"]) if progression_event else None,
            delivery_target_id=int(candidate["delivery_target_id"]),
            telegram_account_id=(
                int(candidate["telegram_account_id"]) if candidate.get("telegram_account_id") is not None else None
            ),
            executor_type=str(candidate["executor_type"]),
            idempotency_key=idempotency_key,
            planned_message_text=planned_message_text,
            stake_plan=stake_plan,
            execute_after=_iso_z(execute_after),
            expire_at=_iso_z(expire_at),
            leg_key="group" if stake_plan.get("group_play") else "",
        )
        jobs.append(created["job"])
        if created["created"]:
            created_count += 1
        else:
            existing_count += 1

    return {
        "signal_id": int(signal_id),
        "subscription_id": int(subscription_id) if subscription_id is not None else None,
        "candidate_count": len(candidates) + int(active_route_result.get("candidate_count") or 0),
        "created_count": created_count + int(active_route_result.get("created_count") or 0),
        "existing_count": existing_count + int(active_route_result.get("existing_count") or 0),
        "skipped_count": skipped_count + int(active_route_result.get("skipped_count") or 0),
        "skipped_routes": list(active_route_result.get("skipped_routes") or []),
        "jobs": list(active_route_result.get("jobs") or []) + jobs,
    }
