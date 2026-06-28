"""Dispatch planning from signals to execution jobs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict

from pc28touzhu.domain.pc28_play_filter import strategy_matches_signal
from pc28touzhu.domain.settlement_rules import build_settlement_snapshot
from pc28touzhu.domain.subscription_strategy import (
    resolve_dispatch_policy,
    resolve_settlement_runtime_policy,
    resolve_staking_runtime_policy,
    upgrade_subscription_strategy,
)


PLAY_FILTER_KEYS_BY_METRIC = {
    "big_small": ["big_small:大", "big_small:小"],
    "odd_even": ["odd_even:单", "odd_even:双"],
    "combo": ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
}


def _utc_now() -> datetime:
    return datetime.now(timezone.utc).replace(microsecond=0)


def _parse_iso_z(value: str | None) -> datetime:
    if not value:
        return _utc_now()
    text = str(value).strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    return datetime.fromisoformat(text).astimezone(timezone.utc)


def _iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


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
    if context and int(context.get("rule_id") or 0) > 0 and str(context.get("stat_date") or "").strip():
        stat = repository.get_auto_trigger_rule_daily_stat(
            rule_id=int(context["rule_id"]),
            user_id=int(candidate["user_id"]),
            stat_date=str(context["stat_date"]),
        )
        if str(stat.get("status") or "") == "stopped":
            return {**context, "stopped": True, "stopped_reason": str(stat.get("stopped_reason") or "daily_risk_stopped")}
        return {**context, "stopped": False}

    if not hasattr(repository, "get_latest_auto_trigger_rule_run_for_subscription"):
        return None
    run = repository.get_latest_auto_trigger_rule_run_for_subscription(
        subscription_id=int(candidate["subscription_id"]),
        user_id=int(candidate["user_id"]),
    )
    if not run:
        return None
    if str(run.get("status") or "") == "stopped":
        return {
            "rule_id": int(run["rule_id"]),
            "rule_run_id": int(run["id"]),
            "stat_date": str(run.get("stat_date") or ""),
            "stopped": True,
            "stopped_reason": str(run.get("stop_reason") or "daily_risk_stopped"),
        }
    if str(run.get("status") or "") != "active":
        return None
    stat_date = str(run.get("stat_date") or "").strip()
    rule_id = int(run["rule_id"])
    stat = repository.get_auto_trigger_rule_daily_stat(
        rule_id=rule_id,
        user_id=int(candidate["user_id"]),
        stat_date=stat_date,
    )
    if str(stat.get("status") or "") == "stopped":
        return {
            "rule_id": rule_id,
            "rule_run_id": int(run["id"]),
            "stat_date": stat_date,
            "stopped": True,
            "stopped_reason": str(stat.get("stopped_reason") or "daily_risk_stopped"),
        }
    return {
        "rule_id": rule_id,
        "rule_run_id": int(run["id"]),
        "stat_date": stat_date,
        "stopped": False,
    }


def _build_stake_plan(strategy: Dict[str, Any], signal: Dict[str, Any], current_step: int = 1) -> Dict[str, Any]:
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


def _dispatch_signal_for_auto_trigger_routes(
    repository: Any,
    signal: Dict[str, Any],
    *,
    subscription_id: int,
    auto_trigger_context: Dict[str, Any],
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
    jobs = []

    for route in routes:
        route_id = int(route["id"])
        route_stat_date = str(route.get("_auto_trigger_stat_date") or stat_date).strip()
        route_rule_run_id = route.get("_auto_trigger_rule_run_id") or auto_trigger_context.get("rule_run_id")
        delivery_target_id = int(route["delivery_target_id"])
        if str(route.get("target_status") or "active") != "active":
            skipped_count += 1
            continue
        account_status = str(route.get("telegram_account_status") or "active")
        if route.get("telegram_account_id") is not None and account_status != "active":
            skipped_count += 1
            continue
        if route_stat_date and _route_is_stopped(
            repository,
            route=route,
            user_id=user_id,
            stat_date=route_stat_date,
        ):
            skipped_count += 1
            continue
        if _route_subscription_is_stopped(
            repository,
            route=route,
            subscription_id=int(subscription_id),
            user_id=user_id,
        ):
            skipped_count += 1
            continue
        if repository.auto_trigger_route_has_open_run(
            route_id=route_id,
            subscription_id=int(subscription_id),
            user_id=user_id,
        ).get("has_open_run"):
            skipped_count += 1
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
        if not strategy_matches_signal(strategy, signal):
            skipped_count += 1
            continue

        progression_event = repository.get_progression_event_by_signal(
            subscription_id=int(subscription_id),
            signal_id=int(signal["id"]),
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
                signal,
                current_step=int(progression_state.get("current_step") or 1),
            )
            settlement_policy = resolve_settlement_runtime_policy(strategy, signal.get("normalized_payload"))
            progression_event = repository.create_progression_event_record(
                subscription_id=int(subscription_id),
                user_id=user_id,
                signal_id=int(signal["id"]),
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
        stake_plan = _build_stake_plan(strategy, signal, current_step=current_step)
        stake_plan.setdefault("meta", {})
        stake_plan["meta"].update({
            "progression_event_id": int(progression_event["id"]),
            "progression_step": current_step,
            "auto_trigger_route_id": route_id,
        })

        template = None
        template_id = _route_template_id(route)
        if template_id is not None and hasattr(repository, "get_message_template"):
            template = repository.get_message_template(template_id)
        planned_message_text = _message_text_from_template(signal, float(stake_plan["amount"]), template)
        execute_after = base_execute_after
        expire_after_seconds = int(resolve_dispatch_policy(strategy).get("expire_after_seconds") or 120)
        expire_at = execute_after + timedelta(seconds=max(30, expire_after_seconds))
        idempotency_key = "signal:%s:rule:%s:route:%s:target:%s" % (
            signal["id"],
            auto_trigger_context.get("rule_id") or "",
            route_id,
            delivery_target_id,
        )

        created = repository.create_execution_job_record(
            user_id=user_id,
            signal_id=int(signal["id"]),
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
        "jobs": jobs,
    }


def _dispatch_signal_for_active_auto_trigger_routes(repository: Any, signal: Dict[str, Any]) -> Dict[str, Any]:
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
    jobs = []

    for candidate in candidates:
        route = candidate.get("route") if isinstance(candidate.get("route"), dict) else {}
        if not route:
            skipped_count += 1
            continue
        strategy = _route_continuation_strategy(
            candidate.get("strategy_json") if isinstance(candidate.get("strategy_json"), dict) else {},
            route,
            candidate,
        )
        if not strategy_matches_signal(strategy, signal):
            skipped_count += 1
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
        )
        created_count += int(result.get("created_count") or 0)
        existing_count += int(result.get("existing_count") or 0)
        skipped_count += int(result.get("skipped_count") or 0)
        jobs.extend(result.get("jobs") or [])

    return {
        "signal_id": int(signal["id"]),
        "candidate_count": len(candidates),
        "created_count": created_count,
        "existing_count": existing_count,
        "skipped_count": skipped_count,
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


def dispatch_signal(
    repository: Any,
    signal_id: int,
    *,
    subscription_id: int | None = None,
    auto_trigger_context: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    signal = repository.get_signal(signal_id)
    if not signal:
        raise ValueError("signal 不存在")
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
        )

    active_route_result = _dispatch_signal_for_active_auto_trigger_routes(repository, signal)
    raw_candidates = (
        repository.list_dispatch_candidates_for_subscription(signal_id, subscription_id=subscription_id)
        if subscription_id is not None and hasattr(repository, "list_dispatch_candidates_for_subscription")
        else repository.list_dispatch_candidates(signal_id)
    )
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
        if resolved_auto_trigger_context and resolved_auto_trigger_context.get("stopped"):
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
        stake_plan = _build_stake_plan(strategy, signal, current_step=current_step)
        if progression_event:
            stake_plan.setdefault("meta", {})
            stake_plan["meta"].update({
                "progression_event_id": int(progression_event["id"]),
                "progression_step": current_step,
            })
        template = None
        if candidate.get("template_id") is not None and hasattr(repository, "get_message_template"):
            template = repository.get_message_template(int(candidate["template_id"]))
        planned_message_text = _message_text_from_template(signal, float(stake_plan["amount"]), template)
        execute_after = base_execute_after
        expire_after_seconds = int(resolve_dispatch_policy(strategy).get("expire_after_seconds") or 120)
        expire_at = execute_after + timedelta(seconds=max(30, expire_after_seconds))
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
        "jobs": list(active_route_result.get("jobs") or []) + jobs,
    }
