"""Dispatch planning from signals to execution jobs."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict


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


def _build_stake_plan(strategy: Dict[str, Any], signal: Dict[str, Any], current_step: int = 1) -> Dict[str, Any]:
    payload = signal.get("normalized_payload") or {}
    mode = str(strategy.get("mode") or "flat").strip() or "flat"
    base_stake = float(
        strategy.get("base_stake")
        or strategy.get("stake_amount")
        or payload.get("base_stake")
        or payload.get("stake_amount")
        or 10.0
    )
    multiplier = float(strategy.get("multiplier") or payload.get("multiplier") or 2.0)
    max_steps = int(strategy.get("max_steps") or payload.get("max_steps") or 1)
    refund_action = str(strategy.get("refund_action") or payload.get("refund_action") or "hold").strip() or "hold"
    cap_action = str(strategy.get("cap_action") or payload.get("cap_action") or "reset").strip() or "reset"
    if mode == "martingale":
        amount = round(base_stake * (multiplier ** max(0, int(current_step or 1) - 1)), 2)
    else:
        amount = float(strategy.get("stake_amount") or payload.get("stake_amount") or base_stake)
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
        "legacy_stake_amount": float(strategy.get("stake_amount") or payload.get("stake_amount") or 0) if (
            strategy.get("stake_amount") is not None or payload.get("stake_amount") is not None
        ) else None
    }
    stake_plan["meta"] = {key: value for key, value in meta.items() if value is not None}
    return stake_plan


def dispatch_signal(repository: Any, signal_id: int) -> Dict[str, Any]:
    signal = repository.get_signal(signal_id)
    if not signal:
        raise ValueError("signal 不存在")

    candidates = repository.list_dispatch_candidates(signal_id)
    now = _utc_now()
    base_execute_after = max(_parse_iso_z(signal.get("published_at")), now)
    base_expire_at = base_execute_after + timedelta(minutes=2)

    created_count = 0
    existing_count = 0
    jobs = []
    progression_events: dict[int, Dict[str, Any] | None] = {}

    for candidate in candidates:
        strategy = candidate.get("strategy_json")
        if isinstance(strategy, str):
            # defensive; repository currently returns dict rows only
            strategy = {}
        strategy = strategy or {}
        subscription_id = int(candidate["subscription_id"])
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
        expire_after_seconds = int(strategy.get("expire_after_seconds") or 120)
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
        "candidate_count": len(candidates),
        "created_count": created_count,
        "existing_count": existing_count,
        "jobs": jobs,
    }
