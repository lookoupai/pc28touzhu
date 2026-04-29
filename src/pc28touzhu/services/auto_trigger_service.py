"""自动触发跟单新一轮服务。"""
from __future__ import annotations

import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen

from pc28touzhu.domain.subscription_strategy import upgrade_subscription_strategy
from pc28touzhu.services.dispatch_service import dispatch_signal


ALLOWED_METRICS = {"big_small", "odd_even", "combo"}
ALLOWED_CONDITION_TYPES = {"hit_rate", "miss_streak"}
ALLOWED_OPERATORS = {"lt", "lte", "gt", "gte"}
ALLOWED_SCOPE_MODES = {"all_subscriptions", "selected_subscriptions"}
ALLOWED_PLAY_FILTER_ACTIONS = {"keep", "matched_metric", "fixed_metric"}
METRIC_LABELS = {
    "big_small": "大小",
    "odd_even": "单双",
    "combo": "组合",
}
PLAY_FILTER_KEYS_BY_METRIC = {
    "big_small": ["big_small:大", "big_small:小"],
    "odd_even": ["odd_even:单", "odd_even:双"],
    "combo": ["combo:大单", "combo:大双", "combo:小单", "combo:小双"],
}
EVENT_RETENTION_DAYS = {
    "skipped": 7,
    "triggered": 30,
    "failed": 30,
}
RUNTIME_RETENTION_DAYS = {
    "runs": 90,
    "stats": 365,
}
SHANGHAI_TZ = timezone(timedelta(hours=8))


def _format_utc_iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _event_retention_cutoffs(*, now: Optional[datetime] = None) -> Dict[str, str]:
    reference = now or datetime.now(timezone.utc)
    return {
        status: _format_utc_iso(reference - timedelta(days=days))
        for status, days in EVENT_RETENTION_DAYS.items()
    }


def _to_positive_int(value: Any, field_name: str, *, allow_none: bool = False) -> Optional[int]:
    if allow_none and (value is None or str(value).strip() == ""):
        return None
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        raise ValueError("%s 必须为正整数" % field_name)
    if normalized <= 0:
        raise ValueError("%s 必须为正整数" % field_name)
    return normalized


def _to_float(value: Any, field_name: str) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ValueError("%s 必须为数字" % field_name)


def _round_money(value: Any) -> float:
    try:
        return round(float(value or 0), 2)
    except (TypeError, ValueError):
        return 0.0


def _today_stat_date(*, now: Optional[datetime] = None, timezone_name: str = "Asia/Shanghai") -> str:
    tz = SHANGHAI_TZ if str(timezone_name or "").strip() == "Asia/Shanghai" else timezone.utc
    return (now or datetime.now(timezone.utc)).astimezone(tz).strftime("%Y-%m-%d")


def _normalize_stat_date(value: Any, *, timezone_name: str = "Asia/Shanghai") -> str:
    text = str(value or "").strip()
    if not text:
        return _today_stat_date(timezone_name=timezone_name)
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d")
    except ValueError:
        raise ValueError("stat_date 必须为 YYYY-MM-DD")
    return parsed.strftime("%Y-%m-%d")


def _runtime_retention_cutoffs(*, now: Optional[datetime] = None) -> Dict[str, str]:
    reference = now or datetime.now(timezone.utc)
    return {
        "runs": (reference.astimezone(SHANGHAI_TZ).date() - timedelta(days=RUNTIME_RETENTION_DAYS["runs"])).isoformat(),
        "stats": (reference.astimezone(SHANGHAI_TZ).date() - timedelta(days=RUNTIME_RETENTION_DAYS["stats"])).isoformat(),
    }


def _normalize_subscription_ids(value: Any) -> list[int]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("subscription_ids 必须为数组")
    normalized: list[int] = []
    seen: set[int] = set()
    for item in value:
        item_id = _to_positive_int(item, "subscription_id")
        if item_id and item_id not in seen:
            seen.add(item_id)
            normalized.append(item_id)
    return normalized


def _normalize_conditions(value: Any) -> list[dict]:
    if not isinstance(value, list) or not value:
        raise ValueError("conditions 至少需要配置一条条件")
    normalized = []
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            raise ValueError("conditions[%s] 必须为对象" % index)
        metric = str(item.get("metric") or "").strip()
        if metric not in ALLOWED_METRICS:
            raise ValueError("conditions[%s].metric 仅支持 big_small、odd_even、combo" % index)
        condition_type = str(item.get("type") or "hit_rate").strip() or "hit_rate"
        if condition_type not in ALLOWED_CONDITION_TYPES:
            raise ValueError("conditions[%s].type 仅支持 hit_rate 或 miss_streak" % index)
        operator = str(item.get("operator") or "").strip()
        if operator not in ALLOWED_OPERATORS:
            raise ValueError("conditions[%s].operator 仅支持 lt、lte、gt、gte" % index)
        threshold = _to_float(item.get("threshold"), "conditions[%s].threshold" % index)
        if condition_type == "hit_rate" and (threshold < 0 or threshold > 100):
            raise ValueError("conditions[%s].threshold 必须在 0 到 100 之间" % index)
        if condition_type == "miss_streak" and threshold < 0:
            raise ValueError("conditions[%s].threshold 不能小于 0" % index)
        min_sample_count = int(item.get("min_sample_count") or 100)
        if min_sample_count < 1:
            raise ValueError("conditions[%s].min_sample_count 必须大于 0" % index)
        normalized.append(
            {
                "type": condition_type,
                "metric": metric,
                "window": "recent_100" if condition_type == "hit_rate" else "current",
                "operator": operator,
                "threshold": int(threshold) if condition_type == "miss_streak" else round(threshold, 2),
                "min_sample_count": min_sample_count,
            }
        )
    return normalized


def _normalize_guard_groups(value: Any) -> list[dict]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError("guard_groups 必须为数组")
    normalized = []
    for index, item in enumerate(value, start=1):
        if not isinstance(item, dict):
            raise ValueError("guard_groups[%s] 必须为对象" % index)
        name = str(item.get("name") or "").strip()
        normalized.append(
            {
                "name": name,
                "conditions": _normalize_conditions(item.get("conditions")),
            }
        )
    return normalized


def _normalize_action(value: Any) -> dict:
    action = value if isinstance(value, dict) else {}
    play_filter_action = str(action.get("play_filter_action") or "keep").strip() or "keep"
    if play_filter_action not in ALLOWED_PLAY_FILTER_ACTIONS:
        raise ValueError("action.play_filter_action 仅支持 keep、matched_metric 或 fixed_metric")
    fixed_metric = str(action.get("fixed_metric") or "").strip()
    if play_filter_action == "fixed_metric" and fixed_metric not in ALLOWED_METRICS:
        raise ValueError("固定切换玩法时，action.fixed_metric 不能为空")
    return {
        "type": "restart_cycle",
        "dispatch_latest_signal": bool(action.get("dispatch_latest_signal", True)),
        "play_filter_action": play_filter_action,
        "fixed_metric": fixed_metric if fixed_metric in ALLOWED_METRICS else "",
        "skip_multiple_metrics_matched": bool(action.get("skip_multiple_metrics_matched", False)),
    }


def _normalize_daily_risk_control(value: Any) -> dict:
    payload = value if isinstance(value, dict) else {}
    enabled = bool(payload.get("enabled", False))
    profit_target = max(0.0, _round_money(payload.get("profit_target")))
    loss_limit = max(0.0, _round_money(payload.get("loss_limit")))
    if enabled and profit_target <= 0 and loss_limit <= 0:
        raise ValueError("启用规则日止盈止损时，止盈或止损至少需要配置一个")
    timezone_name = str(payload.get("timezone") or "Asia/Shanghai").strip() or "Asia/Shanghai"
    if timezone_name != "Asia/Shanghai":
        raise ValueError("daily_risk_control.timezone 当前仅支持 Asia/Shanghai")
    return {
        "enabled": enabled,
        "profit_target": profit_target,
        "loss_limit": loss_limit,
        "timezone": timezone_name,
        "cancel_pending_jobs": bool(payload.get("cancel_pending_jobs", True)),
    }


def normalize_rule_payload(payload: Dict[str, Any], *, current: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    source = current or {}
    name = str(payload.get("name", source.get("name") or "") or "").strip()
    if not name:
        raise ValueError("name 不能为空")
    status = str(payload.get("status", source.get("status") or "active") or "active").strip()
    if status not in {"active", "inactive", "archived"}:
        raise ValueError("status 仅支持 active、inactive、archived")
    scope_mode = str(payload.get("scope_mode", source.get("scope_mode") or "selected_subscriptions") or "").strip()
    if scope_mode not in ALLOWED_SCOPE_MODES:
        raise ValueError("scope_mode 仅支持 all_subscriptions 或 selected_subscriptions")
    subscription_ids = _normalize_subscription_ids(payload.get("subscription_ids", source.get("subscription_ids") or []))
    if scope_mode == "selected_subscriptions" and not subscription_ids:
        raise ValueError("选择指定跟单方案时，subscription_ids 不能为空")
    condition_mode = str(payload.get("condition_mode", source.get("condition_mode") or "any") or "any").strip()
    if condition_mode != "any":
        raise ValueError("condition_mode 当前仅支持 any")
    cooldown_issues = int(payload.get("cooldown_issues", source.get("cooldown_issues") or 10) or 10)
    if cooldown_issues < 0:
        raise ValueError("cooldown_issues 不能小于 0")
    return {
        "name": name,
        "status": status,
        "scope_mode": scope_mode,
        "subscription_ids": subscription_ids,
        "condition_mode": condition_mode,
        "conditions": _normalize_conditions(payload.get("conditions", source.get("conditions") or [])),
        "guard_groups": _normalize_guard_groups(payload.get("guard_groups", source.get("guard_groups") or [])),
        "action": _normalize_action(payload.get("action", source.get("action") or {})),
        "daily_risk_control": _normalize_daily_risk_control(
            payload.get("daily_risk_control", source.get("daily_risk_control") or {})
        ),
        "cooldown_issues": cooldown_issues,
    }


def create_auto_trigger_rule(repository: Any, *, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    normalized = normalize_rule_payload(payload)
    item = repository.create_auto_trigger_rule_record(user_id=normalized_user_id, **normalized)
    return {"item": item}


def update_auto_trigger_rule(repository: Any, *, rule_id: Any, user_id: Any, payload: Dict[str, Any]) -> Dict[str, Any]:
    normalized_rule_id = _to_positive_int(rule_id, "rule_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = repository.get_auto_trigger_rule(normalized_rule_id)
    if not current or int(current.get("user_id") or 0) != normalized_user_id:
        raise ValueError("rule_id 对应的自动触发规则不存在")
    normalized = normalize_rule_payload(payload, current=current)
    item = repository.update_auto_trigger_rule_record(rule_id=normalized_rule_id, user_id=normalized_user_id, **normalized)
    if not item:
        raise ValueError("rule_id 对应的自动触发规则不存在")
    return {"item": item}


def update_auto_trigger_rule_status(repository: Any, *, rule_id: Any, user_id: Any, status: Any) -> Dict[str, Any]:
    normalized_rule_id = _to_positive_int(rule_id, "rule_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    normalized_status = str(status or "").strip()
    if normalized_status not in {"active", "inactive", "archived"}:
        raise ValueError("status 仅支持 active、inactive、archived")
    item = repository.update_auto_trigger_rule_status(
        rule_id=normalized_rule_id,
        user_id=normalized_user_id,
        status=normalized_status,
    )
    if not item:
        raise ValueError("rule_id 对应的自动触发规则不存在")
    return {"item": item}


def delete_auto_trigger_rule(repository: Any, *, rule_id: Any, user_id: Any) -> Dict[str, Any]:
    normalized_rule_id = _to_positive_int(rule_id, "rule_id")
    normalized_user_id = _to_positive_int(user_id, "user_id")
    current = repository.get_auto_trigger_rule(normalized_rule_id)
    if not current or int(current.get("user_id") or 0) != normalized_user_id:
        raise ValueError("rule_id 对应的自动触发规则不存在")
    if str(current.get("status") or "") != "archived":
        raise ValueError("请先归档自动触发规则，再执行删除")
    deleted = repository.delete_auto_trigger_rule_record(rule_id=normalized_rule_id, user_id=normalized_user_id)
    return {"deleted": bool(deleted), "id": normalized_rule_id}


def list_auto_trigger_rules(repository: Any, *, user_id: Any, stat_date: Any = None) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    items = []
    for rule in repository.list_auto_trigger_rules(user_id=normalized_user_id):
        daily_risk_control = rule.get("daily_risk_control") if isinstance(rule.get("daily_risk_control"), dict) else {}
        resolved_stat_date = _normalize_stat_date(
            stat_date,
            timezone_name=str(daily_risk_control.get("timezone") or "Asia/Shanghai"),
        )
        daily_stat = repository.get_auto_trigger_rule_daily_stat(
            rule_id=int(rule["id"]),
            user_id=int(rule["user_id"]),
            stat_date=resolved_stat_date,
        )
        items.append(
            {
                **rule,
                "stat_date": resolved_stat_date,
                "daily_stat": daily_stat,
            }
        )
    return {"items": items}


def list_auto_trigger_events(repository: Any, *, user_id: Any, rule_id: Any = None, status: Any = None, limit: Any = 50) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    normalized_rule_id = _to_positive_int(rule_id, "rule_id", allow_none=True)
    normalized_status = str(status or "").strip()
    if normalized_status and normalized_status not in {"triggered", "skipped", "failed"}:
        raise ValueError("status 仅支持 triggered、skipped 或 failed")
    return {
        "items": repository.list_auto_trigger_events(
            user_id=normalized_user_id,
            rule_id=normalized_rule_id,
            status=normalized_status or None,
            limit=max(1, min(int(limit or 50), 200)),
        )
    }


def _http_json_fetch(url: str, *, timeout: int = 10) -> dict:
    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0 (compatible; pc28touzhu-auto-trigger/1.0)",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        raw = response.read()
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("表现接口返回值必须为 JSON 对象")
    return payload


def _predictor_id_from_url(url: str) -> Optional[int]:
    match = re.search(r"/predictors/(\d+)(?:/|$)", str(url or ""))
    return int(match.group(1)) if match else None


def _performance_url_from_source(source: Dict[str, Any]) -> str:
    config = source.get("config") if isinstance(source.get("config"), dict) else {}
    fetch = config.get("fetch") if isinstance(config.get("fetch"), dict) else {}
    raw_url = str(fetch.get("performance_url") or "").strip()
    if raw_url:
        return raw_url
    raw_url = str(fetch.get("url") or "").strip()
    if not raw_url:
        raise ValueError("来源未配置 fetch.url")
    predictor_id = _predictor_id_from_url(raw_url)
    parsed = urlparse(raw_url)
    if not predictor_id or not parsed.scheme or not parsed.netloc:
        raise ValueError("无法从来源 URL 解析 AITradingSimulator 方案 ID")
    path = re.sub(r"/api/export/predictors/\d+/signals/?$", f"/api/export/predictors/{predictor_id}/performance", parsed.path)
    if path == parsed.path:
        path = re.sub(r"/public/predictors/\d+/?$", f"/api/export/predictors/{predictor_id}/performance", parsed.path)
    if path == parsed.path:
        path = f"/api/export/predictors/{predictor_id}/performance"
    return urlunparse((parsed.scheme, parsed.netloc, path, "", "", ""))


def _compare(value: float, operator: str, threshold: float) -> bool:
    if operator == "lt":
        return value < threshold
    if operator == "lte":
        return value <= threshold
    if operator == "gt":
        return value > threshold
    if operator == "gte":
        return value >= threshold
    return False


def _match_condition(condition: Dict[str, Any], performance: Dict[str, Any]) -> Optional[dict]:
    metrics = performance.get("metrics") if isinstance(performance.get("metrics"), dict) else {}
    metric_key = str(condition.get("metric") or "")
    metric_payload = metrics.get(metric_key) if isinstance(metrics.get(metric_key), dict) else {}
    condition_type = str(condition.get("type") or "hit_rate").strip() or "hit_rate"
    if condition_type == "miss_streak":
        streaks = metric_payload.get("streaks") if isinstance(metric_payload.get("streaks"), dict) else {}
        miss_streak = streaks.get("current_miss_streak")
        if miss_streak is None:
            return None
        try:
            streak_count = int(miss_streak)
        except (TypeError, ValueError):
            return None
        if _compare(float(streak_count), str(condition.get("operator") or ""), float(condition.get("threshold") or 0)):
            return {
                **condition,
                "actual_miss_streak": streak_count,
                "metric_label": METRIC_LABELS.get(metric_key, metric_key),
            }
        return None

    recent_100 = metric_payload.get("recent_100") if isinstance(metric_payload.get("recent_100"), dict) else {}
    hit_rate = recent_100.get("hit_rate")
    sample_count = int(recent_100.get("sample_count") or 0)
    if hit_rate is None or sample_count < int(condition.get("min_sample_count") or 1):
        return None
    rate = float(hit_rate)
    if _compare(rate, str(condition.get("operator") or ""), float(condition.get("threshold") or 0)):
        return {
            **condition,
            "actual_hit_rate": rate,
            "sample_count": sample_count,
            "hit_count": int(recent_100.get("hit_count") or 0),
            "metric_label": METRIC_LABELS.get(metric_key, metric_key),
        }
    return None


def _matched_conditions(conditions: list[dict], performance: Dict[str, Any]) -> list[dict]:
    matched = []
    for condition in conditions or []:
        item = _match_condition(condition, performance)
        if item is not None:
            matched.append(item)
    return matched


def _match_guard_group(group: Dict[str, Any], performance: Dict[str, Any], primary_metric: str) -> Optional[dict]:
    conditions = group.get("conditions") if isinstance(group.get("conditions"), list) else []
    same_metric_conditions = [
        condition for condition in conditions
        if str(condition.get("metric") or "").strip() == primary_metric
    ]
    active_conditions = same_metric_conditions or conditions
    matched_conditions = _matched_conditions(active_conditions, performance)
    if not matched_conditions:
        return None
    return {
        "name": str(group.get("name") or ""),
        "matched_conditions": matched_conditions,
        "uses_same_metric": bool(same_metric_conditions),
        "active_metric": primary_metric if same_metric_conditions else "",
    }


def _resolve_trigger_match(rule: Dict[str, Any], performance: Dict[str, Any]) -> Optional[dict]:
    primary_matches = _matched_conditions(rule.get("conditions") or [], performance)
    if not primary_matches:
        return None
    guard_groups = rule.get("guard_groups") if isinstance(rule.get("guard_groups"), list) else []
    successful_paths = []
    for primary_condition in primary_matches:
        guard_matches = []
        primary_metric = str(primary_condition.get("metric") or "").strip()
        matched_conditions = [
            condition for condition in primary_matches
            if str(condition.get("metric") or "").strip() == primary_metric
        ]
        is_success = True
        for group in guard_groups:
            guard_match = _match_guard_group(group, performance, primary_metric)
            if guard_match is None:
                is_success = False
                break
            guard_matches.append(guard_match)
            matched_conditions.extend(guard_match.get("matched_conditions") or [])
        if is_success:
            successful_paths.append(
                {
                    "primary_condition": primary_condition,
                    "guard_matches": guard_matches,
                    "matched_conditions": matched_conditions,
                }
            )
    if not successful_paths:
        return None
    selected_path = successful_paths[0]
    distinct_metrics = {
        str((path.get("primary_condition") or {}).get("metric") or "").strip()
        for path in successful_paths
        if str((path.get("primary_condition") or {}).get("metric") or "").strip() in ALLOWED_METRICS
    }
    return {
        "primary_matches": primary_matches,
        "successful_paths": successful_paths,
        "successful_primary_conditions": [
            dict(path.get("primary_condition") or {})
            for path in successful_paths
        ],
        "selected_path": selected_path,
        "matched_conditions": list(selected_path.get("matched_conditions") or []),
        "primary_condition": dict(selected_path.get("primary_condition") or {}),
        "guard_matches": list(selected_path.get("guard_matches") or []),
        "distinct_metrics": distinct_metrics,
    }


def _has_multiple_matched_metrics(matched_conditions: list[dict]) -> bool:
    metrics = {
        str(condition.get("metric") or "").strip()
        for condition in matched_conditions
        if str(condition.get("metric") or "").strip() in ALLOWED_METRICS
    }
    return len(metrics) >= 2


def _has_multiple_trigger_metrics(trigger_match: Dict[str, Any]) -> bool:
    metrics = trigger_match.get("distinct_metrics")
    if isinstance(metrics, set):
        return len(metrics) >= 2
    return _has_multiple_matched_metrics(trigger_match.get("matched_conditions") or [])


def _resolve_target_metric(rule: Dict[str, Any], matched_conditions: list[dict]) -> Optional[str]:
    action = rule.get("action") if isinstance(rule.get("action"), dict) else {}
    play_filter_action = str(action.get("play_filter_action") or "keep").strip() or "keep"
    if play_filter_action == "keep":
        return None
    if play_filter_action == "fixed_metric":
        fixed_metric = str(action.get("fixed_metric") or "").strip()
        return fixed_metric if fixed_metric in ALLOWED_METRICS else None
    if not matched_conditions:
        return None
    metric = str(matched_conditions[0].get("metric") or "").strip()
    return metric if metric in ALLOWED_METRICS else None


def _apply_subscription_play_filter(
    repository: Any,
    *,
    rule: Dict[str, Any],
    subscription: Dict[str, Any],
    matched_conditions: list[dict],
) -> Optional[dict]:
    target_metric = _resolve_target_metric(rule, matched_conditions)
    if target_metric is None:
        return None
    current = repository.get_subscription(int(subscription["id"]))
    if not current:
        raise ValueError("subscription_id 对应的订阅不存在")
    strategy = upgrade_subscription_strategy(current.get("strategy_v2") or current.get("strategy"))
    previous_play_filter = dict(strategy.get("play_filter") or {})
    next_play_filter = {
        "mode": "selected",
        "selected_keys": list(PLAY_FILTER_KEYS_BY_METRIC[target_metric]),
    }
    strategy["play_filter"] = next_play_filter
    updated = repository.update_subscription_record(
        subscription_id=int(subscription["id"]),
        user_id=int(rule["user_id"]),
        source_id=int(current["source_id"]),
        strategy=strategy,
        status=str(current.get("status") or "active"),
    )
    if not updated:
        raise ValueError("subscription_id 对应的订阅不存在")
    return {
        "target_metric": target_metric,
        "target_metric_label": METRIC_LABELS.get(target_metric, target_metric),
        "previous_play_filter": previous_play_filter,
        "next_play_filter": next_play_filter,
    }


def _issue_distance(current_issue: str, previous_issue: str) -> Optional[int]:
    try:
        return int(str(current_issue)) - int(str(previous_issue))
    except (TypeError, ValueError):
        return None


def _is_in_cooldown(repository: Any, rule: Dict[str, Any], subscription_id: int, latest_issue_no: str) -> bool:
    cooldown_issues = int(rule.get("cooldown_issues") or 0)
    if cooldown_issues <= 0:
        return False
    last_event = repository.get_latest_auto_trigger_event(rule_id=int(rule["id"]), subscription_id=subscription_id)
    if not last_event:
        return False
    distance = _issue_distance(latest_issue_no, str(last_event.get("latest_issue_no") or ""))
    if distance is None:
        return str(latest_issue_no or "") == str(last_event.get("latest_issue_no") or "")
    return distance < cooldown_issues


def _record_event(
    repository: Any,
    *,
    rule: Dict[str, Any],
    subscription: Dict[str, Any],
    performance: Optional[Dict[str, Any]],
    status: str,
    reason: str,
    matched_conditions: Optional[list[dict]] = None,
    trigger_match: Optional[dict] = None,
    dispatch_result: Optional[dict] = None,
    play_filter_result: Optional[dict] = None,
) -> Dict[str, Any]:
    source = subscription.get("source") if isinstance(subscription.get("source"), dict) else {}
    latest_issue_no = str((performance or {}).get("latest_settled_issue") or "")
    predictor_id = None
    try:
        predictor_id = _predictor_id_from_url(_performance_url_from_source(source)) if source else None
    except Exception:
        predictor_id = None
    snapshot = {
        "rule_name": rule.get("name"),
        "source_name": source.get("name"),
        "performance": performance or {},
    }
    if trigger_match is not None:
        snapshot["matched_primary_condition"] = trigger_match.get("primary_condition") or {}
        snapshot["matched_guard_groups"] = trigger_match.get("guard_matches") or []
        snapshot["successful_path_count"] = len(trigger_match.get("successful_paths") or [])
    if dispatch_result is not None:
        snapshot["dispatch_result"] = dispatch_result
    if play_filter_result is not None:
        snapshot["play_filter_result"] = play_filter_result
    if status == "skipped":
        latest_skipped = repository.get_latest_auto_trigger_event(
            rule_id=int(rule["id"]),
            subscription_id=int(subscription["id"]),
            status="skipped",
        )
        if (
            latest_skipped
            and str(latest_skipped.get("reason") or "") == str(reason or "")
            and str(latest_skipped.get("latest_issue_no") or "") == latest_issue_no
            and str(latest_skipped.get("created_at") or "") >= _event_retention_cutoffs()["skipped"]
        ):
            return latest_skipped
    return repository.record_auto_trigger_event(
        rule_id=int(rule["id"]),
        user_id=int(rule["user_id"]),
        subscription_id=int(subscription["id"]),
        source_id=int(subscription["source_id"]),
        predictor_id=predictor_id,
        latest_issue_no=latest_issue_no,
        status=status,
        reason=reason,
        matched_conditions=matched_conditions or [],
        snapshot=snapshot,
    )


def _dispatch_latest_signal_if_available(
    repository: Any,
    *,
    subscription: Dict[str, Any],
    latest_settled_issue_no: str,
    auto_trigger_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    signal = repository.get_latest_ready_signal_for_source(source_id=int(subscription["source_id"]))
    if not signal:
        return {"skipped": True, "reason": "latest_signal_not_found", "created_count": 0}
    distance = _issue_distance(str(signal.get("issue_no") or ""), latest_settled_issue_no)
    if distance is None or distance <= 0:
        return {
            "skipped": True,
            "reason": "latest_signal_not_newer_than_performance",
            "signal_issue_no": str(signal.get("issue_no") or ""),
            "latest_settled_issue_no": str(latest_settled_issue_no or ""),
            "created_count": 0,
        }
    return dispatch_signal(
        repository,
        int(signal["id"]),
        subscription_id=int(subscription["id"]),
        auto_trigger_context=auto_trigger_context,
    )


def _is_rule_day_stopped(repository: Any, rule: Dict[str, Any], *, stat_date: str) -> Dict[str, Any]:
    daily_risk_control = rule.get("daily_risk_control") if isinstance(rule.get("daily_risk_control"), dict) else {}
    if not bool(daily_risk_control.get("enabled")):
        return {"stopped": False, "stat": None}
    stat = repository.get_auto_trigger_rule_daily_stat(
        rule_id=int(rule["id"]),
        user_id=int(rule["user_id"]),
        stat_date=stat_date,
    )
    return {"stopped": str(stat.get("status") or "") == "stopped", "stat": stat}


def _subscription_threshold_status(subscription: Dict[str, Any]) -> str:
    financial = subscription.get("financial") if isinstance(subscription.get("financial"), dict) else {}
    return str(financial.get("threshold_status") or "").strip()


def _can_restart_subscription_cycle(subscription: Dict[str, Any]) -> bool:
    if str(subscription.get("status") or "") == "standby":
        return True
    return _subscription_threshold_status(subscription) in {"profit_target_hit", "loss_limit_hit"}


def evaluate_auto_trigger_rule(repository: Any, rule: Dict[str, Any], *, fetcher=None) -> Dict[str, Any]:
    daily_risk_control = rule.get("daily_risk_control") if isinstance(rule.get("daily_risk_control"), dict) else {}
    stat_date = _today_stat_date(timezone_name=str(daily_risk_control.get("timezone") or "Asia/Shanghai"))
    day_state = _is_rule_day_stopped(repository, rule, stat_date=stat_date)
    subscription_ids = rule.get("subscription_ids") if rule.get("scope_mode") == "selected_subscriptions" else None
    subscriptions = repository.list_auto_trigger_candidate_subscriptions(
        user_id=int(rule["user_id"]),
        subscription_ids=subscription_ids,
    )
    fetch = fetcher or _http_json_fetch
    summary = {"checked_count": 0, "triggered_count": 0, "skipped_count": 0, "failed_count": 0}
    events = []

    for subscription in subscriptions:
        source = subscription.get("source") if isinstance(subscription.get("source"), dict) else {}
        summary["checked_count"] += 1
        try:
            if day_state.get("stopped"):
                events.append(
                    _record_event(
                        repository,
                        rule=rule,
                        subscription=subscription,
                        performance=None,
                        status="skipped",
                        reason="daily_risk_stopped",
                        play_filter_result={"stat_date": stat_date, "stat": day_state.get("stat")},
                    )
                )
                summary["skipped_count"] += 1
                continue
            subscription_status = str(subscription.get("status") or "")
            if subscription_status not in {"active", "standby"}:
                events.append(_record_event(repository, rule=rule, subscription=subscription, performance=None, status="skipped", reason="subscription_not_active"))
                summary["skipped_count"] += 1
                continue
            if str(source.get("status") or "") != "active":
                events.append(_record_event(repository, rule=rule, subscription=subscription, performance=None, status="skipped", reason="source_not_active"))
                summary["skipped_count"] += 1
                continue
            open_state = repository.subscription_has_open_run(subscription_id=int(subscription["id"]), user_id=int(rule["user_id"]))
            if open_state.get("has_open_run"):
                events.append(_record_event(repository, rule=rule, subscription=subscription, performance=None, status="skipped", reason="subscription_has_open_run"))
                summary["skipped_count"] += 1
                continue
            if not _can_restart_subscription_cycle(subscription):
                events.append(_record_event(repository, rule=rule, subscription=subscription, performance=None, status="skipped", reason="subscription_not_ready_for_restart"))
                summary["skipped_count"] += 1
                continue

            performance_url = _performance_url_from_source(source)
            performance = fetch(performance_url)
            latest_issue_no = str(performance.get("latest_settled_issue") or "")
            if not latest_issue_no:
                events.append(_record_event(repository, rule=rule, subscription=subscription, performance=performance, status="skipped", reason="performance_issue_empty"))
                summary["skipped_count"] += 1
                continue
            if _is_in_cooldown(repository, rule, int(subscription["id"]), latest_issue_no):
                events.append(_record_event(repository, rule=rule, subscription=subscription, performance=performance, status="skipped", reason="cooldown"))
                summary["skipped_count"] += 1
                continue

            trigger_match = _resolve_trigger_match(rule, performance)
            if not trigger_match:
                continue
            matched = trigger_match.get("matched_conditions") or []
            action = rule.get("action") if isinstance(rule.get("action"), dict) else {}
            if bool(action.get("skip_multiple_metrics_matched", False)) and _has_multiple_trigger_metrics(trigger_match):
                skipped_matches = trigger_match.get("successful_primary_conditions") or matched
                events.append(
                    _record_event(
                        repository,
                        rule=rule,
                        subscription=subscription,
                        performance=performance,
                        status="skipped",
                        reason="multiple_metrics_matched",
                        matched_conditions=skipped_matches,
                        trigger_match=trigger_match,
                    )
                )
                summary["skipped_count"] += 1
                continue

            if subscription_status == "standby":
                activated = repository.update_subscription_status(
                    subscription_id=int(subscription["id"]),
                    user_id=int(rule["user_id"]),
                    status="active",
                )
                if not activated:
                    raise ValueError("subscription_id 对应的订阅不存在")
                subscription = activated
                subscription["source"] = source

            play_filter_result = _apply_subscription_play_filter(
                repository,
                rule=rule,
                subscription=subscription,
                matched_conditions=matched,
            )
            rule_run = repository.ensure_auto_trigger_rule_run(
                rule_id=int(rule["id"]),
                user_id=int(rule["user_id"]),
                subscription_id=int(subscription["id"]),
                stat_date=stat_date,
                started_issue_no=latest_issue_no,
            )
            repository.reset_subscription_runtime(
                subscription_id=int(subscription["id"]),
                user_id=int(rule["user_id"]),
                note="自动触发规则：%s" % str(rule.get("name") or ""),
            )
            dispatch_result = None
            if bool((rule.get("action") or {}).get("dispatch_latest_signal", True)):
                dispatch_result = _dispatch_latest_signal_if_available(
                    repository,
                    subscription=subscription,
                    latest_settled_issue_no=latest_issue_no,
                    auto_trigger_context={
                        "rule_id": int(rule["id"]),
                        "rule_run_id": int(rule_run["id"]),
                        "stat_date": stat_date,
                    },
                )
            event = _record_event(
                repository,
                rule=rule,
                subscription=subscription,
                performance=performance,
                status="triggered",
                reason="conditions_matched",
                matched_conditions=matched,
                trigger_match=trigger_match,
                dispatch_result=dispatch_result,
                play_filter_result=play_filter_result,
            )
            repository.mark_auto_trigger_rule_triggered(
                rule_id=int(rule["id"]),
                user_id=int(rule["user_id"]),
                issue_no=latest_issue_no,
            )
            events.append(event)
            summary["triggered_count"] += 1
        except Exception as exc:
            try:
                events.append(
                    _record_event(
                        repository,
                        rule=rule,
                        subscription=subscription,
                        performance=None,
                        status="failed",
                        reason=str(exc) or exc.__class__.__name__,
                    )
                )
            finally:
                summary["failed_count"] += 1

    return {"rule_id": int(rule["id"]), "summary": summary, "events": events}


def run_auto_trigger_cycle(repository: Any, *, user_id: Any = None, rule_id: Any = None, fetcher=None) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id", allow_none=True)
    normalized_rule_id = _to_positive_int(rule_id, "rule_id", allow_none=True)
    if normalized_rule_id is not None:
        rule = repository.get_auto_trigger_rule(normalized_rule_id)
        if not rule or (normalized_user_id is not None and int(rule.get("user_id") or 0) != normalized_user_id):
            raise ValueError("rule_id 对应的自动触发规则不存在")
        rules = [rule] if str(rule.get("status") or "") == "active" else []
    else:
        rules = repository.list_auto_trigger_rules(user_id=normalized_user_id, status="active")

    result = {
        "summary": {"rule_count": len(rules), "checked_count": 0, "triggered_count": 0, "skipped_count": 0, "failed_count": 0},
        "rules": [],
    }
    for rule in rules:
        item = evaluate_auto_trigger_rule(repository, rule, fetcher=fetcher)
        summary = item.get("summary") or {}
        for key in ["checked_count", "triggered_count", "skipped_count", "failed_count"]:
            result["summary"][key] += int(summary.get(key) or 0)
        result["rules"].append(item)
    cleanup_user_id = normalized_user_id
    if cleanup_user_id is None and normalized_rule_id is not None and rules:
        cleanup_user_id = int(rules[0]["user_id"])
    result["cleanup"] = repository.prune_auto_trigger_events(
        user_id=cleanup_user_id,
        cutoffs_by_status=_event_retention_cutoffs(),
    )
    runtime_cutoffs = _runtime_retention_cutoffs()
    result["runtime_cleanup"] = repository.prune_auto_trigger_rule_runtime_data(
        user_id=cleanup_user_id,
        runs_cutoff=runtime_cutoffs["runs"],
        stats_cutoff=runtime_cutoffs["stats"],
    )
    return result
