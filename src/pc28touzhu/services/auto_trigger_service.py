"""自动触发跟单新一轮服务。"""
from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlunparse
from urllib.request import Request, urlopen

from pc28touzhu.services.dispatch_service import dispatch_signal


ALLOWED_METRICS = {"big_small", "odd_even", "combo"}
ALLOWED_OPERATORS = {"lt", "lte", "gt", "gte"}
ALLOWED_SCOPE_MODES = {"all_subscriptions", "selected_subscriptions"}
METRIC_LABELS = {
    "big_small": "大小",
    "odd_even": "单双",
    "combo": "组合",
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
        operator = str(item.get("operator") or "").strip()
        if operator not in ALLOWED_OPERATORS:
            raise ValueError("conditions[%s].operator 仅支持 lt、lte、gt、gte" % index)
        threshold = _to_float(item.get("threshold"), "conditions[%s].threshold" % index)
        if threshold < 0 or threshold > 100:
            raise ValueError("conditions[%s].threshold 必须在 0 到 100 之间" % index)
        min_sample_count = int(item.get("min_sample_count") or 100)
        if min_sample_count < 1:
            raise ValueError("conditions[%s].min_sample_count 必须大于 0" % index)
        normalized.append(
            {
                "metric": metric,
                "window": "recent_100",
                "operator": operator,
                "threshold": round(threshold, 2),
                "min_sample_count": min_sample_count,
            }
        )
    return normalized


def _normalize_action(value: Any) -> dict:
    action = value if isinstance(value, dict) else {}
    return {
        "type": "restart_cycle",
        "dispatch_latest_signal": bool(action.get("dispatch_latest_signal", True)),
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
        "action": _normalize_action(payload.get("action", source.get("action") or {})),
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


def list_auto_trigger_rules(repository: Any, *, user_id: Any) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    return {"items": repository.list_auto_trigger_rules(user_id=normalized_user_id)}


def list_auto_trigger_events(repository: Any, *, user_id: Any, rule_id: Any = None, limit: Any = 50) -> Dict[str, Any]:
    normalized_user_id = _to_positive_int(user_id, "user_id")
    normalized_rule_id = _to_positive_int(rule_id, "rule_id", allow_none=True)
    return {
        "items": repository.list_auto_trigger_events(
            user_id=normalized_user_id,
            rule_id=normalized_rule_id,
            limit=max(1, min(int(limit or 50), 200)),
        )
    }


def _http_json_fetch(url: str, *, timeout: int = 10) -> dict:
    request = Request(url, headers={"Accept": "application/json"})
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


def _matched_conditions(rule: Dict[str, Any], performance: Dict[str, Any]) -> list[dict]:
    metrics = performance.get("metrics") if isinstance(performance.get("metrics"), dict) else {}
    matched = []
    for condition in rule.get("conditions") or []:
        metric_key = str(condition.get("metric") or "")
        recent_100 = (metrics.get(metric_key) or {}).get("recent_100") or {}
        hit_rate = recent_100.get("hit_rate")
        sample_count = int(recent_100.get("sample_count") or 0)
        if hit_rate is None or sample_count < int(condition.get("min_sample_count") or 1):
            continue
        rate = float(hit_rate)
        if _compare(rate, str(condition.get("operator") or ""), float(condition.get("threshold") or 0)):
            matched.append(
                {
                    **condition,
                    "actual_hit_rate": rate,
                    "sample_count": sample_count,
                    "hit_count": int(recent_100.get("hit_count") or 0),
                    "metric_label": METRIC_LABELS.get(metric_key, metric_key),
                }
            )
    return matched


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
    dispatch_result: Optional[dict] = None,
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
    if dispatch_result is not None:
        snapshot["dispatch_result"] = dispatch_result
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
    return dispatch_signal(repository, int(signal["id"]), subscription_id=int(subscription["id"]))


def evaluate_auto_trigger_rule(repository: Any, rule: Dict[str, Any], *, fetcher=None) -> Dict[str, Any]:
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
            if str(subscription.get("status") or "") != "active":
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

            matched = _matched_conditions(rule, performance)
            if not matched:
                continue

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
                )
            event = _record_event(
                repository,
                rule=rule,
                subscription=subscription,
                performance=performance,
                status="triggered",
                reason="conditions_matched",
                matched_conditions=matched,
                dispatch_result=dispatch_result,
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
    return result
