"""Minimal source adapter for fetching external JSON into raw items."""
from __future__ import annotations

import json
import urllib.error
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, Optional

DEFAULT_FETCH_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/123.0.0.0 Safari/537.36"
    ),
}


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _extract_path(payload: Any, path: str) -> Any:
    segments = [item for item in str(path or "").split(".") if item]
    if not segments:
        return None

    current = payload
    for segment in segments:
        if not isinstance(current, dict) or segment not in current:
            return None
        current = current[segment]
    return current


def _merge_headers(headers: Optional[Dict[str, str]]) -> Dict[str, str]:
    merged = dict(DEFAULT_FETCH_HEADERS)
    if isinstance(headers, dict):
        for key, value in headers.items():
            key_text = str(key).strip()
            if not key_text:
                continue
            merged[key_text] = str(value)
    return merged


def _read_http_error_body(error: urllib.error.HTTPError) -> str:
    try:
        body = error.read().decode("utf-8", errors="ignore").strip()
    except Exception:
        body = ""
    return body[:280]


def _default_fetch_json(url: str, *, headers: Optional[Dict[str, str]] = None, timeout: int = 10) -> Any:
    request = urllib.request.Request(url, headers=_merge_headers(headers), method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return json.loads(response.read().decode(charset))
    except urllib.error.HTTPError as exc:
        body = _read_http_error_body(exc)
        message = "上游接口返回 HTTP %s %s" % (exc.code, exc.reason)
        if body:
            message = "%s：%s" % (message, body)
        raise ValueError(message) from exc
    except urllib.error.URLError as exc:
        raise ValueError("请求上游接口失败：%s" % exc.reason) from exc


def _resolve_fetch_config(source: Dict[str, Any]) -> Dict[str, Any]:
    config = source.get("config") or {}
    return config.get("fetch") if isinstance(config.get("fetch"), dict) else config


def _find_existing_raw_item(
    repository: Any,
    *,
    source_id: int,
    external_item_id: Optional[str],
    issue_no: str,
    published_at: str,
) -> Optional[Dict[str, Any]]:
    normalized_external_item_id = str(external_item_id or "").strip()
    normalized_issue_no = str(issue_no or "").strip()
    normalized_published_at = str(published_at or "").strip()
    for item in repository.list_raw_items(source_id=int(source_id)):
        if normalized_external_item_id and str(item.get("external_item_id") or "").strip() == normalized_external_item_id:
            return item
        if (
            normalized_issue_no
            and str(item.get("issue_no") or "").strip() == normalized_issue_no
            and normalized_published_at
            and str(item.get("published_at") or "").strip() == normalized_published_at
        ):
            return item
    return None


def _create_http_json_raw_item(repository: Any, source_id: int, fetch_config: Dict[str, Any], payload: Any) -> tuple[Dict[str, Any], bool]:
    external_item_id = _extract_path(payload, str(fetch_config.get("external_item_id_path") or ""))
    issue_no = _extract_path(payload, str(fetch_config.get("issue_no_path") or ""))
    published_at = _extract_path(payload, str(fetch_config.get("published_at_path") or ""))

    if isinstance(payload, dict):
        if external_item_id in {None, ""}:
            external_item_id = payload.get("external_item_id") or payload.get("signal_id") or payload.get("id")
        if issue_no in {None, ""}:
            issue_no = payload.get("issue_no")
        if published_at in {None, ""}:
            published_at = payload.get("published_at")

    normalized_external_item_id = str(external_item_id) if external_item_id not in {None, ""} else None
    normalized_issue_no = str(issue_no) if issue_no not in {None, ""} else ""
    normalized_published_at = str(published_at) if published_at not in {None, ""} else _utc_now_iso()
    existing = _find_existing_raw_item(
        repository,
        source_id=source_id,
        external_item_id=normalized_external_item_id,
        issue_no=normalized_issue_no,
        published_at=normalized_published_at,
    )
    if existing:
        return existing, False

    return repository.create_raw_item_record(
        source_id=int(source_id),
        external_item_id=normalized_external_item_id,
        issue_no=normalized_issue_no,
        published_at=normalized_published_at,
        raw_payload=payload if isinstance(payload, dict) else {"data": payload},
        parse_status="pending",
        parse_error=None,
    ), True


def _create_ai_trading_simulator_raw_item(
    repository: Any,
    source_id: int,
    fetch_config: Dict[str, Any],
    payload: Any,
) -> tuple[Dict[str, Any], bool]:
    if not isinstance(payload, dict):
        raise ValueError("AITradingSimulator 导出接口必须返回 JSON 对象")

    items = payload.get("items")
    if not isinstance(items, list) or not items:
        raise ValueError("AITradingSimulator 导出接口未返回 items 列表")

    first_item = items[0] if isinstance(items[0], dict) else {}
    external_item_id = str(first_item.get("signal_id") or "").strip() or None
    issue_no = str(first_item.get("issue_no") or "").strip()
    published_at = str(first_item.get("published_at") or "").strip() or _utc_now_iso()

    existing = _find_existing_raw_item(
        repository,
        source_id=source_id,
        external_item_id=external_item_id,
        issue_no=issue_no,
        published_at=published_at,
    )
    if existing:
        return existing, False

    return repository.create_raw_item_record(
        source_id=int(source_id),
        external_item_id=external_item_id,
        issue_no=issue_no,
        published_at=published_at,
        raw_payload=payload,
        parse_status="pending",
        parse_error=None,
    ), True


def fetch_source_to_raw_item(repository: Any, source_id: int, fetcher=None) -> Dict[str, Any]:
    source = repository.get_source(source_id)
    if not source:
        raise ValueError("source 不存在")

    source_type = str(source.get("source_type") or "").strip()
    if source_type not in {"http_json", "ai_trading_simulator_export"}:
        raise ValueError("当前仅支持 source_type=http_json 或 ai_trading_simulator_export 的自动抓取")

    fetch_config = _resolve_fetch_config(source)
    url = str(fetch_config.get("url") or "").strip()
    if not url:
        raise ValueError("source.config.url 或 source.config.fetch.url 不能为空")

    headers = _merge_headers(fetch_config.get("headers") if isinstance(fetch_config.get("headers"), dict) else {})
    timeout = int(fetch_config.get("timeout") or 10)
    payload = (fetcher or _default_fetch_json)(url, headers=headers, timeout=timeout)

    raw_item, created = (
        _create_ai_trading_simulator_raw_item(repository, source_id, fetch_config, payload)
        if source_type == "ai_trading_simulator_export"
        else _create_http_json_raw_item(repository, source_id, fetch_config, payload)
    )
    return {"source": source, "raw_item": raw_item, "created": created}
