"""Minimal source adapter for fetching external JSON into raw items."""
from __future__ import annotations

import json
import hashlib
import time
from collections import OrderedDict
from dataclasses import dataclass
from threading import RLock
from weakref import WeakKeyDictionary
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


@dataclass
class _HTTPJSONResult:
    payload: Any = None
    etag: Optional[str] = None
    not_modified: bool = False


class _ValidatorCache:
    def __init__(self):
        self.entries = OrderedDict()
        self.lock = RLock()

    def get(self, key):
        with self.lock:
            entry = self.entries.get(key)
            if entry and entry['expires_at'] > time.monotonic():
                self.entries.move_to_end(key)
                return dict(entry)
            self.entries.pop(key, None)
            return None

    def put(self, key, entry):
        with self.lock:
            if not entry or not entry.get('etag'):
                self.entries.pop(key, None)
                return
            self.entries[key] = dict(entry)
            self.entries.move_to_end(key)
            while len(self.entries) > 128:
                self.entries.popitem(last=False)


_validator_caches = WeakKeyDictionary()
_validator_lock = RLock()


def _validator_cache_for(repository):
    with _validator_lock:
        if repository not in _validator_caches:
            _validator_caches[repository] = _ValidatorCache()
        return _validator_caches[repository]


def _unchanged_source_result(source):
    return {'source': source, 'raw_item': None, 'created': False,
            'skipped': True, 'skipped_reason': 'upstream_not_modified'}


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


def _default_fetch_json(url: str, *, headers: Optional[Dict[str, str]] = None, timeout: int = 10, include_metadata: bool = False) -> Any:
    request = urllib.request.Request(url, headers=_merge_headers(headers), method="GET")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            payload = json.loads(response.read().decode(charset))
            if include_metadata:
                return _HTTPJSONResult(payload=payload, etag=response.headers.get('ETag'))
            return payload
    except urllib.error.HTTPError as exc:
        if exc.code == 304 and include_metadata:
            etag = exc.headers.get('ETag') if exc.headers else None
            exc.close()
            return _HTTPJSONResult(etag=etag, not_modified=True)
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
    return repository.find_raw_item_by_identity(
        source_id=int(source_id),
        external_item_id=external_item_id,
        issue_no=issue_no,
        published_at=published_at,
    )


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
    validator_cache = None
    validator_key = None
    response_etag = None
    if fetcher is None and source_type == 'ai_trading_simulator_export':
        validator_cache = _validator_cache_for(repository)
        # Isolate URLs, credentials and adapter settings. A source/config change
        # must never reuse another source's validator or already-parsed marker.
        validator_key = (int(source_id), hashlib.sha256(json.dumps(
            [url, headers, fetch_config], sort_keys=True, ensure_ascii=False
        ).encode('utf-8')).hexdigest())
        cached = validator_cache.get(validator_key)
        conditional_headers = {k: v for k, v in headers.items() if k.lower() != 'if-none-match'}
        if cached:
            conditional_headers['If-None-Match'] = cached['etag']
        response = _default_fetch_json(url, headers=conditional_headers, timeout=timeout, include_metadata=True)
        if response.not_modified:
            if cached and (cached['raw_item_id'] is None or cached['parsed']):
                return _unchanged_source_result(source)
            if cached:
                raw_item = repository.get_raw_item(cached['raw_item_id'])
                if raw_item:
                    # Preserve retries when a previous normalize step failed.
                    if raw_item.get('parse_status') == 'parsed':
                        cached['parsed'] = True
                        validator_cache.put(validator_key, cached)
                        return _unchanged_source_result(source)
                    return {'source': source, 'raw_item': raw_item, 'created': False}
            # A pruned raw item or missing local state needs a full response.
            conditional_headers.pop('If-None-Match', None)
            response = _default_fetch_json(url, headers=conditional_headers, timeout=timeout, include_metadata=True)
            if response.not_modified:
                raise ValueError('上游在未携带缓存版本时仍返回 HTTP 304')
        payload, response_etag = response.payload, response.etag
    else:
        payload = (fetcher or _default_fetch_json)(url, headers=headers, timeout=timeout)

    def remember(raw_item=None):
        if validator_cache is not None:
            validator_cache.put(validator_key, {
                'etag': response_etag,
                'raw_item_id': raw_item.get('id') if raw_item else None,
                'parsed': raw_item.get('parse_status') == 'parsed' if raw_item else True,
                'expires_at': time.monotonic() + 60,
            })

    # 共识类来源没有满足门槛的期号时用空 items 表示“本期无信号”，这是正常状态。
    # 不创建 raw_item，避免后续标准化把空结果误报成来源故障。
    if source_type == "ai_trading_simulator_export" and isinstance(payload, dict) and payload.get("items") == []:
        remember()
        return {
            "source": source,
            "raw_item": None,
            "created": False,
            "skipped": True,
            "skipped_reason": "upstream_no_signal",
        }

    raw_item, created = (
        _create_ai_trading_simulator_raw_item(repository, source_id, fetch_config, payload)
        if source_type == "ai_trading_simulator_export"
        else _create_http_json_raw_item(repository, source_id, fetch_config, payload)
    )
    remember(raw_item)
    return {"source": source, "raw_item": raw_item, "created": created}
