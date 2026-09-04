"""PC28 轻量开奖抓取服务。"""
from __future__ import annotations

import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pc28touzhu.domain.settlement_rules import derive_pc28_draw_snapshot


PC28_API_BASE_URL = os.getenv("PC28_API_BASE_URL", "https://pc28.help").rstrip("/")
PC28_REQUEST_TIMEOUT = max(3, int(os.getenv("PC28_REQUEST_TIMEOUT", "10")))
PC28_DRAW_CLOCK_TTL_SECONDS = max(0, int(os.getenv("PC28_DRAW_CLOCK_TTL_SECONDS", "10")))
PC28_JND_RECENT_URL = os.getenv("PC28_JND_RECENT_URL", "https://jnd-28.vip/api/recent")
PC28_FEIJI_RECENT_URL = os.getenv("PC28_FEIJI_RECENT_URL", "https://feiji28.com/api/keno/latest")
PC28_RECENT_SOURCE_ORDER = tuple(
    item.strip().lower()
    for item in os.getenv("PC28_RECENT_SOURCE_ORDER", "official,jnd,feiji").split(",")
    if item.strip()
)

DEFAULT_BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0 Safari/537.36",
    "Accept": "application/json,text/plain,*/*",
}


Fetcher = Callable[[str, Optional[dict], Optional[dict], int], Any]

# 官方接口的 date/time 是北京时（UTC+8），入库统一转 UTC。
_CN_TIMEZONE = timezone(timedelta(hours=8))

_DRAW_CLOCK_LOCK = threading.Lock()
_DRAW_CLOCK_CACHE: Dict[str, Any] = {}


def _default_fetcher(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 10) -> Any:
    query = urlencode(params or {})
    request_url = url + (("&" if "?" in url else "?") + query if query else "")
    request = Request(request_url, headers=headers or {})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _safe_fetch(fetcher: Optional[Fetcher], url: str, params: Optional[dict] = None, timeout: int = PC28_REQUEST_TIMEOUT) -> Any:
    effective_fetcher = fetcher or _default_fetcher
    return effective_fetcher(url, params, dict(DEFAULT_BROWSER_HEADERS), timeout)


def _utc_now_text() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_countdown_seconds(value: Any) -> Optional[int]:
    """解析 countdown，兼容 MM:SS 与 HH:MM:SS；'--:--:--' 之类返回 None。"""
    parts = str(value or "").strip().split(":")
    if len(parts) not in (2, 3) or not all(part.isdigit() for part in parts):
        return None
    seconds = 0
    for part in parts:
        seconds = seconds * 60 + int(part)
    return seconds


def _parse_open_time(date_text: Any, time_text: Any) -> Optional[str]:
    """把官方接口的北京时 date + time 合成 UTC ISO(Z) 开奖时刻。"""
    combined = "%s %s" % (str(date_text or "").strip(), str(time_text or "").strip())
    try:
        parsed = datetime.strptime(combined, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None
    return parsed.replace(tzinfo=_CN_TIMEZONE).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_draw(item: dict, *, source: str) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    issue_no = ""
    open_code = ""
    result_number = None
    open_time = None

    if source == "official":
        issue_no = str(item.get("nbr") or "").strip()
        open_code = str(item.get("number") or item.get("num") or "").strip()
        result_number = item.get("num") or item.get("number")
        open_time = _parse_open_time(item.get("date"), item.get("time"))
    elif source == "jnd":
        issue_no = str(item.get("draw_number") or "").strip()
        open_code = str(item.get("number") or item.get("canada28_result") or "").strip()
        result_number = item.get("canada28_result")
    elif source == "feiji":
        issue_no = str(item.get("draw_nbr") or "").strip()
        open_code = str(item.get("number") or item.get("open_code") or item.get("final_sum") or "").strip()
        result_number = item.get("final_sum")
    else:
        return None

    if not issue_no:
        return None
    snapshot = derive_pc28_draw_snapshot(
        {
            "open_code": open_code,
            "result_number": result_number,
            "open_time": open_time,
        }
    )
    if snapshot.get("result_number") is None:
        return None
    return {
        "issue_no": issue_no,
        "result_number": snapshot.get("result_number"),
        "big_small": snapshot.get("big_small"),
        "odd_even": snapshot.get("odd_even"),
        "combo": snapshot.get("combo"),
        "triplet": snapshot.get("triplet"),
        "open_time": snapshot.get("open_time"),
        "draw_context": {
            "open_code": open_code or snapshot.get("result_number"),
            "result_number": snapshot.get("result_number"),
            "triplet": snapshot.get("triplet"),
            "big_small": snapshot.get("big_small"),
            "odd_even": snapshot.get("odd_even"),
            "combo": snapshot.get("combo"),
            "open_time": snapshot.get("open_time"),
        },
        "source_payload": dict(item),
        "source": source,
    }


def _fetch_official_recent_draws(limit: int, *, fetcher: Optional[Fetcher]) -> list[dict]:
    payload = _safe_fetch(fetcher, PC28_API_BASE_URL + "/api/kj.json", {"nbr": limit})
    if not isinstance(payload, dict) or payload.get("message") != "success":
        raise ValueError("PC28 官方开奖接口返回异常")
    items = payload.get("data") or []
    return [draw for draw in (_normalize_draw(item, source="official") for item in items) if draw]


def fetch_pc28_draw_clock(*, fetcher: Optional[Fetcher] = None) -> Dict[str, Any]:
    """抓取开奖时钟：最近已开奖期号 + 该期开奖时刻 + 距下一期开奖秒数。"""
    payload = _safe_fetch(fetcher, PC28_API_BASE_URL + "/api/kj.json", {"nbr": 1})
    if not isinstance(payload, dict) or payload.get("message") != "success":
        raise ValueError("PC28 官方开奖接口返回异常")
    items = payload.get("data") if isinstance(payload.get("data"), list) else []
    latest = items[0] if items and isinstance(items[0], dict) else {}
    latest_issue_no = str(latest.get("nbr") or "").strip()
    if not latest_issue_no:
        raise ValueError("PC28 官方开奖接口未返回期号")
    return {
        "latest_issue_no": latest_issue_no,
        "latest_open_time": _parse_open_time(latest.get("date"), latest.get("time")),
        "countdown_seconds": _parse_countdown_seconds(payload.get("countdown")),
        "fetched_at": _utc_now_text(),
        "stale": False,
    }


def get_pc28_draw_clock(
    *,
    fetcher: Optional[Fetcher] = None,
    ttl_seconds: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """带短 TTL 缓存的开奖时钟。

    抓取失败时退回上一次结果并标记 stale —— 期距固定 210 秒且停机只会让开奖更晚，
    陈旧时钟按「countdown 扣除已流逝时间」仍是安全下界，比完全没有时钟可用。
    """
    ttl = PC28_DRAW_CLOCK_TTL_SECONDS if ttl_seconds is None else max(0, int(ttl_seconds))
    with _DRAW_CLOCK_LOCK:
        cached = _DRAW_CLOCK_CACHE.get("value")
        cached_at = _DRAW_CLOCK_CACHE.get("cached_at")
        if cached and isinstance(cached_at, float) and (time.monotonic() - cached_at) < ttl:
            return dict(cached)
        try:
            clock = fetch_pc28_draw_clock(fetcher=fetcher)
        except Exception:
            return {**cached, "stale": True} if cached else None
        _DRAW_CLOCK_CACHE["value"] = clock
        _DRAW_CLOCK_CACHE["cached_at"] = time.monotonic()
        return dict(clock)


def _fetch_jnd_recent_draws(limit: int, *, fetcher: Optional[Fetcher]) -> list[dict]:
    payload = _safe_fetch(fetcher, PC28_JND_RECENT_URL, {"limit": limit})
    if not isinstance(payload, list):
        raise ValueError("JND28 开奖接口返回异常")
    return [draw for draw in (_normalize_draw(item, source="jnd") for item in payload) if draw]


def _fetch_feiji_recent_draws(limit: int, *, fetcher: Optional[Fetcher]) -> list[dict]:
    return _fetch_feiji_recent_draws_page(limit, offset=0, fetcher=fetcher)


def _fetch_feiji_recent_draws_page(limit: int, *, offset: int = 0, fetcher: Optional[Fetcher]) -> list[dict]:
    payload = _safe_fetch(fetcher, PC28_FEIJI_RECENT_URL, {"limit": limit, "offset": max(0, int(offset or 0))})
    data = payload.get("data") if isinstance(payload, dict) else None
    if not isinstance(data, list):
        raise ValueError("Feiji28 开奖接口返回异常")
    return [draw for draw in (_normalize_draw(item, source="feiji") for item in data) if draw]


def _dedupe_draws(items: list[dict]) -> list[dict]:
    deduped = []
    seen: set[str] = set()
    for item in items or []:
        issue_no = str((item or {}).get("issue_no") or "").strip()
        if not issue_no or issue_no in seen:
            continue
        seen.add(issue_no)
        deduped.append(item)
    return deduped


def fetch_pc28_recent_draws(limit: int = 50, *, fetcher: Optional[Fetcher] = None) -> Dict[str, Any]:
    normalized_limit = max(1, min(int(limit or 50), 100))
    errors = []
    for source in PC28_RECENT_SOURCE_ORDER or ("official", "jnd", "feiji"):
        try:
            if source == "official":
                items = _fetch_official_recent_draws(normalized_limit, fetcher=fetcher)
            elif source == "jnd":
                items = _fetch_jnd_recent_draws(normalized_limit, fetcher=fetcher)
            elif source == "feiji":
                items = _fetch_feiji_recent_draws(normalized_limit, fetcher=fetcher)
            else:
                errors.append("%s: 未知数据源" % source)
                continue
            if items:
                return {"items": items, "source": source}
            errors.append("%s: 返回空数据" % source)
        except Exception as exc:
            errors.append("%s: %s" % (source, exc))
    raise RuntimeError("PC28 最近开奖接口全部不可用: " + " | ".join(errors))


def fetch_pc28_recent_draws_deep(limit: int = 200, *, fetcher: Optional[Fetcher] = None) -> Dict[str, Any]:
    normalized_limit = max(1, min(int(limit or 200), 2000))
    errors = []
    best_result: Optional[Dict[str, Any]] = None

    for source in PC28_RECENT_SOURCE_ORDER or ("official", "jnd", "feiji"):
        try:
            if source == "official":
                items = _fetch_official_recent_draws(normalized_limit, fetcher=fetcher)
            elif source == "jnd":
                items = _fetch_jnd_recent_draws(normalized_limit, fetcher=fetcher)
            elif source == "feiji":
                items = []
                offset = 0
                while len(items) < normalized_limit:
                    batch_limit = min(200, normalized_limit - len(items))
                    batch = _fetch_feiji_recent_draws_page(batch_limit, offset=offset, fetcher=fetcher)
                    if not batch:
                        break
                    items.extend(batch)
                    deduped = _dedupe_draws(items)
                    if len(deduped) >= normalized_limit or len(batch) < batch_limit:
                        items = deduped
                        break
                    items = deduped
                    offset += max(1, len(batch) - 1)
            else:
                errors.append("%s: 未知数据源" % source)
                continue

            items = _dedupe_draws(items)
            if not items:
                errors.append("%s: 返回空数据" % source)
                continue
            current = {"items": items[:normalized_limit], "source": source}
            if len(current["items"]) >= normalized_limit:
                return current
            if best_result is None or len(current["items"]) > len(best_result["items"]):
                best_result = current
            errors.append("%s: 仅返回 %s 条开奖数据" % (source, len(current["items"])))
        except Exception as exc:
            errors.append("%s: %s" % (source, exc))

    if best_result:
        return best_result
    raise RuntimeError("PC28 历史开奖接口全部不可用: " + " | ".join(errors))
