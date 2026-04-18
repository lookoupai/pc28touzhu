"""PC28 轻量开奖抓取服务。"""
from __future__ import annotations

import json
import os
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from pc28touzhu.domain.settlement_rules import derive_pc28_draw_snapshot


PC28_API_BASE_URL = os.getenv("PC28_API_BASE_URL", "https://pc28.help").rstrip("/")
PC28_REQUEST_TIMEOUT = max(3, int(os.getenv("PC28_REQUEST_TIMEOUT", "10")))
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


def _default_fetcher(url: str, params: Optional[dict] = None, headers: Optional[dict] = None, timeout: int = 10) -> Any:
    query = urlencode(params or {})
    request_url = url + (("&" if "?" in url else "?") + query if query else "")
    request = Request(request_url, headers=headers or {})
    with urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _safe_fetch(fetcher: Optional[Fetcher], url: str, params: Optional[dict] = None, timeout: int = PC28_REQUEST_TIMEOUT) -> Any:
    effective_fetcher = fetcher or _default_fetcher
    return effective_fetcher(url, params, dict(DEFAULT_BROWSER_HEADERS), timeout)


def _normalize_draw(item: dict, *, source: str) -> Optional[dict]:
    if not isinstance(item, dict):
        return None
    issue_no = ""
    open_code = ""
    result_number = None

    if source == "official":
        issue_no = str(item.get("nbr") or "").strip()
        open_code = str(item.get("number") or item.get("num") or "").strip()
        result_number = item.get("num") or item.get("number")
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
        "draw_context": {
            "open_code": open_code or snapshot.get("result_number"),
            "result_number": snapshot.get("result_number"),
            "triplet": snapshot.get("triplet"),
            "big_small": snapshot.get("big_small"),
            "odd_even": snapshot.get("odd_even"),
            "combo": snapshot.get("combo"),
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
