from __future__ import annotations

from dataclasses import dataclass
from typing import Optional
from urllib.parse import urlparse


@dataclass(frozen=True)
class TargetKeyParseResult:
    normalized: str
    source: str


def _strip_leading_at(text: str) -> str:
    value = text.strip()
    return value[1:] if value.startswith("@") else value


def _parse_telegram_url(raw_url: str) -> Optional[TargetKeyParseResult]:
    text = str(raw_url or "").strip()
    if not text:
        return None

    candidate = text
    if candidate.startswith("t.me/") or candidate.startswith("telegram.me/"):
        candidate = "https://" + candidate

    if not (candidate.startswith("http://") or candidate.startswith("https://")):
        return None

    parsed = urlparse(candidate)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    if host not in {"t.me", "telegram.me"}:
        return None

    parts = [segment for segment in (parsed.path or "").split("/") if segment]
    if not parts:
        return None

    head = parts[0]
    if head.startswith("+") or head == "joinchat":
        raise ValueError("邀请链接无法解析，请使用 @username 或 -100...，或用 @userinfobot 获取 Chat ID")

    if head == "c" and len(parts) >= 2 and parts[1].isdigit():
        return TargetKeyParseResult(normalized="-100%s" % parts[1], source="t.me/c")

    if head == "s" and len(parts) >= 2:
        return TargetKeyParseResult(normalized=_strip_leading_at(parts[1]), source="t.me/s")

    return TargetKeyParseResult(normalized=_strip_leading_at(head), source="t.me")


def normalize_telegram_target_key(raw_value: str) -> str:
    text = str(raw_value or "").strip()
    if not text:
        raise ValueError("target_key 不能为空")

    parsed = _parse_telegram_url(text)
    if parsed is not None:
        return parsed.normalized

    normalized = _strip_leading_at(text)
    if not normalized:
        raise ValueError("target_key 不能为空")
    if any(char.isspace() for char in normalized) or "/" in normalized:
        raise ValueError("target_key 格式不正确，请使用 @username、-100... 或 t.me 链接")
    return normalized


def normalize_target_key(raw_value: str) -> str:
    return normalize_telegram_target_key(raw_value)
