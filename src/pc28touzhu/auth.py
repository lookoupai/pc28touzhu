"""Lightweight auth helpers for the WSGI app."""
from __future__ import annotations

import base64
import hashlib
import hmac
import os
import time
from http.cookies import SimpleCookie
from typing import Optional


SESSION_COOKIE_NAME = "platform_session"
PASSWORD_PREFIX = "pbkdf2_sha256"
PASSWORD_ITERATIONS = 120000


def hash_password(password: str, *, salt: str | None = None) -> str:
    raw_password = str(password or "")
    if not raw_password:
        raise ValueError("password 不能为空")
    salt_value = salt or os.urandom(16).hex()
    digest = hashlib.pbkdf2_hmac(
        "sha256",
        raw_password.encode("utf-8"),
        salt_value.encode("utf-8"),
        PASSWORD_ITERATIONS,
    ).hex()
    return "%s$%s$%s" % (PASSWORD_PREFIX, salt_value, digest)


def verify_password(password: str, encoded_password: str) -> bool:
    try:
        algorithm, salt, expected = str(encoded_password or "").split("$", 2)
    except ValueError:
        return False
    if algorithm != PASSWORD_PREFIX:
        return False
    return hmac.compare_digest(hash_password(password, salt=salt), encoded_password)


def _sign(value: str, secret: str) -> str:
    return hmac.new(secret.encode("utf-8"), value.encode("utf-8"), hashlib.sha256).hexdigest()


def build_session_cookie_value(user_id: int, secret: str, *, session_version: int = 1) -> str:
    timestamp = str(int(time.time()))
    payload = "%s:%s:%s" % (int(user_id), max(1, int(session_version or 1)), timestamp)
    signed = "%s:%s" % (payload, _sign(payload, secret))
    return base64.urlsafe_b64encode(signed.encode("utf-8")).decode("ascii")


def parse_session_cookie_value(
    value: str,
    secret: str,
    *,
    max_age_seconds: int = 2592000,
) -> Optional[tuple[int, int]]:
    try:
        decoded = base64.urlsafe_b64decode(str(value or "").encode("ascii")).decode("utf-8")
        parts = decoded.split(":")
    except Exception:
        return None

    if len(parts) == 3:
        user_id_text, timestamp_text, signature = parts
        session_version_text = "1"
        payload = "%s:%s" % (user_id_text, timestamp_text)
    elif len(parts) == 4:
        user_id_text, session_version_text, timestamp_text, signature = parts
        payload = "%s:%s:%s" % (user_id_text, session_version_text, timestamp_text)
    else:
        return None

    if not hmac.compare_digest(signature, _sign(payload, secret)):
        return None

    try:
        timestamp = int(timestamp_text)
        user_id = int(user_id_text)
        session_version = int(session_version_text)
    except ValueError:
        return None

    if user_id <= 0:
        return None
    if session_version <= 0:
        return None
    if max_age_seconds > 0 and (int(time.time()) - timestamp) > max_age_seconds:
        return None
    return user_id, session_version


def get_cookie_value(environ: dict, name: str) -> Optional[str]:
    raw_cookie = str(environ.get("HTTP_COOKIE") or "").strip()
    if not raw_cookie:
        return None
    cookie = SimpleCookie()
    cookie.load(raw_cookie)
    morsel = cookie.get(name)
    return morsel.value if morsel else None


def build_set_cookie_header(
    name: str,
    value: str,
    *,
    path: str = "/",
    http_only: bool = True,
    max_age: Optional[int] = None,
    same_site: str = "Lax",
    secure: bool = False,
) -> tuple[str, str]:
    cookie = SimpleCookie()
    cookie[name] = value
    cookie[name]["path"] = path
    if http_only:
        cookie[name]["httponly"] = True
    if max_age is not None:
        cookie[name]["max-age"] = max_age
    if same_site:
        cookie[name]["samesite"] = same_site
    if secure:
        cookie[name]["secure"] = True
    return ("Set-Cookie", cookie.output(header="").strip())
