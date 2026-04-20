"""Minimal WSGI application for executor-facing APIs."""
from __future__ import annotations

import json
import re
from io import BytesIO
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Optional, Tuple
from urllib.parse import parse_qs

from pc28touzhu.auth import (
    SESSION_COOKIE_NAME,
    build_session_cookie_value,
    build_set_cookie_header,
    get_cookie_value,
    hash_password,
    parse_session_cookie_value,
    verify_password,
)
from pc28touzhu.services.job_service import heartbeat_executor, pull_jobs, report_job
from pc28touzhu.services.platform_service import (
    begin_telegram_account_login,
    create_user,
    create_delivery_target,
    create_message_template,
    list_executor_instances,
    list_recent_execution_failures,
    list_platform_alerts,
    list_execution_jobs,
    list_message_templates,
    list_support_snapshot,
    retry_execution_job,
    create_telegram_account,
    create_raw_item,
    create_signal,
    create_source,
    create_subscription,
    delete_source,
    dispatch_signal,
    delete_delivery_target,
    delete_subscription,
    delete_telegram_account,
    fetch_source,
    import_telegram_account_session,
    list_users,
    list_delivery_targets,
    list_telegram_accounts,
    list_raw_items,
    list_signals,
    list_sources,
    list_subscriptions,
    normalize_raw_item,
    restart_subscription_cycle,
    reset_subscription_runtime,
    resolve_subscription_progression,
    resolve_pending_subscription_progressions,
    settle_subscription_progression,
    update_source,
    update_source_status,
    update_delivery_target,
    update_delivery_target_status,
    update_message_template,
    update_message_template_status,
    test_delivery_target_send,
    update_subscription,
    update_subscription_status,
    update_telegram_account,
    update_telegram_account_status,
    verify_telegram_account_login_code,
    verify_telegram_account_login_password,
)
from pc28touzhu.services.telegram_bot_service import (
    clear_telegram_binding,
    create_telegram_bind_token,
    get_telegram_binding_status,
)
from pc28touzhu.services.telegram_runtime_settings_service import (
    get_telegram_runtime_settings_for_admin,
    update_telegram_runtime_settings,
)


StartResponse = Callable[[str, list], None]
UI_DIR = Path(__file__).resolve().parents[1] / "ui"
ADMIN_PAGE_ROUTES = {
    "/admin",
    "/admin/sources",
    "/admin/signals",
    "/admin/execution",
    "/admin/alerts",
    "/admin/support",
    "/admin/telegram",
}


def _json_response(
    start_response: StartResponse,
    status_code: int,
    payload: Dict[str, Any],
    extra_headers: Optional[list[tuple[str, str]]] = None,
) -> Iterable[bytes]:
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    status_text = {
        200: "200 OK",
        400: "400 Bad Request",
        401: "401 Unauthorized",
        404: "404 Not Found",
        405: "405 Method Not Allowed",
        500: "500 Internal Server Error",
    }.get(status_code, f"{status_code} Unknown")
    headers = [
        ("Content-Type", "application/json; charset=utf-8"),
        ("Content-Length", str(len(body))),
    ]
    if extra_headers:
        headers.extend(extra_headers)
    start_response(status_text, headers)
    return [body]


def _text_response(
    start_response: StartResponse,
    status_code: int,
    body: str,
    content_type: str,
    extra_headers: Optional[list[tuple[str, str]]] = None,
) -> Iterable[bytes]:
    data = body.encode("utf-8")
    status_text = {
        200: "200 OK",
        401: "401 Unauthorized",
        403: "403 Forbidden",
        404: "404 Not Found",
        405: "405 Method Not Allowed",
        500: "500 Internal Server Error",
    }.get(status_code, f"{status_code} Unknown")
    headers = [
        ("Content-Type", content_type),
        ("Content-Length", str(len(data))),
    ]
    if content_type.startswith("text/html"):
        headers.append(("Cache-Control", "no-store, max-age=0"))
    if extra_headers:
        headers.extend(extra_headers)
    start_response(status_text, headers)
    return [data]


def _load_ui_file(filename: str) -> str:
    path = UI_DIR / filename
    if not path.exists():
        raise FileNotFoundError(filename)
    return path.read_text(encoding="utf-8")


def _asset_version(filename: str) -> str:
    path = UI_DIR / filename
    if not path.exists():
        return "0"
    return str(int(path.stat().st_mtime))


def _load_ui_html_file(filename: str) -> str:
    text = _load_ui_file(filename)

    def replace_asset(match: re.Match[str]) -> str:
        asset_name = str(match.group(1) or "").strip()
        if not asset_name:
            return match.group(0)
        return '/assets/%s?v=%s' % (asset_name, _asset_version(asset_name))

    return re.sub(r'/assets/([A-Za-z0-9_.-]+)', replace_asset, text)


def _read_json_body(environ: Dict[str, Any]) -> Dict[str, Any]:
    raw_length = environ.get("CONTENT_LENGTH") or "0"
    try:
        content_length = int(raw_length)
    except (TypeError, ValueError):
        content_length = 0

    data = environ["wsgi.input"].read(content_length) if content_length > 0 else b""
    if not data:
        return {}

    payload = json.loads(data.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("请求体必须为 JSON 对象")
    return payload


def _parse_authorization(environ: Dict[str, Any]) -> str:
    header = str(environ.get("HTTP_AUTHORIZATION") or "").strip()
    if not header.startswith("Bearer "):
        return ""
    return header[7:].strip()


def _query_value(environ: Dict[str, Any], key: str, default: str = "") -> str:
    query_string = str(environ.get("QUERY_STRING") or "")
    values = parse_qs(query_string).get(key)
    return values[0] if values else default


class PlatformApiApplication:
    def __init__(
        self,
        repository: Any,
        executor_api_token: str,
        session_secret: str,
        platform_config: Any = None,
        telegram_bot_config: Any = None,
        runtime_config: Any = None,
    ):
        self.repository = repository
        self.executor_api_token = executor_api_token
        self.session_secret = session_secret
        self.platform_config = platform_config
        self.telegram_bot_config = telegram_bot_config
        self.runtime_config = runtime_config
        self.executor_stale_after_seconds = int(getattr(platform_config, "executor_stale_after_seconds", 60) or 60)
        self.executor_offline_after_seconds = int(getattr(platform_config, "executor_offline_after_seconds", 300) or 300)
        self.auto_retry_max_attempts = int(getattr(platform_config, "auto_retry_max_attempts", 3) or 3)
        self.auto_retry_base_delay_seconds = int(getattr(platform_config, "auto_retry_base_delay_seconds", 30) or 30)
        self.alert_failure_streak_threshold = int(getattr(platform_config, "alert_failure_streak_threshold", 3) or 3)
        self.telegram_bind_token_ttl_seconds = int(
            getattr(telegram_bot_config, "bind_token_ttl_seconds", 600) or 600
        )

    def __call__(self, environ: Dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        try:
            return self._dispatch(environ, start_response)
        except PermissionError as exc:
            return _json_response(start_response, 401, {"error": str(exc)})
        except ValueError as exc:
            payload = getattr(exc, "payload", None)
            if isinstance(payload, dict) and payload.get("error"):
                return _json_response(start_response, 400, payload)
            return _json_response(start_response, 400, {"error": str(exc)})
        except Exception as exc:
            return _json_response(start_response, 500, {"error": str(exc)})

    def _dispatch(self, environ: Dict[str, Any], start_response: StartResponse) -> Iterable[bytes]:
        method = str(environ.get("REQUEST_METHOD") or "GET").upper()
        path = str(environ.get("PATH_INFO") or "/")
        normalized_path = path if path == "/" else path.rstrip("/")

        if path == "/":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("home.html"), "text/html; charset=utf-8")

        if normalized_path in ADMIN_PAGE_ROUTES:
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            try:
                self._require_admin_console_user(environ)
            except PermissionError as exc:
                status_code = 401 if "登录" in str(exc) else 403
                return _text_response(
                    start_response,
                    status_code,
                    self._render_admin_access_denied(str(exc)),
                    "text/html; charset=utf-8",
                )
            return _text_response(start_response, 200, _load_ui_html_file("dashboard.html"), "text/html; charset=utf-8")

        if path == "/records":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("records.html"), "text/html; charset=utf-8")

        if path == "/autobet":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("autobet.html"), "text/html; charset=utf-8")

        if path == "/autobet/sources":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("autobet.html"), "text/html; charset=utf-8")

        if path == "/autobet/accounts":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("autobet.html"), "text/html; charset=utf-8")

        if path == "/autobet/templates":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("autobet.html"), "text/html; charset=utf-8")

        if path == "/autobet/targets":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("autobet.html"), "text/html; charset=utf-8")

        if path == "/autobet/subscriptions":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("autobet.html"), "text/html; charset=utf-8")

        if path == "/alerts":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_html_file("alerts.html"), "text/html; charset=utf-8")

        if path == "/assets/home.css":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_file("home.css"), "text/css; charset=utf-8")

        if path == "/assets/home.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("home.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/assets/records.css":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_file("records.css"), "text/css; charset=utf-8")

        if path == "/assets/records.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("records.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/assets/autobet.css":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_file("autobet.css"), "text/css; charset=utf-8")

        if path == "/assets/autobet.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("autobet.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/assets/ui-text.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("ui_text.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/assets/account-menu.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("account_menu.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/assets/auth-guard.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("auth_guard.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/assets/auth-panel.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("auth_panel.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/assets/alerts.css":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_file("alerts.css"), "text/css; charset=utf-8")

        if path == "/assets/alerts.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("alerts.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/assets/dashboard.css":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(start_response, 200, _load_ui_file("dashboard.css"), "text/css; charset=utf-8")

        if path == "/assets/dashboard.js":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _text_response(
                start_response,
                200,
                _load_ui_file("dashboard.js"),
                "application/javascript; charset=utf-8",
            )

        if path == "/api/health":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            return _json_response(start_response, 200, {"status": "ok"})

        if path == "/api/auth/register":
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            payload = _read_json_body(environ)
            username = str(payload.get("username") or "").strip()
            password = str(payload.get("password") or "")
            email = str(payload.get("email") or "").strip()
            if not username:
                return _json_response(start_response, 400, {"error": "username 不能为空"})
            if not password:
                return _json_response(start_response, 400, {"error": "password 不能为空"})
            existing_user = self.repository.get_user_by_username(username)
            if existing_user:
                if str(existing_user.get("password_hash") or "").strip():
                    return _json_response(start_response, 400, {"error": "用户名已存在"})
                user = self.repository.update_user_password(
                    existing_user["id"],
                    hash_password(password),
                    email=email or None,
                )
                cookie = build_set_cookie_header(
                    SESSION_COOKIE_NAME,
                    build_session_cookie_value(user["id"], self.session_secret),
                    max_age=2592000,
                )
                return _json_response(
                    start_response,
                    200,
                    {"user": user, "message": "已完成历史账号密码初始化"},
                    extra_headers=[cookie],
                )
            user = create_user(
                self.repository,
                payload={
                    "username": username,
                    "email": email,
                    "password_hash": hash_password(password),
                    "role": "user",
                },
            )["item"]
            cookie = build_set_cookie_header(
                SESSION_COOKIE_NAME,
                build_session_cookie_value(user["id"], self.session_secret),
                max_age=2592000,
            )
            return _json_response(start_response, 200, {"user": user}, extra_headers=[cookie])

        if path == "/api/auth/login":
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            payload = _read_json_body(environ)
            username = str(payload.get("username") or "").strip()
            password = str(payload.get("password") or "")
            user = self.repository.get_user_by_username(username) if username else None
            if user and not str(user.get("password_hash") or "").strip():
                return _json_response(start_response, 401, {"error": "该账号尚未设置密码，请先注册完成初始化"})
            if not user or not verify_password(password, user.get("password_hash") or ""):
                return _json_response(start_response, 401, {"error": "用户名或密码错误"})
            cookie = build_set_cookie_header(
                SESSION_COOKIE_NAME,
                build_session_cookie_value(user["id"], self.session_secret),
                max_age=2592000,
            )
            return _json_response(start_response, 200, {"user": user}, extra_headers=[cookie])

        if path == "/api/auth/logout":
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            cookie = build_set_cookie_header(SESSION_COOKIE_NAME, "", max_age=0)
            return _json_response(start_response, 200, {"message": "已退出登录"}, extra_headers=[cookie])

        if path == "/api/auth/me":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            user = self._get_current_user(environ)
            if not user:
                return _json_response(start_response, 401, {"error": "未登录"})
            return _json_response(start_response, 200, {"user": user})

        if path.startswith("/api/executor/"):
            if not self._is_executor_authorized(environ):
                return _json_response(start_response, 401, {"error": "unauthorized"})
        elif path.startswith("/api/platform/admin/"):
            current_user = self._require_admin_console_user(environ)
        elif path.startswith("/api/platform/"):
            current_user = self._require_platform_user(environ)
        else:
            current_user = None

        if (
            path.startswith("/api/")
            and path.startswith("/api/platform/") is False
            and path.startswith("/api/platform/admin/") is False
            and path.startswith("/api/executor/") is False
        ):
            return _json_response(start_response, 401, {"error": "unauthorized"})

        if path == "/api/platform/admin/support":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            payload = list_support_snapshot(
                self.repository,
                user_id=_query_value(environ, "user_id"),
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/admin/telegram-settings":
            if method == "GET":
                payload = get_telegram_runtime_settings_for_admin(
                    self.repository,
                    runtime_config=self.runtime_config,
                )
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = update_telegram_runtime_settings(
                    self.repository,
                    payload=_read_json_body(environ),
                    runtime_config=self.runtime_config,
                )
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/executor/jobs/pull":
            if method != "GET":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            executor_id = str(environ.get("HTTP_X_EXECUTOR_ID") or "").strip()
            limit = int(_query_value(environ, "limit", "20") or "20")
            payload = pull_jobs(
                self.repository,
                executor_id=executor_id,
                limit=limit,
                auto_retry_max_attempts=self.auto_retry_max_attempts,
                auto_retry_base_delay_seconds=self.auto_retry_base_delay_seconds,
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/executor/heartbeat":
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            executor_id = str(environ.get("HTTP_X_EXECUTOR_ID") or "").strip()
            payload = heartbeat_executor(self.repository, executor_id=executor_id, payload=_read_json_body(environ))
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/users":
            if method == "GET":
                if _query_value(environ, "scope") == "all":
                    payload = list_users(self.repository)
                else:
                    payload = {"items": [current_user]}
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = create_user(self.repository, payload=_read_json_body(environ))
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/platform/sources":
            if method == "GET":
                owner_user_id = current_user["id"]
                if _query_value(environ, "scope") == "all":
                    owner_user_id = None
                elif _query_value(environ, "owner_user_id"):
                    owner_user_id = _query_value(environ, "owner_user_id")
                payload = list_sources(self.repository, owner_user_id=owner_user_id)
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = create_source(self.repository, payload={**_read_json_body(environ), "owner_user_id": current_user["id"]})
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        source_prefix = "/api/platform/sources/"
        if path.startswith(source_prefix) and path.endswith("/delete"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            source_id = path[len(source_prefix) : -len("/delete")]
            payload = delete_source(
                self.repository,
                source_id=source_id,
                owner_user_id=current_user["id"],
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(source_prefix) and path.endswith("/status"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            source_id = path[len(source_prefix) : -len("/status")]
            payload = update_source_status(
                self.repository,
                source_id=source_id,
                owner_user_id=current_user["id"],
                status=_read_json_body(environ).get("status"),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(source_prefix) and path.endswith("/fetch"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            source_id = path[len(source_prefix) : -len("/fetch")]
            payload = fetch_source(self.repository, source_id=source_id)
            return _json_response(start_response, 200, payload)
        if path.startswith(source_prefix):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            source_id = path[len(source_prefix) :]
            payload = update_source(
                self.repository,
                source_id=source_id,
                owner_user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/telegram-accounts":
            if method == "GET":
                payload = list_telegram_accounts(
                    self.repository,
                    user_id=_query_value(environ, "user_id") or current_user["id"],
                )
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = create_telegram_account(self.repository, payload={**_read_json_body(environ), "user_id": current_user["id"]})
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        telegram_account_status_prefix = "/api/platform/telegram-accounts/"
        if path.startswith(telegram_account_status_prefix) and path.endswith("/delete"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            telegram_account_id = path[len(telegram_account_status_prefix) : -len("/delete")]
            payload = delete_telegram_account(
                self.repository,
                telegram_account_id=telegram_account_id,
                user_id=current_user["id"],
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(telegram_account_status_prefix) and path.endswith("/import-session"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            telegram_account_id = path[len(telegram_account_status_prefix) : -len("/import-session")]
            payload = import_telegram_account_session(
                self.repository,
                telegram_account_id=telegram_account_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(telegram_account_status_prefix) and path.endswith("/auth/send-code"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            telegram_account_id = path[len(telegram_account_status_prefix) : -len("/auth/send-code")]
            payload = begin_telegram_account_login(
                self.repository,
                telegram_account_id=telegram_account_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(telegram_account_status_prefix) and path.endswith("/auth/verify-code"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            telegram_account_id = path[len(telegram_account_status_prefix) : -len("/auth/verify-code")]
            payload = verify_telegram_account_login_code(
                self.repository,
                telegram_account_id=telegram_account_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(telegram_account_status_prefix) and path.endswith("/auth/verify-password"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            telegram_account_id = path[len(telegram_account_status_prefix) : -len("/auth/verify-password")]
            payload = verify_telegram_account_login_password(
                self.repository,
                telegram_account_id=telegram_account_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(telegram_account_status_prefix) and path.endswith("/status") is False:
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            telegram_account_id = path[len(telegram_account_status_prefix) :]
            payload = update_telegram_account(
                self.repository,
                telegram_account_id=telegram_account_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)

        if path.startswith(telegram_account_status_prefix) and path.endswith("/status"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            telegram_account_id = path[len(telegram_account_status_prefix) : -len("/status")]
            payload = update_telegram_account_status(
                self.repository,
                telegram_account_id=telegram_account_id,
                user_id=current_user["id"],
                status=_read_json_body(environ).get("status"),
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/subscriptions":
            if method == "GET":
                payload = list_subscriptions(
                    self.repository,
                    user_id=_query_value(environ, "user_id") or current_user["id"],
                )
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = create_subscription(self.repository, payload={**_read_json_body(environ), "user_id": current_user["id"]})
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/platform/telegram-binding":
            if method == "GET":
                payload = get_telegram_binding_status(
                    self.repository,
                    user_id=current_user["id"],
                )
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/platform/telegram-binding/token":
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            body = _read_json_body(environ)
            payload = create_telegram_bind_token(
                self.repository,
                user_id=current_user["id"],
                ttl_seconds=body.get("ttl_seconds") or self.telegram_bind_token_ttl_seconds,
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/telegram-binding/unbind":
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            payload = clear_telegram_binding(
                self.repository,
                user_id=current_user["id"],
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/subscriptions/progression/resolve-batch":
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            payload = resolve_pending_subscription_progressions(
                self.repository,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)

        subscription_status_prefix = "/api/platform/subscriptions/"
        if path.startswith(subscription_status_prefix) and path.endswith("/progression/resolve"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            subscription_id = path[len(subscription_status_prefix) : -len("/progression/resolve")]
            payload = resolve_subscription_progression(
                self.repository,
                subscription_id=subscription_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(subscription_status_prefix) and path.endswith("/progression/settle"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            subscription_id = path[len(subscription_status_prefix) : -len("/progression/settle")]
            payload = settle_subscription_progression(
                self.repository,
                subscription_id=subscription_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(subscription_status_prefix) and path.endswith("/reset"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            subscription_id = path[len(subscription_status_prefix) : -len("/reset")]
            payload = reset_subscription_runtime(
                self.repository,
                subscription_id=subscription_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(subscription_status_prefix) and path.endswith("/restart"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            subscription_id = path[len(subscription_status_prefix) : -len("/restart")]
            payload = restart_subscription_cycle(
                self.repository,
                subscription_id=subscription_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(subscription_status_prefix) and path.endswith("/delete"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            subscription_id = path[len(subscription_status_prefix) : -len("/delete")]
            payload = delete_subscription(
                self.repository,
                subscription_id=subscription_id,
                user_id=current_user["id"],
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(subscription_status_prefix) and path.endswith("/status") is False:
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            subscription_id = path[len(subscription_status_prefix) :]
            payload = update_subscription(
                self.repository,
                subscription_id=subscription_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)

        if path.startswith(subscription_status_prefix) and path.endswith("/status"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            subscription_id = path[len(subscription_status_prefix) : -len("/status")]
            payload = update_subscription_status(
                self.repository,
                subscription_id=subscription_id,
                user_id=current_user["id"],
                status=_read_json_body(environ).get("status"),
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/message-templates":
            if method == "GET":
                payload = list_message_templates(
                    self.repository,
                    user_id=_query_value(environ, "user_id") or current_user["id"],
                )
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = create_message_template(
                    self.repository,
                    payload={**_read_json_body(environ), "user_id": current_user["id"]},
                )
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        message_template_prefix = "/api/platform/message-templates/"
        if path.startswith(message_template_prefix) and path.endswith("/status"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            template_id = path[len(message_template_prefix) : -len("/status")]
            payload = update_message_template_status(
                self.repository,
                template_id=template_id,
                user_id=current_user["id"],
                status=_read_json_body(environ).get("status"),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(message_template_prefix):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            template_id = path[len(message_template_prefix) :]
            payload = update_message_template(
                self.repository,
                template_id=template_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/delivery-targets":
            if method == "GET":
                payload = list_delivery_targets(
                    self.repository,
                    user_id=_query_value(environ, "user_id") or current_user["id"],
                )
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = create_delivery_target(self.repository, payload={**_read_json_body(environ), "user_id": current_user["id"]})
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        delivery_target_status_prefix = "/api/platform/delivery-targets/"
        if path.startswith(delivery_target_status_prefix) and path.endswith("/test-send"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            delivery_target_id = path[len(delivery_target_status_prefix) : -len("/test-send")]
            payload = test_delivery_target_send(
                self.repository,
                delivery_target_id=delivery_target_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(delivery_target_status_prefix) and path.endswith("/delete"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            delivery_target_id = path[len(delivery_target_status_prefix) : -len("/delete")]
            payload = delete_delivery_target(
                self.repository,
                delivery_target_id=delivery_target_id,
                user_id=current_user["id"],
            )
            return _json_response(start_response, 200, payload)
        if path.startswith(delivery_target_status_prefix) and path.endswith("/status") is False:
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            delivery_target_id = path[len(delivery_target_status_prefix) :]
            payload = update_delivery_target(
                self.repository,
                delivery_target_id=delivery_target_id,
                user_id=current_user["id"],
                payload=_read_json_body(environ),
            )
            return _json_response(start_response, 200, payload)

        if path.startswith(delivery_target_status_prefix) and path.endswith("/status"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            delivery_target_id = path[len(delivery_target_status_prefix) : -len("/status")]
            payload = update_delivery_target_status(
                self.repository,
                delivery_target_id=delivery_target_id,
                user_id=current_user["id"],
                status=_read_json_body(environ).get("status"),
            )
            return _json_response(start_response, 200, payload)

        if path == "/api/platform/execution-jobs":
            if method == "GET":
                query_user_id = current_user["id"]
                if _query_value(environ, "scope") == "all":
                    query_user_id = None
                elif _query_value(environ, "user_id"):
                    query_user_id = _query_value(environ, "user_id")
                payload = list_execution_jobs(
                    self.repository,
                    user_id=query_user_id,
                    signal_id=_query_value(environ, "signal_id"),
                    status=_query_value(environ, "status"),
                    limit=_query_value(environ, "limit", "100"),
                )
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/platform/executors":
            if method == "GET":
                payload = list_executor_instances(
                    self.repository,
                    limit=_query_value(environ, "limit", "20"),
                    stale_after_seconds=self.executor_stale_after_seconds,
                    offline_after_seconds=self.executor_offline_after_seconds,
                    failure_streak_threshold=self.alert_failure_streak_threshold,
                )
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/platform/execution-failures":
            if method == "GET":
                query_user_id = current_user["id"]
                if _query_value(environ, "scope") == "all":
                    query_user_id = None
                elif _query_value(environ, "user_id"):
                    query_user_id = _query_value(environ, "user_id")
                payload = list_recent_execution_failures(
                    self.repository,
                    user_id=query_user_id,
                    limit=_query_value(environ, "limit", "20"),
                    auto_retry_max_attempts=self.auto_retry_max_attempts,
                    auto_retry_base_delay_seconds=self.auto_retry_base_delay_seconds,
                )
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/platform/alerts":
            if method == "GET":
                query_user_id = current_user["id"]
                if _query_value(environ, "scope") == "all":
                    query_user_id = None
                elif _query_value(environ, "user_id"):
                    query_user_id = _query_value(environ, "user_id")
                payload = list_platform_alerts(
                    self.repository,
                    user_id=query_user_id,
                    limit=_query_value(environ, "limit", "50"),
                    stale_after_seconds=self.executor_stale_after_seconds,
                    offline_after_seconds=self.executor_offline_after_seconds,
                    auto_retry_max_attempts=self.auto_retry_max_attempts,
                    auto_retry_base_delay_seconds=self.auto_retry_base_delay_seconds,
                    failure_streak_threshold=self.alert_failure_streak_threshold,
                )
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/platform/signals":
            if method == "GET":
                payload = list_signals(self.repository, source_id=_query_value(environ, "source_id"))
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = create_signal(self.repository, payload=_read_json_body(environ))
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        if path == "/api/platform/raw-items":
            if method == "GET":
                payload = list_raw_items(self.repository, source_id=_query_value(environ, "source_id"))
                return _json_response(start_response, 200, payload)
            if method == "POST":
                payload = create_raw_item(self.repository, payload=_read_json_body(environ))
                return _json_response(start_response, 200, payload)
            return _json_response(start_response, 405, {"error": "method not allowed"})

        signal_dispatch_prefix = "/api/platform/signals/"
        if path.startswith(signal_dispatch_prefix) and path.endswith("/dispatch"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            signal_id = path[len(signal_dispatch_prefix) : -len("/dispatch")]
            payload = dispatch_signal(self.repository, signal_id=signal_id)
            return _json_response(start_response, 200, payload)

        raw_normalize_prefix = "/api/platform/raw-items/"
        if path.startswith(raw_normalize_prefix) and path.endswith("/normalize"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            raw_item_id = path[len(raw_normalize_prefix) : -len("/normalize")]
            payload = normalize_raw_item(self.repository, raw_item_id=raw_item_id)
            return _json_response(start_response, 200, payload)

        retry_job_prefix = "/api/platform/execution-jobs/"
        if path.startswith(retry_job_prefix) and path.endswith("/retry"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            job_id = path[len(retry_job_prefix) : -len("/retry")]
            body = _read_json_body(environ)
            payload = retry_execution_job(
                self.repository,
                job_id=job_id,
                user_id=body.get("user_id") or current_user["id"],
            )
            return _json_response(start_response, 200, payload)

        report_prefix = "/api/executor/jobs/"
        if path.startswith(report_prefix) and path.endswith("/report"):
            if method != "POST":
                return _json_response(start_response, 405, {"error": "method not allowed"})
            job_id = path[len(report_prefix) : -len("/report")]
            payload = report_job(self.repository, job_id=job_id, payload=_read_json_body(environ))
            return _json_response(start_response, 200, payload)

        return _json_response(start_response, 404, {"error": "not found"})

    def _is_executor_authorized(self, environ: Dict[str, Any]) -> bool:
        if not self.executor_api_token:
            return True
        return _parse_authorization(environ) == self.executor_api_token

    def _render_admin_access_denied(self, message: str) -> str:
        template = _load_ui_html_file("admin_access_denied.html")
        return template.replace("__ADMIN_ACCESS_MESSAGE__", str(message or "当前请求不具备后台访问权限。"))

    def _get_current_user(self, environ: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        cookie_value = get_cookie_value(environ, SESSION_COOKIE_NAME)
        if not cookie_value:
            return None
        user_id = parse_session_cookie_value(cookie_value, self.session_secret)
        if not user_id:
            return None
        return self.repository.get_user(user_id)

    def _require_platform_user(self, environ: Dict[str, Any]) -> Dict[str, Any]:
        user = self._get_current_user(environ)
        if not user:
            raise PermissionError("请先登录")
        return user

    def _require_admin_console_user(self, environ: Dict[str, Any]) -> Dict[str, Any]:
        user = self._get_current_user(environ)
        if not user:
            raise PermissionError("请先登录后再进入后台控制台")
        if str(user.get("status") or "").strip().lower() not in {"", "active"}:
            raise PermissionError("当前账号已停用，无法访问后台控制台")
        return user


def build_testing_environ(
    path: str,
    method: str = "GET",
    body: Optional[Dict[str, Any]] = None,
    headers: Optional[Dict[str, str]] = None,
    query: str = "",
) -> Dict[str, Any]:
    payload = json.dumps(body or {}, ensure_ascii=False).encode("utf-8") if body is not None else b""
    environ = {
        "REQUEST_METHOD": method.upper(),
        "PATH_INFO": path,
        "QUERY_STRING": query,
        "CONTENT_LENGTH": str(len(payload)),
        "CONTENT_TYPE": "application/json",
        "wsgi.input": BytesIO(payload),
    }
    for key, value in (headers or {}).items():
        environ[key] = value
    return environ
