"""Telegram runtime settings from env defaults plus DB overrides."""
from __future__ import annotations

from typing import Any, Dict, Optional

from pc28touzhu.config import get_runtime_config


TELEGRAM_RUNTIME_SETTINGS_KEY = "telegram_runtime_settings"
ALLOWED_REPORT_TIMEZONES = {"Asia/Shanghai", "UTC"}


def _runtime_config_or_default(runtime_config: Any = None):
    return runtime_config or get_runtime_config()


def _to_bool(value: Any, field_name: str) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"1", "true", "yes", "on"}:
            return True
        if normalized in {"0", "false", "no", "off"}:
            return False
    raise ValueError("%s 必须为布尔值" % field_name)


def _to_int(value: Any, field_name: str, *, minimum: int, maximum: Optional[int] = None) -> int:
    try:
        normalized = int(value)
    except (TypeError, ValueError):
        raise ValueError("%s 必须为整数" % field_name)
    if normalized < minimum:
        raise ValueError("%s 不能小于 %s" % (field_name, minimum))
    if maximum is not None and normalized > maximum:
        raise ValueError("%s 不能大于 %s" % (field_name, maximum))
    return normalized


def _to_optional_secret(value: Any, current_value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return str(current_value or "")
    return text


def _mask_secret(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 8:
        return text[:2] + "***"
    return text[:4] + "..." + text[-4:]


def build_default_telegram_runtime_settings(runtime_config: Any = None) -> Dict[str, Any]:
    config = _runtime_config_or_default(runtime_config)
    return {
        "alert": {
            "enabled": bool(config.alert_notifier.enabled),
            "bot_token": str(config.alert_notifier.bot_token or ""),
            "target_chat_id": str(config.alert_notifier.target_chat_id or ""),
            "repeat_interval_seconds": int(config.alert_notifier.repeat_interval_seconds or 1800),
            "interval_seconds": int(config.alert_notifier.interval_seconds or 30),
        },
        "bot": {
            "enabled": bool(config.telegram_bot.enabled),
            "bot_token": str(config.telegram_bot.bot_token or ""),
            "poll_interval_seconds": int(config.telegram_bot.poll_interval_seconds or 30),
            "bind_token_ttl_seconds": int(config.telegram_bot.bind_token_ttl_seconds or 600),
        },
        "report": {
            "enabled": bool(config.telegram_report.enabled),
            "target_chat_id": str(config.telegram_report.target_chat_id or ""),
            "interval_seconds": int(config.telegram_report.interval_seconds or 30),
            "send_hour": int(config.telegram_report.send_hour or 9),
            "send_minute": int(config.telegram_report.send_minute or 0),
            "top_n": int(config.telegram_report.top_n or 10),
            "timezone": str(config.telegram_report.timezone or "Asia/Shanghai"),
        },
    }


def _normalize_saved_settings(defaults: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, Any]:
    payload = overrides if isinstance(overrides, dict) else {}
    return {
        "alert": {
            "enabled": bool((payload.get("alert") or {}).get("enabled", defaults["alert"]["enabled"])),
            "bot_token": str((payload.get("alert") or {}).get("bot_token", defaults["alert"]["bot_token"]) or ""),
            "target_chat_id": str((payload.get("alert") or {}).get("target_chat_id", defaults["alert"]["target_chat_id"]) or ""),
            "repeat_interval_seconds": max(
                60,
                int((payload.get("alert") or {}).get("repeat_interval_seconds", defaults["alert"]["repeat_interval_seconds"]) or 60),
            ),
            "interval_seconds": max(
                5,
                int((payload.get("alert") or {}).get("interval_seconds", defaults["alert"]["interval_seconds"]) or 5),
            ),
        },
        "bot": {
            "enabled": bool((payload.get("bot") or {}).get("enabled", defaults["bot"]["enabled"])),
            "bot_token": str((payload.get("bot") or {}).get("bot_token", defaults["bot"]["bot_token"]) or ""),
            "poll_interval_seconds": max(
                1,
                int((payload.get("bot") or {}).get("poll_interval_seconds", defaults["bot"]["poll_interval_seconds"]) or 1),
            ),
            "bind_token_ttl_seconds": max(
                60,
                int((payload.get("bot") or {}).get("bind_token_ttl_seconds", defaults["bot"]["bind_token_ttl_seconds"]) or 60),
            ),
        },
        "report": {
            "enabled": bool((payload.get("report") or {}).get("enabled", defaults["report"]["enabled"])),
            "target_chat_id": str((payload.get("report") or {}).get("target_chat_id", defaults["report"]["target_chat_id"]) or ""),
            "interval_seconds": max(
                5,
                int((payload.get("report") or {}).get("interval_seconds", defaults["report"]["interval_seconds"]) or 5),
            ),
            "send_hour": min(
                23,
                max(0, int((payload.get("report") or {}).get("send_hour", defaults["report"]["send_hour"]) or 0)),
            ),
            "send_minute": min(
                59,
                max(0, int((payload.get("report") or {}).get("send_minute", defaults["report"]["send_minute"]) or 0)),
            ),
            "top_n": max(1, int((payload.get("report") or {}).get("top_n", defaults["report"]["top_n"]) or 1)),
            "timezone": str((payload.get("report") or {}).get("timezone", defaults["report"]["timezone"]) or "Asia/Shanghai"),
        },
    }


def get_effective_telegram_runtime_settings(repository: Any, *, runtime_config: Any = None) -> Dict[str, Any]:
    defaults = build_default_telegram_runtime_settings(runtime_config)
    stored = repository.get_platform_runtime_setting(TELEGRAM_RUNTIME_SETTINGS_KEY)
    item = _normalize_saved_settings(defaults, (stored or {}).get("value") or {})
    return {
        "item": item,
        "updated_at": (stored or {}).get("updated_at"),
        "source": "database" if stored else "env_default",
    }


def get_telegram_runtime_settings_for_admin(repository: Any, *, runtime_config: Any = None) -> Dict[str, Any]:
    resolved = get_effective_telegram_runtime_settings(repository, runtime_config=runtime_config)
    item = resolved["item"]
    return {
        "item": {
            "alert": {
                "enabled": bool(item["alert"]["enabled"]),
                "target_chat_id": str(item["alert"]["target_chat_id"] or ""),
                "repeat_interval_seconds": int(item["alert"]["repeat_interval_seconds"] or 1800),
                "interval_seconds": int(item["alert"]["interval_seconds"] or 30),
                "has_bot_token": bool(str(item["alert"]["bot_token"] or "").strip()),
                "bot_token_masked": _mask_secret(item["alert"]["bot_token"]),
            },
            "bot": {
                "enabled": bool(item["bot"]["enabled"]),
                "poll_interval_seconds": int(item["bot"]["poll_interval_seconds"] or 30),
                "bind_token_ttl_seconds": int(item["bot"]["bind_token_ttl_seconds"] or 600),
                "has_bot_token": bool(str(item["bot"]["bot_token"] or "").strip()),
                "bot_token_masked": _mask_secret(item["bot"]["bot_token"]),
            },
            "report": {
                "enabled": bool(item["report"]["enabled"]),
                "target_chat_id": str(item["report"]["target_chat_id"] or ""),
                "interval_seconds": int(item["report"]["interval_seconds"] or 30),
                "send_hour": int(item["report"]["send_hour"] or 9),
                "send_minute": int(item["report"]["send_minute"] or 0),
                "top_n": int(item["report"]["top_n"] or 10),
                "timezone": str(item["report"]["timezone"] or "Asia/Shanghai"),
            },
            "updated_at": resolved.get("updated_at"),
            "source": resolved.get("source"),
        }
    }


def update_telegram_runtime_settings(
    repository: Any,
    *,
    payload: Dict[str, Any],
    runtime_config: Any = None,
) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        raise ValueError("payload 必须为对象")

    current = get_effective_telegram_runtime_settings(repository, runtime_config=runtime_config)["item"]
    alert_payload = payload.get("alert") or {}
    bot_payload = payload.get("bot") or {}
    report_payload = payload.get("report") or {}
    if not isinstance(alert_payload, dict):
        raise ValueError("alert 必须为对象")
    if not isinstance(bot_payload, dict):
        raise ValueError("bot 必须为对象")
    if not isinstance(report_payload, dict):
        raise ValueError("report 必须为对象")

    timezone_value = str(report_payload.get("timezone") or current["report"]["timezone"]).strip() or "Asia/Shanghai"
    if timezone_value not in ALLOWED_REPORT_TIMEZONES:
        raise ValueError("report.timezone 仅支持 Asia/Shanghai 或 UTC")

    normalized = {
        "alert": {
            "enabled": _to_bool(alert_payload.get("enabled"), "alert.enabled"),
            "bot_token": _to_optional_secret(alert_payload.get("bot_token"), current["alert"]["bot_token"]),
            "target_chat_id": str(alert_payload.get("target_chat_id") or "").strip(),
            "repeat_interval_seconds": _to_int(
                alert_payload.get("repeat_interval_seconds"),
                "alert.repeat_interval_seconds",
                minimum=60,
            ),
            "interval_seconds": _to_int(
                alert_payload.get("interval_seconds"),
                "alert.interval_seconds",
                minimum=5,
            ),
        },
        "bot": {
            "enabled": _to_bool(bot_payload.get("enabled"), "bot.enabled"),
            "bot_token": _to_optional_secret(bot_payload.get("bot_token"), current["bot"]["bot_token"]),
            "poll_interval_seconds": _to_int(
                bot_payload.get("poll_interval_seconds"),
                "bot.poll_interval_seconds",
                minimum=1,
            ),
            "bind_token_ttl_seconds": _to_int(
                bot_payload.get("bind_token_ttl_seconds"),
                "bot.bind_token_ttl_seconds",
                minimum=60,
            ),
        },
        "report": {
            "enabled": _to_bool(report_payload.get("enabled"), "report.enabled"),
            "target_chat_id": str(report_payload.get("target_chat_id") or "").strip(),
            "interval_seconds": _to_int(
                report_payload.get("interval_seconds"),
                "report.interval_seconds",
                minimum=5,
            ),
            "send_hour": _to_int(report_payload.get("send_hour"), "report.send_hour", minimum=0, maximum=23),
            "send_minute": _to_int(report_payload.get("send_minute"), "report.send_minute", minimum=0, maximum=59),
            "top_n": _to_int(report_payload.get("top_n"), "report.top_n", minimum=1, maximum=100),
            "timezone": timezone_value,
        },
    }
    if normalized["alert"]["enabled"] and not normalized["alert"]["bot_token"]:
        raise ValueError("启用告警通知时，alert.bot_token 不能为空")
    if normalized["alert"]["enabled"] and not normalized["alert"]["target_chat_id"]:
        raise ValueError("启用告警通知时，alert.target_chat_id 不能为空")
    if normalized["bot"]["enabled"] and not normalized["bot"]["bot_token"]:
        raise ValueError("启用收益查询 Bot 时，bot.bot_token 不能为空")
    if normalized["report"]["enabled"] and not normalized["bot"]["bot_token"]:
        raise ValueError("启用日报推送时，必须先配置 bot.bot_token")
    if normalized["report"]["enabled"] and not normalized["report"]["target_chat_id"]:
        raise ValueError("启用日报推送时，report.target_chat_id 不能为空")
    repository.upsert_platform_runtime_setting(
        setting_key=TELEGRAM_RUNTIME_SETTINGS_KEY,
        value=normalized,
    )
    return get_telegram_runtime_settings_for_admin(repository, runtime_config=runtime_config)
