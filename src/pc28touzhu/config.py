"""Centralized project configuration."""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = PROJECT_ROOT / ".env"


def _strip_wrapping_quotes(value: str) -> str:
    text = value.strip()
    if len(text) >= 2 and text[0] == text[-1] and text[0] in {"'", '"'}:
        return text[1:-1]
    return text


def load_env_file(env_path: str | os.PathLike[str] | None = None, *, override: bool = False) -> None:
    file_path = Path(env_path) if env_path else DEFAULT_ENV_FILE
    if not file_path.exists() or not file_path.is_file():
        return

    for raw_line in file_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = _strip_wrapping_quotes(raw_value)
        if override or key not in os.environ:
            os.environ[key] = value


def _get_str(key: str, default: str) -> str:
    return str(os.getenv(key, default))


def _get_int(key: str, default: int) -> int:
    value = str(os.getenv(key, str(default))).strip()
    try:
        return int(value)
    except ValueError:
        return default


def _get_float(key: str, default: float) -> float:
    value = str(os.getenv(key, str(default))).strip()
    try:
        return float(value)
    except ValueError:
        return default


def _get_bool(key: str, default: bool) -> bool:
    value = os.getenv(key)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _resolve_project_path(raw_value: str, *, base_dir: Path) -> str:
    text = str(raw_value or "").strip()
    if not text:
        return str(base_dir)
    path = Path(text)
    if path.is_absolute():
        return str(path)
    return str((base_dir / path).resolve())


@dataclass(frozen=True)
class PlatformConfig:
    database_path: str
    executor_api_token: str
    session_secret: str
    host: str
    port: int
    executor_stale_after_seconds: int
    executor_offline_after_seconds: int
    auto_retry_max_attempts: int
    auto_retry_base_delay_seconds: int
    alert_failure_streak_threshold: int


@dataclass(frozen=True)
class ExecutorConfig:
    platform_base_url: str
    executor_api_token: str
    executor_id: str
    pull_limit: int
    once: bool
    telegram_api_id: int
    telegram_api_hash: str
    telegram_phone: str
    telegram_session: str


@dataclass(frozen=True)
class DemoSeedConfig:
    issue_no: str
    bet_type: str
    bet_value: str
    target_key: str
    idempotency_key: str
    message_text: str
    stake_amount: float


@dataclass(frozen=True)
class AlertNotifierConfig:
    enabled: bool
    bot_token: str
    target_chat_id: str
    repeat_interval_seconds: int
    interval_seconds: int
    once: bool


@dataclass(frozen=True)
class TelegramBotConfig:
    enabled: bool
    bot_token: str
    poll_interval_seconds: int
    bind_token_ttl_seconds: int
    once: bool


@dataclass(frozen=True)
class TelegramReportConfig:
    enabled: bool
    target_chat_id: str
    interval_seconds: int
    once: bool
    send_hour: int
    send_minute: int
    top_n: int
    timezone: str


@dataclass(frozen=True)
class PC28AutoSettlementConfig:
    enabled: bool
    interval_seconds: int
    once: bool
    draw_limit: int


@dataclass(frozen=True)
class SourceSyncConfig:
    enabled: bool
    interval_seconds: int
    once: bool


@dataclass(frozen=True)
class AutoTriggerConfig:
    enabled: bool
    interval_seconds: int
    once: bool


@dataclass(frozen=True)
class RuntimeConfig:
    platform: PlatformConfig
    executor: ExecutorConfig
    demo_seed: DemoSeedConfig
    alert_notifier: AlertNotifierConfig
    telegram_bot: TelegramBotConfig
    telegram_report: TelegramReportConfig
    pc28_auto_settlement: PC28AutoSettlementConfig
    source_sync: SourceSyncConfig
    auto_trigger: AutoTriggerConfig


def get_platform_config() -> PlatformConfig:
    base_dir = DEFAULT_ENV_FILE.parent
    executor_stale_after_seconds = max(10, _get_int("EXECUTOR_STALE_AFTER_SECONDS", 60))
    executor_offline_after_seconds = max(
        executor_stale_after_seconds + 1,
        _get_int("EXECUTOR_OFFLINE_AFTER_SECONDS", 300),
    )
    return PlatformConfig(
        database_path=_resolve_project_path(_get_str("DATABASE_PATH", "pc28touzhu.db"), base_dir=base_dir),
        executor_api_token=_get_str("EXECUTOR_API_TOKEN", "change-me"),
        session_secret=_get_str("SESSION_SECRET", "pc28touzhu-dev-secret"),
        host=_get_str("HOST", "0.0.0.0"),
        port=_get_int("PORT", 35100),
        executor_stale_after_seconds=executor_stale_after_seconds,
        executor_offline_after_seconds=executor_offline_after_seconds,
        auto_retry_max_attempts=max(1, _get_int("AUTO_RETRY_MAX_ATTEMPTS", 3)),
        auto_retry_base_delay_seconds=max(5, _get_int("AUTO_RETRY_BASE_DELAY_SECONDS", 30)),
        alert_failure_streak_threshold=max(1, _get_int("ALERT_FAILURE_STREAK_THRESHOLD", 3)),
    )


def get_executor_config() -> ExecutorConfig:
    base_dir = DEFAULT_ENV_FILE.parent
    platform = get_platform_config()
    return ExecutorConfig(
        platform_base_url=_get_str("PLATFORM_BASE_URL", "http://127.0.0.1:%s" % platform.port),
        executor_api_token=_get_str("EXECUTOR_API_TOKEN", "change-me"),
        executor_id=_get_str("EXECUTOR_ID", "executor-001"),
        pull_limit=max(1, _get_int("PULL_LIMIT", 10)),
        once=_get_bool("ONCE", True),
        telegram_api_id=max(0, _get_int("TELEGRAM_API_ID", 0)),
        telegram_api_hash=_get_str("TELEGRAM_API_HASH", ""),
        telegram_phone=_get_str("TELEGRAM_PHONE", ""),
        telegram_session=_resolve_project_path(_get_str("TELEGRAM_SESSION", "telegram-session"), base_dir=base_dir),
    )


def get_demo_seed_config() -> DemoSeedConfig:
    return DemoSeedConfig(
        issue_no=_get_str("ISSUE_NO", "20260407001"),
        bet_type=_get_str("BET_TYPE", "big_small"),
        bet_value=_get_str("BET_VALUE", "大"),
        target_key=_get_str("TARGET_KEY", "-1001234567890"),
        idempotency_key=_get_str("IDEMPOTENCY_KEY", "demo-idemp-001"),
        message_text=_get_str("MESSAGE_TEXT", "大10"),
        stake_amount=max(0.01, _get_float("STAKE_AMOUNT", 10.0)),
    )


def get_alert_notifier_config() -> AlertNotifierConfig:
    return AlertNotifierConfig(
        enabled=_get_bool("ALERT_TELEGRAM_ENABLED", False),
        bot_token=_get_str("ALERT_TELEGRAM_BOT_TOKEN", ""),
        target_chat_id=_get_str("ALERT_TELEGRAM_TARGET_CHAT_ID", ""),
        repeat_interval_seconds=max(60, _get_int("ALERT_NOTIFY_REPEAT_SECONDS", 1800)),
        interval_seconds=max(5, _get_int("ALERT_NOTIFIER_INTERVAL_SECONDS", 30)),
        once=_get_bool("ALERT_NOTIFIER_ONCE", True),
    )


def get_telegram_bot_config() -> TelegramBotConfig:
    return TelegramBotConfig(
        enabled=_get_bool("TG_BOT_ENABLED", False),
        bot_token=_get_str("TG_BOT_TOKEN", ""),
        poll_interval_seconds=max(1, _get_int("TG_BOT_POLL_INTERVAL_SECONDS", 30)),
        bind_token_ttl_seconds=max(60, _get_int("TG_BOT_BIND_TOKEN_TTL_SECONDS", 600)),
        once=_get_bool("TG_BOT_ONCE", True),
    )


def get_telegram_report_config() -> TelegramReportConfig:
    return TelegramReportConfig(
        enabled=_get_bool("TG_REPORT_ENABLED", False),
        target_chat_id=_get_str("TG_REPORT_TARGET_CHAT_ID", ""),
        interval_seconds=max(5, _get_int("TG_REPORT_INTERVAL_SECONDS", 30)),
        once=_get_bool("TG_REPORT_ONCE", True),
        send_hour=min(max(0, _get_int("TG_REPORT_SEND_HOUR", 9)), 23),
        send_minute=min(max(0, _get_int("TG_REPORT_SEND_MINUTE", 0)), 59),
        top_n=max(1, _get_int("TG_REPORT_TOP_N", 10)),
        timezone=_get_str("TG_REPORT_TIMEZONE", "Asia/Shanghai"),
    )


def get_pc28_auto_settlement_config() -> PC28AutoSettlementConfig:
    return PC28AutoSettlementConfig(
        enabled=_get_bool("PC28_AUTO_SETTLEMENT_ENABLED", False),
        interval_seconds=max(5, _get_int("PC28_AUTO_SETTLEMENT_INTERVAL_SECONDS", 30)),
        once=_get_bool("PC28_AUTO_SETTLEMENT_ONCE", True),
        draw_limit=max(10, _get_int("PC28_AUTO_SETTLEMENT_DRAW_LIMIT", 60)),
    )


def get_source_sync_config() -> SourceSyncConfig:
    return SourceSyncConfig(
        enabled=_get_bool("SOURCE_SYNC_ENABLED", True),
        interval_seconds=max(5, _get_int("SOURCE_SYNC_INTERVAL_SECONDS", 30)),
        once=_get_bool("SOURCE_SYNC_ONCE", True),
    )


def get_auto_trigger_config() -> AutoTriggerConfig:
    return AutoTriggerConfig(
        enabled=_get_bool("AUTO_TRIGGER_ENABLED", True),
        interval_seconds=max(5, _get_int("AUTO_TRIGGER_INTERVAL_SECONDS", 30)),
        once=_get_bool("AUTO_TRIGGER_ONCE", True),
    )


def get_runtime_config(env_path: str | os.PathLike[str] | None = None) -> RuntimeConfig:
    load_env_file(env_path)
    if env_path:
        env_base_dir = Path(env_path).resolve().parent
        if "DATABASE_PATH" in os.environ:
            os.environ["DATABASE_PATH"] = _resolve_project_path(os.environ["DATABASE_PATH"], base_dir=env_base_dir)
        if "TELEGRAM_SESSION" in os.environ:
            os.environ["TELEGRAM_SESSION"] = _resolve_project_path(os.environ["TELEGRAM_SESSION"], base_dir=env_base_dir)
    return RuntimeConfig(
        platform=get_platform_config(),
        executor=get_executor_config(),
        demo_seed=get_demo_seed_config(),
        alert_notifier=get_alert_notifier_config(),
        telegram_bot=get_telegram_bot_config(),
        telegram_report=get_telegram_report_config(),
        pc28_auto_settlement=get_pc28_auto_settlement_config(),
        source_sync=get_source_sync_config(),
        auto_trigger=get_auto_trigger_config(),
    )
