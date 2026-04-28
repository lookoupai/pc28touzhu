"""SQLite 仓储：提供平台侧最小 job/heartbeat/attempt 能力。"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from pc28touzhu.domain.pc28_profit_rules import resolve_pc28_hit_profit
from pc28touzhu.domain.settlement_rules import build_settlement_snapshot
from pc28touzhu.domain.subscription_strategy import (
    legacy_profit_rule_args_from_settlement_rule_id,
    present_subscription_item,
    resolve_risk_control_policy,
    resolve_settlement_runtime_policy,
)


SHANGHAI_TZ = timezone(timedelta(hours=8))


def _utc_now_iso() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _parse_iso8601(value: Optional[str]) -> datetime:
    text = str(value or "").strip()
    if not text:
        return datetime.now(timezone.utc).replace(microsecond=0)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _format_iso8601(value: datetime) -> str:
    return value.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _shanghai_date(value: Optional[str]) -> str:
    return _parse_iso8601(value).astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d")


def _safe_json_loads(value: Optional[str]) -> dict:
    if not value:
        return {}
    try:
        payload = json.loads(value)
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _safe_json_loads_list(value: Optional[str]) -> list:
    if not value:
        return []
    try:
        payload = json.loads(value)
    except Exception:
        return []
    return payload if isinstance(payload, list) else []


def _safe_json_dumps(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return "{}"


def _round_money(value: Any, digits: int = 2) -> float:
    try:
        return round(float(value or 0), digits)
    except (TypeError, ValueError):
        return 0.0


def _subscription_risk_control(strategy: Optional[dict]) -> Dict[str, Any]:
    risk_control = resolve_risk_control_policy(strategy)
    return {
        "enabled": bool(risk_control.get("enabled")),
        "profit_target": max(0.0, _round_money(risk_control.get("profit_target"))),
        "loss_limit": max(0.0, _round_money(risk_control.get("loss_limit"))),
    }


def _event_settlement_context(
    *,
    current_event: Optional[Dict[str, Any]],
    signal: Optional[Dict[str, Any]],
    strategy: Optional[dict],
) -> Dict[str, Any]:
    payload = (signal or {}).get("normalized_payload") if isinstance((signal or {}).get("normalized_payload"), dict) else {}
    snapshot = (current_event or {}).get("settlement_snapshot") if isinstance((current_event or {}).get("settlement_snapshot"), dict) else {}
    event_rule_id = str((current_event or {}).get("settlement_rule_id") or "").strip().lower()
    if event_rule_id or snapshot:
        normalized_rule_id = event_rule_id or str(snapshot.get("settlement_rule_id") or "").strip().lower() or None
        fallback_profit_ratio = round(float(snapshot.get("fallback_profit_ratio") or 1.0), 4)
        if not snapshot:
            snapshot = build_settlement_snapshot(
                rule_source="event_snapshot",
                settlement_rule_id=normalized_rule_id,
                fallback_profit_ratio=fallback_profit_ratio,
                resolved_from="event_row",
                signal=signal,
            )
        return {
            "rule_source": str(snapshot.get("rule_source") or ""),
            "settlement_rule_id": normalized_rule_id,
            "fallback_profit_ratio": fallback_profit_ratio,
            "resolved_from": str(snapshot.get("resolved_from") or "event_snapshot"),
            "snapshot": dict(snapshot),
        }
    settlement_policy = resolve_settlement_runtime_policy(strategy, payload)
    snapshot = build_settlement_snapshot(
        rule_source=str(settlement_policy.get("rule_source") or ""),
        settlement_rule_id=settlement_policy.get("settlement_rule_id"),
        fallback_profit_ratio=float(settlement_policy.get("fallback_profit_ratio") or 1.0),
        resolved_from=str(settlement_policy.get("resolved_from") or ""),
        signal=signal,
    )
    return {
        **settlement_policy,
        "snapshot": snapshot,
    }


def _progression_hit_profit_delta(*, settlement_context: Dict[str, Any], signal: Optional[Dict[str, Any]], stake_amount: float) -> float:
    settlement_rule_id = settlement_context.get("settlement_rule_id")
    fallback_profit_ratio = settlement_context.get("fallback_profit_ratio")
    if settlement_rule_id:
        legacy_rule = legacy_profit_rule_args_from_settlement_rule_id(settlement_rule_id)
        if legacy_rule:
            profit_delta = resolve_pc28_hit_profit(
                stake_amount=float(stake_amount or 0),
                bet_type=str((signal or {}).get("bet_type") or ""),
                bet_value=str((signal or {}).get("bet_value") or ""),
                profit_rule_id=legacy_rule.get("profit_rule_id"),
                odds_profile=legacy_rule.get("odds_profile"),
            )
            if profit_delta is not None:
                return _round_money(profit_delta)
    return _round_money(float(stake_amount or 0) * float(fallback_profit_ratio or 1.0))


class DatabaseRepository:
    """最小可运行 SQLite schema + 执行器接口仓储。"""

    SCHEMA_SQL = [
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT,
            password_hash TEXT NOT NULL DEFAULT '',
            role TEXT NOT NULL DEFAULT 'user',
            status TEXT NOT NULL DEFAULT 'inactive',
            telegram_user_id INTEGER,
            telegram_chat_id TEXT,
            telegram_username TEXT,
            telegram_bound_at TEXT,
            telegram_bind_token TEXT,
            telegram_bind_token_expire_at TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS signal_sources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_user_id INTEGER,
            source_type TEXT NOT NULL,
            name TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            visibility TEXT NOT NULL DEFAULT 'private',
            config_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(owner_user_id) REFERENCES users(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS source_raw_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER NOT NULL,
            external_item_id TEXT,
            issue_no TEXT,
            published_at TEXT,
            raw_payload TEXT NOT NULL DEFAULT '{}',
            parse_status TEXT NOT NULL DEFAULT 'pending',
            parse_error TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(source_id) REFERENCES signal_sources(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS normalized_signals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_id INTEGER NOT NULL,
            source_raw_item_id INTEGER,
            lottery_type TEXT NOT NULL,
            issue_no TEXT NOT NULL,
            bet_type TEXT NOT NULL,
            bet_value TEXT NOT NULL,
            confidence REAL,
            normalized_payload TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'ready',
            published_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(source_id) REFERENCES signal_sources(id),
            FOREIGN KEY(source_raw_item_id) REFERENCES source_raw_items(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS delivery_targets (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            telegram_account_id INTEGER,
            executor_type TEXT NOT NULL,
            target_key TEXT NOT NULL,
            target_name TEXT NOT NULL DEFAULT '',
            template_id INTEGER,
            status TEXT NOT NULL DEFAULT 'active',
            last_test_status TEXT NOT NULL DEFAULT '',
            last_test_error_code TEXT NOT NULL DEFAULT '',
            last_test_message TEXT NOT NULL DEFAULT '',
            last_tested_at TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(telegram_account_id) REFERENCES telegram_accounts(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS telegram_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            phone TEXT,
            session_path TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            meta_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS user_subscriptions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            strategy_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(source_id) REFERENCES signal_sources(id),
            UNIQUE(user_id, source_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscription_progression_state (
            subscription_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            current_step INTEGER NOT NULL DEFAULT 1,
            last_signal_id INTEGER,
            last_issue_no TEXT NOT NULL DEFAULT '',
            last_result_type TEXT NOT NULL DEFAULT '',
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(subscription_id) REFERENCES user_subscriptions(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscription_financial_state (
            subscription_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            realized_profit REAL NOT NULL DEFAULT 0,
            realized_loss REAL NOT NULL DEFAULT 0,
            net_profit REAL NOT NULL DEFAULT 0,
            threshold_status TEXT NOT NULL DEFAULT '',
            stopped_reason TEXT NOT NULL DEFAULT '',
            baseline_reset_at TEXT,
            baseline_reset_note TEXT NOT NULL DEFAULT '',
            last_settled_event_id INTEGER,
            last_settled_at TEXT,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(subscription_id) REFERENCES user_subscriptions(id),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscription_progression_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscription_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            signal_id INTEGER NOT NULL,
            issue_no TEXT NOT NULL DEFAULT '',
            progression_step INTEGER NOT NULL DEFAULT 1,
            stake_amount REAL NOT NULL DEFAULT 0,
            base_stake REAL NOT NULL DEFAULT 0,
            multiplier REAL NOT NULL DEFAULT 2,
            max_steps INTEGER NOT NULL DEFAULT 1,
            refund_action TEXT NOT NULL DEFAULT 'hold',
            cap_action TEXT NOT NULL DEFAULT 'reset',
            status TEXT NOT NULL DEFAULT 'pending',
            resolved_result_type TEXT NOT NULL DEFAULT '',
            settlement_rule_id TEXT NOT NULL DEFAULT '',
            profit_delta REAL NOT NULL DEFAULT 0,
            loss_delta REAL NOT NULL DEFAULT 0,
            net_delta REAL NOT NULL DEFAULT 0,
            settlement_snapshot_json TEXT NOT NULL DEFAULT '{}',
            result_context_json TEXT NOT NULL DEFAULT '{}',
            auto_trigger_rule_id INTEGER,
            auto_trigger_rule_run_id INTEGER,
            auto_trigger_stat_date TEXT NOT NULL DEFAULT '',
            settled_at TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(subscription_id) REFERENCES user_subscriptions(id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(signal_id) REFERENCES normalized_signals(id),
            UNIQUE(subscription_id, signal_id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS message_templates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            lottery_type TEXT NOT NULL,
            bet_type TEXT NOT NULL,
            template_text TEXT NOT NULL,
            config_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS execution_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            signal_id INTEGER NOT NULL,
            subscription_id INTEGER,
            progression_event_id INTEGER,
            delivery_target_id INTEGER NOT NULL,
            telegram_account_id INTEGER,
            executor_type TEXT NOT NULL,
            idempotency_key TEXT NOT NULL,
            planned_message_text TEXT NOT NULL DEFAULT '',
            stake_plan_json TEXT NOT NULL DEFAULT '{}',
            execute_after TEXT NOT NULL,
            expire_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            error_message TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(signal_id) REFERENCES normalized_signals(id),
            FOREIGN KEY(subscription_id) REFERENCES user_subscriptions(id),
            FOREIGN KEY(progression_event_id) REFERENCES subscription_progression_events(id),
            FOREIGN KEY(delivery_target_id) REFERENCES delivery_targets(id),
            FOREIGN KEY(telegram_account_id) REFERENCES telegram_accounts(id),
            UNIQUE(user_id, idempotency_key)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS execution_attempts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            executor_instance_id TEXT NOT NULL,
            attempt_no INTEGER NOT NULL,
            delivery_status TEXT NOT NULL,
            remote_message_id TEXT,
            raw_result TEXT NOT NULL DEFAULT '{}',
            error_message TEXT,
            executed_at TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(job_id) REFERENCES execution_jobs(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS executor_instances (
            executor_id TEXT PRIMARY KEY,
            version TEXT NOT NULL DEFAULT '',
            capabilities TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'online',
            last_seen_at TEXT NOT NULL,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS platform_alert_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_key TEXT UNIQUE NOT NULL,
            alert_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            metadata_json TEXT NOT NULL DEFAULT '{}',
            status TEXT NOT NULL DEFAULT 'active',
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            resolved_at TEXT,
            last_sent_at TEXT,
            send_count INTEGER NOT NULL DEFAULT 0,
            occurrence_count INTEGER NOT NULL DEFAULT 1,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscription_daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stat_date TEXT NOT NULL,
            user_id INTEGER NOT NULL,
            subscription_id INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            profit_amount REAL NOT NULL DEFAULT 0,
            loss_amount REAL NOT NULL DEFAULT 0,
            net_profit REAL NOT NULL DEFAULT 0,
            settled_event_count INTEGER NOT NULL DEFAULT 0,
            hit_count INTEGER NOT NULL DEFAULT 0,
            miss_count INTEGER NOT NULL DEFAULT 0,
            refund_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(subscription_id) REFERENCES user_subscriptions(id),
            FOREIGN KEY(source_id) REFERENCES signal_sources(id),
            UNIQUE(subscription_id, stat_date)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscription_runtime_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            subscription_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            status TEXT NOT NULL DEFAULT 'active',
            started_signal_id INTEGER,
            started_issue_no TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL,
            start_reason TEXT NOT NULL DEFAULT '',
            ended_at TEXT,
            end_reason TEXT NOT NULL DEFAULT '',
            last_issue_no TEXT NOT NULL DEFAULT '',
            last_result_type TEXT NOT NULL DEFAULT '',
            realized_profit REAL NOT NULL DEFAULT 0,
            realized_loss REAL NOT NULL DEFAULT 0,
            net_profit REAL NOT NULL DEFAULT 0,
            settled_event_count INTEGER NOT NULL DEFAULT 0,
            hit_count INTEGER NOT NULL DEFAULT 0,
            miss_count INTEGER NOT NULL DEFAULT 0,
            refund_count INTEGER NOT NULL DEFAULT 0,
            baseline_reset_at TEXT,
            baseline_reset_note TEXT NOT NULL DEFAULT '',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(subscription_id) REFERENCES user_subscriptions(id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(started_signal_id) REFERENCES normalized_signals(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS telegram_daily_report_records (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            report_key TEXT UNIQUE NOT NULL,
            stat_date TEXT NOT NULL,
            target_chat_id TEXT NOT NULL,
            report_type TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'sent',
            send_count INTEGER NOT NULL DEFAULT 0,
            last_sent_at TEXT,
            last_error TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS telegram_bot_runtime_state (
            bot_name TEXT PRIMARY KEY,
            last_update_id INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS platform_runtime_settings (
            setting_key TEXT PRIMARY KEY,
            value_json TEXT NOT NULL DEFAULT '{}',
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now'))
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS auto_trigger_rules (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active',
            scope_mode TEXT NOT NULL DEFAULT 'selected_subscriptions',
            subscription_ids_json TEXT NOT NULL DEFAULT '[]',
            condition_mode TEXT NOT NULL DEFAULT 'any',
            conditions_json TEXT NOT NULL DEFAULT '[]',
            guard_groups_json TEXT NOT NULL DEFAULT '[]',
            action_json TEXT NOT NULL DEFAULT '{}',
            daily_risk_control_json TEXT NOT NULL DEFAULT '{}',
            cooldown_issues INTEGER NOT NULL DEFAULT 10,
            last_triggered_issue_no TEXT NOT NULL DEFAULT '',
            last_triggered_at TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS auto_trigger_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            subscription_id INTEGER,
            source_id INTEGER,
            predictor_id INTEGER,
            latest_issue_no TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'triggered',
            reason TEXT NOT NULL DEFAULT '',
            matched_conditions_json TEXT NOT NULL DEFAULT '[]',
            snapshot_json TEXT NOT NULL DEFAULT '{}',
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(rule_id) REFERENCES auto_trigger_rules(id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(subscription_id) REFERENCES user_subscriptions(id),
            FOREIGN KEY(source_id) REFERENCES signal_sources(id)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS auto_trigger_rule_daily_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            stat_date TEXT NOT NULL,
            profit_amount REAL NOT NULL DEFAULT 0,
            loss_amount REAL NOT NULL DEFAULT 0,
            net_profit REAL NOT NULL DEFAULT 0,
            settled_event_count INTEGER NOT NULL DEFAULT 0,
            hit_count INTEGER NOT NULL DEFAULT 0,
            miss_count INTEGER NOT NULL DEFAULT 0,
            refund_count INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL DEFAULT 'active',
            stopped_reason TEXT NOT NULL DEFAULT '',
            stopped_at TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(rule_id) REFERENCES auto_trigger_rules(id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            UNIQUE(rule_id, stat_date)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS auto_trigger_rule_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            subscription_id INTEGER NOT NULL,
            stat_date TEXT NOT NULL,
            started_issue_no TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'active',
            stop_reason TEXT NOT NULL DEFAULT '',
            started_at TEXT NOT NULL,
            stopped_at TEXT,
            created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            FOREIGN KEY(rule_id) REFERENCES auto_trigger_rules(id),
            FOREIGN KEY(user_id) REFERENCES users(id),
            FOREIGN KEY(subscription_id) REFERENCES user_subscriptions(id),
            UNIQUE(rule_id, subscription_id, stat_date)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_execution_jobs_status_time ON execution_jobs(status, execute_after, expire_at)",
        "CREATE INDEX IF NOT EXISTS idx_execution_attempts_job ON execution_attempts(job_id, attempt_no)",
        "CREATE INDEX IF NOT EXISTS idx_platform_alert_records_status_seen ON platform_alert_records(status, last_seen_at)",
        "CREATE INDEX IF NOT EXISTS idx_signal_sources_owner ON signal_sources(owner_user_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_telegram_accounts_user ON telegram_accounts(user_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_delivery_targets_user ON delivery_targets(user_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_user_subscriptions_user ON user_subscriptions(user_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_subscription_financial_state_user ON subscription_financial_state(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_subscription_daily_stats_date_user ON subscription_daily_stats(stat_date, user_id)",
        "CREATE INDEX IF NOT EXISTS idx_subscription_daily_stats_date_net ON subscription_daily_stats(stat_date, net_profit)",
        "CREATE INDEX IF NOT EXISTS idx_subscription_runtime_runs_subscription ON subscription_runtime_runs(subscription_id, user_id, started_at DESC)",
        "CREATE INDEX IF NOT EXISTS idx_auto_trigger_rules_user_status ON auto_trigger_rules(user_id, status)",
        "CREATE INDEX IF NOT EXISTS idx_auto_trigger_events_user_time ON auto_trigger_events(user_id, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_auto_trigger_rule_daily_stats_user_date ON auto_trigger_rule_daily_stats(user_id, stat_date, status)",
        "CREATE INDEX IF NOT EXISTS idx_auto_trigger_rule_runs_subscription ON auto_trigger_rule_runs(subscription_id, user_id, id)",
    ]

    def __init__(self, db_path: str = "pc28touzhu.db"):
        self.db_path = db_path

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def initialize_database(self) -> None:
        with self._connect() as conn:
            cursor = conn.cursor()
            for ddl in self.SCHEMA_SQL:
                cursor.execute(ddl)
            self._ensure_user_telegram_columns(conn)
            self._ensure_delivery_target_columns(conn)
            self._ensure_message_template_columns(conn)
            self._ensure_execution_job_columns(conn)
            self._ensure_progression_event_columns(conn)
            self._ensure_auto_trigger_rule_columns(conn)
            self._ensure_user_telegram_indexes(conn)
            conn.commit()

    def _ensure_user_telegram_columns(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(users)").fetchall()
        existing_columns = {str(row["name"]) for row in rows}
        column_definitions = {
            "telegram_user_id": "ALTER TABLE users ADD COLUMN telegram_user_id INTEGER",
            "telegram_chat_id": "ALTER TABLE users ADD COLUMN telegram_chat_id TEXT",
            "telegram_username": "ALTER TABLE users ADD COLUMN telegram_username TEXT",
            "telegram_bound_at": "ALTER TABLE users ADD COLUMN telegram_bound_at TEXT",
            "telegram_bind_token": "ALTER TABLE users ADD COLUMN telegram_bind_token TEXT",
            "telegram_bind_token_expire_at": "ALTER TABLE users ADD COLUMN telegram_bind_token_expire_at TEXT",
        }
        for column_name, ddl in column_definitions.items():
            if column_name not in existing_columns:
                conn.execute(ddl)

    def _ensure_user_telegram_indexes(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_users_telegram_user_id_unique
            ON users(telegram_user_id)
            WHERE telegram_user_id IS NOT NULL
            """
        )
        conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_users_telegram_bind_token_unique
            ON users(telegram_bind_token)
            WHERE telegram_bind_token IS NOT NULL AND telegram_bind_token <> ''
            """
        )

    def _ensure_delivery_target_columns(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(delivery_targets)").fetchall()
        existing_columns = {str(row["name"]) for row in rows}
        column_definitions = {
            "last_test_status": "ALTER TABLE delivery_targets ADD COLUMN last_test_status TEXT NOT NULL DEFAULT ''",
            "last_test_error_code": "ALTER TABLE delivery_targets ADD COLUMN last_test_error_code TEXT NOT NULL DEFAULT ''",
            "last_test_message": "ALTER TABLE delivery_targets ADD COLUMN last_test_message TEXT NOT NULL DEFAULT ''",
            "last_tested_at": "ALTER TABLE delivery_targets ADD COLUMN last_tested_at TEXT",
        }
        for column_name, ddl in column_definitions.items():
            if column_name not in existing_columns:
                conn.execute(ddl)

    def _ensure_message_template_columns(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(message_templates)").fetchall()
        existing_columns = {str(row["name"]) for row in rows}
        column_definitions = {
            "name": "ALTER TABLE message_templates ADD COLUMN name TEXT NOT NULL DEFAULT ''",
            "config_json": "ALTER TABLE message_templates ADD COLUMN config_json TEXT NOT NULL DEFAULT '{}'",
        }
        for column_name, ddl in column_definitions.items():
            if column_name not in existing_columns:
                conn.execute(ddl)

    def _ensure_execution_job_columns(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(execution_jobs)").fetchall()
        existing_columns = {str(row["name"]) for row in rows}
        column_definitions = {
            "subscription_id": "ALTER TABLE execution_jobs ADD COLUMN subscription_id INTEGER",
            "progression_event_id": "ALTER TABLE execution_jobs ADD COLUMN progression_event_id INTEGER",
        }
        for column_name, ddl in column_definitions.items():
            if column_name not in existing_columns:
                conn.execute(ddl)

    def _ensure_progression_event_columns(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(subscription_progression_events)").fetchall()
        existing_columns = {str(row["name"]) for row in rows}
        column_definitions = {
            "settlement_rule_id": "ALTER TABLE subscription_progression_events ADD COLUMN settlement_rule_id TEXT NOT NULL DEFAULT ''",
            "profit_delta": "ALTER TABLE subscription_progression_events ADD COLUMN profit_delta REAL NOT NULL DEFAULT 0",
            "loss_delta": "ALTER TABLE subscription_progression_events ADD COLUMN loss_delta REAL NOT NULL DEFAULT 0",
            "net_delta": "ALTER TABLE subscription_progression_events ADD COLUMN net_delta REAL NOT NULL DEFAULT 0",
            "settlement_snapshot_json": "ALTER TABLE subscription_progression_events ADD COLUMN settlement_snapshot_json TEXT NOT NULL DEFAULT '{}'",
            "result_context_json": "ALTER TABLE subscription_progression_events ADD COLUMN result_context_json TEXT NOT NULL DEFAULT '{}'",
            "auto_trigger_rule_id": "ALTER TABLE subscription_progression_events ADD COLUMN auto_trigger_rule_id INTEGER",
            "auto_trigger_rule_run_id": "ALTER TABLE subscription_progression_events ADD COLUMN auto_trigger_rule_run_id INTEGER",
            "auto_trigger_stat_date": "ALTER TABLE subscription_progression_events ADD COLUMN auto_trigger_stat_date TEXT NOT NULL DEFAULT ''",
        }
        for column_name, ddl in column_definitions.items():
            if column_name not in existing_columns:
                conn.execute(ddl)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_progression_events_auto_trigger_rule
            ON subscription_progression_events(auto_trigger_rule_id, user_id, auto_trigger_stat_date)
            """
        )

    def _ensure_auto_trigger_rule_columns(self, conn: sqlite3.Connection) -> None:
        rows = conn.execute("PRAGMA table_info(auto_trigger_rules)").fetchall()
        existing_columns = {str(row["name"]) for row in rows}
        column_definitions = {
            "guard_groups_json": "ALTER TABLE auto_trigger_rules ADD COLUMN guard_groups_json TEXT NOT NULL DEFAULT '[]'",
            "daily_risk_control_json": "ALTER TABLE auto_trigger_rules ADD COLUMN daily_risk_control_json TEXT NOT NULL DEFAULT '{}'",
        }
        for column_name, ddl in column_definitions.items():
            if column_name not in existing_columns:
                conn.execute(ddl)

    def _fetch_one(self, query: str, params: tuple) -> Optional[Dict[str, Any]]:
        with self._connect() as conn:
            row = conn.execute(query, params).fetchone()
        return dict(row) if row else None

    def _fetch_all(self, query: str, params: tuple = ()) -> list[Dict[str, Any]]:
        with self._connect() as conn:
            rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]

    def _serialize_source_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "owner_user_id": int(row["owner_user_id"]) if row["owner_user_id"] is not None else None,
            "source_type": str(row["source_type"]),
            "name": str(row["name"]),
            "status": str(row["status"]),
            "visibility": str(row["visibility"]),
            "config": _safe_json_loads(row.get("config_json")),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_user_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "username": str(row["username"]),
            "email": str(row["email"]) if row["email"] is not None else "",
            "role": str(row["role"]),
            "status": str(row["status"]),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_user_row_with_secret(self, row: Dict[str, Any]) -> Dict[str, Any]:
        data = self._serialize_user_row(row)
        data["password_hash"] = str(row.get("password_hash") or "")
        return data

    def _serialize_user_telegram_binding(self, row: Dict[str, Any]) -> Dict[str, Any]:
        bind_token = str(row.get("telegram_bind_token") or "").strip()
        return {
            "user_id": int(row["id"]),
            "is_bound": row.get("telegram_user_id") is not None and str(row.get("telegram_chat_id") or "").strip() != "",
            "telegram_user_id": int(row["telegram_user_id"]) if row.get("telegram_user_id") is not None else None,
            "telegram_chat_id": str(row.get("telegram_chat_id") or ""),
            "telegram_username": str(row.get("telegram_username") or ""),
            "telegram_bound_at": row.get("telegram_bound_at"),
            "bind_token": bind_token,
            "bind_token_expire_at": row.get("telegram_bind_token_expire_at"),
            "has_active_bind_token": bool(bind_token),
        }

    def _serialize_target_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "telegram_account_id": int(row["telegram_account_id"]) if row["telegram_account_id"] is not None else None,
            "executor_type": str(row["executor_type"]),
            "target_key": str(row["target_key"]),
            "target_name": str(row.get("target_name") or ""),
            "template_id": int(row["template_id"]) if row["template_id"] is not None else None,
            "status": str(row["status"]),
            "last_test_status": str(row.get("last_test_status") or ""),
            "last_test_error_code": str(row.get("last_test_error_code") or ""),
            "last_test_message": str(row.get("last_test_message") or ""),
            "last_tested_at": row.get("last_tested_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_telegram_account_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "label": str(row["label"]),
            "phone": str(row["phone"]) if row["phone"] is not None else "",
            "session_path": str(row["session_path"]),
            "status": str(row["status"]),
            "meta": _safe_json_loads(row.get("meta_json")),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_subscription_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        data = present_subscription_item(
            {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "source_id": int(row["source_id"]),
            "status": str(row["status"]),
            "strategy": _safe_json_loads(row.get("strategy_json")),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            }
        )
        if row.get("progression_current_step") is not None or row.get("pending_event_id") is not None:
            data["progression"] = {
                "current_step": int(row["progression_current_step"]) if row.get("progression_current_step") is not None else 1,
                "last_signal_id": int(row["progression_last_signal_id"]) if row.get("progression_last_signal_id") is not None else None,
                "last_issue_no": str(row.get("progression_last_issue_no") or ""),
                "last_result_type": str(row.get("progression_last_result_type") or ""),
                "pending_event_id": int(row["pending_event_id"]) if row.get("pending_event_id") is not None else None,
                "pending_issue_no": str(row.get("pending_issue_no") or ""),
                "pending_status": str(row.get("pending_status") or ""),
            }
        if "financial_realized_profit" in row:
            data["financial"] = {
                "subscription_id": int(row["id"]),
                "user_id": int(row["financial_user_id"]) if row.get("financial_user_id") is not None else None,
                "realized_profit": _round_money(row.get("financial_realized_profit")),
                "realized_loss": _round_money(row.get("financial_realized_loss")),
                "net_profit": _round_money(row.get("financial_net_profit")),
                "threshold_status": str(row.get("financial_threshold_status") or ""),
                "stopped_reason": str(row.get("financial_stopped_reason") or ""),
                "baseline_reset_at": row.get("financial_baseline_reset_at"),
                "baseline_reset_note": str(row.get("financial_baseline_reset_note") or ""),
                "last_settled_event_id": (
                    int(row["financial_last_settled_event_id"])
                    if row.get("financial_last_settled_event_id") is not None
                    else None
                ),
                "last_settled_at": row.get("financial_last_settled_at"),
                "updated_at": row.get("financial_updated_at"),
            }
        return data

    def _default_subscription_financial_state(self, subscription_id: int) -> Dict[str, Any]:
        return {
            "subscription_id": int(subscription_id),
            "user_id": None,
            "realized_profit": 0.0,
            "realized_loss": 0.0,
            "net_profit": 0.0,
            "threshold_status": "",
            "stopped_reason": "",
            "baseline_reset_at": None,
            "baseline_reset_note": "",
            "last_settled_event_id": None,
            "last_settled_at": None,
            "updated_at": None,
        }

    def _serialize_subscription_financial_state_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "subscription_id": int(row["subscription_id"]),
            "user_id": int(row["user_id"]) if row.get("user_id") is not None else None,
            "realized_profit": _round_money(row.get("realized_profit")),
            "realized_loss": _round_money(row.get("realized_loss")),
            "net_profit": _round_money(row.get("net_profit")),
            "threshold_status": str(row.get("threshold_status") or ""),
            "stopped_reason": str(row.get("stopped_reason") or ""),
            "baseline_reset_at": row.get("baseline_reset_at"),
            "baseline_reset_note": str(row.get("baseline_reset_note") or ""),
            "last_settled_event_id": int(row["last_settled_event_id"]) if row.get("last_settled_event_id") is not None else None,
            "last_settled_at": row.get("last_settled_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_message_template_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "name": str(row.get("name") or ""),
            "lottery_type": str(row["lottery_type"]),
            "bet_type": str(row["bet_type"]),
            "template_text": str(row.get("template_text") or ""),
            "config": _safe_json_loads(row.get("config_json")),
            "status": str(row.get("status") or "active"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_progression_event_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "subscription_id": int(row["subscription_id"]),
            "user_id": int(row["user_id"]),
            "signal_id": int(row["signal_id"]),
            "issue_no": str(row.get("issue_no") or ""),
            "progression_step": int(row.get("progression_step") or 1),
            "stake_amount": float(row.get("stake_amount") or 0),
            "base_stake": float(row.get("base_stake") or 0),
            "multiplier": float(row.get("multiplier") or 2),
            "max_steps": int(row.get("max_steps") or 1),
            "refund_action": str(row.get("refund_action") or "hold"),
            "cap_action": str(row.get("cap_action") or "reset"),
            "status": str(row.get("status") or "pending"),
            "resolved_result_type": str(row.get("resolved_result_type") or ""),
            "settlement_rule_id": str(row.get("settlement_rule_id") or ""),
            "profit_delta": _round_money(row.get("profit_delta")),
            "loss_delta": _round_money(row.get("loss_delta")),
            "net_delta": _round_money(row.get("net_delta")),
            "settlement_snapshot": _safe_json_loads(row.get("settlement_snapshot_json")),
            "result_context": _safe_json_loads(row.get("result_context_json")),
            "auto_trigger_rule_id": int(row["auto_trigger_rule_id"]) if row.get("auto_trigger_rule_id") is not None else None,
            "auto_trigger_rule_run_id": int(row["auto_trigger_rule_run_id"]) if row.get("auto_trigger_rule_run_id") is not None else None,
            "auto_trigger_stat_date": str(row.get("auto_trigger_stat_date") or ""),
            "settled_at": row.get("settled_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_subscription_runtime_run_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "subscription_id": int(row["subscription_id"]),
            "user_id": int(row["user_id"]),
            "status": str(row.get("status") or "active"),
            "started_signal_id": int(row["started_signal_id"]) if row.get("started_signal_id") is not None else None,
            "started_issue_no": str(row.get("started_issue_no") or ""),
            "started_at": row.get("started_at"),
            "start_reason": str(row.get("start_reason") or ""),
            "ended_at": row.get("ended_at"),
            "end_reason": str(row.get("end_reason") or ""),
            "last_issue_no": str(row.get("last_issue_no") or ""),
            "last_result_type": str(row.get("last_result_type") or ""),
            "realized_profit": _round_money(row.get("realized_profit")),
            "realized_loss": _round_money(row.get("realized_loss")),
            "net_profit": _round_money(row.get("net_profit")),
            "settled_event_count": int(row.get("settled_event_count") or 0),
            "hit_count": int(row.get("hit_count") or 0),
            "miss_count": int(row.get("miss_count") or 0),
            "refund_count": int(row.get("refund_count") or 0),
            "baseline_reset_at": row.get("baseline_reset_at"),
            "baseline_reset_note": str(row.get("baseline_reset_note") or ""),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_auto_trigger_rule_daily_stat_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "rule_id": int(row["rule_id"]),
            "user_id": int(row["user_id"]),
            "stat_date": str(row.get("stat_date") or ""),
            "profit_amount": _round_money(row.get("profit_amount")),
            "loss_amount": _round_money(row.get("loss_amount")),
            "net_profit": _round_money(row.get("net_profit")),
            "settled_event_count": int(row.get("settled_event_count") or 0),
            "hit_count": int(row.get("hit_count") or 0),
            "miss_count": int(row.get("miss_count") or 0),
            "refund_count": int(row.get("refund_count") or 0),
            "status": str(row.get("status") or "active"),
            "stopped_reason": str(row.get("stopped_reason") or ""),
            "stopped_at": row.get("stopped_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_auto_trigger_rule_run_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "rule_id": int(row["rule_id"]),
            "user_id": int(row["user_id"]),
            "subscription_id": int(row["subscription_id"]),
            "stat_date": str(row.get("stat_date") or ""),
            "started_issue_no": str(row.get("started_issue_no") or ""),
            "status": str(row.get("status") or "active"),
            "stop_reason": str(row.get("stop_reason") or ""),
            "started_at": row.get("started_at"),
            "stopped_at": row.get("stopped_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_auto_trigger_rule_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "name": str(row.get("name") or ""),
            "status": str(row.get("status") or "active"),
            "scope_mode": str(row.get("scope_mode") or "selected_subscriptions"),
            "subscription_ids": [
                int(item) for item in _safe_json_loads_list(row.get("subscription_ids_json"))
                if str(item).strip().isdigit()
            ],
            "condition_mode": str(row.get("condition_mode") or "any"),
            "conditions": _safe_json_loads_list(row.get("conditions_json")),
            "guard_groups": _safe_json_loads_list(row.get("guard_groups_json")),
            "action": _safe_json_loads(row.get("action_json")),
            "daily_risk_control": _safe_json_loads(row.get("daily_risk_control_json")),
            "cooldown_issues": int(row.get("cooldown_issues") or 0),
            "last_triggered_issue_no": str(row.get("last_triggered_issue_no") or ""),
            "last_triggered_at": row.get("last_triggered_at"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_auto_trigger_event_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "rule_id": int(row["rule_id"]),
            "user_id": int(row["user_id"]),
            "subscription_id": int(row["subscription_id"]) if row.get("subscription_id") is not None else None,
            "source_id": int(row["source_id"]) if row.get("source_id") is not None else None,
            "predictor_id": int(row["predictor_id"]) if row.get("predictor_id") is not None else None,
            "latest_issue_no": str(row.get("latest_issue_no") or ""),
            "status": str(row.get("status") or ""),
            "reason": str(row.get("reason") or ""),
            "matched_conditions": _safe_json_loads_list(row.get("matched_conditions_json")),
            "snapshot": _safe_json_loads(row.get("snapshot_json")),
            "created_at": row.get("created_at"),
            "rule_name": str(row.get("rule_name") or "") if "rule_name" in row else "",
            "source_name": str(row.get("source_name") or "") if "source_name" in row else "",
        }

    def _serialize_signal_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "source_id": int(row["source_id"]),
            "source_raw_item_id": int(row["source_raw_item_id"]) if row["source_raw_item_id"] is not None else None,
            "lottery_type": str(row["lottery_type"]),
            "issue_no": str(row["issue_no"]),
            "bet_type": str(row["bet_type"]),
            "bet_value": str(row["bet_value"]),
            "confidence": float(row["confidence"]) if row["confidence"] is not None else None,
            "normalized_payload": _safe_json_loads(row.get("normalized_payload")),
            "status": str(row["status"]),
            "published_at": row.get("published_at"),
            "created_at": row.get("created_at"),
        }

    def _serialize_raw_item_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "source_id": int(row["source_id"]),
            "external_item_id": str(row["external_item_id"]) if row["external_item_id"] is not None else None,
            "issue_no": str(row["issue_no"]) if row["issue_no"] is not None else "",
            "published_at": row.get("published_at"),
            "raw_payload": _safe_json_loads(row.get("raw_payload")),
            "parse_status": str(row["parse_status"]),
            "parse_error": row.get("parse_error"),
            "created_at": row.get("created_at"),
        }

    def _serialize_execution_job_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "user_id": int(row["user_id"]),
            "signal_id": int(row["signal_id"]),
            "subscription_id": int(row["subscription_id"]) if row.get("subscription_id") is not None else None,
            "progression_event_id": int(row["progression_event_id"]) if row.get("progression_event_id") is not None else None,
            "delivery_target_id": int(row["delivery_target_id"]),
            "telegram_account_id": int(row["telegram_account_id"]) if row["telegram_account_id"] is not None else None,
            "executor_type": str(row["executor_type"]),
            "idempotency_key": str(row["idempotency_key"]),
            "planned_message_text": str(row.get("planned_message_text") or ""),
            "stake_plan": _safe_json_loads(row.get("stake_plan_json")),
            "execute_after": row.get("execute_after"),
            "expire_at": row.get("expire_at"),
            "status": str(row["status"]),
            "error_message": row.get("error_message"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "lottery_type": str(row["lottery_type"]) if row.get("lottery_type") is not None else "",
            "issue_no": str(row["issue_no"]) if row.get("issue_no") is not None else "",
            "bet_type": str(row["bet_type"]) if row.get("bet_type") is not None else "",
            "bet_value": str(row["bet_value"]) if row.get("bet_value") is not None else "",
            "target_key": str(row["target_key"]) if row.get("target_key") is not None else "",
            "target_name": str(row["target_name"]) if row.get("target_name") is not None else "",
            "telegram_account_label": (
                str(row["telegram_account_label"]) if row.get("telegram_account_label") is not None else ""
            ),
            "attempt_count": int(row["attempt_count"]) if row.get("attempt_count") is not None else 0,
            "last_attempt_no": int(row["last_attempt_no"]) if row.get("last_attempt_no") is not None else None,
            "last_delivery_status": (
                str(row["last_delivery_status"]) if row.get("last_delivery_status") is not None else None
            ),
            "last_remote_message_id": (
                str(row["last_remote_message_id"]) if row.get("last_remote_message_id") is not None else None
            ),
            "last_error_message": row.get("last_error_message"),
            "last_executed_at": row.get("last_executed_at"),
            "last_executor_instance_id": row.get("last_executor_instance_id"),
        }

    def _serialize_executor_instance_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "executor_id": str(row["executor_id"]),
            "version": str(row.get("version") or ""),
            "capabilities": _safe_json_loads(row.get("capabilities")),
            "status": str(row.get("status") or "online"),
            "last_seen_at": row.get("last_seen_at"),
            "updated_at": row.get("updated_at"),
            "total_attempt_count": int(row["total_attempt_count"]) if row.get("total_attempt_count") is not None else 0,
            "delivered_attempt_count": (
                int(row["delivered_attempt_count"]) if row.get("delivered_attempt_count") is not None else 0
            ),
            "failed_attempt_count": (
                int(row["failed_attempt_count"]) if row.get("failed_attempt_count") is not None else 0
            ),
            "last_executed_at": row.get("last_executed_at"),
            "last_failure_at": row.get("last_failure_at"),
            "last_failure_status": str(row["last_failure_status"]) if row.get("last_failure_status") is not None else None,
            "last_failure_error_message": row.get("last_failure_error_message"),
        }

    def _serialize_execution_failure_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "job_id": int(row["job_id"]),
            "user_id": int(row["user_id"]),
            "signal_id": int(row["signal_id"]),
            "delivery_target_id": int(row["delivery_target_id"]),
            "telegram_account_id": int(row["telegram_account_id"]) if row["telegram_account_id"] is not None else None,
            "job_status": str(row["job_status"]),
            "planned_message_text": str(row.get("planned_message_text") or ""),
            "lottery_type": str(row.get("lottery_type") or ""),
            "issue_no": str(row.get("issue_no") or ""),
            "bet_type": str(row.get("bet_type") or ""),
            "bet_value": str(row.get("bet_value") or ""),
            "target_key": str(row.get("target_key") or ""),
            "target_name": str(row.get("target_name") or ""),
            "telegram_account_label": str(row.get("telegram_account_label") or ""),
            "executor_instance_id": str(row.get("executor_instance_id") or ""),
            "attempt_no": int(row["attempt_no"]) if row.get("attempt_no") is not None else 0,
            "delivery_status": str(row.get("delivery_status") or ""),
            "remote_message_id": str(row["remote_message_id"]) if row.get("remote_message_id") is not None else None,
            "error_message": row.get("error_message"),
            "executed_at": row.get("executed_at"),
            "attempt_count": int(row["attempt_count"]) if row.get("attempt_count") is not None else 0,
            "raw_result": _safe_json_loads(row.get("raw_result")),
        }

    def _serialize_execution_attempt_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "job_id": int(row["job_id"]),
            "executor_instance_id": str(row["executor_instance_id"]),
            "attempt_no": int(row["attempt_no"]),
            "delivery_status": str(row["delivery_status"]),
            "remote_message_id": str(row["remote_message_id"]) if row.get("remote_message_id") is not None else None,
            "raw_result": _safe_json_loads(row.get("raw_result")),
            "error_message": row.get("error_message"),
            "executed_at": row.get("executed_at"),
            "created_at": row.get("created_at"),
        }

    def _serialize_platform_alert_record_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "alert_key": str(row["alert_key"]),
            "alert_type": str(row["alert_type"]),
            "severity": str(row["severity"]),
            "title": str(row["title"]),
            "message": str(row["message"]),
            "metadata": _safe_json_loads(row.get("metadata_json")),
            "status": str(row["status"]),
            "first_seen_at": row.get("first_seen_at"),
            "last_seen_at": row.get("last_seen_at"),
            "resolved_at": row.get("resolved_at"),
            "last_sent_at": row.get("last_sent_at"),
            "send_count": int(row["send_count"]) if row.get("send_count") is not None else 0,
            "occurrence_count": int(row["occurrence_count"]) if row.get("occurrence_count") is not None else 0,
            "last_error": row.get("last_error"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_subscription_daily_stat_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]) if row.get("id") is not None else None,
            "stat_date": str(row.get("stat_date") or ""),
            "user_id": int(row["user_id"]),
            "subscription_id": int(row["subscription_id"]),
            "source_id": int(row["source_id"]),
            "source_name": str(row.get("source_name") or ""),
            "profit_amount": _round_money(row.get("profit_amount")),
            "loss_amount": _round_money(row.get("loss_amount")),
            "net_profit": _round_money(row.get("net_profit")),
            "settled_event_count": int(row.get("settled_event_count") or 0),
            "hit_count": int(row.get("hit_count") or 0),
            "miss_count": int(row.get("miss_count") or 0),
            "refund_count": int(row.get("refund_count") or 0),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_telegram_daily_report_record_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": int(row["id"]),
            "report_key": str(row.get("report_key") or ""),
            "stat_date": str(row.get("stat_date") or ""),
            "target_chat_id": str(row.get("target_chat_id") or ""),
            "report_type": str(row.get("report_type") or ""),
            "status": str(row.get("status") or ""),
            "send_count": int(row.get("send_count") or 0),
            "last_sent_at": row.get("last_sent_at"),
            "last_error": row.get("last_error"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
        }

    def _serialize_platform_runtime_setting_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "setting_key": str(row.get("setting_key") or ""),
            "value": _safe_json_loads(row.get("value_json")),
            "updated_at": row.get("updated_at"),
        }

    # ====== Seed helpers (tests / scripts) ======

    def create_user(self, username: str, *, email: str = "", password_hash: str = "") -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO users(username, email, password_hash) VALUES (?, ?, ?)",
                (username, email, password_hash),
            )
            return int(cur.lastrowid)

    def create_user_record(
        self,
        *,
        username: str,
        email: str = "",
        password_hash: str = "",
        role: str = "user",
        status: str = "active",
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO users(username, email, password_hash, role, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (username, email or None, password_hash, role, status, now, now),
            )
            user_id = int(cur.lastrowid)
        return self.get_user(user_id) or {}

    def get_user(self, user_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM users WHERE id = ?", (int(user_id),))
        return self._serialize_user_row(row) if row else None

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM users WHERE username = ? LIMIT 1", (str(username),))
        return self._serialize_user_row_with_secret(row) if row else None

    def update_user_password(self, user_id: int, password_hash: str, *, email: str | None = None) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            if email is None:
                conn.execute(
                    "UPDATE users SET password_hash = ?, updated_at = ? WHERE id = ?",
                    (password_hash, now, int(user_id)),
                )
            else:
                conn.execute(
                    "UPDATE users SET password_hash = ?, email = ?, updated_at = ? WHERE id = ?",
                    (password_hash, email or None, now, int(user_id)),
                )
        return self.get_user(user_id)

    def list_users(self) -> list[Dict[str, Any]]:
        rows = self._fetch_all("SELECT * FROM users ORDER BY id ASC")
        return [self._serialize_user_row(row) for row in rows]

    def get_user_telegram_binding(self, user_id: int) -> Dict[str, Any]:
        row = self._fetch_one("SELECT * FROM users WHERE id = ? LIMIT 1", (int(user_id),))
        if not row:
            raise ValueError("user_id 对应的用户不存在")
        return self._serialize_user_telegram_binding(row)

    def get_user_by_telegram_user_id(self, telegram_user_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM users WHERE telegram_user_id = ? LIMIT 1", (int(telegram_user_id),))
        return self._serialize_user_row(row) if row else None

    def get_user_by_telegram_bind_token(self, bind_token: str) -> Optional[Dict[str, Any]]:
        normalized_token = str(bind_token or "").strip()
        if not normalized_token:
            return None
        row = self._fetch_one("SELECT * FROM users WHERE telegram_bind_token = ? LIMIT 1", (normalized_token,))
        return self._serialize_user_row(row) if row else None

    def set_user_telegram_bind_token(self, *, user_id: int, bind_token: str, expire_at: str) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE users
                SET telegram_bind_token = ?, telegram_bind_token_expire_at = ?, updated_at = ?
                WHERE id = ?
                """,
                (str(bind_token or "").strip(), expire_at, now, int(user_id)),
            )
            if cursor.rowcount <= 0:
                raise ValueError("user_id 对应的用户不存在")
        return self.get_user_telegram_binding(int(user_id))

    def clear_user_telegram_bind_token(self, *, user_id: int) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE users
                SET telegram_bind_token = NULL, telegram_bind_token_expire_at = NULL, updated_at = ?
                WHERE id = ?
                """,
                (now, int(user_id)),
            )
            if cursor.rowcount <= 0:
                raise ValueError("user_id 对应的用户不存在")
        return self.get_user_telegram_binding(int(user_id))

    def update_user_telegram_binding(
        self,
        *,
        user_id: int,
        telegram_user_id: int,
        telegram_chat_id: str,
        telegram_username: str = "",
        telegram_bound_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        normalized_bound_at = telegram_bound_at or _utc_now_iso()
        now = _utc_now_iso()
        with self._connect() as conn:
            current = conn.execute("SELECT id FROM users WHERE id = ? LIMIT 1", (int(user_id),)).fetchone()
            if not current:
                raise ValueError("user_id 对应的用户不存在")
            conflict = conn.execute(
                "SELECT id FROM users WHERE telegram_user_id = ? AND id <> ? LIMIT 1",
                (int(telegram_user_id), int(user_id)),
            ).fetchone()
            if conflict:
                raise ValueError("该 Telegram 账号已绑定其他平台用户")
            conn.execute(
                """
                UPDATE users
                SET telegram_user_id = ?,
                    telegram_chat_id = ?,
                    telegram_username = ?,
                    telegram_bound_at = ?,
                    telegram_bind_token = NULL,
                    telegram_bind_token_expire_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    int(telegram_user_id),
                    str(telegram_chat_id or "").strip(),
                    str(telegram_username or "").strip(),
                    normalized_bound_at,
                    now,
                    int(user_id),
                ),
            )
        return self.get_user_telegram_binding(int(user_id))

    def clear_user_telegram_binding(self, *, user_id: int) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE users
                SET telegram_user_id = NULL,
                    telegram_chat_id = NULL,
                    telegram_username = NULL,
                    telegram_bound_at = NULL,
                    telegram_bind_token = NULL,
                    telegram_bind_token_expire_at = NULL,
                    updated_at = ?
                WHERE id = ?
                """,
                (now, int(user_id)),
            )
            if cursor.rowcount <= 0:
                raise ValueError("user_id 对应的用户不存在")
        return self.get_user_telegram_binding(int(user_id))

    def create_source(self, source_type: str, name: str, *, owner_user_id: Optional[int] = None) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO signal_sources(owner_user_id, source_type, name) VALUES (?, ?, ?)",
                (owner_user_id, source_type, name),
            )
            return int(cur.lastrowid)

    def create_telegram_account_record(
        self,
        *,
        user_id: int,
        label: str,
        session_path: str,
        phone: str = "",
        status: str = "active",
        meta: Optional[dict] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO telegram_accounts(
                    user_id, label, phone, session_path, status, meta_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), label, phone or None, session_path, status, _safe_json_dumps(meta or {}), now, now),
            )
            account_id = int(cur.lastrowid)
        return self.get_telegram_account(account_id) or {}

    def get_telegram_account(self, telegram_account_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM telegram_accounts WHERE id = ?", (int(telegram_account_id),))
        return self._serialize_telegram_account_row(row) if row else None

    def telegram_account_belongs_to_user(self, telegram_account_id: int, user_id: int) -> bool:
        row = self._fetch_one(
            "SELECT id FROM telegram_accounts WHERE id = ? AND user_id = ? LIMIT 1",
            (int(telegram_account_id), int(user_id)),
        )
        return row is not None

    def list_telegram_accounts(self, user_id: int) -> list[Dict[str, Any]]:
        rows = self._fetch_all(
            "SELECT * FROM telegram_accounts WHERE user_id = ? ORDER BY id ASC",
            (int(user_id),),
        )
        return [self._serialize_telegram_account_row(row) for row in rows]

    def update_telegram_account_status(self, *, telegram_account_id: int, user_id: int, status: str) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE telegram_accounts
                SET status = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (status, now, int(telegram_account_id), int(user_id)),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_telegram_account(telegram_account_id)

    def update_telegram_account_record(
        self,
        *,
        telegram_account_id: int,
        user_id: int,
        label: str,
        session_path: str,
        phone: str = "",
        meta: Optional[dict] = None,
    ) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE telegram_accounts
                SET label = ?, phone = ?, session_path = ?, meta_json = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (
                    label,
                    phone or None,
                    session_path,
                    _safe_json_dumps(meta or {}),
                    now,
                    int(telegram_account_id),
                    int(user_id),
                ),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_telegram_account(telegram_account_id)

    def count_delivery_targets_by_telegram_account(self, telegram_account_id: int, *, user_id: Optional[int] = None) -> int:
        if user_id is None:
            row = self._fetch_one(
                "SELECT COUNT(1) AS total FROM delivery_targets WHERE telegram_account_id = ?",
                (int(telegram_account_id),),
            )
        else:
            row = self._fetch_one(
                """
                SELECT COUNT(1) AS total
                FROM delivery_targets
                WHERE telegram_account_id = ? AND user_id = ?
                """,
                (int(telegram_account_id), int(user_id)),
            )
        return int((row or {}).get("total") or 0)

    def count_execution_jobs_by_telegram_account(self, telegram_account_id: int, *, user_id: Optional[int] = None) -> int:
        if user_id is None:
            row = self._fetch_one(
                "SELECT COUNT(1) AS total FROM execution_jobs WHERE telegram_account_id = ?",
                (int(telegram_account_id),),
            )
        else:
            row = self._fetch_one(
                """
                SELECT COUNT(1) AS total
                FROM execution_jobs
                WHERE telegram_account_id = ? AND user_id = ?
                """,
                (int(telegram_account_id), int(user_id)),
            )
        return int((row or {}).get("total") or 0)

    def delete_telegram_account_record(self, *, telegram_account_id: int, user_id: int) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM telegram_accounts WHERE id = ? AND user_id = ?",
                (int(telegram_account_id), int(user_id)),
            )
            return cursor.rowcount > 0

    def create_raw_item_record(
        self,
        *,
        source_id: int,
        external_item_id: Optional[str] = None,
        issue_no: str = "",
        published_at: Optional[str] = None,
        raw_payload: Optional[dict] = None,
        parse_status: str = "pending",
        parse_error: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO source_raw_items(
                    source_id, external_item_id, issue_no, published_at, raw_payload, parse_status, parse_error, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(source_id),
                    external_item_id,
                    issue_no or None,
                    published_at or now,
                    _safe_json_dumps(raw_payload or {}),
                    parse_status,
                    parse_error,
                    now,
                ),
            )
            raw_item_id = int(cur.lastrowid)
        return self.get_raw_item(raw_item_id) or {}

    def get_raw_item(self, raw_item_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM source_raw_items WHERE id = ?", (int(raw_item_id),))
        return self._serialize_raw_item_row(row) if row else None

    def raw_item_belongs_to_user(self, raw_item_id: int, user_id: int) -> bool:
        row = self._fetch_one(
            """
            SELECT r.id
            FROM source_raw_items r
            JOIN signal_sources s ON s.id = r.source_id
            WHERE r.id = ? AND s.owner_user_id = ?
            LIMIT 1
            """,
            (int(raw_item_id), int(user_id)),
        )
        return row is not None

    def list_raw_items(self, source_id: Optional[int] = None) -> list[Dict[str, Any]]:
        if source_id is None:
            rows = self._fetch_all("SELECT * FROM source_raw_items ORDER BY id DESC")
        else:
            rows = self._fetch_all(
                "SELECT * FROM source_raw_items WHERE source_id = ? ORDER BY id DESC",
                (int(source_id),),
            )
        return [self._serialize_raw_item_row(row) for row in rows]

    def update_raw_item_parse_result(
        self,
        raw_item_id: int,
        *,
        parse_status: str,
        parse_error: Optional[str] = None,
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE source_raw_items
                SET parse_status = ?, parse_error = ?
                WHERE id = ?
                """,
                (parse_status, parse_error, int(raw_item_id)),
            )
        return self.get_raw_item(raw_item_id) or {}

    def create_source_record(
        self,
        *,
        owner_user_id: Optional[int],
        source_type: str,
        name: str,
        status: str = "active",
        visibility: str = "private",
        config: Optional[dict] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO signal_sources(
                    owner_user_id, source_type, name, status, visibility, config_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    owner_user_id,
                    source_type,
                    name,
                    status,
                    visibility,
                    _safe_json_dumps(config or {}),
                    now,
                    now,
                ),
            )
            source_id = int(cur.lastrowid)
        return self.get_source(source_id) or {}

    def get_source(self, source_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM signal_sources WHERE id = ?", (int(source_id),))
        return self._serialize_source_row(row) if row else None

    def source_belongs_to_user(self, source_id: int, user_id: int) -> bool:
        row = self._fetch_one(
            "SELECT id FROM signal_sources WHERE id = ? AND owner_user_id = ? LIMIT 1",
            (int(source_id), int(user_id)),
        )
        return row is not None

    def list_sources(self, owner_user_id: Optional[int] = None) -> list[Dict[str, Any]]:
        if owner_user_id is None:
            rows = self._fetch_all("SELECT * FROM signal_sources ORDER BY id ASC")
        else:
            rows = self._fetch_all(
                "SELECT * FROM signal_sources WHERE owner_user_id = ? ORDER BY id ASC",
                (int(owner_user_id),),
            )
        return [self._serialize_source_row(row) for row in rows]

    def update_source_record(
        self,
        *,
        source_id: int,
        owner_user_id: int,
        name: str,
        visibility: str,
        status: str,
        config: Optional[dict] = None,
    ) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE signal_sources
                SET name = ?, visibility = ?, status = ?, config_json = ?, updated_at = ?
                WHERE id = ? AND owner_user_id = ?
                """,
                (
                    name,
                    visibility,
                    status,
                    _safe_json_dumps(config or {}),
                    now,
                    int(source_id),
                    int(owner_user_id),
                ),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_source(source_id)

    def update_source_status(self, *, source_id: int, owner_user_id: int, status: str) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE signal_sources
                SET status = ?, updated_at = ?
                WHERE id = ? AND owner_user_id = ?
                """,
                (
                    status,
                    now,
                    int(source_id),
                    int(owner_user_id),
                ),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_source(source_id)

    def count_raw_items_by_source(self, source_id: int) -> int:
        row = self._fetch_one(
            "SELECT COUNT(1) AS total FROM source_raw_items WHERE source_id = ?",
            (int(source_id),),
        )
        return int((row or {}).get("total") or 0)

    def count_signals_by_source(self, source_id: int) -> int:
        row = self._fetch_one(
            "SELECT COUNT(1) AS total FROM normalized_signals WHERE source_id = ?",
            (int(source_id),),
        )
        return int((row or {}).get("total") or 0)

    def count_subscriptions_by_source(self, source_id: int, *, user_id: Optional[int] = None) -> int:
        if user_id is None:
            row = self._fetch_one(
                "SELECT COUNT(1) AS total FROM user_subscriptions WHERE source_id = ?",
                (int(source_id),),
            )
        else:
            row = self._fetch_one(
                """
                SELECT COUNT(1) AS total
                FROM user_subscriptions
                WHERE source_id = ? AND user_id = ?
                """,
                (int(source_id), int(user_id)),
            )
        return int((row or {}).get("total") or 0)

    def delete_source_record(self, *, source_id: int, owner_user_id: int) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM signal_sources WHERE id = ? AND owner_user_id = ?",
                (int(source_id), int(owner_user_id)),
            )
            return cursor.rowcount > 0

    def create_signal(
        self,
        *,
        source_id: int,
        lottery_type: str,
        issue_no: str,
        bet_type: str,
        bet_value: str,
        source_raw_item_id: Optional[int] = None,
        confidence: Optional[float] = None,
        normalized_payload: Optional[dict] = None,
        published_at: Optional[str] = None,
        status: str = "ready",
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO normalized_signals(
                    source_id, source_raw_item_id, lottery_type, issue_no, bet_type, bet_value,
                    confidence, normalized_payload, status, published_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(source_id),
                    int(source_raw_item_id) if source_raw_item_id is not None else None,
                    lottery_type,
                    issue_no,
                    bet_type,
                    bet_value,
                    confidence,
                    _safe_json_dumps(normalized_payload or {}),
                    status,
                    published_at or _utc_now_iso(),
                ),
            )
            return int(cur.lastrowid)

    def create_signal_record(
        self,
        *,
        source_id: int,
        lottery_type: str,
        issue_no: str,
        bet_type: str,
        bet_value: str,
        source_raw_item_id: Optional[int] = None,
        confidence: Optional[float] = None,
        normalized_payload: Optional[dict] = None,
        published_at: Optional[str] = None,
        status: str = "ready",
    ) -> Dict[str, Any]:
        signal_id = self.create_signal(
            source_id=source_id,
            lottery_type=lottery_type,
            issue_no=issue_no,
            bet_type=bet_type,
            bet_value=bet_value,
            source_raw_item_id=source_raw_item_id,
            confidence=confidence,
            normalized_payload=normalized_payload,
            published_at=published_at,
            status=status,
        )
        return self.get_signal(signal_id) or {}

    def get_signal(self, signal_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM normalized_signals WHERE id = ?", (int(signal_id),))
        return self._serialize_signal_row(row) if row else None

    def signal_belongs_to_user(self, signal_id: int, user_id: int) -> bool:
        row = self._fetch_one(
            """
            SELECT n.id
            FROM normalized_signals n
            JOIN signal_sources s ON s.id = n.source_id
            WHERE n.id = ? AND s.owner_user_id = ?
            LIMIT 1
            """,
            (int(signal_id), int(user_id)),
        )
        return row is not None

    def list_signals(self, source_id: Optional[int] = None) -> list[Dict[str, Any]]:
        if source_id is None:
            rows = self._fetch_all("SELECT * FROM normalized_signals ORDER BY id DESC")
        else:
            rows = self._fetch_all(
                "SELECT * FROM normalized_signals WHERE source_id = ? ORDER BY id DESC",
                (int(source_id),),
            )
        return [self._serialize_signal_row(row) for row in rows]

    def create_delivery_target(
        self,
        *,
        user_id: int,
        telegram_account_id: Optional[int] = None,
        executor_type: str,
        target_key: str,
        target_name: str = "",
        template_id: Optional[int] = None,
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO delivery_targets(user_id, telegram_account_id, executor_type, target_key, target_name, template_id, status)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), telegram_account_id, executor_type, target_key, target_name, template_id, "inactive"),
            )
            return int(cur.lastrowid)

    def create_delivery_target_record(
        self,
        *,
        user_id: int,
        telegram_account_id: Optional[int] = None,
        executor_type: str,
        target_key: str,
        target_name: str = "",
        template_id: Optional[int] = None,
        status: str = "inactive",
        last_test_status: str = "",
        last_test_error_code: str = "",
        last_test_message: str = "",
        last_tested_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO delivery_targets(
                    user_id, telegram_account_id, executor_type, target_key, target_name, template_id,
                    status, last_test_status, last_test_error_code, last_test_message, last_tested_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id),
                    telegram_account_id,
                    executor_type,
                    target_key,
                    target_name,
                    template_id,
                    status,
                    last_test_status,
                    last_test_error_code,
                    last_test_message,
                    last_tested_at,
                    now,
                    now,
                ),
            )
            target_id = int(cur.lastrowid)
        return self.get_delivery_target(target_id) or {}

    def get_delivery_target(self, delivery_target_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM delivery_targets WHERE id = ?", (int(delivery_target_id),))
        return self._serialize_target_row(row) if row else None

    def list_delivery_targets(self, user_id: int) -> list[Dict[str, Any]]:
        rows = self._fetch_all(
            "SELECT * FROM delivery_targets WHERE user_id = ? ORDER BY id ASC",
            (int(user_id),),
        )
        return [self._serialize_target_row(row) for row in rows]

    def update_delivery_target_status(self, *, delivery_target_id: int, user_id: int, status: str) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE delivery_targets
                SET status = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (status, now, int(delivery_target_id), int(user_id)),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_delivery_target(delivery_target_id)

    def update_delivery_target_test_result(
        self,
        *,
        delivery_target_id: int,
        user_id: int,
        last_test_status: str,
        last_test_error_code: str = "",
        last_test_message: str = "",
        last_tested_at: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        tested_at = last_tested_at or now
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE delivery_targets
                SET last_test_status = ?, last_test_error_code = ?, last_test_message = ?, last_tested_at = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (
                    last_test_status,
                    last_test_error_code,
                    last_test_message,
                    tested_at,
                    now,
                    int(delivery_target_id),
                    int(user_id),
                ),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_delivery_target(delivery_target_id)

    def update_delivery_target_record(
        self,
        *,
        delivery_target_id: int,
        user_id: int,
        telegram_account_id: Optional[int],
        executor_type: str,
        target_key: str,
        target_name: str = "",
        template_id: Optional[int] = None,
    ) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE delivery_targets
                SET telegram_account_id = ?, executor_type = ?, target_key = ?, target_name = ?, template_id = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (
                    telegram_account_id,
                    executor_type,
                    target_key,
                    target_name,
                    template_id,
                    now,
                    int(delivery_target_id),
                    int(user_id),
                ),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_delivery_target(delivery_target_id)

    def count_execution_jobs_by_delivery_target(self, delivery_target_id: int, *, user_id: Optional[int] = None) -> int:
        if user_id is None:
            row = self._fetch_one(
                "SELECT COUNT(1) AS total FROM execution_jobs WHERE delivery_target_id = ?",
                (int(delivery_target_id),),
            )
        else:
            row = self._fetch_one(
                """
                SELECT COUNT(1) AS total
                FROM execution_jobs
                WHERE delivery_target_id = ? AND user_id = ?
                """,
                (int(delivery_target_id), int(user_id)),
            )
        return int((row or {}).get("total") or 0)

    def delete_delivery_target_record(self, *, delivery_target_id: int, user_id: int) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM delivery_targets WHERE id = ? AND user_id = ?",
                (int(delivery_target_id), int(user_id)),
            )
            return cursor.rowcount > 0

    def create_subscription_record(
        self,
        *,
        user_id: int,
        source_id: int,
        strategy: Optional[dict] = None,
        status: str = "active",
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO user_subscriptions(
                    user_id, source_id, status, strategy_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (int(user_id), int(source_id), status, _safe_json_dumps(strategy or {}), now, now),
            )
            subscription_id = int(cur.lastrowid)
        return self.get_subscription(subscription_id) or {}

    def create_message_template_record(
        self,
        *,
        user_id: int,
        name: str,
        lottery_type: str,
        bet_type: str = "*",
        template_text: str,
        config: Optional[dict] = None,
        status: str = "active",
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO message_templates(
                    user_id, name, lottery_type, bet_type, template_text, config_json, status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id),
                    name,
                    lottery_type,
                    bet_type,
                    template_text,
                    _safe_json_dumps(config or {}),
                    status,
                    now,
                    now,
                ),
            )
            template_id = int(cur.lastrowid)
        return self.get_message_template(template_id) or {}

    def get_message_template(self, template_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM message_templates WHERE id = ?", (int(template_id),))
        return self._serialize_message_template_row(row) if row else None

    def list_message_templates(self, user_id: int) -> list[Dict[str, Any]]:
        rows = self._fetch_all(
            "SELECT * FROM message_templates WHERE user_id = ? ORDER BY id ASC",
            (int(user_id),),
        )
        return [self._serialize_message_template_row(row) for row in rows]

    def update_message_template_record(
        self,
        *,
        template_id: int,
        user_id: int,
        name: str,
        lottery_type: str,
        bet_type: str,
        template_text: str,
        config: Optional[dict] = None,
    ) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE message_templates
                SET name = ?, lottery_type = ?, bet_type = ?, template_text = ?, config_json = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (
                    name,
                    lottery_type,
                    bet_type,
                    template_text,
                    _safe_json_dumps(config or {}),
                    now,
                    int(template_id),
                    int(user_id),
                ),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_message_template(template_id)

    def update_message_template_status(self, *, template_id: int, user_id: int, status: str) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE message_templates
                SET status = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (status, now, int(template_id), int(user_id)),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_message_template(template_id)

    def get_subscription(self, subscription_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one(
            """
            SELECT
                us.*,
                sps.current_step AS progression_current_step,
                sps.last_signal_id AS progression_last_signal_id,
                sps.last_issue_no AS progression_last_issue_no,
                sps.last_result_type AS progression_last_result_type,
                sfs.user_id AS financial_user_id,
                sfs.realized_profit AS financial_realized_profit,
                sfs.realized_loss AS financial_realized_loss,
                sfs.net_profit AS financial_net_profit,
                sfs.threshold_status AS financial_threshold_status,
                sfs.stopped_reason AS financial_stopped_reason,
                sfs.baseline_reset_at AS financial_baseline_reset_at,
                sfs.baseline_reset_note AS financial_baseline_reset_note,
                sfs.last_settled_event_id AS financial_last_settled_event_id,
                sfs.last_settled_at AS financial_last_settled_at,
                sfs.updated_at AS financial_updated_at,
                pending.id AS pending_event_id,
                pending.issue_no AS pending_issue_no,
                pending.status AS pending_status
            FROM user_subscriptions us
            LEFT JOIN subscription_progression_state sps ON sps.subscription_id = us.id
            LEFT JOIN subscription_financial_state sfs ON sfs.subscription_id = us.id
            LEFT JOIN subscription_progression_events pending ON pending.id = (
                SELECT spe.id
                FROM subscription_progression_events spe
                WHERE spe.subscription_id = us.id
                  AND spe.status IN ('pending', 'placed')
                ORDER BY spe.id DESC
                LIMIT 1
            )
            WHERE us.id = ?
            LIMIT 1
            """,
            (int(subscription_id),),
        )
        return self._serialize_subscription_row(row) if row else None

    def list_subscriptions(self, user_id: int) -> list[Dict[str, Any]]:
        rows = self._fetch_all(
            """
            SELECT
                us.*,
                sps.current_step AS progression_current_step,
                sps.last_signal_id AS progression_last_signal_id,
                sps.last_issue_no AS progression_last_issue_no,
                sps.last_result_type AS progression_last_result_type,
                sfs.user_id AS financial_user_id,
                sfs.realized_profit AS financial_realized_profit,
                sfs.realized_loss AS financial_realized_loss,
                sfs.net_profit AS financial_net_profit,
                sfs.threshold_status AS financial_threshold_status,
                sfs.stopped_reason AS financial_stopped_reason,
                sfs.baseline_reset_at AS financial_baseline_reset_at,
                sfs.baseline_reset_note AS financial_baseline_reset_note,
                sfs.last_settled_event_id AS financial_last_settled_event_id,
                sfs.last_settled_at AS financial_last_settled_at,
                sfs.updated_at AS financial_updated_at,
                pending.id AS pending_event_id,
                pending.issue_no AS pending_issue_no,
                pending.status AS pending_status
            FROM user_subscriptions us
            LEFT JOIN subscription_progression_state sps ON sps.subscription_id = us.id
            LEFT JOIN subscription_financial_state sfs ON sfs.subscription_id = us.id
            LEFT JOIN subscription_progression_events pending ON pending.id = (
                SELECT spe.id
                FROM subscription_progression_events spe
                WHERE spe.subscription_id = us.id
                  AND spe.status IN ('pending', 'placed')
                ORDER BY spe.id DESC
                LIMIT 1
            )
            WHERE us.user_id = ?
            ORDER BY us.id ASC
            """,
            (int(user_id),),
        )
        return [self._serialize_subscription_row(row) for row in rows]

    def list_auto_trigger_candidate_subscriptions(
        self,
        *,
        user_id: int,
        subscription_ids: Optional[list[int]] = None,
    ) -> list[Dict[str, Any]]:
        params: list[Any] = [int(user_id)]
        subscription_filter = ""
        normalized_ids = [int(item) for item in (subscription_ids or []) if int(item) > 0]
        if normalized_ids:
            subscription_filter = " AND us.id IN (%s)" % ",".join(["?"] * len(normalized_ids))
            params.extend(normalized_ids)
        rows = self._fetch_all(
            """
            SELECT
                us.*,
                ss.name AS source_name,
                ss.status AS source_status,
                ss.source_type AS source_type,
                ss.config_json AS source_config_json,
                sps.current_step AS progression_current_step,
                sps.last_signal_id AS progression_last_signal_id,
                sps.last_issue_no AS progression_last_issue_no,
                sps.last_result_type AS progression_last_result_type,
                sfs.user_id AS financial_user_id,
                sfs.realized_profit AS financial_realized_profit,
                sfs.realized_loss AS financial_realized_loss,
                sfs.net_profit AS financial_net_profit,
                sfs.threshold_status AS financial_threshold_status,
                sfs.stopped_reason AS financial_stopped_reason,
                sfs.baseline_reset_at AS financial_baseline_reset_at,
                sfs.baseline_reset_note AS financial_baseline_reset_note,
                sfs.last_settled_event_id AS financial_last_settled_event_id,
                sfs.last_settled_at AS financial_last_settled_at,
                sfs.updated_at AS financial_updated_at,
                pending.id AS pending_event_id,
                pending.issue_no AS pending_issue_no,
                pending.status AS pending_status
            FROM user_subscriptions us
            JOIN signal_sources ss ON ss.id = us.source_id
            LEFT JOIN subscription_progression_state sps ON sps.subscription_id = us.id
            LEFT JOIN subscription_financial_state sfs ON sfs.subscription_id = us.id
            LEFT JOIN subscription_progression_events pending ON pending.id = (
                SELECT spe.id
                FROM subscription_progression_events spe
                WHERE spe.subscription_id = us.id
                  AND spe.status IN ('pending', 'placed')
                ORDER BY spe.id DESC
                LIMIT 1
            )
            WHERE us.user_id = ?
            """ + subscription_filter + """
            ORDER BY us.id ASC
            """,
            tuple(params),
        )
        items = []
        for row in rows:
            item = self._serialize_subscription_row(row)
            item["source"] = {
                "id": int(item["source_id"]),
                "name": str(row.get("source_name") or ""),
                "status": str(row.get("source_status") or ""),
                "source_type": str(row.get("source_type") or ""),
                "config": _safe_json_loads(row.get("source_config_json")),
            }
            items.append(item)
        return items

    def subscription_has_open_run(self, *, subscription_id: int, user_id: int) -> Dict[str, Any]:
        self.expire_due_jobs()
        row = self._fetch_one(
            """
            SELECT
                (SELECT COUNT(1)
                 FROM subscription_progression_events
                 WHERE subscription_id = ? AND user_id = ? AND status IN ('pending', 'placed')) AS open_event_count,
                (SELECT COUNT(1)
                 FROM execution_jobs
                 WHERE subscription_id = ? AND user_id = ? AND status = 'pending') AS pending_job_count
            """,
            (int(subscription_id), int(user_id), int(subscription_id), int(user_id)),
        ) or {}
        open_event_count = int(row.get("open_event_count") or 0)
        pending_job_count = int(row.get("pending_job_count") or 0)
        return {
            "has_open_run": open_event_count > 0 or pending_job_count > 0,
            "open_event_count": open_event_count,
            "pending_job_count": pending_job_count,
        }

    def create_auto_trigger_rule_record(
        self,
        *,
        user_id: int,
        name: str,
        status: str = "active",
        scope_mode: str = "selected_subscriptions",
        subscription_ids: Optional[list[int]] = None,
        condition_mode: str = "any",
        conditions: Optional[list[dict]] = None,
        guard_groups: Optional[list[dict]] = None,
        action: Optional[dict] = None,
        daily_risk_control: Optional[dict] = None,
        cooldown_issues: int = 10,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO auto_trigger_rules(
                    user_id, name, status, scope_mode, subscription_ids_json, condition_mode,
                    conditions_json, guard_groups_json, action_json, daily_risk_control_json,
                    cooldown_issues, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id),
                    name,
                    status,
                    scope_mode,
                    _safe_json_dumps(subscription_ids or []),
                    condition_mode,
                    _safe_json_dumps(conditions or []),
                    _safe_json_dumps(guard_groups or []),
                    _safe_json_dumps(action or {}),
                    _safe_json_dumps(daily_risk_control or {}),
                    int(cooldown_issues),
                    now,
                    now,
                ),
            )
            rule_id = int(cur.lastrowid)
        return self.get_auto_trigger_rule(rule_id) or {}

    def get_auto_trigger_rule(self, rule_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM auto_trigger_rules WHERE id = ?", (int(rule_id),))
        return self._serialize_auto_trigger_rule_row(row) if row else None

    def list_auto_trigger_rules(self, *, user_id: Optional[int] = None, status: Optional[str] = None) -> list[Dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if user_id is not None:
            clauses.append("user_id = ?")
            params.append(int(user_id))
        if status:
            clauses.append("status = ?")
            params.append(str(status))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        rows = self._fetch_all(
            "SELECT * FROM auto_trigger_rules" + where + " ORDER BY id DESC",
            tuple(params),
        )
        return [self._serialize_auto_trigger_rule_row(row) for row in rows]

    def update_auto_trigger_rule_record(
        self,
        *,
        rule_id: int,
        user_id: int,
        name: str,
        status: str,
        scope_mode: str,
        subscription_ids: Optional[list[int]],
        condition_mode: str,
        conditions: Optional[list[dict]],
        guard_groups: Optional[list[dict]],
        action: Optional[dict],
        daily_risk_control: Optional[dict],
        cooldown_issues: int,
    ) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE auto_trigger_rules
                SET name = ?,
                    status = ?,
                    scope_mode = ?,
                    subscription_ids_json = ?,
                    condition_mode = ?,
                    conditions_json = ?,
                    guard_groups_json = ?,
                    action_json = ?,
                    daily_risk_control_json = ?,
                    cooldown_issues = ?,
                    updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (
                    name,
                    status,
                    scope_mode,
                    _safe_json_dumps(subscription_ids or []),
                    condition_mode,
                    _safe_json_dumps(conditions or []),
                    _safe_json_dumps(guard_groups or []),
                    _safe_json_dumps(action or {}),
                    _safe_json_dumps(daily_risk_control or {}),
                    int(cooldown_issues),
                    now,
                    int(rule_id),
                    int(user_id),
                ),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_auto_trigger_rule(rule_id)

    def update_auto_trigger_rule_status(self, *, rule_id: int, user_id: int, status: str) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE auto_trigger_rules
                SET status = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (status, now, int(rule_id), int(user_id)),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_auto_trigger_rule(rule_id)

    def delete_auto_trigger_rule_record(self, *, rule_id: int, user_id: int) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                "DELETE FROM auto_trigger_rules WHERE id = ? AND user_id = ?",
                (int(rule_id), int(user_id)),
            )
            return cursor.rowcount > 0

    def mark_auto_trigger_rule_triggered(self, *, rule_id: int, user_id: int, issue_no: str) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE auto_trigger_rules
                SET last_triggered_issue_no = ?, last_triggered_at = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (str(issue_no or ""), now, now, int(rule_id), int(user_id)),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_auto_trigger_rule(rule_id)

    def record_auto_trigger_event(
        self,
        *,
        rule_id: int,
        user_id: int,
        subscription_id: Optional[int] = None,
        source_id: Optional[int] = None,
        predictor_id: Optional[int] = None,
        latest_issue_no: str = "",
        status: str = "triggered",
        reason: str = "",
        matched_conditions: Optional[list[dict]] = None,
        snapshot: Optional[dict] = None,
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO auto_trigger_events(
                    rule_id, user_id, subscription_id, source_id, predictor_id, latest_issue_no,
                    status, reason, matched_conditions_json, snapshot_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(rule_id),
                    int(user_id),
                    int(subscription_id) if subscription_id is not None else None,
                    int(source_id) if source_id is not None else None,
                    int(predictor_id) if predictor_id is not None else None,
                    str(latest_issue_no or ""),
                    status,
                    reason,
                    _safe_json_dumps(matched_conditions or []),
                    _safe_json_dumps(snapshot or {}),
                ),
            )
            event_id = int(cur.lastrowid)
        row = self._fetch_one("SELECT * FROM auto_trigger_events WHERE id = ?", (event_id,))
        return self._serialize_auto_trigger_event_row(row) if row else {}

    def list_auto_trigger_events(
        self,
        *,
        user_id: Optional[int] = None,
        rule_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 50,
    ) -> list[Dict[str, Any]]:
        clauses = []
        params: list[Any] = []
        if user_id is not None:
            clauses.append("e.user_id = ?")
            params.append(int(user_id))
        if rule_id is not None:
            clauses.append("e.rule_id = ?")
            params.append(int(rule_id))
        if status:
            clauses.append("e.status = ?")
            params.append(str(status))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        params.append(max(1, min(int(limit or 50), 200)))
        rows = self._fetch_all(
            """
            SELECT e.*, r.name AS rule_name, ss.name AS source_name
            FROM auto_trigger_events e
            LEFT JOIN auto_trigger_rules r ON r.id = e.rule_id
            LEFT JOIN signal_sources ss ON ss.id = e.source_id
            """ + where + """
            ORDER BY e.id DESC
            LIMIT ?
            """,
            tuple(params),
        )
        return [self._serialize_auto_trigger_event_row(row) for row in rows]

    def prune_auto_trigger_events(
        self,
        *,
        cutoffs_by_status: Dict[str, str],
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        deleted_by_status: Dict[str, int] = {}
        with self._connect() as conn:
            for status, cutoff in cutoffs_by_status.items():
                if user_id is None:
                    cursor = conn.execute(
                        "DELETE FROM auto_trigger_events WHERE status = ? AND created_at < ?",
                        (str(status), str(cutoff)),
                    )
                else:
                    cursor = conn.execute(
                        """
                        DELETE FROM auto_trigger_events
                        WHERE user_id = ? AND status = ? AND created_at < ?
                        """,
                        (int(user_id), str(status), str(cutoff)),
                    )
                deleted_by_status[str(status)] = int(cursor.rowcount or 0)
        return {
            "deleted_count": sum(deleted_by_status.values()),
            "deleted_by_status": deleted_by_status,
        }

    def get_auto_trigger_rule_daily_stat(self, *, rule_id: int, user_id: int, stat_date: str) -> Dict[str, Any]:
        row = self._fetch_one(
            """
            SELECT *
            FROM auto_trigger_rule_daily_stats
            WHERE rule_id = ? AND user_id = ? AND stat_date = ?
            LIMIT 1
            """,
            (int(rule_id), int(user_id), str(stat_date or "").strip()),
        )
        if row:
            return self._serialize_auto_trigger_rule_daily_stat_row(row)
        return {
            "id": None,
            "rule_id": int(rule_id),
            "user_id": int(user_id),
            "stat_date": str(stat_date or "").strip(),
            "profit_amount": 0.0,
            "loss_amount": 0.0,
            "net_profit": 0.0,
            "settled_event_count": 0,
            "hit_count": 0,
            "miss_count": 0,
            "refund_count": 0,
            "status": "active",
            "stopped_reason": "",
            "stopped_at": None,
            "created_at": None,
            "updated_at": None,
        }

    def get_auto_trigger_rule_run(self, rule_run_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one(
            "SELECT * FROM auto_trigger_rule_runs WHERE id = ? LIMIT 1",
            (int(rule_run_id),),
        )
        return self._serialize_auto_trigger_rule_run_row(row) if row else None

    def get_latest_auto_trigger_rule_run_for_subscription(self, *, subscription_id: int, user_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one(
            """
            SELECT *
            FROM auto_trigger_rule_runs
            WHERE subscription_id = ? AND user_id = ?
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(subscription_id), int(user_id)),
        )
        return self._serialize_auto_trigger_rule_run_row(row) if row else None

    def ensure_auto_trigger_rule_run(
        self,
        *,
        rule_id: int,
        user_id: int,
        subscription_id: int,
        stat_date: str,
        started_issue_no: str = "",
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO auto_trigger_rule_runs(
                    rule_id, user_id, subscription_id, stat_date, started_issue_no,
                    status, started_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, 'active', ?, ?, ?)
                ON CONFLICT(rule_id, subscription_id, stat_date) DO UPDATE SET
                    user_id = excluded.user_id,
                    started_issue_no = CASE
                        WHEN auto_trigger_rule_runs.started_issue_no = '' THEN excluded.started_issue_no
                        ELSE auto_trigger_rule_runs.started_issue_no
                    END,
                    status = CASE
                        WHEN auto_trigger_rule_runs.status = 'closed' THEN 'active'
                        ELSE auto_trigger_rule_runs.status
                    END,
                    updated_at = excluded.updated_at
                """,
                (
                    int(rule_id),
                    int(user_id),
                    int(subscription_id),
                    str(stat_date or "").strip(),
                    str(started_issue_no or ""),
                    now,
                    now,
                    now,
                ),
            )
        row = self._fetch_one(
            """
            SELECT *
            FROM auto_trigger_rule_runs
            WHERE rule_id = ? AND subscription_id = ? AND stat_date = ?
            LIMIT 1
            """,
            (int(rule_id), int(subscription_id), str(stat_date or "").strip()),
        )
        return self._serialize_auto_trigger_rule_run_row(row) if row else {}

    def stop_auto_trigger_rule_day(
        self,
        *,
        rule_id: int,
        user_id: int,
        stat_date: str,
        reason: str,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO auto_trigger_rule_daily_stats(
                    rule_id, user_id, stat_date, status, stopped_reason, stopped_at, created_at, updated_at
                ) VALUES (?, ?, ?, 'stopped', ?, ?, ?, ?)
                ON CONFLICT(rule_id, stat_date) DO UPDATE SET
                    user_id = excluded.user_id,
                    status = 'stopped',
                    stopped_reason = CASE
                        WHEN auto_trigger_rule_daily_stats.stopped_reason = '' THEN excluded.stopped_reason
                        ELSE auto_trigger_rule_daily_stats.stopped_reason
                    END,
                    stopped_at = COALESCE(auto_trigger_rule_daily_stats.stopped_at, excluded.stopped_at),
                    updated_at = excluded.updated_at
                """,
                (int(rule_id), int(user_id), str(stat_date or "").strip(), str(reason or ""), now, now, now),
            )
            conn.execute(
                """
                UPDATE auto_trigger_rule_runs
                SET status = 'stopped',
                    stop_reason = CASE WHEN stop_reason = '' THEN ? ELSE stop_reason END,
                    stopped_at = COALESCE(stopped_at, ?),
                    updated_at = ?
                WHERE rule_id = ? AND user_id = ? AND stat_date = ? AND status = 'active'
                """,
                (str(reason or ""), now, now, int(rule_id), int(user_id), str(stat_date or "").strip()),
            )
        return self.get_auto_trigger_rule_daily_stat(rule_id=rule_id, user_id=user_id, stat_date=stat_date)

    def upsert_auto_trigger_rule_daily_stat(
        self,
        conn: sqlite3.Connection,
        *,
        rule_id: int,
        user_id: int,
        stat_date: str,
        profit_delta: float,
        loss_delta: float,
        net_delta: float,
        result_type: str,
        updated_at: str,
    ) -> Dict[str, Any]:
        hit_count = 1 if result_type == "hit" else 0
        miss_count = 1 if result_type == "miss" else 0
        refund_count = 1 if result_type == "refund" else 0
        conn.execute(
            """
            INSERT INTO auto_trigger_rule_daily_stats(
                rule_id, user_id, stat_date,
                profit_amount, loss_amount, net_profit,
                settled_event_count, hit_count, miss_count, refund_count,
                status, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'active', ?, ?)
            ON CONFLICT(rule_id, stat_date) DO UPDATE SET
                user_id = excluded.user_id,
                profit_amount = ROUND(auto_trigger_rule_daily_stats.profit_amount + excluded.profit_amount, 2),
                loss_amount = ROUND(auto_trigger_rule_daily_stats.loss_amount + excluded.loss_amount, 2),
                net_profit = ROUND(auto_trigger_rule_daily_stats.net_profit + excluded.net_profit, 2),
                settled_event_count = auto_trigger_rule_daily_stats.settled_event_count + excluded.settled_event_count,
                hit_count = auto_trigger_rule_daily_stats.hit_count + excluded.hit_count,
                miss_count = auto_trigger_rule_daily_stats.miss_count + excluded.miss_count,
                refund_count = auto_trigger_rule_daily_stats.refund_count + excluded.refund_count,
                updated_at = excluded.updated_at
            """,
            (
                int(rule_id),
                int(user_id),
                str(stat_date or "").strip(),
                _round_money(profit_delta),
                _round_money(loss_delta),
                _round_money(net_delta),
                1,
                hit_count,
                miss_count,
                refund_count,
                updated_at,
                updated_at,
            ),
        )
        row = conn.execute(
            """
            SELECT *
            FROM auto_trigger_rule_daily_stats
            WHERE rule_id = ? AND user_id = ? AND stat_date = ?
            LIMIT 1
            """,
            (int(rule_id), int(user_id), str(stat_date or "").strip()),
        ).fetchone()
        return self._serialize_auto_trigger_rule_daily_stat_row(dict(row)) if row else {}

    def cancel_pending_auto_trigger_rule_jobs(
        self,
        *,
        rule_id: int,
        user_id: int,
        stat_date: str,
        reason: str,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            job_cursor = conn.execute(
                """
                UPDATE execution_jobs
                SET status = 'expired',
                    error_message = ?,
                    updated_at = ?
                WHERE user_id = ?
                  AND status = 'pending'
                  AND progression_event_id IN (
                      SELECT id
                      FROM subscription_progression_events
                      WHERE auto_trigger_rule_id = ?
                        AND user_id = ?
                        AND auto_trigger_stat_date = ?
                  )
                """,
                (str(reason or "规则日风控已停止"), now, int(user_id), int(rule_id), int(user_id), str(stat_date or "").strip()),
            )
            event_cursor = conn.execute(
                """
                UPDATE subscription_progression_events
                SET status = 'cancelled',
                    result_context_json = ?,
                    updated_at = ?
                WHERE auto_trigger_rule_id = ?
                  AND user_id = ?
                  AND auto_trigger_stat_date = ?
                  AND status = 'pending'
                  AND id NOT IN (
                      SELECT progression_event_id
                      FROM execution_jobs
                      WHERE progression_event_id IS NOT NULL
                        AND status NOT IN ('pending', 'expired')
                  )
                """,
                (
                    _safe_json_dumps({"cancel_reason": str(reason or "规则日风控已停止")}),
                    now,
                    int(rule_id),
                    int(user_id),
                    str(stat_date or "").strip(),
                ),
            )
        return {
            "expired_job_count": int(job_cursor.rowcount or 0),
            "cancelled_event_count": int(event_cursor.rowcount or 0),
        }

    def prune_auto_trigger_rule_runtime_data(
        self,
        *,
        runs_cutoff: str,
        stats_cutoff: str,
        user_id: Optional[int] = None,
    ) -> Dict[str, Any]:
        with self._connect() as conn:
            if user_id is None:
                runs_cursor = conn.execute(
                    "DELETE FROM auto_trigger_rule_runs WHERE stat_date < ?",
                    (str(runs_cutoff or ""),),
                )
                stats_cursor = conn.execute(
                    "DELETE FROM auto_trigger_rule_daily_stats WHERE stat_date < ?",
                    (str(stats_cutoff or ""),),
                )
            else:
                runs_cursor = conn.execute(
                    "DELETE FROM auto_trigger_rule_runs WHERE user_id = ? AND stat_date < ?",
                    (int(user_id), str(runs_cutoff or "")),
                )
                stats_cursor = conn.execute(
                    "DELETE FROM auto_trigger_rule_daily_stats WHERE user_id = ? AND stat_date < ?",
                    (int(user_id), str(stats_cutoff or "")),
                )
        return {
            "deleted_count": int(runs_cursor.rowcount or 0) + int(stats_cursor.rowcount or 0),
            "deleted_runs_count": int(runs_cursor.rowcount or 0),
            "deleted_stats_count": int(stats_cursor.rowcount or 0),
        }

    def get_latest_auto_trigger_event(
        self,
        *,
        rule_id: int,
        subscription_id: Optional[int] = None,
        status: str = "triggered",
    ) -> Optional[Dict[str, Any]]:
        if subscription_id is None:
            row = self._fetch_one(
                """
                SELECT * FROM auto_trigger_events
                WHERE rule_id = ? AND status = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(rule_id), status),
            )
        else:
            row = self._fetch_one(
                """
                SELECT * FROM auto_trigger_events
                WHERE rule_id = ? AND subscription_id = ? AND status = ?
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(rule_id), int(subscription_id), status),
            )
        return self._serialize_auto_trigger_event_row(row) if row else None

    def update_subscription_status(self, *, subscription_id: int, user_id: int, status: str) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE user_subscriptions
                SET status = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (status, now, int(subscription_id), int(user_id)),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_subscription(subscription_id)

    def update_subscription_record(
        self,
        *,
        subscription_id: int,
        user_id: int,
        source_id: int,
        strategy: Optional[dict] = None,
        status: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        current = self.get_subscription(subscription_id)
        if not current or int(current["user_id"]) != int(user_id):
            return None
        next_status = status if status is not None else current["status"]
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE user_subscriptions
                SET source_id = ?, strategy_json = ?, status = ?, updated_at = ?
                WHERE id = ? AND user_id = ?
                """,
                (
                    int(source_id),
                    _safe_json_dumps(strategy or {}),
                    next_status,
                    now,
                    int(subscription_id),
                    int(user_id),
                ),
            )
            if cursor.rowcount <= 0:
                return None
        return self.get_subscription(subscription_id)

    def delete_subscription_record(self, *, subscription_id: int, user_id: int) -> bool:
        with self._connect() as conn:
            conn.execute(
                "DELETE FROM subscription_daily_stats WHERE subscription_id = ? AND user_id = ?",
                (int(subscription_id), int(user_id)),
            )
            conn.execute(
                "DELETE FROM subscription_progression_events WHERE subscription_id = ? AND user_id = ?",
                (int(subscription_id), int(user_id)),
            )
            conn.execute(
                "DELETE FROM subscription_progression_state WHERE subscription_id = ? AND user_id = ?",
                (int(subscription_id), int(user_id)),
            )
            conn.execute(
                "DELETE FROM subscription_financial_state WHERE subscription_id = ? AND user_id = ?",
                (int(subscription_id), int(user_id)),
            )
            cursor = conn.execute(
                "DELETE FROM user_subscriptions WHERE id = ? AND user_id = ?",
                (int(subscription_id), int(user_id)),
            )
            return cursor.rowcount > 0

    def get_subscription_progression_state(self, subscription_id: int) -> Dict[str, Any]:
        row = self._fetch_one(
            "SELECT * FROM subscription_progression_state WHERE subscription_id = ? LIMIT 1",
            (int(subscription_id),),
        )
        if not row:
            return {
                "subscription_id": int(subscription_id),
                "user_id": None,
                "current_step": 1,
                "last_signal_id": None,
                "last_issue_no": "",
                "last_result_type": "",
                "updated_at": None,
            }
        return {
            "subscription_id": int(row["subscription_id"]),
            "user_id": int(row["user_id"]),
            "current_step": int(row["current_step"] or 1),
            "last_signal_id": int(row["last_signal_id"]) if row["last_signal_id"] is not None else None,
            "last_issue_no": str(row.get("last_issue_no") or ""),
            "last_result_type": str(row.get("last_result_type") or ""),
            "updated_at": row.get("updated_at"),
        }

    def get_subscription_financial_state(self, subscription_id: int) -> Dict[str, Any]:
        row = self._fetch_one(
            "SELECT * FROM subscription_financial_state WHERE subscription_id = ? LIMIT 1",
            (int(subscription_id),),
        )
        if not row:
            return self._default_subscription_financial_state(int(subscription_id))
        return self._serialize_subscription_financial_state_row(row)

    def _bootstrap_subscription_runtime_run(
        self,
        conn: sqlite3.Connection,
        *,
        subscription_id: int,
        user_id: int,
        issue_no: str,
        signal_id: Optional[int],
        started_at: str,
    ) -> Dict[str, Any]:
        financial_row = conn.execute(
            "SELECT * FROM subscription_financial_state WHERE subscription_id = ? LIMIT 1",
            (int(subscription_id),),
        ).fetchone()
        financial = (
            self._serialize_subscription_financial_state_row(dict(financial_row))
            if financial_row
            else self._default_subscription_financial_state(int(subscription_id))
        )
        baseline_reset_at = financial.get("baseline_reset_at")
        baseline_reset_note = str(financial.get("baseline_reset_note") or "")
        if baseline_reset_at:
            aggregate_row = conn.execute(
                """
                SELECT
                    COALESCE(COUNT(1), 0) AS settled_event_count,
                    COALESCE(SUM(CASE WHEN resolved_result_type = 'hit' THEN 1 ELSE 0 END), 0) AS hit_count,
                    COALESCE(SUM(CASE WHEN resolved_result_type = 'miss' THEN 1 ELSE 0 END), 0) AS miss_count,
                    COALESCE(SUM(CASE WHEN resolved_result_type = 'refund' THEN 1 ELSE 0 END), 0) AS refund_count
                FROM subscription_progression_events
                WHERE subscription_id = ? AND user_id = ? AND status = 'settled' AND settled_at >= ?
                """,
                (int(subscription_id), int(user_id), str(baseline_reset_at)),
            ).fetchone()
            first_event = conn.execute(
                """
                SELECT signal_id, issue_no, created_at
                FROM subscription_progression_events
                WHERE subscription_id = ? AND user_id = ? AND status = 'settled' AND settled_at >= ?
                ORDER BY settled_at ASC, id ASC
                LIMIT 1
                """,
                (int(subscription_id), int(user_id), str(baseline_reset_at)),
            ).fetchone()
            last_event = conn.execute(
                """
                SELECT issue_no, resolved_result_type
                FROM subscription_progression_events
                WHERE subscription_id = ? AND user_id = ? AND status = 'settled' AND settled_at >= ?
                ORDER BY settled_at DESC, id DESC
                LIMIT 1
                """,
                (int(subscription_id), int(user_id), str(baseline_reset_at)),
            ).fetchone()
        else:
            aggregate_row = conn.execute(
                """
                SELECT
                    COALESCE(COUNT(1), 0) AS settled_event_count,
                    COALESCE(SUM(CASE WHEN resolved_result_type = 'hit' THEN 1 ELSE 0 END), 0) AS hit_count,
                    COALESCE(SUM(CASE WHEN resolved_result_type = 'miss' THEN 1 ELSE 0 END), 0) AS miss_count,
                    COALESCE(SUM(CASE WHEN resolved_result_type = 'refund' THEN 1 ELSE 0 END), 0) AS refund_count
                FROM subscription_progression_events
                WHERE subscription_id = ? AND user_id = ? AND status = 'settled'
                """,
                (int(subscription_id), int(user_id)),
            ).fetchone()
            first_event = conn.execute(
                """
                SELECT signal_id, issue_no, created_at
                FROM subscription_progression_events
                WHERE subscription_id = ? AND user_id = ? AND status = 'settled'
                ORDER BY settled_at ASC, id ASC
                LIMIT 1
                """,
                (int(subscription_id), int(user_id)),
            ).fetchone()
            last_event = conn.execute(
                """
                SELECT issue_no, resolved_result_type
                FROM subscription_progression_events
                WHERE subscription_id = ? AND user_id = ? AND status = 'settled'
                ORDER BY settled_at DESC, id DESC
                LIMIT 1
                """,
                (int(subscription_id), int(user_id)),
            ).fetchone()

        aggregate = dict(aggregate_row) if aggregate_row else {}
        first = dict(first_event) if first_event else {}
        last = dict(last_event) if last_event else {}
        run_started_signal_id = int(first["signal_id"]) if first.get("signal_id") is not None else signal_id
        run_started_issue_no = str(first.get("issue_no") or issue_no or "")
        run_started_at = str(first.get("created_at") or baseline_reset_at or started_at)
        settled_event_count = int(aggregate.get("settled_event_count") or 0)
        hit_count = int(aggregate.get("hit_count") or 0)
        miss_count = int(aggregate.get("miss_count") or 0)
        refund_count = int(aggregate.get("refund_count") or 0)
        last_issue_no = str(last.get("issue_no") or "")
        last_result_type = str(last.get("resolved_result_type") or "")

        cur = conn.execute(
            """
            INSERT INTO subscription_runtime_runs(
                subscription_id, user_id, status,
                started_signal_id, started_issue_no, started_at, start_reason,
                ended_at, end_reason, last_issue_no, last_result_type,
                realized_profit, realized_loss, net_profit,
                settled_event_count, hit_count, miss_count, refund_count,
                baseline_reset_at, baseline_reset_note,
                created_at, updated_at
            ) VALUES (?, ?, 'active', ?, ?, ?, ?, NULL, '', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(subscription_id),
                int(user_id),
                int(run_started_signal_id) if run_started_signal_id is not None else None,
                run_started_issue_no,
                run_started_at,
                "baseline_reset" if baseline_reset_at else "auto_started",
                last_issue_no,
                last_result_type,
                _round_money(financial.get("realized_profit")),
                _round_money(financial.get("realized_loss")),
                _round_money(financial.get("net_profit")),
                settled_event_count,
                hit_count,
                miss_count,
                refund_count,
                baseline_reset_at,
                baseline_reset_note,
                started_at,
                started_at,
            ),
        )
        row = conn.execute(
            "SELECT * FROM subscription_runtime_runs WHERE id = ? LIMIT 1",
            (int(cur.lastrowid),),
        ).fetchone()
        return self._serialize_subscription_runtime_run_row(dict(row)) if row else {}

    def _ensure_active_subscription_runtime_run(
        self,
        conn: sqlite3.Connection,
        *,
        subscription_id: int,
        user_id: int,
        issue_no: str,
        signal_id: Optional[int],
        started_at: str,
    ) -> Dict[str, Any]:
        row = conn.execute(
            """
            SELECT *
            FROM subscription_runtime_runs
            WHERE subscription_id = ? AND user_id = ? AND status = 'active'
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(subscription_id), int(user_id)),
        ).fetchone()
        if row:
            return self._serialize_subscription_runtime_run_row(dict(row))
        return self._bootstrap_subscription_runtime_run(
            conn,
            subscription_id=int(subscription_id),
            user_id=int(user_id),
            issue_no=str(issue_no or ""),
            signal_id=int(signal_id) if signal_id is not None else None,
            started_at=str(started_at or _utc_now_iso()),
        )

    def list_subscription_runtime_runs(self, *, subscription_id: int, user_id: int, limit: int = 5) -> list[Dict[str, Any]]:
        with self._connect() as conn:
            self._reconcile_subscription_runtime_run_closure(
                conn,
                subscription_id=int(subscription_id),
                user_id=int(user_id),
            )
            rows = conn.execute(
                """
                SELECT *
                FROM subscription_runtime_runs
                WHERE subscription_id = ? AND user_id = ?
                ORDER BY
                    CASE WHEN status = 'active' THEN 0 ELSE 1 END ASC,
                    started_at DESC,
                    id DESC
                LIMIT ?
                """,
                (int(subscription_id), int(user_id), max(1, min(int(limit or 5), 20))),
            ).fetchall()
        return [self._serialize_subscription_runtime_run_row(dict(row)) for row in rows]

    def _reconcile_subscription_runtime_run_closure(
        self,
        conn: sqlite3.Connection,
        *,
        subscription_id: int,
        user_id: int,
    ) -> None:
        active_run_row = conn.execute(
            """
            SELECT *
            FROM subscription_runtime_runs
            WHERE subscription_id = ? AND user_id = ? AND status = 'active'
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(subscription_id), int(user_id)),
        ).fetchone()
        if not active_run_row:
            return

        financial_row = conn.execute(
            """
            SELECT *
            FROM subscription_financial_state
            WHERE subscription_id = ? AND user_id = ?
            LIMIT 1
            """,
            (int(subscription_id), int(user_id)),
        ).fetchone()
        if not financial_row:
            return

        threshold_status = str(financial_row["threshold_status"] or "").strip()
        if threshold_status not in {"profit_target_hit", "loss_limit_hit"}:
            return

        ended_at = financial_row["last_settled_at"] or financial_row["updated_at"] or _utc_now_iso()
        conn.execute(
            """
            UPDATE subscription_runtime_runs
            SET status = 'closed',
                ended_at = COALESCE(ended_at, ?),
                end_reason = CASE WHEN end_reason = '' THEN ? ELSE end_reason END,
                updated_at = ?
            WHERE id = ? AND status = 'active'
            """,
            (
                ended_at,
                threshold_status,
                _utc_now_iso(),
                int(active_run_row["id"]),
            ),
        )

    def _upsert_subscription_daily_stat(
        self,
        conn: sqlite3.Connection,
        *,
        stat_date: str,
        user_id: int,
        subscription_id: int,
        source_id: int,
        profit_delta: float,
        loss_delta: float,
        net_delta: float,
        result_type: str,
        updated_at: str,
    ) -> None:
        hit_count = 1 if result_type == "hit" else 0
        miss_count = 1 if result_type == "miss" else 0
        refund_count = 1 if result_type == "refund" else 0
        conn.execute(
            """
            INSERT INTO subscription_daily_stats(
                stat_date, user_id, subscription_id, source_id,
                profit_amount, loss_amount, net_profit,
                settled_event_count, hit_count, miss_count, refund_count,
                created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(subscription_id, stat_date) DO UPDATE SET
                user_id = excluded.user_id,
                source_id = excluded.source_id,
                profit_amount = ROUND(subscription_daily_stats.profit_amount + excluded.profit_amount, 2),
                loss_amount = ROUND(subscription_daily_stats.loss_amount + excluded.loss_amount, 2),
                net_profit = ROUND(subscription_daily_stats.net_profit + excluded.net_profit, 2),
                settled_event_count = subscription_daily_stats.settled_event_count + excluded.settled_event_count,
                hit_count = subscription_daily_stats.hit_count + excluded.hit_count,
                miss_count = subscription_daily_stats.miss_count + excluded.miss_count,
                refund_count = subscription_daily_stats.refund_count + excluded.refund_count,
                updated_at = excluded.updated_at
            """,
            (
                str(stat_date or ""),
                int(user_id),
                int(subscription_id),
                int(source_id),
                _round_money(profit_delta),
                _round_money(loss_delta),
                _round_money(net_delta),
                1,
                hit_count,
                miss_count,
                refund_count,
                updated_at,
                updated_at,
            ),
        )

    def list_user_daily_subscription_stats(self, *, user_id: int, stat_date: str) -> list[Dict[str, Any]]:
        rows = self._fetch_all(
            """
            SELECT
                sds.*,
                ss.name AS source_name
            FROM subscription_daily_stats sds
            JOIN signal_sources ss ON ss.id = sds.source_id
            WHERE sds.user_id = ? AND sds.stat_date = ?
            ORDER BY sds.net_profit DESC, ss.name ASC, sds.subscription_id ASC
            """,
            (int(user_id), str(stat_date or "").strip()),
        )
        return [self._serialize_subscription_daily_stat_row(row) for row in rows]

    def list_subscription_daily_stats(self, *, subscription_id: int, user_id: int, limit: int = 7) -> list[Dict[str, Any]]:
        rows = self._fetch_all(
            """
            SELECT
                sds.*,
                ss.name AS source_name
            FROM subscription_daily_stats sds
            JOIN signal_sources ss ON ss.id = sds.source_id
            WHERE sds.subscription_id = ? AND sds.user_id = ?
            ORDER BY sds.stat_date DESC, sds.updated_at DESC, sds.id DESC
            LIMIT ?
            """,
            (int(subscription_id), int(user_id), max(1, min(int(limit or 7), 30))),
        )
        return [self._serialize_subscription_daily_stat_row(row) for row in rows]

    def list_user_subscription_source_names(self, *, user_id: int) -> list[str]:
        rows = self._fetch_all(
            """
            SELECT DISTINCT ss.name
            FROM user_subscriptions us
            JOIN signal_sources ss ON ss.id = us.source_id
            WHERE us.user_id = ?
            ORDER BY ss.name ASC, us.id ASC
            """,
            (int(user_id),),
        )
        return [str(row.get("name") or "") for row in rows if str(row.get("name") or "").strip()]

    def get_user_daily_profit_summary(self, *, user_id: int, stat_date: str) -> Dict[str, Any]:
        row = self._fetch_one(
            """
            SELECT
                COALESCE(SUM(profit_amount), 0) AS profit_amount,
                COALESCE(SUM(loss_amount), 0) AS loss_amount,
                COALESCE(SUM(net_profit), 0) AS net_profit,
                COALESCE(SUM(settled_event_count), 0) AS settled_event_count,
                COALESCE(SUM(hit_count), 0) AS hit_count,
                COALESCE(SUM(miss_count), 0) AS miss_count,
                COALESCE(SUM(refund_count), 0) AS refund_count,
                COUNT(1) AS plan_count
            FROM subscription_daily_stats
            WHERE user_id = ? AND stat_date = ?
            """,
            (int(user_id), str(stat_date or "").strip()),
        ) or {}
        return {
            "stat_date": str(stat_date or "").strip(),
            "user_id": int(user_id),
            "profit_amount": _round_money(row.get("profit_amount")),
            "loss_amount": _round_money(row.get("loss_amount")),
            "net_profit": _round_money(row.get("net_profit")),
            "settled_event_count": int(row.get("settled_event_count") or 0),
            "hit_count": int(row.get("hit_count") or 0),
            "miss_count": int(row.get("miss_count") or 0),
            "refund_count": int(row.get("refund_count") or 0),
            "plan_count": int(row.get("plan_count") or 0),
        }

    def list_daily_user_profit_rankings(self, *, stat_date: str) -> list[Dict[str, Any]]:
        rows = self._fetch_all(
            """
            SELECT
                sds.user_id,
                u.username,
                COALESCE(SUM(sds.profit_amount), 0) AS profit_amount,
                COALESCE(SUM(sds.loss_amount), 0) AS loss_amount,
                COALESCE(SUM(sds.net_profit), 0) AS net_profit,
                COALESCE(SUM(sds.settled_event_count), 0) AS settled_event_count,
                COALESCE(SUM(sds.hit_count), 0) AS hit_count,
                COALESCE(SUM(sds.miss_count), 0) AS miss_count,
                COALESCE(SUM(sds.refund_count), 0) AS refund_count,
                COUNT(1) AS plan_count
            FROM subscription_daily_stats sds
            JOIN users u ON u.id = sds.user_id
            WHERE sds.stat_date = ?
            GROUP BY sds.user_id, u.username
            ORDER BY sds.user_id ASC
            """,
            (str(stat_date or "").strip(),),
        )
        return [
            {
                "stat_date": str(stat_date or "").strip(),
                "user_id": int(row["user_id"]),
                "username": str(row.get("username") or ""),
                "profit_amount": _round_money(row.get("profit_amount")),
                "loss_amount": _round_money(row.get("loss_amount")),
                "net_profit": _round_money(row.get("net_profit")),
                "settled_event_count": int(row.get("settled_event_count") or 0),
                "hit_count": int(row.get("hit_count") or 0),
                "miss_count": int(row.get("miss_count") or 0),
                "refund_count": int(row.get("refund_count") or 0),
                "plan_count": int(row.get("plan_count") or 0),
            }
            for row in rows
        ]

    def get_telegram_daily_report_record(self, report_key: str) -> Optional[Dict[str, Any]]:
        row = self._fetch_one(
            "SELECT * FROM telegram_daily_report_records WHERE report_key = ? LIMIT 1",
            (str(report_key or "").strip(),),
        )
        return self._serialize_telegram_daily_report_record_row(row) if row else None

    def mark_telegram_daily_report_sent(
        self,
        *,
        report_key: str,
        stat_date: str,
        target_chat_id: str,
        report_type: str,
        sent_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = sent_at or _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO telegram_daily_report_records(
                    report_key, stat_date, target_chat_id, report_type, status,
                    send_count, last_sent_at, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'sent', 1, ?, NULL, ?, ?)
                ON CONFLICT(report_key) DO UPDATE SET
                    stat_date = excluded.stat_date,
                    target_chat_id = excluded.target_chat_id,
                    report_type = excluded.report_type,
                    status = 'sent',
                    send_count = telegram_daily_report_records.send_count + 1,
                    last_sent_at = excluded.last_sent_at,
                    last_error = NULL,
                    updated_at = excluded.updated_at
                """,
                (
                    str(report_key or "").strip(),
                    str(stat_date or "").strip(),
                    str(target_chat_id or "").strip(),
                    str(report_type or "").strip(),
                    now,
                    now,
                    now,
                ),
            )
        return self.get_telegram_daily_report_record(str(report_key or "").strip()) or {}

    def mark_telegram_daily_report_failed(
        self,
        *,
        report_key: str,
        stat_date: str,
        target_chat_id: str,
        report_type: str,
        error_message: str,
        failed_at: Optional[str] = None,
    ) -> Dict[str, Any]:
        now = failed_at or _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO telegram_daily_report_records(
                    report_key, stat_date, target_chat_id, report_type, status,
                    send_count, last_sent_at, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, 'failed', 1, NULL, ?, ?, ?)
                ON CONFLICT(report_key) DO UPDATE SET
                    stat_date = excluded.stat_date,
                    target_chat_id = excluded.target_chat_id,
                    report_type = excluded.report_type,
                    status = 'failed',
                    send_count = telegram_daily_report_records.send_count + 1,
                    last_error = excluded.last_error,
                    updated_at = excluded.updated_at
                """,
                (
                    str(report_key or "").strip(),
                    str(stat_date or "").strip(),
                    str(target_chat_id or "").strip(),
                    str(report_type or "").strip(),
                    str(error_message or "").strip(),
                    now,
                    now,
                ),
            )
        return self.get_telegram_daily_report_record(str(report_key or "").strip()) or {}

    def get_telegram_bot_runtime_state(self, *, bot_name: str = "default") -> Dict[str, Any]:
        row = self._fetch_one(
            "SELECT * FROM telegram_bot_runtime_state WHERE bot_name = ? LIMIT 1",
            (str(bot_name or "default"),),
        )
        if not row:
            return {"bot_name": str(bot_name or "default"), "last_update_id": 0, "updated_at": None}
        return {
            "bot_name": str(row.get("bot_name") or ""),
            "last_update_id": int(row.get("last_update_id") or 0),
            "updated_at": row.get("updated_at"),
        }

    def update_telegram_bot_runtime_state(self, *, bot_name: str = "default", last_update_id: int) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO telegram_bot_runtime_state(bot_name, last_update_id, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(bot_name) DO UPDATE SET
                    last_update_id = excluded.last_update_id,
                    updated_at = excluded.updated_at
                """,
                (str(bot_name or "default"), int(last_update_id or 0), now),
            )
        return self.get_telegram_bot_runtime_state(bot_name=str(bot_name or "default"))

    def get_platform_runtime_setting(self, setting_key: str) -> Optional[Dict[str, Any]]:
        row = self._fetch_one(
            "SELECT * FROM platform_runtime_settings WHERE setting_key = ? LIMIT 1",
            (str(setting_key or "").strip(),),
        )
        return self._serialize_platform_runtime_setting_row(row) if row else None

    def upsert_platform_runtime_setting(self, *, setting_key: str, value: Dict[str, Any]) -> Dict[str, Any]:
        normalized_key = str(setting_key or "").strip()
        if not normalized_key:
            raise ValueError("setting_key 不能为空")
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO platform_runtime_settings(setting_key, value_json, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(setting_key) DO UPDATE SET
                    value_json = excluded.value_json,
                    updated_at = excluded.updated_at
                """,
                (normalized_key, _safe_json_dumps(value or {}), now),
            )
        return self.get_platform_runtime_setting(normalized_key) or {}

    def upsert_subscription_progression_state(
        self,
        *,
        subscription_id: int,
        user_id: int,
        current_step: int,
        last_signal_id: Optional[int] = None,
        last_issue_no: str = "",
        last_result_type: str = "",
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO subscription_progression_state(
                    subscription_id, user_id, current_step, last_signal_id, last_issue_no, last_result_type, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subscription_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    current_step = excluded.current_step,
                    last_signal_id = excluded.last_signal_id,
                    last_issue_no = excluded.last_issue_no,
                    last_result_type = excluded.last_result_type,
                    updated_at = excluded.updated_at
                """,
                (
                    int(subscription_id),
                    int(user_id),
                    max(1, int(current_step or 1)),
                    int(last_signal_id) if last_signal_id is not None else None,
                    str(last_issue_no or ""),
                    str(last_result_type or ""),
                    now,
                ),
            )
        return self.get_subscription_progression_state(subscription_id)

    def get_progression_event_by_signal(self, *, subscription_id: int, signal_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one(
            """
            SELECT * FROM subscription_progression_events
            WHERE subscription_id = ? AND signal_id = ?
            LIMIT 1
            """,
            (int(subscription_id), int(signal_id)),
        )
        return self._serialize_progression_event_row(row) if row else None

    def get_latest_pending_progression_event(self, *, subscription_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one(
            """
            SELECT * FROM subscription_progression_events
            WHERE subscription_id = ? AND status IN ('pending', 'placed')
            ORDER BY id DESC
            LIMIT 1
            """,
            (int(subscription_id),),
        )
        return self._serialize_progression_event_row(row) if row else None

    def list_open_progression_events(
        self,
        *,
        user_id: Optional[int] = None,
        subscription_id: Optional[int] = None,
        statuses: Optional[list[str]] = None,
        limit: int = 5000,
    ) -> list[Dict[str, Any]]:
        normalized_statuses = [str(item or "").strip() for item in (statuses or ["pending", "placed"]) if str(item or "").strip()]
        where_clauses = []
        params: list[Any] = []
        if user_id is not None:
            where_clauses.append("user_id = ?")
            params.append(int(user_id))
        if subscription_id is not None:
            where_clauses.append("subscription_id = ?")
            params.append(int(subscription_id))
        if normalized_statuses:
            placeholders = ",".join(["?"] * len(normalized_statuses))
            where_clauses.append("status IN (%s)" % placeholders)
            params.extend(normalized_statuses)
        where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
        rows = self._fetch_all(
            """
            SELECT *
            FROM subscription_progression_events
            %s
            ORDER BY CAST(issue_no AS INTEGER) ASC, id ASC
            LIMIT ?
            """
            % where_sql,
            tuple(params + [max(1, min(int(limit or 5000), 10000))]),
        )
        return [self._serialize_progression_event_row(row) for row in rows]

    def create_progression_event_record(
        self,
        *,
        subscription_id: int,
        user_id: int,
        signal_id: int,
        issue_no: str,
        progression_step: int,
        stake_amount: float,
        base_stake: float,
        multiplier: float,
        max_steps: int,
        refund_action: str,
        cap_action: str,
        settlement_rule_id: Optional[str] = None,
        settlement_snapshot: Optional[dict] = None,
        auto_trigger_rule_id: Optional[int] = None,
        auto_trigger_rule_run_id: Optional[int] = None,
        auto_trigger_stat_date: str = "",
        status: str = "pending",
    ) -> Dict[str, Any]:
        existing = self.get_progression_event_by_signal(subscription_id=subscription_id, signal_id=signal_id)
        if existing:
            return existing
        now = _utc_now_iso()
        with self._connect() as conn:
            self._ensure_active_subscription_runtime_run(
                conn,
                subscription_id=int(subscription_id),
                user_id=int(user_id),
                issue_no=str(issue_no or ""),
                signal_id=int(signal_id),
                started_at=now,
            )
            cur = conn.execute(
                """
                INSERT INTO subscription_progression_events(
                    subscription_id, user_id, signal_id, issue_no, progression_step, stake_amount,
                    base_stake, multiplier, max_steps, refund_action, cap_action,
                    settlement_rule_id, settlement_snapshot_json,
                    auto_trigger_rule_id, auto_trigger_rule_run_id, auto_trigger_stat_date,
                    status, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(subscription_id),
                    int(user_id),
                    int(signal_id),
                    str(issue_no or ""),
                    max(1, int(progression_step or 1)),
                    float(stake_amount or 0),
                    float(base_stake or 0),
                    float(multiplier or 2),
                    max(1, int(max_steps or 1)),
                    str(refund_action or "hold"),
                    str(cap_action or "reset"),
                    str(settlement_rule_id or ""),
                    _safe_json_dumps(settlement_snapshot or {}),
                    int(auto_trigger_rule_id) if auto_trigger_rule_id is not None else None,
                    int(auto_trigger_rule_run_id) if auto_trigger_rule_run_id is not None else None,
                    str(auto_trigger_stat_date or ""),
                    str(status or "pending"),
                    now,
                    now,
                ),
            )
            event_id = int(cur.lastrowid)
        return self.get_progression_event(event_id) or {}

    def get_progression_event(self, progression_event_id: int) -> Optional[Dict[str, Any]]:
        row = self._fetch_one(
            "SELECT * FROM subscription_progression_events WHERE id = ? LIMIT 1",
            (int(progression_event_id),),
        )
        return self._serialize_progression_event_row(row) if row else None

    def update_progression_event_status(self, *, progression_event_id: int, status: str) -> Optional[Dict[str, Any]]:
        now = _utc_now_iso()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE subscription_progression_events
                SET status = ?, updated_at = ?
                WHERE id = ?
                """,
                (str(status), now, int(progression_event_id)),
            )
        return self.get_progression_event(progression_event_id)

    def settle_progression_event(
        self,
        *,
        subscription_id: int,
        user_id: int,
        result_type: str,
        progression_event_id: Optional[int] = None,
        result_context: Optional[dict] = None,
    ) -> Dict[str, Any]:
        current_event = (
            self.get_progression_event(int(progression_event_id))
            if progression_event_id is not None
            else self.get_latest_pending_progression_event(subscription_id=int(subscription_id))
        )
        if not current_event or int(current_event["subscription_id"]) != int(subscription_id):
            raise ValueError("当前没有待结算的倍投状态")
        if int(current_event["user_id"]) != int(user_id):
            raise ValueError("无权结算该倍投状态")

        normalized_result = str(result_type or "").strip().lower()
        if normalized_result not in {"hit", "refund", "miss"}:
            raise ValueError("result_type 仅支持 hit/refund/miss")

        step = int(current_event["progression_step"] or 1)
        max_steps = max(1, int(current_event.get("max_steps") or 1))
        refund_action = str(current_event.get("refund_action") or "hold")
        cap_action = str(current_event.get("cap_action") or "reset")
        if normalized_result == "hit":
            next_step = 1
        elif normalized_result == "refund":
            next_step = step if refund_action == "hold" else 1
        else:
            if step >= max_steps:
                next_step = 1 if cap_action == "reset" else max_steps
            else:
                next_step = step + 1

        now = _utc_now_iso()
        auto_trigger_daily_risk: Dict[str, Any] = {}
        with self._connect() as conn:
            signal = self.get_signal(int(current_event["signal_id"])) if current_event.get("signal_id") is not None else None
            conn.execute(
                """
                UPDATE subscription_progression_events
                SET status = 'settled',
                    resolved_result_type = ?,
                    settled_at = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (normalized_result, now, now, int(current_event["id"])),
            )
            conn.execute(
                """
                INSERT INTO subscription_progression_state(
                    subscription_id, user_id, current_step, last_signal_id, last_issue_no, last_result_type, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subscription_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    current_step = excluded.current_step,
                    last_signal_id = excluded.last_signal_id,
                    last_issue_no = excluded.last_issue_no,
                    last_result_type = excluded.last_result_type,
                    updated_at = excluded.updated_at
                """,
                (
                    int(subscription_id),
                    int(user_id),
                    max(1, int(next_step or 1)),
                    int(current_event["signal_id"]),
                    str(current_event.get("issue_no") or ""),
                    normalized_result,
                    now,
                ),
            )

            subscription_row = conn.execute(
                "SELECT source_id, strategy_json FROM user_subscriptions WHERE id = ? AND user_id = ? LIMIT 1",
                (int(subscription_id), int(user_id)),
            ).fetchone()
            strategy = _safe_json_loads(subscription_row["strategy_json"]) if subscription_row else {}
            risk_control = _subscription_risk_control(strategy)
            source_id = int(subscription_row["source_id"]) if subscription_row and subscription_row["source_id"] is not None else None

            financial_row = conn.execute(
                "SELECT * FROM subscription_financial_state WHERE subscription_id = ? LIMIT 1",
                (int(subscription_id),),
            ).fetchone()
            current_financial = (
                self._serialize_subscription_financial_state_row(dict(financial_row))
                if financial_row
                else self._default_subscription_financial_state(int(subscription_id))
            )

            stake_amount = _round_money(current_event.get("stake_amount"))
            settlement_context = _event_settlement_context(
                current_event=current_event,
                signal=signal,
                strategy=strategy,
            )
            profit_delta = 0.0
            loss_delta = 0.0
            net_delta = 0.0
            if normalized_result == "hit":
                profit_delta = _progression_hit_profit_delta(
                    settlement_context=settlement_context,
                    signal=signal,
                    stake_amount=stake_amount,
                )
                net_delta = profit_delta
            elif normalized_result == "miss":
                loss_delta = stake_amount
                net_delta = -stake_amount
            settlement_snapshot = dict(settlement_context.get("snapshot") or {})
            result_context_payload = {
                "result_type": normalized_result,
                "next_step": max(1, int(next_step or 1)),
            }
            if isinstance(result_context, dict):
                result_context_payload.update(result_context)
            conn.execute(
                """
                UPDATE subscription_progression_events
                SET settlement_rule_id = ?,
                    profit_delta = ?,
                    loss_delta = ?,
                    net_delta = ?,
                    settlement_snapshot_json = ?,
                    result_context_json = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    str(settlement_context.get("settlement_rule_id") or ""),
                    profit_delta,
                    loss_delta,
                    net_delta,
                    _safe_json_dumps(settlement_snapshot),
                    _safe_json_dumps(result_context_payload),
                    now,
                    int(current_event["id"]),
                ),
            )

            next_realized_profit = _round_money(current_financial["realized_profit"] + profit_delta)
            next_realized_loss = _round_money(current_financial["realized_loss"] + loss_delta)
            next_net_profit = _round_money(current_financial["net_profit"] + net_delta)
            threshold_status = str(current_financial.get("threshold_status") or "")
            stopped_reason = str(current_financial.get("stopped_reason") or "")
            if not threshold_status and bool(risk_control["enabled"]):
                if float(risk_control["profit_target"]) > 0 and next_net_profit >= float(risk_control["profit_target"]):
                    threshold_status = "profit_target_hit"
                    stopped_reason = "达到止盈阈值，当前轮次已停止"
                elif float(risk_control["loss_limit"]) > 0 and next_net_profit <= -float(risk_control["loss_limit"]):
                    threshold_status = "loss_limit_hit"
                    stopped_reason = "达到止损阈值，当前轮次已停止"

            conn.execute(
                """
                INSERT INTO subscription_financial_state(
                    subscription_id, user_id, realized_profit, realized_loss, net_profit,
                    threshold_status, stopped_reason, baseline_reset_at, baseline_reset_note,
                    last_settled_event_id, last_settled_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(subscription_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    realized_profit = excluded.realized_profit,
                    realized_loss = excluded.realized_loss,
                    net_profit = excluded.net_profit,
                    threshold_status = excluded.threshold_status,
                    stopped_reason = excluded.stopped_reason,
                    baseline_reset_at = excluded.baseline_reset_at,
                    baseline_reset_note = excluded.baseline_reset_note,
                    last_settled_event_id = excluded.last_settled_event_id,
                    last_settled_at = excluded.last_settled_at,
                    updated_at = excluded.updated_at
                """,
                (
                    int(subscription_id),
                    int(user_id),
                    next_realized_profit,
                    next_realized_loss,
                    next_net_profit,
                    threshold_status,
                    stopped_reason,
                    current_financial.get("baseline_reset_at"),
                    str(current_financial.get("baseline_reset_note") or ""),
                    int(current_event["id"]),
                    now,
                    now,
                ),
            )

            active_run = self._ensure_active_subscription_runtime_run(
                conn,
                subscription_id=int(subscription_id),
                user_id=int(user_id),
                issue_no=str(current_event.get("issue_no") or ""),
                signal_id=int(current_event["signal_id"]) if current_event.get("signal_id") is not None else None,
                started_at=now,
            )
            conn.execute(
                """
                UPDATE subscription_runtime_runs
                SET status = ?,
                    ended_at = ?,
                    end_reason = ?,
                    last_issue_no = ?,
                    last_result_type = ?,
                    realized_profit = ?,
                    realized_loss = ?,
                    net_profit = ?,
                    settled_event_count = ?,
                    hit_count = ?,
                    miss_count = ?,
                    refund_count = ?,
                    baseline_reset_at = ?,
                    baseline_reset_note = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (
                    "closed" if threshold_status else "active",
                    now if threshold_status else None,
                    threshold_status,
                    str(current_event.get("issue_no") or ""),
                    normalized_result,
                    next_realized_profit,
                    next_realized_loss,
                    next_net_profit,
                    int(active_run.get("settled_event_count") or 0) + 1,
                    int(active_run.get("hit_count") or 0) + (1 if normalized_result == "hit" else 0),
                    int(active_run.get("miss_count") or 0) + (1 if normalized_result == "miss" else 0),
                    int(active_run.get("refund_count") or 0) + (1 if normalized_result == "refund" else 0),
                    current_financial.get("baseline_reset_at"),
                    str(current_financial.get("baseline_reset_note") or ""),
                    now,
                    int(active_run["id"]),
                ),
            )

            if source_id is not None:
                self._upsert_subscription_daily_stat(
                    conn,
                    stat_date=_shanghai_date(now),
                    user_id=int(user_id),
                    subscription_id=int(subscription_id),
                    source_id=int(source_id),
                    profit_delta=profit_delta,
                    loss_delta=loss_delta,
                    net_delta=net_delta,
                    result_type=normalized_result,
                    updated_at=now,
                )

            auto_trigger_rule_id = current_event.get("auto_trigger_rule_id")
            auto_trigger_stat_date = str(current_event.get("auto_trigger_stat_date") or "").strip()
            if auto_trigger_rule_id is not None and auto_trigger_stat_date:
                rule_stat = self.upsert_auto_trigger_rule_daily_stat(
                    conn,
                    rule_id=int(auto_trigger_rule_id),
                    user_id=int(user_id),
                    stat_date=auto_trigger_stat_date,
                    profit_delta=profit_delta,
                    loss_delta=loss_delta,
                    net_delta=net_delta,
                    result_type=normalized_result,
                    updated_at=now,
                )
                rule_row = conn.execute(
                    "SELECT daily_risk_control_json FROM auto_trigger_rules WHERE id = ? AND user_id = ? LIMIT 1",
                    (int(auto_trigger_rule_id), int(user_id)),
                ).fetchone()
                daily_risk_control = _safe_json_loads(rule_row["daily_risk_control_json"]) if rule_row else {}
                stop_reason = ""
                if bool(daily_risk_control.get("enabled")) and str(rule_stat.get("status") or "") != "stopped":
                    profit_target = _round_money(daily_risk_control.get("profit_target"))
                    loss_limit = _round_money(daily_risk_control.get("loss_limit"))
                    current_rule_net = _round_money(rule_stat.get("net_profit"))
                    if profit_target > 0 and current_rule_net >= profit_target:
                        stop_reason = "profit_target_hit"
                    elif loss_limit > 0 and current_rule_net <= -loss_limit:
                        stop_reason = "loss_limit_hit"
                if stop_reason:
                    conn.execute(
                        """
                        UPDATE auto_trigger_rule_daily_stats
                        SET status = 'stopped',
                            stopped_reason = ?,
                            stopped_at = COALESCE(stopped_at, ?),
                            updated_at = ?
                        WHERE rule_id = ? AND user_id = ? AND stat_date = ?
                        """,
                        (stop_reason, now, now, int(auto_trigger_rule_id), int(user_id), auto_trigger_stat_date),
                    )
                    conn.execute(
                        """
                        UPDATE auto_trigger_rule_runs
                        SET status = 'stopped',
                            stop_reason = CASE WHEN stop_reason = '' THEN ? ELSE stop_reason END,
                            stopped_at = COALESCE(stopped_at, ?),
                            updated_at = ?
                        WHERE rule_id = ? AND user_id = ? AND stat_date = ? AND status = 'active'
                        """,
                        (stop_reason, now, now, int(auto_trigger_rule_id), int(user_id), auto_trigger_stat_date),
                    )
                    auto_trigger_daily_risk = {
                        "stopped": True,
                        "reason": stop_reason,
                        "rule_id": int(auto_trigger_rule_id),
                        "stat_date": auto_trigger_stat_date,
                        "cancel_pending_jobs": bool(daily_risk_control.get("cancel_pending_jobs", True)),
                    }
                else:
                    auto_trigger_daily_risk = {
                        "stopped": False,
                        "rule_id": int(auto_trigger_rule_id),
                        "stat_date": auto_trigger_stat_date,
                        "net_profit": _round_money(rule_stat.get("net_profit")),
                    }

        if auto_trigger_daily_risk.get("stopped") and auto_trigger_daily_risk.get("cancel_pending_jobs", True):
            auto_trigger_daily_risk["cancel"] = self.cancel_pending_auto_trigger_rule_jobs(
                rule_id=int(auto_trigger_daily_risk["rule_id"]),
                user_id=int(user_id),
                stat_date=str(auto_trigger_daily_risk["stat_date"]),
                reason="规则日风控已停止：%s" % str(auto_trigger_daily_risk.get("reason") or ""),
            )

        state = self.get_subscription_progression_state(int(subscription_id))
        return {
            "event": self.get_progression_event(int(current_event["id"])) or {},
            "state": state,
            "financial": self.get_subscription_financial_state(int(subscription_id)),
            "subscription": self.get_subscription(int(subscription_id)) or {},
            "auto_trigger_daily_risk": auto_trigger_daily_risk,
        }

    def reset_subscription_runtime(
        self,
        *,
        subscription_id: int,
        user_id: int,
        note: str = "",
    ) -> Dict[str, Any]:
        current = self.get_subscription(int(subscription_id))
        if not current or int(current["user_id"]) != int(user_id):
            raise ValueError("subscription_id 对应的订阅不存在")

        now = _utc_now_iso()
        reset_note = str(note or "").strip()
        voided_event_ids = []
        with self._connect() as conn:
            active_run_row = conn.execute(
                """
                SELECT *
                FROM subscription_runtime_runs
                WHERE subscription_id = ? AND user_id = ? AND status = 'active'
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(subscription_id), int(user_id)),
            ).fetchone()
            active_run = self._serialize_subscription_runtime_run_row(dict(active_run_row)) if active_run_row else None
            current_financial = current.get("financial") if isinstance(current.get("financial"), dict) else {}
            if active_run:
                end_reason = str(current_financial.get("threshold_status") or "").strip() or "manual_reset"
                conn.execute(
                    """
                    UPDATE subscription_runtime_runs
                    SET status = 'closed',
                        ended_at = ?,
                        end_reason = ?,
                        realized_profit = ?,
                        realized_loss = ?,
                        net_profit = ?,
                        baseline_reset_at = ?,
                        baseline_reset_note = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (
                        now,
                        end_reason,
                        _round_money(current_financial.get("realized_profit")),
                        _round_money(current_financial.get("realized_loss")),
                        _round_money(current_financial.get("net_profit")),
                        current_financial.get("baseline_reset_at"),
                        str(reset_note or current_financial.get("baseline_reset_note") or ""),
                        now,
                        int(active_run["id"]),
                    ),
                )
            open_events = conn.execute(
                """
                SELECT id FROM subscription_progression_events
                WHERE subscription_id = ? AND user_id = ? AND status IN ('pending', 'placed')
                ORDER BY id ASC
                """,
                (int(subscription_id), int(user_id)),
            ).fetchall()
            voided_event_ids = [int(row["id"]) for row in open_events]
            if voided_event_ids:
                placeholders = ",".join(["?"] * len(voided_event_ids))
                conn.execute(
                    """
                    UPDATE subscription_progression_events
                    SET status = 'reset',
                        resolved_result_type = 'reset',
                        result_context_json = ?,
                        settled_at = ?,
                        updated_at = ?
                    WHERE id IN (""" + placeholders + """)
                    """,
                    (_safe_json_dumps({"result_type": "reset", "reason": "subscription_runtime_reset"}), now, now, *voided_event_ids),
                )

            conn.execute(
                """
                UPDATE execution_jobs
                SET status = 'skipped',
                    error_message = ?,
                    updated_at = ?
                WHERE subscription_id = ?
                  AND user_id = ?
                  AND status = 'pending'
                """,
                ("策略已重置，旧轮次未执行任务已跳过", now, int(subscription_id), int(user_id)),
            )
            conn.execute(
                """
                INSERT INTO subscription_progression_state(
                    subscription_id, user_id, current_step, last_signal_id, last_issue_no, last_result_type, updated_at
                ) VALUES (?, ?, 1, NULL, '', '', ?)
                ON CONFLICT(subscription_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    current_step = 1,
                    last_signal_id = NULL,
                    last_issue_no = '',
                    last_result_type = '',
                    updated_at = excluded.updated_at
                """,
                (int(subscription_id), int(user_id), now),
            )
            conn.execute(
                """
                INSERT INTO subscription_financial_state(
                    subscription_id, user_id, realized_profit, realized_loss, net_profit,
                    threshold_status, stopped_reason, baseline_reset_at, baseline_reset_note,
                    last_settled_event_id, last_settled_at, updated_at
                ) VALUES (?, ?, 0, 0, 0, '', '', ?, ?, NULL, NULL, ?)
                ON CONFLICT(subscription_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    realized_profit = 0,
                    realized_loss = 0,
                    net_profit = 0,
                    threshold_status = '',
                    stopped_reason = '',
                    baseline_reset_at = excluded.baseline_reset_at,
                    baseline_reset_note = excluded.baseline_reset_note,
                    last_settled_event_id = NULL,
                    last_settled_at = NULL,
                    updated_at = excluded.updated_at
                """,
                (int(subscription_id), int(user_id), now, reset_note, now),
            )

        return {
            "item": self.get_subscription(int(subscription_id)) or {},
            "progression": self.get_subscription_progression_state(int(subscription_id)),
            "financial": self.get_subscription_financial_state(int(subscription_id)),
            "reset_at": now,
            "reset_note": reset_note,
            "voided_event_ids": voided_event_ids,
        }

    def create_execution_job(
        self,
        *,
        user_id: int,
        signal_id: int,
        subscription_id: Optional[int] = None,
        progression_event_id: Optional[int] = None,
        delivery_target_id: int,
        telegram_account_id: Optional[int] = None,
        executor_type: str,
        idempotency_key: str,
        planned_message_text: str,
        stake_plan: Optional[dict],
        execute_after: str,
        expire_at: str,
        status: str = "pending",
    ) -> int:
        with self._connect() as conn:
            cur = conn.execute(
                """
                INSERT INTO execution_jobs(
                    user_id, signal_id, subscription_id, progression_event_id, delivery_target_id, telegram_account_id, executor_type, idempotency_key,
                    planned_message_text, stake_plan_json, execute_after, expire_at, status
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(user_id),
                    int(signal_id),
                    int(subscription_id) if subscription_id is not None else None,
                    int(progression_event_id) if progression_event_id is not None else None,
                    int(delivery_target_id),
                    telegram_account_id,
                    executor_type,
                    idempotency_key,
                    planned_message_text,
                    _safe_json_dumps(stake_plan or {}),
                    execute_after,
                    expire_at,
                    status,
                ),
            )
            return int(cur.lastrowid)

    def get_execution_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        self.expire_due_jobs()
        row = self._fetch_one("SELECT * FROM execution_jobs WHERE id = ?", (int(job_id),))
        return self._serialize_execution_job_row(row) if row else None

    def expire_due_jobs(self) -> int:
        now = _utc_now_iso()
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE execution_jobs
                SET status = 'expired',
                    error_message = CASE
                        WHEN error_message IS NULL OR trim(error_message) = '' THEN '任务已过期'
                        ELSE error_message
                    END,
                    updated_at = ?
                WHERE status = 'pending'
                  AND expire_at <= ?
                """,
                (now, now),
            )
        return int(cursor.rowcount or 0)

    def list_execution_jobs(
        self,
        *,
        user_id: Optional[int] = None,
        signal_id: Optional[int] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        self.expire_due_jobs()
        conditions = []
        params: list[Any] = []
        if user_id is not None:
            conditions.append("j.user_id = ?")
            params.append(int(user_id))
        if signal_id is not None:
            conditions.append("j.signal_id = ?")
            params.append(int(signal_id))
        if status:
            conditions.append("j.status = ?")
            params.append(str(status))

        where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
        query = """
            SELECT
                j.*,
                s.lottery_type,
                s.issue_no,
                s.bet_type,
                s.bet_value,
                t.target_key,
                t.target_name,
                a.label AS telegram_account_label,
                COALESCE(attempt_stats.attempt_count, 0) AS attempt_count,
                last_attempt.attempt_no AS last_attempt_no,
                last_attempt.delivery_status AS last_delivery_status,
                last_attempt.remote_message_id AS last_remote_message_id,
                last_attempt.error_message AS last_error_message,
                last_attempt.executed_at AS last_executed_at,
                last_attempt.executor_instance_id AS last_executor_instance_id
            FROM execution_jobs j
            JOIN normalized_signals s ON s.id = j.signal_id
            JOIN delivery_targets t ON t.id = j.delivery_target_id
            LEFT JOIN telegram_accounts a ON a.id = j.telegram_account_id
            LEFT JOIN (
                SELECT
                    job_id,
                    COUNT(*) AS attempt_count,
                    MAX(id) AS last_attempt_id
                FROM execution_attempts
                GROUP BY job_id
            ) attempt_stats ON attempt_stats.job_id = j.id
            LEFT JOIN execution_attempts last_attempt ON last_attempt.id = attempt_stats.last_attempt_id
            %s
            ORDER BY j.id DESC
            LIMIT ?
        """ % where_clause
        rows = self._fetch_all(query, tuple(params + [max(1, min(int(limit or 100), 500))]))
        return [self._serialize_execution_job_row(row) for row in rows]

    def list_executor_instances(self, *, limit: int = 50) -> list[Dict[str, Any]]:
        query = """
            SELECT
                e.*,
                COALESCE(attempt_stats.total_attempt_count, 0) AS total_attempt_count,
                COALESCE(attempt_stats.delivered_attempt_count, 0) AS delivered_attempt_count,
                COALESCE(attempt_stats.failed_attempt_count, 0) AS failed_attempt_count,
                attempt_stats.last_executed_at AS last_executed_at,
                last_failure.executed_at AS last_failure_at,
                last_failure.delivery_status AS last_failure_status,
                last_failure.error_message AS last_failure_error_message
            FROM executor_instances e
            LEFT JOIN (
                SELECT
                    executor_instance_id,
                    COUNT(*) AS total_attempt_count,
                    SUM(CASE WHEN delivery_status = 'delivered' THEN 1 ELSE 0 END) AS delivered_attempt_count,
                    SUM(CASE WHEN delivery_status IN ('failed', 'expired', 'skipped') THEN 1 ELSE 0 END) AS failed_attempt_count,
                    MAX(executed_at) AS last_executed_at
                FROM execution_attempts
                GROUP BY executor_instance_id
            ) attempt_stats ON attempt_stats.executor_instance_id = e.executor_id
            LEFT JOIN (
                SELECT
                    executor_instance_id,
                    MAX(id) AS last_failure_id
                FROM execution_attempts
                WHERE delivery_status IN ('failed', 'expired', 'skipped')
                GROUP BY executor_instance_id
            ) failure_stats ON failure_stats.executor_instance_id = e.executor_id
            LEFT JOIN execution_attempts last_failure ON last_failure.id = failure_stats.last_failure_id
            ORDER BY e.last_seen_at DESC, e.executor_id ASC
            LIMIT ?
        """
        rows = self._fetch_all(query, (max(1, min(int(limit or 50), 200)),))
        return [self._serialize_executor_instance_row(row) for row in rows]

    def list_executor_attempts(self, *, executor_id: str, limit: int = 20) -> list[Dict[str, Any]]:
        rows = self._fetch_all(
            """
            SELECT *
            FROM execution_attempts
            WHERE executor_instance_id = ?
            ORDER BY executed_at DESC, id DESC
            LIMIT ?
            """,
            (str(executor_id), max(1, min(int(limit or 20), 200))),
        )
        return [self._serialize_execution_attempt_row(row) for row in rows]

    def get_platform_alert_record(self, alert_key: str) -> Optional[Dict[str, Any]]:
        row = self._fetch_one("SELECT * FROM platform_alert_records WHERE alert_key = ?", (str(alert_key),))
        return self._serialize_platform_alert_record_row(row) if row else None

    def list_platform_alert_records(self, *, status: Optional[str] = None, limit: int = 100) -> list[Dict[str, Any]]:
        if status:
            rows = self._fetch_all(
                """
                SELECT *
                FROM platform_alert_records
                WHERE status = ?
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (str(status), max(1, min(int(limit or 100), 500))),
            )
        else:
            rows = self._fetch_all(
                """
                SELECT *
                FROM platform_alert_records
                ORDER BY updated_at DESC, id DESC
                LIMIT ?
                """,
                (max(1, min(int(limit or 100), 500)),),
            )
        return [self._serialize_platform_alert_record_row(row) for row in rows]

    def list_platform_alert_records_by_keys(self, alert_keys: list[str]) -> dict[str, Dict[str, Any]]:
        normalized_keys = [str(item) for item in alert_keys if str(item).strip()]
        if not normalized_keys:
            return {}
        placeholders = ",".join(["?"] * len(normalized_keys))
        rows = self._fetch_all(
            "SELECT * FROM platform_alert_records WHERE alert_key IN (%s)" % placeholders,
            tuple(normalized_keys),
        )
        return {
            str(row["alert_key"]): self._serialize_platform_alert_record_row(row)
            for row in rows
        }

    def sync_platform_alert_records(
        self,
        alerts: list[Dict[str, Any]],
        *,
        repeat_interval_seconds: int,
    ) -> list[Dict[str, Any]]:
        now = _utc_now_iso()
        normalized_repeat_interval_seconds = max(60, int(repeat_interval_seconds or 60))
        all_rows = self._fetch_all("SELECT * FROM platform_alert_records")
        rows_by_key = {str(row["alert_key"]): row for row in all_rows}
        active_rows = [row for row in all_rows if str(row.get("status") or "") == "active"]
        active_by_key = {str(row["alert_key"]): row for row in active_rows}
        current_keys = {str(item["alert_key"]) for item in alerts}
        pending_notifications: list[Dict[str, Any]] = []

        with self._connect() as conn:
            for alert in alerts:
                alert_key = str(alert["alert_key"])
                current = rows_by_key.get(alert_key)
                metadata_text = _safe_json_dumps(alert.get("metadata") or {})
                if current is None:
                    conn.execute(
                        """
                        INSERT INTO platform_alert_records(
                            alert_key, alert_type, severity, title, message,
                            metadata_json, status, first_seen_at, last_seen_at,
                            resolved_at, last_sent_at, send_count, occurrence_count,
                            last_error, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, 'active', ?, ?, NULL, NULL, 0, 1, NULL, ?, ?)
                        """,
                        (
                            alert_key,
                            str(alert["alert_type"]),
                            str(alert["severity"]),
                            str(alert["title"]),
                            str(alert["message"]),
                            metadata_text,
                            now,
                            now,
                            now,
                            now,
                        ),
                    )
                    pending_notifications.append({**alert, "notification_event": "firing"})
                    continue

                last_sent_at = current.get("last_sent_at")
                should_notify = False
                notification_event = "firing"
                if str(current.get("status") or "") != "active":
                    should_notify = True
                    notification_event = "firing"
                elif current.get("last_sent_at") in {None, ""}:
                    should_notify = True
                    notification_event = "firing"
                else:
                    elapsed = max(0, int((datetime.now(timezone.utc) - _parse_iso8601(last_sent_at)).total_seconds()))
                    if elapsed >= normalized_repeat_interval_seconds:
                        should_notify = True
                        notification_event = "reminder"

                conn.execute(
                    """
                    UPDATE platform_alert_records
                    SET alert_type = ?,
                        severity = ?,
                        title = ?,
                        message = ?,
                        metadata_json = ?,
                        status = 'active',
                        last_seen_at = ?,
                        resolved_at = NULL,
                        occurrence_count = occurrence_count + 1,
                        updated_at = ?
                    WHERE alert_key = ?
                    """,
                    (
                        str(alert["alert_type"]),
                        str(alert["severity"]),
                        str(alert["title"]),
                        str(alert["message"]),
                        metadata_text,
                        now,
                        now,
                        alert_key,
                    ),
                )
                if should_notify:
                    pending_notifications.append({**alert, "notification_event": notification_event})

            for alert_key, row in active_by_key.items():
                if alert_key in current_keys:
                    continue
                conn.execute(
                    """
                    UPDATE platform_alert_records
                    SET status = 'resolved',
                        resolved_at = ?,
                        last_seen_at = ?,
                        updated_at = ?
                    WHERE alert_key = ?
                    """,
                    (now, now, now, alert_key),
                )
                pending_notifications.append(
                    {
                        "alert_key": alert_key,
                        "alert_type": str(row["alert_type"]),
                        "severity": "info",
                        "title": "告警已恢复",
                        "message": str(row["title"]),
                        "metadata": _safe_json_loads(row.get("metadata_json")),
                        "notification_event": "resolved",
                    }
                )

        records_map = self.list_platform_alert_records_by_keys([str(item["alert_key"]) for item in pending_notifications])
        return [{**item, "record": records_map.get(str(item["alert_key"]))} for item in pending_notifications]

    def mark_platform_alert_sent(
        self,
        *,
        alert_key: str,
        error: Optional[str] = None,
        sent_at: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        now = sent_at or _utc_now_iso()
        with self._connect() as conn:
            if error:
                conn.execute(
                    """
                    UPDATE platform_alert_records
                    SET last_error = ?, updated_at = ?
                    WHERE alert_key = ?
                    """,
                    (str(error), now, str(alert_key)),
                )
            else:
                conn.execute(
                    """
                    UPDATE platform_alert_records
                    SET last_sent_at = ?,
                        send_count = send_count + 1,
                        last_error = NULL,
                        updated_at = ?
                    WHERE alert_key = ?
                    """,
                    (now, now, str(alert_key)),
                )
        return self.get_platform_alert_record(str(alert_key))

    def list_recent_execution_failures(
        self,
        *,
        user_id: Optional[int] = None,
        limit: int = 20,
    ) -> list[Dict[str, Any]]:
        self.expire_due_jobs()
        conditions = ["j.status IN ('failed', 'expired', 'skipped')"]
        params: list[Any] = []
        if user_id is not None:
            conditions.append("j.user_id = ?")
            params.append(int(user_id))

        query = """
            SELECT
                j.id AS job_id,
                j.user_id,
                j.signal_id,
                j.delivery_target_id,
                j.telegram_account_id,
                j.status AS job_status,
                j.planned_message_text,
                s.lottery_type,
                s.issue_no,
                s.bet_type,
                s.bet_value,
                t.target_key,
                t.target_name,
                a.label AS telegram_account_label,
                fail_attempt.executor_instance_id,
                fail_attempt.attempt_no,
                fail_attempt.delivery_status,
                fail_attempt.remote_message_id,
                fail_attempt.error_message,
                fail_attempt.executed_at,
                attempt_stats.attempt_count,
                fail_attempt.raw_result
            FROM execution_jobs j
            JOIN normalized_signals s ON s.id = j.signal_id
            JOIN delivery_targets t ON t.id = j.delivery_target_id
            LEFT JOIN telegram_accounts a ON a.id = j.telegram_account_id
            JOIN (
                SELECT
                    job_id,
                    COUNT(*) AS attempt_count
                FROM execution_attempts
                GROUP BY job_id
            ) attempt_stats ON attempt_stats.job_id = j.id
            JOIN (
                SELECT
                    job_id,
                    MAX(id) AS last_failure_id
                FROM execution_attempts
                WHERE delivery_status IN ('failed', 'expired', 'skipped')
                GROUP BY job_id
            ) failure_stats ON failure_stats.job_id = j.id
            JOIN execution_attempts fail_attempt ON fail_attempt.id = failure_stats.last_failure_id
            WHERE %s
            ORDER BY fail_attempt.executed_at DESC, fail_attempt.id DESC
            LIMIT ?
        """ % " AND ".join(conditions)
        rows = self._fetch_all(query, tuple(params + [max(1, min(int(limit or 20), 200))]))
        return [self._serialize_execution_failure_row(row) for row in rows]

    def requeue_auto_retry_jobs(
        self,
        *,
        max_attempts: int,
        base_delay_seconds: int,
        limit: int = 100,
    ) -> list[Dict[str, Any]]:
        self.expire_due_jobs()
        bounded_max_attempts = max(1, int(max_attempts or 1))
        bounded_base_delay_seconds = max(5, int(base_delay_seconds or 5))
        rows = self._fetch_all(
            """
            SELECT
                j.id AS job_id,
                j.execute_after,
                j.expire_at,
                attempt_stats.attempt_count,
                last_attempt.executed_at AS last_executed_at,
                last_attempt.delivery_status AS last_delivery_status
            FROM execution_jobs j
            JOIN (
                SELECT
                    job_id,
                    COUNT(*) AS attempt_count,
                    MAX(id) AS last_attempt_id
                FROM execution_attempts
                GROUP BY job_id
            ) attempt_stats ON attempt_stats.job_id = j.id
            JOIN execution_attempts last_attempt ON last_attempt.id = attempt_stats.last_attempt_id
            WHERE j.status = 'failed'
              AND last_attempt.delivery_status = 'failed'
            ORDER BY last_attempt.executed_at ASC, j.id ASC
            LIMIT ?
            """,
            (max(1, min(int(limit or 100), 500)),),
        )
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        updated_at = _utc_now_iso()
        requeued: list[Dict[str, Any]] = []
        with self._connect() as conn:
            for row in rows:
                attempt_count = int(row["attempt_count"] or 0)
                if attempt_count >= bounded_max_attempts:
                    continue

                backoff_seconds = bounded_base_delay_seconds * (2 ** max(0, attempt_count - 1))
                retry_at = _parse_iso8601(row["last_executed_at"]) + timedelta(seconds=backoff_seconds)
                if retry_at > now_dt:
                    continue

                execute_after = _parse_iso8601(row["execute_after"])
                expire_at = _parse_iso8601(row["expire_at"])
                window_seconds = max(30, int((expire_at - execute_after).total_seconds() or 120))
                next_execute_after = _format_iso8601(now_dt)
                next_expire_at = _format_iso8601(now_dt + timedelta(seconds=window_seconds))
                conn.execute(
                    """
                    UPDATE execution_jobs
                    SET status = 'pending',
                        error_message = NULL,
                        execute_after = ?,
                        expire_at = ?,
                        updated_at = ?
                    WHERE id = ?
                      AND status = 'failed'
                    """,
                    (next_execute_after, next_expire_at, updated_at, int(row["job_id"])),
                )
                requeued.append(
                    {
                        "job_id": int(row["job_id"]),
                        "attempt_count": attempt_count,
                        "backoff_seconds": backoff_seconds,
                        "retry_at": _format_iso8601(retry_at),
                        "execute_after": next_execute_after,
                        "expire_at": next_expire_at,
                    }
                )
        return requeued

    def list_dispatch_candidates(self, signal_id: int) -> list[Dict[str, Any]]:
        query = """
            SELECT
                s.id AS signal_id,
                s.source_id,
                s.issue_no,
                s.bet_type,
                s.bet_value,
                s.lottery_type,
                s.normalized_payload,
                s.published_at,
                us.id AS subscription_id,
                us.user_id,
                us.strategy_json,
                dt.id AS delivery_target_id,
                dt.telegram_account_id,
                dt.template_id,
                dt.executor_type,
                dt.target_key,
                dt.target_name
            FROM normalized_signals s
            JOIN user_subscriptions us ON us.source_id = s.source_id
            JOIN delivery_targets dt ON dt.user_id = us.user_id
            LEFT JOIN telegram_accounts ta ON ta.id = dt.telegram_account_id
            LEFT JOIN subscription_financial_state sfs ON sfs.subscription_id = us.id
            WHERE s.id = ?
              AND s.status = 'ready'
              AND us.status = 'active'
              AND COALESCE(sfs.threshold_status, '') = ''
              AND dt.status = 'active'
              AND (dt.telegram_account_id IS NULL OR ta.status = 'active')
            ORDER BY us.user_id ASC, dt.id ASC
        """
        rows = self._fetch_all(query, (int(signal_id),))
        items = []
        for row in rows:
            row["normalized_payload"] = _safe_json_loads(row.get("normalized_payload"))
            row["strategy_json"] = _safe_json_loads(row.get("strategy_json"))
            items.append(row)
        return items

    def list_dispatch_candidates_for_subscription(self, signal_id: int, *, subscription_id: int) -> list[Dict[str, Any]]:
        query = """
            SELECT
                s.id AS signal_id,
                s.source_id,
                s.issue_no,
                s.bet_type,
                s.bet_value,
                s.lottery_type,
                s.normalized_payload,
                s.published_at,
                us.id AS subscription_id,
                us.user_id,
                us.strategy_json,
                dt.id AS delivery_target_id,
                dt.telegram_account_id,
                dt.template_id,
                dt.executor_type,
                dt.target_key,
                dt.target_name
            FROM normalized_signals s
            JOIN user_subscriptions us ON us.source_id = s.source_id
            JOIN delivery_targets dt ON dt.user_id = us.user_id
            LEFT JOIN telegram_accounts ta ON ta.id = dt.telegram_account_id
            LEFT JOIN subscription_financial_state sfs ON sfs.subscription_id = us.id
            WHERE s.id = ?
              AND us.id = ?
              AND s.status = 'ready'
              AND us.status = 'active'
              AND COALESCE(sfs.threshold_status, '') = ''
              AND dt.status = 'active'
              AND (dt.telegram_account_id IS NULL OR ta.status = 'active')
            ORDER BY dt.id ASC
        """
        rows = self._fetch_all(query, (int(signal_id), int(subscription_id)))
        items = []
        for row in rows:
            row["normalized_payload"] = _safe_json_loads(row.get("normalized_payload"))
            row["strategy_json"] = _safe_json_loads(row.get("strategy_json"))
            items.append(row)
        return items

    def get_latest_ready_signal_for_source(self, *, source_id: int, issue_no: Optional[str] = None) -> Optional[Dict[str, Any]]:
        if issue_no:
            row = self._fetch_one(
                """
                SELECT * FROM normalized_signals
                WHERE source_id = ? AND issue_no = ? AND status = 'ready'
                ORDER BY id DESC
                LIMIT 1
                """,
                (int(source_id), str(issue_no)),
            )
        else:
            row = self._fetch_one(
                """
                SELECT * FROM normalized_signals
                WHERE source_id = ? AND status = 'ready'
                ORDER BY CAST(COALESCE(NULLIF(issue_no, ''), '0') AS INTEGER) DESC, id DESC
                LIMIT 1
                """,
                (int(source_id),),
            )
        return self._serialize_signal_row(row) if row else None

    def create_execution_job_record(
        self,
        *,
        user_id: int,
        signal_id: int,
        subscription_id: Optional[int] = None,
        progression_event_id: Optional[int] = None,
        delivery_target_id: int,
        telegram_account_id: Optional[int] = None,
        executor_type: str,
        idempotency_key: str,
        planned_message_text: str,
        stake_plan: Optional[dict],
        execute_after: str,
        expire_at: str,
        status: str = "pending",
    ) -> Dict[str, Any]:
        existing = self._fetch_one(
            "SELECT * FROM execution_jobs WHERE user_id = ? AND idempotency_key = ?",
            (int(user_id), idempotency_key),
        )
        if existing:
            return {"created": False, "job": self.get_execution_job(int(existing["id"])) or {}}

        job_id = self.create_execution_job(
            user_id=user_id,
            signal_id=signal_id,
            subscription_id=subscription_id,
            progression_event_id=progression_event_id,
            delivery_target_id=delivery_target_id,
            telegram_account_id=telegram_account_id,
            executor_type=executor_type,
            idempotency_key=idempotency_key,
            planned_message_text=planned_message_text,
            stake_plan=stake_plan,
            execute_after=execute_after,
            expire_at=expire_at,
            status=status,
        )
        return {"created": True, "job": self.get_execution_job(job_id) or {}}

    # ====== Executor-facing APIs ======

    def pull_ready_jobs(self, executor_id: str, limit: int = 10) -> list[Dict[str, Any]]:
        self.expire_due_jobs()
        now = _utc_now_iso()
        limit = max(1, min(int(limit or 10), 100))
        query = """
            SELECT
                j.id AS job_id,
                s.id AS signal_id,
                s.lottery_type,
                s.issue_no,
                s.bet_type,
                s.bet_value,
                j.planned_message_text AS message_text,
                j.stake_plan_json AS stake_plan_json,
                j.idempotency_key,
                j.execute_after,
                j.expire_at,
                t.executor_type AS target_type,
                t.target_key AS target_key,
                t.target_name AS target_name,
                a.id AS telegram_account_id,
                a.label AS telegram_account_label,
                a.phone AS telegram_account_phone,
                a.session_path AS telegram_account_session_path
            FROM execution_jobs j
            JOIN normalized_signals s ON s.id = j.signal_id
            JOIN delivery_targets t ON t.id = j.delivery_target_id
            LEFT JOIN telegram_accounts a ON a.id = j.telegram_account_id
            WHERE j.status = 'pending'
              AND j.execute_after <= ?
              AND j.expire_at > ?
              AND t.status = 'active'
              AND (j.telegram_account_id IS NULL OR a.status = 'active')
            ORDER BY j.execute_after ASC, j.id ASC
            LIMIT ?
        """
        with self._connect() as conn:
            rows = conn.execute(query, (now, now, limit)).fetchall()
            items: list[Dict[str, Any]] = []
            for row in rows:
                items.append(
                    {
                        "job_id": str(row["job_id"]),
                        "signal_id": str(row["signal_id"]),
                        "lottery_type": str(row["lottery_type"]),
                        "issue_no": str(row["issue_no"]),
                        "bet_type": str(row["bet_type"]),
                        "bet_value": str(row["bet_value"]),
                        "message_text": str(row["message_text"] or ""),
                        "stake_plan": _safe_json_loads(row["stake_plan_json"]),
                        "target": {
                            "type": str(row["target_type"]),
                            "key": str(row["target_key"]),
                            "name": str(row["target_name"] or ""),
                        },
                        "telegram_account": (
                            {
                                "id": int(row["telegram_account_id"]),
                                "label": str(row["telegram_account_label"] or ""),
                                "phone": str(row["telegram_account_phone"] or ""),
                                "session_path": str(row["telegram_account_session_path"] or ""),
                            }
                            if row["telegram_account_id"] is not None
                            else None
                        ),
                        "idempotency_key": str(row["idempotency_key"]),
                        "execute_after": str(row["execute_after"]),
                        "expire_at": str(row["expire_at"]),
                    }
                )
            return items

    def retry_execution_job(self, *, job_id: int, user_id: int) -> Dict[str, Any]:
        self.expire_due_jobs()
        current = self._fetch_one(
            "SELECT * FROM execution_jobs WHERE id = ? AND user_id = ?",
            (int(job_id), int(user_id)),
        )
        if not current:
            raise ValueError("execution_job 不存在")

        current_status = str(current["status"])
        if current_status not in {"failed", "expired", "skipped"}:
            raise ValueError("当前任务状态不支持重试")

        execute_after = _parse_iso8601(current.get("execute_after"))
        expire_at = _parse_iso8601(current.get("expire_at"))
        window_seconds = max(30, int((expire_at - execute_after).total_seconds() or 120))
        now_dt = datetime.now(timezone.utc).replace(microsecond=0)
        next_execute_after = _format_iso8601(now_dt)
        next_expire_at = _format_iso8601(now_dt + timedelta(seconds=window_seconds))
        updated_at = _utc_now_iso()

        with self._connect() as conn:
            conn.execute(
                """
                UPDATE execution_jobs
                SET status = 'pending',
                    error_message = NULL,
                    execute_after = ?,
                    expire_at = ?,
                    updated_at = ?
                WHERE id = ?
                  AND user_id = ?
                """,
                (next_execute_after, next_expire_at, updated_at, int(job_id), int(user_id)),
            )
        return self.get_execution_job(int(job_id)) or {}

    def report_job_result(
        self,
        job_id: str,
        executor_id: str,
        attempt_no: int,
        delivery_status: str,
        remote_message_id: Optional[str],
        executed_at: str,
        raw_result: Optional[Dict[str, Any]],
        error_message: Optional[str],
    ) -> Dict[str, Any]:
        normalized_job_id = str(job_id).strip()
        if not normalized_job_id.isdigit():
            raise ValueError("job_id 不合法")

        now = _utc_now_iso()
        raw_result_text = _safe_json_dumps(raw_result or {})
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE execution_jobs
                SET status = ?, error_message = ?, updated_at = ?
                WHERE id = ?
                """,
                (delivery_status, error_message, now, int(normalized_job_id)),
            )
            conn.execute(
                """
                INSERT INTO execution_attempts (
                    job_id, executor_instance_id, attempt_no,
                    delivery_status, remote_message_id, raw_result,
                    error_message, executed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    int(normalized_job_id),
                    executor_id,
                    int(attempt_no),
                    delivery_status,
                    remote_message_id,
                    raw_result_text,
                    error_message,
                    executed_at,
                ),
            )
            job_row = conn.execute(
                """
                SELECT id, signal_id, subscription_id, progression_event_id
                FROM execution_jobs
                WHERE id = ?
                LIMIT 1
                """,
                (int(normalized_job_id),),
            ).fetchone()
            if job_row and job_row["progression_event_id"] is not None:
                aggregate = conn.execute(
                    """
                    SELECT
                        SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered_count,
                        SUM(CASE WHEN status IN ('failed', 'expired', 'skipped') THEN 1 ELSE 0 END) AS terminal_count,
                        COUNT(*) AS total_count
                    FROM execution_jobs
                    WHERE signal_id = ? AND subscription_id = ?
                    """,
                    (int(job_row["signal_id"]), int(job_row["subscription_id"])),
                ).fetchone()
                delivered_count = int(aggregate["delivered_count"] or 0) if aggregate else 0
                terminal_count = int(aggregate["terminal_count"] or 0) if aggregate else 0
                total_count = int(aggregate["total_count"] or 0) if aggregate else 0
                if delivered_count > 0:
                    conn.execute(
                        """
                        UPDATE subscription_progression_events
                        SET status = 'placed', updated_at = ?
                        WHERE id = ? AND status = 'pending'
                        """,
                        (now, int(job_row["progression_event_id"])),
                    )
                elif total_count > 0 and terminal_count >= total_count:
                    conn.execute(
                        """
                        UPDATE subscription_progression_events
                        SET status = 'void', updated_at = ?
                        WHERE id = ? AND status = 'pending'
                        """,
                        (now, int(job_row["progression_event_id"])),
                    )

        return {
            "job_id": normalized_job_id,
            "executor_id": executor_id,
            "attempt_no": int(attempt_no),
            "delivery_status": delivery_status,
            "remote_message_id": remote_message_id,
            "executed_at": executed_at,
            "raw_result": raw_result or {},
            "error_message": error_message,
            "updated_at": now,
        }

    def upsert_executor_heartbeat(
        self,
        executor_id: str,
        version: str,
        capabilities: Optional[Dict[str, Any]],
        status: str,
        last_seen_at: str,
    ) -> Dict[str, Any]:
        now = _utc_now_iso()
        capabilities_text = _safe_json_dumps(capabilities or {})
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO executor_instances (
                    executor_id, version, capabilities, status, last_seen_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(executor_id) DO UPDATE SET
                    version = excluded.version,
                    capabilities = excluded.capabilities,
                    status = excluded.status,
                    last_seen_at = excluded.last_seen_at,
                    updated_at = excluded.updated_at
                """,
                (
                    executor_id,
                    version,
                    capabilities_text,
                    status,
                    last_seen_at,
                    now,
                ),
            )

        return {
            "executor_id": executor_id,
            "version": version,
            "capabilities": capabilities or {},
            "status": status,
            "last_seen_at": last_seen_at,
            "updated_at": now,
        }
