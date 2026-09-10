"""Durable daily reporting, independent of disposable betting runtime data.

SQLite triggers keep reports in the settlement transaction, including writes by
older workers during a rolling upgrade. Deleting event details is retention,
not a reversal; only an explicit settlement update changes booked profit.
"""
from __future__ import annotations

import sqlite3


BACKFILL_KEY = "profit_report_settlements_v1"
REPORT_COLUMNS = (
    "user_id", "subscription_id", "source_id", "source_name", "origin_type",
    "rule_id", "rule_name", "stat_date", "profit_amount", "loss_amount",
    "net_profit", "settled_event_count", "hit_count", "miss_count", "refund_count",
)
REPORT_KEY = "user_id, origin_type, stat_date, subscription_id, rule_id"
MONEY_COLUMNS = ("profit_amount", "loss_amount", "net_profit")
COUNT_COLUMNS = ("settled_event_count", "hit_count", "miss_count", "refund_count")


def _settlement_projection(event: str, from_sql: str) -> str:
    # event/from_sql are internal SQL fragments, never request parameters.
    return f"""
        SELECT {event}.user_id, {event}.subscription_id,
               COALESCE(signal.source_id, subscription.source_id, 0) AS source_id,
               COALESCE(source.name, '') AS source_name,
               CASE WHEN {event}.auto_trigger_rule_id IS NULL
                          AND {event}.auto_trigger_route_id IS NULL
                    THEN 'manual' ELSE 'auto' END AS origin_type,
               COALESCE({event}.auto_trigger_rule_id, route.rule_id, 0) AS rule_id,
               COALESCE(rule.name, '') AS rule_name,
               date({event}.settled_at, '+8 hours') AS stat_date,
               MAX({event}.profit_delta, 0) AS profit_amount,
               MAX({event}.loss_delta, 0) AS loss_amount,
               {event}.net_delta AS net_profit,
               1 AS settled_event_count,
               CASE WHEN {event}.resolved_result_type = 'hit' THEN 1 ELSE 0 END AS hit_count,
               CASE WHEN {event}.resolved_result_type = 'miss' THEN 1 ELSE 0 END AS miss_count,
               CASE WHEN {event}.resolved_result_type = 'refund' THEN 1 ELSE 0 END AS refund_count
        FROM {from_sql}
        LEFT JOIN normalized_signals signal ON signal.id = {event}.signal_id
        LEFT JOIN user_subscriptions subscription
          ON subscription.id = {event}.subscription_id AND subscription.user_id = {event}.user_id
        LEFT JOIN signal_sources source ON source.id = COALESCE(signal.source_id, subscription.source_id)
        LEFT JOIN auto_trigger_rule_routes route
          ON route.id = {event}.auto_trigger_route_id AND route.user_id = {event}.user_id
        LEFT JOIN auto_trigger_rules rule
          ON rule.id = COALESCE({event}.auto_trigger_rule_id, route.rule_id)
         AND rule.user_id = {event}.user_id
        WHERE {event}.status = 'settled' AND date({event}.settled_at, '+8 hours') IS NOT NULL
    """


def _insert_report_sql(select_sql: str) -> str:
    updates = [
        "source_id = excluded.source_id",
        "source_name = CASE WHEN excluded.source_name <> '' THEN excluded.source_name ELSE profit_report_daily_stats.source_name END",
        "rule_name = CASE WHEN excluded.rule_name <> '' THEN excluded.rule_name ELSE profit_report_daily_stats.rule_name END",
    ]
    updates.extend(
        f"{column} = ROUND(profit_report_daily_stats.{column} + excluded.{column}, 2)"
        for column in MONEY_COLUMNS
    )
    updates.extend(
        f"{column} = profit_report_daily_stats.{column} + excluded.{column}"
        for column in COUNT_COLUMNS
    )
    updates.append("updated_at = strftime('%Y-%m-%dT%H:%M:%SZ','now')")
    return (
        f"INSERT INTO profit_report_daily_stats ({', '.join(REPORT_COLUMNS)}) "
        f"{select_sql} ON CONFLICT({REPORT_KEY}) DO UPDATE SET {', '.join(updates)};"
    )


def ensure_profit_reporting(conn: sqlite3.Connection) -> None:
    """Install reports and backfill available settlements once, atomically."""
    if not conn.in_transaction:
        conn.execute("BEGIN IMMEDIATE")
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS profit_report_daily_stats (
            user_id INTEGER NOT NULL,
            subscription_id INTEGER NOT NULL,
            source_id INTEGER NOT NULL,
            source_name TEXT NOT NULL DEFAULT '',
            origin_type TEXT NOT NULL CHECK(origin_type IN ('manual', 'auto')),
            rule_id INTEGER NOT NULL DEFAULT 0,
            rule_name TEXT NOT NULL DEFAULT '',
            stat_date TEXT NOT NULL,
            profit_amount REAL NOT NULL DEFAULT 0,
            loss_amount REAL NOT NULL DEFAULT 0,
            net_profit REAL NOT NULL DEFAULT 0,
            settled_event_count INTEGER NOT NULL DEFAULT 0,
            hit_count INTEGER NOT NULL DEFAULT 0,
            miss_count INTEGER NOT NULL DEFAULT 0,
            refund_count INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%SZ','now')),
            PRIMARY KEY ({REPORT_KEY}),
            FOREIGN KEY(user_id) REFERENCES users(id)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_profit_report_date_user "
        "ON profit_report_daily_stats(stat_date, user_id)"
    )
    migrated = conn.execute(
        "SELECT 1 FROM platform_runtime_settings WHERE setting_key = ?", (BACKFILL_KEY,)
    ).fetchone()
    if not migrated:
        projection = _settlement_projection("e", "subscription_progression_events e")
        aggregates = [
            "user_id", "subscription_id", "MAX(source_id)", "MAX(source_name)",
            "origin_type", "rule_id", "MAX(rule_name)", "stat_date",
        ]
        aggregates.extend(f"ROUND(SUM({column}), 2)" for column in MONEY_COLUMNS)
        aggregates.extend(f"SUM({column})" for column in COUNT_COLUMNS)
        conn.execute(_insert_report_sql(
            f"SELECT {', '.join(aggregates)} FROM ({projection}) "
            f"WHERE 1 GROUP BY {REPORT_KEY}"
        ))
        conn.execute(
            "INSERT INTO platform_runtime_settings(setting_key, value_json) "
            "VALUES (?, ?)",
            (BACKFILL_KEY, '{"version":1}'),
        )

    addition = _insert_report_sql(_settlement_projection("NEW", "(SELECT 1)"))
    old_key = """
        user_id = OLD.user_id AND subscription_id = OLD.subscription_id
        AND origin_type = CASE WHEN OLD.auto_trigger_rule_id IS NULL AND OLD.auto_trigger_route_id IS NULL
                               THEN 'manual' ELSE 'auto' END
        AND rule_id = COALESCE(OLD.auto_trigger_rule_id,
            (SELECT rule_id FROM auto_trigger_rule_routes WHERE id = OLD.auto_trigger_route_id AND user_id = OLD.user_id), 0)
        AND stat_date = date(OLD.settled_at, '+8 hours') AND OLD.status = 'settled'
    """
    conn.execute(f"""
        CREATE TRIGGER IF NOT EXISTS profit_report_settlement_insert
        AFTER INSERT ON subscription_progression_events
        WHEN NEW.status = 'settled'
        BEGIN
            {addition}
        END
    """)
    conn.execute(f"""
        CREATE TRIGGER IF NOT EXISTS profit_report_settlement_update
        AFTER UPDATE OF user_id, subscription_id, signal_id, status, settled_at,
                        auto_trigger_rule_id, auto_trigger_route_id, resolved_result_type,
                        profit_delta, loss_delta, net_delta ON subscription_progression_events
        WHEN OLD.status = 'settled' OR NEW.status = 'settled'
        BEGIN
            UPDATE profit_report_daily_stats
            SET profit_amount = ROUND(profit_amount - MAX(OLD.profit_delta, 0), 2),
                loss_amount = ROUND(loss_amount - MAX(OLD.loss_delta, 0), 2),
                net_profit = ROUND(net_profit - OLD.net_delta, 2),
                settled_event_count = settled_event_count - 1,
                hit_count = hit_count - CASE WHEN OLD.resolved_result_type = 'hit' THEN 1 ELSE 0 END,
                miss_count = miss_count - CASE WHEN OLD.resolved_result_type = 'miss' THEN 1 ELSE 0 END,
                refund_count = refund_count - CASE WHEN OLD.resolved_result_type = 'refund' THEN 1 ELSE 0 END
            WHERE {old_key};
            {addition}
            DELETE FROM profit_report_daily_stats WHERE {old_key} AND settled_event_count = 0;
        END
    """)
    # Preserve the last visible name even if it changed after the last settlement.
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS profit_report_subscription_delete
        BEFORE DELETE ON user_subscriptions
        BEGIN
            UPDATE profit_report_daily_stats
            SET source_name = COALESCE((SELECT name FROM signal_sources WHERE id = OLD.source_id), source_name)
            WHERE subscription_id = OLD.id AND user_id = OLD.user_id;
        END
    """)
    conn.execute("""
        CREATE TRIGGER IF NOT EXISTS profit_report_rule_delete
        BEFORE DELETE ON auto_trigger_rules
        BEGIN
            UPDATE profit_report_daily_stats SET rule_name = OLD.name
            WHERE rule_id = OLD.id AND user_id = OLD.user_id AND origin_type = 'auto';
        END
    """)
