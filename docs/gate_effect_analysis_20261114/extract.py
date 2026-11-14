#!/usr/bin/env python3
"""提取命中率熔断效果分析所需数据（只读）。

数据源：
- /www/wwwroot/pc28touzhu/pc28touzhu.db   (跟单系统账本)
- /www/wwwroot/AITradingSimulator/pc28_predictor.db (开奖数据)

输出 CSV 到同目录 data/ 下。
"""
import csv
import json
import os
import sqlite3
import time

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "data")
os.makedirs(OUT, exist_ok=True)

TOUZHU_DB = "file:/www/wwwroot/pc28touzhu/pc28touzhu.db?mode=ro"
PREDICTOR_DB = "file:/www/wwwroot/AITradingSimulator/pc28_predictor.db?mode=ro"

RULE_IDS = (4, 5, 6, 7, 8)


def connect(uri):
    conn = sqlite3.connect(uri, timeout=30)
    conn.row_factory = sqlite3.Row
    return conn


def q(conn, sql, params=(), retries=5):
    """带退避重试的查询（pc28touzhu.db 为 delete journal，可能撞写锁）。"""
    for attempt in range(retries):
        try:
            return conn.execute(sql, params).fetchall()
        except sqlite3.OperationalError as exc:
            if "locked" in str(exc) or "busy" in str(exc):
                time.sleep(1.5 * (attempt + 1))
                continue
            raise
    raise RuntimeError("db busy after retries: %s" % sql[:120])


def write_csv(name, rows, fields):
    path = os.path.join(OUT, name)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in fields})
    print("wrote %s rows=%d" % (name, len(rows)))


def extract_rules(conn):
    rows = q(conn, "SELECT id, name, status, subscription_ids_json, daily_risk_control_json, schedule_json, created_at, updated_at FROM auto_trigger_rules WHERE id IN (4,5,6,7,8)")
    out = []
    for r in rows:
        out.append({
            "id": r["id"], "name": r["name"], "status": r["status"],
            "subscription_ids": r["subscription_ids_json"],
            "daily_risk_control": r["daily_risk_control_json"],
            "schedule": r["schedule_json"],
            "created_at": r["created_at"], "updated_at": r["updated_at"],
        })
    write_csv("rules_config.csv", out,
              ["id", "name", "status", "subscription_ids", "daily_risk_control", "schedule", "created_at", "updated_at"])


def extract_rule_daily_stats(conn):
    rows = q(conn, "SELECT rule_id, stat_date, status, stopped_reason, stopped_at, profit_amount, loss_amount, net_profit, settled_event_count, hit_count, miss_count, refund_count, updated_at FROM auto_trigger_rule_daily_stats WHERE rule_id IN (4,5,6,7,8) AND stat_date >= '2026-09-01' ORDER BY rule_id, stat_date")
    write_csv("rule_daily_stats.csv", [dict(r) for r in rows], [d[0] for d in conn.execute("SELECT * FROM auto_trigger_rule_daily_stats LIMIT 0").description] if False else [k for k in rows[0].keys()] if rows else ["rule_id", "stat_date"])


def extract_ledger(conn):
    """注级账本：每条结算事件 + 开奖快照关键字段。"""
    rows = q(conn, """
        SELECT p.id, p.auto_trigger_rule_id AS rule_id, p.subscription_id, p.auto_trigger_stat_date AS stat_date,
               p.settled_at, p.issue_no, p.progression_step, p.stake_amount,
               p.resolved_result_type, p.profit_delta, p.loss_delta, p.net_delta,
               p.leg_key, p.auto_trigger_route_id AS route_id,
               p.result_context_json, p.settlement_snapshot_json, s.bet_value, s.published_at
        FROM subscription_progression_events p
        LEFT JOIN normalized_signals s ON s.id = p.signal_id
        WHERE p.status='settled' AND p.auto_trigger_rule_id IN (4,5,6,7,8)
          AND p.auto_trigger_stat_date >= '2026-09-15'
        ORDER BY p.auto_trigger_rule_id, p.settled_at, p.id
    """)
    out = []
    for r in rows:
        ctx = {}
        try:
            ctx = json.loads(r["result_context_json"] or "{}")
        except (ValueError, TypeError):
            ctx = {}
        snap = ctx.get("draw_snapshot") or {}
        flags = snap.get("special_flags") or {}
        out.append({
            "id": r["id"], "rule_id": r["rule_id"], "subscription_id": r["subscription_id"],
            "stat_date": r["stat_date"], "settled_at": r["settled_at"], "issue_no": r["issue_no"],
            "progression_step": r["progression_step"], "stake_amount": r["stake_amount"],
            "result_type": r["resolved_result_type"],
            "profit_delta": r["profit_delta"], "loss_delta": r["loss_delta"], "net_delta": r["net_delta"],
            "leg_key": r["leg_key"], "route_id": r["route_id"],
            "refund_reason": ctx.get("refund_reason") or "",
            "metric": ctx.get("metric") or "",
            "predicted_value": ctx.get("predicted_value") or "",
            "actual_value": ctx.get("actual_value") or "",
            "sum_value": snap.get("sum_value", ""),
            "big_small": snap.get("big_small", ""), "odd_even": snap.get("odd_even", ""),
            "combo": snap.get("combo", ""), "triplet": json.dumps(snap.get("triplet"), ensure_ascii=False),
            "is_special_sum": int(bool(flags.get("is_special_sum"))),
            "is_pair": int(bool(flags.get("is_pair"))),
            "is_straight": int(bool(flags.get("is_straight"))),
            "is_baozi": int(bool(flags.get("is_baozi"))),
            "is_extreme_sum": int(bool(flags.get("is_extreme_sum"))),
            "is_wraparound_straight": int(bool(flags.get("is_wraparound_straight"))),
            "has_abc_zero_or_nine": int(bool(flags.get("has_abc_zero_or_nine"))),
            "bet_value": r["bet_value"] or "", "published_at": r["published_at"] or "",
            "open_time": snap.get("open_time", ""),
        })
    fields = list(out[0].keys()) if out else ["id"]
    write_csv("ledger.csv", out, fields)


def extract_runs(conn):
    rows = q(conn, "SELECT rule_id, stat_date, window_id, status, stop_reason, started_at, stopped_at, started_issue_no FROM auto_trigger_rule_runs WHERE rule_id IN (4,5,6,7,8) AND stat_date >= '2026-09-15' ORDER BY rule_id, stat_date, started_at")
    write_csv("rule_runs.csv", [dict(r) for r in rows], [k for k in rows[0].keys()] if rows else ["rule_id"])


def extract_trigger_events(conn):
    rows = q(conn, "SELECT rule_id, subscription_id, latest_issue_no, status, reason, created_at FROM auto_trigger_events WHERE rule_id IN (4,5,6,7,8) AND created_at >= '2026-09-15' ORDER BY rule_id, created_at")
    write_csv("trigger_events.csv", [dict(r) for r in rows], [k for k in rows[0].keys()] if rows else ["rule_id"])


def extract_signals(conn):
    rows = q(conn, "SELECT id, source_id, lottery_type, issue_no, bet_type, bet_value, confidence, status, published_at, dispatch_blocked_at, dispatch_block_reason FROM normalized_signals WHERE source_id IN (8, 14, 7, 16) AND published_at >= '2026-09-15' ORDER BY source_id, CAST(issue_no AS INTEGER)")
    write_csv("signals.csv", [dict(r) for r in rows], [k for k in rows[0].keys()] if rows else ["id"])


def extract_subscriptions(conn):
    rows = q(conn, "SELECT id, source_id, name, status, strategy_json FROM user_subscriptions")
    out = []
    for r in rows:
        out.append({"id": r["id"], "source_id": r["source_id"], "name": r["name"], "status": r["status"],
                    "strategy": (r["strategy_json"] or "")[:2000]})
    write_csv("subscriptions.csv", out, ["id", "source_id", "name", "status", "strategy"])


def extract_draws(conn2):
    rows = q(conn2, "SELECT issue_no, draw_date, open_time, result_number, result_number_text, big_small, odd_even, combo, source_payload FROM lottery_draws WHERE lottery_type='pc28' AND draw_date >= '2026-09-01' ORDER BY CAST(issue_no AS INTEGER)")
    out = []
    for r in rows:
        payload = {}
        try:
            payload = json.loads(r["source_payload"] or "{}")
        except (ValueError, TypeError):
            payload = {}
        out.append({
            "issue_no": r["issue_no"], "draw_date": r["draw_date"], "open_time": r["open_time"],
            "result_number": r["result_number"], "result_number_text": r["result_number_text"],
            "big_small": r["big_small"], "odd_even": r["odd_even"], "combo": r["combo"],
            "triplet": str(payload.get("number") or "").replace("+", ","),
        })
    write_csv("draws.csv", out, ["issue_no", "draw_date", "open_time", "result_number",
                                 "result_number_text", "big_small", "odd_even", "combo", "triplet"])


def main():
    conn = connect(TOUZHU_DB)
    conn2 = connect(PREDICTOR_DB)
    extract_rules(conn)
    extract_rule_daily_stats(conn)
    extract_ledger(conn)
    extract_runs(conn)
    extract_trigger_events(conn)
    extract_signals(conn)
    extract_subscriptions(conn)
    extract_draws(conn2)
    conn.close()
    conn2.close()
    print("done")


if __name__ == "__main__":
    main()
