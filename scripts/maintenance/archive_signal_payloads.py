#!/usr/bin/env python3
"""normalized_signals / source_raw_items 载荷瘦身维护。

背景：这两张表没有清理机制、默认累积（2026-11 时合计 ~1.9GB）。
行本身必须保留（去重幂等、账本外键、UI 历史），但行内的载荷正文在
N 天后只剩归档价值。

三步：
  1. --archive  把 cutoff 之前的载荷导出为 gzip CSV（先归档后动手）；
  2. --apply    分批把 cutoff 之前的载荷置空（保留全部行与元数据），
     每批短事务，间隔 sleep，避免阻塞线上写入（应用 busy_timeout=30s）；
  3. --vacuum   VACUUM 回收磁盘（独占锁数分钟，务必低峰期执行）。

安全设计：
  - 默认 dry-run，只打印将影响的行数与可释放字节数；
  - --apply 前必须存在对应表的成功归档（除非 --force）；
  - 归档后校验行数一致才允许瘦身；
  - 不删除任何行，只清空两个载荷列，所有外键/去重/按 id 回查不受影响。
"""
import argparse
import csv
import gzip
import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone

DEFAULT_DB = "/www/wwwroot/pc28touzhu/pc28touzhu.db"
DEFAULT_ARCHIVE_DIR = "/www/wwwroot/pc28touzhu/backups/payload_archive"
BATCH_SIZE = 5000
BATCH_SLEEP = 0.25

NORMALIZED_COLUMNS = ["id", "source_id", "issue_no", "bet_type", "bet_value",
                      "published_at", "normalized_payload", "dispatch_block_verdict_json"]
RAW_COLUMNS = ["id", "source_id", "external_item_id", "issue_no", "published_at",
               "raw_payload", "created_at"]


def connect(db_path):
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


def cutoff_iso(days):
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace(
        "+00:00", "Z") if False else (datetime.now(timezone.utc) - timedelta(days=days)
                                      ).strftime("%Y-%m-%dT%H:%M:%SZ")


def stats(conn, cutoff, table, date_col, payload_cols):
    """分片聚合（避免长读事务阻塞线上写入）。"""
    cond = " OR ".join("COALESCE(%s,'') != ''" % c for c in payload_cols)
    len_sum = "COALESCE(SUM(%s),0)" % " + ".join(
        "LENGTH(COALESCE(%s,''))" % c for c in payload_cols)
    lo = conn.execute("SELECT MIN(id) FROM %s WHERE %s < ?" % (table, date_col), (cutoff,)).fetchone()[0]
    hi = conn.execute("SELECT MAX(id) FROM %s WHERE %s < ?" % (table, date_col), (cutoff,)).fetchone()[0]
    if lo is None:
        return 0, 0
    total_n = 0
    total_b = 0
    window = 200000
    cur = lo
    while cur <= hi:
        agg = conn.execute(
            "SELECT COUNT(1) AS n, %s AS b FROM %s "
            "WHERE id >= ? AND id < ? AND %s < ? AND (%s)"
            % (len_sum, table, date_col, cond),
            (cur, cur + window, cutoff),
        ).fetchone()
        total_n += agg["n"]
        total_b += agg["b"]
        conn.commit()
        cur += window
        time.sleep(0.2)
    return total_n, total_b


def archive_table(conn, cutoff, table, date_col, columns, out_path, batch_sleep=0.25):
    """分片读 + 每片短事务（DELETE 日志模式下长读会阻塞线上写入）。"""
    sel = ", ".join("COALESCE(%s,'') AS %s" % (c, c) for c in columns)
    n = conn.execute("SELECT COUNT(1) FROM %s WHERE %s < ?" % (table, date_col), (cutoff,)).fetchone()[0]
    lo = conn.execute("SELECT MIN(id) FROM %s WHERE %s < ?" % (table, date_col), (cutoff,)).fetchone()[0]
    hi = conn.execute("SELECT MAX(id) FROM %s WHERE %s < ?" % (table, date_col), (cutoff,)).fetchone()[0]
    written = 0
    chunk = 20000
    if lo is None:
        return 0, 0
    with gzip.open(out_path, "wt", encoding="utf-8", newline="", compresslevel=6) as gz:
        writer = csv.writer(gz)
        writer.writerow(columns)
        cur_id = lo
        while cur_id <= hi:
            rows = conn.execute(
                "SELECT %s FROM %s WHERE id >= ? AND id < ? AND %s < ? ORDER BY id"
                % (sel, table, date_col), (cur_id, cur_id + chunk, cutoff),
            ).fetchall()
            for row in rows:
                writer.writerow([row[c] for c in columns])
                written += 1
            conn.commit()
            cur_id += chunk
            time.sleep(batch_sleep)
    if written != n:
        raise RuntimeError("归档行数不一致 %s: 期望 %d 实际 %d" % (table, n, written))
    return written, os.path.getsize(out_path)


def slim_table(conn, cutoff, table, date_col, payload_cols, batch_sleep):
    total_freed = 0
    total_rows = 0
    lo = conn.execute("SELECT MIN(id) FROM %s WHERE %s < ?" % (table, date_col), (cutoff,)).fetchone()[0]
    hi = conn.execute("SELECT MAX(id) FROM %s WHERE %s < ?" % (table, date_col), (cutoff,)).fetchone()[0]
    if lo is None:
        return 0, 0
    cur_id = lo
    sets = ", ".join("%s=''" % c for c in payload_cols)
    cond = " OR ".join("COALESCE(%s,'') != ''" % c for c in payload_cols)
    while cur_id <= hi:
        cur = conn.execute(
            "UPDATE %s SET %s WHERE id >= ? AND id < ? AND %s < ? AND (%s)"
            % (table, sets, date_col, cond),
            (cur_id, cur_id + BATCH_SIZE, cutoff),
        )
        conn.commit()
        total_rows += cur.rowcount or 0
        cur_id += BATCH_SIZE
        time.sleep(batch_sleep)
    # 释放字节数按行统计
    return total_rows, -1


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--days", type=int, default=90, help="保留最近 N 天的载荷（默认90）")
    ap.add_argument("--archive-dir", default=DEFAULT_ARCHIVE_DIR)
    ap.add_argument("--archive", action="store_true", help="导出 cutoff 前载荷到 gzip CSV")
    ap.add_argument("--apply", action="store_true", help="执行瘦身（需先成功归档）")
    ap.add_argument("--force", action="store_true", help="跳过归档校验直接瘦身（不建议）")
    ap.add_argument("--vacuum", action="store_true", help="VACUUM 回收磁盘（独占锁数分钟，低峰期执行）")
    ap.add_argument("--batch-sleep", type=float, default=BATCH_SLEEP)
    args = ap.parse_args()

    cutoff = cutoff_iso(args.days)
    stamp = cutoff[:10].replace("-", "")
    conn = connect(args.db)
    print("cutoff = %s (%d 天前)，库 = %s" % (cutoff, args.days, args.db))
    for table, date_col, cols in (
        ("normalized_signals", "published_at", ["normalized_payload", "dispatch_block_verdict_json"]),
        ("source_raw_items", "created_at", ["raw_payload"]),
    ):
        n, b = stats(conn, cutoff, table, date_col, cols)
        print("  %-22s 将影响 %8d 行，可释放约 %.1f MB" % (table, n, b / 1048576))

    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
    page_size = conn.execute("PRAGMA page_size").fetchone()[0]
    print("  文件大小 %.2f GB，空闲页 %.2f GB（VACUUM 可回收）" % (
        page_count * page_size / 1073741824, freelist * page_size / 1073741824))

    archive_meta = {}
    if args.archive:
        os.makedirs(args.archive_dir, exist_ok=True)
        jobs = [
            ("normalized_signals", "published_at", NORMALIZED_COLUMNS,
             os.path.join(args.archive_dir, "normalized_signals_payload_upto_%s.csv.gz" % stamp)),
            ("source_raw_items", "created_at", RAW_COLUMNS,
             os.path.join(args.archive_dir, "source_raw_items_payload_upto_%s.csv.gz" % stamp)),
        ]
        for table, date_col, cols, path in jobs:
            n, size = archive_table(conn, cutoff, table, date_col, cols, path)
            archive_meta[table] = path
            print("归档 %s：%d 行 → %s（%.1f MB）" % (table, n, path, size / 1048576))

    if args.apply:
        if not args.force:
            for table, date_col, cols in (
                ("normalized_signals", "published_at", ["normalized_payload", "dispatch_block_verdict_json"]),
                ("source_raw_items", "created_at", ["raw_payload"]),
            ):
                n_before, _ = stats(conn, cutoff, table, date_col, cols)
                path = archive_meta.get(table) or (
                    os.path.join(args.archive_dir, "*%s*" % table))
                import glob
                hits = glob.glob(os.path.join(args.archive_dir, "%s_payload_upto_%s.csv.gz" % (table, stamp)))
                if not hits:
                    raise SystemExit("缺少 %s 的归档文件，拒绝瘦身（--force 可跳过）" % table)
        for table, date_col, cols in (
            ("normalized_signals", "published_at", ["normalized_payload", "dispatch_block_verdict_json"]),
            ("source_raw_items", "created_at", ["raw_payload"]),
        ):
            t0 = time.time()
            rows, _ = slim_table(conn, cutoff, table, date_col, cols, args.batch_sleep)
            print("瘦身 %s：%d 行，耗时 %.0f 秒" % (table, rows, time.time() - t0))

    if args.vacuum:
        print("开始 VACUUM（独占锁，期间线上写入会失败，预计数分钟）...")
        t0 = time.time()
        conn.execute("VACUUM")
        print("VACUUM 完成，耗时 %.0f 秒" % (time.time() - t0))

    page_count = conn.execute("PRAGMA page_count").fetchone()[0]
    freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
    print("结束：文件 %.2f GB，空闲页 %.2f GB" % (
        page_count * page_size / 1073741824, freelist * page_size / 1073741824))
    conn.close()


if __name__ == "__main__":
    main()
