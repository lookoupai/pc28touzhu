#!/usr/bin/env python3
"""增量归档并清空过期正文，保留记录、关联字段和所有历史归档。

默认只预览；--archive --apply 先持久化、校验每批归档，再清空相同版本的正文。
--apply 只恢复 pending/ 中的新格式归档，不接受旧 CSV 或跳过归档校验。
--vacuum 是独立的人工维护操作，自动任务不执行数据库压缩。
"""
from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from uuid import uuid4

DEFAULT_DB = "/www/wwwroot/pc28touzhu/pc28touzhu.db"
DEFAULT_ARCHIVE_DIR = "/www/wwwroot/pc28touzhu/backups/payload_archive"
FORMAT = "pc28-payload-archive-v2"
MAX_BATCH_SIZE = 500
MAX_ARCHIVE_BYTES = 64 * 1024 * 1024
TABLES = {
    "normalized_signals": ("published_at", ("normalized_payload", "dispatch_block_verdict_json")),
    "source_raw_items": ("created_at", ("raw_payload",)),
}
OPEN_EVENTS = "status NOT IN ('settled', 'void', 'cancelled', 'reset')"
# failed jobs can be retried, so keep their signal payloads conservatively.
OPEN_JOBS = "status NOT IN ('delivered', 'expired', 'skipped', 'cancelled')"
OPEN_RUNS = "status <> 'closed'"


def utc_now():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def cutoff_iso(days):
    if days < 1:
        raise ValueError("保留天数必须大于 0")
    return (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y-%m-%dT%H:%M:%SZ")


def connect(db_path, *, readonly=False):
    path = Path(db_path).resolve(strict=True)
    conn = sqlite3.connect(path.as_uri() + ("?mode=ro" if readonly else "?mode=rw"),
                           uri=True, timeout=5, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    if readonly:
        conn.execute("PRAGMA query_only = ON")
    return conn


def database_identity(conn):
    path = Path(conn.execute("PRAGMA database_list").fetchone()[2]).resolve(strict=True)
    info = path.stat()
    return {"path": str(path), "device": info.st_dev, "inode": info.st_ino}


@contextmanager
def maintenance_lock(db_path):
    # One lock per canonical database path, even with different archive directories.
    path = str(Path(db_path).resolve(strict=True)) + ".payload-maintenance.lock"
    with open(path, "a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("同一数据库已有正文维护任务运行") from None
        try:
            yield
        finally:
            fcntl.flock(lock, fcntl.LOCK_UN)


def has_payload(table, alias=""):
    prefix = alias + "." if alias else ""
    return "(" + " OR ".join("COALESCE(%s%s, '') <> ''" % (prefix, c)
                              for c in TABLES[table][1]) + ")"


def signal_is_idle(alias):
    parts = []
    for table, column, condition in (
        ("subscription_progression_events", "signal_id", OPEN_EVENTS),
        ("execution_jobs", "signal_id", OPEN_JOBS),
        ("subscription_runtime_runs", "started_signal_id", OPEN_RUNS),
        ("auto_trigger_route_subscription_runtime_runs", "started_signal_id", OPEN_RUNS),
    ):
        parts.append("NOT EXISTS (SELECT 1 FROM %s WHERE %s = %s.id AND %s)"
                     % (table, column, alias, condition))
    return " AND ".join(parts)


def eligibility(table):
    date_col = TABLES[table][0]
    clauses = [has_payload(table, "t"), "t.%s < :cutoff" % date_col,
               "julianday(t.%s) < julianday(:cutoff)" % date_col,
               "julianday(t.created_at) < julianday(:cutoff)"]
    if table == "normalized_signals":
        clauses.append(signal_is_idle("t"))
    else:
        clauses.extend([
            "t.parse_status = 'parsed'",
            "NOT EXISTS (SELECT 1 FROM normalized_signals s WHERE s.source_raw_item_id = t.id "
            "AND (NOT (%s) OR julianday(s.created_at) >= julianday(:cutoff)))" % signal_is_idle("s"),
        ])
    return " AND ".join(clauses)


def prepare_indexes(conn):
    """Partial indexes remove emptied rows from future scans. Bound each initial build."""
    definitions = [
        ("payload_signals_due_v2", "normalized_signals", "published_at, id", has_payload("normalized_signals")),
        ("payload_raw_due_v2", "source_raw_items", "created_at, id", has_payload("source_raw_items") + " AND parse_status = 'parsed'"),
        ("payload_open_events_v2", "subscription_progression_events", "signal_id", OPEN_EVENTS),
        ("payload_open_jobs_v2", "execution_jobs", "signal_id", OPEN_JOBS),
        ("payload_open_runs_v2", "subscription_runtime_runs", "started_signal_id", OPEN_RUNS),
        ("payload_open_route_runs_v2", "auto_trigger_route_subscription_runtime_runs", "started_signal_id", OPEN_RUNS),
    ]
    for name, table, columns, condition in definitions:
        deadline = time.monotonic() + 10
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        try:
            conn.execute("CREATE INDEX IF NOT EXISTS %s ON %s(%s) WHERE %s"
                         % (name, table, columns, condition))
        finally:
            conn.set_progress_handler(None, 0)


def candidate_batches(conn, table, cutoff, batch_size=MAX_BATCH_SIZE, max_rows=50000):
    date_col = TABLES[table][0]
    after_date, after_id = "", 0
    remaining = max_rows
    while remaining > 0:
        rows = conn.execute(
            "SELECT t.* FROM %s t WHERE %s AND (t.%s, t.id) > (:after_date, :after_id) "
            "ORDER BY t.%s, t.id LIMIT :limit" % (table, eligibility(table), date_col, date_col),
            {"cutoff": cutoff, "after_date": after_date, "after_id": after_id,
             "limit": min(batch_size, remaining)},
        ).fetchall()
        if not rows:
            return
        batch = [dict(row) for row in rows]
        after_date, after_id = batch[-1][date_col], batch[-1]["id"]
        remaining -= len(batch)
        yield batch


def json_line(value):
    return (json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8")


def sync_directory(path):
    fd = os.open(str(path), os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def durable_directory(path):
    path = Path(path)
    if not path.exists():
        durable_directory(path.parent)
        path.mkdir(mode=0o770)
        sync_directory(path.parent)


def publish_file(temporary, destination):
    """A hard link publishes a complete file atomically and refuses to overwrite."""
    os.link(str(temporary), str(destination))
    sync_directory(destination.parent)
    temporary.unlink()
    sync_directory(temporary.parent)


def archive_batch(conn, archive_dir, table, cutoff, rows):
    if not rows or len(rows) > MAX_BATCH_SIZE:
        raise ValueError("归档批次行数无效")
    pending = Path(archive_dir) / "pending"
    durable_directory(pending)
    batch_id = uuid4().hex
    destination = pending / (table + "_" + batch_id + ".jsonl.gz")
    header = {"format": FORMAT, "batch_id": batch_id, "database": database_identity(conn),
              "table": table, "cutoff": cutoff, "created_at": utc_now(), "columns": sorted(rows[0])}
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(dir=pending, prefix=".partial-", delete=False) as out:
            temporary = Path(out.name)
            # Preserve group access for manual root runs and the www systemd service.
            os.fchmod(out.fileno(), 0o660)
            digest = hashlib.sha256()
            with gzip.GzipFile(fileobj=out, mode="wb", mtime=0) as gz:
                gz.write(json_line(header))
                for row in rows:
                    line = json_line(row)
                    digest.update(line)
                    gz.write(line)
                gz.write(json_line({"rows": len(rows), "sha256": digest.hexdigest()}))
            out.flush()
            os.fsync(out.fileno())
        publish_file(temporary, destination)
    finally:
        if temporary is not None and temporary.exists():
            temporary.unlink()
    return destination


def read_archive(conn, path):
    # Read to EOF before any database mutation: validates gzip CRC and rejects truncation.
    with gzip.open(path, "rb") as archive:
        data = archive.read(MAX_ARCHIVE_BYTES + 1)
    if len(data) > MAX_ARCHIVE_BYTES:
        raise ValueError("归档批次超过安全大小限制")
    lines = data.splitlines(keepends=True)
    if len(lines) < 3 or len(lines) > MAX_BATCH_SIZE + 2:
        raise ValueError("归档结构或行数无效")
    header, footer = json.loads(lines[0]), json.loads(lines[-1])
    if not isinstance(header, dict) or not isinstance(footer, dict):
        raise ValueError("归档头尾格式无效")
    if header.get("format") != FORMAT or header.get("database") != database_identity(conn):
        raise ValueError("归档格式或数据库归属不匹配，拒绝清理")
    table = header.get("table")
    if table not in TABLES:
        raise ValueError("归档表不受支持")
    columns = sorted(row[1] for row in conn.execute("PRAGMA table_info(%s)" % table))
    if header.get("columns") != columns:
        raise ValueError("归档与当前数据库字段不匹配")
    datetime.strptime(header["cutoff"], "%Y-%m-%dT%H:%M:%SZ")
    if header["cutoff"] >= utc_now():
        raise ValueError("归档截止时间必须早于当前时间")
    digest = hashlib.sha256(b"".join(lines[1:-1])).hexdigest()
    if footer != {"rows": len(lines) - 2, "sha256": digest}:
        raise ValueError("归档行数或 SHA256 校验失败")
    rows = [json.loads(line) for line in lines[1:-1]]
    ids = set()
    for row in rows:
        if not isinstance(row, dict) or sorted(row) != columns or type(row.get("id")) is not int or row["id"] <= 0 or row["id"] in ids:
            raise ValueError("归档记录字段或 ID 无效")
        ids.add(row["id"])
    return header, rows


def apply_batch(conn, path, *, cutoff=None):
    header, rows = read_archive(conn, path)
    table = header["table"]
    effective_cutoff = min(header["cutoff"], cutoff or header["cutoff"])
    payload_cols = TABLES[table][1]
    result = {"cleared": 0, "already_empty": 0, "changed": 0, "protected": 0}
    # Archive compression/fsync happens outside this transaction. Recheck both row
    # contents and business state under the same write lock as the UPDATE.
    conn.execute("BEGIN IMMEDIATE")
    try:
        for archived in rows:
            current = conn.execute("SELECT * FROM %s WHERE id = ?" % table, (archived["id"],)).fetchone()
            if current is not None and all(not current[c] for c in payload_cols):
                result["already_empty"] += 1
                continue
            if current is None or dict(current) != archived:
                result["changed"] += 1
                continue
            eligible = conn.execute("SELECT 1 FROM %s t WHERE t.id = :id AND %s" % (table, eligibility(table)),
                                    {"id": archived["id"], "cutoff": effective_cutoff}).fetchone()
            if not eligible:
                result["protected"] += 1
                continue
            conn.execute("UPDATE %s SET %s WHERE id = ?" % (table, ", ".join(c + " = ''" for c in payload_cols)),
                         (archived["id"],))
            result["cleared"] += 1
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return result


def complete_batch(path):
    path = Path(path)
    complete = path.parent.parent / "complete"
    durable_directory(complete)
    destination = complete / path.name
    if destination.exists():
        if not os.path.samefile(path, destination):
            raise ValueError("完成目录存在不同的同名归档，拒绝覆盖")
        path.unlink()  # Recovery after link succeeded but unlink was interrupted.
        sync_directory(path.parent)
    else:
        publish_file(path, destination)


def run_maintenance(conn, archive_dir, *, cutoff, archive=False, apply=False,
                    batch_size=MAX_BATCH_SIZE, max_rows=50000, batch_sleep=0.25):
    summary = {"started_at": utc_now(), "cutoff": cutoff, "archived": 0, "resumed_batches": 0,
               "cleared": 0, "already_empty": 0, "changed": 0, "protected": 0}
    def finish(path, resumed=False):
        started = time.monotonic()
        result = apply_batch(conn, path, cutoff=cutoff)
        complete_batch(path)
        for key, value in result.items():
            summary[key] += value
        summary["resumed_batches"] += int(resumed)
        print(json.dumps({"batch": path.name, "seconds": round(time.monotonic() - started, 3), **result},
                         ensure_ascii=False), flush=True)
        time.sleep(batch_sleep)

    if apply:
        for path in sorted((Path(archive_dir) / "pending").glob("*.jsonl.gz")):
            finish(path, resumed=True)
    if archive:
        for table in TABLES:
            for rows in candidate_batches(conn, table, cutoff, batch_size, max_rows):
                path = archive_batch(conn, archive_dir, table, cutoff, rows)
                read_archive(conn, path)
                summary["archived"] += len(rows)
                if apply:
                    finish(path)
                else:
                    time.sleep(batch_sleep)
    summary["finished_at"] = utc_now()
    return summary


def main(argv=None):
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=DEFAULT_DB)
    ap.add_argument("--days", type=int, default=90)
    ap.add_argument("--archive-dir", default=DEFAULT_ARCHIVE_DIR)
    ap.add_argument("--archive", action="store_true")
    ap.add_argument("--apply", action="store_true", help="恢复 pending/ 中通过校验的批次")
    ap.add_argument("--vacuum", action="store_true", help="独立压缩数据库，需自行安排维护窗口")
    ap.add_argument("--batch-size", type=int, default=MAX_BATCH_SIZE)
    ap.add_argument("--batch-sleep", type=float, default=0.25)
    ap.add_argument("--max-rows", type=int, default=50000, help="每张表本次最多归档行数")
    args = ap.parse_args(argv)
    if args.days < 1 or not 1 <= args.batch_size <= MAX_BATCH_SIZE or args.max_rows < 1 or not math.isfinite(args.batch_sleep) or args.batch_sleep < 0:
        ap.error("days/max-rows 必须为正数，batch-size 为 1–500，batch-sleep 必须非负且有限")
    if args.vacuum and (args.archive or args.apply):
        ap.error("VACUUM 必须单独执行，不能与归档清理合并")
    os.umask(0o007)
    cutoff = cutoff_iso(args.days)
    readonly = not (args.archive or args.apply or args.vacuum)
    conn = connect(args.db, readonly=readonly)
    try:
        if readonly:
            for table in TABLES:
                rows_count, byte_count = 0, 0
                for rows in candidate_batches(conn, table, cutoff, args.batch_size, args.max_rows):
                    rows_count += len(rows)
                    byte_count += sum(len((row[c] or "").encode("utf-8")) for row in rows for c in TABLES[table][1])
                print("预览 %s：可归档 %s 行，正文 %.2f MiB（每表最多预览 %s 行）" %
                      (table, rows_count, byte_count / 1048576, args.max_rows))
            return 0
        with maintenance_lock(args.db):
            if args.vacuum:
                conn.execute("VACUUM")
                print("VACUUM 完成")
                return 0
            prepare_indexes(conn)
            summary = run_maintenance(conn, args.archive_dir, cutoff=cutoff, archive=args.archive, apply=args.apply,
                                      batch_size=args.batch_size, max_rows=args.max_rows, batch_sleep=args.batch_sleep)
            print(json.dumps(summary, ensure_ascii=False), flush=True)
            return 0
    finally:
        conn.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OSError, ValueError, RuntimeError, sqlite3.Error, EOFError) as exc:
        print("正文维护失败，未完成批次将在下次运行时重试：%s" % exc, file=sys.stderr)
        sys.exit(1)
