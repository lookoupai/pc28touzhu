from __future__ import annotations

import contextlib
import gzip
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from pc28touzhu.executor.db_repository import DatabaseRepository


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/maintenance/archive_signal_payloads.py"
spec = importlib.util.spec_from_file_location("payload_maintenance", SCRIPT)
maintenance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(maintenance)
OLD = "2020-01-01T00:00:00Z"
CUTOFF = "2021-01-01T00:00:00Z"
RECENT = "2022-01-01T00:00:00Z"


class PayloadMaintenanceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.db = self.root / "test.db"
        DatabaseRepository(str(self.db)).initialize_database()
        self.conn = maintenance.connect(self.db)
        self.addCleanup(self.conn.close)
        maintenance.prepare_indexes(self.conn)
        self.archives = self.root / "archives"

    def signal(self, *, payload='{"message":"原文\\n第二行"}', published=OLD, created=OLD, raw_id=None):
        return self.conn.execute(
            "INSERT INTO normalized_signals(source_id, source_raw_item_id, lottery_type, issue_no, bet_type, "
            "bet_value, normalized_payload, published_at, created_at) VALUES(1, ?, 'pc28', 'test', 'odd_even', '单', ?, ?, ?)",
            (raw_id, payload, published, created),
        ).lastrowid

    def raw(self, *, status="parsed"):
        return self.conn.execute(
            "INSERT INTO source_raw_items(source_id, raw_payload, parse_status, created_at, published_at) "
            "VALUES(1, ?, ?, ?, ?)", ('{"signals":[]}', status, OLD, OLD),
        ).lastrowid

    def row(self, row_id, table="normalized_signals"):
        return dict(self.conn.execute("SELECT * FROM %s WHERE id = ?" % table, (row_id,)).fetchone())

    def archive(self, ids, table="normalized_signals"):
        return maintenance.archive_batch(self.conn, self.archives, table, CUTOFF,
                                         [self.row(i, table) for i in ids])

    def run_job(self, **kwargs):
        with contextlib.redirect_stdout(io.StringIO()):
            return maintenance.run_maintenance(self.conn, self.archives, cutoff=CUTOFF,
                                               archive=True, apply=True, batch_sleep=0, **kwargs)

    def add_event(self, signal_id, status="pending"):
        self.conn.execute("INSERT INTO subscription_progression_events(subscription_id, user_id, signal_id, status) VALUES(1,1,?,?)",
                          (signal_id, status))

    def add_job(self, signal_id, status="pending"):
        self.conn.execute("INSERT INTO execution_jobs(user_id, signal_id, delivery_target_id, executor_type, idempotency_key, "
                          "execute_after, expire_at, status) VALUES(1,?,1,'telegram',?, ?, ?, ?)",
                          (signal_id, str(signal_id), OLD, RECENT, status))

    def test_archives_only_expired_payloads_and_preserves_metadata(self):
        raw_id = self.raw()
        old = self.signal(raw_id=raw_id)
        recent = self.signal(published=RECENT)
        backfill = self.signal(created=RECENT)
        original = self.row(old)
        result = self.run_job(batch_size=1)
        self.assertEqual(result["cleared"], 2)
        expected = {**original, "normalized_payload": "", "dispatch_block_verdict_json": ""}
        self.assertEqual(self.row(old), expected)
        self.assertTrue(self.row(recent)["normalized_payload"])
        self.assertTrue(self.row(backfill)["normalized_payload"])
        archive = next((self.archives / "complete").glob("normalized_signals*.gz"))
        header, rows = maintenance.read_archive(self.conn, archive)
        self.assertEqual(rows, [original])
        self.assertEqual(header["cutoff"], CUTOFF)

    def test_repeated_run_does_not_overwrite_or_rearchive_empty_rows(self):
        self.signal()
        self.archives.mkdir()
        legacy = self.archives / "normalized_signals_payload_upto_20260816.csv.gz"
        legacy.write_bytes(b"original legacy archive")
        self.run_job()
        before = {p.name: p.read_bytes() for p in (self.archives / "complete").iterdir()}
        result = self.run_job()
        self.assertEqual(result["archived"], 0)
        self.assertEqual(before, {p.name: p.read_bytes() for p in (self.archives / "complete").iterdir()})
        self.assertEqual(legacy.read_bytes(), b"original legacy archive")
        self.signal()
        self.run_job()
        self.assertEqual(len(list((self.archives / "complete").glob("*.gz"))), 2)

    def test_late_backfill_and_changed_content_are_not_cleared_by_old_archive(self):
        unchanged, changed = self.signal(), self.signal()
        path = self.archive([unchanged, changed])
        late = self.signal()
        self.conn.execute("UPDATE normalized_signals SET normalized_payload = 'updated' WHERE id = ?", (changed,))
        result = maintenance.apply_batch(self.conn, path)
        self.assertEqual(result["cleared"], 1)
        self.assertEqual(result["changed"], 1)
        self.assertEqual(self.row(changed)["normalized_payload"], "updated")
        self.assertTrue(self.row(late)["normalized_payload"])

    def test_pending_events_jobs_runs_and_unparsed_raw_items_are_protected(self):
        protected = []
        for status in ("pending", "placed", "unknown_future_status"):
            signal = self.signal()
            self.add_event(signal, status)
            protected.append(signal)
        for status in ("pending", "failed"):
            signal = self.signal()
            self.add_job(signal, status)
            protected.append(signal)
        for table, extra_cols, extra_values in (
            ("subscription_runtime_runs", "", ""),
            ("auto_trigger_route_subscription_runtime_runs", ",route_id,rule_id", ",1,1"),
        ):
            signal = self.signal()
            self.conn.execute("INSERT INTO %s(subscription_id,user_id,started_signal_id,started_at%s) VALUES(1,1,?,?%s)"
                              % (table, extra_cols, extra_values), (signal, OLD))
            protected.append(signal)
        pending_raw = self.raw(status="pending")
        failed_raw = self.raw(status="failed")
        linked_raw = self.raw()
        child = self.signal(raw_id=linked_raw)
        self.add_event(child)
        protected.append(child)
        terminal = self.signal()
        self.add_event(terminal, "settled")
        self.add_job(terminal, "delivered")
        self.run_job()
        self.assertEqual(self.row(terminal)["normalized_payload"], "")
        for signal in protected:
            self.assertTrue(self.row(signal)["normalized_payload"])
        for raw in (pending_raw, failed_raw, linked_raw):
            self.assertTrue(self.row(raw, "source_raw_items")["raw_payload"])

    def test_new_business_reference_after_archiving_is_rechecked(self):
        raw = self.raw()
        signal = self.signal(raw_id=raw)
        signal_path = self.archive([signal])
        raw_path = self.archive([raw], "source_raw_items")
        self.add_event(signal)
        self.assertEqual(maintenance.apply_batch(self.conn, signal_path)["protected"], 1)
        self.assertEqual(maintenance.apply_batch(self.conn, raw_path)["protected"], 1)

    def test_restart_resumes_archive_and_already_committed_batch_safely(self):
        first, second = self.signal(), self.signal()
        pending = self.archive([first])
        committed = self.archive([second])
        saved_bytes = committed.read_bytes()
        maintenance.apply_batch(self.conn, committed)  # Simulate crash before moving file.
        result = self.run_job()
        self.assertEqual(result["resumed_batches"], 2)
        self.assertEqual(result["already_empty"], 1)
        self.assertEqual(result["cleared"], 1)
        self.assertFalse(pending.exists())
        self.assertEqual((self.archives / "complete" / committed.name).read_bytes(), saved_bytes)

    def test_truncated_or_tampered_archive_never_changes_database(self):
        signal = self.signal()
        for corrupt in ("gzip", "hash", "count"):
            with self.subTest(corrupt=corrupt):
                path = self.archive([signal])
                if corrupt == "gzip":
                    path.write_bytes(path.read_bytes()[:-8])
                else:
                    lines = gzip.decompress(path.read_bytes()).splitlines(keepends=True)
                    footer = json.loads(lines[-1])
                    footer["sha256" if corrupt == "hash" else "rows"] = "invalid"
                    lines[-1] = maintenance.json_line(footer)
                    path.write_bytes(gzip.compress(b"".join(lines)))
                with self.assertRaises((ValueError, OSError, EOFError)):
                    maintenance.apply_batch(self.conn, path)
                self.assertTrue(self.row(signal)["normalized_payload"])

    def test_archive_is_bound_to_database_and_current_retention(self):
        signal = self.signal()
        path = self.archive([signal])
        other_db = self.root / "other.db"
        DatabaseRepository(str(other_db)).initialize_database()
        with contextlib.closing(maintenance.connect(other_db)) as other:
            with self.assertRaisesRegex(ValueError, "归属"):
                maintenance.apply_batch(other, path)
        result = maintenance.apply_batch(self.conn, path, cutoff="2019-01-01T00:00:00Z")
        self.assertEqual(result["protected"], 1)
        self.assertTrue(self.row(signal)["normalized_payload"])

    def test_partial_database_failure_rolls_back_whole_batch(self):
        first, second = self.signal(), self.signal()
        path = self.archive([first, second])
        self.conn.execute("CREATE TRIGGER fail_update BEFORE UPDATE ON normalized_signals WHEN old.id=%d "
                          "BEGIN SELECT RAISE(ABORT, 'test interruption'); END" % second)
        with self.assertRaises(sqlite3.IntegrityError):
            maintenance.apply_batch(self.conn, path)
        self.assertTrue(self.row(first)["normalized_payload"])
        self.assertTrue(self.row(second)["normalized_payload"])
        self.assertTrue(path.exists())

    def test_publication_never_overwrites_existing_archive(self):
        temporary, destination = self.root / "temp", self.root / "final"
        temporary.write_bytes(b"new")
        destination.write_bytes(b"old")
        with self.assertRaises(FileExistsError):
            maintenance.publish_file(temporary, destination)
        self.assertEqual(destination.read_bytes(), b"old")

    def test_database_lock_prevents_overlapping_maintenance(self):
        with maintenance.maintenance_lock(self.db):
            with self.assertRaisesRegex(RuntimeError, "已有"):
                with maintenance.maintenance_lock(self.db):
                    self.fail("second lock must not succeed")
        with maintenance.maintenance_lock(self.db):
            pass

    def test_legacy_archive_is_not_authorization_to_apply(self):
        signal = self.signal()
        self.archives.mkdir()
        (self.archives / "normalized_signals_payload_upto_20260816.csv.gz").write_bytes(b"not gzip")
        with contextlib.redirect_stdout(io.StringIO()):
            result = maintenance.run_maintenance(self.conn, self.archives, cutoff=CUTOFF, apply=True)
        self.assertEqual(result["cleared"], 0)
        self.assertTrue(self.row(signal)["normalized_payload"])

    def test_nonexistent_database_is_not_created(self):
        missing = self.root / "typo.db"
        with self.assertRaises(FileNotFoundError):
            maintenance.connect(missing)
        self.assertFalse(missing.exists())

    def test_cli_defaults_to_read_only_and_rejects_unsafe_flags(self):
        self.signal()
        before = hashlib.sha256(self.db.read_bytes()).digest()
        original_umask = os.umask(0o022)
        try:
            with contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(maintenance.main(["--db", str(self.db)]), 0)
            self.assertEqual(hashlib.sha256(self.db.read_bytes()).digest(), before)
            for flags in (["--days", "0"], ["--force"], ["--batch-size", "501"],
                          ["--batch-sleep", "nan"], ["--vacuum", "--apply"]):
                with self.subTest(flags=flags), contextlib.redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
                    maintenance.main(flags)
        finally:
            os.umask(original_umask)


if __name__ == "__main__":
    unittest.main()
