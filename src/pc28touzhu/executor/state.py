from __future__ import annotations

import threading
import json
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional


class ExecutorStateStore:
    def __init__(self, path=None):
        self._records: Dict[str, Dict[str, Optional[str]]] = {}
        self._attempts: Dict[str, int] = {}
        self._lock = threading.RLock()
        self._path = str(path) if path else None
        if self._path:
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
            fd = os.open(self._path, os.O_CREAT | os.O_RDWR, 0o600)
            os.close(fd)
            with sqlite3.connect(self._path) as conn:
                conn.execute('CREATE TABLE IF NOT EXISTS send_records(identity TEXT PRIMARY KEY,payload TEXT NOT NULL)')
                for identity, payload in conn.execute('SELECT identity,payload FROM send_records'):
                    record=json.loads(payload);self._records[identity]=record
                    self._attempts[identity]=int(record.get('attempt_no') or 0)

    def _save(self, identity):
        if self._path:
            with sqlite3.connect(self._path) as conn:
                conn.execute('INSERT INTO send_records VALUES (?,?) ON CONFLICT(identity) DO UPDATE SET payload=excluded.payload',
                             (identity,json.dumps(self._records[identity],ensure_ascii=False)))

    def prepare(self, job, executor_id, attempt_no):
        with self._lock:
            self._records[job.idempotency_key]={'delivery_status':'sending','executor_id':executor_id,
                'job_id':job.job_id,'attempt_no':attempt_no,'attempt_token':job.attempt_token,
                'executed_at':datetime.now(timezone.utc).isoformat()}
            self._save(job.idempotency_key)

    def store_result(self, job, payload):
        with self._lock:
            self._records[job.idempotency_key]={**payload,'job_id':job.job_id,'pending_report':True}
            self._save(job.idempotency_key)

    def pending_reports(self):
        with self._lock:
            return [(key,dict(value)) for key,value in self._records.items() if value.get('pending_report')]

    def reported(self, identity):
        with self._lock:
            self._records[identity]['pending_report']=False
            self._save(identity)

    def has_delivered(self, idempotency_key: str) -> bool:
        with self._lock:
            record = self._records.get(idempotency_key)
            return bool(record and record.get("delivery_status") == "delivered")

    def record_attempt(
        self,
        idempotency_key: str,
        delivery_status: str,
        executor_id: str,
        attempt_no: int,
        remote_message_id: Optional[str],
        error_message: Optional[str],
        executed_at: Optional[datetime] = None,
    ) -> None:
        with self._lock:
            self._records[idempotency_key] = {
                "executor_id": executor_id,
                "delivery_status": delivery_status,
                "remote_message_id": remote_message_id,
                "error_message": error_message,
                "executed_at": (
                    executed_at.astimezone(timezone.utc).isoformat()
                    if executed_at
                    else datetime.now(timezone.utc).isoformat()
                ),
                "attempt_no": attempt_no,
            }
            self._attempts[idempotency_key] = max(
                self._attempts.get(idempotency_key, 0), attempt_no
            )

    def next_attempt_no(self, idempotency_key: str) -> int:
        with self._lock:
            next_no = self._attempts.get(idempotency_key, 0) + 1
            self._attempts[idempotency_key] = next_no
            return next_no

    def get_record(self, idempotency_key: str) -> Optional[Dict[str, Optional[str]]]:
        with self._lock:
            record = self._records.get(idempotency_key)
            return dict(record) if record else None
