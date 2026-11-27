from __future__ import annotations

import threading
import asyncio
import copy
import json
import logging
import os
import sqlite3
import time
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Optional

from ..domain.betting_protocol import normalize_protocol


def delivery_identity(job):
    account = job.telegram_account
    return {'job_id': job.job_id, 'signal_id': job.signal_id, 'issue_no': job.issue_no,
            'text': job.message_text, 'target_key': job.target.key,
            'telegram_account_id': account.id if account else None,
            'session_path': account.session_path if account else None,
            'protocol': normalize_protocol((job.target.metadata or {}).get('betting_protocol'))}


class ExecutorStateStore:
    def __init__(self, path=None):
        self._records: Dict[str, Dict[str, Optional[str]]] = {}
        self._attempts: Dict[str, int] = {}
        self._lock = threading.RLock()
        self._path = str(path) if path else None
        self._reconciliation_thread = None
        self._next_reconciliation_at = 0.0
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
                'delivery_identity': delivery_identity(job),
                'executed_at':datetime.now(timezone.utc).isoformat()}
            self._save(job.idempotency_key)

    def store_transport_evidence(self, job, evidence):
        """Record the RPC result before waiting for a separate business reply."""
        with self._lock:
            record = self._records.get(job.idempotency_key) or {}
            if record.get('attempt_token') != job.attempt_token or str(record.get('job_id')) != job.job_id:
                raise ValueError('transport evidence does not belong to the prepared attempt')
            record.update(transport_evidence=copy.deepcopy(evidence),
                          remote_message_id=str(evidence.get('message_id') or ''),
                          delivery_identity=delivery_identity(job))
            self._records[job.idempotency_key] = record
            self._save(job.idempotency_key)

    def store_result(self, job, payload):
        with self._lock:
            previous = self._records.get(job.idempotency_key) or {}
            self._records[job.idempotency_key]={
                **{key: previous[key] for key in ('transport_evidence', 'delivery_identity', 'attempt_token') if key in previous},
                **payload, 'job_id':job.job_id, 'pending_report':True,
                'delivery_identity': previous.get('delivery_identity') or delivery_identity(job),
            }
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
            previous = self._records.get(idempotency_key) or {}
            self._records[idempotency_key] = {
                **{key: previous[key] for key in ('transport_evidence', 'delivery_identity', 'attempt_token') if key in previous},
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
            return copy.deepcopy(record) if record else None

    def start_reconciliation(self, callback):
        """At most one best-effort read worker; never wait in the send loop."""
        with self._lock:
            if (self._reconciliation_thread and self._reconciliation_thread.is_alive()) or time.monotonic() < self._next_reconciliation_at:
                return False
            self._next_reconciliation_at = time.monotonic() + 5

            def run():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    callback()
                except Exception as exc:
                    # The platform retains the lease/evidence and retries later.
                    # A failed history read must not terminate current sends.
                    logging.getLogger(__name__).warning('Reconciliation cycle failed: %s', type(exc).__name__)
                finally:
                    loop.close()
                    asyncio.set_event_loop(None)

            self._reconciliation_thread = threading.Thread(target=run, name='pc28-reconcile', daemon=True)
            self._reconciliation_thread.start()
            return True
