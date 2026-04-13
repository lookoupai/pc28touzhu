from __future__ import annotations

from datetime import datetime, timezone
from typing import Dict, Optional


class ExecutorStateStore:
    def __init__(self):
        self._records: Dict[str, Dict[str, Optional[str]]] = {}
        self._attempts: Dict[str, int] = {}

    def has_delivered(self, idempotency_key: str) -> bool:
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
        return self._attempts.get(idempotency_key, 0) + 1

    def get_record(self, idempotency_key: str) -> Optional[Dict[str, Optional[str]]]:
        return self._records.get(idempotency_key)
