from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from .models import ExecutorJob
from .state import ExecutorStateStore


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def should_send_job(
    job: ExecutorJob,
    *,
    state: Optional[ExecutorStateStore] = None,
    reference_time: Optional[datetime] = None,
) -> bool:
    now = reference_time or now_utc()
    if now < job.execute_after:
        return False
    if now >= job.expire_at:
        return False
    if state and state.has_delivered(job.idempotency_key):
        return False
    return True
