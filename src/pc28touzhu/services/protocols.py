"""Repository protocols used by the service layer."""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Protocol


class JobRepositoryProtocol(Protocol):
    def requeue_auto_retry_jobs(
        self,
        *,
        max_attempts: int,
        base_delay_seconds: int,
        limit: int,
    ) -> List[Dict[str, Any]]:
        ...

    def pull_ready_jobs(self, executor_id: str, limit: int) -> List[Dict[str, Any]]:
        ...

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
        ...

    def upsert_executor_heartbeat(
        self,
        executor_id: str,
        version: str,
        capabilities: Optional[Dict[str, Any]],
        status: str,
        last_seen_at: str,
    ) -> Dict[str, Any]:
        ...
