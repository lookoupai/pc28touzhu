from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Protocol

from .models import ExecutorJob, ExecutorResult
from .state import ExecutorStateStore


class TextMessageSender(Protocol):
    def send_text(self, job: ExecutorJob) -> Dict[str, Any]:
        ...


def _record_and_report_result(
    *,
    api_client: Any,
    state_store: ExecutorStateStore,
    executor_id: str,
    job: ExecutorJob,
    result: ExecutorResult,
) -> None:
    state_store.record_attempt(
        idempotency_key=job.idempotency_key,
        delivery_status=result.delivery_status,
        executor_id=executor_id,
        attempt_no=result.attempt_no,
        remote_message_id=result.remote_message_id,
        error_message=result.error_message,
        executed_at=result.executed_at,
    )
    api_client.report_job(job_id=job.job_id, payload=result.to_payload())


def _replay_delivered_attempt(
    *,
    api_client: Any,
    state_store: ExecutorStateStore,
    job: ExecutorJob,
) -> bool:
    record = state_store.get_record(job.idempotency_key) or {}
    if record.get("delivery_status") != "delivered":
        return False

    api_client.report_job(
        job_id=job.job_id,
        payload={
            "executor_id": record.get("executor_id") or "",
            "attempt_no": int(record.get("attempt_no") or 1),
            "delivery_status": "delivered",
            "executed_at": str(record.get("executed_at") or datetime.now(timezone.utc).isoformat()),
            "remote_message_id": record.get("remote_message_id"),
            "raw_result": {"replayed_from_local_state": True},
            "error_message": record.get("error_message"),
        },
    )
    return True


def run_executor_cycle(
    *,
    api_client: Any,
    message_sender: TextMessageSender,
    state_store: ExecutorStateStore,
    executor_id: str,
    limit: int,
    version: str,
    capabilities: Dict[str, Any],
) -> Dict[str, Any]:
    heartbeat = api_client.heartbeat(version=version, capabilities=capabilities)
    raw_jobs = api_client.pull_jobs(limit=limit)

    delivered_count = 0
    failed_count = 0
    expired_count = 0
    skipped_count = 0
    replayed_count = 0
    for raw in raw_jobs:
        job = ExecutorJob.from_payload(raw)
        now = datetime.now(timezone.utc)
        if now < job.execute_after:
            skipped_count += 1
            continue

        attempt_no = state_store.next_attempt_no(job.idempotency_key)
        if now >= job.expire_at:
            result = ExecutorResult(
                job_id=job.job_id,
                executor_id=executor_id,
                attempt_no=attempt_no,
                delivery_status="expired",
                executed_at=now,
                raw_result={"reason": "expired_before_send"},
                error_message="任务已过期",
            )
            _record_and_report_result(
                api_client=api_client,
                state_store=state_store,
                executor_id=executor_id,
                job=job,
                result=result,
            )
            expired_count += 1
            continue

        if state_store.has_delivered(job.idempotency_key):
            if _replay_delivered_attempt(api_client=api_client, state_store=state_store, job=job):
                replayed_count += 1
                continue
            skipped_count += 1
            continue

        try:
            send_result = message_sender.send_text(job)
            result = ExecutorResult(
                job_id=job.job_id,
                executor_id=executor_id,
                attempt_no=attempt_no,
                delivery_status="delivered",
                executed_at=datetime.now(timezone.utc),
                remote_message_id=str(send_result.get("message_id") or ""),
                raw_result=dict(send_result),
                error_message=None,
            )
            delivered_count += 1
        except Exception as exc:
            result = ExecutorResult(
                job_id=job.job_id,
                executor_id=executor_id,
                attempt_no=attempt_no,
                delivery_status="failed",
                executed_at=datetime.now(timezone.utc),
                raw_result={"exception_type": exc.__class__.__name__},
                error_message=str(exc) or exc.__class__.__name__,
            )
            failed_count += 1

        _record_and_report_result(
            api_client=api_client,
            state_store=state_store,
            executor_id=executor_id,
            job=job,
            result=result,
        )

    return {
        "heartbeat": heartbeat,
        "pulled_count": len(raw_jobs),
        "delivered_count": delivered_count,
        "failed_count": failed_count,
        "expired_count": expired_count,
        "skipped_count": skipped_count,
        "replayed_count": replayed_count,
    }
