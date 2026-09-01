from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
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


def _process_job(
    *,
    raw: Any,
    api_client: Any,
    state_store: ExecutorStateStore,
    executor_id: str,
    message_sender: TextMessageSender,
) -> str:
    job = ExecutorJob.from_payload(raw)
    now = datetime.now(timezone.utc)
    if now < job.execute_after:
        return "skipped"

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
        return "expired"

    if state_store.has_delivered(job.idempotency_key):
        if _replay_delivered_attempt(api_client=api_client, state_store=state_store, job=job):
            return "replayed"
        return "skipped"

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
        status = "delivered"
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
        status = "failed"

    _record_and_report_result(
        api_client=api_client,
        state_store=state_store,
        executor_id=executor_id,
        job=job,
        result=result,
    )
    return status


def _cycle_result(heartbeat: Dict[str, Any], raw_jobs: list[Any], statuses: list[str]) -> Dict[str, Any]:
    return {
        "heartbeat": heartbeat,
        "pulled_count": len(raw_jobs),
        "delivered_count": statuses.count("delivered"),
        "failed_count": statuses.count("failed"),
        "expired_count": statuses.count("expired"),
        "skipped_count": statuses.count("skipped"),
        "replayed_count": statuses.count("replayed"),
    }


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
    statuses = [
        _process_job(
            raw=raw,
            api_client=api_client,
            state_store=state_store,
            executor_id=executor_id,
            message_sender=message_sender,
        )
        for raw in raw_jobs
    ]
    return _cycle_result(heartbeat, raw_jobs, statuses)


def run_executor_cycle_concurrent(
    *,
    api_client: Any,
    message_sender: TextMessageSender,
    state_store: ExecutorStateStore,
    executor_id: str,
    limit: int,
    max_concurrent: int = 4,
    version: str,
    capabilities: Dict[str, Any],
) -> Dict[str, Any]:
    """Process different accounts concurrently; the sender serializes each account."""
    heartbeat = api_client.heartbeat(version=version, capabilities=capabilities)
    raw_jobs = api_client.pull_jobs(limit=limit)
    worker_count = max(1, min(int(max_concurrent or 1), len(raw_jobs) or 1))
    statuses: list[str] = []
    with ThreadPoolExecutor(max_workers=worker_count, thread_name_prefix="pc28-send") as pool:
        futures = [
            pool.submit(
                _process_job,
                raw=raw,
                api_client=api_client,
                state_store=state_store,
                executor_id=executor_id,
                message_sender=message_sender,
            )
            for raw in raw_jobs
        ]
        for future in as_completed(futures):
            statuses.append(future.result())
    return _cycle_result(heartbeat, raw_jobs, statuses)
