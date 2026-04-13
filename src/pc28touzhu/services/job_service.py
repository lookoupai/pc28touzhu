"""Service helpers for executor-facing APIs."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from pc28touzhu.domain.models import (
    ExecutorHeartbeat,
    JobPullItem,
    JobReportPayload,
    JobTarget,
    StakePlan,
    TelegramAccount,
)
from pc28touzhu.services.protocols import JobRepositoryProtocol


ALLOWED_DELIVERY_STATUSES = {"delivered", "failed", "expired", "skipped"}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_job_pull_items(rows: List[Dict[str, Any]]) -> List[JobPullItem]:
    items = []
    for row in rows:
        stake_plan_payload = row.get("stake_plan") or {}
        target_payload = row.get("target") or {}
        account_payload = row.get("telegram_account") or {}
        items.append(
            JobPullItem(
                job_id=str(row["job_id"]),
                signal_id=str(row["signal_id"]),
                lottery_type=str(row["lottery_type"]),
                issue_no=str(row["issue_no"]),
                bet_type=str(row["bet_type"]),
                bet_value=str(row["bet_value"]),
                message_text=str(row["message_text"]),
                stake_plan=StakePlan(
                    mode=str(stake_plan_payload.get("mode") or "flat"),
                    amount=float(stake_plan_payload.get("amount") or 0),
                    base_stake=float(stake_plan_payload.get("base_stake") or stake_plan_payload.get("amount") or 0),
                    multiplier=float(stake_plan_payload.get("multiplier") or 2),
                    max_steps=int(stake_plan_payload.get("max_steps") or 1),
                    refund_action=str(stake_plan_payload.get("refund_action") or "hold"),
                    cap_action=str(stake_plan_payload.get("cap_action") or "reset"),
                    meta=dict(stake_plan_payload.get("meta") or {}),
                ),
                target=JobTarget(
                    type=str(target_payload.get("type") or "telegram_group"),
                    key=str(target_payload.get("key") or ""),
                    name=str(target_payload.get("name") or ""),
                ),
                telegram_account=(
                    TelegramAccount(
                        id=int(account_payload["id"]) if account_payload.get("id") is not None else None,
                        label=str(account_payload.get("label") or ""),
                        phone=str(account_payload.get("phone") or ""),
                        session_path=str(account_payload.get("session_path") or ""),
                    )
                    if account_payload
                    else None
                ),
                idempotency_key=str(row["idempotency_key"]),
                execute_after=row.get("execute_after"),
                expire_at=row.get("expire_at"),
            )
        )
    return items


def apply_auto_retry_policy(
    repository: JobRepositoryProtocol,
    *,
    max_attempts: int,
    base_delay_seconds: int,
    limit: int = 100,
) -> Dict[str, Any]:
    requeued = repository.requeue_auto_retry_jobs(
        max_attempts=max(1, int(max_attempts or 1)),
        base_delay_seconds=max(5, int(base_delay_seconds or 5)),
        limit=max(1, min(int(limit or 100), 500)),
    )
    return {
        "requeued_count": len(requeued),
        "job_ids": [int(item["job_id"]) for item in requeued],
    }


def pull_jobs(
    repository: JobRepositoryProtocol,
    executor_id: str,
    limit: int = 20,
    *,
    auto_retry_max_attempts: int = 3,
    auto_retry_base_delay_seconds: int = 30,
) -> Dict[str, Any]:
    if not executor_id:
        raise ValueError("缺少执行器标识")

    limit = max(1, min(int(limit or 20), 100))
    retry_result = apply_auto_retry_policy(
        repository,
        max_attempts=auto_retry_max_attempts,
        base_delay_seconds=auto_retry_base_delay_seconds,
        limit=limit * 5,
    )
    rows = repository.pull_ready_jobs(executor_id=executor_id, limit=limit)
    return {
        "items": [item.to_dict() for item in parse_job_pull_items(rows)],
        "auto_retried_count": retry_result["requeued_count"],
        "auto_retried_job_ids": retry_result["job_ids"],
    }


def validate_report_payload(payload: Dict[str, Any]) -> JobReportPayload:
    executor_id = str(payload.get("executor_id") or "").strip()
    if not executor_id:
        raise ValueError("executor_id 不能为空")

    attempt_no = int(payload.get("attempt_no") or 0)
    if attempt_no <= 0:
        raise ValueError("attempt_no 必须大于 0")

    delivery_status = str(payload.get("delivery_status") or "").strip()
    if delivery_status not in ALLOWED_DELIVERY_STATUSES:
        raise ValueError("delivery_status 不合法")

    executed_at = str(payload.get("executed_at") or "").strip() or utc_now_iso()
    raw_result = payload.get("raw_result") or {}
    if not isinstance(raw_result, dict):
        raise ValueError("raw_result 必须为对象")

    remote_message_id = payload.get("remote_message_id")
    if remote_message_id is not None:
        remote_message_id = str(remote_message_id)

    error_message = payload.get("error_message")
    if error_message is not None:
        error_message = str(error_message)

    return JobReportPayload(
        executor_id=executor_id,
        attempt_no=attempt_no,
        delivery_status=delivery_status,
        remote_message_id=remote_message_id,
        executed_at=executed_at,
        raw_result=raw_result,
        error_message=error_message,
    )


def report_job(repository: JobRepositoryProtocol, job_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if not str(job_id or "").strip():
        raise ValueError("job_id 不能为空")

    report = validate_report_payload(payload)
    return repository.report_job_result(
        job_id=str(job_id),
        executor_id=report.executor_id,
        attempt_no=report.attempt_no,
        delivery_status=report.delivery_status,
        remote_message_id=report.remote_message_id,
        executed_at=report.executed_at,
        raw_result=report.raw_result,
        error_message=report.error_message,
    )


def validate_heartbeat_payload(executor_id: str, payload: Dict[str, Any]) -> ExecutorHeartbeat:
    normalized_executor_id = str(executor_id or payload.get("executor_id") or "").strip()
    if not normalized_executor_id:
        raise ValueError("executor_id 不能为空")

    capabilities = payload.get("capabilities") or {}
    if not isinstance(capabilities, dict):
        raise ValueError("capabilities 必须为对象")

    return ExecutorHeartbeat(
        executor_id=normalized_executor_id,
        version=str(payload.get("version") or "").strip(),
        status=str(payload.get("status") or "online").strip() or "online",
        capabilities=capabilities,
        last_seen_at=str(payload.get("last_seen_at") or "").strip() or utc_now_iso(),
    )


def heartbeat_executor(
    repository: JobRepositoryProtocol,
    executor_id: str,
    payload: Dict[str, Any],
) -> Dict[str, Any]:
    heartbeat = validate_heartbeat_payload(executor_id=executor_id, payload=payload)
    return repository.upsert_executor_heartbeat(
        executor_id=heartbeat.executor_id,
        version=heartbeat.version,
        capabilities=heartbeat.capabilities,
        status=heartbeat.status,
        last_seen_at=heartbeat.last_seen_at,
    )
