from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, Protocol

from .models import ExecutorJob, ExecutorResult, JobSendBlocked, JobSendUnconfirmed, JobSendDeferred, JobSendRetryable
from .state import ExecutorStateStore, delivery_identity
from ..domain.betting_protocol import normalize_protocol, parse_acceptance


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
    if job.attempt_token:
        result.raw_result = {**(result.raw_result or {}), 'attempt_token':job.attempt_token}
    payload=result.to_payload()
    state_store.store_result(job,payload)
    api_client.report_job(job_id=job.job_id, payload=payload)
    state_store.reported(job.idempotency_key)


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
            "raw_result": {**(record.get('raw_result') or {}), "replayed_from_local_state": True},
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
    # 已确认发送成功的本地记录优先回报，不能因新的截止时间将其改记为过期。
    if state_store.has_delivered(job.idempotency_key):
        if _replay_delivered_attempt(api_client=api_client, state_store=state_store, job=job):
            return "replayed"
        return "skipped"
    now = datetime.now(timezone.utc)
    if now < job.execute_after:
        return "skipped"

    record=state_store.get_record(job.idempotency_key) or {}
    attempt_no = job.attempt_no or state_store.next_attempt_no(job.idempotency_key)
    if record.get('delivery_status') in ('sending','unconfirmed','rejected'):
        result=ExecutorResult(job_id=job.job_id,executor_id=executor_id,attempt_no=attempt_no,
            delivery_status='unconfirmed',executed_at=now,error_message='此前发送结果需要核对，禁止重复发送',
            remote_message_id=record.get('remote_message_id'),
            raw_result={**(record.get('transport_evidence') or {}), **(record.get('raw_result') or {}),
                        'reason':'previous_send_unconfirmed'})
        _record_and_report_result(api_client=api_client,state_store=state_store,executor_id=executor_id,job=job,result=result)
        return 'skipped'
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

    try:
        if job.attempt_token:
            state_store.prepare(job,executor_id,attempt_no)
            job.transport_evidence_callback = lambda evidence: state_store.store_transport_evidence(job, evidence)
        send_result = message_sender.send_text(job)
        acceptance=send_result.get('acceptance') or {}
        status={'accepted':'delivered','rejected':'rejected','unconfirmed':'unconfirmed'}.get(acceptance.get('state'),'delivered')
        result = ExecutorResult(
            job_id=job.job_id,
            executor_id=executor_id,
            attempt_no=attempt_no,
            delivery_status=status,
            executed_at=datetime.now(timezone.utc),
            remote_message_id=str(send_result.get("message_id") or ""),
            raw_result={"job_received_at": now.isoformat(), **dict(send_result)},
            error_message=None if status=='delivered' else ('投注平台拒单' if status=='rejected' else '消息已送达，投注受理待核对'),
        )
    except JobSendDeferred as exc:
        status='pending'
        result=ExecutorResult(job_id=job.job_id,executor_id=executor_id,attempt_no=attempt_no,
            delivery_status=status,executed_at=datetime.now(timezone.utc),error_message=str(exc),
            raw_result={**exc.details,'deferred':True})
    except JobSendRetryable as exc:
        status='failed'
        result=ExecutorResult(job_id=job.job_id,executor_id=executor_id,attempt_no=attempt_no,
            delivery_status=status,executed_at=datetime.now(timezone.utc),error_message=str(exc),raw_result=exc.details)
    except (JobSendBlocked, JobSendUnconfirmed) as exc:
        status = "expired" if isinstance(exc, JobSendBlocked) else ("unconfirmed" if job.attempt_token else "skipped")
        result = ExecutorResult(
            job_id=job.job_id, executor_id=executor_id, attempt_no=attempt_no,
            delivery_status=status, executed_at=datetime.now(timezone.utc),
            raw_result={"exception_type": exc.__class__.__name__, **exc.details},
            error_message=str(exc),
        )
    except Exception as exc:
        status = "unconfirmed" if job.attempt_token else "failed"
        result = ExecutorResult(
            job_id=job.job_id,
            executor_id=executor_id,
            attempt_no=attempt_no,
            delivery_status=status,
            executed_at=datetime.now(timezone.utc),
            raw_result={"exception_type": exc.__class__.__name__},
            error_message=str(exc) or exc.__class__.__name__,
        )

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
        "waiting_count": statuses.count('pending'),
        "rejected_count": statuses.count('rejected'),
        "unconfirmed_count": statuses.count('unconfirmed'),
    }


def _flush_reports(api_client, state_store):
    for identity, record in state_store.pending_reports():
        payload={k:v for k,v in record.items() if k not in (
            'job_id', 'pending_report', 'delivery_identity', 'transport_evidence', 'attempt_token',
        )}
        api_client.report_job(job_id=record['job_id'],payload=payload)
        state_store.reported(identity)


def run_reconciliation_cycle(*, api_client, message_sender, state_store, executor_id):
    """Read one original delivery's evidence; this path never sends a message."""
    items = api_client.pull_reconciliation_jobs(limit=1)
    outcomes = []
    for raw in items[:1]:
        original = raw.get('original_attempt') or {}
        result = {'outcome': 'unknown', 'evidence': {}, 'reason': 'message_id_unavailable'}
        try:
            job = ExecutorJob.from_payload(raw)
            record = state_store.get_record(job.idempotency_key) or {}
            identity = record.get('delivery_identity')
            record_token = (record.get('raw_result') or {}).get('attempt_token') or record.get('attempt_token')
            if raw.get('attempt_token'):
                result['reason'] = 'reconciliation_is_not_send_claim'
            elif record and (str(record.get('job_id')) != job.job_id or
                             record.get('executor_id') != original.get('executor_id') or
                             record_token != original.get('token')):
                result['reason'] = 'local_attempt_mismatch'
            elif identity and identity != delivery_identity(job):
                result['reason'] = 'local_identity_mismatch'
            else:
                evidence = dict(raw.get('delivery_evidence') or {})
                local_evidence = {**(record.get('transport_evidence') or {}), **(record.get('raw_result') or {})}
                local_message_id = local_evidence.get('message_id') or record.get('remote_message_id')
                local_verified = bool(identity and local_message_id
                    and local_evidence.get('target_key') == job.target.key
                    and local_evidence.get('text') == job.message_text
                    and local_evidence.get('telegram_account_id') == (job.telegram_account.id if job.telegram_account else None))
                if local_verified:
                    evidence.update(local_evidence, message_id=local_message_id, issue_no=job.issue_no,
                                    provenance='local_result')
                    protocol = normalize_protocol((job.target.metadata or {}).get('betting_protocol'))
                    if protocol['mode'] == 'legacy' and (record.get('transport_evidence') or record.get('delivery_status') == 'delivered'):
                        result = {'outcome': 'transport_confirmed', 'evidence': evidence, 'reason': 'durable_transport_evidence'}
                    elif evidence.get('acceptance_reply'):
                        verified = parse_acceptance([evidence['acceptance_reply']], protocol,
                            local_message_id, job.issue_no, job.message_text,
                            evidence.get('opening_evidence') or {}, evidence.get('current_opening_evidence') or {})
                        round_id = str(verified.get('target_round_id') or '')
                        opaque = verified.get('state') == 'accepted' and not (round_id.isdigit() and len(round_id) < 20)
                        contemporaneous = False
                        if opaque and evidence.get('acceptance_checked_at'):
                            checked = datetime.fromisoformat(evidence['acceptance_checked_at'].replace('Z', '+00:00'))
                            replied = datetime.fromisoformat(evidence['acceptance_reply']['date'].replace('Z', '+00:00'))
                            sent = datetime.fromisoformat(evidence['send_finished_at'].replace('Z', '+00:00'))
                            contemporaneous = (sent.timestamp() - 2 <= replied.timestamp() <= checked.timestamp() + 2
                                and 0 <= checked.timestamp() - sent.timestamp() <= protocol['ack_timeout_seconds'] + 6
                                and replied.timestamp() <= job.expire_at.timestamp() + protocol['ack_timeout_seconds'] + 6)
                        if verified.get('state') in ('accepted', 'rejected') and (not opaque or contemporaneous):
                            evidence['acceptance'] = verified
                            result = {'outcome': verified['state'], 'evidence': evidence, 'reason': 'durable_acceptance_evidence'}
                if result['outcome'] == 'unknown' and evidence.get('message_id'):
                    result = message_sender.reconcile_text(job, evidence)
        except Exception as exc:
            result = {'outcome': 'unknown', 'evidence': {}, 'reason': 'reconciliation_read_failed:' + type(exc).__name__}
        payload = {
            'reconciliation_token': raw.get('reconciliation_token'),
            'original_attempt_token': original.get('token'),
            'outcome': result['outcome'], 'evidence': result.get('evidence') or {},
            'reason': result.get('reason') or '',
        }
        api_client.reconcile_job(job_id=str(raw['job_id']), payload=payload)
        outcomes.append(result['outcome'])
    return {'checked_count': len(outcomes), 'outcomes': outcomes}


def _start_background_reconciliation(*, api_client, message_sender, state_store, executor_id, raw_jobs):
    if raw_jobs or not all(callable(getattr(owner, name, None)) for owner, name in (
        (api_client, 'pull_reconciliation_jobs'), (api_client, 'reconcile_job'), (message_sender, 'reconcile_text'),
    )):
        return
    state_store.start_reconciliation(lambda: run_reconciliation_cycle(
        api_client=api_client, message_sender=message_sender, state_store=state_store, executor_id=executor_id,
    ))


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
    _flush_reports(api_client,state_store)
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
    _start_background_reconciliation(api_client=api_client, message_sender=message_sender,
        state_store=state_store, executor_id=executor_id, raw_jobs=raw_jobs)
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
    _flush_reports(api_client,state_store)
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
    _start_background_reconciliation(api_client=api_client, message_sender=message_sender,
        state_store=state_store, executor_id=executor_id, raw_jobs=raw_jobs)
    return _cycle_result(heartbeat, raw_jobs, statuses)
