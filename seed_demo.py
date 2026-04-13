from __future__ import annotations

from datetime import datetime, timedelta, timezone

from pc28touzhu.config import get_runtime_config
from pc28touzhu.executor.db_repository import DatabaseRepository


def iso_z(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main() -> None:
    config = get_runtime_config()
    platform = config.platform
    demo = config.demo_seed

    repo = DatabaseRepository(platform.database_path)
    repo.initialize_database()

    user_id = repo.create_user("demo")
    source_id = repo.create_source("internal_ai", "demo-source", owner_user_id=user_id)
    signal_id = repo.create_signal(
        source_id=source_id,
        lottery_type="pc28",
        issue_no=demo.issue_no,
        bet_type=demo.bet_type,
        bet_value=demo.bet_value,
        confidence=0.7,
        normalized_payload={"note": "demo"},
    )
    target_id = repo.create_delivery_target(
        user_id=user_id,
        executor_type="telegram_group",
        target_key=demo.target_key,
        target_name="demo-group",
    )

    now = datetime.now(timezone.utc)
    execute_after = iso_z(now - timedelta(seconds=2))
    expire_at = iso_z(now + timedelta(minutes=2))
    job_id = repo.create_execution_job(
        user_id=user_id,
        signal_id=signal_id,
        delivery_target_id=target_id,
        executor_type="telegram_group",
        idempotency_key=demo.idempotency_key,
        planned_message_text=demo.message_text,
        stake_plan={"mode": "flat", "amount": demo.stake_amount},
        execute_after=execute_after,
        expire_at=expire_at,
    )

    print("seed ok")
    print("DATABASE_PATH=%s" % platform.database_path)
    print("job_id=%s signal_id=%s delivery_target_id=%s" % (job_id, signal_id, target_id))
    print("execute_after=%s expire_at=%s" % (execute_after, expire_at))


if __name__ == "__main__":
    main()
