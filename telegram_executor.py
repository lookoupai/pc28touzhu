from __future__ import annotations

import time

from pc28touzhu.config import get_runtime_config
from pc28touzhu.executor import (
    ExecutorApiClient,
    ExecutorStateStore,
    TelethonSenderPool,
    run_executor_cycle_concurrent,
)


def main() -> int:
    config = get_runtime_config()
    executor = config.executor
    api_client = ExecutorApiClient(
        base_url=executor.platform_base_url,
        token=executor.executor_api_token,
        executor_id=executor.executor_id,
    )
    state_store = ExecutorStateStore()
    sender_pool = TelethonSenderPool(
        api_id=executor.telegram_api_id,
        api_hash=executor.telegram_api_hash,
        default_phone=executor.telegram_phone,
        default_session=executor.telegram_session,
    )

    try:
        while True:
            try:
                result = run_executor_cycle_concurrent(
                    api_client=api_client,
                    message_sender=sender_pool,
                    state_store=state_store,
                    executor_id=executor.executor_id,
                    limit=executor.pull_limit,
                    max_concurrent=4,
                    version="telegram-executor/0.3.0",
                    capabilities={
                        "send": True,
                        "provider": "telethon",
                        "account_scoped_concurrency": True,
                    },
                )
                print(
                    "cycle pulled=%s delivered=%s failed=%s expired=%s skipped=%s replayed=%s"
                    % (
                        result["pulled_count"],
                        result["delivered_count"],
                        result["failed_count"],
                        result["expired_count"],
                        result["skipped_count"],
                        result["replayed_count"],
                    )
                )
            except Exception as exc:
                print("cycle error:", str(exc))
                return 1

            if executor.once:
                return 0
            time.sleep(2)
    finally:
        sender_pool.disconnect()


if __name__ == "__main__":
    raise SystemExit(main())
