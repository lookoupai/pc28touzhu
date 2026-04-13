from __future__ import annotations

import time
import urllib.error
from typing import Any, Dict

from pc28touzhu.config import get_runtime_config
from pc28touzhu.executor import ExecutorApiClient, ExecutorStateStore, run_executor_cycle


class FakeMessageSender:
    def send_text(self, job) -> Dict[str, Any]:
        print("send ->", job.target.key, job.message_text)
        return {
            "message_id": "fake-msg-001",
            "target_key": job.target.key,
            "text": job.message_text,
            "telegram_account_id": job.telegram_account.id if job.telegram_account else None,
        }


def main() -> int:
    config = get_runtime_config()
    executor = config.executor
    api_client = ExecutorApiClient(
        base_url=executor.platform_base_url,
        token=executor.executor_api_token,
        executor_id=executor.executor_id,
    )

    store = ExecutorStateStore()
    sender = FakeMessageSender()

    while True:
        try:
            result = run_executor_cycle(
                api_client=api_client,
                message_sender=sender,
                state_store=store,
                executor_id=executor.executor_id,
                limit=executor.pull_limit,
                version="fake-executor/0.1.0",
                capabilities={"send": False, "provider": "fake"},
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

        except urllib.error.HTTPError as exc:
            print("http error:", exc.code, exc.reason)
            return 2
        except Exception as exc:
            print("error:", str(exc))
            return 1

        if executor.once:
            return 0
        time.sleep(2)


if __name__ == "__main__":
    raise SystemExit(main())
