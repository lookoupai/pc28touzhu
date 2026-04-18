from __future__ import annotations

import time

from pc28touzhu.config import get_runtime_config
from pc28touzhu.main import build_repository
from pc28touzhu.services.source_sync_service import run_source_sync_cycle


def main() -> int:
    config = get_runtime_config()
    repo = build_repository()

    while True:
        if not config.source_sync.enabled:
            print("source sync worker disabled")
            if config.source_sync.once:
                return 0
            time.sleep(max(5, int(config.source_sync.interval_seconds or 5)))
            continue

        result = run_source_sync_cycle(repo)
        summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
        print(
            "source sync cycle sources=%s processed=%s fetched=%s normalized=%s candidates=%s created_jobs=%s existing_jobs=%s skipped=%s failed=%s"
            % (
                int(summary.get("source_count") or 0),
                int(summary.get("processed_count") or 0),
                int(summary.get("fetched_count") or 0),
                int(summary.get("normalized_signal_count") or 0),
                int(summary.get("dispatch_candidate_count") or 0),
                int(summary.get("created_job_count") or 0),
                int(summary.get("existing_job_count") or 0),
                int(summary.get("skipped_duplicate_count") or 0),
                int(summary.get("failed_count") or 0),
            )
        )
        if config.source_sync.once:
            return 1 if int(summary.get("failed_count") or 0) > 0 else 0
        time.sleep(max(5, int(config.source_sync.interval_seconds or 30)))


if __name__ == "__main__":
    raise SystemExit(main())
