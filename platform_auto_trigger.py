from __future__ import annotations

import time

from pc28touzhu.config import get_runtime_config
from pc28touzhu.main import build_repository
from pc28touzhu.services.auto_trigger_service import run_auto_trigger_cycle


def main() -> int:
    config = get_runtime_config()
    repo = build_repository()

    while True:
        if not config.auto_trigger.enabled:
            print("auto trigger worker disabled")
            if config.auto_trigger.once:
                return 0
            time.sleep(max(5, int(config.auto_trigger.interval_seconds or 5)))
            continue

        result = run_auto_trigger_cycle(repo)
        summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
        print(
            "auto trigger cycle rules=%s checked=%s triggered=%s skipped=%s failed=%s"
            % (
                int(summary.get("rule_count") or 0),
                int(summary.get("checked_count") or 0),
                int(summary.get("triggered_count") or 0),
                int(summary.get("skipped_count") or 0),
                int(summary.get("failed_count") or 0),
            )
        )
        if config.auto_trigger.once:
            return 1 if int(summary.get("failed_count") or 0) > 0 else 0
        time.sleep(max(5, int(config.auto_trigger.interval_seconds or 30)))


if __name__ == "__main__":
    raise SystemExit(main())
