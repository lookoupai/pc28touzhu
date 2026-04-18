"""PC28 自动结算 worker 入口。"""
from __future__ import annotations

import time
from datetime import datetime, timezone

from pc28touzhu.config import get_runtime_config
from pc28touzhu.main import build_repository
from pc28touzhu.services.pc28_auto_settlement_service import run_pc28_auto_settlement_cycle
from pc28touzhu.services.telegram_runtime_settings_service import (
    get_effective_telegram_runtime_settings,
    update_pc28_auto_settlement_runtime_state,
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def run_worker() -> None:
    config = get_runtime_config()
    repository = build_repository()
    while True:
        worker_settings = get_effective_telegram_runtime_settings(repository, runtime_config=config)["item"].get("auto_settlement") or {}
        if not bool(worker_settings.get("enabled")):
            print("pc28 auto settlement worker disabled")
            break
        started_at = _utc_now_iso()
        try:
            result = run_pc28_auto_settlement_cycle(
                repository,
                draw_limit=int(worker_settings.get("draw_limit") or config.pc28_auto_settlement.draw_limit or 60),
            )
            update_pc28_auto_settlement_runtime_state(
                repository,
                last_run_at=started_at,
                last_status="success",
                last_summary=result.get("summary") if isinstance(result.get("summary"), dict) else {},
                last_error="",
            )
            summary = result.get("summary") if isinstance(result.get("summary"), dict) else {}
            print(
                "[%s] pc28 auto settlement cycle: pending=%s resolved=%s hit=%s refund=%s miss=%s unmatched=%s source=%s skipped=%s"
                % (
                    started_at,
                    int(summary.get("pending_count") or 0),
                    int(summary.get("resolved_count") or 0),
                    int(summary.get("hit_count") or 0),
                    int(summary.get("refund_count") or 0),
                    int(summary.get("miss_count") or 0),
                    int(summary.get("unmatched_count") or 0),
                    str(result.get("draw_source") or ""),
                    bool(result.get("skipped")),
                )
            )
        except Exception as exc:
            update_pc28_auto_settlement_runtime_state(
                repository,
                last_run_at=started_at,
                last_status="failed",
                last_summary={},
                last_error=str(exc) or exc.__class__.__name__,
            )
            print("[%s] pc28 auto settlement cycle failed: %s" % (started_at, str(exc) or exc.__class__.__name__))

        if bool(config.pc28_auto_settlement.once):
            break
        time.sleep(max(5, int(worker_settings.get("interval_seconds") or config.pc28_auto_settlement.interval_seconds or 30)))


def main() -> None:
    run_worker()


if __name__ == "__main__":
    main()
