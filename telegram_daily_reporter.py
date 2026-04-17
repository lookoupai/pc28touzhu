from __future__ import annotations

import time

from pc28touzhu.config import get_runtime_config
from pc28touzhu.main import build_repository
from pc28touzhu.services.telegram_report_service import run_daily_report_cycle
from pc28touzhu.services.telegram_runtime_settings_service import get_effective_telegram_runtime_settings
from pc28touzhu.telegram_bot_sender import TelegramBotSender


def main() -> int:
    config = get_runtime_config()

    repo = build_repository()
    sender = None
    current_token = ""

    while True:
        resolved = get_effective_telegram_runtime_settings(repo, runtime_config=config)
        bot = resolved["item"]["bot"]
        report = resolved["item"]["report"]
        if not report["enabled"]:
            print("telegram report disabled")
            if config.telegram_report.once:
                return 0
            time.sleep(max(5, int(report.get("interval_seconds") or 5)))
            continue
        if not bot["bot_token"]:
            print("TG_BOT_TOKEN 未配置")
            if config.telegram_report.once:
                return 2
            time.sleep(max(5, int(report.get("interval_seconds") or 5)))
            continue
        if not report["target_chat_id"]:
            print("TG_REPORT_TARGET_CHAT_ID 未配置")
            if config.telegram_report.once:
                return 2
            time.sleep(max(5, int(report.get("interval_seconds") or 5)))
            continue
        if sender is None or current_token != bot["bot_token"]:
            sender = TelegramBotSender(bot_token=bot["bot_token"])
            current_token = bot["bot_token"]

        result = run_daily_report_cycle(
            repo,
            sender=sender,
            target_chat_id=report["target_chat_id"],
            send_hour=report["send_hour"],
            send_minute=report["send_minute"],
            top_n=report["top_n"],
            timezone_name=report["timezone"],
        )
        print(
            "report cycle stat_date=%s skipped=%s reason=%s delivery=%s"
            % (
                result.get("stat_date") or "--",
                result.get("skipped"),
                result.get("reason") or "--",
                result.get("delivery_status") or "--",
            )
        )
        if config.telegram_report.once:
            return 1 if result.get("delivery_status") == "failed" else 0
        time.sleep(report["interval_seconds"])


if __name__ == "__main__":
    raise SystemExit(main())
