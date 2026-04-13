from __future__ import annotations

import time

from pc28touzhu.config import get_runtime_config
from pc28touzhu.main import build_repository
from pc28touzhu.services.alert_notification_service import deliver_platform_alerts
from pc28touzhu.services.platform_service import list_platform_alerts
from pc28touzhu.telegram_bot_sender import TelegramBotSender


def main() -> int:
    config = get_runtime_config()
    platform = config.platform
    notifier = config.alert_notifier
    if not notifier.enabled:
        print("alert notifier disabled")
        return 0
    if not notifier.bot_token:
        print("ALERT_TELEGRAM_BOT_TOKEN 未配置")
        return 2
    if not notifier.target_chat_id:
        print("ALERT_TELEGRAM_TARGET_CHAT_ID 未配置")
        return 2

    repo = build_repository()
    sender = TelegramBotSender(bot_token=notifier.bot_token)

    while True:
        alerts = list_platform_alerts(
            repo,
            user_id=None,
            limit=200,
            stale_after_seconds=platform.executor_stale_after_seconds,
            offline_after_seconds=platform.executor_offline_after_seconds,
            auto_retry_max_attempts=platform.auto_retry_max_attempts,
            auto_retry_base_delay_seconds=platform.auto_retry_base_delay_seconds,
            failure_streak_threshold=platform.alert_failure_streak_threshold,
        )["items"]
        result = deliver_platform_alerts(
            repo,
            alerts=alerts,
            sender=sender,
            target_chat_id=notifier.target_chat_id,
            repeat_interval_seconds=notifier.repeat_interval_seconds,
        )
        print(
            "alert cycle total=%s pending=%s sent=%s failed=%s"
            % (
                len(alerts),
                result["pending_count"],
                result["sent_count"],
                result["failed_count"],
            )
        )
        if notifier.once:
            return 1 if result["failed_count"] > 0 else 0
        time.sleep(notifier.interval_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
