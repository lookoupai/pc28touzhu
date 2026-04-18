from __future__ import annotations

import time

from pc28touzhu.config import get_runtime_config
from pc28touzhu.main import build_repository
from pc28touzhu.services.telegram_bot_service import process_telegram_bot_cycle, sync_telegram_bot_commands
from pc28touzhu.services.telegram_runtime_settings_service import get_effective_telegram_runtime_settings
from pc28touzhu.telegram_bot_sender import TelegramBotSender


def main() -> int:
    config = get_runtime_config()

    repo = build_repository()
    client = None
    current_token = ""
    commands_synced_token = ""

    while True:
        resolved = get_effective_telegram_runtime_settings(repo, runtime_config=config)
        bot = resolved["item"]["bot"]
        if not bot["enabled"]:
            print("telegram bot disabled")
            if config.telegram_bot.once:
                return 0
            time.sleep(max(1, int(bot.get("poll_interval_seconds") or 1)))
            continue
        if not bot["bot_token"]:
            print("TG_BOT_TOKEN 未配置")
            if config.telegram_bot.once:
                return 2
            time.sleep(max(1, int(bot.get("poll_interval_seconds") or 1)))
            continue
        if client is None or current_token != bot["bot_token"]:
            client = TelegramBotSender(bot_token=bot["bot_token"])
            current_token = bot["bot_token"]
        if commands_synced_token != current_token:
            sync_telegram_bot_commands(client)
            commands_synced_token = current_token

        result = process_telegram_bot_cycle(
            repo,
            bot_client=client,
            poll_timeout_seconds=bot["poll_interval_seconds"],
        )
        print(
            "bot cycle updates=%s handled=%s replied=%s ignored=%s last_update_id=%s"
            % (
                result["update_count"],
                result["handled_count"],
                result["replied_count"],
                result["ignored_count"],
                result["last_update_id"],
            )
        )
        if config.telegram_bot.once:
            return 0


if __name__ == "__main__":
    raise SystemExit(main())
