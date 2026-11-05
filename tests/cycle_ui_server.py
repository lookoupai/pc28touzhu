"""UI 回放专用的本地 WSGI fixture；不读 .env、不启动 worker，不发送消息。"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from wsgiref.simple_server import WSGIRequestHandler, make_server

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.executor.db_repository import DatabaseRepository


class QuietHandler(WSGIRequestHandler):
    def log_message(self, format, *args):
        pass


def main() -> int:
    with tempfile.TemporaryDirectory(prefix="pc28-cycle-ui-") as temp:
        db_path = Path(temp) / "fixture.db"
        repo = DatabaseRepository(str(db_path))
        repo.initialize_database()
        user = repo.create_user_record(username="cycle-ui-local", status="active")
        uid = int(user["id"])
        source = repo.create_source_record(owner_user_id=uid, source_type="internal_ai", name="本地测试来源", status="active")
        target = repo.create_delivery_target_record(user_id=uid, executor_type="telegram_group", target_key="-100000000001", target_name="仅本地测试目标", status="active")
        strategy = {
            "play_filter": {"mode": "all"},
            "staking_policy": {"mode": "win_streak_roll", "structure": "B", "base_amount": 10, "target_streak": 3, "roll_mode": "chain",
                "stop_policy": {"on_target": "close_day", "on_bust": "close_day", "max_cycles_per_day": 1}},
            "settlement_policy": {"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_fullpay_netdisk_regular"},
            "dispatch": {"delivery_target_ids": [target["id"]]},
        }
        sub = repo.create_subscription_record(user_id=uid, source_id=source["id"], name="本地过关方案", status="standby", strategy=strategy)
        common = {"user_id": uid, "scope_mode": "selected_subscriptions", "subscription_ids": [sub["id"]],
            "conditions": [{"type": "draw_streak", "metric": "combo", "bet_value": "大双", "threshold": 2, "operator": "gte"}],
            "action": {"play_filter_action": "keep", "dispatch_latest_signal": False}, "cooldown_issues": 0}
        old = repo.create_auto_trigger_rule_record(name="本地兼容规则", **common)
        closed = repo.create_auto_trigger_rule_record(name="本地已收工规则", cycle_policy={"mode": "rule", "max_starts_per_day": 3, "on_target": "wait_condition", "on_bust": "wait_condition"}, **common)
        today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
        repo.upsert_auto_trigger_cycle_session(rule_id=closed["id"], subscription_id=sub["id"], user_id=uid,
            values={"status": "day_closed", "start_stat_date": today, "last_completed_issue": "4000000", "stop_reason": "cycle_balance_depleted"})
        with sqlite3.connect(str(db_path)) as con:
            con.execute("INSERT INTO auto_trigger_cycle_daily_starts(rule_id,subscription_id,user_id,stat_date,starts_count,last_start_issue,updated_at) VALUES(?,?,?,?,?,?,?)",
                (closed["id"], sub["id"], uid, today, 1, "3999999", datetime.now(timezone.utc).isoformat()))
        app = PlatformApiApplication(repository=repo, executor_api_token="local-ui-test-only", session_secret="local-ui-test-only")
        # 明确禁止所有可能运行真实网络或派单的端点，测试只访问配置和恢复。
        def local_app(environ, start_response):
            path = environ.get("PATH_INFO", "")
            allowed = (path.startswith("/assets/") or path in {"/api/auth/me", "/api/health", "/auto-triggers", "/autobet/subscriptions"}
                or path.startswith("/api/platform/auto-trigger-rules") or path.startswith("/api/platform/subscriptions")
                or path in {"/api/platform/sources", "/api/platform/sources/summary", "/api/platform/delivery-targets", "/api/platform/delivery-targets/summary",
                    "/api/platform/message-templates", "/api/platform/telegram-accounts", "/api/platform/auto-trigger-events", "/api/platform/execution-jobs"})
            forbidden_suffix = ("/run-once", "/dispatch", "/fetch", "/test-send", "/resolve", "/resolve-batch", "/status")
            if not allowed or path.endswith(forbidden_suffix):
                start_response("403 Forbidden", [("Content-Type", "application/json")])
                return [b'{"error":"network/dispatch actions are disabled in the UI fixture"}']
            return app(environ, start_response)

        with make_server("127.0.0.1", 0, local_app, handler_class=QuietHandler) as server:
            print(json.dumps({"url": "http://127.0.0.1:%s" % server.server_port, "cookie": "%s=%s" % (SESSION_COOKIE_NAME, build_session_cookie_value(uid, "local-ui-test-only")),
                "subscription_id": sub["id"], "legacy_rule_id": old["id"], "closed_rule_id": closed["id"]}), flush=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            # 父进程关闭 stdin 即结束并清理临时库，不留下测试进程或数据库。
            sys.stdin.read()
            server.shutdown()
        with sqlite3.connect(str(db_path)) as con:
            if con.execute("SELECT COUNT(*) FROM execution_jobs").fetchone()[0]:
                raise AssertionError("UI 配置验证不应创建投注任务")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
