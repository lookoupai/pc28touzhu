"""Local navigation fixture. Temporary database, no workers or external actions."""
import json
import sys
import tempfile
import threading
from pathlib import Path
from wsgiref.simple_server import make_server

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value, hash_password
from pc28touzhu.executor.db_repository import DatabaseRepository
from tests.cycle_ui_server import QuietHandler


def main():
    with tempfile.TemporaryDirectory(prefix="pc28-navigation-") as directory:
        repo = DatabaseRepository(str(Path(directory) / "fixture.db"))
        repo.initialize_database()
        regular = repo.create_user_record(username="navigation-user", password_hash=hash_password("local-pass"))
        admin = repo.create_user_record(username="navigation-admin", role="admin", password_hash=hash_password("local-pass"))
        for user in (regular, admin):
            uid = user["id"]
            source = repo.create_source_record(owner_user_id=uid, source_type="ai_trading_simulator_export", name="示例来源", status="active")
            account = repo.create_telegram_account_record(user_id=uid, label="测试账号", session_path="local-test-only", status="active", meta={"auth_state": "authorized"})
            template = repo.create_message_template_record(user_id=uid, name="测试模板", lottery_type="pc28", template_text="{{bet_value}}{{amount}}")
            target = repo.create_delivery_target_record(user_id=uid, telegram_account_id=account["id"], executor_type="telegram_group", target_key="-10000000000" + str(uid), target_name="测试群组", template_id=template["id"], status="active", last_test_status="success")
            sub = repo.create_subscription_record(user_id=uid, source_id=source["id"], name="示例跟单方案", status="standby", strategy={"dispatch": {"delivery_target_ids": [target["id"]]}})
            rule = repo.create_auto_trigger_rule_record(user_id=uid, name="示例自动规则", scope_mode="selected_subscriptions", subscription_ids=[sub["id"]], conditions=[{"type": "draw_streak", "metric": "big_small", "bet_value": "大", "threshold": 2, "operator": "gte"}], action={"play_filter_action": "keep", "dispatch_latest_signal": False}, cooldown_issues=0)
            user["subscription_id"] = sub["id"]
            user["rule_id"] = rule["id"]
        app = PlatformApiApplication(repo, "local-test-only", "navigation-local")

        def local_app(environ, start_response):
            path = environ.get("PATH_INFO", "")
            # The browser exercises configuration navigation and login, never live execution.
            if environ.get("REQUEST_METHOD") != "GET" and path not in {"/api/auth/login", "/api/auth/logout"}:
                start_response("403 Forbidden", [("Content-Type", "application/json")])
                return [b'{"error":"writes disabled in navigation fixture"}']
            return app(environ, start_response)

        with make_server("127.0.0.1", 0, local_app, handler_class=QuietHandler) as server:
            cookies = {"user": build_session_cookie_value(regular["id"], "navigation-local"), "admin": build_session_cookie_value(admin["id"], "navigation-local")}
            print(json.dumps({"url": "http://127.0.0.1:%s" % server.server_port, "cookie_name": SESSION_COOKIE_NAME, "cookies": cookies, "subscription_id": regular["subscription_id"], "rule_id": regular["rule_id"]}), flush=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            sys.stdin.read()
            server.shutdown()


if __name__ == "__main__":
    main()
