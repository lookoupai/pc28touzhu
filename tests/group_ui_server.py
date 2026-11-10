"""双组/杀组浏览器 fixture：仅临时数据库，无 worker 或外部消息。"""
from __future__ import annotations

import json
import re
import sys
import tempfile
import threading
from pathlib import Path
from wsgiref.simple_server import make_server

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.dispatch_service import dispatch_signal
from pc28touzhu.services.platform_service import resolve_subscription_progression
from tests.cycle_ui_server import QuietHandler


def main():
    with tempfile.TemporaryDirectory(prefix="pc28-group-ui-") as directory:
        repo = DatabaseRepository(str(Path(directory) / "fixture.db"))
        repo.initialize_database()
        user = repo.create_user_record(username="group-ui-local")
        uid = user["id"]
        source = repo.create_source_record(owner_user_id=uid, source_type="ai_trading_simulator_export", name="双组杀组测试来源", status="active")
        account = repo.create_telegram_account_record(user_id=uid, label="离线测试账号", session_path="local-only", status="active", meta={"auth_state": "authorized"})
        template = repo.create_message_template_record(user_id=uid, name="组合模板", lottery_type="pc28", template_text="{{bet_value}}{{amount}}")
        target = repo.create_delivery_target_record(user_id=uid, telegram_account_id=account["id"], executor_type="telegram_group", target_key="-1000000000001", target_name="离线测试群", template_id=template["id"], status="active", last_test_status="success")
        sub = repo.create_subscription_record(user_id=uid, source_id=source["id"], name="已结算杀组示例", strategy={
            "play_filter": {"mode": "selected", "selected_keys": ["kill_group:小双"]},
            "staking_policy": {"mode": "fixed", "fixed_amount": 10},
            "settlement_policy": {"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_fullpay_2_0_regular"},
            "dispatch": {"delivery_target_ids": [target["id"]]},
        })
        signal = repo.create_signal_record(source_id=source["id"], lottery_type="pc28", issue_no="1001", bet_type="kill_group", bet_value="小双")
        job = dispatch_signal(repo, signal["id"])["jobs"][0]
        # 本地标记模拟送达；绝不调用 Telegram sender。
        repo.report_job_result(job_id=str(job["id"]), executor_id="offline", attempt_no=1,
            delivery_status="delivered", remote_message_id="offline-fixture-only", executed_at=job["execute_after"], raw_result={}, error_message=None)
        resolve_subscription_progression(repo, subscription_id=sub["id"], user_id=uid, payload={"progression_event_id": job["progression_event_id"], "draw_context": {"result_number": 13}})
        app = PlatformApiApplication(repo, "offline-only", "group-ui-local")

        def local_app(environ, start_response):
            endpoint = environ.get("PATH_INFO", "")
            method = environ.get("REQUEST_METHOD")
            allowed_write = re.fullmatch(r"/api/platform/(subscriptions|auto-trigger-rules)(/\d+(/status)?)?", endpoint) is not None
            if method != "GET" and not allowed_write:
                start_response("403 Forbidden", [("Content-Type", "application/json")])
                return [b'{"error":"only local configuration writes are allowed"}']
            return app(environ, start_response)

        with make_server("127.0.0.1", 0, local_app, handler_class=QuietHandler) as server:
            print(json.dumps({"url": "http://127.0.0.1:%s" % server.server_port, "cookie_name": SESSION_COOKIE_NAME, "cookie": build_session_cookie_value(uid, "group-ui-local"), "source_id": source["id"], "target_id": target["id"]}), flush=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            sys.stdin.read()
            server.shutdown()


if __name__ == "__main__":
    main()
