"""Rule lab browser fixture: local WSGI and temporary databases only."""
from __future__ import annotations

import json
import os
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
    def log_message(self, *args):
        pass


def make_source(path):
    with sqlite3.connect(path) as conn:
        conn.executescript("""
        CREATE TABLE predictors(id INTEGER PRIMARY KEY, name TEXT, lottery_type TEXT,
            enabled INTEGER, share_predictions INTEGER, share_level TEXT);
        CREATE TABLE lottery_draws(id INTEGER PRIMARY KEY, lottery_type TEXT, issue_no TEXT,
            open_time TEXT, result_number INTEGER, source_payload TEXT, created_at TEXT,
            UNIQUE(lottery_type,issue_no));
        CREATE TABLE predictions(id INTEGER PRIMARY KEY, predictor_id INTEGER, lottery_type TEXT,
            issue_no TEXT, prediction_big_small TEXT, prediction_odd_even TEXT, prediction_combo TEXT,
            status TEXT, created_at TEXT, settled_at TEXT, UNIQUE(predictor_id,issue_no));
        """)
        conn.execute("INSERT INTO predictors VALUES(1,?,'pc28',1,1,'records')", ('单双共识 · 本地测试来源',))
        conn.execute("INSERT INTO predictors VALUES(2,?,'pc28',1,1,'records')",
                     ('公开来源 <img src=x onerror="window.labXss=1">',))
        now=datetime.now(timezone.utc).replace(microsecond=0)
        for index in range(6):
            opened=now-timedelta(seconds=1800-index*210)
            conn.execute("INSERT INTO lottery_draws(lottery_type,issue_no,open_time,result_number,source_payload,created_at) VALUES('pc28',?,?,?,?,?)",
                (str(2000+index),opened.astimezone(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S'),17,
                 json.dumps({'number':'3+6+8'}),(opened+timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')))
            conn.execute("INSERT INTO predictions(predictor_id,lottery_type,issue_no,prediction_big_small,prediction_odd_even,prediction_combo,status,created_at,settled_at) VALUES(1,'pc28',?,'大','单','大单','settled',?,?)",
                (str(2000+index),(opened-timedelta(seconds=120)).strftime('%Y-%m-%d %H:%M:%S'),(opened+timedelta(seconds=15)).strftime('%Y-%m-%d %H:%M:%S')))


def main():
    with tempfile.TemporaryDirectory(prefix="pc28-rule-lab-ui-") as directory:
        root = Path(directory)
        os.environ["RULE_LAB_DATA_DIR"] = str(root / "lab")
        os.environ["RULE_LAB_SOURCE_DB"] = str(root / "source.db")
        make_source(root / "source.db")
        repo = DatabaseRepository(str(root / "main.db"))
        repo.initialize_database()
        admin = repo.create_user_record(username="rule-lab-admin", role="admin")
        regular = repo.create_user_record(username="rule-lab-user", role="user")
        other = repo.create_user_record(username="rule-lab-other-admin", role="admin")
        secret = "rule-lab-offline-fixture"
        class Presets:
            def list_auto_trigger_rules(self,*,user_id):
                if user_id!=admin['id']:return []
                return [dict(id=6,user_id=user_id,name='组合连错参考',scope_mode='selected_subscriptions',subscription_ids=[7],cooldown_issues=10,
                    daily_risk_control=dict(enabled=True,profit_target=20,loss_limit=20,hit_rate_gate=dict(enabled=True,threshold=25,min_settled_count=20)),
                    cycle_policy={'mode':'legacy'},routes=[dict(id=9,name='参考路由',route_risk_mode='inherit_rule',subscription_risk_mode='override',
                        subscription_risk_control=dict(enabled=True,profit_target=10,loss_limit=10))])]
            def list_subscriptions(self,user_id):
                return [dict(id=7,user_id=user_id,name='参考方案（不同来源）',strategy_v2=dict(play_filter=dict(mode='all'),
                    staking_policy=dict(mode='fixed',fixed_amount=9),risk_control=dict(enabled=True,profit_target=5,loss_limit=5)))] if user_id==admin['id'] else []
        from pc28touzhu.services.rule_lab_service import RuleLabService
        app = PlatformApiApplication(repo, "offline-only", secret,rule_lab_service=RuleLabService(platform_repository=Presets()))

        def local_app(environ, start_response):
            if environ.get('PATH_INFO')=='/__fixture/study-tick' and environ.get('REQUEST_METHOD')=='POST':
                from pc28touzhu.rule_lab_worker import RuleLabWorker
                result=RuleLabWorker(app.rule_lab_service.store,app.rule_lab_service.source).tick()
                start_response('200 OK',[('Content-Type','application/json')]);return [json.dumps(result).encode()]
            # Test-only helper uses the real independent ledger and production settlement.
            if environ.get("PATH_INFO") == "/__fixture/settle" and environ.get("REQUEST_METHOD") == "POST":
                from pc28touzhu.domain.rule_lab import settle_bet
                from pc28touzhu.services.rule_lab_store import RuleLabStore
                body = json.loads(environ["wsgi.input"].read(int(environ.get("CONTENT_LENGTH") or 0)))
                store = RuleLabStore(root / "lab" / "rule_lab.db")
                store.initialize()
                batch_id = int(body["batch_id"])
                work = store.get_work_item(batch_id)
                now = datetime.now(timezone.utc).replace(microsecond=0)
                for i, balls in enumerate(([3, 6, 8], [1, 1, 3], [2, 4, 8])):
                    work = store.get_work_item(batch_id)
                    timestamp = (now - timedelta(minutes=20 - i * 4)).isoformat().replace("+00:00", "Z")
                    decided = (now - timedelta(minutes=22 - i * 4)).isoformat().replace("+00:00", "Z")
                    decisions, outcomes = [], []
                    for exp in work["experiments"][:2]:
                        issue = str(1001 + i)
                        decisions.append(dict(experiment_id=exp["id"], issue_no=issue, bet_value="单",
                            stake_cents=100, decided_at=decided, decision_mode="live",
                            feature_issue_no=str(1000 + i), condition_value=1,condition_matched=True,
                            risk_date=datetime.fromisoformat(timestamp.replace('Z','+00:00')).astimezone(timezone(timedelta(hours=8))).date().isoformat()))
                        result = settle_bet(exp["settlement_snapshot"], "odd_even", "单", 100, balls)
                        outcomes.append(dict(experiment_id=exp["id"], issue_no=issue, status="settled",
                                             settled_at=timestamp, skip_reason="", **result))
                    store.commit_step(batch_id, work["batch"]["revision"], cursor={"last_issue_no": str(1001 + i)},
                                      now=timestamp, decisions=decisions, outcomes=outcomes,
                                      progress={"processed_issues_delta": 1})
                start_response("200 OK", [("Content-Type", "application/json")])
                return [b'{"ok":true}']
            # No other test writes reach unrelated project configuration or execution routes.
            if environ.get("REQUEST_METHOD") != "GET" and not environ.get("PATH_INFO", "").startswith("/api/platform/rule-lab/"):
                start_response("403 Forbidden", [("Content-Type", "application/json")])
                return [b'{"error":"fixture permits lab configuration only"}']
            return app(environ, start_response)

        with make_server("127.0.0.1", 0, local_app, handler_class=QuietHandler) as server:
            print(json.dumps({"url": "http://127.0.0.1:%s" % server.server_port,
                              "cookie_name": SESSION_COOKIE_NAME,
                              "cookie": build_session_cookie_value(admin["id"], secret),
                              "regular_cookie": build_session_cookie_value(regular["id"], secret),
                              "other_cookie": build_session_cookie_value(other["id"], secret)}), flush=True)
            threading.Thread(target=server.serve_forever, daemon=True).start()
            sys.stdin.read()
            server.shutdown()


if __name__ == "__main__":
    main()
