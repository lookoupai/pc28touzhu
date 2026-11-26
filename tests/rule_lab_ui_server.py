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
        app = PlatformApiApplication(repo, "offline-only", secret)

        def local_app(environ, start_response):
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
                            feature_issue_no=str(1000 + i), condition_value=1))
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
