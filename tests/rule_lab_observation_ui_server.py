"""Temporary real WSGI/SQLite reports for list-observation interaction tests.

The ledger is deliberately seeded with known daily outcomes: this tests stored
report arithmetic and UI behavior, not prediction or betting simulation.
"""
from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
import threading
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from wsgiref.simple_server import make_server

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from pc28touzhu.api.app import PlatformApiApplication
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments, empty_stats, normalize_batch_payload
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.rule_lab_ui_server import QuietHandler
from tests.test_rule_lab_resources import FakeResources
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_source import SourceFixture

NOW = '2026-11-28T04:00:00Z'


def month_days(month, count, net, days):
    counts = [count // days + (i < count % days) for i in range(days)]
    # First day dips before recovery; remaining daily gains sum exactly to net.
    tail = net + 200
    values = [-200] + [tail // (days - 1) + (i < tail % (days - 1)) for i in range(days - 1)]
    return [(f'2026-{month:02d}-{i+1:02d}', counts[i], values[i]) for i in range(days)]


def seed_reports(store, batch_ids):
    with sqlite3.connect(store.db_path) as c:
        for bid in batch_ids:
            cursor = dict(replay_scheduler_version=2, simulation_time='2026-11-27T15:59:00Z',
                retention_v1=dict(first_open_at='2026-10-01T00:05:00+08:00',
                    processed_open_at='2026-11-27T15:59:40Z', replay_cutoff_at=NOW, frontier_verified=True),
                evidence=dict(digest='fixture-same-inputs', count=23316, warm_count=100, first_issue='3488000', last_issue='3511315'))
            c.execute("UPDATE lab_batches SET status='completed',started_at=?,processed_issues=23316,cursor_json=? WHERE id=?",
                      ('2026-10-01T00:00:00Z', json.dumps(cursor), bid))
            ids = [r[0] for r in c.execute('SELECT id FROM lab_experiments WHERE batch_id=? ORDER BY id', (bid,))]
            for index, eid in enumerate(ids):
                category = index % 10
                count, days, first_net, last_net = 300, 15, 1200, 1350
                if category == 1: count, days, first_net, last_net = 200, 10, 599, 599
                elif category == 2: count, days, first_net, last_net = 199, 10, 1194, 1194
                elif category == 3: count, days, first_net, last_net = 300, 5, 1200, 1350
                elif category == 4: last_net = -600
                elif category == 5: count, days, first_net, last_net = 500, 10, 2000, 3002
                rows = month_days(10, count, first_net, days) + month_days(11, count, last_net, days)
                if category == 6: rows = rows[days:]
                stats = empty_stats()
                equity = peak = 0
                for stamp, n, net in rows:
                    hits, refunds = n // 2, 0
                    c.execute('''INSERT INTO lab_daily(experiment_id,stat_date,settled_count,hit_count,refund_count,miss_count,direction_hit_count,stake_cents,net_cents)
                        VALUES(?,?,?,?,?,?,?,?,?)''', (eid, stamp, n, hits, refunds, n-hits, hits, n*100, net))
                    stats['settled_count'] += n; stats['hit_count'] += hits; stats['miss_count'] += n-hits
                    stats['direction_hit_count'] += hits; stats['stake_cents'] += n*100; stats['net_cents'] += net
                    equity += net; peak = max(peak, equity); stats['max_drawdown_cents'] = max(stats['max_drawdown_cents'], peak-equity)
                stats['equity_peak_cents'] = peak; stats['max_loss_streak'] = 4
                c.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?', (json.dumps(stats), eid))
        c.execute("UPDATE lab_studies SET status='completed',cutoff_issue='3511315',cutoff_at=?", (NOW,))


def main():
    with tempfile.TemporaryDirectory(prefix='pc28-observation-ui-') as directory:
        root = Path(directory)
        fixture = SourceFixture(root / 'source.db')
        store = RuleLabStore(root / 'lab' / 'rule_lab.db')
        service = RuleLabService(store, RuleLabSource(fixture.path), resources=FakeResources())
        risk = policy(); risk['day']['hit_rate_gate'].update(enabled=True, threshold=25, min_settled_count=20)
        config = normalize_batch_payload(dict(name='观察列表 <img onerror=alert(1)>', mode='backtest', predictor_id=1,
            metric='big_small', start_date='2026-10-01', end_date='2026-11-27', risk_policy=risk), now=NOW)
        batch = store.create_batch(1, config, '本地测试来源', build_experiments('big_small'), now=NOW)
        study = service.create_study(1, dict(source_batch_id=batch['id'], name='五方案观察列表', request_id='observation-dom-study'), now=NOW)['study']
        children = [child['batch_id'] for child in study['children']]
        seed_reports(store, [batch['id']] + children)
        repo = DatabaseRepository(str(root / 'main.db')); repo.initialize_database()
        admin = repo.create_user_record(username='observation-admin', role='admin')
        secret = 'observation-dom-local-fixture'
        app = PlatformApiApplication(repo, 'offline-only', secret, rule_lab_service=service)
        with make_server('127.0.0.1', 0, app, handler_class=QuietHandler) as server:
            print(json.dumps(dict(url='http://127.0.0.1:' + str(server.server_port), cookie_name=SESSION_COOKIE_NAME,
                cookie=build_session_cookie_value(admin['id'], secret), batch_id=batch['id'], study_id=study['id'],
                child_ids=children)), flush=True)
            threading.Thread(target=server.serve_forever, daemon=True).start(); sys.stdin.read(); server.shutdown()


if __name__ == '__main__': main()
