"""Same-input result/space measurements on temporary ledgers, including old code.

Run with PYTHONPATH selecting the implementation under measurement.  No source
or experiment production path is opened, and the benchmark records no long-term
per-bet trace.  Economic digests deliberately exclude rows the new policy removes.
"""
from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import resource
import sqlite3
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.test_rule_lab_source import SourceFixture, SHANGHAI, UTC, stamp


STATS_FIELDS = (
    'settled_count', 'hit_count', 'refund_count', 'miss_count', 'direction_hit_count',
    'stake_cents', 'net_cents', 'equity_peak_cents', 'max_drawdown_cents',
    'current_loss_streak', 'max_loss_streak', 'trigger_count', 'skipped_count',
)
RISK_FIELDS = (
    'round_seq', 'last_start_issue', 'active_round', 'last_stop_reason', 'latest_day',
    'completed_rounds', 'profit_rounds', 'loss_rounds', 'flat_rounds', 'invalid_rounds',
    'round_profit_stops', 'round_loss_stops',
)
DAY_FIELDS = (
    'experiment_id', 'stat_date', 'settled_count', 'hit_count', 'refund_count',
    'miss_count', 'direction_hit_count', 'stake_cents', 'net_cents', 'trigger_count',
    'skipped_count',
)
CURSOR_FIELDS = (
    'last_issue_no', 'scan_after_issue', 'replay_max_issue', 'next_issue',
    'simulation_time', 'replay_complete', 'replay_scheduler_version',
)


def canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


def sha_file(path):
    digest = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def economic_result(path):
    """Compare authority and pending obligations, not removed audit history."""
    result = dict(experiments=[], days=[], batches=[], pending=[])
    with sqlite3.connect(path) as connection:
        connection.row_factory = sqlite3.Row
        for row in connection.execute('SELECT * FROM lab_experiments ORDER BY id'):
            stats, risk = json.loads(row['stats_json']), json.loads(row['risk_runtime_json'])
            result['experiments'].append(dict(id=row['id'], batch_id=row['batch_id'],
                condition_key=row['condition_key'], condition=json.loads(row['condition_json']),
                settlement_snapshot=json.loads(row['settlement_snapshot_json']),
                stats={key: stats[key] for key in STATS_FIELDS},
                risk={key: risk.get(key) for key in RISK_FIELDS},
                last_decided_issue=row['last_decided_issue'], last_settled_issue=row['last_settled_issue']))
        for row in connection.execute('SELECT * FROM lab_daily ORDER BY experiment_id,stat_date'):
            result['days'].append({key: row[key] for key in DAY_FIELDS})
        for row in connection.execute('SELECT * FROM lab_batches ORDER BY id'):
            cursor = json.loads(row['cursor_json'])
            result['batches'].append(dict(id=row['id'], user_id=row['user_id'], status=row['status'],
                processed_issues=row['processed_issues'], skip_counts=json.loads(row['skip_counts_json']),
                risk_policy=json.loads(row['risk_policy_json']),
                cursor={key: cursor.get(key) for key in CURSOR_FIELDS}))
        columns = ('experiment_id', 'issue_no', 'bet_value', 'stake_cents', 'decided_at',
                   'decision_mode', 'feature_issue_no', 'condition_value_json', 'round_no', 'risk_date')
        for row in connection.execute("SELECT * FROM lab_decisions WHERE status='pending' ORDER BY experiment_id,issue_key"):
            result['pending'].append({key: row[key] for key in columns})
    return result


def seed(path, issues):
    SourceFixture(path)
    first = datetime(2026, 11, 25, 16, tzinfo=UTC)
    draws, predictions = [], []
    for offset in range(-100, issues):
        issue = str(5000000 + offset)
        opened = first + timedelta(seconds=210 * offset)
        balls = [(offset * 7 + 11) % 10, (offset * 3 + 8) % 10, (offset * 9 + 4) % 10]
        big = '大' if (offset // 7) % 2 else '小'
        odd = '单' if (offset // 5) % 2 else '双'
        draws.append(('pc28', issue, opened.astimezone(SHANGHAI).strftime('%Y-%m-%d %H:%M:%S'),
                      sum(balls), json.dumps({'number': '+'.join(map(str, balls))}),
                      (opened + timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')))
        predictions.append((1, 'pc28', issue, big, odd, big + odd, 'settled',
                            (opened - timedelta(seconds=120)).strftime('%Y-%m-%d %H:%M:%S'),
                            (opened + timedelta(seconds=15)).strftime('%Y-%m-%d %H:%M:%S')))
    with sqlite3.connect(path) as connection:
        connection.executemany('''INSERT INTO lottery_draws(lottery_type,issue_no,open_time,result_number,
            source_payload,created_at) VALUES(?,?,?,?,?,?)''', draws)
        connection.executemany('''INSERT INTO predictions(predictor_id,lottery_type,issue_no,prediction_big_small,
            prediction_odd_even,prediction_combo,status,created_at,settled_at) VALUES(?,?,?,?,?,?,?,?,?)''', predictions)
        connection.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
    return first


def policies():
    base = dict(version=1, mode='round',
        round=dict(enabled=True, profit_target=10, loss_limit=10, max_bets=0),
        day=dict(enabled=True, profit_target=20, loss_limit=20, max_bets=0, max_rounds=0,
                 hit_rate_gate=dict(enabled=False, threshold=0, min_settled_count=20)),
        additional_day_guards=[], cooldown_issues=0, source={})
    values = []
    for index in range(5):
        value = copy.deepcopy(base)
        if index == 0:
            value['day']['enabled'] = False
            value['round'].update(profit_target=2, loss_limit=3)
        elif index == 1:
            value['cooldown_issues'] = 10
            value['day'].update(profit_target=7, loss_limit=8)
        elif index == 2:
            value['round'].update(profit_target=100, loss_limit=100)
            value['day']['hit_rate_gate'].update(enabled=True, threshold=60, min_settled_count=8)
        elif index == 3:
            value['round'].update(profit_target=5, loss_limit=4, max_bets=5)
            value['day'].update(max_bets=25, max_rounds=4)
            value['cooldown_issues'] = 2
        else:
            value['round'].update(profit_target=1, loss_limit=20)
            value['day']['enabled'] = False
            extra = copy.deepcopy(base['day'])
            extra.update(profit_target=9, loss_limit=9)
            extra['hit_rate_gate'].update(enabled=True, threshold=35, min_settled_count=12)
            value['additional_day_guards'] = [extra]
            value['cooldown_issues'] = 5
        values.append(value)
    return values


def measure(root, versions, issues):
    source_path = root / 'source.db'
    first = seed(source_path, issues)
    source_hash = sha_file(source_path)
    now = stamp(first + timedelta(seconds=210 * issues, days=1))
    end = (first + timedelta(seconds=210 * (issues - 1))).astimezone(SHANGHAI).date().isoformat()
    source = RuleLabSource(source_path)
    store = RuleLabStore(root / 'lab' / 'lab.db')
    store.initialize()
    batches = []
    selected = (0, 1, 2, 3, 10, 11, 18, 19, 24, 25)
    for repeat in range(versions // 100):
        for number, risk in enumerate(policies()):
            for metric in ('big_small', 'odd_even'):
                owner = len(batches) // 50 + 1
                cfg = normalize_batch_payload(dict(name='matrix-%s-%s-%s' % (repeat, number, metric),
                    mode='backtest', predictor_id=1, metric=metric, stake_amount=1 if number % 2 == 0 else 10,
                    start_date='2026-11-26', end_date=end, risk_policy=risk), now=now)
                definitions = build_experiments(metric)
                batch = store.create_batch(owner, cfg, 'fixture', [definitions[index] for index in selected], now=now)
                batches.append((owner, batch['id']))
    initial_bytes = store.storage_status()['used_bytes']
    wall_start, cpu_start = time.perf_counter(), time.process_time()
    ticks, maximum_tick, peak_running = 0, 0, 0
    for start in range(0, len(batches), 10):
        group = batches[start:start + 10]
        for owner, batch_id in group:
            store.set_batch_state(owner, batch_id, 'running', now=now)
        worker = RuleLabWorker(store, source)
        for _ in range(2000):
            active = store.capacity()['running']
            peak_running = max(peak_running, active)
            assert active <= 100, active
            tick = worker.tick(now=now)
            ticks += 1
            maximum_tick = max(maximum_tick, tick['elapsed_ms'])
            states = [store.get_batch(owner, batch_id) for owner, batch_id in group]
            assert all(row['status'] not in ('error', 'resource_paused') for row in states), states
            if all(row['status'] == 'completed' for row in states):
                break
        else:
            raise AssertionError('matrix replay failed to finish within bounded ticks')
    replay_wall = time.perf_counter() - wall_start
    replay_cpu = time.process_time() - cpu_start
    # Measure post-replay maintenance separately. It must not silently disappear
    # from the performance cost, and an over-budget loop is not "complete".
    maintenance_start = time.perf_counter()
    maintenance_ticks = 0
    if callable(getattr(store, 'retention_maintenance', None)):
        maintenance_at = stamp(datetime.fromisoformat(now.replace('Z', '+00:00')) + timedelta(minutes=2))
        for _ in range(5000):
            outcome = store.retention_maintenance(now=maintenance_at, limit=500)
            maintenance_ticks += 1
            if not outcome['maintenance_pending']:
                break
        else:
            raise AssertionError('post-replay retention did not converge')
        # Existing maintenance performs the actual bounded incremental vacuum.
        for _ in range(5000):
            outcome = store.management_maintenance(now=maintenance_at, limit=500, max_pages=256)
            if not outcome['maintenance_pending']:
                break
        else:
            raise AssertionError('post-replay page reclaim did not converge')
    maintenance_wall = time.perf_counter() - maintenance_start
    economic = economic_result(store.db_path)
    assert all(row['status'] == 'completed' and row['processed_issues'] == issues for row in economic['batches'])
    assert len(economic['experiments']) == versions
    assert not economic['pending']
    assert sha_file(source_path) == source_hash
    with sqlite3.connect(store.db_path) as connection:
        counts = {
            'decisions_total': connection.execute('SELECT count(*) FROM lab_decisions').fetchone()[0],
            'terminal_decisions': connection.execute("SELECT count(*) FROM lab_decisions WHERE status!='pending'").fetchone()[0],
            'closed_rounds': connection.execute("SELECT count(*) FROM lab_rounds WHERE status='closed'").fetchone()[0],
            'daily': connection.execute('SELECT count(*) FROM lab_daily').fetchone()[0],
            'draws': connection.execute('SELECT count(*) FROM lab_draws').fetchone()[0],
            'predictions': connection.execute('SELECT count(*) FROM lab_predictions').fetchone()[0],
        }
    return dict(versions=versions, issues_per_batch=issues, policy_variants=5, metrics=['big_small', 'odd_even'],
        conditions_per_policy_metric=10, peak_running_versions=peak_running, economic_digest=hashlib.sha256(canonical(economic).encode()).hexdigest(),
        settled_count=sum(row['stats']['settled_count'] for row in economic['experiments']),
        net_cents=sum(row['stats']['net_cents'] for row in economic['experiments']),
        completed_rounds=sum(row['risk']['completed_rounds'] or 0 for row in economic['experiments']),
        replay_wall_seconds=round(replay_wall, 4), replay_cpu_seconds=round(replay_cpu, 4),
        post_maintenance_wall_seconds=round(maintenance_wall, 4), post_maintenance_ticks=maintenance_ticks,
        total_work_wall_seconds=round(replay_wall + maintenance_wall, 4),
        max_tick_ms=round(maximum_tick, 3), tick_count=ticks,
        rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        initial_store_bytes=initial_bytes, final_store_bytes=store.storage_status()['used_bytes'],
        source_unchanged=True, counts=counts,
        runtime_module=str(Path(__import__('pc28touzhu.services.rule_lab_store', fromlist=['__file__']).__file__).resolve()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--versions', type=int, choices=(100, 600), default=100)
    parser.add_argument('--issues', type=int, default=400)
    parser.add_argument('--output', type=Path)
    args = parser.parse_args()
    if not 1 <= args.issues <= 5000:
        parser.error('issues must be between 1 and 5000')
    if hasattr(os, 'sched_setaffinity'):
        os.sched_setaffinity(0, {min(os.sched_getaffinity(0))})
    resource.setrlimit(resource.RLIMIT_AS, (256 * 1024 * 1024, 256 * 1024 * 1024))
    with tempfile.TemporaryDirectory(prefix='rule-lab-results-benchmark-') as temporary:
        result = measure(Path(temporary), args.versions, args.issues)
    if args.output:
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2))
        args.output.chmod(0o600)
    print(json.dumps(result, ensure_ascii=False))


if __name__ == '__main__':
    main()
