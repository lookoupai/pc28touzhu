"""Bounded offline measurements; never opens production data or sends bets.

Run: PYTHONPATH=src python3 -m tests.rule_lab_benchmark --issues 400
Each size executes in a fresh subprocess. 500 saved experiments run in groups
of at most 100, preserving the real service limit rather than monkeypatching it.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import resource
import sqlite3
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timedelta
from pathlib import Path

from pc28touzhu.api.app import PlatformApiApplication, build_testing_environ
from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value
from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.test_rule_lab_source import SourceFixture, SHANGHAI, UTC, stamp


def seed_source(path, count):
    SourceFixture(path)
    first = datetime(2026, 11, 25, 16, tzinfo=UTC)  # Beijing 11/26 midnight.
    draws, predictions = [], []
    for offset in range(-100, count):
        issue = str(5000000 + offset)
        opened = first + timedelta(seconds=210 * offset)
        balls = [(offset * 7 + 11) % 10, (offset * 3 + 8) % 10, (offset * 9 + 4) % 10]
        # Blocks deliberately contain both streaks and mixed results; this is workload data, not alpha.
        big_small = '大' if (offset // 7) % 2 else '小'
        odd_even = '单' if (offset // 5) % 2 else '双'
        draws.append(('pc28', issue, opened.astimezone(SHANGHAI).strftime('%Y-%m-%d %H:%M:%S'),
                      sum(balls), json.dumps({'number': '+'.join(map(str, balls))}),
                      (opened + timedelta(seconds=10)).strftime('%Y-%m-%d %H:%M:%S')))
        predictions.append((1, 'pc28', issue, big_small, odd_even, big_small + odd_even, 'settled',
                            (opened - timedelta(seconds=120)).strftime('%Y-%m-%d %H:%M:%S'),
                            (opened + timedelta(seconds=15)).strftime('%Y-%m-%d %H:%M:%S')))
    with sqlite3.connect(path) as connection:
        connection.executemany('''INSERT INTO lottery_draws(lottery_type,issue_no,open_time,result_number,
            source_payload,created_at) VALUES(?,?,?,?,?,?)''', draws)
        connection.executemany('''INSERT INTO predictions(predictor_id,lottery_type,issue_no,prediction_big_small,
            prediction_odd_even,prediction_combo,status,created_at,settled_at) VALUES(?,?,?,?,?,?,?,?,?)''', predictions)


def percentiles(values):
    ordered = sorted(values)
    return {key: round(ordered[max(0, math.ceil(len(ordered) * fraction) - 1)], 3)
            for key, fraction in [('p50_ms', .5), ('p95_ms', .95), ('p99_ms', .99)]}


def measure_api(root, store, source, batch_id):
    """Actual WSGI handlers plus SQLite session/user lookup, without network overhead."""
    repo = DatabaseRepository(str(root / 'platform-fixture.db'))
    repo.initialize_database()
    user = repo.create_user_record(username='bench-admin', role='admin')
    if user['id'] != 1:
        raise AssertionError('unexpected owner fixture')
    app = PlatformApiApplication(repo, 'offline-benchmark-only', 'offline-bench-session',
                                  rule_lab_service=RuleLabService(store, source))
    cookie = '%s=%s' % (SESSION_COOKIE_NAME, build_session_cookie_value(1, 'offline-bench-session'))
    results = {}
    for label, endpoint in [('overview', '/api/platform/rule-lab'),
                            ('batch_results', '/api/platform/rule-lab/batches/%d' % batch_id)]:
        latencies = []
        for i in range(110):
            captured = {}
            def start_response(status, headers):
                captured['status'] = status
            env = build_testing_environ(endpoint, headers={'HTTP_COOKIE': cookie})
            begin = time.perf_counter()
            body = b''.join(app(env, start_response))
            elapsed_ms = (time.perf_counter() - begin) * 1000
            if captured['status'] != '200 OK' or not json.loads(body):
                raise AssertionError('WSGI result query failed')
            if i >= 10:
                latencies.append(elapsed_ms)
        results[label] = {'samples': len(latencies), **percentiles(latencies)}
    return results


def run_case(experiments, issues):
    now = '2026-11-26T16:00:00Z'
    with tempfile.TemporaryDirectory(prefix='rule-lab-benchmark-') as directory:
        root = Path(directory)
        source_path = root / 'source.db'
        seed_source(source_path, issues)
        source_hash = hashlib.sha256(source_path.read_bytes()).hexdigest()
        source = RuleLabSource(source_path)
        store = RuleLabStore(root / 'lab' / 'rule_lab.db')
        store.initialize()
        batches = []
        create_start = time.perf_counter()
        for index in range(experiments // 50):
            config = normalize_batch_payload(dict(name='负载样本 %d' % index, mode='backtest',
                predictor_id=1, metric='big_small', start_date='2026-11-26', end_date='2026-11-26'), now=now)
            batches.append(store.create_batch(1, config, '本地基准来源', build_experiments('big_small'), now=now))
        creation_seconds = time.perf_counter() - create_start
        initial_bytes = store.storage_status()['used_bytes']
        started, cpu_started = time.perf_counter(), time.process_time()
        ticks, tick_ms, peak_active = 0, [], 0
        for group_start in range(0, len(batches), 2):
            group = batches[group_start:group_start + 2]
            for batch in group:
                store.set_batch_state(1, batch['id'], 'running', now=now)
            worker = RuleLabWorker(store, source, page_size=200)
            for _ in range(1000):
                running = [b for b in store.list_work_batches() if b['status'] == 'running']
                active_count = sum(store.get_batch(b['user_id'], b['id'])['experiment_count'] for b in running)
                peak_active = max(peak_active, active_count)
                if active_count > 100:
                    raise AssertionError('real active quota exceeded')
                result = worker.tick(now=now)
                ticks += 1
                tick_ms.append(result['elapsed_ms'])
                states = [store.get_batch(1, batch['id']) for batch in group]
                if any(b['status'] in ('error', 'resource_paused') for b in states):
                    raise AssertionError(states)
                if all(b['status'] == 'completed' for b in states):
                    break
            else:
                raise AssertionError('bounded benchmark did not complete')
        cpu_seconds = time.process_time() - cpu_started
        wall_seconds = time.perf_counter() - started
        final_bytes = store.storage_status()['used_bytes']
        with sqlite3.connect(store.db_path) as connection:
            ledger_counts = {table: connection.execute('SELECT count(*) FROM ' + table).fetchone()[0]
                             for table in ['lab_decisions', 'lab_daily', 'lab_draws', 'lab_predictions']}
        for batch in batches:
            baseline = store.list_experiments(1, batch['id'])[0]
            if baseline['settled_count'] != issues:
                raise AssertionError(('missing baseline settlements', baseline['settled_count'], issues))
        if hashlib.sha256(source_path.read_bytes()).hexdigest() != source_hash:
            raise AssertionError('source fixture was mutated')
        replay_peak_rss_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        api = measure_api(root, store, source, batches[0]['id'])
        return dict(saved_experiments=experiments, peak_running_experiments=peak_active,
                    issues_per_batch=issues, creation_seconds=round(creation_seconds, 3),
                    replay_wall_seconds=round(wall_seconds, 3), replay_cpu_seconds=round(cpu_seconds, 3),
                    replay_peak_rss_kib=replay_peak_rss_kib, total_peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                    initial_lab_bytes=initial_bytes, final_lab_bytes=final_bytes,
                    growth_bytes=final_bytes - initial_bytes, ticks=ticks,
                    tick_max_ms=round(max(tick_ms), 3), counts=ledger_counts, api=api,
                    source_unchanged=True,
                    measurement='offline temporary SQLite; no cgroup throttling; 500 run in <=100 groups')


def run_live_case(experiments, cycles=20):
    """Actual live tick path: 100 historical rows, pending capture, then draw settlement."""
    with tempfile.TemporaryDirectory(prefix='rule-lab-live-benchmark-') as directory:
        root = Path(directory)
        source_path = root / 'source.db'
        seed_source(source_path, 1)
        fixture = SourceFixture.__new__(SourceFixture)
        fixture.path = source_path
        source = RuleLabSource(source_path)
        store = RuleLabStore(root / 'lab' / 'rule_lab.db')
        store.initialize()
        opened = datetime(2026, 11, 25, 16, tzinfo=UTC)
        now = stamp(opened + timedelta(seconds=60))
        batches = []
        for index in range(experiments // 50):
            config = normalize_batch_payload(dict(name='前向负载 %d' % index, mode='live',
                                                   predictor_id=1, metric='big_small'), now=now)
            batches.append(store.create_batch(1, config, '本地基准来源', build_experiments('big_small'), now=now))
        for batch in batches[:2]:
            store.set_batch_state(1, batch['id'], 'running', now=now)
        initial_bytes = store.storage_status()['used_bytes']
        worker = RuleLabWorker(store, source)
        capture_ms, settlement_ms, idle_ms, cpu_seconds = [], [], [], 0.0
        def measure(now, timings):
            nonlocal cpu_seconds
            before = hashlib.sha256(source_path.read_bytes()).hexdigest()
            start, cpu_start = time.perf_counter(), time.process_time()
            result = worker.tick(now=stamp(now))
            cpu_seconds += time.process_time() - cpu_start
            timings.append((time.perf_counter() - start) * 1000)
            if result['state'] in ('error', 'resource_paused'):
                raise AssertionError(result)
            if hashlib.sha256(source_path.read_bytes()).hexdigest() != before:
                raise AssertionError('live worker mutated source')
        for index in range(cycles):
            issue = 5000001 + index
            target_open = opened + timedelta(seconds=210)
            fixture.prediction(issue, target_open, status='pending', created=opened + timedelta(seconds=30))
            measure(opened + timedelta(seconds=60), capture_ms)
            fixture.draw(issue, target_open, (3, 6, 8) if index % 3 else (1, 3, 5))
            fixture.execute("UPDATE predictions SET status='settled',settled_at=? WHERE issue_no=?",
                            ((target_open + timedelta(seconds=15)).strftime('%Y-%m-%d %H:%M:%S'), str(issue)))
            measure(target_open + timedelta(seconds=30), settlement_ms)
            opened = target_open
        for index in range(20):
            measure(opened + timedelta(seconds=45 + index * 15), idle_ms)
        for batch in batches[:2]:
            if store.list_experiments(1, batch['id'])[0]['settled_count'] != cycles:
                raise AssertionError('live baseline missed an observed period')
        return dict(saved_experiments=experiments, running_experiments=min(experiments, 100),
                    observed_periods=cycles, capture_tick={'samples': cycles, **percentiles(capture_ms)},
                    settlement_tick={'samples': cycles, **percentiles(settlement_ms)},
                    idle_tick={'samples': len(idle_ms), **percentiles(idle_ms)},
                    cpu_seconds=round(cpu_seconds, 4), peak_rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                    initial_lab_bytes=initial_bytes, final_lab_bytes=store.storage_status()['used_bytes'],
                    source_unchanged_by_worker=True,
                    measurement='virtual-clock live capture/settlement on temporary SQLite, no cgroup throttling')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--issues', type=int, default=400)
    parser.add_argument('--child', type=int, choices=[50, 100, 500])
    parser.add_argument('--live-only', action='store_true')
    arguments = parser.parse_args()
    if not 1 <= arguments.issues <= 400:
        parser.error('issues must be between 1 and 400')
    if arguments.child:
        result = run_live_case(arguments.child) if arguments.live_only else run_case(arguments.child, arguments.issues)
        print(json.dumps(result, ensure_ascii=False))
        return
    measurements = []
    for count in (50, 100, 500):
        command = [sys.executable, '-m', 'tests.rule_lab_benchmark', '--child', str(count),
                   '--issues', str(arguments.issues)]
        if arguments.live_only:
            command.append('--live-only')
        result = subprocess.run(command, capture_output=True, text=True, timeout=240)
        if result.returncode:
            raise SystemExit('benchmark %d failed: %s' % (count, result.stderr[-3000:]))
        measurements.append(json.loads(result.stdout))
    print(json.dumps({'cases': measurements}, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
