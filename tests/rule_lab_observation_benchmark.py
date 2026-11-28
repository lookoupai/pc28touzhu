"""Bounded observation-read benchmark and fixed-ledger semantic verification.

Usage: PYTHONPATH=src python3 -m tests.rule_lab_observation_benchmark \
    --database /tmp/readonly-snapshot.db --synthetic --output /tmp/result.json

The supplied database is opened read-only and never initialized. Synthetic
performance rows live in a temporary directory and are removed on exit.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import resource
import sqlite3
import tempfile
import time
from contextlib import contextmanager
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, empty_stats, normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabStore
from tests.test_rule_lab_risk import policy

NOW = '2026-11-28T04:00:00Z'


def readonly_connection(path):
    c = sqlite3.connect(Path(path).resolve().as_uri() + '?mode=ro', uri=True, timeout=2)
    c.row_factory = sqlite3.Row
    c.execute('PRAGMA query_only=ON')
    c.execute('PRAGMA cache_size=-4096')
    return c


def database_digest(path):
    """Stream table content through a stable digest; no whole-ledger JSON copy."""
    digest = hashlib.sha256()
    with readonly_connection(path) as c:
        c.execute('BEGIN')
        names = [r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        for name in names:
            assert name.replace('_', '').isalnum()
            digest.update(name.encode() + b'\n')
            for row in c.execute('SELECT * FROM "' + name + '" ORDER BY rowid'):
                values = [dict(blob_sha256=hashlib.sha256(v).hexdigest(), length=len(v)) if isinstance(v, bytes) else v for v in row]
                digest.update(json.dumps(values, ensure_ascii=False, separators=(',', ':')).encode() + b'\n')
    return digest.hexdigest()


def measure(store, action, repeats):
    queries = []
    original = store._connect
    @contextmanager
    def traced(*args, **kwargs):
        assert not kwargs.get('write', False), 'A benchmark read requested a write transaction'
        with original(*args, **kwargs) as c:
            c.set_trace_callback(queries.append)
            yield c
    store._connect = traced
    timings = []; sql_counts = []; day_queries = []; payload_bytes = []
    try:
        for _ in range(repeats):
            queries.clear()
            started = time.perf_counter()
            value = action()
            timings.append(round(time.perf_counter()-started, 6))
            sql_counts.append(len(queries))
            day_queries.append(sum('FROM LAB_DAILY' in q.upper() for q in queries))
            encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode()
            payload_bytes.append(len(encoded))
            rows = value.get('experiments', value.get('rows', []))
            for row in rows:
                if 'observation' in row:
                    observation = row['observation']
                    assert 'daily' not in observation and 'equity' not in observation
                    assert len(observation['months']) == observation['window']['requested_months']
            del encoded, value
    finally:
        store._connect = original
    return dict(seconds=timings, sql_statements=sql_counts, daily_queries=day_queries,
                json_bytes=payload_bytes)


def amount_cents(value):
    return None if value is None else int(Decimal(str(value))*100)


def fixed_snapshot(path, repeats):
    store = RuleLabStore(path)
    before = database_digest(path)
    with readonly_connection(path) as c:
        batches = [dict(r) for r in c.execute('SELECT id,user_id FROM lab_batches ORDER BY id')]
        studies = [dict(r) for r in c.execute('SELECT id,user_id FROM lab_studies ORDER BY id')]
    examples = {}
    checked = 0
    for batch in batches:
        result = store.batch_observation_detail(batch['user_id'], batch['id'], months=2, include_partial=True, now=NOW)
        with readonly_connection(path) as c:
            c.execute('BEGIN')
            raw_batch = c.execute('SELECT * FROM lab_batches WHERE id=?', (batch['id'],)).fetchone()
            for row in result['experiments']:
                raw = c.execute('SELECT * FROM lab_experiments WHERE id=?', (row['id'],)).fetchone()
                legacy = store._monthly_report(c, raw_batch, raw)
                expected_months = {r['month']:r for r in legacy['rows']}
                for month in row['observation']['months']:
                    expected = expected_months.get(month['month'])
                    if expected is None:
                        assert month['status'] == 'missing'
                        continue
                    for name in ('status', 'complete_month', 'settled_count', 'betting_days'):
                        assert month[name] == expected[name], (row['id'], month['month'], name, month[name], expected[name])
                    displayed_roi = round(month['roi_percent'], 2) if month['roi_percent'] is not None else None
                    assert displayed_roi == expected['roi_percent'], (row['id'], month['month'], 'displayed ROI')
                    for actual, expected_name in [('net_cents', 'net_profit'), ('stake_cents', 'stake_total'),
                                                  ('daily_close_drawdown_cents', 'daily_close_drawdown'), ('worst_day_cents', 'worst_day_profit')]:
                        assert month[actual] == amount_cents(expected[expected_name]), (row['id'], month['month'], actual)
                assert row['observation']['quality']['history_consistent']
                if row['id'] in (880, 1113, 1117):
                    examples[str(row['id'])] = row['observation']
                checked += 1
    benchmark = {}
    candidate = next((b for b in batches if b['id'] == 11), batches[0])
    uid, bid = candidate['user_id'], candidate['id']
    benchmark['batch92_legacy'] = measure(store, lambda:dict(batch=store.get_batch(uid,bid),experiments=store.list_experiments(uid,bid)), repeats)
    benchmark['batch92_observation'] = measure(store, lambda:store.batch_observation_detail(uid,bid,months=2,include_partial=True,now=NOW), repeats)
    if studies:
        candidate = next((s for s in studies if s['id']==2), studies[0]);uid,sid = candidate['user_id'],candidate['id']
        benchmark['study10_legacy'] = measure(store, lambda:store.study_comparison(uid,sid), repeats)
        benchmark['study10_observation'] = measure(store, lambda:store.study_comparison(uid,sid,observation_months=2,observation_partial=True,now=NOW), repeats)
    after = database_digest(path)
    assert after == before, 'Read-only fixed database changed'
    return dict(database_bytes=Path(path).stat().st_size, checked_experiments=checked,
                unchanged_sha256=before, examples=examples, benchmark=benchmark)


def synthetic_fixture(directory):
    store = RuleLabStore(Path(directory)/'ledger'/'lab.db');store.initialize()
    rules = build_experiments('combo', template_pack='basic_v2')
    keys = {(r['condition_key'],r['settlement_rule_id']) for r in rules}
    rules += [r for r in build_experiments('combo',template_pack='basic_v1')
              if (r['condition_key'],r['settlement_rule_id']) not in keys]
    assert len(rules) == 100
    risk = policy();risk['day']['hit_rate_gate'].update(enabled=True,threshold=25)
    config = normalize_batch_payload(dict(name='Synthetic observation resource fixture', mode='backtest',
        predictor_id=7, metric='combo', start_date='2026-09-01', end_date='2026-11-27',
        risk_policy=risk),now=NOW)
    parent = store.create_batch(1,config,'Synthetic source',rules,now=NOW)
    study = store.create_study(1,parent['id'],'Synthetic 500-version history','qa-observation-resource-1',now=NOW)['study']
    ids=[r['batch_id'] for r in study['children']]
    first,last=date(2025,12,1),date(2026,11,27)
    days=[];stats=empty_stats();balance=peak=drawdown=0
    day=first
    while day<=last:
        offset=(day-first).days;net=(400 if offset%7<4 else -300);count=10;stake=1000
        days.append((day.isoformat(),count,stake,net,count if net>0 else 0,count if net<0 else 0))
        stats['settled_count']+=count;stats['stake_cents']+=stake;stats['net_cents']+=net
        stats['hit_count']+=count if net>0 else 0;stats['miss_count']+=count if net<0 else 0
        balance+=net;peak=max(peak,balance);drawdown=max(drawdown,peak-balance)
        day+=timedelta(days=1)
    stats.update(equity_peak_cents=peak,max_drawdown_cents=drawdown)
    cursor=dict(replay_scheduler_version=2,replay_complete=True,
                retention_v1=dict(first_open_at=first.isoformat()+'T00:00:00+08:00',
                                  processed_open_at=last.isoformat()+'T23:59:00+08:00',replay_cutoff_at=NOW),
                evidence=dict(digest='synthetic-common-input',count=144000,warm_count=100,
                              first_issue='1000000',last_issue='1144000'))
    with sqlite3.connect(store.db_path) as c:
        # A retained year is a fixture representing accumulated history; the public
        # create endpoint still limits each new historical run to 90 days.
        for bid in ids:
            c.execute('UPDATE lab_batches SET start_date=?,status=?,started_at=?,processed_issues=144000,cursor_json=? WHERE id=?',
                      (first.isoformat(),'completed',first.isoformat()+'T00:00:00+08:00',json.dumps(cursor),bid))
            for eid, in c.execute('SELECT id FROM lab_experiments WHERE batch_id=?',(bid,)).fetchall():
                c.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?',(json.dumps(stats),eid))
                c.executemany('''INSERT INTO lab_daily(experiment_id,stat_date,settled_count,stake_cents,net_cents,hit_count,miss_count)
                                 VALUES(?,?,?,?,?,?,?)''',((eid,)+row for row in days))
        c.execute("UPDATE lab_studies SET status='completed',cutoff_issue='1144000',cutoff_at=? WHERE id=?",(NOW,study['id']))
    return store,ids[0],study['id'],len(days)*500


def synthetic(repeats):
    with tempfile.TemporaryDirectory(prefix='pc28-observation-resource-') as directory:
        store,bid,sid,days=synthetic_fixture(directory)
        before=database_digest(store.db_path)
        report=dict(experiments=500,daily_rows=days)
        report['batch100_legacy']=measure(store,lambda:dict(batch=store.get_batch(1,bid),experiments=store.list_experiments(1,bid)),repeats)
        report['batch100_12months']=measure(store,lambda:store.batch_observation_detail(1,bid,months=12,include_partial=True,now=NOW),repeats)
        report['study500_legacy']=measure(store,lambda:store.study_comparison(1,sid),repeats)
        report['study500_12months']=measure(store,lambda:store.study_comparison(1,sid,observation_months=12,observation_partial=True,now=NOW),repeats)
        assert database_digest(store.db_path)==before
        report['unchanged_sha256']=before
        return report


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',type=Path)
    parser.add_argument('--synthetic',action='store_true')
    parser.add_argument('--repeats',type=int,default=3)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    if not args.database and not args.synthetic:parser.error('Choose --database and/or --synthetic')
    if not 1<=args.repeats<=10:parser.error('repeats must be 1..10')
    if hasattr(os,'sched_setaffinity'):os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})
    resource.setrlimit(resource.RLIMIT_AS,(268435456,268435456))
    result=dict(constraints=dict(cpu_count=1,address_space_bytes=268435456),now=NOW)
    if args.database:result['fixed_snapshot']=fixed_snapshot(args.database.resolve(),args.repeats)
    if args.synthetic:result['synthetic']=synthetic(args.repeats)
    result['max_rss_kib']=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2));args.output.chmod(0o600)
    summary={k:v for k,v in result.items() if k not in ('fixed_snapshot','synthetic')}
    for key in ('fixed_snapshot','synthetic'):
        if key in result:
            value=result[key]
            summary[key]={k:v for k,v in value.items() if k!='examples'}
    print(json.dumps(summary,ensure_ascii=False))


if __name__=='__main__':main()
