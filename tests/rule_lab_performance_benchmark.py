"""Offline 100-version round-risk replay measurement on one CPU, with ledger digest."""
from __future__ import annotations

import argparse
import cProfile
import hashlib
import json
import os
import pstats
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
from tests.rule_lab_benchmark import seed_source
from tests.test_rule_lab_risk import policy
from tests.test_rule_lab_source import SourceFixture, stamp


def ledger_result(store):
    result = {}
    with sqlite3.connect(store.db_path) as connection:
        for table, columns, order in (
            ('lab_experiments', 'id,stats_json,risk_runtime_json,last_decided_issue,last_settled_issue', 'id'),
            ('lab_decisions', 'experiment_id,issue_no,bet_value,stake_cents,decided_at,status,settled_at,result_type,net_cents,refund_reason,skip_reason,round_no,risk_date', 'experiment_id,issue_key'),
            ('lab_daily', '*', 'experiment_id,stat_date'),
            ('lab_risk_days', '*', 'experiment_id,stat_date'),
            ('lab_rounds', 'experiment_id,round_no,status,summary_json', 'experiment_id,round_no'),
        ):
            result[table] = connection.execute('SELECT '+columns+' FROM '+table+' ORDER BY '+order).fetchall()
        result['batches'] = connection.execute('SELECT id,status,processed_issues,skip_counts_json FROM lab_batches ORDER BY id').fetchall()
    digest = hashlib.sha256(json.dumps(result, ensure_ascii=False, separators=(',',':')).encode()).hexdigest()
    return digest, {key:len(value) for key,value in result.items()}, result


def run_case(root, issues=400, *, profile=None, scenario='paired'):
    source_path = root/'source.db'
    seed_source(source_path, issues)
    source = RuleLabSource(source_path)
    store = RuleLabStore(root/'lab'/'lab.db');store.initialize()
    now = (datetime(2026,11,25,16,tzinfo=timezone.utc)+timedelta(seconds=210*issues,days=1)).isoformat().replace('+00:00','Z')
    end = (datetime(2026,11,26)+timedelta(seconds=210*(issues-1))).date().isoformat()
    risk=policy(cooldown_issues=10);risk['day']['hit_rate_gate'].update(enabled=True,threshold=25,min_settled_count=20)
    metrics=('combo',) if scenario=='combo' else ('big_small','odd_even')
    batch_ids=[]
    for metric in metrics:
        cfg=normalize_batch_payload(dict(name='profile-'+metric,mode='backtest',predictor_id=1,metric=metric,
            start_date='2026-11-25' if scenario=='continuous' else '2026-11-26',
            end_date='2026-11-25' if scenario=='continuous' else end,stake_amount=1,risk_policy=risk),now=now)
        batch=store.create_batch(1,cfg,'fixture',build_experiments(metric),now=now)
        store.set_batch_state(1,batch['id'],'running',now=now)
        batch_ids.append(batch['id'])
    worker=RuleLabWorker(store,source)
    if scenario=='continuous':
        # A completed previous day supplies the real 100-period warm boundary.
        for _ in range(200):
            worker.tick(now=now)
            if all(store.get_batch(1,bid)['status']=='completed' for bid in batch_ids):break
        fixture=SourceFixture.__new__(SourceFixture);fixture.path=source_path
        opened=datetime.fromisoformat(end).replace(tzinfo=timezone(timedelta(hours=8)))+timedelta(days=1,minutes=2)
        fixture.draw(5000000+issues,opened);fixture.prediction(5000000+issues,opened)
        with sqlite3.connect(source_path) as connection:
            connection.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        for bid in batch_ids:
            preview=store.continuation_preview(1,'batch',bid,now=now)
            assert preview['eligible'],preview
            store.set_continuation(1,'batch',bid,dict(request_id='benchmark-enable-'+str(bid),
                expected_revision=preview['expected_revision'],capture_enabled=True,compute_enabled=False,
                daily_at='00:10',timezone='Asia/Shanghai'),now=now)
        for index in range(200):
            RuleLabWorker(store,source).tick(now=now)
            if all(int(store.continuation_preview(1,'batch',bid,now=now)['continuation']['captured_through_issue'] or 0)>=5000000+issues for bid in batch_ids):break
        for bid in batch_ids:
            preview=store.continuation_preview(1,'batch',bid,now=now)
            store.request_append(1,'batch',bid,dict(request_id='benchmark-append-'+str(bid),
                expected_revision=preview['expected_revision'],end_date=end),now=now)
        store.prepare_due_appends(now=now)
        worker=RuleLabWorker(store,source)
    profiler=cProfile.Profile() if profile else None
    if profiler:profiler.enable()
    wall_start,cpu_start=time.perf_counter(),time.process_time()
    ticks=[]
    for _ in range(2000):
        result=worker.tick(now=now);ticks.append(result['elapsed_ms'])
        if not any(b['status']=='running' for b in store.list_work_batches()):break
    else:raise AssertionError('bounded replay did not finish')
    elapsed,cpu=time.perf_counter()-wall_start,time.process_time()-cpu_start
    if profiler:
        profiler.disable();profiler.dump_stats(str(profile))
        with Path(str(profile)+'.txt').open('w') as output:pstats.Stats(profiler,stream=output).strip_dirs().sort_stats('cumtime').print_stats(35)
    digest,rows,ledger=ledger_result(store)
    expected=issues+(100 if scenario=='continuous' else 0)
    assert all(row[1]=='completed' and row[2]==expected for row in ledger['batches']),ledger['batches']
    return dict(scenario=scenario,issues_per_batch=issues,versions=sum(len(build_experiments(metric)) for metric in metrics),wall_seconds=round(elapsed,4),cpu_seconds=round(cpu,4),
        processed_periods=len(metrics)*issues,periods_per_second=round(len(metrics)*issues/elapsed,3),tick_count=len(ticks),
        max_tick_ms=max(ticks),rss_kib=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
        ledger_digest=digest,rows=rows,store_bytes=store.storage_status()['used_bytes'])


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--issues',type=int,default=400)
    parser.add_argument('--profile',type=Path)
    parser.add_argument('--scenario',choices=('paired','combo','continuous'),default='paired')
    parser.add_argument('--output',type=Path)
    args=parser.parse_args()
    if hasattr(os,'sched_setaffinity'):os.sched_setaffinity(0,{min(os.sched_getaffinity(0))})
    resource.setrlimit(resource.RLIMIT_AS,(256*1024*1024,256*1024*1024))
    with tempfile.TemporaryDirectory(prefix='rule-lab-perf-') as directory:
        result=run_case(Path(directory),args.issues,profile=args.profile,scenario=args.scenario)
    if args.output:args.output.write_text(json.dumps(result,ensure_ascii=False,indent=2))
    print(json.dumps(result,ensure_ascii=False))


if __name__=='__main__':main()
