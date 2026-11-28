"""Batch observations on the caller's single read-only SQLite snapshot."""
from __future__ import annotations

import copy
import json
from collections import defaultdict
from datetime import date, timedelta

from pc28touzhu.domain.rule_lab_monthly import report_window
from pc28touzhu.domain.rule_lab_observation import build_observation, observation_window


def load_observations(store, connection, batches, experiments, *, months, include_partial, now,
                      study_comparable=True, continuations=None, window=None):
    """Read daily totals twice (bounded rows + grouped audit), never once per row."""
    if not connection.in_transaction:
        raise ValueError('观察统计必须在同一只读事务中读取')
    if not batches or not experiments:
        return {}
    from pc28touzhu.services.rule_lab_series import public_summary
    continuations = continuations or {}
    contexts = {}
    issue_keys = set()
    for batch in batches:
        internal = store._internal_batch(batch)
        continuation = continuations.get(batch['id'])
        if continuation is None:
            continuation = public_summary(connection, batch)
        internal['continuation'] = copy.deepcopy(continuation)
        if continuation.get('enabled'):
            internal['end_date'] = continuation.get('effective_end_date') or internal['end_date']
        cursor = internal['cursor']
        for issue in (cursor.get('last_issue_no'), cursor.get('replay_max_issue'),
                      cursor.get('warm_before_issue') or (cursor.get('evidence') or {}).get('first_issue')):
            if issue:
                issue_keys.add(int(issue))
        contexts[batch['id']] = dict(internal=internal, legacy_engine=store._replay_review_required(batch))
    owner_end = max(report_window(item['internal'], now=now)[1] for item in contexts.values())
    window = copy.deepcopy(window) if window is not None else observation_window(owner_end.isoformat(), months=months, include_partial=include_partial, now=now)
    end = date.fromisoformat(window['end_date'])
    first = min(date.fromisoformat(window['start_date']), end-timedelta(days=27))
    ids = [experiment['id'] for experiment in experiments]
    marks = ','.join('?' for _ in ids)
    by_experiment = defaultdict(list)
    for row in connection.execute('''SELECT experiment_id,stat_date,settled_count,hit_count,refund_count,
        miss_count,stake_cents,net_cents FROM lab_daily WHERE experiment_id IN ('''+marks+''')
        AND stat_date>=? AND stat_date<=? ORDER BY experiment_id,stat_date''', ids+[first.isoformat(), end.isoformat()]):
        by_experiment[row['experiment_id']].append(dict(row))
    totals = {row['experiment_id']: dict(row) for row in connection.execute('''SELECT experiment_id,
        SUM(settled_count) AS settled_count,SUM(stake_cents) AS stake_cents,SUM(net_cents) AS net_cents,
        MAX(stat_date) AS last_date FROM lab_daily WHERE experiment_id IN ('''+marks+''') GROUP BY experiment_id''', ids)}
    pending = defaultdict(dict)
    for row in connection.execute('''SELECT experiment_id,
        COALESCE(risk_date,date(datetime(decided_at,'+8 hours'))) AS target_date,COUNT(*) AS n
        FROM lab_decisions WHERE status='pending' AND experiment_id IN ('''+marks+''')
        GROUP BY experiment_id,target_date''', ids):
        if row['target_date']:
            pending[row['experiment_id']][row['target_date']] = row['n']
    opened = {}
    if issue_keys:
        keys = sorted(issue_keys)
        opened = {row['issue_key']: row['open_at'] for row in connection.execute(
            'SELECT issue_key,open_at FROM lab_draws WHERE issue_key IN ('+','.join('?' for _ in keys)+')', keys)}
    batch_ids = list(contexts)
    cutoffs = {row['batch_id']: row['cutoff_at'] for row in connection.execute('''SELECT sb.batch_id,s.cutoff_at
        FROM lab_study_batches sb JOIN lab_studies s ON s.id=sb.study_id
        WHERE sb.batch_id IN ('''+','.join('?' for _ in batch_ids)+')', batch_ids)}
    for bid, context in contexts.items():
        internal = context['internal']
        cursor = internal['cursor']
        frontier = cursor.get('retention_v1') or {}
        def open_time(issue):
            return opened.get(int(issue)) if issue else None
        observed = frontier.get('processed_open_at') or open_time(cursor.get('last_issue_no'))
        if not observed and cursor.get('simulation_time'):
            from pc28touzhu.domain.rule_lab_monthly import local_time
            observed = (local_time(cursor['simulation_time']) + timedelta(seconds=40)).isoformat()
        context.update(observed_at=observed,
            cutoff_at=cutoffs.get(bid) or frontier.get('replay_cutoff_at') or open_time(cursor.get('replay_max_issue')),
            first_observed_at=frontier.get('first_open_at') or open_time(cursor.get('warm_before_issue') or (cursor.get('evidence') or {}).get('first_issue')))
    result = {}
    for experiment in experiments:
        eid = experiment['id']
        context = contexts[experiment['batch_id']]
        aggregate = totals.get(eid, {})
        observed = context['observed_at']
        if not observed and aggregate.get('last_date'):
            # Match legacy report fallback: this is activity, never proof the day finished.
            observed = aggregate['last_date']+'T00:00:00+08:00'
        result[eid] = build_observation(context['internal'], json.loads(experiment['stats_json']),
            by_experiment[eid], window=window, now=now, observed_at=observed,
            cutoff_at=context['cutoff_at'], first_observed_at=context['first_observed_at'],
            pending_days=pending[eid], history_totals=aggregate,
            legacy_engine=context['legacy_engine'], study_comparable=study_comparable)
    return result
