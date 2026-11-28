"""Bounded list observations from durable daily totals; no replay or source reads."""
from __future__ import annotations

from datetime import date, datetime, timedelta

from pc28touzhu.domain.rule_lab_monthly import (
    SHANGHAI, local_time, month_date, month_index, monthly_report, report_window,
)

MAX_OBSERVATION_MONTHS = 12


def observation_window(end_date, *, months, include_partial, now):
    if type(months) is not int or not 1 <= months <= MAX_OBSERVATION_MONTHS:
        raise ValueError('观察月份须为1～12的整数')
    if type(include_partial) is not bool:
        raise ValueError('月份口径必须为布尔值')
    today = local_time(now).date()
    end = min(date.fromisoformat(end_date), today)
    last = month_index(end)
    if not include_partial:
        month_end = month_date(last + 1) - timedelta(days=1)
        if month_end > end or month_end >= today:
            last -= 1
        end = month_date(last + 1) - timedelta(days=1)
    start = month_date(last - months + 1)
    return dict(requested_months=months, include_partial=include_partial,
                start_month=start.strftime('%Y-%m'), end_month=month_date(last).strftime('%Y-%m'),
                start_date=start.isoformat(), end_date=end.isoformat())


def _roi(net, stake):
    return round(net / stake * 100, 6) if stake else None


def _coverage(batch, *, now, observed_at, cutoff_at, first_observed_at):
    start, end, _ = report_window(batch, now=now)
    observed = local_time(observed_at)
    cutoff = local_time(cutoff_at)
    first = local_time(first_observed_at)
    actual_start = max(start, first.date()) if first else start
    # A later first record leaves that morning unobserved. The normal first
    # scheduled draw on the configured start date follows existing report semantics.
    if first and first.date() > start and first.time() != datetime.min.time():
        actual_start = first.date() + timedelta(days=1)
    if batch['mode'] == 'live':
        started = local_time(batch.get('started_at') or batch.get('created_at'))
        if started and started.time() != datetime.min.time():
            actual_start = max(actual_start, started.date() + timedelta(days=1))
    if not batch.get('processed_issues') or not observed:
        return actual_start, None, None
    actual = min(end, observed.date())
    # A frontier during a day proves activity, but not coverage of its remainder.
    covered = observed.date() - timedelta(days=1)
    if batch['status'] == 'completed' and cutoff:
        covered = max(covered, min(end, cutoff.date() - timedelta(days=1)))
    continuation = batch.get('continuation') or {}
    if continuation.get('enabled') and continuation.get('completed_through_date'):
        covered = max(covered, date.fromisoformat(continuation['completed_through_date']))
    covered = min(covered, end, local_time(now).date() - timedelta(days=1))
    return actual_start, actual, covered


def _period(daily, pending_days, start, end, *, actual_start, covered):
    available = bool(covered and start >= actual_start and end <= covered
                     and not any(start.isoformat() <= stamp <= end.isoformat() and count
                                 for stamp, count in pending_days.items()))
    result = dict(start_date=start.isoformat(), end_date=end.isoformat(), available=available,
                  settled_count=None, stake_cents=None, net_cents=None, roi_percent=None)
    if available:
        selected = [row for row in daily if start.isoformat() <= row['stat_date'] <= end.isoformat()]
        count = sum(row['settled_count'] for row in selected)
        stake = sum(row['stake_cents'] for row in selected)
        net = sum(row['net_cents'] for row in selected)
        result.update(settled_count=count, stake_cents=stake, net_cents=net, roi_percent=_roi(net, stake))
    return result


def _underwater(daily, start, end):
    by_day = {row['stat_date']: row['net_cents'] for row in daily}
    equity = peak = longest = current = 0
    last_peak = start - timedelta(days=1)
    stamp = start
    while stamp <= end:
        equity += by_day.get(stamp.isoformat(), 0)
        if equity >= peak:
            if current:
                longest = max(longest, (stamp - last_peak).days)
            peak = equity
            last_peak = stamp
            current = 0
        else:
            current = (stamp - last_peak).days
            longest = max(longest, current)
        stamp += timedelta(days=1)
    return longest, current


def build_observation(batch, stats, daily, *, window, now, observed_at=None, cutoff_at=None,
                      first_observed_at=None, pending_days=None, history_totals=None,
                      legacy_engine=False, study_comparable=True):
    """Return one compact view; supplied batch/window/daily objects are never mutated."""
    start = date.fromisoformat(window['start_date'])
    end = date.fromisoformat(window['end_date'])
    actual_start, actual, covered = _coverage(batch, now=now, observed_at=observed_at,
        cutoff_at=cutoff_at, first_observed_at=first_observed_at)
    pending_days = pending_days or {}
    selected = [row for row in daily if window['start_date'] <= row['stat_date'] <= window['end_date']]
    counts_by_month = {}
    for stamp, count in pending_days.items():
        if window['start_date'] <= stamp <= window['end_date']:
            key = stamp[:7]
            counts_by_month[key] = counts_by_month.get(key, 0) + count
    report = monthly_report(batch, stats, selected, now=now, observed_at=observed_at,
        cutoff_at=cutoff_at, first_observed_at=first_observed_at, pending_counts=counts_by_month)
    existing = {row['month']: row for row in report['rows']}
    daily_months = {}
    for row in selected:
        daily_months.setdefault(row['stat_date'][:7], []).append(row)
    months = []
    for index in range(month_index(start), month_index(end) + 1):
        month_start, month_end = month_date(index), month_date(index + 1) - timedelta(days=1)
        key = month_start.strftime('%Y-%m')
        original = existing.get(key)
        days = sorted(daily_months.get(key, []), key=lambda row: row['stat_date'])
        if original is None:
            months.append(dict(month=key, status='missing', complete_month=False,
                range_start=month_start.isoformat(), range_end=min(end, month_end).isoformat(),
                settled_count=0, betting_days=0, stake_cents=0, net_cents=0, roi_percent=None,
                daily_close_drawdown_cents=None, worst_day_cents=None, pending_count=0))
            continue
        stake = sum(row['stake_cents'] for row in days)
        net = sum(row['net_cents'] for row in days)
        count = sum(row['settled_count'] for row in days)
        betting_days = [row for row in days if row['settled_count'] > 0]
        equity = peak = drawdown = 0
        for day in days:
            equity += day['net_cents']
            peak = max(peak, equity)
            drawdown = max(drawdown, peak - equity)
        months.append(dict(month=key, status=original['status'], complete_month=original['complete_month'],
            range_start=max(original['range_start'], window['start_date']),
            range_end=min(original['range_end'], window['end_date']),
            settled_count=count, betting_days=len(betting_days), stake_cents=stake, net_cents=net,
            roi_percent=_roi(net, stake), daily_close_drawdown_cents=drawdown if count else None,
            worst_day_cents=min((day['net_cents'] for day in betting_days), default=None),
            pending_count=counts_by_month.get(key, 0)))
    period_reached = bool(covered and covered >= end)
    rois = [row['net_cents'] / row['stake_cents'] * 100 for row in months if row['stake_cents']]
    drawdowns = [row['daily_close_drawdown_cents'] for row in months if row['daily_close_drawdown_cents'] is not None]
    net = sum(row['net_cents'] for row in selected)
    count = sum(row['settled_count'] for row in selected)
    positive = sorted((row['net_cents'] for row in selected if row['net_cents'] > 0), reverse=True)
    best_three = sum(positive[:3])
    longest = current = None
    if count and period_reached and start >= actual_start:
        longest, current = _underwater(selected, start, end)
    left_censored = actual_start < start
    history_totals = history_totals or {}
    consistent = all(history_totals.get(key, 0) == stats.get(key, 0)
                     for key in ('settled_count', 'stake_cents', 'net_cents'))
    skips = {key: value for key, value in (batch.get('skip_counts') or {}).items() if value}
    cursor = batch.get('cursor') or {}
    continuation = batch.get('continuation') or {}
    conflict = bool((cursor.get('evidence') or {}).get('conflict') or skips.get('input_conflict')
                    or continuation.get('coverage_quality') == 'conflict')
    gap = bool(cursor.get('input_expired') or skips.get('input_expired')
               or continuation.get('state') == 'archive_gap' or continuation.get('coverage_quality') == 'archive_gap')
    blockers = []
    for code, active in [('legacy_engine', legacy_engine), ('input_conflict', conflict), ('archive_gap', gap),
                         ('window_pending', sum(counts_by_month.values()) > 0),
                         ('history_mismatch', not consistent), ('study_not_comparable', not study_comparable)]:
        if active:
            blockers.append(code)
    has_partial = any(row['settled_count'] and not row['complete_month'] for row in months)
    warnings = []
    for code, active in [('known_skips', bool(skips)), ('partial_window', has_partial),
                         ('window_not_reached', not period_reached), ('recovery_window_boundary', left_censored)]:
        if active:
            warnings.append(code)
    result_window = dict(window, actual_through_date=min(actual, end).isoformat() if actual else None,
        available_months=sum(bool(row['settled_count'] and actual and row['month'] <= actual.strftime('%Y-%m')) for row in months),
        complete_months=sum(bool(row['complete_month'] and row['settled_count']) for row in months),
        has_partial=has_partial, period_reached=period_reached)
    return dict(version=1, original_stake_cents=batch['stake_cents'], window=result_window, months=months,
        summary=dict(net_cents=net, stake_cents=sum(row['stake_cents'] for row in selected), settled_count=count,
            min_month_roi_percent=round(min(rois), 6) if rois else None,
            roi_spread_pp=round(max(rois)-min(rois), 6) if len(rois) >= 2 else None,
            min_month_count=min(row['settled_count'] for row in months),
            min_month_days=min(row['betting_days'] for row in months),
            min_month_drawdown_cents=min(drawdowns) if drawdowns else None,
            max_month_drawdown_cents=max(drawdowns) if drawdowns else None,
            worst_day_cents=min((row['net_cents'] for row in selected if row['settled_count'] > 0), default=None),
            best_three_days_cents=best_three, net_without_best_three_days_cents=net-best_three,
            longest_underwater_days=longest, current_underwater_days=current, recovery_left_censored=left_censored),
        recent=_period(daily, pending_days, end-timedelta(days=13), end, actual_start=actual_start, covered=covered),
        previous=_period(daily, pending_days, end-timedelta(days=27), end-timedelta(days=14), actual_start=actual_start, covered=covered),
        quality=dict(blockers=blockers, warnings=warnings, input_warnings=skips,
                     history_consistent=consistent, study_comparable=bool(study_comparable)))
