"""Read-only calendar-month statistics from durable daily totals, in integer cents."""
from __future__ import annotations

from calendar import monthrange
from datetime import date, datetime, timedelta, timezone

SHANGHAI = timezone(timedelta(hours=8))
MAX_MONTHS = 120


def local_time(value):
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('统计时间必须包含时区')
    return parsed.astimezone(SHANGHAI)


def month_index(day):
    return day.year * 12 + day.month - 1


def month_date(index):
    year, month = divmod(index, 12)
    return date(year, month + 1, 1)


def report_window(batch, *, now):
    start = (date.fromisoformat(batch['start_date']) if batch['mode'] == 'backtest'
             else local_time(batch.get('started_at') or batch['created_at']).date())
    end = date.fromisoformat(batch['end_date']) if batch['mode'] == 'backtest' else local_time(now).date()
    end = max(start, end)
    first = month_date(max(month_index(start), month_index(end) - MAX_MONTHS + 1))
    return start, end, first


def monthly_report(batch, stats, daily, *, now, observed_at=None, cutoff_at=None,
                   first_observed_at=None, pending_since=None):
    start, end, first = report_window(batch, now=now)
    observed = local_time(observed_at)
    cutoff = local_time(cutoff_at)
    first_observed = local_time(first_observed_at)
    pending = local_time(pending_since)
    started = local_time(batch.get('started_at'))
    rows = []
    by_month = {}
    for day in daily:
        by_month.setdefault(day['stat_date'][:7], []).append(day)
    for index in range(month_index(first), month_index(end) + 1):
        month_start = month_date(index)
        next_month = month_date(index + 1)
        month_end = next_month - timedelta(days=1)
        boundary = datetime.combine(next_month, datetime.min.time(), SHANGHAI)
        key = month_start.strftime('%Y-%m')
        days = sorted(by_month.get(key, []), key=lambda d: d['stat_date'])
        count = sum(d['settled_count'] for d in days)
        stake = sum(d['stake_cents'] for d in days)
        net = sum(d['net_cents'] for d in days)
        betting_days = [d for d in days if d['settled_count'] > 0]
        equity = peak = drawdown = 0
        for day in days:
            equity += day['net_cents']
            peak = max(peak, equity)
            drawdown = max(drawdown, peak - equity)
        processed = batch.get('processed_issues', 0) > 0
        full_start = start <= month_start
        if batch['mode'] == 'live' and started:
            full_start = started <= datetime.combine(month_start, datetime.min.time(), SHANGHAI)
        if first_observed and first_observed.date() > month_start:
            full_start = False
        closed = bool(observed and observed >= boundary)
        if batch['status'] == 'completed' and cutoff and cutoff >= boundary and end >= month_end:
            closed = True
        full = bool(processed and full_start and end >= month_end and closed)
        if pending and pending < boundary:
            full = False
        if full:
            status = 'complete' if count else 'no_samples'
        elif not count and (not observed or observed.date() < month_start):
            status = 'not_replayed' if batch['mode'] == 'backtest' else 'not_observed'
        elif pending and pending < boundary:
            status = 'pending'
        else:
            status = 'partial'
        rows.append(dict(month=key,range_start=max(start,month_start).isoformat(),range_end=min(end,month_end).isoformat(),
            status=status,complete_month=full,eligible=full and count>0,settled_count=count,
            hit_count=sum(d['hit_count'] for d in days),refund_count=sum(d['refund_count'] for d in days),
            miss_count=sum(d['miss_count'] for d in days),stake_total=stake/100,net_profit=net/100,
            roi_percent=round(net/stake*100,2) if stake else None,betting_days=len(betting_days),
            profitable_days=sum(d['net_cents']>0 for d in betting_days),losing_days=sum(d['net_cents']<0 for d in betting_days),
            worst_day_profit=min((d['net_cents'] for d in betting_days),default=0)/100 if count else None,
            daily_close_drawdown=drawdown/100 if count else None,
            calendar_days=monthrange(month_start.year,month_start.month)[1]))
    eligible = [r for r in rows if r['eligible']]
    profitable = sum(r['net_profit']>0 for r in eligible)
    losing = sum(r['net_profit']<0 for r in eligible)
    streak = longest = 0
    for row in rows:
        streak = streak+1 if row['eligible'] and row['net_profit']<0 else 0
        longest = max(longest, streak)
    skips = dict(batch.get('skip_counts', {}))
    if (batch.get('cursor',{}).get('evidence') or {}).get('conflict'):
        skips['input_conflict']=max(1,skips.get('input_conflict',0))
    # Daily totals remain available after individual decisions are pruned. Preserve
    # the actual accumulated PnL at the start of a truncated reporting window.
    curve_days=sorted((d for d in daily if d['settled_count']>0),key=lambda d:d['stat_date'])
    opening_net=stats['net_cents']-sum(d['net_cents'] for d in curve_days)
    cumulative=opening_net;points=[]
    for day in curve_days:
        cumulative+=day['net_cents']
        points.append(dict(date=day['stat_date'],cumulative_net_cents=cumulative))
    full_curve=(month_index(first)==month_index(start) and opening_net==0
                and sum(d['settled_count'] for d in curve_days)==stats['settled_count'])
    return dict(timezone='Asia/Shanghai',rows=rows,
        window=dict(start=first.isoformat(),end=end.isoformat(),truncated=month_index(first)>month_index(start),max_months=MAX_MONTHS),
        observed_through=observed.isoformat() if observed else None,
        input_warnings={k:v for k,v in skips.items() if v},
        equity=dict(points=points,opening_net_cents=opening_net,complete_history=full_curve,
                    basis='daily_close',window_start=first.isoformat(),window_end=end.isoformat()),
        summary=dict(eligible_months=len(eligible),profitable_months=profitable,losing_months=losing,
            flat_months=len(eligible)-profitable-losing,
            profitable_month_ratio=round(profitable/len(eligible)*100,2) if eligible else None,
            best_month_profit=max((r['net_profit'] for r in eligible),default=None),
            worst_month_profit=min((r['net_profit'] for r in eligible),default=None),
            longest_losing_months=longest),
        capital=dict(stake_amount=batch['stake_cents']/100,settled_count=stats['settled_count'],
            max_drawdown=stats['max_drawdown_cents']/100,net_profit=stats['net_cents']/100,
            batch_status=batch['status'],scope='entire_processed_interval',
            available=stats['settled_count']>0 and stats['max_drawdown_cents']>0))
