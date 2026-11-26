"""Small deterministic rule experiments; no I/O and no execution side effects."""
from __future__ import annotations

import copy
import hashlib
import json
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation

from pc28touzhu.domain.settlement_rules import (
    build_settlement_snapshot, resolve_pc28_result_for_signal,
    resolve_pc28_settled_hit_detail,
)

ALLOWED_SETTLEMENT_RULE_IDS = ('pc28_netdisk_regular', 'pc28_high_regular')
METRICS = ('big_small', 'odd_even', 'combo')
VALUES = {'big_small': ('大', '小'), 'odd_even': ('单', '双'),
          'combo': ('大单', '大双', '小单', '小双')}
SHANGHAI = timezone(timedelta(hours=8))


def _time(value):
    if not isinstance(value, str):
        raise ValueError('时间必须是带时区的字符串')
    parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('时间必须包含时区')
    return parsed.astimezone(timezone.utc)


def normalize_batch_payload(payload: dict, *, now: str) -> dict:
    allowed = {'name', 'mode', 'predictor_id', 'metric', 'template_pack', 'stake_amount', 'start_date', 'end_date'}
    if not isinstance(payload, dict) or set(payload)-allowed:
        raise ValueError('实验参数包含不支持的字段')
    name = payload.get('name')
    if not isinstance(name, str) or not 1 <= len(name.strip()) <= 80:
        raise ValueError('名称必须为1到80个字符')
    mode, metric = payload.get('mode'), payload.get('metric')
    if not isinstance(mode, str) or not isinstance(metric, str) or mode not in {'live', 'backtest'} or metric not in METRICS:
        raise ValueError('请选择有效的模拟模式和玩法')
    pid = payload.get('predictor_id')
    if isinstance(pid, bool) or not isinstance(pid, int) or pid <= 0:
        raise ValueError('预测器编号必须为正整数')
    if payload.get('template_pack', 'basic_v1') != 'basic_v1':
        raise ValueError('不支持的规则模板包')
    amount = payload.get('stake_amount', 1)
    if isinstance(amount, bool) or not isinstance(amount, (int, float)):
        raise ValueError('每注金额必须是数字')
    try:
        money = Decimal(str(amount))
        if not money.is_finite() or not Decimal('.01') <= money <= 10000 or money*100 != (money*100).to_integral_value():
            raise ValueError('每注金额须为0.01到10000，最多两位小数')
    except InvalidOperation as exc:
        raise ValueError('每注金额无效') from exc
    start, end = payload.get('start_date'), payload.get('end_date')
    if mode == 'live':
        if start is not None or end is not None:
            raise ValueError('前向模拟不能指定历史日期')
    else:
        try:
            if not isinstance(start, str) or not isinstance(end, str):
                raise ValueError()
            first, last = date.fromisoformat(start), date.fromisoformat(end)
            if first.isoformat() != start or last.isoformat() != end:
                raise ValueError()
            if not 0 <= (last-first).days < 90 or last > _time(now).astimezone(SHANGHAI).date():
                raise ValueError()
        except (ValueError, TypeError) as exc:
            raise ValueError('回放日期须完整、按先后排列、不晚于今天，且最多90天') from exc
    return dict(name=name.strip(), mode=mode, predictor_id=pid, metric=metric, template_pack='basic_v1',
                stake_cents=int(money*100), start_date=start, end_date=end)


def build_conditions(metric: str) -> list[dict]:
    if metric not in METRICS:
        raise ValueError('不支持的实验玩法')
    conditions = {}

    def add(kind, threshold=None, value=None, window=None, operator='gte'):
        if kind == 'draw_absence' and metric != 'combo':
            kind = 'draw_streak'
            value = next(v for v in VALUES[metric] if v != value)
        condition = dict(type=kind, metric=metric, operator=None if kind == 'always' else operator,
                         threshold=threshold, window_size=window, bet_value=value)
        key = json.dumps(condition, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        labels = {'always':'无条件基准', 'miss_streak':'预测连错', 'hit_streak':'预测连中',
                  'draw_streak':f'{value or ""}连续开出', 'draw_absence':f'{value or ""}连续不出',
                  'hit_rate':f'近{window}期命中率'}
        name = labels[kind]
        if threshold is not None:
            name += (' ≥ ' if operator == 'gte' else ' ≤ ') + str(threshold) + ('%' if kind == 'hit_rate' else '期')
        conditions[key] = {**condition, 'condition_key':hashlib.sha256(key.encode()).hexdigest()[:24], 'name':name}

    add('always')
    for n in (3, 4, 5, 6, 7): add('miss_streak', n)
    for n in (2, 3, 4): add('hit_streak', n)
    for value in VALUES[metric]:
        for n in (3, 4, 5): add('draw_streak', n, value)
    for value in VALUES[metric]:
        for n in (5, 8, 12): add('draw_absence', n, value)
    low, high = (15, 35) if metric == 'combo' else (35, 60)
    for window in (20, 50, 100):
        add('hit_rate', low, window=window, operator='lte')
        add('hit_rate', high, window=window)
    return list(conditions.values())


def build_experiments(metric: str) -> list[dict]:
    result = []
    snapshots = {rid:build_settlement_snapshot(rule_source='subscription_fixed', settlement_rule_id=rid,
                fallback_profit_ratio=1, resolved_from='rule_lab') for rid in ALLOWED_SETTLEMENT_RULE_IDS}
    for condition in build_conditions(metric):
        for rule_id in ALLOWED_SETTLEMENT_RULE_IDS:
            snapshot = copy.deepcopy(snapshots[rule_id])
            snapshot['catalog_hash'] = hashlib.sha256(json.dumps(snapshot, ensure_ascii=False, sort_keys=True).encode()).hexdigest()
            result.append(dict(condition_key=condition['condition_key'], name=condition['name'], condition=copy.deepcopy(condition),
                               settlement_rule_id=rule_id, settlement_rule_name=snapshot['rule']['name'], settlement_snapshot=snapshot))
    return result


def _actual(metric, triplet):
    if not isinstance(triplet, (list, tuple)) or len(triplet) != 3 or any(type(x) is not int or not 0 <= x <= 9 for x in triplet):
        raise ValueError('开奖必须含三个0到9的整数')
    total = sum(triplet)
    values = {'big_small':'大' if total >= 14 else '小', 'odd_even':'单' if total % 2 else '双'}
    values['combo'] = values['big_small'] + values['odd_even']
    return values[metric]


def build_features(metric: str, history: list[dict], *, decision_at: str) -> dict:
    if metric not in METRICS:
        raise ValueError('不支持的实验玩法')
    reference = _time(decision_at)
    rows = sorted(history[-100:], key=lambda r:int(r['issue_no']))
    processed = []
    gap = False
    previous = None
    latest = None
    for row in rows:
        issue = int(row['issue_no'])
        consecutive = previous is None or issue == previous+1
        gap |= not consecutive
        previous = issue
        actual = prediction_hit = None
        try:
            opened = _time(row['open_at'])
            draw_conflict = row.get('draw_conflict', row.get('conflict', False))
            prediction_conflict = row.get('prediction_conflict', row.get('conflict', False))
            if not draw_conflict and opened <= reference and _time(row['draw_available_at']) <= reference:
                actual = _actual(metric, row['triplet'])
                latest = row['issue_no']
                predicted = row.get('prediction_value') if 'prediction_value' in row else (row.get('prediction_values') or {}).get(metric)
                if (not prediction_conflict and predicted in VALUES[metric]
                        and _time(row.get('prediction_created_at')) <= opened-timedelta(seconds=40)
                        and opened <= _time(row.get('prediction_settled_at')) <= reference):
                    prediction_hit = predicted == actual
        except (ValueError, TypeError, KeyError):
            pass
        processed.append((issue, consecutive, actual, prediction_hit))
    draw_streaks = {v:0 for v in VALUES[metric]}
    absences = {v:0 for v in VALUES[metric]}
    hits = []
    next_issue = None
    for issue, _, actual, hit in reversed(processed):
        if actual is None or (next_issue is not None and issue != next_issue-1):
            break
        for value in VALUES[metric]:
            if actual == value and draw_streaks[value] == len(hits):
                draw_streaks[value] += 1
            if actual != value and absences[value] == len(hits):
                absences[value] += 1
        hits.append(hit)
        next_issue = issue
    prediction_hits = []
    next_issue = None
    for issue, _, _, hit in reversed(processed):
        if hit is None or (next_issue is not None and issue != next_issue-1):
            break
        prediction_hits.append(hit)
        next_issue = issue
    hit_streak = miss_streak = 0
    for hit in prediction_hits:
        if hit:
            if miss_streak: break
            hit_streak += 1
        else:
            if hit_streak: break
            miss_streak += 1
    rates = {}
    for size in (20, 50, 100):
        window = prediction_hits[:size]
        rates[str(size)] = dict(sample_count=len(window), hit_count=sum(window), rate=100*sum(window)/size if len(window)==size else None)
    return dict(metric=metric, latest_issue_no=latest, hit_streak=hit_streak, miss_streak=miss_streak,
                draw_streaks=draw_streaks, draw_absences=absences, rates=rates, gap_detected=gap)


def match_condition(condition: dict, features: dict) -> dict:
    kind = condition['type']
    if kind == 'always':
        return dict(matched=True, value=1, reason='')
    if kind == 'hit_rate':
        value = features['rates'][str(condition['window_size'])]['rate']
    elif kind == 'draw_streak':
        value = features['draw_streaks'].get(condition['bet_value'])
    elif kind == 'draw_absence':
        value = features['draw_absences'].get(condition['bet_value'])
    elif kind in {'hit_streak', 'miss_streak'}:
        value = features[kind]
    else:
        raise ValueError('不支持的实验条件')
    if value is None:
        return dict(matched=False, value=None, reason='insufficient_history')
    matched = value >= condition['threshold'] if condition['operator']=='gte' else value <= condition['threshold']
    return dict(matched=matched, value=value, reason='' if matched else 'not_matched')


def settle_bet(snapshot: dict, metric: str, bet_value: str, stake_cents: int, triplet: list[int]) -> dict:
    rule_id = snapshot.get('settlement_rule_id')
    if rule_id not in ALLOWED_SETTLEMENT_RULE_IDS or not snapshot.get('rule') or not snapshot.get('resolved_odds'):
        raise ValueError('实验必须使用冻结的OK结算规则')
    if metric not in METRICS or bet_value not in VALUES[metric] or type(stake_cents) is not int or stake_cents <= 0:
        raise ValueError('模拟投注内容无效')
    actual = _actual(metric, triplet)
    signal = dict(lottery_type='pc28',bet_type=metric,bet_value=bet_value)
    resolved = resolve_pc28_result_for_signal(signal=signal, settlement_rule_id=rule_id, draw_context={'triplet':triplet}, rule=snapshot['rule'])
    kind = resolved['result_type']
    net = -stake_cents if kind == 'miss' else 0
    if kind == 'hit':
        detail = resolve_pc28_settled_hit_detail(stake_amount=stake_cents/100, signal=signal, settlement_rule_id=rule_id,
                                              draw_snapshot=resolved['draw_snapshot'], snapshot=snapshot)
        if not detail:
            raise ValueError('冻结规则缺少该玩法赔率')
        net = int(round(detail['profit']*100))
    return dict(result_type=kind, direction_hit=actual==bet_value, net_cents=net, refund_reason=resolved.get('refund_reason') or '')


def empty_stats() -> dict:
    return dict.fromkeys(('settled_count','hit_count','refund_count','miss_count','direction_hit_count','stake_cents','net_cents',
                          'equity_peak_cents','max_drawdown_cents','current_loss_streak','max_loss_streak','trigger_count','skipped_count'), 0)


def apply_outcome(stats: dict, outcome: dict, *, stake_cents: int) -> dict:
    kind, net = outcome.get('result_type'), outcome.get('net_cents')
    if kind not in {'hit','refund','miss'} or type(net) is not int or type(stake_cents) is not int or stake_cents <= 0:
        raise ValueError('结算结果无效')
    result = {**empty_stats(), **stats}
    result['settled_count'] += 1
    result[kind+'_count'] += 1
    result['direction_hit_count'] += int(bool(outcome.get('direction_hit')))
    result['stake_cents'] += stake_cents
    result['net_cents'] += net
    result['equity_peak_cents'] = max(result['equity_peak_cents'], result['net_cents'])
    result['max_drawdown_cents'] = max(result['max_drawdown_cents'], result['equity_peak_cents']-result['net_cents'])
    result['current_loss_streak'] = result['current_loss_streak']+1 if net < 0 else 0
    result['max_loss_streak'] = max(result['max_loss_streak'],result['current_loss_streak'])
    return result
