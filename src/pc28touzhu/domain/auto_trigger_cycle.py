"""自动过关的轮次政策；投注算法仍由 win_streak_roll 提供。"""
from __future__ import annotations

import copy
import re
from typing import Any

from pc28touzhu.domain.subscription_strategy import upgrade_subscription_strategy
from pc28touzhu.domain.win_streak_roll import normalize_stop_policy, normalize_win_streak_roll_policy


END_ACTIONS = {'wait_condition', 'new_cycle', 'close_day'}
DEFAULT_CYCLE_POLICY = {'mode': 'rule', 'max_starts_per_day': 1, 'max_starts_per_window': 1,
                        'on_target': 'wait_condition', 'on_bust': 'wait_condition'}


def normalize_cycle_policy(value: Any, *, current: dict | None = None, creating: bool = False) -> dict:
    if value is None:
        return dict(DEFAULT_CYCLE_POLICY) if creating else dict(current or {'mode': 'legacy'})
    if not isinstance(value, dict):
        raise ValueError('cycle_policy 必须为对象')
    mode = str(value.get('mode') or (current or {}).get('mode') or 'rule')
    if mode == 'legacy':
        if creating or (current or {}).get('mode', 'legacy') != 'legacy':
            raise ValueError('新规则不能选择沿用旧设置，已切换的规则不能退回旧模式')
        return {'mode': 'legacy'}
    if mode != 'rule':
        raise ValueError('cycle_policy.mode 仅支持 rule 或 legacy')
    merged = {**DEFAULT_CYCLE_POLICY, **(current or {}), **value, 'mode': 'rule'}
    limit = merged.get('max_starts_per_day')
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 1000:
        raise ValueError('每个方案每天最多启动次数必须为 1 到 1000 的整数')
    window_limit = merged.get('max_starts_per_window')
    if isinstance(window_limit, bool) or not isinstance(window_limit, int) or not 0 <= window_limit <= 1000:
        raise ValueError('每个时段最多启动次数必须为 0 到 1000 的整数，0 表示不限')
    for field in ('on_target', 'on_bust'):
        if merged.get(field) not in END_ACTIONS:
            raise ValueError('cycle_policy.%s 仅支持 wait_condition、new_cycle 或 close_day' % field)
    return {key: merged[key] for key in DEFAULT_CYCLE_POLICY}


def schedule_window_start_counts(runs: list[dict]) -> dict[str, int]:
    """从持久化轮次统计时段用量，兼容普通窗口和 roll:序号:窗口。"""
    counts: dict[str, int] = {}
    for run in runs:
        if run.get('status') == 'blocked' and not run.get('started_issue_no'):
            continue  # 历史跨日空占位不是实际开轮。
        key = re.sub(r'(?:#reopened-\d+)+$', '', str(run.get('window_id') or ''))
        if re.fullmatch(r'roll:\d+', key):
            continue  # 无时段的条件轮。
        match = re.fullmatch(r'roll:\d+:(.+)', key)
        window_id = match.group(1) if match else key
        if window_id:
            counts[window_id] = counts.get(window_id, 0) + 1
    return counts


def subscription_strategy(subscription: dict) -> dict:
    return upgrade_subscription_strategy(subscription.get('strategy_v2') or subscription.get('strategy') or subscription.get('strategy_json') or {})


def is_roll_subscription(subscription: dict) -> bool:
    return subscription_strategy(subscription).get('staking_policy', {}).get('mode') == 'win_streak_roll'


def auto_roll_policy(strategy: dict, cycle_policy: dict, legacy_policy: dict | None = None) -> dict:
    """下注和生命周期都按轮快照。独立模式让引擎仅结束当前轮。"""
    policy = normalize_win_streak_roll_policy(strategy.get('staking_policy') or {})
    if cycle_policy.get('mode') == 'legacy':
        policy['stop_policy'] = normalize_stop_policy(legacy_policy or policy.get('stop_policy'))
    else:
        policy['stop_policy'] = {'on_target': 'close_day', 'on_bust': 'close_day', 'close_scope': 'route', 'max_cycles_per_day': 1}
    policy['_cycle_policy'] = copy.deepcopy(cycle_policy)
    policy['_strategy_snapshot'] = copy.deepcopy(strategy)
    return policy
