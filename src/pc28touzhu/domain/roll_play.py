"""过关的投注内容；单注与组玩法共用一条资金链。"""
from __future__ import annotations

import math

from pc28touzhu.domain.pc28_group_play import GROUP_PLAY_TYPES, normalize_group_selection


SINGLE_PLAY = 'single_play'
PLAY_TYPES = ('big_small', 'odd_even', 'combo', 'number', *GROUP_PLAY_TYPES)
VALUES = {'big_small': ('大', '小'), 'odd_even': ('单', '双'),
          'combo': ('大单', '大双', '小单', '小双'), 'number': tuple(str(i) for i in range(28))}


def play_unit_count(policy):
    if policy.get('structure') != SINGLE_PLAY:
        return 2
    return {'double_group': 2, 'kill_group': 3}.get(policy.get('play_type'), 1)


def normalize_play_value(metric, value):
    text = str(value if value is not None else '').strip()
    if metric in GROUP_PLAY_TYPES:
        return normalize_group_selection(metric, text)['bet_value']
    if metric == 'number' and text.isdigit():
        text = str(int(text))
    if text not in VALUES.get(metric, ()):
        raise ValueError('投注方向与玩法不匹配')
    return text


def normalize_roll_play(data):
    metric = str(data.get('play_type') or 'big_small')
    mode = str(data.get('selection_mode') or 'follow')
    if metric not in PLAY_TYPES or mode not in ('follow', 'fixed', 'filter'):
        raise ValueError('过关玩法或方向选择方式无效')
    value = normalize_play_value(metric, data.get('fixed_value')) if mode != 'follow' else None
    return {'play_type': metric, 'selection_mode': mode, 'fixed_value': value}


def selected_play_signal(policy, signal):
    source = signal or {}
    metric = policy.get('play_type') or 'big_small'
    mode = policy.get('selection_mode') or 'follow'
    if source.get('bet_type') != metric:
        raise ValueError('当前信号与过关玩法不匹配')
    value = normalize_play_value(metric, source.get('bet_value'))
    if mode == 'filter' and value != policy.get('fixed_value'):
        raise ValueError('当前信号方向不符合过关筛选')
    if mode == 'fixed':
        value = policy['fixed_value']
    payload = dict(source.get('normalized_payload') or {})
    # 重选方向必须移除来源组合派生与文本，避免冻结成旧方向。
    for key in ('selected_combos', 'excluded_combo', 'unit_count', 'group_derivation', 'origin_combo', 'message_text'):
        payload.pop(key, None)
    payload.update(bet_type=metric, bet_value=value)
    if metric in GROUP_PLAY_TYPES:
        payload.update(normalize_group_selection(metric, value))
    return {**source, 'bet_type': metric, 'bet_value': value, 'normalized_payload': payload}


def single_play_stake(amount, policy):
    value = float(amount)
    if not math.isfinite(value):
        raise ValueError('过关资金必须为有限金额')
    units = play_unit_count(policy)
    per_unit = int(round(value, 2) / units + 1e-9)
    return max(0, per_unit) * units
