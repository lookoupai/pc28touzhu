"""Validate reusable rule templates without reading or changing runtime state."""
from __future__ import annotations

import hashlib
import json
from decimal import Decimal

from pc28touzhu.domain.rule_lab import METRICS, VALUES
from pc28touzhu.domain.rule_lab_risk import normalize_risk_policy


MAX_CONDITIONS = 50
MAX_INPUT_CONDITIONS = 200
MAX_DEFINITION_BYTES = 32768
CONDITION_TYPES = ('always', 'miss_streak', 'hit_streak', 'draw_streak', 'draw_absence', 'hit_rate')
CONDITION_FIELDS = {'type', 'metric', 'operator', 'threshold', 'window_size', 'bet_value', 'name', 'condition_key'}


def _object(value, allowed, label):
    if not isinstance(value, dict) or set(value) - set(allowed):
        raise ValueError(label + '包含不支持的字段')
    return value


def _metric(value):
    if not isinstance(value, str) or value not in METRICS:
        raise ValueError('请选择有效的玩法')
    return value


def _definition_size(value):
    try:
        encoded = json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')
    except (TypeError, ValueError, UnicodeError) as exc:
        raise ValueError('模板定义必须为有效JSON') from exc
    if len(encoded) > MAX_DEFINITION_BYTES:
        raise ValueError('单个模板定义不能超过32KiB')


def normalize_condition(metric, value):
    """Produce the same six semantic fields and keys as the frozen built-ins."""
    metric = _metric(metric)
    value = _object(value, CONDITION_FIELDS, '条件')
    kind = value.get('type')
    if not isinstance(kind, str) or kind not in CONDITION_TYPES:
        raise ValueError('请选择有效的条件类型')
    if 'metric' in value and value['metric'] != metric:
        raise ValueError('条件玩法与模板玩法不一致')
    operator = threshold = window = direction = None
    applicable = {'type', 'metric', 'name', 'condition_key'}
    if kind != 'always':
        applicable.update(('operator', 'threshold'))
        operator = value.get('operator', 'gte')
        if not isinstance(operator, str) or operator not in ('gte', 'lte'):
            raise ValueError('条件比较符必须为gte或lte')
        raw_threshold = value.get('threshold')
        if kind == 'hit_rate':
            if type(raw_threshold) not in (int, float):
                raise ValueError('命中率阈值必须为数字')
            parsed = Decimal(str(raw_threshold))
            if not parsed.is_finite() or not 0 <= parsed <= 100 or parsed * 100 != (parsed * 100).to_integral_value():
                raise ValueError('命中率阈值须为0到100且最多两位小数')
            threshold = int(parsed) if parsed == parsed.to_integral_value() else float(parsed)
            applicable.add('window_size')
            window = value.get('window_size')
            if type(window) is not int or window not in (20, 50, 100):
                raise ValueError('命中率窗口必须为20、50或100期')
        else:
            if type(raw_threshold) is not int or not 1 <= raw_threshold <= 100:
                raise ValueError('连续期数必须为1到100的整数')
            threshold = raw_threshold
        if kind in ('draw_streak', 'draw_absence'):
            applicable.add('bet_value')
            direction = value.get('bet_value')
            if not isinstance(direction, str) or direction not in VALUES[metric]:
                raise ValueError('条件方向与玩法不匹配')
    if any(value.get(field) is not None for field in CONDITION_FIELDS - applicable):
        raise ValueError('条件包含当前类型不适用的参数')
    if kind == 'draw_absence' and metric != 'combo':
        kind = 'draw_streak'
        direction = next(item for item in VALUES[metric] if item != direction)
    condition = dict(type=kind, metric=metric, operator=operator, threshold=threshold,
                     window_size=window, bet_value=direction)
    encoded = json.dumps(condition, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
    labels = {'always': '无条件基准', 'miss_streak': '预测连错', 'hit_streak': '预测连中',
              'draw_streak': f'{direction or ""}连续开出', 'draw_absence': f'{direction or ""}连续不出',
              'hit_rate': f'近{window}期命中率'}
    name = labels[kind]
    if threshold is not None:
        name += (' ≥ ' if operator == 'gte' else ' ≤ ') + str(threshold) + ('%' if kind == 'hit_rate' else '期')
    return {**condition, 'condition_key': hashlib.sha256(encoded.encode('utf-8')).hexdigest()[:24], 'name': name}


def compile_conditions(metric, values, *, require_baseline=True):
    """Deduplicate semantic conditions and optionally retain the entry baseline."""
    metric = _metric(metric)
    if type(require_baseline) is not bool:
        raise ValueError('基准选项必须为开关')
    if not isinstance(values, list) or not 1 <= len(values) <= MAX_INPUT_CONDITIONS:
        raise ValueError('条件列表须包含1到200项')
    unique = {}
    for value in values:
        condition = normalize_condition(metric, value)
        unique.setdefault(condition['condition_key'], condition)
    duplicates = len(values) - len(unique)
    warnings = []
    baseline = normalize_condition(metric, {'type': 'always'})
    key = baseline['condition_key']
    if require_baseline and key not in unique:
        unique[key] = baseline
        warnings.append('已自动补充无条件基准')
    if len(unique) > MAX_CONDITIONS:
        raise ValueError('去重后最多保留50个条件（含无条件基准）')
    if duplicates:
        warnings.append(f'已合并{duplicates}个同义重复条件')
    conditions = ([unique.pop(key)] if key in unique else []) + list(unique.values())
    return dict(conditions=conditions, duplicates_removed=duplicates, warnings=warnings)


def normalize_template(kind, name, description, definition, *, preview=False):
    """Validate metadata and freeze a condition or risk template definition."""
    if not isinstance(kind, str) or kind not in ('condition', 'risk'):
        raise ValueError('模板类型必须为condition或risk')
    if type(preview) is not bool:
        raise ValueError('预览选项必须为开关')
    if not isinstance(name, str) or not (0 if preview else 1) <= len(name.strip()) <= 60:
        raise ValueError('模板名称必须为1到60个字符')
    if not isinstance(description, str) or len(description.strip()) > 300:
        raise ValueError('模板说明不能超过300个字符')
    _definition_size(definition)
    if kind == 'condition':
        value = _object(definition, ('metric', 'conditions'), '条件模板')
        metric = _metric(value.get('metric'))
        compiled = compile_conditions(metric, value.get('conditions'))
        normalized = dict(metric=metric, conditions=compiled['conditions'])
        metrics = [metric]
        count = len(compiled['conditions'])
        duplicates = compiled['duplicates_removed']
        warnings = compiled['warnings']
    else:
        value = _object(definition, ('metrics', 'policy'), '风控模板')
        selected = value.get('metrics')
        if not isinstance(selected, list) or not selected:
            raise ValueError('风控模板须至少选择一种玩法')
        for metric in selected:
            _metric(metric)
        metrics = [metric for metric in METRICS if metric in selected]
        if not isinstance(value.get('policy'), dict):
            raise ValueError('风控模板须包含风控定义')
        normalized = dict(metrics=metrics, policy=normalize_risk_policy(value['policy']))
        count, duplicates, warnings = None, 0, []
    _definition_size(normalized)
    return dict(kind=kind, name=name.strip(), description=description.strip(), metrics=metrics,
                definition=normalized, condition_count=count, duplicates_removed=duplicates, warnings=warnings)
