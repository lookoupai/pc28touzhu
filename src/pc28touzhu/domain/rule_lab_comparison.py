"""One-factor risk candidates and deterministic replay-input fingerprints."""
from __future__ import annotations

import copy
import hashlib
import json
from decimal import Decimal

from pc28touzhu.domain.rule_lab_risk import normalize_risk_policy


def risk_variants(policy):
    original=normalize_risk_policy(policy)
    if original['mode']!='round':raise ValueError('只有按轮风控批次可以生成比较组')
    result=[dict(key='baseline',label='原风控基准',available=True,reason='',policy=copy.deepcopy(original),changes=['完整复制原风控'])]
    for key,label,section,field in [('round_tp_half','提前止盈','round','profit_target'),
        ('round_sl_half','提前止损','round','loss_limit'),('day_sl_half','降低日止损','day','loss_limit')]:
        variant=copy.deepcopy(original);old=variant[section][field]
        cents=int(Decimal(str(old))*100);half=cents//2
        available=bool(variant[section]['enabled'] and cents>0 and half>=1)
        reason='' if available else '对应控制未启用、阈值为0或减半后小于0.01'
        changes=[]
        if available:
            variant[section][field]=half/100
            changes=[('单轮' if section=='round' else '主日层')+('止盈' if field=='profit_target' else '止损')+f' {old:g} → {half/100:g}；其余保持']
        result.append(dict(key=key,label=label,available=available,reason=reason,policy=variant if available else None,changes=changes))
    variant=copy.deepcopy(original)
    controls=[variant['day']]+variant.get('additional_day_guards',[])
    available=any(control['enabled'] and control['hit_rate_gate']['enabled'] for control in controls)
    for control in controls:control['hit_rate_gate']['enabled']=False
    result.append(dict(key='gate_off',label='关闭日命中率熔断',available=available,
        reason='' if available else '所有生效日层的命中率熔断原本已关闭',policy=variant if available else None,
        changes=['关闭所有日层命中率熔断；阈值/样本量及其他风控保持'] if available else []))
    seen=set()
    for item in result:
        if not item['available']:continue
        current=item['policy']
        fingerprint=json.dumps({k:v for k,v in current.items() if k!='source'},sort_keys=True,separators=(',',':'))
        if fingerprint in seen:
            item.update(available=False,reason='与已有候选无有效差异',policy=None);continue
        seen.add(fingerprint)
        if item['key']!='baseline' and current.get('source'):current['source']['modified']=True
        item['policy']=normalize_risk_policy(current)
    return result


def evidence_step(previous,event):
    def stable(value):
        if isinstance(value,dict):
            return {k:stable(v) for k,v in value.items() if k not in {'observed_at','finalized_at','source_id'}}
        if isinstance(value,(list,tuple)):return [stable(v) for v in value]
        return value
    encoded=json.dumps(stable(event),ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    return hashlib.sha256((previous or '').encode()+b'\n'+encoded).hexdigest()
