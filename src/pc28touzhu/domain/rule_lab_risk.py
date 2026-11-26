"""Pure per-experiment round/day limits; amounts remain production-rounded cents."""
from __future__ import annotations

import copy
from decimal import Decimal

from pc28touzhu.domain.subscription_strategy import hit_rate_gate_stop_reason


def _object(value, allowed, label):
    if not isinstance(value, dict) or set(value)-set(allowed):
        raise ValueError(label+'字段无效')
    return value


def _boolean(value, label):
    if type(value) is not bool:
        raise ValueError(label+'必须为开关')
    return value


def _number(value, label, *, integer=False, maximum=100000000):
    if type(value) not in (int,float) or integer and type(value) is not int:
        raise ValueError(label+'必须为'+('整数' if integer else '数字'))
    parsed=Decimal(str(value))
    if not parsed.is_finite() or not 0<=parsed<=maximum or not integer and parsed*100!=(parsed*100).to_integral_value():
        raise ValueError(label+'超出范围或超过两位小数')
    return int(parsed) if integer else float(parsed)


def _control(value, *, day=False):
    allowed=('enabled','profit_target','loss_limit','max_bets')+ (('max_rounds','hit_rate_gate') if day else ())
    p=_object(value,allowed,'日风控' if day else '单轮风控')
    result=dict(enabled=_boolean(p.get('enabled',False),'启用'),
                profit_target=_number(p.get('profit_target',0),'止盈'),loss_limit=_number(p.get('loss_limit',0),'止损'),
                max_bets=_number(p.get('max_bets',0),'最多下注',integer=True,maximum=1000000))
    if day:
        gate=_object(p.get('hit_rate_gate',{}),('enabled','threshold','min_settled_count'),'命中率熔断')
        enabled=_boolean(gate.get('enabled',False),'命中率熔断')
        threshold=_number(gate.get('threshold',0),'命中率阈值',maximum=100)
        minimum=_number(gate.get('min_settled_count',20),'最少结算数',integer=True,maximum=1000000)
        if minimum<1 or enabled and not 0<threshold<100:
            raise ValueError('命中率阈值须在0与100之间，最少结算数须为正整数')
        result.update(max_rounds=_number(p.get('max_rounds',0),'最多开轮',integer=True,maximum=1000000),
                      hit_rate_gate=dict(enabled=enabled,threshold=threshold,min_settled_count=minimum))
    return result


def normalize_risk_policy(value):
    if value is None:
        return dict(version=1,mode='legacy')
    p=_object(value,('version','mode','round','day','additional_day_guards','cooldown_issues','source'),'风控')
    if type(p.get('version',1)) is not int or p.get('version',1)!=1 or p.get('mode') not in ('legacy','round'):
        raise ValueError('风控版本或模式无效')
    if p['mode']=='legacy':
        if set(p)-{'version','mode'}:
            raise ValueError('逐期对照模式不能混入按轮风控')
        return dict(version=1,mode='legacy')
    guards=p.get('additional_day_guards',[])
    if not isinstance(guards,list) or len(guards)>1:
        raise ValueError('最多保留一套附加规则日保护')
    result=dict(version=1,mode='round',round=_control(p.get('round',{})),day=_control(p.get('day',{}),day=True),
                additional_day_guards=[_control(g,day=True) for g in guards],
                cooldown_issues=_number(p.get('cooldown_issues',0),'冷却期数',integer=True,maximum=1000000))
    source=_object(p.get('source',{}),('rule_id','rule_name','route_id','route_name','subscription_id','subscription_name','modified'),'预设出处')
    source=copy.deepcopy(source)
    for key,item in source.items():
        if key.endswith('_id') and (type(item) is not int or item<0):
            raise ValueError('预设出处编号无效')
        if key.endswith('_name') and (not isinstance(item,str) or len(item)>200):
            raise ValueError('预设出处名称无效')
        if key=='modified':_boolean(item,'预设已修改')
    result['source']=source
    controls=[result['round'],result['day']]+result['additional_day_guards']
    if not any(c['enabled'] and any(c[k]>0 for k in ('profit_target','loss_limit','max_bets')) for c in controls):
        raise ValueError('尚未设置退出条件：请启用至少一项单轮或日止盈、止损、最多下注')
    return result


def empty_risk_runtime():
    return dict(round_seq=0,last_start_issue=None,active_round=None,last_stop_reason='',latest_day='',
                completed_rounds=0,profit_rounds=0,loss_rounds=0,flat_rounds=0,
                invalid_rounds=0,round_profit_stops=0,round_loss_stops=0)


def empty_risk_day(day):
    return dict(stat_date=day,net_cents=0,hit_count=0,miss_count=0,refund_count=0,bet_count=0,round_count=0,stop_reason='')


def _day_reason(policy,day,*,starting=False,pending_count=0):
    if day.get('stop_reason'):
        return day['stop_reason']
    for control in [policy['day']]+policy.get('additional_day_guards',[]):
        if not control['enabled']:continue
        if control['profit_target'] and day['net_cents']>=round(control['profit_target']*100):return 'day_profit_target'
        if control['loss_limit'] and day['net_cents']<=-round(control['loss_limit']*100):return 'day_loss_limit'
        if control['max_bets'] and day['bet_count']+pending_count>=control['max_bets']:return 'day_max_bets'
        if hit_rate_gate_stop_reason(control,hit_count=day['hit_count'],miss_count=day['miss_count']):return 'daily_hit_rate_low'
        if starting and control['max_rounds'] and day['round_count']>=control['max_rounds']:return 'day_max_rounds'
    return ''


def admit_risk(policy,runtime,day,*,issue_no,at,matched,pending_count):
    r=copy.deepcopy(runtime or empty_risk_runtime());d=dict(day)
    if policy.get('mode','legacy')=='legacy':
        return dict(allowed=bool(matched),reason='' if matched else 'condition_not_matched',runtime=r,day=d)
    active=r.get('active_round')
    reason='pending_settlement' if pending_count else _day_reason(policy,d,starting=active is None,pending_count=pending_count)
    if not reason and not active:
        if r['last_start_issue'] is not None and int(issue_no)-int(r['last_start_issue'])<policy['cooldown_issues']:
            reason='cooldown'
        elif not matched:reason='condition_not_matched'
    if not reason and active and policy['round']['enabled'] and policy['round']['max_bets'] and active['bet_count']>=policy['round']['max_bets']:
        reason='round_max_bets'
    if reason:
        return dict(allowed=False,reason=reason,runtime=r,day=d)
    if not active:
        r['round_seq']+=1;r['last_start_issue']=str(issue_no);d['round_count']+=1
        active=dict(round_no=r['round_seq'],started_at=at,start_issue=str(issue_no),start_date=d['stat_date'],
                    bet_count=0,settled_count=0,hit_count=0,miss_count=0,refund_count=0,skipped_count=0,net_cents=0,
                    status='active',stop_reason='',ended_at=None,end_issue=None)
    active['bet_count']+=1;r['active_round']=active;r['latest_day']=max(r.get('latest_day',''),d['stat_date'])
    r['last_stop_reason']=''
    return dict(allowed=True,reason='',runtime=r,day=d)


def settle_risk(policy,runtime,day,outcome):
    r=copy.deepcopy(runtime);d=dict(day);active=r.get('active_round')
    if policy.get('mode','legacy')=='legacy' or active is None:
        return dict(runtime=r,day=d,closed_round=None)
    reason=''
    if outcome['status']=='settled':
        kind=outcome['result_type'];net=outcome['net_cents']
        active['settled_count']+=1;active[kind+'_count']+=1;active['net_cents']+=net
        d['bet_count']+=1;d[kind+'_count']+=1;d['net_cents']+=net
        reason=_day_reason(policy,d)
        if reason:d['stop_reason']=reason
        control=policy['round']
        if not reason and control['enabled']:
            if control['profit_target'] and active['net_cents']>=round(control['profit_target']*100):reason='round_profit_target'
            elif control['loss_limit'] and active['net_cents']<=-round(control['loss_limit']*100):reason='round_loss_limit'
            elif control['max_bets'] and active['bet_count']>=control['max_bets']:reason='round_max_bets'
    else:
        active['skipped_count']+=1
        if active['settled_count']==0:reason='no_valid_bets'
        elif policy['round']['enabled'] and policy['round']['max_bets'] and active['bet_count']>=policy['round']['max_bets']:reason='round_max_bets'
    r['latest_day']=max(r.get('latest_day',''),d['stat_date'])
    closed=None
    if reason:
        active.update(status='closed',stop_reason=reason,ended_at=outcome['settled_at'],end_issue=str(outcome['issue_no']))
        closed=copy.deepcopy(active);r['active_round']=None;r['last_stop_reason']=reason
        if reason=='no_valid_bets':r['invalid_rounds']+=1
        else:
            r['completed_rounds']+=1
            r['profit_rounds' if active['net_cents']>0 else 'loss_rounds' if active['net_cents']<0 else 'flat_rounds']+=1
        if reason=='round_profit_target':r['round_profit_stops']+=1
        if reason=='round_loss_limit':r['round_loss_stops']+=1
    else:r['active_round']=active
    return dict(runtime=r,day=d,closed_round=closed)
