"""Read-only common-window research ranks and owned experiment selectors."""
from __future__ import annotations

import base64,json,re
from collections import defaultdict
from datetime import date
from pc28touzhu.domain.rule_lab import METRICS,ALLOWED_SETTLEMENT_RULE_IDS
from pc28touzhu.domain.rule_lab_observation import observation_window
from pc28touzhu.domain.rule_lab_monthly import local_time
from pc28touzhu.domain.rule_lab_risk import normalize_risk_policy
from pc28touzhu.domain.settlement_rules import _metric_spec_for_rule,_rule_straight_wraparound
from pc28touzhu.services.rule_lab_store import RuleLabError,_load
from pc28touzhu.services.rule_lab_templates import validate_request,fingerprint
from pc28touzhu.services.rule_lab_management import _token,_owned
from pc28touzhu.services.rule_lab_observation import load_observations


def encode_cursor(value):return base64.urlsafe_b64encode(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).decode().rstrip('=')


def decode_cursor(value):
    if not isinstance(value,str) or not 1<=len(value)<=2048 or not re.fullmatch(r'[A-Za-z0-9_-]+',value):raise RuleLabError('invalid_input','分页标识无效')
    try:
        result=json.loads(base64.urlsafe_b64decode(value+'='*((-len(value))%4)))
        if not isinstance(result,dict):raise ValueError()
        return result
    except (ValueError,UnicodeError) as exc:raise RuleLabError('invalid_input','分页标识无效') from exc


def owner_item(store,c,uid,kind,oid):
    row=_owned(store,c,uid,kind,oid)
    if kind=='batch':
        public=store._batch(c,row);head=row;count=public['experiment_count'];continuation=public['continuation']
    else:
        public=store._study(c,row);head=c.execute('SELECT b.* FROM lab_batches b JOIN lab_study_batches s ON s.batch_id=b.id WHERE s.study_id=? ORDER BY s.position LIMIT 1',(oid,)).fetchone()
        count=sum(child['experiment_count'] for child in public['children']);continuation=public.get('continuation',{})
    return dict(owner_kind=kind,owner_id=oid,name=row['name'],status=row['status'],display_status=public.get('display_status',row['status']),
        predictor_id=head['predictor_id'] if head else None,predictor_name=head['predictor_name'] if head else '',metric=head['metric'] if head else None,
        start_date=head['start_date'] if head else None,end_date=head['end_date'] if head else None,
        experiment_count=count,expected_revision=_token(c,kind,row),continuation=continuation)


def options(store,uid):
    with store._connect() as c:
        c.execute('BEGIN');sources={};conditions={};rules={};owners=[]
        batches=c.execute('SELECT * FROM lab_batches WHERE user_id=? ORDER BY id DESC',(uid,)).fetchall()
        for batch in batches:
            if not c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(batch['id'],)).fetchone():owners.append(owner_item(store,c,uid,'batch',batch['id']))
            if batch['status']=='deleting':continue
            sources[batch['predictor_id']]=dict(id=batch['predictor_id'],name=batch['predictor_name'])
            for exp in c.execute('SELECT condition_key,name,settlement_rule_id,settlement_snapshot_json FROM lab_experiments WHERE batch_id=?',(batch['id'],)):
                key=(batch['metric'],exp['condition_key'])
                if key not in conditions:conditions[key]=dict(metric=batch['metric'],condition_key=exp['condition_key'],name=exp['name'],sources=set())
                conditions[key]['sources'].add(batch['predictor_id']);rid=exp['settlement_rule_id']
                rules[rid]=dict(id=rid,name=_load(exp['settlement_snapshot_json'])['rule']['name'])
        for study in c.execute('SELECT id FROM lab_studies WHERE user_id=? ORDER BY id DESC',(uid,)):owners.append(owner_item(store,c,uid,'study',study['id']))
        return dict(sources=list(sources.values()),conditions=[dict(metric=v['metric'],condition_key=v['condition_key'],name=v['name'],source_count=len(v['sources'])) for v in conditions.values()],
            settlement_rules=list(rules.values()),owners=owners,limits=dict(max_sources=20,max_owners=20,max_favorites=500,max_months=12,max_quality_days=366,quality_page_size=200))


REASONS={'legacy_engine':'旧引擎结果需复核','input_conflict':'输入存在冲突','archive_gap':'输入有过期或缺档','window_pending':'窗口内仍有待结算',
    'history_mismatch':'日账和累计结果不一致','study_not_comparable':'原比较组输入不可比','known_skips':'回放含已知跳过',
    'window_not_reached':'尚未覆盖整个窗口','partial_window':'窗口未完整','no_samples':'窗口没有有效已结算样本','not_backtest':'仅历史回测参与排名',
    'deleting':'来源正在删除','insufficient_sources':'此口径尚无两个不同来源的合格候选'}


def settlement_behavior(snapshot,metric):
    """Mirror settlement precedence, excluding names and catalog provenance."""
    rule=snapshot.get('rule') or {};resolved=snapshot.get('resolved_odds')
    effective=resolved if isinstance(resolved,dict) and resolved else snapshot.get('implemented_odds') or rule.get('implemented_odds') or {}
    return dict(settlement_rule_id=snapshot.get('settlement_rule_id'),metric=metric,
        profit_rule_id=rule.get('profit_rule_id'),odds_profile=rule.get('odds_profile'),
        straight_wraparound=_rule_straight_wraparound(rule),metric_settlement=_metric_spec_for_rule(rule,metric),
        effective_odds=effective.get(metric),odds_overrides=None if resolved else snapshot.get('odds_overrides') or {},
        fallback_profit_ratio=snapshot.get('fallback_profit_ratio'))


def rankings(store,uid,payload,*,now):
    validate_request(payload,('predictor_ids','metric','condition_key','settlement_rule_id','months','include_partial','end_date','sort','limit','cursor'))
    ids=payload['predictor_ids']
    if not isinstance(ids,list) or not 1<=len(ids)<=20 or any(type(v) is not int or v<=0 for v in ids):raise RuleLabError('invalid_input','请选择1至20个有效来源')
    ids=list(dict.fromkeys(ids));metric=payload['metric'];condition=payload['condition_key'];rule=payload['settlement_rule_id'];sort=payload['sort'];limit=payload['limit']
    if metric not in METRICS or not isinstance(condition,str) or not re.fullmatch(r'[0-9a-f]{24}',condition) or rule not in ALLOWED_SETTLEMENT_RULE_IDS:
        raise RuleLabError('invalid_input','请选择有效的玩法、条件和赔率')
    if sort not in ('roi','net_profit','drawdown') or type(limit) is not int or not 1<=limit<=100:raise RuleLabError('invalid_input','排序或分页参数无效')
    try:
        end=date.fromisoformat(payload['end_date'])
        if end.isoformat()!=payload['end_date'] or end>local_time(now).date():raise ValueError('结束日期无效')
        window=observation_window(payload['end_date'],months=payload['months'],include_partial=payload['include_partial'],now=now)
    except (ValueError,TypeError) as exc:raise RuleLabError('invalid_input',str(exc)) from exc
    filters={k:v for k,v in payload.items() if k!='cursor'};filters['predictor_ids']=ids
    with store._connect() as c:
        c.execute('BEGIN')
        # Ownership joins occur before all grouping or statistics queries.
        experiments=c.execute('''SELECT e.* FROM lab_experiments e JOIN lab_batches b ON b.id=e.batch_id
            WHERE b.user_id=? AND b.predictor_id IN ('''+','.join('?' for _ in ids)+''') AND b.metric=?
            AND e.condition_key=? AND e.settlement_rule_id=? ORDER BY e.id''',[uid]+ids+[metric,condition,rule]).fetchall()
        batches={row['id']:row for row in c.execute('SELECT * FROM lab_batches WHERE user_id=?',(uid,))}
        selected=[batches[bid] for bid in dict.fromkeys(e['batch_id'] for e in experiments)]
        observations=load_observations(store,c,selected,experiments,months=payload['months'],include_partial=payload['include_partial'],now=now,window=window)
        favs={r['source_experiment_id']:r['id'] for r in c.execute('SELECT id,source_experiment_id FROM lab_favorites WHERE user_id=? AND deleted_at IS NULL',(uid,))}
        daily=defaultdict(list)
        if experiments:
            eids=[e['id'] for e in experiments]
            for row in c.execute('SELECT * FROM lab_daily WHERE experiment_id IN ('+','.join('?' for _ in eids)+') AND stat_date>=? AND stat_date<=? ORDER BY experiment_id,stat_date',eids+[window['start_date'],window['end_date']]):daily[row['experiment_id']].append(row)
        items=[];groups={}
        for exp in experiments:
            batch=batches[exp['batch_id']];observation=observations[exp['id']];policy=normalize_risk_policy(_load(batch['risk_policy_json']));policy={k:v for k,v in policy.items() if k!='source'}
            snapshot=_load(exp['settlement_snapshot_json']);frozen_condition=_load(exp['condition_json'])
            semantic={k:v for k,v in frozen_condition.items() if k not in ('name','condition_key')}
            key=fingerprint(dict(condition=semantic,settlement=settlement_behavior(snapshot,metric),risk=policy,stake_cents=batch['stake_cents'],mode=batch['mode'],window=window))
            if key not in groups:groups[key]=dict(key=key,label='口径 '+str(len(groups)+1),eligible_count=0,sources=set(),stake_cents=batch['stake_cents'],risk_policy=policy,settlement=settlement_behavior(snapshot,metric))
            blocked=list(observation['quality']['blockers'])
            if batch['mode']!='backtest':blocked.append('not_backtest')
            if batch['status']=='deleting':blocked.append('deleting')
            if not observation['window']['period_reached']:blocked.append('window_not_reached')
            if observation['window']['has_partial'] or not all(month['complete_month'] for month in observation['months']):blocked.append('partial_window')
            if 'known_skips' in observation['quality']['warnings']:blocked.append('known_skips')
            values=daily[exp['id']];stake=sum(v['stake_cents'] for v in values);net=sum(v['net_cents'] for v in values);count=sum(v['settled_count'] for v in values)
            if not count or not stake:blocked.append('no_samples')
            equity=peak=drawdown=0
            for value in values:equity+=value['net_cents'];peak=max(peak,equity);drawdown=max(drawdown,peak-equity)
            item=dict(batch_id=batch['id'],experiment_id=exp['id'],batch_name=batch['name'],predictor_id=batch['predictor_id'],predictor_name=batch['predictor_name'],
                metric=metric,condition_key=condition,condition_name=exp['name'],settlement_rule_id=rule,settlement_rule_name=snapshot['rule']['name'],group_key=key,group_label=groups[key]['label'],
                rank=None,eligible=not blocked,reasons=[dict(code=v,message=REASONS.get(v,v)) for v in dict.fromkeys(blocked)],
                metrics=dict(net_cents=net,stake_cents=stake,roi_percent=round(net/stake*100,6) if stake else None,settled_count=count,betting_days=sum(v['settled_count']>0 for v in values),daily_drawdown_cents=drawdown if count else None),
                observation=observation,favorite_id=favs.get(exp['id']),source_updated_at=batch['updated_at'])
            if not blocked:groups[key]['eligible_count']+=1;groups[key]['sources'].add(batch['predictor_id'])
            items.append(item)
        metric_key={'roi':'roi_percent','net_profit':'net_cents','drawdown':'daily_drawdown_cents'}[sort]
        def value(item):
            raw=item['metrics'][metric_key]
            return (raw if sort=='drawdown' else -raw) if raw is not None else float('inf')
        for group in groups.values():
            ranked=sorted((r for r in items if r['group_key']==group['key'] and r['eligible']),key=lambda r:(value(r),r['experiment_id']))
            if len(group['sources'])<2:
                for row in ranked:row['reasons'].append(dict(code='insufficient_sources',message=REASONS['insufficient_sources']))
                continue
            previous=None;rank=0
            for position,row in enumerate(ranked,1):
                actual=row['metrics'][metric_key]
                if previous is None or actual!=previous:rank=position
                row['rank']=rank;previous=actual
        items.sort(key=lambda r:(r['group_label'],not r['eligible'],value(r),r['experiment_id']))
        public_groups=[dict(key=g['key'],label=g['label'],eligible_count=g['eligible_count'],source_count=len(g['sources']),stake_cents=g['stake_cents'],risk_policy=g['risk_policy'],settlement=g['settlement']) for g in groups.values()]
        revision='ranking:'+fingerprint(dict(filters=filters,window=window,items=items,groups=public_groups));offset=0
        if payload['cursor'] is not None:
            cursor=decode_cursor(payload['cursor'])
            if set(cursor)!= {'revision','offset'} or cursor['revision']!=revision:raise RuleLabError('stale_revision','排名结果或筛选已变化，请从第一页刷新',409)
            offset=cursor['offset']
            if type(offset) is not int or not 0<=offset<len(items):raise RuleLabError('invalid_input','分页位置无效')
        return dict(window=window,revision=revision,items=items[offset:offset+limit],groups=public_groups,total=len(items),
            next_cursor=encode_cursor(dict(revision=revision,offset=offset+limit)) if offset+limit<len(items) else None,
            warnings=['跨来源结果为同口径参考，预测输入不同，不代表因果优劣'],generated_at=now)
