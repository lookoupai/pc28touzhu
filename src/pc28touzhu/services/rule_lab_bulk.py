"""Bounded previews and atomic creation of independent experiments and studies."""
from __future__ import annotations

import copy
import re

from pc28touzhu.domain.rule_lab import METRICS
from pc28touzhu.domain.rule_lab_risk import normalize_risk_policy
from pc28touzhu.domain.rule_lab_comparison import risk_variants
from pc28touzhu.services.rule_lab_store import RuleLabError, _dump, _load, MAX_BATCHES_PER_USER
from pc28touzhu.services.rule_lab_templates import (
    validate_request, fingerprint, receipt, save_receipt, resolve_risk,
)
from pc28touzhu.services.rule_lab_management import _insert_batch, allocate_id
from pc28touzhu.services.rule_lab_series import _configuration

BULK_FIELDS=('mode','sources','start_date','end_date','stake_amount','metric_configs')
STUDY_FIELDS=('source_batch_id','condition_keys','settlement_rule_ids','candidates')


def capacity(store,c,user_id):
    result=store._capacity(c)
    count=c.execute('SELECT COUNT(*) FROM lab_batches WHERE user_id=?',(user_id,)).fetchone()[0]
    return dict(result,saved_batches=count,max_batches=MAX_BATCHES_PER_USER,remaining_batches=max(0,MAX_BATCHES_PER_USER-count))


def blockers_for(cap,batches,experiments):
    blockers=[]
    if cap['remaining_saved']<experiments:
        blockers.append(dict(code='capacity_exceeded',message='保存版本额度不足，还需释放%s个版本'%(experiments-cap['remaining_saved'])))
    if cap['remaining_batches']<batches:
        blockers.append(dict(code='capacity_exceeded',message='当前账号批次额度不足，还需释放%s个批次'%(batches-cap['remaining_batches'])))
    return blockers


def _bulk_rows(service,user_id,payload,now):
    validate_request(payload,BULK_FIELDS,optional=('name_pattern','names'))
    sources=payload['sources'];metrics=payload['metric_configs'];warnings=[]
    if not isinstance(sources,list) or not 1<=len(sources)<=20 or any(type(pid) is not int or pid<1 for pid in sources):
        raise RuleLabError('invalid_input','请选择1至20个有效来源')
    unique=list(dict.fromkeys(sources))
    if len(unique)!=len(sources):warnings.append('已合并重复来源')
    if not isinstance(metrics,list) or not 1<=len(metrics)<=3:raise RuleLabError('invalid_input','请选择1至3种玩法')
    metric_values=[]
    for item in metrics:
        validate_request(item,('metric','condition_selection','risk_selection'))
        if not isinstance(item['metric'],str) or item['metric'] not in METRICS or item['metric'] in metric_values:
            raise RuleLabError('invalid_input','玩法无效或重复')
        metric_values.append(item['metric'])
    pattern=payload.get('name_pattern','{source} · {metric} · {template}')
    if not isinstance(pattern,str) or not 1<=len(pattern)<=150 or re.sub(r'\{(?:source|metric|template)\}','',pattern).count('{') or re.sub(r'\{(?:source|metric|template)\}','',pattern).count('}'):
        raise RuleLabError('invalid_input','名称格式只支持{source}、{metric}和{template}')
    names=payload.get('names',[])
    if not isinstance(names,list) or len(names)>60:raise RuleLabError('invalid_input','自定义批次名称列表无效')
    overrides={}
    for item in names:
        validate_request(item,('predictor_id','metric','name'))
        if type(item['predictor_id']) is not int or item['predictor_id'] not in unique or item['metric'] not in metric_values:
            raise RuleLabError('invalid_input','批次名称包含未选择的来源或玩法')
        key=(item['predictor_id'],item['metric'])
        if key in overrides:raise RuleLabError('invalid_input','同一批次不能重复指定名称')
        overrides[key]=item['name']
    rows=[]
    for pid in unique:
        for metric in metrics:
            draft={k:payload[k] for k in ('mode','start_date','end_date','stake_amount')}
            draft.update(name='批量实验',predictor_id=pid,**metric)
            config,predictor,experiments=service._resolve_definition(user_id,draft,now=now)
            label={'big_small':'大小','odd_even':'单双','combo':'组合'}[metric['metric']]
            template=config.get('template_snapshot',{}).get('name',config['template_pack'])
            name=overrides.get((pid,metric['metric']),pattern.replace('{source}',predictor['name']).replace('{metric}',label).replace('{template}',template))
            if not isinstance(name,str) or not 1<=len(name.strip())<=80:raise RuleLabError('invalid_input','批次名称须为1至80个字符')
            config['name']=name.strip()
            rows.append(dict(config=config,predictor_name=predictor['name'],experiments=experiments))
    return rows,warnings


def _bulk_preview(service,user_id,rows,warnings,*,c=None):
    if c is None:
        with service.store._connect() as connection:cap=capacity(service.store,connection,user_id)
    else:cap=capacity(service.store,c,user_id)
    count=sum(len(row['experiments']) for row in rows)
    blockers=blockers_for(cap,len(rows),count)
    batches=[]
    for row in rows:
        config=row['config']
        batches.append(dict(predictor_id=config['predictor_id'],predictor_name=row['predictor_name'],metric=config['metric'],
            name=config['name'],condition_count=len({e['condition_key'] for e in row['experiments']}),experiment_count=len(row['experiments']),
            start_date=config['start_date'],end_date=config['end_date'],template_snapshot=config.get('template_snapshot',{}),
            risk_template_snapshot=config.get('risk_template_snapshot',{}),risk_policy=config['risk_policy']))
    return dict(available=not blockers,preview_revision='bulk:'+fingerprint(rows),batches=batches,batch_count=len(rows),
        new_experiments=count,capacity=cap,blockers=blockers,warnings=warnings)


def preview_bulk(service,user_id,payload,*,now):
    rows,warnings=_bulk_rows(service,user_id,payload,now)
    return _bulk_preview(service,user_id,rows,warnings)


def create_bulk(service,user_id,payload,*,now):
    validate_request(payload,BULK_FIELDS+('request_id','preview_revision','enqueue'),optional=('name_pattern','names'))
    if type(payload['enqueue']) is not bool or not isinstance(payload['preview_revision'],str):raise RuleLabError('invalid_input','预览版本或入队开关无效')
    with service.store._connect() as c:
        previous=receipt(c,user_id,'bulk.create',payload)
        if previous:return previous
    if payload['enqueue'] and payload['mode']!='backtest':raise RuleLabError('invalid_input','前向模拟仅保存草稿，请从批次单独启动')
    raw={k:v for k,v in payload.items() if k not in ('request_id','preview_revision','enqueue')}
    rows,warnings=_bulk_rows(service,user_id,raw,now)
    with service.store._connect(write=True) as c:
        previous=receipt(c,user_id,'bulk.create',payload)
        if previous:return previous
        preview=_bulk_preview(service,user_id,rows,warnings,c=c)
        if preview['preview_revision']!=payload['preview_revision']:raise RuleLabError('stale_revision','创建配置已变化，请重新预览',409)
        if preview['blockers']:raise RuleLabError('capacity_exceeded',preview['blockers'][0]['message'],409)
        service.store._require_space();ids=[]
        for row in rows:
            config=dict(row['config'],creation_request_id=payload['request_id'])
            bid=_insert_batch(c,user_id,config,row['predictor_name'],row['experiments'],now);ids.append(bid)
            if payload['enqueue']:
                from pc28touzhu.services.rule_lab_queue import register
                register(service.store,c,user_id,'batch',bid,now,origin='bulk')
        result=dict(created=True,batches=[service.store._batch(c,service.store._owned(c,user_id,bid)) for bid in ids],
            generated_count=preview['new_experiments'],queued_count=len(ids) if payload['enqueue'] else 0,creation_request_id=payload['request_id'])
        save_receipt(c,user_id,'bulk.create',payload,result,now);return result


def _policy_identity(policy):
    return fingerprint({k:v for k,v in normalize_risk_policy(policy).items() if k!='source'})


def _changes(baseline,policy):
    changes=[]
    def display(value,percent=False):
        if value is None:return '未配置'
        if type(value) is bool:return '开启' if value else '关闭'
        return format(value,'g')+('%' if percent else '')
    def field(left,right,key,label,percent=False):
        if left.get(key)!=right.get(key):
            changes.append(label+' '+display(left.get(key),percent)+' → '+display(right.get(key),percent))
    def control(left,right,label):
        left=left or {};right=right or {}
        for key,title in [('enabled','启用'),('profit_target','止盈'),('loss_limit','止损'),
                          ('max_bets','最多下注'),('max_rounds','最多开轮')]:
            field(left,right,key,label+title)
        old_gate=left.get('hit_rate_gate',{});new_gate=right.get('hit_rate_gate',{})
        field(old_gate,new_gate,'enabled',label+'命中率熔断')
        field(old_gate,new_gate,'threshold',label+'实赔命中率阈值',True)
        field(old_gate,new_gate,'min_settled_count',label+'最少有效单数')
    control(baseline.get('round'),policy.get('round'),'单轮')
    control(baseline.get('day'),policy.get('day'),'主日层')
    old_guards=baseline.get('additional_day_guards',[]);new_guards=policy.get('additional_day_guards',[])
    for index in range(max(len(old_guards),len(new_guards))):
        control(old_guards[index] if index<len(old_guards) else {},
                new_guards[index] if index<len(new_guards) else {},'附加日层'+str(index+1))
    field(baseline,policy,'cooldown_issues','开轮间隔期数')
    if len(changes)>1:changes.insert(0,'多项参数变化')
    return changes


def _study_selection(service,c,user_id,payload):
    validate_request(payload,STUDY_FIELDS)
    bid=payload['source_batch_id']
    if type(bid) is not int or bid<1:raise RuleLabError('invalid_input','请选择有效的比较来源批次')
    batch,all_experiments=service.store._comparison_source(c,user_id,bid)
    keys=payload['condition_keys'];rules=payload['settlement_rule_ids'];candidates=payload['candidates']
    if not isinstance(keys,list) or not 1<=len(keys)<=50 or any(not isinstance(v,str) for v in keys):raise RuleLabError('invalid_input','请选择有效的条件')
    if not isinstance(rules,list) or not 1<=len(rules)<=2 or any(not isinstance(v,str) for v in rules):raise RuleLabError('invalid_input','请选择实际存在的赔率')
    if not isinstance(candidates,list) or not 1<=len(candidates)<=48:raise RuleLabError('invalid_input','请选择1至48项风控候选')
    pairs={(e['condition_key'],e['settlement_rule_id']):e for e in all_experiments}
    if any((key,rid) not in pairs for key in keys for rid in rules):raise RuleLabError('invalid_input','来源没有选定条件与赔率的完整冻结组合')
    experiments=[e for e in all_experiments if e['condition_key'] in keys and e['settlement_rule_id'] in rules]
    original=_load(batch['risk_policy_json']);presets={v['key']:v for v in risk_variants(original,metric=batch['metric'])}
    baseline=dict(presets['baseline'],risk_template_snapshot=_load(batch['risk_template_snapshot_json']))
    variants=[baseline];seen={_policy_identity(original)};warnings=[];references=[]
    for candidate in candidates:
        if not isinstance(candidate,dict):raise RuleLabError('invalid_input','风控候选参数无效')
        kind=candidate.get('kind');policy=None;available=True;reason='';snapshot={}
        if kind=='preset':
            validate_request(candidate,('kind','key'))
            if not isinstance(candidate['key'],str) or candidate['key'] not in presets:raise RuleLabError('invalid_input','未知风控快捷项')
            item=copy.deepcopy(presets[candidate['key']])
            if item['key']=='baseline':continue
            policy=item['policy'];key=item['key'];label=item['label'];reason=item['reason'];available=item['available']
        elif kind=='gate':
            validate_request(candidate,('kind','threshold','min_settled_count'))
            policy=copy.deepcopy(original);controls=[policy['day']]+policy.get('additional_day_guards',[])
            available=any(control['enabled'] for control in controls)
            # Validate even when no active layer can use the candidate.
            probe=copy.deepcopy(policy);probe['day']['hit_rate_gate']=dict(enabled=True,threshold=candidate['threshold'],min_settled_count=candidate['min_settled_count'])
            normalize_risk_policy(probe)
            for control in controls:
                if control['enabled']:control['hit_rate_gate'].update(enabled=True,threshold=candidate['threshold'],min_settled_count=candidate['min_settled_count'])
            key='gate_'+str(candidate['threshold'])+'_'+str(candidate['min_settled_count']);label='熔断 %s%% / %s 单'%(candidate['threshold'],candidate['min_settled_count'])
            reason='' if available else '所有日风控层均未启用'
        elif kind=='risk_template':
            validate_request(candidate,('kind','template_id','expected_revision'))
            reference=dict(template_ref=dict(kind='custom',id=candidate['template_id']),expected_revision=candidate['expected_revision'])
            policy,snapshot=resolve_risk(c,user_id,reference,batch['metric']);references.append(reference)
            key='template_'+str(candidate['template_id']);label=snapshot['name']
        elif kind=='policy':
            validate_request(candidate,('kind','label','policy'))
            if not isinstance(candidate['label'],str) or not 1<=len(candidate['label'].strip())<=60:raise RuleLabError('invalid_input','对照名称须为1至60个字符')
            policy=normalize_risk_policy(candidate['policy']);key='policy_'+_policy_identity(policy)[:16];label=candidate['label'].strip()
        else:raise RuleLabError('invalid_input','未知风控候选类型')
        if not available:
            warnings.append(label+'：'+reason);continue
        policy=normalize_risk_policy(policy)
        if policy['mode']!='round':raise RuleLabError('invalid_input','比较方案必须使用按轮风控')
        identity=_policy_identity(policy)
        if identity in seen:
            warnings.append(label+'：与基准或已有方案相同，不重复生成');continue
        seen.add(identity)
        if kind!='policy' and policy.get('source'):policy['source']['modified']=True
        variants.append(dict(key=key,label=label,available=True,reason='',policy=policy,
            risk_template_snapshot=snapshot,changes=_changes(original,policy)))
    if not 2<=len(variants)<=12:raise RuleLabError('invalid_input','去重后需要2至12个有效风控方案（包含基准）')
    source_revision=_configuration(c,[batch]);cap=capacity(service.store,c,user_id)
    blocked=blockers_for(cap,len(variants),len(experiments)*len(variants))
    conditions=list({e['condition_key']:_load(e['condition_json']) for e in experiments}.values())
    selected_rules=list({e['settlement_rule_id']:dict(id=e['settlement_rule_id'],name=_load(e['settlement_snapshot_json'])['rule']['name']) for e in experiments}.values())
    # Runtime stats and cursor changes are deliberately absent from preview identity.
    revision='study:'+fingerprint(dict(source_revision=source_revision,pairs=sorted((e['condition_key'],e['settlement_rule_id']) for e in experiments),variants=variants,references=references))
    preview=dict(available=not blocked,preview_revision=revision,source_batch_id=bid,source_name=batch['name'],source_revision=source_revision,
        conditions=conditions,settlement_rules=selected_rules,variants=variants,candidate_count=len(variants),
        new_experiments=len(experiments)*len(variants),capacity=cap,blockers=blocked,warnings=warnings)
    return preview,batch,experiments


def _validate_candidate_sources(service,user_id,payload):
    payload=copy.deepcopy(payload)
    if isinstance(payload,dict) and type(payload.get('source_batch_id')) is int:
        batch=service.store.get_batch(user_id,payload['source_batch_id'])
        candidates=payload.get('candidates',[])
        if isinstance(candidates,list):
            for candidate in candidates:
                if isinstance(candidate,dict) and candidate.get('kind')=='policy':
                    candidate['policy']=service._risk_snapshot(user_id,candidate.get('policy'),batch['metric'])
    return payload


def preview_study(service,user_id,payload):
    payload=_validate_candidate_sources(service,user_id,payload)
    with service.store._connect() as c:
        c.execute('BEGIN')
        return _study_selection(service,c,user_id,payload)[0]


def create_selected_study(service,user_id,payload,*,now):
    validate_request(payload,STUDY_FIELDS+('name','request_id','preview_revision','enqueue'))
    if not isinstance(payload['name'],str) or not 1<=len(payload['name'].strip())<=80 or type(payload['enqueue']) is not bool or not isinstance(payload['preview_revision'],str):
        raise RuleLabError('invalid_input','比较组名称、预览版本或入队开关无效')
    with service.store._connect() as c:
        previous=receipt(c,user_id,'study.create',payload)
        if previous:return previous
    prepared=_validate_candidate_sources(service,user_id,payload)
    raw={k:prepared[k] for k in STUDY_FIELDS}
    with service.store._connect(write=True) as c:
        previous=receipt(c,user_id,'study.create',payload)
        if previous:return previous
        if c.execute('SELECT 1 FROM lab_studies WHERE user_id=? AND request_id=?',(user_id,payload['request_id'])).fetchone():
            raise RuleLabError('request_conflict','同一请求标识已用于其他比较组内容',409)
        preview,batch,experiments=_study_selection(service,c,user_id,raw)
        if preview['preview_revision']!=payload['preview_revision']:raise RuleLabError('stale_revision','比较配置已变化，请重新预览',409)
        if preview['blockers']:raise RuleLabError('capacity_exceeded',preview['blockers'][0]['message'],409)
        service.store._require_space();sid=allocate_id(c,'lab_studies');name=payload['name'].strip()
        c.execute('''INSERT INTO lab_studies(id,user_id,request_id,name,source_batch_id,request_hash,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?)''',(sid,user_id,payload['request_id'],name,batch['id'],fingerprint(payload),now,now))
        definitions=[dict(condition_key=e['condition_key'],name=e['name'],condition=_load(e['condition_json']),settlement_rule_id=e['settlement_rule_id'],settlement_snapshot=_load(e['settlement_snapshot_json'])) for e in experiments]
        template_snapshot=_load(batch['template_snapshot_json'])
        if template_snapshot:template_snapshot['condition_keys']=list(dict.fromkeys(e['condition_key'] for e in experiments))
        for position,variant in enumerate(preview['variants']):
            config={k:batch[k] for k in ('mode','predictor_id','metric','template_pack','stake_cents','start_date','end_date')}
            config.update(name=(name+' · '+variant['label'])[:80],risk_policy=variant['policy'],
                template_snapshot=template_snapshot,risk_template_snapshot=variant['risk_template_snapshot'],
                creation_request_id=payload['request_id'])
            bid=_insert_batch(c,user_id,config,batch['predictor_name'],definitions,now)
            c.execute('INSERT INTO lab_study_batches(study_id,batch_id,variant_key,position,label,change_json) VALUES(?,?,?,?,?,?)',
                (sid,bid,variant['key'],position,variant['label'],_dump(variant['changes'])))
        if payload['enqueue']:
            from pc28touzhu.services.rule_lab_queue import register
            register(service.store,c,user_id,'study',sid,now,origin='study')
        result=dict(created=True,study=service.store._study(c,service.store._owned_study(c,user_id,sid)))
        save_receipt(c,user_id,'study.create',payload,result,now);return result
