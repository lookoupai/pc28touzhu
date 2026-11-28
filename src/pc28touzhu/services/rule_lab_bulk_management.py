"""Preview every selected owner, then mutate all owners in one transaction."""
from __future__ import annotations
from datetime import date
from pc28touzhu.services.rule_lab_store import RuleLabError
from pc28touzhu.services.rule_lab_templates import validate_request,receipt,save_receipt,fingerprint
from pc28touzhu.services.rule_lab_management import _owned,_token,_name,_delete_preview,rename_owner_tx,request_delete_tx
from pc28touzhu.services.rule_lab_series import _preview,request_append_tx,_time,SHANGHAI
from pc28touzhu.services.rule_lab_research import owner_item
FIELDS=('action','owners','names','end_date')


def normalize(payload,*,now):
    validate_request(payload,FIELDS)
    action=payload['action'];selected=payload['owners']
    if action not in ('rename','delete','append') or not isinstance(selected,list) or not 1<=len(selected)<=20:
        raise RuleLabError('invalid_input','请选择1至20个对象及合法批量操作')
    owners=[];seen=set();warnings=[]
    for item in selected:
        validate_request(item,('owner_kind','owner_id'))
        kind=item['owner_kind'];oid=item['owner_id']
        if kind not in ('batch','study') or type(oid) is not int or oid<=0:raise RuleLabError('invalid_input','管理对象无效')
        key=(kind,oid)
        if key in seen:warnings.append('重复选择已合并');continue
        seen.add(key);owners.append(item)
    names={}
    if action=='rename':
        if not isinstance(payload['names'],list) or len(payload['names'])!=len(owners):raise RuleLabError('invalid_input','请为每个对象提供一个名称')
        for item in payload['names']:
            validate_request(item,('owner_kind','owner_id','name'))
            if item['owner_kind'] not in ('batch','study') or type(item['owner_id']) is not int:raise RuleLabError('invalid_input','改名对象无效')
            key=(item['owner_kind'],item['owner_id'])
            if key not in seen or key in names:raise RuleLabError('invalid_input','名称对象重复或未被选择')
            names[key]=_name(item['name'])
    elif payload['names'] is not None:raise RuleLabError('invalid_input','本操作不接受改名列表')
    if action=='append':
        try:
            end=date.fromisoformat(payload['end_date'])
            if end.isoformat()!=payload['end_date'] or end>_time(now).astimezone(SHANGHAI).date():raise ValueError()
        except (ValueError,TypeError):raise RuleLabError('invalid_input','追加截止日期无效')
    elif payload['end_date'] is not None:raise RuleLabError('invalid_input','本操作不接受追加日期')
    return action,owners,names,list(dict.fromkeys(warnings))


def preview_tx(service,c,uid,payload,*,now,source_errors=None):
    action,owners,names,warnings=normalize(payload,now=now);items=[];blockers=[];revisions=[];affected_batches=set();total_experiments=0
    allowed={item['owner_id'] for item in owners if item['owner_kind']=='study'}
    for ref in owners:
        kind=ref['owner_kind'];oid=ref['owner_id'];row=_owned(service.store,c,uid,kind,oid)
        item=owner_item(service.store,c,uid,kind,oid);problems=[];counts=None;continuation=item['continuation'];revision=item['expected_revision']
        if kind=='batch' and c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(oid,)).fetchone():
            problems.append(dict(code='group_action_required',message='比较组子方案需从比较组整体管理'))
        if row['status']=='deleting':problems.append(dict(code='deleting',message='对象正在删除'))
        if action=='delete':
            deletion,_,bids,_,_=_delete_preview(service.store,c,uid,kind,oid,allowed_study_ids=allowed)
            counts=deletion['counts'];problems.extend(deletion['blockers']);affected_batches.update(bids)
        elif action=='append':
            value,rows,series=_preview(service.store,c,uid,kind,oid,now);continuation=value['continuation'];revision=value['expected_revision'];problems.extend(value['blockers'])
            last=continuation.get('calculated_through_date') or (rows[0]['end_date'] if rows else None)
            if last and not 0<=(date.fromisoformat(payload['end_date'])-date.fromisoformat(last)).days<=90:problems.append(dict(code='invalid_range',message='单次续算不能倒退或超过90天'))
            if series:
                if not series['capture_enabled']:problems.append(dict(code='capture_disabled',message='请先开启该对象后续数据保留'))
                if c.execute("SELECT 1 FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused')",(series['id'],)).fetchone():problems.append(dict(code='segment_active',message='已有未结束计算段，请先完成或恢复'))
                if c.execute("SELECT 1 FROM lab_continuation_requests WHERE series_id=? AND status='waiting_inputs' AND kind='append'",(series['id'],)).fetchone():problems.append(dict(code='append_waiting',message='已有追加请求等待输入'))
            if source_errors and item['predictor_id'] in source_errors:problems.append(source_errors[item['predictor_id']])
        if action!='delete':
            bids=[oid] if kind=='batch' else [r[0] for r in c.execute('SELECT batch_id FROM lab_study_batches WHERE study_id=?',(oid,))]
            affected_batches.update(bids)
        total_experiments+=item['experiment_count'];revisions.append(dict(ref=ref,revision=revision))
        item.update(eligible=not problems,blockers=problems,old_name=row['name'],new_name=names.get((kind,oid)),counts=counts,continuation=continuation)
        items.append(item)
        blockers.extend(dict(ref,**problem) for problem in problems)
    return dict(available=not blockers,preview_revision='bulk-management:'+fingerprint(dict(payload=payload,revisions=revisions)),action=action,items=items,
        totals=dict(owners=len(items),batches=len(affected_batches),experiments=total_experiments),blockers=blockers,warnings=warnings,
        confirmation_text='删除 %s 个对象'%len(items) if action=='delete' else None)


def _source_checks(service,uid,payload,*,now):
    action,owners,_,_=normalize(payload,now=now)
    if action!='append':return {}
    with service.store._connect() as c:
        predictors={owner_item(service.store,c,uid,r['owner_kind'],r['owner_id'])['predictor_id'] for r in owners}
    errors={}
    for pid in predictors:
        try:service._require_continuation_source(pid)
        except RuleLabError as exc:errors[pid]=dict(code=exc.code,message=exc.message)
    return errors


def preview(service,uid,payload,*,now):
    errors=_source_checks(service,uid,payload,now=now)
    with service.store._connect() as c:
        c.execute('BEGIN');return preview_tx(service,c,uid,payload,now=now,source_errors=errors)


def execute(service,uid,payload,*,now):
    validate_request(payload,FIELDS+('request_id','preview_revision','confirm_text'))
    if not isinstance(payload['preview_revision'],str):raise RuleLabError('invalid_input','请先预览批量操作')
    with service.store._connect() as c:
        previous=receipt(c,uid,'management.bulk',payload)
        if previous:return previous
    raw={k:payload[k] for k in FIELDS};errors=_source_checks(service,uid,raw,now=now)
    with service.store._connect(write=True,reserve=payload['action']=='delete') as c:
        previous=receipt(c,uid,'management.bulk',payload)
        if previous:return previous
        value=preview_tx(service,c,uid,raw,now=now,source_errors=errors)
        if payload['preview_revision']!=value['preview_revision']:raise RuleLabError('stale_revision','所选对象已变化，请重新预览',409)
        if not value['available']:raise RuleLabError(value['blockers'][0]['code'],value['blockers'][0]['message'],409)
        if payload['confirm_text']!=value['confirmation_text']:raise RuleLabError('confirmation_mismatch','请填写完整删除确认短语',409)
        action=payload['action'];result_items=[];jobs=[]
        selected=value['items'];allowed={item['owner_id'] for item in selected if item['owner_kind']=='study'}
        if action=='delete':selected=sorted(selected,key=lambda item:item['owner_kind']!='study')
        for index,item in enumerate(selected):
            kind=item['owner_kind'];oid=item['owner_id'];token='bulk-'+fingerprint(dict(user_id=uid,request_id=payload['request_id'],kind=kind,owner_id=oid))[:56]
            common=dict(request_id=token,expected_revision=item['expected_revision']);job=append=None;name=item['new_name'] if action=='rename' else item['old_name']
            if action=='rename':
                # All selected tokens were validated before any write. A prior
                # selected study rename may change its source dependency label.
                common['expected_revision']=_token(c,kind,_owned(service.store,c,uid,kind,oid))
                rename_owner_tx(service.store,c,uid,kind,oid,dict(common,name=name),now=now)
            elif action=='delete':
                # A preceding selected group's state change does not alter this
                # source's authorized configuration or dependency membership.
                common['expected_revision']=_token(c,kind,_owned(service.store,c,uid,kind,oid))
                result=request_delete_tx(service.store,c,uid,kind,oid,dict(common,confirm_name=item['old_name']),now=now,allowed_study_ids=allowed)
                job=result['job'];jobs.append(job)
            else:
                continuation,_,_=_preview(service.store,c,uid,kind,oid,now)
                common['expected_revision']=continuation['expected_revision']
                append=request_append_tx(service.store,c,uid,kind,oid,dict(common,end_date=payload['end_date']),now=now)
            result_items.append(dict(owner_kind=kind,owner_id=oid,name=name,job=job,append=append))
        result=dict(created=True,action=action,affected_owners=len(selected),items=result_items,jobs=jobs,request_id=payload['request_id'])
        save_receipt(c,uid,'management.bulk',payload,result,now);return result
