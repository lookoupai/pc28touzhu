"""Owned experiment edits and resumable deletion jobs; no upstream data writes."""
from __future__ import annotations

import hashlib
import json
import re

from pc28touzhu.domain.rule_lab import ALLOWED_SETTLEMENT_RULE_IDS, empty_stats
from pc28touzhu.domain.rule_lab_risk import normalize_risk_policy
from pc28touzhu.services.rule_lab_store import RuleLabError, _dump, _load

ID_TABLES = ('lab_batches','lab_experiments','lab_studies','lab_capture_streams','lab_archive_revisions','lab_series')


def init_schema(c):
    c.execute('''CREATE TABLE IF NOT EXISTS lab_id_watermarks (
        table_name TEXT PRIMARY KEY,last_id INTEGER NOT NULL)''')
    for table in ID_TABLES:
        c.execute('INSERT INTO lab_id_watermarks(table_name,last_id) SELECT ?,COALESCE(MAX(id),0) FROM '+table+
                  ' WHERE 1 ON CONFLICT(table_name) DO UPDATE SET last_id=MAX(last_id,excluded.last_id)',(table,))
    c.execute('''CREATE TABLE IF NOT EXISTS lab_object_controls (
        owner_kind TEXT NOT NULL,owner_id INTEGER NOT NULL,user_id INTEGER NOT NULL,
        generation INTEGER NOT NULL DEFAULT 0,queue_optout INTEGER NOT NULL DEFAULT 0,
        deleted INTEGER NOT NULL DEFAULT 0,updated_at TEXT NOT NULL,
        PRIMARY KEY(owner_kind,owner_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS lab_management_jobs (
        id INTEGER PRIMARY KEY,user_id INTEGER NOT NULL,kind TEXT NOT NULL,owner_kind TEXT,owner_id INTEGER,
        status TEXT NOT NULL DEFAULT 'queued',phase TEXT NOT NULL DEFAULT '',payload_json TEXT NOT NULL,
        processed_rows INTEGER NOT NULL DEFAULT 0,total_rows INTEGER NOT NULL DEFAULT 0,
        freed_bytes INTEGER NOT NULL DEFAULT 0,error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL)''')
    c.execute('CREATE INDEX IF NOT EXISTS lab_management_jobs_active ON lab_management_jobs(status,id)')
    c.execute('''CREATE TABLE IF NOT EXISTS lab_management_receipts (
        user_id INTEGER NOT NULL,request_id TEXT NOT NULL,action TEXT NOT NULL,owner_kind TEXT NOT NULL,
        owner_id INTEGER NOT NULL,payload_hash TEXT NOT NULL,result_json TEXT NOT NULL,job_id INTEGER,
        created_at TEXT NOT NULL,PRIMARY KEY(user_id,request_id))''')


def allocate_id(c, table):
    if table not in ID_TABLES:raise ValueError('Unsupported identifier table')
    row=c.execute('SELECT last_id FROM lab_id_watermarks WHERE table_name=?',(table,)).fetchone()
    # Max also accommodates explicit IDs inserted by migrations and fixtures.
    maximum=c.execute('SELECT COALESCE(MAX(id),0) FROM '+table).fetchone()[0]
    value=max(row[0] if row else 0,maximum)+1
    c.execute('INSERT INTO lab_id_watermarks(table_name,last_id) VALUES(?,?) ON CONFLICT(table_name) DO UPDATE SET last_id=excluded.last_id',(table,value))
    return value


def _hash(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def _request(payload,fields):
    if not isinstance(payload,dict) or set(payload)!={'request_id'}|set(fields):
        raise RuleLabError('invalid_input','管理请求字段不完整或包含未知字段')
    if not isinstance(payload['request_id'],str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,80}',payload['request_id']):
        raise RuleLabError('invalid_input','请求标识必须为8至80位字母、数字、下划线或短横线')
    if 'expected_revision' in fields and (not isinstance(payload['expected_revision'],str) or not 1<=len(payload['expected_revision'])<=150):
        raise RuleLabError('invalid_input','管理版本标识无效')


def _name(value):
    if not isinstance(value,str) or not 1<=len(value.strip())<=80:raise RuleLabError('invalid_input','名称必须为1至80个字符')
    return value.strip()


def _owned(store,c,user_id,kind,owner_id):
    if kind not in ('batch','study') or type(owner_id) is not int or owner_id<=0:raise RuleLabError('invalid_input','管理对象无效')
    return store._owned(c,user_id,owner_id) if kind=='batch' else store._owned_study(c,user_id,owner_id)


def _control(c,kind,owner_id,user_id,now,*,optout=False,deleted=False):
    c.execute('''INSERT INTO lab_object_controls(owner_kind,owner_id,user_id,generation,queue_optout,deleted,updated_at)
        VALUES(?,?,?,1,?,?,?) ON CONFLICT(owner_kind,owner_id) DO UPDATE SET generation=generation+1,
        queue_optout=MAX(queue_optout,excluded.queue_optout),deleted=MAX(deleted,excluded.deleted),updated_at=excluded.updated_at''',
        (kind,owner_id,user_id,int(optout),int(deleted),now))


def _members(c,kind,row):
    if kind=='batch':return [row['id']]
    return [r[0] for r in c.execute('SELECT batch_id FROM lab_study_batches WHERE study_id=? ORDER BY position',(row['id'],))]


def _dependencies(c,kind,row):
    if kind=='study':return []
    return [dict(id=r['id'],name=r['name']) for r in c.execute('SELECT id,name FROM lab_studies WHERE source_batch_id=? ORDER BY id',(row['id'],))]


def _token(c,kind,row):
    if kind=='batch':
        fields=('id','user_id','name','mode','predictor_id','metric','template_pack','stake_cents','start_date','end_date','status','risk_policy_json')
        fixed={k:row[k] for k in fields};fixed['started']=bool(row['started_at'])
        fixed['experiments']=[tuple(r) for r in c.execute('SELECT id,condition_json,settlement_snapshot_json FROM lab_experiments WHERE batch_id=? ORDER BY id',(row['id'],))]
        fixed['study_id']=next(iter(c.execute('SELECT study_id FROM lab_study_batches WHERE batch_id=?',(row['id'],))),[None])[0]
        fixed['dependencies']=_dependencies(c,kind,row)
    else:
        fixed={k:row[k] for k in ('id','user_id','name','source_batch_id','status')}
        fixed['children']=[tuple(r) for r in c.execute('''SELECT b.id,b.status,b.name,b.risk_policy_json FROM lab_study_batches x
            JOIN lab_batches b ON b.id=x.batch_id WHERE x.study_id=? ORDER BY b.id''',(row['id'],))]
    meta=c.execute('SELECT generation,deleted FROM lab_object_controls WHERE owner_kind=? AND owner_id=?',(kind,row['id'])).fetchone()
    return 'management:'+_hash([kind,fixed,list(meta) if meta else None])


def _config(row):
    return dict(name=row['name'],mode=row['mode'],predictor_id=row['predictor_id'],metric=row['metric'],
        template_pack=row['template_pack'],stake_amount=row['stake_cents']/100,start_date=row['start_date'],end_date=row['end_date'],risk_policy=_load(row['risk_policy_json']))


def _editable(c,row):
    return row['status']=='paused' and row['started_at'] is None and row['processed_issues']==0 and not any((
        c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(row['id'],)).fetchone(),
        c.execute("SELECT 1 FROM lab_series WHERE owner_kind='batch' AND owner_id=?",(row['id'],)).fetchone(),
        c.execute('SELECT 1 FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id WHERE e.batch_id=? LIMIT 1',(row['id'],)).fetchone()))


def management_preview(store,user_id,kind,owner_id,*,now):
    with store._connect() as c:
        row=_owned(store,c,user_id,kind,owner_id)
        child=kind=='batch' and c.execute('SELECT study_id FROM lab_study_batches WHERE batch_id=?',(owner_id,)).fetchone()
        controls=c.execute('SELECT queue_optout FROM lab_object_controls WHERE owner_kind=? AND owner_id=?',(kind,owner_id)).fetchone()
        blockers=[]
        if child:blockers.append(dict(code='group_action_required',message='比较组子方案请从比较组统一管理',study_id=child[0]))
        if row['status']=='deleting':blockers.append(dict(code='deleting',message='实验正在删除'))
        receipt=c.execute('SELECT request_id,action,job_id,result_json FROM lab_management_receipts WHERE user_id=? AND owner_kind=? AND owner_id=? ORDER BY created_at DESC,rowid DESC LIMIT 1',(user_id,kind,owner_id)).fetchone()
        latest=dict(request_id=receipt['request_id'],action=receipt['action'],job_id=receipt['job_id'],result=_load(receipt['result_json'])) if receipt else None
        return dict(latest_request=latest,owner_kind=kind,owner_id=owner_id,name=row['name'],expected_revision=_token(c,kind,row),
            editable=bool(kind=='batch' and _editable(c,row)),can_fork=bool(kind=='batch' and not child and row['status']!='deleting'),
            config=_config(row) if kind=='batch' else None,blockers=blockers,
            queue_managed=bool(kind=='batch' and not (controls and controls[0]) and row['started_at'] is None))


def _job(row):
    value={k:row[k] for k in ('id','kind','status','owner_kind','owner_id','processed_rows','total_rows','freed_bytes','error','created_at','updated_at','phase')}
    payload=_load(row['payload_json']);value['released_versions']=payload.get('released_versions',0)
    return value


def _receipt(c,user_id,kind,owner_id,payload,action):
    row=c.execute('SELECT * FROM lab_management_receipts WHERE user_id=? AND request_id=?',(user_id,payload.get('request_id'))).fetchone()
    if row is None:return None
    if (row['owner_kind']!=kind or row['owner_id']!=owner_id or row['action']!=action or row['payload_hash']!=_hash(payload)):
        raise RuleLabError('request_conflict','同一请求标识已用于不同管理内容',409)
    result=_load(row['result_json']);result['created']=False
    if row['job_id']:
        job=c.execute('SELECT * FROM lab_management_jobs WHERE id=? AND user_id=?',(row['job_id'],user_id)).fetchone()
        result['job']=_job(job)
    return result


def management_receipt(store,user_id,kind,owner_id,payload,action):
    if not isinstance(payload,dict) or not isinstance(payload.get('request_id'),str):return None
    with store._connect() as c:return _receipt(c,user_id,kind,owner_id,payload,action)


def _save_receipt(c,user_id,kind,owner_id,payload,action,result,now):
    c.execute('INSERT INTO lab_management_receipts(user_id,request_id,action,owner_kind,owner_id,payload_hash,result_json,job_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)',
        (user_id,payload['request_id'],action,kind,owner_id,_hash(payload),_dump(result),result.get('job',{}).get('id'),now))


def _validate_revision(c,kind,row,payload):
    if payload['expected_revision']!=_token(c,kind,row):raise RuleLabError('stale_revision','实验设置或状态已变化，请刷新后重试',409)
    if row['status']=='deleting':raise RuleLabError('deleting','实验正在删除，不能继续修改',409)


def rename_owner(store,user_id,kind,owner_id,payload,*,now):
    _request(payload,('expected_revision','name'));name=_name(payload['name'])
    with store._connect(write=True,reserve=True) as c:
        previous=_receipt(c,user_id,kind,owner_id,payload,'rename')
        if previous:return previous
        row=_owned(store,c,user_id,kind,owner_id);_validate_revision(c,kind,row,payload)
        table='lab_batches' if kind=='batch' else 'lab_studies'
        c.execute('UPDATE '+table+' SET name=?,updated_at=? WHERE id=?',(name,now,owner_id))
        _control(c,kind,owner_id,user_id,now)
        fresh=_owned(store,c,user_id,kind,owner_id)
        owner=store._batch(c,fresh) if kind=='batch' else store._study(c,fresh)
        result=dict(created=True,owner=owner);_save_receipt(c,user_id,kind,owner_id,payload,'rename',result,now)
        return result


def _definitions(experiments):
    if not isinstance(experiments,list) or not 1<=len(experiments)<=100:raise RuleLabError('invalid_input','实验版本数量无效')
    for e in experiments:
        if e['settlement_rule_id'] not in ALLOWED_SETTLEMENT_RULE_IDS or e['settlement_snapshot'].get('settlement_rule_id')!=e['settlement_rule_id']:
            raise RuleLabError('invalid_input','结算版本与快照不一致')


def insert_experiments(c,batch_id,experiments):
    _definitions(experiments)
    for e in experiments:
        eid=allocate_id(c,'lab_experiments')
        c.execute('''INSERT INTO lab_experiments(id,batch_id,condition_key,name,condition_json,settlement_rule_id,settlement_snapshot_json,stats_json)
            VALUES(?,?,?,?,?,?,?,?)''',(eid,batch_id,e['condition_key'],e['name'],_dump(e['condition']),e['settlement_rule_id'],_dump(e['settlement_snapshot']),_dump(empty_stats())))


def _insert_batch(c,user_id,config,predictor_name,experiments,now):
    bid=allocate_id(c,'lab_batches')
    c.execute('''INSERT INTO lab_batches(id,user_id,name,mode,predictor_id,predictor_name,metric,template_pack,stake_cents,start_date,end_date,
        created_at,updated_at,risk_policy_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
        (bid,user_id,config['name'],config['mode'],config['predictor_id'],predictor_name,config['metric'],config['template_pack'],config['stake_cents'],
         config['start_date'],config['end_date'],now,now,_dump(normalize_risk_policy(config['risk_policy']))))
    insert_experiments(c,bid,experiments);return bid


def edit_batch(store,user_id,batch_id,payload,*,predictor_name,experiments,now,request_payload=None):
    _request(payload,('expected_revision','config'));raw=request_payload or payload;_definitions(experiments)
    with store._connect(write=True) as c:
        previous=_receipt(c,user_id,'batch',batch_id,raw,'edit')
        if previous:return previous
        row=_owned(store,c,user_id,'batch',batch_id);_validate_revision(c,'batch',row,payload)
        if not _editable(c,row):raise RuleLabError('edit_locked','只有尚未启动且无记录的独立暂停批次可以原地编辑',409)
        old_count=c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(batch_id,)).fetchone()[0]
        if store._capacity(c)['remaining_saved']+old_count<len(experiments):raise RuleLabError('capacity_exceeded','修改后超过保存版本额度，原批次未改变',409)
        store._require_space();config=payload['config']
        c.execute('DELETE FROM lab_experiments WHERE batch_id=?',(batch_id,))
        insert_experiments(c,batch_id,experiments)
        c.execute('''UPDATE lab_batches SET name=?,mode=?,predictor_id=?,predictor_name=?,metric=?,template_pack=?,stake_cents=?,
            start_date=?,end_date=?,risk_policy_json=?,revision=revision+1,updated_at=? WHERE id=?''',
            (config['name'],config['mode'],config['predictor_id'],predictor_name,config['metric'],config['template_pack'],config['stake_cents'],
             config['start_date'],config['end_date'],_dump(normalize_risk_policy(config['risk_policy'])),now,batch_id))
        _control(c,'batch',batch_id,user_id,now,optout=True)
        result=dict(created=True,batch=store._batch(c,store._owned(c,user_id,batch_id)),queue_removed=True)
        _save_receipt(c,user_id,'batch',batch_id,raw,'edit',result,now);return result


def fork_batch(store,user_id,batch_id,payload,*,predictor_name,experiments,now,request_payload=None):
    _request(payload,('expected_revision','config'));raw=request_payload or payload;_definitions(experiments)
    with store._connect(write=True) as c:
        previous=_receipt(c,user_id,'batch',batch_id,raw,'fork')
        if previous:return previous
        row=_owned(store,c,user_id,'batch',batch_id);_validate_revision(c,'batch',row,payload)
        if c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(batch_id,)).fetchone():raise RuleLabError('group_action_required','请从独立批次建立新版本',409)
        if store._capacity(c)['remaining_saved']<len(experiments) or c.execute('SELECT COUNT(*) FROM lab_batches WHERE user_id=?',(user_id,)).fetchone()[0]>=50:
            raise RuleLabError('capacity_exceeded','保存版本或批次数量已达上限',409)
        store._require_space();bid=_insert_batch(c,user_id,payload['config'],predictor_name,experiments,now)
        result=dict(created=True,batch=store._batch(c,store._owned(c,user_id,bid)),source_batch_id=batch_id)
        _save_receipt(c,user_id,'batch',batch_id,raw,'fork',result,now);return result


def _counts(c,bids,sids):
    marks=','.join('?' for _ in bids);expr='SELECT id FROM lab_experiments WHERE batch_id IN ('+marks+')'
    result=dict(batches=len(bids),experiments=c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id IN ('+marks+')',bids).fetchone()[0])
    for key,table in (('decisions','lab_decisions'),('daily_rows','lab_daily'),('risk_days','lab_risk_days'),('rounds','lab_rounds')):
        result[key]=c.execute('SELECT COUNT(*) FROM '+table+' WHERE experiment_id IN ('+expr+')',bids).fetchone()[0]
    result['pending']=c.execute("SELECT COUNT(*) FROM lab_decisions WHERE status='pending' AND experiment_id IN ("+expr+')',bids).fetchone()[0]
    for key,table in (('segments','lab_segments'),('checkpoints','lab_checkpoints'),('monthly_snapshots','lab_monthly_snapshots')):
        if table=='lab_segments':
            result[key]=c.execute('SELECT COUNT(*) FROM lab_segments WHERE series_id IN ('+','.join('?' for _ in sids)+')',sids).fetchone()[0] if sids else 0
        else:result[key]=c.execute('SELECT COUNT(*) FROM '+table+' WHERE batch_id IN ('+marks+')',bids).fetchone()[0]
    return result


def _delete_preview(store,c,user_id,kind,owner_id):
    row=_owned(store,c,user_id,kind,owner_id);bids=_members(c,kind,row);blockers=[]
    if kind=='batch':
        child=c.execute('SELECT study_id FROM lab_study_batches WHERE batch_id=?',(owner_id,)).fetchone()
        if child:blockers.append(dict(code='group_action_required',message='比较组子方案需从比较组整体删除',study_id=child[0]))
    dependencies=_dependencies(c,kind,row)
    if dependencies:blockers.append(dict(code='dependent_studies',message='此批次仍被比较组引用，请先处理这些比较组',studies=dependencies))
    if row['status']=='deleting':blockers.append(dict(code='deleting',message='删除已在后台处理'))
    sids=[r[0] for r in c.execute('SELECT id FROM lab_series WHERE owner_kind=? AND owner_id=?',(kind,owner_id))]
    pids=[r[0] for r in c.execute('SELECT DISTINCT predictor_id FROM lab_batches WHERE id IN ('+','.join('?' for _ in bids)+')',bids)]
    shared=any(c.execute('SELECT 1 FROM lab_batches WHERE predictor_id=? AND id NOT IN ('+','.join('?' for _ in bids)+') LIMIT 1',[pid]+bids).fetchone() for pid in pids)
    return dict(eligible=not blockers,expected_revision=_token(c,kind,row),name=row['name'],counts=_counts(c,bids,sids),
                blockers=blockers,shared_inputs_retained=shared),row,bids,sids,pids


def delete_preview(store,user_id,kind,owner_id,*,now):
    with store._connect() as c:return _delete_preview(store,c,user_id,kind,owner_id)[0]


def request_delete(store,user_id,kind,owner_id,payload,*,now):
    _request(payload,('expected_revision','confirm_name'))
    if not isinstance(payload['confirm_name'],str):raise RuleLabError('invalid_input','确认名称必须为文本')
    with store._connect(write=True,reserve=True) as c:
        previous=_receipt(c,user_id,kind,owner_id,payload,'delete')
        if previous:return previous
        preview,row,bids,sids,pids=_delete_preview(store,c,user_id,kind,owner_id)
        _validate_revision(c,kind,row,payload)
        if payload['confirm_name']!=row['name']:raise RuleLabError('confirmation_mismatch','确认名称与实验名称不一致',409)
        if not preview['eligible']:raise RuleLabError(preview['blockers'][0]['code'],preview['blockers'][0]['message'],409)
        marks=','.join('?' for _ in bids)
        c.execute("UPDATE lab_batches SET status='deleting',revision=revision+1,error='',updated_at=? WHERE id IN ("+marks+')',[now]+bids)
        for bid in bids:_control(c,'batch',bid,user_id,now,optout=True,deleted=True)
        if kind=='study':
            c.execute("UPDATE lab_studies SET status='deleting',updated_at=? WHERE id=?",(now,owner_id));_control(c,kind,owner_id,user_id,now,deleted=True)
        for sid in sids:
            c.execute("UPDATE lab_series SET capture_enabled=0,compute_enabled=0,state='deleting',revision=revision+1,next_run_at=NULL,updated_at=? WHERE id=?",(now,sid))
            c.execute("UPDATE lab_segments SET status='cancelled' WHERE series_id=? AND status IN ('queued','running','paused')",(sid,))
            c.execute("UPDATE lab_continuation_requests SET status='cancelled',updated_at=? WHERE series_id=? AND status IN ('waiting_inputs','running')",(now,sid))
        # Invalidate any capture page selected before the last consumer was
        # disabled. It must not recreate orphaned input after deletion drains it.
        for pid in pids:
            c.execute('''UPDATE lab_capture_streams SET revision=revision+1,updated_at=? WHERE predictor_id=?
                AND NOT EXISTS(SELECT 1 FROM lab_series s WHERE s.stream_id=lab_capture_streams.id AND s.capture_enabled=1)''',(now,pid))
        data=dict(batch_ids=bids,series_ids=sids,predictor_ids=pids,released_versions=0,experiment_count=preview['counts']['experiments'],initial_bytes=store.storage_status()['used_bytes'])
        total=sum(value for key,value in preview['counts'].items() if key!='pending')
        jid=c.execute('''INSERT INTO lab_management_jobs(user_id,kind,owner_kind,owner_id,phase,payload_json,total_rows,created_at,updated_at)
            VALUES(?,'delete',?,?,'owned_records',?,?,?,?)''',(user_id,kind,owner_id,_dump(data),total,now,now)).lastrowid
        result=dict(created=True,job=_job(c.execute('SELECT * FROM lab_management_jobs WHERE id=?',(jid,)).fetchone()))
        _save_receipt(c,user_id,kind,owner_id,payload,'delete',result,now);return result


def management_job(store,user_id,job_id):
    with store._connect() as c:
        row=c.execute('SELECT * FROM lab_management_jobs WHERE id=? AND user_id=?',(job_id,user_id)).fetchone()
        if row is None:raise RuleLabError('not_found','管理任务不存在',404)
        return _job(row)


def queue_eligibility(store,user_id,batch_id,configuration_hash):
    from pc28touzhu.services.rule_lab_series import _configuration
    with store._connect() as c:
        row=c.execute('SELECT * FROM lab_batches WHERE id=? AND user_id=?',(batch_id,user_id)).fetchone()
        control=c.execute("SELECT queue_optout,deleted FROM lab_object_controls WHERE owner_kind='batch' AND owner_id=? AND user_id=?",(batch_id,user_id)).fetchone()
        removed=not row or row['status']=='deleting' or bool(control and (control['queue_optout'] or control['deleted'])) or (row is not None and _configuration(c,[row])!=configuration_hash)
        return dict(eligible=not removed,removed=bool(removed),reason='实验已删除或修改，已移出原批准队列' if removed else '')


def _delete_page(c,table,where,params,limit,*,order='rowid'):
    c.execute('DELETE FROM '+table+' WHERE rowid IN (SELECT rowid FROM '+table+' WHERE '+where+' ORDER BY '+order+' LIMIT ?)',list(params)+[limit])
    return c.execute('SELECT changes()').fetchone()[0]


def _cleanup(store,c,job,limit,now):
    data=_load(job['payload_json']);bids=data['batch_ids'];sids=data['series_ids'];marks=','.join('?' for _ in bids)
    exp='experiment_id IN (SELECT id FROM lab_experiments WHERE batch_id IN ('+marks+'))'
    steps=[(t,exp,bids,'rowid') for t in ('lab_decisions','lab_daily','lab_risk_days','lab_rounds')]
    steps.extend((t,'batch_id IN ('+marks+')',bids,'rowid') for t in ('lab_monthly_snapshots','lab_checkpoints','lab_segment_batches'))
    if sids:
        sm=','.join('?' for _ in sids)
        steps.extend((t,'series_id IN ('+sm+')',sids,'id DESC' if t=='lab_segments' else 'rowid') for t in ('lab_segments','lab_continuation_requests'))
        steps.append(('lab_series','id IN ('+sm+')',sids,'rowid'))
    if job['owner_kind']=='study':steps.append(('lab_study_batches','study_id=?',[job['owner_id']],'rowid'))
    steps.append(('lab_experiments','batch_id IN ('+marks+')',bids,'rowid'))
    if job['owner_kind']=='study':steps.append(('lab_studies','id=?',[job['owner_id']],'rowid'))
    steps.append(('lab_batches','id IN ('+marks+')',bids,'rowid'))
    processed=0
    for table,where,params,order in steps:
        amount=_delete_page(c,table,where,params,limit-processed,order=order);processed+=amount
        if table=='lab_experiments':data['released_versions']+=amount
        if processed>=limit:break
    own_left=c.execute('SELECT 1 FROM lab_batches WHERE id IN ('+marks+')',bids).fetchone()
    if not own_left and processed<limit:
        # Input archives are shared; only a source without any remaining batch or
        # series can be reclaimed. The source application's database is untouched.
        for pid in data['predictor_ids']:
            if c.execute('SELECT 1 FROM lab_batches WHERE predictor_id=? LIMIT 1',(pid,)).fetchone() or c.execute('SELECT 1 FROM lab_series WHERE predictor_id=? LIMIT 1',(pid,)).fetchone():continue
            streams=[r[0] for r in c.execute('SELECT id FROM lab_capture_streams WHERE predictor_id=?',(pid,))]
            gc=[('lab_predictions','predictor_id=?',[pid]),('lab_input_versions',"kind='prediction' AND predictor_id=?",[pid])]
            if streams:
                sm=','.join('?' for _ in streams)
                gc.extend((t,'stream_id IN ('+sm+')',streams) for t in ('lab_input_rechecks','lab_input_coverage'))
                gc.append(('lab_capture_streams','id IN ('+sm+')',streams))
            for table,where,params in gc:
                processed+=_delete_page(c,table,where,params,limit-processed)
                if processed>=limit:break
            if processed>=limit:break
        if processed<limit and not c.execute('SELECT 1 FROM lab_batches LIMIT 1').fetchone() and not c.execute('SELECT 1 FROM lab_series LIMIT 1').fetchone():
            for table in ('lab_draws','lab_input_versions'):
                processed+=_delete_page(c,table,'1=1',[],limit-processed)
                if processed>=limit:break
        # Revision records are tiny immutable fences also referenced by frozen
        # segments without SQL foreign keys. Keep them rather than recycling IDs.
    # Another bounded tick is needed if the allowance was consumed, even if that
    # last row happened to finish cleanup; this avoids unbounded existence scans.
    done=not own_left and processed<limit
    c.execute('''UPDATE lab_management_jobs SET status=?,phase=?,payload_json=?,processed_rows=processed_rows+?,
        total_rows=MAX(total_rows,processed_rows+?),updated_at=? WHERE id=?''',
        ('completed' if done else 'running','completed' if done else 'owned_records',_dump(data),processed,processed,now,job['id']))
    if done:
        _queue_reclaim(c,job['user_id'],{'request_id':'delete-reclaim-'+str(job['id'])},now)
    return processed,done


def management_maintenance(store,*,now,limit=500,max_pages=256):
    if type(limit) is not int or not 1<=limit<=500 or type(max_pages) is not int or not 1<=max_pages<=256:raise ValueError('维护批量无效')
    processed=0;delete_completed=False
    with store._connect(write=True,reserve=True) as c:
        job=c.execute("SELECT * FROM lab_management_jobs WHERE kind='delete' AND status IN ('queued','running','waiting') ORDER BY id LIMIT 1").fetchone()
        if job:processed,delete_completed=_cleanup(store,c,job,limit,now)
    from pc28touzhu.services.rule_lab_storage import process_reclaim
    reclaimed=process_reclaim(store,now=now,max_pages=max_pages)
    with store._connect() as c:pending=bool(c.execute("SELECT 1 FROM lab_management_jobs WHERE status IN ('queued','running','waiting') LIMIT 1").fetchone())
    return dict(processed_rows=processed,freed_bytes=reclaimed,maintenance_pending=pending)


def _queue_reclaim(c,user_id,payload,now):
    previous=_receipt(c,user_id,'storage',0,payload,'reclaim')
    if previous:return previous
    jid=c.execute("""INSERT INTO lab_management_jobs(user_id,kind,owner_kind,owner_id,payload_json,created_at,updated_at)
        VALUES(?,'reclaim','storage',0,'{}',?,?)""",(user_id,now,now)).lastrowid
    result=dict(created=True,job=_job(c.execute('SELECT * FROM lab_management_jobs WHERE id=?',(jid,)).fetchone()))
    _save_receipt(c,user_id,'storage',0,payload,'reclaim',result,now);return result


def request_reclaim(store,user_id,payload,*,now):
    _request(payload,())
    with store._connect(write=True,reserve=True) as c:return _queue_reclaim(c,user_id,payload,now)


def management_jobs(store,user_id,*,limit=20):
    if type(limit) is not int or not 1<=limit<=50:raise RuleLabError('invalid_input','任务读取数量无效')
    with store._connect() as c:
        return [_job(r) for r in c.execute('SELECT * FROM lab_management_jobs WHERE user_id=? ORDER BY id DESC LIMIT ?',(user_id,limit))]


def start_queued_batch(store,user_id,batch_id,configuration_hash,*,now):
    from pc28touzhu.services.rule_lab_series import _configuration
    with store._connect(write=True) as c:
        row=c.execute('SELECT * FROM lab_batches WHERE id=? AND user_id=?',(batch_id,user_id)).fetchone()
        control=c.execute("SELECT queue_optout,deleted FROM lab_object_controls WHERE owner_kind='batch' AND owner_id=? AND user_id=?",(batch_id,user_id)).fetchone()
        if not row or row['status']=='deleting' or (control and (control['queue_optout'] or control['deleted'])) or _configuration(c,[row])!=configuration_hash:
            return dict(started=False,removed=True,reason='实验已修改或删除，已移出原批准队列')
        if row['started_at'] is not None or row['status']!='paused':return dict(started=False,removed=False,reason='not_pristine')
        count=c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(batch_id,)).fetchone()[0]
        if count>store._capacity(c)['remaining_running']:return dict(started=False,removed=False,reason='capacity')
        store._require_space()
        c.execute("UPDATE lab_batches SET status='running',revision=revision+1,started_at=?,updated_at=?,error='' WHERE id=?",(now,now,batch_id))
        return dict(started=True,removed=False,batch=store._batch(c,store._owned(c,user_id,batch_id)))
