"""实验模板持久化与命令收据；运行实验只使用自己的冻结快照。"""
from __future__ import annotations

import copy
import hashlib
import json
import re

from pc28touzhu.domain.rule_lab import METRICS, build_conditions
from pc28touzhu.domain.rule_lab_templates import normalize_template, compile_conditions
from pc28touzhu.services.rule_lab_store import RuleLabError, _dump, _load


def init_schema(c):
    c.execute('''CREATE TABLE IF NOT EXISTS lab_templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,kind TEXT NOT NULL,
        name TEXT NOT NULL,description TEXT NOT NULL DEFAULT '',definition_json TEXT NOT NULL,
        revision INTEGER NOT NULL DEFAULT 1,deleted_at TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)''')
    c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS lab_templates_active_name
        ON lab_templates(user_id,kind,name) WHERE deleted_at IS NULL''')
    c.execute('''CREATE TABLE IF NOT EXISTS lab_command_receipts (
        user_id INTEGER NOT NULL,request_id TEXT NOT NULL,action TEXT NOT NULL,
        payload_hash TEXT NOT NULL,result_json TEXT NOT NULL,created_at TEXT NOT NULL,
        PRIMARY KEY(user_id,request_id))''')
    columns={row[1] for row in c.execute('PRAGMA table_info(lab_batches)')}
    for name,definition in [('template_snapshot_json',"TEXT NOT NULL DEFAULT '{}'"),
        ('risk_template_snapshot_json',"TEXT NOT NULL DEFAULT '{}'"),('creation_request_id','TEXT')]:
        if name not in columns:c.execute('ALTER TABLE lab_batches ADD COLUMN '+name+' '+definition)


def fingerprint(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()).hexdigest()


def validate_request(payload, fields, *, optional=()):
    if not isinstance(payload,dict) or set(payload)-set(fields)-set(optional) or set(fields)-set(payload):
        raise RuleLabError('invalid_input','请求字段不完整或包含未知字段')
    if 'request_id' in fields and (not isinstance(payload['request_id'],str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,80}',payload['request_id'])):
        raise RuleLabError('invalid_input','请求标识必须为8至80位字母、数字、短横线或下划线')


def receipt(c,user_id,action,payload):
    row=c.execute('SELECT * FROM lab_command_receipts WHERE user_id=? AND request_id=?',(user_id,payload['request_id'])).fetchone()
    if row is None:return None
    if row['action']!=action or row['payload_hash']!=fingerprint(payload):
        raise RuleLabError('request_conflict','同一请求标识已用于不同操作或内容',409)
    result=_load(row['result_json']);result['created']=False
    return result


def save_receipt(c,user_id,action,payload,result,now):
    c.execute('INSERT INTO lab_command_receipts(user_id,request_id,action,payload_hash,result_json,created_at) VALUES(?,?,?,?,?,?)',
        (user_id,payload['request_id'],action,fingerprint(payload),_dump(result),now))


def _owned(c,user_id,tid,*,deleted=False):
    if type(tid) is not int or tid<1:raise RuleLabError('not_found','模板不存在',404)
    row=c.execute('SELECT * FROM lab_templates WHERE id=? AND user_id=?'+('' if deleted else ' AND deleted_at IS NULL'),(tid,user_id)).fetchone()
    if row is None:raise RuleLabError('not_found','模板不存在',404)
    return row


def present(row):
    definition=_load(row['definition_json']);condition=row['kind']=='condition'
    return dict(id=row['id'],ref=dict(kind='custom',id=row['id']),kind=row['kind'],name=row['name'],description=row['description'],
        metrics=[definition['metric']] if condition else definition['metrics'],builtin=False,revision=row['revision'],
        definition=definition,condition_count=len(definition['conditions']) if condition else None,
        created_at=row['created_at'],updated_at=row['updated_at'])


def builtins():
    items=[]
    for key,name,revision in [('basic_v2','综合条件 v2',2),('combo_long_miss_v1','组合长连错',1)]:
        for metric in METRICS:
            if key=='combo_long_miss_v1' and metric!='combo':continue
            conditions=build_conditions(metric,template_pack=key)
            items.append(dict(id=None,ref=dict(kind='builtin',key=key,metric=metric),kind='condition',name=name,
                description='',metrics=[metric],builtin=True,revision=revision,definition=dict(metric=metric,conditions=conditions),
                condition_count=len(conditions),created_at=None,updated_at=None))
    return items


def list_templates(store,user_id,*,kind=None,metric=None):
    if kind not in (None,'condition','risk') or metric not in (None,*METRICS):
        raise RuleLabError('invalid_input','模板类型或玩法无效')
    with store._connect() as c:
        rows=c.execute('SELECT * FROM lab_templates WHERE user_id=? AND deleted_at IS NULL ORDER BY id DESC LIMIT 200',(user_id,)).fetchall()
        items=builtins()+[present(row) for row in rows]
    return dict(templates=[item for item in items if (kind is None or item['kind']==kind) and (metric is None or metric in item['metrics'])],
        limits=dict(max_conditions=50,max_templates_per_kind=100,windows=[20,50,100]))


def get_template(store,user_id,tid):
    with store._connect() as c:return dict(template=present(_owned(c,user_id,tid)))


def normalize_payload(payload,*,kind=None,preview=False):
    try:return normalize_template(kind or payload['kind'],payload.get('name',''),payload.get('description',''),payload['definition'],preview=preview)
    except (ValueError,TypeError,KeyError) as exc:raise RuleLabError('invalid_input',str(exc)) from exc


def preview_template(payload):
    validate_request(payload,('kind','name','description','definition'))
    value=normalize_payload(payload,preview=True)
    return dict(valid=True,definition=value['definition'],conditions=value['definition'].get('conditions',[]),
        condition_count=value['condition_count'],duplicates_removed=value['duplicates_removed'],warnings=value['warnings'])


def save_template(store,user_id,payload,*,now,tid=None,request_payload=None):
    fields=('request_id','kind','name','description','definition') if tid is None else ('request_id','expected_revision','name','description','definition')
    validate_request(payload,fields)
    action='template.create' if tid is None else 'template.edit:'+str(tid)
    raw=request_payload or payload
    with store._connect(write=True) as c:
        if tid is not None:_owned(c,user_id,tid,deleted=True)
        previous=receipt(c,user_id,action,raw)
        if previous:return previous
        old=_owned(c,user_id,tid) if tid is not None else None
        if old and (type(payload['expected_revision']) is not int or payload['expected_revision']!=old['revision']):
            raise RuleLabError('stale_revision','模板已更新，请重新读取后保存',409)
        value=normalize_payload(payload,kind=old['kind'] if old else None)
        if c.execute('SELECT 1 FROM lab_templates WHERE user_id=? AND kind=? AND name=? AND deleted_at IS NULL AND id!=?',
            (user_id,value['kind'],value['name'],tid or 0)).fetchone():raise RuleLabError('name_conflict','已有同名模板，请修改名称',409)
        if old is None:
            if c.execute('SELECT COUNT(*) FROM lab_templates WHERE user_id=? AND kind=? AND deleted_at IS NULL',(user_id,value['kind'])).fetchone()[0]>=100:
                raise RuleLabError('capacity_exceeded','同一类型最多保存100个自定义模板',409)
            tid=c.execute('INSERT INTO lab_templates(user_id,kind,name,description,definition_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?)',
                (user_id,value['kind'],value['name'],value['description'],_dump(value['definition']),now,now)).lastrowid
        else:
            c.execute('UPDATE lab_templates SET name=?,description=?,definition_json=?,revision=revision+1,updated_at=? WHERE id=?',
                (value['name'],value['description'],_dump(value['definition']),now,tid))
        result=dict(created=True,template=present(_owned(c,user_id,tid)))
        save_receipt(c,user_id,action,raw,result,now)
        return result


def delete_template(store,user_id,tid,payload,*,now):
    validate_request(payload,('request_id','expected_revision'))
    with store._connect(write=True) as c:
        row=_owned(c,user_id,tid,deleted=True)
        action='template.delete:'+str(tid);previous=receipt(c,user_id,action,payload)
        if previous:return previous
        if row['deleted_at']:raise RuleLabError('not_found','模板不存在',404)
        if type(payload['expected_revision']) is not int or payload['expected_revision']!=row['revision']:
            raise RuleLabError('stale_revision','模板已更新，请重新读取',409)
        c.execute('UPDATE lab_templates SET deleted_at=?,updated_at=?,revision=revision+1 WHERE id=?',(now,now,tid))
        result=dict(created=True,deleted=True,template_id=tid)
        save_receipt(c,user_id,action,payload,result,now);return result


def resolve_conditions(c,user_id,selection,metric):
    validate_request(selection,('template_ref','expected_revision'),optional=('condition_keys',))
    ref=selection['template_ref']
    if not isinstance(ref,dict):raise RuleLabError('invalid_input','条件模板引用无效')
    if ref.get('kind')=='builtin':
        validate_request(ref,('kind','key','metric'))
        item=next((item for item in builtins() if item['ref']==ref),None)
        if item is None:raise RuleLabError('invalid_input','内置模板不存在')
    elif ref.get('kind')=='custom':
        validate_request(ref,('kind','id'));item=present(_owned(c,user_id,ref['id']))
    elif ref.get('kind')=='batch_snapshot':
        validate_request(ref,('kind','batch_id'))
        if type(ref['batch_id']) is not int or ref['batch_id']<1:raise RuleLabError('invalid_input','冻结批次编号无效')
        from pc28touzhu.services.rule_lab_series import _configuration
        row=c.execute('SELECT * FROM lab_batches WHERE id=? AND user_id=?',(ref['batch_id'],user_id)).fetchone()
        if row is None:raise RuleLabError('not_found','实验批次不存在',404)
        if row['status']=='deleting':raise RuleLabError('deleting','实验正在删除',409)
        conditions={r['condition_key']:_load(r['condition_json']) for r in c.execute('SELECT condition_key,condition_json FROM lab_experiments WHERE batch_id=? ORDER BY id',(row['id'],))}
        item=dict(kind='condition',metrics=[row['metric']],ref=ref,name=_load(row['template_snapshot_json']).get('name') or row['template_pack'],
            revision=_configuration(c,[row]),definition=dict(metric=row['metric'],conditions=list(conditions.values())))
    else:raise RuleLabError('invalid_input','条件模板引用无效')
    if item['kind']!='condition' or metric not in item['metrics']:raise RuleLabError('invalid_input','条件模板不适用于当前玩法')
    if type(selection['expected_revision']) is not type(item['revision']) or selection['expected_revision']!=item['revision']:
        raise RuleLabError('stale_revision','条件模板已变化，请重新预览',409)
    conditions=item['definition']['conditions'];keys=selection.get('condition_keys')
    if keys is not None:
        if not isinstance(keys,list) or not keys or any(not isinstance(k,str) for k in keys) or set(keys)-{v['condition_key'] for v in conditions}:
            raise RuleLabError('invalid_input','请选择模板内的有效条件')
        conditions=[v for v in conditions if v['condition_key'] in keys]
    try:conditions=compile_conditions(metric,conditions,require_baseline=ref['kind']!='batch_snapshot')['conditions']
    except ValueError as exc:raise RuleLabError('invalid_input',str(exc)) from exc
    snapshot=dict(ref=copy.deepcopy(ref),name=item['name'],revision=item['revision'],condition_keys=[v['condition_key'] for v in conditions])
    return conditions,snapshot


def resolve_risk(c,user_id,selection,metric):
    if not isinstance(selection,dict):raise RuleLabError('invalid_input','风控选择无效')
    if 'policy' in selection:
        validate_request(selection,('policy',));return copy.deepcopy(selection['policy']),{}
    validate_request(selection,('template_ref','expected_revision'))
    ref=selection['template_ref'];validate_request(ref,('kind','id'))
    if ref['kind']!='custom':raise RuleLabError('invalid_input','风控模板引用无效')
    item=present(_owned(c,user_id,ref['id']))
    if item['kind']!='risk' or metric not in item['metrics']:raise RuleLabError('invalid_input','风控模板不适用于当前玩法')
    if type(selection['expected_revision']) is not int or selection['expected_revision']!=item['revision']:
        raise RuleLabError('stale_revision','风控模板已变化，请重新预览',409)
    return copy.deepcopy(item['definition']['policy']),dict(ref=ref,name=item['name'],revision=item['revision'])


def write_snapshots(c,bid,config):
    c.execute('UPDATE lab_batches SET template_snapshot_json=?,risk_template_snapshot_json=?,creation_request_id=? WHERE id=?',
        (_dump(config.get('template_snapshot',{})),_dump(config.get('risk_template_snapshot',{})),config.get('creation_request_id'),bid))


def check_selections(c,user_id,config):
    if config.get('_condition_selection'):
        resolve_conditions(c,user_id,config['_condition_selection'],config['metric'])
    if config.get('_risk_selection'):
        resolve_risk(c,user_id,config['_risk_selection'],config['metric'])
