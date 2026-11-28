"""Owned, immutable candidate snapshots independent of source experiment lifetime."""
from __future__ import annotations

import copy
from pc28touzhu.domain.rule_lab import normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabError,_dump,_load,MAX_BATCHES_PER_USER
from pc28touzhu.services.rule_lab_templates import validate_request,receipt,save_receipt,fingerprint
from pc28touzhu.services.rule_lab_management import _insert_batch

MAX_FAVORITES=500


def init_schema(c):
    c.execute('''CREATE TABLE IF NOT EXISTS lab_favorites (
        id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,source_batch_id INTEGER NOT NULL,
        source_experiment_id INTEGER NOT NULL,name TEXT NOT NULL,notes TEXT NOT NULL DEFAULT '',
        snapshot_json TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,deleted_at TEXT)''')
    c.execute('''CREATE UNIQUE INDEX IF NOT EXISTS lab_favorites_active_source
        ON lab_favorites(user_id,source_experiment_id) WHERE deleted_at IS NULL''')


def _owned(c,uid,fid,*,deleted=False):
    if type(fid) is not int or fid<=0:raise RuleLabError('not_found','收藏不存在',404)
    row=c.execute('SELECT * FROM lab_favorites WHERE id=? AND user_id=?'+('' if deleted else ' AND deleted_at IS NULL'),(fid,uid)).fetchone()
    if row is None:raise RuleLabError('not_found','收藏不存在',404)
    return row


def _text(payload):
    if not isinstance(payload['name'],str) or not 1<=len(payload['name'].strip())<=80:
        raise RuleLabError('invalid_input','候选名称须为1至80字符')
    if not isinstance(payload['notes'],str) or len(payload['notes'])>500:
        raise RuleLabError('invalid_input','候选笔记最多500字符')
    return payload['name'].strip(),payload['notes'].strip()


def definition(batch,exp):
    return dict(predictor_id=batch['predictor_id'],metric=batch['metric'],stake_cents=batch['stake_cents'],
        condition=_load(exp['condition_json']),settlement_rule_id=exp['settlement_rule_id'],
        settlement_snapshot=_load(exp['settlement_snapshot_json']),risk_policy=_load(batch['risk_policy_json']))


def score(exp):
    stats=_load(exp['stats_json']);stake=stats['stake_cents']
    return dict(net_cents=stats['net_cents'],stake_cents=stake,settled_count=stats['settled_count'],
        roi_percent=round(stats['net_cents']/stake*100,6) if stake else None)


def present(c,row):
    snapshot=_load(row['snapshot_json']);batch=c.execute('SELECT * FROM lab_batches WHERE id=? AND user_id=?',(row['source_batch_id'],row['user_id'])).fetchone()
    exp=c.execute('SELECT * FROM lab_experiments WHERE id=? AND batch_id=?',(row['source_experiment_id'],row['source_batch_id'])).fetchone() if batch else None
    available=bool(batch and exp and batch['status']!='deleting' and fingerprint(definition(batch,exp))==snapshot['definition_hash'])
    source=dict(available=available,batch_id=row['source_batch_id'],experiment_id=row['source_experiment_id'],
        state=batch['status'] if available else 'deleted' if batch is None or batch['status']=='deleting' else 'definition_changed',
        updated_at=batch['updated_at'] if available else None)
    return dict(id=row['id'],name=row['name'],notes=row['notes'],revision=row['revision'],snapshot=snapshot,
        created_at=row['created_at'],updated_at=row['updated_at'],source=source,current_result=score(exp) if available else None,
        saved_result=snapshot['saved_result'],result_label='当前成绩' if available else '收藏时快照')


def list_favorites(store,uid,*,limit=50,before_id=None):
    if type(limit) is not int or not 1<=limit<=50 or before_id is not None and (type(before_id) is not int or before_id<=0):
        raise RuleLabError('invalid_input','收藏分页参数无效')
    with store._connect() as c:
        c.execute('BEGIN')
        rows=c.execute('SELECT * FROM lab_favorites WHERE user_id=? AND deleted_at IS NULL'+(' AND id<?' if before_id else '')+' ORDER BY id DESC LIMIT ?',
            [uid]+([before_id] if before_id else [])+[limit+1]).fetchall()
        total=c.execute('SELECT COUNT(*) FROM lab_favorites WHERE user_id=? AND deleted_at IS NULL',(uid,)).fetchone()[0]
        return dict(items=[present(c,row) for row in rows[:limit]],next_cursor=rows[limit-1]['id'] if len(rows)>limit else None,total=total,limit=limit)


def save_favorite(store,uid,payload,*,now,fid=None):
    fields=('request_id','batch_id','experiment_id','name','notes') if fid is None else ('request_id','expected_revision','name','notes')
    validate_request(payload,fields);name,notes=_text(payload);action='favorite.create' if fid is None else 'favorite.edit:'+str(fid)
    with store._connect(write=True) as c:
        if fid is not None:_owned(c,uid,fid,deleted=True)
        previous=receipt(c,uid,action,payload)
        if previous:return previous
        if fid is None:
            if any(type(payload[k]) is not int or payload[k]<=0 for k in ('batch_id','experiment_id')):raise RuleLabError('invalid_input','收藏来源编号无效')
            batch=store._owned(c,uid,payload['batch_id'])
            if batch['status']=='deleting':raise RuleLabError('deleting','来源正在删除',409)
            exp=c.execute('SELECT * FROM lab_experiments WHERE id=? AND batch_id=?',(payload['experiment_id'],batch['id'])).fetchone()
            if exp is None:raise RuleLabError('not_found','实验版本不存在',404)
            existing=c.execute('SELECT * FROM lab_favorites WHERE user_id=? AND source_experiment_id=? AND deleted_at IS NULL',(uid,exp['id'])).fetchone()
            if existing:
                result=dict(created=True,already_saved=True,favorite=present(c,existing))
                save_receipt(c,uid,action,payload,result,now);return result
            if c.execute('SELECT COUNT(*) FROM lab_favorites WHERE user_id=? AND deleted_at IS NULL',(uid,)).fetchone()[0]>=MAX_FAVORITES:
                raise RuleLabError('capacity_exceeded','最多保存500个候选收藏',409)
            store._require_space();frozen=definition(batch,exp)
            snapshot=dict(frozen,definition_hash=fingerprint(frozen),batch_id=batch['id'],experiment_id=exp['id'],
                batch_name=batch['name'],predictor_name=batch['predictor_name'],condition_name=exp['name'],
                condition_key=exp['condition_key'],template_pack=batch['template_pack'],
                template_snapshot=_load(batch['template_snapshot_json']),risk_template_snapshot=_load(batch['risk_template_snapshot_json']),
                saved_result=score(exp),saved_at=now,source_updated_at=batch['updated_at'])
            fid=c.execute('INSERT INTO lab_favorites(user_id,source_batch_id,source_experiment_id,name,notes,snapshot_json,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)',
                (uid,batch['id'],exp['id'],name,notes,_dump(snapshot),now,now)).lastrowid
        else:
            row=_owned(c,uid,fid)
            if type(payload['expected_revision']) is not int or payload['expected_revision']!=row['revision']:raise RuleLabError('stale_revision','候选已修改，请刷新',409)
            c.execute('UPDATE lab_favorites SET name=?,notes=?,revision=revision+1,updated_at=? WHERE id=?',(name,notes,now,fid))
        result=dict(created=True,favorite=present(c,_owned(c,uid,fid)))
        save_receipt(c,uid,action,payload,result,now);return result


def delete_favorite(store,uid,fid,payload,*,now):
    validate_request(payload,('request_id','expected_revision'))
    with store._connect(write=True) as c:
        row=_owned(c,uid,fid,deleted=True);action='favorite.delete:'+str(fid);previous=receipt(c,uid,action,payload)
        if previous:return previous
        if row['deleted_at']:raise RuleLabError('not_found','收藏不存在',404)
        if type(payload['expected_revision']) is not int or payload['expected_revision']!=row['revision']:raise RuleLabError('stale_revision','候选已修改，请刷新',409)
        c.execute('UPDATE lab_favorites SET deleted_at=?,updated_at=?,revision=revision+1 WHERE id=?',(now,now,fid))
        result=dict(created=True,deleted=True,favorite_id=fid);save_receipt(c,uid,action,payload,result,now);return result


def create_from_favorite(service,uid,fid,payload,*,now):
    validate_request(payload,('request_id','expected_revision','name','start_date','end_date'))
    action='favorite.experiment:'+str(fid)
    with service.store._connect() as c:
        row=_owned(c,uid,fid,deleted=True);previous=receipt(c,uid,action,payload)
        if previous:return previous
        if row['deleted_at']:raise RuleLabError('not_found','收藏不存在',404)
        frozen=_load(row['snapshot_json'])
    predictor=service._source_predictor(frozen['predictor_id'])
    try:config=normalize_batch_payload(dict(name=payload['name'],mode='backtest',predictor_id=frozen['predictor_id'],metric=frozen['metric'],
        stake_amount=frozen['stake_cents']/100,start_date=payload['start_date'],end_date=payload['end_date'],risk_policy=frozen['risk_policy']),now=now)
    except ValueError as exc:raise RuleLabError('invalid_input',str(exc)) from exc
    config.update(template_pack='custom_v1',template_snapshot=dict(frozen['template_snapshot'],favorite_id=fid,name=row['name'],condition_keys=[frozen['condition_key']]),
        risk_template_snapshot=frozen['risk_template_snapshot'],creation_request_id=payload['request_id'])
    definitions=[dict(condition_key=frozen['condition_key'],name=frozen['condition_name'],condition=frozen['condition'],settlement_rule_id=frozen['settlement_rule_id'],settlement_snapshot=frozen['settlement_snapshot'])]
    with service.store._connect(write=True) as c:
        previous=receipt(c,uid,action,payload)
        if previous:return previous
        current=_owned(c,uid,fid)
        if type(payload['expected_revision']) is not int or payload['expected_revision']!=current['revision']:raise RuleLabError('stale_revision','候选已修改，请刷新',409)
        if service.store._capacity(c)['remaining_saved']<1 or c.execute('SELECT COUNT(*) FROM lab_batches WHERE user_id=?',(uid,)).fetchone()[0]>=MAX_BATCHES_PER_USER:
            raise RuleLabError('capacity_exceeded','实验保存容量不足',409)
        service.store._require_space();bid=_insert_batch(c,uid,config,predictor['name'],definitions,now)
        result=dict(created=True,batch=service.store._batch(c,service.store._owned(c,uid,bid)),generated_count=1,favorite_id=fid)
        save_receipt(c,uid,action,payload,result,now);return result
