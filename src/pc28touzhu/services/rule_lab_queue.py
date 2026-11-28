"""One persistent, bounded scheduler for finite batch and study experiments."""
from __future__ import annotations

import time

from pc28touzhu.services.rule_lab_store import RuleLabError, _dump, _load
from pc28touzhu.services.rule_lab_templates import validate_request, receipt, save_receipt, fingerprint
from pc28touzhu.services.rule_lab_management import _owned, _members, _token, _control
from pc28touzhu.services.rule_lab_series import _configuration


STATES=('queued','running','paused','removed','completed','error','resource_paused')


def init_schema(c):
    c.execute('''CREATE TABLE IF NOT EXISTS lab_queue_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,owner_kind TEXT NOT NULL,owner_id INTEGER NOT NULL,
        state TEXT NOT NULL,queue_order INTEGER NOT NULL,configuration_hash TEXT NOT NULL,revision INTEGER NOT NULL DEFAULT 1,
        origin TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
        UNIQUE(owner_kind,owner_id))''')
    c.execute('CREATE INDEX IF NOT EXISTS lab_queue_schedule ON lab_queue_items(state,queue_order,id)')


def token(row):return 'queue:%s:%s'%(row['id'],row['revision'])


def _order(c):return c.execute('SELECT COALESCE(MAX(queue_order),0)+1 FROM lab_queue_items').fetchone()[0]


def configuration(c,kind,owner):
    rows=[owner] if kind=='batch' else c.execute('SELECT b.* FROM lab_batches b JOIN lab_study_batches s ON s.batch_id=b.id WHERE s.study_id=? ORDER BY s.position',(owner['id'],)).fetchall()
    return fingerprint(dict(frozen=_configuration(c,rows),range=[(r['mode'],r['start_date'],r['end_date']) for r in rows]))


def _existing(c,kind,oid):
    return c.execute('SELECT * FROM lab_queue_items WHERE owner_kind=? AND owner_id=?',(kind,oid)).fetchone()


def _eligible(store,c,user_id,kind,oid):
    owner=_owned(store,c,user_id,kind,oid)
    if owner['status']=='deleting':raise RuleLabError('deleting','实验正在删除',409)
    if c.execute('SELECT 1 FROM lab_series WHERE owner_kind=? AND owner_id=?',(kind,oid)).fetchone():
        raise RuleLabError('series_managed','该对象由连续续算管理，请使用续算入口',409)
    if kind=='batch':
        if owner['mode']!='backtest':raise RuleLabError('invalid_input','有限队列只接受历史回测，前向模拟请单独启动')
        if c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(oid,)).fetchone():raise RuleLabError('study_managed','比较组子方案不能单独加入队列',409)
        if _load(owner['cursor_json']).get('input_expired'):raise RuleLabError('input_expired','所需输入已过期，请新建实验',409)
    if owner['status']=='completed':raise RuleLabError('invalid_transition','已完成实验不能重新运行，请使用续算或创建新版本',409)
    return owner


def _owner_state(c,kind,owner,state,now):
    table='lab_batches' if kind=='batch' else 'lab_studies'
    stored='paused' if state in ('queued','removed') and kind=='batch' else 'paused' if state=='removed' else state
    if kind=='batch':
        c.execute('UPDATE lab_batches SET status=?,revision=revision+1,error=?,updated_at=? WHERE id=?',
            (stored,'' if state in ('queued','paused','removed') else owner['error'],now,owner['id']))
    else:
        c.execute('UPDATE lab_studies SET status=?,updated_at=?,error=? WHERE id=?',(stored,now,'' if state in ('queued','paused','removed') else owner['error'],owner['id']))
        if state in ('paused','removed'):
            c.execute("UPDATE lab_batches SET status='paused',revision=revision+1,updated_at=? WHERE status='running' AND id IN (SELECT batch_id FROM lab_study_batches WHERE study_id=?)",(now,owner['id']))


def _update(c,row,state,now,*,tail=False):
    if row['state']==state and not tail:return
    c.execute('UPDATE lab_queue_items SET state=?,queue_order=?,revision=revision+1,updated_at=? WHERE id=?',
        (state,_order(c) if tail else row['queue_order'],now,row['id']))


def register(store,c,user_id,kind,oid,now,*,origin='user',allow_retry=False):
    owner=_eligible(store,c,user_id,kind,oid);existing=_existing(c,kind,oid)
    if existing and existing['state'] in ('running','queued'):
        if existing['configuration_hash']==configuration(c,kind,owner):return existing
    if owner['status'] in ('error','resource_paused') and not allow_retry:
        raise RuleLabError('retry_required','修复问题后请使用重试操作',409)
    if owner['status']=='running':state='running'
    else:state='queued'
    if kind=='study' and allow_retry:
        c.execute("UPDATE lab_batches SET status='paused',error='',revision=revision+1,updated_at=? WHERE status IN ('error','resource_paused') AND id IN (SELECT batch_id FROM lab_study_batches WHERE study_id=?)",(now,oid))
    if existing:
        c.execute('UPDATE lab_queue_items SET state=?,queue_order=?,configuration_hash=?,revision=revision+1,origin=?,updated_at=? WHERE id=?',
            (state,_order(c),configuration(c,kind,owner),origin,now,existing['id']))
    else:
        c.execute('INSERT INTO lab_queue_items(user_id,owner_kind,owner_id,state,queue_order,configuration_hash,origin,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)',
            (user_id,kind,oid,state,_order(c),configuration(c,kind,owner),origin,now,now))
    if state=='queued':_owner_state(c,kind,owner,state,now)
    # An explicit new instruction supersedes the old manifest opt-out only for
    # this database queue. The manifest itself remains permanently opted out.
    return _existing(c,kind,oid)


def remove_owner(c,kind,oid,now):
    row=_existing(c,kind,oid)
    if row:_update(c,row,'removed',now)


def legacy_state(store,c,user_id,kind,oid,state,now):
    owner=_eligible(store,c,user_id,kind,oid)
    if state=='running':
        row=register(store,c,user_id,kind,oid,now,origin='legacy_api',allow_retry=True)
        # Only batch starts used to be synchronous; preserve that useful behavior
        # while admission still observes the one global fair queue.
        if kind=='batch':advance(store,c,now=now)
    else:
        row=_existing(c,kind,oid)
        if row:_update(c,row,'paused',now)
        _owner_state(c,kind,owner,'paused',now)
        _control(c,kind,oid,user_id,now,optout=True)
        if row:_update(c,_existing(c,kind,oid),'paused',now)


def _children(c,kind,owner):
    if kind=='batch':return [owner]
    return c.execute('SELECT b.* FROM lab_batches b JOIN lab_study_batches s ON s.batch_id=b.id WHERE s.study_id=? ORDER BY s.position',(owner['id'],)).fetchall()


def present(store,c,row):
    kind=row['owner_kind'];oid=row['owner_id']
    owner=_owned(store,c,row['user_id'],kind,oid);children=_children(c,kind,owner)
    counts={b['id']:c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(b['id'],)).fetchone()[0] for b in children}
    unfinished=next((b for b in children if b['status']!='completed'),None)
    needed=counts.get(unfinished['id'],0) if unfinished else 0
    remaining=store._capacity(c)['remaining_running'];waiting=''
    if row['state']=='queued':
        if needed>remaining:waiting='需要%s个运行名额，当前剩余%s'%(needed,remaining)
        elif kind=='study' and not owner['cutoff_issue']:waiting='等待获取共同历史截止'
        else:waiting='等待队列调度'
    actions={'queued':['pause','remove'],'running':['pause','remove'],'paused':['resume','remove'],
        'removed':['resume'],'error':['retry','remove'],'resource_paused':['retry','remove'],'completed':['view']}[row['state']]
    if owner['status']=='deleting':actions=[]
    return dict(id=row['id'],owner_kind=kind,owner_id=oid,name=owner['name'],state=row['state'],expected_revision=token(row),
        queue_order=row['queue_order'],experiment_count=sum(counts.values()),needed_experiments=needed,
        processed_issues=sum(b['processed_issues'] for b in children),total_issues=sum(b['total_issues'] for b in children) if children and all(b['total_issues'] is not None for b in children) else None,
        waiting_reason=waiting,actions=actions,error=owner['error'],start_date=children[0]['start_date'] if children else None,
        end_date=children[0]['end_date'] if children else None,predictor_name=children[0]['predictor_name'] if children else '',
        predictor_id=children[0]['predictor_id'] if children else None,metric=children[0]['metric'] if children else None,
        current_child=unfinished['name'] if kind=='study' and unfinished else None)


def batch_fields(c,row,continuation):
    item=_existing(c,'batch',row['id'])
    state=item['state'] if item else None
    # A completed finite queue entry must never conceal later series progress.
    if continuation.get('series_id') or c.execute("SELECT 1 FROM lab_series WHERE owner_kind='batch' AND owner_id=?",(row['id'],)).fetchone():
        display=row['status']
    elif state=='queued':display='queued'
    elif row['status']=='paused' and row['started_at'] is None and state in (None,'removed'):display='draft'
    else:display=row['status']
    return dict(queue_state=state,queue_item_id=item['id'] if item else None,display_status=display)


def list_queue(store,user_id,*,state='active',now):
    if state not in ('active','completed','all'):raise RuleLabError('invalid_input','队列筛选只支持active、completed或all')
    from pc28touzhu.services.rule_lab_bulk import capacity
    with store._connect() as c:
        c.execute('BEGIN')
        where=" AND state='completed'" if state=='completed' else " AND state!='completed'" if state=='active' else ''
        owned=""" AND ((owner_kind='batch' AND EXISTS(SELECT 1 FROM lab_batches b WHERE b.id=lab_queue_items.owner_id AND b.user_id=lab_queue_items.user_id))
            OR (owner_kind='study' AND EXISTS(SELECT 1 FROM lab_studies s WHERE s.id=lab_queue_items.owner_id AND s.user_id=lab_queue_items.user_id)))"""
        rows=c.execute('SELECT * FROM lab_queue_items WHERE user_id=?'+where+owned+' ORDER BY queue_order,id LIMIT 1000',(user_id,)).fetchall()
        items=[]
        for row in rows:
            try:items.append(present(store,c,row))
            except RuleLabError as exc:
                if exc.code!='not_found':raise
        drafts=[]
        if state!='completed':
            for row in c.execute("""SELECT b.* FROM lab_batches b WHERE b.user_id=? AND b.mode='backtest' AND b.status='paused' AND b.started_at IS NULL
                AND NOT EXISTS(SELECT 1 FROM lab_study_batches s WHERE s.batch_id=b.id)
                AND NOT EXISTS(SELECT 1 FROM lab_series s WHERE s.owner_kind='batch' AND s.owner_id=b.id)
                AND NOT EXISTS(SELECT 1 FROM lab_queue_items q WHERE q.owner_kind='batch' AND q.owner_id=b.id)
                ORDER BY b.id LIMIT 80""",(user_id,)):
                drafts.append(dict(owner_kind='batch',owner_id=row['id'],name=row['name'],expected_revision=_token(c,'batch',row),
                    experiment_count=c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(row['id'],)).fetchone()[0],
                    predictor_name=row['predictor_name'],predictor_id=row['predictor_id'],metric=row['metric']))
        return dict(items=items,drafts=drafts,capacity=capacity(store,c,user_id),updated_at=now)


def enqueue(store,user_id,payload,*,now):
    validate_request(payload,('request_id','owner_kind','owner_id','expected_revision'))
    with store._connect(write=True) as c:
        owner=_owned(store,c,user_id,payload['owner_kind'],payload['owner_id'])
        previous=receipt(c,user_id,'queue.enqueue',payload)
        if previous:return previous
        row=_existing(c,payload['owner_kind'],payload['owner_id'])
        expected=token(row) if row else _token(c,payload['owner_kind'],owner)
        if payload['expected_revision']!=expected:raise RuleLabError('stale_revision','实验或队列状态已变化，请刷新',409)
        row=register(store,c,user_id,payload['owner_kind'],payload['owner_id'],now)
        result=dict(created=True,item=present(store,c,row));save_receipt(c,user_id,'queue.enqueue',payload,result,now);return result


def set_state(store,user_id,qid,payload,*,now):
    validate_request(payload,('request_id','expected_revision','action'))
    if payload['action'] not in ('pause','resume','remove','retry'):raise RuleLabError('invalid_input','队列操作无效')
    with store._connect(write=True) as c:
        row=c.execute('SELECT * FROM lab_queue_items WHERE id=? AND user_id=?',(qid,user_id)).fetchone()
        if row is None:raise RuleLabError('not_found','队列对象不存在',404)
        previous=receipt(c,user_id,'queue.state:'+str(qid),payload)
        if previous:return previous
        owner=_owned(store,c,user_id,row['owner_kind'],row['owner_id'])
        if payload['expected_revision']!=token(row):raise RuleLabError('stale_revision','队列状态已变化，请刷新',409)
        action=payload['action']
        if action not in present(store,c,row)['actions']:raise RuleLabError('invalid_transition','当前状态不支持此操作',409)
        if action in ('resume','retry'):
            register(store,c,user_id,row['owner_kind'],row['owner_id'],now,allow_retry=action=='retry')
        else:
            state='paused' if action=='pause' else 'removed'
            _owner_state(c,row['owner_kind'],owner,state,now);_update(c,row,state,now)
            _control(c,row['owner_kind'],row['owner_id'],user_id,now,optout=True)
            _update(c,_existing(c,row['owner_kind'],row['owner_id']),state,now)
        result=dict(created=True,item=present(store,c,_existing(c,row['owner_kind'],row['owner_id'])))
        save_receipt(c,user_id,'queue.state:'+str(qid),payload,result,now);return result


def advance(store,c,*,now,cutoff=None,max_items=20,budget_ms=100):
    """One SQLite writer owns reconciliation, fair admission and capacity checks."""
    started=time.monotonic();deadline=started+budget_ms/1000;reconcile_deadline=started+budget_ms/2000;handled=0
    # Existing study rows from schema6 join this scheduler lazily, without
    # touching any frozen experiment data or restarting paused studies.
    for owner in c.execute("SELECT * FROM lab_studies s WHERE status IN ('running','queued') AND NOT EXISTS(SELECT 1 FROM lab_queue_items q WHERE q.owner_kind='study' AND q.owner_id=s.id) AND NOT EXISTS(SELECT 1 FROM lab_series r WHERE r.owner_kind='study' AND r.owner_id=s.id) ORDER BY id LIMIT ?",(min(5,max_items),)).fetchall():
        if time.monotonic()>=reconcile_deadline:break
        handled+=1
        register(store,c,owner['user_id'],'study',owner['id'],now,origin='legacy_study')
    scan=c.execute("SELECT value_json FROM lab_runtime WHERE key='queue_scan_after'").fetchone()
    cursor=_load(scan[0]) if scan else 0
    for row in c.execute("SELECT * FROM lab_queue_items WHERE state='running' ORDER BY CASE WHEN id>? THEN 0 ELSE 1 END,id LIMIT ?",(cursor,min(10,max(0,max_items-handled)))).fetchall():
        if time.monotonic()>=reconcile_deadline:break
        handled+=1;kind=row['owner_kind'];oid=row['owner_id']
        c.execute("INSERT INTO lab_runtime(key,value_json) VALUES('queue_scan_after',?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",(_dump(row['id']),))
        try:owner=_owned(store,c,row['user_id'],kind,oid)
        except RuleLabError:_update(c,row,'removed',now);continue
        if c.execute('SELECT 1 FROM lab_series WHERE owner_kind=? AND owner_id=?',(kind,oid)).fetchone():
            _update(c,row,'completed',now);continue
        children=_children(c,kind,owner)
        if owner['status']=='deleting':_update(c,row,'removed',now);continue
        if configuration(c,kind,owner)!=row['configuration_hash']:
            _owner_state(c,kind,owner,'paused',now);_update(c,row,'removed',now);continue
        failed=next((b for b in children if b['status'] in ('error','resource_paused')),None)
        if failed:
            if kind=='study':c.execute('UPDATE lab_studies SET status=?,error=?,updated_at=? WHERE id=?',(failed['status'],failed['error'],now,oid))
            _update(c,row,failed['status'],now)
        elif children and all(b['status']=='completed' for b in children):
            if kind=='study':c.execute("UPDATE lab_studies SET status='completed',updated_at=? WHERE id=?",(now,oid))
            _update(c,row,'completed',now)
        elif owner['status']=='paused':_update(c,row,'paused',now)
        elif kind=='study' and not any(b['status']=='running' for b in children):
            _owner_state(c,kind,owner,'queued',now);_update(c,row,'queued',now,tail=True)
    for row in c.execute("SELECT * FROM lab_queue_items WHERE state='queued' ORDER BY queue_order,id LIMIT ?",(max(0,max_items-handled),)).fetchall():
        if time.monotonic()>=deadline:break
        kind=row['owner_kind'];oid=row['owner_id']
        try:owner=_eligible(store,c,row['user_id'],kind,oid)
        except RuleLabError as exc:
            state='completed' if exc.code=='invalid_transition' else 'removed' if exc.code in ('not_found','deleting','series_managed') else 'error'
            if state=='error':
                table='lab_batches' if kind=='batch' else 'lab_studies'
                c.execute("UPDATE "+table+" SET status='error',error=?,updated_at=? WHERE id=?",(exc.message,now,oid))
            _update(c,row,state,now);continue
        if configuration(c,kind,owner)!=row['configuration_hash']:
            _owner_state(c,kind,owner,'paused',now);_update(c,row,'removed',now);continue
        children=_children(c,kind,owner);child=next((b for b in children if b['status']!='completed'),None)
        if child is None:
            if kind=='study':c.execute("UPDATE lab_studies SET status='completed',updated_at=? WHERE id=?",(now,oid))
            _update(c,row,'completed',now);continue
        if child['status'] in ('error','resource_paused'):
            if kind=='study':c.execute('UPDATE lab_studies SET status=?,error=?,updated_at=? WHERE id=?',(child['status'],child['error'],now,oid))
            _update(c,row,child['status'],now);continue
        if owner['status']=='running':_update(c,row,'running',now);continue
        count=c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(child['id'],)).fetchone()[0]
        if count>store._capacity(c)['remaining_running']:break
        if kind=='study' and not owner['cutoff_issue'] and not cutoff:break
        try:store._require_space()
        except RuleLabError as exc:
            table='lab_batches' if kind=='batch' else 'lab_studies'
            c.execute("UPDATE "+table+" SET status='resource_paused',error=?,updated_at=? WHERE id=?",(exc.message,now,oid));_update(c,row,'resource_paused',now);break
        if _load(child['cursor_json']).get('input_expired'):
            table='lab_batches' if kind=='batch' else 'lab_studies'
            c.execute("UPDATE "+table+" SET status='error',error='所需输入已过期，请新建实验',updated_at=? WHERE id=?",(now,oid));_update(c,row,'error',now);continue
        if kind=='study' and not owner['cutoff_issue']:
            from pc28touzhu.services.rule_lab_store import _issue,_time
            _issue(cutoff['issue_no']);_time(cutoff['at'])
            c.execute('UPDATE lab_studies SET cutoff_issue=?,cutoff_at=? WHERE id=?',(cutoff['issue_no'],cutoff['at'],oid))
        c.execute("UPDATE lab_batches SET status='running',revision=revision+1,started_at=COALESCE(started_at,?),updated_at=?,error='' WHERE id=?",(now,now,child['id']))
        if kind=='study':c.execute("UPDATE lab_studies SET status='running',updated_at=?,error='' WHERE id=?",(now,oid))
        _update(c,row,'running',now)
