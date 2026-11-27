"""Persistent continuation lifecycle; all mutations share the ledger transaction."""
from __future__ import annotations

import hashlib
import json
import re
import zlib
from datetime import date, datetime, timedelta, timezone

from pc28touzhu.domain.rule_lab_continuation import fingerprint
from pc28touzhu.services.rule_lab_store import RuleLabError, _day, _dump, _issue, _load, _time

SHANGHAI=timezone(timedelta(hours=8))
ENGINE_VERSION=3
ACTIVE=('queued','running','paused')


def init_schema(c):
    statements=(
        '''CREATE TABLE IF NOT EXISTS lab_series (
            id INTEGER PRIMARY KEY,owner_kind TEXT NOT NULL,owner_id INTEGER NOT NULL,user_id INTEGER NOT NULL,
            predictor_id INTEGER NOT NULL,stream_id INTEGER NOT NULL,configuration_hash TEXT NOT NULL,
            revision INTEGER NOT NULL DEFAULT 1,capture_enabled INTEGER NOT NULL DEFAULT 0,
            compute_enabled INTEGER NOT NULL DEFAULT 0,daily_at TEXT NOT NULL DEFAULT '00:10',
            timezone TEXT NOT NULL DEFAULT 'Asia/Shanghai',next_run_at TEXT,
            state TEXT NOT NULL DEFAULT 'idle',error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
            UNIQUE(owner_kind,owner_id))''',
        '''CREATE TABLE IF NOT EXISTS lab_continuation_requests (
            id INTEGER PRIMARY KEY,series_id INTEGER NOT NULL REFERENCES lab_series(id),request_id TEXT NOT NULL,
            kind TEXT NOT NULL,payload_hash TEXT NOT NULL,status TEXT NOT NULL,target_end_date TEXT,
            segment_id INTEGER,result_json TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
            UNIQUE(series_id,request_id))''',
        '''CREATE TABLE IF NOT EXISTS lab_segments (
            id INTEGER PRIMARY KEY,series_id INTEGER NOT NULL REFERENCES lab_series(id),sequence INTEGER NOT NULL,
            predecessor_id INTEGER REFERENCES lab_segments(id),kind TEXT NOT NULL,request_id INTEGER,
            requested_end_date TEXT NOT NULL,cutoff_issue INTEGER NOT NULL,cutoff_at TEXT NOT NULL,
            archive_revision INTEGER NOT NULL,manifest_hash TEXT NOT NULL,status TEXT NOT NULL,
            summary_json TEXT NOT NULL DEFAULT '{}',created_at TEXT NOT NULL,completed_at TEXT,
            UNIQUE(series_id,sequence))''',
        "CREATE UNIQUE INDEX IF NOT EXISTS lab_segments_active ON lab_segments(series_id) WHERE status IN ('queued','running','paused')",
        '''CREATE TABLE IF NOT EXISTS lab_segment_batches (
            segment_id INTEGER NOT NULL REFERENCES lab_segments(id),batch_id INTEGER NOT NULL REFERENCES lab_batches(id),
            status TEXT NOT NULL,baseline_json BLOB NOT NULL,summary_json TEXT NOT NULL DEFAULT '{}',
            PRIMARY KEY(segment_id,batch_id))''',
        '''CREATE TABLE IF NOT EXISTS lab_checkpoints (
            id INTEGER PRIMARY KEY,segment_id INTEGER NOT NULL REFERENCES lab_segments(id),
            batch_id INTEGER NOT NULL REFERENCES lab_batches(id),engine_version INTEGER NOT NULL,
            configuration_hash TEXT NOT NULL,state_hash TEXT NOT NULL,payload BLOB NOT NULL,
            created_at TEXT NOT NULL,UNIQUE(segment_id,batch_id))''',
        '''CREATE TABLE IF NOT EXISTS lab_monthly_snapshots (
            id INTEGER PRIMARY KEY,series_id INTEGER NOT NULL REFERENCES lab_series(id),
            batch_id INTEGER NOT NULL,experiment_id INTEGER NOT NULL,month TEXT NOT NULL,revision INTEGER NOT NULL,
            payload_json TEXT NOT NULL,reason TEXT NOT NULL,created_at TEXT NOT NULL,
            UNIQUE(series_id,experiment_id,month,revision))''',
    )
    for statement in statements:c.execute(statement)
    existing={r[1] for r in c.execute('PRAGMA table_info(lab_series)')}
    for name,definition in (('replay_target_end','TEXT'),('replay_target_issue','INTEGER'),('replay_archive_revision','INTEGER'),('replay_cutoff_at','TEXT')):
        if name not in existing:c.execute('ALTER TABLE lab_series ADD COLUMN '+name+' '+definition)


def _pack(value):return zlib.compress(_dump(value).encode(),6)
def _unpack(value):return json.loads(zlib.decompress(value).decode())
def _stamp(value):return value.astimezone(timezone.utc).isoformat().replace('+00:00','Z')


def _owner(store,c,user_id,kind,owner_id):
    if kind not in ('batch','study') or type(owner_id) is not int or owner_id<=0:
        raise RuleLabError('invalid_input','续算对象无效')
    if kind=='batch':
        row=store._owned(c,user_id,owner_id)
        if c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(owner_id,)).fetchone():
            raise RuleLabError('group_action_required','比较组子方案请从比较组统一续算',409)
        return [row],None
    study=store._owned_study(c,user_id,owner_id)
    rows=c.execute('''SELECT b.* FROM lab_batches b JOIN lab_study_batches s ON s.batch_id=b.id
        WHERE s.study_id=? ORDER BY s.position''',(owner_id,)).fetchall()
    return rows,study


def series_for_batch(c,batch_id):
    return c.execute('''SELECT s.* FROM lab_series s WHERE (s.owner_kind='batch' AND s.owner_id=?)
        OR (s.owner_kind='study' AND s.owner_id IN (SELECT study_id FROM lab_study_batches WHERE batch_id=?)) LIMIT 1''',
        (batch_id,batch_id)).fetchone()


def _batches(c,series):
    if series['owner_kind']=='batch':
        return c.execute('SELECT * FROM lab_batches WHERE id=?',(series['owner_id'],)).fetchall()
    return c.execute('''SELECT b.* FROM lab_batches b JOIN lab_study_batches s ON s.batch_id=b.id
        WHERE s.study_id=? ORDER BY s.position''',(series['owner_id'],)).fetchall()


def _configuration(c,rows):
    values=[]
    for row in rows:
        value={key:row[key] for key in ('id','predictor_id','metric','template_pack','stake_cents','risk_policy_json')}
        value['experiments']=[{key:e[key] for key in ('id','condition_key','condition_json','settlement_rule_id','settlement_snapshot_json')}
            for e in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id',(row['id'],))]
        values.append(value)
    return fingerprint(values)


def _pending_count(c,batch_ids):
    if not batch_ids:return 0
    return c.execute('''SELECT COUNT(*) FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id
        WHERE e.batch_id IN ('''+','.join('?' for _ in batch_ids)+") AND d.status='pending'",batch_ids).fetchone()[0]


def _warm(c,predictor_id,last):
    return [dict(r) for r in c.execute('''SELECT d.*,p.created_at AS prediction_created_at,
        p.settled_at AS prediction_settled_at,p.status AS prediction_status,p.big_small,p.odd_even,p.combo,
        p.input_hash AS prediction_hash,p.conflict AS prediction_conflict FROM lab_draws d
        LEFT JOIN lab_predictions p ON p.issue_no=d.issue_no AND p.predictor_id=?
        WHERE d.issue_key<=? ORDER BY d.issue_key DESC LIMIT 100''',(predictor_id,last))][::-1]


def _last_processed(rows):
    values=[int(_load(row['cursor_json']).get('last_issue_no') or 0) for row in rows]
    return min(values) if values else 0


def _opened(c,issue):
    value=c.execute('SELECT open_at FROM lab_draws WHERE issue_key=?',(issue,)).fetchone()
    return value[0] if value else None


def _revision(series,state_hash=None):
    return 'series:%s:%s'%(series['id'],series['revision']) if series else 'legacy:'+state_hash


def _summary(c,series,rows=None):
    rows=_batches(c,series) if rows is None and series else (rows or [])
    last=_last_processed(rows);opened=None
    for row in rows:
        if int(_load(row['cursor_json']).get('last_issue_no') or 0)==last:
            opened=(_load(row['cursor_json']).get('retention_v1') or {}).get('processed_open_at')
            if opened:break
    opened=opened or (_opened(c,last) if last else None)
    ids=[row['id'] for row in rows]
    pending=_pending_count(c,ids)
    earliest_pending=c.execute('''SELECT MIN(d.issue_key) FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id
        WHERE e.batch_id IN ('''+','.join('?' for _ in ids)+") AND d.status='pending'",ids).fetchone()[0] if ids else None
    settled=min(last,int(earliest_pending)-1) if earliest_pending else last
    active=c.execute("SELECT * FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused') ORDER BY id DESC LIMIT 1",(series['id'],)).fetchone() if series else None
    stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(series['stream_id'],)).fetchone() if series else None
    if series and last and not opened:
        from pc28touzhu.services.rule_lab_archive import read_input
        saved=c.execute('SELECT archive_revision FROM lab_segments WHERE series_id=? ORDER BY sequence DESC LIMIT 1',(series['id'],)).fetchone()
        value=read_input(c,stream['source_key'],'draw',0,last,saved[0] if saved else None)
        if value:opened=value.get('open_at')
    captured=int(stream['covered_through_issue'] or 0) if stream else 0
    captured_open=_opened(c,captured) if captured else None
    if stream and captured:
        from pc28touzhu.services.rule_lab_archive import read_input
        value=read_input(c,stream['source_key'],'draw',0,captured)
        if value:captured_open=value.get('open_at')
    effective=max([row['end_date'] or '' for row in rows]+([active['requested_end_date']] if active else [])) or None
    if series:
        latest=c.execute("SELECT requested_end_date FROM lab_segments WHERE series_id=? AND status='completed' ORDER BY sequence DESC LIMIT 1",(series['id'],)).fetchone()
        if latest:effective=max(effective or '',latest[0])
        if series['replay_target_end']:effective=max(effective or '',series['replay_target_end'])
    state=series['state'] if series else 'disabled'
    if active:state='paused' if active['status']=='paused' else 'running' if active['status']=='running' else 'waiting_inputs'
    if stream and stream['state'] in ('archive_gap','storage_paused','source_unavailable') and not active:state=stream['state']
    complete_through=None
    if series:
        for finished in c.execute("SELECT requested_end_date,kind,summary_json FROM lab_segments WHERE series_id=? AND status='completed' ORDER BY sequence DESC",(series['id'],)):
            if finished['kind']=='baseline' or _load(finished['summary_json']).get('time_complete'):
                complete_through=finished['requested_end_date'];break
    return dict(enabled=bool(series),capture_enabled=bool(series and series['capture_enabled']),
        compute_enabled=bool(series and series['compute_enabled']),revision=_revision(series) if series else None,
        state=state,calculated_through_issue=str(last) if last else None,
        calculated_through_date=_day(opened) if opened else (rows[0]['end_date'] if rows else None),
        captured_through_issue=str(captured) if captured else None,captured_through_date=_day(captured_open) if captured_open else None,
        settled_through_issue=str(last) if last and not pending else (str(settled) if settled else None),pending_count=pending,
        daily_at=series['daily_at'] if series else '00:10',timezone='Asia/Shanghai',
        next_run_at=series['next_run_at'] if series and series['compute_enabled'] else None,
        active_segment_id=active['id'] if active else None,error=series['error'] if series else '',
        effective_end_date=effective,completed_through_date=complete_through,coverage_quality='known_skips' if any(_load(r['skip_counts_json']) for r in rows) else 'complete',
        comparable=not active)


def public_summary(c,batch):
    series=series_for_batch(c,batch['id'])
    return _summary(c,series,[batch] if not series else None)


def _preview(store,c,user_id,kind,owner_id,now):
    rows,study=_owner(store,c,user_id,kind,owner_id)
    series=c.execute('SELECT * FROM lab_series WHERE owner_kind=? AND owner_id=?',(kind,owner_id)).fetchone()
    blockers=[]
    def block(code,message):
        if not any(x['code']==code for x in blockers):blockers.append(dict(code=code,message=message))
    config_hash=_configuration(c,rows)
    last=_last_processed(rows);ids=[r['id'] for r in rows]
    experiments=[dict(e) for row in rows for e in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id',(row['id'],))]
    warm=_warm(c,rows[0]['predictor_id'],last) if rows and last else []
    if not rows:block('incomplete_group','缺少续算批次')
    if any(row['status']=='deleting' for row in rows):block('deleting','实验正在删除，不能继续续算')
    if not series:
        if any(row['mode']!='backtest' for row in rows):block('not_backtest','只有历史实验支持增量续算')
        if any(row['status']!='completed' for row in rows):block('not_completed','当前回测完成后才能接入增量续算')
        if not last or any(int(_load(r['cursor_json']).get('last_issue_no') or 0)!=last for r in rows):block('unequal_cursors','缺少共同的实际计算边界')
        if any(not _load(row['cursor_json']).get('replay_complete') for row in rows):block('replay_incomplete','尚未保存回放完成状态')
        if any(store._replay_review_required(row) for row in rows):block('review_required','旧回放引擎成绩需复核，不能直接续算')
        if any(_load(row['cursor_json']).get('input_expired') or (_load(row['cursor_json']).get('evidence') or {}).get('conflict') for row in rows):block('input_conflict','历史输入过期或冲突，不能直接续算')
        if _pending_count(c,ids):block('pending_settlements','旧回测仍有待结算记录，尚不能建立基线')
        if [r['issue_key'] for r in warm]!=list(range(last-99,last+1)):block('warm_window_missing','最后100期暖机记录不完整')
        if any(not r['prediction_hash'] for r in warm):block('warm_prediction_missing','暖机预测缺少逐期证据，不能把缓存缺行当作原本无预测')
        if any(r['conflict'] or r['prediction_conflict'] for r in warm):block('warm_window_conflict','暖机输入存在冲突')
        for experiment in experiments:
            owner=next(r for r in rows if r['id']==experiment['batch_id'])
            verified=(_load(owner['cursor_json']).get('retention_v1') or {}).get('frontier_verified')
            if experiment['last_settled_issue'] and not verified and not c.execute('SELECT 1 FROM lab_draws WHERE issue_key=?',(experiment['last_settled_issue'],)).fetchone():
                block('settlement_frontier_missing','最后已结期的可见时间证据缺失，不能安全接管旧成绩')
        if study and not store._study(c,study)['comparable']:block('incomparable_history','比较组历史输入不一致，不能共同续算')
    elif series['configuration_hash']!=config_hash:block('configuration_changed','策略配置已变化，必须建立新实验')
    state_hash=fingerprint(dict(rows=[dict(r) for r in rows],experiments=experiments,warm=warm))
    summary=_summary(c,series,rows);summary['revision']=_revision(series,state_hash)
    latest=c.execute('SELECT * FROM lab_continuation_requests WHERE series_id=? ORDER BY id DESC LIMIT 1',(series['id'],)).fetchone() if series else None
    receipt=dict(request_id=latest['request_id'],kind=latest['kind'],status=latest['status'],end_date=latest['target_end_date'],segment_id=latest['segment_id']) if latest else None
    return dict(eligible=not blockers,blockers=blockers,expected_revision=summary['revision'],
        next_issue=str(last+1) if last else None,configuration_hash=config_hash,state_hash=state_hash,
        saved_experiments=len(experiments),predictor_id=rows[0]['predictor_id'] if rows else None,
        continuation=summary,latest_request=receipt),rows,series


def continuation_preview(store,user_id,kind,owner_id,*,now):
    with store._connect() as c:return _preview(store,c,user_id,kind,owner_id,now)[0]


def _request_id(value):
    if not isinstance(value,str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,80}',value):
        raise RuleLabError('invalid_input','请求标识必须为8至80位字母、数字、下划线或短横线')
    return value


def _validate_payload(payload,kind,now):
    required={'request_id','expected_revision'}|({'end_date'} if kind=='append' else {'capture_enabled','compute_enabled','daily_at','timezone'})
    if not isinstance(payload,dict) or set(payload)!=required:raise RuleLabError('invalid_input','续算请求字段不完整或包含未知字段')
    _request_id(payload['request_id'])
    if not isinstance(payload['expected_revision'],str) or not 1<=len(payload['expected_revision'])<=150:raise RuleLabError('invalid_input','续算版本标识无效')
    if kind=='configure':
        if type(payload['capture_enabled']) is not bool or type(payload['compute_enabled']) is not bool:raise RuleLabError('invalid_input','续算开关必须为布尔值')
        if payload['compute_enabled'] and not payload['capture_enabled']:raise RuleLabError('invalid_input','自动续算需要先保留后续数据')
        if payload['timezone']!='Asia/Shanghai' or not isinstance(payload['daily_at'],str) or not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]',payload['daily_at']):raise RuleLabError('invalid_input','每日时间必须为北京时间HH:MM')
    else:
        try:target=date.fromisoformat(payload['end_date'])
        except (ValueError,TypeError):raise RuleLabError('invalid_input','追加结束日期无效')
        if target.isoformat()!=payload['end_date'] or target>_time(now).astimezone(SHANGHAI).date():raise RuleLabError('invalid_input','追加结束日期不能超过今天')


def _next_due(now,daily_at):
    local=_time(now).astimezone(SHANGHAI);hour,minute=map(int,daily_at.split(':'))
    target=local.replace(hour=hour,minute=minute,second=0,microsecond=0)
    if target<=local:target+=timedelta(days=1)
    return _stamp(target)


def _snapshot(c,row):
    experiments=[dict(e) for e in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id',(row['id'],))]
    ids=[e['id'] for e in experiments];marks=','.join('?' for _ in ids)
    cursor=_load(row['cursor_json']);frontier_day=_day(cursor['simulation_time']) if cursor.get('simulation_time') else row['end_date']
    last=int(cursor.get('last_issue_no') or 0)
    frontier=cursor.get('retention_v1') or {}
    visible=frontier.get('settled_available_at')
    if not frontier.get('frontier_verified'):
        from pc28touzhu.services.rule_lab_retention import _backfill
        _backfill(c,row)
        row=c.execute('SELECT * FROM lab_batches WHERE id=?',(row['id'],)).fetchone()
        visible=(_load(row['cursor_json']).get('retention_v1') or {}).get('settled_available_at')
    return dict(batch=dict(row),experiments=experiments,settled_frontier_available_at=visible,
        risk_days=[dict(r) for r in c.execute('''SELECT * FROM lab_risk_days WHERE experiment_id IN ('''+marks+''')
            AND (stat_date>=? OR EXISTS(SELECT 1 FROM lab_decisions d WHERE d.experiment_id=lab_risk_days.experiment_id
                AND d.status='pending' AND d.risk_date=lab_risk_days.stat_date)) ORDER BY experiment_id,stat_date''',ids+[frontier_day])],
        active_rounds=[dict(r) for r in c.execute("SELECT * FROM lab_rounds WHERE experiment_id IN ("+marks+") AND status!='closed' ORDER BY experiment_id,round_no",ids)],
        pending=[dict(r) for r in c.execute("SELECT * FROM lab_decisions WHERE experiment_id IN ("+marks+") AND status='pending' ORDER BY issue_key,id",ids)],
        history=_warm(c,row['predictor_id'],int(cursor.get('last_issue_no') or 0)),warm_inputs=_warm_map(c,row))


def _checkpoint(c,segment_id,row,config_hash,now):
    payload=_snapshot(c,row)
    c.execute('INSERT INTO lab_checkpoints(segment_id,batch_id,engine_version,configuration_hash,state_hash,payload,created_at) VALUES(?,?,?,?,?,?,?)',
        (segment_id,row['id'],ENGINE_VERSION,config_hash,fingerprint(payload),_pack(payload),now))
    return payload


def _adopt(store,c,rows,user_id,kind,owner_id,preview,now):
    from pc28touzhu.services.rule_lab_archive import ensure_stream,archive_revision
    store._require_space()
    last=_last_processed(rows);stream=ensure_stream(c,rows[0]['predictor_id'],last,now)
    from pc28touzhu.services.rule_lab_management import allocate_id
    sid=allocate_id(c,'lab_series')
    c.execute('''INSERT INTO lab_series(id,owner_kind,owner_id,user_id,predictor_id,stream_id,configuration_hash,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?)''',(sid,kind,owner_id,user_id,rows[0]['predictor_id'],stream['id'],preview['configuration_hash'],now,now))
    cursor=_load(rows[0]['cursor_json']);opened=_opened(c,last) or cursor.get('simulation_time') or now
    end=max(r['end_date'] for r in rows)
    rev=archive_revision(c)
    seg=c.execute('''INSERT INTO lab_segments(series_id,sequence,kind,requested_end_date,cutoff_issue,cutoff_at,archive_revision,manifest_hash,status,created_at,completed_at)
        VALUES(?,1,'baseline',?,?,?,?,?,'completed',?,?)''',(sid,end,last,opened,rev,preview['state_hash'],now,now)).lastrowid
    summaries=[]
    for row in rows:
        snapshot=_checkpoint(c,seg,row,preview['configuration_hash'],now)
        item=_segment_summary(snapshot,snapshot)
        c.execute("INSERT INTO lab_segment_batches(segment_id,batch_id,status,baseline_json,summary_json) VALUES(?,?,'completed',?,?)",(seg,row['id'],_pack(dict(retention_version=1,checkpoint_retained=False)),_dump(item)))
        summaries.append(item)
    c.execute('UPDATE lab_segments SET summary_json=? WHERE id=?',(_dump(dict(batches=summaries,baseline_audit=True)),seg))
    return c.execute('SELECT * FROM lab_series WHERE id=?',(sid,)).fetchone()


def _receipt(c,series,payload,kind):
    row=c.execute('SELECT * FROM lab_continuation_requests WHERE series_id=? AND request_id=?',(series['id'],payload['request_id'])).fetchone()
    if row and (row['payload_hash']!=fingerprint(payload) or row['kind']!=kind):raise RuleLabError('request_conflict','同一请求标识已用于不同内容',409)
    return row


def set_continuation(store,user_id,kind,owner_id,payload,*,now):
    _validate_payload(payload,'configure',now)
    with store._connect(write=True) as c:
        preview,rows,series=_preview(store,c,user_id,kind,owner_id,now)
        receipt=_receipt(c,series,payload,'configure') if series else None
        if receipt:return dict(created=False,continuation=_summary(c,series))
        if payload['expected_revision']!=preview['expected_revision']:raise RuleLabError('stale_revision','续算状态已变化，请刷新后重试',409)
        if not preview['eligible']:raise RuleLabError(preview['blockers'][0]['code'],preview['blockers'][0]['message'],409)
        created=series is None
        if series is None:series=_adopt(store,c,rows,user_id,kind,owner_id,preview,now)
        due=_next_due(now,payload['daily_at']) if payload['compute_enabled'] else None
        c.execute('''UPDATE lab_series SET capture_enabled=?,compute_enabled=?,daily_at=?,next_run_at=?,revision=revision+1,
            state=CASE WHEN state IN ('idle','disabled') THEN ? ELSE state END,error='',updated_at=? WHERE id=?''',
            (int(payload['capture_enabled']),int(payload['compute_enabled']),payload['daily_at'],due,
             'idle' if payload['capture_enabled'] else 'disabled',now,series['id']))
        c.execute('''INSERT INTO lab_continuation_requests(series_id,request_id,kind,payload_hash,status,result_json,created_at,updated_at)
            VALUES(?,?,'configure',?,'completed','{}',?,?)''',(series['id'],payload['request_id'],fingerprint(payload),now,now))
        series=c.execute('SELECT * FROM lab_series WHERE id=?',(series['id'],)).fetchone()
        return dict(created=created,continuation=_summary(c,series))


def _segment_json(row):
    if row is None:return None
    result={key:row[key] for key in ('id','series_id','sequence','predecessor_id','kind','requested_end_date','cutoff_at','archive_revision','manifest_hash','status','created_at','completed_at')}
    result['cutoff_issue']=str(row['cutoff_issue']);result['summary']=_load(row['summary_json'])
    return result


def _append_response(c,series,receipt,*,created=False):
    segment=c.execute('SELECT * FROM lab_segments WHERE id=?',(receipt['segment_id'],)).fetchone() if receipt['segment_id'] else None
    return dict(created=created,queued=receipt['status']=='waiting_inputs',up_to_date=receipt['status']=='up_to_date',
        request_id=receipt['request_id'],continuation=_summary(c,series),segment=_segment_json(segment))


def request_append(store,user_id,kind,owner_id,payload,*,now):
    _validate_payload(payload,'append',now)
    with store._connect(write=True) as c:
        preview,rows,series=_preview(store,c,user_id,kind,owner_id,now)
        receipt=_receipt(c,series,payload,'append') if series else None
        if receipt:return _append_response(c,series,receipt)
        if preview['expected_revision']!=payload['expected_revision']:raise RuleLabError('stale_revision','续算状态已变化，请刷新后重试',409)
        if not preview['eligible']:raise RuleLabError(preview['blockers'][0]['code'],preview['blockers'][0]['message'],409)
        last_date=preview['continuation']['calculated_through_date'] or rows[0]['end_date']
        delta=(date.fromisoformat(payload['end_date'])-date.fromisoformat(last_date)).days
        if delta<0 or delta>90:raise RuleLabError('invalid_input','每次追加范围不能倒退或超过90天')
        if series is None:
            series=_adopt(store,c,rows,user_id,kind,owner_id,preview,now)
            c.execute('UPDATE lab_series SET capture_enabled=1 WHERE id=?',(series['id'],))
        elif not series['capture_enabled']:raise RuleLabError('capture_disabled','请先开启保留后续数据',409)
        if c.execute("SELECT 1 FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused')",(series['id'],)).fetchone():raise RuleLabError('segment_active','当前追加尚未结束，请先完成或恢复当前段',409)
        if c.execute("SELECT 1 FROM lab_continuation_requests WHERE series_id=? AND kind='append' AND status='waiting_inputs'",(series['id'],)).fetchone():raise RuleLabError('append_waiting','已有追加请求等待输入，请勿重复提交',409)
        rid=c.execute('''INSERT INTO lab_continuation_requests(series_id,request_id,kind,payload_hash,status,target_end_date,created_at,updated_at)
            VALUES(?,?,'append',?,'waiting_inputs',?,?,?)''',(series['id'],payload['request_id'],fingerprint(payload),payload['end_date'],now,now)).lastrowid
        c.execute("UPDATE lab_series SET revision=revision+1,state='waiting_inputs',error='',updated_at=? WHERE id=?",(now,series['id']))
        series=c.execute('SELECT * FROM lab_series WHERE id=?',(series['id'],)).fetchone()
        receipt=c.execute('SELECT * FROM lab_continuation_requests WHERE id=?',(rid,)).fetchone()
        return _append_response(c,series,receipt,created=True)


def list_segments(store,user_id,kind,owner_id,*,limit=20,before_id=None):
    if type(limit) is not int or not 1<=limit<=50 or (before_id is not None and (type(before_id) is not int or before_id<=0)):raise RuleLabError('invalid_input','分段分页参数无效')
    with store._connect() as c:
        _owner(store,c,user_id,kind,owner_id)
        series=c.execute('SELECT * FROM lab_series WHERE owner_kind=? AND owner_id=?',(kind,owner_id)).fetchone()
        if not series:return dict(segments=[],next_cursor=None)
        sql='SELECT * FROM lab_segments WHERE series_id=?';params=[series['id']]
        if before_id is not None:sql+=' AND id<?';params.append(before_id)
        rows=c.execute(sql+' ORDER BY id DESC LIMIT ?',params+[limit+1]).fetchall()
        more=len(rows)>limit;rows=rows[:limit]
        values=[]
        for row in rows:
            value=_segment_json(row)
            value['checkpoint_retained']=bool(c.execute('SELECT 1 FROM lab_checkpoints WHERE segment_id=? LIMIT 1',(row['id'],)).fetchone())
            values.append(value)
        return dict(segments=values,next_cursor=rows[-1]['id'] if more else None)


def input_coverage(store,user_id,kind,owner_id):
    with store._connect() as c:
        rows,_=_owner(store,c,user_id,kind,owner_id)
        series=c.execute('SELECT * FROM lab_series WHERE owner_kind=? AND owner_id=?',(kind,owner_id)).fetchone()
        coverage=dict(counts={},gaps=[],state='disabled',last_success_at=None,retained_scope='runtime_dependencies',
            history_notice='覆盖明细仅含计算依赖范围；已消费旧输入按结果存储策略清理，历史成绩和缺样统计保持',
            historical_skip_counts={str(row['id']):_load(row['skip_counts_json']) for row in rows})
        if series:
            stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(series['stream_id'],)).fetchone()
            coverage.update(state=stream['state'],last_success_at=stream['last_success_at'],error=stream['error'],
                highest_seen_issue=str(stream['highest_seen_issue']) if stream['highest_seen_issue'] else None)
            records=c.execute('''SELECT v.class,COUNT(*) AS n FROM lab_input_coverage v WHERE v.stream_id=?
                AND v.revision=(SELECT MAX(q.revision) FROM lab_input_coverage q WHERE q.stream_id=v.stream_id AND q.issue_key=v.issue_key)
                GROUP BY v.class''',(stream['id'],)).fetchall()
            coverage['counts']={r['class']:r['n'] for r in records}
            coverage['gaps']=[dict(issue_no=str(r['issue_key']),reason=r['class']) for r in c.execute('''SELECT v.* FROM lab_input_coverage v
                WHERE v.stream_id=? AND v.class IN ('waiting','archive_gap')
                AND v.revision=(SELECT MAX(q.revision) FROM lab_input_coverage q WHERE q.stream_id=v.stream_id AND q.issue_key=v.issue_key)
                ORDER BY v.issue_key LIMIT 50''',(stream['id'],))]
        return dict(continuation=_summary(c,series,rows),coverage=coverage,storage=store.storage_status())


def _segment_summary(before,after):
    previous={e['id']:_load(e['stats_json']) for e in before['experiments']}
    additive=('trigger_count','settled_count','hit_count','refund_count','miss_count','skipped_count','direction_hit_count','stake_cents','net_cents')
    total=dict.fromkeys(additive,0)
    for e in after['experiments']:
        stats=_load(e['stats_json']);old=previous.get(e['id'],{})
        for key in additive:total[key]+=stats.get(key,0)-old.get(key,0)
    return dict(batch_id=after['batch']['id'],processed_issues=after['batch']['processed_issues']-before['batch']['processed_issues'],
        last_issue_no=_load(after['batch']['cursor_json']).get('last_issue_no'),pending_count=len(after['pending']),incremental=total)


def study_summary(c,row):
    series=c.execute("SELECT * FROM lab_series WHERE owner_kind='study' AND owner_id=?",(row['id'],)).fetchone()
    rows=c.execute('SELECT b.* FROM lab_batches b JOIN lab_study_batches s ON s.batch_id=b.id WHERE s.study_id=? ORDER BY s.position',(row['id'],)).fetchall()
    return _summary(c,series,rows)


def snapshots_for_experiment(c,experiment_id):
    return [dict(month=r['month'],revision=r['revision'],reason=r['reason'],created_at=r['created_at'],report=_load(r['payload_json']))
        for r in c.execute('''SELECT * FROM lab_monthly_snapshots v WHERE experiment_id=?
            AND revision=(SELECT MAX(q.revision) FROM lab_monthly_snapshots q WHERE q.experiment_id=v.experiment_id AND q.month=v.month)
            ORDER BY month DESC LIMIT 120''',(experiment_id,))]


def monthly_snapshots(store,user_id,batch_id,experiment_id):
    with store._connect() as c:
        store._owned(c,user_id,batch_id)
        if not c.execute('SELECT 1 FROM lab_experiments WHERE id=? AND batch_id=?',(experiment_id,batch_id)).fetchone():raise RuleLabError('not_found','实验版本不存在',404)
        return snapshots_for_experiment(c,experiment_id)


def work_context(c,batch):
    series=series_for_batch(c,batch['id'])
    if not series:return None
    segment=c.execute('''SELECT s.* FROM lab_segments s JOIN lab_segment_batches b ON b.segment_id=s.id
        WHERE s.series_id=? AND b.batch_id=? ORDER BY s.sequence DESC LIMIT 1''',(series['id'],batch['id'])).fetchone()
    if not segment:return None
    previous=c.execute('SELECT * FROM lab_segments WHERE id=?',(segment['predecessor_id'],)).fetchone() if segment['predecessor_id'] else segment
    cursor=_load(batch['cursor_json'])
    return dict(series_id=series['id'],segment_id=segment['id'],segment_kind=segment['kind'],
        segment_status=segment['status'],archive_revision=segment['archive_revision'],
        effective_end_date=segment['requested_end_date'],cutoff_issue=str(segment['cutoff_issue']),cutoff_at=segment['cutoff_at'],
        frontier_time=cursor.get('simulation_time'),engine_version=ENGINE_VERSION,
        baseline_archive_revision=previous['archive_revision'],baseline_through_issue=str(previous['cutoff_issue']),
        stream_id=series['stream_id'],predictor_id=series['predictor_id'],capture_enabled=bool(series['capture_enabled']),
        compute_enabled=bool(series['compute_enabled']))


def archive_source(store,batch_id):
    from pc28touzhu.services.rule_lab_archive import ArchiveSource
    with store._connect() as c:
        batch=c.execute('SELECT * FROM lab_batches WHERE id=?',(batch_id,)).fetchone()
        if not batch:raise RuleLabError('not_found','实验批次不存在',404)
        context=work_context(c,batch)
        if not context:raise RuleLabError('continuation_disabled','实验尚未启用续算',409)
        stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(context['stream_id'],)).fetchone()
        segment=c.execute('SELECT * FROM lab_segments WHERE id=?',(context['segment_id'],)).fetchone()
        checkpoint_id=segment['id'] if segment['status']=='completed' else segment['predecessor_id'] or segment['id']
        checkpoint=c.execute('SELECT payload FROM lab_checkpoints WHERE segment_id=? AND batch_id=?',(checkpoint_id,batch_id)).fetchone()
        warm_inputs=_unpack(checkpoint[0]).get('warm_inputs',{}) if checkpoint else {}
    return ArchiveSource(store,context['predictor_id'],context['archive_revision'],int(context['cutoff_issue']),context['cutoff_at'],
        source_key=stream['source_key'],baseline_revision=context['baseline_archive_revision'],baseline_through_issue=int(context['baseline_through_issue']),warm_inputs=warm_inputs)


def _input_from_cache(c,predictor_id,issue):
    d=c.execute('SELECT * FROM lab_draws WHERE issue_key=?',(issue,)).fetchone()
    p=c.execute('SELECT * FROM lab_predictions WHERE predictor_id=? AND issue_key=?',(predictor_id,issue)).fetchone()
    draw=None;prediction=None
    if d:
        draw={key:d[key] for key in ('issue_no','source_id','open_at','source_created_at','observed_at','input_hash')}
        draw['triplet']=_load(d['triplet_json'])
    if p:prediction={key:p[key] for key in ('predictor_id','issue_no','source_id','created_at','settled_at','observed_at','status','big_small','odd_even','combo','input_hash')}
    return dict(draw=draw,prediction=prediction,coverage='present' if prediction else 'no_prediction')


def _warm_map(c,row):
    """Carry the exact prior warm inputs through every segment, including revisions."""
    from pc28touzhu.services.rule_lab_archive import read_input
    last=int(_load(row['cursor_json']).get('last_issue_no') or 0)
    context=work_context(c,row)
    previous={}
    if context:
        segment=c.execute('SELECT * FROM lab_segments WHERE id=?',(context['segment_id'],)).fetchone()
        parent_id=segment['id'] if segment['status']=='completed' else segment['predecessor_id']
        checkpoint=c.execute('SELECT payload FROM lab_checkpoints WHERE segment_id=? AND batch_id=?',(parent_id,row['id'])).fetchone() if parent_id else None
        if checkpoint:previous=_unpack(checkpoint[0]).get('warm_inputs',{})
        stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(context['stream_id'],)).fetchone()
    result={}
    needed=set(range(max(1,last-99),last+1))
    needed.update(r[0] for r in c.execute('''SELECT DISTINCT d.issue_key FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id
        WHERE e.batch_id=? AND d.status='pending' ''',(row['id'],)))
    for issue in sorted(needed):
        key=str(issue)
        if key in previous:
            result[key]=dict(previous[key])
            prediction=result[key].get('prediction')
            if context and prediction:
                observed=read_input(c,stream['source_key'],'prediction',row['predictor_id'],issue,context['archive_revision'])
                if observed and observed.get('input_hash')==prediction.get('input_hash') and observed.get('settled_at'):
                    result[key]['prediction']=dict(prediction,settled_at=observed['settled_at'],status=observed['status'])
            continue
        if context and context['segment_kind']!='baseline':
            draw=read_input(c,stream['source_key'],'draw',0,issue,context['archive_revision'])
            prediction=read_input(c,stream['source_key'],'prediction',row['predictor_id'],issue,context['archive_revision'])
            result[key]=dict(draw=draw,prediction=prediction,coverage='present' if prediction else 'no_prediction')
        else:result[key]=_input_from_cache(c,row['predictor_id'],issue)
    return result


def _auto_request(c,series,now):
    if not series['compute_enabled'] or not series['capture_enabled'] or not series['next_run_at'] or _time(series['next_run_at'])>_time(now):return
    if c.execute("SELECT 1 FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused')",(series['id'],)).fetchone():return
    if c.execute("SELECT 1 FROM lab_continuation_requests WHERE series_id=? AND status='waiting_inputs'",(series['id'],)).fetchone():return
    rows=_batches(c,series);summary=_summary(c,series,rows)
    last=date.fromisoformat(summary['calculated_through_date']);yesterday=_time(now).astimezone(SHANGHAI).date()-timedelta(days=1)
    target=min(yesterday,last+timedelta(days=90))
    if target<last:
        c.execute('UPDATE lab_series SET next_run_at=? WHERE id=?',(_next_due(now,series['daily_at']),series['id']));return
    request_id='daily-'+target.isoformat()
    payload=dict(request_id=request_id,end_date=target.isoformat(),automatic=True)
    c.execute('''INSERT OR IGNORE INTO lab_continuation_requests(series_id,request_id,kind,payload_hash,status,target_end_date,created_at,updated_at)
        VALUES(?,?,'append',?,'waiting_inputs',?,?,?)''',(series['id'],request_id,fingerprint(payload),target.isoformat(),now,now))
    c.execute("UPDATE lab_series SET state='waiting_inputs',next_run_at=?,updated_at=? WHERE id=?",(_next_due(now,series['daily_at']),now,series['id']))


def _newest_target(c,stream,revision,last,boundary,now):
    from pc28touzhu.services.rule_lab_archive import read_input
    rows=c.execute('''SELECT v.issue_key FROM lab_input_versions v WHERE source_key=? AND kind='draw' AND predictor_id=0
        AND issue_key>? AND revision<=? AND open_at<? AND source_created_at<=?
        AND revision=(SELECT MAX(q.revision) FROM lab_input_versions q WHERE q.source_key=v.source_key AND q.kind='draw'
            AND q.predictor_id=0 AND q.issue_key=v.issue_key AND q.revision<=?) ORDER BY issue_key DESC LIMIT 1''',
        (stream['source_key'],last,revision,boundary,now,revision)).fetchone()
    if not rows:return None
    return read_input(c,stream['source_key'],'draw',0,rows['issue_key'],revision)


def _start_children(store,c,series,segment,now):
    if segment['status']=='paused':return
    children=c.execute('''SELECT sb.*,b.status AS batch_status FROM lab_segment_batches sb JOIN lab_batches b ON b.id=sb.batch_id
        WHERE sb.segment_id=? ORDER BY b.id''',(segment['id'],)).fetchall()
    for child in children:
        if child['status']=='completed':continue
        if child['batch_status']=='running':return
        if child['status']=='paused':return
        if child['batch_status'] in ('error','resource_paused'):
            c.execute("UPDATE lab_series SET state='error',error='追加执行暂停，请核对错误后恢复' WHERE id=?",(series['id'],));return
        count=c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(child['batch_id'],)).fetchone()[0]
        if store._capacity(c)['running']+count>100:return
        c.execute("UPDATE lab_batches SET status='running',phase='replaying',error='',revision=revision+1,updated_at=? WHERE id=?",(now,child['batch_id']))
        c.execute("UPDATE lab_segment_batches SET status='running' WHERE segment_id=? AND batch_id=?",(segment['id'],child['batch_id']))
        c.execute("UPDATE lab_segments SET status='running' WHERE id=?",(segment['id'],))
        c.execute("UPDATE lab_series SET state='running',error='',updated_at=? WHERE id=?",(now,series['id']))
        if series['owner_kind']=='study':c.execute("UPDATE lab_studies SET status='running',error='',updated_at=? WHERE id=?",(now,series['owner_id']))
        return


def prepare_due_appends(store,*,now,limit=10):
    from pc28touzhu.services.rule_lab_archive import archive_revision,read_input,COMPLETE_CLASSES
    if type(limit) is not int or not 1<=limit<=50:raise ValueError('续算调度批量无效')
    activated=[]
    with store._connect(write=True,reserve=True) as c:
        saved=c.execute("SELECT value_json FROM lab_runtime WHERE key='continuation_last_series'").fetchone()
        previous=int(_load(saved[0])) if saved else 0
        rows=c.execute("SELECT * FROM lab_series WHERE state!='deleting' ORDER BY CASE WHEN id>? THEN 0 ELSE 1 END,id LIMIT ?",(previous,limit)).fetchall()
        if rows:c.execute("INSERT INTO lab_runtime(key,value_json) VALUES('continuation_last_series',?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",(_dump(rows[-1]['id']),))
        for old in rows:
            # Fair scheduling includes inactive and failed consumers; successful
            # progress is not the only event that moves an item to the queue tail.
            c.execute('UPDATE lab_series SET updated_at=? WHERE id=?',(now,old['id']))
            _auto_request(c,old,now)
            series=c.execute('SELECT * FROM lab_series WHERE id=?',(old['id'],)).fetchone()
            active=c.execute("SELECT * FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused')",(series['id'],)).fetchone()
            if active:
                _start_children(store,c,series,active,now);continue
            request=c.execute("SELECT * FROM lab_continuation_requests WHERE series_id=? AND kind='append' AND status='waiting_inputs' ORDER BY id LIMIT 1",(series['id'],)).fetchone()
            if not request or not series['capture_enabled']:continue
            batches=_batches(c,series)
            if _configuration(c,batches)!=series['configuration_hash']:
                c.execute("UPDATE lab_series SET state='error',error='策略配置已变化，不能继续原实验' WHERE id=?",(series['id'],));continue
            stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(series['stream_id'],)).fetchone()
            if not stream['last_success_at'] or stream['state'] in ('archive_gap','source_unavailable','storage_paused'):
                c.execute('UPDATE lab_series SET state=?,error=? WHERE id=?',(stream['state'] if stream['state']!='idle' else 'waiting_inputs',stream['error'],series['id']));continue
            last=_last_processed(batches);revision=archive_revision(c)
            target_date=date.fromisoformat(request['target_end_date'])
            boundary=_stamp(datetime.combine(target_date+timedelta(days=1),datetime.min.time(),tzinfo=SHANGHAI))
            # Freeze what has actually been captured now. The target date limits
            # decision issues; the replay clock separately limits result visibility.
            cutoff_at=now
            target=_newest_target(c,stream,revision,last,boundary,cutoff_at)
            lookahead=c.execute('''SELECT 1 FROM lab_input_versions v WHERE source_key=? AND kind='draw'
                AND predictor_id=0 AND revision<=? AND open_at>=? AND source_created_at<=?
                AND revision=(SELECT MAX(q.revision) FROM lab_input_versions q WHERE q.source_key=v.source_key
                    AND q.kind='draw' AND q.predictor_id=0 AND q.issue_key=v.issue_key AND q.revision<=?) LIMIT 1''',
                (stream['source_key'],revision,boundary,now,revision)).fetchone()
            caught_up=int(stream['after_issue'])>=int(stream['highest_seen_issue'])
            today=_time(now).astimezone(SHANGHAI).date()
            # Past dates need proof that capture reached beyond the target day's
            # close; a page prefix must not masquerade as a whole completed day.
            day_complete=target_date<today and bool(lookahead)
            if target_date<today and not day_complete:continue
            if target_date==today and not caught_up:continue
            if target is None:
                if day_complete or (caught_up and int(stream['highest_seen_issue'])<=last):
                    c.execute("UPDATE lab_continuation_requests SET status='up_to_date',updated_at=? WHERE id=?",(now,request['id']))
                    c.execute("UPDATE lab_series SET state='idle',error='',revision=revision+1,updated_at=? WHERE id=?",(now,series['id']))
                continue
            through=int(target['issue_no'])
            if int(stream['covered_through_issue'])<through:continue
            coverage=c.execute('''SELECT COUNT(*) AS n,MIN(issue_key) AS first,MAX(issue_key) AS last,
                SUM(CASE WHEN class IN ('present','no_prediction','invalid_prediction') THEN 0 ELSE 1 END) AS bad
                FROM lab_input_coverage v WHERE stream_id=? AND issue_key>? AND issue_key<=?
                AND revision=(SELECT MAX(q.revision) FROM lab_input_coverage q WHERE q.stream_id=v.stream_id AND q.issue_key=v.issue_key AND q.revision<=?)''',
                (stream['id'],last,through,revision)).fetchone()
            if coverage['n']!=through-last or coverage['bad']:
                c.execute("UPDATE lab_series SET state='archive_gap',error='新增区间尚有缺档，不能按无预测跳过' WHERE id=?",(series['id'],));continue
            first=read_input(c,stream['source_key'],'draw',0,last+1,revision)
            if first is None:continue
            prior=c.execute("SELECT * FROM lab_segments WHERE series_id=? AND status='completed' ORDER BY sequence DESC LIMIT 1",(series['id'],)).fetchone()
            # Old completed runs flushed pending with a later wallclock; audit actual settled visibility.
            if prior['kind']=='baseline':
                first_decision=_time(first['open_at'])-timedelta(seconds=40)
                visibility=[]
                for checkpoint in c.execute('SELECT payload FROM lab_checkpoints WHERE segment_id=?',(prior['id'],)):
                    value=_unpack(checkpoint[0]).get('settled_frontier_available_at')
                    if value:visibility.append(value)
                visible=max(visibility,key=_time) if visibility else None
                if visible and _time(visible)>first_decision:
                    c.execute("UPDATE lab_series SET state='error',error='旧基线已结结果晚于下一期决策，无法安全续接' WHERE id=?",(series['id'],));continue
            if store.storage_status()['used_bytes']>=store.budget_bytes*.8:
                c.execute("UPDATE lab_series SET state='storage_paused',error='计算空间接近预算，已暂停追加并保留采集空间' WHERE id=?",(series['id'],));continue
            manifest=fingerprint(dict(stream_id=stream['id'],archive_revision=revision,after_issue=last,through_issue=through,
                predecessor=prior['id'],configuration_hash=series['configuration_hash']))
            sid=c.execute('''INSERT INTO lab_segments(series_id,sequence,predecessor_id,kind,request_id,requested_end_date,
                cutoff_issue,cutoff_at,archive_revision,manifest_hash,status,summary_json,created_at) VALUES(?,?,?,'append',?,?,?,?,?,?,'queued',?,?)''',
                (series['id'],prior['sequence']+1,prior['id'],request['id'],request['target_end_date'],through,cutoff_at,revision,manifest,
                 _dump(dict(time_complete=day_complete)),now)).lastrowid
            for batch in batches:
                before=_snapshot(c,batch)
                c.execute("INSERT INTO lab_segment_batches(segment_id,batch_id,status,baseline_json) VALUES(?,?,'queued',?)",(sid,batch['id'],_pack(before)))
                cursor=_load(before['batch']['cursor_json'])
                cursor.update(replay_complete=False,replay_max_issue=str(through),scan_after_issue=str(last),
                    replay_scheduler_version=ENGINE_VERSION,continuation_segment_id=sid)
                # Preserve cumulative evidence but begin a common per-segment input digest.
                if cursor.get('evidence') is not None:
                    cursor['prior_evidence']=cursor['evidence'];cursor['evidence']=dict(digest=manifest,count=0,warm_count=100,first_issue=None,last_issue=None,conflict=False)
                c.execute("UPDATE lab_batches SET cursor_json=?,status='paused',phase='replaying',total_issues=?,revision=revision+1,updated_at=?,error='' WHERE id=?",
                    (_dump(cursor),batch['processed_issues']+through-last,now,batch['id']))
            c.execute("UPDATE lab_continuation_requests SET status='running',segment_id=?,updated_at=? WHERE id=?",(sid,now,request['id']))
            c.execute("UPDATE lab_series SET revision=revision+1,state='running',error='',updated_at=? WHERE id=?",(now,series['id']))
            segment=c.execute('SELECT * FROM lab_segments WHERE id=?',(sid,)).fetchone()
            _start_children(store,c,series,segment,now)
            activated.append(_segment_json(segment))
    return activated


def _save_months(store,c,series,batch,now):
    from pc28touzhu.domain.rule_lab_monthly import monthly_report
    internal=store._internal_batch(batch);internal['continuation']=_summary(c,series)
    internal['end_date']=internal['continuation']['effective_end_date']
    context=work_context(c,batch)
    for experiment in c.execute('SELECT * FROM lab_experiments WHERE batch_id=?',(batch['id'],)).fetchall():
        days=[dict(r) for r in c.execute('SELECT * FROM lab_daily WHERE experiment_id=? ORDER BY stat_date LIMIT 3720',(experiment['id'],))]
        pending=c.execute("SELECT MIN(decided_at) FROM lab_decisions WHERE experiment_id=? AND status='pending'",(experiment['id'],)).fetchone()[0]
        internal['continuation']['pending_counts']={r['month']:r['n'] for r in c.execute('''SELECT
            substr(COALESCE(risk_date,date(datetime(decided_at,'+8 hours'))),1,7) AS month,COUNT(*) AS n
            FROM lab_decisions WHERE experiment_id=? AND status='pending' GROUP BY month''',(experiment['id'],))}
        observed=internal['cursor'].get('simulation_time')
        if observed:observed=_stamp(_time(observed)+timedelta(seconds=40))
        report=monthly_report(internal,_load(experiment['stats_json']),days,now=now,observed_at=observed,cutoff_at=context['cutoff_at'] if context else now,
            first_observed_at=internal['start_date']+'T00:00:00+08:00',pending_since=pending)
        for month in report['rows']:
            if month['month']>=_time(now).astimezone(SHANGHAI).strftime('%Y-%m') or not month.get('time_complete',month['complete_month']):continue
            old=c.execute('SELECT * FROM lab_monthly_snapshots WHERE series_id=? AND experiment_id=? AND month=? ORDER BY revision DESC LIMIT 1',(series['id'],experiment['id'],month['month'])).fetchone()
            # These labels describe the current whole series. Later-month gaps or
            # an in-progress comparison must not rewrite a closed month's facts.
            if old:
                old_payload=_load(old['payload_json'])
                for key in ('coverage_quality','comparable'):
                    if key in old_payload:month[key]=old_payload[key]
            elif 'comparable' in month:month['comparable']=True
            payload=_dump(month)
            if old and old['payload_json']==payload:continue
            revision=old['revision']+1 if old else 1
            c.execute('''INSERT INTO lab_monthly_snapshots(series_id,batch_id,experiment_id,month,revision,payload_json,reason,created_at)
                VALUES(?,?,?,?,?,?,?,?)''',(series['id'],batch['id'],experiment['id'],month['month'],revision,
                payload,'late_settlement' if old else 'period_closed',now))
            c.execute('DELETE FROM lab_monthly_snapshots WHERE series_id=? AND experiment_id=? AND month=? AND revision<?',
                (series['id'],experiment['id'],month['month'],revision))


def complete_segment(store,batch_id,expected_revision,*,cursor,now):
    with store._connect(write=True,reserve=True) as c:
        batch=c.execute('SELECT * FROM lab_batches WHERE id=?',(batch_id,)).fetchone()
        if not batch:raise RuleLabError('not_found','实验批次不存在',404)
        if batch['revision']!=expected_revision:return dict(committed=False,reason='stale_revision')
        series=series_for_batch(c,batch_id);context=work_context(c,batch)
        if not context or context['segment_kind']=='baseline':return dict(committed=False,reason='no_active_segment')
        segment=c.execute('SELECT * FROM lab_segments WHERE id=?',(context['segment_id'],)).fetchone()
        member=c.execute('SELECT * FROM lab_segment_batches WHERE segment_id=? AND batch_id=?',(segment['id'],batch_id)).fetchone()
        if member['status']=='completed':return dict(committed=False,reason='already_completed')
        if batch['status']!='running' or int(cursor.get('last_issue_no') or 0)!=segment['cutoff_issue']:
            return dict(committed=False,reason='segment_not_finished')
        cursor=dict(cursor,replay_complete=True)
        c.execute("UPDATE lab_batches SET cursor_json=?,status='completed',phase='idle',revision=revision+1,updated_at=? WHERE id=?",(_dump(cursor),now,batch_id))
        batch=c.execute('SELECT * FROM lab_batches WHERE id=?',(batch_id,)).fetchone()
        snapshot=_checkpoint(c,segment['id'],batch,series['configuration_hash'],now)
        summary=_segment_summary(_unpack(member['baseline_json']),snapshot)
        c.execute("UPDATE lab_segment_batches SET status='completed',summary_json=?,baseline_json=? WHERE segment_id=? AND batch_id=?",
            (_dump(summary),_pack(dict(retention_version=1,checkpoint_retained=False)),segment['id'],batch_id))
        remaining=c.execute("SELECT COUNT(*) FROM lab_segment_batches WHERE segment_id=? AND status!='completed'",(segment['id'],)).fetchone()[0]
        if not remaining:
            summaries=[_load(r[0]) for r in c.execute('SELECT summary_json FROM lab_segment_batches WHERE segment_id=? ORDER BY batch_id',(segment['id'],))]
            pending=sum(s['pending_count'] for s in summaries)
            result=dict(batches=summaries,pending_count=pending,comparable=True,
                time_complete=bool(_load(segment['summary_json']).get('time_complete')),
                coverage_quality=_summary(c,series)['coverage_quality'])
            c.execute("UPDATE lab_segments SET status='completed',summary_json=?,completed_at=? WHERE id=?",(_dump(result),now,segment['id']))
            c.execute("UPDATE lab_continuation_requests SET status='completed',updated_at=? WHERE segment_id=?",(now,segment['id']))
            c.execute("UPDATE lab_series SET state='idle',revision=revision+1,error='',updated_at=? WHERE id=?",(now,series['id']))
            completed_request=c.execute('SELECT request_id FROM lab_continuation_requests WHERE id=?',(segment['request_id'],)).fetchone() if segment['request_id'] else None
            if series['compute_enabled'] and completed_request and completed_request[0].startswith('daily-') and date.fromisoformat(segment['requested_end_date'])<_time(now).astimezone(SHANGHAI).date()-timedelta(days=1):
                c.execute('UPDATE lab_series SET next_run_at=? WHERE id=?',(now,series['id']))
            if series['owner_kind']=='study':c.execute("UPDATE lab_studies SET status='completed',updated_at=? WHERE id=?",(now,series['owner_id']))
            for row in _batches(c,series):_save_months(store,c,series,row,now)
            if segment['kind']=='replay':_next_replay(store,c,series,segment,batch,now)
        else:_start_children(store,c,series,segment,now)
        return dict(committed=True,segment=_segment_json(c.execute('SELECT * FROM lab_segments WHERE id=?',(segment['id'],)).fetchone()))


def validate_step_inputs(c,batch,draws,predictions,decisions,outcomes):
    """Continuous commits must use their frozen manifest, independent of ordinary caches."""
    from pc28touzhu.services.rule_lab_archive import read_input
    context=work_context(c,batch)
    if not context:return False
    if context['segment_kind']=='baseline' and (draws or predictions or decisions or outcomes):
        raise RuleLabError('segment_inactive','基线快照不能继续记账，请先追加新段',409)
    stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(context['stream_id'],)).fetchone()
    segment=c.execute('SELECT * FROM lab_segments WHERE id=?',(context['segment_id'],)).fetchone()
    checkpoint=c.execute('SELECT payload FROM lab_checkpoints WHERE segment_id=? AND batch_id=?',(segment['predecessor_id'],batch['id'])).fetchone()
    warm=_unpack(checkpoint[0]).get('warm_inputs',{}) if checkpoint else {}
    def saved(kind,issue):
        if str(issue) in warm:return warm[str(issue)].get(kind)
        return read_input(c,stream['source_key'],kind,0 if kind=='draw' else batch['predictor_id'],issue,context['archive_revision'])
    for kind,items in (('draw',draws),('prediction',predictions)):
        for item in items:
            value=saved(kind,int(item['issue_no']))
            if value is None or value.get('input_hash')!=item.get('input_hash'):
                raise RuleLabError('archive_conflict','输入与当前段冻结版本不一致',409)
    for decision in decisions:
        issue=int(decision['issue_no']);prediction=saved('prediction',issue)
        if issue>int(context['cutoff_issue']) or not prediction or prediction.get(batch['metric'])!=decision['bet_value']:
            raise RuleLabError('archive_conflict','决策不属于当前段冻结输入',409)
    for outcome in outcomes:
        if int(outcome['issue_no'])>int(context['cutoff_issue']) or not saved('draw',int(outcome['issue_no'])):
            raise RuleLabError('archive_conflict','结算不属于当前段冻结输入',409)
    return True


def protected_floors(c):
    floors={}
    for series in c.execute('SELECT * FROM lab_series').fetchall():
        rows=_batches(c,series);last=_last_processed(rows)
        floor=max(1,last-99) if last else 0
        stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(series['stream_id'],)).fetchone()
        if stream['seed_after_issue']<stream['seed_until_issue']:
            floor=min(floor,stream['seed_after_issue']+1) if floor else 0
        pending=c.execute('''SELECT MIN(d.issue_key) FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id
            WHERE e.batch_id IN ('''+','.join('?' for _ in rows)+") AND d.status='pending'",[r['id'] for r in rows]).fetchone()[0]
        if pending:floor=min(floor,pending) if floor else 0
        predictor=series['predictor_id'];floors[predictor]=min(floors.get(predictor,floor),floor)
    return floors


def set_active_state(store,c,series,state,now):
    if series['state']=='deleting':raise RuleLabError('deleting','实验正在删除',409)
    segment=c.execute("SELECT * FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused')",(series['id'],)).fetchone()
    if not segment:raise RuleLabError('invalid_transition','当前没有可暂停或恢复的追加段',409)
    if state=='paused':
        c.execute("UPDATE lab_segments SET status='paused' WHERE id=?",(segment['id'],))
        c.execute("UPDATE lab_segment_batches SET status='paused' WHERE segment_id=? AND status!='completed'",(segment['id'],))
        c.execute("UPDATE lab_batches SET status='paused',revision=revision+1,updated_at=? WHERE id IN (SELECT batch_id FROM lab_segment_batches WHERE segment_id=? AND status!='completed')",(now,segment['id']))
        c.execute("UPDATE lab_series SET state='paused',revision=revision+1,updated_at=? WHERE id=?",(now,series['id']))
    else:
        c.execute("UPDATE lab_segments SET status='queued' WHERE id=?",(segment['id'],))
        c.execute("UPDATE lab_segment_batches SET status='queued' WHERE segment_id=? AND status!='completed'",(segment['id'],))
        c.execute("UPDATE lab_series SET state='waiting_inputs',revision=revision+1,updated_at=? WHERE id=?",(now,series['id']))
        c.execute("UPDATE lab_batches SET status='paused',error='',revision=revision+1,updated_at=? WHERE id IN (SELECT batch_id FROM lab_segment_batches WHERE segment_id=? AND status!='completed')",(now,segment['id']))
        _start_children(store,c,series,c.execute('SELECT * FROM lab_segments WHERE id=?',(segment['id'],)).fetchone(),now)
    if series['owner_kind']=='study':
        c.execute('UPDATE lab_studies SET status=?,updated_at=? WHERE id=?',('paused' if state=='paused' else 'running',now,series['owner_id']))


def continuation_receipt(store,user_id,kind,owner_id,payload,request_kind):
    with store._connect() as c:
        _owner(store,c,user_id,kind,owner_id)
        series=c.execute('SELECT * FROM lab_series WHERE owner_kind=? AND owner_id=?',(kind,owner_id)).fetchone()
        if not series:return None
        receipt=_receipt(c,series,payload,'configure' if request_kind in ('configure','continuation') else 'append')
        if not receipt:return None
        if receipt['kind']=='configure':return dict(created=False,continuation=_summary(c,series))
        return _append_response(c,series,receipt)


def _replay_chunk(c,stream,archive_revision,start_date,target_end,target_issue):
    end=min(date.fromisoformat(target_end),date.fromisoformat(start_date)+timedelta(days=89))
    boundary=_stamp(datetime.combine(end+timedelta(days=1),datetime.min.time(),tzinfo=SHANGHAI))
    row=c.execute('''SELECT MAX(issue_key) FROM lab_input_versions WHERE source_key=? AND kind='draw'
        AND predictor_id=0 AND revision<=? AND issue_key<=? AND open_at<?''',
        (stream['source_key'],archive_revision,target_issue,boundary)).fetchone()
    if not row[0]:raise RuleLabError('archive_gap','所选重测区间缺少已保存开奖',409)
    return end.isoformat(),row[0]


def create_archive_replay(store,user_id,batch_id,experiment_id,config,*,now):
    from pc28touzhu.domain.rule_lab import empty_stats
    from pc28touzhu.services.rule_lab_management import allocate_id
    from pc28touzhu.domain.rule_lab_risk import normalize_risk_policy
    from pc28touzhu.services.rule_lab_archive import archive_revision,read_input
    with store._connect(write=True,reserve=True) as c:
        source=store._owned(c,user_id,batch_id);series=series_for_batch(c,batch_id)
        original=c.execute('SELECT * FROM lab_experiments WHERE id=? AND batch_id=?',(experiment_id,batch_id)).fetchone()
        if not original:raise RuleLabError('not_found','实验版本不存在',404)
        if source['status']=='deleting':raise RuleLabError('deleting','实验正在删除',409)
        if not series:raise RuleLabError('continuation_disabled','原实验尚未保存续算档案',409)
        if c.execute("SELECT 1 FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused')",(series['id'],)).fetchone():raise RuleLabError('segment_active','请等待当前追加完成后重测',409)
        stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(series['stream_id'],)).fetchone()
        if stream['seed_after_issue']<stream['seed_until_issue']:raise RuleLabError('archive_catching_up','原区间输入尚在保存，请完成采集后重测',409)
        store._require_space()
        if store._capacity(c)['remaining_saved']<1 or c.execute('SELECT COUNT(*) FROM lab_batches WHERE user_id=?',(user_id,)).fetchone()[0]>=50:raise RuleLabError('capacity_exceeded','保存版本或批次数量已达上限',409)
        last=_last_processed([source]);last_open=read_input(c,stream['source_key'],'draw',0,last)
        if not last_open:raise RuleLabError('archive_gap','原实验计算末期输入档案缺失',409)
        start_date=source['start_date'];target_end=_day(last_open['open_at']);revision=archive_revision(c)
        boundary=start_date+'T00:00:00+08:00';utc_start=_stamp(_time(boundary))
        first=c.execute('''SELECT MIN(issue_key) FROM lab_input_versions WHERE source_key=? AND kind='draw'
            AND predictor_id=0 AND issue_key<=? AND open_at>=? AND revision<=?''',(stream['source_key'],last,utc_start,revision)).fetchone()[0]
        if not first:raise RuleLabError('archive_gap','原实验开始日期的输入档案缺失',409)
        original_cursor=_load(source['cursor_json'])
        expected_first=original_cursor.get('warm_before_issue') or (original_cursor.get('evidence') or {}).get('first_issue')
        if expected_first and int(expected_first)!=first:
            raise RuleLabError('archive_gap','原实验起始期输入已缺失，不能缩短区间重测',409)
        warm_first=max(1,first-100)
        for warm_issue in range(warm_first,first):
            if read_input(c,stream['source_key'],'draw',0,warm_issue,revision) is None:
                raise RuleLabError('archive_gap','原实验起点之前的100期暖机开奖缺失，不能完整重测',409)
            warm_coverage=c.execute('''SELECT class FROM lab_input_coverage WHERE stream_id=? AND issue_key=?
                AND revision<=? ORDER BY revision DESC LIMIT 1''',(stream['id'],warm_issue,revision)).fetchone()
            if not warm_coverage or warm_coverage[0] not in ('present','no_prediction','invalid_prediction'):
                raise RuleLabError('archive_gap','原实验起点之前的暖机预测覆盖缺失，不能完整重测',409)
        coverage=c.execute('''SELECT COUNT(*) AS n,SUM(CASE WHEN class IN ('present','no_prediction','invalid_prediction') THEN 0 ELSE 1 END) AS bad
            FROM lab_input_coverage v WHERE stream_id=? AND issue_key>=? AND issue_key<=?
            AND revision=(SELECT MAX(q.revision) FROM lab_input_coverage q WHERE q.stream_id=v.stream_id AND q.issue_key=v.issue_key AND q.revision<=?)''',
            (stream['id'],first,last,revision)).fetchone()
        if coverage['n']!=last-first+1 or coverage['bad']:raise RuleLabError('archive_gap','原区间输入档案不完整，不能完整重测',409)
        chunk_end,chunk_issue=_replay_chunk(c,stream,revision,start_date,target_end,last)
        bid=allocate_id(c,'lab_batches')
        c.execute('''INSERT INTO lab_batches(id,user_id,name,mode,predictor_id,predictor_name,metric,template_pack,stake_cents,
            start_date,end_date,status,created_at,updated_at,risk_policy_json) VALUES(?,?,?,'backtest',?,?,?,?,?,?,?,'paused',?,?,?)''',
            (bid,user_id,config['name'],source['predictor_id'],source['predictor_name'],source['metric'],source['template_pack'],config['stake_cents'],
             start_date,chunk_end,now,now,_dump(normalize_risk_policy(config['risk_policy']))))
        c.execute('''INSERT INTO lab_experiments(id,batch_id,condition_key,name,condition_json,settlement_rule_id,settlement_snapshot_json,stats_json)
            VALUES(?,?,?,?,?,?,?,?)''',(allocate_id(c,'lab_experiments'),bid,original['condition_key'],original['name'],original['condition_json'],original['settlement_rule_id'],original['settlement_snapshot_json'],_dump(empty_stats())))
        new=c.execute('SELECT * FROM lab_batches WHERE id=?',(bid,)).fetchone();config_hash=_configuration(c,[new])
        sid=allocate_id(c,'lab_series')
        c.execute('''INSERT INTO lab_series(id,owner_kind,owner_id,user_id,predictor_id,stream_id,configuration_hash,state,created_at,updated_at,
            replay_target_end,replay_target_issue,replay_archive_revision,replay_cutoff_at) VALUES(?,'batch',?,?,?,?,?,'paused',?,?,?,?,?,?)''',
            (sid,bid,user_id,source['predictor_id'],stream['id'],config_hash,now,now,target_end,last,revision,now))
        cursor=dict(version=1,replay_scheduler_version=ENGINE_VERSION,replay_max_issue=str(chunk_issue),scan_after_issue=str(first-1),
            last_issue_no=str(first-1),history_floor_issue=str(max(1,first-100)),warm_before_issue=str(first),replay_complete=False)
        c.execute('UPDATE lab_batches SET cursor_json=?,total_issues=? WHERE id=?',(_dump(cursor),last-first+1,bid))
        new=c.execute('SELECT * FROM lab_batches WHERE id=?',(bid,)).fetchone()
        seg=c.execute('''INSERT INTO lab_segments(series_id,sequence,kind,requested_end_date,cutoff_issue,cutoff_at,archive_revision,
            manifest_hash,status,summary_json,created_at) VALUES(?,1,'replay',?,?,?,?,?,'paused',?,?)''',
            (sid,chunk_end,chunk_issue,now,revision,fingerprint(dict(replay_from=batch_id,experiment_id=experiment_id,revision=revision)),
             _dump(dict(time_complete=chunk_end<target_end,input_view='latest_captured_revision',input_notice='按创建重测时冻结的最新已保存输入重算；源修订可能与原实验不同')),now)).lastrowid
        snapshot=_snapshot(c,new)
        c.execute("INSERT INTO lab_segment_batches(segment_id,batch_id,status,baseline_json) VALUES(?,?,'paused',?)",(seg,bid,_pack(snapshot)))
        return store._batch(c,new)


def _next_replay(store,c,series,previous,batch,now):
    if not series['replay_target_issue'] or previous['cutoff_issue']>=series['replay_target_issue']:return False
    stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(series['stream_id'],)).fetchone()
    start=(date.fromisoformat(previous['requested_end_date'])+timedelta(days=1)).isoformat()
    end,cutoff=_replay_chunk(c,stream,series['replay_archive_revision'],start,series['replay_target_end'],series['replay_target_issue'])
    snapshot=_snapshot(c,batch)
    seg=c.execute('''INSERT INTO lab_segments(series_id,sequence,predecessor_id,kind,requested_end_date,cutoff_issue,cutoff_at,
        archive_revision,manifest_hash,status,summary_json,created_at) VALUES(?,?,?,'replay',?,?,?,?,?,'queued',?,?)''',
        (series['id'],previous['sequence']+1,previous['id'],end,cutoff,series['replay_cutoff_at'],series['replay_archive_revision'],
         fingerprint(dict(previous=previous['manifest_hash'],cutoff=cutoff)),_dump(dict(time_complete=end<series['replay_target_end'])),now)).lastrowid
    c.execute("INSERT INTO lab_segment_batches(segment_id,batch_id,status,baseline_json) VALUES(?,?,'queued',?)",(seg,batch['id'],_pack(snapshot)))
    cursor=_load(snapshot['batch']['cursor_json']);cursor.update(replay_complete=False,replay_max_issue=str(cutoff),continuation_segment_id=seg)
    c.execute("UPDATE lab_batches SET status='paused',cursor_json=?,revision=revision+1,updated_at=? WHERE id=?",(_dump(cursor),now,batch['id']))
    _start_children(store,c,series,c.execute('SELECT * FROM lab_segments WHERE id=?',(seg,)).fetchone(),now)
    return True
