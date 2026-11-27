"""Bounded result-only retention. Runtime dependencies outlive historical detail."""
from __future__ import annotations

from datetime import timedelta

from pc28touzhu.services.rule_lab_store import RuleLabError, _dump, _load, _time

VERSION=1
KEY='result_retention'
PHASES=('frontiers','streams','decisions','rounds','risk_days','checkpoints','segment_batches',
        'segments','monthly','coverage','rechecks','input_predictions','input_draws','predictions','draws')


def init_schema(c):
    # This is deliberately small: API initialization never scans historical data.
    c.execute('INSERT OR IGNORE INTO lab_runtime(key,value_json) VALUES(?,?)',
              (KEY,_dump(dict(mode='results_only',version=VERSION,phase='frontiers',cursor=0,
                   removed_rows=0,scanned_rows=0,error='',updated_at=None,sweep_finished_at=None))))


def status(c):
    row=c.execute('SELECT value_json FROM lab_runtime WHERE key=?',(KEY,)).fetchone()
    return _load(row[0]) if row else dict(mode='results_only',version=VERSION,phase='frontiers',
        removed_rows=0,error='',updated_at=None)


def _draw(c, issue, predictor_id=None):
    if not issue:return None
    row=c.execute('SELECT open_at,source_created_at FROM lab_draws WHERE issue_key=?',(int(issue),)).fetchone()
    if row:return dict(row)
    row=c.execute("SELECT * FROM lab_input_versions WHERE source_key='pc28-upstream' AND kind='draw' AND predictor_id=0 AND issue_key=? ORDER BY revision DESC LIMIT 1",(int(issue),)).fetchone()
    if row:
        from pc28touzhu.services.rule_lab_archive import _payload
        return _payload(row)
    return None


def update_frontier(c,batch,cursor,*,draws=(),outcomes=()):
    """Return metadata for consumed issues; never advance from a prefetched page."""
    result=dict(cursor)
    prior=_load(batch['cursor_json']).get('retention_v1') or {}
    saved={**prior,**(cursor.get('retention_v1') or {})}
    by_issue={int(d['issue_no']):d for d in draws}
    from pc28touzhu.services.rule_lab_series import work_context,_unpack
    context=work_context(c,batch)
    warm=None
    def actual_draw(issue):
        nonlocal warm
        if not context:return by_issue.get(issue) or _draw(c,issue,batch['predictor_id'])
        from pc28touzhu.services.rule_lab_archive import read_input
        if issue<=int(context['baseline_through_issue']):
            if warm is None:
                segment=c.execute('SELECT predecessor_id,status FROM lab_segments WHERE id=?',(context['segment_id'],)).fetchone()
                checkpoint_id=context['segment_id'] if segment['status']=='completed' else segment['predecessor_id'] or context['segment_id']
                checkpoint=c.execute('SELECT payload FROM lab_checkpoints WHERE segment_id=? AND batch_id=?',(checkpoint_id,batch['id'])).fetchone()
                warm=_unpack(checkpoint[0]).get('warm_inputs',{}) if checkpoint else {}
            if str(issue) in warm:return warm[str(issue)].get('draw')
            revision=context['baseline_archive_revision']
        else:revision=context['archive_revision']
        stream=c.execute('SELECT source_key FROM lab_capture_streams WHERE id=?',(context['stream_id'],)).fetchone()
        return read_input(c,stream[0],'draw',0,issue,revision) if stream else None
    last=int(cursor.get('last_issue_no') or 0)
    first=int(cursor.get('warm_before_issue') or (cursor.get('evidence') or {}).get('first_issue') or 0)
    if last:
        item=actual_draw(last)
        if item and item.get('open_at'):saved['processed_open_at']=item['open_at']
    if not saved.get('first_open_at') and first and first<=last:
        item=actual_draw(first)
        if item and item.get('open_at'):saved['first_open_at']=item['open_at']
    available=saved.get('settled_available_at')
    for outcome in outcomes:
        if outcome.get('status')!='settled':continue
        issue=int(outcome['issue_no'])
        item=actual_draw(issue)
        if item and item.get('source_created_at'):
            candidate=item['source_created_at']
            if not available or _time(candidate)>_time(available):available=candidate
    saved['settled_available_at']=available
    maximum=int(cursor.get('replay_max_issue') or 0)
    if maximum and not saved.get('replay_cutoff_at'):
        cutoff=actual_draw(maximum)
        if cutoff and cutoff.get('open_at'):saved['replay_cutoff_at']=cutoff['open_at']
    saved['frontier_verified']=bool(saved.get('first_open_at') and saved.get('processed_open_at') and
        (available or not c.execute('SELECT 1 FROM lab_experiments WHERE batch_id=? AND last_settled_issue>0 LIMIT 1',(batch['id'],)).fetchone()))
    result['retention_v1']=saved
    return result


def _backfill(c,row):
    cursor=_load(row['cursor_json'])
    if (cursor.get('retention_v1') or {}).get('frontier_verified'):return False
    updated=update_frontier(c,row,cursor)
    last=int(cursor.get('last_issue_no') or 0)
    saved=updated['retention_v1']
    # Legacy engine used this conservative visibility proof at adoption. Compute
    # it once before removing cache rows, never once per simulated decision.
    if last and not saved.get('settled_available_at'):
        missing=False;values=[];has_settlement=False
        for exp in c.execute('SELECT last_settled_issue FROM lab_experiments WHERE batch_id=?',(row['id'],)):
            if not exp[0]:continue
            has_settlement=True
            value=_draw(c,exp[0],row['predictor_id'])
            if not value or not value.get('source_created_at'):missing=True
            else:values.append(value['source_created_at'])
        historical=c.execute('SELECT MAX(source_created_at) FROM lab_draws WHERE issue_key<=?',(last,)).fetchone()[0]
        if historical and has_settlement:values.append(historical)
        latest=c.execute('SELECT payload FROM lab_checkpoints WHERE batch_id=? ORDER BY id DESC LIMIT 1',(row['id'],)).fetchone()
        if latest:
            from pc28touzhu.services.rule_lab_series import _unpack
            value=_unpack(latest[0]).get('settled_frontier_available_at')
            if value:values.append(value)
        if values and not missing:saved['settled_available_at']=max(values,key=_time)
    saved['frontier_verified']=bool(saved.get('first_open_at') and saved.get('processed_open_at') and
        (saved.get('settled_available_at') or not c.execute('SELECT 1 FROM lab_experiments WHERE batch_id=? AND last_settled_issue>0 LIMIT 1',(row['id'],)).fetchone()))
    if updated!=cursor:
        c.execute('UPDATE lab_batches SET cursor_json=? WHERE id=?',(_dump(updated),row['id']))
        return True
    return False


def dependencies(c):
    """Small current consumer state, ranges and isolated pending issue references."""
    from pc28touzhu.services.rule_lab_series import series_for_batch
    result=dict(ranges={},points={},dates={},risk_days={},checkpoints=set(),streams={},batches={})
    def interval(pid,low,high):
        if low and high and low<=high:result['ranges'].setdefault(pid,[]).append((int(low),int(high)))
    for stream in c.execute('SELECT * FROM lab_capture_streams'):
        result['streams'][stream['id']]=dict(stream)
    for row in c.execute("SELECT * FROM lab_batches WHERE status!='deleting'").fetchall():
        pid=row['predictor_id'];bid=row['id'];cursor=_load(row['cursor_json']);last=int(cursor.get('last_issue_no') or 0)
        result['batches'][bid]=dict(row)
        if last:interval(pid,max(1,last-99),last)
        series=series_for_batch(c,bid)
        if series:
            stream=result['streams'].get(series['stream_id'])
            if stream and series['capture_enabled']:
                interval(pid,last+1,stream['highest_seen_issue'])
                interval(pid,max(1,stream['after_issue']-19),stream['after_issue'])
            if series['replay_target_issue']:interval(pid,last+1,series['replay_target_issue'])
            active=c.execute("SELECT * FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused')",(series['id'],)).fetchone()
            if active:
                interval(pid,last+1,active['cutoff_issue'])
                if active['predecessor_id']:result['checkpoints'].add((active['predecessor_id'],bid))
            latest=c.execute('SELECT segment_id FROM lab_checkpoints WHERE batch_id=? ORDER BY id DESC LIMIT 1',(bid,)).fetchone()
            if latest:result['checkpoints'].add((latest[0],bid))
        if row['mode']=='backtest' and not cursor.get('replay_complete') and not series:
            maximum=int(cursor.get('replay_max_issue') or 0)
            if last and maximum:interval(pid,last+1,maximum)
            elif row['start_date'] and row['end_date']:
                # A not-yet-started replay protects only its selected interval,
                # plus the preceding feature window, rather than all cache data.
                begin=_time(row['start_date']+'T00:00:00+08:00')
                end=_time(row['end_date']+'T00:00:00+08:00')+timedelta(days=1)
                result['dates'].setdefault(pid,[]).append((begin,end))
                first=cursor.get('warm_before_issue')
                if not first:
                    found=c.execute('SELECT issue_key FROM lab_draws WHERE open_at>=? AND open_at<? ORDER BY issue_key LIMIT 1',
                        (begin.isoformat().replace('+00:00','Z'),end.isoformat().replace('+00:00','Z'))).fetchone()
                    first=found[0] if found else None
                if first:interval(pid,max(1,int(first)-100),int(first)-1)
        elif row['mode']=='live' and not last:
            highest=c.execute('SELECT MAX(issue_key) FROM lab_draws').fetchone()[0]
            if highest:interval(pid,max(1,highest-99),highest)
        pending=list(c.execute('''SELECT d.issue_key,d.risk_date,d.decided_at FROM lab_decisions d
            JOIN lab_experiments e ON e.id=d.experiment_id WHERE e.batch_id=? AND d.status='pending' ''',(bid,)))
        result['points'].setdefault(pid,set()).update(r['issue_key'] for r in pending)
        if not (cursor.get('retention_v1') or {}).get('frontier_verified'):
            points=result['points'].setdefault(pid,set())
            if cursor.get('warm_before_issue'):points.add(int(cursor['warm_before_issue']))
            points.update(r[0] for r in c.execute('SELECT last_settled_issue FROM lab_experiments WHERE batch_id=? AND last_settled_issue>0',(bid,)))
        for exp in c.execute('SELECT id,risk_runtime_json FROM lab_experiments WHERE batch_id=?',(bid,)):
            runtime=_load(exp['risk_runtime_json']);days=set()
            if runtime.get('latest_day'):days.add(runtime['latest_day'])
            if cursor.get('simulation_time'):
                from pc28touzhu.services.rule_lab_store import _day
                days.add(_day(cursor['simulation_time']))
            if runtime.get('active_round',{}):days.add(runtime['active_round'].get('start_date'))
            for pending_row in c.execute("SELECT risk_date,decided_at FROM lab_decisions WHERE experiment_id=? AND status='pending'",(exp['id'],)):
                from pc28touzhu.services.rule_lab_store import _day
                days.add(pending_row['risk_date'] or _day(pending_row['decided_at']))
            result['risk_days'][exp['id']]=days
    return result


def input_required(protection,kind,predictor_id,issue,opened=None):
    pids=set(protection['ranges'])|set(protection['points'])|set(protection['dates']) if kind=='draw' else {predictor_id}
    for pid in pids:
        if issue in protection['points'].get(pid,()):return True
        if any(low<=issue<=high for low,high in protection['ranges'].get(pid,())):return True
        if opened:
            try:value=_time(opened)
            except (ValueError,TypeError):return True
            if any(begin<=value<end for begin,end in protection['dates'].get(pid,())):return True
    return False


def _small_summary(value):
    if not isinstance(value,dict):return value
    result={k:v for k,v in value.items() if k!='experiments'}
    if 'experiments' in value:
        total={}
        for experiment in value['experiments']:
            for key,amount in experiment.get('incremental',{}).items():
                if type(amount) is int:total[key]=total.get(key,0)+amount
        result['incremental']=total
    if 'batches' in result:result['batches']=[_small_summary(v) for v in result['batches']]
    return result


def _rows(c,phase,after,limit):
    tables={'frontiers':'lab_batches','streams':'lab_capture_streams','decisions':'lab_decisions','rounds':'lab_rounds','risk_days':'lab_risk_days',
        'checkpoints':'lab_checkpoints','segment_batches':'lab_segment_batches','segments':'lab_segments',
        'monthly':'lab_monthly_snapshots','input_predictions':'lab_input_versions','input_draws':'lab_input_versions','coverage':'lab_input_coverage',
        'rechecks':'lab_input_rechecks','predictions':'lab_predictions','draws':'lab_draws'}
    table=tables[phase]
    kind=" AND kind='prediction'" if phase=='input_predictions' else " AND kind='draw'" if phase=='input_draws' else ''
    return table,c.execute('SELECT rowid AS retention_rowid,* FROM '+table+' WHERE rowid>?'+kind+' ORDER BY rowid LIMIT ?',(after,limit)).fetchall()


def _finish_phase(c,phase):
    if phase=='decisions' and not c.execute("SELECT 1 FROM lab_decisions WHERE status!='pending' LIMIT 1").fetchone():
        c.execute('DROP INDEX IF EXISTS lab_decisions_finished')
        current=c.execute("SELECT sql FROM sqlite_master WHERE type='index' AND name='lab_decisions_pending'").fetchone()
        if current and 'WHERE' not in current[0].upper():c.execute('DROP INDEX lab_decisions_pending')
        c.execute("CREATE INDEX IF NOT EXISTS lab_decisions_pending ON lab_decisions(status,issue_key) WHERE status='pending'")
    if phase=='rounds' and not c.execute("SELECT 1 FROM lab_rounds WHERE status='closed' LIMIT 1").fetchone():
        c.execute('DROP INDEX IF EXISTS lab_rounds_finished')


def _clean(c,phase,row,p,now):
    key=row['retention_rowid'];remove=False
    if phase=='frontiers':
        return int(_backfill(c,row))
    if phase=='streams':
        if row['seed_after_issue']<row['seed_until_issue']:
            c.execute('UPDATE lab_capture_streams SET seed_after_issue=seed_until_issue WHERE id=?',(row['id'],))
            return 1
        return 0
    if phase=='decisions':remove=row['status'] in ('settled','skipped')
    elif phase=='rounds':remove=row['status']=='closed'
    elif phase=='risk_days':remove=row['stat_date'] not in p['risk_days'].get(row['experiment_id'],set())
    elif phase=='checkpoints':remove=(row['segment_id'],row['batch_id']) not in p['checkpoints']
    elif phase=='segment_batches':
        if row['status']=='completed':
            from pc28touzhu.services.rule_lab_series import _pack
            marker=_pack(dict(retention_version=VERSION,checkpoint_retained=False))
            summary=_dump(_small_summary(_load(row['summary_json'])))
            if row['baseline_json']!=marker or row['summary_json']!=summary:
                c.execute('UPDATE lab_segment_batches SET baseline_json=?,summary_json=? WHERE rowid=?',(marker,summary,key));return 1
    elif phase=='segments':
        if row['status']=='completed':
            small=_dump(_small_summary(_load(row['summary_json'])))
            if small!=row['summary_json']:
                c.execute('UPDATE lab_segments SET summary_json=? WHERE id=?',(small,row['id']));return 1
    elif phase=='monthly':
        remove=bool(c.execute('SELECT 1 FROM lab_monthly_snapshots WHERE series_id=? AND experiment_id=? AND month=? AND revision>? LIMIT 1',
            (row['series_id'],row['experiment_id'],row['month'],row['revision'])).fetchone())
    elif phase in ('input_predictions','input_draws'):
        opened=row['open_at']
        if row['kind']=='prediction' and not opened:
            draw=_draw(c,row['issue_key'],row['predictor_id']);opened=draw.get('open_at') if draw else None
            if not opened and p['dates'].get(row['predictor_id']):return 0
        remove=not input_required(p,row['kind'],row['predictor_id'],row['issue_key'],opened)
    elif phase in ('coverage','rechecks'):
        stream=p['streams'].get(row['stream_id']);pid=stream['predictor_id'] if stream else -1
        draw=_draw(c,row['issue_key'],pid)
        remove=not stream or not input_required(p,'prediction',pid,row['issue_key'],draw.get('open_at') if draw else None)
    elif phase=='predictions':
        draw=_draw(c,row['issue_key'],row['predictor_id'])
        remove=not input_required(p,'prediction',row['predictor_id'],row['issue_key'],draw.get('open_at') if draw else None)
        if draw is None and p['dates'].get(row['predictor_id']):remove=False
    elif phase=='draws':remove=not input_required(p,'draw',0,row['issue_key'],row['open_at'])
    if remove:
        table={'decisions':'lab_decisions','rounds':'lab_rounds','risk_days':'lab_risk_days','checkpoints':'lab_checkpoints',
            'monthly':'lab_monthly_snapshots','input_predictions':'lab_input_versions','input_draws':'lab_input_versions','coverage':'lab_input_coverage',
            'rechecks':'lab_input_rechecks','predictions':'lab_predictions','draws':'lab_draws'}[phase]
        c.execute('DELETE FROM '+table+' WHERE rowid=?',(key,))
        return 1
    return 0


def maintain(store,*,now,limit=500):
    if type(limit) is not int or not 1<=limit<=500:raise ValueError('维护批量必须为1至500')
    processed=scanned=deleted=0
    with store._connect(write=True,reserve=True) as c:
        init_schema(c);state=status(c)
        if state['phase']=='idle':
            finished=state.get('sweep_finished_at')
            if finished and (_time(now)-_time(finished)).total_seconds()<60:
                return dict(processed_rows=0,scanned_rows=0,maintenance_pending=False,phase='idle')
            state.update(phase=PHASES[0],cursor=0)
        protection=None
        while scanned<limit:
            phase=state['phase'];table,rows=_rows(c,phase,state.get('cursor',0),limit-scanned)
            if rows:
                if phase not in ('frontiers','streams','decisions','rounds') and protection is None:protection=dependencies(c)
                if phase in ('decisions','rounds'):
                    keys=[r['retention_rowid'] for r in rows if r['status'] in (('settled','skipped') if phase=='decisions' else ('closed',))]
                    if keys:
                        c.execute('DELETE FROM '+table+' WHERE rowid IN ('+','.join('?' for _ in keys)+')',keys)
                        processed+=len(keys);deleted+=len(keys)
                    scanned+=len(rows);state['cursor']=rows[-1]['retention_rowid']
                else:
                    for row in rows:
                        changed=_clean(c,phase,row,protection,now)
                        processed+=changed
                        if phase not in ('frontiers','streams','segment_batches','segments'):deleted+=changed
                        scanned+=1;state['cursor']=row['retention_rowid']
                if scanned>=limit:break
            # A short page completed this phase. Protected rows still consume
            # scan budget, so one obsolete pending can never force a full scan.
            _finish_phase(c,phase)
            index=PHASES.index(phase)+1
            if index==len(PHASES):
                state.update(phase='idle',cursor=0,sweep_finished_at=now);break
            state.update(phase=PHASES[index],cursor=0)
        state.update(mode='results_only',version=VERSION,removed_rows=state.get('removed_rows',0)+deleted,
            scanned_rows=state.get('scanned_rows',0)+scanned,error='',updated_at=now)
        c.execute('UPDATE lab_runtime SET value_json=? WHERE key=?',(_dump(state),KEY))
        if deleted and not c.execute("SELECT 1 FROM lab_management_jobs WHERE kind='reclaim' AND status IN ('queued','running','waiting') LIMIT 1").fetchone():
            # No per-tick receipt payload: this is internal housekeeping. The
            # original management loop performs the single 256-page step.
            owner=c.execute('SELECT MIN(user_id) FROM lab_batches').fetchone()[0] or 1
            existing=c.execute("SELECT id FROM lab_management_jobs WHERE kind='reclaim' AND owner_kind='retention' ORDER BY id LIMIT 1").fetchone()
            if existing:
                c.execute("UPDATE lab_management_jobs SET status='queued',payload_json='{}',freed_bytes=0,error='',updated_at=? WHERE id=?",(now,existing[0]))
            else:
                c.execute("""INSERT INTO lab_management_jobs(user_id,kind,owner_kind,owner_id,payload_json,created_at,updated_at)
                    VALUES(?,'reclaim','retention',0,'{}',?,?)""",(owner,now,now))
    return dict(processed_rows=processed,scanned_rows=scanned,maintenance_pending=state['phase']!='idle',phase=state['phase'])


def replay_input_preview(store,user_id,batch_id,experiment_id,*,now):
    with store._connect() as c:
        c.execute('BEGIN')
        row=store._owned(c,user_id,batch_id)
        if not c.execute('SELECT 1 FROM lab_experiments WHERE id=? AND batch_id=?',(experiment_id,batch_id)).fetchone():
            raise RuleLabError('not_found','实验版本不存在',404)
        cursor=_load(row['cursor_json']);last=int(cursor.get('last_issue_no') or 0)
        end=(cursor.get('retention_v1') or {}).get('processed_open_at')
        if not end and last:
            item=_draw(c,last,row['predictor_id']);end=item.get('open_at') if item else None
        from pc28touzhu.services.rule_lab_store import _day
        requested=dict(start_date=row['start_date'],end_date=_day(end) if end else row['end_date'])
        result=dict(available=False,reason='原区间输入未完整保留，需要核对当前来源',requested_range=requested,
                    verified_range=None,input_basis='unavailable')
        if row['mode']!='backtest':return dict(result,reason='只有历史实验支持金额重测')
        from pc28touzhu.services.rule_lab_series import series_for_batch
        series=series_for_batch(c,batch_id)
        if not series:return result
        if c.execute("SELECT 1 FROM lab_segments WHERE series_id=? AND status IN ('queued','running','paused')",(series['id'],)).fetchone():
            return dict(result,reason='请等待当前追加完成后重测')
        stream=c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(series['stream_id'],)).fetchone()
        first=int(cursor.get('warm_before_issue') or (cursor.get('evidence') or {}).get('first_issue') or 0)
        if not first or not last or not stream:return result
        low=max(1,first-100);predictor_id=series['predictor_id']
        for table,where,parameters in (
            ('lab_input_versions',"source_key=? AND kind='draw' AND predictor_id=0",[stream['source_key']]),
            ('lab_input_coverage','stream_id=?',[stream['id']])):
            row=c.execute('SELECT COUNT(DISTINCT issue_key) FROM '+table+' WHERE '+where+' AND issue_key BETWEEN ? AND ?',parameters+[low,last]).fetchone()
            if row[0]!=last-low+1:return result
        bad=c.execute("""SELECT 1 FROM lab_input_coverage v WHERE stream_id=? AND issue_key BETWEEN ? AND ?
            AND revision=(SELECT MAX(q.revision) FROM lab_input_coverage q WHERE q.stream_id=v.stream_id AND q.issue_key=v.issue_key)
            AND class NOT IN ('present','no_prediction','invalid_prediction') LIMIT 1""",(stream['id'],low,last)).fetchone()
        if bad:return result
        missing_payload=c.execute("""SELECT 1 FROM lab_input_coverage v
            WHERE v.stream_id=? AND v.issue_key BETWEEN ? AND ? AND v.class='present'
            AND v.revision=(SELECT MAX(q.revision) FROM lab_input_coverage q
                WHERE q.stream_id=v.stream_id AND q.issue_key=v.issue_key)
            AND NOT EXISTS(SELECT 1 FROM lab_input_versions p WHERE p.source_key=?
                AND p.kind='prediction' AND p.predictor_id=? AND p.issue_key=v.issue_key
                AND (p.payload_json IS NOT NULL OR p.payload_blob IS NOT NULL)) LIMIT 1""",
            (stream['id'],low,last,stream['source_key'],predictor_id)).fetchone()
        if missing_payload:return result
        return dict(result,available=True,reason='',verified_range=requested,input_basis='local_frozen')
