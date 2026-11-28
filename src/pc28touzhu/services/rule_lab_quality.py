"""Bounded read-only pages of currently retained source data, never replay proof."""
from __future__ import annotations

import base64,hashlib,hmac,json,os
from datetime import date,datetime,timedelta,timezone
from pc28touzhu.domain.rule_lab import METRICS,VALUES
from pc28touzhu.services.rule_lab_store import RuleLabError
from pc28touzhu.services.rule_lab_source import _PUBLIC,_DRAW_COLUMNS,_utc,_issue,SHANGHAI
from pc28touzhu.services.rule_lab_templates import validate_request,fingerprint

COUNT_KEYS=('draw_rows','valid_draws','missing_draw_issues','invalid_draws','present','on_time','late','invalid_prediction','confirmed_no_prediction','unknown_or_archived','waiting','conflicting_prediction')


def _encoded(value):return base64.urlsafe_b64encode(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).decode().rstrip('=')


def _sign(source,value):
    if not hasattr(source,'_quality_cursor_key'):source._quality_cursor_key=os.urandom(32)
    body=_encoded(value);return body+'.'+hmac.new(source._quality_cursor_key,body.encode(),hashlib.sha256).hexdigest()


def _decode(source,value):
    if not isinstance(value,str) or not 1<=len(value)<=4096:raise RuleLabError('invalid_input','检查分页标识无效')
    try:
        body,signature=value.split('.')
        if not hasattr(source,'_quality_cursor_key') or not hmac.compare_digest(hmac.new(source._quality_cursor_key,body.encode(),hashlib.sha256).hexdigest(),signature):
            raise ValueError()
        result=json.loads(base64.urlsafe_b64decode(body+'='*((-len(body))%4)))
        if not isinstance(result,dict):raise ValueError()
        return result
    except (ValueError,UnicodeError) as exc:raise RuleLabError('invalid_input','检查分页标识无效或服务已重启，请重新检查') from exc


def page(source,payload,*,now):
    validate_request(payload,('predictor_id','metric','start_date','end_date','cursor','limit'))
    pid=payload['predictor_id'];metric=payload['metric'];limit=payload['limit']
    if type(pid) is not int or pid<=0 or metric not in METRICS or type(limit) is not int or not 1<=limit<=200:
        raise RuleLabError('invalid_input','来源、玩法或分页大小无效（每页最多200期）')
    try:
        start=date.fromisoformat(payload['start_date']);end=date.fromisoformat(payload['end_date'])
        if start.isoformat()!=payload['start_date'] or end.isoformat()!=payload['end_date'] or not 0<=(end-start).days<366 or end>datetime.fromisoformat(_utc(now).replace('Z','+00:00')).astimezone(SHANGHAI).date():raise ValueError()
    except (ValueError,TypeError):raise RuleLabError('invalid_input','请选择不超过366天且不晚于今天的日期范围')
    scope=fingerprint({k:payload[k] for k in ('predictor_id','metric','start_date','end_date')});cursor=_decode(source,payload['cursor']) if payload['cursor'] is not None else None
    if cursor and cursor.get('scope')!=scope:raise RuleLabError('stale_revision','检查范围已变化，请重新检查',409)
    observed=cursor['observed_at'] if cursor else _utc(now);reference=datetime.fromisoformat(observed.replace('Z','+00:00'))
    if cursor:
        first=cursor['first_issue'];stop=cursor['cutoff_issue'];after=cursor['after_issue']
    else:
        # Indexed seeks have bounded probes. Actual selected rows and both
        # boundaries are verified again in the page's read transaction below.
        first=source.find_start_issue(start.isoformat(),observed_at=observed)
        after=None;stop_start=source.find_start_issue((end+timedelta(days=1)).isoformat(),observed_at=observed)
        stop=int(stop_start)-1 if stop_start else None
    counts=dict.fromkeys(COUNT_KEYS,0);days={};examples=[];warnings=[]
    def example(issue,stamp,category,message):
        if len(examples)<20:examples.append(dict(issue_no=issue,date=stamp,category=category,message=message))
    with source._read() as (c,deadline):
        c.execute('BEGIN');query=lambda sql,params=():source._query(c,deadline,sql,params)
        owned=query('SELECT id,name FROM predictors WHERE id=? AND '+_PUBLIC,(pid,))
        if not owned:raise RuleLabError('source_forbidden','来源已停用或不再共享逐期预测',403)
        columns={r['name'] for r in query('PRAGMA table_info(predictors)')};created=query('SELECT created_at FROM predictors WHERE id=?',(pid,))[0][0] if 'created_at' in columns else None
        identity=fingerprint(dict(predictor_id=pid,created_at=created,file_identity=source.db_path.stat().st_ino,path_hash=hashlib.sha256(str(source.db_path).encode()).hexdigest()))
        if cursor and identity!=cursor['source_identity']:raise RuleLabError('stale_revision','来源身份已变化，请重新检查',409)
        latest=query("SELECT issue_no FROM lottery_draws WHERE lottery_type='pc28' ORDER BY issue_no DESC LIMIT 1")
        highest=int(_issue(latest[0][0])) if latest else 0
        if stop is None:stop=highest
        if first is None:first=str(highest+1)
        first=int(_issue(first));after=first-1 if after is None else int(_issue(after))
        if cursor and not first-1<=after<=stop:raise RuleLabError('invalid_input','检查分页位置超出固定范围')
        # Boundaries require surviving evidence on both sides, not a guessed
        # 210-second schedule. An empty/truncated edge remains unknown.
        prior=query('SELECT '+_DRAW_COLUMNS+" FROM lottery_draws WHERE lottery_type='pc28' AND issue_no<? ORDER BY issue_no DESC LIMIT 1",(str(first),))
        following=query('SELECT '+_DRAW_COLUMNS+" FROM lottery_draws WHERE lottery_type='pc28' AND issue_no>? ORDER BY issue_no LIMIT 1",(str(stop),))
        def boundary_ok(rows,before):
            if not rows:return False
            try:
                value=source._draw(rows[0],observed);day=datetime.fromisoformat(value['open_at'].replace('Z','+00:00')).astimezone(SHANGHAI).date()
                visible=max(datetime.fromisoformat(value[k].replace('Z','+00:00')) for k in ('open_at','source_created_at'))<=reference
                return visible and (day<start and int(value['issue_no'])+1==first if before else day>end and int(value['issue_no'])==stop+1)
            except (ValueError,TypeError,KeyError):return False
        boundary=dict(start_verified=boundary_ok(prior,True),end_verified=boundary_ok(following,False))
        if cursor:
            # Loss of boundary evidence after a prior page must not silently
            # upgrade the aggregate scan's completeness.
            boundary={key:bool(boundary[key] and cursor['boundary'][key]) for key in boundary}
        rows=query('SELECT '+_DRAW_COLUMNS+" FROM lottery_draws WHERE lottery_type='pc28' AND issue_no>? AND issue_no<=? ORDER BY issue_no LIMIT ?",(str(after).zfill(len(str(highest))),str(stop).zfill(len(str(highest))),limit+1)) if stop>=first else []
        selected=rows[:limit];has_more=len(rows)>limit
        issues=[str(row['issue_no']) for row in selected]
        predictions=query('''SELECT issue_no,prediction_big_small,prediction_odd_even,prediction_combo,status,created_at
            FROM predictions WHERE predictor_id=? AND lottery_type='pc28' AND issue_no IN ('''+','.join('?' for _ in issues)+')',[pid]+issues) if issues else []
        by_issue={}
        for prediction in predictions:by_issue.setdefault(str(prediction['issue_no']),[]).append(prediction)
        metadata=bool(query("SELECT 1 FROM sqlite_master WHERE type='table' AND name='pc28_prediction_daily_summary'"))
        archived=query('SELECT MAX(summary_date) FROM pc28_prediction_daily_summary WHERE predictor_id=?',(pid,))[0][0] if metadata else None
        uncertain=(date.fromisoformat(archived)+timedelta(days=1)).isoformat() if archived else None
        previous=after
        for raw in selected:
            issue=_issue(raw['issue_no']);number=int(issue);counts['draw_rows']+=1
            if number>previous+1:
                counts['missing_draw_issues']+=number-previous-1;example(str(previous+1),None,'missing_draw','缺少开奖期号至'+str(number-1))
            previous=number;stamp=None;daily=None
            try:
                stamp=datetime.fromisoformat(_utc(raw['open_time'],SHANGHAI).replace('Z','+00:00')).astimezone(SHANGHAI).date().isoformat()
                if start.isoformat()<=stamp<=end.isoformat():
                    daily=days.setdefault(stamp,dict(date=stamp,**dict.fromkeys(COUNT_KEYS,0)));daily['draw_rows']+=1
            except (ValueError,TypeError):pass
            try:
                draw=source._draw(raw,observed);opened=datetime.fromisoformat(draw['open_at'].replace('Z','+00:00'));stamp=opened.astimezone(SHANGHAI).date().isoformat()
                if not start.isoformat()<=stamp<=end.isoformat():raise RuleLabError('source_changed','区间内开奖日期发生变化，请重新检查',409)
            except (ValueError,TypeError,KeyError):
                counts['invalid_draws']+=1
                if daily is not None:daily['invalid_draws']+=1
                example(issue,stamp,'invalid_draw','开奖三球、和值或时间无效');continue
            daily=days.setdefault(stamp,dict(date=stamp,**dict.fromkeys(COUNT_KEYS,0)))
            counts['valid_draws']+=1;daily['valid_draws']+=1
            category=None
            if max(opened,datetime.fromisoformat(draw['source_created_at'].replace('Z','+00:00')))>reference:category='waiting'
            found=by_issue.get(issue,[])
            if category is None:
                if len(found)>1:category='conflicting_prediction'
                elif not found:category='unknown_or_archived' if not metadata or uncertain and stamp<=uncertain else 'confirmed_no_prediction'
                else:
                    pred=found[0]
                    try:
                        generated=datetime.fromisoformat(_utc(pred['created_at']).replace('Z','+00:00'))
                        if pred['status'] not in ('pending','settled') or pred['prediction_'+metric] not in VALUES[metric]:raise ValueError()
                        category='waiting' if generated>reference else 'late' if generated>opened-timedelta(seconds=40) else 'on_time'
                        if category!='waiting':counts['present']+=1;daily['present']+=1
                    except (ValueError,TypeError):category='invalid_prediction'
            counts[category]+=1;daily[category]+=1
            if category!='on_time':example(issue,stamp,category,{'late':'晚于开奖前40秒决策截止','invalid_prediction':'预测方向或时间无效','unknown_or_archived':'缺行可能已归档，不能确认原本无预测','confirmed_no_prediction':'保留范围内确认无预测','waiting':'首次检查时间尚不可见','conflicting_prediction':'同一期存在重复预测'}.get(category,category))
        if not has_more and stop>=first and previous<stop:
            counts['missing_draw_issues']+=stop-previous;example(str(previous+1),None,'missing_draw','固定截止前缺少开奖至'+str(stop))
    uncertain_count=sum(counts[k] for k in ('unknown_or_archived','invalid_prediction','invalid_draws','missing_draw_issues','waiting','conflicting_prediction'))
    if uncertain_count:warnings.append(dict(code='unverified_absence',message='存在归档、无效、等待或缺档证据，不能给出完整缺失率'))
    if not all(boundary.values()):warnings.append(dict(code='unverified_boundary',message='范围边界缺少保留证据，不能确认完整历史'))
    warnings.append(dict(code='paged_current_source',message='当前保留数据的分批检查，源数据可能更新；不代表旧实验冻结输入'))
    complete=not has_more;next_cursor=None
    if has_more:
        next_cursor=_sign(source,dict(scope=scope,first_issue=first,cutoff_issue=stop,after_issue=previous,observed_at=observed,source_identity=identity,boundary=boundary))
    denominator=counts['valid_draws'];verified=not uncertain_count and all(boundary.values()) and bool(denominator)
    return dict(predictor=dict(id=pid,name=str(owned[0]['name'])[:200]),metric=metric,range=dict(start_date=start.isoformat(),end_date=end.isoformat()),
        scan=dict(observed_at=observed,read_at=_utc(now),cutoff_issue=str(stop) if stop else None,first_issue=issues[0] if issues else None,last_issue=issues[-1] if issues else None,
            scanned_issues=len(selected),next_cursor=next_cursor,complete=complete,source_identity=identity),counts=counts,
        rates=dict(prediction_coverage_percent=round(counts['present']/denominator*100,6) if denominator else None,
            confirmed_missing_percent=round(counts['confirmed_no_prediction']/denominator*100,6) if verified else None),
        days=list(days.values()),examples=examples,warnings=warnings,boundary=boundary)
