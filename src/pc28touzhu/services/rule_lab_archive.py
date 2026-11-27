"""Versioned, bounded PC28 inputs owned by the independent simulation ledger."""
from __future__ import annotations

import hashlib
import json
import zlib
from datetime import datetime, timedelta, timezone

from pc28touzhu.services.rule_lab_store import RuleLabError

UTC = timezone.utc
SHANGHAI = timezone(timedelta(hours=8))
COMPLETE_CLASSES = frozenset(('present', 'no_prediction', 'invalid_prediction'))
COVERAGE_CLASSES = COMPLETE_CLASSES | frozenset(('waiting', 'archive_gap'))


def _dump(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'), allow_nan=False)


def _time(value):
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('档案时间必须包含时区')
    return parsed.astimezone(UTC)


def _issue(value, *, zero=False):
    text = str(value)
    if not text.isascii() or not text.isdigit() or str(int(text)) != text or not (0 if zero else 1) <= int(text) < 2**63:
        raise ValueError('档案期号无效')
    return int(text)


def _limit(value, maximum=200):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError('档案读取批量无效')
    return value


def init_schema(c):
    c.execute('''CREATE TABLE IF NOT EXISTS lab_capture_streams (
        id INTEGER PRIMARY KEY,source_key TEXT NOT NULL,predictor_id INTEGER NOT NULL,
        source_identity TEXT NOT NULL DEFAULT '',revision INTEGER NOT NULL DEFAULT 0,
        after_issue INTEGER NOT NULL DEFAULT 0,highest_seen_issue INTEGER NOT NULL DEFAULT 0,
        covered_through_issue INTEGER NOT NULL DEFAULT 0,last_success_at TEXT,
        state TEXT NOT NULL DEFAULT 'idle',error TEXT NOT NULL DEFAULT '',
        seed_after_issue INTEGER NOT NULL DEFAULT 0,seed_until_issue INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,updated_at TEXT NOT NULL,UNIQUE(source_key,predictor_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS lab_archive_revisions (
        id INTEGER PRIMARY KEY,observed_at TEXT NOT NULL)''')
    c.execute('''CREATE TABLE IF NOT EXISTS lab_input_versions (
        id INTEGER PRIMARY KEY,source_key TEXT NOT NULL,kind TEXT NOT NULL,predictor_id INTEGER NOT NULL,
        issue_key INTEGER NOT NULL,revision INTEGER NOT NULL REFERENCES lab_archive_revisions(id),
        content_hash TEXT NOT NULL,payload_hash TEXT NOT NULL,payload_json TEXT,payload_blob BLOB,
        observed_at TEXT NOT NULL,open_at TEXT,source_created_at TEXT,
        UNIQUE(source_key,kind,predictor_id,issue_key,revision))''')
    c.execute('''CREATE INDEX IF NOT EXISTS lab_input_versions_lookup
        ON lab_input_versions(source_key,kind,predictor_id,issue_key,revision DESC)''')
    c.execute('''CREATE INDEX IF NOT EXISTS lab_input_versions_age ON lab_input_versions(observed_at,issue_key)''')
    c.execute('''CREATE TABLE IF NOT EXISTS lab_input_coverage (
        stream_id INTEGER NOT NULL REFERENCES lab_capture_streams(id),issue_key INTEGER NOT NULL,
        revision INTEGER NOT NULL REFERENCES lab_archive_revisions(id),class TEXT NOT NULL,
        checked_at TEXT NOT NULL,details_json TEXT NOT NULL,
        PRIMARY KEY(stream_id,issue_key,revision))''')
    columns={r[1] for r in c.execute('PRAGMA table_info(lab_capture_streams)')}
    if 'recheck_after_issue' not in columns:
        c.execute('ALTER TABLE lab_capture_streams ADD COLUMN recheck_after_issue INTEGER NOT NULL DEFAULT 0')
    if 'pending_recheck_after_issue' not in columns:
        c.execute('ALTER TABLE lab_capture_streams ADD COLUMN pending_recheck_after_issue INTEGER NOT NULL DEFAULT 0')
    c.execute('''CREATE TABLE IF NOT EXISTS lab_input_rechecks (
        stream_id INTEGER NOT NULL REFERENCES lab_capture_streams(id),issue_key INTEGER NOT NULL,
        PRIMARY KEY(stream_id,issue_key))''')


def archive_revision(c):
    return int(c.execute('SELECT COALESCE(MAX(id),0) FROM lab_archive_revisions').fetchone()[0])


def _new_revision(c, now):
    from pc28touzhu.services.rule_lab_management import allocate_id
    _time(now)
    value=allocate_id(c,'lab_archive_revisions')
    c.execute('INSERT INTO lab_archive_revisions(id,observed_at) VALUES(?,?)',(value,now))
    return value


def _payload(row):
    if row is None:
        return None
    try:
        raw = row['payload_json'].encode() if row['payload_json'] is not None else zlib.decompress(row['payload_blob'])
        if len(raw) > 32768 or hashlib.sha256(raw).hexdigest() != row['payload_hash']:
            raise ValueError('corrupt_payload')
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError('invalid_payload')
        return value
    except (ValueError, TypeError, zlib.error, UnicodeError) as exc:
        raise RuleLabError('archive_corrupt', '实验输入档案校验失败，已保留成绩并停止续算', 409) from exc


def read_input(c, source_key, kind, predictor_id, issue_key, revision=None):
    revision = archive_revision(c) if revision is None else int(revision)
    row = c.execute('''SELECT * FROM lab_input_versions WHERE source_key=? AND kind=? AND predictor_id=?
        AND issue_key=? AND revision<=? ORDER BY revision DESC LIMIT 1''',
        (source_key,kind,int(predictor_id),int(issue_key),revision)).fetchone()
    return _payload(row)


def coverage_rows(c, stream_id, after_issue, through_issue, revision=None, *, limit=10000):
    """Bounded current coverage rows; callers verify the requested interval."""
    if type(limit) is not int or not 1 <= limit <= 40000:
        raise ValueError('覆盖读取范围无效')
    revision = archive_revision(c) if revision is None else int(revision)
    rows = c.execute('''SELECT v.* FROM lab_input_coverage v WHERE stream_id=? AND issue_key>? AND issue_key<=?
        AND revision=(SELECT MAX(p.revision) FROM lab_input_coverage p WHERE p.stream_id=v.stream_id
            AND p.issue_key=v.issue_key AND p.revision<=?) ORDER BY issue_key LIMIT ?''',
        (stream_id,int(after_issue),int(through_issue),revision,limit)).fetchall()
    return [dict(r, issue_no=str(r['issue_key']), details=json.loads(r['details_json'])) for r in rows]


def _put_input(c, stream, kind, value, revision, now):
    value = dict(value)
    issue = _issue(value['issue_no'])
    predictor = 0 if kind == 'draw' else int(value['predictor_id'])
    if kind == 'prediction' and predictor != stream['predictor_id']:
        raise ValueError('档案预测来源不匹配')
    value['issue_no'] = str(issue)
    value.setdefault('observed_at', now)
    raw = _dump(value).encode()
    if len(raw) > 32768:
        raise ValueError('档案单条输入过大')
    content = {k:v for k,v in value.items() if k != 'observed_at'}
    content_hash = hashlib.sha256(_dump(content).encode()).hexdigest()
    previous = c.execute('''SELECT content_hash FROM lab_input_versions WHERE source_key=? AND kind=?
        AND predictor_id=? AND issue_key=? ORDER BY revision DESC LIMIT 1''',
        (stream['source_key'],kind,predictor,issue)).fetchone()
    if previous and previous[0] == content_hash:
        return
    c.execute('''INSERT INTO lab_input_versions(source_key,kind,predictor_id,issue_key,revision,content_hash,
        payload_hash,payload_json,observed_at,open_at,source_created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
        (stream['source_key'],kind,predictor,issue,revision,content_hash,hashlib.sha256(raw).hexdigest(),raw.decode(),
         value['observed_at'],value.get('open_at'),value.get('source_created_at',value.get('created_at'))))


def _put_coverage(c, stream_id, entry, revision, now):
    issue = _issue(entry.get('issue_no', entry.get('issue_key')))
    category = entry.get('class')
    if category not in COVERAGE_CLASSES:
        raise ValueError('档案覆盖分类无效')
    details = _dump(entry.get('details') or {})
    if len(details.encode()) > 4096:
        raise ValueError('覆盖证据过大')
    old = c.execute('''SELECT class,details_json FROM lab_input_coverage WHERE stream_id=? AND issue_key=?
        ORDER BY revision DESC LIMIT 1''',(stream_id,issue)).fetchone()
    if old and old['class'] == category and old['details_json'] == details:
        return
    c.execute('''INSERT INTO lab_input_coverage(stream_id,issue_key,revision,class,checked_at,details_json)
        VALUES(?,?,?,?,?,?)''',(stream_id,issue,revision,category,now,details))


def _save_page(c, stream, draws, predictions, coverage, now):
    revision = _new_revision(c,now)
    for row in draws:
        _put_input(c,stream,'draw',row,revision,now)
    for row in predictions:
        _put_input(c,stream,'prediction',row,revision,now)
        if row.get('status')=='pending' or not row.get('settled_at'):
            c.execute('INSERT OR IGNORE INTO lab_input_rechecks(stream_id,issue_key) VALUES(?,?)',(stream['id'],int(row['issue_no'])))
        else:c.execute('DELETE FROM lab_input_rechecks WHERE stream_id=? AND issue_key=?',(stream['id'],int(row['issue_no'])))
    for row in coverage:
        _put_coverage(c,stream['id'],row,revision,now)
    return revision


def _cached_page(c, stream, first, last, *, limit):
    rows = c.execute('''SELECT * FROM lab_draws WHERE issue_key>=? AND issue_key<=?
        ORDER BY issue_key LIMIT ?''',(first,last,limit)).fetchall()
    draws, predictions, coverage = [], [], []
    for row in rows:
        draw = dict(row)
        draw['triplet'] = json.loads(draw.pop('triplet_json'))
        draw.pop('issue_key',None)
        draws.append(draw)
        prediction = c.execute('SELECT * FROM lab_predictions WHERE predictor_id=? AND issue_key=?',
            (stream['predictor_id'],row['issue_key'])).fetchone()
        if prediction:
            value = dict(prediction);value.pop('issue_key',None);predictions.append(value)
        category = 'archive_gap' if row['conflict'] or (prediction and prediction['conflict']) else 'present' if prediction else 'waiting'
        coverage.append(dict(issue_no=row['issue_no'], **{'class':category}, details={'origin':'legacy_cache',
            'reason':'cached_input' if prediction else 'missing_cache_needs_source_check'}))
    return draws,predictions,coverage


def ensure_stream(c, predictor_id, last_issue, now, history_start_issue=None, source_key='pc28-upstream'):
    if type(predictor_id) is not int or predictor_id <= 0:
        raise ValueError('预测来源无效')
    last = _issue(last_issue,zero=True)
    # A completed history is a result, not a seed for another history archive.
    first=max(1,last-99)
    from pc28touzhu.services.rule_lab_management import allocate_id
    if not c.execute('SELECT 1 FROM lab_capture_streams WHERE source_key=? AND predictor_id=?',(source_key,predictor_id)).fetchone():
        c.execute('''INSERT INTO lab_capture_streams(id,source_key,predictor_id,after_issue,highest_seen_issue,
            covered_through_issue,seed_after_issue,seed_until_issue,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)''',
            (allocate_id(c,'lab_capture_streams'),source_key,predictor_id,last,last,last,last,last,now,now))
    stream = dict(c.execute('SELECT * FROM lab_capture_streams WHERE source_key=? AND predictor_id=?',
        (source_key,predictor_id)).fetchone())
    # Only the boundary feature window is copied. New input is captured forward.
    draws,predictions,coverage = _cached_page(c,stream,max(1,last-99),last,limit=100)
    if draws:
        _save_page(c,stream,draws,predictions,coverage,now)
    c.execute('''UPDATE lab_capture_streams SET seed_after_issue=MAX(seed_until_issue,?),
        seed_until_issue=MAX(seed_until_issue,?),highest_seen_issue=MAX(highest_seen_issue,?),updated_at=? WHERE id=?''',
        (last,last,last,now,stream['id']))
    stream = dict(c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(stream['id'],)).fetchone())
    stream['archive_revision'] = archive_revision(c)
    return stream


def seed_cache_page(c, stream_id, *, limit=200, now):
    limit = _limit(limit)
    stream = c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(stream_id,)).fetchone()
    if stream is None:
        raise RuleLabError('not_found','输入采集来源不存在',404)
    if stream['seed_after_issue']<stream['seed_until_issue']:
        c.execute('UPDATE lab_capture_streams SET seed_after_issue=seed_until_issue,updated_at=? WHERE id=?',(now,stream_id))
    return dict(seeded=0,done=True,archive_revision=archive_revision(c))


def _advance_coverage(c, stream_id, current, through):
    # Each page is bounded. A repaired early gap can uncover previously saved pages.
    rows = coverage_rows(c,stream_id,current,through,limit=40000)
    result = current
    for row in rows:
        if row['issue_key'] != result+1 or row['class'] not in COMPLETE_CLASSES:
            break
        result = row['issue_key']
    return result


def commit_capture(store, stream_id, expected_revision, *, draws, predictions, coverage, next_issue, now,
                   highest_seen_issue=None, source_identity=None):
    if len(draws)>200 or len(predictions)>200 or len(coverage)>400:
        raise ValueError('单次档案采集超过批量限制')
    store._require_space(reserve=True)
    with store._connect(write=True,reserve=True) as c:
        stream = c.execute('SELECT * FROM lab_capture_streams WHERE id=?',(stream_id,)).fetchone()
        if stream is None:
            raise RuleLabError('not_found','输入采集来源不存在',404)
        c.execute("INSERT INTO lab_runtime(key,value_json) VALUES('capture_last_stream',?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",(json.dumps(stream_id),))
        if stream['revision'] != expected_revision:
            return dict(committed=False,reason='stale_revision',revision=stream['revision'],archive_revision=archive_revision(c),covered_through_issue=stream['covered_through_issue'])
        identity = source_identity or stream['source_identity']
        if stream['source_identity'] and identity != stream['source_identity']:
            raise RuleLabError('source_changed','预测来源身份发生变化，不能合并到原输入档案',409)
        if c.execute('SELECT 1 FROM lab_series WHERE stream_id=? LIMIT 1',(stream_id,)).fetchone():
            from pc28touzhu.services.rule_lab_retention import dependencies,input_required
            protection=dependencies(c)
            opened={int(value['issue_no']):value.get('open_at') for value in draws}
            def needed(value,kind):
                issue=int(value['issue_no'])
                return issue>stream['after_issue'] or input_required(protection,kind,stream['predictor_id'],issue,opened.get(issue))
            draws=[value for value in draws if needed(value,'draw')]
            predictions=[value for value in predictions if needed(value,'prediction')]
            coverage=[value for value in coverage if needed(value,'prediction')]
        revision = _save_page(c,stream,draws,predictions,coverage,now)
        scanned_issues={int(row['issue_no']) for row in draws}
        attempted_pending=c.execute('''SELECT issue_key FROM lab_input_rechecks WHERE stream_id=?
            ORDER BY CASE WHEN issue_key>? THEN 0 ELSE 1 END,issue_key LIMIT 1''',
            (stream_id,stream['pending_recheck_after_issue'])).fetchone()
        if attempted_pending and attempted_pending[0] in scanned_issues:
            c.execute('UPDATE lab_capture_streams SET pending_recheck_after_issue=? WHERE id=?',(attempted_pending[0],stream_id))
        after = max(stream['after_issue'],_issue(next_issue,zero=True) if next_issue not in (None,'') else stream['after_issue'])
        highest = max(stream['highest_seen_issue'],_issue(highest_seen_issue,zero=True) if highest_seen_issue is not None else after)
        covered = _advance_coverage(c,stream_id,stream['covered_through_issue'],after)
        scanned=_issue(next_issue,zero=True) if next_issue not in (None,'') else stream['after_issue']
        recheck=stream['recheck_after_issue']
        if scanned<=stream['after_issue']:
            recheck=scanned if scanned<stream['after_issue'] else max(0,after-20)
        gap = c.execute('''SELECT x.class FROM lab_input_coverage x WHERE stream_id=? AND issue_key>?
            AND revision=(SELECT MAX(p.revision) FROM lab_input_coverage p WHERE p.stream_id=x.stream_id AND p.issue_key=x.issue_key)
            AND class NOT IN ('present','no_prediction','invalid_prediction') ORDER BY issue_key LIMIT 1''',
            (stream_id,covered)).fetchone()
        state = 'archive_gap' if gap and gap[0]=='archive_gap' else 'waiting_inputs' if gap else 'idle'
        error = '部分所需输入缺失，无法声明连续覆盖' if state=='archive_gap' else ''
        c.execute('''UPDATE lab_capture_streams SET revision=revision+1,after_issue=?,highest_seen_issue=?,
            covered_through_issue=?,source_identity=?,last_success_at=?,updated_at=?,state=?,error=?,recheck_after_issue=? WHERE id=?''',
            (after,highest,covered,identity,now,now,state,error,recheck,stream_id))
        return dict(committed=True,revision=expected_revision+1,archive_revision=revision,covered_through_issue=covered)


def capture_work_items(store, *, now, limit=10):
    limit = _limit(limit,50)
    with store._connect() as c:
        saved=c.execute("SELECT value_json FROM lab_runtime WHERE key='capture_last_stream'").fetchone()
        previous=int(json.loads(saved[0])) if saved else 0
        rows = c.execute('''SELECT s.* FROM lab_capture_streams s WHERE EXISTS (
            SELECT 1 FROM lab_series r WHERE r.stream_id=s.id AND r.capture_enabled=1)
            ORDER BY CASE WHEN s.id>? THEN 0 ELSE 1 END,s.id LIMIT ?''',(previous,limit)).fetchall()
        result=[]
        for row in rows:
            item=dict(row)
            pending=c.execute('''SELECT issue_key FROM lab_input_rechecks WHERE stream_id=?
                ORDER BY CASE WHEN issue_key>? THEN 0 ELSE 1 END,issue_key LIMIT 1''',
                (row['id'],row['pending_recheck_after_issue'])).fetchone()
            missing=c.execute('''SELECT x.issue_key FROM lab_input_coverage x WHERE stream_id=?
                AND revision=(SELECT MAX(p.revision) FROM lab_input_coverage p WHERE p.stream_id=x.stream_id AND p.issue_key=x.issue_key)
                AND class IN ('waiting','archive_gap') ORDER BY issue_key LIMIT 1''',(row['id'],)).fetchone()
            # Recheck a small overlap even after catching up, to observe status/revisions.
            # Forward capture must progress even at the supported page_size=1.
            # Recheck an overlap only after catching the current source horizon.
            forward=row['after_issue']<row['highest_seen_issue']
            # Alternate repair and forward capture so an unrecoverable old gap
            # does not prevent preserving newly available source inputs.
            if pending and row['revision']%3==1:
                item['capture_after_issue']=max(0,pending[0]-1)
            elif missing and row['revision']%2==0:
                item['capture_after_issue']=max(0,missing[0]-1)
            elif forward:item['capture_after_issue']=row['after_issue']
            else:item['capture_after_issue']=max(0,row['after_issue']-20,min(row['recheck_after_issue'],row['after_issue']))
            result.append(item)
        return result


class ArchiveSource:
    """A frozen local input view. Live source authorization belongs to the caller."""
    def __init__(self, store, predictor_id, archive_revision, cutoff_issue, cutoff_at, *,
                 source_key='pc28-upstream',baseline_revision=None,baseline_through_issue=None,warm_inputs=None):
        self.store,self.predictor_id=store,int(predictor_id)
        self.revision=int(archive_revision);self.cutoff_issue=_issue(cutoff_issue,zero=True)
        self.cutoff_at=cutoff_at;_time(cutoff_at)
        self.source_key=source_key
        self.baseline_revision=int(baseline_revision) if baseline_revision is not None else self.revision
        self.baseline_through_issue=int(baseline_through_issue) if baseline_through_issue is not None else 0
        self.warm_inputs=dict(warm_inputs or {})

    def _revision_for(self, issue):
        return self.baseline_revision if int(issue)<=self.baseline_through_issue else self.revision

    def _warm_prediction(self,c,issue,warm):
        value=warm.get('prediction')
        if value is None:return None
        value=dict(value)
        observed=read_input(c,self.source_key,'prediction',self.predictor_id,issue,self.revision)
        # Settlement publication is allowed to advance for unchanged prediction
        # content. Direction or creation-time revisions never alter old warm data.
        if observed and observed.get('input_hash')==value.get('input_hash') and observed.get('settled_at'):
            value['settled_at']=observed['settled_at'];value['status']=observed.get('status',value['status'])
        return value

    def _rows(self,kind,predictor,where,params,limit,reverse=False):
        with self.store._connect() as c:
            rows=c.execute('''SELECT v.* FROM lab_input_versions v WHERE source_key=? AND kind=? AND predictor_id=?
                AND issue_key<=? AND '''+where+''' AND revision=(SELECT MAX(p.revision) FROM lab_input_versions p
                    WHERE p.source_key=v.source_key AND p.kind=v.kind AND p.predictor_id=v.predictor_id AND p.issue_key=v.issue_key
                    AND p.revision<=CASE WHEN v.issue_key<=? THEN ? ELSE ? END)
                ORDER BY issue_key '''+('DESC' if reverse else 'ASC')+' LIMIT ?',
                [self.source_key,kind,predictor,self.cutoff_issue]+list(params)+
                [self.baseline_through_issue,self.baseline_revision,self.revision,limit]).fetchall()
        values=[]
        for row in rows:
            key=str(row['issue_key']);override=self.warm_inputs.get(key)
            value=override.get(kind) if override is not None else _payload(row)
            if value is not None:values.append(dict(value))
        # Checkpoints retain a boundary even if the original hot rows were pruned.
        if kind=='draw':
            by_issue={int(v['issue_no']):v for v in values}
            for key,entry in self.warm_inputs.items():
                issue=int(key);value=entry.get('draw')
                if value is None or issue>self.cutoff_issue:continue
                included=(where=='1=1' or (where=='issue_key<?' and issue<int(params[0]))
                    or (where=='issue_key>? AND issue_key<?' and int(params[0])<issue<int(params[1]))
                    or (where=='open_at>=?' and value['open_at']>=params[0]))
                if included:by_issue[issue]=dict(value)
            values=[by_issue[k] for k in sorted(by_issue,reverse=reverse)[:limit]]
        return values

    def _visible_draw(self,row):
        return row is not None and _time(row['open_at'])<=_time(self.cutoff_at) and _time(row['source_created_at'])<=_time(self.cutoff_at)

    def get_predictor(self,predictor_id):
        return dict(id=self.predictor_id,name='已保存的实验输入') if predictor_id==self.predictor_id else None

    def get_draw_by_issue(self,issue_no,*,observed_at):
        issue=_issue(issue_no)
        if issue>self.cutoff_issue:return None
        if str(issue) in self.warm_inputs:row=self.warm_inputs[str(issue)].get('draw')
        else:
            with self.store._connect() as c:
                row=read_input(c,self.source_key,'draw',0,issue,self._revision_for(issue))
        return row if self._visible_draw(row) else None

    def latest_draw(self,*,observed_at):
        rows=self._rows('draw',0,'1=1',[],1,reverse=True)
        return rows[0] if rows and self._visible_draw(rows[0]) else None

    def draws_page(self,*,after_issue='',before_issue=None,limit=200,observed_at):
        limit=_limit(limit);after=_issue(after_issue,zero=True) if after_issue else 0
        before=_issue(before_issue) if before_issue is not None else self.cutoff_issue+1
        rows=self._rows('draw',0,'issue_key>? AND issue_key<?',[after,before],limit)
        items=[r for r in rows if self._visible_draw(r)]
        return dict(items=items,next_issue=rows[-1]['issue_no'] if rows else None,scanned_count=len(rows),has_more=len(rows)==limit,skip_counts={})

    def history_draws(self,*,before_issue,limit=100,observed_at):
        limit=_limit(limit,100);before=_issue(before_issue)
        rows=self._rows('draw',0,'issue_key<?',[before],limit,reverse=True)
        items=[r for r in reversed(rows) if self._visible_draw(r)]
        return dict(items=items,next_issue=rows[-1]['issue_no'] if rows else None,scanned_count=len(rows),has_more=len(rows)==limit,skip_counts={})

    def get_predictions(self,predictor_id,issue_nos,*,observed_at):
        if predictor_id!=self.predictor_id or len(issue_nos)>200:
            raise ValueError('档案预测读取范围无效')
        issues=list(dict.fromkeys(_issue(i) for i in issue_nos));items=[];skips={}
        with self.store._connect() as c:
            stream=c.execute('SELECT id FROM lab_capture_streams WHERE source_key=? AND predictor_id=?',
                (self.source_key,self.predictor_id)).fetchone()
            for issue in issues:
                if issue>self.cutoff_issue:continue
                revision=self._revision_for(issue)
                warm=self.warm_inputs.get(str(issue))
                coverage={'class':warm['coverage']} if warm is not None else c.execute('''SELECT class FROM lab_input_coverage WHERE stream_id=? AND issue_key=?
                    AND revision<=? ORDER BY revision DESC LIMIT 1''',(stream['id'] if stream else -1,issue,revision)).fetchone()
                if not coverage or coverage['class'] not in COMPLETE_CLASSES:
                    raise RuleLabError('archive_gap','所需预测输入尚未完整保存，已保留当前模拟进度',409)
                if coverage['class']!='present':
                    if coverage['class']=='invalid_prediction':skips['invalid_prediction']=skips.get('invalid_prediction',0)+1
                    continue
                row=self._warm_prediction(c,issue,warm) if warm is not None else read_input(c,self.source_key,'prediction',self.predictor_id,issue,revision)
                if row is None:
                    raise RuleLabError('archive_gap','已保存的预测覆盖缺少对应内容，不能继续模拟',409)
                if _time(row['created_at'])>_time(self.cutoff_at):continue
                if row.get('settled_at') and _time(row['settled_at'])>_time(self.cutoff_at):
                    row={**row,'status':'pending','settled_at':None}
                items.append(row)
        return dict(items=items,skip_counts=skips)

    def find_start_issue(self,start_date,*,observed_at):
        boundary=datetime.fromisoformat(start_date).replace(tzinfo=SHANGHAI).astimezone(UTC).isoformat().replace('+00:00','Z')
        rows=self._rows('draw',0,'open_at>=?',[boundary],1)
        return rows[0]['issue_no'] if rows and self._visible_draw(rows[0]) else None

    def get_history(self,predictor_id,*,before_issue,limit=100):
        draws=self.history_draws(before_issue=before_issue,limit=limit,observed_at=self.cutoff_at)['items']
        predictions={p['issue_no']:p for p in self.get_predictions(predictor_id,[d['issue_no'] for d in draws],observed_at=self.cutoff_at)['items']}
        result=[]
        for d in draws:
            p=predictions.get(d['issue_no']) or {}
            result.append(dict(issue_no=d['issue_no'],open_at=d['open_at'],draw_available_at=d['source_created_at'],triplet=d['triplet'],
                prediction_created_at=p.get('created_at'),prediction_settled_at=p.get('settled_at'),
                prediction_values={k:p.get(k) for k in ('big_small','odd_even','combo')},draw_conflict=bool(d.get('conflict')),
                prediction_conflict=bool(p.get('conflict')),conflict=bool(d.get('conflict') or p.get('conflict'))))
        return result

    def input_evidence(self,predictor_id,issue_no):
        draw=self.get_draw_by_issue(issue_no,observed_at=self.cutoff_at)
        predictions=self.get_predictions(predictor_id,[issue_no],observed_at=self.cutoff_at)['items']
        prediction=predictions[0] if predictions else None
        if draw:
            draw={k:draw.get(k) for k in ('issue_no','open_at','source_created_at','input_hash','conflict')} | {'triplet_json':_dump(draw['triplet'])}
        if prediction:
            prediction={k:prediction.get(k) for k in ('predictor_id','issue_no','created_at','settled_at','status','big_small','odd_even','combo','input_hash','conflict')}
        return dict(draw=draw,prediction=prediction)


def maintenance(store, *, now, protected_floors=None, limit=200):
    """Compatibility entry; all cleanup uses current consumer dependencies."""
    from pc28touzhu.services.rule_lab_retention import maintain
    return maintain(store,now=now,limit=limit)
