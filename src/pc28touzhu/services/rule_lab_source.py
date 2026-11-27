"""Bounded, read-only access to the existing PC28 prediction database."""
from __future__ import annotations

import hashlib
import json
import math
import os
import sqlite3
import time
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.services.rule_lab_store import RuleLabError

UTC = timezone.utc
SHANGHAI = timezone(timedelta(hours=8))
_VALUES = {'big_small': {'大', '小'}, 'odd_even': {'单', '双'},
           'combo': {'大单', '大双', '小单', '小双'}}
_DRAW_COLUMNS = ('id,issue_no,open_time,result_number,'
                 'substr(source_payload,1,16385) AS source_payload,created_at')
_PUBLIC = "lottery_type='pc28' AND enabled=1 AND share_predictions=1 AND share_level IN ('records','analysis')"


def _utc(value, default=UTC):
    if not isinstance(value, str) or not value.strip():
        raise ValueError('missing_time')
    parsed = datetime.fromisoformat(value.strip().replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=default)
    return parsed.astimezone(UTC).isoformat().replace('+00:00', 'Z')


def _issue(value):
    value = str(value)
    if not value.isascii() or not value.isdigit() or str(int(value)) != value or not 0 < int(value) < 2**63:
        raise ValueError('invalid_issue')
    return value


def _cursor(value):
    value = str(value)
    if len(value) > 32 or not value.isascii() or any(ord(c) < 32 for c in value):
        raise ValueError('invalid_cursor')
    return value


def _hash(value):
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(',', ':'), allow_nan=False).encode()).hexdigest()


def _page_limit(value, maximum=200):
    if type(value) is not int or not 1 <= value <= maximum:
        raise ValueError('读取批次大小无效')
    return value


class RuleLabSource:
    def __init__(self, db_path=None, *, timeout_seconds=.25):
        self.db_path = Path(db_path or os.environ.get('RULE_LAB_SOURCE_DB',
                            '/www/wwwroot/AITradingSimulator/pc28_predictor.db')).expanduser().resolve()
        if not isinstance(timeout_seconds, (int, float)) or not math.isfinite(timeout_seconds) or not 0 < timeout_seconds <= 2:
            raise ValueError('数据源超时设置无效')
        self.timeout_seconds = float(timeout_seconds)

    @contextmanager
    def _read(self):
        conn = None
        deadline = time.monotonic() + self.timeout_seconds
        try:
            conn = sqlite3.connect(self.db_path.as_uri() + '?mode=ro', uri=True,
                                   timeout=self.timeout_seconds)
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA query_only=ON')
            conn.execute('PRAGMA cache_size=-2048')
            conn.execute('PRAGMA busy_timeout=%d' % int(self.timeout_seconds * 1000))
            conn.set_progress_handler(lambda: int(time.monotonic() >= deadline), 1000)
            yield conn, deadline
        except (sqlite3.Error, OSError) as exc:
            raise RuleLabError('source_unavailable', '预测数据暂不可读，请稍后重试或检查数据源服务', 503) from exc
        finally:
            if conn is not None:
                conn.close()

    @staticmethod
    def _query(conn, deadline, sql, params=()):
        if time.monotonic() >= deadline:
            raise RuleLabError('source_unavailable', '预测数据读取超时，稍后将重试', 503)
        return conn.execute(sql, params).fetchall()

    def list_predictors(self):
        with self._read() as (c, deadline):
            rows = self._query(c, deadline, 'SELECT id,name FROM predictors WHERE ' + _PUBLIC + ' ORDER BY id LIMIT 501')
        if len(rows) > 500:
            raise RuleLabError('source_unavailable', '可用来源数量超过本次读取预算', 503)
        return [dict(id=r['id'], name=str(r['name'] or '')[:200]) for r in rows]

    def get_predictor(self, predictor_id):
        if type(predictor_id) is not int or predictor_id <= 0:
            raise ValueError('预测器编号无效')
        with self._read() as (c, deadline):
            rows = self._query(c, deadline, 'SELECT id,name FROM predictors WHERE id=? AND ' + _PUBLIC, (predictor_id,))
        return dict(id=rows[0]['id'], name=str(rows[0]['name'] or '')[:200]) if rows else None

    @staticmethod
    def _draw(row, observed_at):
        issue = _issue(row['issue_no'])
        payload_text = row['source_payload'] or ''
        if len(payload_text) > 16384:
            raise ValueError('oversized_draw')
        payload = json.loads(payload_text)
        if not isinstance(payload, dict):
            raise ValueError('invalid_draw')
        if isinstance(payload.get('number'), str):
            raw = payload['number'].split('+')
        elif isinstance(payload.get('triplet'), list):
            raw = payload['triplet']
        else:
            raw = [payload.get('canada28_num' + str(i)) for i in (1, 2, 3)]
        if len(raw) != 3 or any(isinstance(v, bool) or str(v) not in tuple(map(str, range(10))) for v in raw):
            raise ValueError('invalid_triplet')
        balls = list(map(int, raw))
        if type(row['result_number']) is not int or sum(balls) != row['result_number']:
            raise ValueError('draw_sum_mismatch')
        opened = _utc(row['open_time'], SHANGHAI)
        created = _utc(row['created_at'])
        return dict(issue_no=issue, source_id=row['id'], open_at=opened, source_created_at=created,
                    observed_at=_utc(observed_at), triplet=balls,
                    input_hash=_hash([issue, balls, opened, created]))

    def latest_draw(self, *, observed_at):
        with self._read() as (c, deadline):
            rows = self._query(c, deadline, 'SELECT ' + _DRAW_COLUMNS +
                               " FROM lottery_draws WHERE lottery_type='pc28' ORDER BY issue_no DESC LIMIT 1")
        if not rows:
            return None
        try:
            return self._draw(rows[0], observed_at)
        except (ValueError, TypeError, KeyError) as exc:
            raise RuleLabError('source_unavailable', '最新开奖数据不完整，等待有效三球与时间', 503) from exc

    def get_draw_by_issue(self, issue_no, *, observed_at):
        issue = _issue(issue_no)
        with self._read() as (c, deadline):
            rows = self._query(c, deadline, 'SELECT ' + _DRAW_COLUMNS +
                               " FROM lottery_draws WHERE lottery_type='pc28' AND issue_no=?", (issue,))
        if not rows:
            return None
        try:
            return self._draw(rows[0], observed_at)
        except (ValueError, TypeError, KeyError) as exc:
            raise RuleLabError('source_unavailable', '待结算期的开奖数据不完整，已保留冻结决策', 503) from exc

    def _draw_page(self, rows, *, observed_at, limit, width=None, reverse=False):
        items, skips = [], {}
        for row in rows:
            try:
                draw = self._draw(row, observed_at)
                if width is None:
                    width = len(draw['issue_no'])
                if len(draw['issue_no']) != width:
                    raise ValueError('issue_width_changed')
                items.append(draw)
            except (ValueError, TypeError, KeyError):
                skips['invalid_draw'] = skips.get('invalid_draw', 0) + 1
        if reverse:
            items.reverse()
        return dict(items=items, next_issue=str(rows[-1]['issue_no']) if rows else None,
                    scanned_count=len(rows), has_more=len(rows) == limit, skip_counts=skips)

    def draws_page(self, *, after_issue='', before_issue=None, limit=200, observed_at):
        limit = _page_limit(limit)
        after = _cursor(after_issue)
        before = _issue(before_issue) if before_issue is not None else None
        sql = 'SELECT ' + _DRAW_COLUMNS + " FROM lottery_draws WHERE lottery_type='pc28' AND issue_no>?"
        params = [after]
        if before is not None:
            sql += ' AND issue_no<?'
            params.append(before)
        with self._read() as (c, deadline):
            rows = self._query(c, deadline, sql + ' ORDER BY issue_no LIMIT ?', params + [limit])
        width = len(before) if before else len(after) if after.isdigit() else None
        return self._draw_page(rows, observed_at=observed_at, limit=limit, width=width)

    def history_draws(self, *, before_issue, limit=100, observed_at):
        limit, before = _page_limit(limit, 100), _issue(before_issue)
        with self._read() as (c, deadline):
            rows = self._query(c, deadline, 'SELECT ' + _DRAW_COLUMNS +
                               " FROM lottery_draws WHERE lottery_type='pc28' AND issue_no<? ORDER BY issue_no DESC LIMIT ?",
                               (before, limit))
        return self._draw_page(rows, observed_at=observed_at, limit=limit, width=len(before), reverse=True)

    def get_predictions(self, predictor_id, issue_nos, *, observed_at):
        if type(predictor_id) is not int or predictor_id <= 0 or len(issue_nos) > 200:
            raise ValueError('预测读取范围无效')
        issues = list(dict.fromkeys(_issue(i) for i in issue_nos))
        if not issues:
            return dict(items=[], skip_counts={})
        with self._read() as (c, deadline):
            allowed = self._query(c, deadline, 'SELECT id FROM predictors WHERE id=? AND ' + _PUBLIC, (predictor_id,))
            if not allowed:
                return dict(items=[], skip_counts={'source_not_shared': len(issues)})
            rows = self._query(c, deadline, '''SELECT id,predictor_id,issue_no,prediction_big_small,
                prediction_odd_even,prediction_combo,status,created_at,settled_at FROM predictions
                WHERE predictor_id=? AND lottery_type='pc28' AND issue_no IN (''' + ','.join('?' for _ in issues) +
                ') ORDER BY issue_no', [predictor_id] + issues)
        items, skips = [], {}
        for row in rows:
            try:
                issue, created = _issue(row['issue_no']), _utc(row['created_at'])
                if row['status'] not in {'pending', 'settled'}:
                    raise ValueError('unusable_prediction')
                settled = _utc(row['settled_at']) if row['settled_at'] else None
                values = {metric: row['prediction_' + metric] or None for metric in _VALUES}
                if any(value is not None and value not in _VALUES[metric] for metric, value in values.items()):
                    raise ValueError('invalid_direction')
                items.append(dict(predictor_id=predictor_id, issue_no=issue, source_id=row['id'], created_at=created,
                                  settled_at=settled, observed_at=_utc(observed_at), status=row['status'], **values,
                                  input_hash=_hash([predictor_id, issue, created, values])))
            except (ValueError, TypeError, KeyError):
                skips['invalid_prediction'] = skips.get('invalid_prediction', 0) + 1
        return dict(items=items, skip_counts=skips)

    def capture_page(self, predictor_id, *, after_issue='', limit=200, observed_at):
        """Read one consistent page with evidence for absent predictions.

        A missing row is not evidence that the prediction never existed. The
        per-predictor archive summary tells us which dates may have been purged.
        Sources without that metadata remain conservatively incomplete.
        """
        if type(predictor_id) is not int or predictor_id <= 0:
            raise ValueError('预测器编号无效')
        limit = _page_limit(limit)
        after = _cursor(str(after_issue))
        reference = datetime.fromisoformat(_utc(observed_at).replace('Z', '+00:00'))
        with self._read() as (c, deadline):
            c.execute('BEGIN')
            allowed = self._query(c, deadline, 'SELECT id FROM predictors WHERE id=? AND ' + _PUBLIC, (predictor_id,))
            if not allowed:
                raise RuleLabError('source_unavailable', '来源已停用或不再共享逐期预测，已暂停保存后续数据', 403)
            columns = {r['name'] for r in self._query(c, deadline, 'PRAGMA table_info(predictors)')}
            identity_created = None
            if 'created_at' in columns:
                identity_created = self._query(c, deadline, 'SELECT created_at FROM predictors WHERE id=?', (predictor_id,))[0][0]
            identity = _hash([str(self.db_path), predictor_id, identity_created])
            base = 'SELECT ' + _DRAW_COLUMNS + " FROM lottery_draws WHERE lottery_type='pc28'"
            latest = self._query(c, deadline, "SELECT issue_no FROM lottery_draws WHERE lottery_type='pc28' ORDER BY issue_no DESC LIMIT 1")
            highest = str(latest[0][0]) if latest else (after or '0')
            # A synthetic/start cursor can have one fewer digit than the stored
            # issue range. Pad that bound without casting the indexed column.
            seek_after = after.zfill(len(highest)) if after.isdigit() and len(after)<len(highest) else after
            rows = self._query(c, deadline, base + ' AND issue_no>? ORDER BY issue_no LIMIT ?', (seek_after, limit))
            metadata = self._query(c, deadline, "SELECT 1 FROM sqlite_master WHERE type='table' AND name='pc28_prediction_daily_summary'")
            archived_through = None
            if metadata:
                archived_through = self._query(c, deadline,
                    'SELECT MAX(summary_date) FROM pc28_prediction_daily_summary WHERE predictor_id=?', (predictor_id,))[0][0]
            issues = [str(r['issue_no']) for r in rows]
            prediction_rows = self._query(c, deadline, '''SELECT id,predictor_id,issue_no,prediction_big_small,
                prediction_odd_even,prediction_combo,status,created_at,settled_at FROM predictions
                WHERE predictor_id=? AND lottery_type='pc28' AND issue_no IN (''' + ','.join('?' for _ in issues) + ')',
                [predictor_id] + issues) if issues else []
        by_issue = {str(r['issue_no']): r for r in prediction_rows}
        draws, predictions, coverage = [], [], []
        previous = int(after) if after.isdigit() else None
        for row in rows:
            issue = _issue(row['issue_no'])
            if previous is not None and int(issue) > previous + 1:
                coverage.append(dict(issue_no=str(previous + 1), **{'class': 'archive_gap'},
                    details={'reason': 'missing_draw', 'through_issue': str(int(issue) - 1)}))
            previous = int(issue)
            try:
                draw = self._draw(row, observed_at)
            except (ValueError, TypeError, KeyError):
                coverage.append(dict(issue_no=issue, **{'class':'archive_gap'}, details={'reason':'invalid_draw'}))
                continue
            draws.append(draw)
            category, details = 'present', {}
            if max(datetime.fromisoformat(draw[key].replace('Z','+00:00')) for key in ('open_at','source_created_at')) > reference:
                category, details = 'waiting', {'reason': 'draw_not_available'}
            prediction_row = by_issue.get(issue)
            if prediction_row is None:
                draw_day = datetime.fromisoformat(draw['open_at'].replace('Z','+00:00')).astimezone(SHANGHAI).date().isoformat()
                if category != 'waiting':
                    # A prediction generated just before midnight may target the
                    # next date. Old cleanup used creation date, so also protect
                    # the day immediately following the last archived date.
                    uncertain_through = (date.fromisoformat(archived_through)+timedelta(days=1)).isoformat() if archived_through else None
                    if not metadata or (uncertain_through and draw_day <= uncertain_through):
                        category, details = 'archive_gap', {'reason': 'prediction_may_be_archived', 'archived_through': archived_through}
                    else:
                        category, details = 'no_prediction', {'reason': 'successful_read_in_retained_range', 'archived_through': archived_through}
            else:
                try:
                    created = _utc(prediction_row['created_at'])
                    if prediction_row['status'] not in {'pending', 'settled'}:
                        raise ValueError('unusable_prediction')
                    settled = _utc(prediction_row['settled_at']) if prediction_row['settled_at'] else None
                    values = {metric: prediction_row['prediction_' + metric] or None for metric in _VALUES}
                    if any(value is not None and value not in _VALUES[metric] for metric, value in values.items()):
                        raise ValueError('invalid_direction')
                    predictions.append(dict(predictor_id=predictor_id,issue_no=issue,source_id=prediction_row['id'],
                        created_at=created,settled_at=settled,observed_at=_utc(observed_at),status=prediction_row['status'],
                        **values,input_hash=_hash([predictor_id, issue, created, values])))
                except (ValueError, TypeError, KeyError):
                    if category != 'waiting':
                        category, details = 'invalid_prediction', {'reason': 'source_row_unusable'}
            coverage.append(dict(issue_no=issue, **{'class':category}, details=details))
        if not rows and after.isdigit() and highest.isdigit() and int(highest)>int(after):
            coverage.append(dict(issue_no=str(int(after)+1), **{'class':'archive_gap'},
                details={'reason':'missing_draw','through_issue':highest}))
        return dict(draws=draws,predictions=predictions,coverage=coverage,
                    next_issue=str(rows[-1]['issue_no']) if rows else (after or None),
                    highest_seen_issue=highest,source_identity=identity,has_more=len(rows)==limit)

    def find_start_issue(self, start_date, *, observed_at):
        """Seek a monotonic PC28 date using indexed single-row probes, never a date scan."""
        start = date.fromisoformat(start_date)
        boundary = datetime.combine(start, datetime.min.time(), tzinfo=SHANGHAI).astimezone(UTC)
        _utc(observed_at)
        with self._read() as (c, deadline):
            base = "SELECT issue_no,open_time FROM lottery_draws WHERE lottery_type='pc28'"
            first = self._query(c, deadline, base + ' ORDER BY issue_no LIMIT 1')
            last = self._query(c, deadline, base + ' ORDER BY issue_no DESC LIMIT 1')
            if not first:
                return None
            samples = {}
            def inspect(row):
                issue = _issue(row['issue_no'])
                opened = datetime.fromisoformat(_utc(row['open_time'], SHANGHAI).replace('Z', '+00:00'))
                samples[int(issue)] = opened
                ordered = sorted(samples.items())
                if any(a[1] > b[1] for a, b in zip(ordered, ordered[1:])):
                    raise ValueError('non_monotonic_draws')
                return issue, opened
            try:
                first_issue, first_time = inspect(first[0])
                last_issue, last_time = inspect(last[0])
                if len(first_issue) != len(last_issue):
                    raise ValueError('issue_width_changed')
                if last_time < boundary:
                    return None
                if first_time >= boundary:
                    return first_issue
                low, high = int(first_issue), int(last_issue)
                for _ in range(64):
                    if low >= high:
                        break
                    middle = (low + high) // 2
                    row = self._query(c, deadline, base + ' AND issue_no>=? AND issue_no<=? ORDER BY issue_no LIMIT 1',
                                      (str(middle), last_issue))[0]
                    found, opened = inspect(row)
                    if opened >= boundary:
                        high = middle
                    else:
                        low = int(found) + 1
                rows = self._query(c, deadline, base + ' AND issue_no>=? ORDER BY issue_no LIMIT 1', (str(low),))
                if not rows:
                    return None
                found, opened = inspect(rows[0])
                previous = self._query(c, deadline, base + ' AND issue_no<? ORDER BY issue_no DESC LIMIT 1', (found,))
                following = self._query(c, deadline, base + ' AND issue_no>? ORDER BY issue_no LIMIT 1', (found,))
                if previous and inspect(previous[0])[1] >= boundary:
                    raise ValueError('date_seek_inconsistent')
                if following:
                    inspect(following[0])
                if opened < boundary:
                    raise ValueError('date_seek_inconsistent')
                return found
            except (ValueError, TypeError, IndexError) as exc:
                raise RuleLabError('source_unavailable', '开奖期号或时间顺序异常，无法定位回放起点', 503) from exc
