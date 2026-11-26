"""Independent, bounded simulation ledger. Never opens the production ledger."""
from __future__ import annotations

import json
import os
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.domain.rule_lab import ALLOWED_SETTLEMENT_RULE_IDS, apply_outcome, empty_stats

SHANGHAI = timezone(timedelta(hours=8))
MAX_TOTAL_EXPERIMENTS = 500
MAX_RUNNING_EXPERIMENTS = 100


class RuleLabError(Exception):
    def __init__(self, code: str, message: str, status: int = 400):
        super().__init__(message)
        self.code, self.message, self.status = code, message, status


def _dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'), allow_nan=False)


def _load(value):
    return json.loads(value)


def _time(value):
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('时间必须包含时区')
    return parsed.astimezone(timezone.utc)


def _day(value):
    return _time(value).astimezone(SHANGHAI).date().isoformat()


def _issue(value):
    text = str(value)
    if not text.isascii() or not text.isdigit() or not 0 < int(text) < 2**63:
        raise ValueError('无效期号')
    return int(text)


class RuleLabStore:
    def __init__(self, db_path=None, *, budget_bytes=1073741824, detail_retention_days=30):
        root = Path(__file__).resolve().parents[3]
        self.db_path = Path(db_path) if db_path is not None else Path(os.environ.get('RULE_LAB_DATA_DIR', str(root/'data'/'rule_lab')))/'rule_lab.db'
        self.budget_bytes = max(1024, int(budget_bytes))
        self.detail_retention_days = max(1, int(detail_retention_days))

    @contextmanager
    def _connect(self, *, write=False):
        conn = None
        try:
            conn = sqlite3.connect(str(self.db_path), timeout=.25)
            conn.row_factory = sqlite3.Row
            conn.execute('PRAGMA foreign_keys=ON')
            conn.execute('PRAGMA busy_timeout=250')
            conn.execute('PRAGMA cache_size=-2048')
            if write:
                current = conn.execute('PRAGMA page_count').fetchone()[0]
                size = conn.execute('PRAGMA page_size').fetchone()[0]
                max_pages = max(current, int(self.budget_bytes*.8)//size, 1)
                conn.execute('PRAGMA max_page_count=%s' % max_pages)
                conn.execute('PRAGMA wal_autocheckpoint=64')
                conn.execute('PRAGMA journal_size_limit=1048576')
                conn.execute('BEGIN IMMEDIATE')
            else:
                conn.execute('PRAGMA query_only=ON')
            yield conn
            if write:
                conn.commit()
        except sqlite3.Error as exc:
            if conn is not None:
                conn.rollback()
            message = str(exc).lower()
            if 'locked' in message or 'busy' in message:
                raise RuleLabError('store_busy', '模拟账本正忙，请稍后重试', 503) from exc
            if 'full' in message or 'disk' in message:
                raise RuleLabError('storage_limit', '模拟存储空间不足，已保留现有成绩', 503) from exc
            raise RuleLabError('store_unavailable', '模拟账本暂不可用，请检查服务状态', 503) from exc
        except Exception:
            if conn is not None:
                conn.rollback()
            raise
        finally:
            if conn is not None:
                conn.close()

    def initialize(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # journal mode cannot be changed inside the schema transaction.
        with sqlite3.connect(str(self.db_path), timeout=.25) as connection:
            connection.execute('PRAGMA journal_mode=WAL')
        with self._connect(write=True) as c:
            version = c.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1):
                raise RuleLabError('store_unavailable', '模拟账本版本不兼容', 503)
            statements = [
                '''CREATE TABLE IF NOT EXISTS lab_batches (
                id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, name TEXT NOT NULL,
                mode TEXT NOT NULL, predictor_id INTEGER NOT NULL, predictor_name TEXT NOT NULL,
                metric TEXT NOT NULL, template_pack TEXT NOT NULL, stake_cents INTEGER NOT NULL,
                start_date TEXT, end_date TEXT, status TEXT NOT NULL DEFAULT 'paused',
                phase TEXT NOT NULL DEFAULT 'idle', error TEXT NOT NULL DEFAULT '',
                created_at TEXT NOT NULL, started_at TEXT, updated_at TEXT NOT NULL,
                cursor_json TEXT NOT NULL DEFAULT '{}', revision INTEGER NOT NULL DEFAULT 0,
                processed_issues INTEGER NOT NULL DEFAULT 0, total_issues INTEGER,
                skip_counts_json TEXT NOT NULL DEFAULT '{}')''',
                'CREATE INDEX IF NOT EXISTS lab_batches_owner ON lab_batches(user_id,id)',
                '''CREATE TABLE IF NOT EXISTS lab_experiments (
                id INTEGER PRIMARY KEY, batch_id INTEGER NOT NULL REFERENCES lab_batches(id),
                condition_key TEXT NOT NULL, name TEXT NOT NULL, condition_json TEXT NOT NULL,
                settlement_rule_id TEXT NOT NULL, settlement_snapshot_json TEXT NOT NULL,
                stats_json TEXT NOT NULL, last_settled_issue INTEGER NOT NULL DEFAULT 0,
                last_decided_issue INTEGER NOT NULL DEFAULT 0,
                UNIQUE(batch_id,condition_key,settlement_rule_id))''',
                '''CREATE TABLE IF NOT EXISTS lab_draws (
                issue_no TEXT PRIMARY KEY, issue_key INTEGER NOT NULL UNIQUE, source_id INTEGER,
                open_at TEXT NOT NULL, source_created_at TEXT NOT NULL, observed_at TEXT NOT NULL,
                triplet_json TEXT NOT NULL, input_hash TEXT NOT NULL, conflict INTEGER NOT NULL DEFAULT 0)''',
                '''CREATE TABLE IF NOT EXISTS lab_predictions (
                predictor_id INTEGER NOT NULL, issue_no TEXT NOT NULL, issue_key INTEGER NOT NULL,
                source_id INTEGER, created_at TEXT NOT NULL, settled_at TEXT, observed_at TEXT NOT NULL,
                status TEXT NOT NULL, big_small TEXT, odd_even TEXT, combo TEXT, input_hash TEXT NOT NULL,
                conflict INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(predictor_id,issue_no))''',
                'CREATE INDEX IF NOT EXISTS lab_predictions_issue ON lab_predictions(issue_key,predictor_id)',
                '''CREATE TABLE IF NOT EXISTS lab_decisions (
                id INTEGER PRIMARY KEY, experiment_id INTEGER NOT NULL REFERENCES lab_experiments(id),
                issue_no TEXT NOT NULL, issue_key INTEGER NOT NULL, bet_value TEXT NOT NULL,
                stake_cents INTEGER NOT NULL, decided_at TEXT NOT NULL, decision_mode TEXT NOT NULL,
                feature_issue_no TEXT, condition_value_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                skip_reason TEXT NOT NULL DEFAULT '', settled_at TEXT, finalized_at TEXT, result_type TEXT, net_cents INTEGER,
                refund_reason TEXT NOT NULL DEFAULT '', UNIQUE(experiment_id,issue_no))''',
                'CREATE INDEX IF NOT EXISTS lab_decisions_pending ON lab_decisions(status,issue_key)',
                'CREATE INDEX IF NOT EXISTS lab_decisions_finished ON lab_decisions(status,finalized_at)',
                '''CREATE TABLE IF NOT EXISTS lab_daily (
                experiment_id INTEGER NOT NULL REFERENCES lab_experiments(id), stat_date TEXT NOT NULL,
                settled_count INTEGER NOT NULL DEFAULT 0, hit_count INTEGER NOT NULL DEFAULT 0,
                refund_count INTEGER NOT NULL DEFAULT 0, miss_count INTEGER NOT NULL DEFAULT 0,
                direction_hit_count INTEGER NOT NULL DEFAULT 0, stake_cents INTEGER NOT NULL DEFAULT 0,
                net_cents INTEGER NOT NULL DEFAULT 0, trigger_count INTEGER NOT NULL DEFAULT 0,
                skipped_count INTEGER NOT NULL DEFAULT 0, PRIMARY KEY(experiment_id,stat_date))''',
                'CREATE TABLE IF NOT EXISTS lab_runtime (key TEXT PRIMARY KEY,value_json TEXT NOT NULL)',
            ]
            for statement in statements:
                c.execute(statement)
            c.execute('PRAGMA user_version=1')

    def storage_status(self):
        total = 0
        if self.db_path.parent.exists():
            for path in self.db_path.parent.iterdir():
                if path.is_file() and not path.is_symlink():
                    try:
                        total += path.stat().st_size
                    except FileNotFoundError:
                        pass
        return dict(used_bytes=total, budget_bytes=self.budget_bytes, detail_retention_days=self.detail_retention_days)

    def _require_space(self):
        if self.storage_status()['used_bytes'] >= int(self.budget_bytes*15/16):
            raise RuleLabError('storage_limit', '模拟存储接近预算上限，请清理已完成明细后恢复', 503)

    @staticmethod
    def _owned(c, user_id, batch_id):
        row = c.execute('SELECT * FROM lab_batches WHERE id=? AND user_id=?', (batch_id,user_id)).fetchone()
        if row is None:
            raise RuleLabError('not_found', '实验批次不存在', 404)
        return row

    @staticmethod
    def _batch(c, row):
        count = c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(row['id'],)).fetchone()[0]
        return dict(id=row['id'],name=row['name'],mode=row['mode'],predictor_id=row['predictor_id'],predictor_name=row['predictor_name'],
                    metric=row['metric'],template_pack=row['template_pack'],stake_amount=row['stake_cents']/100,
                    start_date=row['start_date'],end_date=row['end_date'],status=row['status'],phase=row['phase'],
                    experiment_count=count,processed_issues=row['processed_issues'],total_issues=row['total_issues'],
                    created_at=row['created_at'],started_at=row['started_at'],updated_at=row['updated_at'],error=row['error'],
                    skip_counts=_load(row['skip_counts_json']),history_notice='历史回放使用现存预测记录，不能证明历史预测版本从未修改。' if row['mode']=='backtest' else '')

    @staticmethod
    def _experiment(row):
        stats = _load(row['stats_json'])
        snapshot = _load(row['settlement_snapshot_json'])
        return dict(id=row['id'],batch_id=row['batch_id'],name=row['name'],condition=_load(row['condition_json']),
                    settlement_rule_id=row['settlement_rule_id'],settlement_rule_name=snapshot['rule']['name'],
                    **{key:stats[key] for key in ('settled_count','hit_count','refund_count','miss_count','direction_hit_count',
                                                  'max_loss_streak','trigger_count','skipped_count')},
                    stake_total=stats['stake_cents']/100,net_profit=stats['net_cents']/100,
                    roi_percent=round(100*stats['net_cents']/stats['stake_cents'],2) if stats['stake_cents'] else None,
                    max_drawdown=stats['max_drawdown_cents']/100,sample_label='样本较少' if stats['settled_count']<100 else '仍需验证')

    def create_batch(self, user_id, config, predictor_name, experiments, *, now):
        if not 1 <= len(experiments) <= 100 or any(e['settlement_rule_id'] not in ALLOWED_SETTLEMENT_RULE_IDS for e in experiments):
            raise RuleLabError('invalid_input','实验仅支持两套OK规则，且每批最多100个版本')
        with self._connect(write=True) as c:
            self._require_space()
            if c.execute('SELECT COUNT(*) FROM lab_experiments').fetchone()[0]+len(experiments)>MAX_TOTAL_EXPERIMENTS:
                raise RuleLabError('capacity_exceeded','全服务最多保存500个实验版本',409)
            if c.execute('SELECT COUNT(*) FROM lab_batches WHERE user_id=?',(user_id,)).fetchone()[0]>=50:
                raise RuleLabError('capacity_exceeded','每位用户最多保存50个实验批次',409)
            params = [user_id,config['name'],config['mode'],config['predictor_id'],str(predictor_name),config['metric'],
                      config['template_pack'],config['stake_cents'],config['start_date'],config['end_date'],now,now]
            cur = c.execute('''INSERT INTO lab_batches (user_id,name,mode,predictor_id,predictor_name,metric,template_pack,
                stake_cents,start_date,end_date,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)''',params)
            bid = cur.lastrowid
            for e in experiments:
                if e['settlement_snapshot'].get('settlement_rule_id') != e['settlement_rule_id']:
                    raise ValueError('冻结规则与实验不一致')
                c.execute('''INSERT INTO lab_experiments(batch_id,condition_key,name,condition_json,settlement_rule_id,
                    settlement_snapshot_json,stats_json) VALUES(?,?,?,?,?,?,?)''',
                    (bid,e['condition_key'],e['name'],_dump(e['condition']),e['settlement_rule_id'],_dump(e['settlement_snapshot']),_dump(empty_stats())))
            return self._batch(c,self._owned(c,user_id,bid))

    def set_batch_state(self,user_id,batch_id,state,*,now):
        if state not in ('running','paused'):
            raise RuleLabError('invalid_input','状态只支持运行或暂停')
        with self._connect(write=True) as c:
            row = self._owned(c,user_id,batch_id)
            if row['status']==state:
                return self._batch(c,row)
            if row['status']=='completed':
                raise RuleLabError('invalid_transition','已完成的历史回放不能重新启动，请新建批次',409)
            if state=='running':
                self._require_space()
                if _load(row['cursor_json']).get('input_expired'):
                    raise RuleLabError('input_expired','所需输入已过期，请新建实验批次',409)
                running = c.execute("SELECT COUNT(*) FROM lab_experiments e JOIN lab_batches b ON b.id=e.batch_id WHERE b.status='running'").fetchone()[0]
                count = c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(batch_id,)).fetchone()[0]
                if running+count>MAX_RUNNING_EXPERIMENTS:
                    raise RuleLabError('capacity_exceeded','全服务同时最多运行100个实验版本，请先暂停其他批次',409)
            c.execute('''UPDATE lab_batches SET status=?,revision=revision+1,updated_at=?,error='',
                       started_at=CASE WHEN ?='running' THEN COALESCE(started_at,?) ELSE started_at END WHERE id=?''',
                      (state,now,state,now,batch_id))
            return self._batch(c,self._owned(c,user_id,batch_id))

    def list_batches(self,user_id):
        with self._connect() as c:
            return [self._batch(c,r) for r in c.execute('SELECT * FROM lab_batches WHERE user_id=? ORDER BY id DESC LIMIT 50',(user_id,)).fetchall()]

    def get_batch(self,user_id,batch_id):
        with self._connect() as c:
            return self._batch(c,self._owned(c,user_id,batch_id))

    def list_experiments(self,user_id,batch_id):
        with self._connect() as c:
            self._owned(c,user_id,batch_id)
            return [self._experiment(r) for r in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id LIMIT 100',(batch_id,))]

    def get_experiment_detail(self,user_id,batch_id,experiment_id):
        with self._connect() as c:
            self._owned(c,user_id,batch_id)
            row=c.execute('SELECT * FROM lab_experiments WHERE batch_id=? AND id=?',(batch_id,experiment_id)).fetchone()
            if row is None:
                raise RuleLabError('not_found','实验版本不存在',404)
            daily=[dict(date=r['stat_date'],settled_count=r['settled_count'],hit_count=r['hit_count'],refund_count=r['refund_count'],
                        miss_count=r['miss_count'],stake_total=r['stake_cents']/100,net_profit=r['net_cents']/100)
                   for r in c.execute('SELECT * FROM lab_daily WHERE experiment_id=? ORDER BY stat_date DESC LIMIT 90',(experiment_id,))]
            recent=[dict(issue_no=r['issue_no'],decided_at=r['decided_at'],settled_at=r['settled_at'],bet_value=r['bet_value'],
                         stake_amount=r['stake_cents']/100,status=r['status'],result_type=r['result_type'],
                         net_profit=r['net_cents']/100 if r['net_cents'] is not None else None,skip_reason=r['skip_reason'],refund_reason=r['refund_reason'])
                    for r in c.execute('SELECT * FROM lab_decisions WHERE experiment_id=? ORDER BY issue_key DESC LIMIT 50',(experiment_id,))]
            return dict(experiment=self._experiment(row),daily=list(reversed(daily)),recent=recent)

    def worker_status(self,*,now):
        with self._connect() as c:
            row=c.execute("SELECT value_json FROM lab_runtime WHERE key='worker'").fetchone()
        if not row:
            return dict(state='starting',heartbeat_at=None,message='等待模拟进程启动',last_tick_ms=None,processed_issues=0)
        value=_load(row[0])
        if (_time(now)-_time(value['heartbeat_at'])).total_seconds()>60:
            value.update(state='stale',message='模拟进程超过60秒未更新')
        return value

    def set_worker_status(self,payload,*,now):
        value=dict(state=payload.get('state','idle'),heartbeat_at=now,message=str(payload.get('message',''))[:300],
                   last_tick_ms=payload.get('last_tick_ms'),processed_issues=int(payload.get('processed_issues',0)))
        with self._connect(write=True) as c:
            c.execute("INSERT INTO lab_runtime(key,value_json) VALUES('worker',?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",(_dump(value),))

    @staticmethod
    def _internal_batch(row):
        result=dict(row)
        result['cursor']=_load(result.pop('cursor_json'))
        result['skip_counts']=_load(result.pop('skip_counts_json'))
        return result

    def list_work_batches(self):
        with self._connect() as c:
            rows=c.execute('''SELECT b.* FROM lab_batches b WHERE status='running' OR EXISTS(
                SELECT 1 FROM lab_experiments e JOIN lab_decisions d ON d.experiment_id=e.id
                WHERE e.batch_id=b.id AND d.status='pending') ORDER BY CASE b.mode WHEN 'live' THEN 0 ELSE 1 END,b.id LIMIT 50''').fetchall()
            return [self._internal_batch(r) for r in rows]

    def get_work_item(self,batch_id):
        with self._connect() as c:
            batch=c.execute('SELECT * FROM lab_batches WHERE id=?',(batch_id,)).fetchone()
            if batch is None:
                raise RuleLabError('not_found','实验批次不存在',404)
            experiments=[]
            for row in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id LIMIT 100',(batch_id,)):
                experiments.append(dict(id=row['id'],batch_id=row['batch_id'],name=row['name'],condition=_load(row['condition_json']),
                    settlement_snapshot=_load(row['settlement_snapshot_json']),settlement_rule_id=row['settlement_rule_id'],stats=_load(row['stats_json'])))
            return dict(batch=self._internal_batch(batch),experiments=experiments)

    def get_history(self,predictor_id,*,before_issue,limit=100):
        with self._connect() as c:
            rows=c.execute('''SELECT d.*,p.created_at AS prediction_created_at,p.settled_at AS prediction_settled_at,
                p.big_small,p.odd_even,p.combo,p.conflict AS prediction_conflict FROM lab_draws d
                LEFT JOIN lab_predictions p ON p.predictor_id=? AND p.issue_no=d.issue_no
                WHERE d.issue_key<? ORDER BY d.issue_key DESC LIMIT ?''',(predictor_id,_issue(before_issue),max(1,min(100,int(limit))))).fetchall()
            return [dict(issue_no=r['issue_no'],open_at=r['open_at'],draw_available_at=r['source_created_at'],triplet=_load(r['triplet_json']),
                         prediction_created_at=r['prediction_created_at'],prediction_settled_at=r['prediction_settled_at'],
                         prediction_values={k:r[k] for k in ('big_small','odd_even','combo')},
                         draw_conflict=bool(r['conflict']),prediction_conflict=bool(r['prediction_conflict']),
                         conflict=bool(r['conflict'] or r['prediction_conflict']))
                    for r in reversed(rows)]

    def get_pending_decisions(self,batch_id,*,limit=200):
        with self._connect() as c:
            rows=c.execute('''SELECT d.* FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id
                WHERE e.batch_id=? AND d.status='pending' ORDER BY d.issue_key,d.id LIMIT ?''',(batch_id,max(1,min(200,int(limit))))).fetchall()
            return [{**dict(r),'condition_value':_load(r['condition_value_json'])} for r in rows]

    @staticmethod
    def _cache(c,draws,predictions):
        for row in draws:
            issue=_issue(row['issue_no'])
            c.execute('''INSERT INTO lab_draws(issue_no,issue_key,source_id,open_at,source_created_at,observed_at,triplet_json,input_hash)
                VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(issue_no) DO UPDATE SET conflict=CASE WHEN lab_draws.input_hash!=excluded.input_hash
                THEN 1 ELSE lab_draws.conflict END''',(str(row['issue_no']),issue,row.get('source_id'),row['open_at'],row['source_created_at'],
                row['observed_at'],_dump(row['triplet']),row['input_hash']))
        for row in predictions:
            c.execute('''INSERT INTO lab_predictions(predictor_id,issue_no,issue_key,source_id,created_at,settled_at,observed_at,status,
                big_small,odd_even,combo,input_hash) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(predictor_id,issue_no) DO UPDATE SET
                conflict=CASE WHEN lab_predictions.input_hash!=excluded.input_hash THEN 1 ELSE lab_predictions.conflict END,
                status=CASE WHEN lab_predictions.input_hash=excluded.input_hash AND lab_predictions.status='pending'
                    THEN excluded.status ELSE lab_predictions.status END,
                settled_at=CASE WHEN lab_predictions.input_hash=excluded.input_hash THEN COALESCE(lab_predictions.settled_at,excluded.settled_at)
                    ELSE lab_predictions.settled_at END''',(row['predictor_id'],str(row['issue_no']),_issue(row['issue_no']),row.get('source_id'),
                    row['created_at'],row.get('settled_at'),row['observed_at'],row['status'],row.get('big_small'),row.get('odd_even'),row.get('combo'),row['input_hash']))

    @staticmethod
    def _daily(c,experiment_id,day,**counts):
        columns=('settled_count','hit_count','refund_count','miss_count','direction_hit_count','stake_cents','net_cents','trigger_count','skipped_count')
        values=[counts.get(k,0) for k in columns]
        c.execute('INSERT INTO lab_daily(experiment_id,stat_date,'+','.join(columns)+') VALUES('+','.join('?' for _ in range(11))+') '
                  'ON CONFLICT(experiment_id,stat_date) DO UPDATE SET '+','.join(k+'=lab_daily.'+k+'+excluded.'+k for k in columns),
                  [experiment_id,day]+values)

    def commit_step(self,batch_id,expected_revision,*,cursor,now,draws=(),predictions=(),decisions=(),outcomes=(),progress=None):
        if len(draws)>200 or len(predictions)>200 or len(decisions)>20000 or len(outcomes)>20000:
            raise ValueError('单次处理超过批量限制')
        progress=dict(progress or {})
        permitted={'phase','processed_issues_delta','total_issues','skip_counts_delta','status','error'}
        if set(progress)-permitted or progress.get('status') not in {None,'completed','error','resource_paused'}:
            raise ValueError('无效的进度更新')
        result=dict(committed=False,reason='',inserted_decisions=0,settled=0,skipped=0)
        with self._connect(write=True) as c:
            batch=c.execute('SELECT * FROM lab_batches WHERE id=?',(batch_id,)).fetchone()
            if batch is None:
                raise RuleLabError('not_found','实验批次不存在',404)
            if batch['revision']!=expected_revision:
                return {**result,'reason':'stale_revision'}
            old_cursor=_load(batch['cursor_json'])
            old_watermark=_issue(old_cursor['last_issue_no']) if old_cursor.get('last_issue_no') else 0
            new_watermark=_issue(cursor['last_issue_no']) if cursor.get('last_issue_no') else old_watermark
            if new_watermark<old_watermark or (old_watermark and not cursor.get('last_issue_no')):
                return {**result,'reason':'cursor_regression'}
            if decisions and batch['status']!='running':
                return {**result,'reason':'batch_not_running'}
            reserve_only = False
            if outcomes and not decisions and not predictions:
                pending_keys = {(r['experiment_id'],r['issue_no']) for r in c.execute('''SELECT d.experiment_id,d.issue_no
                    FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id WHERE e.batch_id=? AND d.status='pending' ''',(batch_id,))}
                outcome_keys = {(o['experiment_id'],str(o['issue_no'])) for o in outcomes}
                reserve_only = outcome_keys.issubset(pending_keys) and all(str(d['issue_no']) in {key[1] for key in outcome_keys} for d in draws)
            if (decisions or draws or predictions) and not reserve_only:
                self._require_space()
            self._cache(c,draws,predictions)
            experiments={r['id']:dict(r) for r in c.execute('SELECT * FROM lab_experiments WHERE batch_id=?',(batch_id,))}
            stats={eid:_load(e['stats_json']) for eid,e in experiments.items()}
            dirty=set()
            input_conflict_outcomes=[]
            for decision in sorted(decisions,key=lambda d:(_issue(d['issue_no']),d['experiment_id'])):
                eid=decision['experiment_id']; issue=_issue(decision['issue_no'])
                if eid not in experiments or decision['stake_cents']!=batch['stake_cents'] or decision['decision_mode']!=batch['mode']:
                    raise ValueError('决策与实验不一致')
                if issue<=max(old_watermark,experiments[eid]['last_decided_issue']):
                    continue
                cur=c.execute('''INSERT OR IGNORE INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,stake_cents,
                    decided_at,decision_mode,feature_issue_no,condition_value_json) VALUES(?,?,?,?,?,?,?,?,?)''',
                    (eid,str(decision['issue_no']),issue,decision['bet_value'],decision['stake_cents'],decision['decided_at'],decision['decision_mode'],
                     decision.get('feature_issue_no'),_dump(decision.get('condition_value'))))
                if cur.rowcount:
                    stats[eid]['trigger_count']+=1; dirty.add(eid);result['inserted_decisions']+=1
                    experiments[eid]['last_decided_issue']=issue
                    c.execute('UPDATE lab_experiments SET last_decided_issue=? WHERE id=?',(issue,eid))
                    self._daily(c,eid,_day(decision['decided_at']),trigger_count=1)
                    conflict=c.execute('''SELECT COALESCE((SELECT conflict FROM lab_predictions WHERE predictor_id=? AND issue_no=?),0)
                        OR COALESCE((SELECT conflict FROM lab_draws WHERE issue_no=?),0)''',
                        (batch['predictor_id'],str(decision['issue_no']),str(decision['issue_no']))).fetchone()[0]
                    if conflict:
                        input_conflict_outcomes.append(dict(experiment_id=eid,issue_no=decision['issue_no'],status='skipped',
                            settled_at=now,result_type=None,net_cents=None,direction_hit=False,refund_reason='',skip_reason='input_conflict'))
            for outcome in sorted(input_conflict_outcomes+list(outcomes),key=lambda o:(_issue(o['issue_no']),o['experiment_id'])):
                eid=outcome['experiment_id'];issue=_issue(outcome['issue_no'])
                if eid not in experiments or outcome.get('status') not in {'settled','skipped'}:
                    raise ValueError('结算与实验不一致')
                pending=c.execute("SELECT * FROM lab_decisions WHERE experiment_id=? AND issue_no=? AND status='pending'",(eid,str(outcome['issue_no']))).fetchone()
                if pending is None:
                    continue
                earlier_pending=c.execute("SELECT 1 FROM lab_decisions WHERE experiment_id=? AND status='pending' AND issue_key<? LIMIT 1",(eid,issue)).fetchone()
                if earlier_pending:
                    # Preserve chronological equity/drawdown, including across separate worker ticks.
                    continue
                if outcome['status']=='settled':
                    draw=c.execute('SELECT conflict FROM lab_draws WHERE issue_no=?',(str(outcome['issue_no']),)).fetchone()
                    if draw and draw['conflict']:
                        progress.update(status='error',error='开奖输入冲突，待核对后恢复；已冻结决策仍保留')
                        continue
                    if issue<experiments[eid]['last_settled_issue']:
                        raise ValueError('结算必须按期号顺序处理')
                    stats[eid]=apply_outcome(stats[eid],outcome,stake_cents=pending['stake_cents'])
                    c.execute('UPDATE lab_experiments SET last_settled_issue=? WHERE id=?',(issue,eid))
                    experiments[eid]['last_settled_issue']=issue
                    counts=dict(settled_count=1,stake_cents=pending['stake_cents'],net_cents=outcome['net_cents'],direction_hit_count=int(bool(outcome.get('direction_hit'))))
                    counts[outcome['result_type']+'_count']=1
                    self._daily(c,eid,_day(outcome['settled_at']),**counts)
                    result['settled']+=1
                else:
                    if outcome.get('result_type') is not None or outcome.get('net_cents') is not None:
                        raise ValueError('跳过的决策不能记收益')
                    stats[eid]['skipped_count']+=1;result['skipped']+=1
                    self._daily(c,eid,_day(outcome['settled_at']),skipped_count=1)
                dirty.add(eid)
                c.execute('''UPDATE lab_decisions SET status=?,settled_at=?,finalized_at=?,result_type=?,net_cents=?,refund_reason=?,skip_reason=? WHERE id=?''',
                          (outcome['status'],outcome['settled_at'],now,outcome.get('result_type'),outcome.get('net_cents'),outcome.get('refund_reason',''),outcome.get('skip_reason',''),pending['id']))
            for eid in dirty:
                c.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?',(_dump(stats[eid]),eid))
            skips=_load(batch['skip_counts_json'])
            for key,value in progress.get('skip_counts_delta',{}).items():
                if type(value) is not int or value<0:
                    raise ValueError('跳过计数无效')
                skips[key]=skips.get(key,0)+value
            delta=progress.get('processed_issues_delta',0)
            if type(delta) is not int or delta<0:
                raise ValueError('处理期数无效')
            c.execute('''UPDATE lab_batches SET revision=revision+1,cursor_json=?,updated_at=?,processed_issues=processed_issues+?,
                total_issues=?,skip_counts_json=?,phase=?,status=?,error=? WHERE id=?''',
                (_dump(cursor),now,delta,progress.get('total_issues',batch['total_issues']),_dump(skips),progress.get('phase',batch['phase']),
                 progress.get('status',batch['status']),str(progress.get('error',batch['error']))[:300],batch_id))
            return {**result,'committed':True}

    def prune(self,*,now,limit=500):
        limit=max(1,min(500,int(limit)))
        cutoff=(_time(now)-timedelta(days=self.detail_retention_days)).isoformat().replace('+00:00','Z')
        input_cutoff=(_time(now)-timedelta(days=90)).isoformat().replace('+00:00','Z')
        removed=dict(decisions=0,predictions=0,draws=0)
        with self._connect(write=True) as c:
            cur=c.execute("DELETE FROM lab_decisions WHERE id IN (SELECT id FROM lab_decisions WHERE status IN ('settled','skipped') AND finalized_at<? ORDER BY finalized_at LIMIT ?)",(cutoff,limit))
            removed['decisions']=cur.rowcount
            remaining=limit-cur.rowcount
            # Keep the latest feature window, each pending issue, and active replay warm-up floors.
            latest_floor=c.execute('SELECT issue_key FROM lab_draws ORDER BY issue_key DESC LIMIT 1 OFFSET 99').fetchone()
            floor=latest_floor[0] if latest_floor else 0
            for active in c.execute("SELECT cursor_json FROM lab_batches WHERE mode='backtest' AND status='running'"):
                history_floor=_load(active['cursor_json']).get('history_floor_issue')
                if not history_floor:
                    floor=0  # A newly started replay has not yet selected its warm-up range.
                    break
                floor=min(floor,_issue(history_floor))
            if remaining and floor:
                cur=c.execute('''DELETE FROM lab_predictions WHERE rowid IN (SELECT rowid FROM lab_predictions WHERE observed_at<?
                    AND issue_key<? AND issue_key NOT IN (SELECT issue_key FROM lab_decisions WHERE status='pending') LIMIT ?)''',
                    (input_cutoff,floor,remaining))
                removed['predictions']=cur.rowcount;remaining-=cur.rowcount
                if remaining:
                    cur=c.execute('''DELETE FROM lab_draws WHERE issue_key IN (SELECT issue_key FROM lab_draws WHERE observed_at<?
                        AND issue_key<? AND issue_key NOT IN (SELECT issue_key FROM lab_decisions WHERE status='pending') LIMIT ?)''',
                        (input_cutoff,floor,remaining))
                    removed['draws']=cur.rowcount
            return removed
