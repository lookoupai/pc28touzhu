"""Independent, bounded simulation ledger. Never opens the production ledger."""
from __future__ import annotations

import json
import os
import hashlib
import re
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.domain.rule_lab import ALLOWED_SETTLEMENT_RULE_IDS, apply_outcome, empty_stats
from pc28touzhu.domain.rule_lab_risk import (
    normalize_risk_policy, empty_risk_runtime, empty_risk_day, admit_risk, settle_risk,
)
from pc28touzhu.domain.rule_lab_comparison import risk_variants

SHANGHAI = timezone(timedelta(hours=8))
MAX_TOTAL_EXPERIMENTS = 650
MAX_RUNNING_EXPERIMENTS = 100
LEGACY_REPLAY_NOTICE = '此成绩由旧版按轮回放生成：时间预算耗尽时可能漏过入场机会，需复核后再用于方案比较。原成绩已保留。'
LEGACY_ODDS_NOTICE = '本批次OK游戏网盘使用旧赔率1.98 / 3.60 / 4.20；当前已更正为1.998 / 3.71 / 4.32。历史成绩未重算，不能当作当前赔率的结果。'


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
    def __init__(self, db_path=None, *, budget_bytes=1073741824, detail_retention_days=0):
        root = Path(__file__).resolve().parents[3]
        self.db_path = Path(db_path) if db_path is not None else Path(os.environ.get('RULE_LAB_DATA_DIR', str(root/'data'/'rule_lab')))/'rule_lab.db'
        self.budget_bytes = max(1024, int(budget_bytes))
        # Accept the legacy constructor keyword without re-enabling historical
        # detail retention. Terminal rows are no longer part of saved results.
        self.detail_retention_days = 0
        self._worker_local = threading.local()

    @contextmanager
    def worker_session(self):
        """Reuse one connection for a bounded tick, never its transaction.

        Every _connect operation still has its own visibility/commit boundary.
        Thread-local ownership keeps concurrent API users independent, and nested
        operations use a separate connection rather than borrowing an active one.
        """
        if getattr(self._worker_local, 'session', None) is not None:
            yield
            return
        session = {'connection': None, 'busy': False}
        self._worker_local.session = session
        try:
            yield
        finally:
            self._worker_local.session = None
            if session['connection'] is not None:
                session['connection'].close()

    def _open_connection(self):
        connection = sqlite3.connect(str(self.db_path), timeout=.25)
        try:
            connection.row_factory = sqlite3.Row
            connection.execute('PRAGMA foreign_keys=ON')
            connection.execute('PRAGMA busy_timeout=250')
            connection.execute('PRAGMA cache_size=-2048')
            connection.execute('PRAGMA wal_autocheckpoint=64')
            connection.execute('PRAGMA journal_size_limit=1048576')
            return connection
        except Exception:
            connection.close()
            raise

    @contextmanager
    def _connect(self, *, write=False, reserve=False):
        conn = None
        session = getattr(self._worker_local, 'session', None)
        pooled = session is not None and not session['busy']
        try:
            if pooled:
                if session['connection'] is None:
                    session['connection'] = self._open_connection()
                conn = session['connection']
                session['busy'] = True
            else:
                conn = self._open_connection()
            conn.execute('PRAGMA query_only=' + ('OFF' if write else 'ON'))
            if write:
                current = conn.execute('PRAGMA page_count').fetchone()[0]
                size = conn.execute('PRAGMA page_size').fetchone()[0]
                max_pages = max(current, int(self.budget_bytes*(.9 if reserve else .8))//size, 1)
                conn.execute('PRAGMA max_page_count=%s' % max_pages)
                conn.execute('BEGIN IMMEDIATE')
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
                if pooled:
                    # Snapshot reads may explicitly BEGIN. End them here so the
                    # next operation sees an external pause/edit and WAL can drain.
                    if conn.in_transaction:
                        conn.rollback()
                    session['busy'] = False
                else:
                    conn.close()

    def initialize(self):
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        # journal mode cannot be changed inside the schema transaction.
        with sqlite3.connect(str(self.db_path), timeout=.25) as connection:
            if not connection.execute("SELECT 1 FROM sqlite_master WHERE type='table' LIMIT 1").fetchone():
                connection.execute('PRAGMA auto_vacuum=INCREMENTAL')
            connection.execute('PRAGMA journal_mode=WAL')
        with self._connect(write=True) as c:
            version = c.execute('PRAGMA user_version').fetchone()[0]
            if version not in (0, 1, 2, 3, 4, 5, 6):
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
            if version==0:
                # Existing ledgers retain their indexes until bounded terminal
                # cleanup finishes. Reinitialization must not scan old history
                # or recreate the retired finished-record indexes.
                c.execute("CREATE INDEX IF NOT EXISTS lab_decisions_pending ON lab_decisions(status,issue_key) WHERE status='pending'")
            additions={
                'lab_batches': [('risk_policy_json',"TEXT NOT NULL DEFAULT '{\"version\":1,\"mode\":\"legacy\"}'")],
                'lab_experiments':[('risk_runtime_json',"TEXT NOT NULL DEFAULT '{}'" )],
                'lab_decisions':[('round_no','INTEGER'),('risk_date','TEXT')],
            }
            for table,columns in additions.items():
                existing={r[1] for r in c.execute('PRAGMA table_info('+table+')')}
                for name,definition in columns:
                    if name not in existing:c.execute('ALTER TABLE '+table+' ADD COLUMN '+name+' '+definition)
            c.execute('''CREATE TABLE IF NOT EXISTS lab_risk_days (
                experiment_id INTEGER NOT NULL REFERENCES lab_experiments(id),stat_date TEXT NOT NULL,
                state_json TEXT NOT NULL,PRIMARY KEY(experiment_id,stat_date))''')
            c.execute('''CREATE TABLE IF NOT EXISTS lab_rounds (
                experiment_id INTEGER NOT NULL REFERENCES lab_experiments(id),round_no INTEGER NOT NULL,
                status TEXT NOT NULL,summary_json TEXT NOT NULL,finalized_at TEXT,
                PRIMARY KEY(experiment_id,round_no))''')
            c.execute('''CREATE TABLE IF NOT EXISTS lab_studies (
                id INTEGER PRIMARY KEY,user_id INTEGER NOT NULL,request_id TEXT NOT NULL,name TEXT NOT NULL,
                source_batch_id INTEGER NOT NULL REFERENCES lab_batches(id),request_hash TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'paused',queued_at TEXT,queue_order INTEGER NOT NULL DEFAULT 0,cutoff_issue TEXT,cutoff_at TEXT,
                error TEXT NOT NULL DEFAULT '',created_at TEXT NOT NULL,updated_at TEXT NOT NULL,
                UNIQUE(user_id,request_id))''')
            c.execute('CREATE INDEX IF NOT EXISTS lab_studies_queue ON lab_studies(status,queued_at,id)')
            c.execute('''CREATE TABLE IF NOT EXISTS lab_study_batches (
                study_id INTEGER NOT NULL REFERENCES lab_studies(id),batch_id INTEGER NOT NULL UNIQUE REFERENCES lab_batches(id),
                variant_key TEXT NOT NULL,position INTEGER NOT NULL,label TEXT NOT NULL,change_json TEXT NOT NULL,
                PRIMARY KEY(study_id,variant_key))''')
            from pc28touzhu.services.rule_lab_archive import init_schema as archive_schema
            from pc28touzhu.services.rule_lab_series import init_schema as series_schema
            archive_schema(c)
            series_schema(c)
            from pc28touzhu.services.rule_lab_management import init_schema as management_schema
            management_schema(c)
            from pc28touzhu.services.rule_lab_retention import init_schema as retention_schema
            retention_schema(c)
            c.execute('PRAGMA user_version=6')

    def storage_status(self):
        from pc28touzhu.services.rule_lab_storage import storage_status
        return storage_status(self)

    def _require_space(self,*,reserve=False):
        from pc28touzhu.services.rule_lab_storage import directory_bytes
        if directory_bytes(self.db_path.parent) >= int(self.budget_bytes*(.9 if reserve else .8)):
            raise RuleLabError('storage_limit', '模拟存储接近预算上限，请清理已完成明细后恢复', 503)

    @staticmethod
    def _owned(c, user_id, batch_id):
        row = c.execute('SELECT * FROM lab_batches WHERE id=? AND user_id=?', (batch_id,user_id)).fetchone()
        if row is None:
            raise RuleLabError('not_found', '实验批次不存在', 404)
        return row

    @staticmethod
    def _replay_review_required(row):
        return (row['mode']=='backtest' and row['processed_issues']>0
                and _load(row['risk_policy_json']).get('mode')=='round'
                and _load(row['cursor_json']).get('replay_scheduler_version',1)<2)

    @staticmethod
    def _batch(c, row):
        count = c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(row['id'],)).fetchone()[0]
        relation=c.execute('SELECT study_id,variant_key FROM lab_study_batches WHERE batch_id=?',(row['id'],)).fetchone()
        saved=c.execute("SELECT settlement_snapshot_json FROM lab_experiments WHERE batch_id=? AND settlement_rule_id='pc28_netdisk_regular' ORDER BY id LIMIT 1",(row['id'],)).fetchone()
        legacy_odds=bool(saved and RuleLabStore._legacy_netdisk_odds(_load(saved[0])))
        result=dict(id=row['id'],name=row['name'],mode=row['mode'],predictor_id=row['predictor_id'],predictor_name=row['predictor_name'],
                    metric=row['metric'],template_pack=row['template_pack'],stake_amount=row['stake_cents']/100,
                    start_date=row['start_date'],end_date=row['end_date'],status=row['status'],phase=row['phase'],
                    experiment_count=count,processed_issues=row['processed_issues'],total_issues=row['total_issues'],
                    created_at=row['created_at'],started_at=row['started_at'],updated_at=row['updated_at'],error=row['error'],
                    skip_counts=_load(row['skip_counts_json']),
                    history_notice=(LEGACY_ODDS_NOTICE+' ' if legacy_odds else '')+(LEGACY_REPLAY_NOTICE+' ' if RuleLabStore._replay_review_required(row) else '')+('历史回放使用现存预测记录，不能证明历史预测版本从未修改。' if row['mode']=='backtest' else ''),
                    legacy_odds=legacy_odds,
                    review_required=RuleLabStore._replay_review_required(row),
                    risk_policy=_load(row['risk_policy_json']),risk_editable=RuleLabStore._risk_editable(c,row),
                    study_id=relation['study_id'] if relation else None,variant_key=relation['variant_key'] if relation else None)
        from pc28touzhu.services.rule_lab_series import public_summary
        result['continuation']=public_summary(c,row)
        return result

    @staticmethod
    def _risk_editable(c,row):
        return (not c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(row['id'],)).fetchone()
                and row['status']=='paused' and row['started_at'] is None and row['processed_issues']==0
                and not c.execute('''SELECT 1 FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id
                    WHERE e.batch_id=? LIMIT 1''',(row['id'],)).fetchone())

    def set_risk_policy(self,user_id,batch_id,policy,*,now):
        policy=normalize_risk_policy(policy)
        with self._connect(write=True) as c:
            row=self._owned(c,user_id,batch_id)
            if c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(batch_id,)).fetchone():
                raise RuleLabError('study_managed','比较组子批次由比较队列统一管理，不能单独修改',409)
            if not self._risk_editable(c,row):
                raise RuleLabError('risk_policy_locked','只有尚未启动且无模拟记录的暂停批次可以修改风控，请新建批次比较其他口径',409)
            c.execute('UPDATE lab_batches SET risk_policy_json=?,revision=revision+1,updated_at=? WHERE id=?',(_dump(policy),now,batch_id))
            return self._batch(c,self._owned(c,user_id,batch_id))

    @staticmethod
    def _risk_day(c,eid,day):
        row=c.execute('SELECT state_json FROM lab_risk_days WHERE experiment_id=? AND stat_date=?',(eid,day)).fetchone()
        return _load(row[0]) if row else empty_risk_day(day)

    @staticmethod
    def _save_risk_day(c,eid,day):
        c.execute('''INSERT INTO lab_risk_days(experiment_id,stat_date,state_json) VALUES(?,?,?)
            ON CONFLICT(experiment_id,stat_date) DO UPDATE SET state_json=excluded.state_json''',(eid,day['stat_date'],_dump(day)))

    @staticmethod
    def _save_round(c,eid,summary,now):
        if summary is None:return
        if summary['status']=='closed':
            # The completed round is already part of risk_runtime's counters.
            # Keep only the in-flight row needed to resume the current round.
            c.execute('DELETE FROM lab_rounds WHERE experiment_id=? AND round_no=?',(eid,summary['round_no']))
            return
        c.execute('''INSERT INTO lab_rounds(experiment_id,round_no,status,summary_json,finalized_at) VALUES(?,?,?,?,?)
            ON CONFLICT(experiment_id,round_no) DO UPDATE SET status=excluded.status,summary_json=excluded.summary_json,
            finalized_at=COALESCE(lab_rounds.finalized_at,excluded.finalized_at)''',
            (eid,summary['round_no'],summary['status'],_dump(summary),now if summary['status']=='closed' else None))

    @staticmethod
    def _risk_view(c,row):
        batch=c.execute('SELECT risk_policy_json,status,mode FROM lab_batches WHERE id=?',(row['batch_id'],)).fetchone()
        policy=_load(batch['risk_policy_json']);runtime={**empty_risk_runtime(),**_load(row['risk_runtime_json'])}
        active=runtime['active_round'];day=RuleLabStore._risk_day(c,row['id'],runtime['latest_day'])
        reason=day['stop_reason'] or runtime['last_stop_reason']
        state='legacy' if policy['mode']=='legacy' else 'active' if active else 'day_stopped' if day['stop_reason'] else 'waiting'
        if policy['mode']=='round' and not active and not day['stop_reason']:
            if any(control['enabled'] and control['max_rounds'] and day['round_count']>=control['max_rounds']
                   for control in [policy['day']]+policy.get('additional_day_guards',[])):
                state='day_stopped';reason='day_max_rounds'
        if active and batch['status']=='completed':state='unfinished'
        return dict(mode=policy['mode'],state=state,day_date=runtime['latest_day'] or None,day_net_profit=day['net_cents']/100,
                    day_round_count=day['round_count'],day_bet_count=day['bet_count'],round_no=active['round_no'] if active else None,
                    round_net_profit=active['net_cents']/100 if active else None,round_bet_count=active['bet_count'] if active else 0,
                    stop_reason=reason,started_rounds=runtime['round_seq'],
                    **{k:runtime[k] for k in ('completed_rounds','profit_rounds','loss_rounds','flat_rounds','invalid_rounds','round_profit_stops','round_loss_stops')})

    @staticmethod
    def _legacy_netdisk_odds(snapshot):
        odds=snapshot.get('resolved_odds') or snapshot.get('implemented_odds') or {}
        combo=odds.get('combo') or {}
        return snapshot.get('settlement_rule_id')=='pc28_netdisk_regular' and (
            odds.get('big_small')==1.98 or odds.get('odd_even')==1.98 or
            any(combo.get(value)==old for value,old in [('小单',3.6),('大双',3.6),('大单',4.2),('小双',4.2)]))

    @staticmethod
    def _experiment(c,row):
        stats = _load(row['stats_json'])
        snapshot = _load(row['settlement_snapshot_json'])
        return dict(id=row['id'],batch_id=row['batch_id'],name=row['name'],condition=_load(row['condition_json']),
                    settlement_rule_id=row['settlement_rule_id'],settlement_rule_name=snapshot['rule']['name'],
                    settlement_odds=snapshot.get('resolved_odds') or snapshot.get('implemented_odds') or {},
                    legacy_odds=RuleLabStore._legacy_netdisk_odds(snapshot),
                    **{key:stats[key] for key in ('settled_count','hit_count','refund_count','miss_count','direction_hit_count',
                                                  'max_loss_streak','trigger_count','skipped_count')},
                    stake_total=stats['stake_cents']/100,net_profit=stats['net_cents']/100,
                    roi_percent=round(100*stats['net_cents']/stats['stake_cents'],2) if stats['stake_cents'] else None,
                    max_drawdown=stats['max_drawdown_cents']/100,sample_label='样本较少' if stats['settled_count']<100 else '仍需验证',
                    risk_state=RuleLabStore._risk_view(c,row))

    def create_batch(self, user_id, config, predictor_name, experiments, *, now, replay_input_basis=None):
        if replay_input_basis not in (None,'current_source'):
            raise RuleLabError('invalid_input','重测输入基础无效')
        if not 1 <= len(experiments) <= 100 or any(e['settlement_rule_id'] not in ALLOWED_SETTLEMENT_RULE_IDS for e in experiments):
            raise RuleLabError('invalid_input','实验仅支持两套OK规则，且每批最多100个版本')
        with self._connect(write=True) as c:
            self._require_space()
            if c.execute('SELECT COUNT(*) FROM lab_experiments').fetchone()[0]+len(experiments)>MAX_TOTAL_EXPERIMENTS:
                raise RuleLabError('capacity_exceeded',f'全服务最多保存{MAX_TOTAL_EXPERIMENTS}个实验版本',409)
            if c.execute('SELECT COUNT(*) FROM lab_batches WHERE user_id=?',(user_id,)).fetchone()[0]>=50:
                raise RuleLabError('capacity_exceeded','每位用户最多保存50个实验批次',409)
            params = [user_id,config['name'],config['mode'],config['predictor_id'],str(predictor_name),config['metric'],
                      config['template_pack'],config['stake_cents'],config['start_date'],config['end_date'],now,now]
            from pc28touzhu.services.rule_lab_management import allocate_id
            bid = allocate_id(c,'lab_batches')
            c.execute('''INSERT INTO lab_batches (id,user_id,name,mode,predictor_id,predictor_name,metric,template_pack,
                stake_cents,start_date,end_date,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',[bid]+params)
            c.execute('UPDATE lab_batches SET risk_policy_json=? WHERE id=?',(_dump(normalize_risk_policy(config.get('risk_policy'))),bid))
            if replay_input_basis is not None:
                c.execute('UPDATE lab_batches SET cursor_json=? WHERE id=?',
                    (_dump(dict(replay_input_basis=replay_input_basis,replay_require_coverage=True)),bid))
            for e in experiments:
                if e['settlement_snapshot'].get('settlement_rule_id') != e['settlement_rule_id']:
                    raise ValueError('冻结规则与实验不一致')
                c.execute('''INSERT INTO lab_experiments(id,batch_id,condition_key,name,condition_json,settlement_rule_id,
                    settlement_snapshot_json,stats_json) VALUES(?,?,?,?,?,?,?,?)''',
                    (allocate_id(c,'lab_experiments'),bid,e['condition_key'],e['name'],_dump(e['condition']),e['settlement_rule_id'],_dump(e['settlement_snapshot']),_dump(empty_stats())))
            return self._batch(c,self._owned(c,user_id,bid))

    def set_batch_state(self,user_id,batch_id,state,*,now):
        if state not in ('running','paused'):
            raise RuleLabError('invalid_input','状态只支持运行或暂停')
        with self._connect(write=True) as c:
            row = self._owned(c,user_id,batch_id)
            if row['status']=='deleting':raise RuleLabError('deleting','实验正在删除',409)
            if c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(batch_id,)).fetchone():
                raise RuleLabError('study_managed','比较组子批次请从比较队列启动或暂停',409)
            from pc28touzhu.services.rule_lab_series import series_for_batch,set_active_state
            series=series_for_batch(c,batch_id)
            if series:
                set_active_state(self,c,series,state,now)
                return self._batch(c,self._owned(c,user_id,batch_id))
            if row['status']==state:
                return self._batch(c,row)
            if row['status']=='completed':
                raise RuleLabError('invalid_transition','已完成的历史回放不能重新启动，请新建批次',409)
            if state=='running':
                normalize_risk_policy(_load(row['risk_policy_json']))
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

    @staticmethod
    def _capacity(c):
        saved=c.execute('SELECT COUNT(*) FROM lab_experiments').fetchone()[0]
        running=c.execute("SELECT COUNT(*) FROM lab_experiments e JOIN lab_batches b ON b.id=e.batch_id WHERE b.status='running'").fetchone()[0]
        return dict(saved=saved,running=running,max_saved=MAX_TOTAL_EXPERIMENTS,max_running=MAX_RUNNING_EXPERIMENTS,
                    remaining_saved=max(0,MAX_TOTAL_EXPERIMENTS-saved),remaining_running=max(0,MAX_RUNNING_EXPERIMENTS-running))

    def capacity(self):
        with self._connect() as c:return self._capacity(c)

    def _comparison_source(self,c,user_id,batch_id):
        batch=self._owned(c,user_id,batch_id)
        if batch['status']=='deleting':raise RuleLabError('deleting','实验正在删除',409)
        if batch['mode']!='backtest' or _load(batch['risk_policy_json']).get('mode')!='round':
            raise RuleLabError('invalid_input','比较组只支持历史回放且已配置按轮风控的批次')
        if c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(batch_id,)).fetchone():
            raise RuleLabError('study_managed','请选择独立批次作为比较基准',409)
        experiments=c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id',(batch_id,)).fetchall()
        if not experiments or len(experiments)>100:raise RuleLabError('invalid_input','原批次实验数量无效')
        pairs={}
        for e in experiments:pairs.setdefault(e['condition_key'],set()).add(e['settlement_rule_id'])
        if any(rules!=set(ALLOWED_SETTLEMENT_RULE_IDS) for rules in pairs.values()) or len(experiments)!=2*len(pairs):
            raise RuleLabError('invalid_input','原批次必须包含每个条件的两套OK冻结结算')
        return batch,experiments

    def comparison_preview(self,user_id,batch_id):
        with self._connect() as c:
            batch,experiments=self._comparison_source(c,user_id,batch_id)
            variants=risk_variants(_load(batch['risk_policy_json']))
            count=sum(v['available'] for v in variants)
            return dict(source_batch_id=batch_id,variants=variants,candidate_count=count,new_experiments=count*len(experiments),
                        capacity=self._capacity(c),source_name=batch['name'])

    @staticmethod
    def _owned_study(c,user_id,study_id):
        row=c.execute('SELECT * FROM lab_studies WHERE id=? AND user_id=?',(study_id,user_id)).fetchone()
        if row is None:raise RuleLabError('not_found','比较组不存在',404)
        return row

    @staticmethod
    def _study(c,row):
        children=[];signatures=[];coverage=[];conflict=False;review_required=False
        for child in c.execute('''SELECT b.*,s.variant_key,s.position,s.label,s.change_json FROM lab_study_batches s
            JOIN lab_batches b ON b.id=s.batch_id WHERE s.study_id=? ORDER BY s.position''',(row['id'],)):
            evidence=_load(child['cursor_json']).get('evidence') or {}
            count=c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(child['id'],)).fetchone()[0]
            children.append(dict(batch_id=child['id'],variant_key=child['variant_key'],label=child['label'],status=child['status'],
                processed_issues=child['processed_issues'],experiment_count=count,changes=_load(child['change_json']),error=child['error']))
            signatures.append((evidence.get('digest'),evidence.get('count',0),evidence.get('warm_count',0),
                               evidence.get('first_issue'),evidence.get('last_issue'),json.dumps(_load(child['skip_counts_json']),sort_keys=True)))
            conflict|=bool(evidence.get('conflict'))
            review_required|=RuleLabStore._replay_review_required(child)
            coverage.append(dict(variant_key=child['variant_key'],first_issue=evidence.get('first_issue'),last_issue=evidence.get('last_issue'),
                count=evidence.get('count',0),warm_count=evidence.get('warm_count',0),digest=evidence.get('digest'),
                skip_counts=_load(child['skip_counts_json'])))
        complete=sum(child['status']=='completed' for child in children)
        comparable=bool(children and complete==len(children) and not conflict and signatures[0][0] and signatures[0][1]>0 and len(set(signatures))==1)
        reason='' if comparable else '全部方案完成后才计算相对基准差异' if complete<len(children) else '输入存在修订或冲突，不可直接比较' if conflict else '实际输入指纹或日期覆盖不一致，或没有有效历史样本'
        if review_required:comparable=False;reason=LEGACY_REPLAY_NOTICE
        capacity=RuleLabStore._capacity(c)
        needed=next((child['experiment_count'] for child in children if child['status']!='completed'),0)
        waiting=''
        if row['status']=='queued':
            waiting='等待其他批次释放运行额度（需要%s，当前剩余%s）'%(needed,capacity['remaining_running']) if needed>capacity['remaining_running'] else '等待队列调度'+('并冻结共同历史截止' if not row['cutoff_issue'] else '')
        result=dict(id=row['id'],name=row['name'],source_batch_id=row['source_batch_id'],status=row['status'],created_at=row['created_at'],
                    updated_at=row['updated_at'],cutoff_issue=row['cutoff_issue'],cutoff_at=row['cutoff_at'],error=row['error'],waiting_reason=waiting,
                    children=children,completed_children=complete,total_children=len(children),comparable=comparable,comparison_reason=reason,actual_coverage=coverage,review_required=review_required)
        from pc28touzhu.services.rule_lab_series import study_summary
        continuation=study_summary(c,row)
        result['continuation']=continuation
        if continuation['active_segment_id']:
            result['comparable']=False
            result['comparison_reason']='当前追加尚未全部完成，请查看上一共同完成段'
        return result

    def create_study(self,user_id,source_batch_id,name,request_id,*,now):
        if type(source_batch_id) is not int or source_batch_id<=0 or not isinstance(name,str) or not 1<=len(name.strip())<=80:
            raise RuleLabError('invalid_input','比较组名称或来源批次无效')
        if not isinstance(request_id,str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,80}',request_id):
            raise RuleLabError('invalid_input','创建请求标识无效')
        name=name.strip();request_hash=hashlib.sha256(_dump([source_batch_id,name]).encode()).hexdigest()
        with self._connect(write=True) as c:
            existing=c.execute('SELECT * FROM lab_studies WHERE user_id=? AND request_id=?',(user_id,request_id)).fetchone()
            if existing:
                if existing['request_hash']!=request_hash:raise RuleLabError('request_conflict','同一请求标识不能用于不同的比较组',409)
                return dict(study=self._study(c,existing),created=False)
            batch,experiments=self._comparison_source(c,user_id,source_batch_id)
            variants=[v for v in risk_variants(_load(batch['risk_policy_json'])) if v['available']]
            if len(variants)<2:raise RuleLabError('invalid_input','至少需要两种不同且有效的风控候选')
            self._require_space()
            if self._capacity(c)['saved']+len(variants)*len(experiments)>MAX_TOTAL_EXPERIMENTS:
                raise RuleLabError('capacity_exceeded',f'整组创建超过{MAX_TOTAL_EXPERIMENTS}个保存版本上限，未创建任何子批次',409)
            if c.execute('SELECT COUNT(*) FROM lab_batches WHERE user_id=?',(user_id,)).fetchone()[0]+len(variants)>50:
                raise RuleLabError('capacity_exceeded','整组创建超过当前账号50个批次上限',409)
            from pc28touzhu.services.rule_lab_management import allocate_id
            sid=allocate_id(c,'lab_studies')
            c.execute('''INSERT INTO lab_studies(id,user_id,request_id,name,source_batch_id,request_hash,created_at,updated_at)
                VALUES(?,?,?,?,?,?,?,?)''',(sid,user_id,request_id,name,source_batch_id,request_hash,now,now))
            for position,variant in enumerate(variants):
                bid=allocate_id(c,'lab_batches')
                c.execute('''INSERT INTO lab_batches(id,user_id,name,mode,predictor_id,predictor_name,metric,template_pack,stake_cents,
                    start_date,end_date,created_at,updated_at,risk_policy_json) VALUES(?,?,?,'backtest',?,?,?,?,?,?,?,?,?,?)''',
                    (bid,user_id,(name+' · '+variant['label'])[:80],batch['predictor_id'],batch['predictor_name'],batch['metric'],batch['template_pack'],
                     batch['stake_cents'],batch['start_date'],batch['end_date'],now,now,_dump(variant['policy'])))
                for experiment in experiments:
                    c.execute('''INSERT INTO lab_experiments(id,batch_id,condition_key,name,condition_json,settlement_rule_id,
                        settlement_snapshot_json,stats_json) VALUES(?,?,?,?,?,?,?,?)''',(allocate_id(c,'lab_experiments'),bid,experiment['condition_key'],experiment['name'],
                        experiment['condition_json'],experiment['settlement_rule_id'],experiment['settlement_snapshot_json'],_dump(empty_stats())))
                c.execute('INSERT INTO lab_study_batches(study_id,batch_id,variant_key,position,label,change_json) VALUES(?,?,?,?,?,?)',
                    (sid,bid,variant['key'],position,variant['label'],_dump(variant['changes'])))
            return dict(study=self._study(c,self._owned_study(c,user_id,sid)),created=True)

    def list_studies(self,user_id):
        with self._connect() as c:return [self._study(c,r) for r in c.execute('SELECT * FROM lab_studies WHERE user_id=? ORDER BY id DESC LIMIT 50',(user_id,)).fetchall()]

    def get_study(self,user_id,study_id):
        with self._connect() as c:return self._study(c,self._owned_study(c,user_id,study_id))

    def inspect_continuation_checkpoint(self,user_id,study_id):
        """Internal read-only preflight; no API or runtime state transitions yet."""
        from pc28touzhu.domain.rule_lab_continuation import checkpoint_readiness
        with self._connect() as c:
            c.execute('BEGIN')
            study=self._study(c,self._owned_study(c,user_id,study_id))
            batches=[];experiments=[];pending_count=0
            for child in study['children']:
                row=c.execute('SELECT * FROM lab_batches WHERE id=?',(child['batch_id'],)).fetchone()
                batch=self._internal_batch(row);batch['experiment_count']=child['experiment_count'];batches.append(batch)
                for item in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id',(row['id'],)):
                    exp=dict(item)
                    for key in ('condition','settlement_snapshot','stats','risk_runtime'):
                        exp[key]=_load(exp.pop(key+'_json'))
                    experiments.append(exp)
                pending_count+=c.execute('''SELECT COUNT(*) FROM lab_decisions d JOIN lab_experiments e ON e.id=d.experiment_id
                    WHERE e.batch_id=? AND d.status='pending' ''',(row['id'],)).fetchone()[0]
            history=[]
            if batches and batches[0]['cursor'].get('last_issue_no'):
                first=batches[0]
                rows=c.execute('''SELECT d.*,p.created_at AS prediction_created_at,p.settled_at AS prediction_settled_at,
                    p.status AS prediction_status,p.big_small,p.odd_even,p.combo,p.conflict AS prediction_conflict
                    FROM lab_draws d LEFT JOIN lab_predictions p ON p.predictor_id=? AND p.issue_no=d.issue_no
                    WHERE d.issue_key<=? ORDER BY d.issue_key DESC LIMIT 100''',
                    (first['predictor_id'],_issue(first['cursor']['last_issue_no']))).fetchall()
                history=[dict(r) for r in reversed(rows)]
                for item in history:item['conflict']=bool(item['conflict'] or item['prediction_conflict'])
            return checkpoint_readiness(study,batches,experiments,history,pending_count=pending_count)

    @staticmethod
    def _enqueue(c,study_id,now):
        seq=c.execute('SELECT COALESCE(MAX(queue_order),0)+1 FROM lab_studies').fetchone()[0]
        c.execute("UPDATE lab_studies SET status='queued',queued_at=?,queue_order=?,updated_at=?,error='' WHERE id=?",(now,seq,now,study_id))

    def set_study_state(self,user_id,study_id,state,*,now):
        if state not in ('running','paused'):raise RuleLabError('invalid_input','比较组状态只支持启动或暂停')
        with self._connect(write=True) as c:
            study=self._owned_study(c,user_id,study_id)
            if study['status']=='deleting':raise RuleLabError('deleting','比较组正在删除',409)
            from pc28touzhu.services.rule_lab_series import set_active_state
            series=c.execute("SELECT * FROM lab_series WHERE owner_kind='study' AND owner_id=?",(study_id,)).fetchone()
            if series:
                set_active_state(self,c,series,state,now)
                return self._study(c,self._owned_study(c,user_id,study_id))
            if study['status']=='completed':raise RuleLabError('invalid_transition','比较组已完成，不能重置成绩',409)
            if state=='paused':
                c.execute("UPDATE lab_studies SET status='paused',updated_at=? WHERE id=?",(now,study_id))
                c.execute("UPDATE lab_batches SET status='paused',revision=revision+1,updated_at=? WHERE status='running' AND id IN (SELECT batch_id FROM lab_study_batches WHERE study_id=?)",(now,study_id))
            elif study['status'] not in ('queued','running'):
                self._require_space();self._enqueue(c,study_id,now)
            return self._study(c,self._owned_study(c,user_id,study_id))

    def studies_need_cutoff(self):
        with self._connect() as c:return bool(c.execute("SELECT 1 FROM lab_studies WHERE status='queued' AND cutoff_issue IS NULL LIMIT 1").fetchone())

    def advance_studies(self,*,now,cutoff=None):
        with self._connect(write=True) as c:
            for study in c.execute("SELECT * FROM lab_studies WHERE status='running' AND id NOT IN (SELECT owner_id FROM lab_series WHERE owner_kind='study') ORDER BY id").fetchall():
                children=c.execute('''SELECT b.* FROM lab_batches b JOIN lab_study_batches s ON s.batch_id=b.id
                    WHERE s.study_id=? ORDER BY s.position''',(study['id'],)).fetchall()
                failed=next((b for b in children if b['status'] in ('error','resource_paused')),None)
                if failed:
                    c.execute('UPDATE lab_studies SET status=?,error=?,updated_at=? WHERE id=?',(failed['status'],failed['error'],now,study['id']))
                elif all(b['status']=='completed' for b in children):
                    c.execute("UPDATE lab_studies SET status='completed',updated_at=? WHERE id=?",(now,study['id']))
                elif not any(b['status']=='running' for b in children):self._enqueue(c,study['id'],now)
            for study in c.execute("SELECT * FROM lab_studies WHERE status='queued' AND id NOT IN (SELECT owner_id FROM lab_series WHERE owner_kind='study') ORDER BY queue_order,id").fetchall():
                child=c.execute('''SELECT b.* FROM lab_study_batches s JOIN lab_batches b ON b.id=s.batch_id
                    WHERE s.study_id=? AND b.status!='completed' ORDER BY s.position LIMIT 1''',(study['id'],)).fetchone()
                if child is None:
                    c.execute("UPDATE lab_studies SET status='completed',updated_at=? WHERE id=?",(now,study['id']));continue
                count=c.execute('SELECT COUNT(*) FROM lab_experiments WHERE batch_id=?',(child['id'],)).fetchone()[0]
                if self._capacity(c)['running']+count>MAX_RUNNING_EXPERIMENTS:break
                if not study['cutoff_issue'] and not cutoff:break
                try:self._require_space()
                except RuleLabError as exc:
                    c.execute("UPDATE lab_studies SET status='resource_paused',error=?,updated_at=? WHERE id=?",(exc.message,now,study['id']));break
                if not study['cutoff_issue']:
                    _issue(cutoff['issue_no']);_time(cutoff['at'])
                    c.execute('UPDATE lab_studies SET cutoff_issue=?,cutoff_at=? WHERE id=?',(cutoff['issue_no'],cutoff['at'],study['id']))
                if _load(child['cursor_json']).get('input_expired'):
                    c.execute("UPDATE lab_studies SET status='error',error='所需输入已清理，请在保留区间重新创建比较组',updated_at=? WHERE id=?",(now,study['id']));continue
                c.execute("UPDATE lab_batches SET status='running',revision=revision+1,started_at=COALESCE(started_at,?),updated_at=?,error='' WHERE id=?",(now,now,child['id']))
                c.execute("UPDATE lab_studies SET status='running',updated_at=?,error='' WHERE id=?",(now,study['id']))

    def study_comparison(self,user_id,study_id,*,observation_months=None,observation_partial=None,now=None):
        with self._connect() as c:
            observing=observation_months is not None or observation_partial is not None
            if observing:c.execute('BEGIN')
            study=self._study(c,self._owned_study(c,user_id,study_id));rows=[];baseline={}
            for child in study['children']:
                batch=c.execute('SELECT risk_policy_json FROM lab_batches WHERE id=?',(child['batch_id'],)).fetchone()
                for experiment in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id',(child['batch_id'],)).fetchall():
                    exp=self._experiment(c,experiment)
                    days=c.execute('''SELECT MIN(net_cents) AS worst,SUM(CASE WHEN net_cents>0 THEN 1 ELSE 0 END) AS profitable,
                        COUNT(*) AS betting FROM lab_daily WHERE experiment_id=? AND stake_cents>0''',(exp['id'],)).fetchone()
                    row=dict(exp,experiment_id=exp['id'],condition_key=experiment['condition_key'],variant_key=child['variant_key'],variant_label=child['label'],
                        risk_policy=_load(batch['risk_policy_json']),worst_day_profit=days['worst']/100 if days['worst'] is not None else None,
                        profitable_days=days['profitable'] or 0,betting_days=days['betting'],completed_rounds=exp['risk_state']['completed_rounds'],
                        comparable=study['comparable'],delta_net_profit=None,delta_roi_pp=None,delta_drawdown=None)
                    key=(row['condition_key'],row['settlement_rule_id'])
                    if child['variant_key']=='baseline':baseline[key]=row
                    rows.append(row)
            if study['comparable']:
                for row in rows:
                    base=baseline[(row['condition_key'],row['settlement_rule_id'])]
                    if not row['settled_count'] or not base['settled_count']:continue
                    row.update(delta_net_profit=round(row['net_profit']-base['net_profit'],2),
                        delta_roi_pp=round(row['roi_percent']-base['roi_percent'],2) if row['roi_percent'] is not None and base['roi_percent'] is not None else None,
                        delta_drawdown=round(row['max_drawdown']-base['max_drawdown'],2))
            if observing:
                from pc28touzhu.services.rule_lab_observation import load_observations
                child_ids=[child['batch_id'] for child in study['children']]
                marks=','.join('?' for _ in child_ids)
                batches=c.execute('SELECT * FROM lab_batches WHERE id IN ('+marks+') ORDER BY id',child_ids).fetchall()
                experiments=c.execute('SELECT * FROM lab_experiments WHERE batch_id IN ('+marks+') ORDER BY id',child_ids).fetchall()
                observations=load_observations(self,c,batches,experiments,months=observation_months,
                    include_partial=observation_partial,now=now or datetime.now(timezone.utc).isoformat(),
                    study_comparable=study['comparable'])
                for row in rows:row['observation']=observations[row['experiment_id']]
            return dict(study=study,rows=rows)

    def get_batch(self,user_id,batch_id):
        with self._connect() as c:
            return self._batch(c,self._owned(c,user_id,batch_id))

    def list_experiments(self,user_id,batch_id):
        with self._connect() as c:
            self._owned(c,user_id,batch_id)
            return [self._experiment(c,r) for r in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id LIMIT 100',(batch_id,))]

    def batch_observation_detail(self,user_id,batch_id,*,months,include_partial,now=None):
        from pc28touzhu.services.rule_lab_observation import load_observations
        with self._connect() as c:
            c.execute('BEGIN')
            batch=self._owned(c,user_id,batch_id)
            public=self._batch(c,batch)
            experiments=c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id LIMIT 100',(batch_id,)).fetchall()
            rows=[self._experiment(c,row) for row in experiments]
            observations=load_observations(self,c,[batch],experiments,months=months,include_partial=include_partial,
                now=now or datetime.now(timezone.utc).isoformat(),continuations={batch_id:public['continuation']})
            for row in rows:row['observation']=observations[row['id']]
            return dict(batch=public,experiments=rows)

    def replay_definition(self,user_id,batch_id,experiment_id):
        """Copy the owned experiment's frozen inputs, never its result or runtime."""
        with self._connect() as c:
            c.execute('BEGIN')
            batch=self._batch(c,self._owned(c,user_id,batch_id))
            row=c.execute('SELECT * FROM lab_experiments WHERE batch_id=? AND id=?',(batch_id,experiment_id)).fetchone()
            if row is None:
                raise RuleLabError('not_found','实验版本不存在',404)
            definition=dict(condition_key=row['condition_key'],name=row['name'],condition=_load(row['condition_json']),
                settlement_rule_id=row['settlement_rule_id'],settlement_snapshot=_load(row['settlement_snapshot_json']))
            return batch,definition

    def get_experiment_detail(self,user_id,batch_id,experiment_id):
        with self._connect() as c:
            # The worker may commit between queries; all detail metrics share one read snapshot.
            c.execute('BEGIN')
            batch=self._owned(c,user_id,batch_id)
            row=c.execute('SELECT * FROM lab_experiments WHERE batch_id=? AND id=?',(batch_id,experiment_id)).fetchone()
            if row is None:
                raise RuleLabError('not_found','实验版本不存在',404)
            daily=[dict(date=r['stat_date'],settled_count=r['settled_count'],hit_count=r['hit_count'],refund_count=r['refund_count'],
                        miss_count=r['miss_count'],stake_total=r['stake_cents']/100,net_profit=r['net_cents']/100)
                   for r in c.execute('SELECT * FROM lab_daily WHERE experiment_id=? ORDER BY stat_date DESC LIMIT 90',(experiment_id,))]
            pending=c.execute("SELECT COUNT(*) FROM lab_decisions WHERE experiment_id=? AND status='pending'",(experiment_id,)).fetchone()[0]
            return dict(experiment=self._experiment(c,row),daily=list(reversed(daily)),recent=[],recent_rounds=[],
                        detail_policy='results_only',retention_version=1,pending_count=pending,
                        monthly=self._monthly_report(c,batch,row))

    @staticmethod
    def _monthly_report(c,batch,experiment):
        from pc28touzhu.domain.rule_lab_monthly import monthly_report, report_window
        now=datetime.now(timezone.utc).isoformat()
        internal=RuleLabStore._internal_batch(batch)
        from pc28touzhu.services.rule_lab_series import public_summary, snapshots_for_experiment
        internal['continuation']=public_summary(c,batch)
        internal['continuation']['pending_counts']={r['month']:r['n'] for r in c.execute('''SELECT
            substr(COALESCE(risk_date,date(datetime(decided_at,'+8 hours'))),1,7) AS month,COUNT(*) AS n
            FROM lab_decisions WHERE experiment_id=? AND status='pending' GROUP BY month''',(experiment['id'],))}
        if internal['continuation']['enabled']:
            internal['end_date']=internal['continuation']['effective_end_date']
        _,end,first=report_window(internal,now=now)
        daily=[dict(r) for r in c.execute('''SELECT * FROM lab_daily WHERE experiment_id=?
            AND stat_date>=? AND stat_date<=? ORDER BY stat_date LIMIT 3720''',
            (experiment['id'],first.isoformat(),end.isoformat()))]
        cursor=internal['cursor']
        frontier=cursor.get('retention_v1') or {}
        def opened(issue):
            if not issue:return None
            row=c.execute('SELECT open_at FROM lab_draws WHERE issue_key=?',(_issue(issue),)).fetchone()
            return row[0] if row else None
        observed=frontier.get('processed_open_at') or opened(cursor.get('last_issue_no'))
        if not observed and cursor.get('simulation_time'):
            observed=(_time(cursor['simulation_time'])+timedelta(seconds=40)).isoformat()
        if not observed and daily:
            # A settlement date alone proves activity that day, not completion of that month.
            observed=daily[-1]['stat_date']+'T00:00:00+08:00'
        study=c.execute('''SELECT s.cutoff_at FROM lab_studies s JOIN lab_study_batches b
            ON b.study_id=s.id WHERE b.batch_id=?''',(batch['id'],)).fetchone()
        cutoff=study[0] if study else frontier.get('replay_cutoff_at') or opened(cursor.get('replay_max_issue'))
        first_observed=frontier.get('first_open_at') or opened(cursor.get('warm_before_issue') or (cursor.get('evidence') or {}).get('first_issue'))
        pending=c.execute("SELECT MIN(decided_at) FROM lab_decisions WHERE experiment_id=? AND status='pending'",(experiment['id'],)).fetchone()[0]
        result=monthly_report(internal,_load(experiment['stats_json']),daily,now=now,observed_at=observed,
                              cutoff_at=cutoff,first_observed_at=first_observed,pending_since=pending)
        result['snapshots']=snapshots_for_experiment(c,experiment['id'])
        return result

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
        result['risk_policy']=_load(result.pop('risk_policy_json'))
        return result

    def list_work_batches(self):
        with self._connect() as c:
            rows=c.execute('''SELECT b.* FROM lab_batches b WHERE status!='deleting' AND (status='running' OR EXISTS(
                SELECT 1 FROM lab_experiments e JOIN lab_decisions d ON d.experiment_id=e.id
                WHERE e.batch_id=b.id AND d.status='pending')) ORDER BY CASE b.mode WHEN 'live' THEN 0 ELSE 1 END,b.id LIMIT 50''').fetchall()
            from pc28touzhu.services.rule_lab_series import work_context
            result=[]
            for row in rows:
                item=self._internal_batch(row);context=work_context(c,row)
                if context:item['continuation']=context
                result.append(item)
            return result

    def _work_batch(self,c,batch):
        internal=self._internal_batch(batch)
        study=c.execute('''SELECT s.id,s.cutoff_issue,s.cutoff_at FROM lab_studies s JOIN lab_study_batches b
            ON b.study_id=s.id WHERE b.batch_id=?''',(batch['id'],)).fetchone()
        internal['study']=dict(study) if study else None
        from pc28touzhu.services.rule_lab_series import work_context
        context=work_context(c,batch)
        if context:
            internal['continuation']=context
            internal['end_date']=context['effective_end_date']
            if internal['study']:
                internal['study'].update(cutoff_issue=context['cutoff_issue'],cutoff_at=context['cutoff_at'])
        return internal

    def get_work_batch(self,batch_id):
        """Fresh control/clock metadata without decoding every experiment."""
        with self._connect() as c:
            batch=c.execute('SELECT * FROM lab_batches WHERE id=?',(batch_id,)).fetchone()
            if batch is None:raise RuleLabError('not_found','实验批次不存在',404)
            return self._work_batch(c,batch)

    def get_work_item(self,batch_id):
        with self._connect() as c:
            batch=c.execute('SELECT * FROM lab_batches WHERE id=?',(batch_id,)).fetchone()
            if batch is None:
                raise RuleLabError('not_found','实验批次不存在',404)
            experiments=[]
            for row in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id LIMIT 100',(batch_id,)):
                experiments.append(dict(id=row['id'],batch_id=row['batch_id'],name=row['name'],condition=_load(row['condition_json']),
                    settlement_snapshot=_load(row['settlement_snapshot_json']),settlement_rule_id=row['settlement_rule_id'],stats=_load(row['stats_json']),
                    risk_runtime={**empty_risk_runtime(),**_load(row['risk_runtime_json'])}))
            return dict(batch=self._work_batch(c,batch),experiments=experiments)

    def input_evidence(self,predictor_id,issue_no):
        """Minimal actual cached values used by replay; no observation timestamps."""
        with self._connect() as c:
            d=c.execute('SELECT issue_no,open_at,source_created_at,triplet_json,input_hash,conflict FROM lab_draws WHERE issue_no=?',(str(issue_no),)).fetchone()
            p=c.execute('''SELECT predictor_id,issue_no,created_at,settled_at,status,big_small,odd_even,combo,input_hash,conflict
                FROM lab_predictions WHERE predictor_id=? AND issue_no=?''',(predictor_id,str(issue_no))).fetchone()
            return dict(draw=dict(d) if d else None,prediction=dict(p) if p else None)

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
        with self._connect(write=True,reserve=bool(outcomes and not decisions and not predictions)) as c:
            batch=c.execute('SELECT * FROM lab_batches WHERE id=?',(batch_id,)).fetchone()
            if batch is None:
                raise RuleLabError('not_found','实验批次不存在',404)
            if batch['revision']!=expected_revision:
                return {**result,'reason':'stale_revision'}
            if batch['status']=='deleting':
                return {**result,'reason':'batch_deleting'}
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
            from pc28touzhu.services.rule_lab_series import validate_step_inputs
            continuous=validate_step_inputs(c,batch,draws,predictions,decisions,outcomes)
            if not continuous:self._cache(c,draws,predictions)
            experiments={r['id']:dict(r) for r in c.execute('SELECT * FROM lab_experiments WHERE batch_id=?',(batch_id,))}
            stats={eid:_load(e['stats_json']) for eid,e in experiments.items()}
            policy=_load(batch['risk_policy_json'])
            round_mode=policy.get('mode')=='round'
            runtimes={eid:{**empty_risk_runtime(),**_load(e['risk_runtime_json'])} for eid,e in experiments.items()}
            if round_mode and len({str(d['issue_no']) for d in decisions})>1:
                raise ValueError('按轮模式必须逐期准入并结算，不能批量提前冻结多期')
            dirty=set()
            input_conflict_outcomes=[]
            applied_outcomes=[]
            for decision in sorted(decisions,key=lambda d:(_issue(d['issue_no']),d['experiment_id'])):
                eid=decision['experiment_id']; issue=_issue(decision['issue_no'])
                if eid not in experiments or decision['stake_cents']!=batch['stake_cents'] or decision['decision_mode']!=batch['mode']:
                    raise ValueError('决策与实验不一致')
                if issue<=max(old_watermark,experiments[eid]['last_decided_issue']):
                    continue
                conflict=False if continuous else c.execute('''SELECT COALESCE((SELECT conflict FROM lab_predictions WHERE predictor_id=? AND issue_no=?),0)
                    OR COALESCE((SELECT conflict FROM lab_draws WHERE issue_no=?),0)''',
                    (batch['predictor_id'],str(decision['issue_no']),str(decision['issue_no']))).fetchone()[0]
                round_no=None;risk_date=None
                if round_mode and not conflict:
                    risk_date=decision.get('risk_date') or _day(decision.get('target_open_at') or decision['decided_at'])
                    if not isinstance(risk_date,str) or datetime.strptime(risk_date,'%Y-%m-%d').date().isoformat()!=risk_date:
                        raise ValueError('风险日期无效')
                    pending_count=c.execute("SELECT COUNT(*) FROM lab_decisions WHERE experiment_id=? AND status='pending'",(eid,)).fetchone()[0]
                    admitted=admit_risk(policy,runtimes[eid],self._risk_day(c,eid,risk_date),issue_no=str(issue),
                        at=decision['decided_at'],matched=decision.get('condition_matched') is True,pending_count=pending_count)
                    if not admitted['allowed']:
                        result['risk_blocked']=result.get('risk_blocked',0)+1
                        continue
                    runtimes[eid]=admitted['runtime'];round_no=runtimes[eid]['active_round']['round_no']
                    self._save_risk_day(c,eid,admitted['day'])
                    self._save_round(c,eid,runtimes[eid]['active_round'],now)
                cur=c.execute('''INSERT OR IGNORE INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,stake_cents,
                    decided_at,decision_mode,feature_issue_no,condition_value_json,round_no,risk_date) VALUES(?,?,?,?,?,?,?,?,?,?,?)''',
                    (eid,str(decision['issue_no']),issue,decision['bet_value'],decision['stake_cents'],decision['decided_at'],decision['decision_mode'],
                     decision.get('feature_issue_no'),_dump(decision.get('condition_value')),round_no,risk_date))
                if cur.rowcount:
                    stats[eid]['trigger_count']+=1; dirty.add(eid);result['inserted_decisions']+=1
                    experiments[eid]['last_decided_issue']=issue
                    c.execute('UPDATE lab_experiments SET last_decided_issue=? WHERE id=?',(issue,eid))
                    self._daily(c,eid,_day(decision['decided_at']),trigger_count=1)
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
                    draw=None if continuous else c.execute('SELECT conflict FROM lab_draws WHERE issue_no=?',(str(outcome['issue_no']),)).fetchone()
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
                if round_mode and pending['round_no'] is not None:
                    active=runtimes[eid]['active_round']
                    if not active or active['round_no']!=pending['round_no']:
                        raise ValueError('模拟轮次与待结算决策不一致')
                    risk_day=self._risk_day(c,eid,_day(outcome['settled_at']))
                    settled_risk=settle_risk(policy,runtimes[eid],risk_day,outcome)
                    runtimes[eid]=settled_risk['runtime']
                    self._save_risk_day(c,eid,settled_risk['day'])
                    self._save_round(c,eid,settled_risk['closed_round'] or runtimes[eid]['active_round'],now)
                dirty.add(eid)
                # Persist results and remove their pending input in the same
                # transaction. Last-decided/settled watermarks survive deletion.
                c.execute('DELETE FROM lab_decisions WHERE id=?',(pending['id'],))
                applied_outcomes.append(outcome)
            for eid in dirty:
                c.execute('UPDATE lab_experiments SET stats_json=?,risk_runtime_json=? WHERE id=?',(_dump(stats[eid]),_dump(runtimes[eid]),eid))
            if cursor.get('evidence') is not None:
                conflict=bool(input_conflict_outcomes) or any(r.get('conflict') for r in draws) or progress.get('status')=='error'
                if conflict:cursor={**cursor,'evidence':{**cursor['evidence'],'conflict':True}}
            skips=_load(batch['skip_counts_json'])
            for key,value in progress.get('skip_counts_delta',{}).items():
                if type(value) is not int or value<0:
                    raise ValueError('跳过计数无效')
                skips[key]=skips.get(key,0)+value
            delta=progress.get('processed_issues_delta',0)
            if type(delta) is not int or delta<0:
                raise ValueError('处理期数无效')
            from pc28touzhu.services.rule_lab_retention import update_frontier
            cursor=update_frontier(c,batch,cursor,draws=draws,outcomes=applied_outcomes)
            c.execute('''UPDATE lab_batches SET revision=revision+1,cursor_json=?,updated_at=?,processed_issues=processed_issues+?,
                total_issues=?,skip_counts_json=?,phase=?,status=?,error=? WHERE id=?''',
                (_dump(cursor),now,delta,progress.get('total_issues',batch['total_issues']),_dump(skips),progress.get('phase',batch['phase']),
                 progress.get('status',batch['status']),str(progress.get('error',batch['error']))[:300],batch_id))
            return {**result,'committed':True}

    def prune(self,*,now,limit=500):
        """Legacy internal entrypoint uses the same bounded dependency policy."""
        return self.retention_maintenance(now=now,limit=max(1,min(500,int(limit))))

    def continuation_preview(self,user_id,owner_kind,owner_id,*,now):
        from pc28touzhu.services.rule_lab_series import continuation_preview
        return continuation_preview(self,user_id,owner_kind,owner_id,now=now)

    def set_continuation(self,user_id,owner_kind,owner_id,payload,*,now):
        from pc28touzhu.services.rule_lab_series import set_continuation
        return set_continuation(self,user_id,owner_kind,owner_id,payload,now=now)

    def request_append(self,user_id,owner_kind,owner_id,payload,*,now):
        from pc28touzhu.services.rule_lab_series import request_append
        return request_append(self,user_id,owner_kind,owner_id,payload,now=now)

    def list_segments(self,user_id,owner_kind,owner_id,*,limit=20,before_id=None):
        from pc28touzhu.services.rule_lab_series import list_segments
        return list_segments(self,user_id,owner_kind,owner_id,limit=limit,before_id=before_id)

    def input_coverage(self,user_id,owner_kind,owner_id):
        from pc28touzhu.services.rule_lab_series import input_coverage
        return input_coverage(self,user_id,owner_kind,owner_id)

    def capture_work_items(self,*,now,limit=10):
        from pc28touzhu.services.rule_lab_archive import capture_work_items,seed_cache_page
        with self._connect(write=True,reserve=True) as c:
            rows=c.execute('''SELECT DISTINCT s.stream_id FROM lab_series s JOIN lab_capture_streams t ON t.id=s.stream_id
                WHERE s.capture_enabled=1 AND t.seed_after_issue<t.seed_until_issue ORDER BY t.updated_at,t.id LIMIT ?''',(max(1,min(10,limit)),)).fetchall()
            for row in rows:seed_cache_page(c,row['stream_id'],limit=200,now=now)
        return capture_work_items(self,now=now,limit=limit)

    def commit_capture(self,stream_id,expected_revision,*,draws,predictions,coverage,next_issue,now,highest_seen_issue=None,source_identity=None):
        from pc28touzhu.services.rule_lab_archive import commit_capture
        return commit_capture(self,stream_id,expected_revision,draws=draws,predictions=predictions,
            coverage=coverage,next_issue=next_issue,now=now,highest_seen_issue=highest_seen_issue,source_identity=source_identity)

    def set_capture_error(self,stream_id,code,message,*,now):
        state=code if code in ('archive_gap','storage_paused','source_unavailable') else 'storage_paused' if code=='storage_limit' else 'source_unavailable'
        with self._connect(write=True,reserve=True) as c:
            c.execute('UPDATE lab_capture_streams SET state=?,error=?,updated_at=? WHERE id=?',(state,str(message)[:300],now,stream_id))
            c.execute("INSERT INTO lab_runtime(key,value_json) VALUES('capture_last_stream',?) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json",(_dump(stream_id),))
            c.execute('UPDATE lab_series SET state=?,error=?,updated_at=? WHERE stream_id=?',(state,str(message)[:300],now,stream_id))

    def prepare_due_appends(self,*,now,limit=10):
        from pc28touzhu.services.rule_lab_series import prepare_due_appends
        return prepare_due_appends(self,now=now,limit=limit)

    def archive_source(self,batch_id):
        from pc28touzhu.services.rule_lab_series import archive_source
        return archive_source(self,batch_id)

    def complete_segment(self,batch_id,expected_revision,*,cursor,now):
        from pc28touzhu.services.rule_lab_series import complete_segment
        return complete_segment(self,batch_id,expected_revision,cursor=cursor,now=now)

    def monthly_snapshots(self,user_id,batch_id,experiment_id):
        from pc28touzhu.services.rule_lab_series import monthly_snapshots
        return monthly_snapshots(self,user_id,batch_id,experiment_id)

    def create_archive_replay(self,user_id,batch_id,experiment_id,config,*,now):
        from pc28touzhu.services.rule_lab_series import create_archive_replay
        return create_archive_replay(self,user_id,batch_id,experiment_id,config,now=now)

    def maintain_archive(self,*,now,limit=200):
        return self.retention_maintenance(now=now,limit=max(1,min(500,int(limit))))

    def continuation_receipt(self,user_id,owner_kind,owner_id,payload,request_kind):
        from pc28touzhu.services.rule_lab_series import continuation_receipt
        return continuation_receipt(self,user_id,owner_kind,owner_id,payload,request_kind)

    def management_preview(self,user_id,owner_kind,owner_id,*,now):
        from pc28touzhu.services.rule_lab_management import management_preview
        return management_preview(self,user_id,owner_kind,owner_id,now=now)

    def management_receipt(self,user_id,owner_kind,owner_id,payload,action):
        from pc28touzhu.services.rule_lab_management import management_receipt
        return management_receipt(self,user_id,owner_kind,owner_id,payload,action)

    def rename_owner(self,user_id,owner_kind,owner_id,payload,*,now):
        from pc28touzhu.services.rule_lab_management import rename_owner
        return rename_owner(self,user_id,owner_kind,owner_id,payload,now=now)

    def edit_batch(self,user_id,batch_id,payload,*,predictor_name,experiments,now,request_payload=None):
        from pc28touzhu.services.rule_lab_management import edit_batch
        return edit_batch(self,user_id,batch_id,payload,predictor_name=predictor_name,experiments=experiments,now=now,request_payload=request_payload)

    def fork_batch(self,user_id,batch_id,payload,*,predictor_name,experiments,now,request_payload=None):
        from pc28touzhu.services.rule_lab_management import fork_batch
        return fork_batch(self,user_id,batch_id,payload,predictor_name=predictor_name,experiments=experiments,now=now,request_payload=request_payload)

    def delete_preview(self,user_id,owner_kind,owner_id,*,now):
        from pc28touzhu.services.rule_lab_management import delete_preview
        return delete_preview(self,user_id,owner_kind,owner_id,now=now)

    def request_delete(self,user_id,owner_kind,owner_id,payload,*,now):
        from pc28touzhu.services.rule_lab_management import request_delete
        return request_delete(self,user_id,owner_kind,owner_id,payload,now=now)

    def management_job(self,user_id,job_id):
        from pc28touzhu.services.rule_lab_management import management_job
        return management_job(self,user_id,job_id)

    def management_jobs(self,user_id,limit=20):
        from pc28touzhu.services.rule_lab_management import management_jobs
        return management_jobs(self,user_id,limit=limit)

    def request_reclaim(self,user_id,payload,*,now):
        from pc28touzhu.services.rule_lab_management import request_reclaim
        return request_reclaim(self,user_id,payload,now=now)

    def management_maintenance(self,*,now,limit=500,max_pages=256):
        from pc28touzhu.services.rule_lab_management import management_maintenance
        return management_maintenance(self,now=now,limit=limit,max_pages=max_pages)

    def retention_maintenance(self,*,now,limit=500):
        from pc28touzhu.services.rule_lab_retention import maintain
        return maintain(self,now=now,limit=limit)

    def replay_input_preview(self,user_id,batch_id,experiment_id,*,now):
        from pc28touzhu.services.rule_lab_retention import replay_input_preview
        return replay_input_preview(self,user_id,batch_id,experiment_id,now=now)

    def queue_eligibility(self,user_id,batch_id,configuration_hash):
        from pc28touzhu.services.rule_lab_management import queue_eligibility
        return queue_eligibility(self,user_id,batch_id,configuration_hash)

    def start_queued_batch(self,user_id,batch_id,configuration_hash,*,now):
        from pc28touzhu.services.rule_lab_management import start_queued_batch
        return start_queued_batch(self,user_id,batch_id,configuration_hash,now=now)
