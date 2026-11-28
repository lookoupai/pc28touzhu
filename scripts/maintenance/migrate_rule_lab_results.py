#!/usr/bin/env python3
"""Preview or migrate the independent experiment ledger to result-only storage.

Stop the platform API, experiment worker and batch queue before --apply. Default
application only initializes and backfills small boundary metadata; --drain runs
bounded resumable cleanup steps, never one unbounded DELETE transaction.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sqlite3
import subprocess
import time
from datetime import datetime,timezone
from pathlib import Path

from pc28touzhu.services.rule_lab_store import RuleLabStore,RuleLabError
from pc28touzhu.services import rule_lab_retention as retention
from pc28touzhu.services.rule_lab_storage import directory_bytes,process_reclaim

WRITERS=('pc28touzhu-platform.service','pc28touzhu-rule-lab.service','pc28touzhu-rule-lab-batch-queue.service')


def _connect(path):
    c=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=.25)
    c.row_factory=sqlite3.Row;c.execute('PRAGMA query_only=ON');return c


def protected_snapshot(path):
    """Only authorized historical detail may disappear; all economic rows stay."""
    result={}
    with _connect(path) as c:
        queries={
            'experiments':'SELECT * FROM lab_experiments ORDER BY id',
            'daily':'SELECT * FROM lab_daily ORDER BY experiment_id,stat_date',
            'pending':"SELECT * FROM lab_decisions WHERE status='pending' ORDER BY id",
            'studies':'SELECT * FROM lab_studies ORDER BY id',
            'study_batches':'SELECT * FROM lab_study_batches ORDER BY study_id,position',
            'series':'SELECT * FROM lab_series ORDER BY id',
        }
        for name,query in queries.items():
            digest=hashlib.sha256();count=0
            for row in c.execute(query):digest.update(repr(tuple(row)).encode());digest.update(b'\n');count+=1
            result[name]=dict(rows=count,sha256=digest.hexdigest())
        rows=[]
        for row in c.execute('SELECT * FROM lab_batches ORDER BY id'):
            value=dict(row);cursor=json.loads(value.pop('cursor_json'));cursor.pop('retention_v1',None)
            for name,default in [('template_snapshot_json','{}'),('risk_template_snapshot_json','{}'),('creation_request_id',None)]:
                value.setdefault(name,default)
            value['cursor']=cursor;rows.append(value)
        result['batches']=rows
        dependencies=retention.dependencies(c)
        risk=[]
        for eid,days in sorted(dependencies['risk_days'].items()):
            for day in sorted(d for d in days if d):
                row=c.execute('SELECT * FROM lab_risk_days WHERE experiment_id=? AND stat_date=?',(eid,day)).fetchone()
                if row:risk.append(tuple(row))
        result['active_risk_days']=risk
        result['active_rounds']=[tuple(r) for r in c.execute("SELECT * FROM lab_rounds WHERE status!='closed' ORDER BY experiment_id,round_no")]
    return result


def _backup(path,target):
    descriptor=os.open(target,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(descriptor)
    with _connect(path) as source,sqlite3.connect(target) as destination:
        source.backup(destination,pages=256,sleep=.01)


def migrate(database_path,*,apply=False,backup_path=None,drain=False,max_steps=100000,now=None):
    path=Path(database_path).resolve()
    if not path.is_file():raise RuleLabError('not_found','实验数据库不存在',404)
    if type(max_steps) is not int or not 1<=max_steps<=1000000:raise ValueError('迁移步数无效')
    now=now or datetime.now(timezone.utc).isoformat().replace('+00:00','Z')
    started=time.monotonic()
    with _connect(path) as c:
        version=c.execute('PRAGMA user_version').fetchone()[0]
        mode=c.execute('PRAGMA auto_vacuum').fetchone()[0]
    before=directory_bytes(path.parent)
    result=dict(applied=False,schema_before=version,schema_target=7,auto_vacuum=mode,
        database_bytes=path.stat().st_size,directory_bytes=before,backup_required=True,
        available_bytes=shutil.disk_usage(path.parent).free)
    if not apply:return result
    if version not in (5,6,7):raise RuleLabError('store_unavailable','只支持schema5/6/7的实验库',409)
    if mode!=2:raise RuleLabError('storage_not_ready','实验库尚未启用分批空间回收，请先完成原空间模式升级',409)
    if backup_path is None:raise RuleLabError('backup_required','迁移必须指定实验目录外的备份',409)
    backup=Path(backup_path).resolve()
    if backup==path or path.parent in backup.parents:raise RuleLabError('invalid_backup','备份必须位于实验目录之外',409)
    if backup.exists():raise RuleLabError('backup_exists','备份路径已存在，拒绝覆盖；后台维护可以直接继续原游标',409)
    if result['available_bytes']<path.stat().st_size+32*1024*1024:raise RuleLabError('storage_limit','空间不足以保存独立回滚备份',503)
    protected=protected_snapshot(path);_backup(path,backup)
    store=RuleLabStore(path);store.initialize()
    # Backfill each batch independently. At most 50 per owner, no large input
    # deletion happens before new code starts writing result-only outcomes.
    with store._connect() as c:ids=[r[0] for r in c.execute('SELECT id FROM lab_batches ORDER BY id')]
    for bid in ids:
        with store._connect(write=True,reserve=True) as c:
            row=c.execute('SELECT * FROM lab_batches WHERE id=?',(bid,)).fetchone()
            if row:retention._backfill(c,row)
    steps=max_rows=idle_steps=0
    drain_complete=False
    if drain:
        for _ in range(max_steps):
            state=retention.maintain(store,now=now,limit=500)
            max_rows=max(max_rows,state['processed_rows'],state['scanned_rows']);steps+=1
            reclaimed=process_reclaim(store,now=now,max_pages=256)
            if not state['maintenance_pending']:
                with store._connect() as c:
                    active=c.execute("SELECT 1 FROM lab_management_jobs WHERE kind='reclaim' AND status IN ('queued','running','waiting') LIMIT 1").fetchone()
                if not active:
                    drain_complete=True;break
                idle_steps=0 if reclaimed else idle_steps+1
                if idle_steps>=3:break
            else:idle_steps=0
    after_protected=protected_snapshot(path)
    if protected!=after_protected:raise RuleLabError('integrity_error','迁移改变了受保护的成绩、参数或运行状态，备份已保留',503)
    with store._connect() as c:
        if c.execute('PRAGMA quick_check').fetchone()[0]!='ok' or c.execute('PRAGMA foreign_key_check').fetchall():
            raise RuleLabError('integrity_error','迁移后完整性检查未通过，备份已保留',503)
        state=retention.status(c)
        schema_after=c.execute('PRAGMA user_version').fetchone()[0]
    return {**result,'applied':True,'schema_after':schema_after,'backup':str(backup),'backup_bytes':backup.stat().st_size,
        'backup_expires_after_hours':72,'protected_unchanged':True,'steps':steps,'max_step_rows':max_rows,
        'drain_requested':drain,'drain_complete':drain_complete if drain else None,
        'retention':state,'after_directory_bytes':directory_bytes(path.parent),'after_database_bytes':path.stat().st_size,
        'elapsed_seconds':round(time.monotonic()-started,3)}


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database',required=True,type=Path)
    parser.add_argument('--backup',type=Path)
    parser.add_argument('--apply',action='store_true')
    parser.add_argument('--drain',action='store_true')
    parser.add_argument('--max-steps',type=int,default=100000)
    args=parser.parse_args()
    if args.apply:
        for unit in WRITERS:
            try:value=subprocess.run(['systemctl','is-active',unit],capture_output=True,text=True,timeout=10)
            except (OSError,subprocess.TimeoutExpired):parser.error('无法确认实验写者已停止：'+unit)
            if value.stdout.strip() not in ('inactive','failed') or value.returncode!=3:
                parser.error('实验写者未明确停止，拒绝迁移：'+unit)
        if args.backup is None:parser.error('--apply requires --backup outside the experiment directory')
    elif args.drain:parser.error('--drain requires --apply')
    print(json.dumps(migrate(args.database,apply=args.apply,backup_path=args.backup,drain=args.drain,
        max_steps=args.max_steps),ensure_ascii=False,indent=2))


if __name__=='__main__':main()
