"""Bounded SQLite page reclamation and explicit offline mode conversion."""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sqlite3
from pathlib import Path

from pc28touzhu.services.rule_lab_store import RuleLabError, _dump, _load


def directory_bytes(root):
    total=0
    if root.exists():
        for folder,dirs,files in os.walk(root,followlinks=False):
            dirs[:]=[name for name in dirs if not (Path(folder)/name).is_symlink()]
            for name in files:
                path=Path(folder)/name
                try:
                    if path.is_file() and not path.is_symlink():total+=path.stat().st_size
                except FileNotFoundError:pass
    return total


def _size(path):
    try:return path.stat().st_size
    except FileNotFoundError:return 0


def storage_status(store):
    path=store.db_path
    result=dict(used_bytes=directory_bytes(path.parent),budget_bytes=store.budget_bytes,
        detail_retention_days=store.detail_retention_days,database_bytes=_size(path),
        wal_bytes=_size(Path(str(path)+'-wal')),shm_bytes=_size(Path(str(path)+'-shm')),
        allocated_bytes=0,reusable_bytes=0,reclaimable_bytes=0,auto_vacuum=0,incremental_ready=False,reclaim_job=None)
    if path.exists():
        with store._connect() as c:
            size=c.execute('PRAGMA page_size').fetchone()[0]
            result['allocated_bytes']=c.execute('PRAGMA page_count').fetchone()[0]*size
            result['reusable_bytes']=c.execute('PRAGMA freelist_count').fetchone()[0]*size
            result['auto_vacuum']=c.execute('PRAGMA auto_vacuum').fetchone()[0]
            result['incremental_ready']=result['auto_vacuum']==2
            result['reclaimable_bytes']=result['reusable_bytes']
            exists=c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='lab_management_jobs'").fetchone()
            if exists:
                row=c.execute("SELECT id,status,freed_bytes,error,updated_at FROM lab_management_jobs WHERE kind='reclaim' ORDER BY id DESC LIMIT 1").fetchone()
                if row:result['reclaim_job']=dict(row)
    return result


def process_reclaim(store,*,now,max_pages=256):
    if type(max_pages) is not int or not 1<=max_pages<=256:raise ValueError('回收页数必须为1至256')
    with store._connect() as c:
        row=c.execute("SELECT * FROM lab_management_jobs WHERE kind='reclaim' AND status IN ('queued','running','waiting') ORDER BY id LIMIT 1").fetchone()
    if row is None:return 0
    before=storage_status(store)
    payload=_load(row['payload_json'])
    payload.setdefault('initial_database_bytes',before['database_bytes'])
    payload.setdefault('initial_wal_bytes',before['wal_bytes'])
    if not before['incremental_ready']:
        status,error='waiting','实验库尚未完成空间回收模式升级，请联系管理员执行一次维护转换'
        freed=0
    else:
        connection=None
        try:
            connection=sqlite3.connect(str(store.db_path),timeout=0)
            connection.execute('PRAGMA busy_timeout=0')
            # No Python transaction surrounds these operations. Consume every
            # result row: SQLite emits a row for each incremental vacuum step.
            connection.execute('PRAGMA incremental_vacuum(%d)'%max_pages).fetchall()
            check=connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
            free=connection.execute('PRAGMA freelist_count').fetchone()[0]
            status='waiting' if check[0] else 'running' if free else 'completed'
            error='正在等待其他读取结束后释放日志空间' if check[0] else ''
            actual=_size(store.db_path)+_size(Path(str(store.db_path)+'-wal'))
            original=payload['initial_database_bytes']+payload['initial_wal_bytes']
            freed=max(0,original-actual) if not check[0] else row['freed_bytes']
        except sqlite3.OperationalError as exc:
            if any(word in str(exc).lower() for word in ('locked','busy')):
                status,error,freed='waiting','实验库正忙，后续将继续分批回收',row['freed_bytes']
            elif any(word in str(exc).lower() for word in ('full','disk')):
                status,error,freed='waiting','磁盘空间不足，已保留现有数据，待空间恢复后重试',row['freed_bytes']
            else:raise RuleLabError('store_unavailable','空间回收暂不可用',503) from exc
        finally:
            if connection is not None:connection.close()
    with store._connect(write=True,reserve=True) as c:
        c.execute('UPDATE lab_management_jobs SET status=?,phase=?,payload_json=?,freed_bytes=MAX(freed_bytes,?),error=?,updated_at=? WHERE id=?',
            (status,'reclaim',_dump(payload),freed,error,now,row['id']))
    return max(0,freed-row['freed_bytes'])


def _fingerprints(c,columns=None):
    if columns is None:
        names=[r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        columns={name:[r[1] for r in c.execute('PRAGMA table_info("'+name.replace('"','""')+'")')] for name in names}
    results={}
    for table,fields in columns.items():
        quoted=','.join('"'+f.replace('"','""')+'"' for f in fields)
        # Explicit business keys are part of the selected tuple. Row order itself
        # may change during VACUUM on tables without INTEGER PRIMARY KEY.
        count=total=combined=0
        modulus=1<<256
        for row in c.execute('SELECT '+quoted+' FROM "'+table.replace('"','""')+'"'):
            digest=int.from_bytes(hashlib.sha256(repr(tuple(row)).encode()).digest(),'big')
            count+=1;total=(total+digest)%modulus;combined^=digest
        results[table]=(count,format(total,'064x'),format(combined,'064x'))
    return columns,results


def prepare_storage(database_path,*,apply=False,backup_path=None):
    """Explicit offline operation. Caller must stop all experiment writers first."""
    path=Path(database_path).resolve()
    if not path.is_file():raise RuleLabError('not_found','实验数据库不存在',404)
    before=_size(path)
    with sqlite3.connect(path.as_uri()+'?mode=ro',uri=True,timeout=.25) as c:
        mode=c.execute('PRAGMA auto_vacuum').fetchone()[0]
        expected={'path':str(path),'database_bytes':before,'auto_vacuum':mode,
                  'required_free_bytes':2*before+16*1024*1024,'available_bytes':shutil.disk_usage(path.parent).free}
    if not apply:return expected
    if mode==2:return {**expected,'old_rows_unchanged':True,'already_ready':True}
    if backup_path is None:raise RuleLabError('backup_required','转换前必须指定独立备份路径',409)
    backup=Path(backup_path).resolve()
    if backup==path or path.parent in backup.parents:raise RuleLabError('invalid_backup','备份必须位于实验数据目录之外',409)
    if expected['available_bytes']<expected['required_free_bytes']:raise RuleLabError('storage_limit','可用空间不足以安全整理实验库',503)
    if backup.exists():raise RuleLabError('backup_exists','备份路径已存在，拒绝覆盖',409)
    connection=sqlite3.connect(str(path),timeout=.25)
    try:
        connection.execute('PRAGMA busy_timeout=250')
        checkpoint=connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
        if checkpoint[0]:raise RuleLabError('store_busy','仍有实验读写连接占用，请停止实验写者后重试',409)
        columns,digests=_fingerprints(connection)
        descriptor=os.open(backup,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);os.close(descriptor)
        with sqlite3.connect(backup) as target:connection.backup(target,pages=256,sleep=.01)
        connection.execute('PRAGMA auto_vacuum=INCREMENTAL')
        connection.execute('VACUUM')
        connection.execute('PRAGMA wal_checkpoint(TRUNCATE)').fetchone()
        if connection.execute('PRAGMA quick_check').fetchone()[0]!='ok' or connection.execute('PRAGMA foreign_key_check').fetchall():
            raise RuleLabError('integrity_error','整理后的实验库完整性检查未通过，备份已保留',503)
        if _fingerprints(connection,columns)[1]!=digests:raise RuleLabError('integrity_error','整理前后的实验数据不一致，备份已保留',503)
        if connection.execute('PRAGMA auto_vacuum').fetchone()[0]!=2:raise RuleLabError('conversion_failed','空间回收模式未生效',503)
        return {**expected,'auto_vacuum':2,'old_rows_unchanged':True,'backup':str(backup),'after_database_bytes':_size(path)}
    finally:connection.close()
