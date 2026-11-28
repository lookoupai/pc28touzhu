#!/usr/bin/env python3
"""Preview a finite legacy manifest; --apply hands its existing IDs to the DB queue.

This command never initializes a database or changes experiments, scores, cursors
or continuation schedules. Apply requires an already migrated schema7 database
and a positively identified, stopped legacy controller.
"""
from __future__ import annotations

import argparse
import json
import re
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from pc28touzhu.services.rule_lab_store import RuleLabError, _dump, _load
from pc28touzhu.services.rule_lab_templates import fingerprint
from pc28touzhu.services.rule_lab_queue import configuration
from pc28touzhu.services.rule_lab_series import _configuration

DEFAULT_LEGACY_UNIT='pc28touzhu-jnd28-experiments.service'


def validate_manifest(manifest):
    if (not isinstance(manifest,dict) or type(manifest.get('version')) is not int or manifest['version']!=1
            or type(manifest.get('user_id')) is not int or manifest['user_id']<1
            or manifest.get('enable_continuation') is not False
            or not isinstance(manifest.get('request_prefix'),str)
            or not re.fullmatch(r'[A-Za-z0-9_-]{1,80}',manifest['request_prefix'])
            or not isinstance(manifest.get('batches'),list) or not 1<=len(manifest['batches'])<=50):
        raise RuleLabError('invalid_manifest','只接受明确关闭续算的有效有限队列清单')
    seen=set()
    for item in manifest['batches']:
        if (not isinstance(item,dict) or type(item.get('batch_id')) is not int or item['batch_id']<1
                or item['batch_id'] in seen or not isinstance(item.get('configuration_hash'),str)
                or not re.fullmatch(r'[0-9a-f]{64}',item['configuration_hash'])
                or any(type(item[key]) is not bool for key in ('started','completed','removed','configured') if key in item)):
            raise RuleLabError('invalid_manifest','清单成员编号、配置指纹或状态无效')
        seen.add(item['batch_id'])
    return manifest


def check_legacy_stopped(unit):
    if not isinstance(unit,str) or not re.fullmatch(r'[A-Za-z0-9_.@:-]+\.service',unit):
        raise RuleLabError('invalid_input','旧启动器服务名称无效')
    try:
        result=subprocess.run(['systemctl','show',unit,'--property=LoadState','--property=ActiveState','--property=MainPID'],
            capture_output=True,text=True,timeout=10)
    except (OSError,subprocess.TimeoutExpired) as error:
        raise RuleLabError('legacy_writer_unknown','无法确认旧启动器已停止，拒绝导入',409) from error
    properties=dict(line.split('=',1) for line in result.stdout.splitlines() if '=' in line)
    if (result.returncode or properties.get('LoadState')!='loaded'
            or properties.get('ActiveState') not in ('inactive','failed') or properties.get('MainPID')!='0'):
        raise RuleLabError('legacy_writer_active','旧启动器尚未明确停止，拒绝导入',409)


def _state(c,user_id,item):
    bid=item['batch_id'];owner=c.execute('SELECT * FROM lab_batches WHERE id=?',(bid,)).fetchone()
    if owner is not None and owner['user_id']!=user_id:
        raise RuleLabError('owner_mismatch','清单包含其他账号的实验，整组未导入',409)
    control=c.execute("SELECT * FROM lab_object_controls WHERE owner_kind='batch' AND owner_id=?",(bid,)).fetchone()
    series=c.execute("SELECT 1 FROM lab_series WHERE owner_kind='batch' AND owner_id=?",(bid,)).fetchone()
    actual_hash=configuration(c,'batch',owner) if owner else item['configuration_hash']
    if item.get('removed') or owner is None or owner['status']=='deleting' or (control and control['deleted']):
        return 'removed',actual_hash,'清单已移出或实验已删除'
    if series or c.execute('SELECT 1 FROM lab_study_batches WHERE batch_id=?',(bid,)).fetchone():
        return 'removed',actual_hash,'实验由续算或比较组管理'
    if owner['mode']!='backtest' or _configuration(c,[owner])!=item['configuration_hash']:
        return 'removed',actual_hash,'实验配置已变化，原清单授权失效'
    if owner['status'] in ('completed','running','error','resource_paused'):
        return owner['status'],actual_hash,''
    if owner['status']=='paused':
        if owner['started_at'] or item.get('started') or (control and control['queue_optout']):
            return 'paused',actual_hash,'保留手动暂停'
        return 'queued',actual_hash,''
    return 'removed',actual_hash,'当前状态不能从原清单自动启动'


def import_queue(database_path,manifest,*,apply=False,legacy_unit=DEFAULT_LEGACY_UNIT,now=None):
    manifest=validate_manifest(manifest);path=Path(database_path).resolve()
    if not path.is_file():raise RuleLabError('not_found','实验数据库不存在',404)
    if apply:check_legacy_stopped(legacy_unit)
    now=now or datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00','Z')
    with sqlite3.connect(path.as_uri()+('?mode=rw' if apply else '?mode=ro'),uri=True,timeout=5) as c:
        c.row_factory=sqlite3.Row;c.execute('PRAGMA foreign_keys=ON')
        if not apply:c.execute('PRAGMA query_only=ON')
        c.execute('BEGIN IMMEDIATE' if apply else 'BEGIN')
        version=c.execute('PRAGMA user_version').fetchone()[0]
        if version not in (6,7) or apply and version!=7:
            raise RuleLabError('schema_required','预览需要schema6/7；正式导入前必须先完成schema7迁移',409)
        has_queue=bool(c.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='lab_queue_items'").fetchone())
        if apply and not has_queue:raise RuleLabError('schema_required','实验队列表尚未初始化',409)
        user_id=manifest['user_id'];items=[];inserted=0
        key='queue_handoff:'+fingerprint(dict(user_id=user_id,request_prefix=manifest['request_prefix']))[:24]
        membership_hash=fingerprint(sorted((item['batch_id'],item['configuration_hash']) for item in manifest['batches']))
        previous=c.execute('SELECT value_json FROM lab_runtime WHERE key=?',(key,)).fetchone()
        if previous and _load(previous[0]).get('membership_hash')!=membership_hash:
            raise RuleLabError('manifest_conflict','同一清单标识已交接其他成员或配置，拒绝覆盖',409)
        order=c.execute('SELECT COALESCE(MAX(queue_order),0) FROM lab_queue_items').fetchone()[0] if has_queue else 0
        for member in manifest['batches']:
            bid=member['batch_id']
            existing=c.execute("SELECT * FROM lab_queue_items WHERE owner_kind='batch' AND owner_id=?",(bid,)).fetchone() if has_queue else None
            if existing:
                if existing['user_id']!=user_id:raise RuleLabError('owner_mismatch','队列归属与清单不一致，整组未导入',409)
                state,config_hash,reason=existing['state'],existing['configuration_hash'],'保留已有队列状态'
            else:state,config_hash,reason=_state(c,user_id,member)
            if not existing:
                order+=1
                if apply:
                    c.execute('''INSERT INTO lab_queue_items(user_id,owner_kind,owner_id,state,queue_order,configuration_hash,origin,created_at,updated_at)
                        VALUES(?,'batch',?,?,?,?,?,?,?)''',(user_id,bid,state,order,config_hash,'legacy_manifest',now,now))
                    inserted+=1
            items.append(dict(owner_kind='batch',owner_id=bid,state=state,configuration_hash=config_hash,
                legacy_configuration_hash=member['configuration_hash'],existing=bool(existing),reason=reason))
        digest=fingerprint(manifest)
        if apply:
            value=dict(user_id=user_id,manifest_sha256=digest,legacy_unit=legacy_unit,
                membership_hash=membership_hash,batch_ids=[item['owner_id'] for item in items],imported_at=now)
            c.execute('INSERT INTO lab_runtime(key,value_json) VALUES(?,?) ON CONFLICT(key) DO NOTHING',(key,_dump(value)))
        return dict(applied=bool(apply),schema=version,manifest_sha256=digest,user_id=user_id,
            inserted=inserted,preserved=sum(item['existing'] for item in items),items=items)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db',required=True,type=Path)
    parser.add_argument('--manifest',required=True,type=Path)
    parser.add_argument('--legacy-unit',default=DEFAULT_LEGACY_UNIT)
    parser.add_argument('--apply',action='store_true')
    args=parser.parse_args()
    try:
        if args.manifest.stat().st_size>131072:raise ValueError('清单不能超过128KiB')
        manifest=json.loads(args.manifest.read_text())
        result=import_queue(args.db,manifest,apply=args.apply,legacy_unit=args.legacy_unit)
    except (OSError,ValueError,RuleLabError,sqlite3.Error) as error:
        parser.error(str(error))
    print(json.dumps(result,ensure_ascii=False,indent=2))


if __name__=='__main__':main()
