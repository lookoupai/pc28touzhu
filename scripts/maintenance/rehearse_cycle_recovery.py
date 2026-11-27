#!/usr/bin/env python3
"""在明确指定的隔离副本上演练周期归档；禁止生产路径和网络访问。

使用已验证的 backup 副本再复制一份作为 --db；--baseline 指向原列哈希基线。
本脚本会修改副本，运行前必须确认待验代码已稳定。不会加载配置或启动 worker。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import sys
import time
from datetime import datetime, timezone


PRODUCTION_ROOT = Path('/www/wwwroot/pc28touzhu')
SAFE_ROOTS = (Path('/var/backups/pc28touzhu/releases'), Path('/tmp'))
OWNER = {'user_id': 1, 'rule_id': 13, 'subscription_id': 21, 'route_id': 13, 'rule_run_id': 1937}
EXPECTED_EVENT_IDS = (64481, 64482)
EXPECTED_JOB_IDS = (64488, 64489)

# 归档只能改这些原表中的指定旧轮行；所有任务、attempt、流水、配置及钱账保持原样。
SCOPED_CHANGES = {
    'auto_trigger_cycle_states': (
        'user_id=1 AND rule_id=13 AND subscription_id=21 AND route_id=13 AND rule_run_id=1937',
        {'balance', 'status', 'stop_reason', 'pending_plan_json', 'last_cycle_stop_reason',
         'day_closed', 'day_close_scope', 'updated_at'},
    ),
    'auto_trigger_cycle_sessions': (
        'user_id=1 AND rule_id=13 AND subscription_id=21',
        {'status', 'active_run_id', 'stop_reason', 'updated_at'},
    ),
    'auto_trigger_rule_runs': (
        'user_id=1 AND rule_id=13 AND subscription_id=21 AND id=1937',
        {'status', 'stop_reason', 'stopped_at', 'updated_at'},
    ),
    'auto_trigger_route_subscription_runtime_runs': (
        'user_id=1 AND auto_trigger_rule_run_id=1937',
        {'status', 'ended_at', 'end_reason', 'updated_at'},
    ),
    'subscription_runtime_runs': (
        'user_id=1 AND auto_trigger_rule_run_id=1937',
        {'status', 'ended_at', 'end_reason', 'updated_at'},
    ),
}


def require(condition, message):
    if not condition:
        raise ValueError(message)


def quoted(value):
    return '"' + value.replace('"', '""') + '"'


def encoded(row):
    values = [{'__sqlite_blob_hex__': value.hex()} if isinstance(value, bytes) else value for value in row]
    return json.dumps(values, ensure_ascii=False, separators=(',', ':'), allow_nan=False).encode('utf-8')


def rows_digest(rows):
    digest = hashlib.sha256()
    count = 0
    for row in rows:
        data = encoded(row)
        digest.update(len(data).to_bytes(8, 'big'))
        digest.update(data)
        count += 1
    return {'row_count': count, 'original_columns_sha256': digest.hexdigest()}


def file_digest(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(4 * 1024 * 1024), b''):
            digest.update(block)
    return digest.hexdigest()


def safe_path(raw, *, must_exist=True):
    value = Path(raw).expanduser()
    require(value.is_absolute(), '路径必须为绝对路径')
    require(not value.is_symlink(), '不允许符号链接文件')
    resolved = value.resolve(strict=must_exist)
    require(resolved != PRODUCTION_ROOT and PRODUCTION_ROOT not in resolved.parents, '禁止访问生产目录')
    require(any(root == resolved or root in resolved.parents for root in SAFE_ROOTS), '路径必须位于备份区或 /tmp')
    if must_exist:
        require(resolved.is_file(), '输入必须是普通文件')
    return resolved


def connection(path):
    conn = sqlite3.connect('file:' + str(path) + '?mode=ro', uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def schema_digest(conn):
    return rows_digest(conn.execute('SELECT type,name,tbl_name,sql FROM sqlite_master ORDER BY type,name'))


def table_spec(conn, name, baseline=None):
    fields = conn.execute('PRAGMA table_info(' + quoted(name) + ')').fetchall()
    require(bool(fields), '原表缺失: ' + name)
    current = [row['name'] for row in fields]
    pk = [row['name'] for row in sorted(fields, key=lambda row: row['pk']) if row['pk']]
    ordering = ','.join(quoted(name) for name in pk) if pk else 'rowid'
    if baseline:
        require(set(baseline['columns']).issubset(current), '原列缺失: ' + name)
        require(baseline['order_by'] == ordering, '原表主键排序变化: ' + name)
        columns = baseline['columns']
    else:
        columns = current
    return columns, ordering


def select_rows(conn, name, columns, ordering, where='1'):
    return conn.execute('SELECT ' + ','.join(quoted(c) for c in columns) + ' FROM ' + quoted(name) +
                        ' WHERE ' + where + ' ORDER BY ' + ordering)


def fingerprint(path, baseline=None):
    started = time.monotonic()
    with connection(path) as conn:
        names = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")]
        specs = (baseline or {}).get('tables', {})
        missing = sorted(set(specs) - set(names))
        require(not missing, '迁移删除原表')
        tables = {}
        for name in names:
            columns, ordering = table_spec(conn, name, specs.get(name))
            tables[name] = {**rows_digest(select_rows(conn, name, columns, ordering)),
                            'columns': columns, 'order_by': ordering}
        integrity = [r[0] for r in conn.execute('PRAGMA quick_check')]
        foreign_keys = [tuple(r) for r in conn.execute('PRAGMA foreign_key_check')]
        schema = schema_digest(conn)
    require(integrity == ['ok'], 'quick_check 未通过')
    return {'tables': tables, 'quick_check': integrity, 'foreign_key_check': foreign_keys,
            'schema': schema, 'elapsed_seconds': round(time.monotonic() - started, 3)}


def changed_tables(before, after):
    return sorted(name for name, value in before['tables'].items() if name not in after['tables'] or
                  any(value[key] != after['tables'][name][key]
                      for key in ('row_count', 'original_columns_sha256')))


def scoped_snapshot(path, baseline):
    result = {}
    with connection(path) as conn:
        for name, (where, _) in SCOPED_CHANGES.items():
            columns, ordering = table_spec(conn, name, baseline['tables'][name])
            result[name] = {
                'outside': rows_digest(select_rows(conn, name, columns, ordering, 'NOT (' + where + ')')),
                'inside': [tuple(row) for row in select_rows(conn, name, columns, ordering, where)],
                'columns': columns,
            }
        result['sqlite_sequence'] = [tuple(r) for r in conn.execute(
            "SELECT name,seq FROM sqlite_sequence WHERE name!='auto_trigger_cycle_archives' ORDER BY name")]
        result['rule7_runs'] = [tuple(r) for r in conn.execute(
            'SELECT * FROM auto_trigger_rule_runs WHERE rule_id=7 ORDER BY id')]
    return result


def compare_scoped(before, after):
    changes = {}
    for name, (_, allowed_fields) in SCOPED_CHANGES.items():
        require(before[name]['outside'] == after[name]['outside'], '改动越出目标旧轮: ' + name)
        require(len(before[name]['inside']) == len(after[name]['inside']), '目标原表行数改变: ' + name)
        fields = set()
        for old, new in zip(before[name]['inside'], after[name]['inside']):
            for index, column in enumerate(before[name]['columns']):
                if old[index] != new[index]:
                    fields.add(column)
        require(fields.issubset(allowed_fields), '目标旧轮改动了禁止字段: ' + name)
        if fields:
            changes[name] = sorted(fields)
    require(before['sqlite_sequence'] == after['sqlite_sequence'], '原有自增计数改变')
    require(before['rule7_runs'] == after['rule7_runs'], '规则 7 的运行记录被改动')
    return changes


def write_json(path, data):
    temp = path.with_suffix(path.suffix + '.partial')
    fd = os.open(temp, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    with os.fdopen(fd, 'w', encoding='utf-8') as stream:
        json.dump(data, stream, ensure_ascii=False, indent=2)
    os.replace(temp, path)


def progress(stage, **details):
    print(json.dumps({'stage': stage, **details}, ensure_ascii=False), flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', required=True, help='可修改的独立数据库克隆绝对路径')
    parser.add_argument('--baseline', required=True, help='protected-main-baseline.json 绝对路径')
    parser.add_argument('--output', required=True, help='新建脱敏演练结果 JSON 绝对路径')
    parser.add_argument('--now', required=True, help='带时区的确定性演练时间，例如 2026-11-27T06:00:00Z')
    args = parser.parse_args()
    db = safe_path(args.db)
    baseline_path = safe_path(args.baseline)
    output = safe_path(args.output, must_exist=False)
    require(output.parent.is_dir() and not output.exists(), '输出目录必须存在且文件不可已存在')
    require(db != baseline_path and db != output and baseline_path != output, '数据库、基线、输出不能重合')
    require(db.stat().st_nlink == 1, '数据库不能是共享 inode 的硬链接')
    require(db.stat().st_mode & 0o077 == 0, '演练数据库必须限制为仅属主访问')
    require(baseline_path.suffix == '.json' and baseline_path.stat().st_size < 4 * 1024 * 1024, '基线文件格式或大小不符')
    baseline = json.loads(baseline_path.read_text(encoding='utf-8'))
    source = safe_path(baseline['database'])
    require(not os.path.samefile(db, source), '不能修改原始预检备份；必须另建克隆')
    instant = datetime.fromisoformat(args.now.replace('Z', '+00:00'))
    require(instant.tzinfo is not None, '演练时间必须包含时区')
    instant = instant.astimezone(timezone.utc)
    report = {'status': 'running', 'db': str(db), 'baseline': str(baseline_path), 'now': instant.isoformat(),
              'started_at': datetime.now(timezone.utc).isoformat(), 'production_mutated': False,
              'network_allowed': False, 'stages': {}}
    started = time.monotonic()

    # 即使错误导入其他入口，也不得打开生产库或访问外部服务。
    def audit(event, values):
        if event in ('socket.connect', 'socket.getaddrinfo'):
            raise RuntimeError('演练禁止网络访问')
        if event == 'sqlite3.connect':
            path = str(values[0])
            plain = path[5:].split('?', 1)[0] if path.startswith('file:') else path
            require(Path(plain).resolve() == db, '演练禁止连接目标克隆以外的数据库')
    sys.addaudithook(audit)
    try:
        require(file_digest(db) == baseline['file_sha256'], '克隆文件与保护基线不一致')
        original = fingerprint(db, baseline)
        require(changed_tables(baseline, original) == [], '克隆的原表内容与基线不一致')
        report['stages']['before'] = original
        write_json(output, report)
        progress('baseline_verified', tables=len(original['tables']))

        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'src'))
        from pc28touzhu.executor.db_repository import DatabaseRepository
        repository = DatabaseRepository(str(db))
        repository.initialize_database()
        first = fingerprint(db, baseline)
        require(changed_tables(original, first) == [], '首次 schema 初始化改动原表内容')
        require(first['foreign_key_check'] == original['foreign_key_check'], '首次迁移新增外键违规')
        report['stages']['initialize_once'] = first
        write_json(output, report)
        progress('initialize_once_verified', original_tables_unchanged=True)

        repository.initialize_database()
        second = fingerprint(db, first)
        require(changed_tables(first, second) == [] and first['schema'] == second['schema'], '重复迁移不幂等')
        require(second['foreign_key_check'] == first['foreign_key_check'], '重复迁移新增外键违规')
        report['stages']['initialize_twice'] = second
        before_scope = scoped_snapshot(db, baseline)
        before = repository.get_auto_trigger_cycle_state(**{k: OWNER[k] for k in ('user_id','rule_id','subscription_id','route_id')})
        require(before and before['rule_run_id'] == 1937 and before['pending_plan']['issue_no'] == '3510978', '副本故障轮身份不符')
        require(before['balance'] == 22.39 and before['pending_plan']['stake_total'] == 13, '副本故障轮金额不符')
        with connection(db) as conn:
            require([r[0] for r in conn.execute('SELECT status FROM execution_jobs WHERE id IN (64488,64489) ORDER BY id')] == ['unconfirmed'] * 2, '原未知任务状态不符')
            require(conn.execute('SELECT count(*) FROM auto_trigger_cycle_archives').fetchone()[0] == 0, '演练初始归档表不是空表')
        write_json(output, report)
        progress('initialize_twice_verified', idempotent=True)

        detach = repository.detach_expired_auto_trigger_cycles(user_id=1, rule_id=13, now=instant)
        require(detach.get('detached_count') == 1 and not detach.get('errors'), '目标旧轮归档失败')
        archives = repository.list_auto_trigger_cycle_archives(user_id=1, rule_id=13)
        require(len(archives) == 1, '目标旧轮归档数量不为一')
        archive = archives[0]
        require(archive['rule_run_id'] == 1937 and archive['route_id'] == 13, '归档归属错误')
        require(archive['initial_reserved_stake'] == 13 and archive['reserved_stake'] == 13, '未决本金错误')
        require(archive['spendable_balance_at_detach'] == 9.39 and archive['status'] == 'awaiting_reconciliation', '归档金额或状态错误')
        require(archive['pending_plan'] == before['pending_plan'], '归档原待结算计划改变')
        current = repository.get_auto_trigger_cycle_state(**{k: OWNER[k] for k in ('user_id','rule_id','subscription_id','route_id')})
        session = repository.get_auto_trigger_cycle_session(user_id=1, rule_id=13, subscription_id=21)
        require(current['balance'] == 9.39 and current['pending_plan'] is None, '当前可用资金或运行占用错误')
        require(session['status'] == 'waiting' and session['active_run_id'] is None, '旧轮未退出调度')
        after_scope = scoped_snapshot(db, baseline)
        field_changes = compare_scoped(before_scope, after_scope)
        after_detach = fingerprint(db, baseline)
        changed = changed_tables(original, after_detach)
        require(set(changed).issubset(set(SCOPED_CHANGES) | {'sqlite_sequence'}), '归档改动了保护表')
        require(after_detach['foreign_key_check'] == original['foreign_key_check'], '归档新增外键违规')
        report['stages']['detach'] = {
            'result': detach, 'archive_count': 1, 'reserved_stake': 13.0, 'spendable_balance': 9.39,
            'old_jobs_events_attempts_unchanged': True, 'other_rule7_preserved': True,
            'changed_original_tables': changed, 'allowed_field_changes': field_changes,
            'fingerprint': after_detach,
        }
        write_json(output, report)
        progress('detach_verified', archive_count=1, reserved_stake=13, spendable_balance=9.39)

        again = repository.detach_expired_auto_trigger_cycles(user_id=1, rule_id=13, now=instant)
        require(again.get('detached_count') == 0 and not again.get('errors'), '重复归档不幂等')
        after_repeat = fingerprint(db, after_detach)
        require(changed_tables(after_detach, after_repeat) == [], '重复归档改动数据或重复冻结本金')
        require(after_repeat['foreign_key_check'] == original['foreign_key_check'], '重复归档新增外键违规')
        report['stages']['repeat_detach'] = {'result': again, 'all_tables_unchanged': True, 'fingerprint': after_repeat}
        report['status'] = 'passed'
        progress('repeat_detach_verified', all_tables_unchanged=True)
    except Exception as exc:
        report['status'] = 'failed'
        report['failure'] = {'type': type(exc).__name__, 'reason': str(exc) if isinstance(exc, ValueError) else type(exc).__name__}
    finally:
        report['elapsed_seconds'] = round(time.monotonic() - started, 3)
        report['finished_at'] = datetime.now(timezone.utc).isoformat()
        write_json(output, report)
    progress('finished', status=report['status'], output=str(output), elapsed_seconds=report['elapsed_seconds'])
    return 0 if report['status'] == 'passed' else 1


if __name__ == '__main__':
    raise SystemExit(main())
