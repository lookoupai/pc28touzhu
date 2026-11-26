"""Read-only checkpoint readiness checks; this module never starts or extends work."""
from __future__ import annotations

import hashlib
import json


def fingerprint(value):
    raw=json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
    return hashlib.sha256(raw).hexdigest()


def checkpoint_readiness(study, batches, experiments, history, *, pending_count):
    """Assess existing state only, without claiming future input/time continuity.

    A completed experiment retains its active round. Snapshot eligibility is not
    permission to reset it, change its fixed settings or consume new input.
    """
    blockers=[]
    def block(code,message):
        if not any(item['code']==code for item in blockers):
            blockers.append(dict(code=code,message=message))
    if not batches or len(batches)!=study['total_children']:
        block('incomplete_group','比较组子方案数量不完整')
    if study['status']!='completed' or any(b['status']!='completed' for b in batches):
        block('study_not_completed','本轮全部方案完成后才能建立续跑快照')
    if pending_count:
        block('pending_settlements','存在待结算记录，不能冻结续跑起点')
    if study['status']=='completed' and not study['comparable']:
        block('incomparable_history','已有方案输入证据不一致或无有效历史，不能声明连续比较')
    batch_ids={b['id'] for b in batches}
    if len(batch_ids)!=len(batches) or any(e['batch_id'] not in batch_ids for e in experiments):
        block('invalid_membership','实验与批次归属异常')
    if len({e['id'] for e in experiments})!=len(experiments):
        block('invalid_membership','实验版本ID重复')
    configs=[];last_issues=[];shared=[];pairs=[];fixed_inputs=[]
    for batch in sorted(batches,key=lambda item:item['id']):
        cursor=batch['cursor']
        if batch['mode']!='backtest':block('not_backtest','只有历史回测支持本次续接')
        if not cursor.get('replay_complete'):
            block('replay_incomplete','尚未保存本段回放完成标记')
        if cursor.get('input_expired') or (cursor.get('evidence') or {}).get('conflict'):
            block('input_conflict','历史输入已过期或存在冲突')
        last=str(cursor.get('last_issue_no') or '')
        if not last.isascii() or not last.isdigit() or int(last)<=0:
            block('missing_cursor','缺少有效的已处理期号')
        else:
            last_issues.append(int(last))
            evidence=cursor.get('evidence') or {}
            if evidence.get('last_issue')!=last or evidence.get('count')!=batch['processed_issues']:
                block('cursor_evidence_mismatch','游标与已处理期数/输入证据不一致')
        shared.append({k:batch[k] for k in ('predictor_id','metric','template_pack','stake_cents','start_date','end_date')})
        members=sorted((e for e in experiments if e['batch_id']==batch['id']),key=lambda item:item['id'])
        if not members or len(members)!=batch['experiment_count']:
            block('incomplete_experiments','缺少实验版本，不能保存完整运行状态')
        pairs.append([(e['condition_key'],e['settlement_rule_id']) for e in members])
        fixed_inputs.append(sorted((e['condition_key'],e['settlement_rule_id'],
                                    fingerprint(e['condition']),fingerprint(e['settlement_snapshot'])) for e in members))
        config={k:batch[k] for k in ('id','predictor_id','metric','template_pack','stake_cents','risk_policy')}
        config['experiments']=[{k:e[k] for k in ('id','condition_key','condition','settlement_rule_id','settlement_snapshot')} for e in members]
        configs.append(config)
    if shared and any(s!=shared[0] for s in shared):
        block('inconsistent_configuration','来源、注额或区间不一致，不能作为同一比较组追加')
    if pairs and any(sorted(p)!=sorted(pairs[0]) for p in pairs):
        block('inconsistent_experiments','方案之间的条件/结算版本不一致')
    if fixed_inputs and any(value!=fixed_inputs[0] for value in fixed_inputs):
        block('inconsistent_snapshots','方案之间的实际条件或赔率快照不一致')
    last_issue=last_issues[0] if last_issues and len(set(last_issues))==1 and len(last_issues)==len(batches) else None
    if last_issue is None:block('unequal_cursors','各方案尚未到达同一个续跑起点')
    if last_issue is not None:
        issues=[int(h['issue_no']) for h in history]
        if issues!=list(range(last_issue-99,last_issue+1)):
            block('warm_window_missing','续跑所需的最后100期历史窗口不完整')
    if any(h.get('conflict') for h in history):
        block('warm_window_conflict','续跑历史窗口存在输入冲突')
    return dict(checkpoint_ready=not blockers,blockers=blockers,study_id=study['id'],
        saved_experiments=len(experiments),last_issue=str(last_issue) if last_issue is not None else None,
        configuration_hash=fingerprint(configs),
        state_hash=fingerprint(dict(study=study,batches=batches,experiments=experiments,history=history,pending_count=pending_count)),
        retained_history_periods=len(history),
        pending_checks=['新增区间数据完整性','边界结算与下一期决策时间衔接','分段快照与事务迁移'],
        continuation_available=False)
