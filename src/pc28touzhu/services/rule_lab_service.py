"""Fast configuration and result queries for the independent rule lab."""
from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
import copy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from pc28touzhu.domain.rule_lab import ALLOWED_SETTLEMENT_RULE_IDS, COMBO_LONG_MISS_THRESHOLDS, build_conditions, build_experiments, build_experiments_from_conditions, normalize_batch_payload
from pc28touzhu.domain.settlement_rules import build_settlement_snapshot
from pc28touzhu.domain.rule_lab_risk import normalize_risk_policy
from pc28touzhu.domain.subscription_strategy import upgrade_subscription_strategy
from pc28touzhu.services.rule_lab_store import (
    MAX_RUNNING_EXPERIMENTS, MAX_TOTAL_EXPERIMENTS, RuleLabError, RuleLabStore,
)


def _now(value=None):
    return value if value is not None else datetime.now(timezone.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')


def _csv_cell(value):
    if not isinstance(value, str):
        return value
    if value and (value[0] in '=+-@' or ord(value[0]) < 32):
        return "'" + value
    return value


class RuleLabService:
    def __init__(self, store=None, source=None, platform_repository=None, resources=None):
        self.store = store if store is not None else RuleLabStore()
        if source is None:
            from pc28touzhu.services.rule_lab_source import RuleLabSource
            source = RuleLabSource()
        self.source = source
        self.platform_repository=platform_repository
        self.resources=resources
        self.store.initialize()

    def research_options(self,user_id):
        from pc28touzhu.services.rule_lab_research import options
        return options(self.store,user_id)

    def research_rankings(self,user_id,payload,*,now=None):
        from pc28touzhu.services.rule_lab_research import rankings
        return rankings(self.store,user_id,payload,now=_now(now))

    def list_favorites(self,user_id,**pagination):
        from pc28touzhu.services.rule_lab_favorites import list_favorites
        return list_favorites(self.store,user_id,**pagination)

    def save_favorite(self,user_id,payload,*,fid=None,now=None):
        from pc28touzhu.services.rule_lab_favorites import save_favorite
        return save_favorite(self.store,user_id,payload,fid=fid,now=_now(now))

    def delete_favorite(self,user_id,fid,payload,*,now=None):
        from pc28touzhu.services.rule_lab_favorites import delete_favorite
        return delete_favorite(self.store,user_id,fid,payload,now=_now(now))

    def create_from_favorite(self,user_id,fid,payload,*,now=None):
        from pc28touzhu.services.rule_lab_favorites import create_from_favorite
        return create_from_favorite(self,user_id,fid,payload,now=_now(now))

    def preview_bulk_management(self,user_id,payload,*,now=None):
        from pc28touzhu.services.rule_lab_bulk_management import preview
        return preview(self,user_id,payload,now=_now(now))

    def execute_bulk_management(self,user_id,payload,*,now=None):
        from pc28touzhu.services.rule_lab_bulk_management import execute
        return execute(self,user_id,payload,now=_now(now))

    def research_data_quality(self,user_id,payload,*,now=None):
        return self.source.quality_page(payload,now=_now(now))

    def performance(self):
        if self.resources is None:
            from pc28touzhu.services.rule_lab_resources import RuleLabResources
            self.resources=RuleLabResources()
        return dict(performance=self.resources.status())

    def set_performance(self,payload):
        if not isinstance(payload,dict) or set(payload)!={'mode'}:
            raise RuleLabError('invalid_input','请提供运行速度mode')
        if not isinstance(payload['mode'],str) or payload['mode'] not in ('economy','fast'):
            raise RuleLabError('invalid_input','运行速度只能选择省资源或加速')
        self.performance()
        return dict(performance=self.resources.set_mode(payload['mode']))

    def _risk_presets(self,user_id):
        if self.platform_repository is None or user_id is None:return []
        rules=self.platform_repository.list_auto_trigger_rules(user_id=user_id)
        subscriptions={s['id']:s for s in self.platform_repository.list_subscriptions(user_id) if s.get('user_id',user_id)==user_id}
        presets=[]
        def control(raw,day=False):
            raw=raw if isinstance(raw,dict) else {}
            result=dict(enabled=bool(raw.get('enabled')),profit_target=raw.get('profit_target') or 0,
                        loss_limit=raw.get('loss_limit') or 0,max_bets=0)
            if day:result.update(max_rounds=0,hit_rate_gate=copy.deepcopy(raw.get('hit_rate_gate') or dict(enabled=False,threshold=0,min_settled_count=20)))
            return result
        for rule in rules:
            if rule.get('user_id',user_id)!=user_id or rule.get('status')=='archived':continue
            selected=subscriptions.values() if rule.get('scope_mode')=='all_subscriptions' else [subscriptions[sid] for sid in rule.get('subscription_ids',[]) if sid in subscriptions]
            routes=rule.get('routes') or [dict(id=0,name='继承方案',subscription_risk_mode='inherit_subscription',route_risk_mode='inherit_rule')]
            for sub in selected:
                if sub.get('status')=='archived':continue
                try:strategy=upgrade_subscription_strategy(sub.get('strategy_v2') or sub.get('strategy') or {})
                except ValueError:continue
                if strategy.get('staking_policy',{}).get('mode')=='win_streak_roll':continue
                for route in routes:
                    if route.get('status')=='archived':continue
                    staking=route.get('staking_policy') if route.get('staking_mode')=='override' else strategy.get('staking_policy',{})
                    if not isinstance(staking,dict) or staking.get('mode')!='fixed':continue
                    play=strategy.get('play_filter',{})
                    metrics={'big_small','odd_even','combo'} if play.get('mode')=='all' else {str(k).partition(':')[0] for k in play.get('selected_keys',[])}
                    play_mode=route.get('play_filter_mode','inherit')
                    action=rule.get('action') or {}
                    if play_mode=='fixed_metric':metrics={route.get('play_filter',{}).get('fixed_metric')}
                    elif play_mode=='matched_metric' or play_mode=='inherit' and action.get('play_filter_action')=='matched_metric':
                        metrics={cond.get('metric') for cond in rule.get('conditions',[])}
                    elif play_mode=='inherit' and action.get('play_filter_action')=='fixed_metric':metrics={action.get('fixed_metric')}
                    for metric in sorted(metrics & {'big_small','odd_even','combo'}):
                        rule_day=control(rule.get('daily_risk_control'),True)
                        route_mode=route.get('route_risk_mode','inherit_rule')
                        extra=[]
                        if route_mode=='override':
                            route_day=control(route.get('route_risk_control') or route.get('risk_control'),True)
                            if route_day!=rule_day and route_day['enabled']:extra.append(route_day)
                        elif route_mode not in ('inherit_rule','disabled'):continue
                        round_mode=route.get('subscription_risk_mode','inherit_rule')
                        if round_mode=='override':round_control=control(route.get('subscription_risk_control'))
                        elif round_mode=='disabled':round_control=control({})
                        elif round_mode=='inherit_rule':round_control=control(rule.get('daily_risk_control'))
                        elif round_mode=='inherit_subscription':
                            risk=strategy.get('risk_control') or {};round_control=control(risk)
                            limits=(risk.get('play_limits') or {}).get(metric) or {}
                            for key in ('profit_target','loss_limit'):
                                if float(limits.get(key) or 0)>0:round_control[key]=limits[key]
                        else:continue
                        cycle=rule.get('cycle_policy') or {}
                        if cycle.get('mode')=='rule':
                            if not rule_day['enabled']:
                                # Cycle quotas operate even when monetary daily risk is disabled.
                                # Do not revive saved but inactive targets or a disabled hit-rate gate.
                                rule_day=control({},True);rule_day['enabled']=True
                            rule_day['max_rounds']=int(cycle.get('max_starts_per_day') or 0)
                        source=dict(rule_id=rule['id'],rule_name=str(rule.get('name') or '')[:200],route_id=int(route.get('id') or 0),
                                    route_name=str(route.get('name') or '继承方案')[:200],subscription_id=sub['id'],subscription_name=str(sub.get('name') or '方案')[:200])
                        try:
                            policy=normalize_risk_policy(dict(version=1,mode='round',round=round_control,day=rule_day,
                                additional_day_guards=extra,cooldown_issues=int(rule.get('cooldown_issues') or 0),source=source))
                        except (ValueError,TypeError):continue
                        label=' · '.join([source['rule_name'],source['route_name'],source['subscription_name'],{'big_small':'大小','odd_even':'单双','combo':'组合'}[metric]])
                        presets.append(dict(id='%s:%s:%s:%s'%(rule['id'],source['route_id'],sub['id'],metric),label=label,metric=metric,policy=policy,
                            note='仅复制当前风控数值；实验来源、触发模板、每注金额及两套OK赔率保持不变。'))
                        if len(presets)>=200:return presets
        return presets

    def _risk_snapshot(self,user_id,policy,metric):
        policy=normalize_risk_policy(policy)
        source=policy.get('source') or {}
        if source:
            matches=[p for p in self._risk_presets(user_id) if p['metric']==metric and all(p['policy']['source'].get(k)==source.get(k) for k in ('rule_id','route_id','subscription_id'))]
            if len(matches)!=1:raise RuleLabError('invalid_input','风控预设已不可用或不属于当前账号，请重新选择或改为手动配置')
            reference=matches[0]['policy']
            policy['source']=copy.deepcopy(reference['source'])
            policy['source']['modified']={k:v for k,v in policy.items() if k!='source'}!={k:v for k,v in reference.items() if k!='source'}
        return policy

    def _source_predictor(self, predictor_id):
        try:
            predictor = self.source.get_predictor(predictor_id)
        except (RuleLabError, OSError, sqlite3.Error) as exc:
            raise RuleLabError('source_unavailable', '预测来源暂不可用，请稍后重试', 503) from exc
        if predictor is None:
            raise RuleLabError('invalid_input', '请选择仍在公开逐期预测的有效来源')
        return predictor

    def options(self, user_id=None, *, now=None):
        available, message = True, ''
        try:
            predictors = self.source.list_predictors()
        except (RuleLabError, OSError, sqlite3.Error):
            predictors = []
            available, message = False, '预测来源暂不可用，已有实验成绩仍可查看'
        rules = []
        for rule_id in ALLOWED_SETTLEMENT_RULE_IDS:
            snapshot = build_settlement_snapshot(rule_source='subscription_fixed', settlement_rule_id=rule_id,
                                                 fallback_profit_ratio=1, resolved_from='rule_lab')
            odds = snapshot['resolved_odds']
            rules.append(dict(id=rule_id, name=snapshot['rule']['name'], big_small_odds=odds['big_small'],
                              small_odd_big_even_odds=odds['combo']['小单'],
                              big_odd_small_even_odds=odds['combo']['大单'],
                              refund_note='命中正常赔付，无特殊退本' if rule_id == 'pc28_netdisk_regular'
                              else '命中遇13/14、对子、顺子或豹子退本；未中全亏'))
        return dict(predictors=predictors, metrics=[dict(id=k, name=v) for k, v in
                    [('big_small', '大小'), ('odd_even', '单双'), ('combo', '组合')]],
                    settlement_rules=rules, template_pack='basic_v2',
                    template_packs=[dict(id='basic_v2',name='综合条件 v2（多种触发）',metrics=['big_small','odd_even','combo'],
                        condition_counts={metric:len(build_conditions(metric)) for metric in ('big_small','odd_even','combo')},
                        description='无条件跟随、预测连错3～7期、连中、连续开出/不出及窗口命中率。',
                        descriptions_by_metric={'combo':'同一批包含无条件基准、预测连错 ≥ '+'、'.join(map(str,COMBO_LONG_MISS_THRESHOLDS))+'期、预测连中、连续开出/不出及窗口命中率。已包含长连错包的全部条件，无需另跑。'}),
                        dict(id='combo_long_miss_v1',name='组合长连错 · 7～30期',metrics=['combo'],
                        condition_counts={'combo':len(COMBO_LONG_MISS_THRESHOLDS)+1},
                        description='仅无条件跟随基准＋预测连错 ≥ '+ '、'.join(map(str,COMBO_LONG_MISS_THRESHOLDS))+'期。若要同时比较其他触发条件，请选综合条件 v2。'),
                        dict(id='basic_v1',name='基础条件 v1（旧版）',selectable=False,metrics=['big_small','odd_even','combo'],
                        condition_counts={metric:len(build_conditions(metric,template_pack='basic_v1')) for metric in ('big_small','odd_even','combo')},
                        description='旧批次的固定模板：预测连错3～7期，其他条件沿用创建时配置。')],
                    default_stake_amount=1,risk_presets=self._risk_presets(user_id),
                    limits=dict(max_experiments_per_batch=100, max_running_experiments=MAX_RUNNING_EXPERIMENTS,
                                max_total_experiments=MAX_TOTAL_EXPERIMENTS, max_backtest_days=90,
                                detail_retention_days=0, detail_policy='results_only',
                                disk_budget_bytes=self.store.budget_bytes, cpu_quota_percent=20,
                                memory_max_bytes=268435456), source_available=available, source_message=message)

    def overview(self, user_id, *, now=None):
        return dict(batches=self.store.list_batches(user_id), worker=self.store.worker_status(now=_now(now)),
                    storage=self.store.storage_status(),management_jobs=self.store.management_jobs(user_id,limit=20))

    def comparison_preview(self,user_id,batch_id):
        return self.store.comparison_preview(user_id,batch_id)

    def continuation_preview(self,user_id,owner_kind,owner_id,*,now=None):
        return self.store.continuation_preview(user_id,owner_kind,owner_id,now=_now(now))

    @staticmethod
    def _continuation_payload(payload,kind,*,now):
        expected={'request_id','expected_revision'}|(
            {'end_date'} if kind=='append' else {'capture_enabled','compute_enabled','daily_at','timezone'})
        if not isinstance(payload,dict) or set(payload)!=expected:
            raise RuleLabError('invalid_input','续算请求字段不完整或包含未知字段')
        if not isinstance(payload['request_id'],str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,80}',payload['request_id']):
            raise RuleLabError('invalid_input','请求标识必须为8至80位字母、数字、下划线或短横线')
        if not isinstance(payload['expected_revision'],str) or not 1<=len(payload['expected_revision'])<=150:
            raise RuleLabError('invalid_input','续算版本标识无效，请重新预览')
        if kind=='append':
            value=payload['end_date']
            try:
                if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):raise ValueError()
                target=date.fromisoformat(value)
            except ValueError as exc:
                raise RuleLabError('invalid_input','追加截止日期无效') from exc
            today=datetime.fromisoformat(now.replace('Z','+00:00')).astimezone(timezone(timedelta(hours=8))).date()
            if target>today:raise RuleLabError('invalid_input','追加截止日期不能晚于北京时间今天')
        else:
            if type(payload['capture_enabled']) is not bool or type(payload['compute_enabled']) is not bool:
                raise RuleLabError('invalid_input','保留数据和自动续算开关必须为布尔值')
            if payload['compute_enabled'] and not payload['capture_enabled']:
                raise RuleLabError('invalid_input','自动续算必须同时保留后续数据')
            if not isinstance(payload['daily_at'],str) or not re.fullmatch(r'(?:[01][0-9]|2[0-3]):[0-5][0-9]',payload['daily_at']):
                raise RuleLabError('invalid_input','每日续算时间须为00:00至23:59')
            if payload['timezone']!='Asia/Shanghai':
                raise RuleLabError('invalid_input','续算时区只支持Asia/Shanghai')

    def _require_continuation_source(self,predictor_id):
        try:predictor=self.source.get_predictor(predictor_id)
        except (RuleLabError,OSError,sqlite3.Error) as exc:
            raise RuleLabError('source_unavailable','预测来源暂不可用，已有成绩仍可查看',503) from exc
        if predictor is None:
            raise RuleLabError('source_forbidden','来源已停用或不再共享逐期预测，不能开始采集或续算',403)

    def set_continuation(self,user_id,owner_kind,owner_id,payload,*,now=None):
        timestamp=_now(now)
        preview=self.continuation_preview(user_id,owner_kind,owner_id,now=timestamp)
        self._continuation_payload(payload,'configure',now=timestamp)
        receipt=self.store.continuation_receipt(user_id,owner_kind,owner_id,payload,'configure')
        if receipt is not None:return receipt
        current=preview['continuation']
        enabling=any(payload[key] and not current.get(key) for key in ('capture_enabled','compute_enabled'))
        if enabling:self._require_continuation_source(preview['predictor_id'])
        # 关闭自动续算/采集属于控制操作，来源故障时也允许保存关闭状态。
        return self.store.set_continuation(user_id,owner_kind,owner_id,payload,now=timestamp)

    def request_append(self,user_id,owner_kind,owner_id,payload,*,now=None):
        timestamp=_now(now)
        preview=self.continuation_preview(user_id,owner_kind,owner_id,now=timestamp)
        self._continuation_payload(payload,'append',now=timestamp)
        receipt=self.store.continuation_receipt(user_id,owner_kind,owner_id,payload,'append')
        if receipt is not None:return receipt
        # 相对上次截止的90天检查由账本在幂等回执查找后执行，旧请求重试不能被后续进度误拒绝。
        self._require_continuation_source(preview['predictor_id'])
        return self.store.request_append(user_id,owner_kind,owner_id,payload,now=timestamp)

    def list_segments(self,user_id,owner_kind,owner_id,*,limit=20,before_id=None):
        return self.store.list_segments(user_id,owner_kind,owner_id,limit=limit,before_id=before_id)

    def input_coverage(self,user_id,owner_kind,owner_id):
        return self.store.input_coverage(user_id,owner_kind,owner_id)

    def management_preview(self,user_id,owner_kind,owner_id,*,now=None):
        return self.store.management_preview(user_id,owner_kind,owner_id,now=_now(now))

    @staticmethod
    def _management_payload(payload,action):
        fields={'request_id'} if action=='reclaim' else {'request_id','expected_revision'}|(
            {'config'} if action in ('edit','fork') else {'confirm_name'} if action=='delete' else {'name'})
        if not isinstance(payload,dict) or set(payload)!=fields:
            raise RuleLabError('invalid_input','管理请求字段不完整或包含未知字段')
        if not isinstance(payload['request_id'],str) or not re.fullmatch(r'[A-Za-z0-9_-]{8,80}',payload['request_id']):
            raise RuleLabError('invalid_input','请求标识必须为8至80位字母、数字、下划线或短横线')
        if action!='reclaim' and (not isinstance(payload['expected_revision'],str) or not 1<=len(payload['expected_revision'])<=200):
            raise RuleLabError('invalid_input','管理版本标识无效，请刷新后重试')
        if action in ('rename','delete'):
            name=payload['name' if action=='rename' else 'confirm_name']
            if not isinstance(name,str) or not 1<=len(name.strip())<=80:
                raise RuleLabError('invalid_input','名称必须为1到80个字符')
        if action in ('edit','fork') and not isinstance(payload['config'],dict):
            raise RuleLabError('invalid_input','请提供完整实验配置')

    def _management_receipt(self,user_id,owner_kind,owner_id,payload,action,*,now):
        try:self._management_payload(payload,action)
        except RuleLabError:
            # Preserve owned-resource errors even when an unauthorized caller
            # submits malformed fields. Valid deletion retries may outlive owner rows.
            self.store.management_preview(user_id,owner_kind,owner_id,now=now)
            raise
        return self.store.management_receipt(user_id,owner_kind,owner_id,payload,action)

    def rename_owner(self,user_id,owner_kind,owner_id,payload,*,now=None):
        timestamp=_now(now)
        receipt=self._management_receipt(user_id,owner_kind,owner_id,payload,'rename',now=timestamp)
        if receipt is not None:return receipt
        return self.store.rename_owner(user_id,owner_kind,owner_id,payload,now=timestamp)

    def _change_definition(self,user_id,batch_id,payload,action,*,now):
        receipt=self._management_receipt(user_id,'batch',batch_id,payload,action,now=now)
        if receipt is not None:return receipt
        preview=self.store.management_preview(user_id,'batch',batch_id,now=now)
        if not preview['editable'] and action=='edit':
            raise RuleLabError('definition_locked','实验已启动或已有记录，请使用“修改并新建版本”',409)
        if action=='fork' and not preview['can_fork']:
            raise RuleLabError('definition_locked','当前实验不能创建修改版本',409)
        config,predictor,definitions=self._resolve_definition(user_id,payload['config'],now=now,original_batch_id=batch_id)
        method=self.store.edit_batch if action=='edit' else self.store.fork_batch
        return method(user_id,batch_id,{**payload,'config':config},predictor_name=predictor['name'],
                      experiments=definitions,request_payload=payload,now=now)

    def edit_batch(self,user_id,batch_id,payload,*,now=None):
        return self._change_definition(user_id,batch_id,payload,'edit',now=_now(now))

    def fork_batch(self,user_id,batch_id,payload,*,now=None):
        return self._change_definition(user_id,batch_id,payload,'fork',now=_now(now))

    def delete_preview(self,user_id,owner_kind,owner_id,*,now=None):
        return self.store.delete_preview(user_id,owner_kind,owner_id,now=_now(now))

    def request_delete(self,user_id,owner_kind,owner_id,payload,*,now=None):
        timestamp=_now(now)
        receipt=self._management_receipt(user_id,owner_kind,owner_id,payload,'delete',now=timestamp)
        if receipt is not None:return receipt
        return self.store.request_delete(user_id,owner_kind,owner_id,payload,now=timestamp)

    def management_job(self,user_id,job_id):
        return dict(job=self.store.management_job(user_id,job_id))

    def storage(self):
        return self.store.storage_status()

    def request_reclaim(self,user_id,payload,*,now=None):
        self._management_payload(payload,'reclaim')
        return self.store.request_reclaim(user_id,payload,now=_now(now))

    def preview_bulk(self,user_id,payload,*,now=None):
        from pc28touzhu.services.rule_lab_bulk import preview_bulk
        return preview_bulk(self,user_id,payload,now=_now(now))

    def create_bulk(self,user_id,payload,*,now=None):
        from pc28touzhu.services.rule_lab_bulk import create_bulk
        return create_bulk(self,user_id,payload,now=_now(now))

    def preview_study(self,user_id,payload):
        from pc28touzhu.services.rule_lab_bulk import preview_study
        return preview_study(self,user_id,payload)

    def create_study(self,user_id,payload,*,now=None):
        if isinstance(payload,dict) and 'condition_keys' in payload:
            from pc28touzhu.services.rule_lab_bulk import create_selected_study
            return create_selected_study(self,user_id,payload,now=_now(now))
        if not isinstance(payload,dict) or set(payload)!={'source_batch_id','name','request_id'}:
            raise RuleLabError('invalid_input','比较组需提供source_batch_id、name和request_id')
        return self.store.create_study(user_id,payload['source_batch_id'],payload['name'],payload['request_id'],now=_now(now))

    def list_queue(self,user_id,*,state='active',now=None):
        from pc28touzhu.services.rule_lab_queue import list_queue
        return list_queue(self.store,user_id,state=state,now=_now(now))

    def enqueue(self,user_id,payload,*,now=None):
        from pc28touzhu.services.rule_lab_queue import enqueue
        return enqueue(self.store,user_id,payload,now=_now(now))

    def set_queue_state(self,user_id,qid,payload,*,now=None):
        from pc28touzhu.services.rule_lab_queue import set_state
        return set_state(self.store,user_id,qid,payload,now=_now(now))

    def list_studies(self,user_id):return dict(studies=self.store.list_studies(user_id))

    def study_detail(self,user_id,study_id,*,observation_months=None,observation_partial=None,now=None):
        return self.store.study_comparison(user_id,study_id,observation_months=observation_months,
            observation_partial=observation_partial,now=now)

    def set_study_state(self,user_id,study_id,payload,*,now=None):
        if not isinstance(payload,dict) or set(payload)!={'state'} or payload['state'] not in ('running','paused'):
            raise RuleLabError('invalid_input','比较组状态只支持running或paused')
        return dict(study=self.store.set_study_state(user_id,study_id,payload['state'],now=_now(now)))

    def export_study_csv(self,user_id,study_id):
        data=self.store.study_comparison(user_id,study_id);study=data['study']
        fields=['experiment_id','batch_id','condition','settlement_rule_name','settlement_odds','legacy_odds','variant_key','variant_label','risk_policy',
            'settled_count','stake_total','net_profit','roi_percent','max_drawdown','worst_day_profit','profitable_days','betting_days',
            'completed_rounds','sample_label','cutoff_issue','cutoff_at','comparable','comparison_reason','input_digest',
            'delta_net_profit','delta_roi_pp','delta_drawdown']
        output=io.StringIO(newline='');writer=csv.writer(output);writer.writerow(fields)
        digest={coverage['variant_key']:coverage.get('digest') for coverage in study['actual_coverage']}
        for source in data['rows']:
            row={**source,'condition':json.dumps(source['condition'],ensure_ascii=False,separators=(',',':')),
                 'settlement_odds':json.dumps(source['settlement_odds'],ensure_ascii=False,separators=(',',':')),
                 'risk_policy':json.dumps(source['risk_policy'],ensure_ascii=False,separators=(',',':')),
                 'cutoff_issue':study['cutoff_issue'],'cutoff_at':study['cutoff_at'],'comparison_reason':study['comparison_reason'],
                 'input_digest':digest[source['variant_key']]}
            writer.writerow([_csv_cell(row.get(k)) for k in fields])
        return output.getvalue().encode('utf-8-sig')

    def create_batch(self, user_id, payload, *, now=None):
        timestamp = _now(now)
        config,predictor,definitions=self._resolve_definition(user_id,payload,now=timestamp)
        batch = self.store.create_batch(user_id, config, predictor['name'], definitions, now=timestamp)
        return dict(batch=batch, generated_count=len(definitions))

    def list_templates(self,user_id,**filters):
        from pc28touzhu.services.rule_lab_templates import list_templates
        return list_templates(self.store,user_id,**filters)

    def get_template(self,user_id,tid):
        from pc28touzhu.services.rule_lab_templates import get_template
        return get_template(self.store,user_id,tid)

    def _template_policy(self,user_id,payload):
        if not isinstance(payload,dict):raise RuleLabError('invalid_input','模板请求须为对象')
        result=copy.deepcopy(payload);definition=result.get('definition')
        if isinstance(definition,dict) and 'policy' in definition:
            if not isinstance(definition.get('metrics'),list):raise RuleLabError('invalid_input','风控模板玩法须为列表')
            for metric in definition['metrics']:
                self._risk_snapshot(user_id,definition['policy'],metric)
            # The independent template keeps values, never a mutable real-rule link.
            if isinstance(definition['policy'],dict) and definition['policy'].get('mode')=='round':definition['policy']['source']={}
        return result

    def preview_template(self,user_id,payload):
        from pc28touzhu.services.rule_lab_templates import preview_template
        return preview_template(self._template_policy(user_id,payload))

    def save_template(self,user_id,payload,*,tid=None,now=None):
        from pc28touzhu.services.rule_lab_templates import save_template,receipt
        # Retry before resolving now-deleted real-rule provenance.
        action='template.create' if tid is None else 'template.edit:'+str(tid)
        if isinstance(payload,dict) and isinstance(payload.get('request_id'),str):
            with self.store._connect() as c:
                previous=receipt(c,user_id,action,payload)
                if previous:return previous
        prepared=self._template_policy(user_id,payload)
        return save_template(self.store,user_id,prepared,now=_now(now),tid=tid,request_payload=payload)

    def delete_template(self,user_id,tid,payload,*,now=None):
        from pc28touzhu.services.rule_lab_templates import delete_template
        return delete_template(self.store,user_id,tid,payload,now=_now(now))

    def _resolve_definition(self,user_id,payload,*,now,original_batch_id=None):
        from pc28touzhu.services.rule_lab_templates import resolve_conditions,resolve_risk
        from pc28touzhu.services.rule_lab_store import _load
        if not isinstance(payload,dict):raise RuleLabError('invalid_input','实验参数无效')
        raw=copy.deepcopy(payload);selection=raw.pop('condition_selection',None);risk_selection=raw.pop('risk_selection',None)
        original=None;frozen=None
        with self.store._connect() as c:
            if original_batch_id is not None:
                original=self.store._owned(c,user_id,original_batch_id)
                preserve=selection is None and raw.get('metric')==original['metric'] and raw.get('template_pack',original['template_pack'])==original['template_pack']
                if raw.get('metric')!=original['metric'] and selection is None and raw.get('template_pack') not in ('basic_v1','basic_v2','combo_long_miss_v1'):
                    raise RuleLabError('invalid_input','更换玩法时必须重新选择条件模板')
                if preserve:
                    frozen=[dict(condition_key=r['condition_key'],name=r['name'],condition=_load(r['condition_json']),
                        settlement_rule_id=r['settlement_rule_id'],settlement_snapshot=_load(r['settlement_snapshot_json']))
                        for r in c.execute('SELECT * FROM lab_experiments WHERE batch_id=? ORDER BY id',(original_batch_id,))]
            pack=raw.get('template_pack','basic_v2')
            if selection is not None or frozen is not None:raw['template_pack']='basic_v2'
            if risk_selection is not None:
                if 'risk_policy' in raw:raise RuleLabError('invalid_input','风控模板与手动风控只能选择一种')
                policy,risk_snapshot=resolve_risk(c,user_id,risk_selection,raw.get('metric'));raw['risk_policy']=policy
            else:risk_snapshot={}
            try:config=normalize_batch_payload(raw,now=now)
            except ValueError as exc:raise RuleLabError('invalid_input',str(exc)) from exc
            if selection is not None:
                conditions,snapshot=resolve_conditions(c,user_id,selection,config['metric'])
                definitions=build_experiments_from_conditions(config['metric'],conditions)
                config.update(template_pack='custom_v1',template_snapshot=snapshot,_condition_selection=selection)
            elif frozen is not None:
                definitions=frozen;config.update(template_pack=original['template_pack'],template_snapshot=_load(original['template_snapshot_json']))
                if risk_selection is None and config['risk_policy']==_load(original['risk_policy_json']):risk_snapshot=_load(original['risk_template_snapshot_json'])
            else:definitions=build_experiments(config['metric'],template_pack=pack)
            config.update(risk_template_snapshot=risk_snapshot)
            if risk_selection is not None:config['_risk_selection']=risk_selection
        predictor=self._source_predictor(config['predictor_id'])
        try:
            if original is None or config['risk_policy']!=_load(original['risk_policy_json']):
                config['risk_policy']=self._risk_snapshot(user_id,config['risk_policy'],config['metric'])
        except ValueError as exc:raise RuleLabError('invalid_input',str(exc)) from exc
        return config,predictor,definitions

    def set_risk_policy(self,user_id,batch_id,payload,*,now=None):
        if not isinstance(payload,dict) or set(payload)!={'risk_policy'}:
            raise RuleLabError('invalid_input','请提供完整risk_policy对象')
        batch=self.store.get_batch(user_id,batch_id)
        if not batch['risk_editable']:
            raise RuleLabError('risk_policy_locked','批次已启动或有记录，风控不可修改，请新建批次',409)
        try:policy=self._risk_snapshot(user_id,payload['risk_policy'],batch['metric'])
        except ValueError as exc:raise RuleLabError('invalid_input',str(exc)) from exc
        return dict(batch=self.store.set_risk_policy(user_id,batch_id,policy,now=_now(now)))

    def replay_preview(self,user_id,batch_id,experiment_id,*,now=None):
        """Verify the available input basis without creating or retaining history."""
        timestamp=_now(now)
        batch,_=self.store.replay_definition(user_id,batch_id,experiment_id)
        preview=self.store.replay_input_preview(user_id,batch_id,experiment_id,now=timestamp)
        if batch['mode']!='backtest':
            return dict(preview,available=False,input_basis='unavailable',reason='只有历史回测支持金额重测')
        if (batch.get('continuation') or {}).get('active_segment_id'):
            return dict(preview,available=False,input_basis='unavailable',verified_range=None,
                        reason='请等待当前追加完成后重测，不能把尚未计算完的日期作为完整重测区间')
        self._source_predictor(batch['predictor_id'])
        if preview['available'] and preview['input_basis']=='local_frozen':
            return preview
        interval=preview['requested_range']
        if not interval or not interval.get('start_date') or not interval.get('end_date'):
            return dict(preview,available=False,input_basis='unavailable',reason=preview.get('reason') or '尚无可重测的已计算区间')
        days=(date.fromisoformat(interval['end_date'])-date.fromisoformat(interval['start_date'])).days+1
        if not 1<=days<=90:
            return dict(preview,available=False,input_basis='unavailable',verified_range=None,
                        reason='完整本地输入已清理；当前来源重测一次最多90天，请新建可用日期范围内的实验')
        available=self.source.replay_availability(batch['predictor_id'],interval['start_date'],interval['end_date'],observed_at=timestamp)
        if available['available'] and available.get('verified_range')!=interval:
            raise RuleLabError('source_unavailable','来源未完整核对所选区间，请稍后重试',503)
        return dict(available,requested_range=interval,
                    input_basis='current_source' if available['available'] else 'unavailable')

    def replay_experiment(self,user_id,batch_id,experiment_id,payload,*,now=None):
        if (not isinstance(payload,dict) or set(payload)!={'stake_amount','risk_amount_mode'}
                or payload['risk_amount_mode'] not in ('scale','keep')):
            raise RuleLabError('invalid_input','请提供每注金额和止盈止损调整方式（scale或keep）')
        batch,definition=self.store.replay_definition(user_id,batch_id,experiment_id)
        if batch['mode']!='backtest':
            raise RuleLabError('invalid_input','请从历史回测实验发起金额重测')
        timestamp=_now(now)
        data={key:batch[key] for key in ('name','mode','predictor_id','metric','template_pack','start_date','end_date','risk_policy')}
        data['stake_amount']=payload['stake_amount']
        try:
            config=normalize_batch_payload(data,now=timestamp)
            policy=config['risk_policy']
            if payload['risk_amount_mode']=='scale' and policy['mode']=='round':
                ratio=Decimal(config['stake_cents'])/(Decimal(str(batch['stake_amount']))*100)
                for control in [policy['round'],policy['day']]+policy['additional_day_guards']:
                    for key in ('profit_target','loss_limit'):
                        old=Decimal(str(control[key]))
                        scaled=(old*ratio).quantize(Decimal('.01'),rounding=ROUND_HALF_UP)
                        if old>0 and scaled==0:
                            raise ValueError('同比调整后的止盈止损小于0.01，请提高单注金额或保留原风控金额')
                        control[key]=float(scaled)
                if policy.get('source'):policy['source']['modified']=True
            config['risk_policy']=normalize_risk_policy(policy)
        except ValueError as exc:
            raise RuleLabError('invalid_input',str(exc)) from exc
        # Preview is only advisory; every valid submission verifies input again.
        preview=self.replay_preview(user_id,batch_id,experiment_id,now=timestamp)
        if not preview['available']:
            raise RuleLabError('replay_input_unavailable',preview['reason'] or '所选区间输入不完整，原成绩仍可查看',409)
        if preview['input_basis']=='current_source':
            config.update(preview['verified_range'])
        # Retain the original rule, condition, period and risk snapshot. The worker
        # settles the new amount per bet, including rounding and all exit gates.
        rule_name=definition['settlement_snapshot']['rule']['name']
        stake=format(Decimal(config['stake_cents'])/100,'f')
        config['name']=(definition['name']+' · '+rule_name+' · 每注'+stake+' · '+('同比风控' if payload['risk_amount_mode']=='scale' else '原额风控'))[:80]
        if preview['input_basis']=='local_frozen':
            result=self.store.create_archive_replay(user_id,batch_id,experiment_id,config,now=timestamp)
        else:
            result=self.store.create_batch(user_id,config,batch['predictor_name'],[definition],now=timestamp,
                                           replay_input_basis='current_source')
        return dict(batch=result,generated_count=1,source_batch_id=batch_id,source_experiment_id=experiment_id,
                    input_basis=preview['input_basis'])

    def set_batch_state(self, user_id, batch_id, payload, *, now=None):
        if not isinstance(payload, dict) or set(payload) != {'state'} or payload['state'] not in ('running', 'paused'):
            raise RuleLabError('invalid_input', '状态参数仅支持 running 或 paused')
        batch = self.store.get_batch(user_id, batch_id)
        if payload['state'] == 'running':
            self._source_predictor(batch['predictor_id'])
            cursor=self.store.get_work_batch(batch_id)['cursor']
            if cursor.get('replay_require_coverage') and not batch.get('started_at'):
                available=self.source.replay_availability(batch['predictor_id'],batch['start_date'],batch['end_date'],observed_at=_now(now))
                if not available['available']:
                    raise RuleLabError('replay_input_unavailable',available.get('reason') or '重测输入已不可用，尚未启动',409)
        return dict(batch=self.store.set_batch_state(user_id, batch_id, payload['state'], now=_now(now)))

    def batch_detail(self, user_id, batch_id, *, observation_months=None, observation_partial=None, now=None):
        if observation_months is not None or observation_partial is not None:
            return self.store.batch_observation_detail(user_id,batch_id,months=observation_months,
                include_partial=observation_partial,now=now)
        return dict(batch=self.store.get_batch(user_id, batch_id), experiments=self.store.list_experiments(user_id, batch_id))

    def experiment_detail(self, user_id, batch_id, experiment_id):
        return self.store.get_experiment_detail(user_id, batch_id, experiment_id)

    def export_csv(self, user_id, batch_id):
        # The ledger returns at most 100 owned experiments; no source access or replay.
        batch=self.store.get_batch(user_id, batch_id)
        experiments = self.store.list_experiments(user_id, batch_id)
        fields = ['experiment_id', 'name', 'condition', 'settlement_rule', 'settlement_odds', 'legacy_odds', 'settled_count', 'hit_count',
                  'refund_count', 'miss_count', 'stake_total', 'net_profit', 'roi_percent', 'max_drawdown',
                  'max_loss_streak', 'trigger_count', 'skipped_count','risk_mode','risk_policy','started_rounds','completed_rounds',
                  'profit_rounds','loss_rounds','flat_rounds','invalid_rounds','round_profit_stops','round_loss_stops','round_net_profit','risk_state','stop_reason']
        output = io.StringIO(newline='')
        writer = csv.writer(output)
        writer.writerow(fields)
        for experiment in experiments:
            row = dict(experiment, experiment_id=experiment['id'], settlement_rule=experiment['settlement_rule_name'],
                       settlement_odds=json.dumps(experiment['settlement_odds'],ensure_ascii=False,separators=(',',':')),
                       condition=json.dumps(experiment['condition'], ensure_ascii=False, separators=(',', ':')))
            row.update(experiment['risk_state']);row.update(risk_mode=batch['risk_policy']['mode'],
                risk_policy=json.dumps(batch['risk_policy'],ensure_ascii=False,separators=(',',':')),risk_state=experiment['risk_state']['state'])
            writer.writerow([_csv_cell(row.get(key)) for key in fields])
        return output.getvalue().encode('utf-8-sig')
