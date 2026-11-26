"""Fast configuration and result queries for the independent rule lab."""
from __future__ import annotations

import csv
import io
import json
import sqlite3
import copy
from datetime import datetime, timezone

from pc28touzhu.domain.rule_lab import ALLOWED_SETTLEMENT_RULE_IDS, COMBO_LONG_MISS_THRESHOLDS, build_conditions, build_experiments, normalize_batch_payload
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
    def __init__(self, store=None, source=None, platform_repository=None):
        self.store = store if store is not None else RuleLabStore()
        if source is None:
            from pc28touzhu.services.rule_lab_source import RuleLabSource
            source = RuleLabSource()
        self.source = source
        self.platform_repository=platform_repository
        self.store.initialize()

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
                    settlement_rules=rules, template_pack='basic_v1',
                    template_packs=[dict(id='basic_v1',name='基础条件 v1',metrics=['big_small','odd_even','combo'],
                        condition_counts={metric:len(build_conditions(metric)) for metric in ('big_small','odd_even','combo')},
                        description='无条件跟随、预测连错3～7期、连中、连续开出/不出及窗口命中率。'),
                        dict(id='combo_long_miss_v1',name='组合长连错 · 7～30期',metrics=['combo'],
                        condition_counts={'combo':len(COMBO_LONG_MISS_THRESHOLDS)+1},
                        description='无条件跟随基准＋预测连错 ≥ '+ '、'.join(map(str,COMBO_LONG_MISS_THRESHOLDS))+'期。预测连错不等于实盘连续亏损；按轮模式只在开轮时判断门槛。')],
                    default_stake_amount=1,risk_presets=self._risk_presets(user_id),
                    limits=dict(max_experiments_per_batch=100, max_running_experiments=MAX_RUNNING_EXPERIMENTS,
                                max_total_experiments=MAX_TOTAL_EXPERIMENTS, max_backtest_days=90,
                                detail_retention_days=self.store.detail_retention_days,
                                disk_budget_bytes=self.store.budget_bytes, cpu_quota_percent=20,
                                memory_max_bytes=268435456), source_available=available, source_message=message)

    def overview(self, user_id, *, now=None):
        return dict(batches=self.store.list_batches(user_id), worker=self.store.worker_status(now=_now(now)),
                    storage=self.store.storage_status())

    def comparison_preview(self,user_id,batch_id):
        return self.store.comparison_preview(user_id,batch_id)

    def create_study(self,user_id,payload,*,now=None):
        if not isinstance(payload,dict) or set(payload)!={'source_batch_id','name','request_id'}:
            raise RuleLabError('invalid_input','比较组需提供source_batch_id、name和request_id')
        return self.store.create_study(user_id,payload['source_batch_id'],payload['name'],payload['request_id'],now=_now(now))

    def list_studies(self,user_id):return dict(studies=self.store.list_studies(user_id))

    def study_detail(self,user_id,study_id):return self.store.study_comparison(user_id,study_id)

    def set_study_state(self,user_id,study_id,payload,*,now=None):
        if not isinstance(payload,dict) or set(payload)!={'state'} or payload['state'] not in ('running','paused'):
            raise RuleLabError('invalid_input','比较组状态只支持running或paused')
        return dict(study=self.store.set_study_state(user_id,study_id,payload['state'],now=_now(now)))

    def export_study_csv(self,user_id,study_id):
        data=self.store.study_comparison(user_id,study_id);study=data['study']
        fields=['experiment_id','batch_id','condition','settlement_rule_name','variant_key','variant_label','risk_policy',
            'settled_count','stake_total','net_profit','roi_percent','max_drawdown','worst_day_profit','profitable_days','betting_days',
            'completed_rounds','sample_label','cutoff_issue','cutoff_at','comparable','comparison_reason','input_digest',
            'delta_net_profit','delta_roi_pp','delta_drawdown']
        output=io.StringIO(newline='');writer=csv.writer(output);writer.writerow(fields)
        digest={coverage['variant_key']:coverage.get('digest') for coverage in study['actual_coverage']}
        for source in data['rows']:
            row={**source,'condition':json.dumps(source['condition'],ensure_ascii=False,separators=(',',':')),
                 'risk_policy':json.dumps(source['risk_policy'],ensure_ascii=False,separators=(',',':')),
                 'cutoff_issue':study['cutoff_issue'],'cutoff_at':study['cutoff_at'],'comparison_reason':study['comparison_reason'],
                 'input_digest':digest[source['variant_key']]}
            writer.writerow([_csv_cell(row.get(k)) for k in fields])
        return output.getvalue().encode('utf-8-sig')

    def create_batch(self, user_id, payload, *, now=None):
        timestamp = _now(now)
        try:
            config = normalize_batch_payload(payload, now=timestamp)
        except ValueError as exc:
            raise RuleLabError('invalid_input', str(exc)) from exc
        predictor = self._source_predictor(config['predictor_id'])
        config['risk_policy']=self._risk_snapshot(user_id,config['risk_policy'],config['metric'])
        definitions = build_experiments(config['metric'],template_pack=config['template_pack'])
        batch = self.store.create_batch(user_id, config, predictor['name'], definitions, now=timestamp)
        return dict(batch=batch, generated_count=len(definitions))

    def set_risk_policy(self,user_id,batch_id,payload,*,now=None):
        if not isinstance(payload,dict) or set(payload)!={'risk_policy'}:
            raise RuleLabError('invalid_input','请提供完整risk_policy对象')
        batch=self.store.get_batch(user_id,batch_id)
        if not batch['risk_editable']:
            raise RuleLabError('risk_policy_locked','批次已启动或有记录，风控不可修改，请新建批次',409)
        try:policy=self._risk_snapshot(user_id,payload['risk_policy'],batch['metric'])
        except ValueError as exc:raise RuleLabError('invalid_input',str(exc)) from exc
        return dict(batch=self.store.set_risk_policy(user_id,batch_id,policy,now=_now(now)))

    def set_batch_state(self, user_id, batch_id, payload, *, now=None):
        if not isinstance(payload, dict) or set(payload) != {'state'} or payload['state'] not in ('running', 'paused'):
            raise RuleLabError('invalid_input', '状态参数仅支持 running 或 paused')
        batch = self.store.get_batch(user_id, batch_id)
        if payload['state'] == 'running':
            self._source_predictor(batch['predictor_id'])
        return dict(batch=self.store.set_batch_state(user_id, batch_id, payload['state'], now=_now(now)))

    def batch_detail(self, user_id, batch_id):
        return dict(batch=self.store.get_batch(user_id, batch_id), experiments=self.store.list_experiments(user_id, batch_id))

    def experiment_detail(self, user_id, batch_id, experiment_id):
        return self.store.get_experiment_detail(user_id, batch_id, experiment_id)

    def export_csv(self, user_id, batch_id):
        # The ledger returns at most 100 owned experiments; no source access or replay.
        batch=self.store.get_batch(user_id, batch_id)
        experiments = self.store.list_experiments(user_id, batch_id)
        fields = ['experiment_id', 'name', 'condition', 'settlement_rule', 'settled_count', 'hit_count',
                  'refund_count', 'miss_count', 'stake_total', 'net_profit', 'roi_percent', 'max_drawdown',
                  'max_loss_streak', 'trigger_count', 'skipped_count','risk_mode','risk_policy','started_rounds','completed_rounds',
                  'profit_rounds','loss_rounds','flat_rounds','invalid_rounds','round_profit_stops','round_loss_stops','round_net_profit','risk_state','stop_reason']
        output = io.StringIO(newline='')
        writer = csv.writer(output)
        writer.writerow(fields)
        for experiment in experiments:
            row = dict(experiment, experiment_id=experiment['id'], settlement_rule=experiment['settlement_rule_name'],
                       condition=json.dumps(experiment['condition'], ensure_ascii=False, separators=(',', ':')))
            row.update(experiment['risk_state']);row.update(risk_mode=batch['risk_policy']['mode'],
                risk_policy=json.dumps(batch['risk_policy'],ensure_ascii=False,separators=(',',':')),risk_state=experiment['risk_state']['state'])
            writer.writerow([_csv_cell(row.get(key)) for key in fields])
        return output.getvalue().encode('utf-8-sig')
