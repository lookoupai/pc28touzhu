"""Fast configuration and result queries for the independent rule lab."""
from __future__ import annotations

import csv
import io
import json
import sqlite3
from datetime import datetime, timezone

from pc28touzhu.domain.rule_lab import ALLOWED_SETTLEMENT_RULE_IDS, build_experiments, normalize_batch_payload
from pc28touzhu.domain.settlement_rules import build_settlement_snapshot
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
    def __init__(self, store=None, source=None):
        self.store = store if store is not None else RuleLabStore()
        if source is None:
            from pc28touzhu.services.rule_lab_source import RuleLabSource
            source = RuleLabSource()
        self.source = source
        self.store.initialize()

    def _source_predictor(self, predictor_id):
        try:
            predictor = self.source.get_predictor(predictor_id)
        except (RuleLabError, OSError, sqlite3.Error) as exc:
            raise RuleLabError('source_unavailable', '预测来源暂不可用，请稍后重试', 503) from exc
        if predictor is None:
            raise RuleLabError('invalid_input', '请选择仍在公开逐期预测的有效来源')
        return predictor

    def options(self, *, now=None):
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
                    settlement_rules=rules, template_pack='basic_v1', default_stake_amount=1,
                    limits=dict(max_experiments_per_batch=100, max_running_experiments=MAX_RUNNING_EXPERIMENTS,
                                max_total_experiments=MAX_TOTAL_EXPERIMENTS, max_backtest_days=90,
                                detail_retention_days=self.store.detail_retention_days,
                                disk_budget_bytes=self.store.budget_bytes, cpu_quota_percent=20,
                                memory_max_bytes=268435456), source_available=available, source_message=message)

    def overview(self, user_id, *, now=None):
        return dict(batches=self.store.list_batches(user_id), worker=self.store.worker_status(now=_now(now)),
                    storage=self.store.storage_status())

    def create_batch(self, user_id, payload, *, now=None):
        timestamp = _now(now)
        try:
            config = normalize_batch_payload(payload, now=timestamp)
        except ValueError as exc:
            raise RuleLabError('invalid_input', str(exc)) from exc
        predictor = self._source_predictor(config['predictor_id'])
        definitions = build_experiments(config['metric'])
        batch = self.store.create_batch(user_id, config, predictor['name'], definitions, now=timestamp)
        return dict(batch=batch, generated_count=len(definitions))

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
        self.store.get_batch(user_id, batch_id)
        experiments = self.store.list_experiments(user_id, batch_id)
        fields = ['experiment_id', 'name', 'condition', 'settlement_rule', 'settled_count', 'hit_count',
                  'refund_count', 'miss_count', 'stake_total', 'net_profit', 'roi_percent', 'max_drawdown',
                  'max_loss_streak', 'trigger_count', 'skipped_count']
        output = io.StringIO(newline='')
        writer = csv.writer(output)
        writer.writerow(fields)
        for experiment in experiments:
            row = dict(experiment, experiment_id=experiment['id'], settlement_rule=experiment['settlement_rule_name'],
                       condition=json.dumps(experiment['condition'], ensure_ascii=False, separators=(',', ':')))
            writer.writerow([_csv_cell(row.get(key)) for key in fields])
        return output.getvalue().encode('utf-8-sig')
