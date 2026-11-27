"""One bounded simulation worker. No production writes or betting dispatch imports."""
from __future__ import annotations

import argparse
import fcntl
import json
import signal
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

from pc28touzhu.domain.rule_lab import build_features, match_condition, settle_bet
from pc28touzhu.domain.rule_lab_comparison import evidence_step
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore

UTC = timezone.utc
SHANGHAI = timezone(timedelta(hours=8))


def _time(value):
    parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
    if parsed.tzinfo is None:
        raise ValueError('运行时间必须包含时区')
    return parsed.astimezone(UTC)


def _stamp(value):
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')


class RuleLabWorker:
    def __init__(self, store, source, *, max_tick_seconds=2.0, page_size=200):
        if not isinstance(max_tick_seconds, (float, int)) or not 0 < max_tick_seconds <= 10:
            raise ValueError('运行预算无效')
        if type(page_size) is not int or not 1 <= page_size <= 200:
            raise ValueError('读取批次大小无效')
        self.store, self.source = store, source
        self.max_tick_seconds, self.page_size = float(max_tick_seconds), page_size
        self._last_live_poll = None
        self._processed_total = 0
        self._last_backtest_id = 0
        self._last_capture_poll = None

    def _source(self, method, *args, **kwargs):
        key = (method, repr(args), repr(sorted(kwargs.items())))
        if key not in self._cache:
            self._cache[key] = getattr(self.source, method)(*args, **kwargs)
        return self._cache[key]

    def _inputs(self, batch, method, *args, **kwargs):
        """Read every continuous input from the same frozen local segment."""
        context = batch.get('continuation')
        if not context:
            return self._source(method, *args, **kwargs)
        identity = (batch['id'], context['segment_id'], context['archive_revision'])
        if identity not in self._archives:
            self._archives[identity] = self.store.archive_source(batch['id'])
        key = ('archive', identity, method, repr(args), repr(sorted(kwargs.items())))
        if key not in self._cache:
            self._cache[key] = getattr(self._archives[identity], method)(*args, **kwargs)
        return self._cache[key]

    def _capture_inputs(self):
        """Reserve part of the existing tick for fair, bounded input capture."""
        if self._last_capture_poll is not None and (self._reference - self._last_capture_poll).total_seconds() < 15:
            return
        capture_deadline = min(self._deadline, time.monotonic() + min(.5, self.max_tick_seconds / 3))
        tasks = self.store.capture_work_items(now=self._now, limit=10)
        caught_up = True
        for task in tasks:
            if time.monotonic() >= capture_deadline or not self._remaining():
                caught_up = False
                break
            try:
                page = self._source('capture_page', task['predictor_id'],
                    after_issue=str(task.get('capture_after_issue', task['after_issue'])),
                    limit=self.page_size, observed_at=self._now)
                committed = self.store.commit_capture(task['id'], task['revision'],
                    draws=page['draws'], predictions=page['predictions'], coverage=page['coverage'],
                    next_issue=page['next_issue'], highest_seen_issue=page['highest_seen_issue'],
                    source_identity=page.get('source_identity'), now=self._now)
                if committed['committed']:
                    self._result['captured_periods'] += len(page['draws'])
                if page.get('has_more') or int(task.get('seed_after_issue', 0)) < int(task.get('seed_until_issue', 0)):
                    caught_up = False
            except RuleLabError as exc:
                self.store.set_capture_error(task['id'], exc.code, exc.message, now=self._now)
                self._result.update(state='resource_paused' if exc.code == 'storage_limit' else 'error', message=exc.message)
            except (ValueError, TypeError, KeyError):
                self.store.set_capture_error(task['id'], 'source_unavailable', '输入采集返回的数据格式异常，稍后重试', now=self._now)
                self._result.update(state='error', message='输入采集返回的数据格式异常，稍后重试')
        self._result['capture_pending'] = bool(tasks) and not caught_up
        self._last_capture_poll = self._reference if caught_up else None

    def _remaining(self):
        return time.monotonic() < self._deadline

    def _commit(self, work, **kwargs):
        result = self.store.commit_step(work['batch']['id'], work['batch']['revision'], now=self._now,
                                       cursor=kwargs.pop('cursor', dict(work['batch']['cursor'])), **kwargs)
        for key in ('inserted_decisions', 'settled', 'skipped'):
            self._result[key] += result[key]
        if result['committed']:
            delta = (kwargs.get('progress') or {}).get('processed_issues_delta', 0)
            self._result['processed_issues'] += delta
        return result['committed']

    def _mark_error(self, batch_id, message, *, resource=False):
        try:
            work = self.store.get_work_item(batch_id)
        except RuleLabError as exc:
            if exc.code == 'not_found':
                return
            raise
        if work['batch']['status'] == 'deleting':
            return
        self._commit(work, progress={'status': 'resource_paused' if resource else 'error', 'error': message})
        self._result.update(state='resource_paused' if resource else 'error', message=message)

    @staticmethod
    def _asof_predictions(items,study):
        if not study:return items
        cutoff=_time(study['cutoff_at']);result=[]
        for item in items:
            if _time(item['created_at'])>cutoff:continue
            item=dict(item)
            if item.get('settled_at') and _time(item['settled_at'])>cutoff:item.update(status='pending',settled_at=None)
            result.append(item)
        return result

    def _history(self, predictor_id, target, study=None, *, batch=None):
        rows = (self._inputs(batch, 'get_history', predictor_id, before_issue=target)
                if batch and batch.get('continuation') else self.store.get_history(predictor_id, before_issue=target))
        if study:
            cutoff=_time(study['cutoff_at'])
            rows=[r for r in rows if _time(r['draw_available_at'])<=cutoff and int(r['issue_no'])<=int(study['cutoff_issue'])]
            for row in rows:
                if row.get('prediction_created_at') and _time(row['prediction_created_at'])>cutoff:
                    row.update(prediction_created_at=None,prediction_settled_at=None,prediction_values={})
                elif row.get('prediction_settled_at') and _time(row['prediction_settled_at'])>cutoff:row['prediction_settled_at']=None
        if rows and int(rows[-1]['issue_no']) < int(target) - 1:
            # A missing immediate predecessor must break streaks rather than make an older run look current.
            rows.append(dict(issue_no=str(int(target) - 1), open_at=None, draw_available_at=None,
                             triplet=None, prediction_values={}, prediction_created_at=None,
                             prediction_settled_at=None, conflict=False))
        return rows[-100:]

    def _outcome(self, experiment, decision, draw):
        if _time(decision['decided_at']) > _time(draw['open_at']) - timedelta(seconds=40):
            return dict(experiment_id=experiment['id'], issue_no=decision['issue_no'], status='skipped',
                        settled_at=draw['open_at'], result_type=None, net_cents=None, direction_hit=False,
                        refund_reason='', skip_reason='late_decision')
        value = settle_bet(experiment['settlement_snapshot'], experiment['condition']['metric'],
                           decision['bet_value'], decision['stake_cents'], draw['triplet'])
        return dict(experiment_id=experiment['id'], issue_no=decision['issue_no'], status='settled',
                    settled_at=draw['open_at'], skip_reason='', **value)

    def _settle_pending(self, batch_id, *, available_at=None):
        batch=self.store.get_work_batch(batch_id)
        continuation=batch.get('continuation')
        if continuation:
            if self._source('get_predictor', batch['predictor_id']) is None:
                raise RuleLabError('source_unavailable', '来源已停用或不再共享逐期预测，已保留续算进度', 403)
            frontier=available_at or batch['cursor'].get('simulation_time') or continuation.get('frontier_time')
            if not frontier or continuation.get('segment_kind')=='baseline':
                return
            if batch['status']!='running':
                frontier=batch['cursor'].get('simulation_time') or continuation.get('frontier_time')
                if not frontier:
                    return
            reference=min(_time(frontier),_time(continuation['cutoff_at']))
        else:
            reference=_time(available_at) if available_at is not None else self._reference
        study=batch.get('study')
        if study:reference=min(reference,_time(study['cutoff_at']))
        while self._remaining():
            pending = self.store.get_pending_decisions(batch_id)
            if not pending:
                return
            groups = {}
            for decision in pending:
                groups.setdefault(decision['issue_no'], []).append(decision)
            for issue, decisions in groups.items():
                if not self._remaining():
                    return
                draw = self._inputs(batch, 'get_draw_by_issue', issue, observed_at=self._now)
                if study and (draw is None or _time(draw['source_created_at'])>_time(study['cutoff_at']) or _time(draw['open_at'])>_time(study['cutoff_at'])):
                    self._mark_error(batch_id,'比较组待结算输入已缺失或变更到共同截止之后，请核对历史数据后重新比较')
                    return
                if draw is None or _time(draw['open_at']) > reference or _time(draw['source_created_at']) > reference:
                    return  # An unresolved earlier result blocks later ones.
                work = self.store.get_work_item(batch_id)
                if (continuation and work['batch']['status']!='running'
                        and (not work['batch']['cursor'].get('simulation_time')
                             or reference>_time(work['batch']['cursor']['simulation_time']))):
                    return
                experiments = {e['id']: e for e in work['experiments']}
                outcomes = [self._outcome(experiments[d['experiment_id']], d, draw) for d in decisions]
                if not self._commit(work, draws=[draw], outcomes=outcomes):
                    return
                unresolved = self.store.get_pending_decisions(batch_id, limit=1)
                if unresolved and int(unresolved[0]['issue_no']) <= int(issue):
                    self._result.update(state='error', message='开奖输入存在冲突，已冻结决策等待核对')
                    return
            # More than one page may become visible at the same event time.
            # Drain all due pages before allowing the next decision to advance.

    def _finish_backtest(self,batch_id):
        work=self.store.get_work_item(batch_id)
        if work['batch']['status']!='running':return
        cursor=dict(work['batch']['cursor'])
        if work['batch'].get('continuation'):
            # The next segment owns the next decision-time event. Keep pending
            # and active rounds rather than flushing them at the real wallclock.
            self.store.complete_segment(batch_id,work['batch']['revision'],cursor=cursor,now=self._now)
            return
        if not cursor.get('replay_complete'):
            cursor['replay_complete']=True
            if not self._commit(work,cursor=cursor):return
        self._settle_pending(batch_id)
        work=self.store.get_work_item(batch_id)
        if work['batch']['status']=='running' and not self.store.get_pending_decisions(batch_id,limit=1):
            self._commit(work,progress={'status':'completed','phase':'idle','total_issues':work['batch']['processed_issues']})

    @staticmethod
    def _decisions(work, features, prediction, *, decided_at, issue, target_open_at=None):
        value = prediction.get(work['batch']['metric'])
        decisions = []
        match_cache = {}
        for experiment in work['experiments']:
            condition = experiment['condition']
            key = condition['condition_key']
            if key not in match_cache:
                match_cache[key] = match_condition(condition, features)
            matched = match_cache[key]
            round_mode=work['batch'].get('risk_policy',{}).get('mode')=='round'
            continuing=round_mode and bool(experiment.get('risk_runtime',{}).get('active_round'))
            if matched['matched'] or continuing:
                decisions.append(dict(experiment_id=experiment['id'], issue_no=issue, bet_value=value,
                    stake_cents=work['batch']['stake_cents'], decided_at=decided_at,
                    decision_mode=work['batch']['mode'], feature_issue_no=features['latest_issue_no'],
                    condition_value=matched['value'],condition_matched=matched['matched'],
                    risk_date=_time(target_open_at or decided_at).astimezone(SHANGHAI).date().isoformat()))
        return decisions

    def _observe_live(self, batch_id):
        work = self.store.get_work_item(batch_id)
        batch, cursor = work['batch'], dict(work['batch']['cursor'])
        if batch['status'] != 'running' or self.store.get_pending_decisions(batch_id, limit=1):
            return
        if self._source('get_predictor', batch['predictor_id']) is None:
            self._mark_error(batch_id, '来源已停用或不再共享逐期预测，请核对后恢复')
            return
        latest = self._source('latest_draw', observed_at=self._now)
        if latest is None or _time(latest['open_at']) > self._reference or _time(latest['source_created_at']) > self._reference:
            return
        target = str(int(latest['issue_no']) + 1)
        if cursor.get('last_decided_issue') and int(cursor['last_decided_issue']) >= int(target):
            return
        if self._reference > _time(latest['open_at']) + timedelta(seconds=170):
            if cursor.get('last_skip_target') != target:
                cursor['last_skip_target'] = target
                self._commit(work, cursor=cursor, progress={'phase': 'observing', 'skip_counts_delta': {'closed_window': 1}})
            return
        predictions = self._source('get_predictions', batch['predictor_id'], [target], observed_at=self._now)
        prediction = next(iter(predictions['items']), None)
        if not prediction or prediction['status'] != 'pending' or _time(prediction['created_at']) > self._reference or not prediction.get(batch['metric']):
            return
        history_page = self._source('history_draws', before_issue=target, limit=100, observed_at=self._now)
        history_draws = history_page['items']
        history_predictions = self._source('get_predictions', batch['predictor_id'],
                    [d['issue_no'] for d in history_draws], observed_at=self._now)
        cursor.update(version=1, history_floor_issue=history_draws[0]['issue_no'] if history_draws else latest['issue_no'],
                      last_observed_draw_issue=latest['issue_no'])
        if not self._commit(work, cursor=cursor, draws=history_draws, predictions=history_predictions['items'] + [prediction],
                            progress={'phase': 'observing'}):
            return
        if not self._remaining():
            return
        work = self.store.get_work_item(batch_id)
        history = self._history(batch['predictor_id'], target)
        features_key = (batch['predictor_id'], batch['metric'], target, self._now)
        if features_key not in self._features:
            self._features[features_key] = build_features(batch['metric'], history, decision_at=self._now)
        features = self._features[features_key]
        freeze_time = self._reference if self._fixed_clock else datetime.now(UTC)
        if freeze_time > _time(latest['open_at']) + timedelta(seconds=170):
            return
        decided_at = _stamp(freeze_time)
        decisions = self._decisions(work, features, prediction, decided_at=decided_at, issue=target,
            target_open_at=_stamp(_time(latest['open_at'])+timedelta(seconds=210)))
        cursor = dict(work['batch']['cursor'])
        cursor.update(last_decided_issue=target, last_issue_no=target)
        gap = max(0, int(target) - int(batch['cursor'].get('last_decided_issue', target)) - 1)
        skipped = dict(history_page['skip_counts'])
        skipped.update(history_predictions['skip_counts'])
        if gap:
            skipped['missed_observation'] = gap
        self._commit(work, cursor=cursor, decisions=decisions,
                     progress={'processed_issues_delta': 1, 'skip_counts_delta': skipped, 'phase': 'observing'})

    def _start_backtest(self, work):
        batch = work['batch']
        study=batch.get('study')
        strict=batch['cursor'].get('replay_require_coverage') is True
        if strict:
            available=self._source('replay_availability',batch['predictor_id'],batch['start_date'],batch['end_date'],observed_at=self._now)
            if not available['available']:
                raise RuleLabError('replay_input_unavailable',available['reason'] or '重测所需输入不完整，已保留原成绩',409)
        latest = {'issue_no':study['cutoff_issue']} if study else self._inputs(batch,'latest_draw', observed_at=self._now)
        first = self._inputs(batch,'find_start_issue', batch['start_date'], observed_at=self._now) if latest else None
        if first is None or int(first)>int(latest['issue_no']):
            if strict:raise RuleLabError('replay_input_unavailable','重测所需开奖已清理，不能声明空区间完成',409)
            self._commit(work, progress={'status': 'completed', 'phase': 'idle', 'total_issues': 0,
                                        'skip_counts_delta': {'no_source_history': 1}})
            return False
        if strict:
            following=(datetime.fromisoformat(batch['end_date'])+timedelta(days=1)).date().isoformat()
            after=self._source('find_start_issue',following,observed_at=self._now)
            if after:latest={**latest,'issue_no':str(min(int(latest['issue_no']),int(after)-1))}
        warm = self._inputs(batch,'history_draws', before_issue=first, limit=100, observed_at=self._now)
        if study:warm={**warm,'items':[d for d in warm['items'] if _time(d['source_created_at'])<=_time(study['cutoff_at'])]}
        predictions = self._inputs(batch,'get_predictions', batch['predictor_id'], [d['issue_no'] for d in warm['items']], observed_at=self._now)
        predictions={**predictions,'items':self._asof_predictions(predictions['items'],study)}
        if strict:
            if [int(d['issue_no']) for d in warm['items']]!=list(range(int(first)-100,int(first))):
                raise RuleLabError('replay_input_unavailable','重测所需暖机区间缺期，已停止本次重测',409)
            self._source('validate_replay_page',batch['predictor_id'],warm['items'],predictions['items'],observed_at=self._now)
        before = str(int(first) - 1)
        cursor = dict(work['batch']['cursor'])
        cursor.update(version=1, replay_scheduler_version=2, replay_max_issue=latest['issue_no'], scan_after_issue=before if len(before) == len(first) else '',
                      history_floor_issue=warm['items'][0]['issue_no'] if warm['items'] else first,warm_before_issue=first)
        skips = Counter(warm['skip_counts']) + Counter(predictions['skip_counts'])
        if not self._commit(work, cursor=cursor, draws=warm['items'], predictions=predictions['items'],
                            progress={'phase': 'replaying', 'skip_counts_delta': dict(skips)}):return False
        if study:
            return self._warm_evidence(self.store.get_work_item(batch['id']))
        return True

    def _warm_evidence(self,work):
        batch=work['batch'];cursor=dict(batch['cursor'])
        if cursor.get('evidence') is not None:return True
        history=self._history(batch['predictor_id'],cursor['warm_before_issue'],batch['study'],batch=batch)
        cursor['evidence']=dict(digest=evidence_step(None,{'kind':'warm','history':history}),count=0,
            warm_count=len(history),first_issue=None,last_issue=None,conflict=any(r.get('conflict') for r in history))
        return self._commit(work,cursor=cursor)

    def _replay_backtest(self, batch_id):
        work = self.store.get_work_item(batch_id)
        batch = work['batch']
        if batch['status'] != 'running':
            return
        if batch['cursor'].get('replay_complete'):
            self._finish_backtest(batch_id)
            return
        if self._source('get_predictor', batch['predictor_id']) is None:
            self._mark_error(batch_id, '来源已停用或不再共享逐期预测，请核对后恢复')
            return
        if not batch['cursor'].get('replay_max_issue'):
            if not self._start_backtest(work) or not self._remaining():
                return
            work = self.store.get_work_item(batch_id)
            batch = work['batch']
        if batch.get('study') and batch['cursor'].get('evidence') is None:
            if not self._warm_evidence(work):return
            work=self.store.get_work_item(batch_id);batch=work['batch']
        cursor = dict(batch['cursor'])
        page = self._inputs(batch, 'draws_page', after_issue=cursor.get('scan_after_issue', ''),
                            before_issue=str(int(cursor['replay_max_issue']) + 1), limit=self.page_size, observed_at=self._now)
        if cursor.get('replay_require_coverage'):
            after=int(cursor.get('scan_after_issue') or 0)
            issues=[int(draw['issue_no']) for draw in page['items']]
            if (page['skip_counts'] or issues!=list(range(after+1,after+1+len(issues)))
                or (not issues and after<int(cursor['replay_max_issue']))
                or (not page['has_more'] and issues and issues[-1]<int(cursor['replay_max_issue']))):
                raise RuleLabError('replay_input_unavailable','重测区间开奖缺失或无效，已停止；不能缩短区间后声明完成',409)
        previous = cursor.get('last_issue_no')
        if previous and int(previous) < int(cursor['replay_max_issue']):
            first_issue = page['items'][0]['issue_no'] if page['items'] else None
            gap = first_issue is None or int(first_issue) > int(previous) + 1
            if gap and self._inputs(batch, 'get_draw_by_issue', previous, observed_at=self._now) is None:
                cursor['input_expired'] = True
                self._commit(work, cursor=cursor, progress={'status': 'error', 'error': '回放所需历史已被清理，请新建批次选择仍保留的日期',
                                                           'skip_counts_delta': {'input_expired': 1}})
                self._result.update(state='error', message='回放所需历史已被清理，请新建批次')
                return
        if not page['scanned_count']:
            self._finish_backtest(batch_id)
            return
        predictions = self._inputs(batch, 'get_predictions', batch['predictor_id'], [d['issue_no'] for d in page['items']], observed_at=self._now)
        if cursor.get('replay_require_coverage'):
            self._source('validate_replay_page',batch['predictor_id'],page['items'],predictions['items'],observed_at=self._now)
        study=batch.get('study')
        predictions={**predictions,'items':self._asof_predictions(predictions['items'],study)}
        cache_draws=[d for d in page['items'] if not study or _time(d['source_created_at'])<=_time(study['cutoff_at'])]
        if not self._commit(work, draws=cache_draws, predictions=predictions['items'], progress={'phase': 'replaying'}):
            return
        prediction_map = {p['issue_no']: p for p in predictions['items']}
        all_processed = True
        for draw in page['items']:
            if not self._remaining():
                all_processed = False
                break
            work = self.store.get_work_item(batch_id)
            if work['batch']['status'] != 'running':
                return
            cursor = dict(work['batch']['cursor'])
            local_date = _time(draw['open_at']).astimezone(SHANGHAI).date().isoformat()
            if local_date > batch['end_date']:
                self._finish_backtest(batch_id)
                return
            if _time(draw['open_at']) > self._reference or _time(draw['source_created_at']) > self._reference:
                # Historical upper bound only includes actually available results.
                return
            issue = draw['issue_no']
            cursor.update(scan_after_issue=issue, last_issue_no=issue)
            if study and _time(draw['source_created_at'])>_time(study['cutoff_at']):
                self._commit(work,cursor=cursor,progress={'skip_counts_delta':{'after_study_cutoff':1}})
                continue
            decision_at = _stamp(_time(draw['open_at']) - timedelta(seconds=40))
            round_mode=batch.get('risk_policy',{}).get('mode')=='round'
            continuous=bool(batch.get('continuation'))
            if continuous and cursor.get('simulation_time') and _time(decision_at)<_time(cursor['simulation_time']):
                raise RuleLabError('archive_conflict','开奖时间顺序倒退，已保留原进度并停止本段续算',409)
            if round_mode or continuous:
                self._settle_pending(batch_id,available_at=decision_at)
                # If the budget expired before due settlements could finish, retry
                # this same draw next tick. Advancing it would silently suppress an
                # otherwise valid entry because the prior decision is still pending.
                if not self._remaining():
                    return
                work=self.store.get_work_item(batch_id)
                if work['batch']['status']!='running':return
                cursor=dict(work['batch']['cursor'])
                cursor.update(scan_after_issue=issue,last_issue_no=issue,simulation_time=decision_at)
            prediction = prediction_map.get(issue)
            decisions, outcomes, skips = [], [], {};features=None
            previous_issue = work['batch']['cursor'].get('last_issue_no')
            if previous_issue and int(issue) > int(previous_issue) + 1:
                skips['draw_gap'] = int(issue) - int(previous_issue) - 1
            if local_date < batch['start_date']:
                self._commit(work, cursor=cursor)
                continue
            if not prediction or not prediction.get(batch['metric']):
                skips['missing_prediction'] = 1
            elif _time(prediction['created_at']) > _time(decision_at):
                skips['late_prediction'] = 1
            else:
                history = self._history(batch['predictor_id'], issue,study,batch=batch)
                if history:
                    cursor['history_floor_issue'] = history[0]['issue_no']
                features = build_features(batch['metric'], history, decision_at=decision_at)
                decisions = self._decisions(work, features, prediction, decided_at=decision_at, issue=issue,target_open_at=draw['open_at'])
                by_id = {e['id']: e for e in work['experiments']}
                outcomes = [] if round_mode or continuous else [self._outcome(by_id[d['experiment_id']], d, draw) for d in decisions]
            if study:
                cached=(self._inputs(batch,'input_evidence',batch['predictor_id'],issue)
                        if continuous else self.store.input_evidence(batch['predictor_id'],issue))
                if cached.get('prediction'):
                    cached['prediction']=self._asof_predictions([cached['prediction']],study)
                evidence=dict(cursor.get('evidence') or {})
                conflict=bool(evidence.get('conflict')) or bool(cached.get('draw') and cached['draw'].get('conflict'))
                prediction_cached=cached.get('prediction') or []
                conflict|=any(p.get('conflict') for p in prediction_cached)
                evidence.update(digest=evidence_step(evidence.get('digest'),{'kind':'period','draw':draw,'prediction':prediction,
                    'cached':cached,'features':features}),count=evidence.get('count',0)+1,
                    first_issue=evidence.get('first_issue') or issue,last_issue=issue,conflict=conflict)
                cursor['evidence']=evidence
            if not self._commit(work, cursor=cursor, decisions=decisions, outcomes=outcomes,
                                progress={'processed_issues_delta': 1, 'skip_counts_delta': skips, 'phase': 'replaying'}):
                return
            if not round_mode and not continuous and self.store.get_pending_decisions(batch_id, limit=1):
                return
        if all_processed:
            work = self.store.get_work_item(batch_id)
            if work['batch']['status'] != 'running':
                return
            cursor = dict(work['batch']['cursor'])
            cursor['scan_after_issue'] = page['next_issue']
            skips = Counter(page['skip_counts'])
            # Missing/unusable prediction is counted per processed issue above. Page-level
            # prediction parse totals would depend on worker budget/restarts and page size.
            if not study and not batch.get('continuation'):skips+=Counter(predictions['skip_counts'])
            progress = {'skip_counts_delta': dict(skips)}
            if not page['has_more'] or (page['next_issue'].isdigit() and int(page['next_issue']) >= int(cursor['replay_max_issue'])):
                cursor['replay_complete']=True
            if self._commit(work, cursor=cursor, progress=progress) and cursor.get('replay_complete'):
                self._finish_backtest(batch_id)

    def tick(self, *, now=None):
        with self.store.worker_session():
            return self._tick(now=now)

    def _tick(self, *, now=None):
        started = time.monotonic()
        self._deadline = started + self.max_tick_seconds
        self._reference = _time(now) if now is not None else datetime.now(UTC)
        self._fixed_clock = now is not None
        self._now = _stamp(self._reference)
        self._cache, self._features, self._archives = {}, {}, {}
        self._result = dict(processed_issues=0, inserted_decisions=0, settled=0, skipped=0,
                            captured_periods=0, capture_pending=False, maintenance_pending=False,
                            maintenance_rows=0, freed_bytes=0, state='idle', message='', elapsed_ms=0.0)
        try:
            maintenance=self.store.management_maintenance(now=self._now,limit=500,max_pages=256)
            self._result.update(maintenance_pending=maintenance['maintenance_pending'],
                                maintenance_rows=maintenance['processed_rows'],freed_bytes=maintenance['freed_bytes'])
            remaining=500-maintenance['processed_rows']
            if remaining>0:
                retained=self.store.retention_maintenance(now=self._now,limit=remaining)
                self._result['maintenance_rows']+=retained['processed_rows']
                self._result['maintenance_pending']|=retained['maintenance_pending']
                self._result['retention_phase']=retained['phase']
        except RuleLabError as exc:
            # Deletion/reclaim has independent durable progress. A busy job must
            # not prevent unrelated simulations from making use of this tick.
            self._result.update(maintenance_pending=True,maintenance_error=exc.message)
        self._capture_inputs()
        if self._remaining():
            self.store.prepare_due_appends(now=self._now,limit=10)
        cutoff=None
        if self.store.studies_need_cutoff():
            try:
                latest=self._source('latest_draw',observed_at=self._now)
                if latest and _time(latest['open_at'])<=self._reference and _time(latest['source_created_at'])<=self._reference:
                    cutoff={'issue_no':latest['issue_no'],'at':self._now}
                elif not latest:self._result.update(state='idle',message='比较队列等待有效历史开奖，尚未冻结共同截止')
            except RuleLabError as exc:self._result.update(state='error',message=exc.message)
        self.store.advance_studies(now=self._now,cutoff=cutoff)
        batches = self.store.list_work_batches()
        poll_live = self._last_live_poll is None or (self._reference - self._last_live_poll).total_seconds() >= 15
        if poll_live:
            self._last_live_poll = self._reference
        blocked = set()
        for batch in batches:
            if not self._remaining():
                break
            try:
                if poll_live:
                    if batch.get('continuation'):
                        self._settle_pending(batch['id'],available_at=batch['cursor'].get('simulation_time'))
                    elif batch['mode']=='backtest' and batch.get('risk_policy',{}).get('mode')=='round' and not batch['cursor'].get('replay_complete'):
                        if batch['cursor'].get('simulation_time'):
                            self._settle_pending(batch['id'],available_at=batch['cursor']['simulation_time'])
                    else:self._settle_pending(batch['id'])
            except RuleLabError as exc:
                blocked.add(batch['id'])
                if exc.code in ('not_found','deleting'):
                    continue
                self._result.update(state='resource_paused' if exc.code == 'storage_limit' else 'error', message=exc.message)
                if exc.code == 'storage_limit':
                    self._mark_error(batch['id'], exc.message, resource=True)
        for mode in ('live', 'backtest'):
            mode_batches = [b for b in batches if b['mode'] == mode]
            if mode == 'backtest':
                mode_batches.sort(key=lambda b: (b['id'] <= self._last_backtest_id, b['id']))
            for batch in mode_batches:
                if not self._remaining():
                    break
                if batch['id'] in blocked or batch['mode'] != mode or batch['status'] != 'running' or (mode == 'live' and not poll_live):
                    continue
                try:
                    if self.store.storage_status()['used_bytes'] >= self.store.budget_bytes * .8:
                        raise RuleLabError('storage_limit', '模拟存储接近预算上限，已暂停新模拟', 503)
                    if mode == 'live':
                        self._observe_live(batch['id'])
                    else:
                        self._last_backtest_id = batch['id']
                        self._replay_backtest(batch['id'])
                    if self._result['state'] == 'idle':
                        self._result['state'] = 'running'
                except RuleLabError as exc:
                    if exc.code in ('not_found','deleting'):
                        continue
                    self._result.update(state='resource_paused' if exc.code == 'storage_limit' else 'error', message=exc.message)
                    if exc.code == 'storage_limit':
                        self._mark_error(batch['id'], exc.message, resource=True)
                    elif exc.code=='replay_input_unavailable':
                        self._mark_error(batch['id'],exc.message)
                    elif batch.get('continuation') and exc.code in ('archive_conflict','archive_corrupt','archive_gap'):
                        self._mark_error(batch['id'], exc.message)
                except (ValueError, TypeError, KeyError):
                    self._mark_error(batch['id'], '模拟输入或状态异常，请核对数据后恢复')
                    self._result.update(state='error', message='模拟输入或状态异常，已暂停相关批次')
        self.store.advance_studies(now=self._now,cutoff=cutoff)
        if self._remaining():
            self.store.prepare_due_appends(now=self._now,limit=10)
        self._processed_total += self._result['processed_issues']
        self._result['elapsed_ms'] = round((time.monotonic() - started) * 1000, 3)
        self.store.set_worker_status(dict(state=self._result['state'], message=self._result['message'],
                    last_tick_ms=self._result['elapsed_ms'], processed_issues=self._processed_total), now=self._now)
        return self._result


def main():
    parser = argparse.ArgumentParser(description='轻量规则实验室模拟进程（不发送投注）')
    parser.add_argument('--once', action='store_true', help='仅执行一次有界工作周期')
    args = parser.parse_args()
    store = RuleLabStore()
    store.initialize()
    stopped = False
    def stop(signum, frame):
        nonlocal stopped
        stopped = True
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    with (store.db_path.parent / 'worker.lock').open('a') as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('模拟进程已经运行')
        worker = RuleLabWorker(store, RuleLabSource())
        failures, last_message = 0, None
        while not stopped:
            try:
                result = worker.tick()
                message = (result['state'], result['message'])
                if message != last_message:
                    print(json.dumps({'state': message[0], 'message': message[1]}, ensure_ascii=False), flush=True)
                    last_message = message
                failures = min(failures + 1, 5) if result['state'] == 'error' else 0
                if args.once:
                    break
                active_history = result.get('capture_pending') or result.get('maintenance_pending') or any(b['mode'] == 'backtest' and b['status'] == 'running' for b in store.list_work_batches())
                delay = min(60.0, 2 ** failures) if failures else .25 if active_history else 15.0
            except (RuleLabError, OSError) as exc:
                failures = min(failures + 1, 5)
                if failures == 1:
                    print(json.dumps({'state': 'error', 'message': '模拟进程暂不可用，保留进度后重试'}, ensure_ascii=False), flush=True)
                if args.once:
                    raise SystemExit(1) from exc
                delay = min(60.0, 2 ** failures)
            until = time.monotonic() + delay
            while not stopped and time.monotonic() < until:
                time.sleep(min(.25, max(0, until - time.monotonic())))


if __name__ == '__main__':
    main()
