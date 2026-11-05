"""自动过关会话调度：规则管启动，周期只管本轮投注与结算。"""
from __future__ import annotations

import copy
from datetime import datetime, timezone
from typing import Any

from pc28touzhu.domain.auto_trigger_cycle import auto_roll_policy, is_roll_subscription, subscription_strategy
from pc28touzhu.domain.pc28_issue_window import evaluate_pc28_issue_dispatch_window
from pc28touzhu.domain.win_streak_roll import create_cycle_state, normalize_stop_policy, CYCLE_DAY_STOP_REASONS


class StartSkipped(Exception):
    """退出整个开轮事务，不能留下额度或空轮次。"""


def _owner(rule: dict, sub: dict) -> dict:
    return {'rule_id': int(rule['id']), 'subscription_id': int(sub['id']), 'user_id': int(rule['user_id'])}


def _newer(issue: str, previous: str) -> bool:
    if not previous:
        return bool(issue)
    try:
        return int(issue) > int(previous)
    except (TypeError, ValueError):
        return False


def _routes(repository: Any, rule: dict, sub: dict) -> list[dict]:
    routes = rule.get('routes') or []
    if routes:
        return list(routes)
    selected = subscription_strategy(sub).get('dispatch', {}).get('delivery_target_ids') or []
    targets = repository.list_delivery_targets(int(rule['user_id']))
    return [repository.ensure_implicit_auto_trigger_route(rule_id=int(rule['id']), user_id=int(rule['user_id']), delivery_target_id=int(t['id']))
            for t in targets if t.get('status') == 'active' and t.get('dispatch_mode') != 'direct_only'
            and (not selected or int(t['id']) in selected)]


def _performance(rule: dict, sub: dict, *, fetcher, draw_fetcher, draw_clock, draw_cache, cache: dict) -> tuple[dict, dict | None]:
    from pc28touzhu.services import auto_trigger_service as ats
    conditions = list(rule.get('conditions') or [])
    for group in rule.get('guard_groups') or []:
        conditions.extend(group.get('conditions') or [])
    needs_draw = any(c.get('type') == 'draw_streak' for c in conditions)
    needs_prediction = any(c.get('type') != 'draw_streak' for c in conditions)
    performance: dict = {'metrics': {}}
    if needs_prediction:
        try:
            url = ats._performance_url_from_source(sub.get('source') or {})
            if url not in cache:
                cache[url] = ats._fetch_performance(url, fetcher=fetcher)[0]
            performance = copy.deepcopy(cache[url])
        except Exception as exc:
            if not needs_draw:
                raise
            performance['prediction_error'] = str(exc)
    if needs_draw:
        try:
            data = ats._load_trigger_draw_data(draw_cache, draw_clock=draw_clock, draw_fetcher=draw_fetcher)
        except Exception as exc:
            if not performance.get('latest_settled_issue'):
                raise
            performance['draw_streak_error'] = str(exc)
        else:
            snapshot = data['snapshot']
            if needs_prediction and str(performance.get('latest_settled_issue') or '') != snapshot['latest_issue_no']:
                performance['metrics'] = {}
            performance['latest_settled_issue'] = snapshot['latest_issue_no']
            performance['draw_streak'] = snapshot
            draw_clock = data['clock']
    return performance, draw_clock


def _session_gate(repository, rule, sub, session, day):
    from pc28touzhu.services import auto_trigger_service as ats
    if rule.get('status') != 'active':
        return 'rule_not_active'
    if sub.get('status') not in {'active', 'standby'}:
        return 'subscription_not_active'
    source = repository.get_source(int(sub['source_id']))
    if not source or source.get('status') != 'active':
        return 'source_not_active'
    if ats._is_rule_day_stopped(repository, rule, stat_date=day).get('stopped'):
        return 'daily_risk_stopped'
    if session.get('status') == 'blocked':
        return session.get('stop_reason') or 'cycle_blocked'
    if session.get('active_run_id') and session.get('status') in {'running', 'day_closed'}:
        run = repository.get_auto_trigger_rule_run(int(session['active_run_id'])) or {}
        if run.get('status') == 'active':
            return 'cycle_running'
    if session.get('status') == 'day_closed' and session.get('start_stat_date') == day:
        return session.get('stop_reason') or 'cycle_day_closed'
    return ''


def evaluate_roll_rule(repository: Any, rule: dict, subscriptions: list[dict], *, fetcher=None,
                       now: datetime | None = None, draw_clock: dict | None = None,
                       draw_fetcher=None, draw_cache: dict | None = None) -> dict:
    from pc28touzhu.services import auto_trigger_service as ats
    from pc28touzhu.services.dispatch_service import dispatch_signal, _route_effective_strategy
    reference = now or datetime.now(timezone.utc)
    day = ats._today_stat_date(now=reference)
    summary = {'checked_count': 0, 'triggered_count': 0, 'skipped_count': 0, 'failed_count': 0}
    events = []
    draw_cache = draw_cache if draw_cache is not None else {}
    performance_cache: dict = {}
    scheduled = rule.get('trigger_mode') == 'schedule'
    timed = rule.get('trigger_mode') in {'schedule', 'condition_schedule'}
    for sub in subscriptions:
        summary['checked_count'] += 1
        owner = _owner(rule, sub)
        performance = None
        matched = []
        trigger_match = None
        try:
            session = repository.get_auto_trigger_cycle_session(**owner) or {}
            # 停用/人工停轮优先于自动续开，不能用新的cycle状态绕过已有run停止。
            if session.get('active_run_id'):
                active_run = repository.get_auto_trigger_rule_run(int(session['active_run_id'])) or {}
                if active_run.get('status') == 'stopped':
                    session = repository.upsert_auto_trigger_cycle_session(**owner, values={'status': 'day_closed' if str(active_run.get('stop_reason') or '').startswith('cycle_') else 'blocked', 'stop_reason': active_run.get('stop_reason') or 'rule_run_stopped'})
            reason = _session_gate(repository, rule, sub, session, day)
            if reason:
                raise StartSkipped(str(reason))
            time_gate = ats._rule_time_gate(rule, now=now)
            if not time_gate['allowed']:
                raise StartSkipped(time_gate['reason'])
            continuation = session.get('status') == 'continuation'
            clock = draw_clock
            if scheduled or continuation:
                if not clock:
                    from pc28touzhu.services.pc28_draw_service import get_pc28_draw_clock
                    clock = get_pc28_draw_clock(fetcher=draw_fetcher)
                latest = str((clock or {}).get('latest_issue_no') or '')
                if not latest or (clock or {}).get('stale'):
                    raise StartSkipped('draw_clock_unavailable')
                performance = {'latest_settled_issue': latest, 'metrics': {}}
            else:
                performance, clock = _performance(rule, sub, fetcher=fetcher, draw_fetcher=draw_fetcher,
                    draw_clock=clock, draw_cache=draw_cache, cache=performance_cache)
                latest = str(performance.get('latest_settled_issue') or '')
                if not latest:
                    raise StartSkipped('performance_issue_empty')
                trigger_match = ats._resolve_trigger_match(rule, performance)
                if not trigger_match:
                    continue
                matched = trigger_match.get('matched_conditions') or []
                if rule.get('action', {}).get('skip_multiple_metrics_matched') and ats._has_multiple_trigger_metrics(trigger_match):
                    raise StartSkipped('multiple_metrics_matched')
            if not continuation:
                if session.get('last_completed_issue') and not _newer(latest, session['last_completed_issue']):
                    raise StartSkipped('cycle_waiting_new_issue')
                if ats._is_in_cooldown(repository, rule, int(sub['id']), latest):
                    raise StartSkipped('cooldown')
            if session.get('last_start_issue') and not _newer(latest, session['last_start_issue']):
                raise StartSkipped('draw_issue_already_triggered')

            with repository.transaction() as unit:
                current_rule = unit.get_auto_trigger_rule(int(rule['id'])) or {}
                current_sub = unit.get_subscription(int(sub['id'])) or {}
                current_sub['source'] = sub.get('source') or {}
                evaluation_fields = ('conditions', 'guard_groups', 'trigger_mode', 'schedule', 'cooldown_issues',
                                     'action', 'scope_mode', 'subscription_ids', 'cycle_policy', 'routes')
                if any(current_rule.get(key) != rule.get(key) for key in evaluation_fields):
                    raise StartSkipped('rule_configuration_changed')
                if subscription_strategy(current_sub) != subscription_strategy(sub):
                    raise StartSkipped('subscription_configuration_changed')
                session = unit.get_auto_trigger_cycle_session(**owner) or {}
                reason = _session_gate(unit, current_rule, current_sub, session, day)
                if reason:
                    raise StartSkipped(reason)
                time_gate = ats._rule_time_gate(current_rule, now=now)
                if not time_gate['allowed']:
                    raise StartSkipped(time_gate['reason'])
                window_id = str(time_gate.get('window_id') or '')
                if continuation != (session.get('status') == 'continuation'):
                    raise StartSkipped('cycle_session_changed')
                if not continuation:
                    if session.get('last_completed_issue') and not _newer(latest, session['last_completed_issue']):
                        raise StartSkipped('cycle_waiting_new_issue')
                    if ats._is_in_cooldown(unit, current_rule, int(sub['id']), latest):
                        raise StartSkipped('cooldown')
                # 并发等待者不得覆盖先完成的新会话；配置保存后使用当前配置启动。
                if session.get('last_start_issue') and not _newer(latest, session['last_start_issue']):
                    raise StartSkipped('draw_issue_already_triggered')
                routes = _routes(unit, current_rule, current_sub)
                effective_rule = {**current_rule, 'routes': routes}
                active_routes, skipped_routes = ats._active_routes_for_subscription(unit, rule=effective_rule, subscription=current_sub, stat_date=day)
                if not active_routes:
                    raise StartSkipped(ats._route_skip_reason(skipped_routes))
                action = current_rule.get('action') or {}
                signal_type = ats._signal_bet_type_for_auto_trigger_context({'rule_action': action, 'matched_conditions': matched})
                signal = ats._pick_signal_for_subscription_on_rule(unit, subscription=current_sub, rule=effective_rule,
                    bet_types=[signal_type] if signal_type else ats._signal_bet_types_for_subscription_on_rule(unit, current_sub, effective_rule))
                dispatch = bool(action.get('dispatch_latest_signal', True))
                if dispatch:
                    if not signal:
                        raise StartSkipped('signal_not_found')
                    if not _newer(str(signal.get('issue_no') or ''), latest):
                        raise StartSkipped('issue_already_drawn')
                    verdict = evaluate_pc28_issue_dispatch_window(issue_no=signal['issue_no'], draw_clock=clock, now=reference)
                    if not verdict.get('allowed'):
                        raise StartSkipped(str(verdict.get('reason') or 'signal_not_dispatchable'))
                    if timed:
                        signal_gate = ats._scheduled_signal_gate(current_rule, signal, now=now)
                        if not signal_gate['allowed']:
                            raise StartSkipped(signal_gate['reason'])
                cycle_policy = current_rule.get('cycle_policy') or {'mode': 'legacy'}
                if scheduled and cycle_policy.get('mode') == 'legacy' and not continuation:
                    for previous_run in unit.list_auto_trigger_rule_runs_for_subscription_date(
                        rule_id=int(rule['id']), subscription_id=int(sub['id']), stat_date=day):
                        old_window = str(previous_run.get('window_id') or '')
                        if old_window == window_id or old_window.endswith(':' + window_id):
                            raise StartSkipped('schedule_day_already_started')
                strategy = subscription_strategy(current_sub)
                legacy_policy = session.get('legacy_policy') or normalize_stop_policy(strategy.get('staking_policy', {}).get('stop_policy'))
                if cycle_policy.get('mode') == 'legacy' and not session.get('legacy_policy'):
                    unit.upsert_auto_trigger_cycle_session(**owner, values={'legacy_policy': legacy_policy})
                limit = int(cycle_policy.get('max_starts_per_day') or 1000)
                claim = unit.claim_auto_trigger_cycle_start(**owner, stat_date=day, started_issue_no=latest,
                    max_starts_per_day=limit, policy=cycle_policy, route_ids=[int(r['id']) for r in active_routes],
                    window_id=window_id)
                if not claim['claimed']:
                    raise StartSkipped(claim['reason'])
                run = claim['run']
                context = {'rule_id': int(rule['id']), 'rule_run_id': int(run['id']), 'stat_date': day,
                           'rule_action': action, 'matched_conditions': matched, 'routes': active_routes}
                prepared = []
                for route in active_routes:
                    route_strategy = _route_effective_strategy(strategy, route, context)
                    policy = auto_roll_policy(route_strategy, cycle_policy, legacy_policy)
                    previous = unit.get_auto_trigger_cycle_state(**owner, route_id=int(route['id'])) or {}
                    account = previous.get('balance') if policy.get('roll_mode') == 'chain' else None
                    if account is not None:
                        account = max(float(account), float(policy['base_amount']))
                    state = create_cycle_state(policy, stat_date=day, account_balance=account)
                    if previous.get('stat_date') == day:
                        state['day_pnl'] = previous.get('day_pnl')
                    unit.upsert_auto_trigger_cycle_state(**owner, route_id=int(route['id']), rule_run_id=int(run['id']), state=state, policy=policy)
                    unit.reset_auto_trigger_route_subscription_runtime(route_id=int(route['id']), **owner, note='自动过关新轮')
                    unit.ensure_auto_trigger_cycle_route_runtime(**owner, route_id=int(route['id']), rule_run_id=int(run['id']),
                        strategy=route_strategy, started_issue_no=latest, signal_id=int(signal['id']) if dispatch and signal else None)
                    prepared.append({**route, '_auto_trigger_rule_run_id': int(run['id']), '_auto_trigger_stat_date': day})
                context['routes'] = prepared
                dispatch_result = dispatch_signal(unit, int(signal['id']), subscription_id=int(sub['id']), auto_trigger_context=context, draw_clock=clock) if dispatch else None
                if dispatch and not int((dispatch_result or {}).get('created_count') or 0):
                    raise StartSkipped('cycle_no_jobs_created')
                if dispatch:
                    started_routes = sorted({int(job['auto_trigger_route_id']) for job in dispatch_result.get('jobs') or [] if job.get('auto_trigger_route_id')})
                    unit.discard_unstarted_auto_trigger_cycle_routes(**owner, rule_run_id=int(run['id']),
                        route_ids=[int(route['id']) for route in prepared if int(route['id']) not in started_routes])
                    unit.upsert_auto_trigger_cycle_session(**owner, values={'participating_route_ids': started_routes})
                event = ats._record_event(unit, rule=current_rule, subscription=sub, performance=performance, status='triggered',
                    reason='cycle_continued' if continuation else ('schedule_started' if scheduled else 'conditions_matched'),
                    matched_conditions=matched, trigger_match=trigger_match, dispatch_result=dispatch_result,
                    stat_date=day, schedule_snapshot=time_gate if timed else None)
                unit.mark_auto_trigger_rule_triggered(rule_id=int(rule['id']), user_id=int(rule['user_id']), issue_no=latest)
                events.append(event)
                summary['triggered_count'] += 1
        except StartSkipped as exc:
            events.append(ats._record_event(repository, rule=rule, subscription=sub, performance=performance, status='skipped',
                                           reason=str(exc), matched_conditions=matched, stat_date=day))
            summary['skipped_count'] += 1
        except Exception as exc:
            events.append(ats._record_event(repository, rule=rule, subscription=sub, performance=performance, status='failed', reason=str(exc), stat_date=day))
            summary['failed_count'] += 1
    return {'rule_id': int(rule['id']), 'summary': summary, 'events': events}


def finish_roll_session(repository: Any, row: dict, *, completed_issue_no: str) -> dict:
    """所有参与投递均结束后决定再入场；本轮与其他方案/手动状态无关。"""
    owner = {key: int(row[key]) for key in ('rule_id', 'subscription_id', 'user_id')}
    session = repository.get_auto_trigger_cycle_session(**owner) or {}
    run_id = int(row.get('rule_run_id') or 0)
    if not run_id or int(session.get('active_run_id') or 0) != run_id:
        return {'completed': False, 'reason': 'cycle_run_stale'}
    if _newer(completed_issue_no, str(session.get('last_completed_issue') or '')):
        session = repository.upsert_auto_trigger_cycle_session(**owner, values={'last_completed_issue': str(completed_issue_no)})
    states = [repository.get_auto_trigger_cycle_state(**owner, route_id=int(route_id)) for route_id in session.get('participating_route_ids') or []]
    if not states or any(not state for state in states):
        return {'completed': False, 'reason': 'cycle_routes_unfinished'}
    policy = session.get('policy') or {'mode': 'legacy'}
    statuses = []
    for state in states:
        if state.get('status') == 'stopped':
            field = 'on_target' if state.get('stop_reason') == 'win_streak_target' else 'on_bust'
            statuses.append('close_day' if policy.get('mode') == 'legacy' else policy.get(field) or 'wait_condition')
    close_requested = 'close_day' in statuses or session.get('status') in {'day_closed', 'blocked'}
    if close_requested:
        if session.get('status') != 'blocked':
            repository.upsert_auto_trigger_cycle_session(**owner, values={'status': 'day_closed', 'stop_reason': str(row.get('last_cycle_stop_reason') or 'cycle_day_closed')})
        for state in states:
            if state.get('pending_plan'):
                continue  # 已发送腿仍按原轮结算，不再投下一期。
            if state.get('status') == 'running':
                state.update(status='stopped', day_closed=True, stop_reason='session_closed')
                repository.upsert_auto_trigger_cycle_state(**owner, route_id=int(state['route_id']), rule_run_id=run_id,
                    state=state, policy=state.get('policy') or {}, pending_plan=None)
    if any(state.get('pending_plan') or state.get('status') == 'running' for state in states):
        return {'completed': False, 'reason': 'cycle_pending_settlement'}
    status = 'day_closed' if close_requested else ('continuation' if statuses and all(s == 'new_cycle' for s in statuses) else 'waiting')
    reason = str(row.get('last_cycle_stop_reason') or row.get('stop_reason') or '')
    daily = repository.get_auto_trigger_cycle_daily_starts(**owner, stat_date=str(session.get('start_stat_date') or row['stat_date']))
    if policy.get('mode') == 'rule' and int(daily['starts_count']) >= int(policy.get('max_starts_per_day') or 1):
        status, reason = 'day_closed', 'cycle_daily_limit'
    run = repository.get_auto_trigger_rule_run(run_id) or {}
    if session.get('status') == 'blocked' or run.get('status') == 'stopped':
        reason = str(session.get('stop_reason') or run.get('stop_reason') or 'manual_stop')
        status = 'day_closed' if reason in CYCLE_DAY_STOP_REASONS else 'blocked'
    last_issue = max([str(completed_issue_no), str(session.get('last_completed_issue') or '')], key=lambda value: int(value or 0))
    return repository.complete_auto_trigger_cycle_session(**owner, rule_run_id=run_id, completed_issue_no=last_issue, status=status, stop_reason=reason)


def resume_rule_cycle(repository: Any, *, rule_id: int, user_id: int, subscription_id: int, now: datetime | None = None) -> dict:
    from pc28touzhu.services import auto_trigger_service as ats
    with repository.transaction() as unit:
        rule = unit.get_auto_trigger_rule(rule_id)
        sub = unit.get_subscription(subscription_id)
        if not rule or int(rule['user_id']) != user_id or not sub or int(sub['user_id']) != user_id:
            raise ValueError('规则或跟单方案不存在')
        if rule.get('scope_mode') == 'selected_subscriptions' and subscription_id not in (rule.get('subscription_ids') or []):
            raise ValueError('该方案不在本规则作用范围内')
        if not is_roll_subscription(sub):
            raise ValueError('该方案不是过关玩法')
        owner = _owner(rule, sub)
        day = ats._today_stat_date(now=now)
        if rule.get('status') != 'active' or sub.get('status') not in {'active', 'standby'}:
            raise ValueError('请先启用规则和跟单方案')
        source = unit.get_source(int(sub['source_id'])) or {}
        if source.get('status') != 'active':
            raise ValueError('请先启用信号来源')
        day_state = ats._is_rule_day_stopped(unit, rule, stat_date=day)
        day_stat = day_state.get('stat') or {}
        needs_scoped_unseal = day_state.get('stopped') and day_stat.get('stopped_reason') in CYCLE_DAY_STOP_REASONS
        if day_state.get('stopped') and not needs_scoped_unseal:
            raise ValueError('规则当日风控尚未解除')
        if needs_scoped_unseal:
            risk_stat = {**day_stat, 'stopped_reason': ''}
            if (rule.get('cycle_policy') or {}).get('mode') != 'rule':
                raise ValueError('请先将过关轮次设置改为由本规则控制')
            if ats._rule_day_stop_reason(rule.get('daily_risk_control') or {}, stat=risk_stat):
                raise ValueError('规则当日风控条件仍触发')
        session = unit.get_auto_trigger_cycle_session(**owner) or {}
        if session.get('status') == 'running':
            raise ValueError('本轮仍在运行，不能恢复待命')
        if str(session.get('stop_reason') or '').startswith('migration_'):
            raise ValueError('迁移归属需要核查，不能自动恢复')
        for row in unit.list_pending_win_streak_cycles(user_id=user_id):
            if row.get('scope') == 'auto' and int(row.get('rule_id') or 0) == rule_id and int(row['subscription_id']) == subscription_id:
                raise ValueError('本规则方案仍有待结算任务')
        cycle_policy = rule.get('cycle_policy') or {'mode': 'legacy'}
        limit = int(cycle_policy.get('max_starts_per_day') or (session.get('legacy_policy') or {}).get('max_cycles_per_day') or 1)
        daily = unit.get_auto_trigger_cycle_daily_starts(**owner, stat_date=day)
        if int(daily['starts_count']) >= limit:
            raise ValueError('今日启动次数已用完，请先明确提高每日上限')
        for route_id in session.get('participating_route_ids') or []:
            route_day = unit.get_auto_trigger_route_daily_stat(route_id=int(route_id), user_id=user_id, stat_date=day)
            if route_day.get('status') == 'stopped':
                if route_day.get('stopped_reason') not in CYCLE_DAY_STOP_REASONS:
                    raise ValueError('投递路由当日风控尚未解除')
                route = unit.get_auto_trigger_rule_route(int(route_id)) or {}
                risk = route.get('route_risk_control') or rule.get('daily_risk_control') or {}
                if ats._rule_day_stop_reason(risk, stat={**route_day, 'stopped_reason': ''}):
                    raise ValueError('投递路由当日风控条件仍触发')
                needs_scoped_unseal = True
        if needs_scoped_unseal:
            unit.resume_auto_trigger_cycle_day_scope(**owner, stat_date=day)
        else:
            unit.upsert_auto_trigger_cycle_session(**owner, values={'status': 'waiting', 'active_run_id': None, 'stop_reason': ''})
        runtime = unit.list_auto_trigger_cycle_runtime(rule_id=rule_id, user_id=user_id, stat_date=day)
        return {'resumed': session.get('status') != 'waiting', 'item': rule,
                'cycle_runtime': next((item for item in runtime if item['subscription_id'] == subscription_id), {})}
