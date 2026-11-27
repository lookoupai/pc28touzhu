"""Expired delivery execution ends independently from its historical ledger."""
from __future__ import annotations

from datetime import datetime
from typing import Any


def detach_expired_delivery_cycles(repository: Any, *, now: datetime | None = None,
                                   user_id: int | None = None, rule_id: int | None = None) -> dict:
    if not hasattr(repository, 'detach_expired_auto_trigger_cycles'):
        return {'detached_count': 0, 'items': [], 'errors': []}
    return repository.detach_expired_auto_trigger_cycles(now=now, user_id=user_id, rule_id=rule_id)


def resolve_archived_cycles(repository: Any, *, now: datetime | None = None,
                            user_id: int | None = None) -> dict:
    if not hasattr(repository, 'resolve_auto_trigger_cycle_archives'):
        return {'resolved_count': 0, 'items': []}
    return repository.resolve_auto_trigger_cycle_archives(now=now, user_id=user_id)


def list_cycle_recoveries(repository: Any, *, user_id: int, rule_id: int,
                          subscription_id: int | None = None, before_id: int | None = None,
                          limit: int = 20) -> dict:
    limit=max(1,min(int(limit),100))
    rows=repository.list_auto_trigger_cycle_archives(user_id=user_id,rule_id=rule_id,
        subscription_id=subscription_id,before_id=before_id,limit=limit)
    fields=('id','rule_id','subscription_id','route_id','rule_run_id','stat_date','issue_no','status','reason',
            'detached_at','resolved_at','initial_reserved_stake','reserved_stake','capital_credit',
            'credit_applied_run_id','credit_applied_at','recovery_error')
    items=[{**{key:row.get(key) for key in fields},'capital_disposition':
        'independent_cycle_return' if row['policy'].get('roll_mode')=='all_in' else 'deferred_chain_credit'} for row in rows]
    return {'items':items,'next_cursor':items[-1]['id'] if len(items)==limit else None}
