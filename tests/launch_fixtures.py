"""Pre-existing active-rule fixtures for legacy execution regression tests.

Production creation now saves drafts. These tests model rules already enabled
before that change; explicit-start behavior uses the real service separately.
"""
from pc28touzhu.services.auto_trigger_service import create_auto_trigger_rule


def create_legacy_rule(repository, *, user_id, payload):
    result = create_auto_trigger_rule(repository, user_id=user_id, payload=payload)
    result['item'] = repository.update_auto_trigger_rule_status(
        rule_id=result['item']['id'], user_id=user_id,
        status=payload.get('status') or 'active',
    )
    return result
