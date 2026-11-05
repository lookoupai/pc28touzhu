"""Anonymous temporary SQLite migration evidence; run with PYTHONPATH=src:."""
import json
from pathlib import Path
from tests.test_cycle_repository import CycleRepositoryTests

cases = []
for name, options in [('auto_pending', {}), ('auto_closed', {'pending': False, 'closed': True}),
                      ('legacy_no_route', {'route': 0}), ('ambiguous', {'conflicting': True})]:
    test = CycleRepositoryTests()
    test.setUp()
    try:
        test.seed_old_cycle(**options)
        tables = ['subscription_progression_events', 'execution_jobs', 'subscription_financial_state',
                  'auto_trigger_rule_daily_stats', 'auto_trigger_route_daily_stats']
        before = {table: test.repo._fetch_all('SELECT * FROM ' + table + ' ORDER BY rowid', ()) for table in tables}
        test.repo.initialize_database()
        test.repo.initialize_database()
        after = {table: test.repo._fetch_all('SELECT * FROM ' + table + ' ORDER BY rowid', ()) for table in tables}
        assert before == after
        cases.append({'case': name, 'migration': test.repo._fetch_all('SELECT status,reason FROM cycle_state_migrations', ()),
                      'ledger_and_job_rows_unchanged': True,
                      'original_event_count': len(before['subscription_progression_events']),
                      'repeat_initialize': 'idempotent'})
    finally:
        test.doCleanups()
result = {'source': 'synthetic anonymous temporary SQLite only', 'passed': len(cases), 'failed': 0, 'skipped': 0, 'cases': cases}
Path(__file__).with_suffix('.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
print(json.dumps({'passed': len(cases), 'failed': 0, 'skipped': 0}))
