"""Independent research workspace checks against temporary real WSGI/SQLite."""
from __future__ import annotations

import json
import sqlite3
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, empty_stats, normalize_batch_payload
from tests import test_rule_lab_api as base
from tests.test_rule_lab_risk import policy

NOW = '2026-11-28T04:00:00Z'


class ResearchIndependentQA(unittest.TestCase):
    setUp = base.RuleLabApiTests.setUp
    request = base.RuleLabApiTests.request

    def call(self, path, payload=None, *, code=200, **kwargs):
        response = self.request(base.PREFIX + path, payload, **kwargs)
        self.assertEqual(response['status_code'], code, response.get('json', response))
        return response['json']

    def create_fixture(self, predictor=7, *, end='2026-11-27', october=100, november=10000):
        config = normalize_batch_payload(dict(name='独立核对来源 '+str(predictor), mode='backtest',
            predictor_id=predictor, metric='big_small', start_date='2026-10-01', end_date=end,
            risk_policy=policy()), now=NOW)
        batch = self.store.create_batch(1, config, '来源 '+str(predictor), build_experiments('big_small')[:1], now=NOW)
        experiment = self.store.list_experiments(1, batch['id'])[0]
        cutoff = NOW if end.startswith('2026-11') else '2026-11-01T00:00:00Z'
        observed = end + 'T15:59:00Z'
        cursor = dict(replay_scheduler_version=2, simulation_time=observed,
                      retention_v1=dict(first_open_at='2026-10-01T00:01:00+08:00',
                                        processed_open_at=observed, replay_cutoff_at=cutoff))
        days = [('2026-10-15', october)]
        if end.startswith('2026-11'):
            days.append(('2026-11-15', november))
        stats = empty_stats()
        stats.update(settled_count=100*len(days), hit_count=60*len(days), miss_count=40*len(days),
                     stake_cents=10000*len(days), net_cents=sum(net for _, net in days))
        with sqlite3.connect(self.store.db_path) as connection:
            connection.execute("UPDATE lab_batches SET status='completed',started_at=?,processed_issues=22000,cursor_json=? WHERE id=?",
                               (NOW, json.dumps(cursor), batch['id']))
            connection.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?', (json.dumps(stats), experiment['id']))
            connection.executemany('''INSERT INTO lab_daily(experiment_id,stat_date,settled_count,
                stake_cents,net_cents,hit_count,miss_count) VALUES(?,?,100,10000,?,60,40)''',
                [(experiment['id'], stamp, net) for stamp, net in days])
        return batch, experiment

    def ranking(self, experiment, **changes):
        payload = dict(predictor_ids=[7, 8], metric='big_small', condition_key=experiment['condition_key'],
            settlement_rule_id=experiment['settlement_rule_id'], months=1, include_partial=False,
            end_date='2026-11-27', sort='roi', limit=50, cursor=None)
        payload.update(changes)
        return payload

    def favorite(self, batch, experiment, token='qa-favorite-create'):
        payload = dict(request_id=token, batch_id=batch['id'], experiment_id=experiment['id'],
                       name='冻结候选', notes='保存当时配置')
        return self.call('/favorites', payload, code=201), payload

    def rows(self, *tables):
        with sqlite3.connect(self.store.db_path) as connection:
            return {table: connection.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall() for table in tables}

    def delete_source(self, batch):
        path = '/batches/'+str(batch['id'])
        preview = self.call(path+'/delete-preview')
        self.call(path+'/delete', dict(request_id='qa-delete-'+str(batch['id']),
            expected_revision=preview['expected_revision'], confirm_name=preview['name']), code=202)
        for _ in range(10):
            self.store.management_maintenance(now=NOW, limit=50, max_pages=50)
            if self.request(base.PREFIX+path)['status_code'] == 404:
                return
        self.fail('Bounded deletion did not finish in the small fixture')

    def test_ranking_uses_one_october_window_not_lifetime_or_each_batch_end(self):
        first, e1 = self.create_fixture(october=100, november=10000)
        second, e2 = self.create_fixture(8, end='2026-10-31', october=200)
        before = self.rows('lab_batches', 'lab_experiments', 'lab_daily', 'lab_series')
        self.source.available = False
        result = self.call('/research/rankings', self.ranking(e1))
        self.assertEqual((result['window']['start_date'], result['window']['end_date']), ('2026-10-01', '2026-10-31'))
        self.assertEqual([item['experiment_id'] for item in result['items']], [e2['id'], e1['id']])
        self.assertEqual([item['rank'] for item in result['items']], [1, 2])
        self.assertEqual([item['metrics']['net_cents'] for item in result['items']], [200, 100])
        self.assertTrue(all(item['eligible'] for item in result['items']))
        self.assertEqual(self.rows(*before), before)

    def test_same_odds_id_with_different_frozen_values_never_gets_shared_rank(self):
        _, first = self.create_fixture()
        _, second = self.create_fixture(8)
        with sqlite3.connect(self.store.db_path) as connection:
            raw = connection.execute('SELECT settlement_snapshot_json FROM lab_experiments WHERE id=?', (second['id'],)).fetchone()[0]
            value = json.loads(raw)
            value['resolved_odds']['big_small'] = 1.98
            connection.execute('UPDATE lab_experiments SET settlement_snapshot_json=? WHERE id=?', (json.dumps(value), second['id']))
        result = self.call('/research/rankings', self.ranking(first))
        self.assertEqual(len(result['groups']), 2)
        self.assertTrue(all(item['rank'] is None for item in result['items']))

    def test_display_metadata_does_not_split_identical_settlement_behavior(self):
        _, first = self.create_fixture()
        second_batch, second = self.create_fixture(8)
        with sqlite3.connect(self.store.db_path) as connection:
            raw = connection.execute('SELECT settlement_snapshot_json FROM lab_experiments WHERE id=?', (second['id'],)).fetchone()[0]
            value = json.loads(raw)
            value['rule']['name'] = '赔率显示名已改'
            value['catalog_hash'] = 'catalog-label-change-with-identical-economics'
            value['resolved_from'] = 'favorite_snapshot'
            connection.execute('UPDATE lab_experiments SET settlement_snapshot_json=? WHERE id=?', (json.dumps(value), second['id']))
            connection.execute('UPDATE lab_batches SET name=?,predictor_name=? WHERE id=?',
                               ('显示名称更改', '来源八的新名称', second_batch['id']))
        result = self.call('/research/rankings', self.ranking(first))
        self.assertEqual(len(result['groups']), 1, 'Pure display/provenance metadata must not define a different economic cohort')
        self.assertEqual([item['rank'] for item in result['items']], [1, 1])

    def test_fully_observed_month_without_bets_is_not_a_missing_month(self):
        first_batch, first = self.create_fixture(end='2026-10-31')
        second_batch, _ = self.create_fixture(8, end='2026-10-31')
        with sqlite3.connect(self.store.db_path) as connection:
            for batch in (first_batch, second_batch):
                cursor = json.loads(connection.execute('SELECT cursor_json FROM lab_batches WHERE id=?', (batch['id'],)).fetchone()[0])
                cursor['retention_v1']['first_open_at'] = '2026-09-01T00:01:00+08:00'
                connection.execute("UPDATE lab_batches SET start_date='2026-09-01',cursor_json=? WHERE id=?",
                                   (json.dumps(cursor), batch['id']))
        result = self.call('/research/rankings', self.ranking(first, months=2))
        self.assertEqual(result['window']['start_date'], '2026-09-01')
        for row in result['items']:
            september = row['observation']['months'][0]
            self.assertTrue(september['complete_month'])
            self.assertEqual(september['settled_count'], 0)
            self.assertTrue(row['eligible'], row['reasons'])
            self.assertEqual(row['rank'], 1)

    def test_ranking_page_cursor_rejects_changed_results(self):
        _, first = self.create_fixture()
        _, second = self.create_fixture(8)
        payload = self.ranking(first, limit=1)
        page = self.call('/research/rankings', payload)
        self.assertIsNotNone(page['next_cursor'])
        with sqlite3.connect(self.store.db_path) as connection:
            connection.execute("UPDATE lab_daily SET net_cents=net_cents+1 WHERE experiment_id=? AND stat_date='2026-10-15'", (second['id'],))
            raw = connection.execute('SELECT stats_json FROM lab_experiments WHERE id=?', (second['id'],)).fetchone()[0]
            stats = json.loads(raw)
            stats['net_cents'] += 1
            connection.execute('UPDATE lab_experiments SET stats_json=? WHERE id=?', (json.dumps(stats), second['id']))
        self.call('/research/rankings', dict(payload, cursor=page['next_cursor']), code=409)

    def test_favorite_retry_survives_source_deletion_and_frozen_draft_keeps_old_odds(self):
        batch, experiment = self.create_fixture()
        with sqlite3.connect(self.store.db_path) as connection:
            raw = connection.execute('SELECT settlement_snapshot_json FROM lab_experiments WHERE id=?', (experiment['id'],)).fetchone()[0]
            odds = json.loads(raw)
            odds['resolved_odds']['big_small'] = 1.98
            connection.execute('UPDATE lab_experiments SET settlement_snapshot_json=? WHERE id=?', (json.dumps(odds), experiment['id']))
        saved, payload = self.favorite(batch, experiment)
        favorite = saved['favorite']
        self.delete_source(batch)
        before = self.rows('lab_favorites', 'lab_batches', 'lab_experiments')
        retry = self.call('/favorites', payload)
        self.assertFalse(retry['created'])
        self.assertEqual(retry['favorite']['id'], favorite['id'])
        self.assertEqual(self.rows(*before), before)
        row = self.call('/favorites')['items'][0]
        self.assertFalse(row['source']['available'])
        self.assertIsNone(row['current_result'])
        self.assertEqual(row['saved_result'], favorite['saved_result'])
        created = self.call('/favorites/'+str(favorite['id'])+'/create', dict(request_id='qa-frozen-new-draft',
            expected_revision=favorite['revision'], name='旧赔率单项草稿', start_date='2026-11-01', end_date='2026-11-26'), code=201)
        self.assertEqual(created['generated_count'], 1)
        detail = self.call('/batches/'+str(created['batch']['id']))
        self.assertEqual(detail['batch']['status'], 'paused')
        self.assertEqual(len(detail['experiments']), 1)
        with sqlite3.connect(self.store.db_path) as connection:
            actual = connection.execute('SELECT settlement_snapshot_json,stats_json FROM lab_experiments WHERE batch_id=?', (created['batch']['id'],)).fetchone()
        self.assertEqual(json.loads(actual[0]), odds)
        self.assertEqual(json.loads(actual[1])['settled_count'], 0)
        self.assertEqual(len(self.rows('lab_series')['lab_series']), 0)

    def test_parallel_favorite_same_receipt_writes_once_and_other_user_cannot_retry(self):
        batch, experiment = self.create_fixture()
        payload = dict(request_id='qa-favorite-race', batch_id=batch['id'], experiment_id=experiment['id'], name='one', notes='original')
        barrier = threading.Barrier(2)
        def submit(_):
            barrier.wait(timeout=5)
            return self.request(base.PREFIX+'/favorites', payload)
        with ThreadPoolExecutor(max_workers=2) as executor:
            responses = list(executor.map(submit, range(2)))
        self.assertEqual(sorted(item['status_code'] for item in responses), [200, 201], responses)
        self.assertEqual(len(self.rows('lab_favorites')['lab_favorites']), 1)
        self.call('/favorites', payload, uid=self.other['id'], code=404)
        self.call('/favorites', dict(payload, notes='changed'), code=409)
        self.assertEqual(self.call('/favorites', uid=self.other['id'])['items'], [])

    def test_bulk_preview_stale_last_owner_does_not_rename_first(self):
        first, _ = self.create_fixture()
        second, _ = self.create_fixture(8)
        owners = [dict(owner_kind='batch', owner_id=b['id']) for b in (first, second)]
        payload = dict(action='rename', owners=owners,
                       names=[dict(owner, name='rename '+str(owner['owner_id'])) for owner in owners], end_date=None)
        preview = self.call('/management/bulk-preview', payload)
        single = self.call('/batches/'+str(second['id'])+'/management')
        self.call('/batches/'+str(second['id'])+'/rename', dict(request_id='qa-change-last-owner',
            expected_revision=single['expected_revision'], name='changed meanwhile'))
        before = self.rows('lab_batches', 'lab_queue_items', 'lab_series', 'lab_command_receipts')
        self.call('/management/bulk', dict(payload, request_id='qa-stale-rename',
            preview_revision=preview['preview_revision'], confirm_text=None), code=409)
        self.assertEqual(self.rows(*before), before)

    def test_second_database_write_failure_rolls_back_first_owner_and_receipt(self):
        first, _ = self.create_fixture()
        second, _ = self.create_fixture(8)
        owners = [dict(owner_kind='batch', owner_id=b['id']) for b in (first, second)]
        payload = dict(action='rename', owners=owners,
                       names=[dict(owner, name='atomic '+str(owner['owner_id'])) for owner in owners], end_date=None)
        preview = self.call('/management/bulk-preview', payload)
        with sqlite3.connect(self.store.db_path) as connection:
            connection.execute('''CREATE TRIGGER qa_fail_second_name BEFORE UPDATE OF name ON lab_batches
                WHEN OLD.id=%d BEGIN SELECT RAISE(ABORT,'injected second write failure'); END''' % second['id'])
        before = self.rows('lab_batches', 'lab_object_controls', 'lab_command_receipts', 'lab_management_receipts')
        body = dict(payload, request_id='qa-write-failure', preview_revision=preview['preview_revision'], confirm_text=None)
        self.call('/management/bulk', body, code=503)
        self.assertEqual(self.rows(*before), before)
        with sqlite3.connect(self.store.db_path) as connection:
            connection.execute('DROP TRIGGER qa_fail_second_name')
        self.call('/management/bulk', body)
        for owner, name in zip(owners, payload['names']):
            self.assertEqual(self.call('/batches/'+str(owner['owner_id']))['batch']['name'], name['name'])

    def test_known_missing_inputs_disqualify_source_instead_of_becoming_low_profit(self):
        _, first = self.create_fixture()
        second, _ = self.create_fixture(8)
        with sqlite3.connect(self.store.db_path) as connection:
            connection.execute('UPDATE lab_batches SET skip_counts_json=? WHERE id=?',
                               (json.dumps({'no_prediction': 1}), second['id']))
        result = self.call('/research/rankings', self.ranking(first))
        affected = next(row for row in result['items'] if row['batch_id'] == second['id'])
        self.assertFalse(affected['eligible'])
        self.assertIn('known_skips', [reason['code'] for reason in affected['reasons']])
        self.assertTrue(all(row['rank'] is None for row in result['items']))

    def test_second_delete_failure_restores_first_status_queue_and_jobs(self):
        first, _ = self.create_fixture()
        second, _ = self.create_fixture(8)
        owners = [dict(owner_kind='batch', owner_id=batch['id']) for batch in (first, second)]
        payload = dict(action='delete', owners=owners, names=None, end_date=None)
        preview = self.call('/management/bulk-preview', payload)
        with sqlite3.connect(self.store.db_path) as connection:
            connection.execute('''CREATE TRIGGER qa_fail_second_delete BEFORE UPDATE OF status ON lab_batches
                WHEN OLD.id=%d AND NEW.status='deleting' BEGIN SELECT RAISE(ABORT,'injected second delete failure'); END''' % second['id'])
        before = self.rows('lab_batches', 'lab_object_controls', 'lab_queue_items', 'lab_series',
                           'lab_management_jobs', 'lab_management_receipts', 'lab_command_receipts')
        self.call('/management/bulk', dict(payload, request_id='qa-delete-write-failure',
            preview_revision=preview['preview_revision'], confirm_text=preview['confirmation_text']), code=503)
        self.assertEqual(self.rows(*before), before)

    def test_second_append_failure_rolls_back_new_baseline_capture_and_receipts(self):
        from tests.test_rule_lab_series import completed_baseline
        self.source.list_predictors = lambda: [dict(id=1, name='临时来源')]
        first, second = completed_baseline(self.store), completed_baseline(self.store)
        payload = dict(action='append', owners=[dict(owner_kind='batch', owner_id=batch['id'])
            for batch in (first, second)], names=None, end_date='2026-11-27')
        preview = self.call('/management/bulk-preview', payload)
        self.assertTrue(preview['available'], preview['blockers'])
        with sqlite3.connect(self.store.db_path) as connection:
            connection.execute('''CREATE TRIGGER qa_fail_second_append BEFORE INSERT ON lab_series
                WHEN NEW.owner_id=%d BEGIN SELECT RAISE(ABORT,'injected second append failure'); END''' % second['id'])
        before = self.rows('lab_batches', 'lab_experiments', 'lab_series', 'lab_segments', 'lab_checkpoints',
            'lab_capture_streams', 'lab_continuation_requests', 'lab_command_receipts')
        self.call('/management/bulk', dict(payload, request_id='qa-append-write-failure',
            preview_revision=preview['preview_revision'], confirm_text=None), code=503)
        self.assertEqual(self.rows(*before), before)

    def test_renaming_dependent_study_before_its_source_does_not_invalidate_same_command(self):
        batch, _ = self.create_fixture()
        study = self.call('/studies', dict(source_batch_id=batch['id'], name='原比较组',
            request_id='qa-rename-dependent-study'), code=201)['study']
        owners = [dict(owner_kind='study', owner_id=study['id']), dict(owner_kind='batch', owner_id=batch['id'])]
        names = [dict(owner, name=name) for owner, name in zip(owners, ('新的比较组名', '新的原实验名'))]
        payload = dict(action='rename', owners=owners, names=names, end_date=None)
        preview = self.call('/management/bulk-preview', payload)
        self.assertTrue(preview['available'], preview['blockers'])
        result = self.call('/management/bulk', dict(payload, request_id='qa-rename-dependent-pair',
            preview_revision=preview['preview_revision'], confirm_text=None))
        self.assertEqual(result['affected_owners'], 2)
        self.assertEqual(self.call('/batches/'+str(batch['id']))['batch']['name'], names[1]['name'])
        self.assertEqual(self.call('/studies/'+str(study['id']))['study']['name'], names[0]['name'])

    def test_new_endpoints_enforce_auth_and_exact_query_fields(self):
        batch, experiment = self.create_fixture()
        pairs = [('/research/options', None), ('/research/rankings', self.ranking(experiment)),
                 ('/favorites', None), ('/management/bulk-preview', dict(action='delete',
                    owners=[dict(owner_kind='batch', owner_id=batch['id'])], names=None, end_date=None))]
        for path, payload in pairs:
            for uid, code in ((None, 401), (self.regular['id'], 403)):
                with self.subTest(path=path, uid=uid):
                    self.call(path, payload, uid=uid, code=code)
            self.call(path, payload, query='unexpected=1', code=400)
        for query in ('limit=1&limit=2', 'limit=01', 'before_id=true', 'limit=0'):
            self.call('/favorites', query=query, code=400)


class ResearchQualityIndependentQA(unittest.TestCase):
    request = base.RuleLabApiTests.request
    call = ResearchIndependentQA.call

    def setUp(self):
        base.RuleLabApiTests.setUp(self)
        from pc28touzhu.services.rule_lab_source import RuleLabSource
        from tests.test_rule_lab_source import SourceFixture
        self.fixture = SourceFixture(Path(self.tmp.name)/'source.db')
        self.source = RuleLabSource(self.fixture.path)
        self.service.source = self.source
        self.boundary = datetime(2026, 11, 25, 16, tzinfo=timezone.utc)

    def source_dump(self):
        with sqlite3.connect(self.fixture.path) as connection:
            return '\n'.join(connection.iterdump())

    def quality_payload(self, **changes):
        payload = dict(predictor_id=1, metric='big_small', start_date='2026-11-26',
                       end_date='2026-11-26', limit=2, cursor=None)
        payload.update(changes)
        return payload

    def mixed_rows(self):
        for issue, minutes in ((1000, -1), (1001, 2), (1002, 5), (1003, 8), (1004, 11), (1005, 14), (1007, 20), (1008, 1441)):
            opened = self.boundary + timedelta(minutes=minutes)
            self.fixture.draw(issue, opened)
            if issue == 1004:
                continue
            self.fixture.prediction(issue, opened, big_small=None if issue==1003 else '大',
                created=opened-timedelta(seconds=39 if issue==1002 else 40))
        self.fixture.execute("UPDATE lottery_draws SET result_number=1 WHERE issue_no='1005'")

    def scan(self, payload=None):
        payload = payload or self.quality_payload()
        pages = []
        for _ in range(20):
            page = self.call('/research/data-quality', payload)
            pages.append(page)
            if page['scan']['complete']:
                return pages
            self.assertTrue(page['scan']['next_cursor'])
            payload = dict(payload, cursor=page['scan']['next_cursor'])
        self.fail('Small fixture scan did not finish')

    def test_quality_pages_classify_decision_cutoff_unknown_and_interior_gap_read_only(self):
        self.mixed_rows()
        before = self.source_dump()
        pages = self.scan()
        self.assertGreater(len(pages), 1)
        self.assertTrue(all(page['scan']['scanned_issues']<=2 for page in pages))
        totals = {key: sum(page['counts'][key] for page in pages) for key in pages[0]['counts']}
        self.assertEqual(totals['draw_rows'], 6)
        self.assertEqual(totals['valid_draws'], 5)
        self.assertEqual(totals['invalid_draws'], 1)
        self.assertEqual(totals['on_time'], 2)
        self.assertEqual(totals['late'], 1)
        self.assertEqual(totals['invalid_prediction'], 1)
        self.assertEqual(totals['confirmed_no_prediction'], 0)
        self.assertEqual(totals['unknown_or_archived'], 1)
        self.assertEqual(totals['missing_draw_issues'], 1)
        self.assertEqual(self.source_dump(), before)
        self.assertNotIn('never-read-this', json.dumps(pages))

    def test_quality_archive_evidence_separates_retained_absence_and_unknown(self):
        self.mixed_rows()
        self.fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        self.fixture.execute("INSERT INTO pc28_prediction_daily_summary VALUES(1,'2026-11-24')")
        pages = self.scan()
        self.assertEqual(sum(page['counts']['confirmed_no_prediction'] for page in pages), 1)
        self.assertEqual(sum(page['counts']['unknown_or_archived'] for page in pages), 0)
        self.fixture.execute("UPDATE pc28_prediction_daily_summary SET summary_date='2026-11-25'")
        pages = self.scan()
        self.assertEqual(sum(page['counts']['confirmed_no_prediction'] for page in pages), 0)
        self.assertEqual(sum(page['counts']['unknown_or_archived'] for page in pages), 1)

    def test_quality_revoked_source_cannot_continue_cached_cursor(self):
        self.mixed_rows()
        payload = self.quality_payload()
        first = self.call('/research/data-quality', payload)
        self.assertTrue(first['scan']['next_cursor'])
        self.fixture.execute('UPDATE predictors SET share_predictions=0 WHERE id=1')
        self.call('/research/data-quality', dict(payload, cursor=first['scan']['next_cursor']), code=403)

    def test_gap_between_boundary_and_first_draw_does_not_claim_verified_start(self):
        self.mixed_rows()
        self.fixture.execute("DELETE FROM lottery_draws WHERE issue_no='1001'")
        first = self.call('/research/data-quality', self.quality_payload(limit=200))
        self.assertFalse(first['boundary']['start_verified'], 'Retained predecessor is separated from first selected draw by a missing issue')
        self.assertIsNone(first['rates']['confirmed_missing_percent'])

    def test_invalid_draw_with_known_date_is_present_in_daily_quality_totals(self):
        self.mixed_rows()
        pages = self.scan()
        days = [day for page in pages for day in page['days']]
        self.assertEqual(sum(day['draw_rows'] for day in days), sum(page['counts']['draw_rows'] for page in pages))
        self.assertEqual(sum(day['invalid_draws'] for day in days), 1)


if __name__ == '__main__':
    unittest.main()
