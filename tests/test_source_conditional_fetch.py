from __future__ import annotations

from contextlib import contextmanager
from email.message import Message
from io import BytesIO
import json
import os
import tempfile
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.normalize_service import normalize_raw_item
from pc28touzhu.services.source_fetch_service import fetch_source_to_raw_item
from pc28touzhu.services.source_sync_service import run_source_sync_cycle


class ConditionalSourceFetchTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmpdir.cleanup)
        self.repo = DatabaseRepository(os.path.join(self.tmpdir.name, 'test.db'))
        self.repo.initialize_database()
        self.user_id = self.repo.create_user('conditional-user')
        self.source = self.make_source('https://example.com/signals')
        self.payload = {'items': [{
            'signal_id': 'pc28-test-120', 'issue_no': '120',
            'published_at': '2026-11-23T10:00:00Z',
            'signals': [{'bet_type': 'big_small', 'bet_value': '大'}],
        }]}

    def make_source(self, url):
        return self.repo.create_source_record(
            owner_user_id=self.user_id, source_type='ai_trading_simulator_export',
            name='conditional-source', status='active', config={'fetch': {'url': url}},
        )

    @contextmanager
    def response(self, payload, etag='W/"v1"'):
        stream = BytesIO(json.dumps(payload).encode())
        stream.headers = Message()
        stream.headers['Content-Type'] = 'application/json; charset=utf-8'
        if etag:
            stream.headers['ETag'] = etag
        yield stream

    def unchanged(self):
        return HTTPError('https://example.com/signals', 304, 'Not Modified', Message(), BytesIO())

    def fetch(self, source=None):
        return fetch_source_to_raw_item(self.repo, source_id=(source or self.source)['id'])

    def test_304_reuses_parsed_signal_without_creating_records(self):
        with patch('urllib.request.urlopen', side_effect=[self.response(self.payload), self.unchanged(), self.unchanged()]) as network:
            first = self.fetch()
            normalize_raw_item(self.repo, raw_item_id=first['raw_item']['id'])
            second = self.fetch()
            self.assertEqual(second['skipped_reason'], 'upstream_not_modified')
            with patch.object(self.repo, 'get_raw_item', side_effect=AssertionError('parsed records need no reread')):
                self.fetch()
            self.assertEqual(network.call_args_list[1].args[0].get_header('If-none-match'), 'W/"v1"')
        self.assertEqual(len(self.repo.list_raw_items(source_id=self.source['id'])), 1)

    def test_empty_signal_is_revalidated_and_late_signal_is_received(self):
        with patch('urllib.request.urlopen', side_effect=[self.response({'items': []}), self.unchanged(), self.response(self.payload, 'W/"v2"')]):
            self.assertEqual(self.fetch()['skipped_reason'], 'upstream_no_signal')
            self.assertEqual(self.fetch()['skipped_reason'], 'upstream_not_modified')
            self.assertTrue(self.fetch()['created'])

    def test_304_preserves_unparsed_record_for_normalization_retry(self):
        with patch('urllib.request.urlopen', side_effect=[self.response(self.payload), self.unchanged()]):
            first = self.fetch()
            second = self.fetch()
            self.assertEqual(second['raw_item']['id'], first['raw_item']['id'])
            self.assertEqual(second['raw_item']['parse_status'], 'pending')
            self.assertFalse(second.get('skipped'))

    def test_validators_are_isolated_by_source_and_config(self):
        another = self.make_source('https://example.com/signals?field=combo')
        with patch('urllib.request.urlopen', side_effect=[self.response(self.payload), self.response(self.payload)]) as network:
            self.fetch()
            self.fetch(another)
            self.assertIsNone(network.call_args.args[0].get_header('If-none-match'))
        changed = dict(self.source)
        changed['config'] = {'fetch': {'url': 'https://example.com/changed', 'headers': {'Authorization': 'Bearer test-only'}}}
        with patch.object(self.repo, 'get_source', return_value=changed), patch('urllib.request.urlopen', return_value=self.response(self.payload)) as network:
            self.fetch()
            self.assertIsNone(network.call_args.args[0].get_header('If-none-match'))

    def test_server_without_etag_remains_supported(self):
        with patch('urllib.request.urlopen', side_effect=[self.response(self.payload, None), self.response(self.payload, None)]) as network:
            self.fetch()
            second = self.fetch()
            self.assertFalse(second['created'])
            self.assertIsNone(network.call_args.args[0].get_header('If-none-match'))

    def test_access_revocation_is_not_hidden_by_cached_validator(self):
        denied = HTTPError('https://example.com/signals', 403, 'Forbidden', Message(), BytesIO())
        with patch('urllib.request.urlopen', side_effect=[self.response(self.payload), denied]):
            self.fetch()
            with self.assertRaisesRegex(ValueError, '403'):
                self.fetch()

    def test_sync_still_retries_blocked_dispatch_when_source_is_unchanged(self):
        self.repo.create_subscription_record(user_id=self.user_id, source_id=self.source['id'], status='active', strategy={'stake_amount': 10})
        with patch('urllib.request.urlopen', side_effect=[self.response({'items': []}), self.unchanged()]):
            run_source_sync_cycle(self.repo, draw_clock={})
            with patch('pc28touzhu.services.source_sync_service.redispatch_gate_blocked_signals', return_value={'checked_count': 1, 'created_job_count': 1}) as retry:
                result = run_source_sync_cycle(self.repo, draw_clock={})
            retry.assert_called_once()
            self.assertEqual(result['summary']['not_modified_count'], 1)
            self.assertEqual(result['summary']['failed_count'], 0)
            self.assertEqual(result['summary']['blocked_retry_created_job_count'], 1)


if __name__ == '__main__':
    unittest.main()
