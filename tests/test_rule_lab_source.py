"""Only temporary source databases are opened by these tests."""
import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabError

UTC = timezone.utc
SHANGHAI = timezone(timedelta(hours=8))


def stamp(value):
    return value.astimezone(UTC).isoformat().replace('+00:00', 'Z')


class SourceFixture:
    def __init__(self, path):
        self.path = Path(path)
        with sqlite3.connect(str(path)) as c:
            c.executescript('''
                CREATE TABLE predictors(id INTEGER PRIMARY KEY,name TEXT,lottery_type TEXT,
                    enabled INTEGER,share_predictions INTEGER,share_level TEXT,api_key TEXT);
                CREATE TABLE lottery_draws(id INTEGER PRIMARY KEY,lottery_type TEXT,issue_no TEXT,
                    open_time TEXT,result_number INTEGER,source_payload TEXT,created_at TEXT,
                    UNIQUE(lottery_type,issue_no));
                CREATE TABLE predictions(id INTEGER PRIMARY KEY,predictor_id INTEGER,lottery_type TEXT,
                    issue_no TEXT,prediction_big_small TEXT,prediction_odd_even TEXT,prediction_combo TEXT,
                    status TEXT,created_at TEXT,settled_at TEXT,UNIQUE(predictor_id,issue_no));
                INSERT INTO predictors VALUES(1,'公开来源','pc28',1,1,'records','never-read-this');
            ''')

    def execute(self, query, params=()):
        with sqlite3.connect(str(self.path)) as c:
            c.execute(query, params)

    def draw(self, issue, opened, balls=(3, 6, 8), *, available=None):
        available = available or opened + timedelta(seconds=10)
        self.execute('''INSERT INTO lottery_draws(lottery_type,issue_no,open_time,result_number,source_payload,created_at)
            VALUES('pc28',?,?,?,?,?)''', (str(issue), opened.astimezone(SHANGHAI).strftime('%Y-%m-%d %H:%M:%S'),
            sum(balls), json.dumps({'number': '+'.join(map(str, balls))}), available.strftime('%Y-%m-%d %H:%M:%S')))

    def prediction(self, issue, opened, *, status='settled', created=None, settled=None,
                   big_small='大', odd_even='单', combo='大单', predictor_id=1):
        created = created or opened - timedelta(seconds=120)
        settled = (settled or opened + timedelta(seconds=15)) if status == 'settled' else None
        self.execute('''INSERT INTO predictions(predictor_id,lottery_type,issue_no,prediction_big_small,
            prediction_odd_even,prediction_combo,status,created_at,settled_at) VALUES(?,'pc28',?,?,?,?,?,?,?)''',
            (predictor_id, str(issue), big_small, odd_even, combo, status,
             created.strftime('%Y-%m-%d %H:%M:%S'), settled.strftime('%Y-%m-%d %H:%M:%S') if settled else None))


class RuleLabSourceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fixture = SourceFixture(Path(self.tmp.name) / 'source.db')
        self.source = RuleLabSource(self.fixture.path)
        self.opened = datetime(2026, 11, 26, 4, tzinfo=UTC)
        self.now = stamp(self.opened + timedelta(hours=1))

    def tearDown(self):
        self.tmp.cleanup()

    def test_shared_predictors_are_whitelisted_without_secret_fields(self):
        for i, share, enabled, lottery in [(2, 'analysis', 1, 'pc28'), (3, 'stats_only', 1, 'pc28'),
                                            (4, 'unknown', 1, 'pc28'), (5, 'records', 0, 'pc28'),
                                            (6, 'records', 1, 'other')]:
            self.fixture.execute('INSERT INTO predictors VALUES(?,?,?,?,1,?,?)', (i, 'x', lottery, enabled, share, 'secret'))
        self.assertEqual([p['id'] for p in self.source.list_predictors()], [1, 2])
        self.assertEqual(set(self.source.get_predictor(1)), {'id', 'name'})
        self.assertIsNone(self.source.get_predictor(4))

    def test_parses_three_balls_and_distinct_source_timezones(self):
        self.fixture.draw(1000, self.opened)
        self.fixture.prediction(1000, self.opened)
        row = self.source.latest_draw(observed_at=self.now)
        self.assertEqual(row['open_at'], '2026-11-26T04:00:00Z')
        self.assertEqual(row['source_created_at'], '2026-11-26T04:00:10Z')
        self.assertEqual(row['triplet'], [3, 6, 8])
        self.assertNotIn('source_payload', row)
        pred = self.source.get_predictions(1, ['1000'], observed_at=self.now)['items'][0]
        self.assertEqual(pred['created_at'], '2026-11-26T03:58:00Z')
        self.assertEqual(pred['settled_at'], '2026-11-26T04:00:15Z')

    def test_invalid_draw_advances_page_without_becoming_latest_fallback(self):
        self.fixture.draw(1000, self.opened)
        self.fixture.draw(1001, self.opened + timedelta(seconds=210))
        self.fixture.execute("UPDATE lottery_draws SET result_number=1 WHERE issue_no='1001'")
        page = self.source.draws_page(limit=2, observed_at=self.now)
        self.assertEqual([r['issue_no'] for r in page['items']], ['1000'])
        self.assertEqual(page['next_issue'], '1001')
        self.assertEqual(page['scanned_count'], 2)
        self.assertEqual(sum(page['skip_counts'].values()), 1)
        with self.assertRaises(RuleLabError):
            self.source.latest_draw(observed_at=self.now)

    def test_history_is_bounded_ascending_and_excludes_target(self):
        for i in range(8):
            self.fixture.draw(1000+i, self.opened + timedelta(seconds=210*i))
        page = self.source.history_draws(before_issue='1007', limit=3, observed_at=self.now)
        self.assertEqual([r['issue_no'] for r in page['items']], ['1004', '1005', '1006'])
        page = self.source.draws_page(after_issue='1004', before_issue='1007', limit=2, observed_at=self.now)
        self.assertEqual([r['issue_no'] for r in page['items']], ['1005', '1006'])
        with self.assertRaises(ValueError):
            self.source.draws_page(limit=201, observed_at=self.now)

    def test_prediction_hash_survives_status_update_but_detects_direction_change(self):
        self.fixture.prediction(1000, self.opened, status='pending')
        before = self.source.get_predictions(1, ['1000'], observed_at=self.now)['items'][0]
        self.fixture.execute("UPDATE predictions SET status='settled',settled_at='2026-11-26 04:00:15'")
        after = self.source.get_predictions(1, ['1000'], observed_at=self.now)['items'][0]
        self.assertEqual(before['input_hash'], after['input_hash'])
        self.assertEqual(after['status'], 'settled')
        self.fixture.execute("UPDATE predictions SET prediction_big_small='小'")
        changed = self.source.get_predictions(1, ['1000'], observed_at=self.now)['items'][0]
        self.assertNotEqual(before['input_hash'], changed['input_hash'])

    def test_reading_does_not_write_source_or_create_missing_database(self):
        self.fixture.draw(1000, self.opened)
        before = hashlib.sha256(self.fixture.path.read_bytes()).hexdigest()
        self.source.list_predictors()
        self.source.latest_draw(observed_at=self.now)
        self.assertEqual(before, hashlib.sha256(self.fixture.path.read_bytes()).hexdigest())
        missing = Path(self.tmp.name) / 'missing.db'
        with self.assertRaises(RuleLabError) as cm:
            RuleLabSource(missing).latest_draw(observed_at=self.now)
        self.assertFalse(missing.exists())
        self.assertNotIn(str(missing), cm.exception.message)

    def test_locked_source_fails_safely_instead_of_hanging(self):
        c = sqlite3.connect(str(self.fixture.path))
        try:
            c.execute('BEGIN EXCLUSIVE')
            with self.assertRaises(RuleLabError) as cm:
                RuleLabSource(self.fixture.path, timeout_seconds=.01).list_predictors()
            self.assertEqual(cm.exception.code, 'source_unavailable')
        finally:
            c.rollback()
            c.close()

    def test_malformed_times_and_directions_are_counted_not_guessed(self):
        self.fixture.prediction(1000, self.opened)
        self.fixture.execute("UPDATE predictions SET created_at='bad',prediction_big_small='unknown'")
        page = self.source.get_predictions(1, ['1000'], observed_at=self.now)
        self.assertEqual(page['items'], [])
        self.assertEqual(sum(page['skip_counts'].values()), 1)

    def test_date_seek_handles_missing_issues_and_out_of_range_dates(self):
        previous = datetime(2026, 11, 25, 15, 55, tzinfo=UTC)
        self.fixture.draw(1000, previous)
        self.fixture.draw(1050, previous + timedelta(minutes=10))
        self.fixture.draw(1099, previous + timedelta(minutes=15))
        self.assertEqual(self.source.find_start_issue('2026-11-26', observed_at=self.now), '1050')
        self.assertEqual(self.source.find_start_issue('2026-11-20', observed_at=self.now), '1000')
        self.assertIsNone(self.source.find_start_issue('2026-11-27', observed_at=self.now))

    def test_date_seek_rejects_observed_nonmonotonic_times(self):
        self.fixture.draw(1000, self.opened)
        self.fixture.draw(1001, self.opened - timedelta(days=1))
        with self.assertRaises(RuleLabError):
            self.source.find_start_issue('2026-11-26', observed_at=self.now)

    def test_seek_and_pagination_use_issue_index_without_date_filter_scan(self):
        self.fixture.draw(1000, self.opened)
        with sqlite3.connect(str(self.fixture.path)) as c:
            plans = c.execute('''EXPLAIN QUERY PLAN SELECT issue_no,open_time FROM lottery_draws
                WHERE lottery_type='pc28' AND issue_no>=? ORDER BY issue_no LIMIT 1''', ('1000',)).fetchall()
        detail = ' '.join(row[3] for row in plans)
        self.assertIn('SEARCH', detail)
        self.assertIn('INDEX', detail)
        self.assertNotIn('TEMP B-TREE', detail)

    def test_malformed_issue_cursor_does_not_stall_pagination(self):
        self.fixture.draw(1000, self.opened)
        self.fixture.execute('''INSERT INTO lottery_draws(lottery_type,issue_no,open_time,result_number,source_payload,created_at)
            VALUES('pc28','1000a','2026-11-26 12:03:30',17,'{"number":"3+6+8"}','2026-11-26 04:03:40')''')
        self.fixture.draw(1001, self.opened + timedelta(seconds=210))
        first = self.source.draws_page(after_issue='1000', limit=1, observed_at=self.now)
        self.assertEqual(first['next_issue'], '1000a')
        self.assertFalse(first['items'])
        second = self.source.draws_page(after_issue=first['next_issue'], limit=1, observed_at=self.now)
        self.assertEqual(second['items'][0]['issue_no'], '1001')


if __name__ == '__main__':
    unittest.main()
