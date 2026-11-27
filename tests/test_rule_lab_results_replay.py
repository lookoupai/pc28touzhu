"""Current-source replay availability is evidence-based and read only."""
import tempfile
import unittest
from datetime import datetime,timedelta
from pathlib import Path

from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabError
from tests.test_rule_lab_source import SourceFixture,UTC,stamp


class ResultsReplayAvailabilityTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.fixture=SourceFixture(Path(self.tmp.name)/'source.db')
        self.source=RuleLabSource(self.fixture.path)
        self.start=datetime(2026,11,25,16,tzinfo=UTC)
        for index in range(-100,5):
            opened=self.start+timedelta(seconds=210*index)
            self.fixture.draw(1100+index,opened);self.fixture.prediction(1100+index,opened)
        self.now=stamp(self.start+timedelta(days=1))

    def tearDown(self):self.tmp.cleanup()

    def preview(self):
        method=getattr(self.source,'replay_availability',None)
        self.assertTrue(callable(method),'source replay availability is not verified')
        return method(1,'2026-11-26','2026-11-26',observed_at=self.now)

    def test_complete_range_and_warm_inputs_are_available(self):
        result=self.preview()
        self.assertTrue(result['available']);self.assertEqual(result['input_basis'],'current_source')
        self.assertEqual(result['verified_range'],dict(start_date='2026-11-26',end_date='2026-11-26'))

    def test_interior_draw_gap_is_not_hidden_by_matching_date_bounds(self):
        self.fixture.execute("DELETE FROM lottery_draws WHERE issue_no='1102'")
        self.assertFalse(self.preview()['available'])

    def test_absent_prediction_without_retention_evidence_is_unavailable(self):
        self.fixture.execute("DELETE FROM predictions WHERE issue_no='1102'")
        self.assertFalse(self.preview()['available'])

    def test_known_no_prediction_is_accepted_but_archived_day_is_not(self):
        self.fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        self.fixture.execute("DELETE FROM predictions WHERE issue_no='1102'")
        self.assertTrue(self.preview()['available'])
        self.fixture.execute("INSERT INTO pc28_prediction_daily_summary VALUES(1,'2026-11-26')")
        self.assertFalse(self.preview()['available'])

    def test_runtime_missing_after_preview_is_rejected(self):
        self.assertTrue(self.preview()['available'])
        draw=self.source.get_draw_by_issue('1102',observed_at=self.now)
        self.fixture.execute("DELETE FROM predictions WHERE issue_no='1102'")
        method=getattr(self.source,'validate_replay_page',None)
        self.assertTrue(callable(method),'strict replay page must recheck source loss')
        with self.assertRaises(RuleLabError) as caught:method(1,[draw],[],observed_at=self.now)
        self.assertEqual(caught.exception.code,'replay_input_unavailable')

    def test_missing_warm_period_is_rejected(self):
        self.fixture.execute("DELETE FROM lottery_draws WHERE issue_no='1001'")
        self.assertFalse(self.preview()['available'])

    def test_source_draw_and_prediction_removed_after_prefetch_are_rejected(self):
        self.assertTrue(self.preview()['available'])
        draw=self.source.get_draw_by_issue('1102',observed_at=self.now)
        self.fixture.execute("DELETE FROM predictions WHERE issue_no='1102'")
        self.fixture.execute("DELETE FROM lottery_draws WHERE issue_no='1102'")
        with self.assertRaises(RuleLabError) as caught:
            self.source.validate_replay_page(1,[draw],[],observed_at=self.now)
        self.assertEqual(caught.exception.code,'replay_input_unavailable')
