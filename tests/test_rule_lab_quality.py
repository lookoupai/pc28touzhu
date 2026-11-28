"""Quality checks use the actual source schema and bounded API pages."""
import hashlib,json,sqlite3,unittest
from datetime import datetime,timedelta,timezone
from pathlib import Path
from tests import test_rule_lab_api as base
from tests.test_rule_lab_source import SourceFixture,stamp
from pc28touzhu.services.rule_lab_source import RuleLabSource
PREFIX=base.PREFIX;UTC=timezone.utc
class QualityTests(unittest.TestCase):
    request=base.RuleLabApiTests.request
    def setUp(self):
        base.RuleLabApiTests.setUp(self)
        self.fixture=SourceFixture(Path(self.tmp.name)/'quality-source.db');self.service.source=RuleLabSource(self.fixture.path)
        self.opened=datetime(2026,11,26,4,tzinfo=UTC)
        self.fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT,total_predictions INTEGER)')
        self.fixture.draw(1999,self.opened-timedelta(days=1));self.fixture.draw(2005,self.opened+timedelta(days=1))
        for i in range(5):self.fixture.draw(2000+i,self.opened+timedelta(seconds=210*i))
        self.fixture.prediction(2000,self.opened)
        self.fixture.prediction(2001,self.opened+timedelta(seconds=210),created=self.opened+timedelta(seconds=180))
        self.fixture.prediction(2002,self.opened+timedelta(seconds=420),odd_even=None)
        self.fixture.prediction(2004,self.opened+timedelta(seconds=840))
    def body(self,**changes):
        value=dict(predictor_id=1,metric='odd_even',start_date='2026-11-26',end_date='2026-11-26',cursor=None,limit=2);value.update(changes);return value
    def call(self,payload,code=200):
        result=self.request(PREFIX+'/research/data-quality',payload);self.assertEqual(result['status_code'],code,result);return result['json']
    def test_pages_are_readonly_and_classify_late_invalid_and_confirmed_absence(self):
        before=hashlib.sha256(self.fixture.path.read_bytes()).hexdigest();totals={};cursor=None;count=0
        while True:
            result=self.call(self.body(cursor=cursor));count+=1
            for key,value in result['counts'].items():totals[key]=totals.get(key,0)+value
            self.assertLessEqual(result['scan']['scanned_issues'],2);cursor=result['scan']['next_cursor']
            if result['scan']['complete']:break
            self.assertIsNotNone(cursor);self.assertLess(count,10)
        self.assertEqual(totals['valid_draws'],5);self.assertEqual(totals['on_time'],2);self.assertEqual(totals['late'],1)
        self.assertEqual(totals['invalid_prediction'],1);self.assertEqual(totals['confirmed_no_prediction'],1)
        self.assertTrue(result['boundary']['start_verified']);self.assertTrue(result['boundary']['end_verified'])
        self.assertEqual(hashlib.sha256(self.fixture.path.read_bytes()).hexdigest(),before)
    def test_archived_absence_is_unknown_and_never_zero_missing(self):
        self.fixture.execute("INSERT INTO pc28_prediction_daily_summary VALUES(1,'2026-11-26',300)")
        result=self.call(self.body(limit=10));self.assertEqual(result['counts']['unknown_or_archived'],1)
        self.assertIsNone(result['rates']['confirmed_missing_percent'])
        self.assertTrue(any(w['code']=='unverified_absence' for w in result['warnings']))
    def test_cursor_scope_tampering_and_source_permissions_are_checked(self):
        first=self.call(self.body());cursor=first['scan']['next_cursor'];self.assertTrue(cursor)
        self.call(self.body(cursor=cursor,metric='combo'),409)
        self.call(self.body(cursor=cursor[:-1]+'!'),400)
        self.call(self.body(limit=201),400);self.call(self.body(predictor_id=True),400)
        self.fixture.execute('UPDATE predictors SET share_predictions=0 WHERE id=1')
        self.call(self.body(cursor=cursor),403)
    def test_missing_draw_and_unverifiable_edges_do_not_claim_complete_history(self):
        self.fixture.execute("DELETE FROM lottery_draws WHERE issue_no IN ('1999','2002','2005')")
        result=self.call(self.body(limit=10));self.assertEqual(result['counts']['missing_draw_issues'],1)
        self.assertFalse(result['boundary']['start_verified']);self.assertFalse(result['boundary']['end_verified'])
        self.assertIsNone(result['rates']['confirmed_missing_percent'])
if __name__=='__main__':unittest.main()
