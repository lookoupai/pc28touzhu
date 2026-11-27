"""Immutable continuation inputs, source coverage and bounded cold storage."""
import importlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore, RuleLabError
from tests.test_rule_lab_source import SourceFixture, UTC, stamp


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.fixture = SourceFixture(Path(self.tmp.name) / 'source.db')
        self.source = RuleLabSource(self.fixture.path)
        self.store = RuleLabStore(Path(self.tmp.name) / 'lab' / 'lab.db')
        self.store.initialize()
        self.opened = datetime(2026, 11, 26, 4, tzinfo=UTC)
        self.now = stamp(self.opened + timedelta(days=2))

    def tearDown(self):
        self.tmp.cleanup()

    def archive(self):
        path = Path(__file__).resolve().parents[1] / 'src/pc28touzhu/services/rule_lab_archive.py'
        self.assertTrue(path.exists(), 'continuation archive is not implemented')
        module = importlib.import_module('pc28touzhu.services.rule_lab_archive')
        with self.store._connect(write=True) as c:
            module.init_schema(c)
        return module

    def stream(self, start=999):
        archive = self.archive()
        with self.store._connect(write=True) as c:
            return archive.ensure_stream(c, 1, start, self.now)

    def page(self, start=999):
        self.assertTrue(hasattr(self.source, 'capture_page'), 'source capture needs coverage proof')
        return self.source.capture_page(1, after_issue=str(start), observed_at=self.now)

    def capture(self, stream, page):
        return self.archive().commit_capture(self.store, stream['id'], stream['revision'], now=self.now,
            **{k:page[k] for k in ('draws','predictions','coverage','next_issue','highest_seen_issue','source_identity')})

    def test_source_distinguishes_archived_absence_and_real_missing_prediction(self):
        self.fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        self.fixture.execute("INSERT INTO pc28_prediction_daily_summary VALUES(1,'2026-11-26')")
        self.now=stamp(self.opened+timedelta(days=3))
        for i,offset in enumerate((0,2,3)):
            self.fixture.draw(1000+i, self.opened + timedelta(days=offset))
        self.fixture.prediction(1000, self.opened)
        page = self.page()
        self.assertEqual([x['class'] for x in page['coverage']], ['present','no_prediction','waiting'])
        self.fixture.execute("DELETE FROM predictions WHERE issue_no='1000'")
        self.assertEqual(self.page()['coverage'][0]['class'], 'archive_gap')

    def test_missing_prediction_at_day_after_archive_boundary_is_uncertain(self):
        self.fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        self.fixture.execute("INSERT INTO pc28_prediction_daily_summary VALUES(1,'2026-11-25')")
        boundary=datetime(2026,11,25,16,2,30,tzinfo=UTC)
        self.fixture.draw(1000,boundary)
        self.assertEqual(self.page()['coverage'][0]['class'],'archive_gap')

    def test_source_without_absence_evidence_does_not_invent_no_prediction(self):
        self.fixture.draw(1000, self.opened)
        self.assertEqual(self.page()['coverage'][0]['class'], 'archive_gap')

    def test_source_permissions_apply_before_capture(self):
        self.fixture.draw(1000, self.opened)
        self.fixture.prediction(1000, self.opened)
        self.fixture.execute('UPDATE predictors SET share_predictions=0')
        self.assertTrue(hasattr(self.source, 'capture_page'), 'source capture is not implemented')
        with self.assertRaises(RuleLabError) as cm:
            self.source.capture_page(1, after_issue='999', observed_at=self.now)
        self.assertEqual(cm.exception.code, 'source_unavailable')

    def test_pending_prediction_is_rechecked_after_leaving_recent_overlap(self):
        for index in range(31):
            opened=self.opened+timedelta(seconds=210*index)
            self.fixture.draw(1000+index,opened)
            self.fixture.prediction(1000+index,opened,status='pending' if index==0 else 'settled')
        stream=self.stream();self.capture(stream,self.page())
        from pc28touzhu.domain.rule_lab import normalize_batch_payload,build_experiments
        cfg=normalize_batch_payload(dict(name='real consumer',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-26',end_date='2026-11-26'),now=self.now)
        consumer=self.store.create_batch(1,cfg,'source',build_experiments('big_small')[:1],now=self.now)
        with self.store._connect(write=True) as c:
            c.execute("UPDATE lab_batches SET status='completed',started_at=?,cursor_json=? WHERE id=?",
                (self.now,json.dumps(dict(last_issue_no='1030',replay_complete=True)),consumer['id']))
            c.execute('''INSERT INTO lab_series(owner_kind,owner_id,user_id,predictor_id,stream_id,
                configuration_hash,capture_enabled,created_at,updated_at) VALUES('batch',?,1,1,?,'test',1,?,?)''',
                (consumer['id'],stream['id'],self.now,self.now))
        settled=stamp(self.opened+timedelta(hours=3))
        self.fixture.execute("UPDATE predictions SET status='settled',settled_at=? WHERE issue_no='1000'",(settled,))
        visited=[]
        for _ in range(6):
            task=self.archive().capture_work_items(self.store,now=self.now)[0]
            visited.append(task['capture_after_issue'])
            page=self.source.capture_page(1,after_issue=str(task['capture_after_issue']),observed_at=self.now)
            self.capture(task,page)
            with self.store._connect() as c:
                value=self.archive().read_input(c,'pc28-upstream','prediction',1,1000)
            if value['status']=='settled':break
        self.assertIn(999,visited)
        self.assertEqual(value['status'],'settled')
        self.assertEqual(value['settled_at'],settled)

    def test_frozen_version_survives_source_revision_and_deletion(self):
        self.fixture.draw(1000, self.opened)
        self.fixture.prediction(1000, self.opened, status='pending')
        stream = self.stream()
        first = self.capture(stream, self.page())
        self.fixture.execute("UPDATE predictions SET prediction_combo='小双',status='settled',settled_at='2026-11-26 04:00:15'")
        stream['revision'] = first['revision']
        second = self.capture(stream, self.page())
        self.fixture.path.unlink()
        archive = self.archive()
        old = archive.ArchiveSource(self.store,1,first['archive_revision'],'1000',self.now)
        new = archive.ArchiveSource(self.store,1,second['archive_revision'],'1000',self.now)
        self.assertEqual(old.get_predictions(1,['1000'],observed_at=self.now)['items'][0]['combo'], '大单')
        self.assertEqual(new.get_predictions(1,['1000'],observed_at=self.now)['items'][0]['combo'], '小双')
        self.assertEqual(old.get_draw_by_issue('1000',observed_at=self.now)['triplet'], [3,6,8])

    def test_pending_to_settled_observation_has_distinct_version(self):
        self.fixture.draw(1000, self.opened)
        self.fixture.prediction(1000, self.opened, status='pending')
        stream = self.stream()
        first = self.capture(stream,self.page())
        self.fixture.execute("UPDATE predictions SET status='settled',settled_at='2026-11-26 04:00:15'")
        stream['revision'] = first['revision']
        second = self.capture(stream,self.page())
        archive = self.archive()
        old = archive.ArchiveSource(self.store,1,first['archive_revision'],'1000',self.now)
        new = archive.ArchiveSource(self.store,1,second['archive_revision'],'1000',self.now)
        self.assertIsNone(old.get_predictions(1,['1000'],observed_at=self.now)['items'][0]['settled_at'])
        self.assertEqual(new.get_predictions(1,['1000'],observed_at=self.now)['items'][0]['settled_at'], '2026-11-26T04:00:15Z')

    def test_stale_capture_does_not_advance_or_duplicate_inputs(self):
        self.fixture.draw(1000,self.opened);self.fixture.prediction(1000,self.opened)
        stream=self.stream();page=self.page()
        first=self.capture(stream,page)
        second=self.capture(stream,page)
        self.assertTrue(first['committed']);self.assertFalse(second['committed'])
        with self.store._connect() as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_input_versions').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT covered_through_issue FROM lab_capture_streams').fetchone()[0],1000)

    def test_gap_stops_coverage_watermark_until_repaired(self):
        self.fixture.draw(1000,self.opened);self.fixture.prediction(1000,self.opened)
        self.fixture.draw(1002,self.opened+timedelta(seconds=420));self.fixture.prediction(1002,self.opened+timedelta(seconds=420))
        stream=self.stream();first=self.capture(stream,self.page())
        self.assertEqual(first['covered_through_issue'],1000)
        self.fixture.draw(1001,self.opened+timedelta(seconds=210));self.fixture.prediction(1001,self.opened+timedelta(seconds=210))
        stream['revision']=first['revision']
        second=self.capture(stream,self.page())
        self.assertEqual(second['covered_through_issue'],1002)

    def test_cache_bootstrap_preserves_only_warm_and_does_not_seed_old_inputs(self):
        archive=self.archive()
        draws=[];predictions=[]
        for i in range(230):
            opened=self.opened+timedelta(seconds=210*i)
            draws.append(dict(issue_no=str(1000+i),source_id=i,open_at=stamp(opened),source_created_at=stamp(opened),observed_at=self.now,triplet=[3,6,8],input_hash=str(i)))
            predictions.append(dict(predictor_id=1,issue_no=str(1000+i),source_id=i,created_at=stamp(opened-timedelta(seconds=120)),settled_at=stamp(opened),observed_at=self.now,status='settled',big_small='大',odd_even='单',combo='大单',input_hash=str(i)))
        with self.store._connect(write=True) as c:
            self.store._cache(c,draws,predictions)
            stream=archive.ensure_stream(c,1,1229,self.now,history_start_issue=1000)
            warm=archive.ArchiveSource(self.store,1,stream['archive_revision'],'1229',self.now)
        self.assertEqual(len(warm.get_history(1,before_issue='1230')),100)
        with self.store._connect(write=True) as c:
            result=archive.seed_cache_page(c,stream['id'],limit=200,now=self.now)
            self.assertLessEqual(result['seeded'],200)
        with self.store._connect(write=True) as c:
            result=archive.seed_cache_page(c,stream['id'],limit=200,now=self.now)
            self.assertTrue(result['done'])
            revision=archive.archive_revision(c)
        reader=archive.ArchiveSource(self.store,1,revision,'1229',self.now)
        self.assertEqual(reader.find_start_issue('2026-11-26',observed_at=self.now),'1130')
        self.assertEqual(result['seeded'],0)

    def test_runtime_dependencies_replace_two_year_archive_retention(self):
        from pc28touzhu.domain.rule_lab import normalize_batch_payload,build_experiments
        self.fixture.draw(1000,self.opened);self.fixture.prediction(1000,self.opened)
        stream=self.stream();result=self.capture(stream,self.page())
        cfg=normalize_batch_payload(dict(name='queued',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-26',end_date='2026-11-26'),now=self.now)
        batch=self.store.create_batch(1,cfg,'source',build_experiments('big_small')[:1],now=self.now)
        archive=self.archive();reader=archive.ArchiveSource(self.store,1,result['archive_revision'],'1000',self.now)
        before=reader.get_history(1,before_issue='1001')
        later=stamp(self.opened+timedelta(days=800))
        archive.maintenance(self.store,now=later,protected_floors={1:2000},limit=200)
        self.assertEqual(reader.get_history(1,before_issue='1001'),before)
        with self.store._connect(write=True) as c:
            c.execute("UPDATE lab_batches SET status='deleting' WHERE id=?",(batch['id'],))
        archive.maintenance(self.store,now=stamp(self.opened+timedelta(days=801)),limit=200)
        self.assertEqual(reader.get_history(1,before_issue='1001'),[])

    def test_source_identity_change_cannot_merge_into_same_stream(self):
        self.fixture.draw(1000,self.opened);self.fixture.prediction(1000,self.opened)
        stream=self.stream();page=self.page();first=self.capture(stream,page)
        stream['revision']=first['revision'];page['source_identity']='another-source'
        with self.assertRaises(RuleLabError) as cm:self.capture(stream,page)
        self.assertEqual(cm.exception.code,'source_changed')
