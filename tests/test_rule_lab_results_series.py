"""Continuous result state remains bounded after consumed inputs are removed."""
import json
import sqlite3
import unittest

from pc28touzhu.services.rule_lab_series import _pack,_unpack
from tests import test_rule_lab_series as fixture
NOW=fixture.NOW


class ResultsSeriesTests(unittest.TestCase):
    setUp=fixture.SeriesTests.setUp
    tearDown=fixture.SeriesTests.tearDown
    enable=fixture.SeriesTests.enable
    preview=fixture.SeriesTests.preview
    capture_future=fixture.SeriesTests.capture_future
    # Keep the production-capable base fixture, without inheriting its unrelated
    # old assertions about archive retention policy.
    def test_completed_segment_stores_aggregate_delta_without_full_experiment_state(self):
        self.enable();self.capture_future()
        self.store.request_append(1,'batch',self.batch['id'],dict(request_id='result-segment',
            expected_revision=self.preview()['expected_revision'],end_date='2026-11-27'),now=NOW)
        self.store.prepare_due_appends(now=NOW)
        work=self.store.get_work_item(self.batch['id'])
        cursor=dict(work['batch']['cursor'],last_issue_no='1102',scan_after_issue='1102',simulation_time='2026-11-26T16:05:50Z')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor=cursor,now=NOW,progress={'processed_issues_delta':2})
        work=self.store.get_work_item(self.batch['id']);self.store.complete_segment(self.batch['id'],work['batch']['revision'],cursor=cursor,now=NOW)
        segment=self.store.list_segments(1,'batch',self.batch['id'])['segments'][0]
        summary=segment['summary']['batches'][0]
        self.assertNotIn('experiments',summary)
        self.assertIn('incremental',summary)
        with self.store._connect() as c:
            payload=c.execute('SELECT baseline_json FROM lab_segment_batches WHERE segment_id=?',(segment['id'],)).fetchone()[0]
        self.assertFalse(_unpack(payload)['checkpoint_retained'])

    def test_no_full_history_seed_when_adopting_completed_batch(self):
        with self.store._connect(write=True) as c:
            c.execute("INSERT INTO lab_draws(issue_no,issue_key,source_id,open_at,source_created_at,observed_at,triplet_json,input_hash) VALUES('1',1,1,?,?,?,'[1,3,5]','old')",(NOW,NOW,NOW))
        self.enable()
        with self.store._connect() as c:
            row=c.execute('SELECT seed_after_issue,seed_until_issue FROM lab_capture_streams').fetchone()
            self.assertEqual(row[0],row[1])
            self.assertIsNone(c.execute("SELECT 1 FROM lab_input_versions WHERE issue_key=1").fetchone())

    def test_local_replay_preview_rejects_present_coverage_with_missing_prediction_payload(self):
        self.enable()
        with self.store._connect(write=True) as c:
            from pc28touzhu.services.rule_lab_archive import _save_page,read_input
            stream=c.execute('SELECT * FROM lab_capture_streams').fetchone()
            draw=read_input(c,'pc28-upstream','draw',0,1001)
            prediction=read_input(c,'pc28-upstream','prediction',1,1001)
            _save_page(c,stream,[dict(draw,issue_no='1000')],[dict(prediction,issue_no='1000')],
                [dict(issue_no='1000',**{'class':'present'})],NOW)
            # Baseline fixture has only one hundred warm rows. Make its selected
            # replay interval the final row, with a complete preceding window.
            c.execute('UPDATE lab_batches SET cursor_json=? WHERE id=?',
                (json.dumps(dict(last_issue_no='1100',warm_before_issue='1100',replay_complete=True,
                    retention_v1=dict(first_open_at=NOW,processed_open_at=NOW,frontier_verified=True))),self.batch['id']))
        eid=self.store.list_experiments(1,self.batch['id'])[0]['id']
        self.assertTrue(self.store.replay_input_preview(1,self.batch['id'],eid,now=NOW)['available'])
        with self.store._connect(write=True) as c:c.execute("DELETE FROM lab_input_versions WHERE kind='prediction' AND issue_key=1099")
        value=self.store.replay_input_preview(1,self.batch['id'],eid,now=NOW)
        self.assertFalse(value['available'])

    def test_default_capture_recheck_does_not_restart_at_old_history(self):
        self.enable()
        task=self.store.capture_work_items(now=NOW)[0]
        self.assertGreaterEqual(task['capture_after_issue'],1080)

    def test_continuous_frontier_uses_frozen_input_instead_of_polluted_ordinary_cache(self):
        self.enable();self.capture_future()
        self.store.request_append(1,'batch',self.batch['id'],dict(request_id='frontier-frozen',
            expected_revision=self.preview()['expected_revision'],end_date='2026-11-27'),now=NOW)
        self.store.prepare_due_appends(now=NOW)
        with self.store._connect(write=True) as c:
            self.store._cache(c,[dict(issue_no='1102',open_at='2026-12-05T00:00:00Z',source_created_at='2026-12-05T00:00:00Z',
                observed_at=NOW,triplet=[1,3,5],input_hash='ordinary-conflict')],[])
        work=self.store.get_work_item(self.batch['id'])
        self.store.commit_step(self.batch['id'],work['batch']['revision'],
            cursor=dict(work['batch']['cursor'],last_issue_no='1102',scan_after_issue='1102'),now=NOW)
        with self.store._connect() as c:
            saved=json.loads(c.execute('SELECT cursor_json FROM lab_batches WHERE id=?',(self.batch['id'],)).fetchone()[0])
        self.assertEqual(saved['retention_v1']['processed_open_at'],'2026-11-26T16:06:30Z')
