"""Continuation persistence uses real disposable SQLite ledgers."""
import hashlib
import json
import sqlite3
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabStore, RuleLabError

NOW = '2026-11-27T04:00:00Z'


def completed_baseline(store, user_id=1, *, first=1001, last=1100):
    config = normalize_batch_payload(dict(name='baseline', mode='backtest', predictor_id=1,
        metric='combo', template_pack='combo_long_miss_v1', start_date='2026-11-26', end_date='2026-11-26'), now=NOW)
    batch = store.create_batch(user_id, config, 'source', build_experiments('combo',template_pack='combo_long_miss_v1'), now=NOW)
    opened = datetime(2026, 11, 26, 15, 59, tzinfo=timezone.utc)
    draws, predictions = [], []
    for issue in range(first,last+1):
        at=opened-timedelta(seconds=210*(last-issue));stamp=at.isoformat().replace('+00:00','Z')
        created=(at-timedelta(seconds=60)).isoformat().replace('+00:00','Z')
        draws.append(dict(issue_no=str(issue),open_at=stamp,source_created_at=stamp,
                          observed_at=NOW,triplet=[1,3,5],input_hash='draw-'+str(issue)))
        predictions.append(dict(predictor_id=1,issue_no=str(issue),created_at=created,
            settled_at=stamp,observed_at=NOW,status='settled',big_small='小',odd_even='单',combo='小单',input_hash='prediction-'+str(issue)))
    work=store.get_work_item(batch['id'])
    store.commit_step(batch['id'],work['batch']['revision'],draws=draws,predictions=predictions,now=NOW,
        cursor=dict(last_issue_no=str(last),scan_after_issue=str(last),replay_max_issue=str(last+10),
            replay_complete=True,replay_scheduler_version=2,simulation_time=(opened-timedelta(seconds=40)).isoformat().replace('+00:00','Z')),
        progress=dict(status='completed',processed_issues_delta=100,total_issues=100))
    return store.get_batch(user_id,batch['id'])


class SeriesTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.path=Path(self.tmp.name)/'lab.db'
        self.store=RuleLabStore(self.path);self.store.initialize();self.batch=completed_baseline(self.store)
    def tearDown(self):self.tmp.cleanup()
    def snapshot(self):
        with sqlite3.connect(self.path) as c:
            result={}
            for table in ('lab_batches','lab_experiments','lab_draws','lab_predictions','lab_daily','lab_decisions','lab_risk_days','lab_rounds'):
                rows=c.execute('SELECT * FROM '+table+' ORDER BY rowid').fetchall()
                if table=='lab_batches':
                    columns=[r[1] for r in c.execute('PRAGMA table_info(lab_batches)')];index=columns.index('cursor_json')
                    values=[]
                    for row in rows:
                        row=list(row);cursor=json.loads(row[index]);cursor.pop('retention_v1',None);row[index]=json.dumps(cursor,sort_keys=True);values.append(tuple(row))
                    rows=values
                result[table]=rows
            return result
    def preview(self):return self.store.continuation_preview(1,'batch',self.batch['id'],now=NOW)
    def enable(self,**changes):
        payload=dict(request_id='configure-series-1',expected_revision=self.preview()['expected_revision'],
                     capture_enabled=True,compute_enabled=False,daily_at='00:10',timezone='Asia/Shanghai')
        payload.update(changes)
        return self.store.set_continuation(1,'batch',self.batch['id'],payload,now=NOW),payload

    def test_schema_migration_is_additive_idempotent_and_default_off(self):
        with sqlite3.connect(self.path) as c:self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],7)
        before=self.snapshot();self.store.initialize();self.assertEqual(self.snapshot(),before)
        preview=self.preview();self.assertTrue(preview['eligible']);self.assertFalse(preview['continuation']['enabled'])
        self.assertEqual(preview['next_issue'],'1101');self.assertEqual(preview['saved_experiments'],26)
        self.assertEqual(self.snapshot(),before)

    def test_adoption_preserves_ids_stats_and_frozen_legacy_dates(self):
        before=self.snapshot();result,payload=self.enable()
        self.assertTrue(result['created']);self.assertTrue(result['continuation']['capture_enabled'])
        self.assertFalse(result['continuation']['compute_enabled']);self.assertEqual(self.snapshot(),before)
        segments=self.store.list_segments(1,'batch',self.batch['id'])['segments']
        self.assertEqual(len(segments),1);self.assertEqual(segments[0]['kind'],'baseline')
        again=self.store.set_continuation(1,'batch',self.batch['id'],payload,now=NOW)
        self.assertFalse(again['created']);self.assertEqual(len(self.store.list_segments(1,'batch',self.batch['id'])['segments']),1)
        self.assertEqual(self.preview()['latest_request']['request_id'],payload['request_id'])

    def test_revision_request_collision_and_ownership_are_rejected(self):
        _,payload=self.enable()
        with self.assertRaises(RuleLabError) as error:self.store.set_continuation(2,'batch',self.batch['id'],payload,now=NOW)
        self.assertEqual(error.exception.status,404)
        changed=dict(payload,compute_enabled=True)
        with self.assertRaises(RuleLabError) as error:self.store.set_continuation(1,'batch',self.batch['id'],changed,now=NOW)
        self.assertEqual(error.exception.code,'request_conflict')
        changed['request_id']='configure-series-2'
        with self.assertRaises(RuleLabError) as error:self.store.set_continuation(1,'batch',self.batch['id'],changed,now=NOW)
        self.assertEqual(error.exception.code,'stale_revision')

    def test_append_receipt_is_persistent_while_waiting_for_inputs(self):
        self.enable()
        payload=dict(request_id='append-series-1',expected_revision=self.preview()['expected_revision'],end_date='2026-11-27')
        result=self.store.request_append(1,'batch',self.batch['id'],payload,now=NOW)
        self.assertTrue(result['queued']);self.assertIsNone(result['segment'])
        self.assertEqual(self.preview()['latest_request']['request_id'],'append-series-1')
        before=self.snapshot();again=self.store.request_append(1,'batch',self.batch['id'],payload,now=NOW)
        self.assertFalse(again['created']);self.assertEqual(self.snapshot(),before)
        self.assertEqual(len(self.store.list_segments(1,'batch',self.batch['id'])['segments']),1)

    def test_old_review_required_and_missing_warm_window_block_adoption(self):
        with sqlite3.connect(self.path) as c:c.execute("DELETE FROM lab_draws WHERE issue_no='1001'")
        self.assertIn('warm_window_missing',{x['code'] for x in self.preview()['blockers']})
        with self.assertRaises(RuleLabError):self.enable()

    def test_space_count_includes_nested_regular_files_without_following_links(self):
        before=self.store.storage_status()['used_bytes'];sub=Path(self.tmp.name)/'archives';sub.mkdir()
        (sub/'payload.bin').write_bytes(b'x'*8192)
        (sub/'ignored').symlink_to('/dev/zero')
        self.assertEqual(self.store.storage_status()['used_bytes'],before+8192)

    def capture_future(self):
        work=self.store.capture_work_items(now=NOW)[0]
        draws=[];predictions=[];coverage=[]
        for issue,minute in ((1101,2),(1102,6)):
            at='2026-11-26T16:%02d:30Z'%minute
            draws.append(dict(issue_no=str(issue),open_at=at,source_created_at=at,observed_at=NOW,
                              triplet=[1,3,5],input_hash='draw-'+str(issue)))
            predictions.append(dict(predictor_id=1,issue_no=str(issue),created_at='2026-11-26T16:%02d:30Z'%(minute-1),
                settled_at=at,observed_at=NOW,status='settled',big_small='小',odd_even='单',combo='小单',input_hash='prediction-'+str(issue)))
            coverage.append(dict(issue_no=str(issue),**{'class':'present'}))
        result=self.store.commit_capture(work['id'],work['revision'],draws=draws,predictions=predictions,
            coverage=coverage,next_issue='1102',highest_seen_issue='1102',now=NOW)
        self.assertTrue(result['committed'])

    def test_manual_append_freezes_new_segment_and_completes_without_new_experiments(self):
        self.enable();self.capture_future()
        request=dict(request_id='manual-segment-1',expected_revision=self.preview()['expected_revision'],end_date='2026-11-27')
        self.store.request_append(1,'batch',self.batch['id'],request,now=NOW)
        activated=self.store.prepare_due_appends(now=NOW)
        self.assertEqual(len(activated),1)
        work=self.store.get_work_item(self.batch['id'])
        self.assertEqual(work['batch']['status'],'running')
        self.assertEqual(work['batch']['cursor']['last_issue_no'],'1100')
        self.assertEqual(work['batch']['cursor']['replay_max_issue'],'1102')
        self.assertEqual(work['batch']['end_date'],'2026-11-27')
        self.assertEqual(self.store.get_batch(1,self.batch['id'])['end_date'],'2026-11-26')
        self.assertEqual(self.store.archive_source(self.batch['id']).get_draw_by_issue('1101',observed_at=NOW)['issue_no'],'1101')
        cursor=dict(work['batch']['cursor'],last_issue_no='1102',scan_after_issue='1102',simulation_time='2026-11-26T16:05:50Z')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor=cursor,now=NOW,progress={'processed_issues_delta':2})
        work=self.store.get_work_item(self.batch['id'])
        result=self.store.complete_segment(self.batch['id'],work['batch']['revision'],cursor=cursor,now=NOW)
        self.assertTrue(result['committed'])
        segments=self.store.list_segments(1,'batch',self.batch['id'])['segments']
        self.assertEqual([s['kind'] for s in segments],['append','baseline'])
        self.assertEqual(segments[0]['status'],'completed')
        self.assertEqual(len(self.store.list_experiments(1,self.batch['id'])),26)
        self.assertFalse(self.preview()['continuation']['compute_enabled'])
        self.assertEqual(segments[0]['summary']['batches'][0]['processed_issues'],2)

    def test_capture_gap_does_not_make_an_executable_segment(self):
        self.enable();work=self.store.capture_work_items(now=NOW)[0]
        self.store.commit_capture(work['id'],work['revision'],draws=[],predictions=[],
            coverage=[dict(issue_no='1101',**{'class':'archive_gap'})],next_issue='1101',highest_seen_issue='1101',now=NOW)
        self.store.request_append(1,'batch',self.batch['id'],dict(request_id='gap-append-1',expected_revision=self.preview()['expected_revision'],end_date='2026-11-27'),now=NOW)
        self.assertEqual(self.store.prepare_due_appends(now=NOW),[])
        self.assertEqual(len(self.store.list_segments(1,'batch',self.batch['id'])['segments']),1)
        self.assertEqual(self.store.get_batch(1,self.batch['id'])['status'],'completed')

    def test_frozen_archive_ignores_common_cache_conflict_and_carries_original_warm_revision(self):
        self.enable();self.capture_future()
        payload=dict(request_id='warm-segment-1',expected_revision=self.preview()['expected_revision'],end_date='2026-11-27')
        self.store.request_append(1,'batch',self.batch['id'],payload,now=NOW);self.store.prepare_due_appends(now=NOW)
        with sqlite3.connect(self.path) as c:
            c.execute("UPDATE lab_predictions SET combo='大双',conflict=1 WHERE issue_no='1100'")
            c.execute("UPDATE lab_draws SET conflict=1 WHERE issue_no='1100'")
        reader=self.store.archive_source(self.batch['id'])
        self.assertEqual(reader.get_predictions(1,['1100'],observed_at=NOW)['items'][0]['combo'],'小单')
        self.assertFalse(reader.get_history(1,before_issue='1101')[-1]['conflict'])
        work=self.store.get_work_item(self.batch['id'])
        cursor=dict(work['batch']['cursor'],last_issue_no='1102',scan_after_issue='1102',simulation_time='2026-11-26T16:05:50Z')
        self.store.commit_step(self.batch['id'],work['batch']['revision'],cursor=cursor,now=NOW,progress={'processed_issues_delta':2})
        work=self.store.get_work_item(self.batch['id']);self.store.complete_segment(self.batch['id'],work['batch']['revision'],cursor=cursor,now=NOW)
        with self.store._connect() as c:
            from pc28touzhu.services.rule_lab_series import _unpack
            latest=c.execute('SELECT payload FROM lab_checkpoints WHERE batch_id=? ORDER BY id DESC LIMIT 1',(self.batch['id'],)).fetchone()
            warm=_unpack(latest[0])['warm_inputs']
        self.assertEqual(warm['1100']['prediction']['combo'],'小单')

    def test_pause_preserves_captured_inputs_after_90_days(self):
        self.enable();self.capture_future()
        before=self.store.input_coverage(1,'batch',self.batch['id'])['coverage']['counts']
        self.store.prune(now='2027-04-01T00:00:00Z',limit=500)
        self.store.maintain_archive(now='2027-04-01T00:00:00Z')
        self.assertEqual(self.store.input_coverage(1,'batch',self.batch['id'])['coverage']['counts'],before)
        with self.store._connect() as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM lab_draws WHERE issue_key BETWEEN 1001 AND 1100').fetchone()[0],100)

    def test_active_manual_segment_pause_resume_does_not_enable_daily_updates(self):
        self.enable();self.capture_future()
        self.store.request_append(1,'batch',self.batch['id'],dict(request_id='pause-segment-1',expected_revision=self.preview()['expected_revision'],end_date='2026-11-27'),now=NOW)
        self.store.prepare_due_appends(now=NOW)
        self.store.set_batch_state(1,self.batch['id'],'paused',now=NOW)
        self.store.prepare_due_appends(now=NOW)
        self.assertEqual(self.store.get_work_item(self.batch['id'])['batch']['status'],'paused')
        self.store.set_batch_state(1,self.batch['id'],'running',now=NOW)
        self.assertEqual(self.store.get_work_item(self.batch['id'])['batch']['status'],'running')
        self.assertFalse(self.preview()['continuation']['compute_enabled'])

    def test_legacy_missing_warm_prediction_is_not_invented_as_no_prediction(self):
        with sqlite3.connect(self.path) as c:c.execute("DELETE FROM lab_predictions WHERE issue_no='1005'")
        self.assertFalse(self.preview()['eligible'])
        self.assertIn('warm_prediction_missing',{b['code'] for b in self.preview()['blockers']})

    def test_archive_replay_rejects_missing_original_warm_input(self):
        store=RuleLabStore(Path(self.tmp.name)/'replay.db');store.initialize()
        batch=completed_baseline(store,first=1000,last=1100)
        with sqlite3.connect(store.db_path) as c:
            # Put the original first decision just after midnight, with100 earlier warm inputs.
            for issue in range(1000,1101):
                at=datetime(2026,11,26,0,1,tzinfo=timezone(timedelta(hours=8)))-timedelta(seconds=210*(1100-issue))
                opened=at.astimezone(timezone.utc).isoformat().replace('+00:00','Z')
                created=(at-timedelta(seconds=60)).astimezone(timezone.utc).isoformat().replace('+00:00','Z')
                c.execute('UPDATE lab_draws SET open_at=?,source_created_at=? WHERE issue_key=?',(opened,opened,issue))
                c.execute('UPDATE lab_predictions SET created_at=?,settled_at=? WHERE issue_key=?',(created,opened,issue))
            row=json.loads(c.execute('SELECT cursor_json FROM lab_batches WHERE id=?',(batch['id'],)).fetchone()[0])
            row['warm_before_issue']='1100';c.execute('UPDATE lab_batches SET cursor_json=? WHERE id=?',(json.dumps(row),batch['id']))
        preview=store.continuation_preview(1,'batch',batch['id'],now=NOW)
        store.set_continuation(1,'batch',batch['id'],dict(request_id='replay-warm-config',expected_revision=preview['expected_revision'],
            capture_enabled=True,compute_enabled=False,daily_at='00:10',timezone='Asia/Shanghai'),now=NOW)
        store.capture_work_items(now=NOW)
        eid=store.list_experiments(1,batch['id'])[0]['id']
        with sqlite3.connect(store.db_path) as c:c.execute("DELETE FROM lab_input_versions WHERE kind='draw' AND issue_key=1000")
        config=normalize_batch_payload(dict(name='replay',mode='backtest',predictor_id=1,metric='combo',
            template_pack='combo_long_miss_v1',stake_amount=10,start_date='2026-11-26',end_date='2026-11-26'),now=NOW)
        with self.assertRaises(RuleLabError) as error:store.create_archive_replay(1,batch['id'],eid,config,now=NOW)
        self.assertEqual(error.exception.code,'archive_gap')
        self.assertIn('暖机',error.exception.message)

    def test_due_scheduler_rotates_past_idle_first_page(self):
        self.enable()
        last=None
        for index in range(10):
            batch=completed_baseline(self.store)
            preview=self.store.continuation_preview(1,'batch',batch['id'],now=NOW)
            self.store.set_continuation(1,'batch',batch['id'],dict(request_id='idle-rotation-'+str(index),
                expected_revision=preview['expected_revision'],capture_enabled=True,compute_enabled=index==9,
                daily_at='00:10',timezone='Asia/Shanghai'),now=NOW)
            last=batch['id']
        later='2026-11-28T04:00:00Z'
        self.store.prepare_due_appends(now=later,limit=10)
        self.store.prepare_due_appends(now=later,limit=10)
        preview=self.store.continuation_preview(1,'batch',last,now=later)
        self.assertEqual(preview['latest_request']['kind'],'append')
        self.assertEqual(preview['latest_request']['end_date'],'2026-11-27')

if __name__=='__main__':unittest.main()
