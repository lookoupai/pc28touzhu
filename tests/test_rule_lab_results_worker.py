"""The worker gives existing deletion and result retention one bounded allowance."""
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

from pc28touzhu.rule_lab_worker import RuleLabWorker
from pc28touzhu.services.rule_lab_store import RuleLabStore
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from tests.test_rule_lab_source import SourceFixture, UTC, stamp


NOW = '2026-11-27T04:00:00Z'


class ResultMaintenanceWorkerTests(unittest.TestCase):
    def make(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        store = RuleLabStore(Path(tmp.name) / 'lab.db')
        store.initialize()
        store.retention_maintenance = Mock(return_value=dict(processed_rows=120, scanned_rows=120,
                                                           maintenance_pending=True, phase='terminal'))
        return store

    def test_idle_worker_drains_retention_with_shared_delete_allowance(self):
        store = self.make()
        store.management_maintenance = Mock(return_value=dict(processed_rows=150, freed_bytes=4096, maintenance_pending=False))
        result = RuleLabWorker(store, object()).tick(now=NOW)
        store.retention_maintenance.assert_called_once_with(now=NOW, limit=350)
        self.assertEqual(result['maintenance_rows'], 270)
        self.assertEqual(result['freed_bytes'], 4096)
        self.assertTrue(result['maintenance_pending'])

    def test_full_management_allowance_leaves_no_second_retention_page(self):
        store = self.make()
        store.management_maintenance = Mock(return_value=dict(processed_rows=500, freed_bytes=0, maintenance_pending=True))
        result = RuleLabWorker(store, object()).tick(now=NOW)
        store.retention_maintenance.assert_not_called()
        self.assertEqual(result['maintenance_rows'], 500)

    def test_old_unbounded_retention_entrypoints_are_not_also_run(self):
        store = self.make()
        store.prune = Mock(side_effect=AssertionError('old age retention must not run'))
        store.maintain_archive = Mock(side_effect=AssertionError('old two-year archive retention must not run'))
        RuleLabWorker(store, object()).tick(now=NOW)
        self.assertEqual(store.retention_maintenance.call_count, 1)


class CurrentSourceReplayWorkerTests(unittest.TestCase):
    def make(self):
        tmp=tempfile.TemporaryDirectory();self.addCleanup(tmp.cleanup)
        root=Path(tmp.name)
        fixture=SourceFixture(root/'source.db')
        first=datetime(2026,11,26,16,tzinfo=UTC)
        for offset in range(-100,5):
            opened=first+timedelta(seconds=210*offset)
            fixture.draw(1100+offset,opened);fixture.prediction(1100+offset,opened)
        fixture.execute('CREATE TABLE pc28_prediction_daily_summary(predictor_id INTEGER,summary_date TEXT)')
        fixture.draw(1105,first+timedelta(days=1));fixture.prediction(1105,first+timedelta(days=1))
        now=stamp(first+timedelta(days=2))
        store=RuleLabStore(root/'lab'/'lab.db');store.initialize()
        config=normalize_batch_payload(dict(name='当前来源重测',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-27',end_date='2026-11-27'),now=now)
        batch=store.create_batch(1,config,'source',build_experiments('big_small')[:1],now=now,replay_input_basis='current_source')
        store.set_batch_state(1,batch['id'],'running',now=now)
        return fixture,store,batch['id'],now

    def test_predictions_removed_after_preview_do_not_become_completed_missing_rows(self):
        fixture,store,bid,now=self.make()
        class DeletingSource(RuleLabSource):
            def draws_page(self,**kwargs):
                page=super().draws_page(**kwargs)
                fixture.execute("DELETE FROM predictions WHERE issue_no='1101'")
                fixture.execute("INSERT INTO pc28_prediction_daily_summary VALUES(1,'2026-11-27')")
                return page
        source=DeletingSource(fixture.path)
        self.assertTrue(source.replay_availability(1,'2026-11-27','2026-11-27',observed_at=now)['available'])
        RuleLabWorker(store,source).tick(now=now)
        batch=store.get_batch(1,bid)
        self.assertEqual(batch['status'],'error')
        self.assertIn('清理',batch['error'])
        self.assertEqual(batch['processed_issues'],0)
        self.assertEqual(store.list_experiments(1,bid)[0]['settled_count'],0)

    def test_draw_gap_after_start_stops_instead_of_claiming_complete_range(self):
        fixture,store,bid,now=self.make()
        class MissingDrawSource(RuleLabSource):
            def draws_page(self,**kwargs):
                fixture.execute("DELETE FROM lottery_draws WHERE issue_no='1102'")
                return super().draws_page(**kwargs)
        source=MissingDrawSource(fixture.path)
        RuleLabWorker(store,source).tick(now=now)
        batch=store.get_batch(1,bid)
        self.assertEqual(batch['status'],'error')
        self.assertIn('缺',batch['error'])
        self.assertFalse(store.get_work_item(bid)['batch']['cursor'].get('replay_complete'))


if __name__ == '__main__':
    unittest.main()
