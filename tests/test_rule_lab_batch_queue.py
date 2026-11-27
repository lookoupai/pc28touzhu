import importlib.util
import tempfile
import unittest
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_service import RuleLabService
from pc28touzhu.services.rule_lab_store import RuleLabStore
from pc28touzhu.services.rule_lab_source import RuleLabSource
from tests import test_rule_lab_series as fixtures
NOW = fixtures.NOW
from tests.test_rule_lab_source import SourceFixture

spec = importlib.util.spec_from_file_location('trial_queue', Path(__file__).parents[1] / 'scripts/maintenance/run_rule_lab_batch_queue.py')
queue = importlib.util.module_from_spec(spec)
spec.loader.exec_module(queue)


class BatchQueueTests(unittest.TestCase):
    def test_only_available_slots_start_and_user_pause_is_preserved(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = RuleLabStore(Path(tmp) / 'lab.db'); store.initialize()
            source = SourceFixture(Path(tmp) / 'source.db')
            service = RuleLabService(store, RuleLabSource(source.path))
            batches = []
            for i in range(3):
                cfg = normalize_batch_payload(dict(name='queue'+str(i), mode='backtest', predictor_id=1,
                    metric='big_small',start_date='2026-11-26',end_date='2026-11-26'),now=NOW)
                batch = store.create_batch(1,cfg,'test',build_experiments('big_small'),now=NOW)
                preview = service.continuation_preview(1,'batch',batch['id'],now=NOW)
                batches.append(dict(batch_id=batch['id'],configuration_hash=preview['configuration_hash']))
            manifest = dict(user_id=1,request_prefix='queue-test',daily_at='00:10',batches=batches)
            result = queue.queue_step(service,manifest,now=NOW)
            self.assertEqual([b['state'] for b in result['batches']],['running','running','queued'])
            self.assertEqual(store.capacity()['running'],100)
            service.set_batch_state(1,batches[0]['batch_id'],{'state':'paused'},now=NOW)
            queue.queue_step(service,manifest,now=NOW)
            self.assertEqual(store.get_batch(1,batches[0]['batch_id'])['status'],'paused')
            self.assertEqual(store.get_batch(1,batches[2]['batch_id'])['status'],'running')
            self.assertEqual(store.capacity()['running'],100)

    def test_completed_batch_enables_daily_once_and_preserves_later_user_disable(self):
        fixture = fixtures.SeriesTests(methodName='runTest'); fixture.setUp(); self.addCleanup(fixture.doCleanups)
        self.addCleanup(fixture.tearDown)
        store = fixture.store
        source = SourceFixture(Path(fixture.tmp.name) / 'source.db')
        service = RuleLabService(store, RuleLabSource(source.path))
        bid = fixture.batch['id']
        preview = service.continuation_preview(1,'batch',bid,now=NOW)
        manifest = dict(user_id=1,request_prefix='queue-test',daily_at='00:10',
                        batches=[dict(batch_id=bid,configuration_hash=preview['configuration_hash'])])
        before = [x['id'] for x in store.list_experiments(1,bid)]
        result = queue.queue_step(service,manifest,now=NOW)
        self.assertTrue(result['complete'],result)
        preview = service.continuation_preview(1,'batch',bid,now=NOW)
        self.assertTrue(preview['continuation']['compute_enabled'])
        service.set_continuation(1,'batch',bid,dict(request_id='queue-user-disable',
            expected_revision=preview['expected_revision'],capture_enabled=True,compute_enabled=False,
            daily_at='00:10',timezone='Asia/Shanghai'),now=NOW)
        self.assertTrue(queue.queue_step(service,manifest,now=NOW)['complete'])
        self.assertFalse(service.continuation_preview(1,'batch',bid,now=NOW)['continuation']['compute_enabled'])
        self.assertEqual([x['id'] for x in store.list_experiments(1,bid)],before)
