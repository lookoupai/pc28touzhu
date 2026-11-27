"""Long combo thresholds run through the real ledger without changing legacy batches."""
import tempfile
import unittest
from datetime import datetime,timedelta
from pathlib import Path

from pc28touzhu.domain.rule_lab import normalize_batch_payload,build_experiments
from pc28touzhu.services.rule_lab_source import RuleLabSource
from pc28touzhu.services.rule_lab_store import RuleLabStore
from pc28touzhu.rule_lab_worker import RuleLabWorker
from tests.test_rule_lab_source import SourceFixture,UTC,stamp
from tests.test_rule_lab_results_store import install_settlement_trace, settlement_trace


class LongComboWorkerTests(unittest.TestCase):
    def test_thirty_misses_opens_at_thirty_and_restart_never_repeats_settlement(self):
        with tempfile.TemporaryDirectory() as tmp:
            source=SourceFixture(Path(tmp)/'source.db')
            opened=datetime(2026,11,26,4,tzinfo=UTC);now=stamp(opened+timedelta(hours=4))
            for i in range(36):
                at=opened+timedelta(seconds=i*210)
                source.draw(1000+i,at,balls=(3,6,8))
                source.prediction(1000+i,at,combo='小双')
            store=RuleLabStore(Path(tmp)/'lab.db');store.initialize()
            install_settlement_trace(store)
            cfg=normalize_batch_payload(dict(name='long',mode='backtest',predictor_id=1,metric='combo',
                template_pack='combo_long_miss_v1',start_date='2026-11-26',end_date='2026-11-26'),now=now)
            batch=store.create_batch(1,cfg,'source',build_experiments('combo',template_pack=cfg['template_pack']),now=now)
            store.set_batch_state(1,batch['id'],'running',now=now)
            reader=RuleLabSource(source.path)
            for _ in range(20):
                RuleLabWorker(store,reader,page_size=7).tick(now=now)
                if store.get_batch(1,batch['id'])['status']=='completed':break
            self.assertEqual(store.get_batch(1,batch['id'])['status'],'completed')
            results=store.list_experiments(1,batch['id'])
            for threshold in (7,8,9,10,11,12,13,14,15,20,25,30):
                pair=[e for e in results if e['condition']['threshold']==threshold]
                self.assertEqual(len(pair),2)
                for exp in pair:
                    detail=store.get_experiment_detail(1,batch['id'],exp['id'])
                    self.assertEqual(exp['settled_count'],36-threshold)
                    self.assertEqual(exp['net_profit'],-(36-threshold))
                    self.assertEqual(detail['recent'],[])
                    self.assertEqual(min(int(d['issue_no']) for d in settlement_trace(store,exp['id'])),1000+threshold)
            RuleLabWorker(store,reader).tick(now=now)
            self.assertEqual(store.list_experiments(1,batch['id']),results)


if __name__=='__main__':unittest.main()
