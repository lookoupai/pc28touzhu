"""Results-only ledger contracts on disposable real SQLite databases."""
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from pc28touzhu.domain.rule_lab import build_experiments, normalize_batch_payload
from pc28touzhu.services.rule_lab_store import RuleLabError, RuleLabStore
from tests.test_rule_lab_risk import policy


NOW = '2026-11-27T04:00:00Z'


def install_settlement_trace(store):
    """Test-only trace of committed removals; never installed by production code."""
    with sqlite3.connect(store.db_path) as c:
        c.execute('''CREATE TABLE IF NOT EXISTS test_settlement_trace(
            experiment_id INTEGER,issue_no TEXT,issue_key INTEGER,bet_value TEXT,round_no INTEGER)''')
        c.execute('''CREATE TRIGGER IF NOT EXISTS test_trace_settlement BEFORE DELETE ON lab_decisions
            WHEN OLD.status='pending' BEGIN
            INSERT INTO test_settlement_trace VALUES(OLD.experiment_id,OLD.issue_no,OLD.issue_key,OLD.bet_value,OLD.round_no);
            END''')


def settlement_trace(store, experiment_id=None):
    with sqlite3.connect(store.db_path) as c:
        c.row_factory=sqlite3.Row
        clause=' WHERE experiment_id=?' if experiment_id is not None else ''
        return [dict(row) for row in c.execute('SELECT * FROM test_settlement_trace'+clause+' ORDER BY experiment_id,issue_key',
                                               (experiment_id,) if experiment_id is not None else ())]


class ResultsOnlyStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'lab.db'
        self.store = RuleLabStore(self.path)
        self.store.initialize()

    def create(self, risk=None):
        payload = dict(name='结果', mode='backtest', predictor_id=1, metric='big_small',
                       start_date='2026-11-27', end_date='2026-11-27')
        if risk is not None:
            payload['risk_policy'] = risk
        config = normalize_batch_payload(payload, now=NOW)
        batch = self.store.create_batch(1, config, 'source', build_experiments('big_small')[:1], now=NOW)
        self.store.set_batch_state(1, batch['id'], 'running', now=NOW)
        return batch['id'], self.store.list_experiments(1, batch['id'])[0]['id']

    def decision(self, eid, issue):
        return dict(experiment_id=eid, issue_no=str(issue), bet_value='大', stake_cents=100,
                    decided_at=NOW, decision_mode='backtest', feature_issue_no=str(issue-1),
                    condition_value=True, condition_matched=True, risk_date='2026-11-27')

    def outcome(self, eid, issue, *, net=100, status='settled'):
        return dict(experiment_id=eid, issue_no=str(issue), status=status,
                    settled_at='2026-11-27T04:05:00Z', result_type=('hit' if net > 0 else 'miss') if status == 'settled' else None,
                    net_cents=net if status == 'settled' else None, direction_hit=net > 0,
                    skip_reason='invalid_input' if status == 'skipped' else '', refund_reason='')

    def commit(self, bid, *, issue=None, decisions=(), outcomes=()):
        work = self.store.get_work_item(bid)
        cursor = dict(work['batch']['cursor'])
        if issue is not None:
            cursor['last_issue_no'] = str(issue)
        return self.store.commit_step(bid, work['batch']['revision'], cursor=cursor, now=NOW,
                                     decisions=decisions, outcomes=outcomes)

    def count(self, table, where='1'):
        with sqlite3.connect(self.path) as c:
            return c.execute('SELECT COUNT(*) FROM '+table+' WHERE '+where).fetchone()[0]

    def test_successful_settlement_keeps_results_without_terminal_rows(self):
        bid, eid = self.create()
        for issue, net in ((1000, 200), (1001, -100), (1002, -100)):
            self.commit(bid, issue=issue, decisions=[self.decision(eid, issue)], outcomes=[self.outcome(eid, issue, net=net)])
        self.assertEqual(self.count('lab_decisions'), 0)
        detail = self.store.get_experiment_detail(1, bid, eid)
        self.assertEqual((detail['experiment']['settled_count'], detail['experiment']['net_profit']), (3, 0))
        self.assertEqual((detail['experiment']['max_drawdown'], detail['experiment']['max_loss_streak']), (2, 2))
        self.assertEqual(detail['daily'][0]['settled_count'], 3)
        self.assertEqual(detail['daily'][0]['net_profit'], 0)
        self.assertEqual(detail['recent'], [])
        self.assertEqual(detail.get('detail_policy'), 'results_only')
        self.assertEqual(detail.get('retention_version'), 1)
        self.assertEqual(detail.get('pending_count'), 0)

    def test_restart_repeated_decision_and_outcome_cannot_recreate_profit(self):
        bid, eid = self.create()
        decision, outcome = self.decision(eid, 1000), self.outcome(eid, 1000)
        self.commit(bid, decisions=[decision], outcomes=[outcome])
        self.assertEqual(self.count('lab_decisions'), 0)
        self.store = RuleLabStore(self.path)
        self.store.initialize()
        result = self.commit(bid, decisions=[decision], outcomes=[outcome])
        self.assertEqual((result['inserted_decisions'], result['settled']), (0, 0))
        detail = self.store.get_experiment_detail(1, bid, eid)
        self.assertEqual(detail['experiment']['net_profit'], 1)
        self.assertEqual(detail['daily'][0]['settled_count'], 1)

    def test_pending_deletion_failure_rolls_back_results_and_watermark(self):
        bid, eid = self.create()
        self.commit(bid, issue=1000, decisions=[self.decision(eid, 1000)])
        before = self.store.get_work_item(bid)
        with sqlite3.connect(self.path) as c:
            c.execute("CREATE TRIGGER abort_pending_delete BEFORE DELETE ON lab_decisions BEGIN SELECT RAISE(ABORT,'interrupt cleanup'); END")
        with self.assertRaises((sqlite3.DatabaseError, RuleLabError)):
            self.commit(bid, outcomes=[self.outcome(eid, 1000)])
        after = self.store.get_work_item(bid)
        self.assertEqual(after, before)
        self.assertEqual(len(self.store.get_pending_decisions(bid)), 1)
        with sqlite3.connect(self.path) as c:
            c.execute('DROP TRIGGER abort_pending_delete')
        self.assertEqual(self.commit(bid, outcomes=[self.outcome(eid, 1000)])['settled'], 1)
        self.assertEqual(self.count('lab_decisions'), 0)

    def test_skipped_settlement_preserves_counts_without_terminal_evidence(self):
        bid, eid = self.create()
        self.commit(bid, issue=1000, decisions=[self.decision(eid, 1000)],
                    outcomes=[self.outcome(eid, 1000, status='skipped')])
        self.assertEqual(self.count('lab_decisions'), 0)
        result = self.commit(bid, outcomes=[self.outcome(eid, 1000, status='skipped')])
        self.assertEqual(result['skipped'], 0)
        work = self.store.get_work_item(bid)
        self.assertEqual(work['experiments'][0]['stats']['skipped_count'], 1)
        self.assertEqual(work['experiments'][0]['stats']['net_cents'], 0)

    def test_closed_round_is_counted_but_not_kept_as_history(self):
        risk = policy()
        risk['round']['profit_target'] = 1
        bid, eid = self.create(risk)
        self.commit(bid, issue=1000, decisions=[self.decision(eid, 1000)])
        self.assertEqual(self.count('lab_rounds', "status='active'"), 1)
        self.commit(bid, outcomes=[self.outcome(eid, 1000)])
        self.assertEqual(self.count('lab_rounds'), 0)
        detail = self.store.get_experiment_detail(1, bid, eid)
        self.assertEqual(detail['recent_rounds'], [])
        self.assertEqual(detail['experiment']['risk_state']['completed_rounds'], 1)
        self.assertEqual(detail['experiment']['risk_state']['round_profit_stops'], 1)

    def test_earlier_pending_blocks_later_outcome_without_losing_it(self):
        bid, eid = self.create()
        self.commit(bid, issue=1001, decisions=[self.decision(eid, 1000), self.decision(eid, 1001)])
        self.assertEqual(self.commit(bid, outcomes=[self.outcome(eid, 1001)])['settled'], 0)
        self.assertEqual(len(self.store.get_pending_decisions(bid)), 2)
        settled = self.commit(bid, outcomes=[self.outcome(eid, 1001, net=-100), self.outcome(eid, 1000, net=200)])
        self.assertEqual(settled['settled'], 2)
        self.assertEqual(self.count('lab_decisions'), 0)
        detail = self.store.get_experiment_detail(1, bid, eid)
        self.assertEqual(detail['experiment']['net_profit'], 1)
        self.assertEqual(detail['experiment']['max_drawdown'], 1)

    def test_stale_write_after_pause_leaves_pending_for_resume(self):
        bid, eid = self.create()
        self.commit(bid, issue=1000, decisions=[self.decision(eid, 1000)])
        work = self.store.get_work_item(bid)
        self.store.set_batch_state(1, bid, 'paused', now=NOW)
        result = self.store.commit_step(bid, work['batch']['revision'], cursor=work['batch']['cursor'], now=NOW,
                                       outcomes=[self.outcome(eid, 1000)])
        self.assertFalse(result['committed'])
        self.assertEqual(len(self.store.get_pending_decisions(bid)), 1)
        self.assertEqual(self.store.list_experiments(1, bid)[0]['settled_count'], 0)

    def test_consumed_frontier_ignores_prefetch_and_unapplied_outcomes(self):
        bid, eid = self.create()
        def draw(issue, opened, visible):
            return dict(issue_no=str(issue), source_id=issue, open_at=opened, source_created_at=visible,
                        observed_at=NOW, triplet=[3, 6, 8], input_hash='draw-'+str(issue))
        draws = [draw(1000, '2026-11-27T04:05:00Z', '2026-11-27T04:05:10Z'),
                 draw(1001, '2026-11-27T04:08:30Z', '2026-11-27T05:00:00Z'),
                 draw(2000, '2026-11-27T08:00:00Z', '2026-11-27T08:00:10Z')]
        work = self.store.get_work_item(bid)
        self.store.commit_step(bid, work['batch']['revision'], cursor={'last_issue_no':'1001','warm_before_issue':'1000'},
                               now=NOW, draws=draws, decisions=[self.decision(eid,1000), self.decision(eid,1001)])
        self.commit(bid, outcomes=[self.outcome(eid,1001)])
        frontier = self.store.get_work_item(bid)['batch']['cursor'].get('retention_v1', {})
        self.assertEqual(frontier.get('first_open_at'), '2026-11-27T04:05:00Z')
        self.assertEqual(frontier.get('processed_open_at'), '2026-11-27T04:08:30Z')
        self.assertIsNone(frontier.get('settled_available_at'))
        self.commit(bid, outcomes=[self.outcome(eid,1000)])
        frontier = self.store.get_work_item(bid)['batch']['cursor']['retention_v1']
        self.assertEqual(frontier['settled_available_at'], '2026-11-27T04:05:10Z')
        self.assertTrue(frontier['frontier_verified'])

    def test_schema_upgrade_defers_old_detail_cleanup_and_keeps_results(self):
        bid, eid = self.create()
        self.commit(bid, issue=1000, decisions=[self.decision(eid,1000)], outcomes=[self.outcome(eid,1000)])
        before = self.store.get_experiment_detail(1,bid,eid)
        with sqlite3.connect(self.path) as c:
            c.execute('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,stake_cents,
                decided_at,decision_mode,condition_value_json,status,net_cents,finalized_at)
                VALUES(?, '1000',1000,'大',100,?,'backtest','true','settled',100,?)''',(eid,NOW,NOW))
            c.execute('PRAGMA user_version=5')
        self.store.initialize()
        self.assertEqual(self.count('lab_decisions'),1)
        self.assertEqual(self.store.get_experiment_detail(1,bid,eid),before)
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],6)

    def test_pending_settlement_can_use_reserved_pages_at_growth_limit(self):
        bid, eid = self.create()
        self.commit(bid, issue=1000, decisions=[self.decision(eid,1000)])
        with sqlite3.connect(self.path) as c:
            # Model a result bucket needing extra SQLite pages during settlement.
            # A trigger keeps the requirement inside the same real transaction.
            c.execute('CREATE TABLE result_bucket(payload BLOB)')
            c.execute('''CREATE TRIGGER settlement_pages BEFORE DELETE ON lab_decisions
                BEGIN INSERT INTO result_bucket(payload) VALUES(zeroblob(16384)); END''')
            pages = c.execute('PRAGMA page_count').fetchone()[0]
            size = c.execute('PRAGMA page_size').fetchone()[0]
        self.store = RuleLabStore(self.path, budget_bytes=int(pages*size/.8)+1)
        result, failure = None, None
        try:
            result = self.commit(bid, outcomes=[self.outcome(eid,1000)])
        except RuleLabError as error:
            failure = error.code
        self.assertIsNone(failure, 'Pending settlement must use the reserved page allowance')
        self.assertEqual(result['settled'], 1)
        self.assertEqual(self.count('lab_decisions'), 0)
        self.assertEqual(self.store.list_experiments(1,bid)[0]['net_profit'], 1)

    def test_current_source_replay_marker_is_created_with_the_batch(self):
        config=normalize_batch_payload(dict(name='来源重测',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-27',end_date='2026-11-27'),now=NOW)
        batch=self.store.create_batch(1,config,'source',build_experiments('big_small')[:1],now=NOW,
                                      replay_input_basis='current_source')
        cursor=self.store.get_work_item(batch['id'])['batch']['cursor']
        self.assertEqual(cursor.get('replay_input_basis'),'current_source')
        self.assertTrue(cursor.get('replay_require_coverage'))
        self.assertEqual(batch['status'],'paused')

    def test_growth_stops_at_compute_budget_while_pending_settlement_remains_possible(self):
        bid,eid=self.create()
        self.commit(bid,issue=1000,decisions=[self.decision(eid,1000)])
        budget=2*1024*1024
        used=sum(p.stat().st_size for p in self.path.parent.iterdir() if p.is_file())
        (self.path.parent/'other-lab-data.bin').write_bytes(b'\0'*(int(budget*.85)-used))
        self.store=RuleLabStore(self.path,budget_bytes=budget)
        denied=False
        try:self.create()
        except RuleLabError as error:
            self.assertEqual(error.code,'storage_limit');denied=True
        self.assertTrue(denied,'New simulations must stop at the same 80% budget as SQLite growth')
        self.assertEqual(self.commit(bid,outcomes=[self.outcome(eid,1000)])['settled'],1)
        self.assertEqual(self.count('lab_decisions'),0)


class ResultsIndexInitializationTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.path=Path(self.tmp.name)/'lab.db'
        self.store=RuleLabStore(self.path)

    def indexes(self):
        with sqlite3.connect(self.path) as c:
            return dict(c.execute("SELECT name,sql FROM sqlite_master WHERE type='index' AND name IN ('lab_decisions_pending','lab_decisions_finished','lab_rounds_finished')"))

    def test_new_database_only_builds_pending_partial_index(self):
        self.store.initialize()
        indexes=self.indexes()
        self.assertEqual(set(indexes),{'lab_decisions_pending'})
        self.assertIn("WHERE status='pending'",indexes['lab_decisions_pending'])

    def test_finalized_schema6_restart_does_not_recreate_history_indexes(self):
        self.store.initialize()
        with sqlite3.connect(self.path) as c:
            for name in ('lab_decisions_pending','lab_decisions_finished','lab_rounds_finished'):
                c.execute('DROP INDEX IF EXISTS '+name)
            c.execute("CREATE INDEX lab_decisions_pending ON lab_decisions(status,issue_key) WHERE status='pending'")
        before=self.indexes()
        self.store.initialize();self.store.initialize()
        self.assertEqual(self.indexes(),before)

    def test_schema5_initialization_leaves_existing_indexes_and_data_for_bounded_maintenance(self):
        self.store.initialize()
        config=normalize_batch_payload(dict(name='old',mode='backtest',predictor_id=1,metric='big_small',
            start_date='2026-11-27',end_date='2026-11-27'),now=NOW)
        batch=self.store.create_batch(1,config,'source',build_experiments('big_small')[:1],now=NOW)
        eid=self.store.list_experiments(1,batch['id'])[0]['id']
        with sqlite3.connect(self.path) as c:
            c.execute('DROP INDEX IF EXISTS lab_decisions_pending')
            c.execute('CREATE INDEX lab_decisions_pending ON lab_decisions(status,issue_key)')
            c.execute('CREATE INDEX IF NOT EXISTS lab_decisions_finished ON lab_decisions(status,finalized_at)')
            c.execute('CREATE INDEX IF NOT EXISTS lab_rounds_finished ON lab_rounds(status,finalized_at)')
            c.executemany('''INSERT INTO lab_decisions(experiment_id,issue_no,issue_key,bet_value,stake_cents,
                decided_at,decision_mode,condition_value_json,status,net_cents,finalized_at)
                VALUES(?,?,?,'大',100,?,'backtest','true','settled',100,?)''',
                ((eid,str(1000+i),1000+i,NOW,NOW) for i in range(2000)))
            c.execute('PRAGMA user_version=5')
            before=c.execute('SELECT COUNT(*),SUM(net_cents) FROM lab_decisions').fetchone()
        indexes=self.indexes()
        self.store.initialize()
        self.assertEqual(self.indexes(),indexes)
        with sqlite3.connect(self.path) as c:
            self.assertEqual(c.execute('SELECT COUNT(*),SUM(net_cents) FROM lab_decisions').fetchone(),before)
            self.assertEqual(c.execute('PRAGMA user_version').fetchone()[0],6)

    def test_schema5_with_missing_indexes_does_not_trigger_history_index_builds(self):
        self.store.initialize()
        with sqlite3.connect(self.path) as c:
            for name in ('lab_decisions_pending','lab_decisions_finished','lab_rounds_finished'):
                c.execute('DROP INDEX IF EXISTS '+name)
            c.execute('PRAGMA user_version=5')
        self.store.initialize()
        self.assertEqual(self.indexes(),{})


if __name__ == '__main__':
    unittest.main()
