"""Cross-source ranks must use one window and exact frozen configurations."""
import copy,json,sqlite3,unittest
from tests import test_rule_lab_api as base
from tests import test_rule_lab_observation_api as observations
from pc28touzhu.domain.rule_lab import build_experiments,normalize_batch_payload
PREFIX=base.PREFIX;NOW='2026-11-28T04:00:00Z'
class ResearchRankTests(unittest.TestCase):
    setUp=base.RuleLabApiTests.setUp
    request=base.RuleLabApiTests.request
    seed=observations.ObservationApiTests.seed
    def fixtures(self):
        batches=[]
        for pid in (7,8):
            cfg=normalize_batch_payload(dict(name='来源'+str(pid),mode='backtest',predictor_id=pid,metric='big_small',start_date='2026-10-01',end_date='2026-11-27'),now=NOW)
            batches.append(self.store.create_batch(1,cfg,'来源'+str(pid),build_experiments('big_small')[:2],now=NOW))
        self.seed([b['id'] for b in batches]);return batches
    def query(self,batches,**kw):
        exp=self.store.list_experiments(1,batches[0]['id'])[0]
        return dict(predictor_ids=[7,8],metric='big_small',condition_key=exp['condition_key'],settlement_rule_id=exp['settlement_rule_id'],months=1,include_partial=False,end_date='2026-11-27',sort='roi',limit=50,cursor=None,**kw)
    def ranked(self,payload,code=200):
        out=self.request(PREFIX+'/research/rankings',payload);self.assertEqual(out['status_code'],code,out);return out['json']
    def test_common_complete_window_and_tied_ranks_are_read_only(self):
        batches=self.fixtures();request=self.query(batches)
        with sqlite3.connect(self.store.db_path) as c:before='\n'.join(c.iterdump())
        result=self.ranked(request)
        self.assertEqual(result['window']['start_date'],'2026-10-01');self.assertEqual(result['window']['end_date'],'2026-10-31')
        self.assertEqual([row['rank'] for row in result['items']],[1,1]);self.assertTrue(all(r['eligible'] for r in result['items']))
        self.assertEqual([r['metrics']['net_cents'] for r in result['items']],[599,599])
        with sqlite3.connect(self.store.db_path) as c:self.assertEqual('\n'.join(c.iterdump()),before)
    def test_different_actual_odds_cannot_share_rank_and_partial_has_no_rank(self):
        batches=self.fixtures();payload=self.query(batches)
        with sqlite3.connect(self.store.db_path) as c:
            row=c.execute('SELECT id,settlement_snapshot_json FROM lab_experiments WHERE batch_id=? ORDER BY id',(batches[1]['id'],)).fetchone()
            value=json.loads(row[1]);value['resolved_odds']['big_small']=1.98
            c.execute('UPDATE lab_experiments SET settlement_snapshot_json=? WHERE id=?',(json.dumps(value),row[0]))
        result=self.ranked(payload);self.assertEqual(len(result['groups']),2);self.assertTrue(all(r['rank'] is None for r in result['items']))
        partial=self.ranked(dict(payload,include_partial=True));self.assertTrue(all(not r['eligible'] for r in partial['items']))
    def test_pagination_binds_result_revision_and_filters(self):
        batches=self.fixtures();payload=self.query(batches);payload['limit']=1
        result=self.ranked(payload);self.assertEqual(len(result['items']),1);self.assertTrue(result['next_cursor'])
        next_page=self.ranked(dict(payload,cursor=result['next_cursor']));self.assertEqual(len(next_page['items']),1)
        self.assertNotEqual(next_page['items'][0]['experiment_id'],result['items'][0]['experiment_id'])
        self.ranked(dict(payload,cursor=result['next_cursor'],sort='net_profit'),409)
        self.ranked(dict(payload,predictor_ids=[True]),400)
if __name__=='__main__':unittest.main()
