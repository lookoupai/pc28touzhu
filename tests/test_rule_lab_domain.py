"""Rules are evaluated against information available before the decision."""
import copy
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from pc28touzhu.domain.rule_lab import (
    apply_outcome, build_conditions, build_experiments, build_features,
    empty_stats, match_condition, normalize_batch_payload, settle_bet,
)
from pc28touzhu.domain.settlement_rules import SETTLEMENT_RULE_CATALOG

NOW = '2026-11-26T04:00:00Z'


def history(count=100, *, hit=True):
    end = datetime(2026, 11, 26, 3, tzinfo=timezone.utc)
    rows = []
    for i in range(count):
        opened = end - timedelta(seconds=(count-i)*210)
        stamp = opened.isoformat().replace('+00:00', 'Z')
        prediction_at = (opened-timedelta(seconds=60)).isoformat().replace('+00:00', 'Z')
        rows.append(dict(issue_no=str(1000+i), open_at=stamp, draw_available_at=stamp,
                         triplet=[3, 6, 8], prediction_created_at=prediction_at,
                         prediction_settled_at=stamp, prediction_value='大' if hit else '小', conflict=False))
    return rows


class RuleLabDomainTests(unittest.TestCase):
    def test_combo_long_miss_pack_preserves_existing_keys_and_has_two_rules_per_threshold(self):
        original=build_conditions('combo')
        conditions=build_conditions('combo',template_pack='combo_long_miss_v1')
        self.assertEqual(len(conditions),13)
        self.assertEqual([c['threshold'] for c in conditions if c['type']=='miss_streak'],[3,4,5,6,7,8,10,12,15,20,25,30])
        old={c['condition_key']:c for c in original if c['type'] in ('always','miss_streak')}
        self.assertTrue(all(c in conditions for c in old.values()))
        definitions=build_experiments('combo',template_pack='combo_long_miss_v1')
        self.assertEqual(len(definitions),26)
        self.assertTrue(all(sum(e['condition_key']==c['condition_key'] for e in definitions)==2 for c in conditions))
        self.assertEqual(build_conditions('combo'),original)
        for metric in ('big_small','odd_even'):
            with self.assertRaises(ValueError):build_conditions(metric,template_pack='combo_long_miss_v1')
        payload=dict(name='long',mode='live',predictor_id=1,metric='combo',template_pack='combo_long_miss_v1')
        self.assertEqual(normalize_batch_payload(payload,now=NOW)['template_pack'],'combo_long_miss_v1')
        with self.assertRaises(ValueError):normalize_batch_payload({**payload,'metric':'odd_even'},now=NOW)

    def test_combo_long_streak_entry_thresholds_use_only_visible_consecutive_predictions(self):
        conditions=[c for c in build_conditions('combo',template_pack='combo_long_miss_v1') if c['type']=='miss_streak']
        for count in (7,8,10,12,15,20,25,29,30,40):
            rows=history(count)
            for row in rows:row['prediction_value']='小双'  # Actual 3+6+8 is 大单.
            features=build_features('combo',rows,decision_at=NOW)
            self.assertEqual(features['miss_streak'],count)
            self.assertEqual([c['threshold'] for c in conditions if match_condition(c,features)['matched']],
                             [c['threshold'] for c in conditions if c['threshold']<=count])
        for field,value in [('prediction_value',None),('prediction_created_at','2026-11-26T04:01:00Z'),('conflict',True)]:
            rows=history(30)
            for row in rows:row['prediction_value']='小双'
            rows[-1][field]=value
            features=build_features('combo',rows,decision_at=NOW)
            self.assertEqual(features['miss_streak'],0)
            self.assertFalse(any(match_condition(c,features)['matched'] for c in conditions))

    def test_templates_are_unique_and_exactly_two_presets(self):
        for metric, count in [('big_small', 25), ('odd_even', 25), ('combo', 39)]:
            conditions = build_conditions(metric)
            self.assertEqual(len(conditions), count)
            self.assertEqual(len({c['condition_key'] for c in conditions}), count)
            self.assertEqual(conditions[0]['type'], 'always')
            exps = build_experiments(metric)
            self.assertEqual(len(exps), 2*count)
            self.assertEqual({e['settlement_rule_id'] for e in exps}, {'pc28_netdisk_regular', 'pc28_high_regular'})
        with self.assertRaises(ValueError):
            build_conditions('number')

    def test_payload_validation_and_defaults(self):
        payload = dict(name='  测试  ', mode='live', predictor_id=1, metric='big_small')
        result = normalize_batch_payload(payload, now=NOW)
        self.assertEqual(result['name'], '测试')
        self.assertEqual(result['stake_cents'], 100)
        for changes in [dict(user_id=4), dict(predictor_id=True), dict(stake_amount=float('nan')),
                        dict(stake_amount=float('inf')), dict(stake_amount=True), dict(stake_amount=.001),
                        dict(start_date='2026-11-01'), dict(mode='bad'), dict(name=' '), dict(metric='number'),
                        dict(mode=[]),dict(mode={}),dict(metric=[]),dict(metric={}),dict(template_pack=[])]:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                normalize_batch_payload({**payload, **changes}, now=NOW)
        backtest = {**payload, 'mode': 'backtest', 'start_date': '2026-11-01', 'end_date': '2026-11-26'}
        self.assertEqual(normalize_batch_payload(backtest, now=NOW)['start_date'], '2026-11-01')
        for changes in [dict(start_date='2026-01-01'), dict(end_date='2026-11-27'), dict(start_date='2026-11-28'), dict(end_date=None)]:
            with self.assertRaises(ValueError):
                normalize_batch_payload({**backtest, **changes}, now=NOW)

    def test_features_and_matching_need_full_window(self):
        f = build_features('big_small', history(100), decision_at=NOW)
        self.assertEqual(f['hit_streak'], 100)
        self.assertEqual(f['draw_streaks']['大'], 100)
        self.assertEqual(f['rates']['100']['rate'], 100)
        condition = next(c for c in build_conditions('big_small') if c['type']=='hit_rate' and c['window_size']==100 and c['operator']=='gte')
        self.assertTrue(match_condition(condition, f)['matched'])
        f = build_features('big_small', history(99), decision_at=NOW)
        self.assertIsNone(f['rates']['100']['rate'])
        self.assertEqual(match_condition(condition, f)['reason'], 'insufficient_history')

    def test_gap_and_missing_prediction_break_prediction_streak(self):
        rows = history(10)
        rows[-1]['prediction_value'] = None
        f = build_features('big_small', rows, decision_at=NOW)
        self.assertEqual(f['hit_streak'], 0)
        self.assertEqual(f['draw_streaks']['大'], 10)
        rows = history(10)
        del rows[-2]
        f = build_features('big_small', rows, decision_at=NOW)
        self.assertEqual(f['hit_streak'], 1)
        self.assertEqual(f['draw_streaks']['大'], 1)
        self.assertTrue(f['gap_detected'])

    def test_late_information_is_not_visible_or_bridged(self):
        for field in ['draw_available_at', 'prediction_settled_at', 'prediction_created_at']:
            rows = history(10)
            rows[-1][field] = '2026-11-26T04:01:00Z'
            f = build_features('big_small', rows, decision_at=NOW)
            self.assertEqual(f['hit_streak'], 0)
            if field == 'draw_available_at':
                self.assertEqual(f['draw_streaks']['大'], 0)
        rows = history(10)
        rows[-1]['conflict'] = True
        self.assertEqual(build_features('big_small', rows, decision_at=NOW)['hit_streak'], 0)

    def test_shared_prediction_values(self):
        rows = history(3)
        for r in rows:
            del r['prediction_value']
            r['prediction_values'] = {'big_small':'大','odd_even':'单','combo':'大单'}
        self.assertEqual(build_features('odd_even', rows, decision_at=NOW)['hit_streak'],3)

    def test_past_prediction_created_after_its_draw_cannot_improve_features(self):
        rows = history(10)
        rows[-1]['prediction_created_at'] = rows[-1]['open_at']
        features = build_features('big_small',rows,decision_at=NOW)
        self.assertEqual(features['hit_streak'],0)
        self.assertEqual(features['draw_streaks']['大'],10)

    def test_prediction_conflict_does_not_erase_actual_draw_streak(self):
        rows=history(10)
        rows[-1].update(conflict=True,draw_conflict=False,prediction_conflict=True)
        features=build_features('big_small',rows,decision_at=NOW)
        self.assertEqual(features['draw_streaks']['大'],10)
        self.assertEqual(features['draw_absences']['小'],10)
        self.assertEqual(features['hit_streak'],0)
        rows[-1].update(draw_conflict=True,prediction_conflict=False)
        self.assertEqual(build_features('big_small',rows,decision_at=NOW)['draw_streaks']['大'],0)

    def test_frozen_settlement_rounding_refunds_and_loss(self):
        definitions = build_experiments('big_small')
        netdisk, high = [d['settlement_snapshot'] for d in definitions[:2]]
        self.assertEqual(settle_bet(high, 'big_small', '大', 100, [3,6,8])['net_cents'],185)
        self.assertEqual(settle_bet(netdisk,'big_small','大',100,[3,6,8])['net_cents'],98)
        for balls in [[4,4,9], [4,5,6], [9,9,9], [1,5,7], [1,5,8], [0,1,9]]:
            target = '大' if sum(balls)>=14 else '小'
            out = settle_bet(high,'big_small',target,100,balls)
            self.assertEqual((out['result_type'],out['net_cents'],out['direction_hit']),('refund',0,True))
            inverse = '小' if target=='大' else '大'
            self.assertEqual(settle_bet(high,'big_small',inverse,100,balls)['net_cents'],-100)
            self.assertEqual(settle_bet(netdisk,'big_small',target,100,balls)['result_type'],'hit')
        changed = copy.deepcopy(SETTLEMENT_RULE_CATALOG['pc28_high_regular'])
        changed['metric_settlement'] = {}
        changed['implemented_odds'] = {}
        with patch.dict(SETTLEMENT_RULE_CATALOG, {'pc28_high_regular':changed}):
            self.assertEqual(settle_bet(high,'big_small','大',100,[3,6,8])['net_cents'],185)
            self.assertEqual(settle_bet(high,'big_small','大',100,[4,4,9])['result_type'],'refund')
        with self.assertRaises(ValueError):
            settle_bet({},'big_small','大',100,[3,6,8])

    def test_incremental_drawdown_and_loss_streak(self):
        original = empty_stats()
        stats = original
        for cents, result in [(185,'hit'),(-100,'miss'),(-100,'miss'),(0,'refund'),(98,'hit')]:
            stats = apply_outcome(stats, dict(net_cents=cents,result_type=result,direction_hit=result!='miss'),stake_cents=100)
        self.assertEqual(original, empty_stats())
        self.assertEqual(stats['net_cents'],83)
        self.assertEqual(stats['max_drawdown_cents'],200)
        self.assertEqual(stats['max_loss_streak'],2)
        self.assertEqual(stats['current_loss_streak'],0)
        self.assertEqual(stats['trigger_count'],0)


if __name__ == '__main__':
    unittest.main()
