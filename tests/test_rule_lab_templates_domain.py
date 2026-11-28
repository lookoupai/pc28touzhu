"""Custom template validation preserves frozen condition identities."""
import copy
import importlib
import importlib.util
import json
import unittest

from pc28touzhu.domain.rule_lab import build_conditions
from pc28touzhu.domain.rule_lab_risk import normalize_risk_policy


class RuleLabTemplateDomainTests(unittest.TestCase):
    def domain(self, name, *args, **kwargs):
        module_name = 'pc28touzhu.domain.rule_lab_templates'
        available = importlib.util.find_spec(module_name)
        self.assertIsNotNone(available, '自定义条件及模板规范化功能尚未提供')
        function = getattr(importlib.import_module(module_name), name, None)
        self.assertTrue(callable(function), '尚未提供模板规范化操作：' + name)
        return function(*args, **kwargs)

    def condition(self, value, metric='combo'):
        return self.domain('normalize_condition', metric, value)

    def template(self, definition, **kwargs):
        return self.domain('normalize_template', kwargs.pop('kind', 'condition'),
                           kwargs.pop('name', '自定义条件'), kwargs.pop('description', ''),
                           definition, **kwargs)

    def test_every_builtin_condition_keeps_its_exact_frozen_identity(self):
        for metric in ('big_small', 'odd_even', 'combo'):
            packs = ['basic_v1', 'basic_v2'] + (['combo_long_miss_v1'] if metric == 'combo' else [])
            for pack in packs:
                original = build_conditions(metric, template_pack=pack)
                with self.subTest(metric=metric, pack=pack):
                    result = self.domain('compile_conditions', metric, original)
                    self.assertEqual(result['conditions'], original)
                    self.assertEqual(result['duplicates_removed'], 0)
                    self.assertEqual(result['warnings'], [])

    def test_custom_decimal_rate_is_canonical_and_rederives_metadata(self):
        value = dict(type='hit_rate', operator='lte', threshold=35.0, window_size=20,
                     name='客户端不能决定名称', condition_key='客户端不能决定键')
        result = self.condition(value)
        baseline = next(c for c in build_conditions('combo') if c['type'] == 'hit_rate'
                        and c['operator'] == 'gte' and c['window_size'] == 20)
        same = self.condition({**value, 'operator': 'gte'})
        self.assertEqual(same, baseline)
        self.assertIs(type(result['threshold']), int)
        self.assertEqual(result['name'], '近20期命中率 ≤ 35%')
        precise = self.condition(dict(type='hit_rate', operator='gte', threshold=33.33, window_size=50))
        self.assertEqual(precise['threshold'], 33.33)
        self.assertEqual(precise['name'], '近50期命中率 ≥ 33.33%')
        self.assertEqual(self.condition(precise), precise)

    def test_semantic_dedup_flips_binary_absence_and_inserts_one_baseline(self):
        conditions = [dict(type='draw_absence', bet_value='大', operator='gte', threshold=8),
                      dict(type='draw_streak', bet_value='小', operator='gte', threshold=8),
                      dict(type='miss_streak', threshold=13), dict(type='miss_streak', threshold=13)]
        before = copy.deepcopy(conditions)
        result = self.domain('compile_conditions', 'big_small', conditions)
        self.assertEqual([c['type'] for c in result['conditions']], ['always', 'draw_streak', 'miss_streak'])
        self.assertEqual(result['conditions'][1]['bet_value'], '小')
        self.assertEqual(result['duplicates_removed'], 2)
        self.assertTrue(result['warnings'])
        self.assertEqual(conditions, before)
        odd = self.condition(dict(type='draw_absence', bet_value='单', threshold=8), 'odd_even')
        self.assertEqual((odd['type'], odd['bet_value']), ('draw_streak', '双'))
        combo = self.condition(dict(type='draw_absence', bet_value='大单', threshold=8))
        self.assertEqual((combo['type'], combo['bet_value']), ('draw_absence', '大单'))

    def test_subset_compilation_does_not_force_an_entry_baseline(self):
        value = dict(type='miss_streak', operator='gte', threshold=11)
        result = self.domain('compile_conditions', 'combo', [value], require_baseline=False)
        self.assertEqual([c['type'] for c in result['conditions']], ['miss_streak'])
        self.assertEqual(result['warnings'], [])

    def test_supplied_baseline_is_first_and_duplicates_are_counted(self):
        result = self.domain('compile_conditions', 'odd_even', [dict(type='miss_streak', threshold=1),
                             dict(type='always'), dict(type='always')])
        self.assertEqual([c['type'] for c in result['conditions']], ['always', 'miss_streak'])
        self.assertEqual(result['duplicates_removed'], 1)

    def test_fifty_unique_conditions_include_the_required_baseline(self):
        values = [dict(type='miss_streak', threshold=i) for i in range(1, 50)]
        self.assertEqual(len(self.domain('compile_conditions', 'combo', values)['conditions']), 50)
        with self.assertRaises(ValueError):
            self.domain('compile_conditions', 'combo', values + [dict(type='miss_streak', threshold=50)])
        self.assertEqual(len(self.domain('compile_conditions', 'combo', values * 4)['conditions']), 50)
        with self.assertRaises(ValueError):
            self.domain('compile_conditions', 'combo', [dict(type='always')] * 201)

    def test_malformed_condition_fields_fail_with_value_error(self):
        invalid = [None, [], {}, dict(type='expression', expression='x'), dict(type=[]),
                   dict(type='always', extra=1), dict(type='always', metric='odd_even'),
                   dict(type='always', operator='gte'), dict(type='always', threshold=0),
                   dict(type='always', window_size=20), dict(type='always', bet_value='大单'),
                   dict(type='miss_streak', threshold=1, window_size=20),
                   dict(type='hit_streak', threshold=1, bet_value='大单'),
                   dict(type='draw_streak', threshold=1),
                   dict(type='draw_absence', threshold=1, bet_value='大'),
                   dict(type='hit_rate', threshold=35, window_size=20, bet_value='大单'),
                   dict(type='miss_streak', threshold=1, operator=None),
                   dict(type='miss_streak', threshold=1, operator='eq')]
        for value in invalid:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.condition(value)
        for metric in (None, [], True, 'unsupported'):
            with self.subTest(metric=metric), self.assertRaises(ValueError):
                self.condition(dict(type='always'), metric)

    def test_numeric_bounds_reject_bool_nonfinite_and_excess_precision(self):
        for threshold in (None, True, False, '1', 0, -1, 101, 1.0, 1.5, float('nan'), float('inf')):
            with self.subTest(streak=threshold), self.assertRaises(ValueError):
                self.condition(dict(type='miss_streak', threshold=threshold))
        for threshold in (None, True, '35', -0.01, 100.01, 0.001, float('nan'), float('inf')):
            with self.subTest(rate=threshold), self.assertRaises(ValueError):
                self.condition(dict(type='hit_rate', threshold=threshold, window_size=20))
        for window in (None, True, '20', 20.0, 1, 10, 21, 200):
            with self.subTest(window=window), self.assertRaises(ValueError):
                self.condition(dict(type='hit_rate', threshold=35, window_size=window))
        for threshold in (0, 100):
            self.assertEqual(self.condition(dict(type='hit_rate', threshold=threshold, window_size=100))['threshold'], threshold)
        self.assertEqual(self.condition(dict(type='miss_streak', threshold=100))['threshold'], 100)

    def test_input_collections_are_strict_and_nullable_inapplicable_fields_are_allowed(self):
        always = self.condition(dict(type='always', metric='combo', operator=None,
                                     threshold=None, window_size=None, bet_value=None))
        self.assertEqual(always, build_conditions('combo')[0])
        for values in (None, {}, (), [], 'always'):
            with self.subTest(values=values), self.assertRaises(ValueError):
                self.domain('compile_conditions', 'combo', values)
        with self.assertRaises(ValueError):
            self.domain('compile_conditions', 'combo', [dict(type='always')], require_baseline=1)

    def test_condition_template_trims_metadata_and_returns_normalized_definition(self):
        source = dict(metric='odd_even', conditions=[dict(type='miss_streak', threshold=7)])
        before = copy.deepcopy(source)
        result = self.template(source, name='  连错精选  ', description='  自用说明  ')
        self.assertEqual(result['kind'], 'condition')
        self.assertEqual(result['name'], '连错精选')
        self.assertEqual(result['description'], '自用说明')
        self.assertEqual(result['metrics'], ['odd_even'])
        self.assertEqual(result['condition_count'], 2)
        self.assertEqual(result['definition']['conditions'][0]['type'], 'always')
        self.assertEqual(source, before)
        self.assertEqual(self.template(result['definition'])['definition'], result['definition'])

    def test_risk_template_uses_existing_policy_and_stable_metric_order(self):
        policy = dict(version=1, mode='round', round=dict(enabled=True, profit_target=12),
                      source=dict(rule_id=7, rule_name='实际规则', modified=True))
        source = dict(metrics=['combo', 'odd_even', 'combo', 'big_small'], policy=policy)
        before = copy.deepcopy(source)
        result = self.template(source, kind='risk')
        self.assertEqual(result['metrics'], ['big_small', 'odd_even', 'combo'])
        self.assertEqual(result['definition']['policy'], normalize_risk_policy(policy))
        self.assertIsNone(result['condition_count'])
        self.assertEqual(result['duplicates_removed'], 0)
        self.assertEqual(source, before)
        result['definition']['policy']['source']['rule_name'] = '不能影响原对象'
        self.assertEqual(source, before)
        legacy = self.template(dict(metrics=['combo'], policy=dict(version=1, mode='legacy')), kind='risk')
        self.assertEqual(legacy['definition']['policy'], dict(version=1, mode='legacy'))

    def test_template_metadata_and_definition_limits_are_enforced(self):
        valid = dict(metric='combo', conditions=[dict(type='always')])
        self.assertEqual(self.template(valid, name='', preview=True)['name'], '')
        self.template(valid, name='名' * 60, description='说' * 300)
        for kwargs in [dict(name=''), dict(name='   '), dict(name=None), dict(name='名' * 61),
                       dict(description=None), dict(description='说' * 301), dict(kind='invalid')]:
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                self.template(valid, **kwargs)
        invalid = [None, [], {}, dict(metric='combo', conditions=[], extra=True),
                   dict(metric='combo', conditions=[dict(type='always')], metrics=['combo'])]
        for definition in invalid:
            with self.subTest(definition=definition), self.assertRaises(ValueError):
                self.template(definition)
        for metrics in (None, [], 'combo', ['unknown'], [True], [['combo']]):
            with self.subTest(metrics=metrics), self.assertRaises(ValueError):
                self.template(dict(metrics=metrics, policy=dict(version=1, mode='legacy')), kind='risk')
        with self.assertRaises(ValueError):
            self.template(dict(metrics=['combo']), kind='risk')
        with self.assertRaises(ValueError):
            self.template(dict(metrics=['combo'], policy=dict(mode='round')), kind='risk')
        too_large = dict(metric='combo', conditions=[dict(type='always', name='字' * 12000)])
        self.assertGreater(len(json.dumps(too_large, ensure_ascii=False).encode()), 32768)
        with self.assertRaises(ValueError):
            self.template(too_large)


if __name__ == '__main__':
    unittest.main()
