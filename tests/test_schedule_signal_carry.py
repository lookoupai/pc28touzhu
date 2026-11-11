"""Window-start signal freshness, including the 3504912 midnight regression."""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from tests.test_cycle_runtime import CycleFixture
from pc28touzhu.services.auto_trigger_service import _normalize_schedule, _scheduled_signal_gate, update_auto_trigger_rule


class ScheduleSignalCarryTests(CycleFixture):
    def setUp(self):
        super().setUp()
        self.now=datetime(2026,11,10,16,0,36,tzinfo=timezone.utc)
        self.latest=3504911

    def scheduled(self, allow=False, combined=False):
        rule=self.rule(targets=[self.targets[1]],limit=1)
        return update_auto_trigger_rule(self.repo,rule_id=rule['id'],user_id=self.user_id,payload={
            'trigger_mode':'condition_schedule' if combined else 'schedule',
            'schedule':{'timezone':'Asia/Shanghai','windows':[{'id':'midnight','start':'00:00','end':'00:59'}],
                        'signal_max_age_seconds':300,'allow_pre_window_signal':allow}})['item']

    def old_signal(self, age=46):
        return self.repo.create_signal_record(source_id=self.source['id'],lottery_type='pc28',issue_no='3504912',
            bet_type='combo',bet_value='小单',published_at=(self.now-timedelta(seconds=age)).isoformat())

    def test_default_still_requires_publication_inside_window(self):
        rule=self.scheduled();self.old_signal()
        result=self.cycle(rule)
        self.assertEqual(result['summary']['triggered_count'],0)
        self.assertEqual(result['rules'][0]['events'][0]['reason'],'schedule_signal_before_window')

    def test_allow_carries_midnight_signal_and_keeps_normal_issue_gate(self):
        rule=self.scheduled(True);self.old_signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)
        jobs=self.repo.list_execution_jobs(user_id=self.user_id)
        self.assertEqual(len(jobs),2)
        self.assertEqual({j['issue_no'] for j in jobs},{'3504912'})

    def test_combined_trigger_uses_same_opt_in(self):
        rule=self.scheduled(True,combined=True);self.old_signal()
        self.assertEqual(self.cycle(rule,matches=False)['summary']['triggered_count'],0)
        self.assertEqual(self.cycle(rule,matches=True)['summary']['triggered_count'],1)

    def test_stale_and_near_draw_and_unknown_clock_remain_blocked(self):
        rule=self.scheduled(True)
        clock={'latest_issue_no':'3504911','latest_open_time':'2026-11-10T15:59:00Z','fetched_at':self.now.isoformat(),'stale':False}
        signal={'issue_no':'3504912','published_at':'2026-11-10T15:59:50Z'}
        for changes,now,clock_value,reason in [
            ({'published_at':'2026-11-10T15:54:00Z'},self.now,clock,'schedule_signal_stale'),
            ({},datetime(2026,11,10,16,2,15,tzinfo=timezone.utc),clock,'window_too_short'),
            ({},self.now,None,'draw_clock_unavailable'),
            ({},self.now,{**clock,'stale':True},'draw_clock_unavailable'),
        ]:
            with self.subTest(reason=reason):
                result=_scheduled_signal_gate(rule,{**signal,**changes},now=now,draw_clock=clock_value)
                self.assertFalse(result['allowed']);self.assertEqual(result['reason'],reason)

    def test_flat_scheduled_path_uses_opt_in(self):
        self.strategy['staking_policy']={'mode':'fixed','fixed_amount':10}
        self.repo.update_subscription_record(subscription_id=self.sub['id'],user_id=self.user_id,source_id=self.source['id'],strategy=self.strategy,status='standby')
        rule=self.scheduled(True);self.old_signal()
        self.assertEqual(self.cycle(rule)['summary']['triggered_count'],1)

    def test_option_validation_and_edit_roundtrip(self):
        rule=self.scheduled(True)
        saved=update_auto_trigger_rule(self.repo,rule_id=rule['id'],user_id=self.user_id,payload={'name':'renamed'})['item']
        self.assertTrue(saved['schedule']['allow_pre_window_signal'])
        self.assertFalse(_normalize_schedule({})['allow_pre_window_signal'])
        for value in ('false',1,None):
            with self.subTest(value=value),self.assertRaises(ValueError):
                _normalize_schedule({'allow_pre_window_signal':value})
