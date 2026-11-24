from datetime import datetime, timezone
import unittest
from pc28touzhu.domain.betting_protocol import target_readiness, parse_acceptance


class BettingProtocolTests(unittest.TestCase):
    protocol = {'mode': 'announcement', 'bot_id': 123}
    def message(self, mid, text, time='2026-11-23T18:34:20Z', reply=None, sender=123):
        return {'id': mid, 'sender_id': sender, 'text': text, 'date': time, 'reply_to': reply}

    def opened(self):
        return self.message(145, '👉期号: 3510182\n封盘时间: 02:36:10\n开奖时间: 02:36:30')

    def test_real_0233_bet_waits_until_023420_opening(self):
        closed = self.message(130, '期号: 3510181 停止下注', '2026-11-23T18:32:45Z')
        first = target_readiness([closed], self.protocol, '3510182', '2026-11-23T18:33:25Z', '2026-11-23T18:35:23Z')
        self.assertEqual(first['state'], 'wait')
        second = target_readiness([closed, self.opened()], self.protocol, '3510182', '2026-11-23T18:34:21Z', '2026-11-23T18:35:23Z')
        self.assertEqual(second['state'], 'ready')
        self.assertEqual(second['send_before'], '2026-11-23T18:35:23+00:00')

    def test_untrusted_old_and_closed_announcements_do_not_open(self):
        for changes in ({'sender_id': 999}, {'reply_to': 1}, {'date': '2026-11-22T18:34:20Z'}):
            with self.subTest(changes=changes):
                result = target_readiness([{**self.opened(), **changes}], self.protocol, '3510182', '2026-11-23T18:34:21Z', '2026-11-23T18:35:23Z')
                self.assertEqual(result['state'], 'wait')
        result = target_readiness([self.opened(),self.message(150,'期号: 3510182 停止下注')], self.protocol,'3510182','2026-11-23T18:34:21Z','2026-11-23T18:35:23Z')
        self.assertEqual(result['state'], 'wait')

    def test_delivered_message_with_closed_reply_is_rejected(self):
        result = parse_acceptance([self.message(133, '已经封盘，暂停投注', reply=132)], self.protocol, 132,'3510182','大单1',{}, {})
        self.assertEqual(result['state'], 'rejected')

    def test_acceptance_requires_correlated_bot_reply_and_amount(self):
        opening = {'state':'ready','issue_no':'3510182','announcement_id':145}
        reply = self.message(160,'期号: abcdef1234567890abcdef1234567890\n大单 -1 KKCOIN - ✅ 投注成功',reply=159)
        result = parse_acceptance([reply],self.protocol,159,'3510182','大单1',opening,opening)
        self.assertEqual(result['state'], 'accepted')
        for changed in ({'reply_to':158},{'sender_id':99},{'text':reply['text'].replace('-1 ', '-2 ')}):
            self.assertEqual(parse_acceptance([{**reply,**changed}],self.protocol,159,'3510182','大单1',opening,opening)['state'],'unconfirmed')
        self.assertEqual(parse_acceptance([reply],self.protocol,159,'3510182','大单1',opening,{**opening,'announcement_id':200})['state'],'unconfirmed')

    def test_private_receipt_does_not_require_group_announcement(self):
        protocol={'mode':'receipt_only','bot_id':123}
        reply=self.message(2,'期号: 3510182\n小双 -3 CNY - 投注成功\n单 -7 CNY - 投注成功',reply=1)
        self.assertEqual(parse_acceptance([reply],protocol,1,'3510182','单7 小双3',{}, {})['state'],'accepted')

    def test_insufficient_budget_and_cross_midnight(self):
        self.assertEqual(target_readiness([self.opened()],self.protocol,'3510182','2026-11-23T18:35:19Z','2026-11-23T18:35:23Z')['state'],'blocked')
        message=self.message(1,'期号: 3510182\n封盘时间: 00:01:10\n开奖时间: 00:01:30','2026-11-23T15:59:20Z')
        result=target_readiness([message],self.protocol,'3510182','2026-11-23T15:59:30Z','2026-11-23T16:01:00Z')
        self.assertEqual(result['state'],'ready')

    def test_mixed_acceptance_is_never_rejected_as_a_whole(self):
        reply=self.message(2,'期号: 3510182\n小双 -3 CNY - 投注成功\n单 -7 CNY - 投注失败',reply=1)
        result=parse_acceptance([reply],self.protocol,1,'3510182','单7 小双3',{}, {})
        self.assertEqual(result['state'],'unconfirmed')
        self.assertEqual(result['reason'],'partial_acceptance')

    def test_letter_prefixed_play_is_not_plain_combo(self):
        reply=self.message(2,'期号: 3510182\nA小单 -1 CNY - 投注成功',reply=1)
        result=parse_acceptance([reply],{'mode':'receipt_only','bot_id':123},1,'3510182','小单1',{}, {})
        self.assertEqual(result['state'],'unconfirmed')
