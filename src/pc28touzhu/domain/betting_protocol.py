"""Target-specific readiness and betting acceptance, independent of transport."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
import re

CN = timezone(timedelta(hours=8))
MODES = {'legacy', 'announcement', 'delay', 'receipt_only'}


def normalize_protocol(value):
    if not value:
        return {'mode': 'legacy'}
    if not isinstance(value, dict):
        raise ValueError('betting_protocol 必须是对象')
    mode = str(value.get('mode') or 'legacy')
    if mode not in MODES:
        raise ValueError('不支持的投注协议')
    if mode == 'legacy':
        return {'mode': mode}
    bot_id = int(value.get('bot_id') or 0)
    if bot_id <= 0:
        raise ValueError('请指定可信受理机器人的数字 ID')
    delay = int(value.get('opening_delay_seconds') or 0)
    if not 0 <= delay <= 150:
        raise ValueError('开奖后等待时间必须在 0～150 秒之间')
    return {'mode': mode, 'bot_id': bot_id, 'opening_delay_seconds': delay,
            'ack_timeout_seconds': max(3, min(15, int(value.get('ack_timeout_seconds') or 8))),
            'min_send_budget_seconds': max(5, min(30, int(value.get('min_send_budget_seconds') or 10)))}


def _instant(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    return datetime.fromisoformat(str(value).replace('Z', '+00:00')).astimezone(timezone.utc)


def target_readiness(messages, protocol, issue_no, now, deadline):
    """Use only trusted broadcast announcements, never another bet's reply."""
    protocol = normalize_protocol(protocol)
    now, deadline = _instant(now), _instant(deadline)
    if (deadline - now).total_seconds() < protocol.get('min_send_budget_seconds', 10):
        return {'state': 'blocked', 'reason': 'insufficient_send_budget'}
    if protocol['mode'] != 'announcement':
        return {'state': 'ready', 'reason': 'configured_time_policy', 'issue_no': str(issue_no)}
    for message in sorted(messages, key=lambda m: int(m['id']), reverse=True):
        if message.get('sender_id') != protocol['bot_id'] or message.get('reply_to'):
            continue
        text = message.get('text') or ''
        issue = re.search(r'期号\s*[:：]\s*(\d{7,})(?!\d)', text)
        if not issue:
            continue
        opened = '封盘时间' in text and '开奖时间' in text
        closed = '停止下注' in text or '停止投注' in text
        if not opened and not closed:
            continue
        observed = _instant(message['date'])
        if not -2 <= (now - observed).total_seconds() <= 240:
            return {'state': 'wait', 'reason': 'announcement_stale'}
        evidence = {'announcement_id': message['id'], 'observed_at': observed.isoformat(), 'issue_no': issue[1]}
        if int(issue[1]) > int(issue_no):
            return {**evidence, 'state': 'blocked', 'reason': 'target_issue_passed'}
        if issue[1] != str(issue_no) or closed:
            return {**evidence, 'state': 'wait', 'reason': 'target_not_open'}
        match = re.search(r'封盘时间\s*[:：]\s*(\d{2}:\d{2}:\d{2})', text)
        if not match:
            return {**evidence, 'state': 'wait', 'reason': 'missing_target_deadline'}
        local = observed.astimezone(CN)
        close = datetime.combine(local.date(), datetime.strptime(match[1], '%H:%M:%S').time(), CN)
        if (close - local).total_seconds() < -43200:
            close += timedelta(days=1)
        effective = min(deadline, close.astimezone(timezone.utc))
        state = 'ready' if (effective - now).total_seconds() >= protocol['min_send_budget_seconds'] else 'blocked'
        return {**evidence, 'state': state, 'reason': 'target_open' if state == 'ready' else 'insufficient_send_budget',
                'send_before': effective.isoformat()}
    return {'state': 'wait', 'reason': 'announcement_unavailable'}


def _expected_bets(text):
    tokens = re.findall(r'(大单|大双|小单|小双|大|小|单|双)\s*(\d+(?:\.\d+)?)', text)
    # This adapter only verifies these plain text plays; unsupported templates
    # require their own acceptance parser rather than guessing from a success word.
    remainder = re.sub(r'(大单|大双|小单|小双|大|小|单|双)\s*\d+(?:\.\d+)?', '', text)
    if remainder.strip() or not tokens:
        return None
    totals = {}
    for value, amount in tokens:
        totals[value] = totals.get(value, Decimal(0)) + Decimal(amount)
    return totals


def parse_acceptance(messages, protocol, outgoing_id, issue_no, message_text, opening, current_opening):
    protocol = normalize_protocol(protocol)
    replies = [m for m in messages if m.get('sender_id') == protocol.get('bot_id')
               and m.get('reply_to') == int(outgoing_id)]
    for reply in sorted(replies, key=lambda m: int(m['id'])):
        text = reply.get('text') or ''
        evidence = {'reply_message_id': reply['id'], 'reply_to': outgoing_id,
                    'reply_time': reply['date'], 'reply_text': text[:3000]}
        if any(word in text for word in ('已经封盘', '已封盘', '暂停投注', '投注失败', '下注失败', '余额不足')):
            if '投注成功' in text or '下注成功' in text:
                return {**evidence, 'state': 'unconfirmed', 'reason': 'partial_acceptance'}
            return {**evidence, 'state': 'rejected', 'reason': 'target_rejected'}
        if '投注成功' not in text and '下注成功' not in text:
            continue
        actual = re.search(r'期号\s*[:：]\s*([a-fA-F0-9]+)', text)
        if not actual:
            return {**evidence, 'state': 'unconfirmed', 'reason': 'acceptance_issue_missing'}
        period = actual[1]
        if period.isdigit() and len(period) < 20:
            matches = period == str(issue_no)
        else:
            # This bot uses an opaque round token. Its trusted numeric opening
            # must be unchanged before and after this directly correlated reply.
            matches = (protocol['mode'] == 'announcement' and opening.get('state') == 'ready'
                       and current_opening.get('state') == 'ready'
                       and opening.get('announcement_id') == current_opening.get('announcement_id')
                       and opening.get('issue_no') == current_opening.get('issue_no') == str(issue_no))
        if not matches:
            return {**evidence, 'state': 'unconfirmed', 'reason': 'acceptance_issue_mismatch'}
        expected = _expected_bets(message_text)
        received = {}
        lines = re.findall(r'(?m)^[^\w\r\n]*(大单|大双|小单|小双|大|小|单|双)\s*[-－]\s*(\d+(?:\.\d+)?)\s*[^\n]*?(?:投注成功|下注成功)', text)
        if len(lines) != sum('投注成功' in line or '下注成功' in line for line in text.splitlines()):
            return {**evidence, 'state':'unconfirmed', 'reason':'acceptance_play_unsupported'}
        for value, amount in lines:
            received[value] = received.get(value, Decimal(0)) + Decimal(amount)
        if expected is None or expected != received:
            return {**evidence, 'state': 'unconfirmed', 'reason': 'acceptance_amount_mismatch'}
        return {**evidence, 'state': 'accepted', 'reason': 'target_accepted',
                'issue_no': str(issue_no), 'target_round_id': period, 'opening_evidence': opening}
    return {'state': 'unconfirmed', 'reason': 'acceptance_reply_missing'}
