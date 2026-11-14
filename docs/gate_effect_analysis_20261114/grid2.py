#!/usr/bin/env python3
"""规则6/7/8 的参数网格。

- 规则6 组合连错 11 次自动跟单（遗漏回补）：触发重建未能校准（streak口径不明），
  只用 10-01→10-23 的无截断账本（样本外）做虚拟网格 + 结构性建议。
- 规则7 大小连错 5 次自动跟单 / 规则8 单双连错 5 次自动跟单 单轮止盈 1：
  触发语义已校准（源16 指标信号连续未中≥5 → 下一期开跑，每期跟，订阅层±10收轮，
  冷却10期），先在“熔断开启”模式下对账验证，再关熔断跑阈值网格。
  所有变体决策只用实时信息。
"""
import sys, os, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from analyze import load_all, parse_dt, mean, stdev
from collections import defaultdict
from datetime import timedelta, timezone

BJ = timezone(timedelta(hours=8))
HERE = os.path.dirname(os.path.abspath(__file__))

data = load_all()
draws_by_issue = {}
for r in csv.DictReader(open(os.path.join(HERE, 'data', 'draws.csv'))):
    draws_by_issue[r['issue_no']] = r

# ---------- 规则6：样本外（10-01→10-23 无截断账本）虚拟网格 ----------

def rule6_pregate_grid():
    led = defaultdict(list)
    for r in data['ledger']:
        if r['rule_id'] == '6' and '2026-10-01' <= r['stat_date'] <= '2026-10-23':
            led[r['stat_date']].append(r)
    for k in led:
        led[k].sort(key=lambda r: (r['settled_at'], int(r['id'])))
    print("=" * 78)
    print("规则6 组合连错 11 次自动跟单（遗漏回补）—— 10-01→10-23 无截断样本外网格")
    print("（触发streak口径未能校准，11月熔断期无法无偏重放；此网格仅覆盖23个样本外交易日）")
    base = sum(sum(float(x['net_delta']) for x in rows) for rows in led.values())
    worst = min(sum(float(x['net_delta']) for x in rows) for rows in led.values())
    print("无熔断基线：%+.2f（最差日 %+.2f，%d 天）" % (base, worst, len(led)))
    rows_out = [("无熔断", base, 0, worst)]
    for thr in (18, 20, 22, 25, 28):
        for m in (10, 15, 20):
            tot = 0.0; fires = 0; worst_v = 0.0
            for day, rows in led.items():
                hits = misses = 0; net = 0.0; fired = None
                for r in rows:
                    net += float(r['net_delta'])
                    if r['result_type'] == 'hit': hits += 1
                    elif r['result_type'] == 'miss': misses += 1
                    if fired is None and net >= 20.0:
                        fired = 'target'  # 规则级日止盈±20仍生效
                        break
                    if fired is None and net <= -20.0:
                        fired = 'limit'
                        break
                    if fired is None and hits + misses >= m and 100.0*hits/(hits+misses) < thr:
                        fired = 'gate'
                        break
                if fired == 'gate':
                    tot += net; fires += 1
                else:
                    tot += net if fired is None else net
            # 上面遇到 target/limit 提前 break 时 net 已含该注
            rows_out.append(("熔断 %d%%/%d单" % (thr, m), tot, fires, 0))
    rows_out.sort(key=lambda x: -x[1])
    print("%-16s %10s %8s" % ("设置", "净利", "触发天"))
    for label, tot, fires, _ in rows_out:
        print("%-16s %+10.2f %8d" % (label, tot, fires))
    print()

# ---------- 规则7/8：触发重建模拟 ----------

SIG16 = defaultdict(dict)
for r in data['signals']:
    if r['source_id'] == '16' and r['bet_value'] and r['bet_type'] in ('big_small', 'odd_even'):
        SIG16[r['issue_no']][r['bet_type']] = r['bet_value']
SIG16_ISSUES = sorted(SIG16.keys(), key=lambda x: int(x))

def simulate_rule78(rid, metric, *, gate_thr=None, gate_min=50, day_target=0.0, day_limit=0.0,
                    run_target=10.0, run_limit=10.0, cooldown=10,
                    era_lo, era_hi, refund_era_end=None, payout=0.99):
    """连续模拟。stat_date = 下注自身的北京日期（近似，run 很少跨午夜）。"""
    st = 0
    run = None            # {'net':, 'day':}
    cooldown_until = ''
    days = defaultdict(lambda: {'hits': 0, 'misses': 0, 'refunds': 0, 'net': 0.0,
                                'bets': 0, 'stopped': '', 'first': '', 'last': ''})
    for iss in SIG16_ISSUES:
        d = draws_by_issue.get(iss)
        if not d:
            continue
        t = parse_dt(d['open_time'])
        day = t.strftime('%Y-%m-%d')
        if not (era_lo <= day <= era_hi):
            # 仍需更新streak以保持连续性
            v = SIG16[iss].get(metric)
            actual = d['big_small'] if metric == 'big_small' else d['odd_even']
            if v and actual:
                st = 0 if v == actual else st + 1
            continue
        dstats = days[day]
        bet_v = SIG16[iss].get(metric)
        # 1) 若run活跃：下注并结算
        if run is not None and bet_v:
            actual = d['big_small'] if metric == 'big_small' else d['odd_even']
            same = (bet_v == actual)
            rtype = 'miss'
            delta = -1.0
            if same:
                refund = False
                if rid == '7' and refund_era_end and day < refund_era_end:
                    num = int(d['result_number'])
                    trip = [int(x) for x in d['triplet'].split(',')] if d['triplet'] and len(d['triplet'].split(',')) == 3 else []
                    baozi = len(trip) == 3 and len(set(trip)) == 1
                    pair = (not baozi) and len(set(trip)) == 2
                    s = sorted(trip) if len(trip) == 3 else []
                    straight = (not baozi) and len(trip) == 3 and ((s[1] == s[0]+1 and s[2] == s[1]+1) or s in ([0,8,9],[0,1,9]))
                    refund = baozi or pair or straight or num in (13, 14, 0, 27)
                if refund:
                    rtype, delta = 'refund', 0.0
                else:
                    rtype, delta = 'hit', payout
            dstats['net'] += delta
            dstats['bets'] += 1
            dstats['last'] = iss
            run['net'] += delta
            if rtype == 'hit': dstats['hits'] += 1
            elif rtype == 'miss': dstats['misses'] += 1
            else: dstats['refunds'] += 1
            if run['net'] >= run_target or run['net'] <= -run_limit:
                run = None
                cooldown_until = iss
        # 2) 日级停止检查（规则级 barriers + 熔断）
        if dstats['stopped'] == '' and day_target and dstats['net'] >= day_target:
            dstats['stopped'] = 'profit_target'
            run = None
        if dstats['stopped'] == '' and day_limit and dstats['net'] <= -day_limit:
            dstats['stopped'] = 'loss_limit'
            run = None
        if dstats['stopped'] == '' and gate_thr is not None:
            settled = dstats['hits'] + dstats['misses']
            if settled >= gate_min and 100.0*dstats['hits']/settled < gate_thr:
                dstats['stopped'] = 'daily_hit_rate_low'
                run = None
        # 3) 更新streak
        v = SIG16[iss].get(metric)
        actual = d['big_small'] if metric == 'big_small' else d['odd_even']
        if v and actual:
            st = 0 if v == actual else st + 1
        # 4) 触发评估（下一期开跑）
        if run is None and dstats['stopped'] == '' and iss > cooldown_until and st >= 5:
            run = {'net': 0.0, 'day': day}
            if dstats['first'] == '':
                dstats['first'] = iss
        elif run is None and dstats['first'] == '' and not dstats['bets']:
            pass
    return days

def validate_and_grid(rid, metric, era_lo, era_hi, day_target, day_limit, thresholds, mins):
    name = {'7': '大小连错 5 次自动跟单', '8': '单双连错 5 次自动跟单 单轮止盈 1'}[rid]
    print("=" * 78)
    print("规则：%s（%s）—— 触发重建模拟，先开熔断(50%%/50)对账" % (name, metric))
    days = simulate_rule78(rid, metric, gate_thr=50.0, gate_min=50, day_target=day_target,
                           day_limit=day_limit, era_lo=era_lo, era_hi=era_hi)
    led = defaultdict(list)
    for r in data['ledger']:
        if r['rule_id'] == rid and era_lo <= r['stat_date'] <= era_hi:
            led[r['stat_date']].append(r)
    sim_bets = sum(v['bets'] for v in days.values())
    act_bets = sum(len(v) for v in led.values())
    sim_net = sum(v['net'] for v in days.values())
    act_net = sum(sum(float(x['net_delta']) for x in v) for v in led.values())
    print("对账：模拟 %d注 净%+.2f | 实际 %d注 净%+.2f （注数差 %.1f%%，净差 %+.2f）" % (
        sim_bets, sim_net, act_bets, act_net, 100*(sim_bets-act_bets)/max(1, act_bets), sim_net-act_net))
    for day in sorted(set(list(days.keys()) + list(led.keys()))):
        s = days.get(day, {'bets': 0, 'net': 0.0, 'stopped': ''})
        a = led.get(day, [])
        anet = sum(float(x['net_delta']) for x in a)
        print("  %s 模拟%3d注 %+7.2f (%s) | 实际%3d注 %+7.2f" % (
            day, s['bets'], s['net'], s['stopped'] or '-', len(a), anet))
    # 网格（关熔断）
    print("  —— 关熔断阈值网格（无截断模拟）——")
    base = simulate_rule78(rid, metric, era_lo=era_lo, era_hi=era_hi,
                           day_target=day_target, day_limit=day_limit)
    bnet = sum(v['net'] for v in base.values())
    bworst = min(v['net'] for v in base.values())
    bfired = sum(1 for v in base.values() if v['bets'] > 0)
    print("  无熔断基线：%+.2f（最差日 %+.2f，有下注 %d 天）" % (bnet, bworst, bfired))
    out = [("无熔断", bnet, bworst, 0)]
    for thr in thresholds:
        for m in mins:
            dd = simulate_rule78(rid, metric, gate_thr=thr, gate_min=m, era_lo=era_lo,
                                 era_hi=era_hi, day_target=day_target, day_limit=day_limit)
            net = sum(v['net'] for v in dd.values())
            worst = min(v['net'] for v in dd.values())
            fires = sum(1 for v in dd.values() if v['stopped'] == 'daily_hit_rate_low')
            out.append(("熔断 %d%%/%d单" % (thr, m), net, worst, fires))
    out.sort(key=lambda x: -x[1])
    for label, net, worst, fires in out:
        print("  %-16s %+10.2f（最差日 %+.2f，触发 %d 天）" % (label, net, worst, fires))
    print()

def validate_and_grid_8(era_lo, era_hi, day_target, day_limit):
    validate_and_grid('8', 'odd_even', era_lo, era_hi,
                      day_target=day_target, day_limit=day_limit,
                      thresholds=(44, 47, 50), mins=(20, 30, 50))


def main():
    rule6_pregate_grid()
    validate_and_grid('7', 'big_small', '2026-10-30', '2026-11-14',
                      day_target=0.0, day_limit=0.0,
                      thresholds=(45, 47, 50, 53), mins=(30, 50, 80))
    # 规则8 的日止盈3/日止损20 是 11-08 之后才出现的配置（11-04 实际78注+11.55未停）；
    # 分两段模拟：11-02→11-07 无日级barriers，11-08→11-14 有。
    validate_and_grid_8('2026-11-02', '2026-11-07', day_target=0.0, day_limit=0.0)
    validate_and_grid_8('2026-11-08', '2026-11-14', day_target=3.0, day_limit=20.0)

if __name__ == '__main__':
    main()
