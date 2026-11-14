#!/usr/bin/env python3
"""熔断参数网格：各规则在不同 阈值×最小单数 下的表现。

原则：
- 所有变体的决策只用实时可得信息（已结算数、当前净利）——无未来函数。
- 在“无截断注流”上回放：熔断期触发点的后续用信号重放补全（已校准）。
- 分时期评估（9-10月旧配置样本外 / 11月新配置），看参数排序是否稳定，
  避免按单月盈亏挑参数（那是样本内拟合）。
"""
import sys, os, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from analyze import (load_all, build_draw_index, build_signal_index, parse_dt,
                     replay_day_to_fire, tail_simulate, settle_odd_even, RULES,
                     mean, stdev, median)
from collections import defaultdict
from datetime import timedelta

DATA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")


def build_streams_rule45(data, draws, sig, rid):
    """该规则每个统计日的无截断注流: day -> [(issue, rtype, delta)]（结算顺序）。"""
    cfg = RULES[rid]
    ledger = defaultdict(list)
    for r in data['ledger']:
        if r['rule_id'] == rid:
            ledger[r['stat_date']].append(r)
    for k in ledger:
        ledger[k].sort(key=lambda r: (r['settled_at'], int(r['id'])))
    daily = {(r['rule_id'], r['stat_date']): r for r in data['daily']}
    streams = {}
    hybrid_days = 0
    for day, rows in sorted(ledger.items()):
        ds = daily.get((rid, day), {})
        stop = ds.get('stopped_reason', '')
        stream = [(r['issue_no'], r['result_type'], float(r['net_delta'])) for r in rows]
        if stop == 'daily_hit_rate_low' and day >= cfg['gate_from']:
            fire_issue = rows[-1]['issue_no']
            pre = replay_day_to_fire(rows, cfg, fire_issue)
            if cfg['barrier'] == 'run':
                run_net = pre['net']
                ft = parse_dt(rows[-1]['settled_at'])
                for rr in data['runs']:
                    if rr['rule_id'] != rid or rr['stat_date'] != day:
                        continue
                    st = parse_dt(rr['started_at'])
                    sp = parse_dt(rr['stopped_at']) if rr['stopped_at'] else None
                    if st <= ft and (sp is None or ft <= sp + timedelta(minutes=2)):
                        run_net = sum(float(x['net_delta']) for x in rows
                                      if st - timedelta(minutes=2) <= parse_dt(x['settled_at']) <= ft)
                        break
            else:
                run_net = pre['net']
            tail = tail_simulate(cfg, draws, sig[cfg['source']], fire_issue, day,
                                 pre['net'], run_net, 420)
            stream = stream + [(b['issue'], b['rtype'], b['delta']) for b in tail['bet_list']]
            hybrid_days += 1
        streams[day] = stream
    return streams, hybrid_days


def walk_gate(stream, thr=None, min_settled=None, day_limit=None, rule_barriers=None):
    """在无截断注流上回放一个设置。返回 (day_net, fire_info, stop_kind)。

    rule_barriers: (target, limit) 规则级日止盈止损（如 单双共识定时跟单 的 ±10）。
    gate 决策只用 hits/misses（实时可知）；日止损决策只用 net（实时可知）。
    """
    hits = misses = 0
    net = 0.0
    fire = None
    stop_kind = ''
    for issue, rtype, delta in stream:
        net += delta
        if rtype == 'hit':
            hits += 1
        elif rtype == 'miss':
            misses += 1
        if rule_barriers:
            tgt, lim = rule_barriers
            if tgt and net >= tgt:
                return net, None, 'profit_target'
            if lim and net <= -lim:
                return net, None, 'loss_limit'
        if day_limit is not None and net <= -day_limit:
            return net, None, 'day_limit'
        if thr is not None:
            settled = hits + misses
            if settled >= min_settled and 100.0 * hits / settled < thr:
                return net, (settled, round(100.0 * hits / settled, 1)), 'gate'
    return net, None, 'day_end'


def evaluate_rule45(name, rid, thresholds, mins, extra_variants=None, rule_barriers=None):
    data = load_all()
    draws = build_draw_index(data['draws'])
    sig = build_signal_index(data['signals'])
    streams, hybrid = build_streams_rule45(data, draws, sig, rid)
    eraA = {d: s for d, s in streams.items() if '2026-09-15' <= d <= '2026-10-29'}
    eraB = {d: s for d, s in streams.items() if '2026-11-01' <= d <= '2026-11-14'}
    print("=" * 78)
    print("规则：%s（基础命中率(除退本) 全期 %.1f%%；无截断日流 %d 天：样本外A %d 天 + 11月 %d 天，其中重放补全 %d 天）" % (
        name, 100 * 0.353 if rid == '4' else 34.8, len(streams), len(eraA), len(eraB), hybrid))
    baseA = sum(sum(x[2] for x in s) for s in eraA.values())
    baseB = sum(sum(x[2] for x in s) for s in eraB.values())
    worstA = min(sum(x[2] for x in s) for s in eraA.values())
    worstB = min(sum(x[2] for x in s) for s in eraB.values())
    print("无熔断基线：A %+.2f | B %+.2f | 合计 %+.2f（最差日 A %+.2f / B %+.2f）" % (
        baseA, baseB, baseA + baseB, worstA, worstB))
    rows = [("无熔断", baseA, baseB, 0, 0, worstA, worstB)]
    variants = []
    for thr in thresholds:
        for m in mins:
            variants.append(("熔断 %.0f%%/%d单" % (thr, m),
                             dict(thr=thr, min_settled=m, rule_barriers=rule_barriers)))
    if extra_variants:
        variants.extend(extra_variants)
    for label, kw in variants:
        netsA = []; netsB = []; firesA = firesB = 0
        for era, store in ((0, eraA), (1, eraB)):
            for day, s in store.items():
                net, fire, kind = walk_gate(s, **kw)
                (netsA if era == 0 else netsB).append(net)
                if kind == 'gate':
                    if era == 0: firesA += 1
                    else: firesB += 1
        sumA, sumB = sum(netsA), sum(netsB)
        rows.append((label, sumA, sumB, firesA, firesB, min(netsA), min(netsB)))
    rows.sort(key=lambda x: -(x[1] + x[2]))
    print("%-16s %9s %9s %9s %6s %6s %8s %8s" % ("设置", "A(9-10月)", "B(11月)", "合计", "触发A", "触发B", "A最差日", "B最差日"))
    for label, a, b, fa, fb, wa, wb in rows:
        print("%-16s %+9.2f %+9.2f %+9.2f %6d %6d %+8.2f %+8.2f" % (label, a, b, a + b, fa, fb, wa, wb))
    print()
    return rows


def main():
    # ---- 规则4：单双共识定时跟单（有日±10；基础命中率35.3%）----
    evaluate_rule45("单双共识定时跟单", '4',
                    thresholds=[30, 33, 35, 38, 40, 43, 45, 50],
                    mins=[20, 30, 40],
                    rule_barriers=(10.0, 10.0))
    # ---- 规则5：7+8+9单双多数票晚间测试（无日级止盈止损；基础命中率34.8%）----
    evaluate_rule45("7+8+9单双多数票晚间测试", '5',
                    thresholds=[30, 33, 35, 38, 40, 43, 45, 50],
                    mins=[20, 30, 40],
                    extra_variants=[
                        ("日止损-10(无熔断)", dict(day_limit=10.0)),
                        ("熔断45/20+日止损-10", dict(thr=45.0, min_settled=20, day_limit=10.0)),
                    ])


if __name__ == "__main__":
    main()
