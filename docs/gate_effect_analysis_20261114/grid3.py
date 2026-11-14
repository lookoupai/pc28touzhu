#!/usr/bin/env python3
"""补充网格：
A. 规则6 组合连错 11 次自动跟单（遗漏回补）：9/22→10/23 完整样本外（32天，现行结算），
   熔断阈值 × min_settled × 日止盈止损 维度。
B. 规则7 大小连错 5 次自动跟单：模拟上的 日止盈止损 网格（当前为0=关闭），
   以及 4 个无熔断样本外日（10-19/20/23/24，1.84旧赔率）上的虚拟熔断表现。
"""
import sys, os, csv
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from analyze import load_all, parse_dt
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
data = load_all()

# ---------- A. 规则6 完整样本外网格 ----------

def rule6_full_grid():
    led = defaultdict(list)
    for r in data['ledger']:
        if r['rule_id'] == '6' and '2026-09-22' <= r['stat_date'] <= '2026-10-23':
            led[r['stat_date']].append(r)
    for k in led:
        led[k].sort(key=lambda r: (r['settled_at'], int(r['id'])))
    print("=" * 78)
    print("A. 规则6 组合连错 11 次自动跟单（遗漏回补）：样本外 9/22→10/23 共 %d 天（全部现行结算2.71/3.32，无熔断时期）" % len(led))

    def walk(rows, gate=None, day_barrier=None):
        """gate=(thr,m)；day_barrier=(target,limit)。决策只用实时信息。"""
        hits = misses = 0; net = 0.0
        for r in rows:
            net += float(r['net_delta'])
            if r['result_type'] == 'hit': hits += 1
            elif r['result_type'] == 'miss': misses += 1
            if day_barrier:
                tgt, lim = day_barrier
                if tgt and net >= tgt: return net, 'target'
                if lim and net <= -lim: return net, 'limit'
            if gate:
                thr, m = gate
                settled = hits + misses
                if settled >= m and 100.0*hits/settled < thr: return net, 'gate'
        return net, 'day_end'

    base = sum(sum(float(x['net_delta']) for x in rows) for rows in led.values())
    worst = min(sum(float(x['net_delta']) for x in rows) for rows in led.values())
    print("无熔断+日±20(现状结构)基线：%+.2f（最差日 %+.2f）" % (base, worst))
    out = []
    for label, gate, barrier in [
        ("无熔断+日±20(现状)", None, (20.0, 20.0)),
        ("熔断18%/15", (18.0, 15), (20.0, 20.0)),
        ("熔断20%/15", (20.0, 15), (20.0, 20.0)),
        ("熔断20%/20", (20.0, 20), (20.0, 20.0)),
        ("熔断22%/15", (22.0, 15), (20.0, 20.0)),
        ("熔断22%/20", (22.0, 20), (20.0, 20.0)),
        ("熔断25%/20(现状)", (25.0, 20), (20.0, 20.0)),
        ("熔断20%/15+日±10", (20.0, 15), (10.0, 10.0)),
        ("熔断22%/15+日±10", (22.0, 15), (10.0, 10.0)),
        ("无熔断+日±10", None, (10.0, 10.0)),
        ("无熔断+日±15", None, (15.0, 15.0)),
    ]:
        tot = 0.0; fires = 0; worst_v = 0.0; kinds = defaultdict(int)
        for day, rows in led.items():
            net, kind = walk(rows, gate=gate, day_barrier=barrier)
            tot += net
            worst_v = min(worst_v, net)
            fires += 1 if kind == 'gate' else 0
            kinds[kind] += 1
        out.append((label, tot, fires, worst_v, dict(kinds)))
    out.sort(key=lambda x: -x[1])
    print("%-22s %10s %8s %9s  %s" % ("设置", "净利", "熔断天", "最差日", "结束方式分布"))
    for label, tot, fires, worst_v, kinds in out:
        print("%-22s %+10.2f %8d %+9.2f  %s" % (label, tot, fires, worst_v, dict(kinds)))
    print()

# ---------- B. 规则7 日止盈止损网格（模拟） ----------

def rule7_day_barrier_grid():
    sys.path.insert(0, HERE)
    from grid2 import simulate_rule78
    print("=" * 78)
    print("B. 规则7 大小连错 5 次自动跟单：日止盈止损网格（当前规则级止盈止损=0关闭）")
    print("   模拟期 10-30→11-14（0.99现行结算；对账注数差+4.4%，净差+21，数值近似、方向可信）")
    out = []
    for label, gate, barrier in [
        ("无熔断+无日闸(裸奔)", None, None),
        ("无熔断+日±10", None, (10.0, 10.0)),
        ("无熔断+日±15", None, (15.0, 15.0)),
        ("无熔断+日±20", None, (20.0, 20.0)),
        ("无熔断+日±30", None, (30.0, 30.0)),
        ("熔断50%/50(现状)", (50.0, 50), None),
        ("熔断50%/50+日±10", (50.0, 50), (10.0, 10.0)),
        ("熔断50%/50+日±15", (50.0, 50), (15.0, 15.0)),
        ("熔断50%/50+日±20", (50.0, 50), (20.0, 20.0)),
        ("熔断47%/30+日±10", (47.0, 30), (10.0, 10.0)),
        ("熔断47%/30+日±20", (47.0, 30), (20.0, 20.0)),
    ]:
        dd = simulate_rule78('7', 'big_small', gate_thr=gate[0] if gate else None,
                             gate_min=gate[1] if gate else 50,
                             day_target=barrier[0] if barrier else 0.0,
                             day_limit=barrier[1] if barrier else 0.0,
                             era_lo='2026-10-30', era_hi='2026-11-14')
        net = sum(v['net'] for v in dd.values())
        worst = min(v['net'] for v in dd.values())
        best = max(v['net'] for v in dd.values())
        fires = sum(1 for v in dd.values() if v['stopped'] == 'daily_hit_rate_low')
        stops = sum(1 for v in dd.values() if v['stopped'] in ('profit_target', 'loss_limit'))
        bets = sum(v['bets'] for v in dd.values())
        out.append((label, net, worst, best, fires, stops, bets))
    out.sort(key=lambda x: -x[1])
    print("%-22s %10s %9s %9s %8s %8s %8s" % ("设置", "净利", "最差日", "最好日", "熔断天", "日闸天", "注数"))
    for label, net, worst, best, fires, stops, bets in out:
        print("%-22s %+10.2f %+9.2f %+9.2f %8d %8d %8d" % (label, net, worst, best, fires, stops, bets))
    print()

    # 规则7 的 4 个无熔断样本外日（1.84旧赔率）上的虚拟熔断
    print("规则7 无熔断样本外日（10-19/20/23/24，1.84旧赔率）虚拟熔断50%/50 vs 实际（实际=无熔断）:")
    led = defaultdict(list)
    for r in data['ledger']:
        if r['rule_id'] == '7' and r['stat_date'] in ('2026-10-19', '2026-10-20', '2026-10-23', '2026-10-24'):
            led[r['stat_date']].append(r)
    for k in led:
        led[k].sort(key=lambda r: (r['settled_at'], int(r['id'])))
    tb = ta = 0
    for day, rows in sorted(led.items()):
        hits = misses = 0; net = 0.0; fired = None
        for r in rows:
            net += float(r['net_delta'])
            if r['result_type'] == 'hit': hits += 1
            elif r['result_type'] == 'miss': misses += 1
            if fired is None and hits + misses >= 50 and 100.0*hits/(hits+misses) < 50:
                fired = net
        full = sum(float(x['net_delta']) for x in rows)
        tb += full
        ta += fired if fired is not None else full
        print("  %s 实际(无熔断) %+8.2f (%d注) | 虚拟熔断 %+8.2f (触发于%s)" % (
            day, full, len(rows), fired if fired is not None else full,
            "第%d结算" % (hits+misses) if fired is not None else "-"))
    print("  合计：无熔断 %+.2f → 虚拟熔断 %+.2f" % (tb, ta))

def main():
    rule6_full_grid()
    rule7_day_barrier_grid()

if __name__ == '__main__':
    main()
