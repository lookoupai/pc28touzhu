#!/usr/bin/env python3
"""汇总报告所需的最终数字：结构性EV、时期对比显著性、11月实际vs反事实总表。"""
import csv
import math
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
BJ = timezone(timedelta(hours=8))


def read_csv(name):
    with open(os.path.join(DATA, name), encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def mean(xs):
    return sum(xs) / len(xs) if xs else float("nan")


def stdev(xs):
    n = len(xs)
    if n < 2:
        return float("nan")
    m = mean(xs)
    return math.sqrt(sum((x - m) ** 2 for x in xs) / (n - 1))


def normal_sf(z):
    return 0.5 * math.erfc(z / math.sqrt(2.0))


draws = read_csv("draws.csv")
ledger = read_csv("ledger.csv")


def flags(t, num):
    ds = [int(x) for x in t.split(",")] if t and len(t.split(",")) == 3 else []
    f = {"special": num in (13, 14), "pair": False, "straight": False, "baozi": False}
    if len(ds) == 3:
        f["baozi"] = len(set(ds)) == 1
        f["pair"] = (not f["baozi"]) and len(set(ds)) == 2
        s = sorted(ds)
        if not f["baozi"]:
            f["straight"] = (s[1] == s[0] + 1 and s[2] == s[1] + 1) or s in ([0, 8, 9], [0, 1, 9])
    return f


print("=" * 70)
print("A. 结构性每注期望（用全部24513期开奖的频率，方向随机=50%命中）")
print("=" * 70)
n = len(draws)
# 单双 1.85 + 退本金（对子/顺子/豹子/13-14）
tot = win = refund = lose = 0
for r in draws:
    num = int(r["result_number"])
    f = flags(r["triplet"], num)
    cond = f["special"] or f["pair"] or f["straight"] or f["baozi"]
    for betv in ("单", "双"):
        if betv == r["odd_even"]:
            if cond:
                refund += 1
            else:
                win += 1
                tot += 1.85
        else:
            lose += 1
            tot -= 1.0
print("  单双 赔率1.85+四类退本金: 命中(实付)%.4f 退本=%.4f 未中=%.4f → EV=%+.4f/注 (%.2f%%)" % (
    win / (2 * n), refund / (2 * n), lose / (2 * n), tot / (2 * n), 100 * tot / (2 * n)))
# 单双 1.99 无退本金
tot2 = sum(0.99 if betv == r["odd_even"] else -1.0 for r in draws for betv in ("单", "双"))
print("  单双 赔率1.99 无退本金(规则8): EV=%+.4f/注" % (tot2 / (2 * n)))
# 大小 1.99 + 退本金（对子/顺子/豹子/13-14/0/27）
tot3 = win3 = refund3 = 0
for r in draws:
    num = int(r["result_number"])
    f = flags(r["triplet"], num)
    cond = f["special"] or f["pair"] or f["straight"] or f["baozi"] or num in (0, 27)
    for betv in ("大", "小"):
        if betv == r["big_small"]:
            if cond:
                refund3 += 1
            else:
                win3 += 1
                tot3 += 0.99
        else:
            tot3 -= 1.0
print("  大小 赔率1.99+五类退本金(规则7现行): EV=%+.4f/注 (实付命中%.4f 退本%.4f)" % (
    tot3 / (2 * n), win3 / (2 * n), refund3 / (2 * n)))
# 组合 2.71/3.32（规则6现行）；账本观察组合退本仅~1.6%，实际发生在 小单@13 / 大双@14
combo_pay = {"小单": 2.71, "大双": 2.71, "大单": 3.32, "小双": 3.32}
tot4 = win4 = refund4 = 0
for r in draws:
    num = int(r["result_number"])
    for betv, pay in combo_pay.items():
        cond = (betv == "小单" and num == 13) or (betv == "大双" and num == 14)
        if betv == r["combo"]:
            if cond:
                refund4 += 1
            else:
                win4 += 1
                tot4 += pay
        else:
            tot4 -= 1.0
print("  组合 赔率3.71/4.32+退本金(规则6现行): EV=%+.4f/注 (实付命中%.4f 退本%.4f)" % (
    tot4 / (4 * n), win4 / (4 * n), refund4 / (4 * n)))

print()
print("=" * 70)
print("B. 时期对比显著性（日均盈亏，Welch z近似）")
print("=" * 70)
by_rule_day = defaultdict(lambda: defaultdict(list))
for r in ledger:
    by_rule_day[r["rule_id"]][r["stat_date"]].append(float(r["net_delta"]))
names = {"4": "单双共识定时跟单", "5": "7+8+9单双多数票", "6": "组合连错11次", "7": "大小连错5次", "8": "单双连错5次"}
eras = [("10月上中(10/1-10/23)", "2026-10-01", "2026-10-23"),
        ("11月熔断期(10/31-11/14)", "2026-10-31", "2026-11-14")]
for rid in ("4", "5", "6"):
    stats = []
    for label, lo, hi in eras:
        days = [d for d in by_rule_day[rid] if lo <= d <= hi]
        vals = [sum(by_rule_day[rid][d]) for d in days]
        stats.append((label, mean(vals), stdev(vals), len(vals)))
    (l1, m1, s1, n1), (l2, m2, s2, n2) = stats
    se = math.sqrt(s1 * s1 / n1 + s2 * s2 / n2)
    z = (m2 - m1) / se if se else 0.0
    print("  规则%s %s: %s %+0.2f/天(n=%d) → %s %+0.2f/天(n=%d) | 差 %+.2f, z=%.2f, p=%.3f %s" % (
        rid, names[rid], l1, m1, n1, l2, m2, n2, m2 - m1, z, 2 * normal_sf(abs(z)),
        "显著" if 2 * normal_sf(abs(z)) < 0.05 else "不显著（运气范围内）"))

print()
print("=" * 70)
print("C. 11月熔断期：实际 vs 无熔断反事实 总表")
print("=" * 70)
rows45 = read_csv("gate_counterfactual_rules45.csv")
tot_a = tot_c = 0
for rid in ("4", "5"):
    rs = [r for r in rows45 if r["rule_id"] == rid]
    a = sum(float(r["actual_net"]) for r in rs)
    c = sum(float(r["cf_net"]) for r in rs)
    tot_a += a
    tot_c += c
    print("  规则%s %s: 实际 %+8.2f | 反事实 %+8.2f | 差 %+.2f (精确重放)" % (
        rid, names[rid], a, c, c - a))
# 规则6/7/8 来自 bootstrap（手工带入上面 PART3 输出）
boot = {"6": (49.35, 49.35 + 109.41), "7": (-25.40, -25.40 - 44.66), "8": (-3.48, -3.48 - 0.20)}
for rid, (a, c) in boot.items():
    tot_a += a
    tot_c += c
    print("  规则%s %s: 实际 %+8.2f | 反事实 %+8.2f | 差 %+.2f (bootstrap, CI宽)" % (
        rid, names[rid], a, c, c - a))
print("  五规则合计: 实际 %+8.2f | 反事实 %+8.2f | 差 %+.2f" % (tot_a, tot_c, tot_c - tot_a))
print("  注：规则6/7/8反事实为bootstrap估计（CI约±300），规则4/5为逐注确定性重放。")

print()
print("=" * 70)
print("D. 规则4/5 熔断日的具体走向（反事实明细）")
print("=" * 70)
for rid in ("4", "5"):
    rs = [r for r in rows45 if r["rule_id"] == rid and r["stop"] == "daily_hit_rate_low"]
    saved = [r for r in rs if float(r["delta"]) < 0]
    lost = [r for r in rs if float(r["delta"]) > 0]
    print("  规则%s: 熔断%d天，其中若无熔断会亏到±10(熔断避免亏损) %d天 合计%+.2f；" % (
        rid, len(rs), len(saved), sum(float(r["delta"]) for r in saved)))
    print("        会恢复到+10(熔断挡住盈利) %d天 合计%+.2f；小幅变化 %d天 合计%+.2f" % (
        len(lost), sum(float(r["delta"]) for r in lost),
        len(rs) - len(saved) - len(lost),
        sum(float(r["delta"]) for r in rs if r not in saved and r not in lost)))
