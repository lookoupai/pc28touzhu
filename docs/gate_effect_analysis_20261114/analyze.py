#!/usr/bin/env python3
"""命中率熔断（daily hit-rate gate）效果分析 v2。

方法：
- PART1 结算模型验证：用“对子/顺子/豹子/13-14和值退本金 + 赔率1.85”模型复算规则4/5全部历史注，验证可复现。
- PART2 规则4/5 熔断反事实（核心）：熔断日实际账本只到触发点为止（被截断），
  从触发点起用信号流+开奖数据确定性重放“如果没有熔断会继续下的每注”，加上各自的止盈止损闸，
  得到无熔断反事实。非熔断日无截断，反事实=实际。
  校准：leave-future-out——把非熔断日的实际运行在中途截断，用同一重放引擎续跑，
  验证能精确复现剩余下注与盈亏。
- PART3 规则6/7/8：触发链路依赖远端 performance streaks，不做链路重建；
  用（a）每注经验期望值±置信区间、（b）bootstrap 从触发点状态续跑到日闸/样本上限，
  估计熔断截断的机会成本；前提（注结果可交换）由 PART4 的开奖检验支撑。
- PART4 开奖层面检验“坏日子”假设（日间同质性卡方、自相关、条件持续性、信号方向）。
- PART5 分时期盈亏汇总 + 熔断期 实际 vs 反事实 总表。
"""
import csv
import json
import math
import os
import random
from collections import defaultdict
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "data")
BJ = timezone(timedelta(hours=8))
random.seed(20261114)

RULES = {
    "4": {"name": "单双共识定时跟单", "source": "8", "threshold": 45.0, "min_settled": 20,
          "target": 10.0, "limit": 10.0, "metric": "odd_even", "payout": 1.85,
          "windows": ["08:01", "14:01", "20:01"], "gate_from": "2026-10-31",
          "barrier": "day", "restart": False},
    "5": {"name": "7+8+9单双多数票晚间测试", "source": "14", "threshold": 45.0, "min_settled": 20,
          "target": 0.0, "limit": 0.0, "metric": "odd_even", "payout": 1.85,
          "windows": ["08:01", "15:01", "20:01"], "gate_from": "2026-10-31",
          "barrier": "run", "run_target": 10.0, "run_limit": 10.0, "restart": True},
    "6": {"name": "组合连错11次自动跟单（遗漏回补）", "source": "7", "threshold": 25.0, "min_settled": 20,
          "target": 20.0, "limit": 20.0, "metric": "combo", "gate_from": "2026-10-24"},
    "7": {"name": "大小连错5次自动跟单", "source": "16", "threshold": 50.0, "min_settled": 50,
          "target": 0.0, "limit": 0.0, "metric": "big_small", "gate_from": "2026-10-25"},
    "8": {"name": "单双连错5次自动跟单 单轮止盈1", "source": "16", "threshold": 50.0, "min_settled": 50,
          "target": 3.0, "limit": 20.0, "metric": "odd_even", "gate_from": "2026-11-02"},
}


def parse_dt(s):
    if "T" in s:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    return datetime.strptime(s, "%Y-%m-%d %H:%M:%S").replace(tzinfo=BJ)


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


def median(xs):
    s = sorted(xs)
    return s[len(s) // 2] if s else float("nan")


def chi2_sf(x, df):
    if x <= 0:
        return 1.0
    a = df / 2.0
    x = x / 2.0
    if x < a + 1:
        term = 1.0 / a
        total = term
        n = 1
        while True:
            term *= x / (a + n)
            total += term
            if term < total * 1e-15 or n > 200000:
                break
            n += 1
        p = total * math.exp(-x + a * math.log(x) - math.lgamma(a))
        return max(0.0, min(1.0, 1.0 - p))
    tiny = 1e-300
    b = x + 1 - a
    c = 1.0 / tiny
    d = 1.0 / b
    h = d
    for i in range(1, 200000):
        an = -i * (i - a)
        b += 2.0
        d = an * d + b
        if abs(d) < tiny:
            d = tiny
        c = b + an / c
        if abs(c) < tiny:
            c = tiny
        d = 1.0 / d
        delta = d * c
        h *= delta
        if abs(delta - 1.0) < 1e-15:
            break
    q = math.exp(-x + a * math.log(x) - math.lgamma(a)) * h
    return max(0.0, min(1.0, q))


def normal_sf(z):
    return 0.5 * math.erfc(z / math.sqrt(2.0))


def draw_flags(triplet, result_number):
    digits = [int(d) for d in triplet.split(",") if d != ""] if triplet else []
    flags = {"special_sum": result_number in (13, 14), "pair": False, "straight": False,
             "baozi": False}
    if len(digits) == 3:
        flags["baozi"] = len(set(digits)) == 1
        flags["pair"] = (not flags["baozi"]) and len(set(digits)) == 2
        s = sorted(digits)
        if not flags["baozi"]:
            consec = (s[1] == s[0] + 1 and s[2] == s[1] + 1)
            wrap = (s == [0, 8, 9]) or (s == [0, 1, 9])
            flags["straight"] = consec or wrap
    return flags


def load_all():
    data = {
        "draws": read_csv("draws.csv"),
        "ledger": read_csv("ledger.csv"),
        "signals": read_csv("signals.csv"),
        "daily": read_csv("rule_daily_stats.csv"),
        "runs": read_csv("rule_runs.csv"),
    }
    return data


def build_draw_index(draws):
    idx = {}
    for r in draws:
        num = int(r["result_number"])
        idx[r["issue_no"]] = {
            "issue": r["issue_no"], "num": num, "odd": r["odd_even"], "big": r["big_small"],
            "open_time": parse_dt(r["open_time"]), "flags": draw_flags(r["triplet"], num),
        }
    return idx


def build_signal_index(signals):
    idx = defaultdict(dict)
    for r in signals:
        if r["bet_value"]:
            idx[r["source_id"]][r["issue_no"]] = r["bet_value"]
    return idx


def settle_odd_even(bet_value, draw, payout=1.85):
    """规则4/5结算模型：对子/顺子/豹子/13-14和值退本金。"""
    same = (bet_value == draw["odd"])
    if not same:
        return -1.0, "miss"
    f = draw["flags"]
    if f["special_sum"] or f["pair"] or f["straight"] or f["baozi"]:
        return 0.0, "refund"
    return payout, "hit"


# ---------------- PART 1 ----------------

def part1_validate(data, draws):
    print("=" * 72)
    print("PART 1 结算模型验证（规则4/5：单双 赔率1.85 + 四类退本金）")
    print("=" * 72)
    rows = [r for r in data["ledger"] if r["rule_id"] in ("4", "5")]
    confusion = defaultdict(int)
    bad = 0
    for r in rows:
        d = draws.get(r["issue_no"])
        if not d:
            bad += 1
            continue
        actual = r["result_type"]
        _, model = settle_odd_even(r["bet_value"], d)
        confusion[(actual, model)] += 1
        if actual != model:
            bad += 1
    n = sum(confusion.values())
    print("  样本 %d 条，模型不匹配 %d 条 (%.3f%%)" % (n, bad, 100.0 * bad / n))
    for k in sorted(confusion):
        print("   实际%-7s 模型%-7s %6d" % (k[0], k[1], confusion[k]))
    n_all = len(draws)
    odd = sum(1 for d in draws.values() if d["odd"] == "单")
    print("  全部开奖: 单占比 %.4f (n=%d, SE=%.4f) — 与0.5一致" % (odd / n_all, n_all, 0.5 / math.sqrt(n_all)))


# ---------------- PART 2 ----------------

def day_windows(cfg, day):
    return [datetime.strptime(day + " " + w, "%Y-%m-%d %H:%M").replace(tzinfo=BJ)
            for w in cfg["windows"]]


def tail_simulate(cfg, draws, sig_idx, start_issue, day, base_day_net, base_run_net, max_day):
    """从 start_issue 之后一期开始确定性重放无熔断的后续。

    cfg["barrier"]: "day" (规则4: 日±10, 停后不再开run) 或 "run" (规则5: run±10, 关闭后下个窗口重开)
    返回 (tail dict, issues_bet list)
    """
    issues = sorted(draws.values(), key=lambda d: d["open_time"])
    hi = datetime.strptime(day + " 23:59", "%Y-%m-%d %H:%M").replace(tzinfo=BJ)
    tail = {"bets": 0, "net": 0.0, "hits": 0, "misses": 0, "refunds": 0, "ends": [], "bet_list": []}
    state = {"run_net": base_run_net}  # 当前run净利（从run起点累计）
    consumed = set()
    cur_day = day
    fired_seen = False
    for d in issues:
        t = d["open_time"]
        if not fired_seen:
            if d["issue"] == start_issue:
                fired_seen = True
            continue
        if t > hi or t.strftime("%Y-%m-%d") != day:
            if t.strftime("%Y-%m-%d") > day:
                break
            continue
        bday = t.strftime("%Y-%m-%d")
        if bday != cur_day:
            cur_day = bday
            consumed = set()
        # 窗口开新run
        for wi, wstart in enumerate(day_windows(cfg, bday)):
            key = (bday, wi)
            if key in consumed:
                continue
            if state is None and t >= wstart:
                state = {"run_net": 0.0}
                consumed.add(key)
                break
        bet = sig_idx.get(d["issue"])
        if state is None or bet is None:
            continue
        delta, rtype = settle_odd_even(bet, d, cfg["payout"])
        tail["bet_list"].append({"issue": d["issue"], "rtype": rtype, "delta": delta,
                                 "settled_at": t.strftime("%Y-%m-%dT%H:%M:%S+08:00")})
        tail["bets"] += 1
        tail["net"] += delta
        tail[rtype + "s" if rtype != "miss" else "misses"] += 1
        state["run_net"] += delta
        closed = False
        if cfg["barrier"] == "run":
            if state["run_net"] >= cfg["run_target"]:
                closed = True
                tail["ends"].append("run_target@%s" % t.strftime("%H:%M"))
            if state["run_net"] <= -cfg["run_limit"]:
                closed = True
                tail["ends"].append("run_loss@%s" % t.strftime("%H:%M"))
        else:  # day barrier：日闸是粘性的，触达即当天彻底结束
            if cfg["target"] and (base_day_net + tail["net"]) >= cfg["target"]:
                tail["ends"].append("day_target@%s" % t.strftime("%H:%M"))
                break
            if cfg["limit"] and (base_day_net + tail["net"]) <= -cfg["limit"]:
                tail["ends"].append("day_loss@%s" % t.strftime("%H:%M"))
                break
        if closed:
            state = None
        if tail["bets"] >= max_day:
            tail["ends"].append("cap")
            break
    return tail


def replay_day_to_fire(rows, cfg, fire_issue):
    """重放实际账本到触发点：返回 fire 状态。"""
    hits = misses = refunds = 0
    net = 0.0
    last = None
    for r in rows:
        net += float(r["net_delta"])
        if r["result_type"] == "hit":
            hits += 1
        elif r["result_type"] == "miss":
            misses += 1
        else:
            refunds += 1
        if fire_issue and r["issue_no"] == fire_issue:
            break
    return {"net": net, "hits": hits, "misses": misses, "refunds": refunds}


def part2_rules45(data, draws):
    print()
    print("=" * 72)
    print("PART 2 规则4/5：熔断日反事实（实际账本到触发点 + 触发点后确定性重放）")
    print("=" * 72)
    sig_idx = build_signal_index(data["signals"])
    ledger = defaultdict(list)
    for r in data["ledger"]:
        if r["rule_id"] in ("4", "5"):
            ledger[(r["rule_id"], r["stat_date"])].append(r)
    for k in ledger:
        ledger[k].sort(key=lambda r: (r["settled_at"], int(r["id"])))
    daily = {(r["rule_id"], r["stat_date"]): r for r in data["daily"]}
    runs = defaultdict(list)
    for r in data["runs"]:
        if r["rule_id"] in ("4", "5"):
            runs[(r["rule_id"], r["stat_date"])].append(r)

    ERA_START, ERA_END = "2026-10-31", "2026-11-14"
    MAX_DAY = 420  # 反事实尾部上限（约一整天期数）

    # ---- 校准1：leave-future-out（非熔断日截断续跑应精确复现） ----
    print("  [校准] leave-future-out：非熔断日在第10注截断后重放，对比实际剩余")
    for rid in ("4", "5"):
        for day in sorted(set(k[1] for k in ledger)):
            if not (ERA_START <= day <= ERA_END):
                continue
            ds = daily.get((rid, day), {})
            if ds.get("stopped_reason") == "daily_hit_rate_low":
                continue
            rows = ledger[(rid, day)]
            if len(rows) < 16:
                continue
            cut = rows[9]
            # 截断时状态
            pre = replay_day_to_fire(rows, RULES[rid], cut["issue_no"])
            cfg = RULES[rid]
            run_net = pre["net"]  # 单run日（rule4）或近似（rule5非熔断日run内）
            tail = tail_simulate(cfg, draws, sig_idx[cfg["source"]], cut["issue_no"], day, pre["net"], run_net, MAX_DAY)
            actual_rest = [r["issue_no"] for r in rows[10:]]
            sim_issues_count = tail["bets"]
            actual_rest_net = sum(float(r["net_delta"]) for r in rows[10:])
            ok = "OK" if abs(tail["net"] - actual_rest_net) < 0.02 and sim_issues_count == len(actual_rest) else "MISMATCH"
            print("    %s %s 截断@10注: 模拟剩余 %d注 净%+.2f | 实际剩余 %d注 净%+.2f → %s" % (
                rid, day, sim_issues_count, tail["net"], len(actual_rest), actual_rest_net, ok))

    # ---- 反事实 ----
    print()
    summary_rows = []
    for rid in ("4", "5"):
        cfg = RULES[rid]
        print("--- 规则%s %s ---" % (rid, cfg["name"]))
        days = sorted(set(k[1] for k in ledger) if False else
                      [k[1] for k in ledger if k[0] == rid and ERA_START <= k[1] <= ERA_END])
        tot_actual = tot_cf = 0.0
        gate_deltas = []
        for day in days:
            rows = ledger[(rid, day)]
            ds = daily.get((rid, day), {})
            stop = ds.get("stopped_reason", "")
            a_net = sum(float(r["net_delta"]) for r in rows)
            a_bets = len(rows)
            if stop != "daily_hit_rate_low":
                cf_net, cf_bets = a_net, a_bets
                note = ""
            else:
                fire_issue = rows[-1]["issue_no"]
                pre = replay_day_to_fire(rows, cfg, fire_issue)
                # 触发点所在run的run净利（rule5）
                if cfg["barrier"] == "run":
                    run_net = None
                    for rr in runs[(rid, day)]:
                        st = parse_dt(rr["started_at"])
                        sp = parse_dt(rr["stopped_at"]) if rr["stopped_at"] else None
                        ft = parse_dt(rows[-1]["settled_at"])
                        if st <= ft and (sp is None or ft <= sp + timedelta(minutes=2)):
                            run_rows = [r for r in rows
                                        if st - timedelta(minutes=2) <= parse_dt(r["settled_at"]) <= ft]
                            run_net = sum(float(r["net_delta"]) for r in run_rows)
                            break
                    if run_net is None:
                        run_net = pre["net"]
                else:
                    run_net = pre["net"]
                tail = tail_simulate(cfg, draws, sig_idx[cfg["source"]], fire_issue, day, pre["net"], run_net, MAX_DAY)
                cf_net = pre["net"] + tail["net"]
                cf_bets = a_bets + tail["bets"]
                note = "| 触发点净%+.2f(%d注) → 续 %d注 %s 反事实%+.2f" % (
                    pre["net"], pre["hits"] + pre["misses"], tail["bets"],
                    ",".join(tail["ends"][:2]), cf_net)
            delta = cf_net - a_net
            tot_actual += a_net
            tot_cf += cf_net
            if stop == "daily_hit_rate_low":
                gate_deltas.append(delta)
            print("  %s 实际 %2d注 %+7.2f (%s) → 反事实 %3d注 %+7.2f 差额%+7.2f %s" % (
                day, a_bets, a_net, stop or "未停", cf_bets, cf_net, delta, note))
            summary_rows.append({"rule_id": rid, "day": day, "actual_bets": a_bets,
                                 "actual_net": round(a_net, 2), "stop": stop,
                                 "cf_bets": cf_bets, "cf_net": round(cf_net, 2),
                                 "delta": round(delta, 2)})
        print("  合计: 实际 %+7.2f | 反事实(无熔断) %+7.2f | 差额(反事实-实际) %+7.2f" % (
            tot_actual, tot_cf, tot_cf - tot_actual))
        if gate_deltas:
            print("  熔断日差额 mean %+.2f median %+.2f（正=无熔断会赚更多→熔断亏了; 负=无熔断会亏更多→熔断省了）" % (
                mean(gate_deltas), median(gate_deltas)))
        print()
    with open(os.path.join(DATA, "gate_counterfactual_rules45.csv"), "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=["rule_id", "day", "actual_bets", "actual_net", "stop",
                                           "cf_bets", "cf_net", "delta"])
        w.writeheader()
        w.writerows(summary_rows)
    return summary_rows


# ---------------- PART 3 ----------------

def outcome_pool(rows):
    """按结果类型汇总每注收益分布（用于bootstrap）。"""
    pool = []
    for r in rows:
        pool.append(float(r["net_delta"]))
    return pool


def part3_bootstrap(data, draws):
    print("=" * 72)
    print("PART 3 规则6/7/8：熔断截断的机会成本（经验期望 + bootstrap）")
    print("=" * 72)
    ledger = defaultdict(list)
    for r in data["ledger"]:
        if r["rule_id"] in ("4", "5", "6", "7", "8"):
            ledger[r["rule_id"]].append(r)
    daily = defaultdict(dict)
    for r in data["daily"]:
        daily[r["rule_id"]][r["stat_date"]] = r

    # (gate_from=熔断实际启用日, pool_from=当前赔率时期起点, len_until=延续长度样本期上限)
    era_bounds = {"6": ("2026-10-24", "2026-10-24", "2026-10-23"),
                  "7": ("2026-10-25", "2026-10-30", "2026-10-29"),
                  "8": ("2026-11-02", "2026-11-02", None)}
    for rid in ("6", "7", "8"):
        cfg = RULES[rid]
        rows = ledger[rid]
        gate_from, pool_from, len_until = era_bounds[rid]
        era_rows = [r for r in rows if r["stat_date"] >= gate_from]
        # bootstrap 收益池用当前赔率时期（规则7在10-30赔率从2.84切到1.99）；
        # 延续长度分布来自无熔断时期（延续长度对赔率不敏感，见报告说明）。
        hist_pool = [r for r in rows if r["stat_date"] >= pool_from]
        len_pool = [r for r in rows if len_until is None or r["stat_date"] <= len_until]
        if not len_pool:
            len_pool = era_rows
        if not hist_pool:
            hist_pool = era_rows
        outcomes = outcome_pool(hist_pool)
        ev = mean(outcomes)
        sd = stdev(outcomes)
        n = len(outcomes)
        se = sd / math.sqrt(n) if n else float("nan")
        print()
        print("--- 规则%s %s (gate %.0f%%/%d, 日闸 %s/%s) ---" % (
            rid, cfg["name"], cfg["threshold"], cfg["min_settled"], cfg["target"], cfg["limit"]))
        print("  历史每注期望: %+0.4f (SD %.3f, n=%d, SE %.4f, 95%%CI [%+.4f, %+.4f])" % (
            ev, sd, n, se, ev - 1.96 * se, ev + 1.96 * se))
        # 熔断日触发点状态
        fired = []
        for day in sorted(daily[rid]):
            if day < gate_from:
                continue
            ds = daily[rid][day]
            if ds.get("stopped_reason") != "daily_hit_rate_low":
                continue
            day_rows = sorted([r for r in era_rows if r["stat_date"] == day],
                              key=lambda r: (r["settled_at"], int(r["id"])))
            if not day_rows:
                continue
            net = sum(float(r["net_delta"]) for r in day_rows)
            hits = sum(1 for r in day_rows if r["result_type"] == "hit")
            misses = sum(1 for r in day_rows if r["result_type"] == "miss")
            fired.append({"day": day, "net": net, "settled": hits + misses, "bets": len(day_rows)})
        print("  熔断触发 %d 天, 触发时净利 mean %+.2f (合计 %+.2f)" % (
            len(fired), mean([f["net"] for f in fired]) if fired else 0,
            sum(f["net"] for f in fired)))
        # 历史延续长度：日内从“settled>=min_settled”起至当日结束的注数（无熔断时期）
        by_day = defaultdict(list)
        for r in len_pool:
            by_day[r["stat_date"]].append(r)
        conts = []
        for day, drows in by_day.items():
            drows.sort(key=lambda r: (r["settled_at"], int(r["id"])))
            hits = misses = 0
            start_idx = None
            for i, r in enumerate(drows):
                if r["result_type"] == "hit":
                    hits += 1
                elif r["result_type"] == "miss":
                    misses += 1
                if start_idx is None and hits + misses >= cfg["min_settled"]:
                    start_idx = i
            if start_idx is not None:
                conts.append(len(drows) - start_idx)
        if conts:
            print("  历史同状态后续注数: mean %.0f median %.0f p90 %d (n=%d天)" % (
                mean(conts), median(conts), sorted(conts)[int(0.9 * (len(conts) - 1))], len(conts)))
        # bootstrap：从每个触发点状态续跑
        k_choices = conts if conts else [50]
        results = []
        for f in fired:
            rests = []
            for _ in range(2000):
                k = random.choice(k_choices)
                net = f["net"]
                cnt = 0
                for _i in range(k):
                    net += random.choice(outcomes)
                    cnt += 1
                    if cfg["target"] and net >= cfg["target"]:
                        break
                    if cfg["limit"] and net <= -cfg["limit"]:
                        break
                rests.append(net - f["net"])
            results.append({"day": f["day"], "net_at_fire": f["net"], "rest_mean": mean(rests),
                            "rest_p10": sorted(rests)[200], "rest_p90": sorted(rests)[1800]})
        if results:
            tot = sum(r["rest_mean"] for r in results)
            lo = sum(r["rest_p10"] for r in results)
            hi = sum(r["rest_p90"] for r in results)
            for r in results:
                print("    %s 触发时%+7.2f → 若继续: %+7.2f [p10 %+7.2f, p90 %+7.2f]" % (
                    r["day"], r["net_at_fire"], r["rest_mean"], r["rest_p10"], r["rest_p90"]))
            print("  合计若继续(无熔断): %+7.2f [%+.2f, %+.2f] → 熔断效果 = 实际-反事实 = %+7.2f [%+.2f, %+.2f]" % (
                tot, lo, hi, -tot, -hi, -lo))
            print("  （正=熔断避免了亏损；负=熔断挡住了本会赚到的钱）")


# ---------------- PART 4 ----------------

def part4_draw_tests(data, draws):
    print()
    print("=" * 72)
    print("PART 4 “坏日子”检验：开奖日间是否真有差异（这是熔断能否“预知坏日子”的前提）")
    print("=" * 72)
    by_day = defaultdict(lambda: {"n": 0, "odd": 0, "big": 0})
    for d in draws.values():
        day = d["open_time"].strftime("%Y-%m-%d")
        b = by_day[day]
        b["n"] += 1
        if d["odd"] == "单":
            b["odd"] += 1
        if d["big"] == "大":
            b["big"] += 1
    days = sorted(by_day)
    print("  覆盖 %d 天，每日 %d 期" % (len(days), mean([by_day[d]["n"] for d in days])))

    def homogeneity(key, label):
        chi2 = sum((by_day[d][key] - by_day[d]["n"] / 2) ** 2 / (by_day[d]["n"] * 0.25) for d in days)
        df = len(days) - 1
        p = chi2_sf(chi2, df)
        print("  %s: 卡方=%.1f df=%d p=%.3f → %s" % (label, chi2, df, p,
              "有日间差异" if p < 0.05 else "与“每天公平抛硬币”一致"))
        return [by_day[d][key] / by_day[d]["n"] for d in days]

    odd_rates = homogeneity("odd", "单/双")
    homogeneity("big", "大/小")
    n = len(odd_rates)
    m = mean(odd_rates)
    c0 = sum((x - m) ** 2 for x in odd_rates) / n
    for lag in (1, 2, 3):
        c = sum((odd_rates[i] - m) * (odd_rates[i + lag] - m) for i in range(n - lag)) / n
        r = c / c0
        p = 2 * normal_sf(abs(r) * math.sqrt(n))
        print("  单占比日自相关 lag%d = %+.3f (p=%.3f)" % (lag, r, p))
    lows = [i for i, r in enumerate(odd_rates[:-1]) if r < 0.47]
    if lows:
        print("  单占比<47%%的 %d 天，次日均值 %.4f vs 全体 %.4f — 差 %+.3f (无持续坏日子)" % (
            len(lows), mean([odd_rates[i + 1] for i in lows]), m,
            mean([odd_rates[i + 1] for i in lows]) - m))
    # 信号方向质量
    sig_idx = build_signal_index(data["signals"])
    for src, label in (("8", "源8 单双共识(规则4)"), ("14", "源14 7+8+9多数票(规则5)"),
                       ("16", "源16 全特征共识(规则7/8)")):
        stats = defaultdict(lambda: [0, 0])
        for issue, val in sig_idx[src].items():
            d = draws.get(issue)
            if not d:
                continue
            if len(val) != 1:
                continue
            day = d["open_time"].strftime("%Y-%m-%d")
            stats[day][0] += 1
            if (val == d["odd"]) or (val == d["big"]):
                stats[day][1] += 1
        ds = [d for d in sorted(stats) if stats[d][0] >= 50]
        tot_n = sum(stats[d][0] for d in ds)
        tot_h = sum(stats[d][1] for d in ds)
        if tot_n == 0:
            print("  %s: 无可用单注信号（该源可能只发组合/号码信号）" % label)
            continue
        chi2 = sum((stats[d][1] - stats[d][0] / 2) ** 2 / (stats[d][0] * 0.25) for d in ds)
        p = chi2_sf(chi2, len(ds) - 1)
        z = (tot_h - tot_n / 2) / math.sqrt(tot_n * 0.25)
        rates = sorted(stats[d][1] / stats[d][0] for d in ds)
        print("  %s: 总方向命中率 %.4f (n=%d, vs50%% z=%.2f p=%.3f) 日间卡方p=%.3f 日命中率[%0.3f,%0.3f]" % (
            label, tot_h / tot_n, tot_n, z, 2 * normal_sf(abs(z)), p, rates[0], rates[-1]))


# ---------------- PART 5 ----------------

def part5_eras(data):
    print()
    print("=" * 72)
    print("PART 5 分时期盈亏与每注期望")
    print("=" * 72)
    by_rule = defaultdict(list)
    for r in data["ledger"]:
        by_rule[r["rule_id"]].append(r)
    periods = [
        ("9月下半 (9/15-9/30)", "2026-09-15", "2026-09-30"),
        ("10月上中 (10/1-10/23)", "2026-10-01", "2026-10-23"),
        ("10月下 (10/24-10/30)", "2026-10-24", "2026-10-30"),
        ("11月熔断期 (10/31-11/14)", "2026-10-31", "2026-11-14"),
    ]
    for rid in ("4", "5", "6", "7", "8"):
        rows = by_rule.get(rid, [])
        print("  规则%s %s" % (rid, RULES[rid]["name"]))
        for label, lo, hi in periods:
            rs = [r for r in rows if lo <= r["stat_date"] <= hi]
            if not rs:
                continue
            net = sum(float(r["net_delta"]) for r in rs)
            ndays = len(set(r["stat_date"] for r in rs))
            day_nets = [sum(float(x["net_delta"]) for x in rs if x["stat_date"] == d)
                        for d in set(r["stat_date"] for r in rs)]
            print("    %s: %5d注 净%+9.2f 日均%+7.2f 日SD%6.2f 每注%+.4f" % (
                label, len(rs), net, net / ndays, stdev(day_nets), net / len(rs)))


def main():
    data = load_all()
    draws = build_draw_index(data["draws"])
    part1_validate(data, draws)
    part2_rules45(data, draws)
    part3_bootstrap(data, draws)
    part4_draw_tests(data, draws)
    part5_eras(data)


if __name__ == "__main__":
    main()
