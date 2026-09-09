"""只读回放 PC28 历史数据，比较每月 100 本金、每注 1 的模拟结果。"""
from __future__ import annotations

import ast
import csv
import heapq
import itertools
import json
import random
import sqlite3
import statistics
import sys
from collections import Counter, defaultdict, deque
from contextlib import closing
from datetime import datetime, timedelta, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
UPSTREAM = ROOT.parent / "AITradingSimulator"
OUT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from pc28touzhu.domain.pc28_profit_rules import resolve_pc28_hit_profit
from pc28touzhu.domain.settlement_rules import (
    derive_pc28_draw_snapshot,
    resolve_pc28_result_for_signal,
)


CN = timezone(timedelta(hours=8))
AS_OF = datetime(2026, 9, 7, 15, 57, 44, tzinfo=timezone.utc)
VALUES = {
    "big_small": ("大", "小"),
    "odd_even": ("单", "双"),
    "combo": ("大单", "大双", "小单", "小双"),
}
RULES = ("pc28_high_regular", "pc28_netdisk_regular")
POLICIES = {
    "bankroll_only": (None, None),
    "first_profit_limit20": (1, 2000),
    "first_profit_limit99": (1, 9900),
    "target1_limit20": (100, 2000),
    "target5_limit20": (500, 2000),
    "target10_limit20": (1000, 2000),
    "target1_limit80": (100, 8000),
}


def number_odds():
    """只解析上游已有赔率常量，避免导入服务层产生运行时副作用。"""
    tree = ast.parse((UPSTREAM / "services" / "profit_simulator.py").read_text(encoding="utf-8"))
    names = {"HIGH_NUMBER_ODDS": RULES[0], "NETDISK_NUMBER_ODDS": RULES[1]}
    result = {}
    for node in tree.body:
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id in names:
                    result[names[target.id]] = ast.literal_eval(node.value)
    if set(result) != set(RULES):
        raise ValueError("无法从上游读取完整数字赔率表")
    return result


NUMBER_ODDS = number_odds()


def parse_time(value, naive_zone=timezone.utc):
    if not value:
        return None
    result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return result.replace(tzinfo=naive_zone) if result.tzinfo is None else result


def connect(path):
    connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only=ON")
    connection.execute("BEGIN")
    return connection


def write_csv(name, rows):
    if not rows:
        return
    with (OUT / name).open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def net_cents(metric, value, rule, draw):
    if metric == "number":
        total = draw.get("sum_value")
        if total is None:
            total = sum(draw["triplet"])
        return int(round((NUMBER_ODDS[rule][int(value)] - 1) * 100)) if int(value) == total else -100
    result = resolve_pc28_result_for_signal(
        signal={"bet_type": metric, "bet_value": value},
        settlement_rule_id=rule,
        draw_context=draw,
    )["result_type"]
    if result == "miss":
        return -100
    if result == "refund":
        return 0
    amount = resolve_pc28_hit_profit(
        stake_amount=1,
        bet_type=metric,
        bet_value=value,
        profit_rule_id="pc28_high" if rule == RULES[0] else "pc28_netdisk",
        odds_profile="regular",
    )
    return int(round(amount * 100))


def simulate(bets, target=None, loss_limit=None):
    """金额使用整数分；逐注结算，在下一注可能突破本金/止损前停手。"""
    pnl = peak = lowest = drawdown = count = wins = refunds = losses = 0
    reason = "sample_end" if bets else "no_signal"
    final_issue = ""
    for issue, amount in bets:
        if target is not None and pnl >= target:
            reason = "profit_target"
            break
        if 10000 + pnl < 100:
            reason = "bankrupt" if pnl <= -10000 else "insufficient_for_one_unit"
            break
        if loss_limit is not None and pnl - 100 < -loss_limit:
            reason = "loss_limit"
            break
        pnl += amount
        count += 1
        final_issue = issue
        wins += amount > 0
        refunds += amount == 0
        losses += amount < 0
        peak = max(peak, pnl)
        lowest = min(lowest, pnl)
        drawdown = max(drawdown, peak - pnl)
    if target is not None and pnl >= target:
        reason = "profit_target"
    elif pnl <= -10000:
        reason = "bankrupt"
    elif 10000 + pnl < 100:
        reason = "insufficient_for_one_unit"
    elif loss_limit is not None and pnl - 100 < -loss_limit:
        reason = "loss_limit"
    return {
        "bets": count, "net": pnl / 100, "ending_bankroll": (10000 + pnl) / 100,
        "min_bankroll": (10000 + lowest) / 100,
        "max_principal_loss": -lowest / 100, "max_drawdown": drawdown / 100,
        "wins": wins, "refunds": refunds, "losses": losses,
        "stop_reason": reason, "last_issue": final_issue,
    }


def load_data():
    with closing(connect(UPSTREAM / "pc28_predictor.db")) as con:
        names = {r["id"]: r["name"] for r in con.execute(
            "SELECT id,name FROM predictors WHERE lottery_type='pc28'"
        )}
        draw_rows = list(con.execute(
            "SELECT issue_no,open_time,result_number,source_payload FROM lottery_draws "
            "WHERE lottery_type='pc28' AND julianday(open_time,'-8 hours')<=julianday(?) "
            "ORDER BY issue_no", (AS_OF.isoformat(),)
        ))
        predictions = [dict(r) for r in con.execute(
            "SELECT id,predictor_id,issue_no,prediction_big_small,prediction_odd_even,"
            "prediction_combo,prediction_number,actual_number,created_at,settled_at FROM predictions "
            "WHERE lottery_type='pc28' AND status='settled' AND julianday(settled_at)<=julianday(?) "
            "ORDER BY created_at,id", (AS_OF.isoformat(),)
        )]
        summary_rows = [dict(r) for r in con.execute(
            "SELECT summary_date,draw_count FROM pc28_draw_daily_summary ORDER BY summary_date"
        )]
    with closing(connect(ROOT / "pc28touzhu.db")) as con:
        events = [dict(r) for r in con.execute(
            "SELECT e.id,e.subscription_id,e.auto_trigger_rule_id,e.auto_trigger_route_id,"
            "e.issue_no,e.created_at,e.settled_at,e.stake_amount,e.net_delta,"
            "e.resolved_result_type,e.settlement_rule_id,e.result_context_json,"
            "s.bet_type,s.bet_value,src.name source_name "
            "FROM subscription_progression_events e "
            "JOIN normalized_signals s ON s.id=e.signal_id "
            "JOIN signal_sources src ON src.id=s.source_id "
            "WHERE e.resolved_result_type IN ('hit','miss','refund') "
            "AND e.stake_amount>0 AND s.lottery_type='pc28' "
            "AND julianday(e.settled_at)<=julianday(?) ORDER BY e.created_at,e.id", (AS_OF.isoformat(),)
        )]
    draws = {}
    audit = Counter()
    for row in draw_rows:
        payload = json.loads(row["source_payload"] or "{}")
        snapshot = derive_pc28_draw_snapshot({
            "open_code": payload.get("number"), "result_number": row["result_number"]
        })
        triplet = snapshot["triplet"]
        opened = parse_time(row["open_time"], CN)
        if opened and opened > AS_OF:
            continue
        if not triplet or len(triplet) != 3 or any(x < 0 or x > 9 for x in triplet):
            audit["draw_missing_valid_triplet"] += 1
            continue
        if sum(triplet) != row["result_number"] or opened is None:
            audit["draw_invalid_sum_or_time"] += 1
            continue
        draws[row["issue_no"]] = {
            "snapshot": snapshot, "opened": opened, "month": opened.strftime("%Y-%m"),
        }
    audit.update({"draw_rows": len(draw_rows), "usable_draws": len(draws),
                  "settled_predictions": len(predictions), "settled_events": len(events)})
    return names, draws, predictions, events, summary_rows, audit


def prediction_streams(names, draws, predictions, audit):
    streams = defaultdict(list)
    cache = {}

    def add(label, metric, value, rule, issue):
        key = (issue, metric, value, rule)
        if key not in cache:
            cache[key] = net_cents(metric, value, rule, draws[issue]["snapshot"])
        streams[label].append((draws[issue]["month"], issue, cache[key]))

    for issue in draws:
        for rule in RULES:
            for metric, values in VALUES.items():
                for value in values:
                    add(f"fixed:{metric}:{value}:{rule}", metric, value, rule, issue)
            for value in range(28):
                add(f"fixed:number:{value}:{rule}", "number", value, rule, issue)

    # 只把当前预测创建前已经结算的旧预测加入触发窗口，避免偷看本期答案。
    history = defaultdict(lambda: deque(maxlen=100))
    pending = defaultdict(list)
    consensus = defaultdict(dict)
    for row in predictions:
        issue = row["issue_no"]
        if issue not in draws:
            audit["prediction_missing_draw"] += 1
            continue
        created = parse_time(row["created_at"])
        settled = parse_time(row["settled_at"])
        opened = draws[issue]["opened"]
        if created is None or settled is None or not created < opened <= settled:
            audit["prediction_bad_chronology"] += 1
            continue
        if row["actual_number"] != draws[issue]["snapshot"]["sum_value"]:
            audit["prediction_result_mismatch"] += 1
            continue
        audit["usable_predictions"] += 1
        pid = row["predictor_id"]
        consensus[issue][pid] = row
        if row["prediction_number"] in range(28):
            for rule in RULES:
                add(f"predictor{pid}:{names.get(pid,pid)}:number:direct:{rule}",
                    "number", row["prediction_number"], rule, issue)
        for metric, values in VALUES.items():
            value = row["prediction_" + metric]
            if value not in values:
                continue
            key = (pid, metric)
            while pending[key] and pending[key][0][0] < created:
                _, _, old_hit = heapq.heappop(pending[key])
                history[key].append(old_hit)
            modes = ["direct"]
            if len(history[key]) == 100:
                hit_count = sum(history[key])
                if hit_count <= (16 if metric == "combo" else 38):
                    modes.append("low100")
                if hit_count >= (30 if metric == "combo" else 60):
                    modes.append("high100")
            for rule in RULES:
                for mode in modes:
                    add(f"predictor{pid}:{names.get(pid,pid)}:{metric}:{mode}:{rule}",
                        metric, value, rule, issue)
                if metric != "combo":
                    inverse = values[1] if value == values[0] else values[0]
                    add(f"predictor{pid}:{names.get(pid,pid)}:{metric}:inverse:{rule}",
                        metric, inverse, rule, issue)
            hit = value == draws[issue]["snapshot"][metric]
            heapq.heappush(pending[key], (settled, row["id"], hit))

    for issue, available in consensus.items():
        if not all(pid in available for pid in (7, 8, 9)):
            continue
        for metric in VALUES:
            counts = Counter(available[pid]["prediction_" + metric] for pid in (7, 8, 9))
            value, n = counts.most_common(1)[0]
            if n < 2 or value not in VALUES[metric]:
                continue
            for rule in RULES:
                add(f"consensus789:{metric}:{rule}", metric, value, rule, issue)
    return streams


def event_streams(events, draws, audit):
    streams = defaultdict(list)
    seen = defaultdict(set)
    for row in events:
        metric = row["bet_type"]
        if metric == "big_small" and row["bet_value"] in VALUES["odd_even"]:
            metric = "odd_even"
        if metric not in VALUES or row["bet_value"] not in VALUES[metric]:
            audit["event_unsupported_bet"] += 1
            continue
        context = json.loads(row["result_context_json"] or "{}")
        snapshot = context.get("draw_snapshot") or {}
        if not snapshot.get("triplet"):
            audit["event_missing_triplet"] += 1
            continue
        triplet = snapshot["triplet"]
        if len(triplet) != 3 or any(not isinstance(x,int) or not 0 <= x <= 9 for x in triplet):
            audit["event_invalid_triplet"] += 1
            continue
        total = sum(triplet)
        if total != snapshot.get("sum_value", snapshot.get("result_number")):
            audit["event_sum_mismatch"] += 1
            continue
        clean = {"triplet": triplet, "result_number": total}
        rule = row["settlement_rule_id"]
        if rule not in RULES:
            audit["event_unknown_rule"] += 1
            continue
        pnl = net_cents(metric, row["bet_value"], rule, clean)
        if abs(pnl / 100 - row["net_delta"] / row["stake_amount"]) > 0.011:
            audit["event_stored_profit_differs_from_current_rules"] += 1
        draw = draws.get(row["issue_no"])
        opened = parse_time(snapshot.get("open_time")) or (draw["opened"] if draw else None)
        settled = parse_time(row["settled_at"])
        created = parse_time(row["created_at"])
        if opened is not None and created >= opened:
            audit["event_created_after_draw"] += 1
            continue
        if opened is None:
            audit["event_month_uses_settlement_time"] += 1
        month = (opened or settled).astimezone(CN).strftime("%Y-%m")
        trigger = row["auto_trigger_rule_id"] or 0
        labels = [f"observed_rule{trigger}",
                  f"observed_sub{row['subscription_id']}:{row['source_name']}:{metric}"]
        if trigger:
            labels.append(f"observed_rule{trigger}:sub{row['subscription_id']}:{metric}")
        for label in labels:
            if row["issue_no"] in seen[label]:
                audit["event_duplicate_issue_candidate_skips"] += 1
                continue
            seen[label].add(row["issue_no"])
            streams[label].append((month, row["issue_no"], pnl))
    return streams


def evaluate(streams, family, policies=None):
    results = []
    policies = POLICIES if policies is None else policies
    all_months = sorted({month for values in streams.values() for month,_,_ in values})
    for label, values in sorted(streams.items()):
        by_month = defaultdict(list)
        for month, issue, pnl in sorted(values, key=lambda x: int(x[1])):
            by_month[month].append((issue, pnl))
        for policy, (target, loss_limit) in policies.items():
            for month in all_months:
                if month < min(by_month):
                    continue
                bets = by_month.get(month, [])
                results.append({"family": family, "strategy": label, "policy": policy,
                                "month": month, "available_bets": len(bets),
                                **simulate(bets, target, loss_limit)})
    return results


def theoretical_results():
    results = []
    for rule in RULES:
        for metric, values in {**VALUES, "number":range(28)}.items():
            for value in values:
                distribution = Counter(net_cents(metric, value, rule, {"triplet": t})
                                       for t in itertools.product(range(10), repeat=3))
                mean = sum(pnl * n for pnl, n in distribution.items()) / 100000
                results.append({
                    "rule": rule, "metric": metric, "value": value,
                    "paid_win_probability": sum(n for p,n in distribution.items() if p>0)/1000,
                    "refund_probability": distribution[0]/1000,
                    "loss_probability": distribution[-100]/1000,
                    "net_expectation": round(mean, 8),
                    "net_stddev": round((sum(n*(p/100-mean)**2 for p,n in distribution.items())/1000)**0.5,6),
                })
    return results


def monte_carlo(trials=20000):
    """独立均匀三球假设下的资金路径；不把模型概率称为历史实测概率。"""
    results = []
    for policy, (target, limit) in POLICIES.items():
        if target is None:
            continue
        rng = random.Random(f"20260907:{policy}")
        outcomes, bet_counts, positive, stopped = [], [], 0, 0
        for _ in range(trials):
            pnl = 0
            count = 0
            while count < 12462 and pnl < target and pnl - 100 >= -limit:
                outcome = rng.random()
                pnl += -100 if outcome < 0.5 else (0 if outcome < 0.73 else 185)
                count += 1
            positive += pnl > 0
            stopped += pnl - 100 < -limit
            outcomes.append(pnl / 100)
            bet_counts.append(count)
        win_rate = positive / trials
        se = (win_rate * (1-win_rate) / trials) ** 0.5
        results.append({
            "policy": policy, "trials": trials, "positive_month_rate": round(win_rate,6),
            "rate_95pct_margin": round(1.96*se,6),
            "all_12_months_positive_probability": round(win_rate**12,6),
            "mean_month_net": round(statistics.mean(outcomes),6),
            "mean_month_net_95pct_margin": round(1.96*statistics.stdev(outcomes)/trials**0.5,6),
            "expected_net_from_expected_turnover": round(-0.0005*statistics.mean(bet_counts),6),
            "mean_bets": round(statistics.mean(bet_counts),3),
            "loss_stop_rate": stopped/trials,
        })
    return results


def select_using_past(results, family, train_months, test_months):
    by_strategy = defaultdict(dict)
    for row in results:
        if row["family"] == family and row["policy"] == "target1_limit20":
            by_strategy[row["strategy"]][row["month"]] = row
    eligible = {key: months for key,months in by_strategy.items()
                if all(m in months and months[m]["available_bets"] >= 100 for m in train_months)}
    if not eligible:
        return {}
    def score(key):
        rows = [eligible[key][m] for m in train_months]
        return (-sum(r["net"] > 0 for r in rows), max(r["max_principal_loss"] for r in rows),
                -min(r["net"] for r in rows), sum(r["bets"] for r in rows), key)
    selected = min(eligible, key=score)
    return {"family":family, "candidate_count":len(eligible), "selected":selected,
            "train_months":train_months, "test_months":test_months,
            "rows":[eligible[selected][m] for m in train_months + test_months
                    if m in eligible[selected]]}


def main():
    names, draws, predictions, events, summaries, audit = load_data()
    streams = prediction_streams(names, draws, predictions, audit)
    recorded = event_streams(events, draws, audit)
    results = evaluate(streams, "predictions") + evaluate(recorded, "observed")
    theory = theoretical_results()
    model = monte_carlo()
    coverage = []
    by_month = defaultdict(list)
    for issue, draw in draws.items():
        by_month[draw["month"]].append((issue,draw["opened"]))
    for month, data in sorted(by_month.items()):
        data.sort()
        gaps = [{"after":a[0], "before":b[0], "missing_issues":int(b[0])-int(a[0])-1}
                for a,b in zip(data,data[1:]) if int(b[0])-int(a[0])>1]
        coverage.append({"month":month, "draws":len(data),
                         "first":data[0][1].isoformat(), "last":data[-1][1].isoformat(),
                         "missing_internal_issues":sum(g["missing_issues"] for g in gaps),
                         "gaps":gaps})
    selections = [
        select_using_past(results,"predictions",["2026-07"],["2026-08","2026-09"]),
        select_using_past(results,"observed",["2026-05","2026-06"],["2026-07","2026-08","2026-09"]),
    ]
    metadata = {"generated_at":datetime.now(timezone.utc).isoformat(), "as_of":AS_OF.isoformat(), "audit":dict(audit),
                "draw_coverage":coverage, "archived_draw_summaries":summaries,
                "prediction_candidate_count":len(streams), "observed_candidate_count":len(recorded),
                "past_only_selections":selections, "model":model,
                "rounding":"沿用本仓每注净利保留两位小数；2.846 含本赔率命中净利 1.85",
                "input_databases_read_only":True}
    write_csv("monthly_results.csv", results)
    write_csv("theoretical_expectation.csv", theory)
    write_csv("model_monthly_risk.csv", model)
    (OUT / "metadata.json").write_text(json.dumps(metadata,ensure_ascii=False,indent=2)+"\n",encoding="utf-8")
    print(json.dumps({"as_of":AS_OF.isoformat(), "audit":dict(audit),
                      "prediction_candidates":len(streams), "observed_candidates":len(recorded),
                      "monthly_result_rows":len(results), "output_directory":str(OUT)},
                     ensure_ascii=False,indent=2))


if __name__ == "__main__":
    main()
