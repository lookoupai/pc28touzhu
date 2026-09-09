"""沿用只读数据与结算逻辑，检验每月本金 100、净赚 100 的新目标。"""
from __future__ import annotations

import itertools
import json
import random
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone

from analyze import (
    AS_OF,
    OUT,
    RULES,
    evaluate,
    event_streams,
    load_data,
    net_cents,
    prediction_streams,
    write_csv,
)


TARGET = 10000
TARGET_POLICIES = {
    "target100_bankroll": (TARGET, None),
    "target100_limit99": (TARGET, 9900),
}


def summarize_candidates(rows):
    groups = defaultdict(list)
    for row in rows:
        groups[(row["family"], row["strategy"], row["policy"])].append(row)
    summaries = []
    for (family, strategy, policy), months in sorted(groups.items()):
        reached = sum(r["net"] >= 100 for r in months)
        summaries.append({
            "family": family, "strategy": strategy, "policy": policy,
            "sample_months": len(months), "months_with_bets": sum(r["bets"] > 0 for r in months),
            "target_reached_months": reached, "all_sample_months_reached": reached == len(months),
            "funding_failure_months": sum(r["stop_reason"] in
                ("bankrupt", "insufficient_for_one_unit") for r in months),
            "lowest_month_net": min(r["net"] for r in months),
            "total_net": round(sum(r["net"] for r in months), 2),
            "max_principal_loss": max(r["max_principal_loss"] for r in months),
            "max_drawdown": max(r["max_drawdown"] for r in months),
        })
    return summaries


def select_on_earlier_months(rows, family, training, validation):
    candidates = defaultdict(dict)
    for row in rows:
        if row["family"] == family and row["policy"] == "target100_bankroll":
            candidates[row["strategy"]][row["month"]] = row
    eligible = {name: months for name, months in candidates.items()
                if all(month in months and months[month]["available_bets"] >= 100 for month in training)}
    if not eligible:
        return {}

    def score(name):
        sample = [eligible[name][month] for month in training]
        return (-sum(r["net"] >= 100 for r in sample),
                -min(min(r["net"], 100) for r in sample),
                max(r["max_principal_loss"] for r in sample),
                max(r["max_drawdown"] for r in sample),
                sum(r["bets"] for r in sample), name)

    chosen = min(eligible, key=score)
    return {
        "family": family, "eligible_candidates": len(eligible), "selected": chosen,
        "training": training, "validation": validation,
        "rows": [eligible[chosen][m] for m in training + validation if m in eligible[chosen]],
    }


def simulate_random_months(metric, value, rule, *, trials=10000, loss_limit=None, max_bets=12462):
    """按当前规则枚举三球分布，再模拟有月末和本金约束的逐注路径。"""
    counts = Counter(net_cents(metric, value, rule, {"triplet": triplet})
                     for triplet in itertools.product(range(10), repeat=3))
    positive = [amount for amount in counts if amount > 0]
    if len(positive) != 1 or any(amount < -100 for amount in counts):
        raise ValueError("随机模型要求单注亏 1、至多一种中奖净利")
    win_amount = positive[0]
    loss_probability = counts[-100] / 1000
    refund_boundary = (counts[-100] + counts[0]) / 1000
    unit_expectation = sum(amount * count for amount, count in counts.items()) / 100000
    rng = random.Random(f"20260907:target100:{metric}:{value}:{rule}:{loss_limit}")
    random_value = rng.random
    target_reached = funding_failure = loss_stopped = month_ended = 0
    outcomes, bet_counts = [], []
    for _ in range(trials):
        pnl = count = 0
        while count < max_bets and pnl < TARGET and 10000 + pnl >= 100:
            if loss_limit is not None and pnl - 100 < -loss_limit:
                break
            value_drawn = random_value()
            if value_drawn < loss_probability:
                pnl -= 100
            elif value_drawn >= refund_boundary:
                pnl += win_amount
            count += 1
        if pnl >= TARGET:
            target_reached += 1
        elif 10000 + pnl < 100:
            funding_failure += 1
        elif loss_limit is not None and pnl - 100 < -loss_limit:
            loss_stopped += 1
        else:
            month_ended += 1
        outcomes.append(pnl / 100)
        bet_counts.append(count)
    rate = target_reached / trials
    return {
        "rule": rule, "metric": metric, "value": value,
        "loss_limit": None if loss_limit is None else loss_limit / 100,
        "trials": trials, "max_bets_per_month": max_bets,
        "target_reached_rate": rate,
        "rate_95pct_margin": round(1.96 * (rate * (1 - rate) / trials) ** 0.5, 6),
        "all_12_months_target_rate": rate ** 12,
        "funding_failure_rate": funding_failure / trials,
        "loss_stop_rate": loss_stopped / trials,
        "month_ended_below_target_rate": month_ended / trials,
        "mean_bets": round(statistics.mean(bet_counts), 2),
        "mean_net": round(statistics.mean(outcomes), 6),
        "unit_expectation": round(unit_expectation, 8),
        "net_expectation_from_turnover": round(unit_expectation * statistics.mean(bet_counts), 6),
    }


def main():
    names, draws, predictions, events, _, audit = load_data()
    streams = prediction_streams(names, draws, predictions, audit)
    observed = event_streams(events, draws, audit)
    rows = evaluate(streams, "predictions", TARGET_POLICIES)
    rows.extend(evaluate(observed, "observed", TARGET_POLICIES))
    for row in rows:
        row["target_reached"] = row["net"] >= 100
        row["last_bet_time_cn"] = draws.get(row["last_issue"], {}).get("opened")
        if row["last_bet_time_cn"] is not None:
            row["last_bet_time_cn"] = row["last_bet_time_cn"].isoformat()
    summaries = summarize_candidates(rows)
    selections = [
        select_on_earlier_months(rows, "predictions", ["2026-07"], ["2026-08", "2026-09"]),
        select_on_earlier_months(rows, "observed", ["2026-05", "2026-06"],
                                ["2026-07", "2026-08", "2026-09"]),
    ]
    write_csv("target100_monthly_results.csv", rows)
    write_csv("target100_candidate_summary.csv", summaries)
    samples = defaultdict(set)
    for row in rows:
        samples[row["family"]].add(row["month"])
    all_reached = [r for r in summaries if r["policy"] == "target100_bankroll"
                   and r["all_sample_months_reached"]
                   and r["sample_months"] == len(samples[r["family"]])]
    metadata = {
        "as_of": AS_OF.isoformat(), "generated_at": datetime.now(timezone.utc).isoformat(),
        "starting_bankroll": 100, "stake": 1, "monthly_net_profit_target": 100,
        "audit": dict(audit), "prediction_candidates": len(streams), "observed_candidates": len(observed),
        "monthly_rows": len(rows), "all_available_months_reached_candidates": all_reached,
        "past_only_selections": selections,
    }
    print(json.dumps(metadata, ensure_ascii=False, indent=2), flush=True)
    models = []
    cases = [
        ("odd_even", "单", RULES[0], None),
        ("combo", "大单", RULES[0], None),
        ("combo", "小单", RULES[0], None),
        ("odd_even", "单", RULES[1], None),
        ("combo", "大单", RULES[1], None),
        ("number", 0, RULES[0], None),
        ("number", 22, RULES[0], None),
        ("combo", "大单", RULES[0], 9900),
        ("number", 23, RULES[0], None),
    ]
    for metric, value, rule, limit in cases:
        result = simulate_random_months(metric, value, rule, loss_limit=limit)
        models.append(result)
        print("MODEL " + json.dumps(result, ensure_ascii=False), flush=True)
    metadata["model_results"] = models
    write_csv("target100_model_risk.csv", models)
    (OUT / "target100_metadata.json").write_text(
        json.dumps(metadata, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
