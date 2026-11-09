"""Offline exhaustive agreement check. Run with AITradingSimulator's Python.

python tests/verify_group_cross_project.py /path/to/AITradingSimulator
No app import, database, external API, or message sender is used.
"""
from __future__ import annotations

import itertools
import json
import sys
from pathlib import Path


def main():
    root = Path(__file__).resolve().parents[1]
    upstream = Path(sys.argv[1]) if len(sys.argv) > 1 else root.parent / "AITradingSimulator"
    sys.path.insert(0, str(root / "src"))
    sys.path.insert(0, str(upstream))
    from services.profit_simulator import ProfitSimulator
    from utils.pc28 import derive_group_bet
    from pc28touzhu.domain.settlement_rules import SETTLEMENT_RULE_CATALOG, resolve_pc28_group_settlement

    simulator = ProfitSimulator(None)
    cases = 0
    partial_refunds = 0
    for rule_id, rule in SETTLEMENT_RULE_CATALOG.items():
        for stake in (10, 0.01, 0.05, 0.1, 0.15, 0.25, 0.99, 1.5, 2001.01):
            strategy = simulator._build_bet_strategy("flat", stake, 2, 5)
            triplets = itertools.product(range(10), repeat=3) if stake == 10 else ((0, 0, 0), (9, 9, 9), (3, 5, 7), (1, 4, 7), (4, 4, 5), (3, 4, 7))
            for digits in triplets:
                number = sum(digits)
                combo = ("小" if number <= 13 else "大") + ("单" if number % 2 else "双")
                draw = {"result_number": number, "combo": combo, "open_time": "2026-11-09 12:00:00", "source_payload": json.dumps({"number": ",".join(map(str, digits))})}
                for predicted, metric in itertools.product(("大单", "大双", "小单", "小双"), ("double_group", "kill_group")):
                    group = derive_group_bet(metric, predicted)
                    record = simulator._build_record({"issue_no": "1001", "prediction_combo": predicted}, draw, metric, rule["profit_rule_id"], rule["odds_profile"], strategy, 1)
                    result = resolve_pc28_group_settlement(
                        signal={"bet_type": metric, "bet_value": group["bet_value"], "normalized_payload": group},
                        stake_amount=round(group["unit_count"] * stake, 2), settlement_rule_id=rule_id,
                        draw_context={"triplet": list(digits)},
                    )
                    key = (rule_id, stake, digits, predicted, metric)
                    expected = (record["prediction_hit"], record["result_type"], record["stake_amount"], record["payout_amount"], record["net_profit"])
                    actual = (result["prediction_hit"], result["result_type"], result["group_settlement"]["stake_amount"], result["group_settlement"]["payout_amount"], result["group_settlement"]["net_delta"])
                    assert actual == expected, (key, actual, expected)
                    assert [leg["result_type"] for leg in record["legs"]] == [leg["result_type"] for leg in result["group_settlement"]["legs"]], key
                    cases += 1
                    partial_refunds += int(result["prediction_hit"] and result["result_type"] == "miss")
    print(json.dumps({"cases": cases, "rule_count": len(SETTLEMENT_RULE_CATALOG), "full_triplet_cases": 64000, "fractional_stake_cases": cases - 64000, "partial_refund_loss_cases": partial_refunds, "status": "passed"}, ensure_ascii=False))


if __name__ == "__main__":
    main()
