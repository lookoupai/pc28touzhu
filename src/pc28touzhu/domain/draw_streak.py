"""指定开奖结果的连续期数，与预测、投注结构和结算赔率无关。"""
from __future__ import annotations

from typing import Any

from pc28touzhu.domain.settlement_rules import derive_pc28_draw_snapshot


DRAW_VALUES_BY_METRIC = {
    "big_small": ("大", "小"),
    "odd_even": ("单", "双"),
    "combo": ("大单", "大双", "小单", "小双"),
}
MAX_DRAW_STREAK = 100


def build_draw_streak_snapshot(items: list[dict], *, latest_issue_no: str, source: str = "") -> dict:
    """从最新已开奖期向前统计。缺期、坏数据和历史截断只提供已证实的下界。

    complete=False 时不能用 count 判断小于/小于等于，避免把截断的连开误当精确值。
    同期重复不累加，同期冲突的数据视为未知，不能跨过未知期继续统计。
    """
    if not str(latest_issue_no).isdigit():
        raise ValueError("draw_clock_unavailable")
    latest = int(latest_issue_no)
    draws: dict[int, Any] = {}
    for item in items:
        if not isinstance(item, dict) or not str(item.get("issue_no") or "").isdigit():
            continue
        issue = int(item["issue_no"])
        payload = item.get("draw_context") if isinstance(item.get("draw_context"), dict) else item
        result = derive_pc28_draw_snapshot(payload).get("result_number")
        if result is None or not 0 <= result <= 27:
            result = None
        if issue in draws and draws[issue] != result:
            draws[issue] = None
        else:
            draws[issue] = result
    if not draws or max(draws) != latest or draws.get(latest) is None:
        raise ValueError("draw_history_not_current")

    metrics = {}
    for metric, values in DRAW_VALUES_BY_METRIC.items():
        metrics[metric] = {}
        for value in values:
            issues = []
            issue = latest
            complete = False
            while draws.get(issue) is not None:
                snapshot = derive_pc28_draw_snapshot({"result_number": draws[issue]})
                if snapshot[metric] != value:
                    complete = True
                    break
                issues.append(str(issue))
                issue -= 1
            metrics[metric][value] = {"count": len(issues), "complete": complete, "issues": issues}
    return {"latest_issue_no": str(latest_issue_no), "source": source, "metrics": metrics}
