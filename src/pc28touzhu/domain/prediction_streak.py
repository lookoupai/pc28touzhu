"""用来源连中/连挂统计和逐期预测方向，求指定方向的当前连续段。"""
from __future__ import annotations

from pc28touzhu.domain.draw_streak import DRAW_VALUES_BY_METRIC


PREDICTION_STREAK_TYPES = {"hit_streak", "miss_streak"}


def prediction_streak_count(value) -> int | None:
    try:
        count = float(value)
        if count >= 0 and count.is_integer():
            return int(count)
    except (TypeError, ValueError, OverflowError):
        pass
    return None


def build_prediction_direction_streaks(items: list[dict], *, latest_issue_no: str, metrics: dict) -> dict:
    """整体连续段内各期的预测结果相同，因此只需进一步检查预测方向。

    只接受从 latest_settled_issue 开始的连续期号；方向改变即中断，不跳过
    其他方向。缺期、无效/冲突方向只提供已证实下界，不能当作精确次数。
    方向段到达来源整体连续段长度时，已由预测对错证明边界，计数完整。
    """
    if not str(latest_issue_no).isdigit():
        return {}
    latest = int(latest_issue_no)
    predictions = {}
    for item in items:
        metric = item.get("bet_type")
        if metric not in DRAW_VALUES_BY_METRIC or not str(item.get("issue_no") or "").isdigit():
            continue
        key = (metric, int(item["issue_no"]))
        value = item.get("bet_value")
        if value not in DRAW_VALUES_BY_METRIC[metric]:
            value = None
        if key in predictions and predictions[key] != value:
            predictions[key] = None
        else:
            predictions[key] = value

    result = {}
    for metric, values in DRAW_VALUES_BY_METRIC.items():
        payload = metrics.get(metric) if isinstance(metrics.get(metric), dict) else {}
        streaks = payload.get("streaks") if isinstance(payload.get("streaks"), dict) else {}
        result[metric] = {}
        for value in values:
            result[metric][value] = {}
            for kind in PREDICTION_STREAK_TYPES:
                total = prediction_streak_count(streaks.get("current_" + kind))
                if total is None or (total > 0 and predictions.get((metric, latest)) is None):
                    continue
                issues = []
                complete = True
                for offset in range(total):
                    issue = latest - offset
                    direction = predictions.get((metric, issue))
                    if direction is None:
                        complete = False
                        break
                    if direction != value:
                        break
                    issues.append(str(issue))
                result[metric][value][kind] = {
                    "count": len(issues), "complete": complete, "issues": issues,
                }
    return result
