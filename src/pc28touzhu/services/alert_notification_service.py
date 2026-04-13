"""Alert notification orchestration for external delivery channels."""
from __future__ import annotations

from typing import Any, Dict, List, Protocol


class AlertTextSender(Protocol):
    def send_text(self, target_chat_id: str, message_text: str) -> Dict[str, Any]:
        ...


def prepare_alert_notifications(
    repository: Any,
    *,
    alerts: List[Dict[str, Any]],
    repeat_interval_seconds: int,
) -> Dict[str, Any]:
    items = repository.sync_platform_alert_records(
        alerts,
        repeat_interval_seconds=max(60, int(repeat_interval_seconds or 60)),
    )
    return {"items": items}


def build_alert_notification_text(item: Dict[str, Any]) -> str:
    event = str(item.get("notification_event") or "firing")
    prefix = {
        "firing": "告警触发",
        "reminder": "告警持续",
        "resolved": "告警恢复",
    }.get(event, "告警通知")
    metadata = item.get("metadata") or {}
    lines = [
        "[%s]" % prefix,
        str(item.get("title") or "").strip() or "未命名告警",
        str(item.get("message") or "").strip() or "--",
        "类型: %s" % (str(item.get("alert_type") or "--")),
    ]
    if event != "resolved":
        lines.append("级别: %s" % (str(item.get("severity") or "--")))
    if metadata.get("executor_id"):
        lines.append("执行器: %s" % metadata["executor_id"])
    if metadata.get("job_id"):
        lines.append("任务: #%s" % metadata["job_id"])
    if metadata.get("signal_id"):
        lines.append("信号: #%s" % metadata["signal_id"])
    return "\n".join(lines)


def deliver_platform_alerts(
    repository: Any,
    *,
    alerts: List[Dict[str, Any]],
    sender: AlertTextSender,
    target_chat_id: str,
    repeat_interval_seconds: int,
) -> Dict[str, Any]:
    normalized_target_chat_id = str(target_chat_id or "").strip()
    if not normalized_target_chat_id:
        raise ValueError("target_chat_id 不能为空")

    prepared = prepare_alert_notifications(
        repository,
        alerts=alerts,
        repeat_interval_seconds=repeat_interval_seconds,
    )
    sent_count = 0
    failed_count = 0
    delivered_items = []

    for item in prepared["items"]:
        message_text = build_alert_notification_text(item)
        try:
            send_result = sender.send_text(normalized_target_chat_id, message_text)
            record = repository.mark_platform_alert_sent(alert_key=item["alert_key"])
            sent_count += 1
            delivered_items.append(
                {
                    "alert_key": item["alert_key"],
                    "notification_event": item["notification_event"],
                    "delivery_status": "sent",
                    "send_result": send_result,
                    "record": record,
                }
            )
        except Exception as exc:
            record = repository.mark_platform_alert_sent(alert_key=item["alert_key"], error=str(exc))
            failed_count += 1
            delivered_items.append(
                {
                    "alert_key": item["alert_key"],
                    "notification_event": item["notification_event"],
                    "delivery_status": "failed",
                    "error_message": str(exc) or exc.__class__.__name__,
                    "record": record,
                }
            )

    return {
        "pending_count": len(prepared["items"]),
        "sent_count": sent_count,
        "failed_count": failed_count,
        "items": delivered_items,
    }
