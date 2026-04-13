from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


def _parse_iso8601(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
    text = str(value).strip()
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    return datetime.fromisoformat(text)


@dataclass(frozen=True)
class StakePlan:
    mode: str
    amount: float
    base_stake: float = 0.0
    multiplier: float = 2.0
    max_steps: int = 1
    refund_action: str = "hold"
    cap_action: str = "reset"


@dataclass(frozen=True)
class DeliveryTarget:
    type: str
    key: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass(frozen=True)
class TelegramAccountInfo:
    id: Optional[int]
    label: str
    phone: str
    session_path: str


@dataclass
class ExecutorJob:
    job_id: str
    signal_id: str
    lottery_type: str
    issue_no: str
    bet_type: str
    bet_value: str
    message_text: str
    stake_plan: StakePlan
    target: DeliveryTarget
    telegram_account: Optional[TelegramAccountInfo]
    idempotency_key: str
    execute_after: datetime
    expire_at: datetime

    @classmethod
    def from_payload(cls, payload: Dict[str, Any]) -> "ExecutorJob":
        stake = payload.get("stake_plan") or {}
        target = payload.get("target") or {}
        account = payload.get("telegram_account") or {}
        return cls(
            job_id=str(payload["job_id"]),
            signal_id=str(payload["signal_id"]),
            lottery_type=str(payload["lottery_type"]),
            issue_no=str(payload["issue_no"]),
            bet_type=str(payload["bet_type"]),
            bet_value=str(payload["bet_value"]),
            message_text=str(payload["message_text"]),
            stake_plan=StakePlan(
                mode=str(stake.get("mode") or "flat"),
                amount=float(stake.get("amount") or 0),
                base_stake=float(stake.get("base_stake") or stake.get("amount") or 0),
                multiplier=float(stake.get("multiplier") or 2),
                max_steps=int(stake.get("max_steps") or 1),
                refund_action=str(stake.get("refund_action") or "hold"),
                cap_action=str(stake.get("cap_action") or "reset"),
            ),
            target=DeliveryTarget(
                type=str(target["type"]),
                key=str(target["key"]),
                metadata=target.get("metadata"),
            ),
            telegram_account=(
                TelegramAccountInfo(
                    id=int(account["id"]) if account.get("id") is not None else None,
                    label=str(account.get("label") or ""),
                    phone=str(account.get("phone") or ""),
                    session_path=str(account.get("session_path") or ""),
                )
                if account
                else None
            ),
            idempotency_key=str(payload["idempotency_key"]),
            execute_after=_parse_iso8601(payload["execute_after"]),
            expire_at=_parse_iso8601(payload["expire_at"]),
        )


@dataclass
class ExecutorResult:
    job_id: str
    executor_id: str
    attempt_no: int
    delivery_status: str
    executed_at: datetime
    remote_message_id: Optional[str] = None
    raw_result: Optional[Dict[str, Any]] = None
    error_message: Optional[str] = None

    def to_payload(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "executor_id": self.executor_id,
            "attempt_no": self.attempt_no,
            "delivery_status": self.delivery_status,
            "executed_at": self.executed_at.astimezone(timezone.utc).isoformat(),
        }
        if self.remote_message_id is not None:
            payload["remote_message_id"] = self.remote_message_id
        if self.raw_result is not None:
            payload["raw_result"] = self.raw_result
        if self.error_message is not None:
            payload["error_message"] = self.error_message
        return payload
