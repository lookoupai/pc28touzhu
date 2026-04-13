"""Shared domain models used across the platform and executor."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Optional


@dataclass
class JobTarget:
    type: str
    key: str
    name: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "type": self.type,
            "key": self.key,
            "name": self.name,
        }


@dataclass
class StakePlan:
    mode: str
    amount: float
    base_stake: float = 0.0
    multiplier: float = 2.0
    max_steps: int = 1
    refund_action: str = "hold"
    cap_action: str = "reset"
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        data = {
            "mode": self.mode,
            "amount": self.amount,
            "base_stake": self.base_stake,
            "multiplier": self.multiplier,
            "max_steps": self.max_steps,
            "refund_action": self.refund_action,
            "cap_action": self.cap_action,
        }
        if self.meta:
            data["meta"] = dict(self.meta)
        return data


@dataclass
class TelegramAccount:
    id: Optional[int]
    label: str = ""
    phone: str = ""
    session_path: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "label": self.label,
            "phone": self.phone,
            "session_path": self.session_path,
        }


@dataclass
class JobPullItem:
    job_id: str
    signal_id: str
    lottery_type: str
    issue_no: str
    bet_type: str
    bet_value: str
    message_text: str
    stake_plan: StakePlan
    target: JobTarget
    telegram_account: Optional[TelegramAccount]
    idempotency_key: str
    execute_after: Optional[str]
    expire_at: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "signal_id": self.signal_id,
            "lottery_type": self.lottery_type,
            "issue_no": self.issue_no,
            "bet_type": self.bet_type,
            "bet_value": self.bet_value,
            "message_text": self.message_text,
            "stake_plan": self.stake_plan.to_dict(),
            "target": self.target.to_dict(),
            "telegram_account": self.telegram_account.to_dict() if self.telegram_account else None,
            "idempotency_key": self.idempotency_key,
            "execute_after": self.execute_after,
            "expire_at": self.expire_at,
        }


@dataclass
class JobReportPayload:
    executor_id: str
    attempt_no: int
    delivery_status: str
    remote_message_id: Optional[str]
    executed_at: str
    raw_result: Dict[str, Any] = field(default_factory=dict)
    error_message: Optional[str] = None


@dataclass
class ExecutorHeartbeat:
    executor_id: str
    version: str = ""
    status: str = "online"
    capabilities: Dict[str, Any] = field(default_factory=dict)
    last_seen_at: str = ""
