from __future__ import annotations

import json
import re
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.auth import SESSION_COOKIE_NAME, build_session_cookie_value, hash_password
from pc28touzhu.api.app import PlatformApiApplication, _asset_version, _load_ui_html_file, build_testing_environ


class FakeRepository:
    def __init__(self):
        self.heartbeats = []
        self.executors = []
        self.reports = []
        self.alert_records = {}
        self.platform_runtime_settings = {}
        self.sources = [
            {
                "id": 1,
                "owner_user_id": 1,
                "source_type": "internal_ai",
                "name": "demo-source",
                "status": "active",
                "visibility": "private",
                "config": {},
                "created_at": "2026-04-07T12:00:00Z",
                "updated_at": "2026-04-07T12:00:00Z",
            }
        ]
        self.users = [
            {
                "id": 1,
                "username": "owner",
                "email": "owner@example.com",
                "password_hash": hash_password("owner-pass"),
                "role": "admin",
                "status": "active",
                "session_version": 1,
                "last_login_at": None,
                "password_changed_at": None,
                "created_at": "2026-04-07T12:00:00Z",
                "updated_at": "2026-04-07T12:00:00Z",
            }
        ]
        self.telegram_accounts = []
        self.message_templates = []
        self.subscriptions = []
        self.targets = []
        self.signals = []
        self.jobs = []
        self.raw_items = []
        self.progression_events = []
        self.subscription_progression_states = {}
        self.subscription_financial_states = {}
        self.subscription_daily_stats = []
        self.subscription_runtime_runs = []

    def requeue_auto_retry_jobs(self, *, max_attempts, base_delay_seconds, limit):
        return []

    def list_platform_alert_records_by_keys(self, alert_keys):
        return {
            str(alert_key): self.alert_records[str(alert_key)]
            for alert_key in alert_keys
            if str(alert_key) in self.alert_records
        }

    def sync_platform_alert_records(self, alerts, *, repeat_interval_seconds):
        items = []
        for alert in alerts:
            key = str(alert["alert_key"])
            record = self.alert_records.get(key)
            if record is None:
                record = {
                    "id": len(self.alert_records) + 1,
                    "alert_key": key,
                    "alert_type": alert["alert_type"],
                    "severity": alert["severity"],
                    "title": alert["title"],
                    "message": alert["message"],
                    "metadata": alert.get("metadata") or {},
                    "status": "active",
                    "first_seen_at": "2026-04-08T12:00:00Z",
                    "last_seen_at": "2026-04-08T12:00:00Z",
                    "resolved_at": None,
                    "last_sent_at": None,
                    "send_count": 0,
                    "occurrence_count": 1,
                    "last_error": None,
                    "created_at": "2026-04-08T12:00:00Z",
                    "updated_at": "2026-04-08T12:00:00Z",
                }
                self.alert_records[key] = record
            items.append({**alert, "notification_event": "firing", "record": record})
        return items

    def mark_platform_alert_sent(self, *, alert_key, error=None, sent_at=None):
        record = self.alert_records.get(str(alert_key))
        if not record:
            return None
        if error:
            record["last_error"] = error
        else:
            record["last_sent_at"] = sent_at or "2026-04-08T12:00:00Z"
            record["send_count"] += 1
            record["last_error"] = None
        return record

    def get_platform_runtime_setting(self, setting_key):
        return self.platform_runtime_settings.get(str(setting_key or ""))

    def upsert_platform_runtime_setting(self, *, setting_key, value):
        item = {
            "setting_key": str(setting_key or ""),
            "value": value or {},
            "updated_at": "2026-04-17T12:00:00Z",
        }
        self.platform_runtime_settings[str(setting_key or "")] = item
        return item

    def pull_ready_jobs(self, executor_id: str, limit: int):
        return [
            {
                "job_id": "job_001",
                "signal_id": "sig_001",
                "lottery_type": "pc28",
                "issue_no": "20260407001",
                "bet_type": "big_small",
                "bet_value": "大",
                "message_text": "大10",
                "stake_plan": {"mode": "flat", "amount": 10},
                "target": {"type": "telegram_group", "key": "-1001234567890", "name": "测试群"},
                "idempotency_key": "idemp-001",
                "execute_after": "2026-04-07T15:00:00Z",
                "expire_at": "2026-04-07T15:01:00Z",
            }
        ][:limit]

    def report_job_result(
        self,
        job_id,
        executor_id,
        attempt_no,
        delivery_status,
        remote_message_id,
        executed_at,
        raw_result,
        error_message,
    ):
        payload = {
            "job_id": job_id,
            "executor_id": executor_id,
            "attempt_no": attempt_no,
            "delivery_status": delivery_status,
            "remote_message_id": remote_message_id,
            "executed_at": executed_at,
            "raw_result": raw_result,
            "error_message": error_message,
        }
        self.reports.append(payload)
        return payload

    def upsert_executor_heartbeat(self, executor_id, version, capabilities, status, last_seen_at):
        payload = {
            "executor_id": executor_id,
            "version": version,
            "capabilities": capabilities,
            "status": status,
            "last_seen_at": last_seen_at,
            "updated_at": last_seen_at,
            "total_attempt_count": 0,
            "delivered_attempt_count": 0,
            "failed_attempt_count": 0,
            "last_executed_at": None,
            "last_failure_at": None,
            "last_failure_status": None,
            "last_failure_error_message": None,
        }
        self.executors = [item for item in self.executors if item["executor_id"] != executor_id]
        self.executors.append(payload)
        self.heartbeats.append(payload)
        return payload

    def list_users(self):
        return list(self.users)

    def get_user(self, user_id):
        for item in self.users:
            if item["id"] == int(user_id):
                return item
        return None

    def get_user_by_username(self, username):
        for item in self.users:
            if item["username"] == str(username):
                return item
        return None

    def create_user_record(self, **kwargs):
        item = {
            "id": len(self.users) + 1,
            "username": kwargs["username"],
            "email": kwargs.get("email", ""),
            "password_hash": kwargs.get("password_hash", ""),
            "role": kwargs.get("role", "user"),
            "status": kwargs.get("status", "active"),
            "session_version": kwargs.get("session_version", 1),
            "last_login_at": kwargs.get("last_login_at"),
            "password_changed_at": kwargs.get("password_changed_at"),
            "telegram_user_id": None,
            "telegram_chat_id": "",
            "telegram_username": "",
            "telegram_bound_at": None,
            "telegram_bind_token": "",
            "telegram_bind_token_expire_at": None,
            "created_at": "2026-04-07T12:00:00Z",
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.users.append(item)
        return item

    def update_user_password(self, user_id, password_hash, *, email=None, bump_session_version=True):
        user = self.get_user(user_id)
        if not user:
            return None
        user["password_hash"] = password_hash
        if email is not None:
            user["email"] = email
        if bump_session_version:
            user["session_version"] = int(user.get("session_version") or 1) + 1
        user["password_changed_at"] = "2026-04-07T12:00:00Z"
        user["updated_at"] = "2026-04-07T12:00:00Z"
        return user

    def touch_user_login(self, user_id):
        user = self.get_user(user_id)
        if not user:
            return None
        user["last_login_at"] = "2026-04-08T12:00:00Z"
        user["updated_at"] = "2026-04-08T12:00:00Z"
        return user

    def get_user_telegram_binding(self, user_id):
        user = self.get_user(user_id)
        if not user:
            raise ValueError("user_id 对应的用户不存在")
        bind_token = str(user.get("telegram_bind_token") or "")
        return {
            "user_id": int(user["id"]),
            "is_bound": user.get("telegram_user_id") is not None and str(user.get("telegram_chat_id") or "") != "",
            "telegram_user_id": user.get("telegram_user_id"),
            "telegram_chat_id": str(user.get("telegram_chat_id") or ""),
            "telegram_username": str(user.get("telegram_username") or ""),
            "telegram_bound_at": user.get("telegram_bound_at"),
            "bind_token": bind_token,
            "bind_token_expire_at": user.get("telegram_bind_token_expire_at"),
            "has_active_bind_token": bool(bind_token),
        }

    def set_user_telegram_bind_token(self, *, user_id, bind_token, expire_at):
        user = self.get_user(user_id)
        if not user:
            raise ValueError("user_id 对应的用户不存在")
        user["telegram_bind_token"] = str(bind_token or "")
        user["telegram_bind_token_expire_at"] = expire_at
        user["updated_at"] = "2026-04-07T12:00:00Z"
        return self.get_user_telegram_binding(user_id)

    def clear_user_telegram_bind_token(self, *, user_id):
        user = self.get_user(user_id)
        if not user:
            raise ValueError("user_id 对应的用户不存在")
        user["telegram_bind_token"] = ""
        user["telegram_bind_token_expire_at"] = None
        user["updated_at"] = "2026-04-07T12:00:00Z"
        return self.get_user_telegram_binding(user_id)

    def get_user_by_telegram_bind_token(self, bind_token):
        for item in self.users:
            if str(item.get("telegram_bind_token") or "") == str(bind_token or ""):
                return item
        return None

    def get_user_by_telegram_user_id(self, telegram_user_id):
        for item in self.users:
            if item.get("telegram_user_id") == int(telegram_user_id):
                return item
        return None

    def update_user_telegram_binding(
        self,
        *,
        user_id,
        telegram_user_id,
        telegram_chat_id,
        telegram_username="",
        telegram_bound_at=None,
    ):
        user = self.get_user(user_id)
        if not user:
            raise ValueError("user_id 对应的用户不存在")
        conflict = self.get_user_by_telegram_user_id(telegram_user_id)
        if conflict and int(conflict["id"]) != int(user_id):
            raise ValueError("该 Telegram 账号已绑定其他平台用户")
        user["telegram_user_id"] = int(telegram_user_id)
        user["telegram_chat_id"] = str(telegram_chat_id or "")
        user["telegram_username"] = str(telegram_username or "")
        user["telegram_bound_at"] = telegram_bound_at or "2026-04-07T12:00:00Z"
        user["telegram_bind_token"] = ""
        user["telegram_bind_token_expire_at"] = None
        user["updated_at"] = "2026-04-07T12:00:00Z"
        return self.get_user_telegram_binding(user_id)

    def clear_user_telegram_binding(self, *, user_id):
        user = self.get_user(user_id)
        if not user:
            raise ValueError("user_id 对应的用户不存在")
        user["telegram_user_id"] = None
        user["telegram_chat_id"] = ""
        user["telegram_username"] = ""
        user["telegram_bound_at"] = None
        user["telegram_bind_token"] = ""
        user["telegram_bind_token_expire_at"] = None
        user["updated_at"] = "2026-04-07T12:00:00Z"
        return self.get_user_telegram_binding(user_id)

    def list_sources(self, owner_user_id=None):
        if owner_user_id is None:
            return list(self.sources)
        return [item for item in self.sources if item["owner_user_id"] == int(owner_user_id)]

    def get_source(self, source_id):
        for item in self.sources:
            if item["id"] == int(source_id):
                return item
        return None

    def source_belongs_to_user(self, source_id, user_id):
        source = self.get_source(source_id)
        return bool(source and source.get("owner_user_id") == int(user_id))

    def list_telegram_accounts(self, user_id):
        return [item for item in self.telegram_accounts if item["user_id"] == int(user_id)]

    def get_telegram_account(self, telegram_account_id):
        for item in self.telegram_accounts:
            if item["id"] == int(telegram_account_id):
                return item
        return None

    def create_telegram_account_record(self, **kwargs):
        item = {
            "id": len(self.telegram_accounts) + 1,
            "user_id": kwargs["user_id"],
            "label": kwargs["label"],
            "phone": kwargs.get("phone", ""),
            "session_path": kwargs["session_path"],
            "status": kwargs.get("status", "active"),
            "meta": kwargs.get("meta") or {},
            "created_at": "2026-04-07T12:00:00Z",
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.telegram_accounts.append(item)
        return item

    def update_telegram_account_status(self, *, telegram_account_id, user_id, status):
        for item in self.telegram_accounts:
            if item["id"] == int(telegram_account_id) and item["user_id"] == int(user_id):
                item["status"] = str(status)
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def update_telegram_account_record(self, *, telegram_account_id, user_id, label, session_path, phone="", meta=None):
        for item in self.telegram_accounts:
            if item["id"] == int(telegram_account_id) and item["user_id"] == int(user_id):
                item["label"] = label
                item["session_path"] = session_path
                item["phone"] = phone
                item["meta"] = meta or {}
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def count_delivery_targets_by_telegram_account(self, telegram_account_id, *, user_id=None):
        return sum(
            1
            for item in self.targets
            if item.get("telegram_account_id") == int(telegram_account_id)
            and (user_id is None or item["user_id"] == int(user_id))
        )

    def count_execution_jobs_by_telegram_account(self, telegram_account_id, *, user_id=None):
        return sum(
            1
            for item in self.jobs
            if item.get("telegram_account_id") == int(telegram_account_id)
            and (user_id is None or item["user_id"] == int(user_id))
        )

    def delete_telegram_account_record(self, *, telegram_account_id, user_id):
        for index, item in enumerate(self.telegram_accounts):
            if item["id"] == int(telegram_account_id) and item["user_id"] == int(user_id):
                del self.telegram_accounts[index]
                return True
        return False

    def update_telegram_account_record(self, *, telegram_account_id, user_id, label, session_path, phone="", meta=None):
        for item in self.telegram_accounts:
            if item["id"] == int(telegram_account_id) and item["user_id"] == int(user_id):
                item["label"] = label
                item["session_path"] = session_path
                item["phone"] = phone
                item["meta"] = meta or {}
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def create_source_record(self, **kwargs):
        item = {
            "id": len(self.sources) + 1,
            "owner_user_id": kwargs.get("owner_user_id"),
            "source_type": kwargs["source_type"],
            "name": kwargs["name"],
            "status": kwargs.get("status", "active"),
            "visibility": kwargs.get("visibility", "private"),
            "config": kwargs.get("config") or {},
            "created_at": "2026-04-07T12:00:00Z",
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.sources.append(item)
        return item

    def update_source_record(self, *, source_id, owner_user_id, name, visibility, status, config=None):
        for item in self.sources:
            if item["id"] == int(source_id) and item["owner_user_id"] == int(owner_user_id):
                item["name"] = name
                item["visibility"] = visibility
                item["status"] = status
                item["config"] = config or {}
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def update_source_status(self, *, source_id, owner_user_id, status):
        for item in self.sources:
            if item["id"] == int(source_id) and item["owner_user_id"] == int(owner_user_id):
                item["status"] = str(status)
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def count_raw_items_by_source(self, source_id):
        return sum(1 for item in self.raw_items if item["source_id"] == int(source_id))

    def count_signals_by_source(self, source_id):
        return sum(1 for item in self.signals if item["source_id"] == int(source_id))

    def count_subscriptions_by_source(self, source_id, *, user_id=None):
        return sum(
            1
            for item in self.subscriptions
            if item["source_id"] == int(source_id) and (user_id is None or item["user_id"] == int(user_id))
        )

    def delete_source_record(self, *, source_id, owner_user_id):
        for index, item in enumerate(self.sources):
            if item["id"] == int(source_id) and item["owner_user_id"] == int(owner_user_id):
                del self.sources[index]
                return True
        return False

    def list_subscriptions(self, user_id):
        return [self.get_subscription(item["id"]) for item in self.subscriptions if item["user_id"] == int(user_id)]

    def list_user_daily_subscription_stats(self, *, user_id, stat_date):
        items = []
        for item in self.subscription_daily_stats:
            if item["user_id"] != int(user_id) or str(item["stat_date"]) != str(stat_date):
                continue
            source = next((source for source in self.sources if source["id"] == int(item["source_id"])), None)
            items.append({
                **item,
                "source_name": (source or {}).get("name", ""),
            })
        return items

    def list_subscription_daily_stats(self, *, subscription_id, user_id, limit=7):
        items = []
        for item in self.subscription_daily_stats:
            if item["subscription_id"] != int(subscription_id) or item["user_id"] != int(user_id):
                continue
            source = next((source for source in self.sources if source["id"] == int(item["source_id"])), None)
            items.append({
                **item,
                "source_name": (source or {}).get("name", ""),
            })
        items.sort(key=lambda item: (str(item.get("stat_date") or ""), str(item.get("updated_at") or "")), reverse=True)
        return items[: max(1, min(int(limit or 7), 365))]

    def list_subscription_runtime_runs(self, *, subscription_id, user_id, limit=5):
        items = [
            dict(item)
            for item in self.subscription_runtime_runs
            if item["subscription_id"] == int(subscription_id) and item["user_id"] == int(user_id)
        ]
        items.sort(
            key=lambda item: (0 if str(item.get("status") or "") == "active" else 1, str(item.get("started_at") or "")),
            reverse=False,
        )
        return items[: max(1, min(int(limit or 5), 20))]

    def get_user_daily_profit_summary(self, *, user_id, stat_date):
        stats = self.list_user_daily_subscription_stats(user_id=user_id, stat_date=stat_date)
        return {
            "stat_date": str(stat_date),
            "user_id": int(user_id),
            "profit_amount": round(sum(float(item.get("profit_amount") or 0) for item in stats), 2),
            "loss_amount": round(sum(float(item.get("loss_amount") or 0) for item in stats), 2),
            "net_profit": round(sum(float(item.get("net_profit") or 0) for item in stats), 2),
            "settled_event_count": sum(int(item.get("settled_event_count") or 0) for item in stats),
            "hit_count": sum(int(item.get("hit_count") or 0) for item in stats),
            "miss_count": sum(int(item.get("miss_count") or 0) for item in stats),
            "refund_count": sum(int(item.get("refund_count") or 0) for item in stats),
            "plan_count": len(stats),
        }

    def create_subscription_record(self, **kwargs):
        item = {
            "id": len(self.subscriptions) + 1,
            "user_id": kwargs["user_id"],
            "source_id": kwargs["source_id"],
            "status": kwargs.get("status", "active"),
            "strategy": kwargs.get("strategy") or {},
            "created_at": "2026-04-07T12:00:00Z",
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.subscriptions.append(item)
        return self.get_subscription(item["id"])

    def list_message_templates(self, user_id):
        return [item for item in self.message_templates if item["user_id"] == int(user_id)]

    def create_message_template_record(self, **kwargs):
        item = {
            "id": len(self.message_templates) + 1,
            "user_id": kwargs["user_id"],
            "name": kwargs["name"],
            "lottery_type": kwargs["lottery_type"],
            "bet_type": kwargs.get("bet_type", "*"),
            "template_text": kwargs["template_text"],
            "config": kwargs.get("config") or {},
            "status": kwargs.get("status", "active"),
            "created_at": "2026-04-07T12:00:00Z",
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.message_templates.append(item)
        return item

    def get_message_template(self, template_id):
        for item in self.message_templates:
            if item["id"] == int(template_id):
                return item
        return None

    def update_message_template_record(self, *, template_id, user_id, name, lottery_type, bet_type, template_text, config=None):
        for item in self.message_templates:
            if item["id"] == int(template_id) and item["user_id"] == int(user_id):
                item["name"] = name
                item["lottery_type"] = lottery_type
                item["bet_type"] = bet_type
                item["template_text"] = template_text
                item["config"] = config or {}
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def update_message_template_status(self, *, template_id, user_id, status):
        for item in self.message_templates:
            if item["id"] == int(template_id) and item["user_id"] == int(user_id):
                item["status"] = str(status)
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def update_subscription_status(self, *, subscription_id, user_id, status):
        for item in self.subscriptions:
            if item["id"] == int(subscription_id) and item["user_id"] == int(user_id):
                item["status"] = str(status)
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def get_subscription(self, subscription_id):
        for item in self.subscriptions:
            if item["id"] == int(subscription_id):
                data = dict(item)
                progression = self.subscription_progression_states.get(int(subscription_id))
                pending = next(
                    (
                        event for event in reversed(self.progression_events)
                        if event["subscription_id"] == int(subscription_id) and event.get("status") in {"pending", "placed"}
                    ),
                    None,
                )
                if progression:
                    data["progression"] = {
                        "current_step": progression.get("current_step", 1),
                        "last_signal_id": progression.get("last_signal_id"),
                        "last_issue_no": progression.get("last_issue_no", ""),
                        "last_result_type": progression.get("last_result_type", ""),
                        "pending_event_id": pending.get("id") if pending else None,
                        "pending_issue_no": pending.get("issue_no") if pending else "",
                        "pending_status": pending.get("status") if pending else "",
                    }
                elif pending:
                    data["progression"] = {
                        "current_step": 1,
                        "last_signal_id": None,
                        "last_issue_no": "",
                        "last_result_type": "",
                        "pending_event_id": pending.get("id"),
                        "pending_issue_no": pending.get("issue_no") or "",
                        "pending_status": pending.get("status") or "",
                    }
                financial = self.subscription_financial_states.get(int(subscription_id))
                data["financial"] = {
                    "subscription_id": int(subscription_id),
                    "user_id": item["user_id"],
                    "realized_profit": float((financial or {}).get("realized_profit") or 0),
                    "realized_loss": float((financial or {}).get("realized_loss") or 0),
                    "net_profit": float((financial or {}).get("net_profit") or 0),
                    "threshold_status": str((financial or {}).get("threshold_status") or ""),
                    "stopped_reason": str((financial or {}).get("stopped_reason") or ""),
                    "baseline_reset_at": (financial or {}).get("baseline_reset_at"),
                    "baseline_reset_note": str((financial or {}).get("baseline_reset_note") or ""),
                    "last_settled_event_id": (financial or {}).get("last_settled_event_id"),
                    "last_settled_at": (financial or {}).get("last_settled_at"),
                    "updated_at": (financial or {}).get("updated_at"),
                }
                return data
        return None

    def update_subscription_record(self, *, subscription_id, user_id, source_id, strategy=None, status=None):
        for item in self.subscriptions:
            if item["id"] == int(subscription_id) and item["user_id"] == int(user_id):
                item["source_id"] = int(source_id)
                item["strategy"] = strategy or {}
                if status is not None:
                    item["status"] = status
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def delete_subscription_record(self, *, subscription_id, user_id):
        for index, item in enumerate(self.subscriptions):
            if item["id"] == int(subscription_id) and item["user_id"] == int(user_id):
                del self.subscriptions[index]
                return True
        return False

    def update_subscription_record(self, *, subscription_id, user_id, source_id, strategy=None, status=None):
        for item in self.subscriptions:
            if item["id"] == int(subscription_id) and item["user_id"] == int(user_id):
                item["source_id"] = int(source_id)
                item["strategy"] = strategy or {}
                if status is not None:
                    item["status"] = status
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def list_delivery_targets(self, user_id):
        return [item for item in self.targets if item["user_id"] == int(user_id)]

    def create_delivery_target_record(self, **kwargs):
        item = {
            "id": len(self.targets) + 1,
            "user_id": kwargs["user_id"],
            "telegram_account_id": kwargs.get("telegram_account_id"),
            "executor_type": kwargs["executor_type"],
            "target_key": kwargs["target_key"],
            "target_name": kwargs.get("target_name", ""),
            "template_id": kwargs.get("template_id"),
            "status": kwargs.get("status", "inactive"),
            "last_test_status": kwargs.get("last_test_status", ""),
            "last_test_error_code": kwargs.get("last_test_error_code", ""),
            "last_test_message": kwargs.get("last_test_message", ""),
            "last_tested_at": kwargs.get("last_tested_at"),
            "created_at": "2026-04-07T12:00:00Z",
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.targets.append(item)
        return item

    def update_delivery_target_status(self, *, delivery_target_id, user_id, status):
        for item in self.targets:
            if item["id"] == int(delivery_target_id) and item["user_id"] == int(user_id):
                item["status"] = str(status)
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def update_delivery_target_test_result(
        self,
        *,
        delivery_target_id,
        user_id,
        last_test_status,
        last_test_error_code="",
        last_test_message="",
        last_tested_at=None,
    ):
        for item in self.targets:
            if item["id"] == int(delivery_target_id) and item["user_id"] == int(user_id):
                item["last_test_status"] = str(last_test_status)
                item["last_test_error_code"] = str(last_test_error_code or "")
                item["last_test_message"] = str(last_test_message or "")
                item["last_tested_at"] = last_tested_at or "2026-04-07T12:00:00Z"
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def get_delivery_target(self, delivery_target_id):
        for item in self.targets:
            if item["id"] == int(delivery_target_id):
                return item
        return None

    def update_delivery_target_record(self, *, delivery_target_id, user_id, telegram_account_id, executor_type, target_key, target_name="", template_id=None):
        for item in self.targets:
            if item["id"] == int(delivery_target_id) and item["user_id"] == int(user_id):
                item["telegram_account_id"] = telegram_account_id
                item["executor_type"] = executor_type
                item["target_key"] = target_key
                item["target_name"] = target_name
                item["template_id"] = template_id
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def count_execution_jobs_by_delivery_target(self, delivery_target_id, *, user_id=None):
        return sum(
            1
            for item in self.jobs
            if item.get("delivery_target_id") == int(delivery_target_id)
            and (user_id is None or item["user_id"] == int(user_id))
        )

    def delete_delivery_target_record(self, *, delivery_target_id, user_id):
        for index, item in enumerate(self.targets):
            if item["id"] == int(delivery_target_id) and item["user_id"] == int(user_id):
                del self.targets[index]
                return True
        return False

    def update_delivery_target_record(self, *, delivery_target_id, user_id, telegram_account_id, executor_type, target_key, target_name="", template_id=None):
        for item in self.targets:
            if item["id"] == int(delivery_target_id) and item["user_id"] == int(user_id):
                item["telegram_account_id"] = telegram_account_id
                item["executor_type"] = executor_type
                item["target_key"] = target_key
                item["target_name"] = target_name
                item["template_id"] = template_id
                item["updated_at"] = "2026-04-07T12:00:00Z"
                return item
        return None

    def list_signals(self, source_id=None, owner_user_id=None):
        items = list(self.signals)
        if source_id is not None:
            items = [item for item in items if item["source_id"] == int(source_id)]
        if owner_user_id is not None:
            items = [
                item for item in items
                if (self.get_source(item["source_id"]) or {}).get("owner_user_id") == int(owner_user_id)
            ]
        return items

    def create_signal_record(self, **kwargs):
        item = {
            "id": len(self.signals) + 1,
            "source_id": kwargs["source_id"],
            "source_raw_item_id": kwargs.get("source_raw_item_id"),
            "lottery_type": kwargs["lottery_type"],
            "issue_no": kwargs["issue_no"],
            "bet_type": kwargs["bet_type"],
            "bet_value": kwargs["bet_value"],
            "confidence": kwargs.get("confidence"),
            "normalized_payload": kwargs.get("normalized_payload") or {},
            "status": kwargs.get("status", "ready"),
            "published_at": "2026-04-07T12:00:00Z",
            "created_at": "2026-04-07T12:00:00Z",
        }
        self.signals.append(item)
        return item

    def create_raw_item_record(self, **kwargs):
        item = {
            "id": len(self.raw_items) + 1,
            "source_id": kwargs["source_id"],
            "external_item_id": kwargs.get("external_item_id"),
            "issue_no": kwargs.get("issue_no", ""),
            "published_at": kwargs.get("published_at") or "2026-04-07T12:00:00Z",
            "raw_payload": kwargs.get("raw_payload") or {},
            "parse_status": kwargs.get("parse_status", "pending"),
            "parse_error": kwargs.get("parse_error"),
            "created_at": "2026-04-07T12:00:00Z",
        }
        self.raw_items.append(item)
        return item

    def get_raw_item(self, raw_item_id):
        for item in self.raw_items:
            if item["id"] == int(raw_item_id):
                return item
        return None

    def raw_item_belongs_to_user(self, raw_item_id, user_id):
        item = self.get_raw_item(raw_item_id)
        if not item:
            return False
        source = self.get_source(item["source_id"]) or {}
        return source.get("owner_user_id") == int(user_id)

    def list_raw_items(self, source_id=None, owner_user_id=None):
        items = list(self.raw_items)
        if source_id is not None:
            items = [item for item in items if item["source_id"] == int(source_id)]
        if owner_user_id is not None:
            items = [
                item for item in items
                if (self.get_source(item["source_id"]) or {}).get("owner_user_id") == int(owner_user_id)
            ]
        return items

    def update_raw_item_parse_result(self, raw_item_id, **kwargs):
        item = self.get_raw_item(raw_item_id)
        if not item:
            return None
        item["parse_status"] = kwargs.get("parse_status", item["parse_status"])
        item["parse_error"] = kwargs.get("parse_error", item.get("parse_error"))
        return item

    def get_signal(self, signal_id):
        for item in self.signals:
            if item["id"] == int(signal_id):
                return item
        return None

    def signal_belongs_to_user(self, signal_id, user_id):
        item = self.get_signal(signal_id)
        if not item:
            return False
        source = self.get_source(item["source_id"]) or {}
        return source.get("owner_user_id") == int(user_id)

    def list_dispatch_candidates(self, signal_id):
        signal = self.get_signal(signal_id)
        if not signal:
            return []
        items = []
        for subscription in self.subscriptions:
            if subscription["source_id"] != signal["source_id"] or subscription["status"] != "active":
                continue
            financial = self.subscription_financial_states.get(int(subscription["id"])) or {}
            if str(financial.get("threshold_status") or ""):
                continue
            for target in self.targets:
                if target["user_id"] != subscription["user_id"] or target["status"] != "active":
                    continue
                account_id = target.get("telegram_account_id")
                if account_id is not None:
                    account = self.get_telegram_account(account_id)
                    if account and account.get("status") != "active":
                        continue
                items.append(
                    {
                        "signal_id": signal["id"],
                        "source_id": signal["source_id"],
                        "issue_no": signal["issue_no"],
                        "bet_type": signal["bet_type"],
                        "bet_value": signal["bet_value"],
                        "lottery_type": signal["lottery_type"],
                        "normalized_payload": signal["normalized_payload"],
                        "published_at": signal["published_at"],
                        "subscription_id": subscription["id"],
                        "user_id": subscription["user_id"],
                        "strategy_json": subscription["strategy"],
                        "delivery_target_id": target["id"],
                        "telegram_account_id": target.get("telegram_account_id"),
                        "template_id": target.get("template_id"),
                        "executor_type": target["executor_type"],
                        "target_key": target["target_key"],
                        "target_name": target["target_name"],
                    }
                )
        return items

    def create_execution_job_record(self, **kwargs):
        for item in self.jobs:
            if item["user_id"] == kwargs["user_id"] and item["idempotency_key"] == kwargs["idempotency_key"]:
                return {"created": False, "job": item}
        job = {
            "id": len(self.jobs) + 1,
            "user_id": kwargs["user_id"],
            "signal_id": kwargs["signal_id"],
            "subscription_id": kwargs.get("subscription_id"),
            "progression_event_id": kwargs.get("progression_event_id"),
            "delivery_target_id": kwargs["delivery_target_id"],
            "telegram_account_id": kwargs.get("telegram_account_id"),
            "executor_type": kwargs["executor_type"],
            "idempotency_key": kwargs["idempotency_key"],
            "planned_message_text": kwargs["planned_message_text"],
            "stake_plan": kwargs.get("stake_plan") or {},
            "execute_after": kwargs["execute_after"],
            "expire_at": kwargs["expire_at"],
            "status": kwargs.get("status", "pending"),
            "error_message": kwargs.get("error_message"),
            "created_at": "2026-04-07T12:00:00Z",
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.jobs.append(job)
        return {"created": True, "job": job}

    def get_subscription_progression_state(self, subscription_id):
        return self.subscription_progression_states.get(int(subscription_id), {
            "subscription_id": int(subscription_id),
            "user_id": None,
            "current_step": 1,
            "last_signal_id": None,
            "last_issue_no": "",
            "last_result_type": "",
            "updated_at": None,
        })

    def get_subscription_financial_state(self, subscription_id):
        return self.subscription_financial_states.get(int(subscription_id), {
            "subscription_id": int(subscription_id),
            "user_id": None,
            "realized_profit": 0.0,
            "realized_loss": 0.0,
            "net_profit": 0.0,
            "threshold_status": "",
            "stopped_reason": "",
            "baseline_reset_at": None,
            "baseline_reset_note": "",
            "last_settled_event_id": None,
            "last_settled_at": None,
            "updated_at": None,
        })

    def get_progression_event_by_signal(self, *, subscription_id, signal_id):
        for item in self.progression_events:
            if item["subscription_id"] == int(subscription_id) and item["signal_id"] == int(signal_id):
                return item
        return None

    def create_progression_event_record(self, **kwargs):
        item = {"id": len(self.progression_events) + 1, **kwargs}
        self.progression_events.append(item)
        return item

    def get_progression_event(self, progression_event_id):
        for item in self.progression_events:
            if item["id"] == int(progression_event_id):
                return item
        return None

    def get_latest_pending_progression_event(self, *, subscription_id):
        for item in reversed(self.progression_events):
            if item["subscription_id"] == int(subscription_id) and item.get("status") in {"pending", "placed"}:
                return item
        return None

    def list_open_progression_events(self, *, user_id, statuses=None, limit=5000):
        normalized_statuses = {str(item or "").strip() for item in (statuses or ["pending", "placed"]) if str(item or "").strip()}
        items = [
            dict(item)
            for item in self.progression_events
            if item["user_id"] == int(user_id) and str(item.get("status") or "") in normalized_statuses
        ]
        items.sort(key=lambda item: int(item.get("id") or 0), reverse=True)
        return items[: max(0, int(limit or 0))]

    def upsert_subscription_progression_state(self, *, subscription_id, user_id, current_step, last_signal_id=None, last_issue_no="", last_result_type=""):
        state = {
            "subscription_id": int(subscription_id),
            "user_id": int(user_id),
            "current_step": int(current_step),
            "last_signal_id": int(last_signal_id) if last_signal_id is not None else None,
            "last_issue_no": str(last_issue_no or ""),
            "last_result_type": str(last_result_type or ""),
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.subscription_progression_states[int(subscription_id)] = state
        return state

    def settle_progression_event(self, *, subscription_id, user_id, result_type, progression_event_id=None, result_context=None):
        event = self.get_progression_event(progression_event_id) if progression_event_id is not None else self.get_latest_pending_progression_event(subscription_id=subscription_id)
        if not event:
            raise ValueError("当前没有待结算的倍投状态")
        event["status"] = "settled"
        event["resolved_result_type"] = result_type
        if isinstance(result_context, dict):
            event["result_context"] = dict(result_context)
        subscription = self.get_subscription(subscription_id) or {}
        strategy = subscription.get("strategy") or {}
        risk_control = strategy.get("risk_control") if isinstance(strategy.get("risk_control"), dict) else {}
        win_profit_ratio = float(risk_control.get("win_profit_ratio") or 1.0)
        max_steps = max(1, int(event.get("max_steps") or 1))
        current_step = int(event.get("progression_step") or 1)
        if result_type == "hit":
            next_step = 1
        elif result_type == "refund":
            next_step = current_step if str(event.get("refund_action") or "hold") == "hold" else 1
        else:
            next_step = 1 if current_step >= max_steps else current_step + 1
        state = self.upsert_subscription_progression_state(
            subscription_id=subscription_id,
            user_id=user_id,
            current_step=next_step,
            last_signal_id=event.get("signal_id"),
            last_issue_no=event.get("issue_no") or "",
            last_result_type=result_type,
        )
        financial = dict(self.get_subscription_financial_state(subscription_id))
        stake_amount = float(event.get("stake_amount") or 0)
        if result_type == "hit":
            financial["realized_profit"] = round(float(financial.get("realized_profit") or 0) + stake_amount * win_profit_ratio, 2)
            financial["net_profit"] = round(float(financial.get("net_profit") or 0) + stake_amount * win_profit_ratio, 2)
        elif result_type == "miss":
            financial["realized_loss"] = round(float(financial.get("realized_loss") or 0) + stake_amount, 2)
            financial["net_profit"] = round(float(financial.get("net_profit") or 0) - stake_amount, 2)
        financial["subscription_id"] = int(subscription_id)
        financial["user_id"] = int(user_id)
        financial["last_settled_event_id"] = event["id"]
        financial["last_settled_at"] = "2026-04-07T12:00:00Z"
        financial["updated_at"] = "2026-04-07T12:00:00Z"
        threshold_status = ""
        stopped_reason = ""
        if bool(risk_control.get("enabled")):
            profit_target = float(risk_control.get("profit_target") or 0)
            loss_limit = float(risk_control.get("loss_limit") or 0)
            if profit_target > 0 and float(financial["net_profit"]) >= profit_target:
                threshold_status = "profit_target_hit"
                stopped_reason = "达到止盈阈值，当前轮次已停止"
            elif loss_limit > 0 and float(financial["net_profit"]) <= -loss_limit:
                threshold_status = "loss_limit_hit"
                stopped_reason = "达到止损阈值，当前轮次已停止"
        financial["threshold_status"] = threshold_status
        financial["stopped_reason"] = stopped_reason
        self.subscription_financial_states[int(subscription_id)] = financial
        return {"event": event, "state": state, "financial": financial, "subscription": self.get_subscription(subscription_id)}

    def reset_subscription_runtime(self, *, subscription_id, user_id, note=""):
        current = self.get_subscription(subscription_id)
        if not current or int(current["user_id"]) != int(user_id):
            raise ValueError("subscription_id 对应的订阅不存在")
        voided_event_ids = []
        for event in self.progression_events:
            if event["subscription_id"] == int(subscription_id) and event.get("status") in {"pending", "placed"}:
                event["status"] = "reset"
                event["resolved_result_type"] = "reset"
                voided_event_ids.append(event["id"])
        for job in self.jobs:
            if job.get("subscription_id") == int(subscription_id) and job.get("status") == "pending":
                job["status"] = "skipped"
                job["error_message"] = "策略已重置，旧轮次未执行任务已跳过"
        self.subscription_progression_states[int(subscription_id)] = {
            "subscription_id": int(subscription_id),
            "user_id": int(user_id),
            "current_step": 1,
            "last_signal_id": None,
            "last_issue_no": "",
            "last_result_type": "",
            "updated_at": "2026-04-07T12:00:00Z",
        }
        self.subscription_financial_states[int(subscription_id)] = {
            "subscription_id": int(subscription_id),
            "user_id": int(user_id),
            "realized_profit": 0.0,
            "realized_loss": 0.0,
            "net_profit": 0.0,
            "threshold_status": "",
            "stopped_reason": "",
            "baseline_reset_at": "2026-04-07T12:00:00Z",
            "baseline_reset_note": str(note or ""),
            "last_settled_event_id": None,
            "last_settled_at": None,
            "updated_at": "2026-04-07T12:00:00Z",
        }
        return {
            "item": self.get_subscription(subscription_id),
            "progression": self.get_subscription_progression_state(subscription_id),
            "financial": self.get_subscription_financial_state(subscription_id),
            "reset_at": "2026-04-07T12:00:00Z",
            "reset_note": str(note or ""),
            "voided_event_ids": voided_event_ids,
        }

    def list_execution_jobs(self, *, user_id=None, signal_id=None, status=None, limit=100):
        items = [item for item in self.jobs if user_id is None or item["user_id"] == int(user_id)]
        if signal_id is not None:
            items = [item for item in items if item["signal_id"] == int(signal_id)]
        if status:
            items = [item for item in items if item["status"] == status]

        enriched = []
        for item in items[:limit]:
            signal = self.get_signal(item["signal_id"]) or {}
            target = next((candidate for candidate in self.targets if candidate["id"] == item["delivery_target_id"]), {})
            account = (
                self.get_telegram_account(item["telegram_account_id"])
                if item.get("telegram_account_id") is not None
                else None
            )
            attempts = [attempt for attempt in self.reports if str(attempt["job_id"]) == str(item["id"])]
            last_attempt = attempts[-1] if attempts else {}
            enriched.append(
                {
                    **item,
                    "lottery_type": signal.get("lottery_type", ""),
                    "issue_no": signal.get("issue_no", ""),
                    "bet_type": signal.get("bet_type", ""),
                    "bet_value": signal.get("bet_value", ""),
                    "target_key": target.get("target_key", ""),
                    "target_name": target.get("target_name", ""),
                    "telegram_account_label": account.get("label", "") if account else "",
                    "attempt_count": len(attempts),
                    "last_attempt_no": last_attempt.get("attempt_no"),
                    "last_delivery_status": last_attempt.get("delivery_status"),
                    "last_remote_message_id": last_attempt.get("remote_message_id"),
                    "last_error_message": last_attempt.get("error_message"),
                    "last_executed_at": last_attempt.get("executed_at"),
                    "last_executor_instance_id": last_attempt.get("executor_id"),
                }
            )
        return enriched

    def list_executor_instances(self, *, limit=50):
        enriched = []
        for item in self.executors[:limit]:
            attempts = [attempt for attempt in self.reports if attempt["executor_id"] == item["executor_id"]]
            last_failure = next(
                (
                    attempt for attempt in reversed(attempts)
                    if attempt["delivery_status"] in {"failed", "expired", "skipped"}
                ),
                None,
            )
            enriched.append(
                {
                    **item,
                    "total_attempt_count": len(attempts),
                    "delivered_attempt_count": len([attempt for attempt in attempts if attempt["delivery_status"] == "delivered"]),
                    "failed_attempt_count": len(
                        [attempt for attempt in attempts if attempt["delivery_status"] in {"failed", "expired", "skipped"}]
                    ),
                    "last_executed_at": attempts[-1]["executed_at"] if attempts else None,
                    "last_failure_at": last_failure["executed_at"] if last_failure else None,
                    "last_failure_status": last_failure["delivery_status"] if last_failure else None,
                    "last_failure_error_message": last_failure["error_message"] if last_failure else None,
                }
            )
        return enriched

    def list_executor_attempts(self, *, executor_id, limit=20):
        attempts = [attempt for attempt in self.reports if attempt["executor_id"] == executor_id]
        attempts = sorted(attempts, key=lambda item: item["executed_at"], reverse=True)
        return [
            {
                "id": index + 1,
                "job_id": int(attempt["job_id"]) if str(attempt["job_id"]).isdigit() else 0,
                "executor_instance_id": attempt["executor_id"],
                "attempt_no": attempt["attempt_no"],
                "delivery_status": attempt["delivery_status"],
                "remote_message_id": attempt["remote_message_id"],
                "raw_result": attempt["raw_result"],
                "error_message": attempt["error_message"],
                "executed_at": attempt["executed_at"],
                "created_at": attempt["executed_at"],
            }
            for index, attempt in enumerate(attempts[:limit])
        ]

    def list_recent_execution_failures(self, *, user_id=None, limit=20):
        items = []
        for job in self.jobs:
            if user_id is not None and job["user_id"] != int(user_id):
                continue
            if job["status"] not in {"failed", "expired", "skipped"}:
                continue
            signal = self.get_signal(job["signal_id"]) or {}
            target = next((candidate for candidate in self.targets if candidate["id"] == job["delivery_target_id"]), {})
            account = (
                self.get_telegram_account(job["telegram_account_id"])
                if job.get("telegram_account_id") is not None
                else None
            )
            attempts = [attempt for attempt in self.reports if str(attempt["job_id"]) == str(job["id"])]
            last_failure = next(
                (
                    attempt for attempt in reversed(attempts)
                    if attempt["delivery_status"] in {"failed", "expired", "skipped"}
                ),
                None,
            )
            if not last_failure:
                continue
            items.append(
                {
                    "job_id": job["id"],
                    "user_id": job["user_id"],
                    "signal_id": job["signal_id"],
                    "delivery_target_id": job["delivery_target_id"],
                    "telegram_account_id": job.get("telegram_account_id"),
                    "job_status": job["status"],
                    "planned_message_text": job["planned_message_text"],
                    "lottery_type": signal.get("lottery_type", ""),
                    "issue_no": signal.get("issue_no", ""),
                    "bet_type": signal.get("bet_type", ""),
                    "bet_value": signal.get("bet_value", ""),
                    "target_key": target.get("target_key", ""),
                    "target_name": target.get("target_name", ""),
                    "telegram_account_label": account.get("label", "") if account else "",
                    "executor_instance_id": last_failure["executor_id"],
                    "attempt_no": last_failure["attempt_no"],
                    "delivery_status": last_failure["delivery_status"],
                    "remote_message_id": last_failure["remote_message_id"],
                    "error_message": last_failure["error_message"],
                    "executed_at": last_failure["executed_at"],
                    "attempt_count": len(attempts),
                    "raw_result": last_failure["raw_result"],
                }
            )
        items.sort(key=lambda item: item["executed_at"], reverse=True)
        return items[:limit]

    def get_execution_job(self, job_id):
        for item in self.jobs:
            if item["id"] == int(job_id):
                return item
        return None

    def retry_execution_job(self, *, job_id, user_id):
        for item in self.jobs:
            if item["id"] == int(job_id) and item["user_id"] == int(user_id):
                if item["status"] not in {"failed", "expired", "skipped"}:
                    raise ValueError("当前任务状态不支持重试")
                item["status"] = "pending"
                item["error_message"] = None
                item["updated_at"] = "2026-04-08T12:00:00Z"
                item["execute_after"] = "2026-04-08T12:00:00Z"
                item["expire_at"] = "2026-04-08T12:02:00Z"
                return item
        raise ValueError("execution_job 不存在")


def invoke(app, environ):
    captured = {"status": None, "headers": None}

    def start_response(status, headers):
        captured["status"] = status
        captured["headers"] = headers

    body = b"".join(app(environ, start_response))
    return captured["status"], dict(captured["headers"]), json.loads(body.decode("utf-8"))


class PlatformApiApplicationTests(unittest.TestCase):
    def setUp(self):
        self.repository = FakeRepository()
        self.app = PlatformApiApplication(
            repository=self.repository,
            executor_api_token="secret",
            session_secret="session-secret",
        )
        self.executor_headers = {
            "HTTP_AUTHORIZATION": "Bearer secret",
            "HTTP_X_EXECUTOR_ID": "executor-001",
        }
        cookie_value = build_session_cookie_value(1, "session-secret")
        self.session_headers = {
            "HTTP_COOKIE": "%s=%s" % (SESSION_COOKIE_NAME, cookie_value),
        }

    def session_headers_for(self, user_id):
        user = self.repository.get_user(user_id) or {}
        cookie_value = build_session_cookie_value(
            int(user_id),
            "session-secret",
            session_version=int(user.get("session_version") or 1),
        )
        return {
            "HTTP_COOKIE": "%s=%s" % (SESSION_COOKIE_NAME, cookie_value),
        }

    def test_health_endpoint_does_not_require_auth(self):
        status, headers, payload = invoke(self.app, build_testing_environ("/api/health"))
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload, {"status": "ok"})
        self.assertEqual(headers["Content-Type"], "application/json; charset=utf-8")

    def test_home_page_renders_html(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("自动投注总览", body)
        self.assertIn("进入自动投注总控台", body)
        self.assertIn("自动投注总控台", body)
        self.assertIn("/autobet/sources#sourcesSection", body)
        self.assertIn("/autobet/accounts#accountsSection", body)
        self.assertIn("/autobet/targets#targetsSection", body)
        self.assertIn("/autobet/templates#templatesSection", body)
        self.assertIn("/autobet/subscriptions#subscriptionsSection", body)
        self.assertIn("快捷导入方案", body)
        self.assertIn("下一步", body)

    def test_home_page_injects_shared_account_dialog_and_versions_shared_assets(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertIn('id="accountDialog"', body)
        self.assertIn('id="authenticatedAccountPanel"', body)
        self.assertNotIn("__ACCOUNT_DIALOG__", body)
        for asset_name in ["ui-text.js", "auth-panel.js", "account-menu.js", "auth-guard.js", "home.js"]:
            match = re.search(r"/assets/%s\?v=([0-9]+)" % re.escape(asset_name), body)
            self.assertIsNotNone(match, asset_name)
            self.assertNotEqual(match.group(1), "0", asset_name)

    def test_shared_account_dialog_template_is_injected_for_all_pages(self):
        for page_name in ["home.html", "records.html", "alerts.html", "autobet.html", "auto_triggers.html", "dashboard.html"]:
            text = _load_ui_html_file(page_name)
            self.assertNotIn("__ACCOUNT_DIALOG__", text, page_name)
            self.assertIn('id="accountDialog"', text, page_name)
            self.assertIn('id="authenticatedAccountPanel"', text, page_name)

    def test_shared_asset_versions_resolve_real_files(self):
        for asset_name in ["ui-text.js", "auth-panel.js", "account-menu.js", "auth-guard.js"]:
            self.assertNotEqual(_asset_version(asset_name), "0", asset_name)

    def test_admin_page_requires_authenticated_session(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/admin"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "401 Unauthorized")
        self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("后台访问受限", body)
        self.assertIn("请先登录后再进入后台控制台", body)

    def test_admin_page_renders_html_for_authenticated_session(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/admin", headers=self.session_headers), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("值班总览", body)
        self.assertIn("来源接入中心", body)
        self.assertIn("执行中心", body)
        self.assertIn("支持查询页", body)

    def test_admin_page_rejects_non_admin_session(self):
        regular_user = self.repository.create_user_record(
            username="normal-user",
            email="normal@example.com",
            password_hash=hash_password("normal-pass"),
            role="user",
            status="active",
        )
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(
            self.app(build_testing_environ("/admin", headers=self.session_headers_for(regular_user["id"])), start_response)
        ).decode("utf-8")
        self.assertEqual(captured["status"], "403 Forbidden")
        self.assertIn("不是管理员", body)

    def test_admin_subpage_renders_same_shell(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/admin/execution", headers=self.session_headers), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("后台控制台", body)

    def test_admin_telegram_page_uses_versioned_assets_and_no_store(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/admin/telegram", headers=self.session_headers), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
        self.assertEqual(captured["headers"]["Cache-Control"], "no-store, max-age=0")
        self.assertIn("Telegram 配置", body)
        self.assertIn("/assets/dashboard.js?v=", body)
        self.assertIn("执行中心", body)

    def test_admin_support_snapshot_returns_cross_user_objects(self):
        user_two = self.repository.create_user_record(
            username="support-user",
            email="support@example.com",
            role="user",
            status="active",
        )
        source = self.repository.create_source_record(
            owner_user_id=user_two["id"],
            source_type="telegram_channel",
            name="support-source",
            visibility="private",
            config={"chat_id": "@support"},
        )
        account = self.repository.create_telegram_account_record(
            user_id=user_two["id"],
            label="支持账号",
            phone="+12016660000",
            session_path="/data/support/account",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        template = self.repository.create_message_template_record(
            user_id=user_two["id"],
            name="支持模板",
            lottery_type="pc28",
            bet_type="*",
            template_text="{{bet_value}}{{amount}}",
            config={"bet_rules": {}},
        )
        self.repository.create_subscription_record(
            user_id=user_two["id"],
            source_id=source["id"],
            strategy={"mode": "follow", "stake_amount": 18},
        )
        self.repository.create_delivery_target_record(
            user_id=user_two["id"],
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100555",
            target_name="支持群",
            template_id=template["id"],
            status="active",
            last_test_status="success",
            last_tested_at="2026-04-08T12:00:00Z",
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/admin/support",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertTrue(any(item["username"] == "owner" for item in payload["users"]))
        self.assertTrue(any(item["username"] == "support-user" for item in payload["users"]))
        self.assertTrue(any(item["label"] == "支持账号" for item in payload["accounts"]))
        self.assertTrue(any(item["target_name"] == "支持群" for item in payload["targets"]))
        self.assertTrue(any(item["source_name"] == "support-source" for item in payload["subscriptions"]))
        self.assertTrue(any(item["name"] == "支持模板" for item in payload["templates"]))

    def test_records_page_renders_html(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/records"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("执行记录与异常回看", body)
        self.assertIn("记录概览", body)
        self.assertIn("执行记录列表", body)
        self.assertIn("记录页处理入口", body)
        self.assertIn("自动投注总控台", body)
        self.assertIn("/autobet/sources#sourcesSection", body)
        self.assertIn("/autobet/accounts#accountsSection", body)
        self.assertIn("/autobet/targets#targetsSection", body)
        self.assertIn("/autobet/subscriptions#subscriptionsSection", body)

    def test_alerts_page_renders_html(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/alerts"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("异常提醒与人工处理", body)
        self.assertIn("告警概览", body)
        self.assertIn("告警列表", body)
        self.assertIn("告警页处理入口", body)
        self.assertIn("自动投注总控台", body)
        self.assertIn("/autobet/sources#sourcesSection", body)
        self.assertIn("/autobet/accounts#accountsSection", body)
        self.assertIn("/autobet/targets#targetsSection", body)
        self.assertIn("/records", body)

    def test_autobet_page_renders_html(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/autobet"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
        self.assertIn("自动投注总控台", body)
        self.assertIn("当前版本的配置语义", body)
        self.assertIn("自动投注状态", body)
        self.assertIn("运行阻塞与建议", body)
        self.assertIn("优先处理动作", body)
        self.assertIn("最近执行反馈", body)
        self.assertIn("只看失败项", body)
        self.assertIn("最近异常提醒", body)
        self.assertIn("当前配置与运行上下文", body)
        self.assertIn("投递群组", body)
        self.assertIn("跟单策略", body)

    def test_autobet_includes_execution_flow_and_workspaces(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/autobet"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertIn("执行链路", body)
        self.assertIn("工作区", body)
        self.assertIn("进入各工作区处理问题", body)
        self.assertIn("/autobet/sources#sourcesSection", body)
        self.assertIn("/autobet/accounts#accountsSection", body)
        self.assertIn("/autobet/targets#targetsSection", body)
        self.assertIn("/autobet/templates#templatesSection", body)


    def test_autobet_focus_routes_render_html(self):
        for path in ("/autobet/sources", "/autobet/accounts", "/autobet/templates", "/autobet/targets", "/autobet/subscriptions"):
            captured = {"status": None, "headers": None}

            def start_response(status, headers):
                captured["status"] = status
                captured["headers"] = dict(headers)

            body = b"".join(self.app(build_testing_environ(path), start_response)).decode("utf-8")
            self.assertEqual(captured["status"], "200 OK")
            self.assertEqual(captured["headers"]["Content-Type"], "text/html; charset=utf-8")
            self.assertIn("自动投注总控台", body)

    def test_autobet_sources_workspace_renders_source_copy(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/autobet/sources"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertIn("方案来源", body)
        self.assertIn("来源链路工作区", body)
        self.assertIn("/autobet/sources#sourcesSection", body)

    def test_autobet_accounts_workspace_renders_account_copy(self):
        captured = {"status": None, "headers": None}

        def start_response(status, headers):
            captured["status"] = status
            captured["headers"] = dict(headers)

        body = b"".join(self.app(build_testing_environ("/autobet/accounts"), start_response)).decode("utf-8")
        self.assertEqual(captured["status"], "200 OK")
        self.assertIn("托管账号", body)
        self.assertIn("托管账号接入、授权进度和账号可执行状态", body)

    def test_ui_assets_render(self):
        for path, content_type in (
            ("/assets/home.css", "text/css; charset=utf-8"),
            ("/assets/home.js", "application/javascript; charset=utf-8"),
            ("/assets/records.css", "text/css; charset=utf-8"),
            ("/assets/records.js", "application/javascript; charset=utf-8"),
            ("/assets/alerts.css", "text/css; charset=utf-8"),
            ("/assets/alerts.js", "application/javascript; charset=utf-8"),
            ("/assets/autobet.css", "text/css; charset=utf-8"),
            ("/assets/autobet.js", "application/javascript; charset=utf-8"),
            ("/assets/dashboard.css", "text/css; charset=utf-8"),
            ("/assets/dashboard.js", "application/javascript; charset=utf-8"),
        ):
            captured = {"status": None, "headers": None}

            def start_response(status, headers):
                captured["status"] = status
                captured["headers"] = dict(headers)

            body = b"".join(self.app(build_testing_environ(path), start_response)).decode("utf-8")
            self.assertEqual(captured["status"], "200 OK")
            self.assertEqual(captured["headers"]["Content-Type"], content_type)
            self.assertTrue(body)

    def test_auth_register_and_me(self):
        create_status, headers, create_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/auth/register",
                method="POST",
                body={
                    "username": "alice",
                    "email": "alice@example.com",
                    "password": "secret",
                },
            ),
        )
        self.assertEqual(create_status, "200 OK")
        self.assertEqual(create_payload["user"]["username"], "alice")
        self.assertIn("Set-Cookie", headers)

        me_status, _, me_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/auth/me",
                headers={"HTTP_COOKIE": headers["Set-Cookie"].split(";", 1)[0]},
            ),
        )
        self.assertEqual(me_status, "200 OK")
        self.assertEqual(me_payload["user"]["username"], "alice")

    def test_platform_telegram_binding_endpoints(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-binding",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertFalse(payload["item"]["is_bound"])

        token_status, _, token_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-binding/token",
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(token_status, "200 OK")
        self.assertTrue(token_payload["item"]["has_active_bind_token"])
        self.assertTrue(token_payload["item"]["bind_token"])

        self.repository.update_user_telegram_binding(
            user_id=1,
            telegram_user_id=9001,
            telegram_chat_id="9001",
            telegram_username="owner_tg",
            telegram_bound_at="2026-04-17T12:00:00Z",
        )
        unbind_status, _, unbind_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-binding/unbind",
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(unbind_status, "200 OK")
        self.assertFalse(unbind_payload["item"]["is_bound"])

    def test_admin_telegram_settings_endpoints(self):
        get_status, _, get_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/admin/telegram-settings",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(get_status, "200 OK")
        self.assertIn("alert", get_payload["item"])
        self.assertIn("bot", get_payload["item"])
        self.assertIn("report", get_payload["item"])
        self.assertIn("auto_settlement", get_payload["item"])

        save_status, _, save_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/admin/telegram-settings",
                method="POST",
                headers=self.session_headers,
                body={
                    "alert": {
                        "enabled": True,
                        "bot_token": "alert-bot-token",
                        "target_chat_id": "-100alert",
                        "repeat_interval_seconds": 900,
                        "interval_seconds": 15,
                    },
                    "bot": {
                        "enabled": True,
                        "bot_token": "query-bot-token",
                        "poll_interval_seconds": 7,
                        "bind_token_ttl_seconds": 1200,
                    },
                    "report": {
                        "enabled": True,
                        "target_chat_id": "-100report",
                        "interval_seconds": 45,
                        "send_hour": 11,
                        "send_minute": 35,
                        "top_n": 12,
                        "timezone": "Asia/Shanghai",
                    },
                    "auto_settlement": {
                        "enabled": True,
                        "interval_seconds": 20,
                        "draw_limit": 80,
                    },
                },
            ),
        )
        self.assertEqual(save_status, "200 OK")
        self.assertTrue(save_payload["item"]["alert"]["has_bot_token"])
        self.assertEqual(save_payload["item"]["report"]["target_chat_id"], "-100report")
        self.assertTrue(save_payload["item"]["auto_settlement"]["enabled"])
        stored = self.repository.get_platform_runtime_setting("telegram_runtime_settings")
        self.assertEqual(stored["value"]["bot"]["poll_interval_seconds"], 7)
        self.assertEqual(stored["value"]["report"]["top_n"], 12)
        self.assertEqual(stored["value"]["auto_settlement"]["draw_limit"], 80)

    def test_auth_register_can_initialize_legacy_user_without_password(self):
        self.repository.users.append(
            {
                "id": 99,
                "username": "legacy-user",
                "email": "legacy@example.com",
                "password_hash": "",
                "role": "user",
                "status": "active",
                "created_at": "2026-04-07T12:00:00Z",
                "updated_at": "2026-04-07T12:00:00Z",
            }
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/auth/register",
                method="POST",
                body={
                    "username": "legacy-user",
                    "email": "legacy@example.com",
                    "password": "secret",
                },
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertIn("初始化", payload["message"])
        self.assertTrue(self.repository.get_user_by_username("legacy-user")["password_hash"])

    def test_auth_login_rejects_legacy_user_without_password(self):
        self.repository.users.append(
            {
                "id": 100,
                "username": "legacy-empty",
                "email": "legacy-empty@example.com",
                "password_hash": "",
                "role": "user",
                "status": "active",
                "created_at": "2026-04-07T12:00:00Z",
                "updated_at": "2026-04-07T12:00:00Z",
            }
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/auth/login",
                method="POST",
                body={
                    "username": "legacy-empty",
                    "password": "secret",
                },
            ),
        )
        self.assertEqual(status, "401 Unauthorized")
        self.assertIn("尚未设置密码", payload["error"])

    def test_auth_change_password_invalidates_old_cookie(self):
        old_headers = dict(self.session_headers)
        status, headers, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/auth/change-password",
                method="POST",
                headers=old_headers,
                body={
                    "current_password": "owner-pass",
                    "new_password": "owner-pass-2",
                    "confirm_password": "owner-pass-2",
                },
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertIn("Set-Cookie", headers)
        self.assertIn("失效", payload["message"])

        expired_status, _, expired_payload = invoke(
            self.app,
            build_testing_environ("/api/auth/me", headers=old_headers),
        )
        self.assertEqual(expired_status, "401 Unauthorized")
        self.assertEqual(expired_payload["error"], "未登录")

        next_cookie = {"HTTP_COOKIE": headers["Set-Cookie"].split(";", 1)[0]}
        next_status, _, next_payload = invoke(
            self.app,
            build_testing_environ("/api/auth/me", headers=next_cookie),
        )
        self.assertEqual(next_status, "200 OK")
        self.assertEqual(next_payload["user"]["username"], "owner")

    def test_auth_change_password_requires_current_password(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/auth/change-password",
                method="POST",
                headers=self.session_headers,
                body={
                    "current_password": "wrong-pass",
                    "new_password": "owner-pass-2",
                    "confirm_password": "owner-pass-2",
                },
            ),
        )
        self.assertEqual(status, "401 Unauthorized")
        self.assertIn("当前密码错误", payload["error"])

    def test_fetch_source_endpoint_returns_raw_item(self):
        with patch("pc28touzhu.api.app.fetch_source") as mocked_fetch_source:
            mocked_fetch_source.return_value = {
                "source": {"id": 1, "name": "remote"},
                "raw_item": {"id": 5, "source_id": 1, "parse_status": "pending"},
            }
            status, _, payload = invoke(
                self.app,
                build_testing_environ(
                    "/api/platform/sources/1/fetch",
                    method="POST",
                    body={},
                    headers=self.session_headers,
                ),
            )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["raw_item"]["id"], 5)
        mocked_fetch_source.assert_called_once()

    def test_pull_jobs_requires_auth(self):
        status, _, payload = invoke(self.app, build_testing_environ("/api/executor/jobs/pull"))
        self.assertEqual(status, "401 Unauthorized")
        self.assertEqual(payload["error"], "unauthorized")

    def test_pull_jobs_returns_items(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/executor/jobs/pull",
                headers=self.executor_headers,
                query="limit=1",
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["job_id"], "job_001")

    def test_heartbeat_endpoint_records_payload(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/executor/heartbeat",
                method="POST",
                body={"version": "0.1.0", "capabilities": {"send": True}},
                headers=self.executor_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["executor_id"], "executor-001")
        self.assertEqual(self.repository.heartbeats[0]["version"], "0.1.0")

    def test_list_executors_returns_heartbeat_summary(self):
        self.repository.upsert_executor_heartbeat(
            executor_id="executor-001",
            version="0.2.0",
            capabilities={"send": True, "provider": "telethon"},
            status="online",
            last_seen_at="2026-04-08T12:00:00Z",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/executors",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["executor_id"], "executor-001")
        self.assertIn("heartbeat_status", payload["items"][0])
        self.assertIn("recent_failure_streak", payload["items"][0])
        self.assertTrue(payload["items"][0]["capabilities"]["send"])

    def test_report_endpoint_validates_payload(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/executor/jobs/job_001/report",
                method="POST",
                body={
                    "executor_id": "executor-001",
                    "attempt_no": 1,
                    "delivery_status": "delivered",
                    "remote_message_id": "123",
                    "executed_at": "2026-04-07T15:00:08Z",
                    "raw_result": {"chat_id": "-1001234567890"},
                },
                headers=self.executor_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["delivery_status"], "delivered")
        self.assertEqual(self.repository.reports[0]["job_id"], "job_001")

    def test_report_endpoint_rejects_invalid_status(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/executor/jobs/job_001/report",
                method="POST",
                body={
                    "executor_id": "executor-001",
                    "attempt_no": 1,
                    "delivery_status": "unknown",
                    "executed_at": "2026-04-07T15:00:08Z",
                    "raw_result": {},
                },
                headers=self.executor_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("delivery_status", payload["error"])

    def test_list_sources_returns_items(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources",
                headers=self.session_headers,
                query="owner_user_id=1",
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["name"], "demo-source")

    def test_list_sources_scope_all_requires_admin(self):
        regular_user = self.repository.create_user_record(
            username="scope-user",
            email="scope@example.com",
            password_hash=hash_password("scope-pass"),
            role="user",
            status="active",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources",
                headers=self.session_headers_for(regular_user["id"]),
                query="scope=all",
            ),
        )
        self.assertEqual(status, "403 Forbidden")
        self.assertIn("管理员", payload["error"])

    def test_list_users_scope_all_returns_all_users(self):
        self.repository.create_user_record(username="second", email="second@example.com", role="user", status="active")

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/users",
                headers=self.session_headers,
                query="scope=all",
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(len(payload["items"]), 2)

    def test_list_sources_scope_all_returns_other_owner_items(self):
        self.repository.create_source_record(
            owner_user_id=2,
            source_type="telegram_channel",
            name="foreign-source",
            visibility="public",
            config={"chat_id": "@demo"},
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources",
                headers=self.session_headers,
                query="scope=all",
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(len(payload["items"]), 2)
        self.assertTrue(any(item["name"] == "foreign-source" for item in payload["items"]))

    def test_list_signals_returns_only_current_user_items_for_regular_user(self):
        regular_user = self.repository.create_user_record(
            username="signal-user",
            email="signal@example.com",
            password_hash=hash_password("signal-pass"),
            role="user",
            status="active",
        )
        foreign_source = self.repository.create_source_record(
            owner_user_id=regular_user["id"],
            source_type="telegram_channel",
            name="regular-source",
            visibility="private",
            config={"chat_id": "@regular"},
        )
        self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260410001",
            bet_type="big_small",
            bet_value="大",
        )
        own_signal = self.repository.create_signal_record(
            source_id=foreign_source["id"],
            lottery_type="pc28",
            issue_no="20260410002",
            bet_type="big_small",
            bet_value="小",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/signals",
                headers=self.session_headers_for(regular_user["id"]),
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual([item["id"] for item in payload["items"]], [own_signal["id"]])

    def test_list_raw_items_returns_only_current_user_items_for_regular_user(self):
        regular_user = self.repository.create_user_record(
            username="raw-user",
            email="raw@example.com",
            password_hash=hash_password("raw-pass"),
            role="user",
            status="active",
        )
        own_source = self.repository.create_source_record(
            owner_user_id=regular_user["id"],
            source_type="telegram_channel",
            name="raw-source",
            visibility="private",
            config={"chat_id": "@raw"},
        )
        self.repository.create_raw_item_record(source_id=1, issue_no="20260411001", raw_payload={"from": "owner"})
        own_raw_item = self.repository.create_raw_item_record(source_id=own_source["id"], issue_no="20260411002", raw_payload={"from": "regular"})
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/raw-items",
                headers=self.session_headers_for(regular_user["id"]),
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual([item["id"] for item in payload["items"]], [own_raw_item["id"]])

    def test_create_source_returns_item(self):
        self.repository.create_user_record(username="u2", email="", role="user", status="active")
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources",
                method="POST",
                body={
                    "source_type": "telegram_channel",
                    "name": "channel-a",
                    "visibility": "public",
                    "config": {"chat_id": "@demo"},
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["source_type"], "telegram_channel")
        self.assertEqual(payload["item"]["config"]["chat_id"], "@demo")

    def test_create_ai_trading_simulator_source_returns_fetch_config(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources",
                method="POST",
                body={
                    "source_type": "ai_trading_simulator_export",
                    "name": "AITradingSimulator 方案 #12",
                    "visibility": "private",
                    "config": {
                        "fetch": {
                            "url": "https://example.com/api/export/predictors/12/signals?view=execution",
                            "headers": {"Accept": "application/json"},
                            "timeout": 10,
                        }
                    },
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["source_type"], "ai_trading_simulator_export")
        self.assertEqual(payload["item"]["visibility"], "private")
        self.assertEqual(
            payload["item"]["config"]["fetch"]["url"],
            "https://example.com/api/export/predictors/12/signals?view=execution",
        )

    def test_update_source_returns_item(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources/1",
                method="POST",
                body={
                    "name": "demo-source-updated",
                    "visibility": "private",
                    "status": "inactive",
                    "config": {
                        "fetch": {
                            "url": "https://example.com/api/export/predictors/9/signals?view=execution",
                            "headers": {"Accept": "application/json"},
                            "timeout": 10,
                        }
                    },
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["name"], "demo-source-updated")
        self.assertEqual(payload["item"]["status"], "inactive")
        self.assertEqual(payload["item"]["visibility"], "private")

    def test_update_source_status_returns_item(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources/1/status",
                method="POST",
                body={"status": "archived"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["status"], "archived")

    def test_delete_source_returns_deleted_when_archived_and_unused(self):
        source = self.repository.create_source_record(
            owner_user_id=1,
            source_type="ai_trading_simulator_export",
            name="deletable-source",
            visibility="private",
            status="archived",
            config={"fetch": {"url": "https://example.com/api/export/predictors/88/signals?view=execution"}},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources/%s/delete" % source["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertTrue(payload["deleted"])
        self.assertIsNone(self.repository.get_source(source["id"]))

    def test_delete_source_rejects_subscription_reference(self):
        source = self.repository.create_source_record(
            owner_user_id=1,
            source_type="ai_trading_simulator_export",
            name="subscribed-source",
            visibility="private",
            status="archived",
            config={"fetch": {"url": "https://example.com/api/export/predictors/77/signals?view=execution"}},
        )
        self.repository.create_subscription_record(user_id=1, source_id=source["id"], strategy={"stake_amount": 10})
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/sources/%s/delete" % source["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("已有跟单策略引用", payload["error"])

    def test_create_telegram_account_and_list(self):
        create_status, _, create_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts",
                method="POST",
                body={
                    "label": "主账号",
                    "phone": "+12019362923",
                    "auth_mode": "phone_login",
                    "meta": {},
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(create_status, "200 OK")
        self.assertEqual(create_payload["item"]["label"], "主账号")
        self.assertEqual(create_payload["item"]["status"], "inactive")
        self.assertEqual(create_payload["item"]["auth_state"], "new")
        self.assertIn("/data/accounts/u1/", create_payload["item"]["session_path"])

        list_status, _, list_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(list_status, "200 OK")
        self.assertEqual(len(list_payload["items"]), 1)

    def test_list_telegram_accounts_support_query_by_user_id(self):
        other_user = self.repository.create_user_record(username="support-user", email="", role="user", status="active")
        self.repository.create_telegram_account_record(
            user_id=other_user["id"],
            label="支持账号",
            phone="+12019990009",
            session_path="/data/u2/support-main",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
            status="active",
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts",
                headers=self.session_headers,
                query="user_id=%s" % other_user["id"],
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["label"], "支持账号")

    def test_update_telegram_account_status_endpoint(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="主账号",
            phone="+12019362923",
            session_path="/data/u2/main",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts/%s/status" % account["id"],
                method="POST",
                body={"status": "inactive"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["status"], "inactive")

    def test_update_telegram_account_endpoint(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="旧账号",
            phone="+12019362923",
            session_path="/data/u2/old",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts/%s" % account["id"],
                method="POST",
                body={
                    "label": "新账号",
                    "phone": "+12010000000",
                    "meta": {"note": "edited"},
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["label"], "新账号")
        self.assertEqual(payload["item"]["session_path"], "/data/u2/old")
        self.assertEqual(payload["item"]["meta"]["note"], "edited")

    def test_update_telegram_account_status_requires_authorized_account(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="待授权账号",
            phone="+12019362923",
            session_path="/data/u2/pending",
            status="inactive",
            meta={"auth_mode": "phone_login", "auth_state": "code_sent"},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts/%s/status" % account["id"],
                method="POST",
                body={"status": "active"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("尚未完成授权", payload["error"])

    def test_import_telegram_account_session_route(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="导入账号",
            phone="",
            session_path="/data/u2/import",
            status="inactive",
            meta={"auth_mode": "session_import", "auth_state": "pending_import"},
        )
        with patch("pc28touzhu.api.app.import_telegram_account_session") as mocked:
            mocked.return_value = {
                "item": {
                    **account,
                    "phone": "+12018880000",
                    "status": "active",
                    "auth_mode": "session_import",
                    "auth_state": "authorized",
                    "is_authorized": True,
                    "meta": {"auth_mode": "session_import", "auth_state": "authorized"},
                }
            }
            status, _, payload = invoke(
                self.app,
                build_testing_environ(
                    "/api/platform/telegram-accounts/%s/import-session" % account["id"],
                    method="POST",
                    body={"file_name": "a.session", "session_file_base64": "ZmFrZQ=="},
                    headers=self.session_headers,
                ),
            )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["auth_state"], "authorized")

    def test_begin_telegram_account_login_route(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="登录账号",
            phone="+12019362923",
            session_path="/data/u2/login",
            status="inactive",
            meta={"auth_mode": "phone_login", "auth_state": "new"},
        )
        with patch("pc28touzhu.api.app.begin_telegram_account_login") as mocked:
            mocked.return_value = {
                "item": {
                    **account,
                    "auth_mode": "phone_login",
                    "auth_state": "code_sent",
                    "is_authorized": False,
                    "meta": {"auth_mode": "phone_login", "auth_state": "code_sent"},
                }
            }
            status, _, payload = invoke(
                self.app,
                build_testing_environ(
                    "/api/platform/telegram-accounts/%s/auth/send-code" % account["id"],
                    method="POST",
                    body={"phone": "+12019362923"},
                    headers=self.session_headers,
                ),
            )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["auth_state"], "code_sent")

    def test_delete_telegram_account_endpoint_requires_archived_and_no_dependencies(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="待删账号",
            phone="+12019360009",
            session_path="/data/u2/delete",
            status="archived",
            meta={},
        )
        self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100909",
            target_name="关联群",
            status="archived",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts/%s/delete" % account["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("投递群组", payload["error"])

    def test_delete_telegram_account_endpoint(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="可删账号",
            phone="+12019360010",
            session_path="/data/u2/delete-free",
            status="archived",
            meta={},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts/%s/delete" % account["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertTrue(payload["deleted"])
        self.assertIsNone(self.repository.get_telegram_account(account["id"]))

    def test_platform_route_requires_login(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/telegram-accounts",
                method="POST",
                body={
                    "label": "坏数据",
                    "session_path": "/data/u999/main",
                    "meta": {},
                },
            ),
        )
        self.assertEqual(status, "401 Unauthorized")
        self.assertIn("请先登录", payload["error"])

    def test_create_subscription_and_list(self):
        create_status, _, create_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions",
                method="POST",
                body={
                    "source_id": 1,
                    "strategy": {"mode": "follow"},
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(create_status, "200 OK")
        self.assertEqual(create_payload["item"]["strategy"]["mode"], "follow")

        list_status, _, list_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(list_status, "200 OK")
        self.assertEqual(len(list_payload["items"]), 1)

    def test_list_subscriptions_supports_stat_date(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={"mode": "follow"},
        )
        self.repository.subscription_daily_stats.append(
            {
                "id": 1,
                "stat_date": "2026-04-28",
                "user_id": 1,
                "subscription_id": created["id"],
                "source_id": 1,
                "profit_amount": 18.5,
                "loss_amount": 10.0,
                "net_profit": 8.5,
                "settled_event_count": 6,
                "hit_count": 3,
                "miss_count": 2,
                "refund_count": 1,
                "updated_at": "2026-04-28T12:00:00Z",
            }
        )
        self.repository.subscription_daily_stats.append(
            {
                "id": 2,
                "stat_date": "2026-04-27",
                "user_id": 1,
                "subscription_id": created["id"],
                "source_id": 1,
                "profit_amount": 12.0,
                "loss_amount": 15.0,
                "net_profit": -3.0,
                "settled_event_count": 5,
                "hit_count": 2,
                "miss_count": 3,
                "refund_count": 0,
                "updated_at": "2026-04-27T12:00:00Z",
            }
        )
        self.repository.subscription_runtime_runs.append(
            {
                "id": 1,
                "subscription_id": created["id"],
                "user_id": 1,
                "status": "active",
                "started_signal_id": None,
                "started_issue_no": "20260428001",
                "started_at": "2026-04-28T10:00:00Z",
                "start_reason": "auto_started",
                "ended_at": None,
                "end_reason": "",
                "last_issue_no": "20260428006",
                "last_result_type": "hit",
                "realized_profit": 18.5,
                "realized_loss": 10.0,
                "net_profit": 8.5,
                "settled_event_count": 6,
                "hit_count": 3,
                "miss_count": 2,
                "refund_count": 1,
                "baseline_reset_at": None,
                "baseline_reset_note": "",
                "created_at": "2026-04-28T10:00:00Z",
                "updated_at": "2026-04-28T12:00:00Z",
            }
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions",
                query="stat_date=2026-04-28",
                headers=self.session_headers,
            ),
        )

        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["stat_date"], "2026-04-28")
        self.assertEqual(payload["daily_summary"]["net_profit"], 8.5)
        self.assertEqual(payload["items"][0]["daily_stat"]["stat_date"], "2026-04-28")
        self.assertEqual(payload["items"][0]["daily_stat"]["settled_event_count"], 6)
        self.assertEqual(payload["items"][0]["daily_stat"]["net_profit"], 8.5)
        self.assertEqual(len(payload["items"][0]["daily_history"]), 2)
        self.assertEqual(payload["items"][0]["daily_history"][0]["stat_date"], "2026-04-28")
        self.assertEqual(payload["items"][0]["daily_history"][1]["stat_date"], "2026-04-27")
        self.assertEqual(len(payload["items"][0]["runtime_history"]), 1)
        self.assertEqual(payload["items"][0]["runtime_history"][0]["status"], "active")
        self.assertEqual(payload["items"][0]["runtime_history"][0]["net_profit"], 8.5)

    def test_list_subscription_daily_stats_endpoint(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={"mode": "follow"},
        )
        for index in range(35):
            day = 28 - (index % 28)
            month = 4 if index < 28 else 3
            self.repository.subscription_daily_stats.append(
                {
                    "id": index + 1,
                    "stat_date": "2026-%02d-%02d" % (month, day),
                    "user_id": 1,
                    "subscription_id": created["id"],
                    "source_id": 1,
                    "profit_amount": float(index + 1),
                    "loss_amount": 0.0,
                    "net_profit": float(index + 1),
                    "settled_event_count": 1,
                    "hit_count": 1,
                    "miss_count": 0,
                    "refund_count": 0,
                    "updated_at": "2026-04-28T12:00:00Z",
                }
            )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s/daily-stats" % created["id"],
                query="limit=35",
                headers=self.session_headers,
            ),
        )

        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["limit"], 35)
        self.assertEqual(len(payload["items"]), 35)
        self.assertEqual(payload["items"][0]["source_name"], "demo-source")

    def test_create_subscription_with_selected_play_filter(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions",
                method="POST",
                body={
                    "source_id": 1,
                    "strategy": {
                        "mode": "follow",
                        "bet_filter": {
                            "mode": "selected",
                            "selected_keys": ["big_small:大", "combo:大双"],
                        },
                    },
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["strategy"]["bet_filter"]["mode"], "selected")
        self.assertEqual(payload["item"]["strategy"]["bet_filter"]["selected_keys"], ["big_small:大", "combo:大双"])

    def test_create_subscription_with_strategy_v2_returns_legacy_projection(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="主号",
            phone="+12015550000",
            session_path="/data/u1/subscription-main",
            meta={},
        )
        target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-1001555",
            target_name="指定群",
            status="active",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions",
                method="POST",
                body={
                    "source_id": 1,
                    "strategy_v2": {
                        "play_filter": {
                            "mode": "selected",
                            "selected_keys": ["big_small:大"],
                        },
                        "staking_policy": {
                            "mode": "fixed",
                            "fixed_amount": 12,
                        },
                        "settlement_policy": {
                            "rule_source": "subscription_fixed",
                            "settlement_rule_id": "pc28_high_regular",
                            "fallback_profit_ratio": 1.2,
                        },
                        "risk_control": {
                            "enabled": True,
                            "profit_target": 50,
                            "loss_limit": 20,
                        },
                        "dispatch": {
                            "expire_after_seconds": 90,
                            "delivery_target_ids": [target["id"]],
                        },
                    },
                    "require_delivery_targets": True,
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["strategy_schema_version"], 2)
        self.assertEqual(payload["item"]["strategy"]["stake_amount"], 12)
        self.assertEqual(payload["item"]["strategy"]["bet_filter"]["selected_keys"], ["big_small:大"])
        self.assertEqual(payload["item"]["strategy"]["risk_control"]["win_profit_ratio"], 1.2)
        self.assertEqual(payload["item"]["strategy"]["delivery_target_ids"], [target["id"]])
        self.assertEqual(payload["item"]["strategy_v2"]["staking_policy"]["mode"], "fixed")
        self.assertEqual(payload["item"]["strategy_v2"]["settlement_policy"]["settlement_rule_id"], "pc28_high_regular")
        self.assertEqual(payload["item"]["strategy_v2"]["dispatch"]["delivery_target_ids"], [target["id"]])

    def test_create_subscription_requires_selected_delivery_targets_when_requested(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions",
                method="POST",
                body={
                    "source_id": 1,
                    "strategy_v2": {
                        "play_filter": {"mode": "all", "selected_keys": []},
                        "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                        "dispatch": {"expire_after_seconds": 120, "delivery_target_ids": []},
                    },
                    "require_delivery_targets": True,
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("至少选择一个发送群组", payload["error"])

    def test_create_subscription_rejects_other_user_delivery_target(self):
        other_user = self.repository.create_user_record(
            username="target-owner",
            email="target-owner@example.com",
            password_hash=hash_password("target-owner-pass"),
            role="user",
            status="active",
        )
        account = self.repository.create_telegram_account_record(
            user_id=other_user["id"],
            label="其他账号",
            phone="+12016660000",
            session_path="/data/other/main",
            meta={},
        )
        target = self.repository.create_delivery_target_record(
            user_id=other_user["id"],
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-1001666",
            target_name="其他用户群",
            status="active",
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions",
                method="POST",
                body={
                    "source_id": 1,
                    "strategy_v2": {
                        "play_filter": {"mode": "all", "selected_keys": []},
                        "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                        "dispatch": {"expire_after_seconds": 120, "delivery_target_ids": [target["id"]]},
                    },
                    "require_delivery_targets": True,
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("发送群组不存在或不属于当前用户", payload["error"])

    def test_update_subscription_status_endpoint(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={"mode": "follow"},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s/status" % created["id"],
                method="POST",
                body={"status": "inactive"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["status"], "inactive")

    def test_restart_subscription_cycle_endpoint(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            status="active",
            strategy={"mode": "follow", "stake_amount": 10},
        )
        self.repository.subscription_financial_states[int(created["id"])] = {
            "subscription_id": int(created["id"]),
            "user_id": 1,
            "realized_profit": 0.0,
            "realized_loss": 10.0,
            "net_profit": -10.0,
            "threshold_status": "loss_limit_hit",
            "stopped_reason": "达到止损阈值，当前轮次已停止",
            "baseline_reset_at": None,
            "baseline_reset_note": "",
            "last_settled_event_id": 1,
            "last_settled_at": "2026-04-07T12:00:00Z",
            "updated_at": "2026-04-07T12:00:00Z",
        }

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s/restart" % created["id"],
                method="POST",
                body={"note": "api restart"},
                headers=self.session_headers,
            ),
        )

        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["status"], "active")
        self.assertEqual(payload["financial"]["net_profit"], 0.0)
        self.assertEqual(payload["financial"]["threshold_status"], "")
        self.assertEqual(payload["reset_note"], "api restart")

    def test_update_subscription_endpoint(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={"mode": "follow", "stake_amount": 10},
        )
        self.repository.create_source_record(
            owner_user_id=1,
            source_type="ai_trading_simulator_export",
            name="额外来源",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s" % created["id"],
                method="POST",
                body={
                    "source_id": 2,
                    "strategy": {"mode": "follow", "stake_amount": 20},
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["source_id"], 2)
        self.assertEqual(payload["item"]["strategy"]["stake_amount"], 20)

    def test_settle_subscription_progression_endpoint(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={"mode": "martingale", "base_stake": 10, "multiplier": 2, "max_steps": 3},
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407011",
            bet_type="big_small",
            bet_value="大",
        )
        event = self.repository.create_progression_event_record(
            subscription_id=created["id"],
            user_id=1,
            signal_id=signal["id"],
            issue_no="20260407011",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=2,
            max_steps=3,
            refund_action="hold",
            cap_action="reset",
            status="placed",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s/progression/settle" % created["id"],
                method="POST",
                body={
                    "progression_event_id": event["id"],
                    "result_type": "miss",
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["progression"]["event"]["resolved_result_type"], "miss")
        self.assertEqual(payload["progression"]["state"]["current_step"], 2)

    def test_resolve_subscription_progression_endpoint_refunds_high_special_cases(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={
                "play_filter": {"mode": "all", "selected_keys": []},
                "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                "settlement_policy": {
                    "rule_source": "subscription_fixed",
                    "settlement_rule_id": "pc28_high_regular",
                    "fallback_profit_ratio": 1.0,
                },
                "risk_control": {"enabled": False, "profit_target": 0, "loss_limit": 0},
                "dispatch": {"expire_after_seconds": 120},
            },
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407012",
            bet_type="big_small",
            bet_value="大",
        )
        event = self.repository.create_progression_event_record(
            subscription_id=created["id"],
            user_id=1,
            signal_id=signal["id"],
            issue_no="20260407012",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            settlement_rule_id="pc28_high_regular",
            settlement_snapshot={"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_high_regular"},
            status="placed",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s/progression/resolve" % created["id"],
                method="POST",
                body={
                    "progression_event_id": event["id"],
                    "draw_context": {
                        "result_number": 14,
                        "triplet": [4, 4, 6],
                    },
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["resolved"]["result_type"], "refund")
        self.assertEqual(payload["resolved"]["refund_reason"], "13/14 退本金")
        self.assertEqual(payload["progression"]["event"]["resolved_result_type"], "refund")
        self.assertEqual(payload["progression"]["event"]["result_context"]["resolution_mode"], "auto")

    def test_resolve_subscription_progression_endpoint_supports_pair_refund(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={
                "play_filter": {"mode": "all", "selected_keys": []},
                "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                "settlement_policy": {
                    "rule_source": "subscription_fixed",
                    "settlement_rule_id": "pc28_high_regular",
                    "fallback_profit_ratio": 1.0,
                },
                "risk_control": {"enabled": False, "profit_target": 0, "loss_limit": 0},
                "dispatch": {"expire_after_seconds": 120},
            },
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407013",
            bet_type="big_small",
            bet_value="小",
        )
        event = self.repository.create_progression_event_record(
            subscription_id=created["id"],
            user_id=1,
            signal_id=signal["id"],
            issue_no="20260407013",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            settlement_rule_id="pc28_high_regular",
            settlement_snapshot={"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_high_regular"},
            status="placed",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s/progression/resolve" % created["id"],
                method="POST",
                body={
                    "progression_event_id": event["id"],
                    "draw_context": {
                        "result_number": 8,
                        "triplet": [1, 1, 6],
                    },
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["resolved"]["result_type"], "refund")
        self.assertEqual(payload["resolved"]["refund_reason"], "对子退本金")

    def test_batch_resolve_subscription_progression_endpoint(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={
                "play_filter": {"mode": "all", "selected_keys": []},
                "staking_policy": {"mode": "fixed", "fixed_amount": 10},
                "settlement_policy": {
                    "rule_source": "subscription_fixed",
                    "settlement_rule_id": "pc28_high_regular",
                    "fallback_profit_ratio": 1.0,
                },
                "risk_control": {"enabled": False, "profit_target": 0, "loss_limit": 0},
                "dispatch": {"expire_after_seconds": 120},
            },
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407014",
            bet_type="big_small",
            bet_value="大",
        )
        self.repository.create_progression_event_record(
            subscription_id=created["id"],
            user_id=1,
            signal_id=signal["id"],
            issue_no="20260407014",
            progression_step=1,
            stake_amount=10,
            base_stake=10,
            multiplier=1,
            max_steps=1,
            refund_action="hold",
            cap_action="reset",
            settlement_rule_id="pc28_high_regular",
            settlement_snapshot={"rule_source": "subscription_fixed", "settlement_rule_id": "pc28_high_regular"},
            status="placed",
        )
        with patch("pc28touzhu.services.platform_service.fetch_pc28_recent_draws_deep") as mocked_fetch:
            mocked_fetch.return_value = {
                "source": "official",
                "items": [
                    {
                        "issue_no": "20260407014",
                        "draw_context": {
                            "open_code": "4+4+6",
                            "result_number": 14,
                            "triplet": [4, 4, 6],
                            "big_small": "大",
                            "odd_even": "双",
                            "combo": "大双",
                        },
                    }
                ],
            }
            status, _, payload = invoke(
                self.app,
                build_testing_environ(
                    "/api/platform/subscriptions/progression/resolve-batch",
                    method="POST",
                    body={},
                    headers=self.session_headers,
                ),
            )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["summary"]["resolved_count"], 1)
        self.assertEqual(payload["summary"]["refund_count"], 1)
        self.assertEqual(payload["items"][0]["result_type"], "refund")

    def test_delete_subscription_endpoint_requires_archived(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={"mode": "follow"},
            status="inactive",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s/delete" % created["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("请先归档", payload["error"])

    def test_delete_subscription_endpoint(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={"mode": "follow"},
            status="archived",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s/delete" % created["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertTrue(payload["deleted"])
        self.assertIsNone(self.repository.get_subscription(created["id"]))

    def test_update_subscription_endpoint(self):
        created = self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={"mode": "follow", "stake_amount": 10},
        )
        self.repository.create_source_record(
            owner_user_id=1,
            source_type="ai_trading_simulator_export",
            name="额外来源",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/subscriptions/%s" % created["id"],
                method="POST",
                body={
                    "source_id": 2,
                    "strategy": {"mode": "follow", "stake_amount": 20},
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["source_id"], 2)
        self.assertEqual(payload["item"]["strategy"]["stake_amount"], 20)

    def test_create_delivery_target_and_list(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        create_status, _, create_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets",
                method="POST",
                body={
                    "telegram_account_id": account["id"],
                    "executor_type": "telegram_group",
                    "target_key": "-100111",
                    "target_name": "投注群",
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(create_status, "200 OK")
        self.assertEqual(create_payload["item"]["target_key"], "-100111")
        self.assertEqual(create_payload["item"]["telegram_account_id"], account["id"])
        self.assertEqual(create_payload["item"]["status"], "inactive")
        self.assertEqual(create_payload["item"]["last_test_status"], "")

        list_status, _, list_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(list_status, "200 OK")
        self.assertEqual(len(list_payload["items"]), 1)

    def test_delivery_targets_list_allows_extra_fields(self):
        enhanced_target = {
            "id": 888,
            "user_id": 1,
            "telegram_account_id": None,
            "executor_type": "telegram_group",
            "target_key": "@demo",
            "target_name": "演示群",
            "template_id": None,
            "status": "active",
            "last_test_status": "success",
            "last_test_error_code": "",
            "last_test_message": "",
            "last_tested_at": "2026-04-08T12:00:00Z",
            "created_at": "2026-04-08T12:00:00Z",
            "updated_at": "2026-04-08T12:00:00Z",
            "subscriptions": [55],
            "recent_tests": [{"status": "failed", "message": "超时"}],
        }
        self.repository.list_delivery_targets = lambda user_id: [enhanced_target]
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["items"][0]["subscriptions"], [55])
        self.assertEqual(payload["items"][0]["recent_tests"][0]["message"], "超时")

    def test_create_message_template_and_list(self):
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/message-templates",
                method="POST",
                body={
                    "name": "加拿大28高倍模板",
                    "lottery_type": "pc28",
                    "bet_type": "*",
                    "template_text": "{{bet_value}}{{amount}}",
                    "config": {
                        "bet_rules": {
                            "number_sum": {"format": "{{bet_value}}/{{amount}}"},
                        }
                    },
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["name"], "加拿大28高倍模板")
        self.assertEqual(payload["item"]["config"]["bet_rules"]["number_sum"]["format"], "{{bet_value}}/{{amount}}")

        list_status, _, list_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/message-templates",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(list_status, "200 OK")
        self.assertEqual(len(list_payload["items"]), 1)

    def test_message_templates_list_allows_extra_metadata(self):
        enhanced_template = {
            "id": 999,
            "user_id": 1,
            "name": "增强模板",
            "lottery_type": "pc28",
            "bet_type": "*",
            "template_text": "{{bet_value}}{{amount}}",
            "config": {"bet_rules": {}},
            "status": "active",
            "created_at": "2026-04-08T12:00:00Z",
            "updated_at": "2026-04-08T12:00:00Z",
            "usage_count": 5,
            "bound_targets": [101, 102],
        }
        self.repository.list_message_templates = lambda user_id: [enhanced_template]
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/message-templates",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["items"][0]["usage_count"], 5)
        self.assertEqual(payload["items"][0]["bound_targets"], [101, 102])

    def test_update_message_template_endpoint(self):
        template = self.repository.create_message_template_record(
            user_id=1,
            name="旧模板",
            lottery_type="pc28",
            bet_type="*",
            template_text="{{bet_value}}{{amount}}",
            config={"bet_rules": {}},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/message-templates/%s" % template["id"],
                method="POST",
                body={
                    "name": "新模板",
                    "lottery_type": "pc28",
                    "bet_type": "*",
                    "template_text": "{{bet_value}}/{{amount}}",
                    "config": {"bet_rules": {"big_small": {"format": "{{bet_value}}{{amount}}"}}},
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["name"], "新模板")
        self.assertEqual(payload["item"]["template_text"], "{{bet_value}}/{{amount}}")

    def test_dispatch_signal_uses_message_template(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="模板账号",
            phone="+12010000001",
            session_path="/data/u3/template-main",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
            status="active",
        )
        template = self.repository.create_message_template_record(
            user_id=1,
            name="高倍数字模板",
            lottery_type="pc28",
            bet_type="*",
            template_text="{{bet_value}}{{amount}}",
            config={
                "bet_rules": {
                    "number_sum": {"format": "{{bet_value}}/{{amount}}"},
                    "big_small": {"format": "{{bet_value}}{{amount}}"},
                }
            },
        )
        self.repository.create_subscription_record(user_id=1, source_id=1, strategy={"stake_amount": 12})
        self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            template_id=template["id"],
            executor_type="telegram_group",
            target_key="-100200",
            target_name="模板群",
            status="active",
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407009",
            bet_type="number_sum",
            bet_value="0",
            normalized_payload={},
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/signals/%s/dispatch" % signal["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["created_count"], 1)
        self.assertEqual(self.repository.jobs[0]["planned_message_text"], "0/12")

    def test_create_delivery_target_normalizes_tme_links(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets",
                method="POST",
                body={
                    "telegram_account_id": account["id"],
                    "executor_type": "telegram_group",
                    "target_key": "https://t.me/c/123456/9",
                    "target_name": "测试群",
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["target_key"], "-100123456")

        status2, _, payload2 = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets",
                method="POST",
                body={
                    "telegram_account_id": account["id"],
                    "executor_type": "telegram_group",
                    "target_key": "https://t.me/s/My_Test_Chat",
                    "target_name": "测试群2",
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status2, "200 OK")
        self.assertEqual(payload2["item"]["target_key"], "My_Test_Chat")

    def test_create_delivery_target_rejects_invite_links(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets",
                method="POST",
                body={
                    "telegram_account_id": account["id"],
                    "executor_type": "telegram_group",
                    "target_key": "https://t.me/+abcdefg",
                    "target_name": "邀请群",
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("邀请链接", payload["error"])

    def test_delivery_target_test_send_requires_authorized_account(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "code_sent"},
        )
        target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100111",
            target_name="投注群",
            status="active",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s/test-send" % target["id"],
                method="POST",
                body={"message_text": "test"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("尚未完成授权", payload["error"])

    def test_delivery_target_test_send_rejects_archived_target(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100111",
            target_name="投注群",
            status="archived",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s/test-send" % target["id"],
                method="POST",
                body={"message_text": "test"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("已归档", payload["error"])

    def test_delivery_target_test_send_success(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
            status="inactive",
        )
        target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="https://t.me/c/123456/9",
            target_name="投注群",
            status="inactive",
        )

        class FakeSender:
            def __init__(self, *, api_id, api_hash, phone, session):
                self.api_id = api_id
                self.api_hash = api_hash
                self.phone = phone
                self.session = session

            def connect(self):
                return None

            def disconnect(self):
                return None

            def send_text(self, target_key, message_text):
                return {"message_id": 1, "target_key": target_key, "text": message_text}

        with patch("pc28touzhu.services.platform_service.TelethonMessageSender", FakeSender):
            status, _, payload = invoke(
                self.app,
                build_testing_environ(
                    "/api/platform/delivery-targets/%s/test-send" % target["id"],
                    method="POST",
                    body={"message_text": "hello"},
                    headers=self.session_headers,
                ),
            )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["target_key"], "-100123456")
        self.assertEqual(payload["result"]["message_id"], 1)
        self.assertEqual(payload["item"]["last_test_status"], "success")
        self.assertEqual(payload["item"]["last_test_error_code"], "")
        self.assertIsNotNone(payload["item"]["last_tested_at"])

    def test_delivery_target_test_send_returns_actionable_reason(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100999",
            target_name="测试群",
            status="inactive",
        )

        class UserNotParticipantError(Exception):
            pass

        class FakeSender:
            def __init__(self, *, api_id, api_hash, phone, session):
                self.api_id = api_id
                self.api_hash = api_hash
                self.phone = phone
                self.session = session

            def connect(self):
                return None

            def disconnect(self):
                return None

            def send_text(self, target_key, message_text):
                raise UserNotParticipantError("user not in chat")

        with patch("pc28touzhu.services.platform_service.TelethonMessageSender", FakeSender):
            status, _, payload = invoke(
                self.app,
                build_testing_environ(
                    "/api/platform/delivery-targets/%s/test-send" % target["id"],
                    method="POST",
                    body={"message_text": "hello"},
                    headers=self.session_headers,
                ),
            )
        self.assertEqual(status, "400 Bad Request")
        self.assertEqual(payload["reason_code"], "target_not_joined")
        self.assertIn("没有进入这个群组", payload["error"])
        updated = self.repository.get_delivery_target(target["id"])
        self.assertEqual(updated["last_test_status"], "failed")
        self.assertEqual(updated["last_test_error_code"], "target_not_joined")

    def test_delivery_target_test_send_reports_missing_telethon_runtime_hint(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100999",
            target_name="测试群",
            status="inactive",
        )

        class FakeSender:
            def __init__(self, *, api_id, api_hash, phone, session):
                self.api_id = api_id
                self.api_hash = api_hash
                self.phone = phone
                self.session = session

            def connect(self):
                raise RuntimeError(
                    "未安装 Telethon，请先安装 `Telethon>=1.42,<2`。 当前进程 Python: /usr/bin/python3。"
                )

            def disconnect(self):
                return None

            def send_text(self, target_key, message_text):
                raise AssertionError("connect 失败时不应继续发送")

        with patch("pc28touzhu.services.platform_service.TelethonMessageSender", FakeSender):
            status, _, payload = invoke(
                self.app,
                build_testing_environ(
                    "/api/platform/delivery-targets/%s/test-send" % target["id"],
                    method="POST",
                    body={"message_text": "hello"},
                    headers=self.session_headers,
                ),
            )
        self.assertEqual(status, "400 Bad Request")
        self.assertEqual(payload["reason_code"], "telethon_missing")
        self.assertIn("当前平台运行进程缺少 Telethon 依赖", payload["error"])
        self.assertIn("当前进程 Python: /usr/bin/python3", payload["why"])
        updated = self.repository.get_delivery_target(target["id"])
        self.assertEqual(updated["last_test_status"], "failed")
        self.assertEqual(updated["last_test_error_code"], "telethon_missing")

    def test_delivery_target_test_send_reports_session_readonly_hint(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100999",
            target_name="测试群",
            status="inactive",
        )

        class FakeSender:
            def __init__(self, *, api_id, api_hash, phone, session):
                self.api_id = api_id
                self.api_hash = api_hash
                self.phone = phone
                self.session = session

            def connect(self):
                raise PermissionError(
                    "Telethon session 文件不可写：/www/wwwroot/pc28touzhu/data/accounts/u1/main.session。当前运行用户对该文件没有写权限，请修正属主或权限。"
                )

            def disconnect(self):
                return None

            def send_text(self, target_key, message_text):
                raise AssertionError("connect 失败时不应继续发送")

        with patch("pc28touzhu.services.platform_service.TelethonMessageSender", FakeSender):
            status, _, payload = invoke(
                self.app,
                build_testing_environ(
                    "/api/platform/delivery-targets/%s/test-send" % target["id"],
                    method="POST",
                    body={"message_text": "hello"},
                    headers=self.session_headers,
                ),
            )
        self.assertEqual(status, "400 Bad Request")
        self.assertEqual(payload["reason_code"], "session_readonly")
        self.assertIn("Session 文件不可写", payload["error"])
        self.assertIn("main.session", payload["why"])
        updated = self.repository.get_delivery_target(target["id"])
        self.assertEqual(updated["last_test_status"], "failed")
        self.assertEqual(updated["last_test_error_code"], "session_readonly")

    def test_update_delivery_target_status_endpoint(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={},
        )
        created = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100111",
            target_name="投注群",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s/status" % created["id"],
                method="POST",
                body={"status": "inactive"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["status"], "inactive")

    def test_update_delivery_target_status_requires_successful_test(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
        )
        created = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100111",
            target_name="投注群",
            status="inactive",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s/status" % created["id"],
                method="POST",
                body={"status": "active"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertEqual(payload["reason_code"], "target_test_required")

    def test_update_delivery_target_status_allows_active_after_successful_test(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={"auth_mode": "phone_login", "auth_state": "authorized"},
            status="active",
        )
        created = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100111",
            target_name="投注群",
            status="inactive",
            last_test_status="success",
            last_tested_at="2026-04-07T12:00:00Z",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s/status" % created["id"],
                method="POST",
                body={"status": "active"},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["status"], "active")

    def test_update_delivery_target_endpoint(self):
        account_a = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={},
        )
        account_b = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号B",
            phone="+12019360001",
            session_path="/data/u7/account-b",
            meta={},
        )
        created = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account_a["id"],
            executor_type="telegram_group",
            target_key="-100111",
            target_name="旧投注群",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s" % created["id"],
                method="POST",
                body={
                    "telegram_account_id": account_b["id"],
                    "executor_type": "telegram_group",
                    "target_key": "-100222",
                    "target_name": "新投注群",
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["telegram_account_id"], account_b["id"])
        self.assertEqual(payload["item"]["target_key"], "-100222")

    def test_delete_delivery_target_endpoint_requires_no_execution_jobs(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号C",
            phone="+12019360002",
            session_path="/data/u7/account-c",
            meta={},
        )
        created = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100333",
            target_name="归档投注群",
            status="archived",
        )
        self.repository.create_execution_job_record(
            user_id=1,
            signal_id=1,
            delivery_target_id=created["id"],
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            idempotency_key="target-delete-job",
            planned_message_text="大单20",
            stake_plan={"mode": "follow", "amount": 20},
            execute_after="2026-04-07T12:00:00Z",
            expire_at="2026-04-07T12:02:00Z",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s/delete" % created["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("执行记录", payload["error"])

    def test_delete_delivery_target_endpoint(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号D",
            phone="+12019360003",
            session_path="/data/u7/account-d",
            meta={},
        )
        created = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100444",
            target_name="可删投注群",
            status="archived",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s/delete" % created["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertTrue(payload["deleted"])
        self.assertIsNone(self.repository.get_delivery_target(created["id"]))

    def test_update_delivery_target_endpoint(self):
        account_a = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号A",
            phone="+12019360000",
            session_path="/data/u7/account-a",
            meta={},
        )
        account_b = self.repository.create_telegram_account_record(
            user_id=1,
            label="账号B",
            phone="+12019360001",
            session_path="/data/u7/account-b",
            meta={},
        )
        created = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account_a["id"],
            executor_type="telegram_group",
            target_key="-100111",
            target_name="旧投注群",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/delivery-targets/%s" % created["id"],
                method="POST",
                body={
                    "telegram_account_id": account_b["id"],
                    "executor_type": "telegram_group",
                    "target_key": "-100222",
                    "target_name": "新投注群",
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["telegram_account_id"], account_b["id"])
        self.assertEqual(payload["item"]["target_key"], "-100222")

    def test_create_signal_and_list(self):
        create_status, _, create_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/signals",
                method="POST",
                body={
                    "source_id": 1,
                    "lottery_type": "pc28",
                    "issue_no": "20260407001",
                    "bet_type": "big_small",
                    "bet_value": "大",
                    "confidence": 0.8,
                    "normalized_payload": {"message_text": "大10"},
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(create_status, "200 OK")
        self.assertEqual(create_payload["item"]["bet_value"], "大")

        list_status, _, list_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/signals",
                headers=self.session_headers,
                query="source_id=1",
            ),
        )
        self.assertEqual(list_status, "200 OK")
        self.assertEqual(len(list_payload["items"]), 1)

    def test_dispatch_signal_creates_jobs(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="主号",
            phone="+12010000000",
            session_path="/data/u3/main",
            meta={},
        )
        self.repository.create_subscription_record(user_id=1, source_id=1, strategy={"stake_amount": 12})
        self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100200",
            target_name="跟单群",
            status="active",
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407002",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={},
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/signals/%s/dispatch" % signal["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["created_count"], 1)
        self.assertEqual(len(self.repository.jobs), 1)
        self.assertEqual(self.repository.jobs[0]["telegram_account_id"], account["id"])

    def test_dispatch_signal_only_uses_subscription_selected_delivery_targets(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="主号",
            phone="+12010000010",
            session_path="/data/u3/selected",
            meta={},
        )
        selected_target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100210",
            target_name="指定跟单群",
            status="active",
        )
        self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100211",
            target_name="不发送群",
            status="active",
        )
        self.repository.create_subscription_record(
            user_id=1,
            source_id=1,
            strategy={
                "play_filter": {"mode": "all", "selected_keys": []},
                "staking_policy": {"mode": "fixed", "fixed_amount": 12},
                "dispatch": {"expire_after_seconds": 120, "delivery_target_ids": [selected_target["id"]]},
            },
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407013",
            bet_type="big_small",
            bet_value="小",
            normalized_payload={},
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/signals/%s/dispatch" % signal["id"],
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["created_count"], 1)
        self.assertEqual(len(self.repository.jobs), 1)
        self.assertEqual(self.repository.jobs[0]["delivery_target_id"], selected_target["id"])

    def test_dispatch_signal_rejects_other_user_access(self):
        regular_user = self.repository.create_user_record(
            username="dispatch-user",
            email="dispatch@example.com",
            password_hash=hash_password("dispatch-pass"),
            role="user",
            status="active",
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407009",
            bet_type="big_small",
            bet_value="大",
            normalized_payload={},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/signals/%s/dispatch" % signal["id"],
                method="POST",
                body={},
                headers=self.session_headers_for(regular_user["id"]),
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("signal_id", payload["error"])

    def test_list_execution_jobs_returns_filtered_items(self):
        account = self.repository.create_telegram_account_record(
            user_id=1,
            label="任务号",
            phone="+12012223333",
            session_path="/data/u3/job-main",
            meta={},
        )
        self.repository.create_subscription_record(user_id=1, source_id=1, strategy={"stake_amount": 10})
        target = self.repository.create_delivery_target_record(
            user_id=1,
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            target_key="-100201",
            target_name="任务群",
        )
        signal = self.repository.create_signal_record(
            source_id=1,
            lottery_type="pc28",
            issue_no="20260407012",
            bet_type="big_small",
            bet_value="大",
            normalized_payload={},
        )
        created = self.repository.create_execution_job_record(
            user_id=1,
            signal_id=signal["id"],
            delivery_target_id=target["id"],
            telegram_account_id=account["id"],
            executor_type="telegram_group",
            idempotency_key="signal:%s:target:%s" % (signal["id"], target["id"]),
            planned_message_text="大10",
            stake_plan={"mode": "flat", "amount": 10},
            execute_after="2026-04-08T12:00:00Z",
            expire_at="2026-04-08T12:02:00Z",
            status="failed",
        )
        self.repository.report_job_result(
            job_id=str(created["job"]["id"]),
            executor_id="executor-001",
            attempt_no=1,
            delivery_status="failed",
            remote_message_id=None,
            executed_at="2026-04-08T12:00:08Z",
            raw_result={"exception_type": "RuntimeError"},
            error_message="network timeout",
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/execution-jobs",
                headers=self.session_headers,
                query="status=failed",
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["status"], "failed")
        self.assertTrue(payload["items"][0]["can_retry"])
        self.assertEqual(payload["items"][0]["last_error_message"], "network timeout")

    def test_retry_execution_job_endpoint_resets_status(self):
        self.repository.jobs.append(
            {
                "id": 9,
                "user_id": 1,
                "signal_id": 1,
                "delivery_target_id": 1,
                "telegram_account_id": None,
                "executor_type": "telegram_group",
                "idempotency_key": "job-9",
                "planned_message_text": "大10",
                "stake_plan": {"mode": "flat", "amount": 10},
                "execute_after": "2026-04-07T12:00:00Z",
                "expire_at": "2026-04-07T12:02:00Z",
                "status": "failed",
                "error_message": "session invalid",
                "created_at": "2026-04-07T12:00:00Z",
                "updated_at": "2026-04-07T12:00:00Z",
            }
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/execution-jobs/9/retry",
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["item"]["status"], "pending")
        self.assertTrue(payload["item"]["can_retry"] is False)
        self.assertIsNone(payload["item"]["error_message"])

    def test_list_execution_jobs_scope_all_returns_other_user_job(self):
        self.repository.targets.append(
            {
                "id": 5,
                "user_id": 2,
                "telegram_account_id": None,
                "executor_type": "telegram_group",
                "target_key": "-100555",
                "target_name": "其他用户群",
                "template_id": None,
                "status": "active",
                "created_at": "2026-04-07T12:00:00Z",
                "updated_at": "2026-04-07T12:00:00Z",
            }
        )
        self.repository.signals.append(
            {
                "id": 5,
                "source_id": 1,
                "source_raw_item_id": None,
                "lottery_type": "pc28",
                "issue_no": "20260409001",
                "bet_type": "big_small",
                "bet_value": "小",
                "confidence": 0.66,
                "normalized_payload": {},
                "status": "ready",
                "published_at": "2026-04-09T12:00:00Z",
                "created_at": "2026-04-09T12:00:00Z",
            }
        )
        self.repository.jobs.append(
            {
                "id": 15,
                "user_id": 2,
                "signal_id": 5,
                "delivery_target_id": 5,
                "telegram_account_id": None,
                "executor_type": "telegram_group",
                "idempotency_key": "job-15",
                "planned_message_text": "小10",
                "stake_plan": {"mode": "flat", "amount": 10},
                "execute_after": "2026-04-09T12:00:00Z",
                "expire_at": "2026-04-09T12:02:00Z",
                "status": "pending",
                "error_message": None,
                "created_at": "2026-04-09T12:00:00Z",
                "updated_at": "2026-04-09T12:00:00Z",
            }
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/execution-jobs",
                headers=self.session_headers,
                query="scope=all",
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertTrue(any(item["id"] == 15 for item in payload["items"]))

    def test_list_recent_execution_failures_returns_failed_jobs(self):
        self.repository.upsert_executor_heartbeat(
            executor_id="executor-001",
            version="0.2.0",
            capabilities={"send": True},
            status="online",
            last_seen_at="2026-04-08T12:00:00Z",
        )
        self.repository.targets.append(
            {
                "id": 1,
                "user_id": 1,
                "telegram_account_id": None,
                "executor_type": "telegram_group",
                "target_key": "-100901",
                "target_name": "失败群",
                "template_id": None,
                "status": "active",
                "created_at": "2026-04-07T12:00:00Z",
                "updated_at": "2026-04-07T12:00:00Z",
            }
        )
        self.repository.signals.append(
            {
                "id": 1,
                "source_id": 1,
                "source_raw_item_id": None,
                "lottery_type": "pc28",
                "issue_no": "20260408001",
                "bet_type": "big_small",
                "bet_value": "大",
                "confidence": 0.8,
                "normalized_payload": {},
                "status": "ready",
                "published_at": "2026-04-08T12:00:00Z",
                "created_at": "2026-04-08T12:00:00Z",
            }
        )
        self.repository.jobs.append(
            {
                "id": 11,
                "user_id": 1,
                "signal_id": 1,
                "delivery_target_id": 1,
                "telegram_account_id": None,
                "executor_type": "telegram_group",
                "idempotency_key": "job-11",
                "planned_message_text": "大10",
                "stake_plan": {"mode": "flat", "amount": 10},
                "execute_after": "2026-04-08T12:00:00Z",
                "expire_at": "2026-04-08T12:02:00Z",
                "status": "failed",
                "error_message": "session expired",
                "created_at": "2026-04-08T12:00:00Z",
                "updated_at": "2026-04-08T12:00:00Z",
            }
        )
        self.repository.report_job_result(
            job_id="11",
            executor_id="executor-001",
            attempt_no=1,
            delivery_status="failed",
            remote_message_id=None,
            executed_at="2026-04-08T12:00:05Z",
            raw_result={"exception_type": "RuntimeError"},
            error_message="session expired",
        )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/execution-failures",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(len(payload["items"]), 1)
        self.assertEqual(payload["items"][0]["job_id"], 11)
        self.assertEqual(payload["items"][0]["executor_instance_id"], "executor-001")
        self.assertTrue(payload["items"][0]["can_retry"])
        self.assertIn(payload["items"][0]["auto_retry_state"], {"scheduled", "due"})
        self.assertTrue(payload["items"][0]["auto_retry_enabled"])

    def test_list_alerts_returns_executor_and_retry_exhausted_alerts(self):
        self.repository.upsert_executor_heartbeat(
            executor_id="executor-offline",
            version="0.2.0",
            capabilities={"send": True},
            status="online",
            last_seen_at="2020-01-01T00:00:00Z",
        )
        self.repository.targets.append(
            {
                "id": 2,
                "user_id": 1,
                "telegram_account_id": None,
                "executor_type": "telegram_group",
                "target_key": "-100902",
                "target_name": "告警群",
                "template_id": None,
                "status": "active",
                "created_at": "2026-04-07T12:00:00Z",
                "updated_at": "2026-04-07T12:00:00Z",
            }
        )
        self.repository.jobs.append(
            {
                "id": 12,
                "user_id": 1,
                "signal_id": 1,
                "delivery_target_id": 2,
                "telegram_account_id": None,
                "executor_type": "telegram_group",
                "idempotency_key": "job-12",
                "planned_message_text": "大12",
                "stake_plan": {"mode": "flat", "amount": 12},
                "execute_after": "2026-04-08T12:00:00Z",
                "expire_at": "2026-04-08T12:02:00Z",
                "status": "failed",
                "error_message": "retry exhausted",
                "created_at": "2026-04-08T12:00:00Z",
                "updated_at": "2026-04-08T12:00:00Z",
            }
        )
        for attempt_no in range(1, 4):
            self.repository.report_job_result(
                job_id="12",
                executor_id="executor-offline",
                attempt_no=attempt_no,
                delivery_status="failed",
                remote_message_id=None,
                executed_at="2026-04-08T12:00:0%sZ" % attempt_no,
                raw_result={"exception_type": "RuntimeError"},
                error_message="retry exhausted",
            )

        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/admin/alerts",
                headers=self.session_headers,
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertTrue(any(item["alert_type"] == "executor_offline" for item in payload["items"]))
        self.assertTrue(any(item["alert_type"] == "job_retry_exhausted" for item in payload["items"]))

    def test_regular_user_alerts_exclude_platform_health(self):
        regular_user = self.repository.create_user_record(
            username="alert-user",
            email="alert@example.com",
            password_hash=hash_password("alert-pass"),
            role="user",
            status="active",
        )
        self.repository.upsert_executor_heartbeat(
            executor_id="executor-offline",
            version="0.2.0",
            capabilities={"send": True},
            status="online",
            last_seen_at="2026-04-07T12:00:00Z",
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/alerts",
                headers=self.session_headers_for(regular_user["id"]),
            ),
        )
        self.assertEqual(status, "200 OK")
        self.assertEqual(payload["items"], [])

    def test_create_raw_item_and_normalize(self):
        create_status, _, create_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/raw-items",
                method="POST",
                body={
                    "source_id": 1,
                    "issue_no": "20260407003",
                    "raw_payload": {
                        "signals": [
                            {
                                "lottery_type": "pc28",
                                "issue_no": "20260407003",
                                "bet_type": "big_small",
                                "bet_value": "大",
                                "confidence": 0.81,
                                "message_text": "大18",
                                "stake_amount": 18,
                            }
                        ]
                    },
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(create_status, "200 OK")
        raw_item_id = create_payload["item"]["id"]

        normalize_status, _, normalize_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/raw-items/%s/normalize" % raw_item_id,
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(normalize_status, "200 OK")
        self.assertEqual(normalize_payload["created_count"], 1)
        self.assertEqual(len(self.repository.signals), 1)
        self.assertEqual(self.repository.raw_items[0]["parse_status"], "parsed")

    def test_normalize_raw_item_rejects_other_user_access(self):
        regular_user = self.repository.create_user_record(
            username="normalize-user",
            email="normalize@example.com",
            password_hash=hash_password("normalize-pass"),
            role="user",
            status="active",
        )
        raw_item = self.repository.create_raw_item_record(
            source_id=1,
            issue_no="20260407005",
            raw_payload={"signals": []},
        )
        status, _, payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/raw-items/%s/normalize" % raw_item["id"],
                method="POST",
                body={},
                headers=self.session_headers_for(regular_user["id"]),
            ),
        )
        self.assertEqual(status, "400 Bad Request")
        self.assertIn("raw_item_id", payload["error"])

    def test_normalize_ai_trading_simulator_export_payload(self):
        create_status, _, create_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/raw-items",
                method="POST",
                body={
                    "source_id": 1,
                    "issue_no": "20260408001",
                    "raw_payload": {
                        "items": [
                            {
                                "signal_id": "pc28-predictor-12-20260408001-big_small",
                                "source_ref": {"predictor_id": 12, "predictor_name": "主策略A"},
                                "lottery_type": "pc28",
                                "issue_no": "20260408001",
                                "published_at": "2026-04-08T12:00:00Z",
                                "signals": [
                                    {
                                        "bet_type": "big_small",
                                        "bet_value": "大",
                                        "confidence": 0.77,
                                        "message_text": "大10",
                                    },
                                    {
                                        "bet_type": "odd_even",
                                        "bet_value": "单",
                                        "confidence": 0.61,
                                    },
                                ],
                            }
                        ]
                    },
                },
                headers=self.session_headers,
            ),
        )
        self.assertEqual(create_status, "200 OK")
        raw_item_id = create_payload["item"]["id"]

        normalize_status, _, normalize_payload = invoke(
            self.app,
            build_testing_environ(
                "/api/platform/raw-items/%s/normalize" % raw_item_id,
                method="POST",
                body={},
                headers=self.session_headers,
            ),
        )
        self.assertEqual(normalize_status, "200 OK")
        self.assertEqual(normalize_payload["created_count"], 2)
        self.assertEqual(len(self.repository.signals), 2)
        self.assertEqual(
            self.repository.signals[0]["normalized_payload"]["source_ref"]["predictor_id"],
            12,
        )


if __name__ == "__main__":
    unittest.main()
