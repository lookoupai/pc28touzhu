from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.services.platform_service import (
    ActionableValueError,
    begin_telegram_account_login,
    verify_telegram_account_login_password,
)


class FakeRepository:
    def __init__(self, account):
        self.account = dict(account)
        self.updates = []

    def get_telegram_account(self, telegram_account_id):
        if int(telegram_account_id) != int(self.account["id"]):
            return None
        return dict(self.account)

    def update_telegram_account_record(self, **kwargs):
        self.updates.append(kwargs)
        self.account.update(
            {
                "label": kwargs.get("label", self.account.get("label")),
                "session_path": kwargs.get("session_path", self.account.get("session_path")),
                "phone": kwargs.get("phone", self.account.get("phone")),
                "meta": kwargs.get("meta", self.account.get("meta")),
            }
        )
        return dict(self.account)

    def update_telegram_account_status(self, **kwargs):
        self.account["status"] = kwargs.get("status")
        return dict(self.account)


class FakeGateway:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def verify_password(self, session_path, *, password):
        self.calls.append((session_path, password))
        return dict(self.result)

    def send_login_code(self, session_path, phone):
        self.calls.append((session_path, phone))
        if isinstance(self.result, Exception):
            raise self.result
        return dict(self.result)


class TelegramLoginPasswordTests(unittest.TestCase):
    def _account(self, **overrides):
        payload = {
            "id": 3,
            "user_id": 1,
            "label": "13809984536",
            "phone": "+13809984536",
            "session_path": "/tmp/account",
            "status": "inactive",
            "meta": {"auth_mode": "phone_login", "auth_state": "password_required"},
        }
        payload.update(overrides)
        return payload

    def test_invalid_password_stays_password_required(self):
        repository = FakeRepository(self._account())
        gateway = FakeGateway({"authorized": False, "password_invalid": True})
        with self.assertRaises(ActionableValueError) as ctx:
            verify_telegram_account_login_password(
                repository,
                telegram_account_id=3,
                user_id=1,
                payload={"password": "wrong"},
                auth_gateway=gateway,
            )
        self.assertEqual(ctx.exception.payload["reason_code"], "password_invalid")
        self.assertEqual(repository.account["meta"]["auth_state"], "password_required")
        self.assertEqual(repository.account["meta"]["last_auth_error"], "二次密码不正确")

    def test_password_flood_stays_password_required(self):
        repository = FakeRepository(self._account())
        gateway = FakeGateway({"authorized": False, "password_flood": True, "wait_seconds": 2867})
        with self.assertRaises(ActionableValueError) as ctx:
            verify_telegram_account_login_password(
                repository,
                telegram_account_id=3,
                user_id=1,
                payload={"password": "secret"},
                auth_gateway=gateway,
            )
        self.assertEqual(ctx.exception.payload["reason_code"], "password_flood")
        self.assertIn("47 分钟", ctx.exception.payload["error"])
        self.assertEqual(repository.account["meta"]["auth_state"], "password_required")
        self.assertIn("47 分钟", repository.account["meta"]["last_auth_error"])

    def test_success_marks_account_authorized(self):
        repository = FakeRepository(self._account())
        gateway = FakeGateway({"authorized": True, "phone": "13809984536"})
        result = verify_telegram_account_login_password(
            repository,
            telegram_account_id=3,
            user_id=1,
            payload={"password": "secret"},
            auth_gateway=gateway,
        )
        self.assertEqual(result["item"]["auth_state"], "authorized")
        self.assertEqual(repository.account["status"], "active")

    def test_expired_password_flow_requires_resend(self):
        repository = FakeRepository(self._account())
        gateway = FakeGateway({"authorized": False, "login_expired": True})
        with self.assertRaises(ActionableValueError) as ctx:
            verify_telegram_account_login_password(
                repository,
                telegram_account_id=3,
                user_id=1,
                payload={"password": "secret"},
                auth_gateway=gateway,
            )
        self.assertEqual(ctx.exception.payload["reason_code"], "login_expired")
        self.assertEqual(repository.account["meta"]["auth_state"], "login_expired")

    def test_send_code_auth_restart_marks_login_expired(self):
        repository = FakeRepository(self._account())
        gateway = FakeGateway(type("AuthRestartError", (Exception,), {})("restart"))
        with self.assertRaises(ActionableValueError) as ctx:
            begin_telegram_account_login(
                repository,
                telegram_account_id=3,
                user_id=1,
                payload={"phone": "+13809984536"},
                auth_gateway=gateway,
            )
        self.assertEqual(ctx.exception.payload["reason_code"], "login_expired")
        self.assertEqual(repository.account["meta"]["auth_state"], "login_expired")
