from __future__ import annotations

import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from pc28touzhu.services.telegram_account_gateway import TelethonAccountGateway


class FakeMe:
    phone = "13809984536"
    first_name = "xingcai"
    last_name = ""
    username = "xingcai_user"


class FakeTelegramClient:
    instances = []

    def __init__(self, session_path, api_id, api_hash):
        self.session_path = session_path
        self.api_id = api_id
        self.api_hash = api_hash
        self.connected = False
        self.authorized = False
        self.calls = []
        FakeTelegramClient.instances.append(self)

    def connect(self):
        self.connected = True
        self.calls.append("connect")

    def disconnect(self):
        self.connected = False
        self.calls.append("disconnect")

    def sign_in(self, **kwargs):
        self.calls.append(("sign_in", kwargs))
        self.authorized = True

    def is_user_authorized(self):
        self.calls.append("is_user_authorized")
        return self.authorized

    def get_me(self):
        self.calls.append("get_me")
        return FakeMe()


class TestableTelethonAccountGateway(TelethonAccountGateway):
    def _load_client_class(self):
        return FakeTelegramClient


class TelethonAccountGatewayTests(unittest.TestCase):
    def setUp(self):
        FakeTelegramClient.instances = []

    def test_verify_code_reuses_current_client_for_authorization_result(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = TestableTelethonAccountGateway(api_id=1, api_hash="hash").verify_code(
                str(Path(tmpdir) / "account"),
                phone="+13809984536",
                code="12345",
                phone_code_hash="hash",
            )

        self.assertTrue(result["authorized"])
        self.assertEqual(result["phone"], "13809984536")
        self.assertEqual(len(FakeTelegramClient.instances), 1)
        self.assertIn("is_user_authorized", FakeTelegramClient.instances[0].calls)
        self.assertIn("get_me", FakeTelegramClient.instances[0].calls)

    def test_verify_password_reuses_current_client_for_authorization_result(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            result = TestableTelethonAccountGateway(api_id=1, api_hash="hash").verify_password(
                str(Path(tmpdir) / "account"),
                password="secret",
            )

        self.assertTrue(result["authorized"])
        self.assertEqual(result["display_name"], "xingcai")
        self.assertEqual(len(FakeTelegramClient.instances), 1)
        self.assertIn("is_user_authorized", FakeTelegramClient.instances[0].calls)
        self.assertIn("get_me", FakeTelegramClient.instances[0].calls)


if __name__ == "__main__":
    unittest.main()
