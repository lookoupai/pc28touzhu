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

    def test_verify_password_maps_invalid_hash_to_password_invalid(self):
        class InvalidPasswordClient(FakeTelegramClient):
            def sign_in(self, **kwargs):
                self.calls.append(("sign_in", kwargs))
                raise type("PasswordHashInvalidError", (Exception,), {})("invalid")

        class Gateway(TelethonAccountGateway):
            def _load_client_class(self):
                return InvalidPasswordClient

        with tempfile.TemporaryDirectory() as tmpdir:
            result = Gateway(api_id=1, api_hash="hash").verify_password(
                str(Path(tmpdir) / "account"),
                password="wrong",
            )

        self.assertEqual(result, {"authorized": False, "password_invalid": True})

    def test_verify_password_maps_flood_wait_to_password_flood(self):
        class FloodWaitClient(FakeTelegramClient):
            def sign_in(self, **kwargs):
                self.calls.append(("sign_in", kwargs))
                error = type("FloodWaitError", (Exception,), {})(
                    "A wait of 2867 seconds is required (caused by CheckPasswordRequest)"
                )
                error.seconds = 2867
                raise error

        class Gateway(TelethonAccountGateway):
            def _load_client_class(self):
                return FloodWaitClient

        with tempfile.TemporaryDirectory() as tmpdir:
            result = Gateway(api_id=1, api_hash="hash").verify_password(
                str(Path(tmpdir) / "account"),
                password="secret",
            )

        self.assertEqual(
            result,
            {"authorized": False, "password_flood": True, "wait_seconds": 2867},
        )

    def test_send_login_code_resets_stale_session_before_first_request(self):
        class FreshClient(FakeTelegramClient):
            send_attempts = 0

            def send_code_request(self, phone):
                type(self).send_attempts += 1
                self.calls.append(("send_code_request", phone))
                session_file = Path(str(self.session_path))
                if session_file.suffix != ".session":
                    session_file = Path(str(self.session_path) + ".session")
                if session_file.exists() and session_file.read_bytes() == b"stale":
                    raise type("AuthRestartError", (Exception,), {})("restart")
                return type("Sent", (), {"phone_code_hash": "new-hash"})()

        class Gateway(TelethonAccountGateway):
            def _load_client_class(self):
                return FreshClient

        with tempfile.TemporaryDirectory() as tmpdir:
            session_path = str(Path(tmpdir) / "account")
            session_file = Path(session_path + ".session")
            session_file.write_bytes(b"stale")
            result = Gateway(api_id=1, api_hash="hash").send_login_code(session_path, "+13809984536")
            self.assertFalse(session_file.exists() and session_file.read_bytes() == b"stale")

        self.assertEqual(result["phone_code_hash"], "new-hash")
        self.assertTrue(result["session_reset"])
        self.assertEqual(FreshClient.send_attempts, 1)

    def test_send_login_code_retries_auth_restart_after_reset(self):
        class RestartThenOkClient(FakeTelegramClient):
            send_attempts = 0

            def send_code_request(self, phone):
                type(self).send_attempts += 1
                self.calls.append(("send_code_request", phone))
                if type(self).send_attempts == 1:
                    raise type("AuthRestartError", (Exception,), {})("restart")
                return type("Sent", (), {"phone_code_hash": "new-hash"})()

        class Gateway(TelethonAccountGateway):
            def _load_client_class(self):
                return RestartThenOkClient

        with tempfile.TemporaryDirectory() as tmpdir:
            session_path = str(Path(tmpdir) / "account")
            result = Gateway(api_id=1, api_hash="hash").send_login_code(session_path, "+13809984536")

        self.assertEqual(result["phone_code_hash"], "new-hash")
        self.assertTrue(result["session_reset"])
        self.assertEqual(RestartThenOkClient.send_attempts, 2)


if __name__ == "__main__":
    unittest.main()
