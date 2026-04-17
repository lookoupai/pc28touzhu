from __future__ import annotations

import os
import tempfile
import unittest
import urllib.error

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.normalize_service import normalize_raw_item
from pc28touzhu.services.source_fetch_service import fetch_source_to_raw_item


class SourceFetchServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()
        self.user_id = self.repo.create_user("fetch-user")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_fetch_http_json_creates_raw_item(self):
        source = self.repo.create_source_record(
            owner_user_id=self.user_id,
            source_type="http_json",
            name="remote-json",
            config={
                "fetch": {
                    "url": "https://example.com/feed",
                    "issue_no_path": "data.issue_no",
                    "external_item_id_path": "data.id",
                    "published_at_path": "meta.published_at",
                }
            },
        )

        def fake_fetcher(url, headers=None, timeout=10):
            self.assertEqual(url, "https://example.com/feed")
            return {
                "data": {"id": "abc-001", "issue_no": "20260407018"},
                "meta": {"published_at": "2026-04-07T12:00:00Z"},
            }

        result = fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        self.assertEqual(result["source"]["id"], source["id"])
        self.assertEqual(result["raw_item"]["external_item_id"], "abc-001")
        self.assertEqual(result["raw_item"]["issue_no"], "20260407018")
        self.assertEqual(result["raw_item"]["parse_status"], "pending")

    def test_fetch_http_json_passes_default_browser_headers(self):
        source = self.repo.create_source_record(
            owner_user_id=self.user_id,
            source_type="http_json",
            name="remote-json",
            config={
                "fetch": {
                    "url": "https://example.com/feed"
                }
            },
        )

        def fake_fetcher(url, headers=None, timeout=10):
            self.assertIn("User-Agent", headers)
            self.assertIn("Mozilla/5.0", headers["User-Agent"])
            self.assertIn("Accept", headers)
            return {"bet_type": "big_small", "bet_value": "大", "issue_no": "20260407019"}

        result = fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        self.assertEqual(result["raw_item"]["issue_no"], "20260407019")

    def test_fetch_source_rejects_non_http_json(self):
        source = self.repo.create_source_record(
            owner_user_id=self.user_id,
            source_type="internal_ai",
            name="internal",
            config={},
        )

        with self.assertRaises(ValueError):
            fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=lambda *args, **kwargs: {})

    def test_fetch_ai_trading_simulator_export_creates_protocol_raw_item(self):
        source = self.repo.create_source_record(
            owner_user_id=self.user_id,
            source_type="ai_trading_simulator_export",
            name="ai-export",
            config={
                "fetch": {
                    "url": "https://example.com/export"
                }
            },
        )

        def fake_fetcher(url, headers=None, timeout=10):
            self.assertEqual(url, "https://example.com/export")
            return {
                "items": [
                    {
                        "signal_id": "pc28-predictor-12-20260408001-big_small",
                        "issue_no": "20260408001",
                        "published_at": "2026-04-08T12:00:00Z",
                        "signals": [
                            {
                                "bet_type": "big_small",
                                "bet_value": "大",
                                "confidence": 0.78,
                                "message_text": "大10",
                                "base_stake": 10,
                                "multiplier": 2,
                                "max_steps": 6,
                                "refund_action": "hold",
                                "cap_action": "reset",
                                "profit_rule_id": "pc28_high",
                                "odds_profile": "regular",
                                "primary_metric": "big_small",
                                "share_level": "records",
                            }
                        ],
                    }
                ]
            }

        result = fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        self.assertEqual(result["raw_item"]["external_item_id"], "pc28-predictor-12-20260408001-big_small")
        self.assertEqual(result["raw_item"]["issue_no"], "20260408001")
        self.assertEqual(result["raw_item"]["raw_payload"]["items"][0]["signals"][0]["bet_value"], "大")
        normalized = normalize_raw_item(self.repo, raw_item_id=result["raw_item"]["id"])
        signal = normalized["items"][0]
        self.assertEqual(signal["normalized_payload"]["message_text"], "大10")
        self.assertEqual(signal["normalized_payload"]["base_stake"], 10)
        self.assertEqual(signal["normalized_payload"]["multiplier"], 2)
        self.assertEqual(signal["normalized_payload"]["max_steps"], 6)
        self.assertEqual(signal["normalized_payload"]["refund_action"], "hold")
        self.assertEqual(signal["normalized_payload"]["cap_action"], "reset")
        self.assertEqual(signal["normalized_payload"]["profit_rule_id"], "pc28_high")
        self.assertEqual(signal["normalized_payload"]["odds_profile"], "regular")
        self.assertEqual(signal["normalized_payload"]["primary_metric"], "big_small")
        self.assertEqual(signal["normalized_payload"]["share_level"], "records")

    def test_fetch_http_json_surfaces_http_403_as_readable_error(self):
        source = self.repo.create_source_record(
            owner_user_id=self.user_id,
            source_type="http_json",
            name="remote-json",
            config={"fetch": {"url": "https://example.com/feed"}},
        )

        def fake_fetcher(url, headers=None, timeout=10):
            raise ValueError("上游接口返回 HTTP 403 Forbidden")

        with self.assertRaises(ValueError) as ctx:
            fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)

        self.assertIn("403", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
