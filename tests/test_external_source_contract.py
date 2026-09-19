from __future__ import annotations

import json
import os
import tempfile
import unittest
from unittest import mock

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.normalize_service import normalize_raw_item
from pc28touzhu.services.source_fetch_service import fetch_source_to_raw_item

FIXTURES = os.path.join(os.path.dirname(__file__), "fixtures")


def load_external_export_sample() -> dict:
    with open(
        os.path.join(FIXTURES, "ai_trading_simulator_external_export_sample.json"),
        "r",
        encoding="utf-8",
    ) as handle:
        return json.load(handle)


class ExternalSourceContractTests(unittest.TestCase):
    """AITradingSimulator 外部预测源方案导出（schema 1.0）的离线消费契约。

    固定样本由上游真实导出链路生成（外部预测源 + JND 量子模型 + 大小/单双/组合），
    本测试证明现有来源采集、标准化、去重链路无需改动即可消费。
    """

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()
        self.user_id = self.repo.create_user("external-contract-user")
        self.sample = load_external_export_sample()

    def tearDown(self):
        self.tmpdir.cleanup()

    def _make_source(self, **config_extra):
        return self.repo.create_source_record(
            owner_user_id=self.user_id,
            source_type="ai_trading_simulator_export",
            name="external-export",
            config={"fetch": {"url": "https://example.com/export", **config_extra}},
        )

    def test_external_export_fetch_creates_protocol_raw_item(self):
        source = self._make_source()

        def fake_fetcher(url, headers=None, timeout=10):
            return json.loads(json.dumps(self.sample))

        result = fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        self.assertTrue(result["created"])
        raw_item = result["raw_item"]
        item = self.sample["items"][0]
        self.assertEqual(raw_item["external_item_id"], item["signal_id"])
        self.assertEqual(raw_item["issue_no"], item["issue_no"])
        self.assertEqual(raw_item["published_at"], item["published_at"])
        self.assertEqual(raw_item["parse_status"], "pending")

    def test_external_export_normalizes_big_small_odd_even_combo(self):
        source = self._make_source()

        def fake_fetcher(url, headers=None, timeout=10):
            return json.loads(json.dumps(self.sample))

        result = fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        normalized = normalize_raw_item(self.repo, raw_item_id=result["raw_item"]["id"])
        entries = normalized["items"]
        bet_pairs = {(entry["bet_type"], entry["bet_value"]) for entry in entries}
        self.assertEqual(
            bet_pairs,
            {("big_small", "大"), ("odd_even", "单"), ("combo", "大单")},
        )
        for entry in entries:
            self.assertEqual(entry["issue_no"], self.sample["items"][0]["issue_no"])
            payload = entry.get("normalized_payload") or {}
            self.assertEqual(payload.get("source_ref", {}).get("engine_type"), "external")
            self.assertEqual(payload.get("source_ref", {}).get("model_key"), "quantum")

    def test_external_export_duplicate_fetch_reuses_existing_raw_item(self):
        source = self._make_source()

        def fake_fetcher(url, headers=None, timeout=10):
            return json.loads(json.dumps(self.sample))

        first = fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        second = fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        self.assertTrue(first["created"])
        self.assertFalse(second["created"])
        self.assertEqual(first["raw_item"]["id"], second["raw_item"]["id"])

    def test_external_export_empty_items_is_normal_no_signal(self):
        source = self._make_source()

        def fake_fetcher(url, headers=None, timeout=10):
            return {"items": []}

        result = fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        self.assertIsNone(result["raw_item"])
        self.assertEqual(result.get("skipped_reason"), "upstream_no_signal")

    def test_external_export_headers_support_bearer_token(self):
        source = self._make_source(headers={"Authorization": "Bearer pts_abcd1234"})

        captured = {}

        def fake_fetcher(url, headers=None, timeout=10):
            captured["headers"] = headers
            return json.loads(json.dumps(self.sample))

        fetch_source_to_raw_item(self.repo, source_id=source["id"], fetcher=fake_fetcher)
        self.assertEqual(captured["headers"].get("Authorization"), "Bearer pts_abcd1234")


if __name__ == "__main__":
    unittest.main()
_ = mock
