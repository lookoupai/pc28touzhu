from __future__ import annotations

import copy
import tempfile
import unittest
from pathlib import Path

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.normalize_service import normalize_raw_item
from pc28touzhu.services.source_fetch_service import fetch_source_to_raw_item
from pc28touzhu.services.dispatch_service import dispatch_signal


class GroupSourceContractTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = DatabaseRepository(str(Path(self.tmp.name) / "source.db"))
        self.repo.initialize_database()
        self.sequence = 0
        self.user = self.repo.create_user("group-source-user")
        self.source = self.repo.create_source_record(
            owner_user_id=self.user, source_type="ai_trading_simulator_export", name="旧来源",
            config={"fetch": {"url": "https://example.com/api/export/predictors/5/signals?view=execution"}},
        )

    def ingest(self, signals, external_id="source-5-1001"):
        self.sequence += 1
        payload = {"items": [{
            "schema_version": "1.0", "signal_id": external_id, "issue_no": "1001", "lottery_type": "pc28",
            "published_at": "2026-11-09T00:00:%02dZ" % self.sequence,
            "source_ref": {"predictor_id": 5}, "signals": copy.deepcopy(signals),
        }]}
        fetched = fetch_source_to_raw_item(self.repo, source_id=self.source["id"], fetcher=lambda *args, **kwargs: payload)
        return fetched["raw_item"]

    def test_legacy_combo_derives_both_groups_without_enabling_all_subscriptions(self):
        raw = self.ingest([{"bet_type": "combo", "bet_value": "大单", "message_text": "大单99"}])
        result = normalize_raw_item(self.repo, raw["id"])
        self.assertEqual([(item["bet_type"], item["bet_value"]) for item in result["items"]], [
            ("double_group", "大单+小双"), ("kill_group", "小双"), ("combo", "大单"),
        ])
        for signal in result["items"][:2]:
            self.assertEqual(signal["normalized_payload"]["origin_combo"], "大单")
            self.assertEqual(signal["normalized_payload"]["source_ref"]["predictor_id"], 5)
        self.repo.create_subscription_record(user_id=self.user, source_id=self.source["id"], strategy={"stake_amount": 10})
        self.repo.create_delivery_target_record(user_id=self.user, executor_type="telegram_group", target_key="-101", status="active")
        self.assertEqual(sum(dispatch_signal(self.repo, item["id"])["created_count"] for item in result["items"]), 1)
        self.assertEqual(normalize_raw_item(self.repo, raw["id"])["created_count"], 0)

    def test_explicit_groups_take_precedence_and_are_not_duplicated(self):
        raw = self.ingest([
            {"bet_type": "combo", "bet_value": "大单"},
            {"bet_type": "double_group", "bet_value": "大双+大单", "normalized_payload": {"selected_combos": ["大双", "大单"]}},
            {"bet_type": "kill_group", "bet_value": "小单", "selected_combos": ["大单", "大双", "小双"], "excluded_combo": "小单"},
        ])
        result = normalize_raw_item(self.repo, raw["id"])
        self.assertEqual(result["created_count"], 3)
        values = {item["bet_type"]: item["bet_value"] for item in result["items"]}
        self.assertEqual(values, {"double_group": "大单+大双", "kill_group": "小单", "combo": "大单"})

    def test_conflicting_payload_leaves_no_partially_ingested_signals(self):
        raw = self.ingest([
            {"bet_type": "combo", "bet_value": "大单"},
            {"bet_type": "kill_group", "bet_value": "小双", "normalized_payload": {"selected_combos": ["大单", "大双", "小双"]}},
        ])
        with self.assertRaises(ValueError):
            normalize_raw_item(self.repo, raw["id"])
        self.assertEqual(self.repo.list_signals_by_raw_item(raw["id"]), [])
        self.assertEqual(self.repo.get_raw_item(raw["id"])["parse_status"], "failed")

    def test_ambiguous_combo_predictions_do_not_fabricate_group_direction(self):
        raw = self.ingest([{"bet_type": "combo", "bet_value": "大单"}, {"bet_type": "combo", "bet_value": "大双"}])
        result = normalize_raw_item(self.repo, raw["id"])
        self.assertEqual([item["bet_type"] for item in result["items"]], ["combo", "combo"])

    def test_duplicate_explicit_group_is_collapsed_but_conflict_is_rejected(self):
        raw = self.ingest([{"bet_type": "double_group", "bet_value": "单组"}, {"bet_type": "double_group", "bet_value": "小双+大单"}])
        self.assertEqual(normalize_raw_item(self.repo, raw["id"])["created_count"], 1)

    def test_new_envelope_does_not_repeat_groups_or_change_frozen_direction(self):
        first = self.ingest([{"bet_type": "combo", "bet_value": "大单"}])
        original = normalize_raw_item(self.repo, first["id"])
        second = self.ingest([{"bet_type": "combo", "bet_value": "大单"}], "source-new-envelope")
        repeated = normalize_raw_item(self.repo, second["id"])
        self.assertEqual(repeated["created_count"], 1)  # Only the legacy combo is a new signal.
        self.assertEqual([item["id"] for item in repeated["items"][:2]], [item["id"] for item in original["items"][:2]])
        revised = self.ingest([{"bet_type": "combo", "bet_value": "大双"}], "source-revised-envelope")
        result = normalize_raw_item(self.repo, revised["id"])
        self.assertEqual([(item["bet_type"], item["bet_value"]) for item in result["items"]], [("combo", "大双")])
        conflict = self.ingest([{"bet_type": "kill_group", "bet_value": "小单"}], "source-conflict-envelope")
        with self.assertRaisesRegex(ValueError, "冻结"):
            normalize_raw_item(self.repo, conflict["id"])


if __name__ == "__main__":
    unittest.main()
