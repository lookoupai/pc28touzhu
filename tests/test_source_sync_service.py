from __future__ import annotations

import os
import tempfile
import unittest

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.services.source_sync_service import collect_active_source_ids, run_source_sync_cycle


class SourceSyncServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmpdir.name, "test.db")
        self.repo = DatabaseRepository(self.db_path)
        self.repo.initialize_database()
        self.user_id = self.repo.create_user("sync-user")
        self.source = self.repo.create_source_record(
            owner_user_id=self.user_id,
            source_type="ai_trading_simulator_export",
            name="sync-source",
            status="active",
            config={
                "fetch": {
                    "url": "https://example.com/api/export/predictors/5/signals?view=execution",
                    "headers": {"Accept": "application/json"},
                    "timeout": 10,
                }
            },
        )
        self.repo.create_subscription_record(
            user_id=self.user_id,
            source_id=self.source["id"],
            status="active",
            strategy={"stake_amount": 10},
        )
        self.repo.create_delivery_target_record(
            user_id=self.user_id,
            executor_type="telegram_group",
            target_key="-100123456",
            target_name="测试群",
            status="active",
        )

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_collect_active_source_ids_returns_active_subscription_sources(self):
        self.assertEqual(collect_active_source_ids(self.repo), [self.source["id"]])

    def test_run_source_sync_cycle_creates_jobs_once(self):
        payload = {
            "items": [
                {
                    "signal_id": "pc28-predictor-5-20260418001-big_small",
                    "issue_no": "20260418001",
                    "published_at": "2026-04-18T09:30:00Z",
                    "signals": [
                        {
                            "bet_type": "big_small",
                            "bet_value": "大",
                        }
                    ],
                }
            ]
        }

        first = run_source_sync_cycle(self.repo, fetcher=lambda *args, **kwargs: payload)
        self.assertEqual(first["summary"]["fetched_count"], 1)
        self.assertEqual(first["summary"]["normalized_signal_count"], 1)
        self.assertEqual(first["summary"]["created_job_count"], 1)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 1)

        second = run_source_sync_cycle(self.repo, fetcher=lambda *args, **kwargs: payload)
        self.assertEqual(second["summary"]["skipped_duplicate_count"], 1)
        self.assertEqual(second["summary"]["created_job_count"], 0)
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 1)


if __name__ == "__main__":
    unittest.main()
