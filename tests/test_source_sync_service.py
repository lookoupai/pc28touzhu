from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone

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

    def test_run_source_sync_cycle_retries_gate_blocked_signal(self):
        now_text = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload = {
            "items": [
                {
                    "signal_id": "pc28-predictor-5-900000001-odd_even",
                    "issue_no": "900000002",
                    "published_at": now_text,
                    "signals": [{"bet_type": "odd_even", "bet_value": "双"}],
                }
            ]
        }
        # 阻塞时钟：data[0] 开奖于 180 秒前 → 目标期余量 30 秒 < 40 秒闸线
        blocking_clock = {
            "latest_issue_no": "900000001",
            "latest_open_time": (datetime.now(timezone.utc) - timedelta(seconds=180)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "countdown_seconds": 30,
            "fetched_at": now_text,
            "stale": False,
        }
        # 恢复时钟：data[0] 开奖于 90 秒前 → 目标期余量 120 秒
        recovered_clock = {
            "latest_issue_no": "900000001",
            "latest_open_time": (datetime.now(timezone.utc) - timedelta(seconds=90)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "countdown_seconds": 120,
            "fetched_at": now_text,
            "stale": False,
        }

        first = run_source_sync_cycle(self.repo, fetcher=lambda *args, **kwargs: payload, draw_clock=blocking_clock)
        self.assertEqual(first["summary"]["created_job_count"], 0)
        signal_id = self.repo.list_signals(source_id=self.source["id"])[0]["id"]
        blocked_signal = self.repo.get_signal(signal_id)
        self.assertIsNotNone(blocked_signal.get("dispatch_blocked_at"))
        self.assertEqual(blocked_signal.get("dispatch_block_reason"), "window_too_short")
        self.assertIn("remaining_seconds", blocked_signal.get("dispatch_block_verdict_json") or "")

        # 第二周期：来源去重跳过，重试扫描用恢复后的时钟补派并清除标记
        second = run_source_sync_cycle(self.repo, fetcher=lambda *args, **kwargs: payload, draw_clock=recovered_clock)
        self.assertEqual(second["summary"]["blocked_retry_checked_count"], 1)
        self.assertEqual(second["summary"]["blocked_retry_created_job_count"], 1)
        retried_signal = self.repo.get_signal(signal_id)
        self.assertIsNone(retried_signal.get("dispatch_blocked_at"))
        self.assertEqual(len(self.repo.list_execution_jobs(user_id=self.user_id)), 1)

    def test_run_source_sync_cycle_skips_upstream_no_signal_without_failure(self):
        result = run_source_sync_cycle(
            self.repo,
            fetcher=lambda *args, **kwargs: {"items": []},
        )

        self.assertEqual(result["summary"]["failed_count"], 0)
        self.assertEqual(result["summary"]["skipped_no_signal_count"], 1)
        self.assertEqual(result["sources"][0]["status"], "skipped")
        self.assertEqual(result["sources"][0]["skipped_reason"], "upstream_no_signal")
        self.assertEqual(len(self.repo.list_raw_items(source_id=self.source["id"])), 0)


if __name__ == "__main__":
    unittest.main()
