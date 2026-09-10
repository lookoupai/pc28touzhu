from __future__ import annotations

import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from pc28touzhu.executor.db_repository import DatabaseRepository
from pc28touzhu.executor.profit_reporting import BACKFILL_KEY
from pc28touzhu.services.telegram_bot_service import handle_telegram_command


class ProfitReportingTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.repo = DatabaseRepository(str(Path(self.tmpdir.name) / "reports.db"))
        self.repo.initialize_database()
        self.sequence = 0
        self.actor = self._actor(701)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _actor(self, telegram_id):
        user = self.repo.create_user_record(username="report-user-%s" % telegram_id, email="", role="user", status="active")
        source = self.repo.create_source_record(
            owner_user_id=user["id"], source_type="internal_ai", name="PC28方案单双共识",
        )
        subscription = self.repo.create_subscription_record(user_id=user["id"], source_id=source["id"], strategy={})
        self.repo.update_user_telegram_binding(
            user_id=user["id"], telegram_user_id=telegram_id, telegram_chat_id=str(telegram_id),
            telegram_username="", telegram_bound_at="2026-09-01T00:00:00Z",
        )
        return {"user_id": user["id"], "source_id": source["id"], "subscription_id": subscription["id"], "telegram_id": telegram_id}

    def _rule(self, *, actor=None, name="单双共识定时跟单"):
        actor = actor or self.actor
        with self.repo._connect() as conn:
            return conn.execute(
                "INSERT INTO auto_trigger_rules(user_id, name, status) VALUES (?, ?, 'archived')",
                (actor["user_id"], name),
            ).lastrowid

    def _route(self, rule_id):
        target = self.repo.create_delivery_target_record(
            user_id=self.actor["user_id"], executor_type="telegram_group", target_key="-1007", target_name="测试群",
        )
        with self.repo._connect() as conn:
            return conn.execute(
                "INSERT INTO auto_trigger_rule_routes(rule_id, user_id, delivery_target_id) VALUES (?, ?, ?)",
                (rule_id, self.actor["user_id"], target["id"]),
            ).lastrowid

    def _event(self, profit, *, actor=None, settled_at="2026-08-15T08:00:00Z", rule_id=None, route_id=None, status="settled"):
        actor = actor or self.actor
        self.sequence += 1
        signal = self.repo.create_signal_record(
            source_id=actor["source_id"], lottery_type="pc28", issue_no=str(self.sequence), bet_type="odd_even", bet_value="单",
        )
        # Raw SQL also represents an older worker writing during an upgrade.
        with self.repo._connect() as conn:
            return conn.execute(
                """
                INSERT INTO subscription_progression_events(
                    subscription_id, user_id, signal_id, status, resolved_result_type,
                    profit_delta, loss_delta, net_delta, settled_at,
                    auto_trigger_rule_id, auto_trigger_route_id, auto_trigger_stat_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '2026-07-31')
                """,
                (actor["subscription_id"], actor["user_id"], signal["id"], status,
                 "hit" if profit > 0 else "miss" if profit < 0 else "refund",
                 max(profit, 0), max(-profit, 0), profit, settled_at, rule_id, route_id),
            ).lastrowid

    def _query(self, text, *, actor=None):
        actor = actor or self.actor
        return handle_telegram_command(
            self.repo, telegram_user_id=actor["telegram_id"], telegram_chat_id=str(actor["telegram_id"]),
            telegram_username="", text=text, reference_time=datetime(2026, 9, 10, tzinfo=timezone.utc),
        )

    def _delete_subscription(self, *, actor=None):
        actor = actor or self.actor
        self.repo.update_subscription_status(subscription_id=actor["subscription_id"], user_id=actor["user_id"], status="archived")
        return self.repo.delete_subscription_record(subscription_id=actor["subscription_id"], user_id=actor["user_id"])

    def _manual_summary(self, month="2026-08"):
        return self.repo.get_user_monthly_profit_summary(user_id=self.actor["user_id"], stat_month=month)

    def test_exact_named_plan_and_rule_queries_accept_relative_months(self):
        rule_id = self._rule()
        self._event(18)
        self._event(-7, rule_id=rule_id, settled_at="2026-09-05T08:00:00Z")
        self.assertIn("净利润: +18.00", self._query("/plan PC28方案单双共识 上个月"))
        for period in ("本月", "这个月", "2026-09"):
            with self.subTest(period=period):
                self.assertIn("净利润: -7.00", self._query("/rule 单双共识定时跟单 " + period))
        self.assertNotIn("-7.00", self._query("/plan PC28方案单双共识 上个月"))

    def test_settlement_corrections_move_month_without_double_counting(self):
        event_id = self._event(10, settled_at="2026-08-31T15:59:59Z")
        self._event(-4)
        self._event(0)
        self.assertEqual(self._manual_summary()["settled_event_count"], 3)
        with self.repo._connect() as conn:
            for _ in range(2):
                conn.execute(
                    "UPDATE subscription_progression_events SET profit_delta = 12, net_delta = 12, settled_at = '2026-08-31T16:00:00Z' WHERE id = ?",
                    (event_id,),
                )
        august, september = self._manual_summary(), self._manual_summary("2026-09")
        self.assertEqual((august["net_profit"], august["settled_event_count"], august["refund_count"]), (-4, 2, 1))
        self.assertEqual((september["net_profit"], september["settled_event_count"], september["hit_count"]), (12, 1, 1))
        with self.repo._connect() as conn:
            conn.execute("UPDATE subscription_progression_events SET status = 'pending', settled_at = NULL WHERE id = ?", (event_id,))
        self.assertEqual(self._manual_summary("2026-09")["settled_event_count"], 0)

    def test_settlement_and_report_roll_back_together(self):
        event_id = self._event(0, status="placed", settled_at=None)
        with self.assertRaisesRegex(RuntimeError, "rollback"):
            with self.repo._connect() as conn:
                conn.execute(
                    "UPDATE subscription_progression_events SET status = 'settled', settled_at = '2026-08-15T08:00:00Z', net_delta = 20, profit_delta = 20 WHERE id = ?",
                    (event_id,),
                )
                raise RuntimeError("rollback")
        self.assertEqual(self._manual_summary()["settled_event_count"], 0)
        self.assertEqual(self.repo.get_progression_event(event_id)["status"], "placed")

    def test_existing_settlements_backfill_once_and_survive_later_detail_purge(self):
        rule_id = self._rule()
        self._event(10)
        self._event(-4, rule_id=rule_id)
        with self.repo._connect() as conn:
            for trigger in (
                "profit_report_settlement_insert", "profit_report_settlement_update",
                "profit_report_subscription_delete", "profit_report_rule_delete",
            ):
                conn.execute("DROP TRIGGER " + trigger)
            conn.execute("DROP TABLE profit_report_daily_stats")
            conn.execute("DELETE FROM platform_runtime_settings WHERE setting_key = ?", (BACKFILL_KEY,))
            # Legacy counters can mix manual/auto or use a different business day.
            conn.execute(
                "INSERT INTO subscription_daily_stats(stat_date, user_id, subscription_id, source_id, net_profit) VALUES ('2026-08-15', ?, ?, ?, 999)",
                (self.actor["user_id"], self.actor["subscription_id"], self.actor["source_id"]),
            )
        self.repo.initialize_database()
        self.repo.initialize_database()
        self.assertEqual(self._manual_summary()["net_profit"], 10)
        self.assertIn("净利润: -4.00", self._query("/rule #%s 上个月" % rule_id))
        with self.repo._connect() as conn:
            conn.execute("DELETE FROM subscription_progression_events")
        self.repo.initialize_database()
        ranking = self.repo.list_monthly_user_profit_rankings(stat_month="2026-08")[0]
        self.assertEqual((ranking["net_profit"], ranking["settled_event_count"]), (6, 2))
        self._event(3)
        self.assertEqual(self._manual_summary()["net_profit"], 13)

    def test_runtime_retention_does_not_remove_reports_older_than_a_year(self):
        rule_id = self._rule()
        self._event(23, rule_id=rule_id, settled_at="2024-08-15T08:00:00Z")
        with self.repo._connect() as conn:
            conn.execute(
                "INSERT INTO auto_trigger_rule_daily_stats(rule_id, user_id, stat_date, net_profit) VALUES (?, ?, '2024-08-15', 23)",
                (rule_id, self.actor["user_id"]),
            )
        result = self.repo.prune_auto_trigger_rule_runtime_data(runs_cutoff="2026-06-01", stats_cutoff="2025-09-01")
        self.assertEqual(result["deleted_stats_count"], 1)
        with self.repo._connect() as conn:
            conn.execute("DELETE FROM subscription_progression_events")
        self.assertIn("净利润: +23.00", self._query("/rule #%s 2024-08" % rule_id))

    def test_subscription_and_source_deletion_keep_manual_and_auto_history(self):
        rule_id = self._rule()
        self._event(10)
        self._event(-4, rule_id=rule_id)
        self.assertTrue(self._delete_subscription())
        with self.repo._connect() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM subscription_progression_events").fetchone()[0], 0)
            conn.execute("DELETE FROM normalized_signals WHERE source_id = ?", (self.actor["source_id"],))
        self.assertTrue(self.repo.delete_source_record(source_id=self.actor["source_id"], owner_user_id=self.actor["user_id"]))
        plan = self._query("/plan PC28方案单双共识 上个月")
        self.assertIn("已删除", plan)
        self.assertIn("净利润: +10.00", plan)
        self.assertIn("净利润: -4.00", self._query("/rule 单双共识定时跟单 上个月"))
        ranking = self.repo.list_monthly_user_profit_rankings(stat_month="2026-08")[0]
        self.assertEqual((ranking["net_profit"], ranking["manual_net_profit"], ranking["auto_net_profit"]), (6, 10, -4))

    def test_rule_deletion_detaches_routes_and_keeps_identity_and_rankings(self):
        rule_id = self._rule()
        route_id = self._route(rule_id)
        event_id = self._event(-15, route_id=route_id)
        with self.repo._connect() as conn:
            conn.execute(
                "INSERT INTO auto_trigger_route_daily_stats(route_id, rule_id, user_id, stat_date) VALUES (?, ?, ?, '2026-08-15')",
                (route_id, rule_id, self.actor["user_id"]),
            )
            conn.execute(
                "INSERT INTO auto_trigger_rule_daily_stats(rule_id, user_id, stat_date) VALUES (?, ?, '2026-08-15')",
                (rule_id, self.actor["user_id"]),
            )
        self.assertTrue(self.repo.delete_auto_trigger_rule_record(rule_id=rule_id, user_id=self.actor["user_id"]))
        event = self.repo.get_progression_event(event_id)
        self.assertEqual(event["auto_trigger_rule_id"], rule_id)
        self.assertIsNone(event["auto_trigger_route_id"])
        self.assertIsNone(self.repo.get_auto_trigger_rule(rule_id))
        report = self._query("/rule 单双共识定时跟单 上个月")
        self.assertIn("已删除", report)
        self.assertIn("净利润: -15.00", report)
        self.assertEqual(self._manual_summary()["net_profit"], 0)
        self._delete_subscription()
        self.assertIn("净利润: -15.00", self._query("/rule #%s 上个月" % rule_id))
        self.assertEqual(self.repo.list_monthly_user_profit_rankings(stat_month="2026-08")[0]["net_profit"], -15)

    def test_deletion_snapshots_the_latest_name_even_without_new_settlements(self):
        rule_id = self._rule()
        self._event(10)
        self._event(20, rule_id=rule_id)
        with self.repo._connect() as conn:
            conn.execute("UPDATE signal_sources SET name = '改名后的方案' WHERE id = ?", (self.actor["source_id"],))
            conn.execute("UPDATE auto_trigger_rules SET name = '改名后的规则' WHERE id = ?", (rule_id,))
        self._delete_subscription()
        self.repo.delete_auto_trigger_rule_record(rule_id=rule_id, user_id=self.actor["user_id"])
        self.assertIn("净利润: +10.00", self._query("/plan 改名后的方案 上个月"))
        self.assertIn("净利润: +20.00", self._query("/rule 改名后的规则 上个月"))

    def test_recreated_same_names_have_separate_ids_and_query_candidates(self):
        old_subscription_id = self.actor["subscription_id"]
        old_rule_id = self._rule()
        self._event(-10)
        self._event(-20, rule_id=old_rule_id)
        self._delete_subscription()
        self.repo.delete_auto_trigger_rule_record(rule_id=old_rule_id, user_id=self.actor["user_id"])
        new_subscription = self.repo.create_subscription_record(user_id=self.actor["user_id"], source_id=self.actor["source_id"], strategy={})
        self.actor["subscription_id"] = new_subscription["id"]
        new_rule_id = self._rule()
        self._event(30)
        self._event(40, rule_id=new_rule_id)
        self.assertNotEqual(old_subscription_id, new_subscription["id"])
        self.assertNotEqual(old_rule_id, new_rule_id)
        plans, rules = self._query("/plan PC28方案单双共识 上个月"), self._query("/rule 单双共识定时跟单 上个月")
        self.assertIn("重名方案", plans)
        self.assertIn("重名规则", rules)
        for entity_id in (old_subscription_id, new_subscription["id"]):
            self.assertIn("#%s" % entity_id, plans)
        for entity_id in (old_rule_id, new_rule_id):
            self.assertIn("#%s" % entity_id, rules)
        self.assertIn("净利润: -10.00", self._query("/plan #%s 上个月" % old_subscription_id))
        self.assertIn("净利润: +30.00", self._query("/plan #%s 上个月" % new_subscription["id"]))
        self.assertIn("净利润: -20.00", self._query("/rule #%s 上个月" % old_rule_id))
        self.assertIn("净利润: +40.00", self._query("/rule #%s 上个月" % new_rule_id))

    def test_private_history_and_deletion_are_scoped_to_the_bound_user(self):
        other = self._actor(702)
        own_rule, other_rule = self._rule(), self._rule(actor=other)
        self._event(10)
        self._event(20, rule_id=own_rule)
        self._event(700, actor=other)
        self._event(900, actor=other, rule_id=other_rule)
        self.assertFalse(self.repo.delete_subscription_record(subscription_id=other["subscription_id"], user_id=self.actor["user_id"]))
        self.assertFalse(self.repo.delete_auto_trigger_rule_record(rule_id=other_rule, user_id=self.actor["user_id"]))
        self._delete_subscription(actor=other)
        self.repo.delete_auto_trigger_rule_record(rule_id=other_rule, user_id=other["user_id"])
        self.assertIn("净利润: +10.00", self._query("/plan PC28方案单双共识 上个月"))
        self.assertIn("净利润: +20.00", self._query("/rule 单双共识定时跟单 上个月"))
        self.assertNotIn("700.00", self._query("/plan #%s 上个月" % other["subscription_id"]))
        self.assertNotIn("900.00", self._query("/rule #%s 上个月" % other_rule))
        self.assertIn("净利润: +700.00", self._query("/plan PC28方案单双共识 上个月", actor=other))
        self.assertIn("净利润: +900.00", self._query("/rule 单双共识定时跟单 上个月", actor=other))

    def test_unsettled_bets_prevent_deletion_without_losing_data(self):
        rule_id = self._rule()
        event_id = self._event(0, rule_id=rule_id, status="placed", settled_at=None)
        with self.assertRaisesRegex(ValueError, "待结算"):
            self._delete_subscription()
        with self.assertRaisesRegex(ValueError, "待结算"):
            self.repo.delete_auto_trigger_rule_record(rule_id=rule_id, user_id=self.actor["user_id"])
        self.assertEqual(self.repo.get_progression_event(event_id)["status"], "placed")
        self.assertIsNotNone(self.repo.get_subscription(self.actor["subscription_id"]))
        self.assertIsNotNone(self.repo.get_auto_trigger_rule(rule_id))

    def test_active_rule_runs_must_be_stopped_before_deletion(self):
        rule_id = self._rule()
        with self.repo._connect() as conn:
            conn.execute(
                "INSERT INTO auto_trigger_rule_runs(rule_id, user_id, subscription_id, stat_date, started_at) VALUES (?, ?, ?, '2026-08-15', '2026-08-15T00:00:00Z')",
                (rule_id, self.actor["user_id"], self.actor["subscription_id"]),
            )
        with self.assertRaisesRegex(ValueError, "停止轮次"):
            self.repo.delete_auto_trigger_rule_record(rule_id=rule_id, user_id=self.actor["user_id"])


if __name__ == "__main__":
    unittest.main()
