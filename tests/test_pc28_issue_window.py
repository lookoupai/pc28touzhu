from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from pc28touzhu.domain.pc28_issue_window import evaluate_pc28_issue_dispatch_window


def _clock(**overrides) -> dict:
    clock = {
        "latest_issue_no": "3477884",
        "latest_open_time": "2026-09-04T10:00:30Z",
        "countdown_seconds": 180,
        "fetched_at": "2026-09-04T10:00:30Z",
        "stale": False,
    }
    clock.update(overrides)
    return clock


NOW = datetime(2026, 9, 4, 10, 0, 30, tzinfo=timezone.utc)


class Pc28IssueWindowTests(unittest.TestCase):
    def test_allows_next_issue_with_enough_room(self):
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885", draw_clock=_clock(), now=NOW,
        )
        self.assertTrue(verdict["allowed"])
        self.assertEqual(verdict["reason"], "ok")
        self.assertEqual(verdict["issue_offset"], 1)
        self.assertEqual(verdict["remaining_seconds"], 180)

    def test_blocks_already_drawn_issue(self):
        for issue_no in ("3477884", "3477883"):
            verdict = evaluate_pc28_issue_dispatch_window(
                issue_no=issue_no, draw_clock=_clock(), now=NOW,
            )
            self.assertFalse(verdict["allowed"], issue_no)
            self.assertEqual(verdict["reason"], "issue_already_drawn", issue_no)

    def test_blocks_when_window_shorter_than_threshold(self):
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885", draw_clock=_clock(countdown_seconds=22), now=NOW,
        )
        self.assertFalse(verdict["allowed"])
        self.assertEqual(verdict["reason"], "window_too_short")
        self.assertEqual(verdict["remaining_seconds"], 22)
        self.assertEqual(verdict["min_remaining_seconds"], 40)

    def test_subtracts_elapsed_time_since_clock_fetch(self):
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885", draw_clock=_clock(countdown_seconds=60), now=NOW + timedelta(seconds=30),
        )
        self.assertFalse(verdict["allowed"])
        self.assertEqual(verdict["remaining_seconds"], 30)

    def test_stale_clock_still_blocks_drawn_issue(self):
        # 时钟落后一整期：countdown 扣掉已流逝时间后仍为负，等价于目标期已开奖。
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885", draw_clock=_clock(countdown_seconds=10), now=NOW + timedelta(seconds=250),
        )
        self.assertFalse(verdict["allowed"])
        self.assertEqual(verdict["reason"], "issue_already_drawn")

    def test_stale_clock_extrapolates_further_issue(self):
        # 时钟停在 3477884（下一期 10 秒后开），250 秒后 3477885/3477886 都已开出，
        # 3477887 才是真正可投的那期：10 - 250 + 210×2 = 180 秒，与实际余量一致。
        stale = _clock(countdown_seconds=10)
        blocked = evaluate_pc28_issue_dispatch_window(
            issue_no="3477886", draw_clock=stale, now=NOW + timedelta(seconds=250),
        )
        self.assertFalse(blocked["allowed"])
        self.assertEqual(blocked["reason"], "issue_already_drawn")
        allowed = evaluate_pc28_issue_dispatch_window(
            issue_no="3477887", draw_clock=stale, now=NOW + timedelta(seconds=250),
        )
        self.assertTrue(allowed["allowed"])
        self.assertEqual(allowed["issue_offset"], 3)
        self.assertEqual(allowed["remaining_seconds"], 180)

    def test_fails_open_without_clock(self):
        for draw_clock in (None, {}, "", []):
            verdict = evaluate_pc28_issue_dispatch_window(
                issue_no="3477885", draw_clock=draw_clock, now=NOW,
            )
            self.assertTrue(verdict["allowed"], draw_clock)
            self.assertEqual(verdict["reason"], "clock_unavailable", draw_clock)

    def test_fails_open_when_countdown_missing(self):
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885", draw_clock=_clock(countdown_seconds=None), now=NOW,
        )
        self.assertTrue(verdict["allowed"])
        self.assertEqual(verdict["reason"], "countdown_unavailable")

    def test_fails_open_when_issue_not_numeric(self):
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="", draw_clock=_clock(), now=NOW,
        )
        self.assertTrue(verdict["allowed"])
        self.assertEqual(verdict["reason"], "issue_not_comparable")

    def test_threshold_override(self):
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885", draw_clock=_clock(countdown_seconds=22), now=NOW,
            min_remaining_seconds=20,
        )
        self.assertTrue(verdict["allowed"])
        self.assertEqual(verdict["min_remaining_seconds"], 20)

    def test_reports_stale_clock_flag(self):
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885", draw_clock=_clock(stale=True), now=NOW,
        )
        self.assertTrue(verdict["allowed"])
        self.assertTrue(verdict["clock_stale"])


if __name__ == "__main__":
    unittest.main()
