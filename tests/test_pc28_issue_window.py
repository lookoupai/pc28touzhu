from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from pc28touzhu.domain.pc28_issue_window import evaluate_pc28_issue_dispatch_window


def _clock(**overrides) -> dict:
    clock = {
        "latest_issue_no": "3477884",
        "latest_open_time": "2026-09-04T10:00:30Z",
        "countdown_seconds": 210,
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
        self.assertEqual(verdict["remaining_seconds"], 210)
        self.assertEqual(verdict["remaining_source"], "open_time")

    def test_open_time_overrides_broken_countdown(self):
        # 实测事故场景：开奖后 data[0] 已滚动到新一期（开奖时刻随结果发布、可信），
        # 但 countdown 字段仍停留在开奖前的旧值——按 countdown 计算会把 177 秒余量算成 0。
        clock = {
            "latest_issue_no": "3478167",
            "latest_open_time": "2026-09-05T03:04:00Z",
            "countdown_seconds": 0,
            "fetched_at": "2026-09-05T03:04:33Z",
            "stale": False,
        }
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3478168",
            draw_clock=clock,
            now=datetime(2026, 9, 5, 3, 4, 33, tzinfo=timezone.utc),
        )
        self.assertTrue(verdict["allowed"])
        self.assertEqual(verdict["reason"], "ok")
        self.assertEqual(verdict["remaining_seconds"], 177)
        self.assertEqual(verdict["remaining_source"], "open_time")

    def test_blocks_already_drawn_issue(self):
        for issue_no in ("3477884", "3477883"):
            verdict = evaluate_pc28_issue_dispatch_window(
                issue_no=issue_no, draw_clock=_clock(), now=NOW,
            )
            self.assertFalse(verdict["allowed"], issue_no)
            self.assertEqual(verdict["reason"], "issue_already_drawn", issue_no)

    def test_blocks_when_window_shorter_than_threshold(self):
        # data[0] 开奖于 180 秒前，下一期 30 秒后开奖 → 余量 30 秒 < 40 秒闸线
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885",
            draw_clock=_clock(latest_open_time="2026-09-04T09:57:30Z"),
            now=NOW,
        )
        self.assertFalse(verdict["allowed"])
        self.assertEqual(verdict["reason"], "window_too_short")
        self.assertEqual(verdict["remaining_seconds"], 30)
        self.assertEqual(verdict["min_remaining_seconds"], 40)

    def test_countdown_fallback_subtracts_elapsed_time_since_clock_fetch(self):
        # 无开奖时刻时回退到 countdown 扣除已流逝时间的旧算法
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885",
            draw_clock=_clock(latest_open_time=None, countdown_seconds=60),
            now=NOW + timedelta(seconds=30),
        )
        self.assertFalse(verdict["allowed"])
        self.assertEqual(verdict["remaining_seconds"], 30)
        self.assertEqual(verdict["remaining_source"], "countdown")

    def test_stale_clock_still_blocks_drawn_issue(self):
        # 时钟停在 3477883（09:57:00 开奖），250 秒后 3477884/3477885 均已开出，
        # 按开奖时刻外推余量为负，等价于目标期已开奖。
        stale = _clock(
            latest_issue_no="3477883",
            latest_open_time="2026-09-04T09:57:00Z",
            countdown_seconds=0,
            fetched_at="2026-09-04T09:57:10Z",
            stale=True,
        )
        for issue_no in ("3477884", "3477885"):
            verdict = evaluate_pc28_issue_dispatch_window(
                issue_no=issue_no, draw_clock=stale, now=NOW + timedelta(seconds=250),
            )
            self.assertFalse(verdict["allowed"], issue_no)
            self.assertEqual(verdict["reason"], "issue_already_drawn", issue_no)

    def test_stale_clock_extrapolates_further_issue(self):
        # 同上场景：3477886 尚未开奖（10:07:30 才开），10:04:40 时余量 170 秒，可投。
        stale = _clock(
            latest_issue_no="3477883",
            latest_open_time="2026-09-04T09:57:00Z",
            countdown_seconds=0,
            fetched_at="2026-09-04T09:57:10Z",
            stale=True,
        )
        allowed = evaluate_pc28_issue_dispatch_window(
            issue_no="3477886", draw_clock=stale, now=NOW + timedelta(seconds=250),
        )
        self.assertTrue(allowed["allowed"])
        self.assertEqual(allowed["issue_offset"], 3)
        self.assertEqual(allowed["remaining_seconds"], 170)
        self.assertEqual(allowed["remaining_source"], "open_time")

    def test_fails_open_without_clock(self):
        for draw_clock in (None, {}, "", []):
            verdict = evaluate_pc28_issue_dispatch_window(
                issue_no="3477885", draw_clock=draw_clock, now=NOW,
            )
            self.assertTrue(verdict["allowed"], draw_clock)
            self.assertEqual(verdict["reason"], "clock_unavailable", draw_clock)

    def test_fails_open_when_countdown_missing(self):
        # 开奖时刻与 countdown 均不可用时才 fail-open
        verdict = evaluate_pc28_issue_dispatch_window(
            issue_no="3477885",
            draw_clock=_clock(latest_open_time=None, countdown_seconds=None),
            now=NOW,
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
