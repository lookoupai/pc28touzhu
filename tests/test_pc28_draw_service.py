from __future__ import annotations

import unittest

from pc28touzhu.services import pc28_draw_service
from pc28touzhu.services.pc28_draw_service import (
    fetch_pc28_draw_clock,
    fetch_pc28_recent_draws,
    fetch_pc28_recent_draws_deep,
    get_pc28_draw_clock,
)


class PC28DrawServiceTests(unittest.TestCase):
    def test_fetch_pc28_recent_draws_prefers_official_and_preserves_triplet(self):
        def fake_fetcher(url, params=None, headers=None, timeout=10):
            self.assertIn("Mozilla/5.0", headers.get("User-Agent", ""))
            if "pc28.help" in url:
                return {
                    "message": "success",
                    "data": [
                        {
                            "nbr": "20260418001",
                            "num": "14",
                            "number": "4+4+6",
                        }
                    ],
                }
            raise AssertionError("不应请求其他来源")

        result = fetch_pc28_recent_draws(limit=5, fetcher=fake_fetcher)
        self.assertEqual(result["source"], "official")
        self.assertEqual(result["items"][0]["issue_no"], "20260418001")
        self.assertEqual(result["items"][0]["result_number"], 14)
        self.assertEqual(result["items"][0]["triplet"], [4, 4, 6])

    def test_fetch_pc28_recent_draws_deep_falls_back_when_official_depth_insufficient(self):
        def fake_fetcher(url, params=None, headers=None, timeout=10):
            self.assertIn("Mozilla/5.0", headers.get("User-Agent", ""))
            if "pc28.help" in url:
                return {
                    "message": "success",
                    "data": [
                        {"nbr": "20260418130", "num": "14", "number": "4+4+6"},
                        {"nbr": "20260418129", "num": "11", "number": "2+4+5"},
                    ],
                }
            if "jnd-28.vip" in url:
                return [
                    {"draw_number": "20260418130", "canada28_result": "14", "number": "4+4+6"},
                    {"draw_number": "20260418129", "canada28_result": "11", "number": "2+4+5"},
                    {"draw_number": "20260418128", "canada28_result": "9", "number": "2+3+4"},
                    {"draw_number": "20260418127", "canada28_result": "13", "number": "3+4+6"},
                ]
            raise AssertionError("不应请求其他来源")

        result = fetch_pc28_recent_draws_deep(limit=4, fetcher=fake_fetcher)
        self.assertEqual(result["source"], "jnd")
        self.assertEqual(len(result["items"]), 4)
        self.assertEqual(result["items"][0]["issue_no"], "20260418130")
        self.assertEqual(result["items"][-1]["issue_no"], "20260418127")

    def test_official_draw_keeps_open_time_as_utc(self):
        def fake_fetcher(url, params=None, headers=None, timeout=10):
            return {
                "message": "success",
                "data": [{"nbr": "3477884", "num": "14", "number": "4+4+6", "date": "2026-09-04", "time": "18:00:30"}],
            }

        item = fetch_pc28_recent_draws(limit=1, fetcher=fake_fetcher)["items"][0]
        self.assertEqual(item["open_time"], "2026-09-04T10:00:30Z")
        self.assertEqual(item["draw_context"]["open_time"], "2026-09-04T10:00:30Z")


class PC28DrawClockTests(unittest.TestCase):
    def setUp(self):
        pc28_draw_service._DRAW_CLOCK_CACHE.clear()

    tearDown = setUp

    @staticmethod
    def _fetcher(countdown="02:10", nbr="3477884"):
        def fake_fetcher(url, params=None, headers=None, timeout=10):
            if "pc28.help" not in url:
                raise AssertionError("开奖时钟只应请求官方接口")
            return {
                "countdown": countdown,
                "message": "success",
                "data": [{"nbr": nbr, "date": "2026-09-04", "time": "18:00:30", "num": "17", "number": "5+6+6"}],
            }

        return fake_fetcher

    def test_fetch_draw_clock_parses_countdown_and_open_time(self):
        clock = fetch_pc28_draw_clock(fetcher=self._fetcher())
        self.assertEqual(clock["latest_issue_no"], "3477884")
        self.assertEqual(clock["latest_open_time"], "2026-09-04T10:00:30Z")
        self.assertEqual(clock["countdown_seconds"], 130)
        self.assertFalse(clock["stale"])

    def test_fetch_draw_clock_accepts_three_part_countdown(self):
        clock = fetch_pc28_draw_clock(fetcher=self._fetcher(countdown="01:02:10"))
        self.assertEqual(clock["countdown_seconds"], 3730)

    def test_fetch_draw_clock_tolerates_unusable_countdown(self):
        clock = fetch_pc28_draw_clock(fetcher=self._fetcher(countdown="--:--:--"))
        self.assertIsNone(clock["countdown_seconds"])
        self.assertEqual(clock["latest_issue_no"], "3477884")

    def test_get_draw_clock_caches_within_ttl(self):
        calls = []

        def counting_fetcher(url, params=None, headers=None, timeout=10):
            calls.append(url)
            return self._fetcher()(url, params, headers, timeout)

        first = get_pc28_draw_clock(fetcher=counting_fetcher, ttl_seconds=60)
        second = get_pc28_draw_clock(fetcher=counting_fetcher, ttl_seconds=60)
        self.assertEqual(first, second)
        self.assertEqual(len(calls), 1)

    def test_get_draw_clock_falls_back_to_stale_cache_on_failure(self):
        get_pc28_draw_clock(fetcher=self._fetcher(), ttl_seconds=0)

        def broken_fetcher(url, params=None, headers=None, timeout=10):
            raise RuntimeError("接口不可用")

        clock = get_pc28_draw_clock(fetcher=broken_fetcher, ttl_seconds=0)
        self.assertEqual(clock["latest_issue_no"], "3477884")
        self.assertTrue(clock["stale"])

    def test_get_draw_clock_returns_none_without_any_cache(self):
        def broken_fetcher(url, params=None, headers=None, timeout=10):
            raise RuntimeError("接口不可用")

        self.assertIsNone(get_pc28_draw_clock(fetcher=broken_fetcher, ttl_seconds=0))


if __name__ == "__main__":
    unittest.main()
