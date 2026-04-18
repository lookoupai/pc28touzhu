from __future__ import annotations

import unittest

from pc28touzhu.services.pc28_draw_service import fetch_pc28_recent_draws


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


if __name__ == "__main__":
    unittest.main()
