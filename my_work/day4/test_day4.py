"""Тесты дня 4 (по разделу «Тестирование» ТЗ):
- rate limiting для одного домена
- rate limiting для разных доменов
- парсинг robots.txt
- блокировка запрещённых URL
- соблюдение задержек
"""
import sys
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from crawler_day4 import RateLimiter, RobotsParser
from _test_helpers import FakeResp, FakeSession

ROBOTS_TXT = "User-agent: *\nDisallow: /private\nCrawl-delay: 2\n"


class TestRateLimiter(unittest.IsolatedAsyncioTestCase):
    async def test_single_domain_spacing(self):
        """Два запроса к одному домену разнесены минимум на интервал."""
        rl = RateLimiter(requests_per_second=20)  # интервал 0.05 c
        t0 = time.perf_counter()
        await rl.acquire("d.com")
        await rl.acquire("d.com")
        self.assertGreaterEqual(time.perf_counter() - t0, 0.04)

    async def test_different_domains_independent(self):
        """Разные домены не ждут друг друга (отдельные лимиты)."""
        rl = RateLimiter(requests_per_second=2)  # интервал 0.5 c
        t0 = time.perf_counter()
        await rl.acquire("a.com")
        await rl.acquire("b.com")
        self.assertLess(time.perf_counter() - t0, 0.3)

    async def test_min_interval_respected(self):
        """min_interval (min_delay/crawl_delay) увеличивает паузу и возвращается как waited."""
        rl = RateLimiter(requests_per_second=100)  # интервал 0.01 c
        await rl.acquire("d.com")  # зафиксировать last_ts
        t0 = time.perf_counter()
        waited = await rl.acquire("d.com", min_interval=0.1)
        self.assertGreaterEqual(time.perf_counter() - t0, 0.09)
        self.assertGreater(waited, 0.0)


class TestRobots(unittest.IsolatedAsyncioTestCase):
    async def test_robots_parsing(self):
        """fetch_robots возвращает dict с crawl_delay; правила кэшируются."""
        rp = RobotsParser(FakeSession(FakeResp(200, ROBOTS_TXT)))
        info = await rp.fetch_robots("https://s.com")
        self.assertIsInstance(info, dict)
        self.assertEqual(info["crawl_delay"], 2.0)
        self.assertEqual(rp.get_crawl_delay("https://s.com/x"), 2.0)

    async def test_blocked_url(self):
        """Запрещённый robots.txt путь блокируется, разрешённый — нет."""
        rp = RobotsParser(FakeSession(FakeResp(200, ROBOTS_TXT)))
        await rp.fetch_robots("https://s.com")
        self.assertFalse(rp.can_fetch("https://s.com/private/secret"))
        self.assertTrue(rp.can_fetch("https://s.com/public/page"))

    async def test_missing_robots_allows_all(self):
        """Если robots.txt недоступен (404) — по умолчанию всё разрешено."""
        rp = RobotsParser(FakeSession(FakeResp(404)))
        await rp.fetch_robots("https://s.com")
        self.assertTrue(rp.can_fetch("https://s.com/anything"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
