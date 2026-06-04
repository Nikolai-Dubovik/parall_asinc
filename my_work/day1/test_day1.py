"""Тесты дня 1 (по разделу «Тестирование» ТЗ):
- загрузка валидных URL
- обработка несуществующих URL
- таймауты
- сравнение последовательной и параллельной загрузки
"""
import asyncio
import sys
import time
import unittest
from pathlib import Path

import aiohttp

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from crawler_day1 import AsyncCrawler
from _test_helpers import FakeResp, FakeCtx, fake_get


class TestDay1(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.crawler = AsyncCrawler(max_concurrent=5)

    async def asyncTearDown(self):
        await self.crawler.close()

    async def test_valid_url(self):
        """Валидный URL → возвращается тело, статус 200 записан."""
        self.crawler.session.get = fake_get(FakeResp(200, "<html>ok</html>"))
        text = await self.crawler.fetch_url("https://site/page")
        self.assertEqual(text, "<html>ok</html>")
        self.assertEqual(self.crawler.statuses["https://site/page"], 200)

    async def test_nonexistent_url_404(self):
        """404 не роняет программу: возвращается '', статус 404."""
        self.crawler.session.get = fake_get(FakeResp(404))
        text = await self.crawler.fetch_url("https://site/missing")
        self.assertEqual(text, "")
        self.assertEqual(self.crawler.statuses["https://site/missing"], 404)

    async def test_connection_error(self):
        """Сетевая ошибка (DNS/refused) обрабатывается без падения."""
        self.crawler.session.get = fake_get(aiohttp.ClientConnectionError("dns"))
        text = await self.crawler.fetch_url("https://nx.invalid")
        self.assertEqual(text, "")
        self.assertIn("https://nx.invalid", self.crawler.statuses)

    async def test_timeout(self):
        """Таймаут перехватывается, статус помечается как 'timeout'."""
        self.crawler.session.get = fake_get(asyncio.TimeoutError())
        text = await self.crawler.fetch_url("https://slow/page")
        self.assertEqual(text, "")
        self.assertEqual(self.crawler.statuses["https://slow/page"], "timeout")

    async def test_parallel_faster_than_sequential(self):
        """Параллельная загрузка ощутимо быстрее последовательной."""
        # каждый запрос «висит» 0.05 c
        class SlowCtx(FakeCtx):
            async def __aenter__(self):
                await asyncio.sleep(0.05)
                return FakeResp(200, "ok")

        self.crawler.session.get = lambda url, **kw: SlowCtx()
        urls = [f"https://site/{i}" for i in range(5)]

        t0 = time.perf_counter()
        for u in urls:
            await self.crawler.fetch_url(u)
        seq = time.perf_counter() - t0

        t0 = time.perf_counter()
        await self.crawler.fetch_urls(urls)
        par = time.perf_counter() - t0

        # 5 запросов по 0.05c: последовательно ~0.25c, параллельно ~0.05c
        self.assertLess(par, seq * 0.6)

    async def test_fetch_urls_returns_map(self):
        """fetch_urls возвращает словарь url -> текст для всех URL."""
        self.crawler.session.get = fake_get(FakeResp(200, "body"))
        urls = ["https://a/1", "https://a/2"]
        res = await self.crawler.fetch_urls(urls)
        self.assertEqual(set(res.keys()), set(urls))
        self.assertTrue(all(v == "body" for v in res.values()))


if __name__ == "__main__":
    unittest.main(verbosity=2)
