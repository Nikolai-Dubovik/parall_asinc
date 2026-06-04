"""Тесты дня 7 (компоненты + «Тестирование производительности» из ТЗ):
- парсинг sitemap (обычный и индексный, рекурсивно)
- статистика (CrawlerStats)
- экспорт в JSON и HTML-отчёт с графиками
- загрузка конфига и фильтров
- масштабируемость (обработка 100 страниц на моках, без сети)
"""
import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from crawler_day7 import AdvancedCrawler, SitemapParser, CrawlerStats
from crawler_day6 import JSONLinesStorage
from _test_helpers import FakeResp, FakeSession

URLSET = """<?xml version="1.0"?>
<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <url><loc>https://s.com/a</loc></url>
  <url><loc>https://s.com/b</loc></url>
</urlset>"""

SITEMAP_INDEX = """<?xml version="1.0"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap><loc>https://s.com/sm1.xml</loc></sitemap>
</sitemapindex>"""


class TestSitemap(unittest.IsolatedAsyncioTestCase):
    async def test_plain_sitemap(self):
        """Из обычного sitemap извлекаются все <loc>."""
        sp = SitemapParser(FakeSession(FakeResp(200, URLSET)))
        urls = await sp.fetch_sitemap("https://s.com/sitemap.xml")
        self.assertEqual(urls, ["https://s.com/a", "https://s.com/b"])

    async def test_sitemap_index_recursive(self):
        """Индексный sitemap рекурсивно подгружает дочерние."""
        def handler(url):
            return FakeResp(200, SITEMAP_INDEX if url.endswith("index.xml") else URLSET)
        sp = SitemapParser(FakeSession(handler))
        urls = await sp.fetch_sitemap("https://s.com/index.xml")
        self.assertEqual(urls, ["https://s.com/a", "https://s.com/b"])


class TestStats(unittest.TestCase):
    def test_stats_summary(self):
        """Статистика считает успехи/ошибки, статус-коды и топ-домены."""
        st = CrawlerStats()
        st.record_success("https://a.com/1", 200)
        st.record_success("https://a.com/2", 200)
        st.record_failure("https://b.com/1", 500)
        s = st.summary()
        self.assertEqual(s["total_pages"], 3)
        self.assertEqual(s["successful"], 2)
        self.assertEqual(s["failed"], 1)
        self.assertEqual(s["status_codes"]["200"], 2)
        self.assertEqual(s["top_domains"]["a.com"], 2)


class FastCrawler(AdvancedCrawler):
    """AdvancedCrawler без сети — для тестов экспорта и масштабируемости."""

    async def fetch_and_parse(self, url: str) -> dict:
        self.statuses[url] = 200
        return {"url": url, "title": "t", "text": "x", "links": [], "metadata": {}}


class TestAdvanced(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = Path(tempfile.mkdtemp())

    async def test_export_json_and_html(self):
        """export_to_json даёт валидный JSON, HTML-отчёт содержит графики."""
        c = FastCrawler(max_concurrent=3, max_depth=0, respect_robots=False)
        try:
            await c.crawl(start_urls=[f"https://s.com/{i}" for i in range(5)],
                          max_pages=5, same_domain_only=False)
            jpath = self.tmp / "stats.json"
            hpath = self.tmp / "report.html"
            c.export_to_json(str(jpath))
            c.export_to_html_report(str(hpath))
            data = json.loads(jpath.read_text(encoding="utf-8"))
            self.assertEqual(data["successful"], 5)
            html = hpath.read_text(encoding="utf-8")
            self.assertIn("Статус-коды", html)
            self.assertIn("class='bar'", html)
        finally:
            await c.close()

    async def test_config_loading_with_filters(self):
        """from_config читает настройки, фильтры и собирает storage."""
        cfg = {
            "urls": ["https://s/1"], "max_concurrent": 3, "max_depth": 1,
            "rate_limit": 2.0, "respect_robots": False, "same_domain_only": True,
            "include_patterns": ["*blog*"], "exclude_patterns": ["*.pdf"],
            "storage": {"type": "json", "path": str(self.tmp / "d.jsonl")},
        }
        cpath = self.tmp / "config.json"
        cpath.write_text(json.dumps(cfg), encoding="utf-8")
        c = AdvancedCrawler.from_config(str(cpath))
        try:
            self.assertEqual(c.config["include_patterns"], ["*blog*"])
            self.assertEqual(c.config["exclude_patterns"], ["*.pdf"])
            self.assertIsInstance(c.storage, JSONLinesStorage)
        finally:
            await c.close()

    async def test_scalability_100_pages(self):
        """Масштабируемость: 100 страниц обрабатываются на моках быстро и полностью."""
        c = FastCrawler(max_concurrent=10, max_depth=0, respect_robots=False)
        try:
            urls = [f"https://s.com/p{i}" for i in range(100)]
            t0 = time.perf_counter()
            await c.crawl(start_urls=urls, max_pages=100, same_domain_only=False)
            elapsed = time.perf_counter() - t0
            stats = c.get_stats()
            self.assertEqual(stats["total_pages"], 100)
            self.assertEqual(stats["successful"], 100)
            self.assertLess(elapsed, 5.0)  # без сети — должно быть быстро
        finally:
            await c.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
