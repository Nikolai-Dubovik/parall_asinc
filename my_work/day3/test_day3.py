"""Тесты дня 3 (по разделу «Тестирование» ТЗ):
- очередь с приоритетами
- ограничение глубины обхода
- фильтрация URL
- отсутствие дубликатов в visited
"""
import sys
import unittest
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from crawler_day3 import CrawlerQueue, QueueCrawler
import _test_helpers  # noqa: F401

# Граф мини-сайта для проверки глубины: A → B,C ; B → D ; C → E
GRAPH = {
    "https://s.com/A": ["https://s.com/B", "https://s.com/C"],
    "https://s.com/B": ["https://s.com/D"],
    "https://s.com/C": ["https://s.com/E"],
    "https://s.com/D": [],
    "https://s.com/E": [],
}


class FakeCrawler(QueueCrawler):
    """QueueCrawler без сети: fetch_and_parse возвращает заранее заданные ссылки."""

    async def fetch_and_parse(self, url: str) -> dict:
        return {
            "url": url, "title": "T", "text": "текст",
            "links": GRAPH.get(url, []), "metadata": {},
        }


class TestQueue(unittest.IsolatedAsyncioTestCase):
    async def test_priority_order(self):
        """get_next выдаёт URL меньшей глубины (выше приоритет) раньше."""
        q = CrawlerQueue()
        q.add_url("https://s/deep", depth=2)
        q.add_url("https://s/shallow", depth=0)
        first = await q.get_next()
        self.assertEqual(first, "https://s/shallow")

    def test_no_duplicates(self):
        """Повторный add_url игнорируется, visited не растёт."""
        q = CrawlerQueue()
        self.assertTrue(q.add_url("https://s/x", depth=0))
        self.assertFalse(q.add_url("https://s/x", depth=0))
        self.assertEqual(len(q.visited), 1)


class TestCrawl(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.crawler = FakeCrawler(max_concurrent=3, max_depth=1, per_domain_limit=3)

    async def asyncTearDown(self):
        await self.crawler.close()

    async def test_depth_limit(self):
        """При max_depth=1 обходятся A(0), B,C(1); D,E(2) не обрабатываются."""
        processed = await self.crawler.crawl(
            start_urls=["https://s.com/A"], max_pages=100, same_domain_only=True)
        self.assertEqual(set(processed),
                         {"https://s.com/A", "https://s.com/B", "https://s.com/C"})
        self.assertNotIn("https://s.com/D", processed)

    async def test_no_duplicate_processing(self):
        """Один и тот же URL не обрабатывается дважды (visited)."""
        # A встречается у нескольких родителей? Проверим, что счётчик processed == уникальные
        processed = await self.crawler.crawl(
            start_urls=["https://s.com/A"], max_pages=100, same_domain_only=True)
        self.assertEqual(len(processed), len(set(processed)))

    def test_filter_same_domain(self):
        """same_domain_only пропускает только стартовый домен."""
        self.crawler.same_domain_only = True
        self.crawler.start_domains = {"s.com"}
        self.crawler.include_patterns = []
        self.crawler.exclude_patterns = []
        self.assertTrue(self.crawler._allowed("https://s.com/page"))
        self.assertFalse(self.crawler._allowed("https://other.com/page"))

    def test_filter_exclude_include(self):
        """exclude_patterns отсекает, include_patterns оставляет только нужное."""
        self.crawler.same_domain_only = False
        self.crawler.start_domains = set()
        self.crawler.exclude_patterns = ["*.pdf"]
        self.crawler.include_patterns = []
        self.assertFalse(self.crawler._allowed("https://s.com/file.pdf"))
        self.assertTrue(self.crawler._allowed("https://s.com/page"))

        self.crawler.exclude_patterns = []
        self.crawler.include_patterns = ["*/blog/*"]
        self.assertTrue(self.crawler._allowed("https://s.com/blog/1"))
        self.assertFalse(self.crawler._allowed("https://s.com/about"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
