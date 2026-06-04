"""Тесты дня 2 (по разделу «Тестирование» ТЗ):
- парсинг валидного HTML
- обработка битого HTML
- извлечение ссылок
- конвертация относительных URL в абсолютные
"""
import sys
import unittest
from pathlib import Path

from bs4 import BeautifulSoup

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from crawler_day2 import HTMLParser
import _test_helpers  # noqa: F401  (глушит логи)

VALID_HTML = """
<html><head>
  <title>Заголовок</title>
  <meta name="description" content="описание">
  <meta name="keywords" content="a, b">
</head><body>
  <h1>H1</h1><h2>H2</h2>
  <p>Привет мир</p>
  <a href="https://ext.com/x">внешняя</a>
  <a href="/page1">относительная</a>
  <a href="page2">относительная2</a>
  <img src="/img.png" alt="картинка">
</body></html>
"""


class TestDay2(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.parser = HTMLParser()

    async def test_parse_valid_html(self):
        """Валидный HTML: title, текст, ссылки, метаданные, картинки."""
        r = await self.parser.parse_html(VALID_HTML, "https://site.com/")
        self.assertEqual(r["title"], "Заголовок")
        self.assertIn("Привет мир", r["text"])
        self.assertEqual(r["metadata"]["description"], "описание")
        self.assertTrue(len(r["links"]) >= 3)
        self.assertEqual(len(r["images"]), 1)
        self.assertEqual(r["images"][0]["alt"], "картинка")

    async def test_broken_html_no_crash(self):
        """Битый/незакрытый HTML не роняет парсер, возвращается валидная структура."""
        r = await self.parser.parse_html("<html><body><p>unclosed", "https://s/")
        self.assertEqual(r["url"], "https://s/")
        self.assertIsInstance(r["links"], list)
        self.assertIsInstance(r["metadata"], dict)

    async def test_empty_html(self):
        """Пустой ввод → частичный (пустой) результат без исключения."""
        r = await self.parser.parse_html("", "https://s/")
        self.assertEqual(r["title"], "")
        self.assertEqual(r["links"], [])

    def test_extract_links(self):
        """Извлекаются только http(s)-ссылки, без дублей."""
        html = """<a href="https://a.com/1">1</a>
                  <a href="https://a.com/1">dup</a>
                  <a href="mailto:x@y.z">mail</a>
                  <a href="https://a.com/2">2</a>"""
        soup = BeautifulSoup(html, "lxml")
        links = self.parser.extract_links(soup, "https://a.com/")
        self.assertEqual(links, ["https://a.com/1", "https://a.com/2"])

    def test_relative_url_conversion(self):
        """Относительные ссылки превращаются в абсолютные относительно base_url."""
        soup = BeautifulSoup('<a href="/page1">p1</a><a href="sub/p2">p2</a>', "lxml")
        links = self.parser.extract_links(soup, "https://site.com/dir/")
        self.assertIn("https://site.com/page1", links)
        self.assertIn("https://site.com/dir/sub/p2", links)

    def test_extract_metadata(self):
        """Метаданные: title/description/keywords."""
        soup = BeautifulSoup(VALID_HTML, "lxml")
        meta = self.parser.extract_metadata(soup)
        self.assertEqual(meta["title"], "Заголовок")
        self.assertEqual(meta["keywords"], "a, b")


if __name__ == "__main__":
    unittest.main(verbosity=2)
