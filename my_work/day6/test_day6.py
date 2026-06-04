"""Тесты дня 6 (по разделу «Тестирование» ТЗ):
- сохранение в JSON
- сохранение в CSV
- сохранение в БД (SQLite)
- обработка ошибок записи
- целостность сохранённых данных
"""
import csv
import json
import sqlite3
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from crawler_day6 import (
    JSONLinesStorage, CSVStorage, SQLiteStorage, StorageCrawler, DataStorage,
)
import _test_helpers  # noqa: F401


def make_payload(url="https://s/x"):
    return {
        "url": url, "title": "Заголовок", "text": "текст",
        "links": ["https://s/a", "https://s/b"],
        "metadata": {"description": "d"},
        "crawled_at": datetime.now(timezone.utc),
        "status_code": 200, "content_type": "text/html",
    }


class FailingStorage(DataStorage):
    """Хранилище, всегда падающее при записи — для проверки обработки ошибок."""

    def __init__(self):
        self.attempts = 0

    async def save(self, data):
        self.attempts += 1
        raise IOError("disk full")

    async def close(self):
        pass


async def _noop_sleep(_):
    return None


class TestStorages(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.dir = Path(tempfile.mkdtemp())

    async def test_json_storage(self):
        """JSON Lines: каждая запись на своей строке, читается обратно."""
        path = self.dir / "out.jsonl"
        st = JSONLinesStorage(path)
        await st.save(make_payload("https://s/1"))
        await st.save(make_payload("https://s/2"))
        await st.close()
        lines = path.read_text(encoding="utf-8").splitlines()
        self.assertEqual(len(lines), 2)
        rec = json.loads(lines[0])
        self.assertEqual(rec["url"], "https://s/1")
        self.assertEqual(rec["links"], ["https://s/a", "https://s/b"])  # целостность

    async def test_csv_storage(self):
        """CSV: заголовок + строки, поля на месте."""
        path = self.dir / "out.csv"
        st = CSVStorage(path)
        await st.save(make_payload())
        await st.close()
        with open(path, newline="", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["url"], "https://s/x")
        self.assertEqual(rows[0]["status_code"], "200")
        self.assertIn("https://s/a", rows[0]["links"])

    async def test_sqlite_storage(self):
        """SQLite: batch-вставка, целостность (links/metadata/status_code)."""
        path = self.dir / "out.db"
        st = SQLiteStorage(path, batch_size=2)
        await st.save(make_payload("https://s/1"))
        await st.save(make_payload("https://s/2"))
        await st.close()
        conn = sqlite3.connect(path)
        rows = conn.execute(
            "SELECT url, links, status_code FROM pages ORDER BY url").fetchall()
        conn.close()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0], "https://s/1")
        self.assertEqual(json.loads(rows[0][1]), ["https://s/a", "https://s/b"])
        self.assertEqual(rows[0][2], 200)

    async def test_write_error_handling(self):
        """Ошибка записи не роняет краулер; делается save_retries попыток."""
        crawler = StorageCrawler(max_concurrent=1, max_depth=0, respect_robots=False,
                                 storage=FailingStorage(), save_retries=3)
        try:
            with patch("asyncio.sleep", _noop_sleep):
                # не должно бросить исключение
                await crawler.on_page("https://s/x", make_payload(), 0)
            self.assertEqual(crawler.storage.attempts, 3)
        finally:
            await crawler.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
