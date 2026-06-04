import abc
import asyncio
import csv
import io
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import aiofiles
import aiosqlite

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day5"))

from crawler_day5 import RetryCrawler
from sample_urls import DAY1_URLS


log = logging.getLogger("crawler")


class DataStorage(abc.ABC):
    @abc.abstractmethod
    async def save(self, data: dict) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...


def _serialize(value) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    if value is None:
        return ""
    return str(value)


class FileStorage(DataStorage):
    """Базовый файловый storage: блокировка + ленивое открытие + close.

    Наследники реализуют только `_format(data) -> str` — как сериализовать одну запись.
    """

    def __init__(self, path: str | Path, encoding: str = "utf-8", newline=None):
        self.path = Path(path)
        self.encoding = encoding
        self._newline = newline
        self._file = None
        self._lock = asyncio.Lock()

    async def _ensure_open(self):
        if self._file is None:
            self._file = await aiofiles.open(
                self.path, "w", encoding=self.encoding, newline=self._newline
            )

    def _format(self, data: dict) -> str:
        raise NotImplementedError

    async def save(self, data: dict) -> None:
        async with self._lock:
            await self._ensure_open()
            await self._file.write(self._format(data))

    async def close(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None


class JSONLinesStorage(FileStorage):
    """Потоковая запись: один JSON-объект на строку (формат JSON Lines / .jsonl)."""

    def _format(self, data: dict) -> str:
        return json.dumps(data, ensure_ascii=False, default=str) + "\n"


# обратная совместимость по имени (CLI/конфиг используют "json")
JSONStorage = JSONLinesStorage


class CSVStorage(FileStorage):
    def __init__(self, path: str | Path, fieldnames: list[str] | None = None,
                 encoding: str = "utf-8"):
        super().__init__(path, encoding=encoding, newline="")
        self.fieldnames = fieldnames
        self._wrote_header = False

    def _format(self, data: dict) -> str:
        if self.fieldnames is None:
            self.fieldnames = list(data.keys())
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=self.fieldnames)
        if not self._wrote_header:
            writer.writeheader()
            self._wrote_header = True
        writer.writerow({k: _serialize(data.get(k)) for k in self.fieldnames})
        return buf.getvalue()


class SQLiteStorage(DataStorage):
    INSERT_SQL = "INSERT OR REPLACE INTO pages VALUES (?, ?, ?, ?, ?, ?, ?, ?)"

    def __init__(self, path: str | Path, batch_size: int = 50):
        self.path = str(path)
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()
        self.batch_size = batch_size
        self._buffer: list[tuple] = []

    async def init_db(self) -> None:
        self._conn = await aiosqlite.connect(self.path)
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS pages (
                url TEXT PRIMARY KEY,
                title TEXT,
                text TEXT,
                links TEXT,
                metadata TEXT,
                crawled_at TEXT,
                status_code INTEGER,
                content_type TEXT
            )
        """)
        await self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_status ON pages(status_code)"
        )
        await self._conn.commit()

    @staticmethod
    def _row(data: dict) -> tuple:
        status = data.get("status_code")
        try:
            status_int = int(status) if status is not None else None
        except (TypeError, ValueError):
            status_int = None
        return (
            data.get("url", ""),
            data.get("title", ""),
            data.get("text", ""),
            json.dumps(data.get("links", []), ensure_ascii=False),
            json.dumps(data.get("metadata", {}), ensure_ascii=False),
            _serialize(data.get("crawled_at")),
            status_int,
            data.get("content_type", ""),
        )

    async def _flush(self) -> None:
        """Записать накопленный буфер одним executemany. Вызывается под self._lock."""
        if not self._buffer:
            return
        await self._conn.executemany(self.INSERT_SQL, self._buffer)
        await self._conn.commit()
        self._buffer.clear()

    async def save(self, data: dict) -> None:
        async with self._lock:
            if self._conn is None:
                await self.init_db()
            self._buffer.append(self._row(data))
            if len(self._buffer) >= self.batch_size:
                await self._flush()

    async def close(self) -> None:
        if self._conn is not None:
            async with self._lock:
                await self._flush()
            await self._conn.close()
            self._conn = None


class StorageCrawler(RetryCrawler):
    def __init__(self, *args, storage: DataStorage | None = None,
                 save_retries: int = 3, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage = storage
        self.save_retries = save_retries

    async def on_page(self, url: str, result: dict, depth: int) -> None:
        if self.storage is None:
            return
        payload = {
            "url": result.get("url", url),
            "title": result.get("title", ""),
            "text": result.get("text", ""),
            "links": result.get("links", []),
            "metadata": result.get("metadata", {}),
            "crawled_at": datetime.now(timezone.utc),
            "status_code": self.statuses.get(url),
            "content_type": "text/html",
        }
        # повтор при ошибке записи; краулинг не падает, даже если все попытки неудачны
        for attempt in range(self.save_retries):
            try:
                await self.storage.save(payload)
                return
            except Exception as e:
                if attempt + 1 >= self.save_retries:
                    log.warning("⚠️ не удалось сохранить %s после %d попыток: %s",
                                url, self.save_retries, e)
                else:
                    await asyncio.sleep(0.2 * (attempt + 1))

    async def close(self) -> None:
        if self.storage is not None:
            try:
                await self.storage.close()
            except Exception as e:
                log.warning("⚠️ ошибка закрытия storage: %s", e)
        await super().close()


async def _run_demo(storage: DataStorage, label: str) -> None:
    crawler = StorageCrawler(
        max_concurrent=3,
        max_depth=0,
        requests_per_second=2.0,
        respect_robots=False,
        user_agent="MyBot/1.0",
        storage=storage,
    )
    try:
        await crawler.crawl(
            start_urls=DAY1_URLS,
            max_pages=5,
            same_domain_only=False,
        )
        log.info("✅ %s: записано", label)
    finally:
        await crawler.close()


async def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    here = Path(__file__).resolve().parent

    await _run_demo(JSONStorage(here / "day6_results.jsonl"), "JSON")
    await _run_demo(CSVStorage(here / "day6_results.csv"), "CSV")

    db_path = here / "day6_results.db"
    await _run_demo(SQLiteStorage(db_path), "SQLite")

    async with aiosqlite.connect(str(db_path)) as conn:
        async with conn.execute("SELECT url, title, status_code FROM pages") as cur:
            rows = await cur.fetchall()
    print(f"Прочитано из SQLite: {len(rows)} строк")
    for row in rows:
        print(" -", row)


if __name__ == "__main__":
    asyncio.run(main())
