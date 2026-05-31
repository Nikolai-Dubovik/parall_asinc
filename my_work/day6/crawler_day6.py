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

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day5"))

from crawler_day5 import RetryCrawler


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


class JSONStorage(DataStorage):
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self._file = None
        self._lock = asyncio.Lock()

    async def _ensure_open(self):
        if self._file is None:
            self._file = await aiofiles.open(self.path, "w", encoding="utf-8")

    async def save(self, data: dict) -> None:
        async with self._lock:
            await self._ensure_open()
            line = json.dumps(data, ensure_ascii=False, default=str) + "\n"
            await self._file.write(line)

    async def close(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None


class CSVStorage(DataStorage):
    def __init__(self, path: str | Path, fieldnames: list[str] | None = None,
                 encoding: str = "utf-8"):
        self.path = Path(path)
        self.fieldnames = fieldnames
        self.encoding = encoding
        self._file = None
        self._wrote_header = False
        self._lock = asyncio.Lock()

    async def save(self, data: dict) -> None:
        async with self._lock:
            if self.fieldnames is None:
                self.fieldnames = list(data.keys())
            if self._file is None:
                self._file = await aiofiles.open(
                    self.path, "w", encoding=self.encoding, newline=""
                )
            buf = io.StringIO()
            writer = csv.DictWriter(buf, fieldnames=self.fieldnames)
            if not self._wrote_header:
                writer.writeheader()
                self._wrote_header = True
            row = {k: _serialize(data.get(k)) for k in self.fieldnames}
            writer.writerow(row)
            await self._file.write(buf.getvalue())

    async def close(self) -> None:
        if self._file is not None:
            await self._file.close()
            self._file = None


class SQLiteStorage(DataStorage):
    def __init__(self, path: str | Path):
        self.path = str(path)
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

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

    async def save(self, data: dict) -> None:
        async with self._lock:
            if self._conn is None:
                await self.init_db()
            status = data.get("status_code")
            try:
                status_int = int(status) if status is not None else None
            except (TypeError, ValueError):
                status_int = None
            await self._conn.execute(
                "INSERT OR REPLACE INTO pages VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    data.get("url", ""),
                    data.get("title", ""),
                    data.get("text", ""),
                    json.dumps(data.get("links", []), ensure_ascii=False),
                    json.dumps(data.get("metadata", {}), ensure_ascii=False),
                    _serialize(data.get("crawled_at")),
                    status_int,
                    data.get("content_type", ""),
                ),
            )
            await self._conn.commit()

    async def close(self) -> None:
        if self._conn is not None:
            await self._conn.close()
            self._conn = None


class StorageCrawler(RetryCrawler):
    def __init__(self, *args, storage: DataStorage | None = None, **kwargs):
        super().__init__(*args, **kwargs)
        self.storage = storage

    async def _process_url(self, url: str, depth: int,
                           start_domains: set[str], opts: dict,
                           max_pages: int) -> None:
        async with self.sem_manager.acquire(url):
            result = await self.fetch_and_parse(url)

        if not result.get("title") and not result.get("text") and not result.get("links"):
            self.queue.mark_failed(url, str(self.statuses.get(url, "no_data")))
            return

        self.queue.mark_processed(url, result)

        if self.storage is not None:
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
            try:
                await self.storage.save(payload)
            except Exception as e:
                log.warning("⚠️ ошибка сохранения %s: %s", url, e)

        if depth + 1 > self.max_depth:
            return
        if len(self.queue.processed) >= max_pages:
            return
        for link in result.get("links", []):
            if link in self.visited:
                continue
            if not self._allowed(link, start_domains, **opts):
                continue
            self.visited.add(link)
            self.queue.add_url(link, priority=0, depth=depth + 1)

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
            start_urls=["https://example.com", "https://httpbin.org/get"],
            max_pages=2,
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
