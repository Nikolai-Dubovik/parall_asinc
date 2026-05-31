import asyncio
import json
import logging
import sys
import time
from contextlib import asynccontextmanager
from fnmatch import fnmatch
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day2"))

from crawler_day2 import ParsingCrawler


log = logging.getLogger("crawler")


class CrawlerQueue:
    def __init__(self):
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._counter = 0
        self.in_queue: set[str] = set()
        self.depths: dict[str, int] = {}
        self.processed: dict[str, dict] = {}
        self.failed: dict[str, str] = {}

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> None:
        if url in self.in_queue or url in self.processed or url in self.failed:
            return
        self.in_queue.add(url)
        self.depths[url] = depth
        self._counter += 1
        # PriorityQueue выдаёт минимальный первым → инвертируем priority
        self._queue.put_nowait((-priority, self._counter, url))

    async def get_next(self) -> str | None:
        try:
            _, _, url = await asyncio.wait_for(self._queue.get(), timeout=0.5)
        except asyncio.TimeoutError:
            return None
        self.in_queue.discard(url)
        return url

    def mark_processed(self, url: str, result: dict | None = None) -> None:
        self.processed[url] = result or {}

    def mark_failed(self, url: str, error: str) -> None:
        self.failed[url] = error

    def get_stats(self) -> dict:
        return {
            "queued": self._queue.qsize(),
            "processed": len(self.processed),
            "failed": len(self.failed),
        }


class SemaphoreManager:
    def __init__(self, global_limit: int = 10, per_domain_limit: int = 3):
        self.global_sem = asyncio.Semaphore(global_limit)
        self.per_domain_limit = per_domain_limit
        self.domain_sems: dict[str, asyncio.Semaphore] = {}
        self.active = 0

    def _domain_sem(self, url: str) -> asyncio.Semaphore:
        domain = urlparse(url).netloc
        if domain not in self.domain_sems:
            self.domain_sems[domain] = asyncio.Semaphore(self.per_domain_limit)
        return self.domain_sems[domain]

    @asynccontextmanager
    async def acquire(self, url: str):
        async with self.global_sem, self._domain_sem(url):
            self.active += 1
            try:
                yield
            finally:
                self.active -= 1


class QueueCrawler(ParsingCrawler):
    def __init__(self, max_concurrent: int = 10, max_depth: int = 2,
                 per_domain_limit: int = 3):
        super().__init__(max_concurrent=max_concurrent)
        self.max_depth = max_depth
        self.queue = CrawlerQueue()
        self.sem_manager = SemaphoreManager(max_concurrent, per_domain_limit)
        self.visited: set[str] = set()
        self._workers_count = max_concurrent

    def _allowed(self, url: str, start_domains: set[str],
                 same_domain_only: bool,
                 include_patterns: list[str],
                 exclude_patterns: list[str]) -> bool:
        if same_domain_only and urlparse(url).netloc not in start_domains:
            return False
        if include_patterns and not any(fnmatch(url, p) for p in include_patterns):
            return False
        if exclude_patterns and any(fnmatch(url, p) for p in exclude_patterns):
            return False
        return True

    async def _process_url(self, url: str, depth: int,
                           start_domains: set[str], opts: dict, max_pages: int) -> None:
        async with self.sem_manager.acquire(url):
            result = await self.fetch_and_parse(url)

        if not result.get("title") and not result.get("text") and not result.get("links"):
            self.queue.mark_failed(url, str(self.statuses.get(url, "no_data")))
            return

        self.queue.mark_processed(url, result)

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

    async def crawl(self, start_urls: list[str], max_pages: int = 100,
                    same_domain_only: bool = False,
                    include_patterns: list[str] | None = None,
                    exclude_patterns: list[str] | None = None) -> dict:
        start_domains = {urlparse(u).netloc for u in start_urls}
        opts = {
            "same_domain_only": same_domain_only,
            "include_patterns": include_patterns or [],
            "exclude_patterns": exclude_patterns or [],
        }

        for u in start_urls:
            self.visited.add(u)
            self.queue.add_url(u, priority=1, depth=0)

        started = time.perf_counter()
        stop_event = asyncio.Event()

        async def worker():
            while not stop_event.is_set():
                if len(self.queue.processed) >= max_pages:
                    stop_event.set()
                    break
                url = await self.queue.get_next()
                if url is None:
                    if self.sem_manager.active == 0 and self.queue._queue.qsize() == 0:
                        stop_event.set()
                        break
                    continue
                depth = self.queue.depths.get(url, 0)
                try:
                    await self._process_url(url, depth, start_domains, opts, max_pages)
                except Exception as e:
                    self.queue.mark_failed(url, type(e).__name__)

        async def progress():
            while not stop_event.is_set():
                await asyncio.sleep(1.0)
                stats = self.queue.get_stats()
                elapsed = time.perf_counter() - started
                speed = stats["processed"] / elapsed if elapsed > 0 else 0
                log.info(
                    "📊 страниц=%d, в очереди=%d, ошибок=%d, активных=%d, скорость=%.2f стр/с",
                    stats["processed"], stats["queued"], stats["failed"],
                    self.sem_manager.active, speed,
                )

        workers = [asyncio.create_task(worker()) for _ in range(self._workers_count)]
        prog = asyncio.create_task(progress())

        await asyncio.gather(*workers, return_exceptions=True)
        prog.cancel()
        try:
            await prog
        except asyncio.CancelledError:
            pass

        return self.queue.processed


async def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    crawler = QueueCrawler(max_concurrent=10, max_depth=2, per_domain_limit=3)
    try:
        results = await crawler.crawl(
            start_urls=["https://example.com"],
            max_pages=20,
            same_domain_only=False,
        )
        print(f"Обработано: {len(results)} страниц")

        out_path = Path(__file__).resolve().parent / "day3_results.json"
        summary = [
            {
                "url": u,
                "title": r.get("title", ""),
                "links_count": len(r.get("links", [])),
                "text_length": len(r.get("text", "")),
            }
            for u, r in results.items()
        ]
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        log.info("💾 сохранено в %s", out_path)
    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())