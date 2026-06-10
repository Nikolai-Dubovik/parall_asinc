import asyncio
import json
import logging
import sys
import time
from fnmatch import fnmatch
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day2"))
from crawler_day2 import AsyncCrawler  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("crawler")


class CrawlerQueue:
    """Очередь URL с приоритетами и трекингом состояния."""

    def __init__(self):
        self._queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        self._counter = 0
        self._depth: dict[str, int] = {}
        self.visited: set[str] = set()
        self.processed: dict[str, dict] = {}
        self.failed: dict[str, str] = {}

    def add_url(self, url: str, priority: int = 0, depth: int = 0) -> bool:
        """Добавить URL в очередь.

        priority — порядок выдачи: меньшее значение = выше приоритет (раньше из
        очереди), как в asyncio.PriorityQueue. depth — глубина обхода (контроль
        max_depth), понятие, независимое от приоритета.
        """
        if url in self.visited:
            return False
        self.visited.add(url)
        self._depth[url] = depth
        self._counter += 1
        # _counter гарантирует FIFO при равных приоритетах
        self._queue.put_nowait((priority, self._counter, url))
        return True

    async def get_next(self) -> str | None:
        """Следующий URL или None, если очередь пуста (без блокировки)."""
        try:
            _, _, url = self._queue.get_nowait()
            return url
        except asyncio.QueueEmpty:
            return None

    async def _get_blocking(self) -> str:
        """Внутреннее блокирующее получение для воркеров (ждёт появления URL)."""
        _, _, url = await self._queue.get()
        return url

    def get_depth(self, url: str) -> int:
        return self._depth.get(url, 0)

    def qsize(self) -> int:
        return self._queue.qsize()

    def task_done(self) -> None:
        self._queue.task_done()

    async def join(self) -> None:
        await self._queue.join()

    def mark_processed(self, url: str, result: dict | None = None) -> None:
        self.processed[url] = result or {}

    def mark_failed(self, url: str, error: str) -> None:
        self.failed[url] = error

    def get_stats(self) -> dict:
        return {
            "in_queue": self.qsize(),
            "processed": len(self.processed),
            "failed": len(self.failed),
            "seen": len(self.visited),
        }


class SemaphoreManager:
    """Глобальный лимит конкурентности + лимит на домен."""

    def __init__(self, global_limit: int = 10, per_domain_limit: int = 3):
        self.global_sem = asyncio.Semaphore(global_limit)
        self.per_domain_limit = per_domain_limit
        self.domain_sems: dict[str, asyncio.Semaphore] = {}

    def for_url(self, url: str) -> asyncio.Semaphore:
        domain = urlparse(url).netloc
        sem = self.domain_sems.get(domain)
        if sem is None:
            sem = asyncio.Semaphore(self.per_domain_limit)
            self.domain_sems[domain] = sem
        return sem


class QueueCrawler(AsyncCrawler):
    """Расширение AsyncCrawler: очередь, конкурентность, глубина, фильтры."""

    def __init__(self, max_concurrent: int = 10, max_depth: int = 2, per_domain_limit: int = 3):
        super().__init__(max_concurrent=max_concurrent)
        self.max_concurrent = max_concurrent
        self.max_depth = max_depth
        self.sem_manager = SemaphoreManager(global_limit=max_concurrent, per_domain_limit=per_domain_limit)
        self.queue: CrawlerQueue | None = None
        self.active_workers = 0
        # параметры фильтрации задаются в crawl()
        self.start_domains: set[str] = set()
        self.same_domain_only = True
        self.include_patterns: list[str] = []
        self.exclude_patterns: list[str] = []
        self.max_pages = 100

    def _allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https"):
            return False
        if self.same_domain_only and parsed.netloc not in self.start_domains:
            return False
        if self.exclude_patterns and any(fnmatch(url, p) for p in self.exclude_patterns):
            return False
        if self.include_patterns and not any(fnmatch(url, p) for p in self.include_patterns):
            return False
        return True

    async def on_page(self, url: str, result: dict, depth: int) -> None:
        """Хук: вызывается после успешной обработки страницы.

        Наследники переопределяют для сохранения данных, сбора статистики и т.п.
        По умолчанию ничего не делает.
        """
        return

    async def on_error(self, url: str, error: str) -> None:
        """Хук: вызывается при неудачной обработке URL. По умолчанию ничего не делает."""
        return

    async def _worker(self):
        try:
            while True:
                # воркер должен ждать появления новых ссылок, а не крутиться вхолостую
                # на None, поэтому используем блокирующее получение (get_next теперь
                # неблокирующий и возвращает None на пустой очереди — он для внешнего API)
                url = await self.queue._get_blocking()
                self.active_workers += 1
                try:
                    # строгий лимит: не запускаем больше max_pages загрузок.
                    # проверка и инкремент без await между ними — атомарны в asyncio
                    if self._dispatched >= self.max_pages:
                        continue
                    self._dispatched += 1
                    depth = self.queue.get_depth(url)
                    async with self.sem_manager.global_sem, self.sem_manager.for_url(url):
                        try:
                            result = await self.fetch_and_parse(url)
                        except Exception as e:
                            self.queue.mark_failed(url, type(e).__name__)
                            await self.on_error(url, type(e).__name__)
                            continue
                        if not (result.get("text") or result.get("title") or result.get("links")):
                            self.queue.mark_failed(url, "пустой ответ")
                            await self.on_error(url, "пустой ответ")
                            continue
                        self.queue.mark_processed(url, {
                            "url": result["url"],
                            "title": result["title"],
                            "depth": depth,
                            "links_count": len(result["links"]),
                            "text_length": len(result["text"]),
                        })
                        await self.on_page(url, result, depth)
                        if depth < self.max_depth:
                            for link in result["links"]:
                                if self._allowed(link):
                                    # BFS: приоритет = глубина, поэтому страницы меньшей
                                    # глубины обходятся раньше
                                    self.queue.add_url(link, priority=depth + 1, depth=depth + 1)
                finally:
                    self.active_workers -= 1
                    self.queue.task_done()
        except asyncio.CancelledError:
            return

    async def _progress(self, start: float):
        try:
            while True:
                await asyncio.sleep(2.0)
                stats = self.queue.get_stats()
                elapsed = time.perf_counter() - start
                rate = stats["processed"] / elapsed if elapsed > 0 else 0
                log.info("📊 обработано=%d в_очереди=%d ошибок=%d активно=%d скорость=%.2f стр/с",
                         stats["processed"], stats["in_queue"], stats["failed"],
                         self.active_workers, rate)
        except asyncio.CancelledError:
            return

    async def crawl(
        self,
        start_urls: list[str],
        max_pages: int = 100,
        same_domain_only: bool = True,
        include_patterns: list[str] | None = None,
        exclude_patterns: list[str] | None = None,
    ) -> dict[str, dict]:
        self.queue = CrawlerQueue()
        self.active_workers = 0
        self._dispatched = 0
        self.max_pages = max_pages
        self.same_domain_only = same_domain_only
        self.include_patterns = include_patterns or []
        self.exclude_patterns = exclude_patterns or []
        self.start_domains = {urlparse(u).netloc for u in start_urls}

        for url in start_urls:
            self.queue.add_url(url, priority=0, depth=0)

        start = time.perf_counter()
        workers = [asyncio.create_task(self._worker()) for _ in range(self.max_concurrent)]
        progress_task = asyncio.create_task(self._progress(start))

        try:
            # ждём, пока очередь полностью разгребётся (включая добавленные ссылки);
            # при достижении max_pages воркеры быстро дренируют остаток без обработки
            await self.queue.join()
        finally:
            for w in workers:
                w.cancel()
            progress_task.cancel()
            await asyncio.gather(*workers, progress_task, return_exceptions=True)

        elapsed = time.perf_counter() - start
        rate = len(self.queue.processed) / elapsed if elapsed > 0 else 0
        log.info("🏁 готово: %d стр. за %.1f с (%.1f стр/с), ошибок %d",
                 len(self.queue.processed), elapsed, rate, len(self.queue.failed))
        return self.queue.processed


async def main():
    crawler = QueueCrawler(max_concurrent=8, max_depth=2, per_domain_limit=4)
    try:
        results = await crawler.crawl(
            start_urls=["https://books.toscrape.com/", "https://onliner.by/", "https://pikabu.ru"],
            max_pages=25,
            same_domain_only=True,
            exclude_patterns=["*.pdf", "*.zip", "*.jpg", "*.png", "*/login*"],
        )

        out_path = Path(__file__).resolve().parent / "day3_results.json"
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(list(results.values()), f, ensure_ascii=False, indent=2)
        log.info("💾 сохранено %d записей в %s", len(results), out_path)
        if crawler.queue.failed:
            log.info("⚠️ неудачных URL: %d (примеры: %s)",
                     len(crawler.queue.failed),
                     list(crawler.queue.failed.items())[:3])
    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
