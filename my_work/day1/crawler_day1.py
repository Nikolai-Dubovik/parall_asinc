import asyncio
import logging
import sys
import time
from pathlib import Path

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sample_urls import DAY1_URLS

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("crawler")


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        # connect — таймаут на установку соединения, sock_read — на чтение ответа
        timeout = aiohttp.ClientTimeout(connect=5, sock_read=10, total=30)
        connector = aiohttp.TCPConnector(limit=max_concurrent, ssl=False)
        # trust_env=True — поддержка прокси из HTTP(S)_PROXY и кредов из .netrc
        self.session = aiohttp.ClientSession(
            timeout=timeout, connector=connector, trust_env=True
        )
        # ограничиваем число одновременных корутин-запросов, а не только TCP-соединений
        self.sem = asyncio.Semaphore(max_concurrent)
        self.statuses: dict[str, object] = {}

    async def fetch_url(self, url: str) -> str:
        async with self.sem:
            log.info("▶️ начало загрузки %s", url)
            try:
                async with self.session.get(url) as resp:
                    resp.raise_for_status()
                    text = await resp.text()
                    self.statuses[url] = resp.status
                    log.info("✅ успешное завершение %s (HTTP %s)", url, resp.status)
                    return text
            except asyncio.TimeoutError as e:
                self.statuses[url] = "timeout"
                log.warning("⚠️ ошибка %s: %s", type(e).__name__, url)
            except aiohttp.ClientResponseError as e:
                self.statuses[url] = e.status
                log.warning("⚠️ ошибка %s (HTTP %s): %s", type(e).__name__, e.status, url)
            except aiohttp.ClientError as e:
                self.statuses[url] = type(e).__name__
                log.warning("⚠️ ошибка %s: %s", type(e).__name__, url)
            return ""

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        results = await asyncio.gather(*(self.fetch_url(u) for u in urls))
        return dict(zip(urls, results))

    async def close(self):
        await self.session.close()


async def main():
    crawler = AsyncCrawler(max_concurrent=5)
    urls = DAY1_URLS

    # последовательный запуск
    log.info("последовательный запуск")
    t0 = time.perf_counter()
    for u in urls:
        await crawler.fetch_url(u)
    seq_time = time.perf_counter() - t0

    # параллельный запуск
    log.info("параллельный запуск")
    t0 = time.perf_counter()
    await crawler.fetch_urls(urls)
    par_time = time.perf_counter() - t0
    await crawler.close()

    log.info("Статус каждого запроса:")
    for u in urls:
        log.info("  %s — %s", u, crawler.statuses.get(u, "?"))

    log.info("Последовательно: %.2f c", seq_time)
    log.info("Параллельно:     %.2f c", par_time)
    if par_time > 0:
        log.info("Ускорение:       %.1f×", seq_time / par_time)


if __name__ == "__main__":
    asyncio.run(main())