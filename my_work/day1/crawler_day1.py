import asyncio
import logging
import time

import aiohttp


logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("crawler")


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        timeout = aiohttp.ClientTimeout(connect=5, total=10)
        connector = aiohttp.TCPConnector(limit=max_concurrent, ssl=False)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        self.sem = asyncio.Semaphore(max_concurrent)
        self.statuses: dict[str, int | str] = {}

    async def fetch_url(self, url: str) -> str:
        async with self.sem:
            log.info("▶️ начало загрузки %s", url)
            try:
                async with self.session.get(url) as resp:
                    resp.raise_for_status()
                    text = await resp.text()
                    self.statuses[url] = resp.status
                    log.info("✅ успешное завершение %s", url)
                    return text
            except asyncio.TimeoutError as e:
                self.statuses[url] = "timeout"
                log.warning("⚠️ ошибка %s: %s", type(e).__name__, url)
            except aiohttp.ClientResponseError as e:
                self.statuses[url] = e.status
                log.warning("⚠️ ошибка %s: %s", type(e).__name__, url)
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
    urls = [
        "https://onliner.by",
        "https://pikabu.ru",
        "https://edvibe.com/",
        "https://nebdeti.ru/",
        "https://t-j.ru/",
    ]

    await crawler.fetch_urls(urls)

    await crawler.close()



if __name__ == "__main__":
    asyncio.run(main())