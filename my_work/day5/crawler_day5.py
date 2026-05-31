import asyncio
import json
import logging
import random
import sys
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day4"))

from crawler_day4 import PoliteCrawler


log = logging.getLogger("crawler")


class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


class NetworkError(Exception):
    pass


class ParseError(Exception):
    pass


class RetryStrategy:
    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0,
                 retry_on: list | None = None):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = tuple(retry_on) if retry_on else (TransientError, NetworkError)
        self.stats = {
            "errors_by_type": Counter(),
            "retries": 0,
            "successful_retries": 0,
            "permanent_failures": [],
        }

    async def execute_with_retry(self, coro_fn, *args, **kwargs):
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            try:
                result = await coro_fn(*args, **kwargs)
                if attempt > 0:
                    self.stats["successful_retries"] += 1
                    log.info("✅ успех после %d повтор(ов): %s", attempt, args)
                return result
            except self.retry_on as e:
                last_exc = e
                self.stats["errors_by_type"][type(e).__name__] += 1
                if attempt >= self.max_retries:
                    self.stats["permanent_failures"].append(str(args[0]) if args else "")
                    log.warning("⛔ %s после %d попыток: %s",
                                type(e).__name__, attempt + 1, args)
                    raise
                wait = self.backoff_factor ** attempt
                self.stats["retries"] += 1
                log.info("🔁 повтор #%d через %.1f c из-за %s: %s",
                         attempt + 1, wait, type(e).__name__, args)
                await asyncio.sleep(wait)
            except Exception as e:
                self.stats["errors_by_type"][type(e).__name__] += 1
                log.warning("⛔ постоянная ошибка %s: %s", type(e).__name__, args)
                raise
        assert last_exc is not None
        raise last_exc


class RetryCrawler(PoliteCrawler):
    TRANSIENT_STATUSES = {408, 429, 500, 502, 503, 504}
    PERMANENT_STATUSES = {401, 403, 404}

    def __init__(self, max_concurrent: int = 10, max_depth: int = 2,
                 requests_per_second: float = 1.0, respect_robots: bool = True,
                 min_delay: float = 0.0, jitter: float = 0.0,
                 user_agent: str = "MyBot/1.0",
                 per_domain_limit: int = 3,
                 retry_strategy: RetryStrategy | None = None):
        super().__init__(
            max_concurrent=max_concurrent,
            max_depth=max_depth,
            requests_per_second=requests_per_second,
            respect_robots=respect_robots,
            min_delay=min_delay,
            jitter=jitter,
            user_agent=user_agent,
            per_domain_limit=per_domain_limit,
        )
        self.retry_strategy = retry_strategy or RetryStrategy()

    async def _do_request(self, url: str) -> str:
        async with self.sem:
            log.info("▶️ начало загрузки %s", url)
            try:
                async with self.session.get(
                    url, headers={"User-Agent": self.user_agent}
                ) as resp:
                    if resp.status in self.PERMANENT_STATUSES:
                        self.statuses[url] = resp.status
                        raise PermanentError(f"HTTP {resp.status}")
                    if resp.status in self.TRANSIENT_STATUSES:
                        self.statuses[url] = resp.status
                        raise TransientError(f"HTTP {resp.status}")
                    resp.raise_for_status()
                    text = await resp.text()
                    self.statuses[url] = resp.status
                    self.request_count += 1
                    log.info("✅ успешное завершение %s", url)
                    return text
            except asyncio.TimeoutError as e:
                self.statuses[url] = "timeout"
                raise TransientError(f"timeout: {e}") from e
            except aiohttp.ClientResponseError as e:
                self.statuses[url] = e.status
                if e.status in self.PERMANENT_STATUSES:
                    raise PermanentError(f"HTTP {e.status}") from e
                if e.status in self.TRANSIENT_STATUSES:
                    raise TransientError(f"HTTP {e.status}") from e
                raise NetworkError(str(e)) from e
            except aiohttp.ClientError as e:
                self.statuses[url] = type(e).__name__
                raise NetworkError(str(e)) from e

    async def fetch_url(self, url: str) -> str:
        if self.started_at is None:
            self.started_at = time.monotonic()

        parsed = urlparse(url)
        domain = parsed.netloc
        base = f"{parsed.scheme}://{domain}"

        if self.respect_robots:
            await self.robots.fetch_robots(base)
            if not self.robots.can_fetch(url, self.user_agent):
                self.blocked_by_robots += 1
                self.statuses[url] = "robots_blocked"
                log.warning("🚫 robots.txt блокирует %s", url)
                return ""

        crawl_delay = (
            self.robots.get_crawl_delay(url, self.user_agent)
            if self.respect_robots else 0.0
        )
        await self.rate_limiter.acquire(domain)

        delay = max(self.min_delay, crawl_delay)
        if self.jitter > 0:
            delay += random.uniform(0, self.jitter)
        if delay > 0:
            await asyncio.sleep(delay)
            self.total_delay += delay
            self.delay_count += 1

        try:
            return await self.retry_strategy.execute_with_retry(self._do_request, url)
        except Exception as e:
            log.warning("⚠️ итоговая ошибка %s: %s", type(e).__name__, url)
            return ""


async def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    strategy = RetryStrategy(
        max_retries=2,
        backoff_factor=1.5,
        retry_on=[TransientError, NetworkError],
    )
    crawler = RetryCrawler(
        max_concurrent=5,
        max_depth=0,
        requests_per_second=2.0,
        respect_robots=False,
        min_delay=0.0,
        jitter=0.0,
        user_agent="MyBot/1.0",
        retry_strategy=strategy,
    )
    try:
        results = await crawler.crawl(
            start_urls=[
                "https://example.com",
                "https://httpbin.org/status/503",
                "https://httpbin.org/status/404",
                "https://httpbin.org/get",
            ],
            max_pages=10,
            same_domain_only=False,
        )
        print(f"Обработано: {len(results)} страниц")
        print(f"Повторов: {strategy.stats['retries']}")
        print(f"Успешных после повтора: {strategy.stats['successful_retries']}")
        print(f"Постоянных провалов: {len(strategy.stats['permanent_failures'])}")
        print(f"Ошибки по типам: {dict(strategy.stats['errors_by_type'])}")

        out_path = Path(__file__).resolve().parent / "day5_results.json"
        summary = {
            "retry_stats": {
                "errors_by_type": dict(strategy.stats["errors_by_type"]),
                "retries": strategy.stats["retries"],
                "successful_retries": strategy.stats["successful_retries"],
                "permanent_failures": strategy.stats["permanent_failures"],
            },
            "statuses": {u: str(s) for u, s in crawler.statuses.items()},
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        log.info("💾 сохранено в %s", out_path)
    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
