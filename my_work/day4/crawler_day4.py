import asyncio
import json
import logging
import random
import sys
import time
import urllib.robotparser
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day3"))

from crawler_day3 import QueueCrawler
from sample_urls import DAY1_URLS


log = logging.getLogger("crawler")


class RateLimiter:
    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True):
        self.interval = 1.0 / requests_per_second if requests_per_second > 0 else 0.0
        self.per_domain = per_domain
        self.last_ts: dict[str, float] = {}
        self.locks: dict[str, asyncio.Lock] = {}

    def _key(self, domain: str | None) -> str:
        if not self.per_domain:
            return "_global"
        return domain or ""

    async def acquire(self, domain: str | None = None, min_interval: float = 0.0) -> float:
        """Ждёт max(interval, min_interval) с прошлого запроса к домену.

        Так min_delay из robots/настроек складывается в единый интервал лимитера —
        без отдельного sleep. Возвращает фактическое время ожидания (для статистики).
        """
        interval = max(self.interval, min_interval)
        if interval <= 0:
            return 0.0
        key = self._key(domain)
        lock = self.locks.setdefault(key, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            wait = interval - (now - self.last_ts.get(key, 0.0))
            if wait > 0:
                await asyncio.sleep(wait)
            self.last_ts[key] = time.monotonic()
            return wait if wait > 0 else 0.0


class RobotsParser:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self.cache: dict[str, urllib.robotparser.RobotFileParser] = {}

    @staticmethod
    def _base(url: str) -> str:
        p = urlparse(url)
        return f"{p.scheme}://{p.netloc}"

    async def fetch_robots(self, base_url: str) -> dict:
        """Загрузить и закэшировать robots.txt домена. Возвращает краткую сводку (dict)."""
        if base_url not in self.cache:
            rp = urllib.robotparser.RobotFileParser()
            robots_url = urljoin(base_url + "/", "/robots.txt")
            rp.set_url(robots_url)
            try:
                async with self.session.get(robots_url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        rp.parse(text.splitlines())
                    else:
                        rp.parse([])
            except Exception as e:
                log.warning("⚠️ не удалось загрузить robots.txt %s: %s", robots_url, e)
                rp.parse([])
            self.cache[base_url] = rp
        rp = self.cache[base_url]
        return {
            "base_url": base_url,
            "can_fetch_root": rp.can_fetch("*", base_url + "/"),
            "crawl_delay": float(rp.crawl_delay("*") or 0.0),
        }

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        rp = self.cache.get(self._base(url))
        if rp is None:
            return True
        return rp.can_fetch(user_agent, url)

    def get_crawl_delay(self, url: str, user_agent: str = "*") -> float:
        rp = self.cache.get(self._base(url))
        if rp is None:
            return 0.0
        delay = rp.crawl_delay(user_agent)
        return float(delay) if delay else 0.0


class PoliteCrawler(QueueCrawler):
    def __init__(self, max_concurrent: int = 10, max_depth: int = 2,
                 requests_per_second: float = 1.0, respect_robots: bool = True,
                 min_delay: float = 0.0, jitter: float = 0.0,
                 user_agent: str = "MyBot/1.0",
                 user_agents: list[str] | None = None,
                 per_domain_limit: int = 3):
        super().__init__(max_concurrent=max_concurrent, max_depth=max_depth,
                         per_domain_limit=per_domain_limit)
        self.user_agent = user_agent
        # опциональная ротация User-Agent: по умолчанию список из одного user_agent
        self.user_agents = user_agents or [user_agent]
        self._ua_index = 0
        self.respect_robots = respect_robots
        self.min_delay = min_delay
        self.jitter = jitter
        self.rate_limiter = RateLimiter(requests_per_second, per_domain=True)
        self.robots = RobotsParser(self.session)
        self.blocked_by_robots = 0
        self.statuses: dict[str, object] = {}
        self.request_count = 0
        self.total_delay = 0.0
        self.delay_count = 0
        self.started_at: float | None = None

    def _next_user_agent(self) -> str:
        """Следующий User-Agent (ротация по кругу, если задан список из нескольких)."""
        ua = self.user_agents[self._ua_index % len(self.user_agents)]
        self._ua_index += 1
        return ua

    async def _polite_preamble(self, url: str) -> bool:
        """robots.txt + rate limiting + задержки перед запросом.

        Возвращает False, если URL заблокирован robots.txt (запрос делать нельзя).
        Переиспользуется наследниками (день 5) перед своей логикой загрузки.
        """
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
                return False

        crawl_delay = (
            self.robots.get_crawl_delay(url, self.user_agent)
            if self.respect_robots else 0.0
        )
        # единый интервал: max(лимитер, min_delay, crawl_delay); jitter — рандомизация поверх
        waited = await self.rate_limiter.acquire(
            domain, min_interval=max(self.min_delay, crawl_delay)
        )
        if self.jitter > 0:
            j = random.uniform(0, self.jitter)
            await asyncio.sleep(j)
            waited += j
        if waited > 0:
            self.total_delay += waited
            self.delay_count += 1
        return True

    async def _do_get(self, url: str) -> str:
        """Один GET-запрос с обработкой ошибок (без повторов)."""
        log.info("▶️ начало загрузки %s", url)
        try:
            async with self.session.get(
                url, headers={"User-Agent": self._next_user_agent()}
            ) as resp:
                resp.raise_for_status()
                text = await resp.text()
                self.statuses[url] = resp.status
                self.request_count += 1
                log.info("✅ успешное завершение %s", url)
                return text
        except asyncio.TimeoutError:
            self.statuses[url] = "timeout"
            log.warning("⚠️ timeout %s", url)
        except aiohttp.ClientResponseError as e:
            self.statuses[url] = e.status
            log.warning("⚠️ %s %s", e.status, url)
        except aiohttp.ClientError as e:
            self.statuses[url] = type(e).__name__
            log.warning("⚠️ %s %s", type(e).__name__, url)
        return ""

    async def fetch_url(self, url: str) -> str:
        if not await self._polite_preamble(url):
            return ""
        return await self._do_get(url)

    def stats(self) -> dict:
        elapsed = (time.monotonic() - self.started_at) if self.started_at else 0.0
        rps = self.request_count / elapsed if elapsed > 0 else 0.0
        avg_delay = self.total_delay / self.delay_count if self.delay_count else 0.0
        return {
            "requests": self.request_count,
            "req_per_sec": round(rps, 2),
            "avg_delay": round(avg_delay, 3),
            "blocked_by_robots": self.blocked_by_robots,
        }


async def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    crawler = PoliteCrawler(
        max_concurrent=5,
        max_depth=1,
        requests_per_second=2.0,
        respect_robots=True,
        min_delay=0.3,
        jitter=0.2,
        user_agent="MyBot/1.0",
    )
    try:
        results = await crawler.crawl(
            start_urls=DAY1_URLS,
            max_pages=10,
            same_domain_only=False,
        )
        s = crawler.stats()
        print(f"Обработано: {len(results)} страниц")
        print(f"Запросов: {s['requests']}, скорость: {s['req_per_sec']} req/s")
        print(f"Средняя задержка: {s['avg_delay']} c")
        print(f"Заблокировано robots.txt: {s['blocked_by_robots']}")

        out_path = Path(__file__).resolve().parent / "day4_results.json"
        summary = {
            "stats": s,
            "pages": [
                {
                    "url": u,
                    "title": r.get("title", ""),
                    "links_count": len(r.get("links", [])),
                    "text_length": len(r.get("text", "")),
                }
                for u, r in results.items()
            ],
        }
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)
        log.info("💾 сохранено в %s", out_path)
    finally:
        await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
