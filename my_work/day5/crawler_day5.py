import asyncio
import inspect
import json
import logging
import sys
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse

import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day4"))

from crawler_day4 import PoliteCrawler
from sample_urls import DAY1_URLS


log = logging.getLogger("crawler")


class TransientError(Exception):
    pass


class PermanentError(Exception):
    pass


class NetworkError(Exception):
    pass


class ParseError(Exception):
    pass


class RateLimitError(TransientError):
    """429 Too Many Requests — временная ошибка с подсказкой задержки (Retry-After)."""

    def __init__(self, message: str = "", retry_after: float = 0.0):
        super().__init__(message)
        self.retry_after = retry_after


class CircuitBreaker:
    """Простой circuit breaker по домену: открывается после серии ошибок и держит паузу."""

    def __init__(self, threshold: int = 5, cooldown: float = 30.0):
        self.threshold = threshold
        self.cooldown = cooldown
        self._failures: dict[str, int] = {}
        self._open_until: dict[str, float] = {}

    def is_open(self, domain: str) -> bool:
        until = self._open_until.get(domain)
        if until is None:
            return False
        if time.monotonic() < until:
            return True
        # пауза прошла — сбрасываем счётчики и снова пропускаем запросы к домену
        self._open_until.pop(domain, None)
        self._failures[domain] = 0
        return False

    def record_success(self, domain: str) -> None:
        self._failures[domain] = 0

    def record_failure(self, domain: str) -> None:
        n = self._failures.get(domain, 0) + 1
        self._failures[domain] = n
        if n >= self.threshold:
            self._open_until[domain] = time.monotonic() + self.cooldown


class RetryStrategy:
    def __init__(self, max_retries: int = 3, backoff_factor: float = 2.0,
                 retry_on: list | None = None,
                 backoff_overrides: dict | None = None):
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor
        self.retry_on = tuple(retry_on) if retry_on else (TransientError, NetworkError)
        # разный backoff для разных типов ошибок: {NetworkError: 1.5, TransientError: 2.0}
        self.backoff_overrides = backoff_overrides or {}
        self.stats = {
            "errors_by_type": Counter(),
            "retries": 0,
            "successful_retries": 0,
            "retry_wait_total": 0.0,
            "permanent_failures": [],
        }

    def avg_retry_wait(self) -> float:
        """Среднее время ожидания на один повтор."""
        n = self.stats["retries"]
        return self.stats["retry_wait_total"] / n if n else 0.0

    async def execute_with_retry(self, coro_fn, *args, **kwargs):
        # если загрузчик принимает номер попытки — прокидываем его (для роста таймаута)
        pass_attempt = "attempt" in inspect.signature(coro_fn).parameters
        target = str(args[0]) if args else ""
        last_exc: Exception | None = None
        for attempt in range(self.max_retries + 1):
            call_kwargs = {**kwargs, "attempt": attempt} if pass_attempt else kwargs
            try:
                result = await coro_fn(*args, **call_kwargs)
                if attempt > 0:
                    self.stats["successful_retries"] += 1
                    log.info("✅ успех после %d повтор(ов): %s", attempt, target)
                return result
            except self.retry_on as e:
                last_exc = e
                self.stats["errors_by_type"][type(e).__name__] += 1
                if attempt >= self.max_retries:
                    self.stats["permanent_failures"].append(target)
                    log.warning("⛔ %s после %d попыток: %s",
                                type(e).__name__, attempt + 1, target)
                    raise
                factor = self.backoff_overrides.get(type(e), self.backoff_factor)
                # учитываем Retry-After (например, для 429), если он больше расчётного backoff
                wait = max(factor ** attempt, getattr(e, "retry_after", 0.0) or 0.0)
                self.stats["retries"] += 1
                self.stats["retry_wait_total"] += wait
                log.info("🔁 повтор #%d через %.1f c из-за %s: %s",
                         attempt + 1, wait, type(e).__name__, target)
                await asyncio.sleep(wait)
            except Exception as e:
                self.stats["errors_by_type"][type(e).__name__] += 1
                log.warning("⛔ постоянная ошибка %s: %s", type(e).__name__, target)
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
                 retry_strategy: RetryStrategy | None = None,
                 retry_timeout: float = 10.0,
                 circuit_breaker: CircuitBreaker | None = None):
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
        self.retry_timeout = retry_timeout
        self.circuit_breaker = circuit_breaker

    @staticmethod
    def _parse_retry_after(value) -> float:
        """Retry-After в секундах (число). HTTP-дата не поддерживается — вернём 0."""
        if not value:
            return 0.0
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    async def _do_request(self, url: str, attempt: int = 0) -> str:
        # таймаут растёт с каждой попыткой: base, 2×base, 3×base …
        timeout = aiohttp.ClientTimeout(total=self.retry_timeout * (attempt + 1))
        log.info("▶️ начало загрузки %s (попытка %d)", url, attempt + 1)
        try:
            async with self.session.get(
                url, headers={"User-Agent": self._next_user_agent()}, timeout=timeout
            ) as resp:
                if resp.status == 429:
                    self.statuses[url] = resp.status
                    retry_after = self._parse_retry_after(resp.headers.get("Retry-After"))
                    raise RateLimitError("HTTP 429", retry_after=retry_after)
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
        domain = urlparse(url).netloc
        if self.circuit_breaker and self.circuit_breaker.is_open(domain):
            self.statuses[url] = "circuit_open"
            log.warning("🔌 circuit breaker открыт для %s — пропуск %s", domain, url)
            return ""
        # вежливая преамбула (robots + rate limit + задержки) переиспользуется из дня 4
        if not await self._polite_preamble(url):
            return ""
        try:
            text = await self.retry_strategy.execute_with_retry(self._do_request, url)
            if self.circuit_breaker:
                self.circuit_breaker.record_success(domain)
            return text
        except Exception as e:
            if self.circuit_breaker:
                self.circuit_breaker.record_failure(domain)
            log.warning("⚠️ итоговая ошибка %s: %s", type(e).__name__, url)
            return ""

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        try:
            return await self.parser.parse_html(html, url)
        except Exception as e:
            # парсинг не повторяем (тот же HTML распарсится так же); классифицируем как ParseError
            self.retry_strategy.stats["errors_by_type"][ParseError.__name__] += 1
            log.warning("⚠️ ошибка парсинга %s: %s", url, e)
            raise ParseError(str(e)) from e


async def main():
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    strategy = RetryStrategy(
        max_retries=2,
        backoff_factor=1.5,
        retry_on=[TransientError, NetworkError],
        # сетевые — мягче, обычные временные — средне, 429 (RateLimitError) — агрессивнее
        backoff_overrides={NetworkError: 1.5, TransientError: 2.0, RateLimitError: 3.0},
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
        circuit_breaker=CircuitBreaker(threshold=5, cooldown=15.0),
    )
    try:
        results = await crawler.crawl(
            start_urls=DAY1_URLS,
            max_pages=10,
            same_domain_only=False,
        )
        print(f"Обработано: {len(results)} страниц")
        print(f"Повторов: {strategy.stats['retries']}")
        print(f"Успешных после повтора: {strategy.stats['successful_retries']}")
        print(f"Постоянных провалов: {len(strategy.stats['permanent_failures'])}")
        print(f"Среднее время на повтор: {strategy.avg_retry_wait():.2f} c")
        print(f"Ошибки по типам: {dict(strategy.stats['errors_by_type'])}")

        out_path = Path(__file__).resolve().parent / "day5_results.json"
        summary = {
            "retry_stats": {
                "errors_by_type": dict(strategy.stats["errors_by_type"]),
                "retries": strategy.stats["retries"],
                "successful_retries": strategy.stats["successful_retries"],
                "avg_retry_wait": round(strategy.avg_retry_wait(), 2),
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
