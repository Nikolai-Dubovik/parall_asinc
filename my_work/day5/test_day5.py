"""Тесты дня 5 (по разделу «Тестирование» ТЗ):
- повторы при таймаутах
- повторы при 503
- отсутствие повторов при 404
- экспоненциальный backoff
- статистика ошибок
"""
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(HERE.parent))

from crawler_day5 import (
    RetryCrawler, RetryStrategy, TransientError, PermanentError, NetworkError,
)
from _test_helpers import FakeResp, fake_get


class Flaky:
    """Корутина, падающая первые N раз заданным исключением, затем возвращающая 'ok'."""

    def __init__(self, fail_times: int, exc: Exception):
        self.calls = 0
        self.fail_times = fail_times
        self.exc = exc

    async def __call__(self):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise self.exc
        return "ok"


async def _noop_sleep(_):
    return None


async def _raise_permanent(url):
    """Корутина, всегда падающая PermanentError (имитация 404 для конкретного URL)."""
    raise PermanentError("404")


class TestRetryStrategy(unittest.IsolatedAsyncioTestCase):
    async def test_retry_on_timeout(self):
        """Временная ошибка (таймаут) повторяется и в итоге проходит."""
        strat = RetryStrategy(max_retries=3, backoff_factor=2.0)
        flaky = Flaky(2, TransientError("timeout"))
        with patch("asyncio.sleep", _noop_sleep):
            result = await strat.execute_with_retry(flaky)
        self.assertEqual(result, "ok")
        self.assertEqual(flaky.calls, 3)              # 1 + 2 повтора
        self.assertEqual(strat.stats["retries"], 2)
        self.assertEqual(strat.stats["successful_retries"], 1)

    async def test_no_retry_on_permanent(self):
        """Постоянная ошибка (как 404) не повторяется — вызов ровно один."""
        strat = RetryStrategy(max_retries=3)
        flaky = Flaky(99, PermanentError("404"))
        with patch("asyncio.sleep", _noop_sleep):
            with self.assertRaises(PermanentError):
                await strat.execute_with_retry(flaky)
        self.assertEqual(flaky.calls, 1)
        self.assertEqual(strat.stats["retries"], 0)

    async def test_permanent_failure_recorded(self):
        """PermanentError без повторов попадает в permanent_failures с нужным URL."""
        strat = RetryStrategy(max_retries=2)
        with patch("asyncio.sleep", _noop_sleep):
            with self.assertRaises(PermanentError):
                await strat.execute_with_retry(_raise_permanent, "https://s/x")
        self.assertEqual(strat.stats["retries"], 0)
        self.assertIn("https://s/x", strat.stats["permanent_failures"])
        self.assertEqual(len(strat.stats["permanent_failures"]), 1)

    async def test_exponential_backoff(self):
        """Паузы между повторами растут как factor**attempt."""
        strat = RetryStrategy(max_retries=3, backoff_factor=2.0)
        waits = []

        async def rec_sleep(d):
            waits.append(d)

        with patch("asyncio.sleep", rec_sleep):
            with self.assertRaises(NetworkError):
                await strat.execute_with_retry(Flaky(99, NetworkError("x")))
        self.assertEqual(waits, [1.0, 2.0, 4.0])

    async def test_error_stats(self):
        """Статистика ошибок ведётся: типы, повторы, постоянные провалы."""
        strat = RetryStrategy(max_retries=2)
        with patch("asyncio.sleep", _noop_sleep):
            with self.assertRaises(TransientError):
                await strat.execute_with_retry(Flaky(99, TransientError("503")))
        self.assertEqual(strat.stats["errors_by_type"]["TransientError"], 3)
        self.assertEqual(strat.stats["retries"], 2)
        self.assertEqual(len(strat.stats["permanent_failures"]), 1)


class TestRetryCrawler(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.strategy = RetryStrategy(max_retries=2, backoff_factor=2.0)
        self.crawler = RetryCrawler(max_concurrent=2, max_depth=0,
                                    respect_robots=False, retry_strategy=self.strategy)

    async def asyncTearDown(self):
        await self.crawler.close()

    async def test_503_classified_and_retried(self):
        """503 → TransientError, повторяется max_retries раз."""
        calls = {"n": 0}

        def handler(url):
            calls["n"] += 1
            return FakeResp(503)

        self.crawler.session.get = fake_get(handler)
        with patch("asyncio.sleep", _noop_sleep):
            with self.assertRaises(TransientError):
                await self.strategy.execute_with_retry(self.crawler._do_request, "https://s/x")
        self.assertEqual(calls["n"], 3)  # 1 + 2 повтора

    async def test_404_no_retry(self):
        """404 → PermanentError, без повторов (один запрос)."""
        calls = {"n": 0}

        def handler(url):
            calls["n"] += 1
            return FakeResp(404)

        self.crawler.session.get = fake_get(handler)
        with patch("asyncio.sleep", _noop_sleep):
            with self.assertRaises(PermanentError):
                await self.strategy.execute_with_retry(self.crawler._do_request, "https://s/x")
        self.assertEqual(calls["n"], 1)
        self.assertEqual(self.strategy.stats["retries"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
