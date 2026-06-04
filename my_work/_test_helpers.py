"""Утилиты для тестов: фейковые HTTP-ответы aiohttp без реальной сети.

Используются во всех test_dayN.py, чтобы тесты были детерминированными и быстрыми.
"""
import logging

import aiohttp

# тесты не должны шуметь логами краулера
logging.getLogger("crawler").setLevel(logging.CRITICAL)


class FakeResp:
    """Подделка aiohttp-ответа: отдаёт заданные status/тело/заголовки."""

    def __init__(self, status: int = 200, body: str = "", headers: dict | None = None):
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def text(self) -> str:
        return self._body

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=None, history=(), status=self.status,
                message=f"HTTP {self.status}",
            )

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False


class FakeCtx:
    """Замена для session.get(...): async-контекст, отдающий ответ или кидающий исключение."""

    def __init__(self, resp: FakeResp | None = None, exc: Exception | None = None):
        self._resp = resp
        self._exc = exc

    async def __aenter__(self):
        if self._exc is not None:
            raise self._exc
        return self._resp

    async def __aexit__(self, *exc):
        return False


def fake_get(handler):
    """Собрать функцию-замену session.get.

    handler — либо FakeResp/Exception, либо callable(url) -> FakeResp/Exception.
    """
    def _get(url, **kwargs):
        result = handler(url) if callable(handler) else handler
        if isinstance(result, Exception):
            return FakeCtx(exc=result)
        return FakeCtx(resp=result)
    return _get


class FakeSession:
    """Минимальная подделка ClientSession — только метод get (для RobotsParser/SitemapParser)."""

    def __init__(self, handler):
        self._get = fake_get(handler)

    def get(self, url, **kwargs):
        return self._get(url, **kwargs)


def patch_session(crawler, handler) -> None:
    """Подменить crawler.session.get фейком (сеть не используется)."""
    crawler.session.get = fake_get(handler)
