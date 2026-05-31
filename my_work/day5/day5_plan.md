# План реализации — День 5: ошибки и повторы

Файл с кодом: `my_work/day5/crawler_day5.py`. Наследуемся от `PoliteCrawler` дня 4.

## Этапы

1. **Импорты** — `asyncio, json, logging, sys, time`, `Path`, `urlparse`, `aiohttp`, `PoliteCrawler` из дня 4.

2. **Классы ошибок (пункт 2)** — четыре пустых исключения: `TransientError`, `PermanentError`, `NetworkError`, `ParseError`.

3. **`RetryStrategy` (пункты 1, 3, 8, 9)**
   - `__init__(max_retries=3, backoff_factor=2.0, retry_on=None)`; по умолчанию `retry_on=(TransientError, NetworkError)`.
   - `stats`: `errors_by_type: Counter`, `retries: int`, `successful_retries: int`, `permanent_failures: list[str]`.
   - `async execute_with_retry(coro_fn, *args, **kwargs)` — цикл `attempt in range(max_retries+1)`:
     - успех → если `attempt>0` инкрементить `successful_retries`, вернуть результат;
     - `except retry_on` → инкрементить счётчики, лог `🔁 повтор #N через …`, `sleep(backoff_factor**attempt)`, продолжить;
     - превышение попыток → лог `⛔`, добавить в `permanent_failures`, прокинуть;
     - `except Exception` (включая `PermanentError`) → инкрементить, прокинуть.

4. **`RetryCrawler(PoliteCrawler)` (пункты 4, 5, 6)**
   - Константы: `TRANSIENT_STATUSES = {408,429,500,502,503,504}`, `PERMANENT_STATUSES = {401,403,404}`.
   - `__init__(..., retry_strategy=None)` → `super().__init__(...)`, `self.retry_strategy = retry_strategy or RetryStrategy()`.
   - `_do_request(url)` — низкоуровневый запрос (HTTP-часть из дня 4), классифицирует исключения:
     - status в PERMANENT → `raise PermanentError`;
     - status в TRANSIENT → `raise TransientError`;
     - `asyncio.TimeoutError` → `TransientError`;
     - `aiohttp.ClientResponseError` → по статусу `Permanent`/`Transient`, иначе `NetworkError`;
     - `aiohttp.ClientError` → `NetworkError`.
   - `fetch_url(url)` — переопределение: повторяет robots/rate-limit/delay из дня 4 + оборачивает `_do_request` через `retry_strategy.execute_with_retry`. `PermanentError`/конечные ошибки → лог, `statuses[url] = …`, return `""`.

5. **`main` (пункт 10)** — настраиваем `RetryStrategy(max_retries=2, backoff_factor=1.5)`, `RetryCrawler` со списком URL, включающим `httpbin.org/status/503` (повторяется) и `/status/404` (не повторяется). Печатаем `strategy.stats`. Сохраняем `day5_results.json`.

## Намеренно не делаем

- Circuit Breaker (опционально по ТЗ).
- Увеличение таймаутов при повторах — не нужно, `backoff_factor` уже даёт паузы.
- Автотесты.
