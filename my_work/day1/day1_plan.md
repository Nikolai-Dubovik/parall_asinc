# План реализации — День 1: базовый асинхронный HTTP-клиент

Файл с кодом: `parall_asinc/crawler_day1.py`

## Этапы

1. **Импорты и логирование**
   - `asyncio`, `logging`, `time`, `aiohttp`
   - `logging.basicConfig(level=logging.INFO)` — минимальная настройка
   - `log = logging.getLogger("crawler")`

2. **Класс `AsyncCrawler` — каркас**
   - `__init__(self, max_concurrent: int = 10)` создаёт:
     - `aiohttp.ClientTimeout(connect=5, total=10)` — таймауты
     - `aiohttp.TCPConnector(limit=max_concurrent)` — connection pooling
     - `aiohttp.ClientSession(...)` — общая сессия
     - `asyncio.Semaphore(max_concurrent)` — ограничение конкурентности
   - Замечание: экземпляр создаётся **внутри** `async def main()`, чтобы event loop уже работал — иначе `ClientSession`/`Semaphore` будут ругаться.

3. **`async def fetch_url(self, url) -> str`**
   - Захват семафора (`async with self.sem`)
   - Лог «→ url»
   - `session.get(url)` → `resp.raise_for_status()` → `await resp.text()`
   - Лог «✓ url (status)»
   - Три обработчика: `asyncio.TimeoutError`, `aiohttp.ClientResponseError`, `aiohttp.ClientError` — каждый просто пишет warning и возвращает пустую строку.

4. **`async def fetch_urls(self, urls) -> dict[str, str]`**
   - `asyncio.gather(*(self.fetch_url(u) for u in urls))`
   - Собрать в `dict(zip(urls, results))`

5. **`async def close(self)`**
   - `await self.session.close()`

6. **Демо `async def main()`**
   - Список из 5–10 URL (`example.com`, `httpbin.org/delay/1`, `httpbin.org/delay/2`, `httpbin.org/status/404`, `httpbin.org/get`)
   - Замер `time.perf_counter()` вокруг `fetch_urls` → вывести общее время
   - Распечатать длину тела для каждого URL
   - Не делаем формальное сравнение «последовательно vs параллельно» — для лёгкого синтаксиса достаточно одного замера и комментария про преимущество.

7. **Точка входа**
   - `if __name__ == "__main__": asyncio.run(main())`

## Установка зависимостей

```bash
pip install aiohttp
```

`aiofiles` для дня 1 не нужен — добавится на день 6.

## Критерии готовности

- Скрипт запускается: `python parall_asinc/crawler_day1.py`
- В логе видно «→» / «✓» / предупреждение про 404
- В конце — суммарное время выполнения