# План реализации — День 6: сохранение данных

Файл с кодом: `my_work/day6/crawler_day6.py`. Наследуемся от `RetryCrawler` дня 5.

## Зависимости

```bash
pip install aiofiles aiosqlite
```

## Этапы

1. **Импорты** — `asyncio, csv, io, json, logging, sys`, `abc`, `datetime.datetime/timezone`, `Path`, `aiofiles`, `aiosqlite`, `RetryCrawler` из дня 5.

2. **Абстрактный `DataStorage` (пункт 2)** — `abc.ABC` с `async save(data)` и `async close()`.

3. **`JSONStorage` (пункт 3)** — формат **JSONL** (одна запись на строку): просто и масштабируется без чтения всего файла. Внутри — `aiofiles.open` лениво при первом `save`, `asyncio.Lock` для конкуретной записи, `json.dumps(..., default=str)` для `datetime`.

4. **`CSVStorage` (пункт 4)** — `aiofiles.open` лениво, заголовки берём из первого `save()`. Сложные значения (`list`, `dict`) сериализуем как `json.dumps`. Запись строки через `csv.DictWriter` в `io.StringIO`, потом дамп в `aiofiles`. `Lock` для конкурентной записи.

5. **`SQLiteStorage` (пункт 5)** — `aiosqlite.connect` лениво в `init_db`. Таблица `pages(url PK, title, text, links, metadata, crawled_at, status_code, content_type)` + индекс по `status_code`. `INSERT OR REPLACE` в `save`. `Lock` чтобы не плодить параллельные `commit`.

6. **`StorageCrawler(RetryCrawler)` (пункты 6, 7, 8)**
   - `__init__(..., storage=None)`.
   - Переопределяем `_process_url(url, depth, …)` — копия из дня 3, но после `mark_processed` добавляем сохранение через `storage.save(payload)`. Структура payload — как в ТЗ (`url, title, text, links, metadata, crawled_at, status_code, content_type`). `try/except` вокруг сохранения, на ошибку — лог, продолжаем.
   - `async close()` — закрыть storage перед `super().close()`.

7. **Демо `main` (пункт 10)** — крутим краулер три раза, по разу на каждый storage (JSON, CSV, SQLite), на `https://example.com` с `max_pages=3`. В конце читаем из SQLite через `aiosqlite` и печатаем `SELECT url, title FROM pages`.

## Намеренно не делаем

- Batch-вставки (пункт 9 — опциональны).
- Повторы при ошибках записи (используем простой `try/except` + лог).
- Автотесты.
