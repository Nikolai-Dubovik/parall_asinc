# План реализации — День 7: финальная интеграция

Файл с кодом: `my_work/day7/crawler_day7.py`. Наследуемся от `StorageCrawler` дня 6.

## Этапы

1. **Импорты** — стандартная библиотека (`argparse, asyncio, json, logging, sys, time`, `xml.etree.ElementTree`, `collections.Counter`, `logging.handlers.RotatingFileHandler`, `datetime`), плюс импорт `StorageCrawler/JSONStorage/CSVStorage/SQLiteStorage` из дня 6.

2. **`SitemapParser` (пункт 1)** — `__init__(session)`; `async fetch_sitemap(sitemap_url) -> list[str]`:
   - GET через `self.session`; на не-200 / любую ошибку → `[]` (логировать предупреждение);
   - парсинг через `xml.etree.ElementTree.fromstring`;
   - namespace `http://www.sitemaps.org/schemas/sitemap/0.9`;
   - если есть `<sitemap><loc>` → рекурсивно вызываем себя по каждому;
   - если есть `<url><loc>` → собираем `loc.text`.

3. **`CrawlerStats` (пункт 2)** — поля: `started: float`, `successful`, `failed`, `statuses: Counter`, `domains: Counter`. Методы: `record_success(url, status)`, `record_failure(url, status)`, `summary()` (возвращает dict с `total_pages`, `successful`, `failed`, `elapsed_sec`, `speed_pps`, `status_codes`, `top_domains`).

4. **`AdvancedCrawler(StorageCrawler)` (пункт 8)**
   - `__init__(...)` → `super().__init__(...)`, создать `self.stats = CrawlerStats()`, `self.sitemap = SitemapParser(self.session)`.
   - `_process_url` — `await super()._process_url(...)` + по `self.statuses[url]` фиксируем success/failure в `stats`.
   - `async crawl_from_sitemap(sitemap_url, max_pages, **kwargs)` — `urls = await self.sitemap.fetch_sitemap(sitemap_url)`; `return await self.crawl(start_urls=urls[:max_pages], max_pages=max_pages, **kwargs)`.
   - `export_to_json(filename)` — обычный `json.dump(stats.summary(), …)` (пункт 3).
   - `export_to_html_report(filename)` (пункт 3) — простая HTML-таблица из `stats.summary()`. Без графиков (опционально).
   - `get_stats()` → `stats.summary()`.
   - Классметод `from_config(path)` — JSON-конфиг (пункт 4), читает параметры краулера и тип storage.

5. **CLI (пункт 5)** — `argparse`: `--urls`, `--max-pages`, `--max-depth`, `--output`, `--config`, `--respect-robots`, `--rate-limit`, `--user-agent`, `--storage` (json/csv/sqlite), `--storage-path`, `--sitemap`, `--log-file`.

6. **Логирование в файл (пункт 6)** — `setup_logging(level, log_file)` через `basicConfig` с `StreamHandler` + опциональным `RotatingFileHandler` (1 МБ, 3 backup).

7. **Демонстрация (пункт 9)** — `run()` собирает краулер из CLI/конфига, запускает `crawl` или `crawl_from_sitemap`, печатает итоговую статистику, экспортирует JSON и HTML отчёты.

8. **`config.json`** — пример конфига рядом со скриптом для проверки `--config`.

## Намеренно не делаем

- YAML конфиг — только JSON (без PyYAML).
- Графики/визуализацию в HTML (опциональны).
- Прогресс-бар в реальном времени — уже есть `📊` лог из дня 3.
- Прокси/cookies/JS-рендеринг, распределённый краулинг — все опциональны.
- Тесты производительности и полную документацию.
