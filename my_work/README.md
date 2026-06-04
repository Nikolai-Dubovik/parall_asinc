# Асинхронный веб-краулер (asyncio + aiohttp)

Учебный проект: модульный асинхронный краулер, собранный за 7 «дней». Каждый день —
отдельный слой, наследующийся от предыдущего:

| День | Класс | Что добавляет |
|------|-------|---------------|
| 1 | `AsyncCrawler` | HTTP-клиент: семафор конкурентности, таймауты, обработка ошибок |
| 2 | `AsyncCrawler` + `HTMLParser` | парсинг HTML, извлечение ссылок/текста/метаданных |
| 3 | `QueueCrawler` | очередь с приоритетом, семафоры, глубина, фильтры, прогресс |
| 4 | `PoliteCrawler` | rate limiting, robots.txt, задержки, ротация User-Agent |
| 5 | `RetryCrawler` | повторы с backoff, классификация ошибок, circuit breaker |
| 6 | `StorageCrawler` | асинхронное сохранение в JSON Lines / CSV / SQLite |
| 7 | `AdvancedCrawler` | sitemap, статистика, HTML-отчёт, CLI, конфиг, логи |

## Установка

```bash
pip install -r requirements.txt
```

Зависимости: `aiohttp`, `aiofiles`, `beautifulsoup4`, `lxml`, `aiosqlite`.

## Запуск демо по дням

Каждый файл запускается напрямую и демонстрирует свой слой (на URL из `sample_urls.py`):

```bash
python day1/crawler_day1.py     # параллельная загрузка, сравнение с последовательной
python day2/crawler_day2.py     # парсинг страниц → day2_results.json
python day3/crawler_day3.py     # обход с очередью/глубиной → day3_results.json
python day4/crawler_day4.py     # robots.txt + rate limiting
python day5/crawler_day5.py     # повторы, backoff, circuit breaker → day5_results.json
python day6/crawler_day6.py     # сохранение в JSON/CSV/SQLite
python day7/crawler_day7.py     # полный краулер (CLI, статистика, отчёт)
```

## CLI (день 7)

```bash
python day7/crawler_day7.py \
    --urls https://example.com \
    --max-pages 100 --max-depth 1 \
    --rate-limit 2.0 --respect-robots \
    --storage sqlite --storage-path out.db \
    --output report.json --log-file crawler.log
```

Основные флаги: `--urls`, `--max-pages`, `--max-depth`, `--rate-limit`,
`--respect-robots`, `--user-agent`, `--storage {json,csv,sqlite}`, `--storage-path`,
`--sitemap`, `--config`, `--output`, `--log-file`.

## Конфигурация (день 7)

Вместо флагов можно передать JSON-конфиг через `--config config.json`:

```json
{
  "urls": ["https://example.com"],
  "sitemap": null,
  "max_concurrent": 5,
  "max_depth": 1,
  "rate_limit": 2.0,
  "respect_robots": true,
  "min_delay": 0.3,
  "jitter": 0.2,
  "user_agent": "MyBot/1.0",
  "same_domain_only": false,
  "include_patterns": ["*example.com*"],
  "exclude_patterns": ["*.pdf", "*/login*"],
  "storage": { "type": "json", "path": "data.jsonl" }
}
```

`include_patterns`/`exclude_patterns`/`same_domain_only` пробрасываются в обход.

## Краткий обзор API

```python
from crawler_day7 import AdvancedCrawler

crawler = AdvancedCrawler.from_config("config.json")
await crawler.crawl(start_urls=[...], max_pages=100, same_domain_only=False)
# либо из sitemap:
await crawler.crawl_from_sitemap("https://site/sitemap.xml",
                                 max_pages=100, sitemap_limit=500)

stats = crawler.get_stats()          # total_pages, successful, failed, top_domains …
crawler.export_to_json("stats.json")
crawler.export_to_html_report("report.html")   # таблица + CSS-графики
await crawler.close()
```

Точки расширения (хуки дня 3, переопределяются наследниками):
- `async def on_page(self, url, result, depth)` — после успешной обработки страницы;
- `async def on_error(self, url, error)` — при неудаче.

Storage (день 6) реализует интерфейс `DataStorage` (`save`/`close`); готовые:
`JSONLinesStorage` (алиас `JSONStorage`), `CSVStorage`, `SQLiteStorage` (batch-вставки).

## Структура

```
my_work/
├── sample_urls.py        # общий список стартовых URL (день 1)
├── requirements.txt
├── day1/ … day7/         # слои краулера + результаты прогонов
├── УЛУЧШЕНИЯ.md (по дням) # ревью соответствия ТЗ и предложения
└── ОСТАЛОСЬ.md           # открытые задачи
```

## Прокси и cookies

Сессия создаётся с `trust_env=True`: прокси берётся из переменных `HTTP_PROXY`/`HTTPS_PROXY`,
креды — из `.netrc`. Cookies между запросами хранит встроенный `cookie_jar` сессии.

## Не реализовано (намеренно)

JavaScript-рендеринг (Playwright/Selenium) и распределённый краулинг — крупные опциональные
фичи из ТЗ дня 7, в учебный проект не включены. Также нет автоматических тестов и perf-тестов.
Полный список открытых пунктов — в `ОСТАЛОСЬ.md`.
```
