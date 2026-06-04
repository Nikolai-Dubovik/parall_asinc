import argparse
import asyncio
import json
import logging
import sys
import time
import xml.etree.ElementTree as ET
from collections import Counter
from logging.handlers import RotatingFileHandler
from pathlib import Path
from urllib.parse import urlparse

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day6"))

from crawler_day6 import (
    CSVStorage,
    JSONStorage,
    SQLiteStorage,
    StorageCrawler,
)
from sample_urls import DAY1_URLS


log = logging.getLogger("crawler")

SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"


class SitemapParser:
    def __init__(self, session):
        self.session = session

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        urls: list[str] = []
        try:
            async with self.session.get(sitemap_url) as resp:
                if resp.status != 200:
                    log.warning("⚠️ sitemap %s статус %d", sitemap_url, resp.status)
                    return urls
                text = await resp.text()
        except Exception as e:
            log.warning("⚠️ не удалось загрузить sitemap %s: %s", sitemap_url, e)
            return urls

        try:
            root = ET.fromstring(text)
        except ET.ParseError as e:
            log.warning("⚠️ ошибка парсинга sitemap %s: %s", sitemap_url, e)
            return urls

        for sm in root.findall(f"{SITEMAP_NS}sitemap"):
            loc = sm.find(f"{SITEMAP_NS}loc")
            if loc is not None and loc.text:
                urls.extend(await self.fetch_sitemap(loc.text.strip()))

        for u in root.findall(f"{SITEMAP_NS}url"):
            loc = u.find(f"{SITEMAP_NS}loc")
            if loc is not None and loc.text:
                urls.append(loc.text.strip())

        return urls


class CrawlerStats:
    def __init__(self):
        self.started = time.monotonic()
        self.successful = 0
        self.failed = 0
        self.statuses: Counter = Counter()
        self.domains: Counter = Counter()

    def record_success(self, url: str, status) -> None:
        self.successful += 1
        self.statuses[str(status)] += 1
        self.domains[urlparse(url).netloc] += 1

    def record_failure(self, url: str, status) -> None:
        self.failed += 1
        self.statuses[str(status)] += 1

    def summary(self) -> dict:
        elapsed = time.monotonic() - self.started
        total = self.successful + self.failed
        speed = self.successful / elapsed if elapsed > 0 else 0.0
        return {
            "total_pages": total,
            "successful": self.successful,
            "failed": self.failed,
            "elapsed_sec": round(elapsed, 2),
            "speed_pps": round(speed, 2),
            "status_codes": dict(self.statuses),
            "top_domains": dict(self.domains.most_common(10)),
        }


def _build_storage(storage_type: str | None, path: str | None):
    if not storage_type:
        return None
    if storage_type == "json":
        return JSONStorage(path)
    if storage_type == "csv":
        return CSVStorage(path)
    if storage_type == "sqlite":
        return SQLiteStorage(path)
    raise ValueError(f"unknown storage: {storage_type}")


class AdvancedCrawler(StorageCrawler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.stats = CrawlerStats()
        self.sitemap = SitemapParser(self.session)
        self.config: dict = {}

    @classmethod
    def from_config(cls, path: str | Path) -> "AdvancedCrawler":
        with open(path, "r", encoding="utf-8") as f:
            cfg = json.load(f)
        storage_cfg = cfg.get("storage") or {}
        storage = _build_storage(storage_cfg.get("type"), storage_cfg.get("path"))
        crawler = cls(
            max_concurrent=cfg.get("max_concurrent", 5),
            max_depth=cfg.get("max_depth", 2),
            requests_per_second=cfg.get("rate_limit", 2.0),
            respect_robots=cfg.get("respect_robots", True),
            min_delay=cfg.get("min_delay", 0.0),
            jitter=cfg.get("jitter", 0.0),
            user_agent=cfg.get("user_agent", "MyBot/1.0"),
            storage=storage,
        )
        crawler.config = cfg
        return crawler

    async def _progress(self, start: float):
        # расширенный прогресс: ASCII прогресс-бар, процент и оценка оставшегося времени (ETA)
        try:
            while True:
                await asyncio.sleep(2.0)
                stats = self.queue.get_stats()
                done = stats["processed"]
                elapsed = time.perf_counter() - start
                rate = done / elapsed if elapsed > 0 else 0.0
                frac = min(done / self.max_pages, 1.0) if self.max_pages else 0.0
                filled = int(frac * 20)
                bar = "#" * filled + "-" * (20 - filled)
                eta = max(self.max_pages - done, 0) / rate if rate > 0 else 0.0
                log.info("📊 [%s] %.0f%% %d/%d в_очереди=%d ошибок=%d %.2f стр/с ETA=%.0f c",
                         bar, frac * 100, done, self.max_pages, stats["in_queue"],
                         stats["failed"], rate, eta)
        except asyncio.CancelledError:
            return

    async def on_page(self, url: str, result: dict, depth: int) -> None:
        await super().on_page(url, result, depth)
        status = self.statuses.get(url)
        if isinstance(status, int) and 200 <= status < 300:
            self.stats.record_success(url, status)
        else:
            self.stats.record_failure(url, status)

    async def on_error(self, url: str, error: str) -> None:
        await super().on_error(url, error)
        self.stats.record_failure(url, self.statuses.get(url, error))

    async def crawl_from_sitemap(self, sitemap_url: str, max_pages: int = 100,
                                 sitemap_limit: int | None = None, **kwargs) -> dict:
        # sitemap_limit — сколько URL взять из sitemap (None = все);
        # max_pages — сколько всего обработать (разные понятия)
        urls = await self.sitemap.fetch_sitemap(sitemap_url)
        log.info("🗺️ из sitemap получено %d URL", len(urls))
        if sitemap_limit is not None:
            urls = urls[:sitemap_limit]
        if not urls:
            return {}
        return await self.crawl(start_urls=urls, max_pages=max_pages, **kwargs)

    def get_stats(self) -> dict:
        return self.stats.summary()

    def export_to_json(self, filename: str) -> None:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(self.get_stats(), f, ensure_ascii=False, indent=2)
        log.info("💾 статистика сохранена в %s", filename)

    @staticmethod
    def _bar_chart(title: str, counts: dict) -> str:
        """Простой горизонтальный bar-chart на CSS (без внешних зависимостей)."""
        if not counts:
            return ""
        mx = max(counts.values()) or 1
        bars = ""
        for label, n in counts.items():
            width = n / mx * 100
            bars += (
                f"<div class='row'><span class='lbl'>{label}</span>"
                f"<span class='bar' style='width:{width:.0f}%'></span>"
                f"<span class='val'>{n}</span></div>"
            )
        return f"<h2>{title}</h2><div class='chart'>{bars}</div>"

    def export_to_html_report(self, filename: str) -> None:
        s = self.get_stats()
        scalar = {k: v for k, v in s.items() if not isinstance(v, (dict, list))}
        table = "".join(f"<tr><td>{k}</td><td>{v}</td></tr>" for k, v in scalar.items())
        charts = (
            self._bar_chart("Статус-коды", s.get("status_codes", {}))
            + self._bar_chart("Топ доменов", s.get("top_domains", {}))
        )
        css = (
            "body{font-family:sans-serif;margin:2rem;}"
            "table{border-collapse:collapse;margin-bottom:1rem;}"
            "td{border:1px solid #ccc;padding:4px 8px;}"
            ".chart{max-width:640px;margin-bottom:1rem;}"
            ".row{display:flex;align-items:center;margin:3px 0;}"
            ".lbl{width:180px;font-size:13px;}.val{margin-left:6px;font-size:13px;}"
            ".bar{height:14px;min-width:2px;background:#4c8bf5;border-radius:3px;}"
        )
        html = (
            "<!doctype html>\n<html><head><meta charset='utf-8'>"
            f"<title>Crawler report</title><style>{css}</style></head><body>"
            "<h1>Crawler Report</h1>"
            f"<h2>Сводка</h2><table><tbody>{table}</tbody></table>"
            f"{charts}"
            "</body></html>\n"
        )
        with open(filename, "w", encoding="utf-8") as f:
            f.write(html)
        log.info("📄 HTML отчёт сохранён в %s", filename)


def setup_logging(level: int = logging.INFO, log_file: str | None = None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler()]
    if log_file:
        handlers.append(
            RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3,
                                encoding="utf-8")
        )
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        handlers=handlers,
        force=True,
    )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Advanced async crawler")
    p.add_argument("--urls", nargs="+", help="стартовые URL")
    p.add_argument("--max-pages", type=int, default=20)
    p.add_argument("--max-depth", type=int, default=0)
    p.add_argument("--output", default="day7_results.json")
    p.add_argument("--config")
    p.add_argument("--respect-robots", action="store_true")
    p.add_argument("--rate-limit", type=float, default=2.0)
    p.add_argument("--user-agent", default="MyBot/1.0")
    p.add_argument("--storage", choices=["json", "csv", "sqlite"], default="json")
    p.add_argument("--storage-path", default="day7_data.jsonl")
    p.add_argument("--sitemap")
    p.add_argument("--log-file")
    return p.parse_args(argv)


async def run(args: argparse.Namespace) -> None:
    setup_logging(log_file=args.log_file)

    if args.config:
        crawler = AdvancedCrawler.from_config(args.config)
        urls = crawler.config.get("urls", [])
        sitemap_url = crawler.config.get("sitemap") or args.sitemap
    else:
        storage = _build_storage(args.storage, args.storage_path)
        crawler = AdvancedCrawler(
            max_concurrent=5,
            max_depth=args.max_depth,
            requests_per_second=args.rate_limit,
            respect_robots=args.respect_robots,
            user_agent=args.user_agent,
            storage=storage,
        )
        urls = args.urls or DAY1_URLS
        sitemap_url = args.sitemap

    # фильтры берём из конфига (для CLI-режима config пуст → значения по умолчанию)
    cfg = crawler.config
    crawl_opts = {
        "same_domain_only": cfg.get("same_domain_only", False),
        "include_patterns": cfg.get("include_patterns"),
        "exclude_patterns": cfg.get("exclude_patterns"),
    }

    try:
        if sitemap_url:
            await crawler.crawl_from_sitemap(sitemap_url, max_pages=args.max_pages,
                                             **crawl_opts)
        else:
            await crawler.crawl(start_urls=urls, max_pages=args.max_pages,
                                **crawl_opts)

        stats = crawler.get_stats()
        print(f"Обработано: {stats['total_pages']} страниц")
        print(f"Успешно: {stats['successful']}")
        print(f"Ошибок: {stats['failed']}")
        print(f"Скорость: {stats['speed_pps']} стр/с за {stats['elapsed_sec']} c")

        out_path = Path(args.output)
        crawler.export_to_json(str(out_path))
        crawler.export_to_html_report(str(out_path.with_suffix(".html")))
    finally:
        await crawler.close()


def main() -> None:
    args = parse_args()
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
