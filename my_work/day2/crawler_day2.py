import asyncio
import json
import logging
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from sample_urls import DAY1_URLS

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger("crawler")


class AsyncCrawler:
    def __init__(self, max_concurrent: int = 10):
        timeout = aiohttp.ClientTimeout(connect=5, sock_read=10, total=30)
        connector = aiohttp.TCPConnector(limit=max_concurrent, ssl=False)
        # trust_env=True — читать прокси из HTTP(S)_PROXY и креды из .netrc;
        # cookies между запросами хранит встроенный cookie_jar сессии
        self.session = aiohttp.ClientSession(
            timeout=timeout, connector=connector, trust_env=True
        )
        self.parser = HTMLParser()

    async def fetch_url(self, url: str) -> str:
        log.info("▶️ начало загрузки %s", url)
        try:
            async with self.session.get(url) as resp:
                resp.raise_for_status()
                text = await resp.text()
                log.info("✅ успешное завершение %s", url)
                return text
        except asyncio.TimeoutError as e:
            log.warning("⚠️ ошибка %s: %s", type(e).__name__, url)
        except aiohttp.ClientResponseError as e:
            log.warning("⚠️ ошибка %s (HTTP %s): %s", type(e).__name__, e.status, url)
        except aiohttp.ClientError as e:
            log.warning("⚠️ ошибка %s: %s", type(e).__name__, url)
        return ""

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        return await self.parser.parse_html(html, url)

    async def close(self):
        await self.session.close()


class HTMLParser:
    async def parse_html(self, html: str, url: str) -> dict:
        # BeautifulSoup — CPU-bound и блокирует event loop, поэтому выносим в пул потоков
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._parse_sync, html, url)

    def _parse_sync(self, html: str, url: str) -> dict:
        # пустой каркас результата — возвращается целиком или частично при ошибках
        result = {
            "url": url, "title": "", "text": "", "links": [],
            "metadata": {}, "images": [], "headings": {},
            "tables": [], "lists": [],
        }
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as e:
            log.warning("⚠️ не удалось разобрать HTML %s: %s", url, e)
            return result

        def safe(name, fn):
            """Вызвать извлекатель; при ошибке залогировать и вернуть значение по умолчанию."""
            try:
                return fn()
            except Exception as exc:
                log.warning("⚠️ ошибка извлечения %s для %s: %s", name, url, exc)
                return result[name]

        metadata = safe("metadata", lambda: self.extract_metadata(soup))
        result["metadata"] = metadata
        result["title"] = metadata.get("title", "")
        result["text"] = safe("text", lambda: self.extract_text(soup))
        result["links"] = safe("links", lambda: self.extract_links(soup, url))
        result["images"] = safe("images", lambda: self.extract_images(soup, url))
        result["headings"] = safe("headings", lambda: self.extract_headings(soup))
        result["tables"] = safe("tables", lambda: self.extract_tables(soup))
        result["lists"] = safe("lists", lambda: self.extract_lists(soup))
        return result

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links = []
        for a in soup.find_all("a", href=True):
            absolute = urljoin(base_url, a["href"].strip())
            parsed = urlparse(absolute)
            if parsed.scheme in ("http", "https") and parsed.netloc:
                links.append(absolute)
        return list(dict.fromkeys(links))

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        target = soup.select_one(selector) if selector else soup
        return target.get_text(separator=" ", strip=True) if target else ""

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        meta = {"title": "", "description": "", "keywords": ""}
        if soup.title and soup.title.string:
            meta["title"] = soup.title.string.strip()
        for tag in soup.find_all("meta"):
            name = (tag.get("name") or "").lower()
            if name in ("description", "keywords"):
                meta[name] = (tag.get("content") or "").strip()
        return meta

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        return [
            {"src": urljoin(base_url, img["src"].strip()),
             "alt": (img.get("alt") or "").strip()}
            for img in soup.find_all("img") if img.get("src")
        ]

    def extract_headings(self, soup: BeautifulSoup) -> dict:
        return {
            level: [h.get_text(strip=True) for h in soup.find_all(level)]
            for level in ("h1", "h2", "h3")
        }

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = []
        for table in soup.find_all("table"):
            rows = [
                [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
                for tr in table.find_all("tr")
            ]
            rows = [r for r in rows if r]
            if rows:
                tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[list[str]]:
        lists = []
        for lst in soup.find_all(["ul", "ol"]):
            items = [li.get_text(strip=True) for li in lst.find_all("li", recursive=False)]
            if items:
                lists.append(items)
        return lists


async def main():
    crawler = AsyncCrawler(max_concurrent=5)
    urls = [
        "https://onliner.by",
        "https://pikabu.ru",
        "https://edvibssse.com/",
        "https://nebdeti.ru/",
        "https://mangabuff.ru/"
    ]

    results = await asyncio.gather(*(crawler.fetch_and_parse(u) for u in urls))

    summary = []
    for r in results:
        stats = {
            "url": r["url"],
            "title": r["title"],
            "text_length": len(r["text"]),
            "links_count": len(r["links"]),
            "links": r["links"][:5],
            "images_count": len(r["images"]),
        }
        summary.append(stats)
        log.info("📊 %s — текст %d симв., ссылок %d, картинок %d",
                 r["url"], stats["text_length"], stats["links_count"], stats["images_count"])

    out_path = Path(__file__).resolve().parent / "day2_results.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False, indent=2)
    log.info("💾 сохранено в %s", out_path)

    await crawler.close()


if __name__ == "__main__":
    asyncio.run(main())
