import asyncio
import json
import logging
import sys
from pathlib import Path
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "day1"))

from crawler_day1 import AsyncCrawler


log = logging.getLogger("crawler")


class HTMLParser:
    async def parse_html(self, html: str, url: str) -> dict:
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as e:
            log.warning("⚠️ ошибка парсинга %s: %s", url, e)
            return {
                "url": url,
                "title": "",
                "text": "",
                "links": [],
                "metadata": {},
                "images": [],
                "headings": {"h1": [], "h2": [], "h3": []},
                "tables": [],
                "lists": [],
            }

        metadata = self.extract_metadata(soup)
        return {
            "url": url,
            "title": metadata.get("title", ""),
            "text": self.extract_text(soup),
            "links": self.extract_links(soup, url),
            "metadata": metadata,
            "images": self.extract_images(soup, url),
            "headings": self.extract_headings(soup),
            "tables": self.extract_tables(soup),
            "lists": self.extract_lists(soup),
        }

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
        if target is None:
            return ""
        return target.get_text(separator=" ", strip=True)

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
        images = []
        for img in soup.find_all("img"):
            src = img.get("src")
            if not src:
                continue
            images.append({
                "src": urljoin(base_url, src.strip()),
                "alt": (img.get("alt") or "").strip(),
            })
        return images

    def extract_headings(self, soup: BeautifulSoup) -> dict:
        return {
            level: [h.get_text(strip=True) for h in soup.find_all(level)]
            for level in ("h1", "h2", "h3")
        }

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = []
        for table in soup.find_all("table"):
            rows = []
            for tr in table.find_all("tr"):
                cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
                if cells:
                    rows.append(cells)
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


class ParsingCrawler(AsyncCrawler):
    def __init__(self, max_concurrent: int = 10):
        super().__init__(max_concurrent=max_concurrent)
        self.parser = HTMLParser()

    async def fetch_and_parse(self, url: str) -> dict:
        html = await self.fetch_url(url)
        if not html:
            return {"url": url, "title": "", "text": "", "links": [], "metadata": {}}
        parsed = await self.parser.parse_html(html, url)
        return {
            "url": parsed["url"],
            "title": parsed["title"],
            "text": parsed["text"],
            "links": parsed["links"],
            "metadata": parsed["metadata"],
            "images": parsed["images"],
            "headings": parsed["headings"],
        }


async def main():
    crawler = ParsingCrawler(max_concurrent=5)
    urls = [
        "https://example.com",
        "https://onliner.by",
        "https://pikabu.ru",
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
            "images_count": len(r.get("images", [])),
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
