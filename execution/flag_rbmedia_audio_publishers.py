#!/usr/bin/env python3
"""
RB Media audiobook publisher flagger.

This script verifies whether a series appears to be represented by an RBmedia
audio imprint by searching either the RBmedia catalog or Amazon audiobook
product pages for title-level evidence. It outputs a CSV that can be written
back into a Google Sheet.

Primary source:
- RBmedia catalog search
- Amazon audiobook product pages

Secondary support:
- Optional local RB Media shortlist/catalog snapshot CSV

Tiebreaker:
- Gemini, only for ambiguous title matching when enabled
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple
from urllib.parse import quote_plus, urlencode
from urllib.request import Request, urlopen

import pandas as pd
from loguru import logger
from playwright.async_api import Browser, BrowserContext, Page, async_playwright

try:
    import google.generativeai as genai
except Exception:  # pragma: no cover - optional dependency
    genai = None


PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_INPUT = PROJECT_ROOT / "outreach" / "sports-romance" / "exports" / "Sports_Romance_Combined_Master.csv"
DEFAULT_OUTPUT = PROJECT_ROOT / ".tmp" / "rbmedia_audit" / "rbmedia_results.csv"
DEFAULT_CACHE_DIR = PROJECT_ROOT / ".tmp" / "rbmedia_audit"
DEFAULT_SUPPORT_CSV = PROJECT_ROOT / "_archive" / "RB Media Shortlist - US D_R - RB Media Final shortlist.csv"
RBMEDIA_SEARCH_BASE = "https://rbmediaglobal.com/audiobooks/search/"
RBMEDIA_SEARCH_API = (
    "https://rbmediaglobal.com/wp-content/themes/visual-composer-starter-child-master/api/audiobook_search.php"
)


RBMEDIA_IMPRINT_ALIASES: Dict[str, Sequence[str]] = {
    "Recorded Books": ("recorded books",),
    "Tantor Media": ("tantor", "tantor media", "tantor audio"),
    "Dreamscape": ("dreamscape", "dreamscape media"),
    "HighBridge": ("highbridge", "highbridge audio"),
    "Christian Audio": ("christian audio", "christianaudio"),
    "Ascent Audio": ("ascent audio",),
    "GraphicAudio": ("graphicaudio", "graphic audio"),
    "Kalorama Audio": ("kalorama audio",),
    "W. F. Howes": ("w. f. howes", "wf howes", "w f howes"),
    "Wavesound": ("wavesound",),
    "BookaVivo": ("bookavivo",),
    "Editions Theleme": ("editions theleme", "editions thélème", "thélème"),
    "RBmedia Verlag": ("rbmedia verlag",),
}


AMAZON_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
)


@dataclass
class ProductCandidate:
    query_title: str
    product_title: str
    product_author: str
    publisher_raw: str
    product_url: str
    score: int
    confidence: str
    source: str = "amazon_audiobook"
    notes: str = ""


def normalize_text(value: Any) -> str:
    text = str(value or "").lower()
    text = text.replace("&", " and ")
    text = re.sub(r"\bseries\b", " ", text)
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def normalize_title(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"\([^)]*\)", " ", text)
    text = re.sub(r"\bbook\s+\d+\b", " ", text, flags=re.I)
    return normalize_text(text)


def normalize_publisher(value: Any) -> str:
    return normalize_text(value)


def get_rbmedia_imprint(publisher: str) -> Optional[str]:
    norm = normalize_publisher(publisher)
    if not norm:
        return None
    for imprint, aliases in RBMEDIA_IMPRINT_ALIASES.items():
        if any(alias in norm for alias in aliases):
            return imprint
    return None


def find_imprint_in_text(value: Any) -> Optional[str]:
    return get_rbmedia_imprint(str(value or ""))


def extract_featured_titles(value: Any) -> List[str]:
    text = str(value or "").strip()
    if not text or text.lower() == "nan":
        return []
    titles: List[str] = []
    for part in text.split("|"):
        cleaned = re.sub(r"\([^)]*\)", "", part).strip()
        if cleaned and cleaned not in titles:
            titles.append(cleaned)
    return titles


def unique_preserve(items: Iterable[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for item in items:
        norm = normalize_title(item)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(item.strip())
    return out


def series_key(series_name: str, author_name: str) -> str:
    return f"{normalize_title(series_name)}::{normalize_text(author_name)}"


def get_series_name(row: pd.Series) -> str:
    return str(row.get("Show Name") or row.get("Series Name") or "").strip()


def extract_byline_parts(byline: str) -> Tuple[str, str]:
    author = ""
    publisher = ""
    for match in re.finditer(r"([^()]+?)\s*\(([^)]+)\)", byline):
        value = match.group(1).strip(" ,")
        role = match.group(2).strip().lower()
        if role == "author" and not author:
            author = value
        elif role == "publisher" and not publisher:
            publisher = value
    return author, publisher


def parse_publisher_from_page_title(page_title: str) -> Tuple[str, str]:
    author = ""
    publisher = ""
    match = re.search(r"\(Audible Audio Edition\):\s*(.+?)\s*:\s*Books$", page_title)
    if not match:
        return author, publisher

    credits = [part.strip() for part in match.group(1).split(",") if part.strip()]
    if not credits:
        return author, publisher

    author = credits[0]
    suffixes = {"inc.", "inc", "llc", "ltd.", "ltd", "co.", "co", "corp.", "corp"}
    if len(credits) >= 2 and credits[-1].lower() in suffixes:
        publisher = f"{credits[-2]}, {credits[-1]}"
    else:
        publisher = credits[-1]
    return author, publisher


def score_match(
    query_title: str,
    product_title: str,
    expected_author: str,
    product_author: str,
    series_name: str,
) -> Tuple[int, str, str]:
    q = normalize_title(query_title)
    p = normalize_title(product_title)
    a_expected = normalize_text(expected_author)
    a_product = normalize_text(product_author)
    s = normalize_title(series_name)

    score = 0
    notes: List[str] = []
    if q and p == q:
        score += 60
        notes.append("exact_title")
    elif q and q in p:
        score += 45
        notes.append("title_contains")
    elif s and p.startswith(s):
        score += 25
        notes.append("series_prefix")

    if a_expected and a_product and a_expected == a_product:
        score += 40
        notes.append("exact_author")
    elif a_expected and a_product and (a_expected in a_product or a_product in a_expected):
        score += 25
        notes.append("partial_author")
    elif a_expected and a_product:
        score -= 25
        notes.append("author_mismatch")

    confidence = "low"
    if score >= 95:
        confidence = "high"
    elif score >= 70:
        confidence = "medium"
    return score, confidence, ",".join(notes)


class GeminiDisambiguator:
    def __init__(self) -> None:
        self.enabled = False
        self.model = None
        api_key = os.getenv("GEMINI_API_KEY")
        if genai is None or not api_key:
            return
        try:
            genai.configure(api_key=api_key)
            self.model = genai.GenerativeModel("gemini-2.5-flash")
            self.enabled = True
        except Exception as exc:  # pragma: no cover - environment-specific
            logger.warning(f"Gemini unavailable: {exc}")

    async def choose(self, series_name: str, author_name: str, candidates: Sequence[ProductCandidate]) -> Optional[int]:
        if not self.enabled or not candidates:
            return None

        prompt_lines = [
            "Choose the best audiobook product candidate for this series.",
            f"Series: {series_name}",
            f"Author: {author_name}",
            "Return JSON only like {\"index\": 0} or {\"index\": null}.",
        ]
        for idx, candidate in enumerate(candidates):
            prompt_lines.append(
                f"{idx}. title={candidate.product_title} | author={candidate.product_author} | "
                f"publisher={candidate.publisher_raw} | url={candidate.product_url}"
            )
        prompt = "\n".join(prompt_lines)

        try:
            response = await asyncio.to_thread(self.model.generate_content, prompt)
            text = (response.text or "").strip()
            if "```json" in text:
                text = text.split("```json", 1)[1].split("```", 1)[0].strip()
            elif text.startswith("```"):
                text = text.split("```", 2)[1].strip()
            parsed = json.loads(text)
            index = parsed.get("index")
            if isinstance(index, int) and 0 <= index < len(candidates):
                return index
        except Exception as exc:  # pragma: no cover - environment-specific
            logger.warning(f"Gemini disambiguation failed for {series_name}: {exc}")
        return None


class AmazonAudiobookVerifier:
    def __init__(self, browser: Browser, cache_dir: Path, gemini: GeminiDisambiguator) -> None:
        self.browser = browser
        self.cache_dir = cache_dir
        self.gemini = gemini
        self.cache_lock = asyncio.Lock()
        self.search_cache_path = cache_dir / "search_cache.json"
        self.product_cache_path = cache_dir / "product_cache.json"
        self.rbmedia_search_cache_path = cache_dir / "rbmedia_search_cache.json"
        self.search_cache = self._load_json(self.search_cache_path)
        self.product_cache = self._load_json(self.product_cache_path)
        self.rbmedia_search_cache = self._load_json(self.rbmedia_search_cache_path)

    @staticmethod
    def _load_json(path: Path) -> Dict[str, Any]:
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text())
        except Exception:
            return {}

    def _save_json(self, path: Path, payload: Dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True))

    @staticmethod
    def _publisher_cache_is_suspicious(publisher: str) -> bool:
        cleaned = normalize_publisher(publisher)
        return cleaned in {"inc", "inc.", "llc", "ltd", "ltd.", "corp", "corp.", "co", "co."}

    async def new_context(self) -> BrowserContext:
        return await self.browser.new_context(user_agent=AMAZON_USER_AGENT, viewport={"width": 1440, "height": 1200})

    async def search_amazon_audiobooks(self, page: Page, query: str) -> List[Dict[str, str]]:
        cache_key = query
        if cache_key in self.search_cache:
            return self.search_cache[cache_key]

        search_url = f"https://www.amazon.com/s?k={quote_plus(query)}"
        await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(2500)

        js = """
        () => {
          const out = [];
          const seen = new Set();
          for (const a of document.querySelectorAll('a[href*="/audiobook/dp/"], a[href*="-audiobook/dp/"]')) {
            const href = a.href;
            if (!href || seen.has(href)) continue;
            const txt = (a.textContent || '').trim();
            out.push({href, title: txt});
            seen.add(href);
            if (out.length >= 12) break;
          }
          return out;
        }
        """
        results = await page.evaluate(js)
        async with self.cache_lock:
            self.search_cache[cache_key] = results
            self._save_json(self.search_cache_path, self.search_cache)
        return results

    async def search_rbmedia_catalog(self, page: Page, query_title: str, author_name: str) -> List[Dict[str, str]]:
        cache_key = f"api_v1::{query_title}::{author_name}"
        if cache_key in self.rbmedia_search_cache:
            return self.rbmedia_search_cache[cache_key]

        payload = {
            "search": "true",
            "pageNumber": 0,
            "pageSize": 15,
            "sort": "+title",
            "view": "grid",
            "audiobookagenice": "",
            "audiobooklanguagenice": "",
            "audiobookquery": query_title,
            "audiobookauthor": author_name,
            "audiobooknarrator": "",
            "audiobookgenre": "",
            "audiobookreleasestart": "",
            "audiobookreleaseend": str(date.today()),
            "AgeGroup": "All Age Groups",
            "Language": "All Languages",
            "audiobookseries": "",
            "audiobookbrand": "",
            "audiobookisbn": "",
        }

        def _post_search() -> List[Dict[str, Any]]:
            request = Request(
                RBMEDIA_SEARCH_API,
                data=urlencode(payload).encode("utf-8"),
                headers={
                    "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
                    "User-Agent": AMAZON_USER_AGENT,
                },
            )
            with urlopen(request, timeout=45) as response:
                raw = response.read().decode("utf-8")
            parsed = json.loads(raw)
            if isinstance(parsed, dict):
                return parsed.get("data", []) or []
            if isinstance(parsed, list):
                return parsed
            return []

        results = await asyncio.to_thread(_post_search)
        async with self.cache_lock:
            self.rbmedia_search_cache[cache_key] = results
            self._save_json(self.rbmedia_search_cache_path, self.rbmedia_search_cache)
        return results

    async def fetch_product_metadata(self, page: Page, product_url: str) -> Dict[str, str]:
        cached = self.product_cache.get(product_url)
        if cached and not self._publisher_cache_is_suspicious(str(cached.get("publisher", ""))):
            return cached

        await page.goto(product_url, wait_until="domcontentloaded", timeout=45000)
        await page.wait_for_timeout(2200)

        title = ""
        byline = ""
        try:
            title = (await page.locator("#productTitle").inner_text()).strip()
        except Exception:
            pass
        try:
            byline = (await page.locator("#bylineInfo").inner_text()).strip()
        except Exception:
            pass

        author, publisher = extract_byline_parts(byline)
        if not publisher:
            page_title = await page.title()
            title_author, title_publisher = parse_publisher_from_page_title(page_title)
            author = author or title_author
            publisher = publisher or title_publisher

        metadata = {
            "title": title,
            "author": author,
            "publisher": publisher,
            "byline": byline,
            "url": product_url,
        }
        async with self.cache_lock:
            self.product_cache[product_url] = metadata
            self._save_json(self.product_cache_path, self.product_cache)
        return metadata

    async def verify_row(
        self,
        page: Page,
        series_name: str,
        author_name: str,
        title_candidates: Sequence[str],
    ) -> Optional[ProductCandidate]:
        all_candidates: List[ProductCandidate] = []

        for query_title in title_candidates:
            query = f"{query_title} {author_name} audiobook"
            try:
                search_results = await self.search_amazon_audiobooks(page, query)
            except Exception as exc:
                logger.warning(f"Amazon search failed for {series_name} / {query_title}: {exc}")
                continue

            for result in search_results[:5]:
                try:
                    metadata = await self.fetch_product_metadata(page, result["href"])
                except Exception as exc:
                    logger.warning(f"Product fetch failed for {result.get('href')}: {exc}")
                    continue

                score, confidence, notes = score_match(
                    query_title=query_title,
                    product_title=metadata.get("title", "") or result.get("title", ""),
                    expected_author=author_name,
                    product_author=metadata.get("author", ""),
                    series_name=series_name,
                )

                if score < 55:
                    continue

                all_candidates.append(
                    ProductCandidate(
                        query_title=query_title,
                        product_title=metadata.get("title", "") or result.get("title", ""),
                        product_author=metadata.get("author", ""),
                        publisher_raw=metadata.get("publisher", ""),
                        product_url=metadata.get("url", result["href"]),
                        score=score,
                        confidence=confidence,
                        notes=notes,
                    )
                )

                if score >= 100 and metadata.get("publisher"):
                    return all_candidates[-1]

        if not all_candidates:
            return None

        all_candidates.sort(key=lambda c: (c.score, bool(c.publisher_raw)), reverse=True)
        if len(all_candidates) == 1:
            return all_candidates[0]

        top = all_candidates[:3]
        if top[0].score >= 90 and top[0].publisher_raw:
            return top[0]

        gemini_choice = await self.gemini.choose(series_name, author_name, top)
        if gemini_choice is not None:
            chosen = top[gemini_choice]
            chosen.source = "gemini_disambiguation"
            return chosen

        return top[0]

    async def verify_rbmedia_row(
        self,
        page: Page,
        series_name: str,
        author_name: str,
        title_candidates: Sequence[str],
    ) -> Optional[ProductCandidate]:
        all_candidates: List[ProductCandidate] = []

        for query_title in title_candidates:
            try:
                search_results = await self.search_rbmedia_catalog(page, query_title, author_name)
            except Exception as exc:
                logger.warning(f"RBmedia search failed for {series_name} / {query_title}: {exc}")
                continue

            for result in search_results[:5]:
                combined_text = " ".join(
                    str(part or "")
                    for part in [
                        result.get("title", ""),
                        result.get("author_names", ""),
                        result.get("narrator_names", ""),
                        result.get("company", ""),
                        result.get("title_imprint", ""),
                    ]
                    if part
                ).strip()
                if not combined_text:
                    continue

                product_author = str(result.get("author_names", "") or "")
                product_title = str(result.get("title", "") or query_title)
                imprint = (
                    find_imprint_in_text(result.get("title_imprint", ""))
                    or find_imprint_in_text(result.get("company", ""))
                    or "RBmedia Catalog"
                )

                score, confidence, notes = score_match(
                    query_title=query_title,
                    product_title=product_title,
                    expected_author=author_name,
                    product_author=product_author,
                    series_name=series_name,
                )
                if score < 55:
                    continue

                all_candidates.append(
                    ProductCandidate(
                        query_title=query_title,
                        product_title=product_title,
                        product_author=product_author,
                        publisher_raw=imprint,
                        product_url=f"https://rbmediaglobal.com/audiobook/{result.get('isbn', '')}/",
                        score=score,
                        confidence=confidence,
                        source="rbmedia_catalog_search",
                        notes=f"{notes},rbmedia_catalog".strip(","),
                    )
                )

                if score >= 100:
                    return all_candidates[-1]

        if not all_candidates:
            return None

        all_candidates.sort(key=lambda c: (c.score, bool(c.product_author)), reverse=True)
        if len(all_candidates) == 1:
            return all_candidates[0]

        top = all_candidates[:3]
        if top[0].score >= 90:
            return top[0]

        gemini_choice = await self.gemini.choose(series_name, author_name, top)
        if gemini_choice is not None:
            chosen = top[gemini_choice]
            chosen.source = "gemini_disambiguation"
            return chosen

        return top[0]


def build_support_lookup(path: Optional[Path]) -> Dict[str, Dict[str, str]]:
    lookup: Dict[str, Dict[str, str]] = {}
    if not path or not path.exists():
        return lookup

    df = pd.read_csv(path)
    series_col = "Series Name" if "Series Name" in df.columns else None
    author_col = "Author Name" if "Author Name" in df.columns else None
    publisher_col = "Publisher" if "Publisher" in df.columns else None
    if not series_col:
        return lookup

    for _, row in df.iterrows():
        key = series_key(row.get(series_col, ""), row.get(author_col, ""))
        if not key:
            continue
        lookup[key] = {
            "publisher": str(row.get(publisher_col, "") or ""),
            "series_name": str(row.get(series_col, "") or ""),
            "author_name": str(row.get(author_col, "") or ""),
        }
    return lookup


def classify_result(
    series_name: str,
    author_name: str,
    product: Optional[ProductCandidate],
    support_lookup: Dict[str, Dict[str, str]],
) -> Dict[str, Any]:
    checked_on = str(date.today())
    key = series_key(series_name, author_name)
    support = support_lookup.get(key)

    result = {
        "RB Media Flag": "",
        "RB Media Imprint": "",
        "RB Media Evidence Source": "",
        "RB Media Evidence URL": "",
        "RB Media Confidence": "",
        "RB Media Review Status": "needs_review",
        "RB Media Checked On": checked_on,
        "Matched Product Title": "",
        "Matched Product Author": "",
        "Matched Publisher Raw": "",
        "RB Media Notes": "",
    }

    if product:
        imprint = get_rbmedia_imprint(product.publisher_raw)
        result.update(
            {
                "RB Media Evidence Source": product.source,
                "RB Media Evidence URL": product.product_url,
                "RB Media Confidence": product.confidence,
                "Matched Product Title": product.product_title,
                "Matched Product Author": product.product_author,
                "Matched Publisher Raw": product.publisher_raw,
                "RB Media Notes": product.notes,
            }
        )
        if product.source == "rbmedia_catalog_search" and product.confidence != "low":
            result["RB Media Flag"] = "yes"
            result["RB Media Imprint"] = imprint or product.publisher_raw
            result["RB Media Review Status"] = "confirmed_yes"
        elif imprint and product.confidence != "low":
            result["RB Media Flag"] = "yes"
            result["RB Media Imprint"] = imprint
            result["RB Media Review Status"] = "confirmed_yes"
        elif imprint:
            result["RB Media Imprint"] = imprint
            result["RB Media Review Status"] = "needs_review"
        elif product.publisher_raw and product.confidence != "low":
            result["RB Media Flag"] = "no"
            result["RB Media Review Status"] = "confirmed_no"
        elif product.publisher_raw:
            result["RB Media Review Status"] = "needs_review"
        return result

    if support:
        imprint = get_rbmedia_imprint(support.get("publisher", "")) or support.get("publisher", "")
        result.update(
            {
                "RB Media Imprint": imprint,
                "RB Media Evidence Source": "rbmedia_catalog",
                "RB Media Confidence": "low",
                "RB Media Review Status": "needs_review",
                "RB Media Notes": "catalog_support_only",
            }
        )
    return result


async def process_rows(
    df: pd.DataFrame,
    output_csv: Path,
    support_lookup: Dict[str, Dict[str, str]],
    sample: Optional[int],
    concurrency: int,
    mode: str,
) -> pd.DataFrame:
    rows = df.copy()
    if sample:
        rows = rows.head(sample).copy()

    gemini = GeminiDisambiguator()
    results: List[Tuple[int, Dict[str, Any]]] = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        verifier = AmazonAudiobookVerifier(browser, DEFAULT_CACHE_DIR, gemini)
        total = len(rows)
        queue: asyncio.Queue[Tuple[int, Dict[str, Any]]] = asyncio.Queue()
        write_lock = asyncio.Lock()

        try:
            for position, (_, row) in enumerate(rows.iterrows(), start=1):
                queue.put_nowait((position, row.to_dict()))

            async def worker(worker_id: int) -> None:
                context = await verifier.new_context()
                page = await context.new_page()
                try:
                    while True:
                        try:
                            position, row_dict = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break

                        row = pd.Series(row_dict)
                        series_name = get_series_name(row)
                        author_name = str(row.get("Author Name", "") or "")
                        title_candidates = unique_preserve(
                            [
                                str(row.get("First Book Name", "") or ""),
                                *extract_featured_titles(row.get("Featured Books", "")),
                                series_name,
                            ]
                        )

                        logger.info(f"[{position}/{total}] Checking {series_name} by {author_name}")
                        if mode == "rbmedia":
                            product = await verifier.verify_rbmedia_row(page, series_name, author_name, title_candidates)
                        else:
                            product = await verifier.verify_row(page, series_name, author_name, title_candidates)
                        classified = classify_result(series_name, author_name, product, support_lookup)

                        payload = {
                            "Sheet Row Number": row.get("Sheet Row Number", ""),
                            "Show Name": series_name,
                            "Author Name": author_name,
                            "First Book Name": str(row.get("First Book Name", "") or ""),
                            "Title Candidates": " | ".join(title_candidates),
                            **classified,
                        }

                        async with write_lock:
                            results.append((position, payload))
                            if len(results) % 10 == 0 or len(results) == total:
                                pd.DataFrame(
                                    [item[1] for item in sorted(results, key=lambda item: item[0])]
                                ).to_csv(output_csv, index=False)
                        queue.task_done()
                finally:
                    await context.close()

            workers = [asyncio.create_task(worker(worker_id)) for worker_id in range(max(1, concurrency))]
            await asyncio.gather(*workers)
        finally:
            await browser.close()

    final_df = pd.DataFrame([item[1] for item in sorted(results, key=lambda item: item[0])])
    final_df.to_csv(output_csv, index=False)
    return final_df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flag RB Media audiobook publishers for sports romance titles.")
    parser.add_argument("--input-csv", default=str(DEFAULT_INPUT))
    parser.add_argument("--output-csv", default=str(DEFAULT_OUTPUT))
    parser.add_argument("--support-csv", default=str(DEFAULT_SUPPORT_CSV))
    parser.add_argument("--sample", type=int, default=None, help="Limit processing to the first N rows for a dry run.")
    parser.add_argument("--concurrency", type=int, default=4, help="Number of parallel browser workers.")
    parser.add_argument("--mode", choices=["amazon", "rbmedia"], default="amazon")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    input_csv = Path(args.input_csv)
    output_csv = Path(args.output_csv)
    support_csv = Path(args.support_csv) if args.support_csv else None

    if not input_csv.exists():
        raise FileNotFoundError(f"Input CSV not found: {input_csv}")

    DEFAULT_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    output_csv.parent.mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(input_csv)
    support_lookup = build_support_lookup(support_csv)
    result_df = asyncio.run(process_rows(df, output_csv, support_lookup, args.sample, args.concurrency, args.mode))
    logger.success(f"Wrote {len(result_df)} rows to {output_csv}")


if __name__ == "__main__":
    main()
