#!/usr/bin/env python3
"""
Re-enrichment for Failed Goodreads Extractions
================================================
Targets entries that have a Goodreads link but 0/null rating.
Uses wait_for_selector + JSON-LD fallback for reliable extraction.
"""

import asyncio
import os
import re
import random
import sys
import json
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "reenrich_failed.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("reenrich")

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
]

WORKER_COUNT = 4
SLEEP_MIN = 2
SLEEP_MAX = 4


async def create_stealth_context(browser):
    ua = random.choice(USER_AGENTS)
    context = await browser.new_context(
        user_agent=ua,
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
        timezone_id="America/New_York",
        extra_http_headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "DNT": "1",
        }
    )
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        window.chrome = { runtime: {} };
    """)
    return context


async def search_and_extract(page, title, author):
    """Fresh search + extract — skips summary/companion pages."""
    data = {"gr_rating": "", "gr_rating_count": "", "gr_pages": "",
            "gr_publisher": "", "gr_description": "", "gr_genres": "",
            "goodreads_link": ""}

    try:
        query = f"{title} {author}".strip()
        url = f"https://www.goodreads.com/search?q={query.replace(' ', '+')}"
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(random.uniform(1.0, 2.0))

        items = await page.query_selector_all("tr[itemtype='http://schema.org/Book']")
        if not items:
            return data

        # Try up to 3 search results — skip summary pages
        for item in items[:3]:
            title_el = await item.query_selector("a.bookTitle")
            if not title_el:
                continue

            href = await title_el.get_attribute("href") or ""
            gr_title = (await title_el.text_content()).strip()

            # Skip "by author" summary pages
            slug = href.split("?")[0].split("/")[-1] if href else ""
            author_parts = author.lower().replace(".", "").split()
            if author_parts:
                author_slug = "-".join(author_parts)
                if f"-by-{author_slug}" in slug.lower():
                    continue
                # Also check partial match for common patterns
                if len(author_parts) >= 2 and f"-by-{author_parts[0]}-{author_parts[-1]}" in slug.lower():
                    continue

            book_url = "https://www.goodreads.com" + href
            data["goodreads_link"] = book_url

            # Navigate and extract
            await page.goto(book_url, wait_until="domcontentloaded", timeout=45000)
            try:
                await page.wait_for_selector("div.RatingStatistics__rating", timeout=10000)
            except:
                await asyncio.sleep(3)

            await asyncio.sleep(random.uniform(0.5, 1.5))

            # Extract rating
            rating_el = await page.query_selector("div.RatingStatistics__rating")
            if rating_el:
                data["gr_rating"] = (await rating_el.text_content()).strip()

            count_el = await page.query_selector("span[data-testid='ratingsCount']")
            if count_el:
                text = (await count_el.text_content()).strip()
                data["gr_rating_count"] = re.sub(r'[^0-9]', '', text)

            # JSON-LD fallback
            if not data["gr_rating"] or data["gr_rating"] in ["", "0", "0.00"]:
                content = await page.content()
                m = re.search(r'"ratingValue"[:\s]*"?([\d.]+)"?', content)
                if m and float(m.group(1)) > 0:
                    data["gr_rating"] = m.group(1)
                m2 = re.search(r'"ratingCount"[:\s]*"?(\d+)"?', content)
                if m2 and int(m2.group(1)) > 0:
                    data["gr_rating_count"] = m2.group(1)

            # If rating found, also get pages/publisher/description
            if data["gr_rating"] and data["gr_rating"] not in ["", "0", "0.00"]:
                try:
                    pages_el = await page.query_selector("p[data-testid='pagesFormat']")
                    if pages_el:
                        text = (await pages_el.text_content()).strip()
                        m = re.search(r'(\d+)\s*pages', text)
                        if m:
                            data["gr_pages"] = m.group(1)
                except:
                    pass
                try:
                    pub_el = await page.query_selector("div.FeaturedDetails")
                    if pub_el:
                        text = (await pub_el.text_content()).strip()
                        m = re.search(r'Published.*?by\s+(.+?)(?:\n|$)', text)
                        if m:
                            data["gr_publisher"] = m.group(1).strip()
                except:
                    pass
                try:
                    desc_el = await page.query_selector("div.BookPageMetadataSection__description span.Formatted")
                    if desc_el:
                        data["gr_description"] = (await desc_el.text_content()).strip()[:500]
                except:
                    pass
                return data  # Got a good result

            # This result was 0 — try next search result
            continue

    except Exception as e:
        log.debug(f"  Search error for '{title}': {e}")

    return data


async def worker(worker_id, browser, queue, results, lock, df):
    context = await create_stealth_context(browser)
    page = await context.new_page()
    processed = 0
    fixed = 0

    while True:
        try:
            idx, title, author = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        try:
            data = await search_and_extract(page, title, author)

            if data["gr_rating"] and data["gr_rating"] not in ["", "0", "0.00"]:
                async with lock:
                    results[idx] = data
                fixed += 1
                log.info(f"  [W{worker_id}] FIXED: '{title[:40]}' -> Rating: {data['gr_rating']}, Count: {data['gr_rating_count']}")
            else:
                log.info(f"  [W{worker_id}] Still 0: '{title[:40]}' by {author[:20]}")
        except Exception as e:
            log.warning(f"  [W{worker_id}] Error '{title[:30]}': {e}")

        processed += 1
        await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

        if processed % 15 == 0:
            try:
                await page.close()
                await context.close()
            except:
                pass
            context = await create_stealth_context(browser)
            page = await context.new_page()

        if processed % 50 == 0:
            remaining = queue.qsize()
            log.info(f"  [W{worker_id}] Progress: {processed} done, {fixed} fixed, ~{remaining} remaining")
            # Worker 0 saves
            if worker_id == 0:
                async with lock:
                    _apply_fixes(df, results)
                    df.to_csv(DATA_DIR / "PRIORITY_SELFPUB_ENRICHED_partial.csv", index=False)
                    log.info(f"  [W{worker_id}] Intermediate save ({len(results)} fixes)")

    try:
        await page.close()
        await context.close()
    except:
        pass
    log.info(f"  [W{worker_id}] Done. Processed {processed}, fixed {fixed}.")


def _apply_fixes(df, results):
    applied = 0
    for idx, data in results.items():
        if idx not in df.index:
            continue
        if data.get("gr_rating") and data["gr_rating"] not in ["", "0", "0.00"]:
            df.at[idx, "First Book Rating"] = data["gr_rating"]
            applied += 1
        if data.get("gr_rating_count") and data["gr_rating_count"] not in ["", "0"]:
            df.at[idx, "First Book Rating Count"] = data["gr_rating_count"]
        if data.get("gr_pages"):
            cur = df.at[idx, "Total Pages"] if "Total Pages" in df.columns else None
            if pd.isna(cur) or str(cur).strip() in ["", "nan", "0"]:
                pages = int(data["gr_pages"])
                bis = df.at[idx, "Books in Series"] if "Books in Series" in df.columns else None
                try:
                    n = int(float(bis)) if pd.notna(bis) else 1
                    df.at[idx, "Total Pages"] = pages * n
                    df.at[idx, "Length of Adaption in Hours"] = round((pages * n) / 33.33, 1)
                except:
                    df.at[idx, "Total Pages"] = pages
        if data.get("gr_publisher"):
            cur = df.at[idx, "Publisher Name"] if "Publisher Name" in df.columns else None
            if pd.isna(cur) or str(cur).strip() in ["", "nan"]:
                df.at[idx, "Publisher Name"] = data["gr_publisher"]
        if data.get("gr_description"):
            cur = df.at[idx, "Subjective Analysis"] if "Subjective Analysis" in df.columns else None
            if pd.isna(cur) or str(cur).strip() in ["", "nan"]:
                df.at[idx, "Subjective Analysis"] = data["gr_description"]
    return applied


async def run():
    log.info("=" * 60)
    log.info("RE-ENRICHMENT: Fixing 0/null Goodreads ratings")
    log.info("=" * 60)

    source = DATA_DIR / "PRIORITY_SELFPUB_ENRICHED.csv"
    if not source.exists():
        log.error("No enriched file found!")
        return

    df = pd.read_csv(source, low_memory=False)
    log.info(f"  Loaded {len(df)} from {source.name}")

    # Find entries with 0/null rating (whether they have a GR link or not)
    needs_retry = []
    for idx, row in df.iterrows():
        rating = str(row.get("First Book Rating", "")).strip()
        if rating in ["0", "0.0", "0.00", "", "nan", "None"]:
            title = str(row.get("First Book Name", "")).strip()
            if not title or title == "nan":
                title = str(row.get("Book Series Name", "")).strip()
            author = str(row.get("Author Name", "")).strip()
            if title and title != "nan" and author and author != "nan":
                needs_retry.append((idx, title, author))

    log.info(f"  Entries needing retry: {len(needs_retry)}")

    if not needs_retry:
        log.info("  Nothing to retry!")
        return

    queue = asyncio.Queue()
    for idx, title, author in needs_retry:
        queue.put_nowait((idx, title, author))

    results = {}
    lock = asyncio.Lock()

    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        log.info(f"  Browser launched. {WORKER_COUNT} workers for {queue.qsize()} entries")

        workers = [worker(i, browser, queue, results, lock, df) for i in range(WORKER_COUNT)]
        await asyncio.gather(*workers)
        await browser.close()

    # Apply all fixes
    _apply_fixes(df, results)
    fixed = sum(1 for d in results.values() if d.get("gr_rating") and d["gr_rating"] not in ["", "0", "0.00"])
    log.info(f"\n  Fixed {fixed} / {len(needs_retry)} entries")

    # Also need to search+extract for entries that had no GR link at all
    # (the "Not found" ones from first pass) — but that's a separate concern

    # Save
    output = DATA_DIR / "PRIORITY_SELFPUB_ENRICHED.csv"
    df.to_csv(output, index=False)
    log.info(f"  Saved to: {output}")

    # Clean up partial
    partial = DATA_DIR / "PRIORITY_SELFPUB_ENRICHED_partial.csv"
    if partial.exists():
        partial.unlink()

    # Stats
    good = sum(1 for _, r in df.iterrows()
               if str(r.get("First Book Rating", "")).strip() not in ["", "nan", "None", "0", "0.0", "0.00"])
    log.info(f"  Total with good ratings: {good} / {len(df)}")


if __name__ == "__main__":
    start = datetime.now()
    asyncio.run(run())
    log.info(f"  Completed in {datetime.now() - start}")
