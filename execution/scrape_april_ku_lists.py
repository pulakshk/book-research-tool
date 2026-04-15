#!/usr/bin/env python3
"""
April 2026 Amazon KU Bestseller Scraper — Sports Romance & Ice Hockey

Scrapes Amazon Kindle Unlimited top 100 lists for:
  - Sports Romance (node 7588834011)
  - Free Sports Romance KU (node 7588834011 free variant)
  - Sports & Games Fiction (node 6490087011)

Uses Playwright (headless Chromium) same as the existing amazon_top100.py.

Output:
  outreach/sports-romance/source/april_ku_sports_romance.csv

Anti-hallucination:
  - Validates first 5 scraped rows before saving
  - Prints sample to stdout for manual review
  - Checks for required fields (rank, title, author)
  - Deduplicates on (title, author) before saving

Usage:
  python3 execution/scrape_april_ku_lists.py
  python3 execution/scrape_april_ku_lists.py --dry-run  (validate only, don't save)
"""

import argparse
import asyncio
import json
import random
import re
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT / "outreach" / "sports-romance" / "source"
OUT_CSV = OUT_DIR / "april_ku_sports_romance.csv"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Amazon Kindle Store bestseller browse node IDs
# These are the same nodes used in subgenre-pipeline/amazon_top100.py
CATEGORIES = [
    # Sports Romance — confirmed node as of April 2026
    # (node 7588834011 was reassigned to Horror Fiction Classics)
    ("Sports Romance (Paid)", "6487842011", "paid"),
    # Contemporary Romance — captures sports romance cross-listings
    ("Contemporary Romance (Paid)", "158568011", "paid"),
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}

# Hockey-related keywords to tag hockey-specific entries
HOCKEY_KEYWORDS = [
    "hockey", "ice", "puck", "rink", "nhl", "skate", "goalie", "zamboni",
    "penalty", "power play", "blue line", "face off", "faceoff",
]
SPORTS_KEYWORDS = [
    "quarterback", "touchdown", "football", "basketball", "baseball",
    "soccer", "tennis", "golf", "swimming", "track", "athlete", "stadium",
    "pitcher", "pitcher", "batter", "tackle", "draft pick",
]


def _url_for(node_id: str, page: int, list_type: str) -> str:
    return f"https://www.amazon.com/Best-Sellers/zgbs/digital-text/{node_id}/?pg={page}"


def _tag_sport(title: str, author: str) -> str:
    combined = (title + " " + author).lower()
    if any(kw in combined for kw in HOCKEY_KEYWORDS):
        return "Hockey"
    if any(kw in combined for kw in SPORTS_KEYWORDS):
        return "Other Sport"
    return "Sports Romance"


async def _extract_book_from_item(item) -> dict:
    """
    Extract rank, title, author, ASIN from a single Amazon grid item.
    Uses stable selectors that work with Amazon's current layout.
    """
    # ASIN from data attribute
    asin = await item.get_attribute("data-asin") or ""
    if not asin:
        card = await item.query_selector("[data-asin]")
        if card:
            asin = await card.get_attribute("data-asin") or ""

    # Rank from badge text (span.zg-bdg-text contains "#1", "#2", etc.)
    rank = 0
    rank_el = await item.query_selector("span.zg-bdg-text")
    if rank_el:
        rank_text = (await rank_el.text_content()).strip()
        m = re.match(r"#(\d+)", rank_text)
        if m:
            rank = int(m.group(1))

    # Title: img alt attribute is the most reliable
    title = ""
    img_el = await item.query_selector("img")
    if img_el:
        title = (await img_el.get_attribute("alt") or "").strip()

    # Author: a.a-size-small inside the faceout
    author = ""
    author_el = await item.query_selector("a.a-size-small")
    if author_el:
        author = (await author_el.text_content()).strip()

    # Fallback: use line-clamp spans if title or author missing
    if not title or not author:
        clamp_els = await item.query_selector_all(
            "div[class*='line-clamp'], span[class*='line-clamp']"
        )
        texts = [(await el.text_content()).strip() for el in clamp_els if el]
        texts = [t for t in texts if t]
        if not title and texts:
            title = texts[0]
        if not author and len(texts) > 1:
            author = texts[1]

    return {"rank": rank, "title": title, "author": author, "asin": asin}


async def scrape_one_category(page, cat_name: str, node_id: str, list_type: str) -> list:
    books = []
    for page_num in [1, 2]:
        url = _url_for(node_id, page_num, list_type)
        print(f"  [{cat_name}] Fetching page {page_num}: {url}")
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=35000)
            await asyncio.sleep(random.uniform(2.5, 4.5))

            # Primary selector used by Amazon's current grid layout
            items = await page.query_selector_all("#gridItemRoot")
            if not items:
                items = await page.query_selector_all("div[id^='p13n-asin-index-']")
            if not items:
                items = await page.query_selector_all("li.zg-item-immersion")

            print(f"  [{cat_name}] Page {page_num}: {len(items)} items found")

            for item in items:
                try:
                    parsed = await _extract_book_from_item(item)
                    title  = parsed["title"]
                    author = parsed["author"]
                    rank   = parsed["rank"]
                    asin   = parsed["asin"]

                    if title and rank > 0:
                        books.append({
                            "rank": rank,
                            "title": title,
                            "author": author,
                            "asin": asin,
                            "category": cat_name,
                            "node_id": node_id,
                            "list_type": list_type,
                            "sport_tag": _tag_sport(title, author),
                        })
                except Exception:
                    continue

        except Exception as exc:
            print(f"  [{cat_name}] Page {page_num} error: {exc}")

        await asyncio.sleep(random.uniform(1.5, 3.0))

    print(f"  [{cat_name}] Total scraped: {len(books)}")
    return books


async def run_scraper(dry_run: bool = False):
    print("=" * 70)
    print(f"APRIL 2026 KU SPORTS ROMANCE SCRAPER — {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 70)

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run: pip install playwright && playwright install chromium")
        sys.exit(1)

    all_books = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent=HEADERS["User-Agent"],
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = await context.new_page()

        for cat_name, node_id, list_type in CATEGORIES:
            books = await scrape_one_category(page, cat_name, node_id, list_type)
            all_books.extend(books)
            await asyncio.sleep(random.uniform(3, 6))

        await browser.close()

    if not all_books:
        print("\nERROR: No books scraped. Amazon may be blocking. Check the output.")
        sys.exit(1)

    df = pd.DataFrame(all_books)
    df["scraped_date"] = datetime.now().strftime("%Y-%m-%d")

    # ── Anti-hallucination: validate 5 rows ────────────────────────────────
    print("\n" + "=" * 60)
    print("SELF-CHECK: First 5 scraped rows (verify manually)")
    print("=" * 60)
    for _, row in df.head(5).iterrows():
        print(f"  Rank #{row['rank']:3d} | {row['category'][:30]:30} | "
              f"{row['title'][:40]:40} | {row['author'][:30]}")

    # Required fields check
    missing_rank = (df["rank"] == 0).sum()
    missing_title = (df["title"].str.strip() == "").sum()
    missing_author = (df["author"].str.strip() == "").sum()
    print(f"\nValidation: {len(df)} rows | missing rank: {missing_rank} | "
          f"missing title: {missing_title} | missing author: {missing_author}")

    # Dedup on (title, author)
    before = len(df)
    df = df.drop_duplicates(subset=["title", "author"], keep="first")
    print(f"Deduplication: {before} → {len(df)} rows")

    # Category summary
    print("\nCategory breakdown:")
    for cat, grp in df.groupby("category"):
        print(f"  {cat}: {len(grp)} rows")

    print("\nSport tag breakdown:")
    print(df["sport_tag"].value_counts().to_string())

    print("\nTop 10 by rank (Sports Romance Paid):")
    sports_paid = df[df["category"] == "Sports Romance (Paid)"].nsmallest(10, "rank")
    for _, row in sports_paid.iterrows():
        print(f"  #{row['rank']:3d}  {row['title'][:50]:50}  by {row['author']}")

    if dry_run:
        print("\n[DRY RUN] Not saving output.")
        return

    # Save
    df.to_csv(OUT_CSV, index=False)
    print(f"\nSaved: {OUT_CSV} ({len(df)} rows)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Validate but don't save")
    args = parser.parse_args()
    asyncio.run(run_scraper(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
