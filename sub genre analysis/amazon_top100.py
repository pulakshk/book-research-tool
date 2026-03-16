#!/usr/bin/env python3
"""
Amazon Top 100 Bestseller Scraper & Mapper
============================================
1. Scrape Amazon bestseller lists for romance subcategories
2. Map scraped titles against our priority dataset
3. Tag entries: "Amazon Top 10 [category]", "Top 50 [category]", "Top 100 [category]"
"""

import asyncio
import re
import random
import logging
import json
from pathlib import Path
from datetime import datetime

import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "output"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "amazon_top100.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("amazon_top100")

# Amazon.com bestseller category browse node IDs
# Using direct browse node URLs to avoid geo-redirect
# Includes neighboring categories per user request
AMAZON_CATEGORIES = [
    # Main Romance categories
    ("Romance", "158566011"),
    ("Contemporary Romance", "6487818011"),
    ("Romantic Suspense", "6487830011"),
    ("Historical Romance", "6487814011"),
    ("Military Romance", "6487822011"),
    ("Christian Romance", "6487826011"),
    ("Sports Romance", "7588834011"),
    ("Dark Romance", "23488356011"),
    ("Small Town Romance", "25381371011"),
    ("Crime Fiction Romance", "7588814011"),
    ("Political Romance", "7588826011"),
    ("New Adult Romance", "17741157011"),
    ("Multicultural Romance", "7588822011"),
    # Neighboring / broader categories
    ("Literature & Fiction", "17"),
    ("Women's Fiction", "542654"),
    ("Mystery Thriller Suspense", "18"),
    ("Christian Fiction", "12290"),
    ("Historical Fiction", "10177"),
    ("Action & Adventure", "720"),
    # Romance sub-subcategories (deeper tree)
    ("Gothic Romance", "6487812011"),
    ("Paranormal Romance", "6487828011"),
    ("Western Romance", "6487832011"),
    ("Holiday Romance", "10197"),
    ("Romantic Comedy", "6487820011"),
    # Free Kindle eBooks - Romance
    ("Free Kindle Romance", "158204011"),
]


async def scrape_category(page, category_name, node_id):
    """Scrape top 100 from an Amazon bestseller category using both pages."""
    books = []

    for page_num in [1, 2]:
        url = f"https://www.amazon.com/Best-Sellers/zgbs/digital-text/{node_id}/?pg={page_num}"
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(random.uniform(2, 4))

            # Try multiple selectors - Amazon uses different layouts
            items = await page.query_selector_all("#gridItemRoot")
            if not items:
                items = await page.query_selector_all("div[id^='p13n-asin-index-']")
            if not items:
                items = await page.query_selector_all("div.a-cardui._cDEzb_grid-cell_1uMOS")
            if not items:
                items = await page.query_selector_all("div[class*='zg-grid-general-faceout']")
            if not items:
                items = await page.query_selector_all("li.zg-item-immersion")
            if not items:
                # Last resort: find any ranked item containers
                items = await page.query_selector_all("div.zg-item, span.zg-item")

            if not items and page_num == 1:
                # Debug: save page content for analysis
                content = await page.content()
                debug_path = DATA_DIR / f"debug_{category_name.replace(' ', '_').replace('/', '_')}.html"
                debug_path.write_text(content[:50000])
                log.warning(f"  [{category_name}] No items found - debug HTML saved")

            log.info(f"  [{category_name}] Page {page_num}: {len(items)} items")

            for item in items:
                try:
                    full_text = (await item.text_content()).strip()

                    # Extract rank from text (#1, #2, etc.)
                    rank_match = re.match(r'#(\d+)', full_text)
                    rank = int(rank_match.group(1)) if rank_match else 0

                    # Parse title and author from the structured text
                    # Format: "#N TitleAuthorRating...Kindle Edition..."
                    # Split on known boundary patterns
                    lines = [l.strip() for l in full_text.split('\n') if l.strip()]

                    title = ""
                    author = ""

                    # Find title: first substantial text after rank
                    for line in lines:
                        line = line.strip()
                        if line.startswith('#'):
                            continue
                        if not title and len(line) > 3 and 'star' not in line and 'Kindle' not in line and 'INR' not in line and '$' not in line:
                            title = line
                        elif title and not author and len(line) > 2 and 'star' not in line and 'Kindle' not in line and 'INR' not in line and '$' not in line and line != title:
                            author = line
                            break

                    if not title:
                        # Fallback: extract from child elements
                        all_divs = await item.query_selector_all("div")
                        texts = []
                        for d in all_divs:
                            t = (await d.text_content()).strip()
                            if t and len(t) < 200 and t not in texts:
                                texts.append(t)
                        # First unique text longer than 5 chars is likely title
                        for t in texts:
                            if len(t) > 5 and not t.startswith('#') and 'star' not in t and 'Kindle' not in t:
                                if not title:
                                    title = t
                                elif not author and t != title and len(t) > 2:
                                    author = t
                                    break

                    if title and rank > 0:
                        books.append({
                            "rank": rank,
                            "title": title,
                            "author": author,
                            "category": category_name,
                        })
                except:
                    continue

        except Exception as e:
            log.warning(f"  [{category_name}] Page {page_num} error: {e}")

        await asyncio.sleep(random.uniform(1, 3))

    log.info(f"  [{category_name}] Total: {len(books)} books")
    return books


async def run_amazon_scraper():
    log.info("=" * 60)
    log.info("AMAZON TOP 100 BESTSELLER SCRAPER")
    log.info("=" * 60)

    from playwright.async_api import async_playwright

    all_books = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
            timezone_id="America/New_York",
        )
        page = await context.new_page()

        for cat_name, node_id in AMAZON_CATEGORIES:
            log.info(f"\n  Scraping: {cat_name}")
            books = await scrape_category(page, cat_name, node_id)
            all_books.extend(books)
            await asyncio.sleep(random.uniform(3, 6))

        await browser.close()

    # Save raw Amazon data
    if all_books:
        amazon_df = pd.DataFrame(all_books)
        amazon_df.to_csv(DATA_DIR / "amazon_top100_raw.csv", index=False)
        log.info(f"\n  Saved {len(all_books)} Amazon bestseller entries")

        for cat in amazon_df["category"].unique():
            count = len(amazon_df[amazon_df["category"] == cat])
            log.info(f"    {cat}: {count} books")
    else:
        log.warning("  No books scraped from Amazon!")
        return

    # ── Map against our dataset ──────────────────────────
    map_amazon_to_dataset(amazon_df)


def map_amazon_to_dataset(amazon_df=None):
    """Map Amazon bestseller data to our priority dataset."""
    log.info(f"\n  {'='*60}")
    log.info(f"  MAPPING AGAINST PRIORITY DATASET")
    log.info(f"  {'='*60}")

    if amazon_df is None:
        raw_path = DATA_DIR / "amazon_top100_raw.csv"
        if raw_path.exists():
            amazon_df = pd.read_csv(raw_path)
        else:
            log.error("No Amazon data to map!")
            return

    source = DATA_DIR / "PRIORITY_SELFPUB_ENRICHED.csv"
    if not source.exists():
        source = DATA_DIR / "PRIORITY_SELFPUB_SERIES_FOR_ENRICHMENT.csv"

    df = pd.read_csv(source, low_memory=False)
    log.info(f"  Loaded {len(df)} from {source.name}")

    # Initialize columns
    if "Amazon_Bestseller_Tag" not in df.columns:
        df["Amazon_Bestseller_Tag"] = ""
    if "Amazon_Best_Rank" not in df.columns:
        df["Amazon_Best_Rank"] = ""

    # Build lookups: normalized title -> [indices], author -> [indices]
    title_to_idx = {}
    author_to_idx = {}
    for idx, row in df.iterrows():
        for col in ["Book Series Name", "First Book Name"]:
            val = str(row.get(col, "")).lower().strip()
            if val and val != "nan":
                norm = re.sub(r'[^\w\s]', '', val).strip()
                if len(norm) > 3:
                    title_to_idx.setdefault(norm, []).append(idx)

        author = str(row.get("Author Name", "")).lower().strip()
        if author and author != "nan":
            author_to_idx.setdefault(author, []).append(idx)

    matched = 0
    for _, amz in amazon_df.iterrows():
        amz_title = re.sub(r'[^\w\s]', '', str(amz["title"]).lower().strip())
        amz_author = str(amz.get("author", "")).lower().strip()
        rank = int(amz["rank"]) if pd.notna(amz["rank"]) else 999
        category = str(amz["category"])

        if rank <= 10:
            tag = f"Amazon Top 10 {category}"
        elif rank <= 50:
            tag = f"Amazon Top 50 {category}"
        else:
            tag = f"Amazon Top 100 {category}"

        matched_indices = set()

        # Exact title match
        if amz_title in title_to_idx:
            matched_indices.update(title_to_idx[amz_title])

        # Substring match
        if not matched_indices and len(amz_title) > 8:
            for our_title, indices in title_to_idx.items():
                if len(our_title) > 5:
                    if our_title in amz_title or amz_title in our_title:
                        matched_indices.update(indices)
                        break

        # Author match (only if author is specific enough)
        if not matched_indices and amz_author and len(amz_author) > 5:
            if amz_author in author_to_idx:
                matched_indices.update(author_to_idx[amz_author])

        for idx in matched_indices:
            current_tag = str(df.at[idx, "Amazon_Bestseller_Tag"]).strip()
            if current_tag and current_tag not in ["", "nan"]:
                if tag not in current_tag:
                    df.at[idx, "Amazon_Bestseller_Tag"] = current_tag + "; " + tag
            else:
                df.at[idx, "Amazon_Bestseller_Tag"] = tag

            current_rank = df.at[idx, "Amazon_Best_Rank"]
            try:
                cr = int(float(current_rank)) if pd.notna(current_rank) and str(current_rank).strip() not in ["", "nan"] else 999
            except:
                cr = 999
            if rank < cr:
                df.at[idx, "Amazon_Best_Rank"] = rank
            matched += 1

    log.info(f"  Matched {matched} Amazon entries to our dataset")

    has_tag = df["Amazon_Bestseller_Tag"].notna() & (~df["Amazon_Bestseller_Tag"].astype(str).str.strip().isin(["", "nan"]))
    log.info(f"  Series with Amazon tags: {has_tag.sum()}")

    df.to_csv(source, index=False)
    log.info(f"  Saved to {source.name}")


if __name__ == "__main__":
    start = datetime.now()
    asyncio.run(run_amazon_scraper())
    log.info(f"  Completed in {datetime.now() - start}")
