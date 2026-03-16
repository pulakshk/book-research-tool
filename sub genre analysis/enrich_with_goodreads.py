#!/usr/bin/env python3
"""
Goodreads Enrichment Pipeline
===============================
Uses the existing Playwright-based Goodreads scraper to enrich the master
dataset with REAL ratings, rating counts, series info, pages, and descriptions.

Does NOT use Gemini for metadata — only real scraped data.
Gemini is used only for trope analysis and subjective analysis (which requires
human-like judgment, not factual data).

Pipeline:
1. Load cleaned master CSV
2. For each title missing Goodreads data, search + extract from Goodreads
3. Compute derived fields (flags, commissioning scores)
4. Save enriched master

Uses: genre_enrichment.py's search_goodreads() and extract_goodreads_data()
"""

import asyncio
import os
import re
import random
import sys
import json
import time
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# Add project paths
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
CRAWL_DIR = BASE_DIR / "New genre crawl"
DATA_DIR = BASE_DIR / "output"
DATA_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(CRAWL_DIR))
sys.path.insert(0, str(PROJECT_DIR))

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "goodreads_enrichment.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("gr_enrich")

# ── Gemini (only for trope/subjective — NOT for metadata) ──
def get_gemini_key():
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()

# ── Stealth browser config ─────────────────────────────────
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
]

WORKER_COUNT = 4
CONTEXT_ROTATION_INTERVAL = 15  # Rotate browser context every N books
SLEEP_MIN = 2
SLEEP_MAX = 5


async def create_stealth_context(browser):
    """Create stealth browser context."""
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
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
    """)
    return context


async def safe_goto(page, url, timeout=45000, retries=3):
    """Network-resilient navigation."""
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await asyncio.sleep(random.uniform(0.5, 1.5))
            return True
        except Exception as e:
            log.debug(f"  goto attempt {attempt}/{retries} failed: {e}")
            await asyncio.sleep(2 * attempt + random.uniform(1.0, 3.0))
    return False


async def search_goodreads(page, title, author):
    """Search Goodreads for a book. Skips summary/companion pages."""
    try:
        query = f"{title} {author}".strip()
        url = f"https://www.goodreads.com/search?q={query.replace(' ', '+')}"
        if not await safe_goto(page, url):
            return None

        await asyncio.sleep(random.uniform(1.0, 2.0))

        items = await page.query_selector_all("tr[itemtype='http://schema.org/Book']")
        if not items:
            return None

        # Check up to 3 results — skip summary/companion pages
        # Summary pages have URLs like "title-by-author-name" and typically no series info
        for item in items[:3]:
            title_el = await item.query_selector("a.bookTitle")
            if not title_el:
                continue

            href = await title_el.get_attribute("href") or ""
            gr_title = (await title_el.text_content()).strip()

            # Skip "by author" summary pages — URL slug ends with -by-authorname
            # These are companion/summary pages with 0 ratings
            slug = href.split("?")[0].split("/")[-1] if href else ""
            author_lower = author.lower().replace(" ", "-").replace(".", "")
            if f"-by-{author_lower}" in slug.lower():
                continue
            # Also skip if title text contains "by Author" pattern (summary page title format)
            if re.search(r'\bby\s+' + re.escape(author.split()[0]) if author else r'NOMATCH', gr_title, re.I):
                if "(" not in gr_title:  # Real books often have "(Series, #N)"
                    continue

            author_el = await item.query_selector("a.authorName span")
            gr_author = (await author_el.text_content()).strip() if author_el else ""

            gr_link = "https://www.goodreads.com" + href
            return {"link": gr_link, "author": gr_author}

        # Fallback: use first result if all were skipped
        first_item = items[0]
        title_el = await first_item.query_selector("a.bookTitle")
        if not title_el:
            return None
        author_el = await first_item.query_selector("a.authorName span")
        gr_author = (await author_el.text_content()).strip() if author_el else ""
        href = await title_el.get_attribute("href")
        gr_link = "https://www.goodreads.com" + href
        return {"link": gr_link, "author": gr_author}
    except Exception as e:
        log.debug(f"  GR search error: {e}")
        return None


async def extract_goodreads_data(page, gr_url):
    """Extract comprehensive data from a Goodreads book page."""
    data = {
        "goodreads_link": gr_url,
        "gr_rating": "",
        "gr_rating_count": "",
        "gr_series_name": "",
        "gr_series_url": "",
        "gr_book_number": "",
        "gr_pages": "",
        "gr_description": "",
        "gr_genres": "",
        "gr_publisher": "",
        "gr_pub_date": "",
    }

    content = None
    try:
        if not await safe_goto(page, gr_url):
            return data

        # Wait for the rating element to appear (key fix — don't just sleep)
        try:
            await page.wait_for_selector("div.RatingStatistics__rating", timeout=10000)
        except:
            # Page may still be loading — give it extra time
            await asyncio.sleep(3)

        await asyncio.sleep(random.uniform(0.5, 1.5))

        # Rating — try primary selector, then JSON-LD fallback
        try:
            rating_el = await page.query_selector("div.RatingStatistics__rating")
            if rating_el:
                data["gr_rating"] = (await rating_el.text_content()).strip()
        except:
            pass

        # Fallback: extract from JSON-LD structured data in page source
        if not data["gr_rating"] or data["gr_rating"] in ["", "0", "0.00"]:
            try:
                content = await page.content()
                m = re.search(r'"ratingValue"[:\s]*"?([\d.]+)"?', content)
                if m:
                    data["gr_rating"] = m.group(1)
            except:
                pass

        # Rating Count
        try:
            count_el = await page.query_selector("span[data-testid='ratingsCount']")
            if count_el:
                text = (await count_el.text_content()).strip()
                num = re.sub(r'[^0-9]', '', text)
                data["gr_rating_count"] = num
        except:
            pass

        # Fallback: JSON-LD for rating count
        if not data["gr_rating_count"] or data["gr_rating_count"] in ["", "0"]:
            try:
                if not content:
                    content = await page.content()
                m = re.search(r'"ratingCount"[:\s]*"?(\d+)"?', content)
                if m:
                    data["gr_rating_count"] = m.group(1)
            except:
                pass

        # Series Info
        try:
            series_el = await page.query_selector("h3.Text__italic a")
            if not series_el:
                series_el = await page.query_selector("div.BookPageTitleSection a[href*='/series/']")
            if series_el:
                series_text = (await series_el.text_content()).strip()
                series_href = await series_el.get_attribute("href")

                m = re.match(r'(.+?)(?:\s*#(\d+\.?\d*))?$', series_text)
                if m:
                    data["gr_series_name"] = m.group(1).strip()
                    if m.group(2):
                        data["gr_book_number"] = m.group(2)

                if series_href:
                    data["gr_series_url"] = (
                        "https://www.goodreads.com" + series_href
                        if series_href.startswith("/")
                        else series_href
                    )
        except:
            pass

        # Pages
        try:
            pages_el = await page.query_selector("p[data-testid='pagesFormat']")
            if pages_el:
                text = (await pages_el.text_content()).strip()
                m = re.search(r'(\d+)\s*pages', text)
                if m:
                    data["gr_pages"] = m.group(1)
        except:
            pass

        # Description
        try:
            desc_el = await page.query_selector("div.BookPageMetadataSection__description span.Formatted")
            if not desc_el:
                show_more = await page.query_selector("button.Button--inline:has-text('Show more')")
                if show_more:
                    await show_more.click()
                    await asyncio.sleep(0.5)
                    desc_el = await page.query_selector("div.BookPageMetadataSection__description span.Formatted")
            if desc_el:
                data["gr_description"] = (await desc_el.text_content()).strip()[:1000]
        except:
            pass

        # Genres
        try:
            genre_els = await page.query_selector_all("span.BookPageMetadataSection__genreButton a .Button__labelItem")
            genres = []
            for g in genre_els[:5]:
                genres.append((await g.text_content()).strip())
            data["gr_genres"] = ", ".join(genres)
        except:
            pass

        # Publisher
        try:
            pub_el = await page.query_selector("div.FeaturedDetails")
            if pub_el:
                text = (await pub_el.text_content()).strip()
                m = re.search(r'Published.*?by\s+(.+?)(?:\n|$)', text)
                if m:
                    data["gr_publisher"] = m.group(1).strip()
            if not data["gr_publisher"]:
                detail_items = await page.query_selector_all("div.DescListItem")
                for item in detail_items:
                    text = (await item.text_content()).strip()
                    if "publisher" in text.lower() or "published by" in text.lower():
                        m = re.search(r'(?:Publisher|Published by)\s*(.+)', text, re.I)
                        if m:
                            data["gr_publisher"] = m.group(1).strip()
                            break
        except:
            pass

        # Publication Date
        try:
            pub_el = await page.query_selector("p[data-testid='publicationInfo']")
            if pub_el:
                data["gr_pub_date"] = (await pub_el.text_content()).strip()
        except:
            pass

    except Exception as e:
        log.warning(f"  Error extracting GR data from {gr_url}: {e}")

    return data


async def enrich_worker(worker_id, browser, queue, results, lock, save_path, df):
    """Worker that processes books from the queue."""
    context = await create_stealth_context(browser)
    page = await context.new_page()
    processed = 0
    total_in_queue = queue.qsize()

    while True:
        try:
            idx, row = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        series_name = str(row.get("Book Series Name", "")).strip()
        first_book = str(row.get("First Book Name", "")).strip()
        author = str(row.get("Author Name", "")).strip()

        # Search title: use first book name if available, else series name
        search_title = first_book if first_book and first_book != "nan" else series_name

        log.info(f"  [W{worker_id}] Searching: '{search_title}' by {author}")

        try:
            # Search Goodreads
            result = await search_goodreads(page, search_title, author)

            if result and result.get("link"):
                gr_data = await extract_goodreads_data(page, result["link"])

                async with lock:
                    results[idx] = gr_data

                log.info(
                    f"  [W{worker_id}] Found: {series_name} -> "
                    f"Rating: {gr_data.get('gr_rating', 'N/A')}, "
                    f"Count: {gr_data.get('gr_rating_count', 'N/A')}"
                )
            else:
                log.info(f"  [W{worker_id}] Not found on GR: '{search_title}'")
        except Exception as e:
            log.warning(f"  [W{worker_id}] Error processing '{search_title}': {e}")

        processed += 1
        await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

        # Rotate context periodically
        if processed % CONTEXT_ROTATION_INTERVAL == 0:
            try:
                await page.close()
                await context.close()
            except:
                pass
            context = await create_stealth_context(browser)
            page = await context.new_page()
            log.info(f"  [W{worker_id}] Context rotated after {processed} books")

        # Save progress periodically (every 50 books per worker = ~200 total across 4 workers)
        if processed % 50 == 0:
            remaining = queue.qsize()
            log.info(f"  [W{worker_id}] Progress: {processed} done, ~{remaining} remaining in queue")
            # Worker 0 does intermediate saves
            if worker_id == 0:
                async with lock:
                    try:
                        _apply_results_to_df(df, results)
                        partial_path = DATA_DIR / "selfpub_master_enriched_gr_partial.csv"
                        df.to_csv(partial_path, index=False)
                        log.info(f"  [W{worker_id}] Intermediate save: {partial_path.name} ({len(results)} enriched so far)")
                    except Exception as e:
                        log.warning(f"  [W{worker_id}] Save error: {e}")

    try:
        await page.close()
        await context.close()
    except:
        pass
    log.info(f"  [W{worker_id}] Done. Processed {processed} books.")


def _apply_results_to_df(df, results):
    """Apply Goodreads results dict to dataframe. Returns count of applied."""
    applied = 0
    for idx, gr_data in results.items():
        if idx not in df.index:
            continue
        if gr_data.get("gr_rating"):
            df.at[idx, "First Book Rating"] = gr_data["gr_rating"]
        if gr_data.get("gr_rating_count"):
            df.at[idx, "First Book Rating Count"] = gr_data["gr_rating_count"]
        if gr_data.get("gr_pages"):
            current_pages = df.at[idx, "Total Pages"] if "Total Pages" in df.columns else None
            if pd.isna(current_pages) or str(current_pages).strip() in ["", "nan", "0"]:
                pages = int(gr_data["gr_pages"])
                books_in_series = df.at[idx, "Books in Series"] if "Books in Series" in df.columns else None
                if pd.notna(books_in_series):
                    try:
                        n_books = int(float(books_in_series))
                        df.at[idx, "Total Pages"] = pages * n_books
                        df.at[idx, "Length of Adaption in Hours"] = round((pages * n_books) / 33.33, 1)
                    except:
                        df.at[idx, "Total Pages"] = pages
        if gr_data.get("gr_description"):
            if "Subjective Analysis" in df.columns:
                val = df.at[idx, "Subjective Analysis"]
                if pd.isna(val) or str(val).strip() in ["", "nan"]:
                    df.at[idx, "Subjective Analysis"] = gr_data["gr_description"][:500]
        if gr_data.get("gr_publisher"):
            if "Publisher Name" in df.columns:
                val = df.at[idx, "Publisher Name"]
                if pd.isna(val) or str(val).strip() in ["", "nan"]:
                    df.at[idx, "Publisher Name"] = gr_data["gr_publisher"]
        if gr_data.get("gr_genres"):
            if "Primary Trope" in df.columns:
                val = df.at[idx, "Primary Trope"]
                if pd.isna(val) or str(val).strip() in ["", "nan"]:
                    df.at[idx, "Primary Trope"] = gr_data["gr_genres"].split(",")[0].strip()
        if gr_data.get("goodreads_link"):
            df.at[idx, "Contact Info"] = gr_data["goodreads_link"]
        applied += 1
    return applied


async def run_goodreads_enrichment():
    """Main enrichment pipeline using Goodreads scraper."""
    log.info("=" * 60)
    log.info("GOODREADS ENRICHMENT PIPELINE")
    log.info("=" * 60)

    # Find best source file — prefer the priority self-pub filtered dataset
    candidates = [
        DATA_DIR / "PRIORITY_SELFPUB_SERIES_FOR_ENRICHMENT.csv",
        DATA_DIR / "selfpub_master_cleaned.csv",
        DATA_DIR / "selfpub_master_multi_platform.csv",
        DATA_DIR / "selfpub_master_expanded_v2.csv",
        DATA_DIR / "selfpub_master_expanded.csv",
    ]

    # Check for existing partial enrichment to resume
    partial_path = DATA_DIR / "selfpub_master_enriched_gr_partial.csv"
    if partial_path.exists():
        source = partial_path
        log.info(f"  RESUMING from partial: {partial_path.name}")
    else:
        source = None
        for c in candidates:
            if c.exists():
                source = c
                break

    if not source:
        log.error("No master CSV found!")
        return

    df = pd.read_csv(source, low_memory=False)
    log.info(f"  Loaded {len(df)} series from {source.name}")

    # Find rows that need Goodreads enrichment (missing First Book Rating or First Book Rating Count)
    def needs_enrichment(row):
        rating = row.get("First Book Rating")
        count = row.get("First Book Rating Count")
        rating_empty = pd.isna(rating) or str(rating).strip() in ["", "nan", "None", "0"]
        count_empty = pd.isna(count) or str(count).strip() in ["", "nan", "None", "0"]
        return rating_empty or count_empty

    mask = df.apply(needs_enrichment, axis=1)
    needs_gr = df[mask]
    log.info(f"  Series needing Goodreads data: {len(needs_gr)} / {len(df)}")

    if len(needs_gr) == 0:
        log.info("  All series already have Goodreads data!")
        return df

    # Build work queue
    queue = asyncio.Queue()
    for idx, row in needs_gr.iterrows():
        queue.put_nowait((idx, row))

    results = {}
    lock = asyncio.Lock()
    save_path = DATA_DIR / "PRIORITY_SELFPUB_ENRICHED.csv"

    # Launch Playwright
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        log.info(f"  Browser launched. Starting {WORKER_COUNT} workers for {queue.qsize()} books")

        workers = [
            enrich_worker(i, browser, queue, results, lock, save_path, df)
            for i in range(WORKER_COUNT)
        ]

        await asyncio.gather(*workers)
        await browser.close()

    # Apply results to dataframe
    applied = _apply_results_to_df(df, results)
    log.info(f"\n  Applied Goodreads data to {applied} series")

    # Compute derived fields
    df = compute_derived_fields(df)

    df.to_csv(save_path, index=False)
    log.info(f"  Saved enriched data to: {save_path}")

    # Remove partial file now that we have the final
    partial_path = DATA_DIR / "selfpub_master_enriched_gr_partial.csv"
    if partial_path.exists():
        partial_path.unlink()
        log.info(f"  Removed partial file: {partial_path.name}")

    # Stats
    has_rating = df["First Book Rating"].notna() & (df["First Book Rating"].astype(str).str.strip() != "")
    has_count = df["First Book Rating Count"].notna() & (df["First Book Rating Count"].astype(str).str.strip() != "")
    log.info(f"\n  DATA QUALITY:")
    log.info(f"  Series with ratings: {has_rating.sum()} / {len(df)}")
    log.info(f"  Series with rating counts: {has_count.sum()} / {len(df)}")

    return df


def compute_derived_fields(df):
    """Compute objective metric flags and commissioning score."""

    def safe_float(val):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def safe_int(val):
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None

    for idx, row in df.iterrows():
        hours = safe_float(row.get("Length of Adaption in Hours"))
        first_rating = safe_float(row.get("First Book Rating"))
        first_count = safe_int(row.get("First Book Rating Count"))
        lowest_rating = safe_float(row.get("Lowest Rated Book Rating"))
        pub_year = safe_int(row.get("First_Book_Pub_Year"))

        # Total Pages estimate if missing
        if not safe_float(row.get("Total Pages")):
            books = safe_int(row.get("Books in Series"))
            if books:
                df.at[idx, "Total Pages"] = books * 300
                df.at[idx, "Length of Adaption in Hours"] = round(books * 300 / 33.33, 1)
                hours = df.at[idx, "Length of Adaption in Hours"]

        # Adaptation_Length_Flag
        if hours:
            if hours >= 80:
                df.at[idx, "Adaptation_Length_Flag"] = "Very High"
            elif hours >= 50:
                df.at[idx, "Adaptation_Length_Flag"] = "High"
            elif hours >= 30:
                df.at[idx, "Adaptation_Length_Flag"] = "Medium"
            else:
                df.at[idx, "Adaptation_Length_Flag"] = "Low"

        # First_Book_Rating_Flag
        if first_rating:
            if first_rating >= 4.3:
                df.at[idx, "First_Book_Rating_Flag"] = "High"
            elif first_rating >= 3.8:
                df.at[idx, "First_Book_Rating_Flag"] = "Medium"
            else:
                df.at[idx, "First_Book_Rating_Flag"] = "Low"

        # Appeal Flag (GR rating count = audience scale)
        if first_count:
            if first_count >= 10000:
                df.at[idx, "Appeal Flag"] = "Very High"
            elif first_count >= 5000:
                df.at[idx, "Appeal Flag"] = "High"
            elif first_count >= 1000:
                df.at[idx, "Appeal Flag"] = "Medium"
            else:
                df.at[idx, "Appeal Flag"] = "Low"

        # Lowest_Book_Rating_Flag
        if lowest_rating:
            if lowest_rating >= 4.0:
                df.at[idx, "Lowest_Book_Rating_Flag"] = "High"
            elif lowest_rating >= 3.5:
                df.at[idx, "Lowest_Book_Rating_Flag"] = "Medium"
            else:
                df.at[idx, "Lowest_Book_Rating_Flag"] = "Low"

        # Rating_Stability_Flag
        if first_rating and lowest_rating:
            spread = first_rating - lowest_rating
            if spread <= 0.3:
                df.at[idx, "Rating_Stability_Flag"] = "High"
            elif spread <= 0.6:
                df.at[idx, "Rating_Stability_Flag"] = "Medium"
            else:
                df.at[idx, "Rating_Stability_Flag"] = "Low"

        # Series_Era
        if pub_year:
            if pub_year >= 2021:
                df.at[idx, "Series_Era"] = "After 2020"
            elif pub_year >= 2015:
                df.at[idx, "Series_Era"] = "2015-2020"
            else:
                df.at[idx, "Series_Era"] = "Before 2015"

        # Commissioning Score (0-100)
        score = 0
        if hours:
            if hours >= 80: score += 25
            elif hours >= 50: score += 20
            elif hours >= 30: score += 12
            else: score += 5

        if first_rating:
            if first_rating >= 4.3: score += 25
            elif first_rating >= 4.0: score += 20
            elif first_rating >= 3.8: score += 15
            else: score += 8

        if first_count:
            if first_count >= 20000: score += 25
            elif first_count >= 10000: score += 22
            elif first_count >= 5000: score += 18
            elif first_count >= 1000: score += 12
            else: score += 5

        if lowest_rating:
            if lowest_rating >= 4.0: score += 15
            elif lowest_rating >= 3.5: score += 10
            else: score += 3

        era_bonus = 0
        if pub_year and pub_year >= 2020: era_bonus = 10
        elif pub_year and pub_year >= 2015: era_bonus = 5
        score += era_bonus

        if score > 0:
            df.at[idx, "Commissioning_Score"] = round(score, 1)
            if score >= 85:
                df.at[idx, "Commissioning_Rank"] = "P0"
            elif score >= 70:
                df.at[idx, "Commissioning_Rank"] = "P1"
            elif score >= 50:
                df.at[idx, "Commissioning_Rank"] = "P2"
            else:
                df.at[idx, "Commissioning_Rank"] = "P3"

    return df


if __name__ == "__main__":
    start = datetime.now()
    asyncio.run(run_goodreads_enrichment())
    elapsed = datetime.now() - start
    log.info(f"\n  Enrichment completed in {elapsed}")
