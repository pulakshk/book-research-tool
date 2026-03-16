#!/usr/bin/env python3
"""
Series Verification Pipeline
==============================
MUST run BEFORE cleanup removes any entries.

Verifies whether "standalone" or "1-book" titles are actually part of a series.
Uses 4 layers of verification (fast → slow):

  Layer 1: Title pattern matching (instant)
           Detects "Book 1", "#1", "Volume", "Box Set", "Trilogy", etc.

  Layer 2: Author clustering (instant)
           Groups titles by author — if same author has 3+ entries, likely a series.

  Layer 3: Google Books API series lookup (fast, free)
           Checks volumeInfo for series metadata.

  Layer 4: Goodreads series check via Playwright (slow, most accurate)
           Searches Goodreads and checks series info on book pages.

Output: Updates "Books in Series" and adds "Series Verified" flag.
"""

import asyncio
import os
import re
import json
import time
import random
import logging
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"
DATA_DIR.mkdir(exist_ok=True)

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "series_verification.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("series_verify")

# ── Configuration ─────────────────────────────────────────
WORKER_COUNT = 4
HEADLESS = True
CONTEXT_ROTATION_INTERVAL = 15  # Rotate browser context every N books
GOOGLE_BOOKS_RATE_LIMIT = 1.0   # seconds between Google Books requests
GR_SLEEP_MIN = 2.0
GR_SLEEP_MAX = 4.5

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0",
]


# ══════════════════════════════════════════════════════════
#  LAYER 1: Title Pattern Matching (instant)
# ══════════════════════════════════════════════════════════

SERIES_PATTERNS = [
    # "Book 1", "Book One", "Book #1"
    re.compile(r'\bbook\s*#?\s*(\d+|one|two|three|four|five|six|seven|eight|nine|ten)\b', re.I),
    # "#1", "#2", etc. — common in series titles
    re.compile(r'#\s*(\d+)', re.I),
    # "Volume 1", "Vol. 1", "Vol 1"
    re.compile(r'\bvol(?:ume)?\.?\s*#?\s*(\d+)', re.I),
    # "Part 1", "Part One"
    re.compile(r'\bpart\s*#?\s*(\d+|one|two|three|four|five)', re.I),
    # "Series", "Trilogy", "Duology", "Saga", "Collection"
    re.compile(r'\b(?:trilogy|duology|saga|quartet|quintet)\b', re.I),
    # "Box Set", "Boxed Set", "Complete Series", "Books 1-3"
    re.compile(r'\bbox\s*(?:ed)?\s*set\b', re.I),
    re.compile(r'\bcomplete\s*series\b', re.I),
    re.compile(r'\bbooks?\s*\d+\s*[-–—]\s*\d+\b', re.I),
    # "(Series Name, #1)" pattern from Goodreads-style titles
    re.compile(r'\(\s*[^)]+,\s*#\s*\d+\s*\)', re.I),
    # "Book One of the X Series"
    re.compile(r'\bof\s+the\s+.+?\s+series\b', re.I),
    # ": A <subgenre> Novel" often indicates series
    re.compile(r':\s*a\s+\w+\s+(?:novel|romance|story)\s*$', re.I),
]

BOX_SET_INDICATORS = [
    re.compile(r'\bbox\s*(?:ed)?\s*set\b', re.I),
    re.compile(r'\bcomplete\s*series\b', re.I),
    re.compile(r'\bcollection\b', re.I),
    re.compile(r'\banthology\b', re.I),
    re.compile(r'\bbooks?\s*\d+\s*[-–—]\s*\d+\b', re.I),
    re.compile(r'\bomnibus\b', re.I),
]

def detect_series_from_title(title, first_book_name=""):
    """Check if the title or first book name has series indicators."""
    combined = f"{title} {first_book_name}"

    for pattern in SERIES_PATTERNS:
        if pattern.search(combined):
            return True, f"title_pattern: {pattern.pattern[:40]}"

    return False, ""


def detect_box_set(title, first_book_name=""):
    """Check if entry is a box set (implies series)."""
    combined = f"{title} {first_book_name}"
    for pattern in BOX_SET_INDICATORS:
        if pattern.search(combined):
            return True
    return False


# ══════════════════════════════════════════════════════════
#  LAYER 2: Author Clustering (instant)
# ══════════════════════════════════════════════════════════

def cluster_by_author(df):
    """Group entries by author — same author with multiple titles = likely series."""
    author_groups = defaultdict(list)

    for idx, row in df.iterrows():
        author = str(row.get("Author Name", "")).lower().strip()
        if author and author != "nan":
            author_groups[author].append({
                "idx": idx,
                "title": str(row.get("Book Series Name", "")),
                "first_book": str(row.get("First Book Name", "")),
            })

    # Find authors with 3+ entries
    series_candidates = {}
    for author, entries in author_groups.items():
        if len(entries) >= 3:
            # Check if titles are similar (might be same series listed multiple times)
            # or different (multiple series by same author)
            titles = [e["title"].lower().strip() for e in entries]

            # Simple similarity: check if any titles share words
            # Group titles that share 2+ significant words
            groups = _group_similar_titles(titles, entries)

            for group in groups:
                if len(group) >= 2:  # 2+ related titles = likely a series
                    for entry in group:
                        series_candidates[entry["idx"]] = {
                            "method": "author_cluster",
                            "reason": f"Author '{author}' has {len(group)} related titles",
                            "estimated_books": len(group),
                        }

    return series_candidates


def _group_similar_titles(titles, entries):
    """Group titles by word similarity."""
    # Extract significant words (>3 chars, not common words)
    STOP_WORDS = {"the", "and", "for", "with", "from", "that", "this", "book",
                  "novel", "romance", "story", "series", "volume", "part"}

    def sig_words(t):
        words = set(re.findall(r'\b[a-z]{4,}\b', t.lower()))
        return words - STOP_WORDS

    groups = []
    used = set()

    for i, entry_i in enumerate(entries):
        if i in used:
            continue
        group = [entry_i]
        used.add(i)
        words_i = sig_words(entry_i["title"])

        for j, entry_j in enumerate(entries):
            if j in used:
                continue
            words_j = sig_words(entry_j["title"])
            # 2+ shared significant words = related
            if len(words_i & words_j) >= 2:
                group.append(entry_j)
                used.add(j)

        groups.append(group)

    # Also add ungrouped entries as single-item groups
    for i, entry in enumerate(entries):
        if i not in used:
            groups.append([entry])

    return groups


# ══════════════════════════════════════════════════════════
#  LAYER 3: Google Books API Series Check (fast, free)
# ══════════════════════════════════════════════════════════

def check_google_books_series(title, author):
    """Check Google Books API for series metadata."""
    query = f'intitle:"{title}" inauthor:"{author}"'
    base_url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": query,
        "maxResults": 5,
        "printType": "books",
        "langRestrict": "en",
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BookResearchTool/3.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            items = data.get("items", [])

            for item in items:
                vol = item.get("volumeInfo", {})

                # Check for series info in various places
                # 1. seriesInfo field (if present)
                series_info = item.get("volumeInfo", {}).get("seriesInfo", {})
                if series_info:
                    return {
                        "is_series": True,
                        "series_name": series_info.get("shortSeriesBookTitle", ""),
                        "book_position": series_info.get("bookDisplayNumber", ""),
                        "method": "google_books_seriesInfo",
                    }

                # 2. Check title for series pattern
                vol_title = vol.get("title", "")
                vol_subtitle = vol.get("subtitle", "")
                full_title = f"{vol_title}: {vol_subtitle}" if vol_subtitle else vol_title

                is_series, pattern = detect_series_from_title(full_title)
                if is_series:
                    return {
                        "is_series": True,
                        "series_name": "",
                        "book_position": "",
                        "method": f"google_books_title_pattern ({pattern})",
                    }

                # 3. Check description for series mentions
                description = vol.get("description", "")
                if description:
                    series_mentions = re.findall(
                        r'(?:book|volume|part)\s+(?:\d+|one|two|three|four|five)\s+(?:in|of)\s+(?:the\s+)?(.+?)(?:\s+series)',
                        description, re.I
                    )
                    if series_mentions:
                        return {
                            "is_series": True,
                            "series_name": series_mentions[0].strip(),
                            "book_position": "",
                            "method": "google_books_description",
                        }

                # 4. Check categories for "Series" keyword
                categories = vol.get("categories", [])
                if any("series" in c.lower() for c in categories):
                    return {
                        "is_series": True,
                        "series_name": "",
                        "book_position": "",
                        "method": "google_books_category",
                    }

                # 5. Check if there are multiple volumes by this author with similar title
                page_count = vol.get("pageCount", 0)

    except Exception as e:
        pass  # Silent fail — we'll try other methods

    return None


# ══════════════════════════════════════════════════════════
#  LAYER 4: Goodreads Series Check via Playwright (accurate)
# ══════════════════════════════════════════════════════════

async def create_stealth_context(browser):
    """Create a stealth browser context."""
    context = await browser.new_context(
        user_agent=random.choice(USER_AGENTS),
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


async def safe_goto(page, url, timeout=45000, retries=3):
    """Network-resilient navigation."""
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await asyncio.sleep(random.uniform(0.5, 1.5))
            return True
        except Exception as e:
            await asyncio.sleep(2 * attempt + random.uniform(1.0, 3.0))
    return False


async def search_goodreads(page, title, author):
    """Search Goodreads for a book and return the first matching link."""
    try:
        clean_author = author if author and author.lower() not in [
            'kindle edition', 'audible audiobook', 'paperback', 'hardcover', '', 'nan'
        ] else ''
        query = f"{title} {clean_author}".strip()
        url = f"https://www.goodreads.com/search?q={urllib.parse.quote_plus(query)}"

        if not await safe_goto(page, url):
            return None

        await asyncio.sleep(random.uniform(1.0, 2.0))

        items = await page.query_selector_all("tr[itemtype='http://schema.org/Book']")
        if not items:
            return None

        first_item = items[0]
        title_el = await first_item.query_selector("a.bookTitle")
        if not title_el:
            return None

        href = await title_el.get_attribute("href")
        gr_link = "https://www.goodreads.com" + href
        return gr_link
    except Exception:
        return None


async def check_goodreads_series(page, gr_url):
    """Check if a Goodreads book is part of a series. Returns series info."""
    try:
        if not await safe_goto(page, gr_url):
            return None

        await asyncio.sleep(random.uniform(1.5, 3.0))

        # Check for series link on the book page
        series_el = await page.query_selector("h3.Text__italic a")
        if not series_el:
            series_el = await page.query_selector("div.BookPageTitleSection a[href*='/series/']")

        if series_el:
            series_text = (await series_el.text_content()).strip()
            series_href = await series_el.get_attribute("href")

            # Parse "Series Name #3"
            m = re.match(r'(.+?)(?:\s*#(\d+\.?\d*))?$', series_text)
            series_name = m.group(1).strip() if m else series_text
            book_number = m.group(2) if m and m.group(2) else ""

            series_url = ""
            if series_href:
                series_url = ("https://www.goodreads.com" + series_href
                             if series_href.startswith('/') else series_href)

            # Try to get total books from series page
            total_books = None
            if series_url:
                total_books = await get_series_book_count(page, series_url)

            return {
                "is_series": True,
                "series_name": series_name,
                "book_number": book_number,
                "series_url": series_url,
                "total_books": total_books,
                "method": "goodreads",
            }

        return {"is_series": False, "method": "goodreads"}

    except Exception as e:
        return None


async def get_series_book_count(page, series_url):
    """Navigate to a Goodreads series page and count the books."""
    try:
        if not await safe_goto(page, series_url):
            return None

        await asyncio.sleep(random.uniform(1.5, 2.5))

        # Method 1: Count book entries on the series page
        book_items = await page.query_selector_all("div.listWithDividers__item")
        if book_items:
            # Filter to primary entries (numbered books, not 0.5, novellas etc.)
            count = 0
            for item in book_items:
                # Check for book number
                num_el = await item.query_selector("h3")
                if num_el:
                    num_text = (await num_el.text_content()).strip()
                    # Match "Book 1", "1", etc. — skip "0.5", "1.5"
                    if re.match(r'^(?:Book\s+)?(\d+)$', num_text):
                        count += 1
            if count > 0:
                return count

        # Method 2: Check the series heading for "X primary works"
        heading_el = await page.query_selector("div.responsiveSeriesHeader__subtitle")
        if heading_el:
            heading_text = (await heading_el.text_content()).strip()
            m = re.search(r'(\d+)\s+primary\s+works?', heading_text)
            if m:
                return int(m.group(1))

        # Method 3: Count all items as fallback
        if book_items:
            return len(book_items)

        return None
    except Exception:
        return None


async def gr_worker(worker_id, browser, queue, results, save_callback):
    """Worker that processes Goodreads lookups."""
    context = await create_stealth_context(browser)
    page = await context.new_page()
    processed = 0

    try:
        while True:
            try:
                item = queue.pop(0)
            except IndexError:
                break

            idx = item["idx"]
            title = item["title"]
            author = item["author"]

            try:
                # Search for the book
                gr_url = await search_goodreads(page, title, author)

                if gr_url:
                    series_info = await check_goodreads_series(page, gr_url)
                    if series_info:
                        results[idx] = series_info
                        if series_info.get("is_series"):
                            log.info(f"    [W{worker_id}] SERIES FOUND: '{title}' -> "
                                    f"'{series_info.get('series_name', '')}' "
                                    f"({series_info.get('total_books', '?')} books)")
                else:
                    results[idx] = {"is_series": False, "method": "goodreads_not_found"}

                processed += 1

                # Rotate context periodically
                if processed % CONTEXT_ROTATION_INTERVAL == 0:
                    await page.close()
                    await context.close()
                    context = await create_stealth_context(browser)
                    page = await context.new_page()
                    log.info(f"    [W{worker_id}] Context rotated after {processed} lookups")

                # Save periodically
                if processed % 50 == 0:
                    save_callback()
                    log.info(f"    [W{worker_id}] Progress: {processed} done, "
                            f"{len(queue)} remaining in queue")

                await asyncio.sleep(random.uniform(GR_SLEEP_MIN, GR_SLEEP_MAX))

            except Exception as e:
                log.warning(f"    [W{worker_id}] Error for '{title}': {e}")
                results[idx] = {"is_series": None, "method": "error"}
                await asyncio.sleep(3)

    finally:
        await page.close()
        await context.close()


# ══════════════════════════════════════════════════════════
#  MAIN PIPELINE
# ══════════════════════════════════════════════════════════

def run_verification():
    """Run the 4-layer series verification pipeline."""
    log.info("=" * 70)
    log.info("  SERIES VERIFICATION PIPELINE")
    log.info("  Verifying standalone/1-book entries before cleanup")
    log.info("=" * 70)

    # Load data — use the latest available file
    candidates = [
        DATA_DIR / "selfpub_master_mega_expanded.csv",
        DATA_DIR / "selfpub_master_multi_platform.csv",
        DATA_DIR / "selfpub_master_cleaned.csv",
        DATA_DIR / "selfpub_master_expanded_v2.csv",
    ]

    source = None
    for c in candidates:
        if c.exists():
            source = c
            break

    if not source:
        log.error("No data file found!")
        return

    df = pd.read_csv(source, on_bad_lines="skip")
    log.info(f"  Loaded {len(df)} entries from {source.name}")

    # Add verification columns if not present
    if "Series_Verified" not in df.columns:
        df["Series_Verified"] = ""
    if "Verification_Method" not in df.columns:
        df["Verification_Method"] = ""
    if "Verified_Series_Name" not in df.columns:
        df["Verified_Series_Name"] = ""
    if "Verified_Books_Count" not in df.columns:
        df["Verified_Books_Count"] = np.nan

    # Identify entries needing verification
    def needs_verification(row):
        """Check if entry needs series verification."""
        count = None
        val = row.get("Books in Series")
        if pd.notna(val):
            try:
                count = int(float(val))
            except (ValueError, TypeError):
                pass

        # Already verified
        if str(row.get("Series_Verified", "")).strip().lower() in ["yes", "true", "verified"]:
            return False

        # Already has 3+ books confirmed
        if count is not None and count >= 3:
            return False

        return True

    needs_check = df[df.apply(needs_verification, axis=1)].copy()
    already_ok = len(df) - len(needs_check)

    log.info(f"  Already confirmed (3+ books): {already_ok}")
    log.info(f"  Need verification: {len(needs_check)}")

    # Distribution of what needs checking
    check_counts = {"0 or unknown": 0, "1 book": 0, "2 books": 0}
    for _, row in needs_check.iterrows():
        val = row.get("Books in Series")
        try:
            c = int(float(val))
            if c == 1:
                check_counts["1 book"] += 1
            elif c == 2:
                check_counts["2 books"] += 1
            else:
                check_counts["0 or unknown"] += 1
        except:
            check_counts["0 or unknown"] += 1

    for k, v in check_counts.items():
        log.info(f"    {k}: {v}")

    # ── LAYER 1: Title Pattern Matching ───────────────────
    log.info(f"\n  LAYER 1: Title Pattern Matching")

    pattern_matches = 0
    box_set_count = 0

    for idx, row in needs_check.iterrows():
        title = str(row.get("Book Series Name", ""))
        first_book = str(row.get("First Book Name", ""))

        is_series, reason = detect_series_from_title(title, first_book)
        if is_series:
            df.at[idx, "Series_Verified"] = "Yes"
            df.at[idx, "Verification_Method"] = reason
            pattern_matches += 1

            # If it's a box set, estimate higher book count
            if detect_box_set(title, first_book):
                # Box sets typically have 3-6 books
                current = df.at[idx, "Books in Series"]
                try:
                    current_count = int(float(current)) if pd.notna(current) else 0
                except:
                    current_count = 0
                if current_count < 3:
                    df.at[idx, "Books in Series"] = 3  # Conservative estimate
                    df.at[idx, "Verified_Books_Count"] = 3
                box_set_count += 1

    log.info(f"    Pattern matches: {pattern_matches}")
    log.info(f"    Box sets detected: {box_set_count}")

    # Update needs_check (remove already verified)
    still_needs = df[
        df.apply(needs_verification, axis=1) &
        (df["Series_Verified"] != "Yes")
    ]
    log.info(f"    Still need verification: {len(still_needs)}")

    # ── LAYER 2: Author Clustering ────────────────────────
    log.info(f"\n  LAYER 2: Author Clustering")

    author_matches = cluster_by_author(df)
    author_verified = 0

    for idx, info in author_matches.items():
        if df.at[idx, "Series_Verified"] != "Yes":
            df.at[idx, "Series_Verified"] = "Likely"
            df.at[idx, "Verification_Method"] = info["method"]

            # Update book count if we found more related titles
            current = df.at[idx, "Books in Series"]
            try:
                current_count = int(float(current)) if pd.notna(current) else 0
            except:
                current_count = 0

            estimated = info.get("estimated_books", 0)
            if estimated > current_count:
                df.at[idx, "Verified_Books_Count"] = estimated

            author_verified += 1

    log.info(f"    Author cluster matches: {author_verified}")

    # ── LAYER 3: Google Books API ─────────────────────────
    log.info(f"\n  LAYER 3: Google Books API Series Check")

    # Get entries still needing verification
    unverified_mask = (
        df.apply(needs_verification, axis=1) &
        (~df["Series_Verified"].isin(["Yes"]))
    )
    unverified = df[unverified_mask]
    log.info(f"    Checking {len(unverified)} entries via Google Books API")

    google_verified = 0
    google_checked = 0

    for idx, row in unverified.iterrows():
        title = str(row.get("Book Series Name", ""))
        author = str(row.get("Author Name", ""))

        if not title or title == "nan" or not author or author == "nan":
            continue

        result = check_google_books_series(title, author)
        google_checked += 1

        if result and result.get("is_series"):
            df.at[idx, "Series_Verified"] = "Yes"
            df.at[idx, "Verification_Method"] = result.get("method", "google_books")
            if result.get("series_name"):
                df.at[idx, "Verified_Series_Name"] = result["series_name"]
            google_verified += 1

        if google_checked % 100 == 0:
            log.info(f"      Checked {google_checked}/{len(unverified)}, "
                    f"found {google_verified} series so far")

        time.sleep(GOOGLE_BOOKS_RATE_LIMIT)

        # Save intermediate results every 500
        if google_checked % 500 == 0:
            intermediate = DATA_DIR / "selfpub_master_series_verified_partial.csv"
            df.to_csv(intermediate, index=False)
            log.info(f"      Intermediate save: {intermediate.name}")

    log.info(f"    Google Books checked: {google_checked}")
    log.info(f"    Google Books confirmed series: {google_verified}")

    # Save after Layer 3 (before slow Goodreads step)
    pre_gr_output = DATA_DIR / "selfpub_master_pre_goodreads.csv"
    df.to_csv(pre_gr_output, index=False)
    log.info(f"    Saved pre-Goodreads: {pre_gr_output.name}")

    # ── LAYER 4: Goodreads Series Check ───────────────────
    log.info(f"\n  LAYER 4: Goodreads Series Check (Playwright)")

    # Get remaining unverified entries
    still_unverified = df[
        df.apply(needs_verification, axis=1) &
        (~df["Series_Verified"].isin(["Yes"]))
    ]
    log.info(f"    Remaining unverified: {len(still_unverified)}")

    if len(still_unverified) > 0:
        # Build queue for Goodreads workers
        gr_queue = []
        for idx, row in still_unverified.iterrows():
            title = str(row.get("Book Series Name", ""))
            author = str(row.get("Author Name", ""))
            first_book = str(row.get("First Book Name", ""))

            if not title or title == "nan":
                continue

            # Use first book name if available for better search results
            search_title = first_book if first_book and first_book != "nan" else title

            gr_queue.append({
                "idx": idx,
                "title": search_title,
                "author": author if author != "nan" else "",
            })

        log.info(f"    Goodreads queue: {len(gr_queue)} entries")

        gr_results = {}

        def save_callback():
            """Save intermediate results during GR scraping."""
            for gidx, ginfo in gr_results.items():
                if ginfo and ginfo.get("is_series"):
                    df.at[gidx, "Series_Verified"] = "Yes"
                    df.at[gidx, "Verification_Method"] = "goodreads"
                    if ginfo.get("series_name"):
                        df.at[gidx, "Verified_Series_Name"] = ginfo["series_name"]
                    if ginfo.get("total_books"):
                        df.at[gidx, "Verified_Books_Count"] = ginfo["total_books"]
                        # Update Books in Series if GR gives us a better count
                        current = df.at[gidx, "Books in Series"]
                        try:
                            current_count = int(float(current)) if pd.notna(current) else 0
                        except:
                            current_count = 0
                        if ginfo["total_books"] > current_count:
                            df.at[gidx, "Books in Series"] = ginfo["total_books"]
                elif ginfo and ginfo.get("is_series") is False:
                    df.at[gidx, "Series_Verified"] = "No"
                    df.at[gidx, "Verification_Method"] = "goodreads_confirmed_standalone"

            df.to_csv(DATA_DIR / "selfpub_master_series_verified_partial.csv", index=False)

        # Run async Goodreads workers
        asyncio.run(_run_gr_workers(gr_queue, gr_results, save_callback))

        # Apply final GR results
        save_callback()

        gr_series_found = sum(1 for v in gr_results.values() if v and v.get("is_series"))
        gr_standalone = sum(1 for v in gr_results.values() if v and v.get("is_series") is False)
        gr_errors = sum(1 for v in gr_results.values() if v is None or v.get("is_series") is None)

        log.info(f"    Goodreads results:")
        log.info(f"      Confirmed SERIES: {gr_series_found}")
        log.info(f"      Confirmed STANDALONE: {gr_standalone}")
        log.info(f"      Errors/unknown: {gr_errors}")

    # ── FINAL SUMMARY ─────────────────────────────────────
    log.info(f"\n  {'='*60}")
    log.info(f"  VERIFICATION SUMMARY")
    log.info(f"  {'='*60}")

    verified_yes = (df["Series_Verified"] == "Yes").sum()
    verified_likely = (df["Series_Verified"] == "Likely").sum()
    verified_no = (df["Series_Verified"] == "No").sum()
    not_checked = ((df["Series_Verified"] == "") | df["Series_Verified"].isna()).sum()
    already_3plus = already_ok

    log.info(f"  Total entries: {len(df)}")
    log.info(f"  Already confirmed (3+ books): {already_3plus}")
    log.info(f"  Verified as SERIES: {verified_yes}")
    log.info(f"  Likely SERIES (author cluster): {verified_likely}")
    log.info(f"  Confirmed STANDALONE: {verified_no}")
    log.info(f"  Unchecked/unknown: {not_checked}")

    # Verification method breakdown
    log.info(f"\n  Verification methods used:")
    for method, count in df["Verification_Method"].value_counts().items():
        if method and str(method) != "nan":
            log.info(f"    {method}: {count}")

    # Updated book count distribution
    log.info(f"\n  Updated book count distribution:")
    valid_counts = []
    for _, row in df.iterrows():
        val = row.get("Books in Series")
        try:
            c = int(float(val))
            if c > 0:
                valid_counts.append(c)
        except:
            pass

    if valid_counts:
        log.info(f"    1 book: {sum(1 for c in valid_counts if c == 1)}")
        log.info(f"    2 books: {sum(1 for c in valid_counts if c == 2)}")
        log.info(f"    3-5 books: {sum(1 for c in valid_counts if 3 <= c <= 5)}")
        log.info(f"    6-10 books: {sum(1 for c in valid_counts if 6 <= c <= 10)}")
        log.info(f"    11+ books: {sum(1 for c in valid_counts if c >= 11)}")

    # Entries SAFE from cleanup (verified series or 3+ books)
    safe_count = 0
    for idx, row in df.iterrows():
        books = None
        try:
            books = int(float(row.get("Books in Series", 0)))
        except:
            pass

        is_verified_series = row.get("Series_Verified") in ["Yes", "Likely"]
        has_3plus = books is not None and books >= 3

        if has_3plus or is_verified_series:
            safe_count += 1

    log.info(f"\n  Entries SAFE from cleanup: {safe_count}")
    log.info(f"  Entries at risk of removal: {len(df) - safe_count}")

    # Save final output
    output = DATA_DIR / "selfpub_master_series_verified.csv"
    df.to_csv(output, index=False)
    log.info(f"\n  Saved to: {output}")

    return df


async def _run_gr_workers(queue, results, save_callback):
    """Launch parallel Goodreads workers."""
    from playwright.async_api import async_playwright

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)

        # Split queue roughly evenly
        workers = []
        for i in range(min(WORKER_COUNT, len(queue))):
            workers.append(
                gr_worker(i, browser, queue, results, save_callback)
            )

        log.info(f"    Launching {len(workers)} Goodreads workers...")
        await asyncio.gather(*workers)

        await browser.close()


if __name__ == "__main__":
    start = datetime.now()
    run_verification()
    elapsed = datetime.now() - start
    hours = elapsed.total_seconds() / 3600
    log.info(f"\n  Verification completed in {elapsed} ({hours:.1f} hours)")
