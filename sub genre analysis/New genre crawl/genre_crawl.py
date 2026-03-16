#!/usr/bin/env python3
"""
GENRE CRAWL — Phase 1: Amazon Discovery
Crawls Amazon bestseller lists and search pages to discover books for a given subgenre.
Produces a raw book-level CSV with titles, authors, series, Amazon links, and bestseller ranks.
"""

import asyncio
import csv
import os
import random
import re
import sys
import time
from datetime import datetime
from urllib.parse import quote_plus, urljoin

import pandas as pd
from loguru import logger
from playwright.async_api import async_playwright

# ============================================================================
# CONFIGURATION
# ============================================================================

MAX_SEARCH_PAGES = 20        # Max pages to crawl per search URL
MAX_BESTSELLER_PAGES = 2     # Bestseller lists are typically 2 pages (100 items)
SLEEP_MIN = 3                # Minimum random delay between pages
SLEEP_MAX = 7                # Maximum random delay between pages
HEADLESS = True              # Set False for debugging
SAVE_INTERVAL = 5            # Save progress every N pages
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# Stealth user agents
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Irrelevant keywords for filtering out non-book / unrelated results
IRRELEVANT_KEYWORDS = [
    'coloring book', 'activity book', 'journal', 'notebook', 'planner',
    'calendar', 'workbook', 'puzzle', 'crossword', 'sudoku', 'sticker',
    'cookbook', 'recipe', 'board game', 'card game', 'manga', 'anime',
    'comic strip', 'graphic novel', 'how to', 'textbook', 'study guide',
]

# ============================================================================
# SUBGENRE URL MAPPING - Parsed from the CSV
# ============================================================================

SUBGENRE_URLS = {
    "Historic Fiction & Romance": {
        "bestseller": [
            "https://www.amazon.com/Best-Sellers-Kindle-Store-Historical-Fiction/zgbs/digital-text/157059011/ref=zg_bs_nav_digital-text_3_157028011",
            "https://www.amazon.com/gp/bestsellers/digital-text/157059011/ref=zg_bs?ie=UTF8&tf=1",
            "https://www.amazon.com/gp/bestsellers/digital-text/214873928011/ref=zg_bs_nav_digital-text_4_157059011?ie=UTF8&tf=1",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-19th-Century-Historical-Fiction/zgbs/digital-text/214873928011/ref=zg_bs",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-20th-Century-Historical-Fiction/zgbs/digital-text/120261031011/ref=zg_bs_nav_digital-text_4_214873928011",
            "https://www.amazon.com/gp/bestsellers/digital-text/120261031011/ref=zg_bs?ie=UTF8&tf=1",
            "https://www.amazon.com/gp/bestsellers/digital-text/7588819011/ref=zg_bs_nav_digital-text_4_157059011?ie=UTF8&tf=1",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-US-Historical-Fiction/zgbs/digital-text/7588819011/ref=zg_bs",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-Religious-Historical-Fiction/zgbs/digital-text/158437011/ref=zg_bs_nav_digital-text_4_7588819011",
            "https://www.amazon.com/gp/bestsellers/digital-text/158437011/ref=zg_bs?ie=UTF8&tf=1",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-Historical-British-Fiction/zgbs/digital-text/7588806011/ref=zg_bs_nav_digital-text_4_7588819011",
            "https://www.amazon.com/gp/bestsellers/digital-text/7588806011/ref=zg_bs?ie=UTF8&tf=1",
            "https://www.amazon.com/gp/bestsellers/digital-text/7588809011/ref=zg_bs_nav_digital-text_4_7588806011?ie=UTF8&tf=1",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-Historical-European-Fiction/zgbs/digital-text/7588809011/ref=zg_bs",
            "https://www.amazon.com/gp/bestsellers/books/13371/ref=pd_zg_hrsr_books",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-Historical-Mystery-Thriller-Suspense-Fiction/zgbs/digital-text/7588800011/ref=zg_bs_nav_digital-text_4_7588809011",
            "https://www.amazon.com/gp/bestsellers/digital-text/7588800011/ref=zg_bs?ie=UTF8&tf=1",
        ],
        "search": [
            "https://www.amazon.com/s?k=historic+fiction+books&i=digital-text&crid=7PGGFQ3MDB7X&sprefix=historic+fiction+bo%2Cdigital-text%2C315&ref=nb_sb_noss_2",
            "https://www.amazon.com/s?k=period+fiction+books&i=digital-text&crid=8JFDDEFUBJD&sprefix=period+fiction+books%2Cdigital-text%2C352&ref=nb_sb_noss_1",
            "https://www.amazon.com/s?k=historical+romance+books&i=digital-text&crid=3V0GGBBF989T7&sprefix=historical+romance+books%2Cdigital-text%2C319&ref=nb_sb_noss_1",
        ]
    },
    "Military Drama/Romance": {
        "bestseller": [
            "https://www.amazon.com/Best-Sellers-Military-Romance/zgbs/digital-text/6487836011",
            "https://www.amazon.com/gp/bestsellers/digital-text/6487836011/ref=zg_bs?ie=UTF8&tf=1",
            "https://www.amazon.com/Best-Sellers-Military-Thrillers/zgbs/digital-text/6361464011",
            "https://www.amazon.com/gp/bestsellers/digital-text/6361464011/ref=zg_bs?ie=UTF8&tf=1",
        ],
        "search": [
            "https://www.amazon.in/Military-Romance-Kindle-eBooks/s?rh=n%3A92041945031%2Cp_n_binding_browse-bin%3A1634951031&page=3",
            "https://www.amazon.com/s?k=military+romance+fiction+books&i=digital-text&crid=33RJRREOB9QNA&sprefix=military+romancee+fiction+books%2Cdigital-text%2C371&ref=nb_sb_noss",
        ]
    },
    "Political Drama/Romance": {
        "bestseller": [
            "https://www.amazon.com/gp/bestsellers/digital-text/6190490011/ref=zg_bs_nav_digital-text_4_6361464011?ie=UTF8&tf=1",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-Political-Thrillers-Suspense/zgbs/digital-text/6190490011/ref=zg_bs",
        ],
        "search": [
            "https://www.amazon.com/s?k=political+drama+fiction+books&i=digital-text&crid=13I3GJLJF7695&sprefix=political+dra+fiction+books%2Cdigital-text%2C378&ref=nb_sb_noss",
            "https://www.amazon.com/s?k=political+romance+fiction+books&i=digital-text&crid=2UUOSB9BUDSDG&sprefix=political+rom+fiction+books%2Cdigital-text%2C356&ref=nb_sb_noss",
        ]
    },
    "Small Town Drama/Romance": {
        "bestseller": [
            "https://www.amazon.com/Best-Sellers-Small-Town-Romance-eBooks/zgbs/digital-text/120220981011",
            "https://www.amazon.com/gp/bestsellers/digital-text/120220981011/ref=zg_bs?ie=UTF8&tf=1",
        ],
        "search": [
            "https://www.amazon.com/s?k=small+town+drama+fiction+books&i=digital-text&crid=3LNAUFFSHHVXN&sprefix=small+town+dramafiction+books%2Cdigital-text%2C354&ref=nb_sb_noss",
            "https://www.amazon.com/s?k=small+town+romance+fiction+books&i=digital-text&crid=2OLINQPUCPER7&sprefix=small+town+ro+fiction+books%2Cdigital-text%2C397&ref=nb_sb_noss",
        ]
    },
    "Christian Drama/Romance": {
        "bestseller": [
            "https://www.amazon.com/Best-Sellers-Christian-Romance/zgbs/digital-text/6190472011",
            "https://www.amazon.com/gp/bestsellers/digital-text/6190472011/ref=zg_bs?ie=UTF8&tf=1",
        ],
        "search": [
            "https://www.amazon.com/s?k=christian+romance+fiction+books&i=digital-text&crid=39E68I3W8EK7K&sprefix=christianromance+fiction+books%2Cdigital-text%2C321&ref=nb_sb_noss",
            "https://www.amazon.com/s?k=christian+drama+fiction+books&i=digital-text&crid=36I2UAHWIG2HY&sprefix=christian+dram+fiction+books%2Cdigital-text%2C357&ref=nb_sb_noss",
        ]
    },
    "Mafia Drama/Romance": {
        "bestseller": [
            "https://www.amazon.in/gp/bestsellers/books/92041939031",
            "https://www.amazon.com/Best-Sellers-Mafia-Romance/zgbs/books/120214348011",
            "https://www.amazon.com/Best-Sellers-Mafia-Romance-eBooks/zgbs/digital-text/120220975011",
            "https://www.amazon.com/gp/bestsellers/digital-text/120220975011/ref=zg_bs?ie=UTF8&tf=1",
        ],
        "search": [
            "https://www.amazon.com/s?k=mafia+drama+fiction+books&i=digital-text&crid=2QC3MSYKI7KRC&sprefix=maf+drama+fiction+books%2Cdigital-text%2C372&ref=nb_sb_noss",
            "https://www.amazon.com/s?k=mafia+romance+fiction+books&i=digital-text&crid=1T5MNAAJ1P7KS&sprefix=mafia+roma+fiction+books%2Cdigital-text%2C350&ref=nb_sb_noss",
        ]
    },
    "Dark Romance": {
        "bestseller": [
            "https://www.amazon.in/gp/bestsellers/books/211758048031",
            "https://www.amazon.com/Best-Sellers-Kindle-Store-Dark-Romance/zgbs/digital-text/214873907011/ref=zg_bs_nav_digital-text_3_158566011",
            "https://www.amazon.com/gp/bestsellers/digital-text/214873907011/ref=zg_bs?ie=UTF8&tf=1",
        ],
        "search": [
            "https://www.amazon.com/s?k=dark+romance+fiction+books&i=digital-text&crid=3IH89OF82B1I1&sprefix=dar+romance+fiction+books%2Cdigital-text%2C357&ref=nb_sb_noss",
        ]
    },
    "Forbidden Romance": {
        "bestseller": [],
        "search": [
            "https://www.amazon.com/forbidden-romance-books/s?k=forbidden+romance+books",
            "https://www.amazon.in/Romance-Forbidden-Kindle-eBooks/s?rh=n%3A1637156031%2Cp_73%3AForbidden",
        ]
    },
    "Romantic Suspense/Psychological Thriller": {
        "bestseller": [
            "https://www.amazon.com/Best-Sellers-Romantic-Suspense/zgbs/digital-text/158574011",
            "https://www.amazon.com/gp/bestsellers/digital-text/158574011/ref=zg_bs?ie=UTF8&tf=1",
        ],
        "search": [
            "https://www.amazon.in/s?k=romantic+suspense+fiction+books&rh=n%3A1637156031&ref=nb_sb_noss",
            "https://www.amazon.in/Kindle-Store-Romantic-Thriller-Series/s?rh=n%3A1571277031%2Cp_73%3ARomantic%2BThriller%2BSeries",
            "https://www.amazon.in/s?k=psychological+thriller+romance+fiction+books&rh=n%3A1637156031&ref=nb_sb_noss",
        ]
    },
}


# ============================================================================
# HELPERS
# ============================================================================

def normalize_title_author(title, author):
    """Normalize title+author for deduplication."""
    t = re.sub(r'[^a-z0-9\s]', '', str(title).lower()).strip()
    a = re.sub(r'[^a-z0-9\s]', '', str(author).lower()).strip()
    # Remove common suffixes
    t = re.sub(r'\s*(a novel|book \d+|volume \d+|series|edition).*$', '', t)
    return f"{t}|||{a}"


def is_relevant(title, subgenre_name):
    """Basic relevance filter: removes non-book items."""
    t_lower = str(title).lower()
    # Filter out non-book items
    for kw in IRRELEVANT_KEYWORDS:
        if kw in t_lower:
            return False
    return True


def extract_list_name(url):
    """Extract a human-readable list name from the Amazon URL."""
    if '/zgbs/' in url:
        # e.g. Best-Sellers-Kindle-Store-Political-Thrillers-Suspense
        parts = url.split('/')
        for i, p in enumerate(parts):
            if p in ('zgbs', 'bestsellers') and i + 1 < len(parts):
                # Get the category segment
                cat_parts = [pp for pp in parts[i+1:] if pp and not pp.startswith('ref=') and not pp.isdigit() and 'digital-text' not in pp and 'books' != pp]
                if cat_parts:
                    name = cat_parts[0].replace('-', ' ').replace('eBooks', '').strip()
                    return name
    if 'gp/bestsellers' in url:
        if 'tf=1' in url:
            return "Free List"
        return "Paid List"
    return "Bestseller List"


async def create_stealth_context(playwright_browser):
    """Create a stealth browser context with anti-detection measures."""
    ua = random.choice(USER_AGENTS)
    context = await playwright_browser.new_context(
        user_agent=ua,
        viewport={"width": 1920, "height": 1080},
        locale="en-US",
        timezone_id="America/New_York",
        java_script_enabled=True,
        extra_http_headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        }
    )
    # Anti-detection scripts
    await context.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
        Object.defineProperty(navigator, 'languages', { get: () => ['en-US', 'en'] });
        window.chrome = { runtime: {} };
    """)
    return context


async def safe_goto(page, url, timeout=45000, retries=3):
    """Network-resilient navigation with retry."""
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await asyncio.sleep(random.uniform(1.0, 2.0))
            return True
        except Exception as e:
            logger.debug(f"  goto attempt {attempt}/{retries} failed: {e}")
            await asyncio.sleep(2 * attempt + random.uniform(1.0, 3.0))
    return False


# ============================================================================
# BESTSELLER LIST CRAWLER
# ============================================================================

async def crawl_bestseller_page(page, url, list_name, subgenre_name):
    """Crawl a single Amazon bestseller page and extract all items."""
    books = []
    logger.info(f"  📋 Crawling bestseller: {list_name} — {url[:80]}...")
    
    if not await safe_goto(page, url):
        logger.warning(f"  ❌ Failed to load: {url[:80]}")
        return books
    
    await asyncio.sleep(random.uniform(2.0, 4.0))
    
    # Detect if this is a zgbs page
    is_zgbs = '/zgbs/' in url or '/bestsellers/' in url
    
    if is_zgbs:
        # --- Strategy: Extract from bestseller grid ---
        items = await page.query_selector_all("div.zg-grid-general-faceout, div[id^='gridItemRoot']")
        if not items:
            # Fallback: try alternate selectors
            items = await page.query_selector_all("li.zg-item-immersion, div.a-section.a-spacing-none.aok-relative")
        
        logger.info(f"    Found {len(items)} items on page")
        
        for idx, item in enumerate(items):
            try:
                rank = idx + 1
                
                # Title
                title_el = await item.query_selector("div._cDEzb_p13n-sc-css-line-clamp-3_g3dy1, span.zg-text-center-align a span div, a.a-link-normal span div")
                if not title_el:
                    title_el = await item.query_selector("a.a-link-normal span")
                title = (await title_el.text_content()).strip() if title_el else None
                
                # Author
                author_el = await item.query_selector("div.a-row.a-size-small span.a-size-small, span.a-size-small.a-color-base")
                author = (await author_el.text_content()).strip() if author_el else ""
                
                # Link
                link_el = await item.query_selector("a.a-link-normal[href*='/dp/'], a.a-link-normal[href*='/product/']")
                if not link_el:
                    link_el = await item.query_selector("a.a-link-normal")
                href = await link_el.get_attribute("href") if link_el else None
                amazon_link = ""
                if href:
                    base = "https://www.amazon.com" if 'amazon.com' in url else "https://www.amazon.in"
                    amazon_link = urljoin(base, href.split("?")[0])
                
                # Rank text
                rank_el = await item.query_selector("span.zg-bdg-text, span.a-badge-text")
                if rank_el:
                    rank_text = (await rank_el.text_content()).strip().replace('#', '')
                    try:
                        rank = int(rank_text)
                    except:
                        pass
                
                if title and is_relevant(title, subgenre_name):
                    books.append({
                        "Book Name": title,
                        "Author Name": author.replace("by ", "").strip() if author else "",
                        "Amazon Link": amazon_link,
                        "Series Name": "",
                        "Source": "Bestseller",
                        "Source Detail": f"#{rank} in {list_name}",
                        "Subgenre": subgenre_name,
                    })
            except Exception as e:
                logger.debug(f"    Error extracting item {idx}: {e}")
    
    # Check for "next page" in bestseller (page 2)
    next_btn = await page.query_selector("li.a-last a, ul.a-pagination li.a-last a")
    if next_btn:
        next_href = await next_btn.get_attribute("href")
        if next_href:
            await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            base = "https://www.amazon.com" if 'amazon.com' in url else "https://www.amazon.in"
            next_url = urljoin(base, next_href)
            logger.info(f"    → Bestseller page 2...")
            page2_books = await crawl_bestseller_page(page, next_url, list_name + " (p2)", subgenre_name)
            # Update ranks for page 2
            for b in page2_books:
                if b["Source Detail"].startswith("#"):
                    old_rank = int(re.search(r'#(\d+)', b["Source Detail"]).group(1))
                    b["Source Detail"] = f"#{old_rank + 50} in {list_name}"
            books.extend(page2_books)
    
    logger.info(f"    ✓ Extracted {len(books)} books from {list_name}")
    return books


# ============================================================================
# SEARCH RESULTS CRAWLER
# ============================================================================

async def crawl_search_page(page, url, subgenre_name, max_pages=MAX_SEARCH_PAGES):
    """Crawl Amazon search results with pagination."""
    all_books = []
    current_url = url
    consecutive_empty = 0
    
    for page_num in range(1, max_pages + 1):
        logger.info(f"  🔍 Search page {page_num}/{max_pages}: {current_url[:80]}...")
        
        if not await safe_goto(page, current_url):
            logger.warning(f"  ❌ Failed to load search page {page_num}")
            break
        
        # Wait for results to render
        try:
            await page.wait_for_selector(
                "div[data-component-type='s-search-result'], div.s-result-item.s-asin",
                timeout=15000
            )
        except:
            logger.debug(f"    Timeout waiting for search results")
        
        await asyncio.sleep(random.uniform(2.0, 4.0))
        
        # Scroll down to trigger lazy loading
        for _ in range(3):
            await page.evaluate("window.scrollBy(0, window.innerHeight)")
            await asyncio.sleep(0.5)
        
        # Check for CAPTCHA
        captcha = await page.query_selector("form[action*='validateCaptcha'], input#captchacharacters")
        if captcha:
            logger.warning(f"  🤖 CAPTCHA detected on page {page_num}! Waiting 30s for manual solve...")
            await asyncio.sleep(30)
        
        # Extract search results — try multiple selector strategies
        items = await page.query_selector_all("div[data-component-type='s-search-result']")
        if not items:
            items = await page.query_selector_all("div.s-result-item.s-asin")
        if not items:
            items = await page.query_selector_all("div.s-result-item[data-asin]:not([data-asin=''])")
        
        logger.info(f"    Found {len(items)} result containers on page {page_num}")
        
        if not items:
            logger.info(f"    No results found on page {page_num}, stopping.")
            break
        
        page_books = 0
        for item in items:
            try:
                # Get ASIN to verify it's a real product
                asin = await item.get_attribute("data-asin")
                if not asin or asin.strip() == '':
                    continue
                
                # Skip sponsored/ad results
                ad_badge = await item.query_selector("span.puis-label-popover-default, span.s-label-popover-default")
                is_sponsored = bool(ad_badge)
                
                # Title — try multiple selectors
                title = None
                for title_sel in [
                    "h2 a.a-link-normal span.a-text-normal",
                    "h2 a span",
                    "h2 span",
                    "h2 a.a-link-normal",
                ]:
                    title_el = await item.query_selector(title_sel)
                    if title_el:
                        title = (await title_el.text_content()).strip()
                        if title:
                            break
                
                if not title:
                    logger.debug(f"    Skipping item {asin}: no title found")
                    continue
                
                # Author — try multiple approaches
                author = ""
                # Strategy 1: Links to author pages
                author_links = await item.query_selector_all("a.a-size-base.a-link-normal")
                for al in author_links:
                    href = await al.get_attribute("href") or ""
                    if '/e/' in href or '/author/' in href or 'field-author' in href:
                        author = (await al.text_content()).strip()
                        break
                
                # Strategy 2: Text in author row
                if not author:
                    rows = await item.query_selector_all("div.a-row.a-size-base")
                    for row in rows:
                        row_text = (await row.text_content()).strip()
                        if row_text.startswith("by "):
                            author = row_text[3:].strip()
                            # Clean up (remove | and everything after)
                            author = author.split('|')[0].strip()
                            break
                
                # Strategy 3: Any span with class a-size-base after the title
                if not author:
                    auth_el = await item.query_selector("div.a-row span.a-size-base")
                    if auth_el:
                        text = (await auth_el.text_content()).strip()
                        if text and not text.startswith('$') and not text.startswith('#'):
                            author = text
                
                # Link — try h2 link first, then fallback to ASIN-based URL
                link_el = await item.query_selector("h2 a.a-link-normal")
                href = await link_el.get_attribute("href") if link_el else None
                amazon_link = ""
                base = "https://www.amazon.com" if 'amazon.com' in url else "https://www.amazon.in"
                if href and href.strip():
                    amazon_link = urljoin(base, href.split("?")[0])
                elif asin:
                    # Fallback: construct URL from ASIN
                    amazon_link = f"{base}/dp/{asin}"
                
                # Series info from subtitle
                series_name = ""
                # Check for "Book X of Y" pattern
                all_secondary = await item.query_selector_all("span.a-size-base.a-color-secondary")
                for sec in all_secondary:
                    text = (await sec.text_content()).strip()
                    m = re.search(r'Book\s+\d+\s+of\s+\d+\s*(?::\s*(.+))?', text)
                    if m:
                        if m.group(1):
                            series_name = m.group(1).strip()
                        break
                
                if title and is_relevant(title, subgenre_name):
                    all_books.append({
                        "Book Name": title,
                        "Author Name": author.replace("by ", "").strip() if author else "",
                        "Amazon Link": amazon_link,
                        "Series Name": series_name,
                        "Source": "Search" + (" (Sponsored)" if is_sponsored else ""),
                        "Source Detail": f"Search Page {page_num}",
                        "Subgenre": subgenre_name,
                    })
                    page_books += 1
            except Exception as e:
                logger.debug(f"    Error extracting search item: {e}")
        
        logger.info(f"    ✓ Extracted {page_books} books from page {page_num}")
        
        if page_books == 0:
            consecutive_empty += 1
            if consecutive_empty >= 2:
                logger.info(f"    2 consecutive empty pages, stopping pagination.")
                break
        else:
            consecutive_empty = 0
        
        # Find next page button
        if page_num < max_pages:
            next_btn = await page.query_selector("a.s-pagination-next:not(.s-pagination-disabled)")
            if not next_btn:
                next_btn = await page.query_selector("li.a-last a")
            if next_btn:
                next_href = await next_btn.get_attribute("href")
                if next_href:
                    base = "https://www.amazon.com" if 'amazon.com' in url else "https://www.amazon.in"
                    current_url = urljoin(base, next_href)
                    await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                else:
                    logger.info(f"    No more pages available.")
                    break
            else:
                logger.info(f"    No next button found, stopping.")
                break
    
    return all_books


# ============================================================================
# MAIN ORCHESTRATOR
# ============================================================================

def deduplicate_books(books):
    """Deduplicate by title+author normalization, keeping bestseller source priority."""
    seen = {}
    for b in books:
        key = normalize_title_author(b["Book Name"], b["Author Name"])
        if key not in seen:
            seen[key] = b
        else:
            # Prefer bestseller source over search
            if b["Source"] == "Bestseller" and seen[key]["Source"] != "Bestseller":
                # Merge: keep bestseller data but note both sources
                b["Source Detail"] = f"{b['Source Detail']} | Also in Search"
                seen[key] = b
            elif b["Source"] == "Bestseller" and seen[key]["Source"] == "Bestseller":
                # Both bestseller: merge rank info
                seen[key]["Source Detail"] += f" | {b['Source Detail']}"
    
    return list(seen.values())


async def crawl_subgenre(subgenre_name):
    """Full crawl pipeline for a single subgenre."""
    logger.info(f"\n{'='*60}")
    logger.info(f"🎯 CRAWLING: {subgenre_name}")
    logger.info(f"{'='*60}\n")
    
    if subgenre_name not in SUBGENRE_URLS:
        logger.error(f"Unknown subgenre: {subgenre_name}")
        logger.info(f"Available: {list(SUBGENRE_URLS.keys())}")
        return
    
    urls = SUBGENRE_URLS[subgenre_name]
    all_books = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await create_stealth_context(browser)
        page = await context.new_page()
        
        try:
            # 1. Crawl bestseller lists
            bestseller_urls = urls.get("bestseller", [])
            if bestseller_urls:
                logger.info(f"\n📊 Phase 1: Crawling {len(bestseller_urls)} bestseller lists\n")
                for i, burl in enumerate(bestseller_urls):
                    list_name = extract_list_name(burl)
                    books = await crawl_bestseller_page(page, burl, list_name, subgenre_name)
                    all_books.extend(books)
                    
                    # Rotate context periodically
                    if (i + 1) % 5 == 0:
                        logger.info("  🔄 Rotating browser context...")
                        await page.close()
                        await context.close()
                        context = await create_stealth_context(browser)
                        page = await context.new_page()
                    
                    await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            
            # 2. Crawl search pages
            search_urls = urls.get("search", [])
            if search_urls:
                logger.info(f"\n🔍 Phase 2: Crawling {len(search_urls)} search URL sets (up to {MAX_SEARCH_PAGES} pages each)\n")
                for i, surl in enumerate(search_urls):
                    # Rotate context before each search
                    await page.close()
                    await context.close()
                    context = await create_stealth_context(browser)
                    page = await context.new_page()
                    
                    books = await crawl_search_page(page, surl, subgenre_name, max_pages=MAX_SEARCH_PAGES)
                    all_books.extend(books)
                    
                    await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
        
        finally:
            await page.close()
            await context.close()
            await browser.close()
    
    # 3. Deduplicate
    logger.info(f"\n📉 Deduplicating {len(all_books)} raw entries...")
    deduped = deduplicate_books(all_books)
    logger.info(f"  ✓ {len(deduped)} unique books after deduplication (removed {len(all_books) - len(deduped)} dupes)")
    
    # 4. Save
    safe_name = re.sub(r'[/\\:*?"<>|]', '_', subgenre_name)
    output_file = os.path.join(OUTPUT_DIR, f"{safe_name}_raw_discovery.csv")
    
    if deduped:
        df = pd.DataFrame(deduped)
        df.to_csv(output_file, index=False)
        logger.success(f"\n✅ Saved {len(deduped)} books to: {output_file}")
    else:
        logger.warning(f"\n⚠ No books found for {subgenre_name}")
    
    return output_file


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Amazon Genre Crawl - Phase 1: Discovery")
    parser.add_argument("--genre", type=str, required=True, help="Subgenre name to crawl")
    parser.add_argument("--max-search-pages", type=int, default=MAX_SEARCH_PAGES, help="Max search pages per URL")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode")
    args = parser.parse_args()
    
    if args.visible:
        HEADLESS = False
    if args.max_search_pages:
        MAX_SEARCH_PAGES = args.max_search_pages
    
    asyncio.run(crawl_subgenre(args.genre))
