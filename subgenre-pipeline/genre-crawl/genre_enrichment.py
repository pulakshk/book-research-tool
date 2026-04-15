#!/usr/bin/env python3
"""
GENRE ENRICHMENT — Phase 2: Goodreads + Amazon Metadata Extraction
Takes the raw discovery CSV and enriches each book with:
- Goodreads: link, rating, rating count, series info, genres, description, publisher (fallback)
- Amazon: publisher (physical edition), page count, BSR, publication date
- Gemini: Primary Trope, Short Synopsis, Subjective Analysis
Produces an enriched book-level CSV.
"""

import asyncio
import os
import random
import re
import sys
import json
import time
from datetime import datetime

import pandas as pd
import numpy as np
from loguru import logger
from playwright.async_api import async_playwright
from dotenv import load_dotenv

# Load env
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# Try importing Gemini
try:
    import google.generativeai as genai
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel('gemini-2.5-flash')
    else:
        gemini_model = None
        logger.warning("No GEMINI_API_KEY found - skipping Gemini enrichment")
except ImportError:
    gemini_model = None
    logger.warning("google-generativeai not installed - skipping Gemini enrichment")

# Add project root to path for imports
PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
sys.path.insert(0, PROJECT_ROOT)

# ============================================================================
# CONFIGURATION
# ============================================================================

WORKER_COUNT = 4          # Parallel workers
SAVE_INTERVAL = 10        # Save every N books
SLEEP_MIN = 2
SLEEP_MAX = 5
HEADLESS = True
OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2.1 Safari/605.1.15",
]

SELF_PUB_KEYWORDS = [
    'independently published', 'self-published', 'createspace', 'draft2digital',
    'smashwords', 'kindle direct', 'kdp', 'lulu', 'blurb', 'author house',
    'authorhouse', 'xlibris', 'iuniverse', 'trafford', 'balboa press'
]

TRADITIONAL_PUBLISHERS = [
    'penguin', 'harpercollins', 'simon & schuster', 'hachette', 'macmillan',
    'random house', 'scholastic', 'wiley', 'bloomsbury', 'sourcebooks',
    'berkley', 'avon', 'ballantine', 'bantam', 'dell', 'tor', 'forge',
    'st. martin', 'entangled', 'montlake', 'forever', 'grand central',
    'kensington', 'zebra', 'dafina', 'carina', 'harlequin', 'mira',
    'mills & boon', 'william morrow', 'putnam', 'dutton', 'atria',
    'gallery', 'scribner', 'vintage', 'anchor', 'crown', 'knopf',
    'doubleday', 'little brown', 'orbit', 'ace', 'roc', 'daw',
    'del rey', 'piatkus', 'hodder', 'headline', 'hq',
]

TROPES = {
    'Enemies to Lovers': ['enemies', 'hate', 'rival', 'nemesis', 'hated', 'despise'],
    'Friends to Lovers': ['best friend', 'friends since', 'friendship', 'known each other'],
    'Forbidden Love': ['forbidden', 'off-limits', 'taboo', 'cant have', 'shouldnt want'],
    'Second Chance': ['ex-', 'divorced', 'second chance', 'years later', 'came back'],
    'Fake Relationship': ['fake', 'pretend', 'arrangement', 'contract', 'deal'],
    'Forced Proximity': ['roommate', 'stuck together', 'forced', 'stranded', 'snowed in'],
    'Grumpy/Sunshine': ['grumpy', 'sunshine', 'brooding', 'cheerful', 'opposites'],
    'Age Gap': ['older', 'younger', 'age gap', 'years older', 'mature'],
    'Dark Romance': ['dark', 'captive', 'stalker', 'obsess', 'possess', 'ruthless'],
    'Billionaire': ['billionaire', 'millionaire', 'wealthy', 'rich', 'ceo', 'mogul'],
    'Slow Burn': ['slow burn', 'tension', 'building', 'finally'],
    'Political': ['senator', 'president', 'political', 'campaign', 'election', 'congress', 'white house', 'governor'],
    'Military': ['military', 'soldier', 'marine', 'navy', 'seal', 'army', 'deployed', 'veteran'],
    'Suspense': ['suspense', 'thriller', 'mystery', 'investigation', 'danger', 'killer'],
    'Mafia': ['mafia', 'cartel', 'mob', 'crime boss', 'don', 'family business'],
    'Historical': ['regency', 'victorian', 'historical', 'medieval', 'civil war', 'colonial'],
    'Small Town': ['small town', 'rural', 'hometown', 'country', 'farm', 'ranch'],
    'Christian/Faith': ['faith', 'christian', 'church', 'prayer', 'god', 'grace', 'redemption'],
}


# ============================================================================
# HELPERS
# ============================================================================

def determine_self_pub(publisher):
    """Determine if publisher is self-pub, indie, or big pub."""
    if not publisher or pd.isna(publisher) or str(publisher).strip() == '':
        return ''
    p_lower = str(publisher).lower()
    if any(kw in p_lower for kw in SELF_PUB_KEYWORDS):
        return 'Self-Pub'
    if any(pub in p_lower for pub in TRADITIONAL_PUBLISHERS):
        return 'Big Pub'
    return 'Indie'


def analyze_trope(description):
    """Detect primary trope from description."""
    if not description or pd.isna(description):
        return ''
    desc_lower = str(description).lower()
    scores = {t: sum(1 for kw in kws if kw in desc_lower) for t, kws in TROPES.items()}
    best = max(scores, key=scores.get) if any(scores.values()) else ''
    return best


async def create_stealth_context(browser):
    """Create a stealth browser context."""
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


async def safe_goto(page, url, timeout=45000, retries=3):
    """Network-resilient navigation."""
    for attempt in range(1, retries + 1):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=timeout)
            await asyncio.sleep(random.uniform(0.5, 1.5))
            return True
        except Exception as e:
            logger.debug(f"  goto attempt {attempt}/{retries} failed for {url[:60]}: {e}")
            await asyncio.sleep(2 * attempt + random.uniform(1.0, 3.0))
    return False


# ============================================================================
# GOODREADS EXTRACTION
# ============================================================================

async def search_goodreads(page, title, author):
    """Search Goodreads for a book and return the first matching link."""
    try:
        # Skip bad author names like 'Kindle Edition'
        clean_author = author if author and author.lower() not in ['kindle edition', 'audible audiobook', 'paperback', 'hardcover', ''] else ''
        query = f"{title} {clean_author}".strip()
        url = f"https://www.goodreads.com/search?q={query.replace(' ', '+')}"
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
        
        # Also extract the author from GR search results
        author_el = await first_item.query_selector("a.authorName span")
        gr_author = (await author_el.text_content()).strip() if author_el else ''
        
        href = await title_el.get_attribute("href")
        gr_link = "https://www.goodreads.com" + href
        return {'link': gr_link, 'author': gr_author}
    except Exception as e:
        logger.debug(f"  GR search error: {e}")
        return None


async def extract_goodreads_data(page, gr_url):
    """Extract comprehensive data from a Goodreads book page."""
    data = {
        'goodreads_link': gr_url,
        'gr_rating': '',
        'gr_rating_count': '',
        'gr_series_name': '',
        'gr_series_url': '',
        'gr_book_number': '',
        'gr_pages': '',
        'gr_description': '',
        'gr_genres': '',
        'gr_publisher': '',
        'gr_pub_date': '',
    }
    
    try:
        if not await safe_goto(page, gr_url):
            return data
        
        await asyncio.sleep(random.uniform(1.5, 3.0))
        
        # --- Rating + Rating Count ---
        try:
            rating_el = await page.query_selector("div.RatingStatistics__rating")
            if rating_el:
                data['gr_rating'] = (await rating_el.text_content()).strip()
        except:
            pass
        
        try:
            count_el = await page.query_selector("span[data-testid='ratingsCount']")
            if count_el:
                text = (await count_el.text_content()).strip()
                # Parse "123,456 ratings" -> 123456
                num = re.sub(r'[^0-9]', '', text)
                data['gr_rating_count'] = num
        except:
            pass
        
        # --- Series Info ---
        try:
            series_el = await page.query_selector("h3.Text__italic a")
            if not series_el:
                series_el = await page.query_selector("div.BookPageTitleSection a[href*='/series/']")
            if series_el:
                series_text = (await series_el.text_content()).strip()
                series_href = await series_el.get_attribute("href")
                
                # Parse "Series Name #3"
                m = re.match(r'(.+?)(?:\s*#(\d+\.?\d*))?$', series_text)
                if m:
                    data['gr_series_name'] = m.group(1).strip()
                    if m.group(2):
                        data['gr_book_number'] = m.group(2)
                
                if series_href:
                    data['gr_series_url'] = "https://www.goodreads.com" + series_href if series_href.startswith('/') else series_href
        except:
            pass
        
        # --- Pages ---
        try:
            pages_el = await page.query_selector("p[data-testid='pagesFormat']")
            if pages_el:
                text = (await pages_el.text_content()).strip()
                m = re.search(r'(\d+)\s*pages', text)
                if m:
                    data['gr_pages'] = m.group(1)
        except:
            pass
        
        # --- Description ---
        try:
            desc_el = await page.query_selector("div.BookPageMetadataSection__description span.Formatted")
            if not desc_el:
                # Try show more button first
                show_more = await page.query_selector("button.Button--inline:has-text('Show more')")
                if show_more:
                    await show_more.click()
                    await asyncio.sleep(0.5)
                    desc_el = await page.query_selector("div.BookPageMetadataSection__description span.Formatted")
            if desc_el:
                data['gr_description'] = (await desc_el.text_content()).strip()[:1000]
        except:
            pass
        
        # --- Genres ---
        try:
            genre_els = await page.query_selector_all("span.BookPageMetadataSection__genreButton a .Button__labelItem")
            genres = []
            for g in genre_els[:5]:
                genres.append((await g.text_content()).strip())
            data['gr_genres'] = ', '.join(genres)
        except:
            pass
        
        # --- Publisher (fallback) ---
        try:
            # Click "Show more" in publication info if needed
            pub_details = await page.query_selector("div.FeaturedDetails")
            if pub_details:
                pub_text_el = await pub_details.query_selector("p[data-testid='publicationInfo']")
                if pub_text_el:
                    pub_text = (await pub_text_el.text_content()).strip()
                    # Parse "First published January 1, 2020" or "Published March 2023 by Publisher Name"
                    m = re.search(r'(?:Published|published)\s+.*?\s+by\s+(.+?)(?:\s*$)', pub_text)
                    if m:
                        data['gr_publisher'] = m.group(1).strip()
                    # Parse date
                    m_date = re.search(r'(?:Published|First published)\s+(.+?)(?:\s+by|$)', pub_text)
                    if m_date:
                        data['gr_pub_date'] = m_date.group(1).strip()
            
            # Also try expanded details 
            if not data['gr_publisher']:
                # Look for "edition details" section
                detail_items = await page.query_selector_all("div.DescListItem")
                for di in detail_items:
                    dt = await di.query_selector("dt")
                    dd = await di.query_selector("dd")
                    if dt and dd:
                        label = (await dt.text_content()).strip().lower()
                        if 'publisher' in label or 'published by' in label:
                            data['gr_publisher'] = (await dd.text_content()).strip()
        except:
            pass
        
    except Exception as e:
        logger.debug(f"  GR extraction error: {e}")
    
    return data


# ============================================================================
# AMAZON EXTRACTION
# ============================================================================

async def get_publisher_from_physical(page, amazon_url):
    """Navigate to the physical edition (paperback/hardcover) and extract publisher."""
    try:
        if not await safe_goto(page, amazon_url):
            return None
        
        await asyncio.sleep(random.uniform(1.5, 3.0))
        
        # Try to find paperback/hardcover format switcher
        selectors = [
            "li.swatchElement:has-text('Paperback') a",
            "li.swatchElement:has-text('Hardcover') a",
            "#tmm-grid-swatch-paperback a",
            "#tmm-grid-swatch-hardcover a",
            "a.a-button-text:has-text('Paperback')",
            "a.a-button-text:has-text('Hardcover')",
        ]
        
        for sel in selectors:
            try:
                el = await page.query_selector(sel)
                if el:
                    href = await el.get_attribute("href")
                    if href and "javascript:void" not in href:
                        base = "https://www.amazon.com" if 'amazon.com' in amazon_url else "https://www.amazon.in"
                        url = href if href.startswith("http") else f"{base}{href}"
                        if await safe_goto(page, url):
                            await asyncio.sleep(random.uniform(1.0, 2.0))
                            # Extract publisher from physical edition
                            pub_selectors = [
                                "#rpi-attribute-book_details-publisher .rpi-attribute-value",
                                "#detailBullets_feature_div li:has-text('Publisher') span:last-child",
                                "li:has-text('Publisher') span:nth-child(2)",
                            ]
                            for ps in pub_selectors:
                                pub_el = await page.query_selector(ps)
                                if pub_el:
                                    text = (await pub_el.text_content()).strip()
                                    text = re.sub(r'(?i)Publisher\s*[:\u200f\u200e\s]*', '', text).strip()
                                    text = re.split(r'\s*\(', text)[0].strip()
                                    if text and len(text) > 2 and 'amazon digital' not in text.lower():
                                        return text
                        break
            except:
                continue
        
        # Fallback: Try publisher from Kindle edition details
        pub_selectors = [
            "#rpi-attribute-book_details-publisher .rpi-attribute-value",
            "#detailBullets_feature_div li:has-text('Publisher') span:last-child",
        ]
        for ps in pub_selectors:
            try:
                pub_el = await page.query_selector(ps)
                if pub_el:
                    text = (await pub_el.text_content()).strip()
                    text = re.sub(r'(?i)Publisher\s*[:\u200f\u200e\s]*', '', text).strip()
                    text = re.split(r'\s*\(', text)[0].strip()
                    if text and len(text) > 2:
                        return text
            except:
                continue
        
    except Exception as e:
        logger.debug(f"  Publisher extraction error: {e}")
    
    return None


async def extract_amazon_metadata(page, amazon_url):
    """Extract metadata from Amazon product page."""
    data = {
        'amz_rating': '',
        'amz_rating_count': '',
        'amz_pages': '',
        'amz_publisher': '',
        'amz_pub_date': '',
        'amz_bsr': '',
        'amz_series_name': '',
        'amz_book_number': '',
        'amz_total_books': '',
        'amz_author': '',
    }
    
    try:
        if not await safe_goto(page, amazon_url):
            return data
        
        await asyncio.sleep(random.uniform(2.0, 4.0))
        
        # Check for CAPTCHA with retries
        max_captcha_retries = 3
        captcha_cleared = False
        
        for attempt in range(max_captcha_retries):
            captcha = await page.query_selector("form[action*='validateCaptcha'], input#captchacharacters")
            if not captcha:
                captcha_cleared = True
                break
                
            logger.warning(f"  🤖 CAPTCHA on Amazon (Attempt {attempt+1}/{max_captcha_retries})! Waiting 45s and reloading...")
            await asyncio.sleep(45)
            await page.reload(wait_until="domcontentloaded", timeout=45000)
            await asyncio.sleep(5)
            
        if not captcha_cleared:
            logger.error(f"  ❌ Failed to clear Amazon CAPTCHA after {max_captcha_retries} retries for {amazon_url}. Skipping to prevent bad data.")
            return data
            
        # --- Author ---
        try:
            author_el = await page.query_selector('.author .a-link-normal, #bylineInfo span.author a.a-link-normal')
            if author_el:
                data['amz_author'] = (await author_el.text_content()).strip()
        except:
            pass
        
        # --- Rating ---
        try:
            rating_el = await page.query_selector(".reviewCountTextLinkedHistogram")
            if rating_el:
                title_attr = await rating_el.get_attribute("title")
                if title_attr:
                    data['amz_rating'] = title_attr.split(' ')[0]
        except:
            pass
        
        # --- Rating Count ---
        try:
            count_el = await page.query_selector("#acrCustomerReviewText")
            if count_el:
                text = (await count_el.text_content()).strip()
                data['amz_rating_count'] = re.sub(r'[^0-9]', '', text)
        except:
            pass
        
        # --- Series Info ---
        try:
            series_el = await page.query_selector("#rpi-icon-link-book_details-series")
            if series_el:
                data['amz_series_name'] = (await series_el.text_content()).strip()
            
            book_info_el = await page.query_selector("#rpi-attribute-book_details-series .rpi-attribute-label span")
            if book_info_el:
                text = (await book_info_el.text_content()).strip()
                m = re.match(r'Book\s+(\d+)\s+of\s+(\d+)', text, re.I)
                if m:
                    data['amz_book_number'] = m.group(1)
                    data['amz_total_books'] = m.group(2)
        except:
            pass
        
        # --- Pages ---
        try:
            pages_el = await page.query_selector("#rpi-attribute-book_details-fiona_pages .rpi-attribute-value span")
            if pages_el:
                text = (await pages_el.text_content()).strip()
                m = re.search(r'(\d+)', text)
                if m:
                    data['amz_pages'] = m.group(1)
        except:
            pass
        
        # --- Publication Date ---
        try:
            from bs4 import BeautifulSoup
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            for li in soup.select('#detailBullets_feature_div ul.detail-bullet-list li'):
                bold = li.select_one('.a-text-bold')
                if bold and 'Publication date' in bold.text:
                    span = bold.find_next_sibling('span')
                    if span:
                        data['amz_pub_date'] = span.text.strip()
        except:
            pass
        
        # --- Best Sellers Rank ---
        try:
            from bs4 import BeautifulSoup
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            for li in soup.select('#detailBullets_feature_div ul.detail-bullet-list li'):
                bold = li.select_one('.a-text-bold')
                if not bold or 'Best Sellers Rank' not in bold.text:
                    continue
                parts = []
                sub_ul = li.select_one('ul.zg_hrsr')
                if sub_ul:
                    for sub_li in sub_ul.select('li span.a-list-item'):
                        rank_text = sub_li.get_text(strip=True)
                        if rank_text:
                            parts.append(rank_text)
                else:
                    rest = li.get_text(separator=' ', strip=True)
                    idx = rest.find('Best Sellers Rank')
                    if idx >= 0:
                        rank_text = rest[idx:].replace('Best Sellers Rank :', '').replace('Best Sellers Rank:', '').strip()
                        rank_text = re.sub(r'\(See Top \d+.*?\)', '', rank_text).strip()
                        if rank_text:
                            parts.append(rank_text)
                if parts:
                    data['amz_bsr'] = ' | '.join(parts)
                break
        except:
            pass
        
        # --- Publisher (from physical edition) ---
        publisher = await get_publisher_from_physical(page, amazon_url)
        if publisher:
            data['amz_publisher'] = publisher
        
    except Exception as e:
        logger.debug(f"  Amazon extraction error: {e}")
    
    return data


# ============================================================================
# GEMINI ENRICHMENT
# ============================================================================

async def gemini_enrich(title, author, description, subgenre):
    """Use Gemini to generate synopsis, trope analysis, and subjective analysis."""
    if not gemini_model:
        return {'synopsis': '', 'trope': '', 'subjective': '', 'differentiator': ''}
    
    try:
        prompt = f"""You are a book industry analyst. For the following book, provide:
1. SHORT_SYNOPSIS: A 1-2 sentence punchy synopsis of the book (based on the description below).
2. PRIMARY_TROPE: The single most dominant romance/fiction trope (e.g., Enemies to Lovers, Forbidden Love, Dark Romance, Political Thriller, etc.)
3. SUBJECTIVE_ANALYSIS: A 1-2 sentence market analysis of this book's appeal.
4. DIFFERENTIATOR: What makes this book unique or stand out in the {subgenre} subgenre.

Book: "{title}" by {author}
Subgenre: {subgenre}
Description: {description[:800] if description else "No description available"}

Return ONLY valid JSON:
{{"synopsis": "...", "trope": "...", "subjective": "...", "differentiator": "..."}}"""
        
        response = await asyncio.to_thread(
            gemini_model.generate_content, prompt
        )
        
        text = response.text.strip()
        # Extract JSON from response (handle markdown code blocks)
        if '```json' in text:
            text = text.split('```json')[1].split('```')[0]
        elif '```' in text:
            text = text.split('```')[1].split('```')[0]
        
        result = json.loads(text)
        return {
            'synopsis': result.get('synopsis', ''),
            'trope': result.get('trope', ''),
            'subjective': result.get('subjective', ''),
            'differentiator': result.get('differentiator', ''),
        }
    except Exception as e:
        logger.debug(f"  Gemini error for {title}: {e}")
        return {'synopsis': '', 'trope': '', 'subjective': '', 'differentiator': ''}


# ============================================================================
# WORKER
# ============================================================================

async def enrich_worker(worker_id, browser, queue, df, lock, subgenre_name, save_path):
    """Worker that processes one book at a time from the queue."""
    context = await create_stealth_context(browser)
    page = await context.new_page()
    processed = 0
    
    try:
        while True:
            try:
                idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            
            async with lock:
                row = df.iloc[idx]
                title = str(row.get('Book Name', '')).strip()
                author = str(row.get('Author Name', '')).strip()
                amazon_link = str(row.get('Amazon Link', '')).strip()
                
                # Skip if already enriched
                if str(row.get('Goodreads Link', '')).startswith('http'):
                    logger.debug(f"  Worker {worker_id}: Skipping enriched row {idx}: {title}")
                    continue
            
            logger.info(f"  Worker {worker_id} [{idx+1}]: {title} — {author}")
            
            try:
                # 1. Search Goodreads
                gr_result = await search_goodreads(page, title, author)
                gr_data = {}
                gr_author = ''
                if gr_result:
                    gr_link = gr_result['link']
                    gr_author = gr_result.get('author', '')
                    gr_data = await extract_goodreads_data(page, gr_link)
                    await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                
                # 2. Amazon metadata
                amz_data = {}
                if amazon_link and amazon_link.startswith('http'):
                    amz_data = await extract_amazon_metadata(page, amazon_link)
                    await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                
                # 3. Gemini enrichment
                description = gr_data.get('gr_description', '')
                gemini_data = await gemini_enrich(title, author, description, subgenre_name)
                
                # 4. Merge data into DataFrame
                publisher = amz_data.get('amz_publisher', '') or gr_data.get('gr_publisher', '')
                
                # Fix bad author names (e.g., 'Kindle Edition')
                real_author = author
                if not real_author or real_author.lower() in ['kindle edition', 'audible audiobook', 'paperback', 'hardcover', '']:
                    real_author = amz_data.get('amz_author', '') or gr_author or author
                
                async with lock:
                    # Update author if it was bad
                    if real_author != author:
                        df.at[idx, 'Author Name'] = real_author
                    df.at[idx, 'Goodreads Link'] = gr_data.get('goodreads_link', '')
                    df.at[idx, 'Goodreads Rating'] = gr_data.get('gr_rating', '')
                    df.at[idx, 'Goodreads # of Ratings'] = gr_data.get('gr_rating_count', '')
                    df.at[idx, 'Series Name'] = gr_data.get('gr_series_name', '') or amz_data.get('amz_series_name', '') or str(row.get('Series Name', ''))
                    df.at[idx, 'Book Number'] = gr_data.get('gr_book_number', '') or amz_data.get('amz_book_number', '')
                    df.at[idx, 'Total Books in Series'] = amz_data.get('amz_total_books', '')
                    df.at[idx, 'Pages'] = amz_data.get('amz_pages', '') or gr_data.get('gr_pages', '')
                    df.at[idx, 'Description'] = description
                    df.at[idx, 'Genres'] = gr_data.get('gr_genres', '')
                    df.at[idx, 'Publisher'] = publisher
                    df.at[idx, 'Self Pub Flag'] = determine_self_pub(publisher)
                    df.at[idx, 'Amazon Rating'] = amz_data.get('amz_rating', '')
                    df.at[idx, 'Amazon # of Ratings'] = amz_data.get('amz_rating_count', '')
                    df.at[idx, 'Amazon BSR'] = amz_data.get('amz_bsr', '')
                    df.at[idx, 'Publication Date'] = amz_data.get('amz_pub_date', '') or gr_data.get('gr_pub_date', '')
                    df.at[idx, 'Primary Trope'] = gemini_data.get('trope', '') or analyze_trope(description)
                    df.at[idx, 'Short Synopsis'] = gemini_data.get('synopsis', '')
                    df.at[idx, 'Subjective Analysis'] = gemini_data.get('subjective', '')
                    df.at[idx, 'Differentiator'] = gemini_data.get('differentiator', '')
                    df.at[idx, 'Goodreads Series URL'] = gr_data.get('gr_series_url', '')
                    
                    processed += 1
                    
                    # Save periodically
                    if processed % SAVE_INTERVAL == 0:
                        df.to_csv(save_path, index=False)
                        logger.info(f"  💾 Worker {worker_id}: Saved progress ({processed} books)")
                
            except Exception as e:
                logger.error(f"  Worker {worker_id}: Error on {title}: {e}")
            
            # Rotate context periodically
            if processed % 15 == 0 and processed > 0:
                logger.info(f"  🔄 Worker {worker_id}: Rotating context...")
                await page.close()
                await context.close()
                context = await create_stealth_context(browser)
                page = await context.new_page()
            
            await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
    
    finally:
        await page.close()
        await context.close()
    
    logger.info(f"  Worker {worker_id}: Done — processed {processed} books")


# ============================================================================
# MAIN
# ============================================================================

async def enrich_subgenre(input_csv, subgenre_name=None):
    """Run enrichment pipeline on a raw discovery CSV."""
    
    if not os.path.exists(input_csv):
        logger.error(f"Input file not found: {input_csv}")
        return
    
    df = pd.read_csv(input_csv)
    logger.info(f"Loaded {len(df)} books from {input_csv}")
    
    if subgenre_name is None:
        subgenre_name = df['Subgenre'].iloc[0] if 'Subgenre' in df.columns else "Unknown"
    
    # Add enrichment columns if not present
    enrich_columns = [
        'Goodreads Link', 'Goodreads Rating', 'Goodreads # of Ratings',
        'Series Name', 'Book Number', 'Total Books in Series', 'Pages',
        'Description', 'Genres', 'Publisher', 'Self Pub Flag',
        'Amazon Rating', 'Amazon # of Ratings', 'Amazon BSR',
        'Publication Date', 'Primary Trope', 'Short Synopsis',
        'Subjective Analysis', 'Differentiator', 'Goodreads Series URL',
    ]
    for col in enrich_columns:
        if col not in df.columns:
            df[col] = ''
    
    # Prepare save path
    safe_name = re.sub(r'[/\\:*?"<>|]', '_', subgenre_name)
    save_path = os.path.join(OUTPUT_DIR, f"{safe_name}_enriched.csv")
    
    # Create work queue
    queue = asyncio.Queue()
    for i in range(len(df)):
        queue.put_nowait(i)
    
    lock = asyncio.Lock()
    
    logger.info(f"\n{'='*60}")
    logger.info(f"📚 ENRICHING: {subgenre_name} ({len(df)} books, {WORKER_COUNT} workers)")
    logger.info(f"{'='*60}\n")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        
        workers = [
            enrich_worker(i, browser, queue, df, lock, subgenre_name, save_path)
            for i in range(WORKER_COUNT)
        ]
        
        await asyncio.gather(*workers)
        await browser.close()
    
    # Final save
    df.to_csv(save_path, index=False)
    logger.success(f"\n✅ Enriched data saved to: {save_path}")
    logger.info(f"  Total books: {len(df)}")
    
    # Stats
    gr_filled = df['Goodreads Link'].apply(lambda x: str(x).startswith('http')).sum()
    pub_filled = df['Publisher'].apply(lambda x: str(x).strip() != '').sum()
    logger.info(f"  Goodreads links found: {gr_filled}/{len(df)} ({gr_filled/len(df)*100:.0f}%)")
    logger.info(f"  Publishers found: {pub_filled}/{len(df)} ({pub_filled/len(df)*100:.0f}%)")
    
    return save_path


# ============================================================================
# CLI
# ============================================================================

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Genre Enrichment - Phase 2: Metadata")
    parser.add_argument("--input", type=str, required=True, help="Path to raw discovery CSV")
    parser.add_argument("--genre", type=str, default=None, help="Subgenre name")
    parser.add_argument("--workers", type=int, default=WORKER_COUNT, help="Number of parallel workers")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode")
    args = parser.parse_args()
    
    if args.visible:
        HEADLESS = False
    WORKER_COUNT = args.workers
    
    asyncio.run(enrich_subgenre(args.input, args.genre))
