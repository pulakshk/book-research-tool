#!/usr/bin/env python3
"""
MEGA BOOK ENRICHMENT - The "Ultimate" script for the book-research-tool pipeline.
Consolidates: 
- Link Discovery & Search (Search GR/AMZ if missing)
- Amazon Link Recovery (Goodreads Bridging)
- Deep Metadata Extraction (Publisher, Pages, Description, BSR, Ratings)
- Robust Publisher Strategy (Physical Fallback + GR Expansion)
- Trope & Synopsis Generation
- Series-Based Publisher Backfill (Shared Cache)
- Parallel Execution (High Worker Count)
"""

import asyncio
import pandas as pd
import os
import re
import random
import sys
from loguru import logger
from playwright.async_api import async_playwright

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto, extract_amazon_from_goodreads
from extractors.amazon_patterns import extract_amazon_comprehensive
from extractors.goodreads_patterns import extract_goodreads_comprehensive
from utils.matcher import BookMatcher

# CONFIGURATION
INPUT_FILE = ".tmp/base_for_parallel.csv"
OUTPUT_FILE = "metadata_enriched.csv"
WORKER_COUNT = 12  # Aggressive scale
SAVE_INTERVAL = 5

# SHARED CACHE FOR SERIES PUBLISHERS
# Format: {(norm_author, norm_series): "Publisher Name"}
series_publisher_cache = {}
publisher_lock = asyncio.Lock()

# KEYWORDS & MAPPINGS
SELF_PUB_KEYWORDS = [
    'independently published', 'self-published', 'createspace', 'draft2digital',
    'smashwords', 'kindle direct', 'kdp', 'lulu', 'blurb', 'author house',
    'authorhouse', 'xlibris', 'iuniverse', 'trafford', 'balboa press'
]
TRADITIONAL_PUBLISHERS = [
    'penguin', 'random house', 'harpercollins', 'simon & schuster', 'macmillan',
    'hachette', 'scholastic', 'wiley', 'pearson', 'mcgraw', 'sourcebooks',
    'berkley', 'avon', 'ballantine', 'bantam', 'dell', 'tor', 'forge',
    'st. martin', 'entangled', 'montlake', 'forever', 'grand central',
    'kensington', 'zebra', 'dafina', 'carina', 'harlequin', 'mira', 'mills & boon'
]
TROPES = {
    'Enemies to Lovers': ['enemies', 'hate', 'rival', 'nemesis', 'hated', 'despise'],
    'Friends to Lovers': ['best friend', 'friends since', 'friendship', 'known each other'],
    'Fake Relationship': ['fake', 'pretend', 'arrangement', 'contract', 'for show'],
    'Second Chance': ['ex', 'past', 'years ago', 'high school sweetheart', 'reunion', 'came back'],
    'Forbidden Love': ['forbidden', "shouldn't", 'off limits', 'wrong', 'taboo'],
    'Forced Proximity': ['stuck', 'stranded', 'roommates', 'snowed in', 'cabin', 'one bed'],
    'Grumpy/Sunshine': ['grumpy', 'sunshine', 'gruff', 'brooding', 'cheerful'],
    'Age Gap': ['older', 'younger', 'age difference', 'years older'],
    "Brother's Best Friend": ["brother's best friend", "sister's best friend", 'off-limits'],
    'Single Dad': ['single dad', 'single father', 'widower', 'his daughter', 'his son'],
    'Secret Baby': ['secret baby', 'pregnant', 'his child', "didn't know"],
    'Sports Romance': ['hockey', 'football', 'baseball', 'basketball', 'athlete', 'player', 'team'],
    'Billionaire': ['billionaire', 'millionaire', 'wealthy', 'rich', 'ceo', 'mogul'],
    'Slow Burn': ['slow burn', 'tension', 'building', 'finally'],
}

# --- HELPERS ---

def analyze_trope(description):
    if not description or pd.isna(description): return None
    desc_lower = str(description).lower()
    scores = {t: sum(1 for kw in kws if kw in desc_lower) for t, kws in TROPES.items()}
    return max(scores, key=scores.get) if any(scores.values()) else None

def determine_self_pub(publisher):
    if not publisher or pd.isna(publisher): return 'Yes'
    p_lower = str(publisher).lower()
    if any(kw in p_lower for kw in SELF_PUB_KEYWORDS): return 'Yes'
    if any(pub in p_lower for pub in TRADITIONAL_PUBLISHERS): return 'No'
    return 'Yes' # Default to Yes for this genre unless known Trad Pub

def create_short_synopsis(description):
    if not description or pd.isna(description): return None
    sentences = re.split(r'(?<=[.!?])\s+', str(description).strip())
    syn = ' '.join(sentences[:2])
    return syn[:300] if len(syn) > 300 else syn

async def get_new_page(browser):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    return context, page

async def search_goodreads_for_links(page, title, author):
    """Discovery logic from ultra_recovery.py"""
    try:
        query = f"{title} {author}".strip()
        url = f"https://www.goodreads.com/search?q={query.replace(' ', '+')}"
        if not await safe_goto(page, url): return None
        items = await page.query_selector_all("tr[itemtype='http://schema.org/Book']")
        if not items: return None
        first_item = items[0]
        title_el = await first_item.query_selector("a.bookTitle")
        if not title_el: return None
        gr_link = "https://www.goodreads.com" + await title_el.get_attribute("href")
        return gr_link
    except:
        return None

async def get_publisher_from_physical(page):
    """Navigate to physical edition for reliable publisher."""
    selectors = [
        "li.swatchElement:has-text('Paperback') a", 
        "li.swatchElement:has-text('Hardcover') a",
        "#tmm-grid-swatch-paperback a",
        "#tmm-grid-swatch-hardcover a"
    ]
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                href = await el.get_attribute("href")
                if href and "javascript:void" not in href:
                    url = href if href.startswith("http") else f"https://www.amazon.com{href}"
                    logger.debug(f"      - Switching to physical edition: {url[:60]}...")
                    if await safe_goto(page, url):
                        await asyncio.sleep(1.0)
                        # Robust publisher selectors from amazon_supply_scraper.py
                        pub_sel = [
                            "#rpi-attribute-book_details-publisher .rpi-attribute-value",
                            "#detailBullets_feature_div li:has-text('Publisher') span:last-child",
                            "li:has-text('Publisher') span:nth-child(2)"
                        ]
                        for ps in pub_sel:
                            pub_el = await page.query_selector(ps)
                            if pub_el:
                                text = (await pub_el.text_content()).strip()
                                text = re.sub(r'(?i)Publisher\s*[:\u200f\u200e\s]*', '', text).strip()
                                text = re.split(r'\s*\(', text)[0].strip()
                                if text and len(text) > 2 and 'amazon digital' not in text.lower():
                                    return text
                    break
        except: continue
    return None

# --- WORKER ---

async def mega_worker(worker_id, browser, queue, df, lock):
    context, page = await get_new_page(browser)
    count = 0
    try:
        while not queue.empty():
            # Check if page is closed/broken and recreate if necessary
            try:
                if page.is_closed():
                    logger.warning(f"[W{worker_id}] Page closed, recreating context...")
                    context, page = await get_new_page(browser)
            except:
                logger.warning(f"[W{worker_id}] Context broken, recreating...")
                context, page = await get_new_page(browser)

            try: idx = queue.get_nowait()
            except: break
            
            row = df.loc[idx]
            title = str(row.get('Book Name', ''))
            author = str(row.get('Author Name', ''))
            gr_link = str(row.get('Goodreads Link', '')) if not pd.isna(row.get('Goodreads Link')) else None
            amz_link = str(row.get('Amazon Link', '')) if not pd.isna(row.get('Amazon Link')) else None
            
            logger.info(f"[W{worker_id}] Mega Enriching: {title[:40]}...")
            
            # 1. DISCOVERY (If links missing)
            if not gr_link:
                gr_link = await search_goodreads_for_links(page, title, author)
                if gr_link:
                    async with lock: df.at[idx, 'Goodreads Link'] = gr_link
                    logger.success(f"    ✓ Discovered GR: {gr_link}")

            # 2. GOODREADS PAGE (Deep Extraction)
            gr_data = {}
            if gr_link:
                if await safe_goto(page, gr_link):
                    await asyncio.sleep(0.5)
                    gr_data = await extract_goodreads_comprehensive(page)
            
            # 3. AMAZON LINK RECOVERY (Bridging)
            if not amz_link and gr_data.get('amazon_bridge_link'):
                amz_link = gr_data['amazon_bridge_link']
                async with lock: df.at[idx, 'Amazon Link'] = amz_link
                logger.success(f"    ✓ Bridged AMZ: {amz_link}")
            
            # 4. AMAZON PAGE (Deep Extraction)
            amz_data = {}
            if amz_link:
                visited = await safe_goto(page, amz_link)
                if visited:
                    # Detect 404
                    body = await page.inner_text("body")
                    if "page not found" in body.lower():
                        logger.warning(f"    - AMZ 404 detected. Trying recovery bridge...")
                        if gr_data.get('amazon_bridge_link'):
                            amz_link = gr_data['amazon_bridge_link']
                            visited = await safe_goto(page, amz_link)
                    
                    if visited:
                        # NEW: PHYSICAL-FIRST PRIORITY
                        # Immediately attempt to switch to physical edition as the very first step
                        logger.info(f"    - Amazon loaded. Attempting immediate switch to physical edition...")
                        phys_pub = await get_publisher_from_physical(page)
                        
                        await asyncio.sleep(0.5)
                        # Extract other metadata (Description, BSR, Ratings, etc.)
                        # Note: extract_amazon_comprehensive will run on whatever page we are now on (physical or original)
                        amz_data = await extract_amazon_comprehensive(page, scroll_first=True)
                        
                        # Apply physical publisher if found
                        if phys_pub:
                            amz_data['publisher'] = phys_pub
                            logger.success(f"    ✓ Using physical publisher: {phys_pub}")

                        # PUBLISHER EXTRACTION (Series Cache Check)
                        author_norm = author.lower().strip()
                        series_norm = str(amz_data.get('series_name') or row.get('Series Name', '')).lower().strip()
                        cache_key = (author_norm, series_norm) if series_norm and series_norm != 'no_series' else None
                        
                        cached_pub = None
                        if cache_key:
                            async with publisher_lock: cached_pub = series_publisher_cache.get(cache_key)
                        
                        if cached_pub:
                            amz_data['publisher'] = cached_pub
                            logger.info(f"    - Series cache HIT: {cached_pub}")
                        
                        # Only update cache if we found a good physical publisher
                        if phys_pub and cache_key:
                            async with publisher_lock: series_publisher_cache[cache_key] = phys_pub

            # 5. DATA MERGE
            async with lock:
                updated = False
                # Publisher Priority Logic
                amz_pub = amz_data.get('publisher')
                gr_pub = gr_data.get('publisher')
                
                # Publisher Priority Logic
                # 1. We prioritize what was found via physical edition switch
                best_pub = amz_data.get('publisher')
                
                # 2. Check GR as fallback if AMZ is generic or missing
                if not best_pub or any(x in best_pub.lower() for x in ['amazon digital', 'independently']):
                    gr_pub = gr_data.get('publisher')
                    if gr_pub and not any(x in gr_pub.lower() for x in ['amazon digital', 'independently']):
                        best_pub = gr_pub
                
                # 3. Final Fallback: If still missing or generic, ensure we use 'Independently published'
                if not best_pub or any(x in best_pub.lower() for x in ['amazon digital']):
                    if amz_link or gr_link:
                        best_pub = "Independently published"
                
                if best_pub and (pd.isna(df.at[idx, 'Publisher']) or any(x in str(df.at[idx, 'Publisher']).lower() for x in ['amazon digital', 'nan', ''])):
                    df.at[idx, 'Publisher'] = best_pub
                    df.at[idx, 'Self Pub Flag'] = determine_self_pub(best_pub)
                    updated = True
                
                # Extra fallback for Self Pub Flag if Publisher is still missing
                if pd.isna(df.at[idx, 'Self Pub Flag']):
                    df.at[idx, 'Self Pub Flag'] = 'Yes'
                    updated = True
                
                # Metadata
                m_maps = {
                    'Description': gr_data.get('description') or amz_data.get('description'),
                    'Pages': gr_data.get('pages') or amz_data.get('pages'),
                    'Goodreads Rating': gr_data.get('rating'),
                    'Amazon Rating': amz_data.get('amazon_rating'),
                    'Series Name': amz_data.get('series_name'),
                    'Publication Date': gr_data.get('publication_date'),
                    'Original Published': gr_data.get('original_publication_date')
                }
                for col, val in m_maps.items():
                    if val and (pd.isna(df.at[idx, col]) or str(df.at[idx, col]) in ['nan', '', '0']):
                        df.at[idx, col] = val
                        updated = True
                
                # Derived
                if updated and not pd.isna(df.at[idx, 'Description']):
                    df.at[idx, 'Short Synopsis'] = create_short_synopsis(df.at[idx, 'Description'])
                    df.at[idx, 'Primary Trope'] = analyze_trope(df.at[idx, 'Description'])

                if updated: logger.success(f"  ✓ Mega-Updated: {title[:30]}")
            
            count += 1
            if count % SAVE_INTERVAL == 0:
                async with lock: df.to_csv(OUTPUT_FILE, index=False)
            await asyncio.sleep(random.uniform(0.2, 0.5))
    finally:
        await context.close()

# --- MAIN ---

async def main():
    logger.info("Starting MEGA BOOK ENRICHMENT Pipeline...")
    df = pd.read_csv(INPUT_FILE)
    
    # Target books with missing publishers or major metadata gaps
    mask = (
        (df['Publisher'].isna() | (df['Publisher'].astype(str).str.lower().str.contains('amazon digital services'))) |
        (df['Description'].isna()) |
        (df['Goodreads Link'].isna()) |
        (df['Amazon Link'].isna())
    )
    rows = df[mask].index.tolist()
    logger.info(f"Targeting {len(rows)} books for deep enrichment.")
    
    queue = asyncio.Queue()
    for r in rows: await queue.put(r)
    
    lock = asyncio.Lock()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        workers = [mega_worker(i, browser, queue, df, lock) for i in range(WORKER_COUNT)]
        await asyncio.gather(*workers)
        await browser.close()
    
    # Post-process: Broadcasting Series Publishers
    logger.info("Final Phase: Broadcasting Series Publishers...")
    broadcast_series_publishers(df)
    
    df.to_csv(OUTPUT_FILE, index=False)
    logger.success("MEGA ENRICHMENT COMPLETE.")

def broadcast_series_publishers(df):
    def get_strong_pub(s):
        v = s[s.notna() & (~s.astype(str).str.lower().str.contains('amazon digital|independently'))]
        return v.iloc[0] if not v.empty else None
    mask = (df['Series Name'].notna()) & (df['Series Name'] != 'NO_SERIES')
    if df[mask].empty: return
    mapping = df[mask].groupby(['Author Name', 'Series Name'])['Publisher'].apply(get_strong_pub).to_dict()
    for idx, row in df[mask].iterrows():
        strong = mapping.get((row['Author Name'], row['Series Name']))
        if strong and any(x in str(row['Publisher']).lower() for x in ['amazon digital', 'independently', 'nan', '']):
            df.at[idx, 'Publisher'] = strong; df.at[idx, 'Self Pub Flag'] = determine_self_pub(strong)

if __name__ == "__main__":
    asyncio.run(main())
