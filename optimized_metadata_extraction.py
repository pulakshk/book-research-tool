#!/usr/bin/env python3
"""
OPTIMIZED Metadata Extraction - Uses proven patterns from all existing extractors.
Integrates:
- extractors/goodreads_patterns.py (JSON-LD, genres, publication_info, descriptions)
- extractors/amazon_patterns.py (series, ratings, BSR, publisher)
- amazon_supply_scraper.py approach (paperback edition for publisher)
"""
import asyncio
import pandas as pd
import os
import re
import random
import sys
from loguru import logger
from playwright.async_api import async_playwright

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto
from extractors.amazon_patterns import extract_amazon_comprehensive
from extractors.goodreads_patterns import extract_goodreads_comprehensive

INPUT_FILE = "unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "unified_book_data_enriched_ultra.csv"

# SHARED CACHE FOR SERIES PUBLISHERS
# Format: {(norm_author, norm_series): "Publisher Name"}
series_publisher_cache = {}
publisher_lock = asyncio.Lock()

# Self-pub detection keywords
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

def analyze_trope(description):
    if not description or pd.isna(description):
        return None
    desc_lower = str(description).lower()
    scores = {}
    for trope, keywords in TROPES.items():
        score = sum(1 for kw in keywords if kw in desc_lower)
        if score > 0:
            scores[trope] = score
    return max(scores, key=scores.get) if scores else None

def determine_self_pub(publisher):
    if not publisher or pd.isna(publisher):
        return None
    pub_lower = str(publisher).lower()
    for kw in SELF_PUB_KEYWORDS:
        if kw in pub_lower:
            return 'Yes'
    for pub in TRADITIONAL_PUBLISHERS:
        if pub in pub_lower:
            return 'No'
    return None

def create_short_synopsis(description):
    if not description or pd.isna(description):
        return None
    sentences = re.split(r'(?<=[.!?])\s+', str(description).strip())
    synopsis = ' '.join(sentences[:2])
    return synopsis[:300] if len(synopsis) > 300 else synopsis

async def get_new_page(browser):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    return context, page

async def extract_publisher_from_page(page):
    """Extract publisher using robust selectors (from amazon_supply_scraper.py)."""
    pub_selectors = [
        "#rpi-attribute-book_details-publisher .rpi-attribute-value",
        "#rpi-attribute-book_details-publisher",
        "#detailBullets_feature_div li:has-text('Publisher') span:last-child",
        "li:has-text('Publisher') span:nth-child(2)",
    ]
    for sel in pub_selectors:
        try:
            pub_el = await page.query_selector(sel)
            if pub_el:
                text = (await pub_el.text_content() or "").strip()
                text = re.sub(r'(?i)Publisher\s*[:\u200f\u200e\s]*', '', text).strip()
                text = re.sub(r'^[:\u200f\u200e\s]+', '', text).strip()
                text = re.split(r'\s*\(', text)[0].strip()
                if text and len(text) > 2 and text not in [':', 'Publisher']:
                    return text[:100]
        except:
            continue
    return None

async def get_publisher_from_physical(page):
    """Navigate to paperback/hardcover edition for reliable publisher (from amazon_supply_scraper.py)."""
    physical_selectors = [
        "li.swatchElement:has-text('Paperback') a",
        "li.swatchElement:has-text('Hardcover') a",
        "#tmm-grid-swatch-paperback a",
        "#tmm-grid-swatch-hardcover a",
    ]
    for sel in physical_selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                href = await el.get_attribute("href")
                if href and "javascript:void" not in href:
                    physical_url = href if href.startswith("http") else f"https://www.amazon.com{href}"
                    logger.debug(f"    - Navigating to physical edition for publisher...")
                    if await safe_goto(page, physical_url):
                        await asyncio.sleep(0.8)
                        pub = await extract_publisher_from_page(page)
                        if pub:
                            return pub
                    break
        except:
            continue
    return None

async def extraction_worker(worker_id, browser, queue, df, lock):
    context, page = await get_new_page(browser)
    count = 0
    
    try:
        while not queue.empty():
            try:
                idx = queue.get_nowait()
            except:
                break
            
            row = df.loc[idx]
            title = str(row.get('Book Name', ''))[:40]
            gr_link = str(row.get('Goodreads Link', ''))
            amz_link = str(row.get('Amazon Link', ''))
            
            logger.info(f"[W{worker_id}] Processing: {title}...")
            
            gr_data = {}
            amz_data = {}
            publisher_from_physical = None
            
            # GOODREADS EXTRACTION - Using proven comprehensive extractor
            if gr_link and 'goodreads.com' in gr_link:
                try:
                    if await safe_goto(page, gr_link):
                        await page.wait_for_load_state("domcontentloaded")
                        await asyncio.sleep(0.8)
                        gr_data = await extract_goodreads_comprehensive(page)
                except Exception as e:
                    logger.warning(f"    - GR extraction error: {e}")
            
            # AMAZON EXTRACTION - Using proven comprehensive extractor
            if amz_link and 'amazon.com' in amz_link:
                try:
                    # Try direct navigation
                    visited = await safe_goto(page, amz_link)
                    
                    # Detect "Page Not Found" or broken links
                    is_invalid = not visited
                    if visited:
                        page_text = await page.inner_text("body")
                        if "page not found" in page_text.lower() or "sorry! we couldn't find that page" in page_text.lower():
                            is_invalid = True
                    
                    # LINK RECOVERY: Bridge from Goodreads if Amazon link is invalid
                    if is_invalid and gr_data.get('amazon_bridge_link'):
                        logger.warning(f"    - Amazon link invalid (404). Bridging from Goodreads...")
                        bridge_url = gr_data['amazon_bridge_link']
                        if await safe_goto(page, bridge_url):
                            await asyncio.sleep(2)
                            # Update amz_link for this worker session
                            amz_link = page.url
                            # Flag to update CSV later
                            recovered_link = amz_link
                            logger.success(f"    ✓ Recovered Amazon Link: {amz_link}")
                            visited = True
                        else:
                            visited = False
                    else:
                        recovered_link = None

                    if visited:
                        await asyncio.sleep(0.5)
                        amz_data = await extract_amazon_comprehensive(page, scroll_first=True)
                        
                        # Apply recovered link to dataframe if found
                        if recovered_link:
                            async with lock:
                                df.at[idx, 'Amazon Link'] = recovered_link
                                updated = True
                        
                        # PUBLISHER EXTRACTION (Optimized with Series Check)
                        author_norm = str(row.get('Author Name', '')).lower().strip()
                        series_norm = str(row.get('Series Name', '')).lower().strip()
                        cache_key = (author_norm, series_norm) if series_norm and series_norm != 'no_series' else None
                        
                        existing_pub = None
                        if cache_key:
                            async with publisher_lock:
                                existing_pub = series_publisher_cache.get(cache_key)
                        
                        if existing_pub:
                            amz_data['publisher'] = existing_pub
                            logger.info(f"    - Using cached publisher for series: {existing_pub}")
                        elif not amz_data.get('publisher'):
                            publisher_from_physical = await get_publisher_from_physical(page)
                            if publisher_from_physical:
                                amz_data['publisher'] = publisher_from_physical
                                logger.success(f"    ✓ Publisher from physical: {publisher_from_physical}")
                                if cache_key:
                                    async with publisher_lock:
                                        series_publisher_cache[cache_key] = publisher_from_physical
                except Exception as e:
                    logger.warning(f"    - AMZ extraction error: {e}")
            
            # MERGE AND UPDATE
            async with lock:
                updated = False
                
                # Description (prefer GR)
                desc = gr_data.get('description') or amz_data.get('description')
                if desc and (pd.isna(df.at[idx, 'Description']) or str(df.at[idx, 'Description']) in ['nan', '']):
                    df.at[idx, 'Description'] = desc[:3000]
                    # Derive Short Synopsis
                    df.at[idx, 'Short Synopsis'] = create_short_synopsis(desc)
                    # Derive Primary Trope
                    trope = analyze_trope(desc)
                    if trope:
                        df.at[idx, 'Primary Trope'] = trope
                    updated = True
                
                # Pages (prefer JSON-LD from GR)
                pages = gr_data.get('pages') or amz_data.get('pages')
                if pages:
                    try:
                        pages_int = int(re.sub(r'[^\d]', '', str(pages)))
                        if pages_int and 50 < pages_int < 5000:
                            if pd.isna(df.at[idx, 'Pages']) or df.at[idx, 'Pages'] == 0:
                                df.at[idx, 'Pages'] = pages_int
                                updated = True
                    except:
                        pass
                
                # PUBLISHER LOGIC (Aggressive)
                current_pub = str(df.at[idx, 'Publisher']).lower().strip() if not pd.isna(df.at[idx, 'Publisher']) else ""
                is_generic = any(x in current_pub for x in ['amazon digital services', 'independently published']) or current_pub in ['', 'nan']
                
                # Get candidates
                amz_pub = amz_data.get('publisher')
                gr_pub = gr_data.get('publisher')
                
                # Identify best candidate (avoid ADS)
                best_candidate = None
                if amz_pub and 'amazon digital services' not in amz_pub.lower():
                    best_candidate = amz_pub
                elif gr_pub and 'amazon digital services' not in gr_pub.lower():
                    best_candidate = gr_pub
                elif amz_pub: # Fallback to ADS if nothing else
                    best_candidate = amz_pub
                
                if best_candidate and (is_generic or pd.isna(df.at[idx, 'Publisher'])):
                    df.at[idx, 'Publisher'] = best_candidate
                    df.at[idx, 'Self Pub Flag'] = determine_self_pub(best_candidate)
                    logger.success(f"    ✓ Publisher Updated: {best_candidate} (from {'AMZ' if best_candidate == amz_pub else 'GR'})")
                    updated = True

                # Publication Date Mapping
                pub_date = gr_data.get('publication_date')
                if pub_date and (pd.isna(df.at[idx, 'Publication Date']) or str(df.at[idx, 'Publication Date']) in ['nan', '']):
                    df.at[idx, 'Publication Date'] = pub_date
                    updated = True
                
                orig_date = gr_data.get('original_publication_date')
                if orig_date and (pd.isna(df.at[idx, 'Original Published']) or str(df.at[idx, 'Original Published']) in ['nan', '']):
                    df.at[idx, 'Original Published'] = orig_date
                    updated = True

                # ASIN/ISBN
                asin = amz_data.get('asin') or gr_data.get('asin')
                
                # Goodreads Rating + Count
                gr_rating = gr_data.get('rating') or amz_data.get('goodreads_rating')
                if gr_rating:
                    try:
                        rating_float = float(gr_rating)
                        if pd.isna(df.at[idx, 'Goodreads Rating']) or df.at[idx, 'Goodreads Rating'] == 0:
                            df.at[idx, 'Goodreads Rating'] = rating_float
                            updated = True
                    except:
                        pass
                
                gr_count = gr_data.get('rating_count') or amz_data.get('goodreads_rating_count')
                if gr_count:
                    try:
                        count_int = int(re.sub(r'[^\d]', '', str(gr_count)))
                        if pd.isna(df.at[idx, 'Goodreads # of Ratings']) or df.at[idx, 'Goodreads # of Ratings'] == 0:
                            df.at[idx, 'Goodreads # of Ratings'] = count_int
                            updated = True
                    except:
                        pass
                
                # Amazon Rating + Count
                amz_rating = amz_data.get('amazon_rating')
                if amz_rating:
                    try:
                        if pd.isna(df.at[idx, 'Amazon Rating']) or df.at[idx, 'Amazon Rating'] == 0:
                            df.at[idx, 'Amazon Rating'] = float(amz_rating)
                            updated = True
                    except:
                        pass
                
                amz_count = amz_data.get('amazon_rating_count')
                if amz_count:
                    try:
                        if pd.isna(df.at[idx, 'Amazon # of Ratings']) or df.at[idx, 'Amazon # of Ratings'] == 0:
                            df.at[idx, 'Amazon # of Ratings'] = int(amz_count)
                            updated = True
                    except:
                        pass
                
                # Genre / Primary Subgenre (from GR genres)
                genres = gr_data.get('genres') or gr_data.get('primary_genre')
                if genres and (pd.isna(df.at[idx, 'Primary Subgenre']) or str(df.at[idx, 'Primary Subgenre']) in ['nan', '']):
                    df.at[idx, 'Primary Subgenre'] = genres
                    updated = True
                
                # Publication Date
                pub_date = gr_data.get('publication_date')
                if pub_date and (pd.isna(df.at[idx, 'Publication Date']) or str(df.at[idx, 'Publication Date']) in ['nan', '']):
                    df.at[idx, 'Publication Date'] = pub_date
                    updated = True
                
                # Featured List and Top Lists (from Amazon BSR)
                bsr = amz_data.get('best_sellers_rank')
                if bsr:
                    if pd.isna(df.at[idx, 'Top Lists']) or str(df.at[idx, 'Top Lists']) in ['nan', '']:
                        df.at[idx, 'Top Lists'] = bsr
                        updated = True
                    
                    if pd.isna(df.at[idx, 'Featured List']) or str(df.at[idx, 'Featured List']) in ['nan', '']:
                        parts = bsr.split(' | ')
                        for part in parts:
                            if '#' in part and 'in' in part.lower():
                                df.at[idx, 'Featured List'] = part.strip()
                                break
                        updated = True
                
                # Series info (from Amazon)
                if amz_data.get('series_name') and (pd.isna(df.at[idx, 'Series Name']) or str(df.at[idx, 'Series Name']) in ['nan', '']):
                    df.at[idx, 'Series Name'] = amz_data['series_name']
                    updated = True
                
                if amz_data.get('book_number'):
                    try:
                        if pd.isna(df.at[idx, 'Book Number']) or df.at[idx, 'Book Number'] == 0:
                            df.at[idx, 'Book Number'] = int(amz_data['book_number'])
                            updated = True
                    except:
                        pass
                
                if amz_data.get('total_books_in_series'):
                    try:
                        if pd.isna(df.at[idx, 'Total Books in Series']) or df.at[idx, 'Total Books in Series'] == 0:
                            df.at[idx, 'Total Books in Series'] = int(amz_data['total_books_in_series'])
                            updated = True
                    except:
                        pass
                
                # Author backup
                author = gr_data.get('author')
                if author and (pd.isna(df.at[idx, 'Author Name']) or str(df.at[idx, 'Author Name']) in ['nan', '']):
                    df.at[idx, 'Author Name'] = author
                    updated = True
                
                if updated:
                    logger.success(f"  [W{worker_id}] ✓ Updated: {title}")
            
            count += 1
            queue.task_done()
            
            if count % 10 == 0:
                async with lock:
                    df.to_csv(OUTPUT_FILE, index=False)
                    logger.info(f"  [Checkpoint] Saved after {count} processed")
            
            await asyncio.sleep(random.uniform(0.8, 1.5))
    finally:
        await context.close()

async def run_optimized_extraction():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"{INPUT_FILE} not found!")
        return
    
    df = pd.read_csv(INPUT_FILE)
    
    # Target: Books with any missing critical metadata
    needs_work = (
        (df['Description'].isna() | (df['Description'].astype(str) == 'nan')) |
        (df['Publisher'].isna() | (df['Publisher'].astype(str) == 'nan')) |
        (df['Goodreads Rating'].isna() | (df['Goodreads Rating'] == 0)) |
        (df['Amazon Rating'].isna() | (df['Amazon Rating'] == 0)) |
        (df['Pages'].isna() | (df['Pages'] == 0)) |
        (df['Publication Date'].isna() | (df['Publication Date'].astype(str) == 'nan'))
    )
    
    rows_to_process = df[needs_work].index.tolist()
    
    if not rows_to_process:
        logger.success("All books have complete metadata!")
        return
    
    logger.info(f"Starting OPTIMIZED extraction for {len(rows_to_process)} books...")
    logger.info("Using comprehensive extractors from goodreads_patterns.py & amazon_patterns.py")
    
    queue = asyncio.Queue()
    for idx in rows_to_process:
        await queue.put(idx)
    
    lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        workers = [extraction_worker(i, browser, queue, df, lock) for i in range(12)]
        await asyncio.gather(*workers)
        await browser.close()
    
    # NEW: Post-process to broadcast publishers across series (consistency)
    broadcast_series_publishers(df)
    
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Final report
    total = len(df)
    print("\n" + "="*60)
    print("OPTIMIZED METADATA EXTRACTION COMPLETE")
    print("="*60)
    key_fields = ['Description', 'Pages', 'Publisher', 'Amazon Rating', 'Goodreads Rating',
                  'Primary Trope', 'Primary Subgenre', 'Featured List', 'Short Synopsis', 'Self Pub flag',
                  'Goodreads # of Ratings', 'Amazon # of Ratings', 'Publication Date']
    for col in key_fields:
        if col in df.columns:
            if df[col].dtype in ['float64', 'int64']:
                missing = df[col].isna().sum() + (df[col] == 0).sum()
            else:
                missing = df[col].isna().sum() + (df[col].astype(str).isin(['nan', ''])).sum()
            pct = (total - missing) / total * 100
            status = '✅' if pct > 80 else '⚠️' if pct > 50 else '❌'
            print(f"  {status} {col:25s}: {pct:5.1f}% ({total-missing}/{total})")

def broadcast_series_publishers(df):
    """
    Final post-processing to fill in missing publishers for all books 
    in a series if at least one book in that series has a solid publisher.
    """
    logger.info("Broadcasting publishers across series for consistency...")
    
    # 1. Identify valid series-publisher mappings
    # Group by Author and Series Name, find common publishers
    def get_strong_publisher(series):
        valid_pubs = series[series.notna() & 
                          (~series.astype(str).str.lower().str.contains('amazon digital services')) & 
                          (~series.astype(str).str.lower().str.contains('independently published'))]
        if not valid_pubs.empty:
            return valid_pubs.iloc[0]
        return None

    # Filter to rows with a series
    mask = (df['Series Name'].notna()) & (df['Series Name'].astype(str) != 'NO_SERIES') & (df['Series Name'] != '')
    series_df = df[mask]
    
    if series_df.empty:
        return

    # Create mapping: (Author, Series) -> Publisher
    mapping = series_df.groupby(['Author Name', 'Series Name'])['Publisher'].apply(get_strong_publisher).to_dict()
    
    broadcast_count = 0
    for idx, row in df[mask].iterrows():
        key = (row['Author Name'], row['Series Name'])
        strong_pub = mapping.get(key)
        
        current_pub = str(row['Publisher']).lower()
        is_generic = any(x in current_pub for x in ['amazon digital services', 'independently published']) or current_pub in ['', 'nan']
        
        if strong_pub and is_generic:
            df.at[idx, 'Publisher'] = strong_pub
            df.at[idx, 'Self Pub Flag'] = determine_self_pub(strong_pub)
            broadcast_count += 1
            
    if broadcast_count > 0:
        logger.success(f"✓ Broadcasted {broadcast_count} publishers across series members.")

if __name__ == "__main__":
    asyncio.run(run_optimized_extraction())
