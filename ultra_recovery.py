
import asyncio
import pandas as pd
import os
import re
import random
import json
import sys
from datetime import datetime
from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import core utilities
from utils.text_normalizer import normalize_title, normalize_author, TextNormalizer
from utils.matcher import BookMatcher
import config
from extractors.amazon_patterns import extract_amazon_comprehensive
from utils.filter import is_sports_hockey_related
from utils.bridge_utils import extract_amazon_from_goodreads, safe_goto

# Config
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
INPUT_FILE = os.path.join(BASE_DIR, "unified_book_data_enriched_ultra.csv")
OUTPUT_FILE = os.path.join(BASE_DIR, "unified_book_data_enriched_ultra.csv")

async def get_new_page(browser):
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    ua = random.choice(user_agents)
    context = await browser.new_context(user_agent=ua, viewport={'width': 1920, 'height': 1080})
    page = await context.new_page()
    return context, page

async def search_goodreads_first(page, title, author=""):
    """
    Core Discovery Logic: Search Goodreads first, then bridge to Amazon.
    """
    try:
        query = f"{title} {author}".strip()
        url = f"https://www.goodreads.com/search?q={query.replace(' ', '+')}"
        logger.debug(f"  [GR Search] -> {url}")
        
        if not await safe_goto(page, url):
            return None
            
        # Check for results
        items = await page.query_selector_all("tr[itemtype='http://schema.org/Book']")
        if not items:
            # Try mobile/alternate layout
            items = await page.query_selector_all("a.bookTitle")
            
        if not items:
            logger.debug(f"    - No GR results for: {title}")
            return None
            
        # Extract first result details
        first_item = items[0]
        title_el = await first_item.query_selector("a.bookTitle")
        author_el = await first_item.query_selector("a.authorName")
        
        found_title = (await title_el.inner_text()).strip() if title_el else "Unknown"
        found_author = (await author_el.inner_text()).strip() if author_el else "Unknown"
        gr_link = "https://www.goodreads.com" + await title_el.get_attribute("href") if title_el else None
        
        # Fuzzy match to ensure relevance
        matcher = BookMatcher()
        if not matcher.normalizer.fuzzy_match_titles(title, found_title):
            logger.debug(f"    - GR title mismatch: '{found_title}' vs target '{title}'")
            return None
            
        logger.success(f"    ✓ Found GR Match: {found_title} by {found_author}")
        
        # BRIDGE to Amazon
        bridge_res = await extract_amazon_from_goodreads(page, gr_link)
        
        return {
            'author': found_author,
            'gr_link': gr_link,
            'amz_link': bridge_res['amazon_link'] if bridge_res else None,
            'strategy': bridge_res['strategy'] if bridge_res else 'GR_Matched'
        }
        
    except Exception as e:
        logger.warning(f"    - GR Search Error: {e}")
        return None

async def fallback_search_amazon(page, title, author=""):
    """
    Final Fallback: Search Amazon directly if Goodreads fails.
    """
    try:
        query = f"{title} {author}".strip()
        url = f"https://www.amazon.com/s?k={query.replace(' ', '+')}"
        logger.debug(f"  [AMZ Fallback Search] -> {url}")
        
        if not await safe_goto(page, url):
            return None
            
        # Check for results
        items = await page.query_selector_all("div[data-component-type='s-search-result']")
        if not items:
            return None
            
        first_item = items[0]
        title_el = await first_item.query_selector("h2 a.a-link-normal")
        if not title_el: return None
        
        href = await title_el.get_attribute("href")
        if not href: return None
        
        amz_url = "https://www.amazon.com" + href.split("?")[0]
        
        # Navigate to verify and extract
        if await safe_goto(page, amz_url):
            data = await extract_amazon_comprehensive(page, scroll_first=False)
            return {
                'amz_link': amz_url,
                'author': data.get('author'),
                'title': data.get('title')
            }
        return None
    except Exception as e:
        logger.warning(f"    - AMZ Fallback Error: {e}")
        return None

async def recovery_worker(worker_id, browser, queue, df, output_lock):
    """Worker to process books using Goodreads-First principle."""
    context, page = await get_new_page(browser)
    try:
        count = 0
        while not queue.empty():
            idx, row = await queue.get()
            title = str(row.get('Book Name', ''))
            author_target = str(row.get('Author Name', '')) if not pd.isna(row.get('Author Name')) else ""
            
            logger.info(f"[Worker {worker_id}] Processing: {title}")
            
            # Period rotation
            if count > 0 and count % 15 == 0:
                await context.close()
                context, page = await get_new_page(browser)

            try:
                # 1. GOODREADS FIRST
                res = await search_goodreads_first(page, title, author_target)
                
                # 2. AMAZON FALLBACK (if GR bridge failed)
                if not res or not res.get('amz_link'):
                    logger.info(f"  [Worker {worker_id}] -> GR Bridge failed or link missing, trying AMZ direct...")
                    amz_res = await fallback_search_amazon(page, title, author_target)
                    if amz_res:
                        if not res: res = {}
                        res.update({
                            'amz_link': amz_res['amz_link'],
                            'author': amz_res.get('author') or res.get('author'),
                            'strategy': res.get('strategy', '') + '_AMZ_Fallback'
                        })

                if res:
                    async with output_lock:
                        if res.get('author') and (pd.isna(df.at[idx, 'Author Name']) or df.at[idx, 'Author Name'] in ["", "nan"]):
                            df.at[idx, 'Author Name'] = res['author']
                        
                        if res.get('amz_link'):
                            df.at[idx, 'Amazon Link'] = res['amz_link']
                            df.at[idx, 'Status'] = 'RESOLVED_DP'
                            logger.success(f"  [Worker {worker_id}] -> Success: {res['amz_link']}")
                            
                        if res.get('gr_link') and (pd.isna(df.at[idx, 'Goodreads Link']) or df.at[idx, 'Goodreads Link'] in ["", "nan"]):
                            df.at[idx, 'Goodreads Link'] = res['gr_link']
                else:
                    logger.warning(f"  [Worker {worker_id}] -> Could not resolve: {title}")
                    async with output_lock:
                        df.at[idx, 'Status'] = 'FAILED_RECOVERY'
                        
            except Exception as e:
                logger.error(f"  [Worker {worker_id}] Error: {e}")
            
            count += 1
            queue.task_done()
            
            if count % 10 == 0:
                async with output_lock:
                    df.to_csv(OUTPUT_FILE)
            
            # Reduced anti-bot delay for speed
            await asyncio.sleep(random.uniform(1, 2.5))
            
    finally:
        await context.close()

async def run_ultra_recovery():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"{INPUT_FILE} not found!")
        return

    df = pd.read_csv(INPUT_FILE)
    if 'index' in df.columns:
        df.set_index('index', inplace=True)
    elif df.index.name != 'index' and df.columns[0] == 'Unnamed: 0':
        df = df.rename(columns={'Unnamed: 0': 'index'}).set_index('index')
    
    # Target: Gaps in links, authors, or ratings
    # NEW: Actively target missing Goodreads links and missing Amazon ratings
    mask = (df['Amazon Link'].isna() | (df['Amazon Link'].astype(str).str.contains('/s?k=', regex=False))) | \
           (df['Goodreads Link'].isna() | (df['Goodreads Link'].astype(str) == "nan")) | \
           (df['Author Name'].isna() | (df['Author Name'].astype(str) == "nan")) | \
           (df['Amazon Rating'].isna() | (df['Amazon Rating'] == 0))
           
    rows_to_process = df[mask].index.tolist()
    if not rows_to_process:
        logger.success("No gaps to recover in ultra_recovery.")
        return

    logger.info(f"PHASE 13: Goodreads-First Recovery for {len(rows_to_process)} books...")
    
    queue = asyncio.Queue()
    for idx in rows_to_process:
        await queue.put((idx, df.loc[idx].to_dict()))
        
    output_lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # 10 Workers for FASTER parallel execution
        workers = [recovery_worker(i, browser, queue, df, output_lock) for i in range(10)]
        await asyncio.gather(*workers)
        await browser.close()
        
    df.to_csv(OUTPUT_FILE)
    logger.success(f"Recovery Pass Complete. Final saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(run_ultra_recovery())
