
import asyncio
import pandas as pd
import os
import re
import random
import sys
from loguru import logger
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from urllib.parse import quote_plus

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.text_normalizer import normalize_title, calculate_similarity
from utils.matcher import BookMatcher
from utils.filter import is_sports_hockey_related
from extractors.amazon_patterns import extract_amazon_comprehensive
from utils.bridge_utils import extract_amazon_from_goodreads, safe_goto

INPUT_FILE = "unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "unified_book_data_enriched_ultra.csv"
HOTFIX_FILE = "recovered_links_hotfix.csv"

async def get_new_page(browser):
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    ua = random.choice(user_agents)
    context = await browser.new_context(user_agent=ua, viewport={'width': 1920, 'height': 1080})
    page = await context.new_page()
    return context, page

async def fallback_search_goodreads(page, title, author):
    """Goodreads-First discovery engine."""
    try:
        query = f"{title} {author}".strip()
        url = f"https://www.goodreads.com/search?q={quote_plus(query)}"
        logger.debug(f"  [GR Search] -> {url}")
        
        if not await safe_goto(page, url): return "NETWORK_FAIL"
        await asyncio.sleep(random.uniform(1, 2))
        
        content = await page.content()
        if "captcha" in content.lower(): return "CAPTCHA"

        book_links = await page.query_selector_all("a.bookTitle")
        if not book_links: return "NO_ITEMS"
            
        matcher = BookMatcher()
        
        for i, link in enumerate(book_links[:5]):
            ext_title = (await link.inner_text()).strip()
            container = await link.query_selector("xpath=ancestor::tr")
            ext_author = ""
            if container:
                author_el = await container.query_selector("a.authorName, span[itemprop='author'] a")
                if author_el: ext_author = await author_el.inner_text()
            
            # Fuzzy match
            if matcher.normalizer.fuzzy_match_titles(title, ext_title):
                g_link = "https://www.goodreads.com" + await link.get_attribute("href")
                logger.success(f"  [GR Match] -> {ext_title} by {ext_author}")
                return {'Goodreads Link': g_link, 'Author Name': ext_author, 'Status': 'MATCHED_GR'}
                
        return "LOW_SIMILARITY_GR"
    except Exception as e:
        logger.warning(f"  - GR search error: {e}")
        return "ERROR_GR"

async def fallback_worker(worker_id, browser, queue, results):
    """Concurrent worker implementing the Goodreads-First + Bridge principle."""
    context, page = await get_new_page(browser)
    try:
        while not queue.empty():
            idx, title, author = await queue.get()
            logger.info(f"[Worker {worker_id}] Processing: {title}")
            
            # 1. Search Goodreads First
            res = await fallback_search_goodreads(page, title, author)
            
            if res == "CAPTCHA":
                logger.error(f"!!! [Worker {worker_id}] Goodreads Captcha Blocked.")
                break
                
            if isinstance(res, dict):
                # 2. Bridge to Amazon immediately
                amz_bridge = await extract_amazon_from_goodreads(page, res['Goodreads Link'])
                if amz_bridge:
                    res['Amazon Link'] = amz_bridge['amazon_link']
                    res['Status'] = 'RESOLVED_DP'
                    logger.success(f"  [Bridge Success] -> Resolved Amazon DP")
                
                res['Title'] = title
            else:
                res = {"Status": f"GR_{res}", "Title": title}
            
            results[idx] = res
            queue.task_done()
            
            # Atomic save
            if len(results) % 5 == 0:
                pd.DataFrame.from_dict(results, orient='index').to_csv(HOTFIX_FILE)
            
            await asyncio.sleep(random.uniform(2, 5))
                
    finally:
        await context.close()

async def run_parallel_fallback():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"{INPUT_FILE} missing.")
        return

    df = pd.read_csv(INPUT_FILE)
    if 'index' in df.columns:
        df.set_index('index', inplace=True)
    elif df.columns[0] == 'Unnamed: 0':
        df = df.rename(columns={'Unnamed: 0': 'index'}).set_index('index')
    
    # Target gaps in Amazon links
    mask = (df['Amazon Link'].isna() | (df['Amazon Link'].astype(str).str.contains('/s?k=', regex=False)))
    gaps = df[mask]
    
    if gaps.empty:
        logger.success("No search links or gaps to resolve in parallel_fallback.")
        return

    logger.info(f"Targeting {len(gaps)} books for Goodreads-First BRIDGE resolution.")
    
    results = {}
    if os.path.exists(HOTFIX_FILE):
        try:
            temp_df = pd.read_csv(HOTFIX_FILE, index_col=0)
            results = temp_df.to_dict(orient='index')
        except:
            pass

    queue = asyncio.Queue()
    for idx, row in gaps.iterrows():
        if idx not in results or '/dp/' not in str(results[idx].get('Amazon Link', '')):
            await queue.put((idx, row['Book Name'], row['Author Name'] if not pd.isna(row['Author Name']) else ""))
            
    if queue.empty():
        logger.info("All gaps covered in existing hotfix.")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Using 5 workers as requested for Phase 13
        workers = [fallback_worker(i, browser, queue, results) for i in range(5)]
        await asyncio.gather(*workers)
        await browser.close()

    # Final Merge
    if results:
        hotfix_df = pd.DataFrame.from_dict(results, orient='index')
        hotfix_df.to_csv(HOTFIX_FILE)
        
        for idx, row in hotfix_df.iterrows():
            if idx in df.index:
                status = str(row.get('Status', ''))
                if 'RESOLVED_DP' in status or 'MATCHED' in status:
                    df.at[idx, 'Amazon Link'] = row.get('Amazon Link')
                    if row.get('Goodreads Link'):
                        df.at[idx, 'Goodreads Link'] = row.get('Goodreads Link')
                    if row.get('Author Name') and (pd.isna(df.at[idx, 'Author Name']) or df.at[idx, 'Author Name'] == ""):
                        df.at[idx, 'Author Name'] = row.get('Author Name')
        
        df.to_csv(OUTPUT_FILE)
        logger.success(f"Resolved links merged into {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(run_parallel_fallback())
