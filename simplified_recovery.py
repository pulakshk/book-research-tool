#!/usr/bin/env python3
"""
Second-pass Goodreads recovery using SIMPLIFIED titles.
Strips subtitles (after : or () and special characters for better matching.
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
from utils.bridge_utils import extract_amazon_from_goodreads, safe_goto

INPUT_FILE = "unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "unified_book_data_enriched_ultra.csv"

def simplify_title(title):
    """Remove subtitle after : or ( and strip special chars."""
    if not title or pd.isna(title):
        return ""
    # Take first part before colon or opening bracket
    simplified = re.split(r'[:\(\[]', str(title))[0].strip()
    # Remove special characters except spaces
    simplified = re.sub(r'[^\w\s]', '', simplified)
    return simplified.strip()

async def get_new_page(browser):
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    ]
    context = await browser.new_context(user_agent=random.choice(user_agents))
    page = await context.new_page()
    return context, page

async def search_gr_simplified(page, title, author):
    """Search Goodreads with simplified title + author."""
    simple_title = simplify_title(title)
    if not simple_title:
        return None
    
    query = f"{simple_title} {author}".strip()
    url = f"https://www.goodreads.com/search?q={query.replace(' ', '+')}"
    logger.info(f"  [Simplified Search] {query}")
    
    if not await safe_goto(page, url):
        return None
    
    try:
        await page.wait_for_selector("a.bookTitle", timeout=10000)
    except:
        logger.debug(f"    - No results for simplified: {simple_title}")
        return None
    
    # Get first result
    first = await page.query_selector("a.bookTitle")
    if not first:
        return None
    
    found_title = (await first.inner_text()).strip()
    href = await first.get_attribute("href")
    gr_link = href if href.startswith("http") else "https://www.goodreads.com" + href
    
    logger.success(f"    ✓ Found: {found_title}")
    
    # Bridge to Amazon
    amz_res = await extract_amazon_from_goodreads(page, gr_link)
    
    return {
        'gr_link': gr_link,
        'amz_link': amz_res['amazon_link'] if amz_res else None
    }

async def recovery_worker(worker_id, browser, queue, df, lock):
    context, page = await get_new_page(browser)
    count = 0
    
    try:
        while not queue.empty():
            try:
                idx = queue.get_nowait()
            except:
                break
            
            row = df.loc[idx]
            title = str(row.get('Book Name', ''))
            author = str(row.get('Author Name', ''))
            
            if not title or title == 'nan':
                queue.task_done()
                continue
            
            logger.info(f"[W{worker_id}] Processing: {title[:50]}...")
            
            res = await search_gr_simplified(page, title, author)
            
            if res:
                async with lock:
                    if res.get('gr_link'):
                        df.at[idx, 'Goodreads Link'] = res['gr_link']
                    # ALWAYS update Amazon link from bridge (it's the correct edition)
                    if res.get('amz_link'):
                        df.at[idx, 'Amazon Link'] = res['amz_link']
                    df.at[idx, 'Status'] = 'RESOLVED_SIMPLIFIED'
                    logger.success(f"  [W{worker_id}] ✓ Resolved: {title[:40]}")
            else:
                async with lock:
                    df.at[idx, 'Status'] = 'FAILED_SIMPLIFIED'
            
            count += 1
            queue.task_done()
            
            if count % 10 == 0:
                async with lock:
                    df.to_csv(OUTPUT_FILE, index=False)
            
            await asyncio.sleep(random.uniform(1, 2))
    finally:
        await context.close()

async def run_simplified_recovery():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"{INPUT_FILE} not found!")
        return
    
    df = pd.read_csv(INPUT_FILE)
    
    # Target: Has Amazon link but missing Goodreads
    has_amz = df['Amazon Link'].notna() & (df['Amazon Link'].astype(str) != '')
    missing_gr = df['Goodreads Link'].isna() | (df['Goodreads Link'].astype(str) == '') | (df['Goodreads Link'].astype(str) == 'nan')
    
    rows_to_process = df[has_amz & missing_gr].index.tolist()
    
    if not rows_to_process:
        logger.success("No books need simplified recovery!")
        return
    
    logger.info(f"Starting SIMPLIFIED recovery for {len(rows_to_process)} books...")
    
    queue = asyncio.Queue()
    for idx in rows_to_process:
        await queue.put(idx)
    
    output_lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # 8 workers for speed
        workers = [recovery_worker(i, browser, queue, df, output_lock) for i in range(8)]
        await asyncio.gather(*workers)
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Report
    resolved = df[df['Status'] == 'RESOLVED_SIMPLIFIED'].shape[0]
    still_missing = df['Goodreads Link'].isna().sum()
    logger.success(f"Simplified Recovery Complete: {resolved} resolved, {still_missing} still missing")

if __name__ == "__main__":
    asyncio.run(run_simplified_recovery())
