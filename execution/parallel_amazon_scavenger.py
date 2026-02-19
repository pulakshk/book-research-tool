#!/usr/bin/env python3
"""
PARALLEL AMAZON SCAVENGER
- Identifies gaps in Amazon metadata (Link, Rating, BSR, Pages).
- Uses Playwright to bridge from Goodreads or search Amazon directly.
- Saves results to amazon_delta.csv to avoid master CSV contention.
- Runs with 10 parallel workers.
"""

import asyncio
import pandas as pd
import os
import re
import random
import json
import sys
from loguru import logger
from playwright.async_api import async_playwright

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import extract_amazon_from_goodreads, safe_goto
from extractors.amazon_patterns import extract_amazon_comprehensive

# CONFIG
INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
DELTA_FILE = "amazon_delta.csv"

async def get_new_page(browser):
    user_agents = [
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ]
    ua = random.choice(user_agents)
    context = await browser.new_context(user_agent=ua, viewport={'width': 1280, 'height': 800})
    page = await context.new_page()
    return context, page

async def worker(worker_id, browser, queue, delta_list, lock):
    """Worker to fulfill Amazon gaps with self-healing."""
    context, page = await get_new_page(browser)
    try:
        while not queue.empty():
            try:
                # SELF-HEALING: Check if page is closed
                try:
                    if page.is_closed():
                        context, page = await get_new_page(browser)
                except:
                    context, page = await get_new_page(browser)

                try:
                    item = queue.get_nowait()
                except asyncio.QueueEmpty:
                    break
                    
                idx = item['index']
                title = item['Book Name']
                author = item['Author Name']
                gr_link = item.get('Goodreads Link')
                
                logger.info(f"[Worker {worker_id}] Handling: {title} by {author}")
                
                res_data = {
                    'index': idx, 
                    'Amazon Link': None, 
                    # 'Amazon Rating': None, # DISABLED per user request (Goodreads Only)
                    # 'Amazon # of Ratings': None, # DISABLED per user request (Goodreads Only)
                    'Pages': None, 
                    'BSR': None,
                    'Publisher': None,
                    'Publication Date': None,
                    'Short Synopsis': None,
                    'Top Lists': None,
                    'Featured List': None
                }
                success = False

                try:
                    # Priority 1: Bridge from Goodreads
                    if gr_link and pd.notna(gr_link) and "goodreads.com" in str(gr_link):
                        logger.debug(f"  [Worker {worker_id}] Bridging from GR: {gr_link}")
                        bridge = await extract_amazon_from_goodreads(page, gr_link)
                        if bridge and bridge.get('amazon_link'):
                            amz_url = bridge['amazon_link']
                            if await safe_goto(page, amz_url):
                                details = await extract_amazon_comprehensive(page, scroll_first=False)
                                res_data.update({
                                    'Amazon Link': amz_url,
                                    # 'Amazon Rating': details.get('rating') or details.get('amazon_rating'),
                                    # 'Amazon # of Ratings': details.get('rating_count') or details.get('amazon_rating_count'),
                                    'Pages': details.get('pages'),
                                    'BSR': details.get('best_sellers_rank'),
                                    'Publisher': details.get('publisher'),
                                    'Publication Date': details.get('publication_date'),
                                    'Short Synopsis': details.get('short_synopsis'),
                                    'Top Lists': details.get('best_sellers_rank'),
                                    'Featured List': details.get('best_sellers_rank')
                                })
                                success = True
                    
                    # Priority 2: Direct Amazon Search
                    if not success:
                        query = f"{title} {author} book".replace(' ', '+')
                        search_url = f"https://www.amazon.com/s?k={query}"
                        logger.debug(f"  [Worker {worker_id}] Searching Amazon: {search_url}")
                        if await safe_goto(page, search_url):
                            # Simple selector for first product
                            first_result = await page.query_selector("div[data-component-type='s-search-result'] h2 a")
                            if first_result:
                                href = await first_result.get_attribute("href")
                                full_url = "https://www.amazon.com" + href.split('?')[0] if href.startswith('/') else href
                                if await safe_goto(page, full_url):
                                    details = await extract_amazon_comprehensive(page, scroll_first=False)
                                    res_data.update({
                                        'Amazon Link': full_url,
                                        # 'Amazon Rating': details.get('rating') or details.get('amazon_rating'),
                                        # 'Amazon # of Ratings': details.get('rating_count') or details.get('amazon_rating_count'),
                                        'Pages': details.get('pages'),
                                        'BSR': details.get('best_sellers_rank'),
                                        'Publisher': details.get('publisher'),
                                        'Publication Date': details.get('publication_date'),
                                        'Short Synopsis': details.get('short_synopsis'),
                                        'Top Lists': details.get('best_sellers_rank'),
                                        'Featured List': details.get('best_sellers_rank')
                                    })
                                    success = True

                    if success:
                        async with lock:
                            delta_list.append(res_data)
                            # Periodic save
                            if len(delta_list) % 5 == 0:
                                pd.DataFrame(delta_list).to_csv(DELTA_FILE, index=False)
                        logger.success(f"  [Worker {worker_id}] ✓ Success: {title}")
                    else:
                        logger.warning(f"  [Worker {worker_id}] ✗ Failed: {title}")

                except Exception as e:
                    logger.error(f"  [Worker {worker_id}] Local error: {e}")
                    # Force page reload on next loop
                    try: await page.close()
                    except: pass
                
                queue.task_done()
                await asyncio.sleep(random.uniform(2, 5))

            except Exception as e:
                logger.error(f"  [Worker {worker_id}] Critical loop error: {e}")
                context, page = await get_new_page(browser)
            
    finally:
        try: await context.close()
        except: pass

async def main():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"{INPUT_FILE} not found.")
        return

    df = pd.read_csv(INPUT_FILE)
    if 'index' not in df.columns:
        df['index'] = df.index
    
    # Identify gaps (Only check Link or Pages, ignore Rating since we don't want Amz Rating anymore)
    mask = (df['Amazon Link'].isna()) # | (df['Amazon Rating'].isna()) | (df['Amazon Rating'] == 0)
    gaps = df[mask].to_dict('records')
    
    logger.info(f"Identified {len(gaps)} books with Amazon gaps. Starting 15 workers.")
    
    queue = asyncio.Queue()
    for item in gaps:
        await queue.put(item)
        
    delta_list = []
    if os.path.exists(DELTA_FILE):
        try:
            delta_list = pd.read_csv(DELTA_FILE).to_dict('records')
            # Filter queue to skip already delta-processed
            done_indices = set([d['index'] for d in delta_list])
            logger.info(f"Resuming: {len(done_indices)} already in delta.")
            # We rebuild the queue
            queue = asyncio.Queue()
            for item in gaps:
                if item['index'] not in done_indices:
                    await queue.put(item)
        except:
            pass

    lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Adjusted to 12 workers per user request
        workers = [worker(i, browser, queue, delta_list, lock) for i in range(12)]
        await asyncio.gather(*workers)
        await browser.close()
        
    # Final save
    if delta_list:
        pd.DataFrame(delta_list).to_csv(DELTA_FILE, index=False)
    logger.success(f"Parallel Scavenger finished. Results in {DELTA_FILE}")

if __name__ == "__main__":
    asyncio.run(main())
