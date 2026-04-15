#!/usr/bin/env python3
"""
PARALLEL BACKFILL AMAZON LINKS
Quickly fill missing Amazon links via parallel workers.
"""

import asyncio
import os
import random
import sys

import pandas as pd
from loguru import logger
from playwright.async_api import async_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from genre_crawl import create_stealth_context, safe_goto

SLEEP_MIN = 1.0
SLEEP_MAX = 2.5
HEADLESS = True
NUM_WORKERS = 5

async def backfill_worker(worker_id, missing_indices, df, p, browser):
    context = await create_stealth_context(browser)
    page = await context.new_page()
    filled = 0
    total = len(missing_indices)
    
    try:
        for i, idx in enumerate(missing_indices):
            row = df.loc[idx]
            title = str(row['Book Name']).strip()
            author = str(row.get('Author Name', '')).strip()
            if author.lower() in ['kindle edition', 'nan', '']:
                author = ''
            
            query = f"{title} {author}".strip().replace(' ', '+')
            search_url = f"https://www.amazon.com/s?k={query}&i=digital-text"
            
            try:
                if not await safe_goto(page, search_url, timeout=30000):
                    continue
                
                await asyncio.sleep(random.uniform(1.0, 2.0))
                
                first_result = await page.query_selector("div[data-component-type='s-search-result']")
                if not first_result:
                    first_result = await page.query_selector("div.s-result-item.s-asin")
                
                if first_result:
                    asin = await first_result.get_attribute("data-asin")
                    if asin:
                        amazon_link = f"https://www.amazon.com/dp/{asin}"
                        df.at[idx, 'Amazon Link'] = amazon_link
                        filled += 1
                        if filled % 5 == 0:
                            logger.info(f"  Worker {worker_id}: {filled}/{total} links filled")
                
            except Exception as e:
                logger.debug(f"  Worker {worker_id} Error for '{title[:40]}': {e}")
            
            await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            
            # Rotate context periodically
            if (i + 1) % 15 == 0:
                await page.close()
                await context.close()
                context = await create_stealth_context(browser)
                page = await context.new_page()
                
    finally:
        await page.close()
        await context.close()
    
    logger.info(f"Worker {worker_id} finished. Filled {filled}/{total}")

async def backfill_amazon_links(csv_path):
    df = pd.read_csv(csv_path)
    missing = df[df['Amazon Link'].isna() | (df['Amazon Link'] == '') | (df['Amazon Link'].astype(str) == 'nan')]
    logger.info(f"Total rows: {len(df)}, Missing Amazon links: {len(missing)}")
    
    if len(missing) == 0:
        return
        
    indices = missing.index.tolist()
    chunk_size = len(indices) // NUM_WORKERS + 1
    chunks = [indices[i:i + chunk_size] for i in range(0, len(indices), chunk_size)]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        tasks = []
        for i, chunk in enumerate(chunks):
            if chunk:
                tasks.append(backfill_worker(i, chunk, df, p, browser))
        
        await asyncio.gather(*tasks)
        await browser.close()
        
    df.to_csv(csv_path, index=False)
    logger.success(f"✅ Parallel backfill complete. Saved to {csv_path}")
    
    still_missing = df['Amazon Link'].isna() | (df['Amazon Link'] == '') | (df['Amazon Link'].astype(str) == 'nan')
    logger.info(f"  Still missing: {still_missing.sum()}/{len(df)}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    args = parser.parse_args()
    asyncio.run(backfill_amazon_links(args.input))
