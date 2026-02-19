#!/usr/bin/env python3
"""
Re-bridge Amazon links for books that have Goodreads links but may have stale Amazon links.
Uses the Goodreads page to get the correct Amazon link.
"""
import asyncio
import pandas as pd
import os
import random
import sys
from loguru import logger
from playwright.async_api import async_playwright

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import extract_amazon_from_goodreads, safe_goto

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "data/unified_book_data_enriched_ultra.csv"

async def get_new_page(browser):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    return context, page

async def bridge_worker(worker_id, browser, queue, df, lock):
    context, page = await get_new_page(browser)
    count = 0
    
    try:
        while not queue.empty():
            try:
                idx = queue.get_nowait()
            except:
                break
            
            row = df.loc[idx]
            gr_link = str(row.get('Goodreads Link', ''))
            title = str(row.get('Book Name', ''))[:40]
            
            logger.info(f"[W{worker_id}] Bridging: {title}...")
            
            res = await extract_amazon_from_goodreads(page, gr_link)
            
            if res and res.get('amazon_link'):
                async with lock:
                    df.at[idx, 'Amazon Link'] = res['amazon_link']
                    df.at[idx, 'Status'] = 'BRIDGED_FRESH'
                    logger.success(f"  [W{worker_id}] ✓ Bridged: {res['amazon_link'][:50]}")
            else:
                logger.warning(f"  [W{worker_id}] Could not bridge: {title}")
            
            count += 1
            queue.task_done()
            
            if count % 10 == 0:
                async with lock:
                    df.to_csv(OUTPUT_FILE, index=False)
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
    finally:
        await context.close()

async def run_rebridge():
    df = pd.read_csv(INPUT_FILE)
    
    # Target: Books resolved via simplified that may have stale Amazon links
    # These have /dp/ format (original) instead of /gp/product/ (bridged)
    has_gr = df['Goodreads Link'].notna() & (df['Goodreads Link'].astype(str).str.contains('goodreads.com'))
    has_old_amz = df['Amazon Link'].astype(str).str.contains('/dp/', na=False) & \
                  ~df['Amazon Link'].astype(str).str.contains('/gp/product/', na=False)
    resolved_simplified = df['Status'] == 'RESOLVED_SIMPLIFIED'
    
    rows_to_process = df[has_gr & has_old_amz & resolved_simplified].index.tolist()
    
    if not rows_to_process:
        logger.success("No books need re-bridging!")
        return
    
    logger.info(f"Re-bridging {len(rows_to_process)} Amazon links via Goodreads...")
    
    queue = asyncio.Queue()
    for idx in rows_to_process:
        await queue.put(idx)
    
    output_lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        workers = [bridge_worker(i, browser, queue, df, output_lock) for i in range(8)]
        await asyncio.gather(*workers)
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    
    bridged = df[df['Status'] == 'BRIDGED_FRESH'].shape[0]
    logger.success(f"Re-bridging Complete: {bridged} Amazon links refreshed")

if __name__ == "__main__":
    asyncio.run(run_rebridge())
