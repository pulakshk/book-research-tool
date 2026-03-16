#!/usr/bin/env python3
"""
PARALLEL BACKFILL GOODREADS METADATA
Fills missing Goodreads data using ASIN lookups.
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
from genre_enrichment import create_stealth_context, extract_goodreads_data

SLEEP_MIN = 2.0
SLEEP_MAX = 4.0
HEADLESS = True
NUM_WORKERS = 5

async def backfill_gr_worker(worker_id, missing_indices, df, browser):
    context = await create_stealth_context(browser)
    page = await context.new_page()
    filled = 0
    total = len(missing_indices)
    
    try:
        for i, idx in enumerate(missing_indices):
            row = df.loc[idx]
            amazon_link = str(row.get('Amazon Link', ''))
            
            asin = ""
            if '/dp/' in amazon_link:
                asin = amazon_link.split('/dp/')[1].split('/')[0].split('?')[0]
                
            if not asin:
                continue
                
            isbn_url = f"https://www.goodreads.com/book/isbn/{asin}"
            
            try:
                # Use extract_goodreads_data directly
                gr_data = await extract_goodreads_data(page, isbn_url)
                
                # Check if it actually found a title or loaded properly
                if gr_data and gr_data.get('gr_rating'):
                    df.at[idx, 'Goodreads Link'] = gr_data['goodreads_link']
                    df.at[idx, 'Goodreads Rating'] = gr_data['gr_rating']
                    df.at[idx, 'Goodreads Rating Count'] = gr_data['gr_rating_count']
                    df.at[idx, 'Series Name'] = gr_data['gr_series_name']
                    df.at[idx, 'Series URL'] = gr_data['gr_series_url']
                    df.at[idx, 'Book Number'] = gr_data['gr_book_number']
                    filled += 1
                    
                    if filled % 5 == 0:
                        logger.info(f"  Worker {worker_id}: {filled}/{total} GR links filled")
            except Exception as e:
                logger.debug(f"  Worker {worker_id} Error processing ASIN {asin}: {e}")
            
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

async def backfill_goodreads(csv_path):
    df = pd.read_csv(csv_path)
    
    missing_mask = df['Goodreads Link'].isna() | (df['Goodreads Link'] == '') | (df['Goodreads Link'].astype(str) == 'nan')
    missing_indices = df[missing_mask].index.tolist()
    
    logger.info(f"Total rows: {len(df)}, Missing GR links: {len(missing_indices)}")
    
    if len(missing_indices) == 0:
        return
        
    chunk_size = len(missing_indices) // NUM_WORKERS + 1
    chunks = [missing_indices[i:i + chunk_size] for i in range(0, len(missing_indices), chunk_size)]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        tasks = []
        for i, chunk in enumerate(chunks):
            if chunk:
                tasks.append(backfill_gr_worker(i, chunk, df, browser))
        
        await asyncio.gather(*tasks)
        await browser.close()
        
    df.to_csv(csv_path, index=False)
    logger.success(f"✅ Parallel GR backfill complete. Saved to {csv_path}")
    
    still_missing = df['Goodreads Link'].isna() | (df['Goodreads Link'] == '') | (df['Goodreads Link'].astype(str) == 'nan')
    logger.info(f"  Still missing: {still_missing.sum()}/{len(df)}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    args = parser.parse_args()
    asyncio.run(backfill_goodreads(args.input))
