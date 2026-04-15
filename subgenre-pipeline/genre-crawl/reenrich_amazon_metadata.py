#!/usr/bin/env python3
"""
RE-ENRICH AMAZON METADATA
Extract Amazon metadata (Publisher, Pages, Rating, BSR) for books that have an Amazon Link but missing Publisher.
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
from genre_crawl import create_stealth_context
from genre_enrichment import extract_amazon_metadata

SLEEP_MIN = 1.0
SLEEP_MAX = 2.5
HEADLESS = True
NUM_WORKERS = 5

async def amz_worker(worker_id, missing_indices, df, p, browser):
    context = await create_stealth_context(browser)
    page = await context.new_page()
    filled = 0
    total = len(missing_indices)
    
    try:
        for i, idx in enumerate(missing_indices):
            row = df.loc[idx]
            amazon_link = str(row['Amazon Link']).strip()
            title = str(row['Book Name']).strip()
            
            try:
                amz_data = await extract_amazon_metadata(page, amazon_link)
                
                # Update DF
                if amz_data:
                    if amz_data.get('amz_rating'): df.at[idx, 'Amazon Rating'] = amz_data['amz_rating']
                    if amz_data.get('amz_rating_count'): df.at[idx, 'Amazon # of Ratings'] = amz_data['amz_rating_count']
                    if amz_data.get('amz_publisher'): df.at[idx, 'Publisher'] = amz_data['amz_publisher']
                    if amz_data.get('amz_self_pub') is not None: df.at[idx, 'Self Pub Flag'] = 'Indie' if amz_data['amz_self_pub'] else 'Trad'
                    if amz_data.get('amz_pages'): df.at[idx, 'Pages'] = amz_data['amz_pages']
                    if amz_data.get('amz_bsr'): df.at[idx, 'Amazon BSR'] = amz_data['amz_bsr']
                    if amz_data.get('amz_pub_date'): df.at[idx, 'Publication Date'] = amz_data['amz_pub_date']
                    if amz_data.get('amz_book_number'): df.at[idx, 'Book Number'] = amz_data['amz_book_number']
                    if amz_data.get('amz_total_books'): df.at[idx, 'Total Books in Series'] = amz_data['amz_total_books']
                    
                    filled += 1
                    if filled % 5 == 0:
                        logger.info(f"Worker {worker_id}: {filled}/{total} enriched")
                    
            except Exception as e:
                logger.debug(f"Worker {worker_id} Error for {title[:30]!r}: {e}")
            
            await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            
            if (i + 1) % 20 == 0:
                await page.close()
                await context.close()
                context = await create_stealth_context(browser)
                page = await context.new_page()
                
    finally:
        await page.close()
        await context.close()
        
    logger.info(f"Worker {worker_id} finished. Filled {filled}/{total}")

async def reenrich(csv_path):
    df = pd.read_csv(csv_path)
    # Target books that *have* an Amazon Link but *lack* a Publisher
    mask = df['Amazon Link'].str.startswith('http', na=False) & (df['Publisher'].isna() | (df['Publisher'] == '') | (df['Publisher'].astype(str) == 'nan'))
    indices = df[mask].index.tolist()
    logger.info(f"Total rows: {len(df)}. Books to re-enrich with Amazon metadata: {len(indices)}")
    
    if len(indices) == 0:
        return
        
    chunk_size = len(indices) // NUM_WORKERS + 1
    chunks = [indices[i:i + chunk_size] for i in range(0, len(indices), chunk_size)]
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        tasks = []
        for i, chunk in enumerate(chunks):
            if chunk: tasks.append(amz_worker(i, chunk, df, p, browser))
        await asyncio.gather(*tasks)
        await browser.close()
        
    df.to_csv(csv_path, index=False)
    logger.success(f"Amazon metadata re-enrichment complete. Saved to {csv_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, required=True)
    args = parser.parse_args()
    asyncio.run(reenrich(args.input))
