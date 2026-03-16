#!/usr/bin/env python3
"""
SCOUT GR RATINGS
Fetches Goodreads Ratings and Rating Counts for the 9-subgenre scouted titles.
Uses Playwright and search_goodreads.
"""
import asyncio
import os
import sys
import pandas as pd
from loguru import logger

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from genre_enrichment import create_stealth_context, search_goodreads, extract_goodreads_data
from playwright.async_api import async_playwright

async def worker(worker_id, df, queue, results_list, lock, browser):
    context = await create_stealth_context(browser)
    page = await context.new_page()
    try:
        while True:
            try:
                idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break
                
            row = df.iloc[idx]
            title = str(row.get('Book Name', '')).strip()
            author = str(row.get('Author Name', '')).strip()
            
            logger.info(f"Worker {worker_id} processing: {title} by {author}")
            gr_result = await search_goodreads(page, title, author)
            
            gr_rating = ''
            gr_rating_count = ''
            gr_link = ''
            
            if gr_result:
                gr_link = gr_result['link']
                gr_data = await extract_goodreads_data(page, gr_link)
                gr_rating = gr_data.get('gr_rating', '')
                gr_rating_count = gr_data.get('gr_rating_count', '')
                
            async with lock:
                results_list.append({
                    'Subgenre': row.get('Subgenre', ''),
                    'Series Name': row.get('Series Name', ''),
                    'Author Name': author,
                    'Book Name': title,
                    'Goodreads Link': gr_link,
                    'Goodreads Rating': gr_rating,
                    'Goodreads # of Ratings': gr_rating_count
                })
    finally:
        await context.close()

async def main():
    input_csv = os.path.join(SCRIPT_DIR, 'All_9_Subgenres_Scout_Top25.csv')
    if not os.path.exists(input_csv):
        logger.error("Scout file not found.")
        return
        
    df = pd.read_csv(input_csv)
    logger.info(f"Loaded {len(df)} titles to fetch GR ratings for.")
    
    queue = asyncio.Queue()
    for idx in range(len(df)):
        queue.put_nowait(idx)
        
    results = []
    lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            tasks = [asyncio.create_task(worker(i, df, queue, results, lock, browser)) for i in range(5)]
            await asyncio.gather(*tasks)
        finally:
            await browser.close()
            
    if results:
        out_df = pd.DataFrame(results)
        out_path = os.path.join(SCRIPT_DIR, 'All_9_Subgenres_Scout_Top25_with_GR_Ratings.csv')
        out_df.to_csv(out_path, index=False)
        logger.success(f"Saved {len(out_df)} records to {out_path}")

if __name__ == '__main__':
    asyncio.run(main())
