#!/usr/bin/env python3
"""
SCOUT SERIES AGGREGATE
1. Reads all discovered series from All_9_Subgenres_Scout_Top25.csv
2. Discards Gemini's book list; finds the official Goodreads Series URL
3. Scrapes all individual books within the series
4. Queries Goodreads for each individual book's ratings and rating counts
5. Aggregates the signals at the TRUE SERIES LEVEL (sums and averages)
Produces a final series-level output dataset.
"""

import asyncio
import os
import re
import sys
import pandas as pd
from loguru import logger

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from genre_enrichment import create_stealth_context, search_goodreads, extract_goodreads_data
from fix_series_exhaustion import fetch_series_books
from playwright.async_api import async_playwright
from genre_aggregate import aggregate_to_series, safe_float, safe_int

# Need an updated fetch that returns the book URLs instead of just titles
async def fetch_series_book_urls(page, series_url):
    from genre_crawl import safe_goto
    if not await safe_goto(page, series_url):
        return []
        
    await asyncio.sleep(2)
    urls = []
    els = await page.query_selector_all('div.listWithDividers__item a[itemprop="url"]')
    if not els:
        els = await page.query_selector_all('a.bookTitle')
        
    for el in els:
        href = await el.get_attribute("href")
        if href:
            urls.append("https://www.goodreads.com" + href if href.startswith('/') else href)
    
    # Optional deduplication preserving order
    deduped = []
    for u in urls:
        if u not in deduped:
            deduped.append(u)
    return deduped

async def worker(worker_id, series_queue, results_list, lock, browser):
    context = await create_stealth_context(browser)
    page = await context.new_page()
    try:
        while True:
            try:
                task = series_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
                
            subgenre = task['Subgenre']
            series_name = task['Series Name']
            author = task['Author Name']
            first_book = task['Book Name'] # Gemini's first provided book for anchor
            
            logger.info(f"Worker {worker_id} processing: {series_name} by {author}")
            
            # Step 1: Search for the book to get the Series URL
            gr_result = await search_goodreads(page, first_book, author)
            if not gr_result:
                logger.warning(f"Could not find GR search result for {first_book} ({series_name})")
                continue
                
            gr_data = await extract_goodreads_data(page, gr_result['link'])
            series_url = gr_data.get('gr_series_url')
            
            if not series_url:
                logger.warning(f"No series URL on GR for {series_name}. Treating as standalone.")
                book_urls = [gr_result['link']]
            else:
                logger.info(f"Found series URL: {series_url}")
                # Step 2: Fetch all book URLs in the series
                book_urls = await fetch_series_book_urls(page, series_url)
                if not book_urls:
                    book_urls = [gr_result['link']]
                    
            logger.info(f"{series_name} has {len(book_urls)} books. Fetching signals...")
            
            series_total_ratings = 0
            series_rating_points = 0.0
            
            for url in book_urls:
                b_data = await extract_goodreads_data(page, url)
                r = b_data.get('gr_rating', '')
                c = b_data.get('gr_rating_count', '')
                
                rf = safe_float(r)
                cf = safe_int(c)
                
                series_total_ratings += cf
                series_rating_points += (rf * cf)
                
            avg_rating = round((series_rating_points / series_total_ratings), 2) if series_total_ratings > 0 else 0.0
            
            async with lock:
                results_list.append({
                    'Subgenre': subgenre,
                    'Series Name': series_name,
                    'Author Name': author,
                    'Goodreads Series URL': series_url if series_url else '',
                    'Total Books in Series': len(book_urls),
                    'Average GR Rating': avg_rating,
                    'Total GR Ratings': series_total_ratings
                })
                
            logger.success(f"✓ {series_name} aggregated: {avg_rating} avg, {series_total_ratings} total RATINGS")
            
    finally:
        await context.close()


async def main():
    input_csv = os.path.join(SCRIPT_DIR, 'All_9_Subgenres_Scout_Top25.csv')
    if not os.path.exists(input_csv):
        logger.error("Scout file not found.")
        return
        
    df = pd.read_csv(input_csv)
    
    # Get unique series! Since df has book-level rows right now, just drop duplicates
    df_unique = df.drop_duplicates(subset=['Subgenre', 'Series Name', 'Author Name']).copy()
    logger.info(f"Loaded {len(df_unique)} unique Series to scout & aggregate.")
    
    queue = asyncio.Queue()
    for _, row in df_unique.iterrows():
        queue.put_nowait({
            'Subgenre': str(row.get('Subgenre', '')),
            'Series Name': str(row.get('Series Name', '')),
            'Author Name': str(row.get('Author Name', '')),
            'Book Name': str(row.get('Book Name', '')) 
        })
        
    results = []
    lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            tasks = [asyncio.create_task(worker(i, queue, results, lock, browser)) for i in range(5)]
            await asyncio.gather(*tasks)
        finally:
            await browser.close()
            
    if results:
        out_df = pd.DataFrame(results)
        # Sort so we see top aggregated
        out_df = out_df.sort_values(by=['Subgenre', 'Total GR Ratings'], ascending=[True, False])
        out_path = os.path.join(SCRIPT_DIR, 'All_9_Subgenres_Scout_Top25_AGGREGATED.csv')
        out_df.to_csv(out_path, index=False)
        logger.success(f"Saved {len(out_df)} aggregated series records to {out_path}")

if __name__ == '__main__':
    asyncio.run(main())
