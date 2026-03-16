#!/usr/bin/env python3
import asyncio
import os
import sys
import pandas as pd
from loguru import logger

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from genre_enrichment import create_stealth_context, search_goodreads, extract_goodreads_data
from scout_series_aggregate import fetch_series_book_urls
from playwright.async_api import async_playwright
from genre_aggregate import safe_float, safe_int

async def process_missing(df, missing_idx, lock, browser):
    context = await create_stealth_context(browser)
    page = await context.new_page()
    try:
        for idx in missing_idx:
            row = df.loc[idx]
            series_name = str(row['Series Name']).strip()
            author = str(row['Author Name']).strip()
            
            # Use series name + author for the direct GR search instead of Book 1
            query = f"{series_name} Series {author}"
            logger.info(f"Retrying: {query}")
            
            gr_result = await search_goodreads(page, query, "")
            if not gr_result:
                logger.warning(f"Still no result for {query}")
                continue
                
            gr_data = await extract_goodreads_data(page, gr_result['link'])
            series_url = gr_data.get('gr_series_url')
            
            if series_url:
                logger.success(f"Found missing URL: {series_url}")
                book_urls = await fetch_series_book_urls(page, series_url)
                if not book_urls:
                    continue
                    
                series_total_ratings = 0
                series_rating_points = 0.0
                
                for url in book_urls:
                    b_data = await extract_goodreads_data(page, url)
                    r = b_data.get('gr_rating', '')
                    c = b_data.get('gr_rating_count', '')
                    series_total_ratings += safe_int(c)
                    series_rating_points += (safe_float(r) * safe_int(c))
                    
                avg_rating = round((series_rating_points / series_total_ratings), 2) if series_total_ratings > 0 else 0.0
                
                async with lock:
                    df.at[idx, 'Goodreads Series URL'] = series_url
                    df.at[idx, 'Total Books in Series'] = len(book_urls)
                    df.at[idx, 'Average GR Rating'] = avg_rating
                    df.at[idx, 'Total GR Ratings'] = series_total_ratings
                
    finally:
        await context.close()


async def main():
    file_path = os.path.join(SCRIPT_DIR, 'All_9_Subgenres_Scout_Top25_AGGREGATED.csv')
    df = pd.read_csv(file_path)
    
    missing_df = df[df['Goodreads Series URL'].isna() | (df['Goodreads Series URL'] == '')]
    if missing_df.empty:
        logger.info("No missing series URLs.")
        return
        
    logger.info(f"Attempting to fix {len(missing_df)} missing Series URLs (Aggressive Search).")
    missing_indices = missing_df.index.tolist()
    
    lock = asyncio.Lock()
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        try:
            # Chunk for 3 workers
            n = len(missing_indices)
            chunks = [missing_indices[i::3] for i in range(3)]
            tasks = [asyncio.create_task(process_missing(df, chunk, lock, browser)) for chunk in chunks if chunk]
            await asyncio.gather(*tasks)
        finally:
            await browser.close()
            
    df.to_csv(file_path, index=False)
    logger.success("Fix pass complete. Saved updated aggregated CSV.")

if __name__ == '__main__':
    asyncio.run(main())
