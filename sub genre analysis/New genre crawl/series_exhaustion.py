#!/usr/bin/env python3
"""
SERIES EXHAUSTION - Phase 1.5
Scans the raw discovery CSV for any assigned Series Name. 
If a book belongs to a series, it fetches the full series list from Goodreads,
and injects the missing book titles into the dataset for enrichment.
"""

import os
import re
import sys
import pandas as pd
from loguru import logger

# Try loading the Goodreads scraping functions from genre_enrichment
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from genre_enrichment import search_goodreads, extract_goodreads_data, create_stealth_context
from playwright.async_api import async_playwright
import asyncio

async def exhaust_series(raw_csv_path):
    logger.info(f"Running Series Exhaustion on {raw_csv_path}")
    
    df = pd.read_csv(raw_csv_path)
    if 'Series Name' not in df.columns:
        logger.warning("No Series Name column found. Returning original CSV.")
        return raw_csv_path
        
    series_groups = df.dropna(subset=['Series Name']).groupby('Series Name')
    logger.info(f"Found {len(series_groups)} distinct series to exhaust.")
    
    missing_books = []
    
    async with async_playwright() as p:
        browser, context, page = await create_stealth_context(p, headless=True)
        
        try:
            for series_name, group in series_groups:
                series_name_clean = str(series_name).strip()
                if not series_name_clean: continue
                
                # We need to find the series URL on Goodreads.
                # First, find a book in our group that hopefully has it.
                author = str(group['Author Name'].iloc[0])
                first_book_title = str(group['Book Name'].iloc[0])
                
                logger.info(f"Exhausting: {series_name_clean} by {author}")
                
                # Fetch first book's GR page to get the Series URL
                gr_url = await search_goodreads(page, first_book_title, author)
                if not gr_url:
                    logger.warning(f"Could not find GR page for {first_book_title}. Skipping series exhaustion.")
                    continue
                    
                gr_data = await extract_goodreads_data(page, gr_url)
                series_url = gr_data.get('gr_series_url')
                
                if not series_url:
                    logger.warning(f"No series URL found on GR for {first_book_title}.")
                    continue
                    
                # Navigate to Series Page
                from genre_crawl import safe_goto
                if await safe_goto(page, series_url):
                    await asyncio.sleep(2)
                    
                    # Extract all book titles in the series
                    # Usually formatted as `div.listWithDividers__item .bookTitle span`
                    books_in_series = await page.query_selector_all('.listWithDividers__item a.bookTitle span')
                    if not books_in_series:
                        books_in_series = await page.query_selector_all('a.bookTitle span[itemprop="name"]')
                        
                    found_titles = []
                    for b in books_in_series:
                        t = await b.text_content()
                        if t:
                            # Clean up title (remove "Book 1", etc if appended by GR)
                            clean_t = re.sub(r'\(.*?\)', '', t).strip()
                            found_titles.append(clean_t)
                            
                    logger.info(f"Found {len(found_titles)} books in the series on Goodreads.")
                    
                    # Check which books are missing from our dataset
                    existing_titles = [str(x).lower().strip() for x in group['Book Name'].tolist()]
                    
                    for ft in found_titles:
                        ft_lower = ft.lower().strip()
                        # If the book isn't in our dataset for this series, add it!
                        is_missing = True
                        for ext in existing_titles:
                            if ft_lower in ext or ext in ft_lower:
                                is_missing = False
                                break
                                
                        if is_missing:
                            missing_books.append({
                                'Book Name': ft,
                                'Author Name': author,
                                'Amazon Link': '',
                                'Series Name': series_name_clean,
                                'Source': 'Goodreads Exhaustion',
                                'Source Detail': 'Series Expansion',
                                'Subgenre': group['Subgenre'].iloc[0] if 'Subgenre' in group.columns else ''
                            })
                            logger.success(f"  + Added missing book: {ft}")
                            
        finally:
            await browser.close()
            
    if missing_books:
        logger.info(f"Total missing books added: {len(missing_books)}")
        missing_df = pd.DataFrame(missing_books)
        df_exhausted = pd.concat([df, missing_df], ignore_index=True)
        
        out_path = raw_csv_path.replace('.csv', '_exhausted.csv')
        df_exhausted.to_csv(out_path, index=False)
        return out_path
    else:
        logger.info("No missing books found across any series.")
        return raw_csv_path

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    args = parser.parse_args()
    asyncio.run(exhaust_series(args.input))
