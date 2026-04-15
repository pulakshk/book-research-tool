#!/usr/bin/env python3
"""
FIX MISSING SERIES BOOKS
Reads the enriched dataset. For any book that has a 'Goodreads Series URL',
visits that URL, scrapes all the books in the series, and identifies which
titles are missing from our dataset.

Outputs a new raw discovery CSV for those missing books so they can be run
through the enrichment pipeline.
"""

import asyncio
import os
import re
import sys
import pandas as pd
from loguru import logger

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from genre_enrichment import create_stealth_context
from genre_crawl import safe_goto
from playwright.async_api import async_playwright

async def fetch_series_books(page, series_url):
    """Scrape book titles from a Goodreads series page."""
    if not await safe_goto(page, series_url):
        return []
        
    await asyncio.sleep(2)
    
    titles = []
    # Goodreads series pages usually list books under item titles
    els = await page.query_selector_all('div.listWithDividers__item a[itemprop="url"] span[itemprop="name"]')
    if not els:
        els = await page.query_selector_all('a.bookTitle span[itemprop="name"]')
        
    for el in els:
        t = await el.text_content()
        if t:
            # Clean up the trailing "(Series #1)" parts that Goodreads appends
            clean_t = re.sub(r'\(.*?\)', '', t).strip()
            titles.append(clean_t)
            
    return titles

async def main():
    enriched_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_enriched.csv'
    if not os.path.exists(enriched_path):
        logger.error(f"Enriched path not found: {enriched_path}")
        return
        
    df = pd.read_csv(enriched_path)
    logger.info(f"Loaded {len(df)} books.")
    
    # Filter to books that actually have a Goodreads Series URL
    df_series = df.dropna(subset=['Goodreads Series URL'])
    df_series = df_series[df_series['Goodreads Series URL'].str.startswith('http')]
    
    # Group by the Series URL so we only visit each series page once
    series_groups = df_series.groupby('Goodreads Series URL')
    logger.info(f"Found {len(series_groups)} valid Goodreads Series URLs to scrape.")
    
    existing_titles = [str(x).lower().strip() for x in df['Book Name'].dropna()]
    missing_records = []
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await create_stealth_context(browser)
        page = await context.new_page()
        try:
            for series_url, group in series_groups:
                series_name = group['Series Name'].iloc[0] if 'Series Name' in group and pd.notna(group['Series Name'].iloc[0]) else "Unknown Series"
                author = group['Author Name'].iloc[0]
                subgenre = group['Subgenre'].iloc[0] if 'Subgenre' in group else 'Political Drama/Romance'
                
                logger.info(f"Scraping series page: {series_name} ({series_url})")
                
                found_books = await fetch_series_books(page, series_url)
                logger.info(f"  -> Found {len(found_books)} books on Goodreads for this series.")
                
                # Check for missing
                for fb in found_books:
                    fb_lower = fb.lower().strip()
                    is_missing = True
                    # Fuzzy match to existing titles
                    for et in existing_titles:
                        if fb_lower == et or fb_lower in et or et in fb_lower:
                            is_missing = False
                            break
                            
                    if is_missing:
                        missing_records.append({
                            'Book Name': fb,
                            'Author Name': author,
                            'Amazon Link': '',
                            'Series Name': series_name,
                            'Goodreads Series URL': series_url,
                            'Source': 'Goodreads Exhaustion',
                            'Source Detail': 'Series Expansion',
                            'Subgenre': subgenre
                        })
                        logger.success(f"     [NEW] Added missing book: {fb}")
        finally:
            await browser.close()
            
    if missing_records:
        out_df = pd.DataFrame(missing_records)
        out_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_series_exhaustion_discovery.csv'
        out_df.to_csv(out_path, index=False)
        logger.info(f"\n✅ Total missing series books found: {len(missing_records)}")
        logger.info(f"Saved to: {out_path}")
    else:
        logger.info("No missing books found across any series.")

if __name__ == '__main__':
    asyncio.run(main())
