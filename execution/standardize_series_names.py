#!/usr/bin/env python3
"""
SERIES NAME STANDARDIZATION
Corrects inconsistent series names by matching books to their canonical series hub.
This prevents duplicates when injecting new books.
"""

import asyncio
import pandas as pd
import re
from loguru import logger
from playwright.async_api import async_playwright
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "data/unified_book_data_enriched_ultra.csv"

async def get_canonical_series_info(page, series_url):
    """Get canonical series name and all book titles from series hub."""
    try:
        # Navigate with networkidle for better JavaScript load
        await page.goto(series_url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        
        # Scroll to trigger lazy loading
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(1)
        
        # Get canonical series name from h1
        h1 = await page.query_selector("h1")
        if not h1:
            return None, []
        series_name = (await h1.inner_text()).strip()
        # Clean up "Series" suffix
        series_name = re.sub(r'\s+Series\s*$', '', series_name, flags=re.IGNORECASE)
        
        # Get all books
        books = []
        
        # Wait for books to load
        try:
            await page.wait_for_selector("div.elementList", timeout=10000)
        except:
            logger.warning(f"  Timeout waiting for book elements")
        
        items = await page.query_selector_all("div.elementList")
        logger.debug(f"  Found {len(items)} elementList divs")
        
        for item in items:
            # Try multiple title selectors
            title_el = await item.query_selector("a.gr-h3")
            if not title_el:
                title_el = await item.query_selector("a.bookTitle")
            if not title_el:
                logger.debug(f"  Skipping item - no title element found")
                continue
            
            title = (await title_el.inner_text()).strip()
            
            # Try multiple author selectors  
            author_el = await item.query_selector("a.authorName")
            if not author_el:
                author_el = await item.query_selector("span[itemprop='author'] a")
            if not author_el:
                author_el = await item.query_selector("a.gr-hyperlink")
            
            author = (await author_el.inner_text()).strip() if author_el else ""
            
            if title:  # Only add if we got a title
                books.append({
                    'title': title,
                    'author': author
                })
        
        logger.info(f"  Canonical series name: '{series_name}' with {len(books)} books")
        return series_name, books
        
    except Exception as e:
        logger.error(f"Error getting canonical series info: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, []

async def main():
    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Starting with {len(df)} books")
    
    # Get unique series (excluding NO_SERIES and NaN)
    unique_series = df[df['Series Name'].notna() & (df['Series Name'] != 'NO_SERIES')]['Series Name'].unique()
    logger.info(f"Processing {len(unique_series)} unique series")
    
    corrections_made = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Non-headless to avoid detection
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        page = await context.new_page()
        
        for i, series_name in enumerate(unique_series, 1):
            logger.info(f"[{i}/{len(unique_series)}] Standardizing: {series_name}")
            
            # Find a seed Goodreads link
            series_rows = df[df['Series Name'] == series_name]
            seed_link = series_rows[series_rows['Goodreads Link'].notna()]['Goodreads Link'].iloc[0] if not series_rows[series_rows['Goodreads Link'].notna()].empty else None
            
            if not seed_link:
                logger.warning(f"  No Goodreads link found")
                continue
            
            # Navigate to book page
            if not await safe_goto(page, seed_link):
                continue
            await asyncio.sleep(1)
            
            # Find series hub link
            series_link_el = await page.query_selector("a[aria-label*='in the'][aria-label*='series']")
            if not series_link_el:
                logger.warning(f"  No series hub link found")
                continue
            
            href = await series_link_el.get_attribute("href")
            series_url = href if href.startswith('http') else f"https://www.goodreads.com{href}"
            
            # Get canonical info
            canonical_name, hub_books = await get_canonical_series_info(page, series_url)
            
            if not canonical_name or not hub_books:
                logger.warning(f"  Could not get canonical series info")
                continue
            
            # Check if we need to standardize
            if canonical_name == series_name:
                logger.info(f"  ✓ Series name already canonical")
                continue
            
            # For each book in the hub, find matches in our dataset and update series name
            for hb in hub_books:
                norm_title = re.sub(r'[^a-z0-9]', '', hb['title'].lower())
                norm_author = re.sub(r'[^a-z0-9]', '', hb['author'].lower())
                
                # Find matching book in dataset
                mask = (df['Book Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_title)) & \
                       (df['Author Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_author))
                
                matches = df[mask]
                if not matches.empty:
                    current_series = matches.iloc[0]['Series Name']
                    if pd.isna(current_series) or current_series != canonical_name:
                        # Update series name
                        df.loc[mask, 'Series Name'] = canonical_name
                        logger.success(f"  ✓ Updated '{hb['title']}': '{current_series}' → '{canonical_name}'")
                        corrections_made += 1
            
            await asyncio.sleep(1)
        
        await browser.close()
    
    # Save
    df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"\n{'='*60}")
    logger.success(f"SERIES NAME STANDARDIZATION COMPLETE")
    logger.success(f"Total corrections: {corrections_made}")
    logger.success(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(main())
