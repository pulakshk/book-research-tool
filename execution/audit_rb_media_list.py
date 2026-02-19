#!/usr/bin/env python3
import asyncio
import pandas as pd
import os
import re
from loguru import logger
from playwright.async_api import async_playwright

# Import existing logic
import sys
EXECUTION_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(EXECUTION_DIR)
if EXECUTION_DIR not in sys.path:
    sys.path.insert(0, EXECUTION_DIR)

from series_exhaustion import get_series_data, get_new_context

# Config
INPUT_FILE = os.path.join(PROJECT_ROOT, "RB Media Shortlist - US D_R - RB Media Final shortlist.csv")
OUTPUT_FILE = os.path.join(PROJECT_ROOT, "RB Media Shortlist - AUDITED.csv")

def count_works(books):
    """
    Count Primary and Total works from the list of books scraped.
    Primary works usually have a clean integer number like "#1", "#2".
    Total works includes everything.
    """
    primary_count = 0
    total_count = len(books)
    
    for b in books:
        series_info = b.get('series_info', '')
        # Pattern for primary: #1, #2, etc. NOT #1.5, #0.5, #1-2
        # Matches # followed by digits, end of string or space
        if re.search(r'#\d+$|#\d+\s', series_info):
            # Check for decimals to be sure
            if '.' not in series_info:
                primary_count += 1
                
    return primary_count, total_count

async def main_audit():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file not found: {INPUT_FILE}")
        return

    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Loaded {len(df)} rows for auditing.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context, page = await get_new_context(browser)
        
        # Only process rows with a Series URL
        target_rows = df[df['Series URL'].notna()].index.tolist()
        
        logger.info(f"Targeting {len(target_rows)} series for auditing.")
        
        for i, idx in enumerate(target_rows):
            series_url = df.at[idx, 'Series URL']
            series_name = df.at[idx, 'Series Name']
            
            logger.info(f"[{i+1}/{len(target_rows)}] Auditing: {series_name} ({series_url})")
            
            try:
                # Reuse existing scraper logic
                books = await get_series_data(page, series_url)
                
                if not books:
                    logger.warning(f"  -> No books found for {series_name}")
                    continue
                
                # Extract Author Name from the first book if missing
                if pd.isna(df.at[idx, 'Author Name']) or df.at[idx, 'Author Name'] == "":
                    # Find the most frequent author in the series to be safe
                    authors = [b['author'] for b in books if b['author']]
                    if authors:
                        main_author = max(set(authors), key=authors.count)
                        df.at[idx, 'Author Name'] = main_author
                        logger.success(f"  -> Set Author: {main_author}")

                # Audit Works Count
                primary, total = count_works(books)
                
                # Check for "X primary works • Y total works" in header if get_series_data didn't find it
                # The existing get_series_data returns meta['total_books'] from header
                # but it might miss 'primary works'. 
                # Let's try to get it directly from the subtitle if possible.
                try:
                    subtitle = await page.inner_text("div.responsiveSeriesHeader__subtitle")
                    if subtitle:
                        p_match = re.search(r'(\d+)\s+primary\s+works', subtitle, re.IGNORECASE)
                        t_match = re.search(r'(\d+)\s+total\s+works', subtitle, re.IGNORECASE)
                        if p_match: primary = int(p_match.group(1))
                        if t_match: total = int(t_match.group(1))
                except:
                    pass

                # Update columns
                df.at[idx, 'Primary Works'] = primary
                df.at[idx, 'Total Works'] = total
                
                logger.success(f"  -> Audited: Primary={primary}, Total={total}")

                # Save intermediate progress
                if (i + 1) % 5 == 0:
                    df.to_csv(OUTPUT_FILE, index=False)
                    logger.info(f"Saved progress to {OUTPUT_FILE}")

            except Exception as e:
                logger.error(f"Error auditing {series_name}: {e}")
            
            # Anti-bot delay
            await asyncio.sleep(random.uniform(3, 6))

        await browser.close()

    df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"Audit complete! Final result saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(main_audit())
