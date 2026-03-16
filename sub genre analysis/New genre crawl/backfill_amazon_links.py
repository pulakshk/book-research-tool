#!/usr/bin/env python3
"""
BACKFILL AMAZON LINKS — Quick fix for books missing Amazon URLs.
Searches Amazon by title+author and extracts the product link.
"""

import asyncio
import os
import random
import re
import sys

import pandas as pd
from loguru import logger
from playwright.async_api import async_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from genre_crawl import create_stealth_context, safe_goto, USER_AGENTS

SLEEP_MIN = 2
SLEEP_MAX = 4
HEADLESS = True


async def backfill_amazon_links(csv_path, batch_size=50):
    """Backfill missing Amazon links by searching Amazon."""
    df = pd.read_csv(csv_path)
    
    # Find rows missing Amazon links
    missing = df[df['Amazon Link'].isna() | (df['Amazon Link'] == '') | (df['Amazon Link'].astype(str) == 'nan')]
    logger.info(f"Total rows: {len(df)}, Missing Amazon links: {len(missing)}")
    
    if len(missing) == 0:
        logger.info("No missing links — nothing to do")
        return
    
    filled = 0
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await create_stealth_context(browser)
        page = await context.new_page()
        
        try:
            for i, (idx, row) in enumerate(missing.iterrows()):
                if i >= batch_size:
                    break
                
                title = str(row['Book Name']).strip()
                author = str(row.get('Author Name', '')).strip()
                if author.lower() in ['kindle edition', 'nan', '']:
                    author = ''
                
                query = f"{title} {author}".strip().replace(' ', '+')
                search_url = f"https://www.amazon.com/s?k={query}&i=digital-text"
                
                try:
                    if not await safe_goto(page, search_url, timeout=30000):
                        continue
                    
                    await asyncio.sleep(random.uniform(1.0, 2.0))
                    
                    # Find first result
                    first_result = await page.query_selector("div[data-component-type='s-search-result']")
                    if not first_result:
                        first_result = await page.query_selector("div.s-result-item.s-asin")
                    
                    if first_result:
                        asin = await first_result.get_attribute("data-asin")
                        if asin:
                            amazon_link = f"https://www.amazon.com/dp/{asin}"
                            df.at[idx, 'Amazon Link'] = amazon_link
                            filled += 1
                            if filled % 10 == 0:
                                logger.info(f"  Progress: {filled} links filled ({i+1}/{min(len(missing), batch_size)})")
                    
                except Exception as e:
                    logger.debug(f"  Error for '{title[:40]}': {e}")
                
                await asyncio.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
                
                # Rotate context periodically
                if (i + 1) % 20 == 0:
                    await page.close()
                    await context.close()
                    context = await create_stealth_context(browser)
                    page = await context.new_page()
        
        finally:
            await page.close()
            await context.close()
            await browser.close()
    
    # Save
    df.to_csv(csv_path, index=False)
    logger.success(f"✅ Backfilled {filled} Amazon links. Saved to {csv_path}")
    
    # Report remaining missing
    still_missing = df['Amazon Link'].isna() | (df['Amazon Link'] == '') | (df['Amazon Link'].astype(str) == 'nan')
    logger.info(f"  Still missing: {still_missing.sum()}/{len(df)}")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Backfill missing Amazon links")
    parser.add_argument("--input", type=str, required=True, help="Path to CSV")
    parser.add_argument("--batch", type=int, default=200, help="Max books to process")
    args = parser.parse_args()
    
    asyncio.run(backfill_amazon_links(args.input, args.batch))
