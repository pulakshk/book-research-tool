#!/usr/bin/env python3
"""
FIX MISSING/INVALID AUTHORS
Extracts author from Amazon and updates the dataset.
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
from genre_enrichment import create_stealth_context

async def fix_authors():
    csv_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_enriched.csv'
    df = pd.read_csv(csv_path)
    
    # Identify bad authors
    bad_authors = ['|', 'kindle edition', 'nan', '']
    mask = df['Author Name'].isna() | df['Author Name'].astype(str).str.strip().str.lower().isin(bad_authors)
    missing_indices = df[mask].index.tolist()
    
    logger.info(f"Books with missing/invalid authors: {len(missing_indices)}")
    if len(missing_indices) == 0:
        return
        
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await create_stealth_context(browser)
        page = await context.new_page()
        
        for idx in missing_indices:
            row = df.loc[idx]
            amazon_link = str(row['Amazon Link'])
            title = str(row['Book Name'])
            
            if not amazon_link.startswith('http'):
                continue
                
            logger.info(f"Fixing author for: {title}")
            try:
                await page.goto(amazon_link, wait_until="domcontentloaded", timeout=45000)
                await asyncio.sleep(2)
                
                # Check different author selectors
                author = None
                selectors = [
                    "span.author.notFaded a.a-link-normal",
                    "div#bylineInfo span.author a.a-link-normal",
                    "a.contributorNameID",
                ]
                for sel in selectors:
                    el = await page.query_selector(sel)
                    if el:
                        author = await el.text_content()
                        if author:
                            author = author.strip()
                            # remove generic stuff
                            if '(' in author: author = author.split('(')[0].strip()
                            if author.lower() != 'kindle edition':
                                break
                            
                if author and author.lower() != 'kindle edition':
                    logger.success(f"  -> Found author: {author}")
                    df.at[idx, 'Author Name'] = author
                else:
                    logger.warning(f"  -> Author not found on page.")
                    
            except Exception as e:
                logger.error(f"  -> Error: {e}")
                
        await browser.close()
        
    df.to_csv(csv_path, index=False)
    logger.info(f"Saved updated authors to {csv_path}")

if __name__ == '__main__':
    asyncio.run(fix_authors())
