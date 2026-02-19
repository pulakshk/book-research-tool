#!/usr/bin/env python3
"""
Fallback: Extract Series Names directly from Goodreads book pages.
For books that have Goodreads links but no Series Name populated.
"""
import asyncio
import pandas as pd
import os
import re
import random
import sys
from loguru import logger
from playwright.async_api import async_playwright

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "data/unified_book_data_enriched_ultra.csv"

async def get_new_page(browser):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    return context, page

async def extract_series_from_gr(page, gr_link):
    """
    Visit a Goodreads book page and extract series name.
    Series appears like: "(Arizona Vengeance #2)" in the title area.
    """
    try:
        if not await safe_goto(page, gr_link):
            return None
        
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(0.5)
        
        # Method 1: Look for series info in the BookPageTitleSection
        series_link = await page.query_selector('a[href*="/series/"]')
        if series_link:
            series_text = await series_link.inner_text()
            # Clean up: "Arizona Vengeance #2" -> "Arizona Vengeance"
            series_name = re.sub(r'#\d+.*$', '', series_text).strip()
            series_name = re.sub(r',?\s*$', '', series_name)  # Remove trailing comma
            if series_name:
                logger.success(f"    ✓ Found series: {series_name}")
                return {'series_name': series_name, 'series_link': await series_link.get_attribute('href')}
        
        # Method 2: Look in title for parenthetical series info
        title_elem = await page.query_selector('h1[data-testid="bookTitle"]')
        if title_elem:
            title_text = await title_elem.inner_text()
            # Look for pattern like "(Series Name, #1)" or "(Series Name #1)"
            match = re.search(r'\(([^)]+(?:#|,\s*#)\d+[^)]*)\)', title_text)
            if match:
                series_part = match.group(1)
                series_name = re.sub(r'[,#]\s*\d+.*$', '', series_part).strip()
                if series_name:
                    logger.success(f"    ✓ Found series in title: {series_name}")
                    return {'series_name': series_name}
        
        # Method 3: Check for series section on the page
        series_section = await page.query_selector('div[data-testid="seriesTitle"]')
        if series_section:
            series_text = await series_section.inner_text()
            series_name = re.sub(r'#\d+.*$', '', series_text).strip()
            if series_name:
                logger.success(f"    ✓ Found series section: {series_name}")
                return {'series_name': series_name}
        
        logger.debug(f"    - No series info found on page")
        return None
        
    except Exception as e:
        logger.warning(f"    - Error extracting series: {e}")
        return None

async def extraction_worker(worker_id, browser, queue, df, lock):
    context, page = await get_new_page(browser)
    count = 0
    
    try:
        while not queue.empty():
            try:
                idx = queue.get_nowait()
            except:
                break
            
            row = df.loc[idx]
            gr_link = str(row.get('Goodreads Link', ''))
            title = str(row.get('Book Name', ''))[:40]
            
            logger.info(f"[W{worker_id}] Extracting series for: {title}...")
            
            res = await extract_series_from_gr(page, gr_link)
            
            if res and res.get('series_name'):
                async with lock:
                    df.at[idx, 'Series Name'] = res['series_name']
                    df.at[idx, 'Status'] = 'SERIES_EXTRACTED'
            else:
                async with lock:
                    df.at[idx, 'Status'] = 'NO_SERIES'
            
            count += 1
            queue.task_done()
            
            if count % 20 == 0:
                async with lock:
                    df.to_csv(OUTPUT_FILE, index=False)
                    logger.info(f"  [Checkpoint] Saved {count} processed")
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
    finally:
        await context.close()

async def run_series_extraction():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"{INPUT_FILE} not found!")
        return
    
    df = pd.read_csv(INPUT_FILE)
    
    # Target: Has Goodreads link but no series name
    has_gr = df['Goodreads Link'].notna() & (df['Goodreads Link'].astype(str).str.contains('goodreads.com'))
    no_series = df['Series Name'].isna() | (df['Series Name'].astype(str) == '') | (df['Series Name'].astype(str) == 'nan')
    
    rows_to_process = df[has_gr & no_series].index.tolist()
    
    if not rows_to_process:
        logger.success("No books need series extraction!")
        return
    
    logger.info(f"Starting series extraction for {len(rows_to_process)} books...")
    
    queue = asyncio.Queue()
    for idx in rows_to_process:
        await queue.put(idx)
    
    output_lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # 8 workers for speed
        workers = [extraction_worker(i, browser, queue, df, output_lock) for i in range(8)]
        await asyncio.gather(*workers)
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    
    extracted = df[df['Status'] == 'SERIES_EXTRACTED'].shape[0]
    no_series = df[df['Status'] == 'NO_SERIES'].shape[0]
    logger.success(f"Series Extraction Complete: {extracted} extracted, {no_series} books are standalones")

if __name__ == "__main__":
    asyncio.run(run_series_extraction())
