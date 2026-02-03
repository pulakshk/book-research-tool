#!/usr/bin/env python3
"""
SERIES EXHAUSTION V2 - Multi-worker, Self-healing, and Robust.
Consolidates:
- Book-to-Series hub navigation
- Exhaustive metadata extraction (Ratings, Links, Series Status)
- Relevance filtering (Sports/Hockey focus)
- Duplicate prevention (Fuzzy Title+Author checking)
- Parallel execution with self-healing contexts
"""

import asyncio
import pandas as pd
import numpy as np
import os
import re
import random
from loguru import logger
from playwright.async_api import async_playwright
import sys

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto
from utils.filter import is_sports_hockey_related

# CONFIG
INPUT_FILE = "base_for_parallel.csv"
OUTPUT_FILE = "series_exhausted.csv"
WORKER_COUNT = 4  # Lower for stability on series pages
SAVE_INTERVAL = 5

async def get_new_page(browser):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    return context, page

async def get_series_data_from_hub(page, series_url):
    """Scrape all books from a Goodreads series hub."""
    try:
        if not await safe_goto(page, series_url): return []
        await asyncio.sleep(2)
        
        meta = {'total_books': 0, 'status': 'Unknown'}
        # Updated selectors from inspection
        header_sub = await page.query_selector(".responsiveSeriesHeader__subtitle")
        if header_sub:
            header_text = await header_sub.inner_text()
            count_match = re.search(r'(\d+)\s+primary', header_text, re.IGNORECASE)
            if count_match: meta['total_books'] = int(count_match.group(1))
            
        header_title = await page.query_selector("h1")
        if header_title:
            ht_text = await header_title.inner_text()
            if any(x in ht_text.lower() for x in ["finished", "complete", "concluded"]):
                meta['status'] = "Completed"
            else: meta['status'] = "Ongoing"

        books = []
        items = await page.query_selector_all("div.elementList, div.listWithDivider__item")
        
        for item in items:
            title_el = await item.query_selector("a.bookTitle, a.gr-h3, .responsiveBook__title a")
            if not title_el: continue
            title = (await title_el.inner_text()).strip()
            
            href = await title_el.get_attribute("href")
            link = href if href.startswith('http') else "https://www.goodreads.com" + href

            author_el = await item.query_selector("a.authorName, span[itemprop='author'] a")
            author = (await author_el.inner_text()).strip() if author_el else ""
            
            # Series Info (Book Number)
            series_info = ""
            s_el = await item.query_selector(".responsiveBook__header, h3, .bookSeries")
            if s_el: series_info = (await s_el.inner_text()).strip()
            
            stats_text = await item.inner_text()
            rating, rating_count = 0.0, 0
            # Support both dot and middle dot
            r_match = re.search(r'([\d.]+)\s*[\u00b7\u2022\.]\s*([\d,]+)\s*Ratings?', stats_text)
            if r_match:
                rating = float(r_match.group(1))
                rating_count = int(r_match.group(2).replace(',', ''))
            
            books.append({
                'title': title, 'link': link, 'author': author,
                'rating': rating, 'rating_count': rating_count,
                'series_info': series_info, 'total_books': meta['total_books'],
                'status': meta['status']
            })
        return books
    except Exception as e:
        logger.error(f"Error scraping {series_url}: {e}")
        return []

async def exhaustion_worker(worker_id, browser, queue, df_ref, lock):
    context, page = await get_new_page(browser)
    count = 0
    try:
        while not queue.empty():
            # Self-healing check
            try:
                if page.is_closed():
                    context, page = await get_new_page(browser)
            except:
                context, page = await get_new_page(browser)

            try: series_name = queue.get_nowait()
            except: break

            logger.info(f"[W{worker_id}] Exhausting Series: {series_name}")
            
            # 1. FIND SEED LINK
            async with lock:
                series_rows = df_ref['df'][df_ref['df']['Series Name'] == series_name]
                existing_glink = series_rows[series_rows['Goodreads Link'].notna()]['Goodreads Link'].iloc[0] if not series_rows[series_rows['Goodreads Link'].notna()].empty else None

            series_url = None
            if existing_glink:
                if await safe_goto(page, existing_glink):
                    # Find series hub link - PRIORITIZE aria-label or header links
                    series_selectors = [
                        "a[aria-label*='in the'][aria-label*='series']",
                        ".BookPageTitleSection__series a",
                        ".BookPage__series a",
                        "a[href*='/series/']",
                        "[data-testid='series'] a"
                    ]
                    for sel in series_selectors:
                        series_link_el = await page.query_selector(sel)
                        if series_link_el:
                            href = await series_link_el.get_attribute("href")
                            series_url = href if href.startswith('http') else "https://www.goodreads.com" + href
                            logger.info(f"  [W{worker_id}] Found series link via: {sel}")
                            break

            if not series_url:
                # Fallback to direct series search
                logger.info(f"  [W{worker_id}] Seeking series hub via search: {series_name}")
                clean_name = re.sub(r'\s*\(#\d+\)', '', str(series_name)).strip()
                search_url = f"https://www.goodreads.com/search?q={clean_name.replace(' ', '+')}&search_type=series"
                if await safe_goto(page, search_url):
                    s_link_el = await page.query_selector("a[href*='/series/']")
                    if s_link_el:
                        href = await s_link_el.get_attribute("href")
                        series_url = href if href.startswith('http') else "https://www.goodreads.com" + href

            if series_url:
                books = await get_series_data_from_hub(page, series_url)
                if books:
                    new_injections = 0
                    async with lock:
                        for b in books:
                            norm_title = re.sub(r'[^a-z0-9]', '', b['title'].lower())
                            norm_author = re.sub(r'[^a-z0-9]', '', b['author'].lower())
                            
                            # Duplicate Check
                            mask = (df_ref['df']['Book Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_title)) & \
                                   (df_ref['df']['Author Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_author))
                            
                            if df_ref['df'][mask].empty:
                                if is_sports_hockey_related(b['title'], {'series': series_name}):
                                    new_row = {col: np.nan for col in df_ref['df'].columns}
                                    new_row.update({
                                        'Series Name': series_name,
                                        'Author Name': b['author'],
                                        'Book Name': b['title'],
                                        'Goodreads Link': b['link'],
                                        'Goodreads Rating': b['rating'],
                                        'Goodreads # of Ratings': b['rating_count'],
                                        'Total Books in Series': b['total_books'],
                                        'Series Status': b['status'],
                                        'Primary Subgenre': 'Hockey Romance'
                                    })
                                    df_ref['df'] = pd.concat([df_ref['df'], pd.DataFrame([new_row])], ignore_index=True)
                                    new_injections += 1
                    
                    if new_injections > 0:
                        logger.success(f"  [W{worker_id}] Injected {new_injections} books for {series_name}")
                else:
                    logger.warning(f"  [W{worker_id}] No data found for series hub: {series_url}")
            else:
                logger.warning(f"  [W{worker_id}] Failed to find series hub for: {series_name}")

            count += 1
            if count % SAVE_INTERVAL == 0:
                async with lock: df_ref['df'].to_csv(OUTPUT_FILE, index=False)
            await asyncio.sleep(random.uniform(2, 4))
    finally:
        await context.close()

async def main():
    if not os.path.exists(INPUT_FILE): return
    df = pd.read_csv(INPUT_FILE)
    
    unique_series = df[df['Series Name'].notna() & (df['Series Name'] != 'NO_SERIES')]['Series Name'].unique().tolist()
    logger.info(f"Targeting {len(unique_series)} series for exhaustive exhaustion.")
    
    queue = asyncio.Queue()
    for s in unique_series: await queue.put(s)
    
    # Use a dictionary to wrap df for shared reference across workers
    df_ref = {'df': df}
    lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        workers = [exhaustion_worker(i, browser, queue, df_ref, lock) for i in range(WORKER_COUNT)]
        await asyncio.gather(*workers)
        await browser.close()
    
    df_ref['df'].to_csv(OUTPUT_FILE, index=False)
    logger.success(f"FINAL EXHAUSTION COMPLETE. Final Book Count: {len(df_ref['df'])}")

if __name__ == "__main__":
    asyncio.run(main())
