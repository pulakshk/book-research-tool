#!/usr/bin/env python3
"""
SERIES EXHAUSTION - SIMPLE & ROBUST
Single-worker approach with maximum stability for series hub scraping.
"""

import asyncio
import pandas as pd
import numpy as np
import os
import re
from loguru import logger
from playwright.async_api import async_playwright
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto
from utils.filter import is_sports_hockey_related

INPUT_FILE = "unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "unified_book_data_enriched_ultra.csv"

async def get_series_books(page, series_url):
    """Extract all books from a Goodreads series page."""
    try:
        if not await safe_goto(page, series_url):
            return []
        await asyncio.sleep(2)
        
        # Get total count
        total_books = 0
        header_sub = await page.query_selector(".responsiveSeriesHeader__subtitle")
        if header_sub:
            text = await header_sub.inner_text()
            m = re.search(r'(\d+)\s+primary', text, re.IGNORECASE)
            if m: total_books = int(m.group(1))
        
        # Get series status
        status = "Unknown"
        h1 = await page.query_selector("h1")
        if h1:
            h1_text = await h1.inner_text()
            if any(x in h1_text.lower() for x in ["finished", "complete", "concluded"]):
                status = "Completed"
            else:
                status = "Ongoing"
        
        books = []
        items = await page.query_selector_all("div.elementList")
        
        for item in items:
            # Title
            title_el = await item.query_selector("a.bookTitle, a.gr-h3")
            if not title_el: continue
            title = (await title_el.inner_text()).strip()
            
            href = await title_el.get_attribute("href")
            link = href if href.startswith('http') else f"https://www.goodreads.com{href}"
            
            # Author
            author_el = await item.query_selector("a.authorName")
            author = (await author_el.inner_text()).strip() if author_el else ""
            
            # Book number
            book_number = None
            header_el = await item.query_selector(".responsiveBook__header")
            if header_el:
                header_text = await header_el.inner_text()
                m = re.search(r'BOOK\s+(\d+\.?\d*)', header_text, re.IGNORECASE)
                if m: book_number = float(m.group(1))
            
            # Ratings
            stats = await item.inner_text()
            rating, rating_count = 0.0, 0
            r_match = re.search(r'([\d.]+)\s*[\u00b7\u2022]\s*([\d,]+)\s*rating', stats, re.IGNORECASE)
            if r_match:
                rating = float(r_match.group(1))
                rating_count = int(r_match.group(2).replace(',', ''))
            
            books.append({
                'title': title,
                'link': link,
                'author': author,
                'book_number': book_number,
                'rating': rating,
                'rating_count': rating_count,
                'total_books': total_books,
                'status': status
            })
        
        logger.info(f"  Extracted {len(books)} books from series hub")
        return books
        
    except Exception as e:
        logger.error(f"Error scraping {series_url}: {e}")
        return []

async def main():
    df = pd.read_csv(INPUT_FILE)
    
    # Get all unique series (that are sports-related)
    unique_series = df[df['Series Name'].notna() & (df['Series Name'] != 'NO_SERIES')]['Series Name'].unique()
    logger.info(f"Found {len(unique_series)} unique series to exhaust")
    
    total_injected = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        page = await context.new_page()
        
        for i, series_name in enumerate(unique_series, 1):
            logger.info(f"[{i}/{len(unique_series)}] Processing: {series_name}")
            
            # Find a seed book link
            series_rows = df[df['Series Name'] == series_name]
            seed_link = series_rows[series_rows['Goodreads Link'].notna()]['Goodreads Link'].iloc[0] if not series_rows[series_rows['Goodreads Link'].notna()].empty else None
            
            if not seed_link:
                logger.warning(f"  No Goodreads link found for series: {series_name}")
                continue
            
            # Navigate to book page
            if not await safe_goto(page, seed_link):
                continue
            
            await asyncio.sleep(1)
            
            # Find series hub link
            series_url = None
            series_link_el = await page.query_selector("a[aria-label*='in the'][aria-label*='series']")
            if series_link_el:
                href = await series_link_el.get_attribute("href")
                series_url = href if href.startswith('http') else f"https://www.goodreads.com{href}"
            
            if not series_url:
                logger.warning(f"  Could not find series hub link for: {series_name}")
                continue
            
            # Scrape series hub
            books = await get_series_books(page, series_url)
            
            if not books:
                logger.warning(f"  No books extracted from series hub")
                continue
            
            # Inject missing books
            new_injections = 0
            for b in books:
                # Normalize for fuzzy matching
                norm_title = re.sub(r'[^a-z0-9]', '', b['title'].lower())
                norm_author = re.sub(r'[^a-z0-9]', '', b['author'].lower())
                
                # Check if exists
                mask = (df['Book Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_title)) & \
                       (df['Author Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_author))
                
                if df[mask].empty:
                    # Relevance check
                    if not is_sports_hockey_related(b['title'], {'series': series_name}):
                        logger.debug(f"  Skipping non-sports book: {b['title']}")
                        continue
                    
                    logger.success(f"  + Injecting: {b['title']}")
                    
                    new_row = {col: np.nan for col in df.columns}
                    new_row.update({
                        'Series Name': series_name,
                        'Author Name': b['author'],
                        'Book Name': b['title'],
                        'Goodreads Link': b['link'],
                        'Goodreads Rating': b['rating'],
                        'Goodreads # of Ratings': b['rating_count'],
                        'Total Books in Series': b['total_books'],
                        'Series Status': b['status'],
                        'Primary Subgenre': 'Hockey Romance',
                        'Book Number': b['book_number']
                    })
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    new_injections += 1
                    total_injected += 1
            
            if new_injections > 0:
                logger.success(f"  ✓ Injected {new_injections} books for {series_name}")
                # Save after each series
                df.to_csv(OUTPUT_FILE, index=False)
            
            await asyncio.sleep(2)
        
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"SERIES EXHAUSTION COMPLETE. Total injected: {total_injected}. Final count: {len(df)}")

if __name__ == "__main__":
    asyncio.run(main())
