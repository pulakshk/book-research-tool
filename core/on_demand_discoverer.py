
import asyncio
import pandas as pd
import os
import re
import sys
import random
from loguru import logger
from playwright.async_api import async_playwright

# Import existing logic safely
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)

from ultra_recovery import search_amazon_for_meta, search_goodreads_for_meta, get_new_page
from enrich_by_series import get_series_data
from core.detailed_scrape import get_book_details

# Config
OUTPUT_FILE = os.path.join(BASE_DIR, "data", "on_demand_discovery.csv")
os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)

async def discover_series(book_title):
    logger.info(f"--- Starting On-Demand Discovery for: {book_title} ---")
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context, page = await get_new_page(browser)
        
        # Step 1: Find Seed Book Meta (Author, Series Link)
        logger.info("Step 1: Finding Seed Meta...")
        data = await search_amazon_for_meta(page, book_title)
        
        if not data or not data.get('author'):
            logger.info("  -> Amazon failed, trying Goodreads search...")
            data = await search_goodreads_for_meta(page, book_title)
        
        if not data:
            logger.error(f"Could not find any data for: {book_title}")
            await browser.close()
            return
        
        author = data.get('author')
        logger.success(f"  -> Found Author: {author}")
        
        # Step 2: Navigate to Book Page to find Series Link
        # If we already have a gr_link from Step 1, use it. Otherwise, search.
        seed_url = data.get('gr_link')
        if not seed_url:
            # Search Goodreads again for the specific book to get the detail page
            query = f"{book_title} {author}".strip()
            search_url = f"https://www.goodreads.com/search?q={query.replace(' ', '+')}"
            await page.goto(search_url, wait_until="domcontentloaded")
            first_book_el = await page.query_selector("a.bookTitle")
            if first_book_el:
                href = await first_book_el.get_attribute("href")
                seed_url = "https://www.goodreads.com" + href
        
        if not seed_url:
            logger.error("Could not find Goodreads book page.")
            await browser.close()
            return

        # Step 3: Extract Series Link from Detail Page
        logger.info(f"Step 2: Extracting Series Link from {seed_url}...")
        await page.goto(seed_url, wait_until="domcontentloaded")
        series_link_selector = "a[aria-label*='series'], .BookPageTitleSection__series a, [data-testid='series'] a, a[href*='/series/']"
        series_link_el = await page.query_selector(series_link_selector)
        
        if not series_link_el:
            logger.warning("No series link found. This might be a standalone book.")
            # Maybe we just scrape this one book?
            book_meta = await get_book_details(page, seed_url)
            final_data = [{
                'Book Name': book_title,
                'Author Name': author,
                'Goodreads Link': seed_url,
                **book_meta
            }]
        else:
            href = await series_link_el.get_attribute("href")
            series_url = href if href.startswith('http') else "https://www.goodreads.com" + href
            series_name_txt = await series_link_el.inner_text()
            logger.success(f"  -> Found Series: {series_name_txt} ({series_url})")
            
            # Step 4: Scrape all books in series
            logger.info("Step 3: Scraping all books in series...")
            books = await get_series_data(page, series_url)
            logger.success(f"  -> Found {len(books)} books in series.")
            
            # Step 5: Deep Meta for each book
            final_data = []
            for b in books:
                logger.info(f"  -> Deep Scraping: {b['title']}")
                meta = await get_book_details(page, b['link'])
                
                # Combine info
                row = {
                    'Book Name': b['title'],
                    'Series Name': series_name_txt,
                    'Author Name': author,
                    'Goodreads Link': b['link'],
                    'Goodreads Rating': b['rating'],
                    'Goodreads # of Ratings': b['rating_count'],
                    'Book Number': b['series_info'],
                    'Total Books in Series': b.get('total_books'),
                    'Series Status': b.get('status'),
                    'Primary Subgenre': 'Hockey Romance', # Default project focus
                    **meta
                }
                
                # Redundant field mapping
                if 'desc' in meta:
                    row['Short Synopsis'] = meta['desc'][:500] + "..."
                if 'first_pub' in meta:
                    row['Publication Date'] = meta['first_pub']
                    row['Original Published'] = meta['first_pub']

                # Self-Pub Check
                pub_name = meta.get('publisher', '').lower()
                if author and pub_name and (author.lower() in pub_name or "independent" in pub_name):
                    row['Self Pub flag'] = "TRUE"
                else:
                    row['Self Pub flag'] = "FALSE"
                
                final_data.append(row)
                await asyncio.sleep(random.uniform(2, 4))
        
        # Step 6: Output
        df = pd.DataFrame(final_data)
        # 24-Column Master Schema
        schema_cols = [
            'Series Name', 'Author Name', 'Book Name', 'Book Number', 'Total Books in Series', 
            'Goodreads Link', 'Goodreads # of Ratings', 'Goodreads Rating', 'First Published', 
            'Original Published', 'Pages', 'Description', 'Primary Trope', 'Primary Subgenre', 
            'Series Status', 'Amazon Link', 'Amazon # of Ratings', 'Amazon Rating', 
            'Publisher', 'Self Pub flag', 'Short Synopsis', 'Publication Date', 
            'Top Lists', 'Featured List'
        ]
        # Reindex to match schema
        df = df.reindex(columns=schema_cols)
        
        if os.path.exists(OUTPUT_FILE):
             df.to_csv(OUTPUT_FILE, mode='a', header=False, index=False)
        else:
             df.to_csv(OUTPUT_FILE, index=False)
             
        logger.success(f"Enrichment complete! Saved to {OUTPUT_FILE}")
        await browser.close()
        return df

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 core/on_demand_discoverer.py \"Book Title\"")
    else:
        title = sys.argv[1]
        asyncio.run(discover_series(title))
