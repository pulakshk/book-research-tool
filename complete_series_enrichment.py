#!/usr/bin/env python3
"""
COMPLETE SERIES ENRICHMENT
Full workflow: Series Hub → Inject Missing Books → Fetch Amazon Links → Extract Metadata
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
SAVE_INTERVAL = 5

async def get_amazon_link_from_goodreads(page, gr_link):
    """Get Amazon link from Goodreads book page."""
    try:
        if not await safe_goto(page, gr_link):
            return None
        await asyncio.sleep(1)
        
        # Click "Get a Copy" button
        get_copy_btn = await page.query_selector("button.Button--buy")
        if get_copy_btn:
            await get_copy_btn.click()
            await asyncio.sleep(0.5)
        
        # Find Amazon link
        amz_link_el = await page.query_selector("a[href*='amazon.com']")
        if amz_link_el:
            href = await amz_link_el.get_attribute("href")
            # Clean Amazon link
            if 'amazon.com' in href:
                clean_link = re.sub(r'/ref=.*$', '', href)
                return clean_link
        return None
    except Exception as e:
        logger.error(f"Error getting Amazon link: {e}")
        return None

async def get_publisher_from_physical(page):
    """Attempt to get publisher from physical edition (Paperback/Hardcover)."""
    selectors = [
        "li.swatchElement:has-text('Paperback') a",
        "li.swatchElement:has-text('Hardcover') a",
        "#tmm-grid-swatch-paperback a",
        "#tmm-grid-swatch-hardcover a"
    ]
    
    for sel in selectors:
        try:
            el = await page.query_selector(sel)
            if el:
                href = await el.get_attribute("href")
                if href and "javascript:void" not in href:
                    url = href if href.startswith("http") else f"https://www.amazon.com{href}"
                    if await safe_goto(page, url):
                        await asyncio.sleep(1.0)
                        
                        pub_selectors = [
                            "#rpi-attribute-book_details-publisher .rpi-attribute-value",
                            "#detailBullets_feature_div li:has-text('Publisher') span:last-child",
                            "li:has-text('Publisher') span:nth-child(2)"
                        ]
                        
                        for ps in pub_selectors:
                            pub_el = await page.query_selector(ps)
                            if pub_el:
                                text = (await pub_el.text_content()).strip()
                                text = re.sub(r'(?i)Publisher\s*[:\u200f\u200e\s]*', '', text).strip()
                                text = re.split(r'\s*\(', text)[0].strip()
                                if text and len(text) > 2 and 'amazon digital' not in text.lower():
                                    return text
                    break
        except:
            continue
    return None

async def extract_amazon_metadata(page):
    """Extract metadata from Amazon page."""
    try:
        # Description
        description = None
        desc_selectors = [
            "#bookDescription_feature_div span",
            "#iframeContent",
            ".book-description"
        ]
        for sel in desc_selectors:
            desc_el = await page.query_selector(sel)
            if desc_el:
                description = (await desc_el.inner_text()).strip()
                if description and len(description) > 50:
                    break
        
        # Amazon rating
        rating = None
        rating_el = await page.query_selector("span.a-icon-alt")
        if rating_el:
            rating_text = await rating_el.inner_text()
            m = re.search(r'([\d.]+)', rating_text)
            if m:
                rating = float(m.group(1))
        
        # Rating count
        rating_count = None
        count_el = await page.query_selector("span#acrCustomerReviewText")
        if count_el:
            count_text = await count_el.inner_text()
            m = re.search(r'([\d,]+)', count_text)
            if m:
                rating_count = int(m.group(1).replace(',', ''))
        
        # BSR
        bsr = None
        page_text = await page.inner_text("body")
        bsr_match = re.search(r'#([\d,]+)\s+in\s+Kindle Store', page_text)
        if bsr_match:
            bsr = int(bsr_match.group(1).replace(',', ''))
        
        return {
            'description': description,
            'rating': rating,
            'rating_count': rating_count,
            'bsr': bsr
        }
    except Exception as e:
        logger.error(f"Error extracting Amazon metadata: {e}")
        return {}

async def get_series_books_with_canonical_name(page, series_url):
    """Extract all books from Goodreads series hub AND get canonical series name."""
    try:
        # Use networkidle for better JS load
        await page.goto(series_url, wait_until="networkidle", timeout=30000)
        await asyncio.sleep(3)
        
        # Scroll to trigger lazy loading
        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        await asyncio.sleep(1)
        await page.evaluate("window.scrollTo(0, 0)")
        await asyncio.sleep(1)
        
        # Get CANONICAL series name from h1
        canonical_name = None
        h1 = await page.query_selector("h1")
        if h1:
            h1_text = await h1.inner_text()
            canonical_name = re.sub(r'\s+Series\s*$', '', h1_text.strip(), flags=re.IGNORECASE)
        
        # Total count
        total_books = 0
        header_sub = await page.query_selector(".responsiveSeriesHeader__subtitle")
        if header_sub:
            text = await header_sub.inner_text()
            m = re.search(r'(\d+)\s+primary', text, re.IGNORECASE)
            if m:
                total_books = int(m.group(1))
        
        # Series status
        status = "Unknown"
        if h1:
            h1_text = await h1.inner_text()
            if any(x in h1_text.lower() for x in ["finished", "complete", "concluded"]):
                status = "Completed"
            else:
                status = "Ongoing"
        
        books = []
        
        # Wait for books to load
        try:
            await page.wait_for_selector("div.elementList", timeout=10000)
        except:
            logger.warning(f"  Timeout waiting for book elements")
        
        items = await page.query_selector_all("div.elementList")
        logger.info(f"  Found {len(items)} book items on series page")
        
        for item in items:
            # Title link
            title_el = await item.query_selector("a.gr-h3")
            if not title_el:
                title_el = await item.query_selector("a.bookTitle")
            if not title_el:
                continue
                
            title = (await title_el.inner_text()).strip()
            href = await title_el.get_attribute("href")
            link = href if href.startswith('http') else f"https://www.goodreads.com{href}"
            
            # Author
            author_el = await item.query_selector("a.authorName")
            if not author_el:
                author_el = await item.query_selector("span[itemprop='author'] a")
            author = (await author_el.inner_text()).strip() if author_el else ""
            
            # Book number
            book_number = None
            header_el = await item.query_selector(".responsiveBook__header")
            if header_el:
                header_text = await header_el.inner_text()
                m = re.search(r'BOOK\s+(\d+\.?\d*)', header_text, re.IGNORECASE)
                if m:
                    book_number = float(m.group(1))
            
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
        
        logger.info(f"  ✓ Extracted {len(books)} books from series hub")
        logger.info(f"  ✓ Canonical series name: '{canonical_name}'")
        return canonical_name, books
        
    except Exception as e:
        logger.error(f"Error scraping series hub: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None, []

async def main():
    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Starting with {len(df)} books")
    
    # Get unique series
    unique_series = df[df['Series Name'].notna() & (df['Series Name'] != 'NO_SERIES')]['Series Name'].unique()
    logger.info(f"Found {len(unique_series)} unique series to process")
    
    total_injected = 0
    books_processed = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)  # Non-headless to avoid detection
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        page = await context.new_page()
        
        for i, series_name in enumerate(unique_series, 1):
            logger.info(f"\n[{i}/{len(unique_series)}] === Processing Series: {series_name} ===")
            
            # 1. Find seed Goodreads link
            series_rows = df[df['Series Name'] == series_name]
            seed_link = series_rows[series_rows['Goodreads Link'].notna()]['Goodreads Link'].iloc[0] if not series_rows[series_rows['Goodreads Link'].notna()].empty else None
            
            if not seed_link:
                logger.warning(f"  No Goodreads link found for series")
                continue
            
            # 2. Navigate to book page
            logger.info(f"  Step 1: Navigating to book page")
            if not await safe_goto(page, seed_link):
                continue
            await asyncio.sleep(1)
            
            # 3. Find series hub link
            logger.info(f"  Step 2: Finding series hub link")
            series_url = None
            series_link_el = await page.query_selector("a[aria-label*='in the'][aria-label*='series']")
            if series_link_el:
                href = await series_link_el.get_attribute("href")
                series_url = href if href.startswith('http') else f"https://www.goodreads.com{href}"
                logger.success(f"  ✓ Found series hub: {series_url}")
            
            if not series_url:
                logger.warning(f"  Could not find series hub link")
                continue
            
            # 4. Extract all books from series hub + get canonical name
            logger.info(f"  Step 3: Extracting all books from series hub")
            canonical_name, books = await get_series_books_with_canonical_name(page, series_url)
            
            if not canonical_name or not books:
                logger.warning(f"  No books extracted or no canonical name found")
                continue
            
            # 4a. Standardize series names for existing books
            if canonical_name != series_name:
                logger.info(f"  Step 3a: Standardizing series name: '{series_name}' → '{canonical_name}'")
                standardized = 0
                for b in books:
                    norm_title = re.sub(r'[^a-z0-9]', '', b['title'].lower())
                    norm_author = re.sub(r'[^a-z0-9]', '', b['author'].lower())
                    
                    mask = (df['Book Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_title)) & \
                           (df['Author Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_author))
                    
                    if not df[mask].empty:
                        old_series = df.loc[mask, 'Series Name'].iloc[0]
                        if pd.isna(old_series) or old_series != canonical_name:
                            df.loc[mask, 'Series Name'] = canonical_name
                            logger.success(f"    ✓ Standardized '{b['title']}': '{old_series}' → '{canonical_name}'")
                            standardized += 1
                
                if standardized > 0:
                    df.to_csv(OUTPUT_FILE, index=False)
                    logger.success(f"  ✓ Standardized {standardized} existing books")
                
                # Update series_name for injection logic
                series_name = canonical_name
            
            # 5. Inject missing books + enrich
            logger.info(f"  Step 4: Checking for missing books (using canonical name: '{series_name}')...")
            new_injections = 0
            
            for b in books:
                # Fuzzy match check
                norm_title = re.sub(r'[^a-z0-9]', '', b['title'].lower())
                norm_author = re.sub(r'[^a-z0-9]', '', b['author'].lower())
                
                mask = (df['Book Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_title)) & \
                       (df['Author Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_author))
                
                if df[mask].empty:
                    # Relevance check
                    if not is_sports_hockey_related(b['title'], {'series': series_name}):
                        logger.debug(f"  Skipping non-sports book: {b['title']}")
                        continue
                    
                    logger.success(f"  + NEW BOOK FOUND: {b['title']}")
                    
                    # Create new row
                    new_row = {col: np.nan for col in df.columns}
                    new_row.update({
                        'Series Name': series_name,  # Using canonical name
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
                    
                    # 6. Fetch Amazon link
                    logger.info(f"      → Fetching Amazon link...")
                    amz_link = await get_amazon_link_from_goodreads(page, b['link'])
                    if amz_link:
                        new_row['Amazon Link'] = amz_link
                        logger.success(f"      ✓ Amazon link: {amz_link[:50]}...")
                        
                        # 7. Extract Amazon metadata
                        logger.info(f"      → Extracting Amazon metadata...")
                        if await safe_goto(page, amz_link):
                            await asyncio.sleep(1)
                            
                            # Get publisher (physical-first)
                            publisher = await get_publisher_from_physical(page)
                            if publisher:
                                new_row['Publisher'] = publisher
                                new_row['Self Pub Flag'] = 'Independently published' in publisher
                                logger.success(f"      ✓ Publisher: {publisher}")
                            else:
                                new_row['Publisher'] = "Independently published"
                                new_row['Self Pub Flag'] = True
                            
                            # Get other metadata
                            metadata = await extract_amazon_metadata(page)
                            if metadata.get('description'):
                                new_row['Description'] = metadata['description']
                            if metadata.get('rating'):
                                new_row['Amazon Rating'] = metadata['rating']
                            if metadata.get('rating_count'):
                                new_row['Amazon # of Ratings'] = metadata['rating_count']
                            if metadata.get('bsr'):
                                new_row['Amazon BSR'] = metadata['bsr']
                            
                            logger.success(f"      ✓ Full metadata extracted")
                    
                    # Add to dataframe
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    new_injections += 1
                    total_injected += 1
                    
                    await asyncio.sleep(1)
            
            if new_injections > 0:
                logger.success(f"  ✓✓✓ Injected {new_injections} new books for {series_name}")
                df.to_csv(OUTPUT_FILE, index=False)
            
            books_processed += 1
            if books_processed % SAVE_INTERVAL == 0:
                df.to_csv(OUTPUT_FILE, index=False)
                logger.info(f"  [CHECKPOINT] Saved progress at {books_processed} series")
            
            await asyncio.sleep(2)
        
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"\n{'='*60}")
    logger.success(f"COMPLETE SERIES ENRICHMENT FINISHED")
    logger.success(f"Total new books injected: {total_injected}")
    logger.success(f"Final book count: {len(df)}")
    logger.success(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(main())
