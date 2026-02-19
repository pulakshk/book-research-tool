#!/usr/bin/env python3
"""
GEMINI-POWERED SERIES ENRICHMENT
Uses Gemini LLM to extract structured book data from Goodreads series pages.
More reliable than selector-based scraping for JavaScript-heavy pages.
"""

import asyncio
import pandas as pd
import numpy as np
import os
import re
import json
from loguru import logger
from playwright.async_api import async_playwright
import sys
import google.generativeai as genai

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto
from utils.filter import is_sports_hockey_related

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "data/unified_book_data_enriched_ultra.csv"

# Configure Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-2.5-flash')  # Latest fast model

EXTRACTION_PROMPT = """
You are a precise data extraction assistant. Extract ALL books from this Goodreads series page HTML.

Return a JSON object with this exact structure:
{
  "series_name": "canonical series name from h1 tag (remove 'Series' suffix if present)",
  "total_books": number of primary books in series,
  "series_status": "Completed" or "Ongoing" (check if h1 contains 'finished', 'complete', or 'concluded'),
  "books": [
    {
      "title": "book title",
      "author": "author name",
      "book_number": book number as float (e.g. 1.0, 1.5, 2.0) or null,
      "goodreads_link": "full goodreads URL to book page",
      "rating": goodreads rating as float,
      "rating_count": number of ratings as integer
    }
  ]
}

Extract ALL books listed. Be thorough and accurate. If a field is missing, use null.
"""

async def extract_series_data_with_gemini(html_content):
    """Use Gemini to extract structured book data from HTML."""
    try:
        # Truncate HTML if too long (Gemini has token limits)
        if len(html_content) > 100000:
            html_content = html_content[:100000]
        
        # Send to Gemini
        prompt = f"{EXTRACTION_PROMPT}\n\nHTML Content:\n{html_content}"
        
        response = model.generate_content(prompt)
        text = response.text.strip()
        
        # Extract JSON from response (might have markdown formatting)
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
        
        data = json.loads(text)
        
        logger.info(f"  ✓ Gemini extracted: {len(data.get('books', []))} books from '{data.get('series_name')}'")
        return data
        
    except Exception as e:
        logger.error(f"Error with Gemini extraction: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None

async def get_amazon_link_from_goodreads(page, gr_link):
    """Get Amazon link from Goodreads book page."""
    try:
        if not await safe_goto(page, gr_link):
            return None
        await asyncio.sleep(1)
        
        get_copy_btn = await page.query_selector("button.Button--buy")
        if get_copy_btn:
            await get_copy_btn.click()
            await asyncio.sleep(0.5)
        
        amz_link_el = await page.query_selector("a[href*='amazon.com']")
        if amz_link_el:
            href = await amz_link_el.get_attribute("href")
            if 'amazon.com' in href:
                clean_link = re.sub(r'/ref=.*$', '', href)
                return clean_link
        return None
    except Exception as e:
        logger.error(f"Error getting Amazon link: {e}")
        return None

async def get_publisher_from_physical(page):
    """Get publisher from physical edition."""
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
                        await asyncio.sleep(1)
                        
                        pub_selectors = [
                            "#rpi-attribute-book_details-publisher .rpi-attribute-value",
                            "#detailBullets_feature_div li:has-text('Publisher') span:last-child"
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
        description = None
        desc_selectors = ["#bookDescription_feature_div span", "#iframeContent"]
        for sel in desc_selectors:
            desc_el = await page.query_selector(sel)
            if desc_el:
                description = (await desc_el.inner_text()).strip()
                if description and len(description) > 50:
                    break
        
        rating = None
        rating_el = await page.query_selector("span.a-icon-alt")
        if rating_el:
            rating_text = await rating_el.inner_text()
            m = re.search(r'([\d.]+)', rating_text)
            if m:
                rating = float(m.group(1))
        
        rating_count = None
        count_el = await page.query_selector("span#acrCustomerReviewText")
        if count_el:
            count_text = await count_el.inner_text()
            m = re.search(r'([\d,]+)', count_text)
            if m:
                rating_count = int(m.group(1).replace(',', ''))
        
        bsr = None
        page_text = await page.inner_text("body")
        bsr_match = re.search(r'#([\d,]+)\s+in\s+Kindle Store', page_text)
        if bsr_match:
            bsr = int(bsr_match.group(1).replace(',', ''))
        
        return {
            'description': description,
            'rating rating': rating,
            'rating_count': rating_count,
            'bsr': bsr
        }
    except Exception as e:
        logger.error(f"Error extracting Amazon metadata: {e}")
        return {}

async def main():
    if not GEMINI_API_KEY:
        logger.error("GEMINI_API_KEY not set! Please export it first.")
        return
    
    df = pd.read_csv(INPUT_FILE)
    logger.info(f"Starting with {len(df)} books")
    
    unique_series = df[df['Series Name'].notna() & (df['Series Name'] != 'NO_SERIES')]['Series Name'].unique()
    logger.info(f"Found {len(unique_series)} unique series to process")
    
    total_injected = 0
    total_standardized = 0
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        page = await context.new_page()
        
        for i, series_name in enumerate(unique_series, 1):
            logger.info(f"\n[{i}/{len(unique_series)}] === Processing: {series_name} ===")
            
            # Find seed Goodreads link
            series_rows = df[df['Series Name'] == series_name]
            seed_link = series_rows[series_rows['Goodreads Link'].notna()]['Goodreads Link'].iloc[0] if not series_rows[series_rows['Goodreads Link'].notna()].empty else None
            
            if not seed_link:
                logger.warning("  No Goodreads link found")
                continue
            
            # Navigate to book page
            if not await safe_goto(page, seed_link):
                continue
            await asyncio.sleep(1)
            
            # Find series hub link
            series_link_el = await page.query_selector("a[aria-label*='in the'][aria-label*='series']")
            if not series_link_el:
                logger.warning("  No series hub link found")
                continue
            
            href = await series_link_el.get_attribute("href")
            series_url = href if href.startswith('http') else f"https://www.goodreads.com{href}"
            logger.success(f"  ✓ Series hub: {series_url}")
            
            # Get HTML content
            if not await safe_goto(page, series_url):
                continue
            await asyncio.sleep(2)
            
            html_content = await page.content()
            logger.info(f"  → Sending HTML to Gemini ({len(html_content)} chars)...")
            
            # Extract with Gemini
            series_data = await extract_series_data_with_gemini(html_content)
            
            if not series_data or not series_data.get('books'):
                logger.warning("  Gemini extraction failed or no books found")
                continue
            
            canonical_name = series_data.get('series_name', series_name)
            books = series_data.get('books', [])
            
            # Standardize existing books
            if canonical_name != series_name:
                logger.info(f"  → Standardizing: '{series_name}' → '{canonical_name}'")
                for b in books:
                    norm_title = re.sub(r'[^a-z0-9]', '', b['title'].lower())
                    norm_author = re.sub(r'[^a-z0-9]', '', b['author'].lower())
                    
                    mask = (df['Book Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_title)) & \
                           (df['Author Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_author))
                    
                    if not df[mask].empty:
                        old_series = df.loc[mask, 'Series Name'].iloc[0]
                        if pd.isna(old_series) or old_series != canonical_name:
                            df.loc[mask, 'Series Name'] = canonical_name
                            logger.success(f"    ✓ Standardized '{b['title']}'")
                            total_standardized += 1
                
                df.to_csv(OUTPUT_FILE, index=False)
                series_name = canonical_name
            
            # Inject missing books
            new_injections = 0
            for b in books:
                norm_title = re.sub(r'[^a-z0-9]', '', b['title'].lower())
                norm_author = re.sub(r'[^a-z0-9]', '', b['author'].lower())
                
                mask = (df['Book Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_title)) & \
                       (df['Author Name'].apply(lambda x: re.sub(r'[^a-z0-9]', '', str(x).lower()) == norm_author))
                
                if df[mask].empty:
                    if not is_sports_hockey_related(b['title'], {'series': series_name}):
                        logger.debug(f"  Skipping non-sports: {b['title']}")
                        continue
                    
                    logger.success(f"  + NEW BOOK: {b['title']}")
                    
                    new_row = {col: np.nan for col in df.columns}
                    new_row.update({
                        'Series Name': series_name,
                        'Author Name': b['author'],
                        'Book Name': b['title'],
                        'Goodreads Link': b.get('goodreads_link'),
                        'Goodreads Rating': b.get('rating'),
                        'Goodreads # of Ratings': b.get('rating_count'),
                        'Total Books in Series': series_data.get('total_books'),
                        'Series Status': series_data.get('series_status'),
                        'Primary Subgenre': 'Hockey Romance',
                        'Book Number': b.get('book_number')
                    })
                    
                    # Fetch Amazon data
                    if b.get('goodreads_link'):
                        logger.info(f"      → Fetching Amazon link...")
                        amz_link = await get_amazon_link_from_goodreads(page, b['goodreads_link'])
                        if amz_link:
                            new_row['Amazon Link'] = amz_link
                            logger.success(f"      ✓ Amazon link found")
                            
                            if await safe_goto(page, amz_link):
                                await asyncio.sleep(1)
                                
                                publisher = await get_publisher_from_physical(page)
                                if publisher:
                                    new_row['Publisher'] = publisher
                                    new_row['Self Pub Flag'] = 'Independently published' in publisher
                                    logger.success(f"      ✓ Publisher: {publisher}")
                                else:
                                    new_row['Publisher'] = "Independently published"
                                    new_row['Self Pub Flag'] = True
                                
                                metadata = await extract_amazon_metadata(page)
                                if metadata.get('description'):
                                    new_row['Description'] = metadata['description']
                                if metadata.get('rating'):
                                    new_row['Amazon Rating'] = metadata['rating']
                                if metadata.get('rating_count'):
                                    new_row['Amazon # of Ratings'] = metadata['rating_count']
                                if metadata.get('bsr'):
                                    new_row['Amazon BSR'] = metadata['bsr']
                    
                    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
                    new_injections += 1
                    total_injected += 1
                    
                    await asyncio.sleep(1)
            
            if new_injections > 0:
                logger.success(f"  ✓✓✓ Injected {new_injections} new books")
                df.to_csv(OUTPUT_FILE, index=False)
            
            await asyncio.sleep(2)
        
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"\n{'='*60}")
    logger.success(f"GEMINI-POWERED SERIES ENRICHMENT COMPLETE")
    logger.success(f"Total standardized: {total_standardized}")
    logger.success(f"Total new books injected: {total_injected}")
    logger.success(f"Final book count: {len(df)}")
    logger.success(f"{'='*60}")

if __name__ == "__main__":
    asyncio.run(main())
