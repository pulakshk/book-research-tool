#!/usr/bin/env python3
"""
Full Metadata Extraction from Goodreads and Amazon.
Extracts: Description, Pages, Publisher, Ratings, Publication Date.
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

INPUT_FILE = "unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "unified_book_data_enriched_ultra.csv"

async def get_new_page(browser):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    return context, page

async def extract_metadata_from_gr(page, gr_link):
    """Extract comprehensive metadata from Goodreads book page."""
    result = {}
    try:
        if not await safe_goto(page, gr_link):
            return result
        
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(0.5)
        
        # Description
        desc_elem = await page.query_selector('div[data-testid="description"] span.Formatted')
        if not desc_elem:
            desc_elem = await page.query_selector('div.DetailsLayoutRightParagraph span.Formatted')
        if desc_elem:
            desc = await desc_elem.inner_text()
            if desc and len(desc) > 20:
                result['description'] = desc.strip()[:2000]
        
        # Rating
        rating_elem = await page.query_selector('div.RatingStatistics__rating')
        if rating_elem:
            rating_text = await rating_elem.inner_text()
            try:
                result['gr_rating'] = float(rating_text.strip())
            except:
                pass
        
        # Pages - look for "X pages" pattern
        details = await page.query_selector_all('p[data-testid="pagesFormat"], span.pagesFormat')
        for d in details:
            text = await d.inner_text()
            pages_match = re.search(r'(\d+)\s*pages?', text, re.I)
            if pages_match:
                result['pages'] = int(pages_match.group(1))
                break
        
        # If no pages found, try alternative location
        if 'pages' not in result:
            all_text = await page.inner_text('body')
            pages_match = re.search(r'(\d+)\s*pages', all_text, re.I)
            if pages_match and int(pages_match.group(1)) < 5000:
                result['pages'] = int(pages_match.group(1))
        
        # Publisher and Publication Date
        details_section = await page.query_selector('div[data-testid="publicationInfo"]')
        if details_section:
            details_text = await details_section.inner_text()
            # Extract publisher
            pub_match = re.search(r'by\s+([^,\n]+)', details_text)
            if pub_match:
                result['publisher'] = pub_match.group(1).strip()
        
        # Author (backup extraction)
        author_elem = await page.query_selector('a.ContributorLink span[data-testid="name"]')
        if author_elem:
            result['author'] = (await author_elem.inner_text()).strip()
        
        return result
        
    except Exception as e:
        logger.warning(f"    - GR extraction error: {e}")
        return result

async def extract_metadata_from_amazon(page, amz_link):
    """Extract metadata from Amazon product page."""
    result = {}
    try:
        if not await safe_goto(page, amz_link):
            return result
        
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(0.5)
        
        # Rating
        rating_elem = await page.query_selector('span.a-icon-alt')
        if rating_elem:
            rating_text = await rating_elem.inner_text()
            match = re.search(r'([\d.]+)\s*out of', rating_text)
            if match:
                result['amz_rating'] = float(match.group(1))
        
        # Pages from product details
        details = await page.query_selector_all('#detailBullets_feature_div li, #productDetailsTable tr')
        for d in details:
            text = await d.inner_text()
            if 'pages' in text.lower():
                pages_match = re.search(r'(\d+)\s*pages?', text, re.I)
                if pages_match:
                    result['pages'] = int(pages_match.group(1))
            if 'publisher' in text.lower():
                pub_match = re.search(r'Publisher\s*[:\s]+([^(;\n]+)', text, re.I)
                if pub_match:
                    result['publisher'] = pub_match.group(1).strip()
        
        # Description
        desc_elem = await page.query_selector('#bookDescription_feature_div span, #productDescription')
        if desc_elem:
            desc = await desc_elem.inner_text()
            if desc and len(desc) > 20:
                result['description'] = desc.strip()[:2000]
        
        return result
        
    except Exception as e:
        logger.warning(f"    - AMZ extraction error: {e}")
        return result

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
            title = str(row.get('Book Name', ''))[:40]
            gr_link = str(row.get('Goodreads Link', ''))
            amz_link = str(row.get('Amazon Link', ''))
            
            logger.info(f"[W{worker_id}] Extracting metadata for: {title}...")
            
            # Try Goodreads first
            gr_data = {}
            if gr_link and 'goodreads.com' in gr_link:
                gr_data = await extract_metadata_from_gr(page, gr_link)
            
            # Then Amazon for additional data
            amz_data = {}
            if amz_link and 'amazon.com' in amz_link:
                amz_data = await extract_metadata_from_amazon(page, amz_link)
            
            # Merge results (prefer GR for description, AMZ for rating)
            async with lock:
                updated = False
                
                # Description (prefer GR)
                if gr_data.get('description') and (pd.isna(df.at[idx, 'Description']) or str(df.at[idx, 'Description']) == 'nan'):
                    df.at[idx, 'Description'] = gr_data['description']
                    updated = True
                elif amz_data.get('description') and (pd.isna(df.at[idx, 'Description']) or str(df.at[idx, 'Description']) == 'nan'):
                    df.at[idx, 'Description'] = amz_data['description']
                    updated = True
                
                # Pages (prefer GR)
                if gr_data.get('pages') and (pd.isna(df.at[idx, 'Pages']) or df.at[idx, 'Pages'] == 0):
                    df.at[idx, 'Pages'] = gr_data['pages']
                    updated = True
                elif amz_data.get('pages') and (pd.isna(df.at[idx, 'Pages']) or df.at[idx, 'Pages'] == 0):
                    df.at[idx, 'Pages'] = amz_data['pages']
                    updated = True
                
                # Publisher
                if gr_data.get('publisher') and (pd.isna(df.at[idx, 'Publisher']) or str(df.at[idx, 'Publisher']) == 'nan'):
                    df.at[idx, 'Publisher'] = gr_data['publisher']
                    updated = True
                elif amz_data.get('publisher') and (pd.isna(df.at[idx, 'Publisher']) or str(df.at[idx, 'Publisher']) == 'nan'):
                    df.at[idx, 'Publisher'] = amz_data['publisher']
                    updated = True
                
                # Ratings
                if gr_data.get('gr_rating') and (pd.isna(df.at[idx, 'Goodreads Rating']) or df.at[idx, 'Goodreads Rating'] == 0):
                    df.at[idx, 'Goodreads Rating'] = gr_data['gr_rating']
                    updated = True
                
                if amz_data.get('amz_rating') and (pd.isna(df.at[idx, 'Amazon Rating']) or df.at[idx, 'Amazon Rating'] == 0):
                    df.at[idx, 'Amazon Rating'] = amz_data['amz_rating']
                    updated = True
                
                # Author backup
                if gr_data.get('author') and (pd.isna(df.at[idx, 'Author Name']) or str(df.at[idx, 'Author Name']) == 'nan'):
                    df.at[idx, 'Author Name'] = gr_data['author']
                    updated = True
                
                if updated:
                    logger.success(f"  [W{worker_id}] ✓ Updated metadata for: {title}")
                    df.at[idx, 'Status'] = 'METADATA_EXTRACTED'
            
            count += 1
            queue.task_done()
            
            if count % 20 == 0:
                async with lock:
                    df.to_csv(OUTPUT_FILE, index=False)
                    logger.info(f"  [Checkpoint] Saved {count} processed")
            
            await asyncio.sleep(random.uniform(0.5, 1.5))
    finally:
        await context.close()

async def run_metadata_extraction():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"{INPUT_FILE} not found!")
        return
    
    df = pd.read_csv(INPUT_FILE)
    
    # Target: Books with any missing metadata
    needs_processing = (
        (df['Description'].isna() | (df['Description'].astype(str) == 'nan')) |
        (df['Pages'].isna() | (df['Pages'] == 0)) |
        (df['Publisher'].isna() | (df['Publisher'].astype(str) == 'nan')) |
        (df['Goodreads Rating'].isna() | (df['Goodreads Rating'] == 0)) |
        (df['Amazon Rating'].isna() | (df['Amazon Rating'] == 0))
    )
    
    rows_to_process = df[needs_processing].index.tolist()
    
    if not rows_to_process:
        logger.success("No books need metadata extraction!")
        return
    
    logger.info(f"Starting metadata extraction for {len(rows_to_process)} books...")
    
    queue = asyncio.Queue()
    for idx in rows_to_process:
        await queue.put(idx)
    
    output_lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # 10 workers for speed
        workers = [extraction_worker(i, browser, queue, df, output_lock) for i in range(10)]
        await asyncio.gather(*workers)
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Final report
    total = len(df)
    print("\n=== FINAL METADATA REPORT ===")
    for col in ['Description', 'Pages', 'Publisher', 'Amazon Rating', 'Goodreads Rating']:
        if col in ['Description', 'Publisher']:
            missing = df[col].isna().sum() + (df[col].astype(str) == 'nan').sum()
        else:
            missing = df[col].isna().sum() + (df[col] == 0).sum()
        pct = missing / total * 100
        status = '✅' if pct < 20 else '⚠️' if pct < 50 else '❌'
        print(f"  {status} {col}: {missing} ({pct:.1f}%) missing")

if __name__ == "__main__":
    asyncio.run(run_metadata_extraction())
