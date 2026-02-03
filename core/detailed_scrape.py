import asyncio
import pandas as pd
import os
import re
import sys
import random
import numpy as np

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from loguru import logger
from playwright.async_api import async_playwright
from utils.filter import is_sports_hockey_related

# Import new extraction patterns
from extractors.goodreads_patterns import (
    extract_json_ld_data,
    extract_genres,
    extract_detail_list_items,
    extract_page_count_goodreads,
    extract_description_goodreads,
    extract_publication_info,
    extract_rating_info_goodreads
)

# Config
# Try to find file in current working directory (orchestrator level) or script level
INPUT_FILE = "unified_book_data_enriched_ultra.csv"
if not os.path.exists(INPUT_FILE):
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    INPUT_FILE = os.path.join(BASE_DIR, "..", "unified_book_data_enriched_ultra.csv")
    
OUTPUT_FILE = INPUT_FILE

async def get_book_details(page, url):
    """
    Scrape detailed metadata from a Goodreads book page.
    ENHANCED: Uses JSON-LD (most reliable) + goodreads_patterns extractors for comprehensive extraction.
    """
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=45000)
        await asyncio.sleep(2)
        
        details = {}
        
        # ===== PRIORITY 1: JSON-LD (most reliable structured data) =====
        json_data = await extract_json_ld_data(page)
        
        if json_data:
            # Description from JSON-LD
            if json_data.get('description'):
                details['desc'] = json_data['description']
            
            # Page count from JSON-LD
            if json_data.get('numberOfPages'):
                details['pages'] = int(json_data['numberOfPages'])
            
            # Rating info from JSON-LD
            if json_data.get('aggregateRating'):
                agg = json_data['aggregateRating']
                details['gr_rating'] = agg.get('ratingValue')
                details['gr_count'] = agg.get('ratingCount')
        
        # ===== PRIORITY 2: Specific extractors for missing data =====
        
        # Description fallback
        if not details.get('desc'):
            details['desc'] = await extract_description_goodreads(page)
        
        # Pages fallback
        if not details.get('pages'):
            pages_str = await extract_page_count_goodreads(page)
            if pages_str:
                details['pages'] = int(pages_str)
        
        # ===== NEW: Extract genres for Primary Trope field =====
        genres = await extract_genres(page, max_genres=5)
        if genres:
            details['primary_trope'] = genres[0]  # First genre as primary trope
            details['genres'] = ', '.join(genres)  # All genres
        
        # ===== Publisher & Publication Info =====
        pub_info = await extract_publication_info(page)
        if pub_info.get('publication_date'):
            details['first_pub'] = pub_info['publication_date']
        if pub_info.get('full_pub_text'):
            details['pub_raw'] = pub_info['full_pub_text']
        
        # ===== Detail List Items (ISBN, ASIN, Publisher, Awards) =====
        detail_items = await extract_detail_list_items(page)
        if detail_items.get('publisher'):
            details['publisher'] = detail_items['publisher']
        elif pub_info.get('full_pub_text') and 'by ' in pub_info['full_pub_text']:
            # Fallback: Extract publisher from pub info text
            details['publisher'] = pub_info['full_pub_text'].split('by ')[-1].strip()
        
        # NEW: ISBN and ASIN
        if detail_items.get('isbn'):
            details['isbn'] = detail_items['isbn']
        if detail_items.get('asin'):
            details['asin'] = detail_items['asin']
        
        # NEW: Awards
        if detail_items.get('awards'):
            details['awards'] = detail_items['awards']
        
        logger.debug(f"Extracted: pages={details.get('pages')}, publisher={details.get('publisher')}, genres={details.get('primary_trope')}")
        
        return details
    except Exception as e:
        logger.error(f"Error scraping detail page {url}: {e}")
        return None

async def scrape_detail_worker(worker_id, browser, semaphore, queue, df, output_lock):
    """Worker to process book detail pages in parallel."""
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36")
    page = await context.new_page()
    try:
        count = 0
        while not queue.empty():
            idx = await queue.get()
            async with semaphore:
                row = df.iloc[idx]
                url = row['Goodreads Link']
                
                # RELEVANCE CHECK: Pre-flight check on title/series
                if not is_sports_hockey_related(row['Book Name'], {'series': row.get('Series Name', '')}):
                    # We might skip it here, BUT the description might contain the keywords.
                    # However, if the user wants only sports/hockey romance, the title usually has a hint.
                    # To be safe, we'll crawl it IF we don't have a description yet, 
                    # but if we do get a description and it's NOT related, we can flag it.
                    pass

                details = await get_book_details(page, url)
                if details:
                    # RELEVANCE CHECK: Definitive check with description/genres
                    if not is_sports_hockey_related(row['Book Name'], {
                        'series': row.get('Series Name', ''),
                        'desc': details.get('desc', ''),
                        'genres': details.get('genres', '')
                    }):
                        logger.warning(f"  [Worker {worker_id}] -> Non-sports book detected: {row['Book Name']}. Marking for removal.")
                        async with output_lock:
                            # Instead of deleting, we'll mark the subgenre as 'Non-Sports' 
                            # so we can filter them out later.
                            df.at[idx, 'Primary Subgenre'] = 'Non-Sports (Filtered)'
                        queue.task_done()
                        continue

                    async with output_lock:
                        if 'desc' in details and details['desc']:
                            if pd.isna(df.at[idx, 'Description']) or df.at[idx, 'Description'] == "":
                                df.at[idx, 'Description'] = details['desc']
                            if pd.isna(df.at[idx, 'Short Synopsis']) or df.at[idx, 'Short Synopsis'] == "":
                                df.at[idx, 'Short Synopsis'] = details['desc'][:500] + "..." if len(details['desc']) > 500 else details['desc']
                        
                        if 'pages' in details and (pd.isna(df.at[idx, 'Pages']) or df.at[idx, 'Pages'] == 0):
                            df.at[idx, 'Pages'] = details['pages']
                        
                        if 'publisher' in details and (pd.isna(df.at[idx, 'Publisher']) or df.at[idx, 'Publisher'] == ""):
                            df.at[idx, 'Publisher'] = details['publisher']
                        
                        if 'first_pub' in details:
                            if pd.isna(df.at[idx, 'First Published']) or df.at[idx, 'First Published'] == "":
                                df.at[idx, 'First Published'] = details['first_pub']
                            if pd.isna(df.at[idx, 'Publication Date']) or df.at[idx, 'Publication Date'] == "":
                                df.at[idx, 'Publication Date'] = details['first_pub']
                            if pd.isna(df.at[idx, 'Original Published']) or df.at[idx, 'Original Published'] == "":
                                df.at[idx, 'Original Published'] = details['first_pub']
                        
                        # NEW: Primary Trope from genres
                        if 'primary_trope' in details and (pd.isna(df.at[idx, 'Primary Trope']) or df.at[idx, 'Primary Trope'] == ""):
                            df.at[idx, 'Primary Trope'] = details['primary_trope']
                        
                        # NEW: Goodreads rating from detailed page (via JSON-LD)
                        if 'gr_rating' in details and (pd.isna(df.at[idx, 'Goodreads Rating']) or df.at[idx, 'Goodreads Rating'] == 0):
                            df.at[idx, 'Goodreads Rating'] = details['gr_rating']
                        if 'gr_count' in details and (pd.isna(df.at[idx, 'Goodreads # of Ratings']) or df.at[idx, 'Goodreads # of Ratings'] == 0):
                            # Clean commas from rating count
                            gr_count = str(details['gr_count']).replace(',', '')
                            df.at[idx, 'Goodreads # of Ratings'] = int(gr_count) if gr_count.isdigit() else details['gr_count']

                        # Self-Pub Logic
                        pub_name = details.get('publisher', '').lower()
                        author_name = str(row.get('Author Name', '')).lower()
                        if pub_name and author_name and (author_name in pub_name or "independent" in pub_name or "amazon" in pub_name):
                            df.at[idx, 'Self Pub flag'] = "TRUE"
                        elif pub_name:
                            df.at[idx, 'Self Pub flag'] = "FALSE"

                        logger.success(f"  [Worker {worker_id}] -> Reached deep meta for {row['Book Name']}")
                
                count += 1
                queue.task_done()
                
                if count % 5 == 0:
                    async with output_lock:
                        df.to_csv(OUTPUT_FILE, index=False)
                
                await asyncio.sleep(random.uniform(2, 5))
    finally:
        await context.close()

async def main_detailed_scrape():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"Input file {INPUT_FILE} not found!")
        return

    df = pd.read_csv(INPUT_FILE)
    
    # Target rows missing critical meta (Description, Publisher, Pages OR Goodreads Rating)
    mask = (df['Description'].isna() | (df['Description'] == "")) | \
           (df['Publisher'].isna() | (df['Publisher'] == "")) | \
           (df['Pages'].isna() | (df['Pages'] == 0)) | \
           (df['Goodreads Rating'].isna() | (df['Goodreads Rating'] == 0))
    
    mask = mask & df['Goodreads Link'].notna() & (df['Goodreads Link'] != "")
    
    rows_to_process = df[mask].index.tolist()
    logger.info(f"TURBO MODE: Targeting {len(rows_to_process)} rows for deep meta with 6 concurrent workers.")
    
    if not rows_to_process:
        logger.info("No rows need deep enrichment right now.")
        return

    queue = asyncio.Queue()
    for idx in rows_to_process:
        await queue.put(idx)
    
    semaphore = asyncio.Semaphore(6)
    output_lock = asyncio.Lock()

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        workers = []
        for i in range(6):
            workers.append(asyncio.create_task(scrape_detail_worker(i, browser, semaphore, queue, df, output_lock)))
            
        await asyncio.gather(*workers)
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    logger.success("Deep meta scrape complete.")

if __name__ == "__main__":
    asyncio.run(main_detailed_scrape())
