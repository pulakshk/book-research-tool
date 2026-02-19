import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import os
import sys
from loguru import logger

# Import extractor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extractors.goodreads_patterns import extract_goodreads_comprehensive

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv" 
# Note: Using 'ultra' as user requested, though 'final' is likely the master. 
# I will check if 'ultra' exists, otherwise fallback to 'final'.
if not os.path.exists(INPUT_FILE):
    INPUT_FILE = "data/unified_book_data_enriched_final.csv"

# Targets to checking
TARGET_SERIES = [
    'Billionaire Rules', 
    'Seattle Sockeyes', 
    'Aces Hockey', 
    'Hot Ice'
]

async def safe_goto(page, url):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        return True
    except Exception as e:
        logger.error(f"Failed load {url}: {e}")
        return False

async def main():
    logger.info(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    # Filter for our targets
    # We want ONE sample book per series to validate the extraction
    samples = []
    seen_series = set()
    
    # Find rows that match our target series names
    for series in TARGET_SERIES:
        mask = df['Series Name'].astype(str).str.contains(series, case=False, na=False)
        rows = df[mask]
        
        if not rows.empty:
            # Pick the first one that has a goodreads link
            for _, row in rows.iterrows():
                url = str(row.get('Goodreads Link', ''))
                if 'goodreads.com' in url:
                    samples.append({
                        'Series Target': series,
                        'Book Name': row['Book Name'],
                        'URL': url
                    })
                    break
        else:
            logger.warning(f"No rows found for target series: {series}")

    if not samples:
        logger.error("No sample books found to validate!")
        return

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # Fix: Use User Agent to avoid block
        context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
        page = await context.new_page()
        
        print("\n" + "="*60)
        print("VALIDATION RUN RESULTS")
        print("="*60)
        
        for sample in samples:
            logger.info(f"Testing: {sample['Book Name']} ({sample['Series Target']})")
            if await safe_goto(page, sample['URL']):
                await asyncio.sleep(2) # Allow heavy hydration
                data = await extract_goodreads_comprehensive(page)
                
                print(f"\nTarget Series: {sample['Series Target']}")
                print(f"Book: {data['title']}")
                print(f"Extracted Series Name: {data.get('series_name')}")
                print(f"Extracted Series URL:  {data.get('series_url')}")
                print("-" * 30)
                
                if data.get('series_name'):
                     logger.success(f"Verified series extraction for {sample['Series Target']}")
                else:
                     logger.error(f"Failed to extract series for {sample['Series Target']}")
            else:
                logger.error(f"Failed to navigate to {sample['URL']}")
                
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
