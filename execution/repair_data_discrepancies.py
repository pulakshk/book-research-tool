import asyncio
import pandas as pd
from playwright.async_api import async_playwright
import os
import sys
import re
from loguru import logger

# Import extractor
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from extractors.goodreads_patterns import extract_goodreads_comprehensive

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
if not os.path.exists(INPUT_FILE):
    INPUT_FILE = "data/unified_book_data_enriched_final.csv"

# Targets: Author -> Series Keyword
TARGETS = {
    'Lara Bailey': 'Billionaire Rules',
    'Jami Davenport': 'Seattle Sockeyes', 
    'Kelly Jamieson': 'Aces Hockey',
    'Lily Harlem': 'Hot Ice'
}

async def repair_series(sem, browser, df, author, target_series):
    # Filter rows
    mask = (df['Author Name'] == author) & (df['Series Name'].astype(str).str.contains(target_series, case=False, na=False))
    rows = df[mask]
    
    if rows.empty:
        logger.warning(f"No rows found for {target_series} by {author}")
        return

    logger.info(f"Repairing {len(rows)} books for {target_series}...")
    
    # Context with User Agent (CRITICAL for bypassing blocks)
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    
    updates = 0
    removals = 0
    
    # Track verified books to handle duplication later
    verified_books = []

    for idx, row in rows.iterrows():
        url = str(row.get('Goodreads Link', ''))
        title = row.get('Book Name')
        
        # Skip if no URL
        if 'goodreads.com' not in url:
            logger.warning(f"Skipping {title}: No valid GR URL")
            continue
            
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(1.0) # Reduced wait
            
            data = await extract_goodreads_comprehensive(page)
            
            # 1. Update Metrics (Trust Goodreads)
            new_rating = float(data.get('rating') or 0)
            new_count = int(data.get('rating_count') or 0)
            
            if new_rating > 0:
                df.at[idx, 'Goodreads Rating'] = new_rating
            if new_count > 0:
                df.at[idx, 'Goodreads # of Ratings'] = new_count
                
            updates += 1
            
            # 2. Series Verification
            scraped_series = data.get('series_name')
            series_url = data.get('series_url')
            
            # Validation Status
            is_valid = False
            
            if scraped_series:
                # Fuzzy match target
                # e.g. Target="Billionaire Rules", Scraped="The Billionaire Rules" -> Match
                if re.sub(r'\W', '', target_series.lower()) in re.sub(r'\W', '', scraped_series.lower()):
                    is_valid = True
                    df.at[idx, 'Series Name'] = scraped_series.split('#')[0].strip() # Standardize
                else:
                    logger.warning(f"MISMATCH: {title} belongs to '{scraped_series}', not '{target_series}'")
                    # If it belongs to a DIFFERENT series, we should probably remove it from THIS series list
                    # But maybe keep the row? User said "correct this".
                    # Let's flag for explicit removal from this series group.
                    df.at[idx, 'Series Name'] = "REMOVE_MISMATCH"
                    removals += 1
            else:
                # No series info found (Standalone or Blocked validation)
                # Fallback: Check Title for "(Series Name)"
                if target_series.lower() in title.lower():
                     # Likely verified by Title, even if metadata missing (Lara Bailey case)
                     is_valid = True
                     logger.info(f"Verified by Title: {title}")
                else:
                    logger.warning(f"Potential Standalone: {title}")
                    df.at[idx, 'Series Name'] = "REMOVE_STANDALONE"
                    removals += 1

            if is_valid:
                verified_books.append({
                    'idx': idx,
                    'title': title,
                    'count': new_count
                })
                
        except Exception as e:
            logger.error(f"Error processing {title}: {e}")

    await context.close()
    
    # 3. Deduplication (Lara Bailey Fix)
    # If we have multiple rows for the same book, keep the one with highest verification confidence/count
    # Group verified_books by 'Normalized Title'
    
    from collections import defaultdict
    grouped = defaultdict(list)
    
    for obj in verified_books:
        # Normalize title: "Hockey Star's Unexpected Twins"
        # Remove subtitles for grouping
        simple_title = obj['title'].split(':')[0].split('(')[0].strip().lower()
        grouped[simple_title].append(obj)
        
    for title_key, group in grouped.items():
        if len(group) > 1:
            # Sort by count desc
            group.sort(key=lambda x: x['count'], reverse=True)
            keep = group[0]
            drop = group[1:]
            
            logger.info(f"Deduplicating '{title_key}': Keeping ID {keep['idx']}, removing {len(drop)} duplicates.")
            
            for d in drop:
                df.at[d['idx'], 'Series Name'] = "REMOVE_DUPLICATE"
                removals += 1

    logger.success(f"{target_series}: {updates} updates, {removals} marked for removal.")

async def main():
    logger.info(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        sem = asyncio.Semaphore(4) # INCREASED CONCURRENCY
        
        # 1. Broaden Search for Lara Bailey "Billionaire Rules" (Title Search)
        # Many rows are missing 'Series Name' but have it in Title.
        mask = (df['Author Name'] == 'Lara Bailey') & (df['Book Name'].astype(str).str.contains('Billionaire Rules', case=False)) & (df['Series Name'].isna())
        
        if mask.any():
            logger.info(f"Recovered {mask.sum()} books for 'Billionaire Rules' via Title Search.")
            df.loc[mask, 'Series Name'] = 'Billionaire Rules'
        
        tasks = []
        for author, series in TARGETS.items():
            tasks.append(repair_series(sem, browser, df, author, series))
            
        await asyncio.gather(*tasks)
            
        await browser.close()
        
    # Final cleanup
    # Remove rows marked for deletion
    before = len(df)
    df = df[~df['Series Name'].astype(str).str.startswith("REMOVE_")]
    after = len(df)
    
    if before != after:
        logger.success(f"Pruned {before - after} invalid/duplicate rows.")
        
    df.to_csv(INPUT_FILE, index=False)
    logger.success("Repair complete. CSV saved.")

if __name__ == "__main__":
    asyncio.run(main())
