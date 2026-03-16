import os
import json
import asyncio
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
import google.generativeai as genai
from playwright.async_api import async_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NEW_GENRE_DIR = os.path.join(SCRIPT_DIR, 'sub genre analysis', 'New genre crawl')

import sys
sys.path.insert(0, NEW_GENRE_DIR)
from genre_enrichment import create_stealth_context, search_goodreads, extract_goodreads_data

load_dotenv(os.path.join(SCRIPT_DIR, '.env'))
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-2.5-flash")

async def gemini_vet_batch(batch):
    prompt = f"""
    You are an expert Romance and Fiction Book Data Analyst.
    Review the following book titles and authors. Provide exactly these fields for each book in a raw JSON array.
    
    Fields per object:
    - "Book Name": exactly as provided
    - "Author Name": exactly as provided
    - "Is English": boolean (True if the title/author heavily implies an English-language edition. False if it's clearly Spanish, French, German, Italian, etc.)
    - "Is Box Set Bundle": boolean (True ONLY if the title indicates a bundle or collection like '#1-3', 'Vol 1-5', 'Collection', 'Box Set'. False if it's a single book like '#6, part 1' or just 'Book 1')
    - "Publisher": the actual primary publisher of this specific book/series (e.g., "Berkley", "Independently published", "Bloom Books"). If you don't know, guess based on the author's usual publisher.
    
    Input Data:
    {json.dumps(batch)}
    """
    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, model.generate_content, prompt)
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        logger.error(f"Gemini error: {e}")
        return []

async def fetch_missing_goodreads(missing_df):
    logger.info(f"Targeting {len(missing_df)} books for missing Goodreads data...")
    updates = {}
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await create_stealth_context(browser)
        page = await context.new_page()
        
        for idx, row in missing_df.iterrows():
            book = row['Book Name']
            author = row['Author Name']
            logger.info(f"Scraping Goodreads for {book} by {author}")
            
            gr_result = await search_goodreads(page, book, author)
            if gr_result:
                gr_data = await extract_goodreads_data(page, gr_result['link'])
                updates[idx] = {
                    'Goodreads Link': gr_result['link'],
                    'Goodreads Rating': gr_data.get('gr_rating', ''),
                    'Goodreads # of Ratings': gr_data.get('gr_rating_count', ''),
                    'Goodreads Series URL': gr_data.get('gr_series_url', '')
                }
            else:
                updates[idx] = None
        await browser.close()
    return updates

async def main():
    csv_path = os.path.join(NEW_GENRE_DIR, 'Political Drama_Romance_enriched_filtered.csv')
    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows from Political Drama Enriched Filtered file.")
    
    # 1. Gemini Vet: Language, Publisher, Box Sets
    batches = []
    chunk_size = 30
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size][['Book Name', 'Author Name']].to_dict(orient='records')
        batches.append(chunk)

    logger.info("Running Gemini vetting on all rows...")
    tasks = [gemini_vet_batch(b) for b in batches]
    results = await asyncio.gather(*tasks)
    
    gemini_data = []
    for r in results:
        gemini_data.extend(r)
    gem_df = pd.DataFrame(gemini_data)
    
    drop_indices = []
    for idx, row in df.iterrows():
        match = gem_df[(gem_df['Book Name'] == row['Book Name']) & (gem_df['Author Name'] == row['Author Name'])]
        if not match.empty:
            g_row = match.iloc[0]
            
            # Drop constraint 1: Non-English
            if not g_row.get('Is English', True):
                logger.warning(f"Dropping Non-English: {row['Book Name']}")
                drop_indices.append(idx)
                continue
                
            # Drop constraint 2: Box set bundles
            if g_row.get('Is Box Set Bundle', False):
                logger.warning(f"Dropping Box Set Bundle: {row['Book Name']}")
                drop_indices.append(idx)
                continue
                
            # Update Publisher if missing
            if pd.isna(row.get('Publisher')) or str(row.get('Publisher')).strip() == '':
                df.at[idx, 'Publisher'] = g_row.get('Publisher', '')

    df = df.drop(index=drop_indices).reset_index(drop=True)
    logger.info(f"Dropped {len(drop_indices)} rows. Remaining: {len(df)}")
    
    # 2. Backfill Goodreads Ratings
    missing_gr_mask = df['Goodreads Rating'].isna() | (df['Goodreads Rating'] == '') | (df['Goodreads Rating'] == 0.0)
    missing_df = df[missing_gr_mask]
    
    if not missing_df.empty:
        updates = await fetch_missing_goodreads(missing_df)
        for idx, update in updates.items():
            if update:
                df.at[idx, 'Goodreads Link'] = update['Goodreads Link']
                df.at[idx, 'Goodreads Rating'] = update['Goodreads Rating']
                df.at[idx, 'Goodreads # of Ratings'] = update['Goodreads # of Ratings']
                if update['Goodreads Series URL']:
                    df.at[idx, 'Goodreads Series URL'] = update['Goodreads Series URL']
    
    out_path = os.path.join(NEW_GENRE_DIR, 'Political Drama_Romance_enriched_filtered_v2.csv')
    df.to_csv(out_path, index=False)
    logger.success(f"V2 Part 1 completed: {out_path}")

if __name__ == '__main__':
    asyncio.run(main())
