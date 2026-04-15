import asyncio
import os
import sys
import json
import pandas as pd
from loguru import logger
import google.generativeai as genai
from dotenv import load_dotenv
from playwright.async_api import async_playwright

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NEW_GENRE_DIR = os.path.join(SCRIPT_DIR, 'subgenre-pipeline', 'genre-crawl')
sys.path.insert(0, NEW_GENRE_DIR)

from genre_crawl import crawl_subgenre, SUBGENRE_URLS
from genre_enrichment import enrich_subgenre, create_stealth_context, search_goodreads, extract_goodreads_data
from genre_aggregate import aggregate_subgenre

load_dotenv(os.path.join(SCRIPT_DIR, '.env'))
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-2.5-flash")

async def gemini_scout_top100(genre_name):
    logger.info(f"Gemini Deep Research: Scouting Top 100 series for {genre_name}")
    prompt = f"""
    You are an expert Romance and Fiction Book Data Analyst.
    Identify the TOP 100 most popular and essential book series in the "{genre_name}" subgenre.
    Focus strictly on English-language series.
    Respond ONLY with a raw JSON array of objects.
    
    Format:
    [
      {{"Book Name": "Book 1 Title", "Author Name": "Author", "Series Name": "Series Name", "Source": "Gemini Scout"}},
      ...
    ]
    """
    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, model.generate_content, prompt)
        text = response.text.replace("```json", "").replace("```", "").strip()
        data = json.loads(text)
        
        safe_name = genre_name.replace('/', '_').replace(':', '_')
        out_path = os.path.join(NEW_GENRE_DIR, f"{safe_name}_gemini_scout.csv")
        df = pd.DataFrame(data)
        df.to_csv(out_path, index=False)
        return out_path
    except Exception as e:
        logger.error(f"Gemini Scout failed: {e}")
        return None

async def gemini_vet_batch(batch):
    prompt = f"""
    Review these books. Provide exactly these fields for each in a raw JSON array:
    - "Book Name"
    - "Author Name"
    - "Is English": boolean (True if English edition)
    - "Is Box Set Bundle": boolean (True ONLY if title indicates bundle like #1-3, Vol 1-5. False for #6 part 1)
    - "Publisher": the true publisher (e.g., Berkley, Independently published, Bloom Books)
    Input Data:
    {json.dumps(batch)}
    """
    try:
        loop = asyncio.get_event_loop()
        response = await loop.run_in_executor(None, model.generate_content, prompt)
        text = response.text.replace("```json", "").replace("```", "").strip()
        return json.loads(text)
    except Exception as e:
        return []

async def fetch_missing_goodreads(missing_df):
    updates = {}
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await create_stealth_context(browser)
        page = await context.new_page()
        for idx, row in missing_df.iterrows():
            book = row['Book Name']
            author = row['Author Name']
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

def map_bsr(book_name, ranks_df):
    if not isinstance(book_name, str) or not book_name.strip(): return ""
    book = book_name.strip().lower()
    found_flags = set()
    matches = ranks_df[ranks_df['Book_Lower'].str.contains(book, regex=False, na=False)]
    for _, mr in matches.iterrows():
        rank, lname = mr['Rank'], mr['List Name']
        if rank <= 10: found_flags.add(f"# Top 10 {lname}")
        elif rank <= 50: found_flags.add(f"# Top 50 {lname}")
        else: found_flags.add(f"# Top 100 {lname}")
    return " | ".join(sorted(list(found_flags)))

async def run_v2_pipeline(genre_name):
    logger.info(f"\n{'='*60}\n🚀 STARTING V2 PIPELINE: {genre_name}\n{'='*60}")
    safe_name = genre_name.replace('/', '_').replace(':', '_')
    
    # 1. Pipeline: Amazon Discovery
    raw_csv = await crawl_subgenre(genre_name)
    if not raw_csv: return
    
    # 2. Pipeline: Gemini Top 100 Discovery
    scout_csv = await gemini_scout_top100(genre_name)
    
    # 3. Merge Discoveries
    df_raw = pd.read_csv(raw_csv)
    if scout_csv and os.path.exists(scout_csv):
        df_scout = pd.read_csv(scout_csv)
        df_combined = pd.concat([df_raw, df_scout]).drop_duplicates(subset=['Book Name', 'Author Name']).reset_index(drop=True)
    else:
        df_combined = df_raw
        
    combined_raw_path = os.path.join(NEW_GENRE_DIR, f"{safe_name}_combined_discovery.csv")
    df_combined.to_csv(combined_raw_path, index=False)
    
    # 4. Enrichment
    logger.info(f"📚 ENRICHING {len(df_combined)} books...")
    enriched_csv = await enrich_subgenre(combined_raw_path, genre_name)
    
    # 5. Data Quality V2 (Vetting, GR Backfill, BSR Mapping)
    logger.info("🧹 RUNNING DATA QUALITY V2 VETTING & MAPPING...")
    df = pd.read_csv(enriched_csv)
    
    # Gemini Vetting
    batches = []
    chunk_size = 40
    for i in range(0, len(df), chunk_size):
        batches.append(df.iloc[i:i+chunk_size][['Book Name', 'Author Name']].to_dict(orient='records'))
        
    tasks = [gemini_vet_batch(b) for b in batches]
    gem_results = await asyncio.gather(*tasks)
    gem_df = pd.DataFrame([item for sublist in gem_results for item in sublist])
    
    drop_indices = []
    for idx, row in df.iterrows():
        if not gem_df.empty:
            match = gem_df[(gem_df['Book Name'] == row['Book Name']) & (gem_df['Author Name'] == row['Author Name'])]
            if not match.empty:
                g_row = match.iloc[0]
                if not g_row.get('Is English', True) or g_row.get('Is Box Set Bundle', False):
                    drop_indices.append(idx)
                    continue
                if pd.isna(row.get('Publisher')) or str(row.get('Publisher')).strip() == '':
                    df.at[idx, 'Publisher'] = g_row.get('Publisher', '')

    df = df.drop(index=drop_indices).reset_index(drop=True)
    
    # GR Backfill
    missing_mask = df['Goodreads Rating'].isna() | (df['Goodreads Rating'] == '') | (df['Goodreads Rating'] == 0.0)
    missing_df = df[missing_mask]
    if not missing_df.empty:
        updates = await fetch_missing_goodreads(missing_df)
        for idx, update in updates.items():
            if update:
                df.at[idx, 'Goodreads Link'] = update['Goodreads Link']
                df.at[idx, 'Goodreads Rating'] = update['Goodreads Rating']
                df.at[idx, 'Goodreads # of Ratings'] = update['Goodreads # of Ratings']
                if update['Goodreads Series URL']: df.at[idx, 'Goodreads Series URL'] = update['Goodreads Series URL']
                
    # BSR Mapping
    ranks_csv = os.path.join(NEW_GENRE_DIR, 'Az_Bestsellers_Master_Ranks.csv')
    if os.path.exists(ranks_csv):
        ranks_df = pd.read_csv(ranks_csv)
        ranks_df['Book_Lower'] = ranks_df['Book'].astype(str).str.lower()
        for idx, row in df.iterrows():
            flags = map_bsr(row['Book Name'], ranks_df)
            if flags:
                existing = str(row.get('Source Detail', ''))
                df.at[idx, 'Source Detail'] = existing + " | " + flags if existing and existing != 'nan' else flags

    cleaned_csv = os.path.join(NEW_GENRE_DIR, f"{safe_name}_enriched_v2_clean.csv")
    df.to_csv(cleaned_csv, index=False)
    
    # 6. Final Aggregation
    final_csv = aggregate_subgenre(cleaned_csv, genre_name)
    logger.success(f"🎉 V2 PIPELINE COMPLETE FOR {genre_name}! Output: {final_csv}")

async def main():
    genres = [
        "Historic Fiction & Romance",
        "Military Drama/Romance",
        "Small Town Drama/Romance",
        "Christian Drama/Romance",
        "Mafia Drama/Romance",
        "Dark Romance",
        "Forbidden Romance",
        "Romantic Suspense / Psychological Thriller"
    ]
    for g in genres:
        await run_v2_pipeline(g)

if __name__ == '__main__':
    asyncio.run(main())
