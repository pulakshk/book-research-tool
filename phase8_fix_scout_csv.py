import os
import json
import asyncio
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
import google.generativeai as genai

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NEW_GENRE_DIR = os.path.join(SCRIPT_DIR, 'sub genre analysis', 'New genre crawl')

load_dotenv(os.path.join(SCRIPT_DIR, '.env'))
genai.configure(api_key=os.environ.get("GEMINI_API_KEY"))
model = genai.GenerativeModel("gemini-2.5-flash")

async def fetch_gemini_fixes(batch):
    prompt = f"""
    You are an expert Romance and Fiction Book Data Analyst.
    For the following book series, provide exactly these missing or corrected fields.
    Respond ONLY with a raw JSON array of objects, no markdown formatting.
    
    Fields per object:
    - "Series Name": exactly as provided
    - "Author Name": exactly as provided
    - "Is Genuinely Standalone": boolean (true ONLY if it is a single book. Box sets/bundles like #1-4 or chronologies are FALSE, they are Series)
    - "True Books In Series List": comma-separated list of ALL book titles in the series in order (e.g. "Book1, Book2, Book3")
    - "Highest Rated Book Name": the specific title in the series that is best rated by fans.
    - "Strict Publisher Flag": analyze the given publisher and strictly return ONLY "Trad Pub", "Indie", or "Self-Pub"
    - "Amazon Series URL": an Amazon USA search URL or series page URL for this specific series
    
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

def map_bsr(series_book_list, ranks_df):
    if not isinstance(series_book_list, str) or not series_book_list.strip():
        return ""
        
    books = [b.strip().lower() for b in series_book_list.split(',')]
    found_flags = set()
    
    for book in books:
        # Check against ranks
        # Simple string match
        mask = ranks_df['Book_Lower'].str.contains(book, regex=False, na=False)
        matches = ranks_df[mask]
        
        for _, match_row in matches.iterrows():
            rank = match_row['Rank']
            lname = match_row['List Name']
            
            if rank <= 10:
                flag = f"Top 10 {lname}"
            elif rank <= 50:
                flag = f"Top 50 {lname}"
            else:
                flag = f"Top 100 {lname}"
                
            found_flags.add(flag)
            
    return " | ".join(sorted(list(found_flags)))

async def main():
    final_csv = os.path.join(SCRIPT_DIR, 'sub genre analysis', 'All_9_Subgenres_Scout_Top25_ENRICHED_FINAL.csv')
    gr_csv = os.path.join(NEW_GENRE_DIR, 'All_9_Subgenres_Scout_Top25_AGGREGATED.csv')
    ranks_csv = os.path.join(NEW_GENRE_DIR, 'Az_Bestsellers_Master_Ranks.csv')
    
    df = pd.read_csv(final_csv)
    gr_df = pd.read_csv(gr_csv)
    
    ranks_df = pd.DataFrame(columns=['Book_Lower', 'Rank', 'List Name'])
    if os.path.exists(ranks_csv):
        ranks_df = pd.read_csv(ranks_csv)
        ranks_df['Book_Lower'] = ranks_df['Book'].astype(str).str.lower()
        logger.info(f"Loaded {len(ranks_df)} Amazon Bestseller records for mapping.")
    else:
        logger.warning("BSR scraper hasn't finished or failed. No BSRs will be mapped.")
        
    # Build batch payload
    batches = []
    chunk_size = 20
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size][['Book Series Name', 'Author Name', 'Publisher Name']].copy()
        chunk.rename(columns={'Book Series Name': 'Series Name'}, inplace=True)
        batches.append(chunk.to_dict(orient='records'))
        
    logger.info(f"Running {len(batches)} batches through Gemini to fix series lists and publishers...")
    tasks = [fetch_gemini_fixes(b) for b in batches]
    results = await asyncio.gather(*tasks)
    
    enriched_data = []
    for r in results:
        enriched_data.extend(r)
    gemini_df = pd.DataFrame(enriched_data)
    
    # Merge Phase
    for idx, row in df.iterrows():
        s_name = row['Book Series Name']
        auth = row['Author Name']
        
        # 1. Attach Goodreads URL
        gr_match = gr_df[(gr_df['Series Name'] == s_name) & (gr_df['Author Name'] == auth)]
        gr_url = gr_match.iloc[0]['Goodreads Series URL'] if not gr_match.empty else ""
        df.at[idx, 'Goodreads Series URL'] = gr_url
        
        # 2. Attach Gemini Fixes
        g_match = gemini_df[(gemini_df['Series Name'] == s_name) & (gemini_df['Author Name'] == auth)]
        g_row = g_match.iloc[0] if not g_match.empty else {}
        
        # Override Standalone flag if Gemini vet says it's not
        is_standalone = g_row.get('Is Genuinely Standalone', False)
        df.at[idx, 'Type'] = 'Standalone' if is_standalone else 'Series'
        
        # Override Books In Series List
        full_list = g_row.get('True Books In Series List', '')
        if full_list and full_list != '...':
            df.at[idx, 'Books_In_Series_List'] = full_list
            
        # Highest Rated Book
        df.at[idx, 'Highest Rated Book Name'] = g_row.get('Highest Rated Book Name', '')
        
        # Strict Publisher
        strict_pub = g_row.get('Strict Publisher Flag', '')
        if strict_pub in ['Trad Pub', 'Indie', 'Self-Pub']:
            df.at[idx, 'Self Pub Flag'] = strict_pub
            
        # Amazon Series URL
        df.at[idx, 'Amazon Series URL'] = g_row.get('Amazon Series URL', '')
        
        # 3. Map BSR
        bsr_flags = map_bsr(full_list if full_list else row['Books_In_Series_List'], ranks_df)
        df.at[idx, 'Books_Featured_Rank_Validation'] = bsr_flags

    # Fix column ordering -> insert the new URLs near the end
    out_path = os.path.join(SCRIPT_DIR, 'sub genre analysis', 'All_9_Subgenres_Scout_Top25_V2_FINAL.csv')
    df.to_csv(out_path, index=False)
    logger.success(f"V2 Data Quality fixes applied. Saved to {out_path}")

if __name__ == '__main__':
    asyncio.run(main())
