import asyncio
import pandas as pd
import google.generativeai as genai
from loguru import logger
from execution.pipeline.config import GEMINI_API_KEY, GEMINI_BATCH_SIZE

# Config
INPUT_FILE = "data/unified_book_data_enriched_final.csv"
BATCH_SIZE = 15 # Faster batches

if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-flash-latest')
    except: model = None

import json

RATING_PROMPT = """
Task: Estimate the current Goodreads statistics for this book.
Book: "{book_title}"
Author: "{author_name}"

Context:
- The user is building a dataset of bestselling books.
- This book is missing its rating data.
- We need *approximate* numbers based on your knowledge.

Return ONLY valid JSON:
{{
  "rating": <float, e.g. 3.98>,
  "count": <int, e.g. 15000>
}}

Rules:
- Rating: Typical Goodreads star rating (1.0 - 5.0).
- Count: Integer number of ratings. If unsure but book is known, guess 100. If completely unknown, return 0.
"""

async def fetch_rating_count(series, title, author, sem):
    """Fetch rating metrics from Gemini."""
    if not model: return {'rating': 0.0, 'count': 0}
    async with sem:
        for _ in range(3):
            try:
                prompt = RATING_PROMPT.format(book_title=title, author_name=author)
                response = await asyncio.to_thread(model.generate_content, prompt)
                text = response.text.strip().replace('```json', '').replace('```', '')
                try:
                    data = json.loads(text)
                    return {
                        'rating': float(data.get('rating', 0.0)), 
                        'count': int(data.get('count', 0))
                    }
                except:
                    # Fallback regex
                    import re
                    r = re.search(r'"rating":\s*([\d.]+)', text)
                    c = re.search(r'"count":\s*(\d+)', text)
                    return {
                        'rating': float(r.group(1)) if r else 0.0,
                        'count': int(c.group(1)) if c else 0
                    }
            except Exception as e:
                if "429" in str(e): await asyncio.sleep(2)
                else: return {'rating': 0.0, 'count': 0}
        return {'rating': 0.0, 'count': 0}

async def main():
    logger.info("Loading dataset...")
    df = pd.read_csv(INPUT_FILE)
    
    # Identify gaps (Missing Count OR Missing Rating)
    # Gap = Count is 0 or Rating is 0.
    mask = (df['Goodreads # of Ratings'] == 0) | (df['Goodreads # of Ratings'].isna()) | \
           (df['Goodreads Rating'] == 0) | (df['Goodreads Rating'].isna())
           
    target_idx = df[mask].index.tolist()
    
    if not target_idx:
        logger.info("No missing Goodreads data found.")
        return

    logger.info(f"Targeting {len(target_idx)} books for Gemini backfill (Goodreads Data)...")
    
    sem = asyncio.Semaphore(20) 
    
    # Process in batches
    for i in range(0, len(target_idx), BATCH_SIZE):
        batch = target_idx[i:i+BATCH_SIZE]
        tasks = []
        for idx in batch:
            row = df.loc[idx]
            tasks.append(fetch_rating_count(
                row.get('Series Name', ''), row.get('Book Name', ''), row.get('Author Name', ''), sem
            ))
        
        results = await asyncio.gather(*tasks)
        
        updates = 0
        for idx, res in zip(batch, results):
            # Only update if we have a valid value to overwrite a missing one
            
            # Count
            try:
                curr_c = float(df.at[idx, 'Goodreads # of Ratings'])
            except: curr_c = 0
            
            if pd.isna(curr_c) or curr_c == 0:
                if res['count'] > 0: 
                    df.at[idx, 'Goodreads # of Ratings'] = res['count']
                    updates += 1
            
            # Rating
            try:
                curr_r = float(df.at[idx, 'Goodreads Rating'])
            except: curr_r = 0
            
            if pd.isna(curr_r) or curr_r == 0:
                if res['rating'] > 0: 
                    df.at[idx, 'Goodreads Rating'] = res['rating']
                    # Don't double count updates count
            
        logger.info(f"Batch {i//BATCH_SIZE + 1}: Updated {updates} books with new data.")
        
        # Save periodically
        if i % (BATCH_SIZE * 5) == 0:
             df.to_csv(INPUT_FILE, index=False)
             
    df.to_csv(INPUT_FILE, index=False)
    logger.success("Backfill complete.")

if __name__ == "__main__":
    asyncio.run(main())
