#!/usr/bin/env python3
import asyncio
import pandas as pd
import numpy as np
import os
import json
import google.generativeai as genai
from loguru import logger

MASTER_FILE = "unified_book_data_enriched_ultra.csv"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-flash-latest')

BACKFILL_PROMPT = """
Enrich the metadata for this book.
Input: Title: "{title}" | Author: "{author}"

Fields to find:
1. Publisher: The name of the publisher.
2. Is Self Published: true or false.
3. Primary Trope: 1-3 primary romance tropes.
4. Primary Subgenre: e.g. "Hockey Romance".
5. Short Synopsis: A compelling hook/summary (max 40 words).
6. Amazon Rating: Current average rating.
7. Amazon Rating Count: Total number of ratings.
8. Pages: Book length.
9. Publication Date: YYYY-MM-DD.

Output Format (Return VALID JSON):
{{
  "publisher": "...",
  "is_self_published": true/false,
  "tropes": "...",
  "subgenre": "...",
  "short_synopsis": "...",
  "amazon_rating": 4.5,
  "amazon_rating_count": 1000,
  "pages": 300,
  "pub_date": "YYYY-MM-DD"
}}
"""

async def enrich_book(title, author, sem):
    async with sem:
        for attempt in range(3):
            try:
                prompt = BACKFILL_PROMPT.format(title=title, author=author)
                # Apply a strict timeout to avoid hangs
                response = await asyncio.wait_for(asyncio.to_thread(model.generate_content, prompt), timeout=30)
                text = response.text.strip()
                if "```json" in text:
                    text = text.split("```json")[1].split("```")[0].strip()
                elif "```" in text:
                    text = text.split("```")[1].split("```")[0].strip()
                return json.loads(text)
            except asyncio.TimeoutError:
                logger.warning(f"  Timeout for {title} (Attempt {attempt+1})")
            except Exception as e:
                if "429" in str(e):
                    await asyncio.sleep(10 * (attempt + 1))
                else:
                    logger.error(f"  Error for {title}: {e}")
                    break
        return None

async def main():
    logger.info("Starting Global Metadata Backfill V2 (Hardened)...")
    df = pd.read_csv(MASTER_FILE)
    
    # Target rows missing Publisher, Trope, or Synopsis
    mask = (df['Publisher'].isna()) | (df['Primary Trope'].isna()) | (df['Short Synopsis'].isna())
    missing_indices = df[mask].index.tolist()
    
    logger.info(f"Targeting {len(missing_indices)} rows for enrichment.")
    
    sem = asyncio.Semaphore(15) # ConservativeConcurrency to avoid silent hangs
    
    batch_size = 30
    for i in range(0, len(missing_indices), batch_size):
        batch = missing_indices[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} ({len(batch)} books)...")
        tasks = []
        for idx in batch:
            row = df.loc[idx]
            tasks.append(enrich_book(row['Book Name'], row['Author Name'], sem))
        
        results = await asyncio.gather(*tasks)
        
        asyncio.Lock() # Just to be safe with dataframe updates
        for idx, res in zip(batch, results):
            if res:
                pub = res.get('publisher') or res.get('metadata', {}).get('publisher')
                is_self = res.get('is_self_published') or res.get('metadata', {}).get('is_self_published')
                tropes = res.get('tropes') or res.get('metadata', {}).get('tropes')
                sub = res.get('subgenre') or res.get('metadata', {}).get('subgenre')
                syn = res.get('short_synopsis') or res.get('metadata', {}).get('short_synopsis') or res.get('synopsis')
                rating = res.get('amazon_rating') or res.get('metadata', {}).get('amazon_rating')
                count = res.get('amazon_rating_count') or res.get('metadata', {}).get('amazon_rating_count')
                pages = res.get('pages') or res.get('metadata', {}).get('pages')
                pdate = res.get('pub_date') or res.get('publication_date') or res.get('metadata', {}).get('pub_date')

                if pd.isna(df.at[idx, 'Publisher']): df.at[idx, 'Publisher'] = pub
                if pd.isna(df.at[idx, 'Self Pub Flag']): df.at[idx, 'Self Pub Flag'] = is_self
                if pd.isna(df.at[idx, 'Primary Trope']): df.at[idx, 'Primary Trope'] = str(tropes)
                if pd.isna(df.at[idx, 'Primary Subgenre']): df.at[idx, 'Primary Subgenre'] = sub
                if pd.isna(df.at[idx, 'Short Synopsis']): df.at[idx, 'Short Synopsis'] = syn
                if pd.isna(df.at[idx, 'Amazon Rating']): df.at[idx, 'Amazon Rating'] = rating
                if pd.isna(df.at[idx, 'Amazon # of Ratings']): df.at[idx, 'Amazon # of Ratings'] = count
                if pd.isna(df.at[idx, 'Pages']): df.at[idx, 'Pages'] = pages
                if pd.isna(df.at[idx, 'Publication Date']): df.at[idx, 'Publication Date'] = pdate
                
                df.at[idx, 'Status'] = 'BACKFILLED_V2'
        
        df.to_csv(MASTER_FILE, index=False)
        logger.info(f"  ✓ Saved Batch. Total Processed: {min(i + batch_size, len(missing_indices))}/{len(missing_indices)}")

    logger.success("Global Backfill Complete.")

if __name__ == "__main__":
    asyncio.run(main())
