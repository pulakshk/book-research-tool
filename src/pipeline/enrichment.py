import asyncio
import pandas as pd
import json
import re
import google.generativeai as genai
from loguru import logger
from src.pipeline.config import GEMINI_API_KEY, MAX_WORKERS, GEMINI_BATCH_SIZE

# Configuration
BATCH_SIZE = GEMINI_BATCH_SIZE

if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-flash-latest')
    except Exception as e:
        logger.error(f"Gemini Config Error: {e}")
        model = None

# ---------------- SCRAPING (PLACEHOLDER FOR FUTURE EXPANSION) ----------------
# Moving scraping logic here would be ideal, but for now we focus on Gemini 
# which was the user's specific "this is working" request.

# ---------------- PROMPTS ----------------
METADATA_PROMPT = """
Enrich metadata for this book:
- Series: "{series_name}"
- Title: "{book_title}"
- Author: "{author_name}"

Return ONLY valid JSON with these fields:
{{
  "publisher": "Publisher name (or 'Unknown')",
  "is_self_published": true/false,
  "primary_trope": "Main romance trope(s), comma-separated",
  "featured_list": "Any Amazon/Goodreads/NYT bestseller lists (e.g. '#1 Sports Romance'), or null"
}}
"""

TRUNCATE_PROMPT = """
Context:
- Book: "{book_title}"
- Author: "{author_name}"
- Current Description: "{description}"

Task:
1. SANITY CHECK: Does the Current Description match this specific Book? If no, IGNORE it.
2. ACTION: Rewrite/Condense into 1-2 compelling, punchy lines (max 200 characters).

Return ONLY the final 1-2 line summary.
"""

# ---------------- ASYNC WORKERS ----------------
async def _call_gemini_metadata(series, title, author, sem):
    """Fetch metadata JSON."""
    if not model: return None
    async with sem:
        for _ in range(3):
            try:
                prompt = METADATA_PROMPT.format(series_name=series, book_title=title, author_name=author)
                response = await asyncio.to_thread(model.generate_content, prompt)
                text = response.text.strip().replace("```json", "").replace("```", "")
                return json.loads(text)
            except Exception as e:
                if "429" in str(e): await asyncio.sleep(2)
                else: return None
        return None

async def _call_gemini_truncate(title, author, desc, sem):
    """Clean description."""
    if not model: return desc
    async with sem:
        for _ in range(3):
            try:
                prompt = TRUNCATE_PROMPT.format(book_title=title, author_name=author, description=desc[:1500])
                response = await asyncio.to_thread(model.generate_content, prompt)
                clean = response.text.strip().strip('"')
                return clean[:200]
            except Exception as e:
                if "429" in str(e): await asyncio.sleep(2)
                else: return desc
        return desc

# ---------------- MAIN FUNCTIONS ----------------
async def enrich_metadata_async(df):
    """Fill gaps in Publisher, Trope, Featured List."""
    logger.info("Starting Gemini Metadata Enrichment...")
    sem = asyncio.Semaphore(MAX_WORKERS)
    
    # Identify gaps
    mask = (df['Publisher'].isna()) | (df['Primary Trope'].isna()) | (df['Featured List'].isna()) 
    target_idx = df[mask].index.tolist()
    
    if not target_idx:
        logger.info("No metadata gaps found.")
        return df

    logger.info(f"Enriching {len(target_idx)} books...")
    
    # Process in batches
    for i in range(0, len(target_idx), BATCH_SIZE):
        batch = target_idx[i:i+BATCH_SIZE]
        tasks = []
        for idx in batch:
            row = df.loc[idx]
            tasks.append(_call_gemini_metadata(
                row.get('Series Name', ''), row.get('Book Name', ''), row.get('Author Name', ''), sem
            ))
        
        results = await asyncio.gather(*tasks)
        
        for idx, res in zip(batch, results):
            if res:
                if not pd.isna(res.get('publisher')): df.at[idx, 'Publisher'] = res['publisher']
                if not pd.isna(res.get('primary_trope')): df.at[idx, 'Primary Trope'] = res['primary_trope']
                if not pd.isna(res.get('featured_list')): df.at[idx, 'Featured List'] = res['featured_list']
                
                # Derive Self Pub Flag
                if 'is_self_published' in res:
                    df.at[idx, 'Self Pub Flag'] = "Self Pub" if res['is_self_published'] else "Big Pub"
                    
        logger.info(f"Processed batch {i//BATCH_SIZE + 1}")
        
    return df

async def truncate_descriptions_async(df):
    """Truncate descriptions."""
    logger.info("Starting Description Truncation...")
    sem = asyncio.Semaphore(MAX_WORKERS)
    
    indices = df.index.tolist()
    # Batch process
    for i in range(0, len(indices), BATCH_SIZE):
        batch = indices[i:i+BATCH_SIZE]
        tasks = []
        for idx in batch:
            row = df.loc[idx]
            tasks.append(_call_gemini_truncate(
                row.get('Book Name', ''), row.get('Author Name', ''), str(row.get('Description', '')), sem
            ))
            
        results = await asyncio.gather(*tasks)
        
        for idx, res in zip(batch, results):
            if res:
                df.at[idx, 'Description'] = res
                
        logger.info(f"Processed batch {i//BATCH_SIZE + 1} for descriptions.")
        
    return df

def run_enrichment_pipeline(df):
    """Sync wrapper for async pipeline."""
    df = asyncio.run(enrich_metadata_async(df))
    df = asyncio.run(truncate_descriptions_async(df))
    return df
