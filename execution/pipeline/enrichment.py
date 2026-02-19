import asyncio
import pandas as pd
import json
import re
import google.generativeai as genai
from loguru import logger
from execution.pipeline.config import GEMINI_API_KEY, MAX_WORKERS, GEMINI_BATCH_SIZE

# Configuration
BATCH_SIZE = GEMINI_BATCH_SIZE
model = None

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

METADATA_PROMPT = """
Enrich metadata for this Specific Book. 
DETERMINISTIC DATA ONLY. If unsure, return "Unknown" or null.

IDENTITY ANCHORS:
- Series: "{series_name}"
- Title: "{book_title}"
- Author: "{author_name}"

CRITICAL RULES:
1. ONLY return data for this exact book/author. 
2. If the description mentions "Fans of [Other Author]", IGNORE the other author.
3. featured_list MUST be a Top 100 Bestseller rank (e.g. "#42 in Sports Romance"). 
4. IGNORE ranks > 100 (e.g. ignore #15,000).

Return ONLY valid JSON:
{{
  "publisher": "Precise Publisher Name",
  "is_self_published": true/false,
  "primary_trope": "Comma-separated tropes",
  "featured_list": "Top 100 rank if available, else null"
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
ADVANCED_METADATA_PROMPT = """
Task: Deterministic Market Intelligence for Book Series.
ANCHORS:
- Series: "{series_name}"
- Book 1: "{book_title}"
- Author: "{author_name}"
- Context: "{featured_info}"

CRITICAL RULES:
1. notable_lists: ONLY include major lists (Amazon Top 100, NYT, USA Today, Goodreads Top).
2. peak_performance: The highest rank (#1-100) achieved. IGNORE ranks above 100.
3. SUBJECTIVE: Provide a 1-line hook focusing on tropes and uniqueness.
4. DIFFERENTIATOR: Precise reason for high-potential (Market appeal, ratings density).
5. DO NOT hallucinate. If list data is not in "Context", return "No Major List Appearances".

Return ONLY valid JSON:
{{
  "notable_lists": "string",
  "peak_performance": "string",
  "subjective_analysis": "string",
  "differentiator": "string"
}}
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

async def _call_gemini_advanced(series, title, author, featured, sem):
    """Fetch advanced qualitative metadata."""
    if not model: return None
    async with sem:
        for _ in range(3):
            try:
                prompt = ADVANCED_METADATA_PROMPT.format(
                    series_name=series, book_title=title, author_name=author, featured_info=featured
                )
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
async def enrich_metadata_async(df, p0_p1_series=None):
    """Fill gaps in Publisher, Trope, Featured List. Optionally focus only on P0/P1."""
    logger.info("Starting Gemini Metadata Enrichment...")
    sem = asyncio.Semaphore(MAX_WORKERS)
    
    # Identify gaps
    mask = (df['Publisher'].isna()) | (df['Primary Trope'].isna()) | (df['Featured List'].isna()) 
    
    if p0_p1_series:
        mask = mask & (df['Series Name'].isin(p0_p1_series))
        
    target_idx = df[mask].index.tolist()
    
    if not target_idx:
        logger.info("No metadata gaps found in target series.")
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

async def enrich_advanced_async(df, p0_p1_series):
    """Specialized enrichment for P0/P1 candidates."""
    logger.info("Starting Advanced Qualitative Enrichment (P0/P1 Focus)...")
    sem = asyncio.Semaphore(MAX_WORKERS)
    
    # Filter for P0/P1
    mask = df['Series Name'].isin(p0_p1_series)
    # We only need to enrich the "representative" book of the series or all of them? 
    # Usually the first book is best for subjective analysis.
    # For now, targeting the first book of each series to save tokens.
    target_idx = df[mask].groupby('Series Name').head(1).index.tolist()
    
    if not target_idx:
        logger.info("No P0/P1 series found for advanced enrichment.")
        return df

    logger.info(f"Enriching {len(target_idx)} priority series...")
    
    for i in range(0, len(target_idx), BATCH_SIZE):
        batch = target_idx[i:i+BATCH_SIZE]
        tasks = []
        for idx in batch:
            row = df.loc[idx]
            tasks.append(_call_gemini_advanced(
                row.get('Series Name', ''), row.get('Book Name', ''), row.get('Author Name', ''), row.get('Featured List', ''), sem
            ))
            
        results = await asyncio.gather(*tasks)
        
        for idx, res in zip(batch, results):
            if res:
                series_name = df.loc[idx, 'Series Name']
                s_mask = df['Series Name'] == series_name
                # Apply series-level data to all books in series? No, let's keep it clean.
                # Actually analysis.py will look at the group. 
                # Better to store these in the first book and let analysis aggregate.
                df.at[idx, 'Notable Lists'] = res.get('notable_lists')
                df.at[idx, 'Peak Performance'] = res.get('peak_performance')
                df.at[idx, 'Subjective Analysis'] = res.get('subjective_analysis')
                df.at[idx, 'Differentiator'] = res.get('differentiator')
                
        logger.info(f"Processed advanced batch {i//BATCH_SIZE + 1}")
        
    return df

async def truncate_descriptions_async(df):
    """Truncate descriptions."""
    logger.info("Starting Description Truncation...")
    sem = asyncio.Semaphore(MAX_WORKERS)
    
    indices = df.index.tolist()
    # Filter for those needing truncation
    target_idx = [idx for idx in indices if len(str(df.at[idx, 'Description'])) > 250]
    
    if not target_idx:
        logger.info("All descriptions are already concise.")
        return df

    logger.info(f"Truncating {len(target_idx)} descriptions...")
    # Batch process
    for i in range(0, len(target_idx), BATCH_SIZE):
        batch = target_idx[i:i+BATCH_SIZE]
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

def run_enrichment_pipeline(df, p0_p1_series=None):
    """Sync wrapper for async pipeline."""
    df = asyncio.run(enrich_metadata_async(df, p0_p1_series=p0_p1_series))
    if p0_p1_series:
        df = asyncio.run(enrich_advanced_async(df, p0_p1_series))
    df = asyncio.run(truncate_descriptions_async(df))
    return df
