#!/usr/bin/env python3
"""
Deep Standardization & Global Series Backfill
- Unifies the format across all rows (New Injections vs Old Rows).
- Standardizes Book Numbers (e.g., '6.0' -> '6').
- Uses Gemini to backfill missing 'Series Name' and 'Book Number' for rows where they are blank.
- Removes rows where no series info can be found after lookup (Filtration activity).
"""
import asyncio
import pandas as pd
import numpy as np
import os
import re
import json
import google.generativeai as genai
from loguru import logger

MASTER_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-flash-latest')

async def lookup_series(title, author, retries=3):
    """Call Gemini to find series info."""
    prompt = f"""
Identify the Book Series and Book Number for this title by this author.
Input: Title: "{title}" | Author: "{author}"

Rules:
1. If it belongs to a series, return the Series Name and its index in that series (e.g., 1, 2.5).
2. If it is a Standalone, return "Standalone".
3. If you are 100% sure it does not exist or information is missing, return "NOT_FOUND".

Output Format (Return VALID JSON):
{{
  "series_name": "Series Name",
  "book_number": 1.0,
  "is_standalone": false
}}
"""
    for _ in range(retries):
        try:
            response = model.generate_content(prompt)
            text = response.text.strip()
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0].strip()
            elif "```" in text:
                text = text.split("```")[1].split("```")[0].strip()
            data = json.loads(text)
            return data
        except:
            await asyncio.sleep(2)
    return None

async def standardize_and_enrich():
    logger.info("Starting Deep Standardization & Series Backfill...")
    if not os.path.exists(MASTER_FILE):
        logger.error(f"{MASTER_FILE} not found.")
        return

    df = pd.read_csv(MASTER_FILE)
    
    # 1. Basic Cleaning
    logger.info("  1. Basic field cleaning...")
    # Standardize Book Number (remove .0)
    def clean_num(x):
        try:
            if pd.isna(x) or str(x).lower() == 'nan': return np.nan
            val = float(x)
            return int(val) if val == int(val) else val
        except:
            return x
    
    if 'Book Number' in df.columns:
        df['Book Number'] = df['Book Number'].apply(clean_num)
    
    # Unify Status labels
    df['Status'] = df['Status'].fillna('LEGACY_ROW')

    # 2. Targeted Backfill for Missing Series
    mask_missing = (df['Series Name'].isna()) | (df['Series Name'] == '') | (df['Series Name'] == 'NO_SERIES')
    rows_to_check = df[mask_missing].index.tolist()
    
    logger.info(f"  2. Backfilling {len(rows_to_check)} rows with missing series info...")
    
    sem = asyncio.Semaphore(20) # STABLE: 20 parallel lookups
    
    async def process_row(idx):
        async with sem:
            row = df.loc[idx]
            title, author = row['Book Name'], row['Author Name']
            res = await lookup_series(title, author)
            if res and isinstance(res, dict):
                if res.get('is_standalone'):
                    # User requested filtration: "if standalone just write standalone... else remove"
                    df.at[idx, 'Series Name'] = 'Standalone'
                    df.at[idx, 'Status'] = 'RESOLVED_STANDALONE'
                    logger.info(f"  [Standardize] ✓ {title}: Standalone")
                elif res.get('series_name') and res['series_name'] != 'NOT_FOUND':
                    # CANONICAL OVERWRITE: Always use exact Gemini strings
                    df.at[idx, 'Book Name'] = res.get('title', title)
                    df.at[idx, 'Author Name'] = res.get('author', author)
                    df.at[idx, 'Series Name'] = res['series_name']
                    df.at[idx, 'Book Number'] = clean_num(res.get('book_number'))
                    df.at[idx, 'Status'] = 'RESOLVED_SERIES_V3'
                    logger.info(f"  [Standardize] ✓ {res.get('title', title)}: {res['series_name']} #{res.get('book_number')}")
                else:
                    df.at[idx, 'Status'] = 'FAILED_LOOKUP'
                    logger.warning(f"  [Standardize] ✗ {title}: No Series Found")
            else:
                df.at[idx, 'Status'] = 'FAILED_LOOKUP'
                logger.error(f"  [Standardize] !! {title}: Invalid Response or Gemini Error")

    tasks = [process_row(idx) for idx in rows_to_check]
    await asyncio.gather(*tasks)

    # 3. Filtration Activity
    # Only remove if specifically requested and it's trash data
    # For now, we keep everything but mark it. 
    # USER: "If you cannot find it, just remove that column from the file" -> interpreted as "remove that row"
    logger.info("  3. Performing filtration (removing unresolved rows)...")
    initial_len = len(df)
    df = df[df['Status'] != 'FAILED_LOOKUP']
    final_len = len(df)
    logger.info(f"  ✓ Filtered {initial_len - final_len} unresolved rows.")

    # 4. Final Formatting
    # Ensure all 27 columns exist and are ordered correctly
    # (Simplified for now, but keeping data intact)
    
    df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"Final standardized data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    asyncio.run(standardize_and_enrich())
