#!/usr/bin/env python3
import asyncio
import pandas as pd
import re
import os
import json
import random
from loguru import logger
from playwright.async_api import async_playwright
import google.generativeai as genai
import sys

# CONFIG
INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") 
NUM_WORKERS = 20

if GEMINI_API_KEY:
    genai.configure(api_key=GEMINI_API_KEY)
    model = genai.GenerativeModel('gemini-flash-latest')

# PROMPT
CLEAN_TITLE_PROMPT = """
Identify the pure, original English title of this book from the HTML.

STRICT RULES:
1. Return ONLY the title text.
2. Remove "by [Author Name]" or "By [Author Name]".
3. Remove anything in parentheses or brackets.
4. Remove "Book X", "Vol X", "Series", or "#X".
5. Remove "Edition", "Spanish Edition", "French Edition", etc.
6. Remove any subtitle that describes the trope (e.g. "An enemies to lovers romance").
7. Ensure the result is in English.

Example: "Icebreaker: A Novel (Maple Hills, #1)" -> "Icebreaker"
Example: "Trick Shot by Kayla Grosse" -> "Trick Shot"
Example: "Las reglas del juego" -> "The Cheat Sheet"

Return just the 1-5 words that make up the actual name of the book.
"""

async def extract_clean_title(content):
    try:
        response = model.generate_content(f"{CLEAN_TITLE_PROMPT}\n\nCONTENT:\n{content[:50000]}")
        return response.text.strip()
    except Exception as e:
        logger.error(f"Gemini Error: {e}")
        return None

async def restoration_worker(worker_id, browser, queue, df_ref, lock):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
    page = await context.new_page()
    
    try:
        while not queue.empty():
            idx = None
            try:
                idx = queue.get_nowait()
            except asyncio.QueueEmpty:
                break

            row = df_ref['df'].loc[idx]
            url = row['Goodreads Link']
            old_title = row['Book Name']

            if pd.isna(url) or "goodreads.com" not in str(url):
                queue.task_done()
                continue

            logger.info(f"[Worker {worker_id}] Cleaning: {old_title}")
            
            try:
                await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                h1_el = await page.query_selector("h1[data-testid='bookTitle']")
                raw_title = await h1_el.inner_text() if h1_el else ""
                
                # ALWAYS clean via Gemini if it looks messy or h1 is empty
                clean_title = await extract_clean_title(f"H1: {raw_title}\n\nCONTENT: " + await page.content())

                if clean_title:
                    # Final sanity strip for common symbols
                    clean_title = clean_title.split(': ')[0] # Remove post-colon subtitles if Gemini missed
                    clean_title = re.sub(r'\(.*?\)', '', clean_title).strip()
                    clean_title = re.sub(r'\[.*?\]', '', clean_title).strip()
                    
                    async with lock:
                        df_ref['df'].at[idx, 'Book Name'] = clean_title
                        df_ref['df'].to_csv(OUTPUT_FILE, index=False)
                    logger.success(f"  [Worker {worker_id}] ✓ {old_title} -> {clean_title}")
                
            except Exception as e:
                logger.error(f"  [Worker {worker_id}] Error cleaning {old_title}: {e}")
            
            queue.task_done()
            await asyncio.sleep(random.uniform(0.5, 2))
            
    finally:
        await context.close()

async def main():
    if not os.path.exists(INPUT_FILE):
        return
        
    df = pd.read_csv(INPUT_FILE)
    
    # Identify messy titles
    # Pattern includes brackets, hashtags, colons (often used for subtitles), "by", "Edition"
    pattern = r'\(|\[|#|\:|By\s+|Book\s+\d|Edition|Translated|Traducción'
    messy_mask = df['Book Name'].str.contains(pattern, na=False, case=False)
    
    # Also check if Author name is in the Book Name
    def has_author_or_series(row):
        book = str(row['Book Name']).lower()
        author = str(row['Author Name']).lower()
        if author in book: return True
        return False
    
    messy_mask = messy_mask | df.apply(has_author_or_series, axis=1)
    
    df_messy = df[messy_mask].copy()
    logger.info(f"Targeting {len(df_messy)} titles for restoration.")

    df_ref = {'df': df}
    lock = asyncio.Lock()
    queue = asyncio.Queue()
    for idx in df_messy.index:
        await queue.put(idx)

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        workers = [restoration_worker(i, browser, queue, df_ref, lock) for i in range(NUM_WORKERS)]
        await asyncio.gather(*workers)
        await browser.close()
    
    logger.success("Title Restoration Complete.")

if __name__ == "__main__":
    asyncio.run(main())
