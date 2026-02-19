import asyncio
import pandas as pd
import random
import re
import json
import google.generativeai as genai
from loguru import logger
from playwright.async_api import async_playwright
from execution.pipeline.config import GEMINI_API_KEY, MAX_WORKERS

# Config
# MAX_WORKERS imported from config... but let's override for scraping? 
# Config has MAX_WORKERS=5 default, we want 15 for scraping.
MAX_WORKERS_SCRAPE = 15

if GEMINI_API_KEY:
    try:
        genai.configure(api_key=GEMINI_API_KEY)
        model = genai.GenerativeModel('gemini-flash-latest')
    except: model = None

# ----------------- PROMPTS -----------------
SUPER_EXTRACT_PROMPT = """
Extract ALL books from this Goodreads series hub HTML.
For each book, provide:
- book_name
- book_number
- book_url (absolute)
- pub_date
- rating
- rating_count

Return valid JSON: { "books": [ ... ] }
"""

# ----------------- HELPERS -----------------
async def safe_goto(page, url, retries=3):
    for attempt in range(retries):
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            return True
        except:
            await asyncio.sleep(random.uniform(1, 3))
    return False

# ----------------- WORKER -----------------
async def process_series(series_name, author, browser, sem):
    async with sem:
        page = await browser.new_page()
        try:
            # 1. Find Hub via Google
            q = f"site:goodreads.com/series {series_name} {author}"
            search_url = f"https://www.google.com/search?q={q.replace(' ', '+')}"
            
            if not await safe_goto(page, search_url): return []
            
            # Extract first series link
            try:
                # Naive selector for Google results
                link = await page.get_attribute("div.g a", "href")
            except: link = None
                
            if not link or "goodreads.com/series" not in link:
                return []
                
            # 2. Go to Hub
            if not await safe_goto(page, link): return []
            
            # 3. Extract HTML & Content
            content = await page.content()
            
            # 4. Gemini Extraction
            if not model: return []
            
            # Truncate HTML for token limits
            html_snippet = content[:30000] 
            
            prompt = SUPER_EXTRACT_PROMPT + f"\nHTML:\n{html_snippet}"
            response = await asyncio.to_thread(model.generate_content, prompt)
            text = response.text.strip().replace("```json", "").replace("```", "")
            data = json.loads(text)
            
            return data.get("books", [])
            
        except Exception as e:
            # logger.debug(f"Error scraping {series_name}: {e}")
            return []
        finally:
            await page.close()

# ----------------- PIPELINE -----------------
async def run_series_exhaustion_async(df):
    logger.info("Starting Gemini Series Exhaustion...")
    
    # Identify Series to check (Unique)
    series_list = df[['Series Name', 'Author Name']].drop_duplicates().to_dict('records')
    # Limit for demo/compactness? No, user wants scale.
    # But checking 600 series takes time.
    # We will check only those with potential gaps?
    # For now, simplistic approach: check all.
    
    sem = asyncio.Semaphore(MAX_WORKERS)
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        
        tasks = []
        for s in series_list:
            if not pd.isna(s['Series Name']):
                tasks.append(process_series(s['Series Name'], s['Author Name'], browser, sem))
            
        results = await asyncio.gather(*tasks)
        await browser.close()
        
    # Merge Logic (Simplified)
    new_books = []
    current_titles = set(df['Book Name'].str.lower().tolist())
    
    for res_list in results:
        for book in res_list:
            if book.get('book_name', '').lower() not in current_titles:
                new_books.append(book)
                
    if new_books:
        logger.success(f"Discovered {len(new_books)} new books!")
        new_df = pd.DataFrame(new_books)
        # Normalize columns to match Master
        new_df = new_df.rename(columns={
            'book_name': 'Book Name', 'book_url': 'Goodreads Link',
            'rating': 'Goodreads Rating', 'rating_count': 'Goodreads # of Ratings'
        })
        return pd.concat([df, new_df], ignore_index=True)
        
    logger.info("No new books found.")
    return df

def run_scraping_pipeline(df):
    return asyncio.run(run_series_exhaustion_async(df))
