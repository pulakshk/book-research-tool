#!/usr/bin/env python3
import asyncio
import os
import sys
import json
import pandas as pd
from loguru import logger
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))
import google.generativeai as genai

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY")
genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel("gemini-2.5-flash")

SUBGENRES = [
    "Historic Fiction & Romance",
    "Military Drama/Romance",
    "Political Drama/Romance",
    "Small Town Drama/Romance",
    "Christian Drama/Romance",
    "Mafia Drama/Romance",
    "Dark Romance",
    "Forbidden Romance",
    "Romantic Suspense / Psychological Thriller"
]

async def ask_gemini_for_series(subgenre):
    logger.info(f"Asking Gemini for top 25 {subgenre} series...")
    prompt = f"""
    You are an expert Romance and Fiction literary agent.
    Identify the TOP 25 most famous, commercially successful, or critically acclaimed book series in the "{subgenre}" genre.
    CRITICAL:
    1. Only include series that have at least 3 books.
    2. Provide the specific titles of every book in the series.
    
    Return the response ONLY as a valid JSON array of objects. 
    Format:
    [
      {{
         "series_name": "Series Title",
         "author": "Author Name",
         "books": ["Book 1 Title", "Book 2 Title", "Book 3 Title"]
      }}
    ]
    """
    
    try:
        # Wrap the API call in asyncio wait_for to prevent infinite hanging
        # Use run_in_executor since generate_content is synchronous
        loop = asyncio.get_event_loop()
        response = await asyncio.wait_for(
            loop.run_in_executor(None, model.generate_content, prompt), 
            timeout=120.0
        )
        
        text = response.text
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
            
        res = json.loads(text)
        logger.success(f"Successfully got {len(res)} series for {subgenre}")
        return res
    except Exception as e:
        logger.error(f"Gemini failed for {subgenre}: {e}")
        return []

async def main():
    all_books = []
    
    # Run all subgenres concurrently
    tasks = [ask_gemini_for_series(sg) for sg in SUBGENRES]
    results = await asyncio.gather(*tasks)
    
    for sg, series_list in zip(SUBGENRES, results):
        for s in series_list:
            s_name = s.get('series_name', '')
            author = s.get('author', '')
            books = s.get('books', [])
            
            for b in books:
                all_books.append({
                    'Subgenre': sg,
                    'Series Name': s_name,
                    'Author Name': author,
                    'Book Name': b
                })
    
    if all_books:
        df = pd.DataFrame(all_books)
        out_path = os.path.join(os.path.dirname(__file__), 'All_9_Subgenres_Scout_Top25.csv')
        df.to_csv(out_path, index=False)
        logger.success(f"Saved {len(df)} total scouted books to {out_path}")
        
if __name__ == '__main__':
    asyncio.run(main())
