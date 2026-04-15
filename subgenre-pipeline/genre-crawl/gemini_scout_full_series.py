#!/usr/bin/env python3
"""
GEMINI SCOUTING FULL SERIES
Asks Gemini for top 100 political drama/romance series (3+ books) AND all individual book titles.
Diffs against our dataset and creates a raw discovery CSV for the missing books.
"""

import os
import json
import pandas as pd
from loguru import logger
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get("GEMINI_API_KEY")

def scout_full_series(genre_name="Political Drama/Romance"):
    logger.info(f"Asking Gemini for Top 100 {genre_name} series and their book titles...")
    
    prompt = f"""
    You are an expert book curator and researcher.
    I need an exhaustive list of the Top 100 most popular and culturally relevant book series in the '{genre_name}' subgenre.
    
    CRITERIAL:
    1. Must be a series with at least 3 books.
    2. Categories are often called "Political Romance", "Political Thriller Romance", or "Political Drama".
    3. Return exactly 100 series if possible.
    
    Format the output as a clean, valid JSON array of objects.
    Each object must have exactly three keys:
    - "series_name": The name of the series (String)
    - "author": The primary author (String)
    - "books": An array of strings, where each string is the title of a single book in the series, in reading order.
    
    Output ONLY the JSON array safely enclosed in brackets []. No markdown wrappers like ```json.
    """
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={API_KEY}"
    headers = {'Content-Type': 'application/json'}
    payload = {
        "contents": [{"parts":[{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2, # Low temp for factual consistency
            "response_mime_type": "application/json",
        }
    }
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        data = resp.json()
        if 'candidates' not in data:
            logger.error(f"Error from Gemini API: {data}")
            return []
            
        text = data['candidates'][0]['content']['parts'][0]['text']
        
        try:
            result = json.loads(text)
            logger.info(f"Gemini returned {len(result)} series.")
            return result
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON: {e}")
            logger.debug(f"Raw text: {text[:500]}...")
            return []
    except Exception as e:
        logger.error(f"Request failed: {e}")
        return []

def main():
    # We will diff against the already processed combined raw to avoid duplicates
    csv_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_combined_raw.csv'
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
    else:
        df = pd.read_csv('/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_enriched.csv')
    
    existing_authors = set(df['Author Name'].dropna().str.lower().str.strip())
    existing_series = set(df['Series Name'].dropna().str.lower().str.strip())
    
    # Let's also track existing book names just in case
    existing_books = set(df['Book Name'].dropna().str.lower().str.strip())
    
    scout_data = scout_full_series()
    if not scout_data:
        logger.error("No data returned from Gemini.")
        return
        
    missing_books_rows = []
    
    for item in scout_data:
        author = item.get('author', '').strip()
        series = item.get('series_name', '').strip()
        books = item.get('books', [])
        
        if not author or not series or not books: continue
        
        author_lower = author.lower()
        series_lower = series.lower()
        
        # We will add books that are not already in the dataset
        for i, book_title in enumerate(books):
            book_lower = book_title.lower().strip()
            
            # Check if book is already in our existing dataset
            book_exists = False
            for ex_b in existing_books:
                if book_lower == ex_b or book_lower in ex_b or ex_b in book_lower:
                    if len(book_lower) > 5 and len(ex_b) > 5:
                        book_exists = True
                        break
            
            if not book_exists:
                missing_books_rows.append({
                    'Book Name': book_title,
                    'Author Name': author,
                    'Amazon Link': '',
                    'Series Name': series,
                    'Source': 'Gemini Series Scout',
                    'Source Detail': 'Top 100 Long Series Expansion',
                    'Subgenre': 'Political Drama/Romance'
                })
            
    logger.info(f"Identified {len(missing_books_rows)} individual missing books from the scouted series.")
    
    if missing_books_rows:
        out_df = pd.DataFrame(missing_books_rows)
        out_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_full_scout_discovery.csv'
        out_df.to_csv(out_path, index=False)
        logger.success(f"Saved missing books to {out_path}")
        
if __name__ == "__main__":
    main()
