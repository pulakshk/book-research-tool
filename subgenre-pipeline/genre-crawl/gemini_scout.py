#!/usr/bin/env python3
"""
GEMINI SCOUTING - Phase 4 (Direct API variant, using requests)
"""

import os
import json
import pandas as pd
from loguru import logger
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.environ.get("GEMINI_API_KEY")

def scout_series(genre_name="Political Drama/Romance"):
    logger.info(f"Asking Gemini for Top 100 {genre_name} series via REST API...")
    
    prompt = f"""
    You are an expert book curator and researcher.
    I need an exhaustive list of the Top 100 most popular and culturally relevant book series in the '{genre_name}' subgenre.
    
    CRITERIAL:
    1. Must be a series with at least 3 books.
    2. Categories are often called "Political Romance", "Political Thriller Romance", or "Political Drama".
    3. Return exactly 100 items if possible.
    
    Format the output as a clean, valid JSON array of objects.
    Each object must have exactly two keys:
    - "series_name": The name of the series (String)
    - "author": The primary author (String)
    
    Output ONLY the JSON array safely enclosed in brackets []. No markdown wrappers.
    """
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={API_KEY}"
    headers = {'Content-Type': 'application/json'}
    payload = {
        "contents": [{"parts":[{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.2,
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
    csv_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_enriched.csv'
    df = pd.read_csv(csv_path)
    
    existing_authors = set(df['Author Name'].dropna().str.lower().str.strip())
    existing_series = set(df['Series Name'].dropna().str.lower().str.strip())
    
    scout_data = scout_series()
    if not scout_data:
        logger.error("No data returned from Gemini.")
        return
        
    missing = []
    
    for item in scout_data:
        author = item.get('author', '').lower().strip()
        series = item.get('series_name', '').lower().strip()
        
        if not author or not series: continue
        
        # Fuzzy match
        author_exists = any(author in ex_a or ex_a in author for ex_a in existing_authors)
        series_exists = any(series in ex_s or ex_s in series for ex_s in existing_series if ex_s)
        
        # We also might check if the author name is reversed (last, first)
        parts = author.split()
        if len(parts) >= 2:
            reversed_author = f"{parts[-1]} {parts[0]}"
            if any(reversed_author in ex_a for ex_a in existing_authors):
                author_exists = True
        
        if not author_exists and not series_exists:
            missing.append(item)
            
    logger.info(f"Identified {len(missing)} missing series out of {len(scout_data)} suggested.")
    
    if missing:
        out_df = pd.DataFrame(missing)
        out_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_missing_scout.csv'
        out_df.to_csv(out_path, index=False)
        logger.success(f"Saved missing series to {out_path}")
        
if __name__ == "__main__":
    main()
