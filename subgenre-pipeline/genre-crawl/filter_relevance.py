#!/usr/bin/env python3
"""
FILTER RELEVANCE - Phase 4 (Sequential with delay for Rate limits)
"""

import os
import time
import json
import pandas as pd
from loguru import logger
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.environ.get("GEMINI_API_KEY")

def check_relevance_batch(batch):
    prompt = f"""
    You are an expert book curator. Review the following list of books.
    Determine if each book is RELEVANT to the fiction subgenre "Political Drama" or "Political Romance".
    Irrelevant books include: non-fiction, academic textbooks, spiritual quests without politics, pure sci-fi without political focus, etc.
    
    Books to review:
    {json.dumps([{ 'id': b['id'], 'title': b['Book Name'], 'trope': b['Primary Trope'], 'desc': str(b['Description'])[:400] } for b in batch], indent=2)}
    
    Return ONLY a valid JSON object mapping the exact string ID to a boolean (true if relevant, false if not). Example:
    {{ "0": true, "1": false }}
    """
    
    url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={API_KEY}"
    headers = {'Content-Type': 'application/json'}
    payload = {
        "contents": [{"parts":[{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.1,
            "response_mime_type": "application/json",
        }
    }
    
    try:
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        data = resp.json()
        if 'candidates' not in data:
            logger.error(f"API Error: {data}")
            return {}
        text = data['candidates'][0]['content']['parts'][0]['text']
        return json.loads(text)
    except Exception as e:
        logger.error(f"Batch failed: {e}")
        return {}

def main():
    csv_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_enriched.csv'
    df = pd.read_csv(csv_path)
    
    df['id'] = df.index.astype(str)
    records = df.to_dict('records')
    batch_size = 35
    batches = [records[i:i + batch_size] for i in range(0, len(records), batch_size)]
    
    logger.info(f"Filtering {len(records)} books in {len(batches)} batches.")
    
    results = {}
    for i, b in enumerate(batches):
        logger.info(f"Processing batch {i+1}/{len(batches)}...")
        res = check_relevance_batch(b)
        results.update(res)
        logger.info(f"  -> Batch {i+1} got {len(res)} results.")
        time.sleep(4) # Respect rate limits
            
    df['Is Relevant'] = df['id'].apply(lambda x: results.get(str(x), True))
    
    irrelevant_count = (~df['Is Relevant']).sum()
    logger.info(f"Identified {irrelevant_count} irrelevant books.")
    
    filtered_df = df[df['Is Relevant']].drop(columns=['id', 'Is Relevant'])
    filtered_df.to_csv('/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_enriched_filtered.csv', index=False)
    
    excluded_df = df[~df['Is Relevant']].drop(columns=['id', 'Is Relevant'])
    excluded_df.to_csv('/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/Political Drama_Romance_excluded.csv', index=False)
    
    logger.success("Filtering complete.")

if __name__ == "__main__":
    main()
