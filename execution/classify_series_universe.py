#!/usr/bin/env python3
import pandas as pd
import os
import json
import google.generativeai as genai
from loguru import logger
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure Gemini
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
if not GEMINI_API_KEY:
    logger.error("GEMINI_API_KEY not found in environment")
    exit(1)

genai.configure(api_key=GEMINI_API_KEY)
model = genai.GenerativeModel('gemini-2.0-flash')

INPUT_FILE = "Amazon Bestsellers _ Jan 2026 _ Hockey Romance - Cleaned Titles_ Sports & Hockey.csv"
OUTPUT_FILE = INPUT_FILE # Overwrite as requested

PROMPT_TEMPLATE = """
You are a literary analysis agent for pocketfm where you need to take the book series x author name and evaluate. Your task is to classify a book series into one of two categories based on character continuity.

Series Name: {series_name}
Author: {author}
Books in Series: {books_list}
Description/Analysis: {analysis}

Categories:
- "same universe-same couple": The main couple from the first book stays the same throughout the books (atleast 3 books).
- "same universe different couple": Each book focuses on a DIFFERENT main couple (very common in Romance series like Bridgerton or sports team series).

Respond with ONLY a JSON object:
{{
  "universe_type": "same universe-same couple" | "same universe different couple",
  "reasoning": "Brief explanation focused on whether the couple changes or stays the same"
}}
"""

def classify_series(series_name, author, books_list, analysis):
    prompt = PROMPT_TEMPLATE.format(
        series_name=series_name,
        author=author,
        books_list=books_list,
        analysis=analysis
    )
    
    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        
        # Extract JSON
        if "```json" in text:
            text = text.split("```json")[1].split("```")[0].strip()
        elif "```" in text:
            text = text.split("```")[1].split("```")[0].strip()
            
        return json.loads(text)
    except Exception as e:
        logger.error(f"Error classifying {series_name}: {e}")
        return {"universe_type": "unknown", "reasoning": str(e)}

def main():
    logger.info(f"Loading {INPUT_FILE}...")
    # Skip first row which is sub-header/metadata
    df = pd.read_csv(INPUT_FILE, header=1)
    
    # Filter for P0, P1, P2
    # mask = df['Commissioning_Rank'].isin(['P0', 'P1', 'P2'])
    # filtered_df = df[mask].copy()
    
    # Process all titles with a series
    filtered_df = df.copy()
    
    if filtered_df.empty:
        logger.warning("No P0, P1, or P2 titles found.")
        return

    # Initialize columns if they don't exist
    if 'Universe Type' not in df.columns:
        df['Universe Type'] = None
    if 'Universe Reasoning' not in df.columns:
        df['Universe Reasoning'] = None

    # Group by Series and Author
    groups = filtered_df.groupby(['Book Series Name', 'Author Name'])
    
    logger.info(f"Found {len(groups)} unique series-author combinations to classify.")
    
    results_map = {}
    
    for (series_name, author), group in groups:
        if pd.isna(series_name) or str(series_name).upper() == 'NO_SERIES' or str(series_name).strip() == '':
            continue
            
        logger.info(f"Classifying: {series_name} by {author}")
        
        books_list = group['Books_In_Series_List'].iloc[0] if 'Books_In_Series_List' in group.columns else ""
        analysis = group['Subjective Analysis'].iloc[0] if 'Subjective Analysis' in group.columns else ""
        
        result = classify_series(series_name, author, books_list, analysis)
        results_map[(series_name, author)] = result
        
        logger.success(f"  Result: {result['universe_type']}")

    # Apply results back to original dataframe
    for (series_name, author), result in results_map.items():
        row_mask = (df['Book Series Name'] == series_name) & (df['Author Name'] == author)
        df.loc[row_mask, 'Universe Type'] = result['universe_type']
        df.loc[row_mask, 'Universe Reasoning'] = result['reasoning']

    logger.info(f"Saving results to {OUTPUT_FILE}...")
    
    # Re-insert the first row (metadata/sub-header) if needed
    # Since we read with header=1, we lost the first line. 
    # Let's read the first line separately.
    with open(INPUT_FILE, 'r') as f:
        first_line = f.readline()
        
    with open(OUTPUT_FILE, 'w') as f:
        f.write(first_line)
        df.to_csv(f, index=False)
        
    logger.success("Done!")

if __name__ == "__main__":
    main()
