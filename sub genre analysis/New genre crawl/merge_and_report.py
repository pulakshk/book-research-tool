#!/usr/bin/env python3
import pandas as pd
import os
from loguru import logger

def main():
    base_dir = '/Users/pocketfm/Documents/book-research-tool/New genre crawl'
    file1 = os.path.join(base_dir, 'Political Drama_Romance_full_scout_discovery.csv')
    file2 = os.path.join(base_dir, 'Political Drama_Romance_series_exhaustion_discovery.csv')
    file3 = os.path.join(base_dir, 'Political Drama_Romance_enriched.csv')
    
    # 1. Load the original enriched data
    df_enriched = pd.read_csv(file3)
    logger.info(f"Original Enriched Base Data: {len(df_enriched)} rows")
    
    # 2. Load the two new datasets (with AMZ Links backfilled)
    df_scout = pd.read_csv(file1) if os.path.exists(file1) else pd.DataFrame()
    logger.info(f"Gemini Scouted Series Titles: {len(df_scout)} rows")
    
    df_exhaust = pd.read_csv(file2) if os.path.exists(file2) else pd.DataFrame()
    logger.info(f"Missing Standalone Series Titles: {len(df_exhaust)} rows")
    
    # 3. Concatenate the new data
    df_new = pd.concat([df_scout, df_exhaust], ignore_index=True)
    
    # Identify unique new titles that aren't already in the enriched dataset
    existing_titles = [str(x).lower().strip() for x in df_enriched['Book Name'].dropna()]
    
    unique_new = []
    for _, row in df_new.iterrows():
        title = str(row.get('Book Name', '')).lower().strip()
        if title not in existing_titles:
            unique_new.append(row)
            # Add to list so we don't add duplicates from within df_new
            existing_titles.append(title)
            
    df_unique_new = pd.DataFrame(unique_new)
    logger.info(f"Unique New Records to add: {len(df_unique_new)} rows")
    
    # 4. Merge together into a staging file
    df_merged = pd.concat([df_enriched, df_unique_new], ignore_index=True)
    out_path = os.path.join(base_dir, 'Political Drama_Romance_staging_merged.csv')
    df_merged.to_csv(out_path, index=False)
    logger.success(f"Successfully saved merged dataset to: {out_path}")
    
if __name__ == '__main__':
    main()
