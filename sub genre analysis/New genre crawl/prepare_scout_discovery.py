#!/usr/bin/env python3
"""
PREPARE SCOUT DISCOVERY - Phase 4
Converts missing Gemini scout series into raw discovery format.
"""

import pandas as pd
import os

def prepare():
    scout_path = '/Users/pocketfm/Documents/book-research-tool/New genre crawl/Political Drama_Romance_missing_scout.csv'
    if not os.path.exists(scout_path):
        print("Scout file not found.")
        return
        
    df = pd.read_csv(scout_path)
    print(f"Loaded {len(df)} missing series from scout.")
    
    # Map to standard discovery format
    discovery_df = pd.DataFrame({
        'Book Name': df['series_name'] + ' Book 1', # Add Book 1 to help search find the first book
        'Author Name': df['author'],
        'Amazon Link': '',
        'Series Name': df['series_name'],
        'Source': 'Gemini Scout',
        'Source Detail': 'Top 100 Missing Series',
        'Subgenre': 'Political Drama/Romance'
    })
    
    out_path = '/Users/pocketfm/Documents/book-research-tool/New genre crawl/Political Drama_Romance_scout_discovery.csv'
    discovery_df.to_csv(out_path, index=False)
    print(f"Created {out_path} with {len(discovery_df)} rows. Ready for backfill parallel.")

if __name__ == '__main__':
    prepare()
