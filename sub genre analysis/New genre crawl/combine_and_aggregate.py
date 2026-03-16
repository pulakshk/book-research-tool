#!/usr/bin/env python3
"""
COMBINE & AGGREGATE - Phase 4 Output
Merges the filtered main dataset and the enriched scout dataset,
then groups by series as per the project requirements.
"""

import pandas as pd
import os
import sys

# Re-use the aggregate logic
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from genre_aggregate import aggregate_subgenre

def main():
    base_file = '/Users/pocketfm/Documents/book-research-tool/New genre crawl/Political Drama_Romance_enriched_filtered.csv'
    scout_file = '/Users/pocketfm/Documents/book-research-tool/New genre crawl/Political Drama_Romance_enriched.csv'
    
    if not os.path.exists(base_file):
        print(f"Error: {base_file} not found.")
        return
        
    df_base = pd.read_csv(base_file)
    print(f"Loaded {len(df_base)} filtered base books.")
    
    if os.path.exists(scout_file):
        df_scout = pd.read_csv(scout_file)
        print(f"Loaded {len(df_scout)} enriched scout books.")
        
        # Combine
        df_combined = pd.concat([df_base, df_scout], ignore_index=True)
        print(f"Combined total: {len(df_combined)} books.")
    else:
        print(f"Warning: {scout_file} not found. Continuing with base only.")
        df_combined = df_base
        
    combined_raw_path = '/Users/pocketfm/Documents/book-research-tool/New genre crawl/Political Drama_Romance_combined_raw.csv'
    df_combined.to_csv(combined_raw_path, index=False)
    
    # Now run the final aggregation
    aggregate_subgenre(combined_raw_path, "Political Drama_Romance")
    print(f"Aggregation complete.")

if __name__ == '__main__':
    main()
