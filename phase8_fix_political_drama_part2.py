import os
import pandas as pd
from loguru import logger

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
NEW_GENRE_DIR = os.path.join(SCRIPT_DIR, 'sub genre analysis', 'New genre crawl')

import sys
sys.path.insert(0, NEW_GENRE_DIR)
import genre_aggregate

def map_bsr(book_name, ranks_df):
    if not isinstance(book_name, str) or not book_name.strip():
        return ""
        
    book = book_name.strip().lower()
    found_flags = set()
    
    mask = ranks_df['Book_Lower'].str.contains(book, regex=False, na=False)
    matches = ranks_df[mask]
    
    for _, match_row in matches.iterrows():
        rank = match_row['Rank']
        lname = match_row['List Name']
        
        if rank <= 10:
            flag = f"# Top 10 {lname}"
        elif rank <= 50:
            flag = f"# Top 50 {lname}"
        else:
            flag = f"# Top 100 {lname}"
            
        found_flags.add(flag)
        
    return " | ".join(sorted(list(found_flags)))

def main():
    csv_path = os.path.join(NEW_GENRE_DIR, 'Political Drama_Romance_enriched_filtered_v2.csv')
    ranks_csv = os.path.join(NEW_GENRE_DIR, 'Az_Bestsellers_Master_Ranks.csv')
    
    if not os.path.exists(csv_path):
        logger.error(f"Part 1 output not found: {csv_path}")
        return
        
    df = pd.read_csv(csv_path)
    
    if os.path.exists(ranks_csv):
        ranks_df = pd.read_csv(ranks_csv)
        ranks_df['Book_Lower'] = ranks_df['Book'].astype(str).str.lower()
        logger.info(f"Loaded {len(ranks_df)} Amazon Bestseller records for mapping.")
        
        # Apply Mapping
        for idx, row in df.iterrows():
            flags = map_bsr(row['Book Name'], ranks_df)
            if flags:
                existing = str(row['Source Detail']) if not pd.isna(row['Source Detail']) else ""
                df.at[idx, 'Source Detail'] = existing + " | " + flags
                
        # Save before aggregation
        df.to_csv(csv_path, index=False)
        logger.success("BSR Mapping injected into Source Detail.")
    else:
        logger.warning(f"Ranks file not found: {ranks_csv}")
        
    # Run Final Aggregation
    logger.info("Running final Series Aggregation on cleaned V2 dataset...")
    genre_aggregate.aggregate_subgenre(csv_path, 'Political Drama_Romance')
    
    logger.success("Part 2 Completed. Political Drama_Romance_final.csv is updated.")

if __name__ == '__main__':
    main()
