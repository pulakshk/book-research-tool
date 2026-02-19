import pandas as pd
from loguru import logger
import re

# Config
MASTER_FILE = "data/unified_book_data_enriched_final.csv"
HOCKEY_FILE = "hockey romance az t100.csv"
SPORTS_FILE = "Sports Romance az t100.csv"

def normalize_text(text):
    if pd.isna(text): return ""
    return str(text).lower().strip().replace("'", "").replace("’", "").replace(":", "").replace("-", "")

def main():
    logger.info("Loading datasets...")
    df = pd.read_csv(MASTER_FILE)
    
    try:
        hockey_df = pd.read_csv(HOCKEY_FILE)
        sports_df = pd.read_csv(SPORTS_FILE)
    except Exception as e:
        logger.error(f"Failed to load rank files: {e}")
        return

    # Create mapping dictionaries: (Normalized Title, Normalized Author) -> Best Rank Info
    # We want the HIGHEST rank (lowest number).
    
    rank_map = {} 
    
    # Process Hockey
    for _, row in hockey_df.iterrows():
        title = normalize_text(row['Book Name'])
        author = normalize_text(row['Author Name'])
        rank = int(row['Rank'])
        
        key = (title, author)
        
        # Store if better
        if key not in rank_map or rank < rank_map[key]['rank']:
            rank_map[key] = {
                'rank': rank,
                'category': 'Hockey Romance',
                'raw_title': row['Book Name']
            }
            
    # Process Sports (Priority? If book is in both, usually Hockey rank is better/more specific? 
    # Or just take absolute lowest number? User said "take the ranking of the highest book". 
    # Usually #1 is better than #10. So purely numeric min.)
    for _, row in sports_df.iterrows():
        title = normalize_text(row['Book Name'])
        author = normalize_text(row['Author Name'])
        rank = int(row['Rank'])
        
        key = (title, author)
        
        # Logic: If existing rank is 12 (Hockey) and new is 1 (Sports), take 1.
        if key not in rank_map or rank < rank_map[key]['rank']:
            rank_map[key] = {
                'rank': rank,
                'category': 'Sports Romance',
                'raw_title': row['Book Name']
            }
            
    logger.info(f"Loaded {len(rank_map)} unique consolidated top rankings.")
    
    # Apply to Master Dataset
    updates = 0
    
    # Create normalized columns for matching
    df['search_title'] = df['Book Name'].apply(normalize_text)
    df['search_author'] = df['Author Name'].apply(normalize_text)
    
    for idx, row in df.iterrows():
        key = (row['search_title'], row['search_author'])
        
        # Direct Match
        match = rank_map.get(key)
        
        # Fuzzy Fallback (Title only if author matches strongly?)
        # For now, strict match + tight normalization should catch most.
        
        if match:
            # Format: "Book Name: #Rank in Category"
            # Note: This overwrites previous scraped Featured List data which might be old.
            # Ideally we Append or Replace? User said "populate featured list column". 
            # Given these are "Live" ranks, they are higher quality. Replace.
            
            new_val = f"{match['raw_title']}: #{match['rank']} in {match['category']}"
            df.at[idx, 'Featured List'] = new_val
            updates += 1
            
    # Cleanup
    df.drop(columns=['search_title', 'search_author'], inplace=True)
    
    logger.success(f"Updated {updates} books with Top 100 rankings.")
    df.to_csv(MASTER_FILE, index=False)

if __name__ == "__main__":
    main()
