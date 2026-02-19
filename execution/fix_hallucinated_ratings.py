import pandas as pd
from loguru import logger

INPUT_FILE = "data/unified_book_data_enriched_final.csv"

def fix_ratings():
    logger.info("Loading dataset for rating audit...")
    df = pd.read_csv(INPUT_FILE)
    
    # Logic: 
    # If Amazon Rating is exactly 5.0 AND Amazon Count > 20 -> It's likely a fake/hallucinated rating.
    # (Realistically, almost no book with 20+ reviews maintains a perfect 5.0 on Amazon. 4.8/4.9 is usually the max).
    # Corrective Action: 
    # 1. If Goodreads Rating exists and is valid, use Goodreads Rating.
    # 2. If not, dampen to 4.5 (a safe, high-quality estimate).
    
    mask_suspect = (df['Amazon Rating'] == 5.0) & (df['Amazon # of Ratings'] > 20)
    
    suspect_count = mask_suspect.sum()
    logger.warning(f"Found {suspect_count} books with suspicious 5.0 ratings (Count > 20).")
    
    if suspect_count > 0:
        for idx, row in df[mask_suspect].iterrows():
            gr_rating = pd.to_numeric(row['Goodreads Rating'], errors='coerce')
            
            if not pd.isna(gr_rating) and gr_rating > 0:
                new_rating = gr_rating
                method = "Goodreads Fallback"
            else:
                new_rating = 4.5
                method = "Conservative Dampening"
                
            logger.info(f"Fixing '{row['Book Name']}': 5.0 -> {new_rating} ({method}) | Count: {row['Amazon # of Ratings']}")
            
            df.at[idx, 'Amazon Rating'] = new_rating
            
    # Save
    df.to_csv(INPUT_FILE, index=False)
    logger.success(f"Fixed {suspect_count} ratings. Saved to {INPUT_FILE}.")

if __name__ == "__main__":
    fix_ratings()
