
import pandas as pd
import os
import sys
from loguru import logger

# Add root to sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.text_normalizer import normalize_title, normalize_author

MAIN_FILE = "unified_book_data_enriched_ultra.csv"
VALIDATED_FILE = "recovered_links_validated.csv"
OUTPUT_FILE = "unified_book_data_enriched_ultra.csv"

def get_key(title, author):
    # Standardize for matching
    t = normalize_title(str(title))['standard']
    a = normalize_author(str(author))
    return f"{t}|{a}".lower()

def robust_merge():
    if not os.path.exists(MAIN_FILE) or not os.path.exists(VALIDATED_FILE):
        logger.error("Required files missing for merge.")
        return

    # 1. Load data
    df_main = pd.read_csv(MAIN_FILE)
    df_val = pd.read_csv(VALIDATED_FILE)
    
    logger.info(f"Merging {len(df_val)} validated links into {len(df_main)} books.")

    # 2. Build lookup for validated data
    # We now capture ALL validated resources
    val_lookup = {}
    for idx, row in df_val.iterrows():
        key = get_key(row.get('Title', ''), row.get('Author Name', ''))
        
        val_lookup[key] = {
            'amz_link': str(row.get('Amazon Link', '')),
            'gr_link': str(row.get('Goodreads Link', '')),
            'author': str(row.get('Author Name', ''))
        }

    logger.info(f"Built lookup with {len(val_lookup)} validated books.")

    # 3. Update main dataframe
    update_count_amz = 0
    update_count_gr = 0
    update_count_author = 0
    
    for idx, row in df_main.iterrows():
        key = get_key(row.get('Book Name', ''), row.get('Author Name', ''))
        if key in val_lookup:
            data = val_lookup[key]
            
            # --- AUTHOR UPDATE ---
            if pd.isna(row.get('Author Name')) or str(row.get('Author Name')).lower() in ["nan", "", "unknown"]:
                if data['author'] and data['author'].lower() not in ["nan", "", "unknown"]:
                    df_main.at[idx, 'Author Name'] = data['author']
                    update_count_author += 1

            # --- AMAZON LINK UPDATE ---
            current_amz = str(row.get('Amazon Link', ''))
            # Resolve if missing or a search link
            if "/dp/" not in current_amz and "/gp/product/" not in current_amz:
                new_amz = data['amz_link']
                if "/dp/" in new_amz or "/gp/product/" in new_amz:
                    df_main.at[idx, 'Amazon Link'] = new_amz
                    update_count_amz += 1
            
            # --- GOODREADS LINK UPDATE ---
            current_gr = str(row.get('Goodreads Link', ''))
            if pd.isna(row.get('Goodreads Link')) or current_gr.lower() in ["nan", ""] or "goodreads.com" not in current_gr:
                new_gr = data['gr_link']
                if "goodreads.com" in new_gr:
                    df_main.at[idx, 'Goodreads Link'] = new_gr
                    update_count_gr += 1

    # 4. Final Audit
    total_books = len(df_main)
    dp_links = df_main['Amazon Link'].astype(str).str.contains('/dp/|/gp/product/', regex=True).sum()
    gr_links = df_main['Goodreads Link'].astype(str).str.contains('goodreads.com', regex=False).sum()
    missing_amz = df_main['Amazon Link'].isna().sum()
    missing_gr = df_main['Goodreads Link'].isna().sum()

    logger.success(f"Robust Merge Complete:")
    logger.info(f" - Amazon Links Updated: {update_count_amz}")
    logger.info(f" - Goodreads Links Updated: {update_count_gr}")
    logger.info(f" - Authors Updated: {update_count_author}")
    
    logger.info(f"Final Data Status:")
    logger.info(f" - Total Books: {total_books}")
    logger.info(f" - Amazon DP Links: {dp_links} ({dp_links/total_books:.1%})")
    logger.info(f" - Goodreads Links: {gr_links} ({gr_links/total_books:.1%})")
    logger.info(f" - Missing Amazon: {missing_amz}")
    logger.info(f" - Missing Goodreads: {missing_gr}")

    # 5. Save
    df_main.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"Final dataset saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    robust_merge()
