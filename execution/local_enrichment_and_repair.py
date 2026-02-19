import pandas as pd
import numpy as np
from loguru import logger
from datetime import datetime

# Input Files
MAIN_FILE = "data/unified_book_data_enriched_ultra.csv"
SPORTS_T100 = "Sports Romance az t100.csv"
HOCKEY_T100 = "hockey romance az t100.csv"
OUTPUT_FILE = "series_commissioning_analysis.csv"

# Lara Bailey Missing Books (Scraped from Amazon)
LARA_BAILEY_BOOKS = [
    "Grumpy Billionaire’s Nanny: An Age Gap Romance (The Billionaire Rules Series)",
    "Hockey Star’s Unexpected Twins: A One Night Stand, Forced Proximity Romance (The Billionaire Rules Series)",
    "Forbidden Billionaire’s Obsession: An Off Limits Brother’s Best Friend Romance (The Billionaire Rules Series)",
    "Billionaire’s Second Chance: A Forced Proximity Single Dad Romance (The Billionaire Rules Series)"
]

def load_t100_map(file_path):
    """
    Loads T100 file. Expects columns for Title and Rank.
    Returns dict: {Normalized_Title: Rank}
    """
    try:
        df = pd.read_csv(file_path)
        # Identify columns
        # User audio implies: Rank, Book Name, Author Name.
        # Let's clean headers
        df.columns = [c.strip() for c in df.columns]
        
        # Heuristic to find Title/Rank cols
        title_col = next((c for c in df.columns if 'name' in c.lower() or 'title' in c.lower()), None)
        rank_col = next((c for c in df.columns if 'rank' in c.lower() or '#' in c), None)
        
        if not title_col or not rank_col:
            logger.error(f"Could not identify Title/Rank columns in {file_path}. Cols: {df.columns}")
            return {}
            
        mapping = {}
        for _, row in df.iterrows():
            t = str(row[title_col])
            # Normalize title for matching
            norm_t = "".join(c for c in t.lower() if c.isalnum())
            try:
                r = int(str(row[rank_col]).replace('#', '').strip())
                mapping[norm_t] = r
            except:
                continue
        return mapping
    except Exception as e:
        logger.error(f"Error loading {file_path}: {e}")
        return {}

def main():
    logger.info(f"Loading {MAIN_FILE}...")
    df = pd.read_csv(MAIN_FILE)
    
    # ==========================
    # 1. REPAIR LARA BAILEY
    # ==========================
    logger.info("Repairing Lara Bailey...")
    # Check if books exist
    current_lara = df[df['Author Name'] == 'Lara Bailey']
    existing_titles = [str(t).lower() for t in current_lara['Book Name'].tolist()]
    
    new_rows = []
    for title in LARA_BAILEY_BOOKS:
        norm = "".join(c for c in title.lower() if c.isalnum())
        # Check against normalized existing
        match = False
        for ex in existing_titles:
            if "".join(c for c in ex if c.isalnum()) in norm: # Loose match
                match = True
                break
        
        if not match:
            logger.info(f"Adding missing Lara Bailey book: {title}")
            new_row = {col: None for col in df.columns}
            new_row['Book Name'] = title
            new_row['Author Name'] = 'Lara Bailey'
            new_row['Series Name'] = 'The Billionaire Rules'
            new_row['Goodreads Rating'] = np.nan # Manual entry needed
            new_row['Goodreads # of Ratings'] = np.nan
            new_rows.append(new_row)
            
    if new_rows:
        df = pd.concat([df, pd.DataFrame(new_rows)], ignore_index=True)
            
    # ==========================
    # 2. MAP T100 LISTS
    # ==========================
    logger.info("Mapping T100 Ranks...")
    sports_map = load_t100_map(SPORTS_T100)
    hockey_map = load_t100_map(HOCKEY_T100)
    
    # New Columns
    df['Sports_Amazon_Rank'] = np.nan
    df['Hockey_Amazon_Rank'] = np.nan
    df['Featured_List'] = 'No List' # Default
    
    for idx, row in df.iterrows():
        title = str(row['Book Name'])
        norm = "".join(c for c in title.lower() if c.isalnum())
        
        s_rank = sports_map.get(norm)
        h_rank = hockey_map.get(norm)
        
        if s_rank:
            df.at[idx, 'Sports_Amazon_Rank'] = s_rank
        if h_rank:
            df.at[idx, 'Hockey_Amazon_Rank'] = h_rank
            
        # Determine Featured List
        # Logic: If in both, take 'Highest Title' (User: "title which is higher").
        # Ambiguous: Higher Rank (1 is higher than 10) or Higher Hierarchy (Sports vs Hockey).
        # Assuming Ranks are equal importance, prioritize the better rank (lower number).
        
        final_list = []
        
        # Categorize Sports
        if s_rank:
            if s_rank <= 10: tag = "Sports Amazon Top 10"
            elif s_rank <= 50: tag = "Sports Amazon Top 50"
            else: tag = "Sports Amazon Top 100"
            final_list.append((s_rank, tag))
            
        if h_rank:
            if h_rank <= 10: tag = "Hockey Amazon Top 10"
            elif h_rank <= 50: tag = "Hockey Amazon Top 50"
            else: tag = "Hockey Amazon Top 100"
            final_list.append((h_rank, tag))
            
        if final_list:
            # Sort by Rank (Ascending)
            final_list.sort(key=lambda x: x[0])
            # Set the best one
            df.at[idx, 'Featured_List'] = final_list[0][1]
            # Populate User's specific list columns if needed?
            # User said "map in the featured list column". Done.
    
    # ==========================
    # 3. SERIES ANALYSIS
    # ==========================
    logger.info("Calculating Series Metrics...")
    
    # Clean Dates (handle NaNs and partials)
    def parse_year(d):
        try:
            return pd.to_datetime(d).year
        except:
            return np.nan
            
    df['Pub_Year'] = df['Publication Date'].apply(parse_year)
    
    # Group by Series
    series_groups = df.groupby('Series Name')
    
    for series, group in series_groups:
        # First Book Pub Date
        years = group['Pub_Year'].dropna()
        if not years.empty:
            min_year = int(years.min())
            era = "After 2020" if min_year >= 2020 else "Before 2020"
        else:
            min_year = np.nan
            era = "Unknown"
            
        # Count Top 100
        # Check against featured list being != 'No List'
        t100_count = len(group[group['Featured_List'] != 'No List'])
        
        # Apply to all rows in series
        df.loc[group.index, 'First_Book_Pub_Year'] = min_year
        df.loc[group.index, 'Series_Era'] = era
        df.loc[group.index, 'Num_Books_In_Top_100'] = t100_count
        
    # ==========================
    # 4. SAVE
    # ==========================
    # Reorder columns as user might like (Featured List prominent)
    # Just save to output for now
    logger.info(f"Saving to {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    # Also update ultra
    df.to_csv(MAIN_FILE, index=False)
    logger.success("Process Complete.")

if __name__ == "__main__":
    main()
