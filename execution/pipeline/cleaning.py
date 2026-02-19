import pandas as pd
import re
from loguru import logger

def filter_unrelated_content(df):
    """Remove rows based on banned genres, titles, or CJK characters."""
    initial_len = len(df)
    
    def is_valid_row(row):
        # strings
        subgenre = str(row.get('Primary Subgenre', '')).lower()
        series = str(row.get('Series Name', '')).lower()
        title = str(row.get('Book Name', '')).lower()
        
        # 0. CJK Character Filter (Chinese/Japanese/Korean)
        text_content = series + title
        if re.search(r'[\u4e00-\u9fff]', text_content):
            return False
        
        # 1. Strict Bans (Titles/Series)
        ban_titles = [
            'game of thrones', 'song of ice and fire', 'harry potter', 
            'captain marvel', 'marvel', 'avengers', 'star wars', 'dune', 
            'lord of the rings', 'hobbit', 'chinese', 'mandarin'
        ]
        for b in ban_titles:
            if b in series or b in title:
                return False
                
        # 2. Strict Bans (Subgenres)
        ban_genres = [
            'fantasy', 'sci-fi', 'science fiction', 'steampunk', 
            'biography', 'nonfiction', 'history', 'children', 'juvenile',
            'middle grade', 'young adult graphic', 'comic', 'manga'
        ]
        
        is_sports_hockey = 'hockey' in subgenre or 'sports' in subgenre or 'football' in subgenre
        
        for g in ban_genres:
            if g in subgenre:
                # Exempt if explicitly 'sports' or 'hockey'
                if not is_sports_hockey:
                    return False
        
        return True

    mask = df.apply(is_valid_row, axis=1)
    df_clean = df[mask].copy()
    
    removed = initial_len - len(df_clean)
    if removed > 0:
        logger.info(f"Cleaned {removed} unrelated rows (Fantasy, CJK, etc).")
    return df_clean

def normalize_text(text):
    if pd.isna(text): return ""
    return str(text).strip().lower().replace(" series", "")

def deduplicate_dataset(df):
    """Deduplicate based on Normalized Series + Book Number."""
    initial_len = len(df)
    
    # helper for sorting (prioritize rows with more info)
    df['completeness'] = df.count(axis=1)
    df = df.sort_values('completeness', ascending=False)
    
    # Create keys
    df['norm_series'] = df['Series Name'].apply(normalize_text)
    df['norm_book'] = df['Book Name'].apply(normalize_text)
    
    # Dedupe logic: Drop duplicates on (Series, BookName)
    # This is a simplification of the complex dedupe logic, but robust enough for the "compact" version.
    # The "Turbo" dedupe logic was complex. Let's start with safe subset.
    
    df_dedup = df.drop_duplicates(subset=['norm_series', 'norm_book'], keep='first')
    
    # Cleanup
    df_dedup = df_dedup.drop(columns=['completeness', 'norm_series', 'norm_book'])
    
    removed = initial_len - len(df_dedup)
    if removed > 0:
        logger.info(f"Deduplicated {removed} rows.")
        
    return df_dedup
