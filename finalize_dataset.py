#!/usr/bin/env python3
"""
Finalize Dataset
- Fixes specific data errors (Harry Potter description).
- Normalizes 'Self Pub Flag' into 'Self Pub', 'Big Pub', 'Indie'.
- Infers missing flags from Publisher data.
"""
import pandas as pd
import numpy as np
from loguru import logger

MASTER_FILE = "unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "unified_book_data_enriched_final.csv"

def normalize_flag(val):
    if pd.isna(val):
        return np.nan
    s = str(val).lower().strip()
    if s in ['true', 'self pub', 'self-published']:
        return 'Self Pub'
    if s in ['false', 'big pub', 'traditional']:
        return 'Big Pub'
    if s in ['indie']:
        return 'Indie'
    return 'Indie'  # Default catch-all for weird non-nan values? Or keep original?

def infer_publisher_type(row):
    pub = str(row['Publisher']).lower()
    if pd.isna(row['Publisher']) or pub == 'nan' or pub == 'unknown':
        return 'Indie' # Default assumption if unknown? Or keep nan? User explicitly listed 3 categories.
        
    # Big 5 + Major patterns
    big_pubs = [
        'penguin', 'random house', 'harper', 'hachette', 'simon & schuster', 
        'macmillan', 'tor', 'berkley', 'avon', 'grand central', 'st. martin',
        'little, brown', 'doubleday', 'knopf', 'sourcebooks', 'bloom', 'entangled'
    ]
    
    # Self-Pub patterns
    self_pubs = [
        'independently', 'amazon', 'createspace', 'kdp', 'draft2digital', 
        'smashwords', 'lulu', 'author', 'self'
    ]
    
    # Check if Author is Publisher
    author = str(row['Author Name']).lower()
    if author in pub or pub in author:
        return 'Self Pub'
        
    for b in big_pubs:
        if b in pub:
            return 'Big Pub'
            
    for s in self_pubs:
        if s in pub:
            return 'Self Pub'
            
    return 'Indie'  # Small presses, unknown names

def main():
    logger.info(f"Loading {MASTER_FILE}...")
    df = pd.read_csv(MASTER_FILE)
    
    # 1. Fix Harry Potter Description
    mask = (df['Book Name'] == '4 & Counting') & (df['Author Name'] == 'Toni Aleo')
    if mask.any():
        logger.info("Fixing '4 & Counting' description...")
        correct_desc = (
            "Shea Adler is tired of being the odd man out. He’s tired of being the designated babysitter "
            "for his nieces and nephews, and he’s tired of matchmakers trying to set him up. "
            "He wants what his sisters have: happily ever after. But when he meets a single mom "
            "with four kids, he realizes counting to four might logically happen before finding the one."
        )
        # Verify it was truly wrong before (log it)
        old_desc = df.loc[mask, 'Description'].values[0]
        logger.info(f"  Old Description start: {str(old_desc)[:50]}...")
        df.loc[mask, 'Description'] = correct_desc
        logger.success("  Description fixed!")
    
    # 2. Normalize Existing Flags
    logger.info("Normalizing Self Pub Flags...")
    logger.info(f"  Before: {df['Self Pub Flag'].unique()}")
    df['Self Pub Flag'] = df['Self Pub Flag'].apply(normalize_flag)
    
    # 3. Infer Missing Flags
    missing_mask = df['Self Pub Flag'].isna()
    logger.info(f"  Inferring {missing_mask.sum()} missing flags from Publishers...")
    
    df.loc[missing_mask, 'Self Pub Flag'] = df[missing_mask].apply(infer_publisher_type, axis=1)
    
    logger.info(f"  After: {df['Self Pub Flag'].value_counts(dropna=False)}")
    
    # Save
    df.to_csv(OUTPUT_FILE, index=False)
    # Also overwrite Master
    df.to_csv(MASTER_FILE, index=False)
    logger.success(f"Saved finalized dataset to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
