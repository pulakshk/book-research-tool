#!/usr/bin/env python3
"""
Phase 1: Consolidation & Cleanup
Merges amazon_delta.csv into unified_book_data_enriched_ultra.csv 
and performs robust deduplication followed by standardization.
"""
import pandas as pd
import os
import re

MASTER_FILE = "data/unified_book_data_enriched_ultra.csv"
DELTA_FILE = "amazon_delta.csv"
OUTPUT_FILE = "data/unified_book_data_enriched_ultra.csv"

def normalize_text(text):
    if not isinstance(text, str): return ""
    return re.sub(r'[^a-z0-9]', '', text.lower())

def consolidate():
    print("Loading datasets...")
    if not os.path.exists(MASTER_FILE):
        print(f"Error: {MASTER_FILE} not found.")
        return
    
    df_master = pd.read_csv(MASTER_FILE)
    
    if os.path.exists(DELTA_FILE):
        print(f"Merging delta from {DELTA_FILE}...")
        df_delta = pd.read_csv(DELTA_FILE)
        
        # Merge on Amazon Link, Author, or Title fallback
        # In our case, the delta has the exact indices or link
        for _, row in df_delta.iterrows():
            # Try to match by Amazon Link first
            link = row.get('Amazon Link')
            if pd.notna(link):
                mask = df_master['Amazon Link'] == link
                if mask.any():
                    idx = df_master[mask].index[0]
                    for col in df_delta.columns:
                        if pd.notna(row[col]) and col in df_master.columns:
                            # Update only if master is empty or placeholder
                            val = str(df_master.at[idx, col]).lower()
                            if pd.isna(df_master.at[idx, col]) or "amazon digital" in val or "nan" == val or "" == val:
                                df_master.at[idx, col] = row[col]
            else:
                # Fallback to Title + Author normalized match
                n_title = normalize_text(row.get('Book Name'))
                n_author = normalize_text(row.get('Author Name'))
                if n_title and n_author:
                    mask = (df_master['Book Name'].apply(normalize_text) == n_title) & \
                           (df_master['Author Name'].apply(normalize_text) == n_author)
                    if mask.any():
                        idx = df_master[mask].index[0]
                        for col in df_delta.columns:
                            if pd.notna(row[col]) and col in df_master.columns:
                                val = str(df_master.at[idx, col]).lower()
                                if pd.isna(df_master.at[idx, col]) or "amazon digital" in val or "nan" == val:
                                    df_master.at[idx, col] = row[col]
    
    print(f"Total rows before deduplication: {len(df_master)}")
    
    # Robust Deduplication
    # Create a normalized key for matching
    df_master['norm_title'] = df_master['Book Name'].apply(normalize_text)
    df_master['norm_author'] = df_master['Author Name'].apply(normalize_text)
    
    # Sort to keep the row with the most non-null/non-placeholder data
    # We count non-null values in each row
    df_master['data_count'] = df_master.notna().sum(axis=1)
    
    # Specifically penalize rows with "Amazon Digital" in Publisher
    mask_placeholder = df_master['Publisher'].astype(str).str.lower().str.contains('amazon digital', na=False)
    df_master.loc[mask_placeholder, 'data_count'] -= 10
    
    df_master = df_master.sort_values(by='data_count', ascending=False)
    df_master = df_master.drop_duplicates(subset=['norm_title', 'norm_author'], keep='first')
    
    # Clean up temp columns
    df_master = df_master.drop(columns=['norm_title', 'norm_author', 'data_count'])
    
    # Final Standardization
    if 'Publisher' in df_master.columns:
        df_master['Publisher'] = df_master['Publisher'].replace({
            'Amazon Digital Services LLC': 'Independently published',
            'Amazon Digital Services': 'Independently published',
            'nan': pd.NA,
            'None': pd.NA
        })
    
    print(f"Total rows after deduplication: {len(df_master)}")
    df_master.to_csv(OUTPUT_FILE, index=False)
    print(f"Consolidated data saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    consolidate()
