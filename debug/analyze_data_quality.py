#!/usr/bin/env python3
"""
Data Quality Analysis Script
Analyzes the current state of the unified book data to identify gaps and issues
"""
import pandas as pd
import numpy as np
import sys
import os

# Add parent to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

def analyze_data_quality(file_path):
    """Comprehensive data quality analysis"""
    print(f"\n{'='*80}")
    print(f"DATA QUALITY ANALYSIS: {os.path.basename(file_path)}")
    print(f"{'='*80}\n")
    
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"ERROR: Could not load file: {e}")
        return
    
    total_rows = len(df)
    print(f"📊 Total Rows: {total_rows}\n")
    
    # 1. Missing Data Summary
    print("="*80)
    print("MISSING DATA SUMMARY")
    print("="*80)
    for col in df.columns:
        missing_count = df[col].isna().sum()
        missing_pct = (missing_count / total_rows) * 100
        present_count = total_rows - missing_count
        print(f"{col:35s}: {present_count:5d} present | {missing_count:5d} missing ({missing_pct:5.1f}%)")
    
    # 2. Critical Fields Analysis
    print(f"\n{'='*80}")
    print("CRITICAL FIELDS COMPLETENESS")
    print("="*80)
    
    critical_fields = {
        'Series Name': df['Series Name'].notna().sum(),
        'Author Name': df['Author Name'].notna().sum(),
        'Book Name': df['Book Name'].notna().sum(),
        'Goodreads Link': df['Goodreads Link'].notna().sum(),
        'Amazon Link': df['Amazon Link'].notna().sum(),
    }
    
    for field, count in critical_fields.items():
        pct = (count / total_rows) * 100
        status = "✓" if pct > 90 else "⚠" if pct > 75 else "✗"
        print(f"{status} {field:35s}: {count:5d}/{total_rows} ({pct:5.1f}%)")
    
    # 3. Title Normalization Issues
    print(f"\n{'='*80}")
    print("TITLE NORMALIZATION ANALYSIS")
    print("="*80)
    
    # Check for special characters
    special_chars_pattern = r'[^\w\s-]'
    titles_with_special = df['Book Name'].str.contains(special_chars_pattern, na=False, regex=True).sum()
    print(f"Titles with special characters: {titles_with_special} ({titles_with_special/total_rows*100:.1f}%)")
    
    # Check for subtitle patterns
    titles_with_colon = df['Book Name'].str.contains(':', na=False).sum()
    titles_with_parens = df['Book Name'].str.contains(r'\(.*\)', na=False, regex=True).sum()
    print(f"Titles with colons (:): {titles_with_colon}")
    print(f"Titles with parentheses: {titles_with_parens}")
    
    # 4. Series Matching Issues
    print(f"\n{'='*80}")
    print("SERIES DATA QUALITY")
    print("="*80)
    
    unique_series = df['Series Name'].nunique()
    series_with_books = df.groupby('Series Name').size()
    print(f"Unique series: {unique_series}")
    print(f"Average books per series: {series_with_books.mean():.1f}")
    print(f"Series with only 1 book: {(series_with_books == 1).sum()}")
    
    # Books missing series but have series indicators
    likely_series_books = df[
        (df['Series Name'].isna()) & 
        (df['Book Name'].str.contains(r'Book \d+|#\d+|\(\d+\)', na=False, regex=True))
    ]
    print(f"Books likely in series but missing Series Name: {len(likely_series_books)}")
    
    # 5. Rating Data Quality
    print(f"\n{'='*80}")
    print("RATING DATA QUALITY")
    print("="*80)
    
    # Check for zero ratings
    gr_zero_ratings = ((df['Goodreads Rating'] == 0) | (df['Goodreads Rating'].isna())).sum()
    amz_zero_ratings = ((df['Amazon Rating'] == 0) | (df['Amazon Rating'].isna())).sum()
    
    print(f"Goodreads Rating missing/zero: {gr_zero_ratings} ({gr_zero_ratings/total_rows*100:.1f}%)")
    print(f"Amazon Rating missing/zero: {amz_zero_ratings} ({amz_zero_ratings/total_rows*100:.1f}%)")
    
    # 6. Potential Duplicates
    print(f"\n{'='*80}")
    print("DUPLICATE DETECTION")
    print("="*80)
    
    # Normalize titles for duplicate detection
    df['normalized_title'] = df['Book Name'].str.lower().str.replace(r'[^\w\s]', '', regex=True).str.strip()
    
    duplicate_titles = df.groupby(['normalized_title', 'Author Name']).size()
    duplicates = duplicate_titles[duplicate_titles > 1]
    
    print(f"Potential duplicate book entries: {len(duplicates)} ({len(duplicates)*2/total_rows*100:.1f}% affected)")
    
    if len(duplicates) > 0:
        print("\nSample duplicates:")
        for (title, author), count in duplicates.head(5).items():
            print(f"  - '{title}' by {author}: {count} entries")
    
    # 7. Data Enrichment Gaps
    print(f"\n{'='*80}")
    print("ENRICHMENT GAPS (Books needing attention)")
    print("="*80)
    
    gaps = {
        'Missing Author': (df['Author Name'].isna() | (df['Author Name'] == '')).sum(),
        'Missing GR Link': (df['Goodreads Link'].isna() | (df['Goodreads Link'] == '')).sum(),
        'Missing AMZ Link': (df['Amazon Link'].isna() | (df['Amazon Link'] == '')).sum(),
        'Missing GR Rating': ((df['Goodreads Rating'].isna()) | (df['Goodreads Rating'] == 0)).sum(),
        'Missing AMZ Rating': ((df['Amazon Rating'].isna()) | (df['Amazon Rating'] == 0)).sum(),
        'Missing Description': (df['Description'].isna() | (df['Description'] == '')).sum(),
        'Missing Pages': (df['Pages'].isna() | (df['Pages'] == 0)).sum(),
        'Missing Publisher': (df['Publisher'].isna() | (df['Publisher'] == '')).sum(),
    }
    
    for gap_type, count in sorted(gaps.items(), key=lambda x: x[1], reverse=True):
        pct = (count / total_rows) * 100
        urgency = "🔴" if pct > 50 else "🟡" if pct > 25 else "🟢"
        print(f"{urgency} {gap_type:30s}: {count:5d} books ({pct:5.1f}%)")
    
    print(f"\n{'='*80}")
    print("ANALYSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    file_path = "data/unified_book_data_enriched_aligned.csv"
    if len(sys.argv) > 1:
        file_path = sys.argv[1]
    
    analyze_data_quality(file_path)
