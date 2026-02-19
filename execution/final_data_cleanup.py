import pandas as pd
import numpy as np
from loguru import logger
import re

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
AUDIT_FILE = "data_cleanup_audit.txt"

def main():
    logger.info(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    initial_count = len(df)
    audit_log = []
    
    # ============================================================
    # 1. FIX MISASSIGNED BOOKS TO CORRECT SERIES
    # ============================================================
    logger.info("Fixing misassigned books...")
    
    # The Pact and The Incident are Briar U books, not Off-Campus
    briar_u_books = ['The Pact', 'The Incident']
    mask = (df['Book Name'].isin(briar_u_books)) & (df['Author Name'] == 'Elle Kennedy')
    count = mask.sum()
    if count > 0:
        df.loc[mask, 'Series Name'] = 'Briar U'
        audit_log.append(f"Moved {count} books from Off-Campus to Briar U: {', '.join(briar_u_books)}")
        logger.info(f"Moved {count} books to Briar U")
    
    # ============================================================
    # 2. CONSOLIDATE SERIES NAME VARIANTS
    # ============================================================
    logger.info("Consolidating series name variants...")
    
    # Normalize all series names first: strip,Title Case, and remove trailing junk
    def normalize_series_name(name):
        if pd.isna(name): return name
        name = str(name).strip()
        # Remove trailing , # or similar junk
        name = re.sub(r'[,#\s]+$', '', name)
        # Force Title Case for consistency
        return name.title()

    df['Series Name'] = df['Series Name'].apply(normalize_series_name)

    # Common patterns to consolidate
    series_consolidations = {
        'Off-Campus Series': 'Off-Campus',
        'Jacksonville Rays Series': 'Jacksonville Rays',
        'Jacksonville Rays, #': 'Jacksonville Rays'
    }
    
    # Normalize keys in mapping too
    series_consolidations = {normalize_series_name(k): normalize_series_name(v) for k, v in series_consolidations.items()}

    for old_name, new_name in series_consolidations.items():
        mask = df['Series Name'] == old_name
        count = mask.sum()
        if count > 0:
            df.loc[mask, 'Series Name'] = new_name
            audit_log.append(f"Renamed '{old_name}' -> '{new_name}' ({count} books)")
            logger.info(f"Consolidated: {old_name} -> {new_name} ({count} books)")
    
    # Check for other obvious case duplicates
    series_counts = df['Series Name'].value_counts()
    logger.info(f"Unique series after normalization: {len(series_counts)}")
    # ============================================================
    # 3. EXTRACT SERIES NAME FROM TITLES IF MISSING
    # ============================================================
    logger.info("Extracting series names from titles where missing...")
    
    def extract_series_from_title(row):
        title = str(row['Book Name'])
        series = str(row['Series Name'])
        
        if series in ['nan', 'NO_SERIES', ''] or pd.isna(row['Series Name']):
            # Look for patterns like "Title (Series Name)" or "Title (Series Name #1)"
            match = re.search(r'\(([^#)]+)(?:\s+#\d+)?\)', title)
            if match:
                return match.group(1).strip().title()
        return series

    df['Series Name'] = df.apply(extract_series_from_title, axis=1)

    # ============================================================
    # 4. NORMALIZE BOOK TITLES (REMOVE SERIES SUFFIXES)
    # ============================================================
    logger.info("Normalizing book titles (removing series suffixes)...")
    
    def normalize_book_title(title):
        if pd.isna(title): return title
        title = str(title).strip()
        # Remove trailing series info in parentheses: "Book Name (Series #1)" -> "Book Name"
        # Also handle cases like "The Deal (Off-Campus" (unclosed paren)
        title = re.sub(r'\s*\([^)]+(\)|$)', '', title)
        # Remove trailing junk
        title = re.sub(r'[,#\s]+$', '', title)
        return title

    df['Book Name'] = df['Book Name'].apply(normalize_book_title)

    # ============================================================
    # 5. REMOVE COMPILATION/DUPLICATE BOOKS
    # ============================================================
    logger.info("Removing compilation and duplicate books...")
    
    # Patterns that indicate compilations or duplicates
    compilation_patterns = [
        r'complete.*series',
        r'la serie completa',
        r'series set',
        r'collection',
        r'boxed set',
        r'box set',
    ]
    
    # ... rest of removal logic ...
    removed_books = []
    for pattern in compilation_patterns:
        mask = df['Book Name'].str.contains(pattern, case=False, na=False, regex=True)
        removed = df[mask]['Book Name'].tolist()
        if removed:
            removed_books.extend(removed)
            df = df[~mask]
            audit_log.append(f"Removed {len(removed)} books matching pattern '{pattern}'")
            logger.info(f"Removed {len(removed)} compilation/duplicate books: {pattern}")
    
    # ============================================================
    # 6. REMOVE BOOKS BY WRONG AUTHORS IN SERIES
    # ============================================================
    logger.info("Removing misclassified books...")
    
    # For each series, find the mode author and remove books by other authors
    series_groups = df.groupby('Series Name')
    rows_to_remove = []
    
    for series_name, group in series_groups:
        if series_name in ['NO_SERIES', 'nan', '']:
            continue
            
        # Get mode author
        author_counts = group['Author Name'].value_counts()
        if len(author_counts) > 1:
            mode_author = author_counts.index[0]
            mode_count = author_counts.iloc[0]
            
            # If there are books by other authors (likely misclassified)
            other_authors = group[group['Author Name'] != mode_author]
            if not other_authors.empty:
                # Only remove if the mode author has significantly more books
                if mode_count >= 3:  # At least 3 books by main author
                    for idx, book in other_authors.iterrows():
                        rows_to_remove.append(idx)
                        audit_log.append(f"Removed '{book['Book Name']}' by {book['Author Name']} from '{series_name}' (main author: {mode_author})")
                        logger.info(f"Removed misclassified: '{book['Book Name']}' by {book['Author Name']} from {series_name}")
    
    if rows_to_remove:
        df = df.drop(rows_to_remove)
    
    # ============================================================
    # 7. DEDUPLICATE EXACT SAME BOOKS
    # ============================================================
    logger.info("Removing exact duplicates...")
    
    # Keep first occurrence of each unique (Series Name, Book Name, Author Name)
    before_dedup = len(df)
    df = df.drop_duplicates(subset=['Series Name', 'Book Name', 'Author Name'], keep='first')
    after_dedup = len(df)
    
    if before_dedup > after_dedup:
        removed_count = before_dedup - after_dedup
        audit_log.append(f"Removed {removed_count} exact duplicate books")
        logger.info(f"Removed {removed_count} exact duplicates")
    
    # ============================================================
    # 8. FINAL STATISTICS
    # ============================================================
    final_count = len(df)
    total_removed = initial_count - final_count
    
    audit_log.insert(0, f"=== DATA CLEANUP AUDIT ===")
    audit_log.insert(1, f"Initial book count: {initial_count}")
    audit_log.insert(2, f"Final book count: {final_count}")
    audit_log.insert(3, f"Total removed: {total_removed}")
    audit_log.insert(4, f"")
    
    # Save cleaned data
    logger.info(f"Saving cleaned data to {OUTPUT_FILE}...")
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Save audit log
    logger.info(f"Saving audit log to {AUDIT_FILE}...")
    with open(AUDIT_FILE, 'w') as f:
        f.write('\n'.join(audit_log))
    
    logger.success(f"Cleanup complete! Removed {total_removed} books. See {AUDIT_FILE} for details.")
    
    # Print summary
    print("\n" + "="*60)
    print("DATA CLEANUP SUMMARY")
    print("="*60)
    print(f"Initial books: {initial_count}")
    print(f"Final books: {final_count}")
    print(f"Removed: {total_removed}")
    print(f"\nAudit log saved to: {AUDIT_FILE}")
    print("="*60)

if __name__ == "__main__":
    main()
