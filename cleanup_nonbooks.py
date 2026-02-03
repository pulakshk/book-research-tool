#!/usr/bin/env python3
"""Remove non-books and prepare for final enrichment"""
import pandas as pd
import os
from datetime import datetime

# Load data
df = pd.read_csv('unified_book_data_enriched_ultra.csv')
initial_count = len(df)

print(f'Initial entries: {initial_count}')
print()

# Backup before cleanup
backup_file = f'backups/manual/backup_before_cleanup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
os.makedirs('backups/manual', exist_ok=True)
df.to_csv(backup_file, index=False)
print(f'✓ Backup created: {backup_file}')
print()

# Remove entries with NO AUTHOR (these are definitely bad data)
no_author = df['Author Name'].isna() | (df['Author Name'] == '') | (df['Author Name'] == 'nan')
removed_no_author = df[no_author].copy()

print(f'Removing {len(removed_no_author)} entries with NO AUTHOR:')
for idx, row in removed_no_author.head(20).iterrows():
    print(f'  - {row["Book Name"]}')
if len(removed_no_author) > 20:
    print(f'  ... and {len(removed_no_author) - 20} more')

df_clean = df[~no_author].copy()

print()
print(f'Removed: {len(removed_no_author)} entries')
print(f'Remaining: {len(df_clean)} entries')
print()

# Save cleaned data
removed_no_author.to_csv('removed_entries.csv', index=False)
df_clean.to_csv('unified_book_data_enriched_ultra.csv', index=False)

print(f'✓ Saved {len(removed_no_author)} removed entries to removed_entries.csv')
print(f'✓ Saved {len(df_clean)} clean entries to unified_book_data_enriched_ultra.csv')
print()

# Analyze remaining gaps
print('=' * 80)
print('REMAINING GAPS AFTER CLEANUP:')
print('=' * 80)

missing_gr_link = (df_clean['Goodreads Link'].isna() | (df_clean['Goodreads Link'] == '')).sum()
missing_series = (df_clean['Series Name'].isna() | (df_clean['Series Name'] == '')).sum()
missing_publisher = (df_clean['Publisher'].isna() | (df_clean['Publisher'] == '')).sum()
missing_pages = (df_clean['Pages'].isna() | (df_clean['Pages'] == 0)).sum()
missing_desc = (df_clean['Description'].isna() | (df_clean['Description'] == '')).sum()
missing_gr_rating = (df_clean['Goodreads Rating'].isna() | (df_clean['Goodreads Rating'] == 0)).sum()

print(f'Goodreads Links missing: {missing_gr_link} ({missing_gr_link/len(df_clean)*100:.1f}%)')
print(f'Series Name missing: {missing_series} ({missing_series/len(df_clean)*100:.1f}%)')
print(f'Publisher missing: {missing_publisher} ({missing_publisher/len(df_clean)*100:.1f}%)')
print(f'Pages missing: {missing_pages} ({missing_pages/len(df_clean)*100:.1f}%)')
print(f'Description missing: {missing_desc} ({missing_desc/len(df_clean)*100:.1f}%)')
print(f'Goodreads Rating missing: {missing_gr_rating} ({missing_gr_rating/len(df_clean)*100:.1f}%)')

print()
print('Next step: Run aggressive enrichment to fill these gaps')
