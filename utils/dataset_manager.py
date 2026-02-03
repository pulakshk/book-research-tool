
import pandas as pd
import numpy as np
import csv
import re
import os
import shutil
from datetime import datetime
from loguru import logger

# Import new utilities for fuzzy matching
from utils.matcher import BookMatcher
from utils.validator import DataValidator
import config

class DatasetManager:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None

    def load(self):
        if os.path.exists(self.file_path):
            self.df = pd.read_csv(self.file_path)
            logger.info(f"Loaded {len(self.df)} rows from {self.file_path}")
        else:
            logger.error(f"File {self.file_path} not found.")

    def repair_newlines(self, output_path=None):
        """Fixes CSVs where newlines inside quotes broke the row parsing."""
        logger.info("Repairing unescaped newlines...")
        rows = []
        with open(self.file_path, 'r', encoding='utf-8', errors='replace') as f:
            content = f.read()
            # This is a complex regex-based repair for specific pocketfm export artifacts
            # Usually handled by standard csv reader if quotes are perfect, but often they aren't.
            # For now, implementing a robust line-joiner
            lines = content.splitlines()
            buffer = ""
            for line in lines:
                buffer += line + " "
                if buffer.count('"') % 2 == 0:
                    rows.append(buffer.strip())
                    buffer = ""
        
        path = output_path or self.file_path
        with open(path, 'w', encoding='utf-8') as f:
            f.write("\n".join(rows))
        logger.success(f"Repair complete. Saved to {path}")

    def align_columns(self):
        """Checks for column shifts and enforces the 24-column master schema."""
        if self.df is None: self.load()
        logger.info("Aligning columns and enforcing 24-column schema...")
        
        # Enforce Master Schema
        schema_cols = [
            'Series Name', 'Author Name', 'Book Name', 'Book Number', 'Total Books in Series', 
            'Goodreads Link', 'Goodreads # of Ratings', 'Goodreads Rating', 'First Published', 
            'Original Published', 'Pages', 'Description', 'Primary Trope', 'Primary Subgenre', 
            'Series Status', 'Amazon Link', 'Amazon # of Ratings', 'Amazon Rating', 
            'Publisher', 'Self Pub flag', 'Short Synopsis', 'Publication Date', 
            'Top Lists', 'Featured List'
        ]
        
        # Check for link swaps
        for idx, row in self.df.iterrows():
            glink = str(row.get('Goodreads Link', ''))
            alink = str(row.get('Amazon Link', ''))
            
            if "amazon.com" in glink.lower() and "goodreads.com" in alink.lower():
                self.df.at[idx, 'Goodreads Link'], self.df.at[idx, 'Amazon Link'] = alink, glink
            elif "amazon.com" in glink.lower():
                self.df.at[idx, 'Amazon Link'] = glink
                if "goodreads.com" not in glink.lower(): # Only null out if it's definitely not a GR link
                    self.df.at[idx, 'Goodreads Link'] = np.nan

        # Final reindex
        self.df = self.df.reindex(columns=schema_cols)
        logger.success("Alignment and schema enforcement complete.")

    def beautify(self, output_path=None):
        """Standardizes nulls, whitespaces, and sorts the data."""
        if self.df is None: self.load()
        logger.info("Beautifying dataset (Strict Null Handling)...")
        
        # 1. Standardize Nulls for Strings
        placeholders = ['0.0', '0', 'nan', 'NAN', 'None', 'null', 'unknown', 'Unknown Publisher']
        self.df = self.df.replace(placeholders, np.nan)
        
        # 2. Handle Numeric Zeroes as Missing (for specific columns)
        numeric_cols = [
            'Amazon Rating', 'Goodreads Rating', 'Amazon # of Ratings', 
            'Goodreads # of Ratings', 'Pages', 'Book Number', 'Total Books in Series'
        ]
        for col in numeric_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')
                # Replace 0 or 0.0 with NaN, except for Book Number which can be 0 (rare) or 1
                if col != 'Book Number':
                    self.df[col] = self.df[col].replace(0, np.nan)
                    self.df[col] = self.df[col].replace(0.0, np.nan)

        # 3. Cleanup whitespace
        for col in self.df.columns:
            if self.df[col].dtype == object:
                self.df[col] = self.df[col].astype(str).str.strip().replace(['nan', 'None', 'null'], np.nan)

        # 4. Sort by Series, then Book Number
        if 'Book Number' in self.df.columns:
            self.df['Book Number'] = pd.to_numeric(self.df['Book Number'], errors='coerce')
        
        self.df = self.df.sort_values(by=['Series Name', 'Book Number', 'Book Name'], na_position='last')

        # 5. Deduplication - NOW USES FUZZY MATCHING WITH AUTHOR PRESERVATION!
        # CRITICAL: Different authors with same title are PRESERVED (not merged)
        if config.ENABLE_FUZZY_DEDUP:
            logger.info("Using fuzzy deduplication (preserves different authors)...")
            matcher = BookMatcher()
            before_count = len(self.df)
            self.df = matcher.deduplicate_books(self.df, keep='first', preserve_different_authors=True)
            after_count = len(self.df)
            if before_count > after_count:
                logger.info(f"  Removed {before_count - after_count} fuzzy duplicates")
        else:
            # Fallback: Simple deduplication
            self.df['norm_title'] = self.df['Book Name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', str(x).lower()) if pd.notna(x) else "")
            self.df = self.df.drop_duplicates(subset=['norm_title', 'Author Name'], keep='first')
            self.df = self.df.drop(columns=['norm_title'])

        # 6. NEW: Create backup before saving
        if config.ENABLE_AUTO_BACKUP:
            backup_dir = os.path.join(os.path.dirname(self.file_path), 'backups', 'auto')
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(backup_dir, f'{timestamp}_beautify_backup.csv')
            if os.path.exists(self.file_path):
                shutil.copy(self.file_path, backup_path)
                logger.info(f"  Backup saved: {backup_path}")

        path = output_path or self.file_path
        self.df.to_csv(path, index=False)
        logger.success(f"Beautification complete. Saved to {path}")

    def audit(self):
        """Prints a summary of missing data."""
        if self.df is None: self.load()
        total = len(self.df)
        logger.info(f"--- Dataset Audit (Total: {total}) ---")
        for col in self.df.columns:
            missing = self.df[col].isna().sum()
            percent = (missing / total) * 100
            logger.info(f"{col}: {total-missing} present ({percent:.1f}% missing)")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 dataset_manager.py [audit|beautify|align] [file_path]")
    else:
        cmd = sys.argv[1]
        path = sys.argv[2] if len(sys.argv) > 2 else "unified_book_data_enriched_ultra.csv"
        mgr = DatasetManager(path)
        if cmd == "audit": mgr.audit()
        elif cmd == "beautify": mgr.beautify()
        elif cmd == "align": mgr.align_columns()
