#!/usr/bin/env python3
"""
Book Matching Utilities
Provides robust book matching with multiple fallback strategies
CRITICAL: Preserves duplicate titles with different authors (creates separate rows)
"""
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
import sys
import os

# Add parent to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.text_normalizer import TextNormalizer


@dataclass
class MatchResult:
    """Result of a book matching operation"""
    matched: bool
    index: Optional[int]  # Index in target DataFrame
    confidence: float  # 0.0 to 1.0
    strategy: str  # Which matching strategy succeeded
    notes: str = ""  # Additional information about the match


class BookMatcher:
    """
    Robust book matching with fallback strategies
    
    CRITICAL RULE: Same title + different author = DIFFERENT BOOKS
    This matcher will NEVER merge books with the same title but different authors
    """
    
    def __init__(self, 
                 title_threshold: float = 0.85,
                 author_threshold: float = 0.90):
        """
        Initialize matcher with thresholds
        
        Args:
            title_threshold: Minimum similarity for fuzzy title matching (0-1)
            author_threshold: Minimum similarity for fuzzy author matching (0-1)
        """
        self.title_threshold = title_threshold
        self.author_threshold = author_threshold
        self.normalizer = TextNormalizer()
    
    def find_exact_match(self, df: pd.DataFrame, title: str, author: str) -> MatchResult:
        """
        Strategy 1: Exact match on normalized title + author
        """
        norm_title = self.normalizer.normalize_title(title)
        norm_author = self.normalizer.normalize_author(author, level='standard')
        
        if not norm_title['standard'] or not norm_author:
            return MatchResult(False, None, 0.0, "exact", "Missing title or author")
        
        # Create normalized columns if not exists (temporary)
        df_temp = df.copy()
        df_temp['_norm_title'] = df_temp['Book Name'].apply(
            lambda x: self.normalizer.normalize_title(str(x))['standard']
        )
        df_temp['_norm_author'] = df_temp['Author Name'].apply(
            lambda x: self.normalizer.normalize_author(str(x), level='standard')
        )
        
        # Find exact match
        mask = (df_temp['_norm_title'] == norm_title['standard']) & \
               (df_temp['_norm_author'] == norm_author)
        
        matches = df_temp[mask]
        
        if len(matches) > 0:
            return MatchResult(True, matches.index[0], 1.0, "exact")
        
        return MatchResult(False, None, 0.0, "exact", "No exact match")
    
    def find_fuzzy_title_match(self, df: pd.DataFrame, title: str, author: str) -> MatchResult:
        """
        Strategy 2: Fuzzy title match + EXACT author match
        CRITICAL: Author must match to avoid merging different books with similar titles
        """
        norm_title = self.normalizer.normalize_title(title)
        norm_author = self.normalizer.normalize_author(author, level='fuzzy')
        
        if not norm_title['fuzzy'] or not norm_author:
            return MatchResult(False, None, 0.0, "fuzzy_title", "Missing title or author")
        
        best_match_idx = None
        best_similarity = 0.0
        
        for idx, row in df.iterrows():
            row_title = str(row.get('Book Name', ''))
            row_author = str(row.get('Author Name', ''))
            
            # CRITICAL: Author must match exactly (fuzzy match with high threshold)
            if not self.normalizer.fuzzy_match_authors(author, row_author, self.author_threshold):
                continue
            
            # Now check title similarity
            title_similarity = self.normalizer.calculate_similarity(
                norm_title['standard'], 
                self.normalizer.normalize_title(row_title)['standard']
            )
            
            if title_similarity >= self.title_threshold and title_similarity > best_similarity:
                best_similarity = title_similarity
                best_match_idx = idx
        
        if best_match_idx is not None:
            return MatchResult(True, best_match_idx, best_similarity, "fuzzy_title")
        
        return MatchResult(False, None, 0.0, "fuzzy_title", "No fuzzy title match found")
    
    def find_core_title_match(self, df: pd.DataFrame, title: str, author: str) -> MatchResult:
        """
        Strategy 3: Core title match (without subtitle) + EXACT author match
        Useful for "The Deal: A Hockey Romance" vs "The Deal"
        """
        norm_title = self.normalizer.normalize_title(title)
        norm_author = self.normalizer.normalize_author(author, level='fuzzy')
        
        if not norm_title['core_title'] or not norm_author:
            return MatchResult(False, None, 0.0, "core_title", "Missing core title or author")
        
        best_match_idx = None
        best_similarity = 0.0
        
        for idx, row in df.iterrows():
            row_title = str(row.get('Book Name', ''))
            row_author = str(row.get('Author Name', ''))
            
            # CRITICAL: Author must match
            if not self.normalizer.fuzzy_match_authors(author, row_author, self.author_threshold):
                continue
            
            # Compare core titles
            row_norm_title = self.normalizer.normalize_title(row_title)
            core_similarity = self.normalizer.calculate_similarity(
                norm_title['core_title'],
                row_norm_title['core_title']
            )
            
            if core_similarity >= self.title_threshold and core_similarity > best_similarity:
                best_similarity = core_similarity
                best_match_idx = idx
        
        if best_match_idx is not None:
            return MatchResult(True, best_match_idx, best_similarity, "core_title")
        
        return MatchResult(False, None, 0.0, "core_title", "No core title match found")
    
    def find_series_match(self, df: pd.DataFrame, title: str, author: str, 
                          series_name: Optional[str] = None,
                          book_number: Optional[str] = None) -> MatchResult:
        """
        Strategy 4: Series-aware matching
        Match by series name + book number + author
        """
        # Extract series info from title if not provided
        if not series_name or not book_number:
            norm_title = self.normalizer.normalize_title(title)
            series_name = series_name or norm_title['series_name']
            book_number = book_number or norm_title['book_number']
        
        if not series_name or not book_number:
            return MatchResult(False, None, 0.0, "series", "No series info available")
        
        norm_author = self.normalizer.normalize_author(author, level='fuzzy')
        
        for idx, row in df.iterrows():
            row_series = str(row.get('Series Name', ''))
            row_book_num = str(row.get('Book Number', ''))
            row_author = str(row.get('Author Name', ''))
            
            # Author must match
            if not self.normalizer.fuzzy_match_authors(author, row_author, self.author_threshold):
                continue
            
            # Series name similarity
            if not row_series or row_series.lower() in ['nan', 'none']:
                continue
            
            series_similarity = self.normalizer.calculate_similarity(
                series_name.lower(), row_series.lower()
            )
            
            # Book number match
            book_num_match = str(book_number) == str(row_book_num)
            
            if series_similarity >= 0.80 and book_num_match:
                return MatchResult(True, idx, series_similarity, "series")
        
        return MatchResult(False, None, 0.0, "series", "No series match found")
    
    def find_best_match(self, df: pd.DataFrame, 
                       search_book: Dict[str, str],
                       strategies: List[str] = None) -> MatchResult:
        """
        Find the best match using multiple strategies in priority order
        
        Args:
            df: DataFrame to search in
            search_book: Dict with keys: 'title', 'author', optionally 'series_name', 'book_number'
            strategies: List of strategies to try, in order. 
                       Default: ['exact', 'fuzzy_title', 'core_title', 'series']
        
        Returns:
            MatchResult with best match found
        """
        if strategies is None:
            strategies = ['exact', 'fuzzy_title', 'core_title', 'series']
        
        title = search_book.get('title', '')
        author = search_book.get('author', '')
        
        if not title:
            return MatchResult(False, None, 0.0, "none", "No title provided")
        
        # Try each strategy in order
        for strategy in strategies:
            if strategy == 'exact':
                result = self.find_exact_match(df, title, author)
            elif strategy == 'fuzzy_title':
                result = self.find_fuzzy_title_match(df, title, author)
            elif strategy == 'core_title':
                result = self.find_core_title_match(df, title, author)
            elif strategy == 'series':
                result = self.find_series_match(
                    df, title, author,
                    search_book.get('series_name'),
                    search_book.get('book_number')
                )
            else:
                continue
            
            if result.matched:
                return result
        
        return MatchResult(False, None, 0.0, "none", "No match found with any strategy")
    
    def deduplicate_books(self, df: pd.DataFrame, 
                         keep: str = 'first',
                         preserve_different_authors: bool = True) -> pd.DataFrame:
        """
        Remove duplicate book entries
        
        CRITICAL: If preserve_different_authors=True (default), same title with different
        authors will NOT be considered duplicates
        
        Args:
            df: DataFrame to deduplicate
            keep: Which duplicate to keep ('first', 'last', False=remove all)
            preserve_different_authors: If True, same title + different author = not a duplicate
        
        Returns:
            Deduplicated DataFrame
        """
        df_copy = df.copy()
        
        # Normalize titles and authors for comparison
        df_copy['_norm_title_fuzzy'] = df_copy['Book Name'].apply(
            lambda x: self.normalizer.normalize_title(str(x))['fuzzy']
        )
        df_copy['_norm_author_fuzzy'] = df_copy['Author Name'].apply(
            lambda x: self.normalizer.normalize_author(str(x), level='fuzzy')
        )
        
        if preserve_different_authors:
            # Drop duplicates based on BOTH title and author
            subset = ['_norm_title_fuzzy', '_norm_author_fuzzy']
        else:
            # Drop duplicates based on title only (potentially dangerous!)
            subset = ['_norm_title_fuzzy']
        
        df_deduped = df_copy.drop_duplicates(subset=subset, keep=keep)
        
        # Remove temporary columns
        df_deduped = df_deduped.drop(columns=['_norm_title_fuzzy', '_norm_author_fuzzy'])
        
        return df_deduped


def merge_dataframes(df_target: pd.DataFrame, 
                    df_source: pd.DataFrame,
                    matcher: BookMatcher = None,
                    update_existing: bool = True,
                    add_new: bool = True) -> Tuple[pd.DataFrame, Dict]:
    """
    Merge source DataFrame into target DataFrame using robust matching
    
    Args:
        df_target: DataFrame to merge into (will be modified)
        df_source: DataFrame to merge from
        matcher: BookMatcher instance (will create default if None)
        update_existing: If True, update existing records with source data
        add_new: If True, add new records from source
    
    Returns:
        (merged_df, stats_dict)
        stats_dict contains: {'matched': int, 'updated': int, 'added': int, 'skipped': int}
    """
    if matcher is None:
        matcher = BookMatcher()
    
    stats = {'matched': 0, 'updated': 0, 'added': 0, 'skipped': 0}
    df_result = df_target.copy()
    
    for idx, row in df_source.iterrows():
        search_book = {
            'title': str(row.get('Book Name', '')),
            'author': str(row.get('Author Name', '')),
            'series_name': str(row.get('Series Name', '')) if pd.notna(row.get('Series Name')) else None,
            'book_number': str(row.get('Book Number', '')) if pd.notna(row.get('Book Number')) else None,
        }
        
        # Try to find match
        match = matcher.find_best_match(df_result, search_book)
        
        if match.matched:
            stats['matched'] += 1
            if update_existing:
                # Update existing record with non-empty values from source
                for col in df_source.columns:
                    if col in df_result.columns:
                        source_val = row[col]
                        target_val = df_result.at[match.index, col]
                        
                        # Only update if source has value and target doesn't
                        if pd.notna(source_val) and source_val != '' and \
                           (pd.isna(target_val) or target_val == '' or target_val == 0):
                            df_result.at[match.index, col] = source_val
                            stats['updated'] += 1
        else:
            # No match found
            if add_new:
                # Add as new record
                df_result = pd.concat([df_result, pd.DataFrame([row])], ignore_index=True)
                stats['added'] += 1
            else:
                stats['skipped'] += 1
    
    return df_result, stats


if __name__ == "__main__":
    # Test cases
    print("Testing BookMatcher...")
    print("=" * 80)
    
    # Create test DataFrame
    test_data = {
        'Book Name': [
            'Pucking Around (Jacksonville Rays, #1)',
            'The Deal: A Hockey Romance',
            'Off Limits',
            'Ice Breaker'
        ],
        'Author Name': [
            'Emily Rath',
            'Elle Kennedy',
            'Deanna Grey',
            'Hannah Grace'
        ],
        'Series Name': [
            'Jacksonville Rays',
            'Off-Campus',
            None,
            'Maple Hills'
        ],
        'Book Number': [1, 1, None, 1]
    }
    
    df_test = pd.DataFrame(test_data)
    
    print("\nTest DataFrame:")
    print(df_test)
    
    # Test matching
    matcher = BookMatcher()
    
    print("\n" + "=" * 80)
    print("Testing Exact Match:")
    result = matcher.find_exact_match(df_test, 'Pucking Around (Jacksonville Rays, #1)', 'Emily Rath')
    print(f"Result: {result}")
    
    print("\n" + "=" * 80)
    print("Testing Fuzzy Title Match:")
    result = matcher.find_fuzzy_title_match(df_test, 'Pucking Around', 'Emily Rath')
    print(f"Result: {result}")
    
    print("\n" + "=" * 80)
    print("Testing Core Title Match:")
    result = matcher.find_core_title_match(df_test, 'The Deal', 'Elle Kennedy')
    print(f"Result: {result}")
    
    print("\n" + "=" * 80)
    print("Testing Series Match:")
    result = matcher.find_series_match(df_test, 'Some Random Title', 'Emily Rath', 
                                       series_name='Jacksonville Rays', book_number='1')
    print(f"Result: {result}")
    
    print("\n" + "=" * 80)
    print("Testing find_best_match:")
    search_books = [
        {'title': 'Pucking Around', 'author': 'Emily Rath'},
        {'title': 'The Deal', 'author': 'Elle Kennedy'},
        {'title': 'Nonexistent Book', 'author': 'Unknown Author'}
    ]
    
    for book in search_books:
        result = matcher.find_best_match(df_test, book)
        print(f"\nSearching for: {book['title']} by {book['author']}")
        print(f"Result: {result}")
    
    print("\n" + "=" * 80)
    print("Testing Deduplication (preserving different authors):")
    
    # Add a duplicate and a same-title-different-author book
    df_with_dupes = pd.concat([
        df_test,
        pd.DataFrame([{
            'Book Name': 'Pucking Around',  # Duplicate
            'Author Name': 'Emily Rath',
            'Series Name': 'Jacksonville Rays',
            'Book Number': 1
        }]),
        pd.DataFrame([{
            'Book Name': 'The Deal',  # Same title, different author
            'Author Name': 'Different Author',
            'Series Name': None,
            'Book Number': None
        }])
    ], ignore_index=True)
    
    print("\nBefore deduplication:")
    print(df_with_dupes[['Book Name', 'Author Name']])
    
    df_deduped = matcher.deduplicate_books(df_with_dupes, preserve_different_authors=True)
    
    print("\nAfter deduplication (preserving different authors):")
    print(df_deduped[['Book Name', 'Author Name']])
    print(f"\nOriginal rows: {len(df_with_dupes)}, After dedup: {len(df_deduped)}")
    print("Note: 'The Deal' by Different Author should be preserved!")
