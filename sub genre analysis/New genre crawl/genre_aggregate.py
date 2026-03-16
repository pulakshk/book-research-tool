#!/usr/bin/env python3
"""
GENRE AGGREGATE — Phase 3: Series-Level Aggregation & Commissioning Analysis
Takes the enriched book-level CSV and produces:
1. A series-level aggregated CSV matching the Sports & Hockey output format
2. Includes commissioning scoring (P0-P5)
"""

import os
import re
import sys
import json
from datetime import datetime

import pandas as pd
import numpy as np
from loguru import logger
from dotenv import load_dotenv

# Load env
load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', '.env'))
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

try:
    import google.generativeai as genai
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel('gemini-2.5-flash')
    else:
        gemini_model = None
except ImportError:
    gemini_model = None

OUTPUT_DIR = os.path.dirname(os.path.abspath(__file__))

# ============================================================================
# SCORING WEIGHTS (adapted from existing pipeline)
# ============================================================================

WEIGHTS = {
    'volume': 0.30,       # Series length
    'quality': 0.25,      # First Book Rating (15%) + Average Rating (10%)
    'retention': 0.25,    # Read-through ratio
    'appeal': 0.20,       # Market size (rating counts)
}

DEFAULT_SCORE = 80  # Default for missing data


# ============================================================================
# HELPERS
# ============================================================================

def safe_float(val, default=0.0):
    """Safely convert to float."""
    try:
        if pd.isna(val) or str(val).strip() == '':
            return default
        return float(re.sub(r'[^0-9.]', '', str(val)))
    except:
        return default


def safe_int(val, default=0):
    """Safely convert to int."""
    try:
        if pd.isna(val) or str(val).strip() == '':
            return default
        return int(float(re.sub(r'[^0-9.]', '', str(val))))
    except:
        return default


def classify_series_type(num_books):
    """Classify series type based on number of books."""
    if num_books >= 6:
        return "Long Series"
    elif num_books >= 4:
        return "Series"
    elif num_books >= 2:
        return "Short Series"
    return "Standalone"


def determine_self_pub(publisher):
    """Determine self-pub flag."""
    SELF_PUB_KEYWORDS = [
        'independently published', 'self-published', 'createspace', 'draft2digital',
        'smashwords', 'kindle direct', 'kdp', 'lulu', 'blurb', 'authorhouse',
    ]
    TRADITIONAL_PUBLISHERS = [
        'penguin', 'harpercollins', 'simon & schuster', 'hachette', 'macmillan',
        'random house', 'scholastic', 'sourcebooks', 'berkley', 'avon',
        'ballantine', 'bantam', 'tor', 'st. martin', 'entangled', 'montlake',
        'kensington', 'harlequin', 'mira', 'william morrow', 'putnam',
        'dutton', 'atria', 'gallery', 'scribner', 'vintage', 'crown', 'knopf',
        'doubleday', 'little brown', 'orbit', 'del rey',
    ]
    if not publisher or pd.isna(publisher) or str(publisher).strip() == '':
        return ''
    p = str(publisher).lower()
    if any(kw in p for kw in SELF_PUB_KEYWORDS):
        return 'Self-Pub'
    if any(pub in p for pub in TRADITIONAL_PUBLISHERS):
        return 'Big Pub'
    return 'Indie'


def compute_flag(value, thresholds, labels=None):
    """Compute a flag label based on thresholds."""
    if labels is None:
        labels = ['Low', 'Medium', 'High', 'Very High']
    if pd.isna(value) or value is None:
        return 'Data Missing'
    v = safe_float(value)
    for i, t in enumerate(thresholds):
        if v <= t:
            return labels[i]
    return labels[-1]


def extract_pub_year(date_str):
    """Extract publication year from various date formats."""
    if not date_str or pd.isna(date_str) or str(date_str).strip() == '':
        return ''
    s = str(date_str).strip()
    # Try patterns
    for fmt in [r'(\d{4})', r'(\d{2}/\d{2}/\d{4})', r'(\w+ \d{1,2}, \d{4})']:
        m = re.search(fmt, s)
        if m:
            matched = m.group(1)
            if len(matched) == 4:
                return matched
            try:
                for dfmt in ['%m/%d/%Y', '%B %d, %Y', '%b %d, %Y']:
                    try:
                        return str(datetime.strptime(matched, dfmt).year)
                    except:
                        continue
            except:
                pass
    return ''


# ============================================================================
# SERIES AGGREGATION
# ============================================================================

def aggregate_to_series(df):
    """Group books by series (or standalone) and compute series-level stats."""
    logger.info("Aggregating books into series...")
    
    # Normalize series name
    df['_series_key'] = df.apply(
        lambda r: (
            str(r.get('Series Name', '')).strip().lower() + '|||' +
            str(r.get('Author Name', '')).strip().lower()
        ) if str(r.get('Series Name', '')).strip() else (
            str(r.get('Book Name', '')).strip().lower() + '|||' +
            str(r.get('Author Name', '')).strip().lower() + '|||standalone'
        ),
        axis=1
    )
    
    series_rows = []
    
    for key, group in df.groupby('_series_key'):
        group = group.copy()
        is_standalone = key.endswith('|||standalone')
        
        # Sort by book number if available
        group['_bn'] = group['Book Number'].apply(lambda x: safe_float(x, 999))
        group = group.sort_values('_bn')
        
        # Series Name
        series_name = group['Series Name'].iloc[0] if not is_standalone else group['Book Name'].iloc[0]
        if not series_name or pd.isna(series_name) or str(series_name).strip() == '':
            series_name = group['Book Name'].iloc[0]
        
        author = group['Author Name'].iloc[0]
        num_books = len(group)
        
        # Book list
        book_names = group['Book Name'].tolist()
        books_list = ', '.join([str(b) for b in book_names if str(b).strip()])
        
        # Total pages
        pages_list = group['Pages'].apply(safe_float)
        total_pages = pages_list.sum()
        adaptation_hours = round(total_pages * 0.03, 2)
        
        # Ratings
        ratings = group['Goodreads Rating'].apply(lambda x: safe_float(x, None))
        rating_counts = group['Goodreads # of Ratings'].apply(lambda x: safe_float(x, None))
        
        # First book (by book number or order)
        first_book = group.iloc[0]
        first_book_name = str(first_book['Book Name'])
        first_book_rating = safe_float(first_book.get('Goodreads Rating', ''))
        first_book_count = safe_float(first_book.get('Goodreads # of Ratings', ''))
        
        # Last book
        last_book = group.iloc[-1] if len(group) > 1 else first_book
        last_book_name = str(last_book['Book Name'])
        last_book_rating = safe_float(last_book.get('Goodreads Rating', ''))
        last_book_count = safe_float(last_book.get('Goodreads # of Ratings', ''))
        
        # Highest rated book
        valid_ratings = group[ratings.notna() & (ratings > 0)]
        if len(valid_ratings) > 0:
            highest_idx = valid_ratings['Goodreads Rating'].apply(safe_float).idxmax()
            highest_row = df.loc[highest_idx]
            highest_name = str(highest_row['Book Name'])
            highest_rating = safe_float(highest_row.get('Goodreads Rating', ''))
            highest_count = safe_float(highest_row.get('Goodreads # of Ratings', ''))
            
            lowest_idx = valid_ratings['Goodreads Rating'].apply(safe_float).idxmin()
            lowest_row = df.loc[lowest_idx]
            lowest_name = str(lowest_row['Book Name'])
            lowest_rating = safe_float(lowest_row.get('Goodreads Rating', ''))
            lowest_count = safe_float(lowest_row.get('Goodreads # of Ratings', ''))
        else:
            highest_name = highest_rating = highest_count = ''
            lowest_name = lowest_rating = lowest_count = ''
        
        # Publisher
        publishers = group['Publisher'].apply(lambda x: str(x).strip() if not pd.isna(x) else '')
        publisher = publishers[publishers != ''].iloc[0] if len(publishers[publishers != '']) > 0 else ''
        self_pub = determine_self_pub(publisher)
        
        # Source detail / Featured ranks
        source_details = group['Source Detail'].apply(lambda x: str(x) if not pd.isna(x) else '')
        bestseller_entries = source_details[source_details.str.contains('#', na=False)]
        featured_rank = ' | '.join(bestseller_entries.tolist()) if len(bestseller_entries) > 0 else ''
        num_featured = len(bestseller_entries)
        
        # Amazon BSR
        bsr_entries = group['Amazon BSR'].apply(lambda x: str(x) if not pd.isna(x) and str(x).strip() else '')
        top_lists = ' | '.join(bsr_entries[bsr_entries != ''].tolist()[:3])
        
        # Publication year (first book)
        pub_date = str(first_book.get('Publication Date', ''))
        first_pub_year = extract_pub_year(pub_date)
        
        # Subgenre & Trope from first book
        primary_trope = str(first_book.get('Primary Trope', ''))
        primary_subgenre = str(first_book.get('Subgenre', ''))
        
        # Subjective Analysis & Differentiator (from first book or Gemini)
        subjective = str(first_book.get('Subjective Analysis', ''))
        differentiator = str(first_book.get('Differentiator', ''))
        
        # Description / Synopsis 
        synopsis = str(first_book.get('Short Synopsis', ''))
        
        # --- Derived Flags ---
        # Adaptation Length Flag
        adaptation_flag = compute_flag(adaptation_hours, [20, 50, 100], ['Low', 'Medium', 'High', 'Very High'])
        
        # First Book Rating Flag
        fb_rating_flag = compute_flag(first_book_rating, [3.5, 3.8, 4.0], ['Low', 'Medium', 'High', 'Very High'])
        
        # Appeal Flag (based on first book rating count)
        appeal_flag = compute_flag(first_book_count, [1000, 10000, 50000], ['Low', 'Medium', 'High', 'Very High'])
        
        # Lowest Book Rating Flag
        lb_flag = compute_flag(lowest_rating, [3.3, 3.6, 3.9], ['Low', 'Medium', 'High', 'Very High'])
        
        # Rating Stability Flag
        if highest_rating and lowest_rating and safe_float(highest_rating) > 0 and safe_float(lowest_rating) > 0:
            delta = safe_float(highest_rating) - safe_float(lowest_rating)
            stability = compute_flag(1 - delta, [0.4, 0.6, 0.8], ['Low', 'Medium', 'High', 'Very High'])
        else:
            stability = 'Data Missing'
        
        # Series Era
        if first_pub_year and first_pub_year.isdigit():
            era = 'After 2020' if int(first_pub_year) >= 2020 else 'Before 2020'
        else:
            era = ''
        
        # --- Commissioning Score ---
        volume_score = min(num_books / 8.0 * 100, 100)  
        quality_score = (safe_float(first_book_rating) / 5.0 * 100) if first_book_rating else DEFAULT_SCORE
        
        # Retention: ratio of last book ratings to first book ratings
        if first_book_count and last_book_count and safe_float(first_book_count) > 0:
            retention_ratio = safe_float(last_book_count) / safe_float(first_book_count)
            retention_score = min(retention_ratio * 100, 100)
        else:
            retention_score = DEFAULT_SCORE
        
        # Appeal: based on first book rating count
        if first_book_count:
            appeal_score = min(safe_float(first_book_count) / 1000 * 10, 100)
        else:
            appeal_score = DEFAULT_SCORE
        
        total_score = (
            WEIGHTS['volume'] * volume_score +
            WEIGHTS['quality'] * quality_score +
            WEIGHTS['retention'] * retention_score +
            WEIGHTS['appeal'] * appeal_score
        )
        
        # Short Penalty
        if num_books < 3:
            commissioning_rank = 'P5'
        elif total_score >= 80:
            commissioning_rank = 'P0'
        elif total_score >= 65:
            commissioning_rank = 'P1'
        elif total_score >= 50:
            commissioning_rank = 'P2'
        elif total_score >= 35:
            commissioning_rank = 'P3'
        elif total_score >= 20:
            commissioning_rank = 'P4'
        else:
            commissioning_rank = 'P5'
        
        # Build the output row matching Sports & Hockey format
        row = {
            'Book Series Name': series_name,
            'Author Name': author,
            'Type': classify_series_type(num_books) if not is_standalone else 'Standalone',
            'Books_In_Series_List': books_list,
            'Verfied Flag': '',
            'Books in Series': num_books,
            'Total Pages': total_pages if total_pages > 0 else '',
            'Length of Adaption in Hours': adaptation_hours if adaptation_hours > 0 else '',
            'First Book Name': first_book_name,
            'First Book Rating': first_book_rating if first_book_rating > 0 else '',
            'First Book Rating Count': int(first_book_count) if first_book_count > 0 else '',
            'Last Book Name': last_book_name,
            'Last Book Rating': last_book_rating if last_book_rating > 0 else '',
            'Last Book Rating Count': int(last_book_count) if last_book_count > 0 else '',
            'Highest Rated Book Name': highest_name,
            'Highest Rated Book Rating': highest_rating if highest_rating else '',
            'Highest Rated Book Rating Count': int(highest_count) if highest_count else '',
            'Lowest Rated Book Name': lowest_name,
            'Lowest Rated Book Rating': lowest_rating if lowest_rating else '',
            'Lowest Rated Book Rating Count': int(lowest_count) if lowest_count else '',
            'Publisher Name': publisher,
            'Self Pub Flag': self_pub,
            'Subjective Analysis': subjective,
            'Differentiator': differentiator,
            'Books_Featured_Rank_Validation': featured_rank,
            'Num_Books_Featured': num_featured,
            'First_Book_Pub_Year': first_pub_year,
            'T100_Mapping': '',  # To be filled manually
            'Adaptation_Length_Flag': adaptation_flag,
            'First_Book_Rating_Flag': fb_rating_flag,
            'Appeal Flag': appeal_flag,
            'Lowest_Book_Rating_Flag': lb_flag,
            'Rating_Stability_Flag': stability,
            'Series_Era': era,
            'Commissioning_Score': round(total_score, 1),
            'Commissioning_Rank': commissioning_rank,
            'Primary Subgenre': primary_subgenre,
            'Primary Trope': primary_trope,
            'Goodreads Series URL': str(first_book.get('Goodreads Series URL', '')),
            'Amazon Series URL': str(first_book.get('Amazon Link', '')),
        }
        
        series_rows.append(row)
    
    result = pd.DataFrame(series_rows)
    
    # Sort by commissioning score descending
    result = result.sort_values('Commissioning_Score', ascending=False).reset_index(drop=True)
    
    logger.info(f"  ✓ Aggregated {len(df)} books into {len(result)} series/standalones")
    
    # Rank distribution
    if 'Commissioning_Rank' in result.columns:
        dist = result['Commissioning_Rank'].value_counts().to_dict()
        logger.info(f"  Rank distribution: {dist}")
    
    return result


# ============================================================================
# MAIN
# ============================================================================

def aggregate_subgenre(input_csv, subgenre_name=None):
    """Run aggregation for a subgenre."""
    if not os.path.exists(input_csv):
        logger.error(f"Input file not found: {input_csv}")
        return
    
    df = pd.read_csv(input_csv)
    logger.info(f"Loaded {len(df)} enriched books from {input_csv}")
    
    if subgenre_name is None:
        subgenre_name = df['Subgenre'].iloc[0] if 'Subgenre' in df.columns else "Unknown"
    
    # Aggregate
    result = aggregate_to_series(df)
    
    # Save
    safe_name = re.sub(r'[/\\:*?"<>|]', '_', subgenre_name)
    output_file = os.path.join(OUTPUT_DIR, f"{safe_name}_final.csv")
    result.to_csv(output_file, index=False)
    logger.success(f"\n✅ Final aggregated sheet saved to: {output_file}")
    logger.info(f"  Total series: {len(result)}")
    
    return output_file


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Genre Aggregation - Phase 3: Series-Level")
    parser.add_argument("--input", type=str, required=True, help="Path to enriched CSV")
    parser.add_argument("--genre", type=str, default=None, help="Subgenre name")
    args = parser.parse_args()
    
    aggregate_subgenre(args.input, args.genre)
