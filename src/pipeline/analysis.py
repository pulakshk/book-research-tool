import pandas as pd
import numpy as np
from loguru import logger
from .config import ANALYSIS_FILE, WORDS_PER_PAGE, WORDS_PER_HOUR, WEIGHTS
from .cleaning import filter_unrelated_content

# Commissioning Thresholds
RANK_THRESHOLDS = {
    'P0': 90, 'P1': 80, 'P2': 70, 'P3': 60, 'P4': 50, 'P5': 0
}

def parse_rating_count(val):
    """Parse string like '1,234' or '1.2k' to float."""
    if pd.isna(val) or str(val).lower() == 'nan': return 0.0
    s = str(val).replace(',', '').lower()
    return float(s)

def sanitize_score(score, metric_name):
    """If score is effectively 0, assume data issue and give 80% Benefit of Doubt."""
    if score < 0.1: return 80.0
    return score

def get_series_metrics(group):
    """Calculate metrics for a single series group."""
    # Sort books
    try:
        group['Book Number'] = pd.to_numeric(group['Book Number'], errors='coerce')
        group = group.sort_values('Book Number')
    except:
        group = group.sort_values('Publication Date')
        
    books = group.to_dict('records')
    if not books: return None
        
    first, last = books[0], books[-1]
    
    # 1. Basic Stats
    n_books = len(books)
    book_names = ", ".join([str(b.get('Book Name', '')) for b in books])
    
    # Author Fix
    author = first.get('Author Name')
    if pd.isna(author) or str(author).lower() == 'nan':
        valid_authors = [b.get('Author Name') for b in books if not pd.isna(b.get('Author Name'))]
        author = valid_authors[0] if valid_authors else "Unknown"
            
    series_name = first.get('Series Name', 'Unknown Series')
    
    # 2. Pages / Duration
    pages = [pd.to_numeric(b.get('Pages', 300), errors='coerce') for b in books]
    pages = [p if not pd.isna(p) else 300 for p in pages]
    avg_pages = sum(pages) / len(pages) if pages else 300
    total_pages = sum(pages)
    adaptation_hours = (total_pages * WORDS_PER_PAGE) / WORDS_PER_HOUR
    
    # 3. Ratings
    def get_rating(b): return pd.to_numeric(b.get('Amazon Rating', b.get('Goodreads Rating', 0)), errors='coerce')
    def get_count(b): return parse_rating_count(b.get('Amazon # of Ratings', b.get('Goodreads # of Ratings', 0)))
    
    book_metrics = [{'rating': get_rating(b), 'count': get_count(b), 'name': b.get('Book Name')} for b in books]
    valid_metrics = [m for m in book_metrics if not pd.isna(m['rating']) and m['rating'] > 0]
    ratings = [m['rating'] for m in valid_metrics]
    
    if valid_metrics:
        min_book = min(valid_metrics, key=lambda x: x['rating'])
        max_book = max(valid_metrics, key=lambda x: x['rating'])
        min_rating, min_count = min_book['rating'], int(min_book['count'])
        max_rating, max_count = max_book['rating'], int(max_book['count'])
    else:
        min_rating, min_count, max_rating, max_count = 0.0, 0, 0.0, 0

    first_rating = get_rating(first)
    if pd.isna(first_rating): first_rating = 0.0
    first_count = get_count(first)
    if pd.isna(first_count): first_count = 0.0
    last_count = get_count(last)
    if pd.isna(last_count): last_count = 0.0
    last_rating = get_rating(last)

    # 4. Retention & Recency Bias
    retention_raw = last_count / first_count if first_count > 0 else 0.0
    
    # Recency
    CURRENT_DATE = pd.to_datetime("2026-02-03")
    try: last_pub = pd.to_datetime(last.get('Publication Date'), errors='coerce')
    except: last_pub = pd.NaT
        
    recency_factor, recency_label = 1.0, "Mature"
    if not pd.isna(last_pub):
        delta = (CURRENT_DATE - last_pub).days
        if delta < 90: recency_factor, recency_label = 1.5, "New Release (<3mo)"
        elif delta < 180: recency_factor, recency_label = 1.3, "Recent (<6mo)"
        elif delta < 365: recency_factor, recency_label = 1.15, "Modern (<1yr)"
            
    retention_adj = min(retention_raw * recency_factor, 1.0)

    # 5. Commissioning Score
    norm_rating_first = sanitize_score(min(first_rating / 5.0, 1.0) * 100, "Q1")
    
    avg_rating = sum(ratings) / len(ratings) if ratings else 0.0
    norm_rating_avg = sanitize_score(min(avg_rating / 5.0, 1.0) * 100, "QAvg")
    
    norm_retention = sanitize_score(min(retention_adj / 0.5, 1.0) * 100, "Ret")
    norm_appeal = sanitize_score(min(first_count / 10000, 1.0) * 100, "App")
    
    # Vol: <3 bad, 3-5 ok, 5+ good
    vol_safe = n_books if n_books else 1
    if vol_safe < 3: norm_vol = (vol_safe / 3.0) * 50
    elif vol_safe <= 5: norm_vol = 70 + (vol_safe - 3) * 10
    else: norm_vol = 100
        
    base_score = (
        (norm_rating_first * WEIGHTS['quality_first']) + 
        (norm_rating_avg * WEIGHTS['quality_avg']) +
        (norm_retention * WEIGHTS['retention']) + 
        (norm_appeal * WEIGHTS['appeal']) +
        (norm_vol * WEIGHTS['volume'])
    )
    
    # Bonus
    self_pub_flag = str(first.get('Self Pub Flag', 'Indie'))
    if self_pub_flag in ['Self Pub', 'Indie']:
        base_score *= 1.20
        
    # Classify Rank
    rank = 'P5'
    for r, thresh in RANK_THRESHOLDS.items():
        if base_score >= thresh:
            rank = r
            break
            
    # Hard Overrides
    if n_books < 3:
        rank, recency_label = 'P5', f"{recency_label} | Short Penalty"
            
    # Source Validation string (condensed)
    source_val = []
    for i, b in enumerate(books):
        lists = str(b.get('Featured List', '')).replace('nan', '')
        top = str(b.get('Top Lists', '')).replace('nan', '')
        award = f"{top} {lists}".strip().replace('\n', '; ').replace(',', ';')
        if len(award) > 5: source_val.append(f"Bk{i+1}: {award[:50]}...")
            
    return {
        'Book Series Name': series_name, 'Author Name': author,
        'No of books': n_books, 'Total Pages (Avg)': round(avg_pages, 1),
        'Length of adaptation in hrs': round(adaptation_hours, 1),
        'First Book Rating': round(first_rating, 2), 'First Book Count': int(first_count),
        'Last Book Rating': round(last_rating if not pd.isna(last_rating) else 0.0, 2),
        'Last Book Count': int(last_count),
        'Lowest Book Rating': round(min_rating, 2), 'Lowest Book Count': min_count,
        'Highest Book Rating': round(max_rating, 2), 'Highest Book Count': max_count,
        'Self Pub Flag': self_pub_flag, 'Commissioning Rank': rank,
        'Rationale': f"Score: {int(base_score)} [{recency_label}] (Vol:{int(norm_vol)} Q1:{int(norm_rating_first)} QAvg:{int(norm_rating_avg)} R:{int(norm_retention)} A:{int(norm_appeal)})",
        'Source Validation': " | ".join(source_val)
    }

def generate_report(df):
    """Generate the commissioning analysis report."""
    logger.info("Starting Series Analysis...")
    
    # 1. Clean
    df_clean = filter_unrelated_content(df)
    
    # 2. Group
    df_clean['Series_Norm'] = df_clean['Series Name'].fillna('').str.lower().str.strip()
    df_clean = df_clean[df_clean['Series_Norm'] != '']
    grouped = df_clean.groupby('Series_Norm')
    
    results = []
    logger.info(f"Processing {len(grouped)} series...")
    for _, group in grouped:
        metrics = get_series_metrics(group)
        if metrics: results.append(metrics)
            
    res_df = pd.DataFrame(results)
    
    # Sort
    rank_map = {'P0': 0, 'P1': 1, 'P2': 2, 'P3': 3, 'P4': 4, 'P5': 5}
    res_df['Rank_Sort'] = res_df['Commissioning Rank'].map(rank_map)
    res_df = res_df.sort_values(['Rank_Sort', 'First Book Rating'], ascending=[True, False]).drop(columns=['Rank_Sort'])
    
    res_df.to_csv(ANALYSIS_FILE, index=False)
    logger.success(f"Analysis saved to {ANALYSIS_FILE} ({len(res_df)} series).")
    return res_df
