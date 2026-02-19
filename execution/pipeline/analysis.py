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
    
    # 3. Ratings & Specific Book Identification
    # STRICT: Use Goodreads Only per user request (Platform Purity)
    def get_rating(b): return pd.to_numeric(b.get('Goodreads Rating', 0), errors='coerce')
    def get_count(b): return parse_rating_count(b.get('Goodreads # of Ratings', 0))
    
    book_metrics = [{'rating': get_rating(b), 'count': get_count(b), 'name': b.get('Book Name'), 'obj': b} for b in books]
    valid_metrics = [m for m in book_metrics if not pd.isna(m['rating']) and m['rating'] > 0]
    ratings = [m['rating'] for m in valid_metrics]
    
    # Highest Rated Book
    if valid_metrics:
        max_book_metric = max(valid_metrics, key=lambda x: (x['rating'], x['count'])) # Break ties with count
        max_rating, max_count = max_book_metric['rating'], int(max_book_metric['count'])
        highest_book_name = max_book_metric['name']
        
        min_book_metric = min(valid_metrics, key=lambda x: x['rating'])
        min_rating, min_count = min_book_metric['rating'], int(min_book_metric['count'])
    else:
        max_rating, max_count, highest_book_name = 0.0, 0, "N/A"
        min_rating, min_count = 0.0, 0

    # First Book (Already sorted by Book Number or Date)
    first_book_name = first.get('Book Name', 'Unknown')
    first_rating = get_rating(first)
    if pd.isna(first_rating): first_rating = 0.0
    first_count = get_count(first)
    if pd.isna(first_count): first_count = 0.0

    # Last Published Book (Explicitly by Date)
    try:
        # Filter for valid dates first
        books_with_dates = [b for b in books if pd.to_datetime(b.get('Publication Date'), errors='coerce') is not pd.NaT]
        if books_with_dates:
            last_pub_book = max(books_with_dates, key=lambda b: pd.to_datetime(b.get('Publication Date'), errors='coerce'))
        else:
            last_pub_book = last # Fallback to last in list
    except:
        last_pub_book = last
        
    last_pub_name = last_pub_book.get('Book Name', 'Unknown')
    last_count = get_count(last_pub_book) # Use metrics from the actually last published book
    if pd.isna(last_count): last_count = 0.0
    last_rating = get_rating(last_pub_book)

    # 4. Retention & Recency Bias
    retention_raw = last_count / first_count if first_count > 0 else 0.0
    
    # Recency (using the Last Published Book's date)
    CURRENT_DATE = pd.to_datetime("2026-02-03")
    try: last_pub_date = pd.to_datetime(last_pub_book.get('Publication Date'), errors='coerce')
    except: last_pub_date = pd.NaT
        
    recency_factor, recency_label = 1.0, "Mature"
    if not pd.isna(last_pub_date):
        delta = (CURRENT_DATE - last_pub_date).days
        if delta < 90: recency_factor, recency_label = 1.5, "New Release (<3mo)"
        elif delta < 180: recency_factor, recency_label = 1.3, "Recent (<6mo)"
        elif delta < 365: recency_factor, recency_label = 1.15, "Modern (<1yr)"
            
    retention_adj = min(retention_raw * recency_factor, 1.0)

    # 5. Peak Rank Signal
    peak_perf = ""
    peak_score_bonus = 0
    # Try to find #1 or top 10 in peak performance
    for b in books:
        pp = str(b.get('Peak Performance', '')).lower()
        if not peak_perf and pp and pp != 'nan': peak_perf = b.get('Peak Performance')
        if '#1' in pp: peak_score_bonus = 15
        elif any(f'#{i}' in pp for i in range(2, 11)): peak_score_bonus = 10
        elif any(f'#{i}' in pp for i in range(11, 101)): peak_score_bonus = 5

    # 6. Commissioning Score
    norm_rating_first = sanitize_score(min(first_rating / 5.0, 1.0) * 100, "Q1")
    
    avg_rating = sum(ratings) / len(ratings) if ratings else 0.0
    norm_rating_avg = sanitize_score(min(avg_rating / 5.0, 1.0) * 100, "QAvg")
    
    norm_retention = sanitize_score(min(retention_adj / 0.5, 1.0) * 100, "Ret")
    norm_appeal = sanitize_score(min(first_count / 10000, 1.0) * 100, "App")
    
    # Vol: <3 ok (Short), 3-5 good, 5+ great
    vol_safe = n_books if n_books else 1
    if vol_safe == 1: 
        norm_vol = 40
        type_label = "Standalone"
    elif vol_safe < 3: 
        norm_vol = 60
        type_label = "Short Series"
    elif vol_safe <= 5: 
        norm_vol = 85
        type_label = "Series"
    else: 
        norm_vol = 100
        type_label = "Long Series"
        
    base_score = (
        (norm_rating_first * WEIGHTS['quality_first']) + 
        (norm_rating_avg * WEIGHTS['quality_avg']) +
        (norm_retention * WEIGHTS['retention']) + 
        (norm_appeal * WEIGHTS['appeal']) +
        (norm_vol * WEIGHTS['volume'])
    )
    
    # Add Peak Bonus
    base_score += peak_score_bonus
    
    # Bonus for Self-Pub
    self_pub_flag = str(first.get('Self Pub Flag', 'Indie'))
    if self_pub_flag in ['Self Pub', 'Indie']:
        base_score *= 1.20
        
    # Classify Rank
    rank = 'P5'
    for r, thresh in RANK_THRESHOLDS.items():
        if base_score >= thresh:
            rank = r
            break
            
    # Source Validation string (Strict: Per-Book Amazon Sports/Hockey Top 100 Rank)
    import re
    source_val_parts = []
    
    notable_lists = []
    subj_analysis = ""
    differentiator = ""
    
    for i, b in enumerate(books):
        # 1. Validation Logic
        lists = str(b.get('Featured List', '')).replace('nan', '')
        top = str(b.get('Top Lists', '')).replace('nan', '')
        peak = str(b.get('Peak Performance', '')).replace('nan', '')
        combined_text = f"{top} {lists} {peak}"
        
        matches = re.findall(r'#([0-9,]+)\s+in\s+([\w\s&]+)', combined_text)
        
        best_book_rank = 999999
        best_book_str = ""
        
        for rank_str, cat_str in matches:
            try:
                rank_num = int(rank_str.replace(',', ''))
                cat_clean = cat_str.strip()
                if rank_num <= 100 and ('sports' in cat_clean.lower() or 'hockey' in cat_clean.lower()):
                    if rank_num < best_book_rank:
                        best_book_rank = rank_num
                        best_book_str = f"#{rank_num} in {cat_clean}"
            except:
                continue
        
        if best_book_str:
            source_val_parts.append(f"{b.get('Book Name')}: {best_book_str}")

        # 2. Aggregate Advanced Fields
        if not notable_lists and not pd.isna(b.get('Notable Lists')):
            notable_lists = b.get('Notable Lists')
        if not subj_analysis and not pd.isna(b.get('Subjective Analysis')):
            subj_analysis = b.get('Subjective Analysis')
        if not differentiator and not pd.isna(b.get('Differentiator')):
            differentiator = b.get('Differentiator')
                
    source_val_final = " | ".join(source_val_parts) if source_val_parts else "No Top 100 Sports/Hockey Rank"

    return {
        'Book Series Name': series_name, 'Author Name': author, 'Type': type_label,
        'Books in Series': book_names,
        'No of books': n_books, 'Total Pages (Avg)': round(avg_pages, 1),
        'Length of adaptation in hrs': round(adaptation_hours, 1),
        'First Book Name': first_book_name,
        'First Book Rating': round(first_rating, 2), 'First Book Count': int(first_count),
        'Last Book Name': last_pub_name,
        'Last Book Rating': round(last_rating if not pd.isna(last_rating) else 0.0, 2),
        'Last Book Count': int(last_count),
        'Highest Book Name': highest_book_name,
        'Highest Book Rating': round(max_rating, 2), 'Highest Book Count': max_count,
        'Lowest Book Rating': round(min_rating, 2), 'Lowest Book Count': min_count,
        # Removed Notable Lists & Peak Performance per request
        'Subjective Analysis': subj_analysis, 'Differentiator': differentiator,
        'Self Pub Flag': self_pub_flag, 'Commissioning Rank': rank,
        'Rationale': f"Score: {int(base_score)} [{recency_label}] (Vol:{int(norm_vol)} Q1:{int(norm_rating_first)} QAvg:{int(norm_rating_avg)} R:{int(norm_retention)} A:{int(norm_appeal)} Peak:+{peak_score_bonus})",
        'Source Validation': source_val_final
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
