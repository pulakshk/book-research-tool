import pandas as pd
import numpy as np
from loguru import logger

# Inputs
DATA_FILE = "data/unified_book_data_enriched_ultra.csv"
SPORTS_T100 = "Sports Romance az t100.csv"
HOCKEY_T100 = "hockey romance az t100.csv"
OUTPUT_FILE = "series_commissioning_analysis.csv"

def load_t100_map(file_path):
    """
    Returns dict: {Normalized_Title: Rank}
    """
    try:
        df = pd.read_csv(file_path)
        df.columns = [c.strip() for c in df.columns]
        
        # Find headers
        title_col = next((c for c in df.columns if 'name' in c.lower() or 'title' in c.lower()), None)
        rank_col = next((c for c in df.columns if 'rank' in c.lower() or '#' in c), None)
        
        if not title_col or not rank_col:
            return {}
            
        mapping = {}
        for _, row in df.iterrows():
            t = str(row[title_col])
            norm = "".join(c for c in t.lower() if c.isalnum())
            try:
                r = int(str(row[rank_col]).replace('#', '').strip())
                mapping[norm] = r
            except:
                continue
        return mapping
    except:
        return {}

def main():
    logger.info(f"Loading data from {DATA_FILE}...")
    df = pd.read_csv(DATA_FILE)
    
    # Load Maps
    sports_map = load_t100_map(SPORTS_T100)
    hockey_map = load_t100_map(HOCKEY_T100)
    
    # helper to check rank
    def get_rank_category(rank, list_type):
        if not rank or pd.isna(rank): return None
        if rank <= 10: return f"{list_type} Top 10"
        if rank <= 50: return f"{list_type} Top 50"
        return f"{list_type} Top 100"

    # Group by Series
    # Filter out standalone/empty series names except where we explicitly fixed them
    # Ensure we capture 'The Billionaire Rules' (Lara Bailey)
    
    # Normalize Series Name for grouping to avoid 'Series Name' vs 'Series Name ' diffs
    df['Series_Group'] = df['Series Name'].fillna('Diff').astype(str).str.strip()
    
    # We only want Series Rows, so exclude 'NO_SERIES' if present? 
    # User said 600 series.
    groups = df.groupby(['Series_Group', 'Author Name'])
    
    series_rows = []
    
    for (series_name, author), group in groups:
        if series_name in ['NO_SERIES', 'Diff', 'nan']:
            continue
            
        # 1. Determine Metrics from books
        # Sort by Book Number (if avail) or Pub Date
        # Try numeric book number first
        try:
            group['Book_Num_Clean'] = group['Book Number'].astype(str).str.extract(r'(\d+)').astype(float)
            sorted_g = group.sort_values('Book_Num_Clean')
        except:
            sorted_g = group.sort_values('Publication Date')
            
        first_book = sorted_g.iloc[0]
        last_book = sorted_g.iloc[-1]
        
        # Pub Date Analysis
        first_pub = first_book.get('Publication Date')
        try:
            year = pd.to_datetime(first_pub).year
            era = "After 2020" if year >= 2020 else "Before 2020"
        except:
            year = np.nan
            era = "Unknown"
            
        # 2. Ranking Analysis (Series Level)
        # Find best rank across ALL books in this series
        best_rank_val = 9999
        best_rank_cat = "No List"
        
        books_in_t100_count = 0
        
        for _, book in group.iterrows():
            t = str(book['Book Name'])
            norm = "".join(c for c in t.lower() if c.isalnum())
            
            s_rank = sports_map.get(norm)
            h_rank = hockey_map.get(norm)
            
            in_list = False
            
            # Check Sports
            if s_rank:
                in_list = True
                if s_rank < best_rank_val:
                    best_rank_val = s_rank
                    best_rank_cat = get_rank_category(s_rank, "Sports Amazon")
            
            # Check Hockey
            if h_rank:
                in_list = True
                if h_rank < best_rank_val:
                    best_rank_val = h_rank
                    best_rank_cat = get_rank_category(h_rank, "Hockey Amazon")
            
            if in_list:
                books_in_t100_count += 1
                
        # 3. Construct Row
        row = {
            'Series Name': series_name,
            'Author Name': author,
            'First Book Name': first_book['Book Name'],
            'First Book Pub Date': first_pub,
            'Last Book Name': last_book['Book Name'],
            'Total Books': len(group),
            'Mapped_Combined_Rank_Flag': best_rank_cat,
            'Num_Books_In_Top_100': books_in_t100_count,
            'Series_Era': era,
            'First_Book_Year': year,
            'Goodreads Series Link': first_book.get('Goodreads Link'), # Approx
            'Average Goodreads Rating': group['Goodreads Rating'].mean(),
            'Total Goodreads Ratings': group['Goodreads # of Ratings'].sum()
        }
        series_rows.append(row)
        
    # Convert to DF
    res_df = pd.DataFrame(series_rows)
    
    # Sort by 'Num_Books_In_Top_100' desc, then Total Ratings
    res_df = res_df.sort_values(['Num_Books_In_Top_100', 'Total Goodreads Ratings'], ascending=[False, False])
    
    logger.info(f"Generated {len(res_df)} series rows.")
    res_df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
