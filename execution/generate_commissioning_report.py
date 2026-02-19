import pandas as pd
import numpy as np
from loguru import logger

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "book_series_analysis.csv"

def get_mode(series):
    m = series.mode()
    if not m.empty: return m.iloc[0]
    return None

def calc_commissioning_score(row):
    score = 0
    
    # A. T100 Presence (Max 15)
    status = str(row['T100_Mapping'])
    if "Top 10" in status: score += 15
    elif "Top 50" in status: score += 10
    elif "Top 100" in status: score += 5
    
    # B. First Book Rating (Max 25)
    r = row['First Book Rating']
    if pd.notna(r):
        if r > 4.5: score += 25
        elif r >= 4.2: score += 20 # Stricter
        elif r >= 4.0: score += 15
        elif r >= 3.8: score += 10
        elif r >= 3.5: score += 5
    else:
        score += 20 # 80% fallback
        
    # C. First Book Count (Max 30)
    c = row['First Book Rating Count']
    if pd.notna(c) and c > 0:
        if c > 20000: score += 30 # Higher threshold for max
        elif c > 10000: score += 25
        elif c >= 5000: score += 18
        elif c >= 1000: score += 10
        elif c > 0: score += 8 
    else:
        score += 24 # 80% fallback
        
    # D. Consistency & Retention (Max 20)
    # 1. Delta (Max 10)
    delta_score = 0
    if pd.notna(row['First Book Rating']) and pd.notna(row['Last Book Rating']):
         if row['Last Book Rating'] - row['First Book Rating'] >= -0.1: # Stricter
             delta_score = 10
    else:
         delta_score = 8
    score += delta_score
    
    # 2. Retention - Count (Max 10)
    retention_score = 0
    if pd.notna(row['First Book Rating Count']) and pd.notna(row['Last Book Rating Count']) and row['First Book Rating Count'] > 0:
        ret = row['Last Book Rating Count'] / row['First Book Rating Count']
        if ret > 0.6: score += 10 # Stricter
        elif ret > 0.4: score += 5
    else:
        retention_score = 8
    score += retention_score
    
    # E. Era (Max 10)
    era = str(row['Series_Era'])
    if "After 2020" in era: score += 10
    elif "Before 2020" in era: score += 5
    else: score += 8
    
    return min(100, score)

def get_rank_label(row, score):
    # Rule 1: Standalone and Short Series (1-3 books) are P5 by default
    if row['Type'] in ['Standalone', 'Short Series']:
        return "P5"

    # Rule 2: Big Publishers can't be above P2
    publisher = str(row['Publisher Name']).lower()
    author = str(row['Author Name']).lower()
    
    # Improved Indie Detection:
    # If publisher is author name, contains 'indie'/'self', or is empty/nan, treat as Indie
    is_indie = (
        'indie' in publisher or 
        'self' in publisher or 
        publisher == 'nan' or 
        publisher == '' or
        publisher in author or
        author in publisher
    )
    is_big_pub = not is_indie
    
    # Determine basic rank
    rank = "P5"
    if score >= 68: rank = "P0" # Lowered again to give out more
    elif score >= 58: rank = "P1" # Was 65
    elif score >= 48: rank = "P2" # Was 55
    elif score >= 30: rank = "P3" # Was 40
    elif score >= 15: rank = "P4" # Was 20
    
    if is_big_pub:
        # P0, P1 -> P2 (Capped)
        if rank in ["P0", "P1"]:
            rank = "P2"
            
    return rank

def classify_series_type(count):
    if count == 1: return "Standalone"
    if count <= 3: return "Short Series"
    if count <= 5: return "Series"
    return "Long Series"

def main():
    logger.info(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    # Conversions
    for c in ['Goodreads Rating', 'Goodreads # of Ratings', 'Pages', 'Book Number']:
        df[c] = pd.to_numeric(df[c], errors='coerce')

    # Normalize Series
    df['Series Name'] = df['Series Name'].fillna('NO_SERIES').astype(str).str.strip()
    
    # ---------------------------------------------------------
    # AGGREGATION - FIX: Group by Series Name ONLY
    # ---------------------------------------------------------
    logger.info("Aggregating Series Data...")
    
    groups = df.groupby('Series Name')
    
    rows = []
    
    for series_name, group in groups:
        if series_name in ['NO_SERIES', 'nan']: continue
        
        # Determine the author for this series (mode author)
        author = get_mode(group['Author Name'])
        
        # Sort
        # Try Book Number, fallback Pub Date
        try:
             # Extract first number from string
            group['Book_Num_Clean'] = group['Book Number']
            sorted_g = group.sort_values('Book_Num_Clean')
        except:
            sorted_g = group.sort_values('Publication Date')
            
        first = sorted_g.iloc[0]
        last = sorted_g.iloc[-1]
        
        # High/Low
        rated = group.dropna(subset=['Goodreads Rating'])
        if not rated.empty:
            high = rated.sort_values('Goodreads Rating', ascending=False).iloc[0]
            low = rated.sort_values('Goodreads Rating', ascending=True).iloc[0]
        else:
            high = first; low = first
            
        # Metadata
        pub_name = get_mode(group['Publisher'])
        self_pub = get_mode(group['Self Pub Flag'])
        subj = " | ".join(group['Subjective Analysis'].dropna().unique())
        diff = " | ".join(group['Differentiator'].dropna().unique())
        
        # T100 Logic
        # Granular lists for validation
        feat_list = []
        best_rank_val = 9999
        t100_map = "No List"
        
        for _, b in group.iterrows():
            ranks_str = []
            s_rank = b.get('Sports_Amazon_Rank')
            h_rank = b.get('Hockey_Amazon_Rank')
            
            # Check exist and valid
            if pd.notna(s_rank):
                ranks_str.append(f"Sports #{int(s_rank)}")
                if s_rank < best_rank_val: best_rank_val = s_rank
            if pd.notna(h_rank):
                ranks_str.append(f"Hockey #{int(h_rank)}")
                if h_rank < best_rank_val: best_rank_val = h_rank
                
            if ranks_str:
                feat_list.append(f"{b['Book Name']} ({', '.join(ranks_str)})")
                
        books_val_str = " | ".join(feat_list)
        num_feat = len(feat_list)
        
        # Determine overall bucket
        if best_rank_val <= 10: t100_map = "Top 10" # Generic "Top 10" as per score logic
        elif best_rank_val <= 50: t100_map = "Top 50"
        elif best_rank_val <= 100: t100_map = "Top 100"
        
        # Specific T100 Mapping (Sports/Hockey specific if needed, but usually just one flag)
        # User requested "T100 Mapping" column. Let's use the best bucket.
        if "Sports" in str(first.get('Featured_List', '')): # If originated from Sports
             t100_map = f"Sports {t100_map}"
        elif "Hockey" in str(first.get('Featured_List', '')):
             t100_map = f"Hockey {t100_map}"
        elif t100_map != "No List":
             # Default if unknown source but has rank
             t100_map = f"Amazon {t100_map}"
             
        # Era
        try:
            y = pd.to_datetime(first['Publication Date']).year
            era = "After 2020" if y >= 2020 else "Before 2020"
        except:
            y = np.nan
            era = "Unknown"

        # Adaption Length - FIX: Use 300 page default for missing values
        # Calculate total pages with 300-page default for NaN/0 values
        page_counts = []
        for _, book in group.iterrows():
            pages = book['Pages']
            if pd.isna(pages) or pages == 0:
                page_counts.append(300)  # Default assumption
            else:
                page_counts.append(pages)
        
        total_pages = sum(page_counts)
        adapt_hours = round(total_pages * 0.03, 1) if total_pages > 0 else 0

        # Books List
        books_list = ", ".join(sorted_g['Book Name'].tolist())

        num_books = len(group)
        series_type = classify_series_type(num_books)

        row_data = {
            'Book Series Name': series_name,
            'Author Name': author,
            'Type': series_type,
            'Books in Series': num_books,
            'Total Pages': total_pages,
            'Length of Adaption in Hours': adapt_hours,
            
            'First Book Name': first['Book Name'],
            'First Book Rating': first['Goodreads Rating'],
            'First Book Rating Count': first['Goodreads # of Ratings'],
            
            'Last Book Name': last['Book Name'],
            'Last Book Rating': last['Goodreads Rating'],
            'Last Book Rating Count': last['Goodreads # of Ratings'],
            
            'Highest Rated Book Name': high['Book Name'],
            'Highest Rated Book Rating': high['Goodreads Rating'],
            'Highest Rated Book Rating Count': high['Goodreads # of Ratings'],
            
            'Lowest Rated Book Name': low['Book Name'],
            'Lowest Rated Book Rating': low['Goodreads Rating'],
            'Lowest Rated Book Rating Count': low['Goodreads # of Ratings'],
            
            'Publisher Name': pub_name,
            'Self Pub Flag': self_pub,
            'Subjective Analysis': subj,
            'Differentiator': diff,
            
            'Books_Featured_Rank_Validation': books_val_str,
            'T100_Mapping': t100_map,
            'Num_Books_Featured': num_feat,
            'Series_Era': era,
            'First_Book_Pub_Year': y,
            'Books_In_Series_List': books_list
        }
        
        # Score
        score = calc_commissioning_score(row_data)
        row_data['Commissioning_Score'] = score
        row_data['Commissioning_Rank'] = get_rank_label(row_data, score)
        
        rows.append(row_data)
        
    out_df = pd.DataFrame(rows)
    # Sort by Rank (P1 best)
    out_df = out_df.sort_values(['Commissioning_Score'], ascending=False)
    
    logger.info(f"Generated {len(out_df)} rows.")
    out_df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
