import pandas as pd
import numpy as np
from loguru import logger

INPUT_FILE = "data/unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "book_series_analysis.csv"

def get_mode(series):
    m = series.mode()
    if not m.empty:
        return m.iloc[0]
    return None

def main():
    logger.info(f"Loading {INPUT_FILE}...")
    df = pd.read_csv(INPUT_FILE)
    
    # Pre-processing
    # Ensure numeric ratings
    df['Goodreads Rating'] = pd.to_numeric(df['Goodreads Rating'], errors='coerce')
    df['Goodreads # of Ratings'] = pd.to_numeric(df['Goodreads # of Ratings'], errors='coerce')
    
    # Clean Series Name
    df['Series Name'] = df['Series Name'].fillna('NO_SERIES').astype(str).str.strip()
    
    # ---------------------------------------------------------
    # AGGREGATION
    # ---------------------------------------------------------
    logger.info("Aggregating Series Data...")
    
    series_groups = df.groupby(['Series Name', 'Author Name'])
    
    output_rows = []
    
    for (series, author), group in series_groups:
        if series == 'NO_SERIES' or series == 'nan':
            continue
            
        # -----------------------------------------------------
        # 1. Series Basics
        # -----------------------------------------------------
        # Sort for First/Last
        try:
            # Try sorting by Book Number
            group['Book_Num_Clean'] = pd.to_numeric(group['Book Number'].astype(str).str.extract(r'(\d+)')[0], errors='coerce')
            sorted_g = group.sort_values('Book_Num_Clean')
        except:
            # Fallback to Pub Date
            sorted_g = group.sort_values('Publication Date')
            
        first_book = sorted_g.iloc[0]
        last_book = sorted_g.iloc[-1]
        
        # Sort for Ratings
        rated_books = group.dropna(subset=['Goodreads Rating'])
        if not rated_books.empty:
            highest_book = rated_books.sort_values('Goodreads Rating', ascending=False).iloc[0]
            lowest_book = rated_books.sort_values('Goodreads Rating', ascending=True).iloc[0]
        else:
            highest_book = first_book # Placeholder
            lowest_book = first_book
            
        # -----------------------------------------------------
        # 2. Aggregations (Concatenate Text, Mode Category)
        # -----------------------------------------------------
        publisher = get_mode(group['Publisher'])
        self_pub = get_mode(group['Self Pub Flag'])
        
        # Concatenate unique non-null subjective analysis
        subj = " | ".join(group['Subjective Analysis'].dropna().unique())
        diff = " | ".join(group['Differentiator'].dropna().unique())
        
        # -----------------------------------------------------
        # 3. T100 Analysis
        # -----------------------------------------------------
        # Collect books in lists
        # Featured_List col has "Sports Amazon Top 10", etc.
        # We need specific granular lists: Sports and Hockey
        
        sports_books = group.dropna(subset=['Sports_Amazon_Rank'])
        hockey_books = group.dropna(subset=['Hockey_Amazon_Rank'])
        
        # "Books_Featured_With_Ranks" List
        # Format: "Title (#Rank in Sports), Title (#Rank in Hockey)"
        feat_list = []
        for _, b in group.iterrows():
            ranks = []
            if pd.notna(b.get('Sports_Amazon_Rank')):
                ranks.append(f"Sports #{int(b['Sports_Amazon_Rank'])}")
            if pd.notna(b.get('Hockey_Amazon_Rank')):
                ranks.append(f"Hockey #{int(b['Hockey_Amazon_Rank'])}")
                
            if ranks:
                feat_list.append(f"{b['Book Name']} ({', '.join(ranks)})")
                
        books_featured_str = " | ".join(feat_list)
        num_featured = len(feat_list)
        
        # Determine Status (Highest Bucket)
        def get_best_status(col_name):
            ranks = group[col_name].dropna()
            if ranks.empty: return "No List"
            best = ranks.min()
            if best <= 10: return "Top 10"
            if best <= 50: return "Top 50"
            return "Top 100"
            
        sports_status = get_best_status('Sports_Amazon_Rank')
        hockey_status = get_best_status('Hockey_Amazon_Rank')
        
        # -----------------------------------------------------
        # 4. Era
        # -----------------------------------------------------
        try:
            d = pd.to_datetime(first_book['Publication Date'])
            year = d.year
            era = "After 2020" if year >= 2020 else "Before 2020"
        except:
            year = ""
            era = "Unknown"
            
        # -----------------------------------------------------
        # 5. Build Row
        # -----------------------------------------------------
        row = {
            'Book Series Name': series,
            'Author Name': author,
            'Type': 'Series', # Could deduce 'Standalone' if count=1 but mostly series here
            'Books in Series': len(group),
            'Total Pages': group['Pages'].sum(),
            'Length of Adaption in Hours': "", # Placeholder/Derived
            
            # First Book
            'First Book Name': first_book['Book Name'],
            'First Book Rating': first_book['Goodreads Rating'],
            'First Book Rating Count': first_book['Goodreads # of Ratings'],
            
            # Last Book
            'Last Book Name': last_book['Book Name'],
            'Last Book Rating': last_book['Goodreads Rating'],
            'Last Book Rating Count': last_book['Goodreads # of Ratings'],
            
            # Highest Rated
            'Highest Rated Book Name': highest_book['Book Name'],
            'Highest Rated Book Rating': highest_book['Goodreads Rating'],
            'Highest Rated Book Rating Count': highest_book['Goodreads # of Ratings'],
            
            # Lowest Rated
            'Lowest Rated Book Name': lowest_book['Book Name'],
            'Lowest Rated Book Rating': lowest_book['Goodreads Rating'],
            'Lowest Rated Book Rating Count': lowest_book['Goodreads # of Ratings'],
            
            # Metadata
            'Publisher Name': publisher,
            'Self Pub Flag': self_pub,
            'Subjective Analysis': subj,
            'Differentiator': diff,
            'Commissioning Rank': "", # Placeholder
            
            # T100
            'Books_Featured_With_Ranks': books_featured_str,
            'Sports_List_Status': sports_status,
            'Hockey_List_Status': hockey_status,
            'Num_Books_Featured': num_featured,
            
            # Era
            'Series_Era': era,
            'First_Book_Pub_Year': year
        }
        output_rows.append(row)
        
    # Convert to DF
    out_df = pd.DataFrame(output_rows)
    
    # Sort: Prioritize Featured Series, then Rating Count
    out_df = out_df.sort_values(['Num_Books_Featured', 'First Book Rating Count'], ascending=[False, False])
    
    logger.info(f"Generated {len(out_df)} rows.")
    out_df.to_csv(OUTPUT_FILE, index=False)
    logger.success(f"Saved to {OUTPUT_FILE}")

if __name__ == "__main__":
    main()
