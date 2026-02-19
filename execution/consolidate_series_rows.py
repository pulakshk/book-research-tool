import pandas as pd
from loguru import logger

FILE = "series_commissioning_analysis.csv"

def main():
    logger.info(f"Loading {FILE}...")
    df = pd.read_csv(FILE)
    
    # Specific fix for Lara Bailey
    # Merge 'Billionaire Rules' and 'The Billionaire Rules'
    mask = df['Author Name'] == 'Lara Bailey'
    bailey = df[mask].copy()
    
    if len(bailey) > 1:
        logger.info("Consolidating Lara Bailey rows...")
        # Take the "The Billionaire Rules" series name
        target_name = "The Billionaire Rules"
        
        # Sum books
        total_books = bailey['Total Books'].sum()
        total_t100 = bailey['Num_Books_In_Top_100'].sum()
        
        # Combine list flags (Prioritize Top 10 > 50 > 100 > No List)
        ranks = bailey['Mapped_Combined_Rank_Flag'].tolist()
        best_rank = "No List"
        for r in ranks:
            if "Top 10" in str(r): best_rank = r; break
            elif "Top 50" in str(r) and "Top 10" not in best_rank: best_rank = r
            elif "Top 100" in str(r) and "Top" not in best_rank: best_rank = r
            
        # Pub Date (Min)
        first_pub = bailey['First_Book_Year'].min()
        era = "After 2020" if first_pub >= 2020 else "Before 2020"
        
        # Create consolidated row (Using the data from the 'main' row if possible)
        # We'll just update the first row index and drop the rest
        idx_to_keep = bailey.index[0]
        idx_to_drop = bailey.index[1:]
        
        df.at[idx_to_keep, 'Series Name'] = target_name
        df.at[idx_to_keep, 'Total Books'] = total_books
        df.at[idx_to_keep, 'Num_Books_In_Top_100'] = total_t100
        df.at[idx_to_keep, 'Mapped_Combined_Rank_Flag'] = best_rank
        df.at[idx_to_keep, 'Series_Era'] = era
        df.at[idx_to_keep, 'First_Book_Year'] = first_pub
        
        # Drop others
        df = df.drop(idx_to_drop)
        logger.success("Consolidated Lara Bailey.")
        
    df.to_csv(FILE, index=False)
    logger.success(f"Saved cleaned {FILE}")

if __name__ == "__main__":
    main()
