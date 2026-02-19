import pandas as pd
from loguru import logger

FILE = "data/unified_book_data_enriched_ultra.csv"

def main():
    logger.info(f"Loading {FILE}...")
    df = pd.read_csv(FILE)
    
    # Fix Lara Bailey
    # Normalize 'Billionaire Rules' -> 'The Billionaire Rules'
    mask = (df['Author Name'] == 'Lara Bailey') & (df['Series Name'].fillna('').str.contains('Billionaire Rules', case=False))
    
    count = mask.sum()
    if count > 0:
        logger.info(f"Normalizing {count} Lara Bailey rows...")
        df.loc[mask, 'Series Name'] = "The Billionaire Rules"
        
    df.to_csv(FILE, index=False)
    logger.success(f"Updated {FILE}")

if __name__ == "__main__":
    main()
