import pandas as pd
from loguru import logger

# Config
INPUT_FILE = "data/unified_book_data_enriched_final.csv"

def audit():
    df = pd.read_csv(INPUT_FILE)
    
    # Filter for rows that have Amazon Ratings (likely backfilled or scraped)
    # AND have Goodreads Ratings (ground truth proxy)
    mask = (df['Amazon # of Ratings'] > 0) & (df['Goodreads # of Ratings'] > 0)
    audit_df = df[mask].copy()
    
    logger.info(f"Auditing {len(audit_df)} books with both Amazon & Goodreads data...")
    
    suspicious = []
    
    for idx, row in audit_df.iterrows():
        amz = float(row['Amazon # of Ratings'])
        gr = float(row['Goodreads # of Ratings'])
        
        # Flag 1: Amazon > Goodreads (Rare, usually Amazon is 10-50% of GR)
        # Exception: Kindle Unlimited heavy books might be close? 
        # But generally if Amazon is 2x Goodreads, it's suspect.
        if amz > gr * 1.5:
            suspicious.append({
                'Book': row['Book Name'],
                'Reason': 'Amazon >> Goodreads',
                'Amazon': amz,
                'Goodreads': gr
            })
            
        # Flag 2: Round Numbers (Gemini Guesses)
        if amz > 1000 and amz % 500 == 0:
             suspicious.append({
                'Book': row['Book Name'],
                'Reason': 'Round Number Estimate',
                'Amazon': amz,
                'Goodreads': gr
            })
            
    # Report
    if suspicious:
        logger.warning(f"Found {len(suspicious)} potential hallucination/estimation artifacts.")
        for s in suspicious[:10]:
            logger.warning(f"{s['Book']}: Amz={s['Amazon']}, GR={s['Goodreads']} ({s['Reason']})")
    else:
        logger.success("No obvious data anomalies found.")

if __name__ == "__main__":
    audit()
