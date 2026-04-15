#!/usr/bin/env python3
import pandas as pd
import os
from loguru import logger

def main():
    file_path = '/Users/pocketfm/Documents/book-research-tool/subgenre-pipeline/genre-crawl/All_9_Subgenres_Scout_Top25_AGGREGATED.csv'
    df = pd.read_csv(file_path)
    
    # 1. First Pass: If a series exists in one subgenre with a URL, copy it to the identical series in another subgenre missing it
    series_url_map = {}
    for _, row in df.dropna(subset=['Goodreads Series URL']).iterrows():
        s_name = str(row['Series Name']).strip().lower()
        if row['Goodreads Series URL'] != '':
            series_url_map[s_name] = {
                'url': row['Goodreads Series URL'],
                'books': row['Total Books in Series'],
                'avg': row['Average GR Rating'],
                'total': row['Total GR Ratings']
            }
            
    fixed_count = 0
    for idx, row in df.iterrows():
        s_name = str(row['Series Name']).strip().lower()
        url = str(row.get('Goodreads Series URL', ''))
        
        if (pd.isna(url) or url == '' or url == 'nan') and s_name in series_url_map:
            df.at[idx, 'Goodreads Series URL'] = series_url_map[s_name]['url']
            df.at[idx, 'Total Books in Series'] = series_url_map[s_name]['books']
            df.at[idx, 'Average GR Rating'] = series_url_map[s_name]['avg']
            df.at[idx, 'Total GR Ratings'] = series_url_map[s_name]['total']
            fixed_count += 1
            logger.info(f"Fixed {row['Series Name']} using crossover data.")
            
    df.to_csv(file_path, index=False)
    logger.success(f"Fixed {fixed_count} crossover series. Re-run complete.")

if __name__ == '__main__':
    main()
