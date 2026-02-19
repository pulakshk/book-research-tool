import pandas as pd
import os

FILE = "data/unified_book_data_enriched_final.csv"
df = pd.read_csv(FILE)

# Data for Daydream
daydream_data = {
    'Series Name': 'Maple Hills Series',
    'Author Name': 'Hannah Grace',
    'Book Name': 'Daydream',
    'Book Number': 3,
    'Goodreads Link': 'https://www.goodreads.com/book/show/199020721-daydream',
    'Goodreads # of Ratings': '150000',
    'Goodreads Rating': 4.1,
    'Publication Date': 'August 27, 2024',
    'Pages': 400,
    'Description': 'Henry and Halle Jacobs, a bookish fellow student. Henry navigating his role as hockey team captain and Halle dealing with her academic commitments and writer\'s block.',
    'Primary Trope': 'Hockey Romance, Slow Burn',
    'Amazon Link': 'https://www.amazon.com/dp/B0CXF6Z2Y7',
    'Amazon # of Ratings': '25000',
    'Amazon Rating': 4.5,
    'Publisher': 'Atria Books',
    'Featured List': 'New York Times Bestseller, Amazon Top 100',
    'Status': 'INJECTED',
    'Self Pub Flag': 'Big Pub'
}

# Add only if not already there
if not df[(df['Book Name'] == 'Daydream') & (df['Author Name'] == 'Hannah Grace')].empty:
    print("Daydream already exists.")
else:
    new_row = pd.DataFrame([daydream_data])
    df = pd.concat([df, new_row], ignore_index=True)
    df.to_csv(FILE, index=False)
    print("Injected Daydream (Maple Hills #3).")
