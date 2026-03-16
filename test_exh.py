import pandas as pd
import re
df = pd.read_csv('/Users/pocketfm/Documents/book-research-tool/New genre crawl/Political Drama_Romance_final.csv')
for _, r in df[df['Type'] == 'Standalone'].head(20).iterrows():
    title = r['Book Name'] if 'Book Name' in r else r.get('First Book Name', '')
    print(f"Standalone: {r['Book Series Name']} | Title: {title}")
