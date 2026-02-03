#!/usr/bin/env python3
"""
Quick fix: Generate Short Synopsis and Primary Trope from existing descriptions.
"""
import pandas as pd
import re

TROPES = {
    'Enemies to Lovers': ['enemies', 'hate', 'rival', 'nemesis', 'hated', 'despise'],
    'Friends to Lovers': ['best friend', 'friends since', 'friendship', 'known each other'],
    'Fake Relationship': ['fake', 'pretend', 'arrangement', 'contract', 'for show'],
    'Second Chance': ['ex', 'past', 'years ago', 'high school sweetheart', 'reunion', 'came back'],
    'Forbidden Love': ['forbidden', "shouldn't", 'off limits', 'wrong', 'taboo'],
    'Forced Proximity': ['stuck', 'stranded', 'roommates', 'snowed in', 'cabin', 'one bed'],
    'Grumpy/Sunshine': ['grumpy', 'sunshine', 'gruff', 'brooding', 'cheerful'],
    'Age Gap': ['older', 'younger', 'age difference', 'years older'],
    "Brother's Best Friend": ["brother's best friend", "sister's best friend", 'off-limits'],
    'Single Dad': ['single dad', 'single father', 'widower', 'his daughter', 'his son'],
    'Secret Baby': ['secret baby', 'pregnant', 'his child', "didn't know"],
    'Sports Romance': ['hockey', 'football', 'baseball', 'basketball', 'athlete', 'player', 'team'],
    'Billionaire': ['billionaire', 'millionaire', 'wealthy', 'rich', 'ceo', 'mogul'],
    'Slow Burn': ['slow burn', 'tension', 'building', 'finally'],
}

def analyze_trope(desc):
    if not desc or pd.isna(desc):
        return None
    desc_lower = str(desc).lower()
    scores = {}
    for trope, keywords in TROPES.items():
        score = sum(1 for kw in keywords if kw in desc_lower)
        if score > 0:
            scores[trope] = score
    return max(scores, key=scores.get) if scores else None

def create_synopsis(desc):
    if not desc or pd.isna(desc):
        return None
    sentences = re.split(r'(?<=[.!?])\s+', str(desc).strip())
    synopsis = ' '.join(sentences[:2])
    return synopsis[:300] if len(synopsis) > 300 else synopsis

if __name__ == "__main__":
    df = pd.read_csv('unified_book_data_enriched_ultra.csv')
    
    # Fix all rows with description
    has_desc = df['Description'].notna() & (df['Description'].astype(str) != 'nan')
    trope_count = 0
    synopsis_count = 0
    
    for idx in df[has_desc].index:
        desc = str(df.at[idx, 'Description'])
        
        # Generate trope if missing
        if pd.isna(df.at[idx, 'Primary Trope']) or str(df.at[idx, 'Primary Trope']) in ['nan', '']:
            trope = analyze_trope(desc)
            if trope:
                df.at[idx, 'Primary Trope'] = trope
                trope_count += 1
        
        # Generate synopsis if missing
        if pd.isna(df.at[idx, 'Short Synopsis']) or str(df.at[idx, 'Short Synopsis']) in ['nan', '']:
            synopsis = create_synopsis(desc)
            if synopsis:
                df.at[idx, 'Short Synopsis'] = synopsis
                synopsis_count += 1
    
    df.to_csv('unified_book_data_enriched_ultra.csv', index=False)
    print(f'✓ Generated {trope_count} Primary Tropes')
    print(f'✓ Generated {synopsis_count} Short Synopses')
