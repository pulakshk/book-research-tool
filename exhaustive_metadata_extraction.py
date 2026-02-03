#!/usr/bin/env python3
"""
EXHAUSTIVE Metadata Extraction from Goodreads and Amazon.
Extracts ALL fields:
- Description, Pages, Publisher, Ratings (GR + AMZ), # of Ratings
- Genre/Primary Subgenre (from GR shelves)
- Primary Trope (analyzed from description)
- Featured List / Top Lists (from Amazon Best Sellers Rank)
- Short Synopsis (first 2 sentences of description)
- Self Pub Flag (from publisher name analysis)
- Publication Date
"""
import asyncio
import pandas as pd
import os
import re
import random
import sys
from loguru import logger
from playwright.async_api import async_playwright

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.bridge_utils import safe_goto
from extractors.amazon_patterns import extract_amazon_comprehensive

INPUT_FILE = "unified_book_data_enriched_ultra.csv"
OUTPUT_FILE = "unified_book_data_enriched_ultra.csv"

# Known self-pub indicators
SELF_PUB_KEYWORDS = [
    'independently published', 'self-published', 'createspace', 'draft2digital',
    'smashwords', 'kindle direct', 'kdp', 'lulu', 'blurb', 'author house',
    'authorhouse', 'xlibris', 'iuniverse', 'trafford', 'balboa press'
]

# Known traditional publishers
TRADITIONAL_PUBLISHERS = [
    'penguin', 'random house', 'harpercollins', 'simon & schuster', 'macmillan',
    'hachette', 'scholastic', 'wiley', 'pearson', 'mcgraw', 'sourcebooks',
    'berkley', 'avon', 'ballantine', 'bantam', 'dell', 'tor', 'forge',
    'st. martin', 'entangled', 'montlake', 'forever', 'grand central',
    'kensington', 'zebra', 'dafina', 'carina', 'harlequin', 'mira', 'mills & boon'
]

# Romance trope keywords
TROPES = {
    'Enemies to Lovers': ['enemies', 'hate', 'rival', 'nemesis', 'hated', 'despise'],
    'Friends to Lovers': ['best friend', 'friends since', 'friendship', 'known each other'],
    'Fake Relationship': ['fake', 'pretend', 'arrangement', 'contract', 'for show'],
    'Second Chance': ['ex', 'past', 'years ago', 'high school sweetheart', 'reunion', 'came back'],
    'Forbidden Love': ['forbidden', 'shouldn\'t', 'off limits', 'wrong', 'taboo'],
    'Forced Proximity': ['stuck', 'stranded', 'roommates', 'snowed in', 'cabin', 'one bed'],
    'Grumpy/Sunshine': ['grumpy', 'sunshine', 'gruff', 'brooding', 'cheerful'],
    'Age Gap': ['older', 'younger', 'age difference', 'years older'],
    'Brother\'s Best Friend': ['brother\'s best friend', 'sister\'s best friend', 'off-limits'],
    'Single Dad': ['single dad', 'single father', 'widower', 'his daughter', 'his son'],
    'Secret Baby': ['secret baby', 'pregnant', 'his child', 'didn\'t know'],
    'Sports Romance': ['hockey', 'football', 'baseball', 'basketball', 'athlete', 'player', 'team'],
    'Billionaire': ['billionaire', 'millionaire', 'wealthy', 'rich', 'ceo', 'mogul'],
    'Slow Burn': ['slow burn', 'tension', 'building', 'finally'],
}

def analyze_trope(description):
    """Analyze description to identify primary trope."""
    if not description or pd.isna(description):
        return None
    
    desc_lower = str(description).lower()
    trope_scores = {}
    
    for trope, keywords in TROPES.items():
        score = sum(1 for kw in keywords if kw in desc_lower)
        if score > 0:
            trope_scores[trope] = score
    
    if trope_scores:
        return max(trope_scores, key=trope_scores.get)
    return None

def determine_self_pub(publisher):
    """Determine if publisher is self-published."""
    if not publisher or pd.isna(publisher):
        return None
    
    pub_lower = str(publisher).lower()
    
    for kw in SELF_PUB_KEYWORDS:
        if kw in pub_lower:
            return 'Yes'
    
    for pub in TRADITIONAL_PUBLISHERS:
        if pub in pub_lower:
            return 'No'
    
    # If unknown, check for author name pattern (often self-pub)
    # But default to None if unclear
    return None

def create_short_synopsis(description):
    """Create short synopsis from first 2 sentences of description."""
    if not description or pd.isna(description):
        return None
    
    desc = str(description).strip()
    # Split by sentence endings
    sentences = re.split(r'(?<=[.!?])\s+', desc)
    if sentences:
        synopsis = ' '.join(sentences[:2])
        return synopsis[:300] if len(synopsis) > 300 else synopsis
    return desc[:200]

async def get_new_page(browser):
    context = await browser.new_context(user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)")
    page = await context.new_page()
    return context, page

async def extract_metadata_from_gr(page, gr_link):
    """Extract comprehensive metadata from Goodreads book page."""
    result = {}
    try:
        if not await safe_goto(page, gr_link):
            return result
        
        await page.wait_for_load_state("domcontentloaded")
        await asyncio.sleep(0.8)
        
        # Description
        desc_elem = await page.query_selector('div[data-testid="description"] span.Formatted')
        if not desc_elem:
            desc_elem = await page.query_selector('div.DetailsLayoutRightParagraph span.Formatted')
        if not desc_elem:
            # Fallback: try clicking "more" button first
            more_btn = await page.query_selector('button[aria-label="Book description"]')
            if more_btn:
                await more_btn.click()
                await asyncio.sleep(0.3)
                desc_elem = await page.query_selector('div[data-testid="description"] span.Formatted')
        
        if desc_elem:
            desc = await desc_elem.inner_text()
            if desc and len(desc) > 20:
                result['description'] = desc.strip()[:3000]
        
        # Rating + Rating Count
        rating_elem = await page.query_selector('div.RatingStatistics__rating')
        if rating_elem:
            rating_text = await rating_elem.inner_text()
            try:
                result['gr_rating'] = float(rating_text.strip())
            except:
                pass
        
        # Rating count
        count_elem = await page.query_selector('span[data-testid="ratingsCount"]')
        if count_elem:
            count_text = await count_elem.inner_text()
            count_clean = re.sub(r'[^\d]', '', count_text)
            if count_clean:
                result['gr_rating_count'] = int(count_clean)
        
        # Pages
        all_text = await page.inner_text('body')
        pages_match = re.search(r'(\d+)\s*pages', all_text, re.I)
        if pages_match and 50 < int(pages_match.group(1)) < 5000:
            result['pages'] = int(pages_match.group(1))
        
        # Publication info (Publisher + Date)
        pub_elem = await page.query_selector('p[data-testid="publicationInfo"]')
        if pub_elem:
            pub_text = await pub_elem.inner_text()
            # Extract publisher: "Published Month Day, Year by Publisher"
            pub_match = re.search(r'by\s+(.+?)(?:\s*$|\s*\()', pub_text)
            if pub_match:
                result['publisher'] = pub_match.group(1).strip()
            # Extract date
            date_match = re.search(r'Published\s+([A-Za-z]+\s+\d+,?\s*\d{4})', pub_text)
            if date_match:
                result['publication_date'] = date_match.group(1)
        
        # Genre from shelves
        shelves = await page.query_selector_all('a.BookPageMetadataSection__genreButton span.Button__labelItem')
        genres = []
        for shelf in shelves[:5]:
            genre = await shelf.inner_text()
            if genre and genre not in ['Audiobook', 'Kindle', 'ebook']:
                genres.append(genre.strip())
        if genres:
            result['genre'] = ', '.join(genres[:3])
        
        # Author (backup)
        author_elem = await page.query_selector('a.ContributorLink span[data-testid="name"]')
        if author_elem:
            result['author'] = (await author_elem.inner_text()).strip()
        
        return result
        
    except Exception as e:
        logger.warning(f"    - GR extraction error: {e}")
        return result

async def extract_publisher_from_page(page):
    """Extract publisher from current Amazon page using robust selectors."""
    pub_selectors = [
        "#rpi-attribute-book_details-publisher .rpi-attribute-value",
        "#rpi-attribute-book_details-publisher",
        "#detailBullets_feature_div li:has-text('Publisher') span:last-child",
        "li:has-text('Publisher') span:nth-child(2)",
    ]
    for sel in pub_selectors:
        try:
            pub_el = await page.query_selector(sel)
            if pub_el:
                text = (await pub_el.text_content() or "").strip()
                text = re.sub(r'(?i)Publisher\s*[:‏‎\s]*', '', text).strip()
                text = re.sub(r'^[:‏‎\s]+', '', text).strip()
                text = re.split(r'\s*\(', text)[0].strip()
                if text and len(text) > 2 and text not in [':', 'Publisher']:
                    return text[:100]
        except:
            continue
    return None

async def extract_metadata_from_amazon(page, amz_link):
    """Extract comprehensive metadata from Amazon using existing patterns."""
    result = {}
    try:
        if not await safe_goto(page, amz_link):
            return result
        
        # Use the comprehensive extractor from amazon_patterns.py
        data = await extract_amazon_comprehensive(page, scroll_first=True)
        
        if data.get('amazon_rating'):
            try:
                result['amz_rating'] = float(data['amazon_rating'])
            except:
                pass
        
        if data.get('amazon_rating_count'):
            try:
                result['amz_rating_count'] = int(data['amazon_rating_count'])
            except:
                pass
        
        if data.get('pages'):
            try:
                result['pages'] = int(data['pages'])
            except:
                pass
        
        if data.get('publisher'):
            result['publisher'] = data['publisher']
        
        # CRITICAL: Try paperback/hardcover edition for better publisher info
        # Kindle editions often lack publisher data for self-pub books
        if not result.get('publisher'):
            physical_selectors = [
                "li.swatchElement:has-text('Paperback') a",
                "li.swatchElement:has-text('Hardcover') a",
                "#tmm-grid-swatch-paperback a",
                "#tmm-grid-swatch-hardcover a",
            ]
            for sel in physical_selectors:
                try:
                    el = await page.query_selector(sel)
                    if el:
                        href = await el.get_attribute("href")
                        if href and "javascript:void" not in href:
                            physical_url = href if href.startswith("http") else f"https://www.amazon.com{href}"
                            logger.debug(f"    - Clicking to physical edition for publisher...")
                            if await safe_goto(page, physical_url):
                                await asyncio.sleep(0.5)
                                pub = await extract_publisher_from_page(page)
                                if pub:
                                    result['publisher'] = pub
                                    logger.success(f"    ✓ Publisher from physical: {pub}")
                            break
                except:
                    continue
        
        # FEATURED LIST & TOP LISTS from Best Sellers Rank
        if data.get('best_sellers_rank'):
            rank_str = data['best_sellers_rank']
            result['top_lists'] = rank_str
            parts = rank_str.split(' | ')
            for part in parts:
                if '#' in part and 'in' in part.lower():
                    result['featured_list'] = part.strip()
                    break
        
        if data.get('goodreads_rating'):
            try:
                result['gr_rating'] = float(data['goodreads_rating'])
            except:
                pass
        
        if data.get('goodreads_rating_count'):
            try:
                result['gr_rating_count'] = int(data['goodreads_rating_count'])
            except:
                pass
        
        # Description from Amazon
        desc_elem = await page.query_selector('#bookDescription_feature_div span, #productDescription p')
        if desc_elem:
            desc = await desc_elem.inner_text()
            if desc and len(desc) > 30:
                result['description'] = desc.strip()[:3000]
        
        if data.get('series_name'):
            result['series_name'] = data['series_name']
        
        if data.get('book_number'):
            result['book_number'] = data['book_number']
        
        if data.get('total_books_in_series'):
            result['total_books'] = data['total_books_in_series']
        
        return result
        
    except Exception as e:
        logger.warning(f"    - AMZ extraction error: {e}")
        return result

async def extraction_worker(worker_id, browser, queue, df, lock):
    context, page = await get_new_page(browser)
    count = 0
    
    try:
        while not queue.empty():
            try:
                idx = queue.get_nowait()
            except:
                break
            
            row = df.loc[idx]
            title = str(row.get('Book Name', ''))[:40]
            gr_link = str(row.get('Goodreads Link', ''))
            amz_link = str(row.get('Amazon Link', ''))
            
            logger.info(f"[W{worker_id}] Processing: {title}...")
            
            # Extract from both sources
            gr_data = {}
            amz_data = {}
            
            if gr_link and 'goodreads.com' in gr_link:
                gr_data = await extract_metadata_from_gr(page, gr_link)
            
            if amz_link and 'amazon.com' in amz_link:
                amz_data = await extract_metadata_from_amazon(page, amz_link)
            
            # Merge and update
            async with lock:
                updated = False
                
                # Description (prefer GR)
                desc = gr_data.get('description') or amz_data.get('description')
                if desc and (pd.isna(df.at[idx, 'Description']) or str(df.at[idx, 'Description']) in ['nan', '']):
                    df.at[idx, 'Description'] = desc
                    updated = True
                    
                    # Derive Short Synopsis
                    if pd.isna(df.at[idx, 'Short Synopsis']) or str(df.at[idx, 'Short Synopsis']) in ['nan', '']:
                        df.at[idx, 'Short Synopsis'] = create_short_synopsis(desc)
                    
                    # Derive Primary Trope
                    if pd.isna(df.at[idx, 'Primary Trope']) or str(df.at[idx, 'Primary Trope']) in ['nan', '']:
                        trope = analyze_trope(desc)
                        if trope:
                            df.at[idx, 'Primary Trope'] = trope
                
                # Pages
                pages = gr_data.get('pages') or amz_data.get('pages')
                if pages and (pd.isna(df.at[idx, 'Pages']) or df.at[idx, 'Pages'] == 0):
                    df.at[idx, 'Pages'] = pages
                    updated = True
                
                # Publisher
                publisher = gr_data.get('publisher') or amz_data.get('publisher')
                if publisher and (pd.isna(df.at[idx, 'Publisher']) or str(df.at[idx, 'Publisher']) in ['nan', '']):
                    df.at[idx, 'Publisher'] = publisher
                    # Derive Self Pub Flag
                    if pd.isna(df.at[idx, 'Self Pub flag']) or str(df.at[idx, 'Self Pub flag']) in ['nan', '']:
                        self_pub = determine_self_pub(publisher)
                        if self_pub:
                            df.at[idx, 'Self Pub flag'] = self_pub
                    updated = True
                
                # Ratings
                gr_rating = gr_data.get('gr_rating') or amz_data.get('gr_rating')
                if gr_rating and (pd.isna(df.at[idx, 'Goodreads Rating']) or df.at[idx, 'Goodreads Rating'] == 0):
                    df.at[idx, 'Goodreads Rating'] = gr_rating
                    updated = True
                
                amz_rating = amz_data.get('amz_rating')
                if amz_rating and (pd.isna(df.at[idx, 'Amazon Rating']) or df.at[idx, 'Amazon Rating'] == 0):
                    df.at[idx, 'Amazon Rating'] = amz_rating
                    updated = True
                
                # Rating Counts
                gr_count = gr_data.get('gr_rating_count') or amz_data.get('gr_rating_count')
                if gr_count and (pd.isna(df.at[idx, 'Goodreads # of Ratings']) or df.at[idx, 'Goodreads # of Ratings'] == 0):
                    df.at[idx, 'Goodreads # of Ratings'] = gr_count
                    updated = True
                
                amz_count = amz_data.get('amz_rating_count')
                if amz_count and (pd.isna(df.at[idx, 'Amazon # of Ratings']) or df.at[idx, 'Amazon # of Ratings'] == 0):
                    df.at[idx, 'Amazon # of Ratings'] = amz_count
                    updated = True
                
                # Genre / Subgenre
                genre = gr_data.get('genre')
                if genre and (pd.isna(df.at[idx, 'Primary Subgenre']) or str(df.at[idx, 'Primary Subgenre']) in ['nan', '']):
                    df.at[idx, 'Primary Subgenre'] = genre
                    updated = True
                
                # Featured List and Top Lists
                if amz_data.get('featured_list') and (pd.isna(df.at[idx, 'Featured List']) or str(df.at[idx, 'Featured List']) in ['nan', '']):
                    df.at[idx, 'Featured List'] = amz_data['featured_list']
                    updated = True
                
                if amz_data.get('top_lists') and (pd.isna(df.at[idx, 'Top Lists']) or str(df.at[idx, 'Top Lists']) in ['nan', '']):
                    df.at[idx, 'Top Lists'] = amz_data['top_lists']
                    updated = True
                
                # Publication Date
                pub_date = gr_data.get('publication_date')
                if pub_date and (pd.isna(df.at[idx, 'Publication Date']) or str(df.at[idx, 'Publication Date']) in ['nan', '']):
                    df.at[idx, 'Publication Date'] = pub_date
                    updated = True
                
                # Series info (from Amazon)
                if amz_data.get('series_name') and (pd.isna(df.at[idx, 'Series Name']) or str(df.at[idx, 'Series Name']) in ['nan', '']):
                    df.at[idx, 'Series Name'] = amz_data['series_name']
                    updated = True
                
                if amz_data.get('book_number') and (pd.isna(df.at[idx, 'Book Number']) or df.at[idx, 'Book Number'] == 0):
                    df.at[idx, 'Book Number'] = amz_data['book_number']
                    updated = True
                
                if amz_data.get('total_books') and (pd.isna(df.at[idx, 'Total Books in Series']) or df.at[idx, 'Total Books in Series'] == 0):
                    df.at[idx, 'Total Books in Series'] = amz_data['total_books']
                    updated = True
                
                # Author backup
                author = gr_data.get('author')
                if author and (pd.isna(df.at[idx, 'Author Name']) or str(df.at[idx, 'Author Name']) in ['nan', '']):
                    df.at[idx, 'Author Name'] = author
                    updated = True
                
                if updated:
                    logger.success(f"  [W{worker_id}] ✓ Updated: {title}")
                    df.at[idx, 'Status'] = 'METADATA_COMPLETE'
            
            count += 1
            queue.task_done()
            
            if count % 15 == 0:
                async with lock:
                    df.to_csv(OUTPUT_FILE, index=False)
                    logger.info(f"  [Checkpoint] Saved {count} processed")
            
            await asyncio.sleep(random.uniform(0.8, 1.8))
    finally:
        await context.close()

async def run_exhaustive_extraction():
    if not os.path.exists(INPUT_FILE):
        logger.error(f"{INPUT_FILE} not found!")
        return
    
    df = pd.read_csv(INPUT_FILE)
    
    # Target: Books with any missing key metadata
    needs_processing = (
        (df['Description'].isna() | (df['Description'].astype(str) == 'nan')) |
        (df['Pages'].isna() | (df['Pages'] == 0)) |
        (df['Publisher'].isna() | (df['Publisher'].astype(str) == 'nan')) |
        (df['Goodreads Rating'].isna() | (df['Goodreads Rating'] == 0)) |
        (df['Amazon Rating'].isna() | (df['Amazon Rating'] == 0)) |
        (df['Primary Trope'].isna() | (df['Primary Trope'].astype(str) == 'nan')) |
        (df['Primary Subgenre'].isna() | (df['Primary Subgenre'].astype(str) == 'nan')) |
        (df['Featured List'].isna() | (df['Featured List'].astype(str) == 'nan')) |
        (df['Short Synopsis'].isna() | (df['Short Synopsis'].astype(str) == 'nan'))
    )
    
    rows_to_process = df[needs_processing].index.tolist()
    
    if not rows_to_process:
        logger.success("No books need metadata extraction!")
        return
    
    logger.info(f"Starting EXHAUSTIVE metadata extraction for {len(rows_to_process)} books...")
    logger.info("Fields: Description, Pages, Publisher, Ratings, # of Ratings, Genre, Trope, Lists, Synopsis, Self-Pub")
    
    queue = asyncio.Queue()
    for idx in rows_to_process:
        await queue.put(idx)
    
    output_lock = asyncio.Lock()
    
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        # 8 workers (balanced for GR + AMZ per book)
        workers = [extraction_worker(i, browser, queue, df, output_lock) for i in range(8)]
        await asyncio.gather(*workers)
        await browser.close()
    
    df.to_csv(OUTPUT_FILE, index=False)
    
    # Final report
    total = len(df)
    print("\n" + "="*60)
    print("EXHAUSTIVE METADATA EXTRACTION COMPLETE")
    print("="*60)
    key_fields = ['Description', 'Pages', 'Publisher', 'Amazon Rating', 'Goodreads Rating',
                  'Primary Trope', 'Primary Subgenre', 'Featured List', 'Short Synopsis', 'Self Pub flag']
    for col in key_fields:
        if col in df.columns:
            if col in ['Description', 'Publisher', 'Primary Trope', 'Primary Subgenre', 'Featured List', 'Short Synopsis', 'Self Pub flag']:
                missing = df[col].isna().sum() + (df[col].astype(str) == 'nan').sum()
            else:
                missing = df[col].isna().sum() + (df[col] == 0).sum()
            pct = missing / total * 100
            status = '✅' if pct < 20 else '⚠️' if pct < 50 else '❌'
            print(f"  {status} {col:20s}: {100-pct:5.1f}% complete")

if __name__ == "__main__":
    asyncio.run(run_exhaustive_extraction())
