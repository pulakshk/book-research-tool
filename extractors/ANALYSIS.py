#!/usr/bin/env python3
"""
Pocketfm Scripts Analysis - Key Extraction Patterns

This document analyzes the pocketfm scripts to identify superior extraction patterns
for integration into the main pipeline.
"""

# ==============================================================================
# ANALYSIS: Amazon_Top_100_Listing.py
# ==============================================================================

"""
Library: DrissionPage (Chromium automation)
Purpose: Scrape Amazon Top 100 lists with comprehensive field extraction

KEY STRENGTHS vs Current Pipeline:

1. COMPREHENSIVE FIELD EXTRACTION
   Current: Only extracts author, link, rating
   Pocketfm: Extracts 15 fields including:
   - Book series name and number (from RPI sections)
   - Page count (from multiple selectors)
   - Best sellers rank (full hierarchy)
   - Goodreads ratings from Amazon pages
   - Publisher with multiple fallback selectors

2. ROBUST SERIES EXTRACTION
   Lines 224-258: Multiple patterns for series detection
   - RPI icon link (#rpi-icon-link-book_details-series)
   - RPI attribute labels (Book X of Y pattern)
   - Detail bullets fallback
   - Regex: r'Book\s+(\d+)\s+of\s+(\d+)'
   
   RECOMMENDATION: Adopt this multi-selector approach

3. GOODREADS DATA FROM AMAZON
   Lines 211-222: Extracts Goodreads ratings directly from Amazon product pages
   - Selector: #reviewFeatureGroup .gr-review-base
   - Gets both rating and count
   
   RECOMMENDATION: Use this to enrich data without separate Goodreads scraping

4. BEST SELLERS RANK EXTRACTION
   Lines 275-297: Complex logic to extract full ranking hierarchy
   - Handles both #detailBullets_feature_div and nested ul.zg_hrsr
   - Parses category ranks (e.g., "#88 Free in Kindle Store")
   
   RECOMMENDATION: Integrate for "Featured List" field

5. PAGE SCROLL STRATEGY
   Lines 360-371: Incremental scroll-and-wait for dynamic content
   - Scrolls by window.innerHeight
   - Checks for height stabilization
   - 15 iterations max with 0.4s delays
   
   RECOMMENDATION: Use for lazy-loaded content

WEAKNESSES:
- Uses DrissionPage (different from Playwright)
- Designed for listing pages, not individual book enrichment
- No fuzzy matching or normalization

VERDICT: Extract patterns #2, #3, #4, #5 into utility functions, keep Playwright
"""

# ==============================================================================
# ANALYSIS: goodread_book_extra_crawl_copy.py  
# ==============================================================================

"""
Library: Selenium with undetected_chromedriver
Purpose: Deep Goodreads series page scraping (8 books per series)

KEY STRENGTHS vs Current Pipeline:

1. COMPREHENSIVE BOOK METADATA
   Lines 193-275: Extracts 14+ fields per book:
   - Rating, reviews, rating count, editions count
   - Page count, genres (first 5), summary
   - Awards, settings, ISBN, ASIN, publisher
   - Publication date with multiple formats
   
   RECOMMENDATION: Integrate genre, awards, ISBN/ASIN extraction

2. ROBUST GENRE EXTRACTION
   Lines 217-222: Extracts top genres
   - Selector: .BookPageMetadataSection__genres .Button__labelItem
   - Joins with comma separator
   
   RECOMMENDATION: Add to detailed_scrape.py for "Primary Trope" field

3. JSON-LD STRUCTURED DATA
   Lines 173-177: Extracts JSON-LD from script tags
   - Type: application/ld+json
   - Contains numberOfPages and other structured data
   
   RECOMMENDATION: Priority #1 - most reliable data source

4. PUBLICATION INFO PARSING
   Lines 223-226: Robust date extraction
   - Selector: p[data-testid="publicationInfo"]
   - Splits on "published" to get date
   
   RECOMMENDATION: Use this pattern in detailed_scrape.py

5. DETAIL LIST EXTRACTION
   Lines 255-273: Iterates through .CollapsableList .DescListItem
   - Extracts ISBN, ASIN, Settings, Awards, Publisher
   - Type-based dispatch for different data types
   
   RECOMMENDATION: Integrate for publisher/ISBN enrichment

6. SERIES PAGE PARSING
   Lines 83-104: Handles book numbering edge cases
   - Skips Book 0, Book 0.5, Book X-Y ranges
   - Respects primary_works_count limit
   
   RECOMMENDATION: Adopt in series_exhaustion.py

WEAKNESSES:
- Uses Selenium (slower than Playwright)
- Hardcoded for series pages, not individual books
- No title normalization
- Resume logic but no incremental saves

VERDICT: Extract patterns #1, #2, #3, #4, #5 into reusable functions, keep Playwright
"""

# ==============================================================================
# INTEGRATION RECOMMENDATIONS
# ==============================================================================

"""
DECISION: Keep Playwright as primary library
REASON: 
- Fastest (async support)
- Best bot detection evasion with proper config
- Already integrated in pipeline
- DrissionPage/Selenium patterns can be adapted

PHASE 3 ACTION PLAN:

1. Create extractors/amazon_patterns.py
   - extract_series_info_amazon() - Multi-selector series detection
   - extract_goodreads_from_amazon() - GR data from Amazon pages
   - extract_best_sellers_rank() - Full rank hierarchy
   - scroll_for_dynamic_content() - Lazy loading helper

2. Create extractors/goodreads_patterns.py
   - extract_json_ld_data() - Structured data from script tags
   - extract_genres() - Top 5 genres
   - extract_detail_list_items() - ISBN/ASIN/Settings/Awards
   - extract_publication_info() - Robust date parsing
   - filter_series_books() - Skip 0, 0.5, range books

3. Update ultra_recovery.py
   - Use extract_goodreads_from_amazon() to get GR data without separate scrape
   - Use new matcher for better matching

4. Update enrich_by_series.py
   - Use filter_series_books() to avoid bad book numbers
   - Use new fuzzy matcher for book-to-df mapping

5. Update detailed_scrape.py
   - Use extract_json_ld_data() as priority #1 source
   - Use extract_genres() for Primary Trope
   - Use extract_detail_list_items() for Publisher/ISBN

EXPECTED IMPROVEMENTS:
- Publisher field: 18% → >70% (from detail lists)
- Pages field: 10% → >85% (from JSON-LD)
- Primary Trope: 18% → >60% (from genres)
- ISBN/ASIN: 0% → >40% (new fields)
"""

# ==============================================================================
# FIELD POPULATION COMPARISON
# ==============================================================================

"""
Current Pipeline (Playwright only):
- Author: ~70%
- Amazon Link: ~42%
- Goodreads Link: ~9%
- Description: ~19%
- Pages: ~10%
- Publisher: ~18%
- Ratings: ~95% (Amazon), ~18% (Goodreads)

With Pocketfm Patterns Integrated:
- Author: ~85% (+15% from better matching)
- Amazon Link: ~75% (+33% from normalized search)
- Goodreads Link: ~70% (+61% from series enrichment + Amazon GR data)
- Description: ~80% (+61% from detailed scrape)
- Pages: ~85% (+75% from JSON-LD + multiple selectors)
- Publisher: ~70% (+52% from detail lists)
- Ratings: ~95% (Amazon), ~70% (Goodreads) (+52%)
- NEW: Genres/Primary Trope: ~60%
- NEW: ISBN/ASIN: ~40%

TOTAL DATA COMPLETENESS: ~40% → ~75%
"""

if __name__ == "__main__":
    print(__doc__)
