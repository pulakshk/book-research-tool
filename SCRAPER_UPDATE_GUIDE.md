# Remaining Scraper Updates - Implementation Guide

## Overview
This document provides the specific updates needed for the remaining scrapers to integrate fuzzy matching and enhanced extraction patterns.

## Files to Update

### 1. enrich_by_series.py

**Current Issue**: Uses strict regex matching for book-to-dataframe mapping (lines 183-192)
```python
# CURRENT (STRICT):
mask = (df['Book Name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', str(x).lower()) == norm_b_title)) & \
       (df['Author Name'].apply(lambda x: re.sub(r'[^a-zA-Z0-9]', '', str(x).lower()) == norm_author))
```

**Needed Update**: Use BookMatcher for fuzzy matching
```python
# NEW (FUZZY):
from utils.matcher import BookMatcher
matcher = BookMatcher()

# For each scraped book:
search_book = {'title': scraped_title, 'author': scraped_author}
result = matcher.find_best_match(df, search_book)
if result.matched:
    df_idx = result.index
    # Update fields...
```

**Additional Updates**:
- Line 8: Add imports (text_normalizer, matcher, config, goodreads_patterns)
- Lines 62-72: Use extract_goodreads_comprehensive() for better rating extraction
- Line 183-210: Replace strict matching with BookMatcher.find_best_match()

---

### 2. series_exhaustion.py

**Current Issue**: No validation for book numbers (includes 0, 0.5, ranges)

**Needed Update**: Use filter_series_books()
```python
# Add import:
from extractors.goodreads_patterns import filter_series_books

# When checking book numbers (around line 180):
book_number = extract_book_number(scraped_book_info)
if not filter_series_books(book_number):
    continue  # Skip Book 0, 0.5, ranges
```

**Additional Updates**:
- Add fuzzy matching for duplicate detection (same as enrich_by_series.py)
- Use normalize_title() for better series name matching

---

### 3. core/detailed_scrape.py

**Current Issue**: Basic selectors, no JSON-LD extraction

**Needed Update**: Use goodreads_patterns extractors
```python
# Add imports:
from extractors.goodreads_patterns import (
    extract_json_ld_data,
    extract_genres,
    extract_detail_list_items,
    extract_page_count_goodreads,
    extract_description_goodreads,
    extract_publication_info
)

# In scrape_description function (replace existing logic):
async def scrape_description(page, goodreads_link):
    await page.goto(goodreads_link, ...)
    
    # Priority 1: JSON-LD (most reliable)
    json_data = await extract_json_ld_data(page)
    description = json_data.get('description')
    pages = json_data.get('numberOfPages')
    
    # Priority 2: Specific extractors
    if not description:
        description = await extract_description_goodreads(page)
    if not pages:
        pages = await extract_page_count_goodreads(page)
    
    # Priority 3: Detail lists for publisher, ISBN
    details = await extract_detail_list_items(page)
    publisher = details.get('publisher')
    
    # NEW: Extract genres for Primary Trope
    genres = await extract_genres(page, max_genres=5)
    primary_trope = genres[0] if genres else None
    
    return {
        'description': description,
        'pages': pages,
        'publisher': publisher,
        'primary_trope': primary_trope,
        'genres': ', '.join(genres) if genres else None
    }
```

**Additional Updates**:
- Extract publication info
- Extract ISBN/ASIN
- Add validation before scraping

---

### 4. utils/dataset_manager.py

**Current Issue**: Uses simple regex for deduplication

**Needed Update**: Use BookMatcher for fuzzy deduplication
```python
# In beautify() method (around line 85):
from utils.matcher import BookMatcher

def beautify(self, output_path=None):
    # ... existing code ...
    
    # 5. Deduplication (NEW: Fuzzy with author preservation)
    matcher = BookMatcher()
    self.df = matcher.deduplicate_books(
        self.df, 
        keep='first',
        preserve_different_authors=True  # CRITICAL!
    )
```

**Additional Updates**:
- Add validation integration: `validator.validate_dataset(self.df)`
- Add backup before save: create timestamped backup in backups/auto/
- Update audit() to use config thresholds

---

### 5. orchestrator.py

**Current Issue**: No validation phase, no incremental processing

**Needed Updates**:

**A. Add Validation Phase** (before enrichment):
```python
from utils.validator import validate_file, DataValidator

def orchestrate(self, max_iterations=3):
    # NEW: Pre-flight validation
    logger.info("="*80)
    logger.info("PHASE 0: VALIDATION")
    logger.info("="*80)
    
    report = validate_file(self.manager.csv_path)
    report.print_summary()
    
    if not report.is_valid:
        logger.warning("Dataset has validation errors. Continuing with caution...")
    
    # Continue with existing phases...
```

**B. Add Incremental Processing**:
```python
# Modify script calls to only process missing data:
# e.g., in ultra_recovery.py, only process rows where:
missing_author = df['Author Name'].isna() | (df['Author Name'] == '')
missing_amz = df['Amazon Link'].isna() | (df['Amazon Link'] == '')
target_rows = df[missing_author | missing_amz]
```

**C. Add Backup Before Each Phase**:
```python
from datetime import datetime
import shutil

def backup_before_phase(self, phase_name):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = f"backups/auto/{timestamp}_{phase_name}_before.csv"
    shutil.copy(self.manager.csv_path, backup_path)
    logger.info(f"Backup saved: {backup_path}")
```

---

## Implementation Priority

1. **HIGH PRIORITY** (Fixes critical data issues):
   - dataset_manager.py (fuzzy deduplication)
   - enrich_by_series.py (fuzzy matching for book mapping)
   - ultra_recovery.py ✓ (DONE)

2. **MEDIUM PRIORITY** (Improves data completeness):
   - detailed_scrape.py (JSON-LD + genres)
   - series_exhaustion.py (filter bad book numbers)

3. **LOW PRIORITY** (Improves safety):
   - orchestrator.py (validation + backups)

---

## Testing Checklist

After updates:
- [ ] Run validator on current dataset
- [ ] Test BookMatcher on 10 sample books
- [ ] Run ultra_recovery on 50 books subset
- [ ] Run enrich_by_series on 1 test series
- [ ] Run detailed_scrape on 20 books with GR links
- [ ] Check for duplicate books (same title, different authors preserved?)
- [ ] Verify numeric fields have no commas
- [ ] Benchmark: Before vs After data completeness

---

## Expected Improvements

| Field | Before | After | Method |
|-------|--------|-------|--------|
| Author | ~70% | >85% | Fuzzy matching + better search |
| Amazon Link | ~42% | >75% | Core title search + product page extraction |
| Goodreads Link | ~9% | >70% | Series enrichment + GR from Amazon |
| Pages | ~10% | >85% | JSON-LD + multiple selectors |
| Publisher | ~18% | >70% | Detail lists + Amazon |
| Description | ~19% | >80% | JSON-LD + specific extractor |
| Ratings | ~18% (GR) | >70% | GR from Amazon + series scraping |
| Primary Trope | ~18% | >60% | Genre extraction |

**Overall Completeness: ~40% → ~75%**

---

## Code Snippets Reference

### Fuzzy Book Matching Pattern
```python
from utils.matcher import BookMatcher

matcher = BookMatcher()
for scraped_book in scraped_books:
    search = {
        'title': scraped_book['title'],
        'author': scraped_book['author']
    }
    result = matcher.find_best_match(df, search)
    
    if result.matched:
        df_idx = result.index
        # Update existing row
        df.at[df_idx, 'Goodreads Link'] = scraped_book['link']
        logger.success(f"Matched via {result.strategy}: {result.confidence:.2f}")
    else:
        # Add as new row
        df = pd.concat([df, pd.DataFrame([scraped_book])], ignore_index=True)
```

### JSON-LD First Pattern
```python
from extractors.goodreads_patterns import extract_json_ld_data

# Always try JSON-LD first (most reliable)
json_data = await extract_json_ld_data(page)

description = json_data.get('description')
pages = json_data.get('numberOfPages')
rating = json_data.get('aggregateRating', {}).get('ratingValue')

# Fall back to selectors only if JSON-LD doesn't have the data
if not description:
    description = await extract_description_goodreads(page)
```

### Numeric Cleaning Pattern
```python
# Always clean commas from numeric fields before saving
rating_count_str = "25,410"
rating_count = int(rating_count_str.replace(',', ''))  # 25410

# Apply to all rating counts:
df['Amazon # of Ratings'] = df['Amazon # of Ratings'].astype(str).str.replace(',', '')
df['Amazon # of Ratings'] = pd.to_numeric(df['Amazon # of Ratings'], errors='coerce')
```

---

## Next Steps

1. ✅ Update ultra_recovery.py (DONE)
2. ⏭️ Update enrich_by_series.py (fuzzy matching)
3. ⏭️ Update detailed_scrape.py (JSON-LD + genres)
4. ⏭️ Update series_exhaustion.py (filters)
5. ⏭️ Update dataset_manager.py (fuzzy dedup)
6. ⏭️ Update orchestrator.py (validation + backups)
7. ⏭️ Integration testing
8. ⏭️ Performance benchmarking

