---
name: Book Research Pipeline
description: Comprehensive guide for book data scraping, matching, enrichment, and commissioning analysis using Gemini AI, Playwright, and Amazon data. Covers both original Ice Hockey pipeline and multi-subgenre analysis (10,544 series across 9 subgenres).
---

# Book Research Pipeline Skill

A high-performance comprehensive book data pipeline. Scales using Gemini for metadata enhancement, Playwright for series exhaustion and Amazon bestseller crawling.

## Table of Contents
1. [Pipeline Overview](#pipeline-overview)
2. [Architecture — Original (Ice Hockey)](#architecture--original)
3. [Architecture — Multi-Subgenre Analysis](#architecture--multi-subgenre)
4. [CLI Usage](#cli-usage)
5. [Gemini Enrichment Details](#gemini-enrichment)
6. [Amazon Bestseller Crawling](#amazon-crawling)
7. [Commissioning Score](#commissioning-score)
8. [Configuration](#configuration)

---

## Pipeline Overview

### Purpose
To generate a "Golden Dataset" of self-published book series, enriched with:
- **Metadata**: Publisher, Tropes, Descriptions, Page Counts, Eras (via Gemini 2.5 Flash)
- **Objective Validation**: NYT, Amazon, BookTok, Goodreads Choice cross-referencing
- **Author Contacts**: Email, website, social media, literary agents
- **Amazon Rankings**: Top 100 bestseller mapping across 37 categories
- **Analysis**: P0-P3 Commissioning Scores with 8-component weighted model

### Key Features
- **Multi-Subgenre**: 9 romance subgenres, 10,544 series
- **3-Phase Gemini Enrichment**: Metadata, Validation, Contacts
- **Amazon Stealth Crawling**: 37 categories, paid + free lists
- **Deep Enrichment**: Fill remaining gaps for book-level details and ratings
- **Excel Output**: Per-subgenre sheets + Amazon Top 100 reference tab

---

## Architecture — Original (Ice Hockey)

```
src/
  pipeline/
    config.py       # Centralized Keys & Constants
    data.py         # Robust I/O & Backups
    cleaning.py     # Regex Filters & Deduplication
    scrapers.py     # Playwright + Gemini Series Exhaustion
    enrichment.py   # Async Gemini Metadata Filling
    analysis.py     # Commissioning Logic (P0-P5)
main.py             # Single Entry Point for all phases
```

---

## Architecture — Multi-Subgenre Analysis

Located in `sub genre analysis/`:

| Script | Purpose | Key Tech |
| :--- | :--- | :--- |
| `mega_discovery.py` | Multi-platform series discovery (Goodreads, Google, Reddit, BookTok) | Playwright |
| `multi_platform_discovery.py` | Additional discovery sources | Playwright |
| `selfpub_expansion_pipeline.py` | Self-pub focused expansion | Gemini + Playwright |
| `gemini_series_check.py` | Verify discovered titles are real series | Gemini |
| `enrich_with_goodreads.py` | Scrape ratings, pages, pub dates | Playwright |
| `gemini_enrich_all.py` | 3-phase AI enrichment (metadata, validation, contacts) | Gemini 2.5 Flash |
| `amazon_top100.py` | Amazon bestseller list scraping (26 categories) | Playwright |
| `amazon_full_crawl.py` | Comprehensive Amazon crawl (37 categories, paid+free) | Playwright |
| `final_aggregate_and_score.py` | Score, rank, deduplicate, generate Excel | pandas + openpyxl |
| `deep_enrich.py` | Fill remaining gaps (book details, ratings, contacts) | Gemini 2.5 Flash |
| `cleanup_and_validate.py` | Data quality checks | pandas |
| `quality_audit.py` | Audit data completeness | pandas |
| `merge_and_filter.py` | Merge multiple data sources | pandas |

---

## CLI Usage

### Original Pipeline
```bash
python3 main.py --all        # Full pipeline
python3 main.py --scrape     # Series Exhaustion
python3 main.py --clean      # Filter & Dedupe
python3 main.py --enrich     # Gemini Metadata
python3 main.py --analyze    # Generate Report
```

### Multi-Subgenre Pipeline
```bash
cd "sub genre analysis"

# Gemini Enrichment (3 phases)
python3 gemini_enrich_all.py --phase all   # All phases (~7.5h)
python3 gemini_enrich_all.py --phase 1     # Metadata only (~3.5h)
python3 gemini_enrich_all.py --phase 2     # Validation only (~2.5h)
python3 gemini_enrich_all.py --phase 3     # Contacts only (~1.7h)

# Amazon Crawling
python3 amazon_full_crawl.py               # Full crawl + Excel tab
python3 amazon_full_crawl.py --map-only    # Map existing data only

# Final Scoring & Output
python3 final_aggregate_and_score.py       # Generate scored CSV + Excel

# Deep Enrichment (fill gaps)
python3 deep_enrich.py --phase all         # All phases (~10h)
python3 deep_enrich.py --phase A           # Book details
python3 deep_enrich.py --phase B           # Missing ratings
python3 deep_enrich.py --phase C           # Missing contacts
```

---

## Gemini Enrichment

### Phase 1: Subjective Analysis (batch 50)
Fields: Subjective Analysis, Primary Trope, Series_Era, Type, Total Pages, Differentiator
- Auto-calculates adaptation hours: Total Pages / 33.33
- Exponential backoff for rate limits (5s start, 60s max)

### Phase 2: Objective Validation (batch 30)
Cross-references against: NYT Bestseller, USA Today Bestseller, Amazon Bestseller, BookTok Viral, Goodreads Choice Awards

### Phase 3: Author Contacts (batch 40)
Fields: email, website, Twitter, Instagram, Facebook, BookBub, TikTok, literary agent
- Deduplicates by author name before querying

### Robust JSON Parsing
All Gemini responses parsed with markdown fence stripping, trailing comma removal, regex array/object extraction fallbacks.

---

## Amazon Bestseller Crawling

### Categories (37 total)
- **Romance** (13): Contemporary, Historical, Dark, Paranormal, Sports, Military, Christian, Gothic, Western, New Adult, Multicultural, Romantic Comedy, Romantic Suspense
- **Broader Fiction** (9): Literature & Fiction, Women's Fiction, Mystery/Thriller, Christian Fiction, Historical Fiction, Action & Adventure, etc.
- **Print Books** (6+): Parallel print category lists

### Stealth Measures
- Webdriver property override (`navigator.webdriver = false`)
- Chrome runtime mock (`window.chrome.runtime`)
- Custom user agents, viewport, locale, timezone
- Random delays between requests

### Title/Author Parsing
Amazon concatenates title and author in a single text block. Parser uses:
- `#N` prefix stripping
- Rating pattern split (`X.X out of 5 star`)
- Lowercase-to-uppercase boundary detection for title/author split

---

## Commissioning Score

8-component weighted model (0-100 scale):

| Component | Weight | Tiers |
| :--- | :--- | :--- |
| Adaptation Length | 15% | >1500pg=100, >1000=80, >500=60, >200=40 |
| First Book Rating | 20% | >4.3=100, >4.0=80, >3.7=60, >3.4=40 |
| Rating Volume | 15% | >50K=100, >10K=80, >1K=60, >100=40 |
| Lowest Book Rating | 10% | >4.0=100, >3.7=80, >3.4=60, >3.0=40 |
| Rating Stability | 10% | gap<0.2=100, <0.4=80, <0.6=60, <0.8=40 |
| Series Era | 10% | post-2015=100, 2010-15=80, 2000-10=60 |
| Books in Series | 10% | >10=100, >6=80, >3=60, >1=40 |
| Validation Bonus | 10% | NYT/Amazon/BookTok/GR Choice presence |

**Ranks**: P0 >= 75 (top priority), P1 >= 55, P2 >= 35, P3 < 35

---

## Configuration

### Environment
- `GEMINI_API_KEY` in `.env` file (project root)
- Python 3.9+ with: `pandas`, `openpyxl`, `google-generativeai`, `playwright`

### Key Constants
- `BATCH_SIZE`: 30-50 per Gemini call
- `SAVE_INTERVAL`: Partial save every 5 batches
- `BACKOFF`: 5s initial, 60s max, 5 retries

### Output Files
- `sub genre analysis/output/FINAL_SELFPUB_SCORED.csv` — Master dataset (49 columns)
- `sub genre analysis/output/FINAL_SELFPUB_SCORED.xlsx` — Excel workbook (Combined + 9 subgenre sheets + Amazon Top 100)
- `sub genre analysis/output/subgenre_outputs/` — Per-subgenre CSVs
- `sub genre analysis/output/amazon_full_crawl_raw.csv` — Raw Amazon bestseller data
- `sub genre analysis/output/PRIORITY_SELFPUB_ENRICHED.csv` — Intermediate enriched file
