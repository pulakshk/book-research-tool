
# Book Data Enrichment Project Guide

This project is a high-performance, autonomous scraping and recovery pipeline designed to enrich book datasets with metadata from Amazon, Goodreads, and Gemini AI. It covers both the original Ice Hockey Romance pipeline and the expanded Multi-Subgenre Analysis pipeline.

---

## Part 1: Original Pipeline (Ice Hockey Romance)

### The Autonomous Pipeline (Turbo Mode)

The project is managed by a self-healing **Orchestrator** that coordinates multiple scrapers to achieve 100% data coverage.

| Component | Script | Goal | Concurrency |
| :--- | :--- | :--- | :--- |
| **Orchestrator** | `orchestrator.py` | The master "brain". Audits gaps and triggers scripts in iterative loops. | 1 Master |
| **Turbo Recovery** | `ultra_recovery.py` | Finds missing Authors, Amazon Links, and Amazon Ratings. | 4 Workers |
| **Series Bulk** | `enrich_by_series.py` | Scrapes entire Goodreads Series pages for bulk mapping. | 1 Worker |
| **Deep Scrape** | `core/detailed_scrape.py` | Extracts Pages, Descriptions, Publisher, and Pub Dates. | 6 Workers |

### Key Performance Stats
- **Sequential Speed**: ~15 books/min
- **Turbo Speed**: **~50-60 books/min** (4x - 6x faster)
- **Target Completion**: ~1.5 hours for the full dataset.

### Project Structure (Original)

- `orchestrator.py`: Main entry point for the autonomous cycle.
- `core/on_demand_discoverer.py`: CLI tool for seed-based series expansion.
- `utils/dataset_manager.py`: Central utility for auditing, alignment, and beautification.
- `src/pipeline/`: Core logic modules (config, data, cleaning, scrapers, enrichment, analysis).
- `main.py`: CLI entry point for all phases.
- `/logs`: Pipeline and recovery logs.

### Essential Commands (Original)
```bash
# Start the Autonomous Pipeline
nohup python3 orchestrator.py > logs/orchestrator.log 2>&1 &

# On-Demand Series Discovery
python3 core/on_demand_discoverer.py "Pucking Around"

# Data Auditing & Cleaning
python3 utils/dataset_manager.py audit [file_path]
python3 utils/dataset_manager.py beautify [file_path]

# Run full pipeline
python3 main.py --all
```

---

## Part 2: Multi-Subgenre Analysis Pipeline

### Overview

A comprehensive pipeline for analyzing self-published romance series across 9 subgenres. Processes 10,544 series through discovery, enrichment, scoring, and Amazon bestseller mapping.

**Subgenres covered**: Christian Romance, Dark & Forbidden, Historical Romance, Ice Hockey & Sports Romance, Mafia Romance, Military Romance, Political Romance, Romantic Suspense/Psychological Thriller, Small Town Romance.

### Pipeline Architecture

| Step | Script | Purpose | Runtime |
| :--- | :--- | :--- | :--- |
| 1. Discovery | `mega_discovery.py` | Multi-platform discovery (Goodreads, Google, Reddit, BookTok) | ~2h |
| 2. Series Verification | `gemini_series_check.py` | Verify discovered titles are real series via Gemini | ~1h |
| 3. Goodreads Enrichment | `enrich_with_goodreads.py` | Scrape ratings, page counts, pub dates from Goodreads | ~4h |
| 4. Self-Pub Expansion | `selfpub_expansion_pipeline.py` | Expand dataset with self-pub focused discovery | ~3h |
| 5. Gemini Enrichment | `gemini_enrich_all.py` | 3-phase AI enrichment (metadata, validation, contacts) | ~7.5h |
| 6. Amazon Top 100 | `amazon_top100.py` | Scrape Amazon bestseller lists across categories | ~30min |
| 7. Amazon Full Crawl | `amazon_full_crawl.py` | Comprehensive paid/free crawl across 37 categories | ~45min |
| 8. Final Aggregation | `final_aggregate_and_score.py` | Score, rank, deduplicate, generate Excel output | ~2min |
| 9. Deep Enrichment | `deep_enrich.py` | Fill remaining gaps (book details, ratings, contacts) | ~10h |

### Gemini Enrichment (gemini_enrich_all.py)

Three-phase AI enrichment using Gemini 2.5 Flash:

**Phase 1 — Subjective Analysis** (batch 50):
- Subjective Analysis (3-sentence summary), Primary Trope, Series Era, Type, Total Pages, Differentiator
- Auto-calculates Length of Adaptation in Hours (pages / 33.33)
- ~211 batches, ~3.5 hours

**Phase 2 — Objective Validation** (batch 30):
- Cross-references against NYT Bestseller, USA Today, Amazon Bestseller, BookTok viral, Goodreads Choice Awards
- Stores validation sources in `Objective_Validation_Source` column
- ~352 batches, ~2.5 hours

**Phase 3 — Author Contacts** (batch 40):
- Deduplicates by author name before querying
- Finds: email, website, Twitter, Instagram, Facebook, BookBub, TikTok, literary agent
- ~118 batches, ~1.7 hours

```bash
cd "sub genre analysis"
python3 gemini_enrich_all.py --phase all   # Run all 3 phases
python3 gemini_enrich_all.py --phase 1     # Metadata only
python3 gemini_enrich_all.py --phase 2     # Validation only
python3 gemini_enrich_all.py --phase 3     # Contacts only
```

### Amazon Top 100 Crawler (amazon_full_crawl.py)

Comprehensive Amazon bestseller scraper across 37 categories (paid + free):
- Romance subcategories (13): Contemporary, Historical, Dark, Paranormal, Sports, Military, etc.
- Broader fiction (9): Literature & Fiction, Mystery/Thriller, Women's Fiction, etc.
- Print book lists (6+)
- Stealth browser with webdriver override, chrome runtime mock, custom user agents
- Smart title/author parsing for Amazon's concatenated text format
- Maps results against scored dataset and adds "Amazon Top 100 Lists" Excel tab

```bash
cd "sub genre analysis"
python3 amazon_full_crawl.py                # Full crawl + map + Excel tab
python3 amazon_full_crawl.py --map-only     # Map existing data to dataset
```

### Amazon Top 100 Lists Excel Tab Format

| Column | Description |
| :--- | :--- |
| Amazon Top 100 List | Category name (e.g., "Romance Paid", "Historical Romance Paid") |
| Rank | Position 1-100 |
| Book Name | Title of the book |
| Book Series Name | Series name (extracted from title) |
| Author Name | Author |
| Crawl Date | Date of scraping (for tracking changes over time) |

### Commissioning Score (0-100)

Weighted scoring model with 8 components:

| Component | Weight | Logic |
| :--- | :--- | :--- |
| Adaptation Length | 15% | Based on total pages (>1500=100, >1000=80, >500=60, >200=40, else 20) |
| First Book Rating | 20% | GR rating scaled (>4.3=100, >4.0=80, >3.7=60, >3.4=40, else 20) |
| Rating Volume | 15% | Count-based (>50K=100, >10K=80, >1K=60, >100=40, else 20) |
| Lowest Book Rating | 10% | Floor quality (>4.0=100, >3.7=80, >3.4=60, >3.0=40, else 20) |
| Rating Stability | 10% | First-to-lowest rating gap (<0.2=100, <0.4=80, <0.6=60, <0.8=40) |
| Series Era | 10% | Contemporary > Classic (post-2015=100, 2010-2015=80, 2000-2010=60) |
| Books in Series | 10% | More = better (>10=100, >6=80, >3=60, >1=40, else 20) |
| Validation Bonus | 10% | NYT/Amazon/BookTok/Goodreads Choice presence |

**Priority Ranks**: P0 >= 75, P1 >= 55, P2 >= 35, P3 < 35

### Deep Enrichment (deep_enrich.py)

Fills remaining gaps after initial enrichment:

**Phase A — Book-Level Details** (batch 50):
- Last Book Name, Last Book Rating, Last Book Rating Count
- Highest/Lowest Rated Book Name, Rating, Rating Count
- Goodreads Series URL

**Phase B — Missing First Book Ratings** (batch 50):
- Targets entries with missing or zero First Book Rating
- Also fills missing rating counts

**Phase C — Remaining Contacts** (batch 50):
- Targets authors with missing email
- Deduplicates by author before querying

```bash
cd "sub genre analysis"
python3 deep_enrich.py --phase all   # All phases
python3 deep_enrich.py --phase A     # Book details only
python3 deep_enrich.py --phase B     # Missing ratings only
python3 deep_enrich.py --phase C     # Missing contacts only
```

### Final Aggregation (final_aggregate_and_score.py)

Generates the final scored output:
1. Loads enriched data
2. Cleans and standardizes columns
3. Deduplicates series
4. Applies Amazon bestseller tags
5. Computes commissioning scores (0-100)
6. Generates flags (Adaptation Length, Rating, Appeal, Stability)
7. Builds human-readable rationale
8. Outputs: Combined CSV, Excel workbook with per-subgenre sheets + Amazon tab

```bash
cd "sub genre analysis"
python3 final_aggregate_and_score.py
```

**Output Files**:
- `output/FINAL_SELFPUB_SCORED.csv` — Combined 10,544 rows, 49 columns
- `output/FINAL_SELFPUB_SCORED.xlsx` — Excel with Combined + 9 subgenre sheets + Amazon Top 100 Lists
- `output/subgenre_outputs/` — Individual subgenre CSVs

### The 49-Column Output Schema

| # | Column | Description |
| :--- | :--- | :--- |
| 1 | Book Series Name | Series title |
| 2 | Author Name | Author |
| 3 | Primary Subgenre | One of 9 subgenres |
| 4 | Type | Standalone / Series |
| 5 | Books in Series | Count |
| 6 | Total Pages | Estimated total across series |
| 7 | Length of Adaption in Hours | Total Pages / 33.33 |
| 8 | First Book Name | Title of first book |
| 9 | First Book Rating | Goodreads rating |
| 10 | First Book Rating Count | Number of ratings |
| 11-13 | Last Book Name/Rating/Count | Last published book details |
| 14-16 | Highest Rated Book Name/Rating/Count | Best rated book |
| 17-19 | Lowest Rated Book Name/Rating/Count | Worst rated book |
| 20 | Publisher Name | Publisher |
| 21 | Self Pub Flag | Yes/No |
| 22 | Commissioning_Score | 0-100 weighted score |
| 23 | Commissioning_Rank | P0/P1/P2/P3 |
| 24 | Subjective Analysis | AI-generated 3-sentence summary |
| 25 | Differentiator | What makes this series unique |
| 26 | Series_Era | Contemporary/Classic/Modern |
| 27 | Rationale | Human-readable scoring explanation |
| 28 | Goodreads Series URL | Link to GR series page |
| 29-37 | Contact fields | Email, Website, Twitter, Instagram, Facebook, BookBub, TikTok, Literary Agent, Contact Source |
| 38 | Discovery Status | How the series was found |
| 39 | Primary Trope | Main trope/theme |
| 40 | Objective_Validation_Source | NYT/Amazon/BookTok/etc. |
| 41-42 | Amazon_Bestseller_Tag/Rank | Amazon Top 100 mapping |
| 43-49 | Flags | Adaptation Length, Rating, Appeal, Lowest Rating, Stability, Publication Year, Books Featured Rank |

---

## Safety & Bot Detection
- **Context Rotation**: Workers automatically rotate browser contexts every 20 books.
- **Dynamic Throttling**: Each worker uses random sleeps (2-7s) to maintain a human-like profile.
- **Amazon Stealth**: Webdriver property override, chrome runtime mock, custom user agents, DNT headers.
- **Gemini Rate Limits**: Exponential backoff (5s start, 60s max, 5 retries) for API rate limits.

---

## Environment Setup

Required dependencies:
```bash
pip install pandas openpyxl google-generativeai playwright
playwright install chromium
```

Environment variables (in `.env`):
```
GEMINI_API_KEY=your_key_here
```

---

*Last Updated: 2026-03-16 (v3.0 Multi-Subgenre)*
