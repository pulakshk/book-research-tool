---
name: Book Research Pipeline
description: Comprehensive guide for robust book data scraping, matching, and enrichment pipeline using Gemini and Playwright.
---

# Book Research Pipeline Skill

A high-performance comprehensive book data pipeline. Scales using Gemini for metadata enhancement and Playwright for series exhaustion.

## Table of Contents
1. [Pipeline Overview](#pipeline-overview)
2. [Architecture](#architecture)
3. [CLI Usage](#cli-usage)
4. [Phases](#phases)
5. [Configuration](#configuration)

---

## Pipeline Overview

### Purpose
To generate a "Golden Dataset" of book series, enriched with:
-   **Metadata**: Publisher, Tropes, Descriptions (via Gemini)
-   **Missing Books**: Exhaustive series lists (via Playwright)
-   **Analysis**: P0-P5 Commissioning Scores (Volume, Quality, Retention)

### Key Features
✅ **Turbo Scraping**: Playwright + Gemini "Super-Call" strategy to extract entire series hubs in one shot.
✅ **Gemini Enrichment**: 30-worker parallel agent to fill missing Publishers/Tropes and truncate descriptions.
✅ **Analysis Engine**: Weighted scoring model (Volume 30%, Appeal 20%, Retention 25%) for acquisition decision making.
✅ **Compact Codebase**: Modular `src/pipeline` package structure replacing loose scripts.

---

## Architecture

The codebase has been consolidated into a modular Python package:

```
src/
  pipeline/
    config.py       # Centralized Keys & Constants (Weights, API Keys)
    data.py         # Robust I/O & Backups (Loads/Saves Master & Ultra files)
    cleaning.py     # Regex Filters (Fantasy/CJK removal) & Deduplication
    scrapers.py     # Playwright + Gemini Series Exhaustion logic
    enrichment.py   # Async Gemini Metadata Filling (Publisher, Tropes)
    analysis.py     # Commissioning Logic (P0-P5 Scoring)
main.py             # Single Entry Point for all phases
```

---

## CLI Usage

Run the unified entry point `main.py` from the root directory:

```bash
# Run ALL phases (Scrape -> Clean -> Enrich -> Analyze)
python3 main.py --all

# Run specific phases
python3 main.py --scrape   # Phase 0: Series Exhaustion (Playwright)
python3 main.py --clean    # Phase 1: Cleaning (Dedupe + Filter)
python3 main.py --enrich   # Phase 2: Enrichment (Gemini)
python3 main.py --analyze  # Phase 3: Analysis (Report Generation)
```

---

## Phases

### Phase 0: Scraping (Series Exhaustion)
-   **Goal**: Find every book in every series to ensure completeness.
-   **Logic**:
    1.  Search Google for "Goodreads series [Series Name] [Author]".
    2.  Navigate to the Series Hub Page using Playwright.
    3.  Dump the page HTML into a Gemini "Super Extract" Prompt.
    4.  Extract JSON list of books (Title, URL, Date, Rating).
    5.  Merge new books into the dataset if they don't exist.

### Phase 1: Cleaning
-   **Goal**: Ensure purity of the Sports/Romance dataset.
-   **Logic**:
    -   **Deduplication**: Removes exact duplicates based on specific normalization rules.
    -   **Filtering**: Strict Regex bans for "Fantasy", "Sci-Fi", "Alien", "Dragon" (unless 'Hockey' is present), and CJK characters.

### Phase 2: Enrichment
-   **Goal**: 100% Data Density.
-   **Logic**:
    -   Uses massive concurrency (30 workers) with `asyncio`.
    -   **Detailed Metadata**: Fills `Publisher`, `Primary Trope`, `Self Pub Flag`, and `Featured List`.
    -   **Description Truncation**: Rewrites long descriptions into 1-2 punchy lines.

### Phase 3: Analysis
-   **Goal**: Commissioning Decision.
-   **Logic**:
    -   **Scoring Model**:
        -   **Volume (30%)**: Proven series length is critical.
        -   **Quality (25%)**: First Book Rating (15%) + Average Rating (10%).
        -   **Retention (25%)**: Read-Through Ratio (Recency Adjusted).
        -   **Appeal (20%)**: Market size (Rating Counts).
    -   **Data Safety**: Metrics with missing data default to **80% score**.
    -   **Short Penalty**: Series < 3 books are forced to **P5**.
    -   **Output**: `series_commissioning_analysis.csv` sorted by Rank (P0 first).

---

## Configuration

Settings are managed in `src/pipeline/config.py`:
-   **Files**: `unified_book_data_enriched_ultra.csv` (Backup/Source), `...final.csv` (Master).
-   **Weights**: Adjust `WEIGHTS` dictionary for scoring.
-   **Concurrency**: `MAX_WORKERS` (Default 30 for enrichment).
