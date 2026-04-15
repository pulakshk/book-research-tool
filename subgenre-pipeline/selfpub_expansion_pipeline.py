#!/usr/bin/env python3
"""
Self-Pub Universe Expansion Pipeline
=====================================
Consolidates all subgenre data, discovers new books from multiple sources,
enriches metadata via Gemini, filters fantasy/romantasy, finds author emails,
and drafts personalized licensing outreach emails.

Usage:
    python3 selfpub_expansion_pipeline.py --consolidate     # Step 1: Merge all subgenre CSVs
    python3 selfpub_expansion_pipeline.py --discover         # Step 2: Find new books from sources
    python3 selfpub_expansion_pipeline.py --enrich           # Step 3: Gemini enrichment
    python3 selfpub_expansion_pipeline.py --filter           # Step 4: Filter fantasy/romantasy
    python3 selfpub_expansion_pipeline.py --emails           # Step 5: Author email discovery
    python3 selfpub_expansion_pipeline.py --draft            # Step 6: Draft licensing emails
    python3 selfpub_expansion_pipeline.py --all              # Run everything
"""

import os
import sys
import csv
import json
import re
import time
import asyncio
import argparse
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"
DRIVE_DIR = BASE_DIR / "drive-download-20260310T121636Z-3-001"
NEW_CRAWL_DIR = BASE_DIR / "genre-crawl"
MASTER_CSV = DATA_DIR / "selfpub_master_consolidated.csv"
EXPANDED_CSV = DATA_DIR / "selfpub_master_expanded.csv"
ENRICHED_CSV = DATA_DIR / "selfpub_master_enriched.csv"
FILTERED_CSV = DATA_DIR / "selfpub_master_filtered.csv"
FINAL_CSV = DATA_DIR / "selfpub_master_final_with_emails.csv"

# Ensure output dir
DATA_DIR.mkdir(exist_ok=True)

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "pipeline.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("selfpub_pipeline")

# ── Gemini Setup ───────────────────────────────────────────
def get_gemini_key():
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()

# ── Master Schema (46 columns, matching ice hockey format) ─
MASTER_COLUMNS = [
    "Book Series Name", "Author Name", "Type", "Books_In_Series_List",
    "Universe Type", "Universe Reasoning", "Verfied Flag",
    "Books in Series", "Total Pages", "Length of Adaption in Hours",
    "First Book Name", "First Book Rating", "First Book Rating Count",
    "Last Book Name", "Last Book Rating", "Last Book Rating Count",
    "Highest Rated Book Name", "Highest Rated Book Rating", "Highest Rated Book Rating Count",
    "Lowest Rated Book Name", "Lowest Rated Book Rating", "Lowest Rated Book Rating Count",
    "Publisher Name", "Self Pub Flag",
    "Subjective Analysis", "Differentiator",
    "Books_Featured_Rank_Validation", "Num_Books_Featured",
    "First_Book_Pub_Year", "T100_Mapping",
    "Adaptation_Length_Flag", "First_Book_Rating_Flag", "Appeal Flag",
    "Lowest_Book_Rating_Flag", "Rating_Stability_Flag",
    "Series_Era", "Commissioning_Score", "Commissioning_Rank",
    # Licensing columns
    "Primary Subgenre", "Primary Trope",
    "Outreach Sent Date", "Duplicate Rightsholders",
    "Outreach Channel", "Contact Info",
    "Response Status", "Response Date", "Licensing Status",
    # Email & draft columns (we add these)
    "Author Email", "Author Website", "Social Links",
    "Draft Email",
]

# ── Subgenre definitions ───────────────────────────────────
SUBGENRES = {
    "christian_drama_romance": {
        "label": "Christian Drama/Romance",
        "search_terms": [
            "christian romance series", "faith based romance",
            "amish romance series", "inspirational romance",
            "clean christian romance", "religious romance fiction",
        ],
        "amazon_categories": [
            "Christian Romance", "Amish Romance", "Inspirational Romance",
        ],
    },
    "dark_and_forbidden_romance": {
        "label": "Dark & Forbidden Romance",
        "search_terms": [
            "dark romance series", "forbidden romance",
            "dark academia romance", "age gap romance",
            "enemies to lovers dark", "taboo romance series",
        ],
        "amazon_categories": [
            "Dark Romance", "Forbidden Romance", "Romantic Suspense",
        ],
    },
    "historical_romance_fiction": {
        "label": "Historical Romance & Fiction",
        "search_terms": [
            "historical romance series", "regency romance",
            "victorian romance", "historical fiction romance",
            "period drama romance", "war romance historical",
        ],
        "amazon_categories": [
            "Historical Romance", "Regency Romance", "Victorian Romance",
        ],
    },
    "mafia_drama_romance": {
        "label": "Mafia Drama/Romance",
        "search_terms": [
            "mafia romance series", "organized crime romance",
            "dark mafia romance", "cartel romance",
            "crime family romance", "mafia arranged marriage",
        ],
        "amazon_categories": [
            "Mafia Romance", "Crime Romance", "Romantic Suspense",
        ],
    },
    "military_romance": {
        "label": "Military Drama/Romance",
        "search_terms": [
            "military romance series", "navy seal romance",
            "army romance", "special forces romance",
            "veteran romance", "military suspense romance",
        ],
        "amazon_categories": [
            "Military Romance", "Romantic Suspense",
        ],
    },
    "political_drama_romance": {
        "label": "Political Drama/Romance",
        "search_terms": [
            "political romance series", "political thriller romance",
            "white house romance", "political drama fiction",
            "election romance", "washington dc romance",
        ],
        "amazon_categories": [
            "Political Romance", "Political Fiction",
        ],
    },
    "small_town_romance": {
        "label": "Small Town Drama/Romance",
        "search_terms": [
            "small town romance series", "small town contemporary romance",
            "rural romance", "hometown romance",
            "country romance series", "cozy small town romance",
        ],
        "amazon_categories": [
            "Small Town Romance", "Contemporary Romance",
        ],
    },
    "ice_hockey_sports": {
        "label": "Ice Hockey & Sports Romance",
        "search_terms": [
            "hockey romance series", "sports romance",
            "ice hockey romance", "athlete romance series",
            "football romance", "baseball romance series",
        ],
        "amazon_categories": [
            "Sports Romance", "Hockey Romance",
        ],
    },
    "romantic_suspense_thriller": {
        "label": "Romantic Suspense/Psychological Thriller",
        "search_terms": [
            "romantic suspense series", "psychological thriller romance",
            "fbi romance series", "detective romance",
            "mystery romance", "crime thriller romance",
        ],
        "amazon_categories": [
            "Romantic Suspense", "Psychological Thriller",
        ],
    },
}

# ── Fantasy/Romantasy filter terms ─────────────────────────
FANTASY_FILTER_TERMS = [
    "werewolf", "shifter", "vampire", "fae", "fairy", "dragon",
    "witch", "wizard", "sorcerer", "mage", "elf", "elven",
    "demon", "angel", "paranormal", "supernatural", "magic",
    "kingdom", "throne", "realm", "enchant", "mythical",
    "shapeshifter", "lycan", "alpha mate", "omega",
    "chosen one", "prophecy", "necromancer", "warlock",
    "romantasy", "high fantasy", "epic fantasy", "urban fantasy",
    "paranormal romance", "fantasy romance",
]

# Light fantasy is OK — these are exceptions
FANTASY_EXCEPTIONS = [
    "lite fantasy", "light fantasy", "magical realism",
    "time travel", "ghost", "second chance",
]


# ═══════════════════════════════════════════════════════════
# PHASE 1: CONSOLIDATE
# ═══════════════════════════════════════════════════════════

def consolidate_all():
    """Merge all existing subgenre CSVs into one master sheet."""
    log.info("=" * 60)
    log.info("PHASE 1: CONSOLIDATING ALL SUBGENRE DATA")
    log.info("=" * 60)

    all_frames = []

    # Source 1: drive-download folder (cleanest enriched data)
    subgenre_file_map = {
        "christian_drama_romance": "christian_drama_romance.csv",
        "dark_and_forbidden_romance": "dark_and_forbidden_romance.csv",
        "historical_romance_fiction": "historical_romance_fiction.csv",
        "mafia_drama_romance": "mafia_drama_romance.csv",
        "military_romance": "military_romance.csv",
        "political_drama_romance": "political_drama_romance.csv",
        "small_town_romance": "small_town.csv",
    }

    for subgenre_key, filename in subgenre_file_map.items():
        fpath = DRIVE_DIR / filename
        if fpath.exists():
            df = _read_multiheader_csv(fpath, subgenre_key)
            if df is not None and len(df) > 0:
                df["Primary Subgenre"] = SUBGENRES[subgenre_key]["label"]
                all_frames.append(df)
                log.info(f"  [{subgenre_key}] {len(df)} rows from drive-download")

    # Source 2: Ice hockey from genre-crawl
    hockey_file = NEW_CRAWL_DIR / "Amazon Bestsellers _ Jan 2026 _ Hockey Romance - Cleaned Titles_ Sports & Hockey.csv"
    if hockey_file.exists():
        df = _read_multiheader_csv(hockey_file, "ice_hockey_sports")
        if df is not None and len(df) > 0:
            df["Primary Subgenre"] = SUBGENRES["ice_hockey_sports"]["label"]
            all_frames.append(df)
            log.info(f"  [ice_hockey_sports] {len(df)} rows from hockey CSV")

    # Source 3: Political drama final
    political_final = BASE_DIR / "Political Drama_Romance_final.csv"
    if political_final.exists():
        try:
            df = pd.read_csv(political_final, on_bad_lines="skip")
            if "Book Series Name" in df.columns and len(df) > 0:
                df["Primary Subgenre"] = "Political Drama/Romance"
                all_frames.append(df)
                log.info(f"  [political_final] {len(df)} additional political rows")
        except Exception as e:
            log.warning(f"  Could not read {political_final}: {e}")

    if not all_frames:
        log.error("No data found to consolidate!")
        return None

    # Merge all frames
    master = pd.concat(all_frames, ignore_index=True, sort=False)

    # Deduplicate by (series name, author) — fuzzy-ish via lowercase strip
    master["_dedup_key"] = (
        master["Book Series Name"].astype(str).str.lower().str.strip()
        + "|"
        + master["Author Name"].astype(str).str.lower().str.strip()
    )
    before = len(master)
    master = master.drop_duplicates(subset=["_dedup_key"], keep="first")
    master = master.drop(columns=["_dedup_key"])
    log.info(f"  Deduplication: {before} -> {len(master)} rows ({before - len(master)} duplicates removed)")

    # Ensure all master columns exist
    for col in MASTER_COLUMNS:
        if col not in master.columns:
            master[col] = ""

    # Stats per subgenre
    log.info("\n  Subgenre breakdown:")
    for sg, count in master["Primary Subgenre"].value_counts().items():
        log.info(f"    {sg}: {count} series")

    master.to_csv(MASTER_CSV, index=False)
    log.info(f"\n  Master consolidated: {len(master)} series -> {MASTER_CSV}")
    return master


def _read_multiheader_csv(fpath, subgenre_key):
    """Read CSVs with the multi-row header format (row 1 = category, row 2 = columns)."""
    try:
        # Read raw to detect header structure
        with open(fpath, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            row1 = next(reader)
            row2 = next(reader)

        # Check if row1 is a category header (has entries like "Series, Long", "Ratings & Appeal")
        is_multiheader = any(
            term in str(row1)
            for term in ["Series, Long", "Ratings & Appeal", "Publishers", "Objective List", "Final Rank", "Licensing Info"]
        )

        if is_multiheader:
            # Row 2 might be split across row2 and row3 if "Last Book\nRating" has a newline
            # Read with header=1 (0-indexed: skip row 0 category header)
            df = pd.read_csv(fpath, header=1, on_bad_lines="skip")
        else:
            df = pd.read_csv(fpath, on_bad_lines="skip")

        # Clean column names — remove leading/trailing whitespace
        df.columns = [str(c).strip() for c in df.columns]

        # Drop fully empty rows
        df = df.dropna(how="all")

        # Drop rows where Book Series Name is empty
        if "Book Series Name" in df.columns:
            df = df[df["Book Series Name"].notna() & (df["Book Series Name"].astype(str).str.strip() != "")]

        return df

    except Exception as e:
        log.warning(f"  Error reading {fpath}: {e}")
        return None


# ═══════════════════════════════════════════════════════════
# PHASE 2: DISCOVER NEW BOOKS
# ═══════════════════════════════════════════════════════════

def discover_new_books():
    """Use Gemini to discover new book series from multiple sources per subgenre."""
    log.info("=" * 60)
    log.info("PHASE 2: DISCOVERING NEW BOOKS VIA GEMINI")
    log.info("=" * 60)

    if not GEMINI_KEY:
        log.error("GEMINI_API_KEY not found in .env!")
        return None

    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    # Load existing master
    if MASTER_CSV.exists():
        master = pd.read_csv(MASTER_CSV, on_bad_lines="skip")
    else:
        log.error("Run --consolidate first!")
        return None

    existing_series = set(
        master["Book Series Name"].astype(str).str.lower().str.strip()
    )
    log.info(f"  Existing series in master: {len(existing_series)}")

    new_discoveries = []

    for subgenre_key, config in SUBGENRES.items():
        label = config["label"]
        subgenre_count = len(master[master["Primary Subgenre"] == label])
        needed = max(0, 500 - subgenre_count)

        if needed == 0:
            log.info(f"  [{label}] Already has {subgenre_count} series, skipping")
            continue

        log.info(f"  [{label}] Has {subgenre_count}, need {needed} more")

        # Batch discovery — ask Gemini to find series from multiple sources
        prompt = _build_discovery_prompt(config, needed, existing_series)

        try:
            response = model.generate_content(prompt)
            text = response.text
            series_list = _parse_discovery_response(text, label)
            log.info(f"    Gemini returned {len(series_list)} new series for {label}")

            # Filter out already existing ones
            truly_new = []
            for s in series_list:
                key = s.get("Book Series Name", "").lower().strip()
                if key and key not in existing_series:
                    existing_series.add(key)
                    s["Primary Subgenre"] = label
                    truly_new.append(s)

            new_discoveries.extend(truly_new[:needed])
            log.info(f"    After dedup: {len(truly_new)} truly new series")

            time.sleep(2)  # Rate limit

        except Exception as e:
            log.error(f"    Gemini error for {label}: {e}")
            continue

    if new_discoveries:
        new_df = pd.DataFrame(new_discoveries)
        # Merge with master
        for col in MASTER_COLUMNS:
            if col not in new_df.columns:
                new_df[col] = ""
        expanded = pd.concat([master, new_df], ignore_index=True, sort=False)
        expanded.to_csv(EXPANDED_CSV, index=False)
        log.info(f"\n  Total after discovery: {len(expanded)} series -> {EXPANDED_CSV}")

        # Stats
        log.info("\n  Updated subgenre breakdown:")
        for sg, count in expanded["Primary Subgenre"].value_counts().items():
            log.info(f"    {sg}: {count} series")

        return expanded

    log.info("  No new discoveries needed")
    return master


def _build_discovery_prompt(config, needed, existing_series):
    """Build a Gemini prompt to discover new book series."""
    label = config["label"]
    search_terms = config["search_terms"]
    categories = config["amazon_categories"]

    # Get a sample of existing series to avoid duplicates
    existing_sample = list(existing_series)[:50]

    return f"""You are a book market research expert. I need you to find {needed + 50} book SERIES
in the "{label}" subgenre that are available as self-published or indie-published titles.

Search across these platforms mentally:
- Amazon Kindle Direct Publishing (KDP) bestsellers
- Google Play Books
- Apple Books
- Audible audiobook originals
- Barnes & Noble / Kobo
- BookBub featured deals

Search terms to consider: {', '.join(search_terms)}
Amazon categories: {', '.join(categories)}

IMPORTANT FILTERS:
- MUST be book SERIES (2+ books), not standalones
- MUST be drama/romance or contemporary romance — NO high fantasy, NO romantasy, NO werewolf/shifter/vampire/fae/dragon themes
- Light fantasy/magical realism is OK, but must be primarily grounded in real-world settings
- Prefer self-published or indie publishers
- Include both well-known and lesser-known series

DO NOT include these series (already in our database):
{chr(10).join(existing_sample[:30])}

Return EXACTLY as a JSON array. Each object must have:
{{
  "Book Series Name": "Series Name",
  "Author Name": "Author Full Name",
  "Type": "Series" or "Long Series" (6+ books),
  "Books in Series": number,
  "First Book Name": "Title of first book",
  "First Book Rating": rating (0.0-5.0) or null,
  "First Book Rating Count": number or null,
  "Publisher Name": "Publisher or Independently published",
  "Self Pub Flag": "Self-Pub" or "Traditional",
  "First_Book_Pub_Year": year or null,
  "Subjective Analysis": "1-2 sentence appeal summary",
  "Differentiator": "What makes this series unique",
  "Primary Trope": "main trope (e.g., enemies to lovers, second chance, etc.)",
  "Universe Type": "same universe different couple" or "same universe same couple" or "standalone in series",
  "Source Platform": "Amazon KDP" or "Google Books" or "Apple Books" or "Audible" or "Multiple"
}}

Return ONLY the JSON array, no other text. Aim for {needed + 50} series."""


def _parse_discovery_response(text, label):
    """Parse Gemini's JSON response into a list of dicts."""
    # Extract JSON from response
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
    except json.JSONDecodeError:
        # Try to find JSON array in the text
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass

    log.warning(f"  Could not parse Gemini response for {label}")
    return []


# ═══════════════════════════════════════════════════════════
# PHASE 3: ENRICH VIA GEMINI
# ═══════════════════════════════════════════════════════════

def enrich_data():
    """Enrich all rows with missing data via Gemini."""
    log.info("=" * 60)
    log.info("PHASE 3: ENRICHING DATA VIA GEMINI")
    log.info("=" * 60)

    if not GEMINI_KEY:
        log.error("GEMINI_API_KEY not found!")
        return None

    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    # Load latest data
    source = EXPANDED_CSV if EXPANDED_CSV.exists() else MASTER_CSV
    if not source.exists():
        log.error("No data file found! Run --consolidate or --discover first.")
        return None

    df = pd.read_csv(source, on_bad_lines="skip")
    log.info(f"  Loaded {len(df)} rows from {source}")

    # Find rows needing enrichment (missing key fields)
    enrichment_fields = [
        "Books in Series", "Total Pages", "Length of Adaption in Hours",
        "First Book Rating", "First Book Rating Count",
        "Subjective Analysis", "Differentiator", "Primary Trope",
        "Universe Type", "Commissioning_Score",
    ]

    needs_enrichment = df[
        df[enrichment_fields].isna().any(axis=1) |
        (df[enrichment_fields].astype(str) == "").any(axis=1)
    ]
    log.info(f"  Rows needing enrichment: {len(needs_enrichment)} / {len(df)}")

    if len(needs_enrichment) == 0:
        log.info("  All rows already enriched!")
        df.to_csv(ENRICHED_CSV, index=False)
        return df

    # Process in batches of 10
    BATCH_SIZE = 10
    total_batches = (len(needs_enrichment) + BATCH_SIZE - 1) // BATCH_SIZE
    enriched_count = 0

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(needs_enrichment))
        batch = needs_enrichment.iloc[start:end]

        batch_data = []
        for _, row in batch.iterrows():
            batch_data.append({
                "series": str(row.get("Book Series Name", "")),
                "author": str(row.get("Author Name", "")),
                "subgenre": str(row.get("Primary Subgenre", "")),
                "books": str(row.get("Books in Series", "")),
                "first_book": str(row.get("First Book Name", "")),
            })

        prompt = _build_enrichment_prompt(batch_data)

        try:
            response = model.generate_content(prompt)
            enriched = _parse_enrichment_response(response.text)

            for item in enriched:
                series_name = item.get("Book Series Name", "").strip()
                if not series_name:
                    continue

                # Find matching row in master df
                mask = df["Book Series Name"].astype(str).str.lower().str.strip() == series_name.lower().strip()
                if mask.any():
                    idx = df[mask].index[0]
                    for key, val in item.items():
                        if key in df.columns and val and str(val).strip() and str(val) != "null":
                            current = str(df.at[idx, key]).strip()
                            if not current or current == "nan" or current == "":
                                df.at[idx, key] = val
                    enriched_count += 1

            log.info(f"    Batch {batch_idx + 1}/{total_batches}: enriched {len(enriched)} series")

        except Exception as e:
            log.error(f"    Batch {batch_idx + 1} error: {e}")

        time.sleep(1.5)  # Rate limit

        # Save progress every 5 batches
        if (batch_idx + 1) % 5 == 0:
            df.to_csv(ENRICHED_CSV, index=False)
            log.info(f"    Progress saved ({enriched_count} enriched so far)")

    # Compute derived fields
    df = _compute_derived_fields(df)

    df.to_csv(ENRICHED_CSV, index=False)
    log.info(f"\n  Enrichment complete: {enriched_count} series enriched -> {ENRICHED_CSV}")
    return df


def _build_enrichment_prompt(batch_data):
    """Build Gemini enrichment prompt for a batch of series."""
    series_list = "\n".join(
        f'{i+1}. "{d["series"]}" by {d["author"]} ({d["subgenre"]}) - {d["books"]} books, first book: "{d["first_book"]}"'
        for i, d in enumerate(batch_data)
    )

    return f"""You are a book metadata expert. Enrich the following book series with accurate data.
For each series, provide the missing information based on your knowledge of these books from
Goodreads, Amazon, and other book databases.

Series to enrich:
{series_list}

For each series, return a JSON object with:
{{
  "Book Series Name": "exact series name",
  "Books_In_Series_List": "comma-separated list of all book titles in order",
  "Universe Type": "same universe different couple" | "same universe same couple" | "standalone in series",
  "Universe Reasoning": "1-2 sentence explanation",
  "Books in Series": number,
  "Total Pages": estimated total pages across all books,
  "Length of Adaption in Hours": total_pages / 33.33 (audio adaptation estimate),
  "First Book Name": "title",
  "First Book Rating": rating (0.0-5.0),
  "First Book Rating Count": number of ratings,
  "Last Book Name": "title of most recent book",
  "Last Book Rating": rating,
  "Last Book Rating Count": number,
  "Highest Rated Book Name": "title",
  "Highest Rated Book Rating": rating,
  "Highest Rated Book Rating Count": number,
  "Lowest Rated Book Name": "title",
  "Lowest Rated Book Rating": rating,
  "Lowest Rated Book Rating Count": number,
  "Publisher Name": "publisher",
  "Self Pub Flag": "Self-Pub" | "Traditional",
  "Subjective Analysis": "2-3 sentence market appeal analysis",
  "Differentiator": "What makes this series unique in its subgenre",
  "First_Book_Pub_Year": year,
  "Primary Trope": "main romance trope",
  "Series_Era": "Before 2015" | "2015-2020" | "After 2020"
}}

IMPORTANT:
- Use REAL data only. If you don't know a value, use null.
- Ratings should be Goodreads ratings where possible.
- Be accurate about publisher names and self-pub status.

Return a JSON array of objects, one per series. Only the JSON array, no other text."""


def _parse_enrichment_response(text):
    """Parse enrichment JSON from Gemini response."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
        return [data]
    except json.JSONDecodeError:
        match = re.search(r'\[.*\]', text, re.DOTALL)
        if match:
            try:
                return json.loads(match.group())
            except json.JSONDecodeError:
                pass
    return []


def _compute_derived_fields(df):
    """Compute objective metric flags and commissioning score."""

    def safe_float(val):
        try:
            return float(val)
        except (ValueError, TypeError):
            return None

    def safe_int(val):
        try:
            return int(float(val))
        except (ValueError, TypeError):
            return None

    for idx, row in df.iterrows():
        hours = safe_float(row.get("Length of Adaption in Hours"))
        first_rating = safe_float(row.get("First Book Rating"))
        first_count = safe_int(row.get("First Book Rating Count"))
        lowest_rating = safe_float(row.get("Lowest Rated Book Rating"))
        pub_year = safe_int(row.get("First_Book_Pub_Year"))

        # Total Pages from books × 300 if missing
        if not safe_float(row.get("Total Pages")):
            books = safe_int(row.get("Books in Series"))
            if books:
                df.at[idx, "Total Pages"] = books * 300
                df.at[idx, "Length of Adaption in Hours"] = round(books * 300 / 33.33, 1)
                hours = df.at[idx, "Length of Adaption in Hours"]

        # Adaptation_Length_Flag
        if hours:
            if hours >= 80:
                df.at[idx, "Adaptation_Length_Flag"] = "Very High"
            elif hours >= 50:
                df.at[idx, "Adaptation_Length_Flag"] = "High"
            elif hours >= 30:
                df.at[idx, "Adaptation_Length_Flag"] = "Medium"
            else:
                df.at[idx, "Adaptation_Length_Flag"] = "Low"

        # First_Book_Rating_Flag
        if first_rating:
            if first_rating >= 4.3:
                df.at[idx, "First_Book_Rating_Flag"] = "High"
            elif first_rating >= 3.8:
                df.at[idx, "First_Book_Rating_Flag"] = "Medium"
            else:
                df.at[idx, "First_Book_Rating_Flag"] = "Low"

        # Appeal Flag (based on rating count)
        if first_count:
            if first_count >= 10000:
                df.at[idx, "Appeal Flag"] = "Very High"
            elif first_count >= 5000:
                df.at[idx, "Appeal Flag"] = "High"
            elif first_count >= 1000:
                df.at[idx, "Appeal Flag"] = "Medium"
            else:
                df.at[idx, "Appeal Flag"] = "Low"

        # Lowest_Book_Rating_Flag
        if lowest_rating:
            if lowest_rating >= 4.0:
                df.at[idx, "Lowest_Book_Rating_Flag"] = "High"
            elif lowest_rating >= 3.5:
                df.at[idx, "Lowest_Book_Rating_Flag"] = "Medium"
            else:
                df.at[idx, "Lowest_Book_Rating_Flag"] = "Low"

        # Rating_Stability_Flag
        if first_rating and lowest_rating:
            spread = first_rating - lowest_rating
            if spread <= 0.3:
                df.at[idx, "Rating_Stability_Flag"] = "High"
            elif spread <= 0.6:
                df.at[idx, "Rating_Stability_Flag"] = "Medium"
            else:
                df.at[idx, "Rating_Stability_Flag"] = "Low"

        # Series_Era
        if pub_year:
            if pub_year >= 2021:
                df.at[idx, "Series_Era"] = "After 2020"
            elif pub_year >= 2015:
                df.at[idx, "Series_Era"] = "2015-2020"
            else:
                df.at[idx, "Series_Era"] = "Before 2015"

        # Commissioning Score (0-100 composite)
        score = 0
        if hours:
            if hours >= 80: score += 25
            elif hours >= 50: score += 20
            elif hours >= 30: score += 12
            else: score += 5

        if first_rating:
            if first_rating >= 4.3: score += 25
            elif first_rating >= 4.0: score += 20
            elif first_rating >= 3.8: score += 15
            else: score += 8

        if first_count:
            if first_count >= 20000: score += 25
            elif first_count >= 10000: score += 22
            elif first_count >= 5000: score += 18
            elif first_count >= 1000: score += 12
            else: score += 5

        if lowest_rating:
            if lowest_rating >= 4.0: score += 15
            elif lowest_rating >= 3.5: score += 10
            else: score += 3

        era_bonus = 0
        if pub_year and pub_year >= 2020: era_bonus = 10
        elif pub_year and pub_year >= 2015: era_bonus = 5
        score += era_bonus

        if score > 0:
            df.at[idx, "Commissioning_Score"] = round(score, 1)
            if score >= 85:
                df.at[idx, "Commissioning_Rank"] = "P0"
            elif score >= 70:
                df.at[idx, "Commissioning_Rank"] = "P1"
            elif score >= 50:
                df.at[idx, "Commissioning_Rank"] = "P2"
            else:
                df.at[idx, "Commissioning_Rank"] = "P3"

    return df


# ═══════════════════════════════════════════════════════════
# PHASE 4: FILTER FANTASY / ROMANTASY
# ═══════════════════════════════════════════════════════════

def filter_fantasy():
    """Remove fantasy/romantasy titles. Keep drama romance / contemporary."""
    log.info("=" * 60)
    log.info("PHASE 4: FILTERING FANTASY / ROMANTASY")
    log.info("=" * 60)

    source = ENRICHED_CSV if ENRICHED_CSV.exists() else (EXPANDED_CSV if EXPANDED_CSV.exists() else MASTER_CSV)
    df = pd.read_csv(source, on_bad_lines="skip")
    before = len(df)

    def is_fantasy(row):
        """Check if a row has fantasy/romantasy markers."""
        text_fields = [
            str(row.get("Book Series Name", "")),
            str(row.get("Subjective Analysis", "")),
            str(row.get("Differentiator", "")),
            str(row.get("Primary Trope", "")),
            str(row.get("Primary Subgenre", "")),
            str(row.get("Books_In_Series_List", "")),
            str(row.get("Universe Reasoning", "")),
        ]
        combined = " ".join(text_fields).lower()

        # Check for exception terms first
        for exc in FANTASY_EXCEPTIONS:
            if exc in combined:
                return False

        # Check for fantasy terms
        for term in FANTASY_FILTER_TERMS:
            if term in combined:
                return True

        return False

    mask = df.apply(is_fantasy, axis=1)
    removed = df[mask]
    df = df[~mask]

    log.info(f"  Before filter: {before} series")
    log.info(f"  Fantasy/romantasy removed: {len(removed)} series")
    log.info(f"  After filter: {len(df)} series")

    if len(removed) > 0:
        # Log some examples of what was removed
        for _, r in removed.head(10).iterrows():
            log.info(f"    Removed: '{r.get('Book Series Name', '')}' by {r.get('Author Name', '')}")

        # Save removed list for review
        removed.to_csv(DATA_DIR / "fantasy_romantasy_removed.csv", index=False)

    df.to_csv(FILTERED_CSV, index=False)
    log.info(f"\n  Filtered data saved -> {FILTERED_CSV}")

    # Stats
    log.info("\n  Filtered subgenre breakdown:")
    for sg, count in df["Primary Subgenre"].value_counts().items():
        log.info(f"    {sg}: {count} series")

    return df


# ═══════════════════════════════════════════════════════════
# PHASE 5: AUTHOR EMAIL DISCOVERY
# ═══════════════════════════════════════════════════════════

def discover_emails():
    """Find author emails using Gemini."""
    log.info("=" * 60)
    log.info("PHASE 5: AUTHOR EMAIL DISCOVERY")
    log.info("=" * 60)

    if not GEMINI_KEY:
        log.error("GEMINI_API_KEY not found!")
        return None

    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    source = FILTERED_CSV if FILTERED_CSV.exists() else ENRICHED_CSV
    if not source.exists():
        source = MASTER_CSV
    df = pd.read_csv(source, on_bad_lines="skip")

    # Only process rows missing email
    needs_email = df[
        df["Author Email"].isna() | (df["Author Email"].astype(str).str.strip() == "")
    ]
    log.info(f"  Authors needing email lookup: {len(needs_email)} / {len(df)}")

    # Get unique authors
    unique_authors = needs_email[["Author Name", "Book Series Name", "Primary Subgenre"]].drop_duplicates(
        subset=["Author Name"]
    )
    log.info(f"  Unique authors to look up: {len(unique_authors)}")

    BATCH_SIZE = 15
    total_batches = (len(unique_authors) + BATCH_SIZE - 1) // BATCH_SIZE
    email_map = {}

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(unique_authors))
        batch = unique_authors.iloc[start:end]

        author_list = "\n".join(
            f'{i+1}. {row["Author Name"]} (series: "{row["Book Series Name"]}", genre: {row["Primary Subgenre"]})'
            for i, (_, row) in enumerate(batch.iterrows())
        )

        prompt = f"""You are a publishing industry research assistant. Find contact information for these authors.
These are self-published and indie-published romance/drama authors.

Authors:
{author_list}

For each author, find:
1. Their professional/public email address (from their website, Goodreads, or public directory)
2. Their website URL
3. Social media handles (Twitter/X, Instagram, BookBub, TikTok)

IMPORTANT:
- Only return REAL, publicly available contact info
- Author websites often have contact pages
- Many self-pub authors list email on Goodreads or BookBub
- If you're not confident about an email, return null
- Literary agent contact is acceptable as fallback

Return a JSON array:
[{{
  "Author Name": "name",
  "Author Email": "email@domain.com" or null,
  "Author Website": "https://..." or null,
  "Social Links": "twitter: @handle, instagram: @handle, bookbub: url" or null,
  "Contact Notes": "source of email or alternative contact method"
}}]

Return ONLY the JSON array."""

        try:
            response = model.generate_content(prompt)
            results = _parse_enrichment_response(response.text)

            for item in results:
                author = item.get("Author Name", "").strip()
                if author:
                    email_map[author.lower()] = item

            log.info(f"    Batch {batch_idx + 1}/{total_batches}: found contacts for {len(results)} authors")

        except Exception as e:
            log.error(f"    Batch {batch_idx + 1} error: {e}")

        time.sleep(2)

        if (batch_idx + 1) % 5 == 0:
            log.info(f"    Progress: {len(email_map)} author contacts found so far")

    # Apply email data back to df
    applied = 0
    for idx, row in df.iterrows():
        author = str(row.get("Author Name", "")).strip().lower()
        if author in email_map:
            info = email_map[author]
            if info.get("Author Email"):
                df.at[idx, "Author Email"] = info["Author Email"]
            if info.get("Author Website"):
                df.at[idx, "Author Website"] = info["Author Website"]
            if info.get("Social Links"):
                df.at[idx, "Social Links"] = info["Social Links"]
            applied += 1

    log.info(f"\n  Email data applied to {applied} rows")
    df.to_csv(FINAL_CSV, index=False)
    log.info(f"  Saved with emails -> {FINAL_CSV}")
    return df


# ═══════════════════════════════════════════════════════════
# PHASE 6: DRAFT PERSONALIZED LICENSING EMAILS
# ═══════════════════════════════════════════════════════════

def draft_emails():
    """Draft personalized licensing outreach emails for each author."""
    log.info("=" * 60)
    log.info("PHASE 6: DRAFTING LICENSING EMAILS")
    log.info("=" * 60)

    if not GEMINI_KEY:
        log.error("GEMINI_API_KEY not found!")
        return None

    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    source = FINAL_CSV if FINAL_CSV.exists() else FILTERED_CSV
    if not source.exists():
        source = MASTER_CSV
    df = pd.read_csv(source, on_bad_lines="skip")

    # Only draft for rows with email and no existing draft
    needs_draft = df[
        df["Author Email"].notna()
        & (df["Author Email"].astype(str).str.strip() != "")
        & (df["Author Email"].astype(str) != "nan")
        & (
            df["Draft Email"].isna()
            | (df["Draft Email"].astype(str).str.strip() == "")
            | (df["Draft Email"].astype(str) == "nan")
        )
    ]
    log.info(f"  Authors needing draft emails: {len(needs_draft)} / {len(df)}")

    # Deduplicate by author (one email per author, covering all their series)
    author_series = {}
    for _, row in needs_draft.iterrows():
        author = str(row.get("Author Name", "")).strip()
        if author:
            if author not in author_series:
                author_series[author] = {
                    "email": str(row.get("Author Email", "")),
                    "series": [],
                    "subgenre": str(row.get("Primary Subgenre", "")),
                    "top_rating": None,
                    "top_count": None,
                }
            author_series[author]["series"].append({
                "name": str(row.get("Book Series Name", "")),
                "books": str(row.get("Books in Series", "")),
                "rating": str(row.get("First Book Rating", "")),
                "count": str(row.get("First Book Rating Count", "")),
                "trope": str(row.get("Primary Trope", "")),
            })

    log.info(f"  Unique authors for email drafts: {len(author_series)}")

    BATCH_SIZE = 5  # Smaller batches for longer outputs
    authors_list = list(author_series.items())
    total_batches = (len(authors_list) + BATCH_SIZE - 1) // BATCH_SIZE
    email_drafts = {}

    for batch_idx in range(total_batches):
        start = batch_idx * BATCH_SIZE
        end = min(start + BATCH_SIZE, len(authors_list))
        batch = authors_list[start:end]

        author_details = []
        for author, info in batch:
            series_text = "; ".join(
                f'"{s["name"]}" ({s["books"]} books, {s["rating"]}★, {s["count"]} ratings, trope: {s["trope"]})'
                for s in info["series"]
            )
            author_details.append(
                f'- {author} ({info["subgenre"]}): {series_text}'
            )

        prompt = f"""You are a licensing outreach specialist for Pocket FM, the world's largest audio
series platform. Draft personalized licensing emails for each author below.

POCKET FM CONTEXT:
- World's largest audio series platform
- 120 billion minutes of listening in last 12 months
- 18 million monthly active listeners
- $342 million in revenue
- 200+ million global app downloads
- 4.7★ on App Store, 4.5★ on Google Play
- Series like "The Duke's Masked Bride" (641M plays, $18M revenue)
- NOT traditional audiobooks — serialized ~10-minute episodes with cliffhangers
- Authors retain IP ownership

DEAL STRUCTURE:
- Exclusive audio serialization rights (English, US/UK markets)
- Minimum Guarantee (negotiable based on series metrics)
- 20% revenue share on gross revenue
- 5-10 year term
- Author retains all other rights
- Optional suitability testing with 500-1000 users first

AUTHORS TO EMAIL:
{chr(10).join(author_details)}

For each author, draft a PERSONALIZED email that:
1. Opens with genuine appreciation for their specific series (mention by name)
2. References what makes their work special (tropes, ratings, reader appeal)
3. Briefly introduces Pocket FM and the opportunity
4. Proposes exclusive audio serialization rights
5. Mentions the commercial terms (MG + rev share)
6. Includes a clear call to action (schedule a call)
7. Professional but warm tone — not corporate
8. Keep to 200-250 words max

Return as JSON array:
[{{
  "Author Name": "name",
  "Subject Line": "email subject",
  "Email Body": "full email text"
}}]

Return ONLY the JSON array."""

        try:
            response = model.generate_content(prompt)
            results = _parse_enrichment_response(response.text)

            for item in results:
                author = item.get("Author Name", "").strip()
                if author:
                    email_drafts[author.lower()] = item

            log.info(f"    Batch {batch_idx + 1}/{total_batches}: drafted {len(results)} emails")

        except Exception as e:
            log.error(f"    Batch {batch_idx + 1} error: {e}")

        time.sleep(2)

    # Apply drafts to dataframe
    applied = 0
    for idx, row in df.iterrows():
        author = str(row.get("Author Name", "")).strip().lower()
        if author in email_drafts:
            draft = email_drafts[author]
            subject = draft.get("Subject Line", "")
            body = draft.get("Email Body", "")
            if body:
                df.at[idx, "Draft Email"] = f"SUBJECT: {subject}\n\n{body}"
                applied += 1

    log.info(f"\n  Email drafts applied to {applied} rows")
    df.to_csv(FINAL_CSV, index=False)
    log.info(f"  Final data saved -> {FINAL_CSV}")

    # Summary stats
    log.info("\n" + "=" * 60)
    log.info("FINAL SUMMARY")
    log.info("=" * 60)
    log.info(f"  Total series: {len(df)}")
    log.info(f"  With emails: {df['Author Email'].notna().sum()}")
    log.info(f"  With draft emails: {(df['Draft Email'].notna() & (df['Draft Email'].astype(str) != '')).sum()}")
    log.info(f"\n  Subgenre breakdown:")
    for sg, count in df["Primary Subgenre"].value_counts().items():
        log.info(f"    {sg}: {count} series")

    return df


# ═══════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Self-Pub Universe Expansion Pipeline")
    parser.add_argument("--consolidate", action="store_true", help="Phase 1: Consolidate all subgenre data")
    parser.add_argument("--discover", action="store_true", help="Phase 2: Discover new books via Gemini")
    parser.add_argument("--enrich", action="store_true", help="Phase 3: Enrich data via Gemini")
    parser.add_argument("--filter", action="store_true", help="Phase 4: Filter fantasy/romantasy")
    parser.add_argument("--emails", action="store_true", help="Phase 5: Author email discovery")
    parser.add_argument("--draft", action="store_true", help="Phase 6: Draft licensing emails")
    parser.add_argument("--all", action="store_true", help="Run all phases")

    args = parser.parse_args()

    if not any([args.consolidate, args.discover, args.enrich, args.filter, args.emails, args.draft, args.all]):
        parser.print_help()
        return

    start = datetime.now()
    log.info(f"Pipeline started at {start}")

    if args.consolidate or args.all:
        consolidate_all()

    if args.discover or args.all:
        discover_new_books()

    if args.enrich or args.all:
        enrich_data()

    if args.filter or args.all:
        filter_fantasy()

    if args.emails or args.all:
        discover_emails()

    if args.draft or args.all:
        draft_emails()

    elapsed = datetime.now() - start
    log.info(f"\nPipeline completed in {elapsed}")


if __name__ == "__main__":
    main()
