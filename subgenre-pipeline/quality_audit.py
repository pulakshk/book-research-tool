#!/usr/bin/env python3
"""
Quality Audit & Series Length Filter
======================================
1. Remove series with < 3 books (need 40+ hours = at least 3 books)
2. Audit standalone titles - use Gemini to check if they're actually part of a series
3. Verify data quality: remove entries with no author, no title, junk data
4. Re-estimate series length where possible
"""

import os
import json
import re
import time
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "quality_audit.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("quality_audit")

# ── Gemini Setup ───────────────────────────────────────────
def get_gemini_key():
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()


def run_audit():
    log.info("=" * 60)
    log.info("QUALITY AUDIT & SERIES LENGTH FILTER")
    log.info("=" * 60)

    # Load latest data
    source = DATA_DIR / "selfpub_master_multi_platform.csv"
    if not source.exists():
        source = DATA_DIR / "selfpub_master_cleaned.csv"
    if not source.exists():
        log.error("No data file found!")
        return

    df = pd.read_csv(source, on_bad_lines="skip")
    log.info(f"  Loaded {len(df)} series from {source.name}")

    # ── Step 1: Basic data quality cleanup ──────────────────
    log.info("\n  STEP 1: Basic data quality cleanup")
    before = len(df)

    # Remove rows with no title
    df = df[df["Book Series Name"].notna() & (df["Book Series Name"].astype(str).str.strip() != "")]

    # Remove rows with no author
    df = df[df["Author Name"].notna() & (df["Author Name"].astype(str).str.strip() != "")]

    # Remove rows where title == author (junk entries)
    df = df[df["Book Series Name"].astype(str).str.lower().str.strip() != df["Author Name"].astype(str).str.lower().str.strip()]

    # Remove very short titles (likely junk)
    df = df[df["Book Series Name"].astype(str).str.len() >= 3]

    quality_removed = before - len(df)
    log.info(f"  Basic quality filter removed: {quality_removed} entries")

    # ── Step 2: Series length audit ─────────────────────────
    log.info("\n  STEP 2: Series length audit")

    def get_books_count(row):
        """Get the best estimate of books in series."""
        val = row.get("Books in Series")
        if pd.notna(val):
            try:
                n = int(float(val))
                if n > 0:
                    return n
            except (ValueError, TypeError):
                pass
        return None

    # Categorize entries
    has_count = []
    no_count = []
    for idx, row in df.iterrows():
        count = get_books_count(row)
        if count is not None:
            has_count.append((idx, count))
        else:
            no_count.append(idx)

    log.info(f"  Series with book count: {len(has_count)}")
    log.info(f"  Series without book count: {len(no_count)}")

    # Distribution of known counts
    if has_count:
        counts = [c for _, c in has_count]
        log.info(f"  Book count distribution:")
        log.info(f"    1 book: {sum(1 for c in counts if c == 1)}")
        log.info(f"    2 books: {sum(1 for c in counts if c == 2)}")
        log.info(f"    3-5 books: {sum(1 for c in counts if 3 <= c <= 5)}")
        log.info(f"    6-10 books: {sum(1 for c in counts if 6 <= c <= 10)}")
        log.info(f"    11+ books: {sum(1 for c in counts if c >= 11)}")

    # ── Step 3: Gemini audit for unknowns + standalones ─────
    log.info("\n  STEP 3: Gemini series verification for unknown/standalone entries")

    # Get entries that need verification:
    # - No book count
    # - Book count == 1 (might be part of a series)
    # - Book count == 2 (borderline)
    needs_verification = []
    for idx, row in df.iterrows():
        count = get_books_count(row)
        if count is None or count <= 2:
            needs_verification.append((idx, row))

    log.info(f"  Entries needing series verification: {len(needs_verification)}")

    if needs_verification and GEMINI_KEY:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash")

        BATCH_SIZE = 25
        total_batches = (len(needs_verification) + BATCH_SIZE - 1) // BATCH_SIZE
        verified_counts = {}

        for batch_idx in range(total_batches):
            start = batch_idx * BATCH_SIZE
            end = min(start + BATCH_SIZE, len(needs_verification))
            batch = needs_verification[start:end]

            titles_text = "\n".join(
                f'{i+1}. "{row.get("Book Series Name", "")}" by {row.get("Author Name", "")} '
                f'(listed as: {row.get("Books in Series", "unknown")} books, '
                f'first book: "{row.get("First Book Name", "N/A")}")'
                for i, (idx, row) in enumerate(batch)
            )

            prompt = f"""You are a book series expert. For each title below, determine:
1. Is this actually part of a book SERIES (not standalone)?
2. How many books are in the complete series?
3. Is this a romance/drama series?

Titles:
{titles_text}

Return JSON array:
[
  {{
    "title": "series name",
    "author": "author",
    "is_series": true/false,
    "books_in_series": number_or_null,
    "first_book": "first book title if known",
    "notes": "brief note"
  }}
]

IMPORTANT: Only return data you're confident about. Use null if unsure about book count.
Return ONLY the JSON array."""

            try:
                response = model.generate_content(prompt)
                text = response.text.strip()

                # Parse JSON
                if text.startswith("```json"):
                    text = text[7:]
                if text.startswith("```"):
                    text = text[3:]
                if text.endswith("```"):
                    text = text[:-3]
                text = re.sub(r',\s*([}\]])', r'\1', text.strip())

                try:
                    data = json.loads(text)
                except json.JSONDecodeError:
                    match = re.search(r'\[[\s\S]*\]', text)
                    if match:
                        data = json.loads(re.sub(r',\s*([}\]])', r'\1', match.group()))
                    else:
                        data = []

                for item in data:
                    title = item.get("title", "").lower().strip()
                    verified_counts[title] = item

                log.info(f"    Batch {batch_idx+1}/{total_batches}: verified {len(data)} entries")

            except Exception as e:
                log.error(f"    Batch {batch_idx+1} error: {e}")

            time.sleep(1.5)

        # Apply verified data
        updated = 0
        for idx, row in needs_verification:
            title = str(row.get("Book Series Name", "")).lower().strip()
            if title in verified_counts:
                info = verified_counts[title]
                if info.get("books_in_series"):
                    df.at[idx, "Books in Series"] = info["books_in_series"]
                    # Update first book if we got it
                    if info.get("first_book"):
                        current_fb = str(df.at[idx, "First Book Name"]).strip()
                        if not current_fb or current_fb == "nan":
                            df.at[idx, "First Book Name"] = info["first_book"]
                    updated += 1

        log.info(f"  Updated book counts for {updated} entries via Gemini")

    # ── Step 4: Apply series length filter (>= 3 books) ─────
    log.info("\n  STEP 4: Applying series length filter (>= 3 books)")

    before_filter = len(df)

    # Now filter: keep only series with >= 3 books OR unknown count (benefit of doubt for now)
    def passes_length_filter(row):
        count = get_books_count(row)
        if count is None:
            # Unknown count - keep if it has other quality signals
            # (has rating, has pages, or has first book name)
            has_rating = pd.notna(row.get("First Book Rating")) and str(row.get("First Book Rating")).strip() not in ["", "nan"]
            has_first_book = pd.notna(row.get("First Book Name")) and str(row.get("First Book Name")).strip() not in ["", "nan"]
            return has_rating or has_first_book
        return count >= 3

    mask = df.apply(passes_length_filter, axis=1)
    removed_short = df[~mask]
    df = df[mask].copy()

    short_removed = before_filter - len(df)
    log.info(f"  Short series (< 3 books) removed: {short_removed}")

    if len(removed_short) > 0:
        removed_short.to_csv(DATA_DIR / "removed_short_series.csv", index=False)
        log.info(f"  Removed entries saved to: removed_short_series.csv")

        # Log some examples
        for _, r in removed_short.head(10).iterrows():
            log.info(f"    Removed: '{r.get('Book Series Name', '')}' by {r.get('Author Name', '')} "
                     f"({r.get('Books in Series', 'unknown')} books, {r.get('Primary Subgenre', '')})")

    # ── Step 5: Estimate adaptation hours ───────────────────
    log.info("\n  STEP 5: Adaptation hours estimation")

    for idx, row in df.iterrows():
        count = get_books_count(row)
        total_pages = None
        try:
            total_pages = float(row.get("Total Pages", 0))
        except (ValueError, TypeError):
            pass

        if (not total_pages or total_pages <= 0) and count:
            # Estimate: 300 pages per book average
            total_pages = count * 300
            df.at[idx, "Total Pages"] = total_pages

        if total_pages and total_pages > 0:
            hours = round(total_pages / 33.33, 1)
            df.at[idx, "Length of Adaption in Hours"] = hours

    # Flag titles under 40 hours
    under_40h = 0
    for idx, row in df.iterrows():
        try:
            hours = float(row.get("Length of Adaption in Hours", 0))
            if 0 < hours < 40:
                under_40h += 1
        except (ValueError, TypeError):
            pass

    log.info(f"  Titles under 40 hours: {under_40h} (kept for now, flagged)")

    # ── Step 6: Final dedup ─────────────────────────────────
    log.info("\n  STEP 6: Final deduplication")

    df["_key"] = (
        df["Book Series Name"].astype(str).str.lower().str.strip()
        + "|"
        + df["Author Name"].astype(str).str.lower().str.strip()
    )
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["_key"], keep="first").drop(columns=["_key"])
    log.info(f"  Dedup: {before_dedup} -> {len(df)} ({before_dedup - len(df)} removed)")

    # ── Final stats ─────────────────────────────────────────
    log.info("\n  FINAL AUDIT RESULTS:")
    log.info(f"  Total series: {len(df)}")
    log.info(f"\n  Subgenre breakdown:")
    for sg, count in df["Primary Subgenre"].value_counts().items():
        log.info(f"    {sg}: {count}")

    # Book count distribution
    known_counts = df["Books in Series"].dropna().astype(str)
    valid_counts = []
    for c in known_counts:
        try:
            valid_counts.append(int(float(c)))
        except:
            pass

    if valid_counts:
        log.info(f"\n  Book count distribution (of {len(valid_counts)} with known counts):")
        log.info(f"    3-5 books: {sum(1 for c in valid_counts if 3 <= c <= 5)}")
        log.info(f"    6-10 books: {sum(1 for c in valid_counts if 6 <= c <= 10)}")
        log.info(f"    11-20 books: {sum(1 for c in valid_counts if 11 <= c <= 20)}")
        log.info(f"    20+ books: {sum(1 for c in valid_counts if c > 20)}")

    output = DATA_DIR / "selfpub_master_audited.csv"
    df.to_csv(output, index=False)
    log.info(f"\n  Saved to: {output}")


if __name__ == "__main__":
    start = datetime.now()
    run_audit()
    elapsed = datetime.now() - start
    log.info(f"\n  Audit completed in {elapsed}")
