#!/usr/bin/env python3
"""
Gemini Bulk Series Verification
=================================
Fast series verification using Gemini API in bulk batches.

Takes the partially verified dataset and sends remaining unverified entries
through Gemini in batches of 80-100 to determine:
  - Is this a standalone book or part of a series?
  - If series: how many books in the series?
  - Series name (if different from title)

Auto-stop: if 500 consecutive entries come back as "standalone" or "unknown",
stop processing that subgenre and move to the next.

After verification: filters to series >= 3 books.

Run AFTER series_verification.py (or its partial results).
"""

import os
import json
import re
import time
import logging
from pathlib import Path
from datetime import datetime
from collections import defaultdict

import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"
DATA_DIR.mkdir(exist_ok=True)

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "gemini_series_check.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("gemini_series")

# ── Gemini Setup ───────────────────────────────────────────
def get_gemini_key():
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()

# ── Auto-stop config ──────────────────────────────────────
CONSECUTIVE_MISS_THRESHOLD = 400  # Stop subgenre after N consecutive non-series results
BATCH_SIZE = 50                    # Titles per Gemini call
SLEEP_BETWEEN_BATCHES = 2.0       # Rate limit
SAVE_INTERVAL = 5                  # Save every N batches


def _robust_json_parse(text):
    """Parse JSON with multiple fallback strategies."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    # Fix trailing commas
    text = re.sub(r',\s*([}\]])', r'\1', text)

    try:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        pass

    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        try:
            cleaned = re.sub(r',\s*([}\]])', r'\1', match.group())
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

    # Individual objects
    objects = []
    for m in re.finditer(r'\{[^{}]+\}', text):
        try:
            objects.append(json.loads(m.group()))
        except json.JSONDecodeError:
            continue
    return objects


def gemini_check_series_batch(model, entries):
    """Send a batch of entries to Gemini to check if they're series or standalone.

    Args:
        model: Gemini model instance
        entries: list of dicts with 'title', 'author', 'first_book', 'subgenre'

    Returns:
        list of dicts with 'title', 'is_series', 'books_in_series', 'series_name'
    """
    titles_text = "\n".join(
        f'{i+1}. "{e["title"]}" by {e["author"]}'
        f'{" (First book: " + e["first_book"] + ")" if e.get("first_book") and e["first_book"] != "nan" else ""}'
        for i, e in enumerate(entries)
    )

    # Get the dominant subgenre for context
    subgenres = [e.get("subgenre", "") for e in entries]
    dominant_sg = max(set(subgenres), key=subgenres.count) if subgenres else "Romance"

    prompt = f"""You are a book database expert. For each title below, determine whether it is a STANDALONE book or part of a book SERIES.

Context: These are {dominant_sg} titles.

Titles:
{titles_text}

For EACH title, return:
- "title": the title as given
- "is_series": true if this book is part of a series with 2+ books, false if standalone
- "books_in_series": integer number of books in the complete series (null if standalone or unknown)
- "series_name": the name of the series if different from the title (null if standalone)
- "pub_type": "Self-Pub", "Indie", "Small Press", or "Traditional" — based on the publisher/imprint
- "publisher": the publisher or imprint name if known (null if unknown)

IMPORTANT RULES:
- A "series" means there are multiple books by the same author in a connected storyline or shared world
- Box sets, omnibus editions, and collections count as series
- If the title contains "Book 1", "#1", "Volume 1", etc. it is definitely a series
- If you're uncertain about series status, mark is_series as null (not false) — we'll verify separately
- Be conservative: only mark is_series=false if you're confident it's truly standalone
- For pub_type: "Self-Pub" = KDP/independently published, "Indie" = small indie publisher, "Small Press" = small but established press, "Traditional" = Big 5 or major imprints (Penguin, HarperCollins, Hachette, S&S, Macmillan and their imprints like Avon, Berkley, Forever, Montlake, etc.)
- Return data for ALL {len(entries)} titles in order

Return ONLY a JSON array:
[
  {{"title": "...", "is_series": true/false/null, "books_in_series": N_or_null, "series_name": "..._or_null", "pub_type": "...", "publisher": "..._or_null"}}
]"""

    try:
        response = model.generate_content(
            prompt,
            request_options={"timeout": 120},
        )
        text = response.text.strip()
        data = _robust_json_parse(text)
        return data
    except Exception as e:
        log.warning(f"    Gemini error: {e}")
        return []


def run_gemini_check():
    """Run Gemini bulk series verification."""
    log.info("=" * 70)
    log.info("  GEMINI BULK SERIES VERIFICATION")
    log.info("=" * 70)

    if not GEMINI_KEY:
        log.error("No GEMINI_API_KEY found!")
        return

    # Load data — prefer Gemini partial (has prior results), then other partials
    candidates = [
        DATA_DIR / "selfpub_master_gemini_verified_partial.csv",
        DATA_DIR / "selfpub_master_series_verified_partial.csv",
        DATA_DIR / "selfpub_master_series_verified.csv",
        DATA_DIR / "selfpub_master_mega_expanded.csv",
        DATA_DIR / "selfpub_master_multi_platform.csv",
    ]

    source = None
    for c in candidates:
        if c.exists():
            source = c
            break

    if not source:
        log.error("No data file found!")
        return

    df = pd.read_csv(source, on_bad_lines="skip", low_memory=False)
    log.info(f"  Loaded {len(df)} entries from {source.name}")

    # Ensure verification columns exist
    for col in ["Series_Verified", "Verification_Method", "Verified_Series_Name", "Verified_Books_Count"]:
        if col not in df.columns:
            df[col] = ""

    # Count what's already verified
    already_series = (df["Series_Verified"].isin(["Yes"])).sum()
    already_likely = (df["Series_Verified"].isin(["Likely"])).sum()
    already_no = (df["Series_Verified"].isin(["No"])).sum()

    log.info(f"  Already verified as SERIES: {already_series}")
    log.info(f"  Already LIKELY series: {already_likely}")
    log.info(f"  Already confirmed STANDALONE: {already_no}")

    # Identify entries needing Gemini check
    def needs_gemini(row):
        # Already verified by previous layers or prior Gemini run
        verified = str(row.get("Series_Verified", "")).strip()
        if verified in ["Yes", "No", "Unknown"]:
            return False
        method = str(row.get("Verification_Method", "")).strip()
        if method.startswith("gemini_bulk"):
            return False
        # Already has 3+ books confirmed
        try:
            count = int(float(row.get("Books in Series", 0)))
            if count >= 3:
                return False
        except:
            pass
        return True

    needs_check_mask = df.apply(needs_gemini, axis=1)
    needs_check_indices = df[needs_check_mask].index.tolist()
    log.info(f"  Entries needing Gemini check: {len(needs_check_indices)}")

    # Group by subgenre for better context
    subgenre_groups = defaultdict(list)
    for idx in needs_check_indices:
        row = df.loc[idx]
        sg = str(row.get("Primary Subgenre", "Unknown"))
        subgenre_groups[sg].append(idx)

    log.info(f"\n  Entries by subgenre:")
    for sg, indices in sorted(subgenre_groups.items(), key=lambda x: -len(x[1])):
        log.info(f"    {sg}: {len(indices)}")

    # Initialize Gemini
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    total_series_found = 0
    total_standalone_found = 0
    total_unknown = 0
    total_checked = 0
    total_batches = 0

    # Process each subgenre
    for sg, indices in sorted(subgenre_groups.items(), key=lambda x: -len(x[1])):
        log.info(f"\n  ── Processing: {sg} ({len(indices)} entries) ──")

        consecutive_misses = 0
        sg_series = 0
        sg_standalone = 0
        sg_checked = 0
        auto_stopped = False

        for batch_start in range(0, len(indices), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(indices))
            batch_indices = indices[batch_start:batch_end]

            # Build batch entries
            batch_entries = []
            for idx in batch_indices:
                row = df.loc[idx]
                batch_entries.append({
                    "title": str(row.get("Book Series Name", "")),
                    "author": str(row.get("Author Name", "")),
                    "first_book": str(row.get("First Book Name", "")),
                    "subgenre": sg,
                })

            # Call Gemini
            results = gemini_check_series_batch(model, batch_entries)

            # Match results back to indices
            batch_new_series = 0
            batch_standalone = 0

            for i, idx in enumerate(batch_indices):
                # Try to match result by position or title
                result = None
                if i < len(results):
                    result = results[i]
                else:
                    # Try matching by title
                    title_lower = str(df.at[idx, "Book Series Name"]).lower().strip()
                    for r in results:
                        if str(r.get("title", "")).lower().strip() == title_lower:
                            result = r
                            break

                if result:
                    is_series = result.get("is_series")
                    books_count = result.get("books_in_series")
                    series_name = result.get("series_name")
                    pub_type = result.get("pub_type")
                    publisher = result.get("publisher")

                    # Always apply pub_type and publisher if available
                    if pub_type and str(pub_type).lower() not in ["null", "none", ""]:
                        df.at[idx, "Self Pub Flag"] = str(pub_type)
                    if publisher and str(publisher).lower() not in ["null", "none", ""]:
                        current_pub = str(df.at[idx, "Publisher Name"]) if pd.notna(df.at[idx, "Publisher Name"]) else ""
                        if not current_pub or current_pub in ["", "nan"]:
                            df.at[idx, "Publisher Name"] = str(publisher)

                    if is_series is True:
                        df.at[idx, "Series_Verified"] = "Yes"
                        df.at[idx, "Verification_Method"] = "gemini_bulk"
                        if series_name and str(series_name).lower() not in ["null", "none", ""]:
                            df.at[idx, "Verified_Series_Name"] = str(series_name)
                        if books_count and str(books_count).lower() not in ["null", "none", ""]:
                            try:
                                bc = int(float(books_count))
                                if bc > 0:
                                    df.at[idx, "Verified_Books_Count"] = bc
                                    # Update Books in Series if Gemini gives better count
                                    current = df.at[idx, "Books in Series"]
                                    try:
                                        current_count = int(float(current)) if pd.notna(current) else 0
                                    except:
                                        current_count = 0
                                    if bc > current_count:
                                        df.at[idx, "Books in Series"] = bc
                            except:
                                pass

                        batch_new_series += 1
                        consecutive_misses = 0

                    elif is_series is False:
                        df.at[idx, "Series_Verified"] = "No"
                        df.at[idx, "Verification_Method"] = "gemini_bulk_standalone"
                        batch_standalone += 1
                        consecutive_misses += 1

                    else:
                        # null / unknown
                        df.at[idx, "Series_Verified"] = "Unknown"
                        df.at[idx, "Verification_Method"] = "gemini_bulk_uncertain"
                        total_unknown += 1
                        consecutive_misses += 1
                else:
                    # No result for this entry
                    consecutive_misses += 1

            sg_series += batch_new_series
            sg_standalone += batch_standalone
            sg_checked += len(batch_indices)
            total_series_found += batch_new_series
            total_standalone_found += batch_standalone
            total_checked += len(batch_indices)
            total_batches += 1

            batch_num = batch_start // BATCH_SIZE + 1
            total_sg_batches = (len(indices) + BATCH_SIZE - 1) // BATCH_SIZE
            log.info(f"    Batch {batch_num}/{total_sg_batches}: "
                    f"+{batch_new_series} series, {batch_standalone} standalone "
                    f"(consecutive misses: {consecutive_misses})")

            # Auto-stop check
            if consecutive_misses >= CONSECUTIVE_MISS_THRESHOLD:
                remaining = len(indices) - sg_checked
                log.info(f"    AUTO-STOP: {consecutive_misses} consecutive non-series results. "
                        f"Skipping remaining {remaining} entries for {sg}.")

                # Mark remaining as "Unknown" (benefit of doubt — don't mark standalone)
                for skip_idx in indices[batch_end:]:
                    df.at[skip_idx, "Series_Verified"] = "Skipped"
                    df.at[skip_idx, "Verification_Method"] = "auto_stop_skipped"

                auto_stopped = True
                break

            # Save intermediate results
            if total_batches % SAVE_INTERVAL == 0:
                intermediate = DATA_DIR / "selfpub_master_gemini_verified_partial.csv"
                df.to_csv(intermediate, index=False)
                log.info(f"    [Intermediate save: {intermediate.name}]")

            time.sleep(SLEEP_BETWEEN_BATCHES)

        log.info(f"  [{sg}] Done: {sg_series} series, {sg_standalone} standalone "
                f"(checked {sg_checked}/{len(indices)})"
                f"{' [AUTO-STOPPED]' if auto_stopped else ''}")

    # ── Final save ────────────────────────────────────────
    log.info(f"\n  {'='*60}")
    log.info(f"  GEMINI VERIFICATION COMPLETE")
    log.info(f"  {'='*60}")
    log.info(f"  Total checked: {total_checked}")
    log.info(f"  Series found: {total_series_found}")
    log.info(f"  Standalone confirmed: {total_standalone_found}")
    log.info(f"  Unknown/uncertain: {total_unknown}")
    log.info(f"  Batches processed: {total_batches}")

    # Save full verified dataset
    verified_output = DATA_DIR / "selfpub_master_gemini_verified.csv"
    df.to_csv(verified_output, index=False)
    log.info(f"\n  Full verified data saved to: {verified_output}")

    # ── Now filter to series >= 3 books ───────────────────
    log.info(f"\n  {'='*60}")
    log.info(f"  FILTERING TO SERIES >= 3 BOOKS")
    log.info(f"  {'='*60}")

    def is_viable_series(row):
        """Check if entry should be kept (series with 3+ books, or strong signals)."""
        # Check verified book count
        verified_count = row.get("Verified_Books_Count")
        try:
            vc = int(float(verified_count)) if pd.notna(verified_count) else 0
        except:
            vc = 0

        books_in_series = row.get("Books in Series")
        try:
            bs = int(float(books_in_series)) if pd.notna(books_in_series) else 0
        except:
            bs = 0

        best_count = max(vc, bs)

        # Definite keeps
        if best_count >= 3:
            return True

        # Verified as series but count unknown — keep (benefit of doubt)
        verified = str(row.get("Series_Verified", "")).strip()
        if verified == "Yes" and best_count == 0:
            return True

        # Likely series from author clustering — keep if count >= 2 or unknown
        if verified == "Likely" and best_count != 1:
            return True

        # Box set pattern detected — definitely keep
        method = str(row.get("Verification_Method", ""))
        if "box" in method.lower() or "title_pattern" in method.lower():
            # Title pattern matched — this has series indicators
            if best_count >= 2 or best_count == 0:
                return True

        # Skipped (auto-stop) — keep if has quality signals
        if verified == "Skipped":
            has_rating = pd.notna(row.get("First Book Rating")) and str(row.get("First Book Rating")).strip() not in ["", "nan"]
            has_first_book = pd.notna(row.get("First Book Name")) and str(row.get("First Book Name")).strip() not in ["", "nan"]
            if has_rating or has_first_book:
                return True

        # Confirmed standalone — remove
        if verified == "No":
            return False

        # Unknown with count 1-2 — remove
        if best_count in [1, 2]:
            return False

        # Everything else: keep if it has any quality signal
        has_rating = pd.notna(row.get("First Book Rating")) and str(row.get("First Book Rating")).strip() not in ["", "nan"]
        return has_rating

    keep_mask = df.apply(is_viable_series, axis=1)
    df_filtered = df[keep_mask].copy()
    removed = df[~keep_mask].copy()

    log.info(f"  Before filter: {len(df)}")
    log.info(f"  After filter (viable series): {len(df_filtered)}")
    log.info(f"  Removed: {len(removed)}")

    # Save removed entries for reference
    if len(removed) > 0:
        removed.to_csv(DATA_DIR / "removed_not_series.csv", index=False)

    # Subgenre breakdown
    log.info(f"\n  Filtered dataset by subgenre:")
    for sg, count in df_filtered["Primary Subgenre"].value_counts().items():
        log.info(f"    {sg}: {count}")

    # Book count distribution in filtered set
    valid_counts = []
    for _, row in df_filtered.iterrows():
        try:
            c = int(float(row.get("Books in Series", 0)))
            if c > 0:
                valid_counts.append(c)
        except:
            pass

    if valid_counts:
        log.info(f"\n  Book count distribution (filtered, {len(valid_counts)} with known counts):")
        log.info(f"    1-2 books (kept for signals): {sum(1 for c in valid_counts if c <= 2)}")
        log.info(f"    3-5 books: {sum(1 for c in valid_counts if 3 <= c <= 5)}")
        log.info(f"    6-10 books: {sum(1 for c in valid_counts if 6 <= c <= 10)}")
        log.info(f"    11-20 books: {sum(1 for c in valid_counts if 11 <= c <= 20)}")
        log.info(f"    20+ books: {sum(1 for c in valid_counts if c > 20)}")

    # Verification method breakdown
    log.info(f"\n  How entries were verified:")
    for method, count in df_filtered["Verification_Method"].value_counts().head(15).items():
        if method and str(method) not in ["", "nan"]:
            log.info(f"    {method}: {count}")

    # Publisher type breakdown
    log.info(f"\n  Publisher type breakdown:")
    pub_counts = df_filtered["Self Pub Flag"].value_counts()
    for pub_type, count in pub_counts.items():
        if pub_type and str(pub_type) not in ["", "nan"]:
            log.info(f"    {pub_type}: {count}")

    selfpub_indie = df_filtered[
        df_filtered["Self Pub Flag"].astype(str).str.lower().isin(["self-pub", "indie", "small press"])
    ]
    log.info(f"\n  Self-Pub + Indie + Small Press: {len(selfpub_indie)} ({100*len(selfpub_indie)/max(len(df_filtered),1):.1f}%)")
    log.info(f"  Traditional: {len(df_filtered) - len(selfpub_indie)} ({100*(len(df_filtered)-len(selfpub_indie))/max(len(df_filtered),1):.1f}%)")

    # Final dedup
    log.info(f"\n  Final deduplication...")
    df_filtered["_key"] = (
        df_filtered["Book Series Name"].astype(str).str.lower().str.strip()
        + "|"
        + df_filtered["Author Name"].astype(str).str.lower().str.strip()
    )
    before_dedup = len(df_filtered)
    df_filtered = df_filtered.drop_duplicates(subset=["_key"], keep="first").drop(columns=["_key"])
    log.info(f"  Dedup: {before_dedup} -> {len(df_filtered)} ({before_dedup - len(df_filtered)} removed)")

    # Save filtered dataset
    filtered_output = DATA_DIR / "selfpub_series_ready_for_enrichment.csv"
    df_filtered.to_csv(filtered_output, index=False)
    log.info(f"\n  Filtered dataset saved to: {filtered_output}")
    log.info(f"  This dataset is ready for Goodreads enrichment.")

    return df_filtered


if __name__ == "__main__":
    start = datetime.now()
    result = run_gemini_check()
    elapsed = datetime.now() - start
    hours = elapsed.total_seconds() / 3600
    log.info(f"\n  Completed in {elapsed} ({hours:.1f} hours)")
