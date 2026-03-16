#!/usr/bin/env python3
"""
Gemini Batch Enrichment — Fill Missing Data for Priority Dataset
================================================================
Three-phase enrichment pipeline using Gemini 2.5 Flash:
  Phase 1: Book metadata (description, trope, era, type, pages, differentiator)
  Phase 2: Objective validation (bestseller lists, awards, recognition)
  Phase 3: Author contact enrichment (email, website, social links)

Usage:
  python3 gemini_enrich_all.py --phase all     # Run all three phases sequentially
  python3 gemini_enrich_all.py --phase 1       # Only Phase 1: Book Metadata
  python3 gemini_enrich_all.py --phase 2       # Only Phase 2: Objective Validation
  python3 gemini_enrich_all.py --phase 3       # Only Phase 3: Author Contact
"""

import os
import sys
import json
import re
import time
import logging
import argparse
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"
INPUT_FILE = DATA_DIR / "PRIORITY_SELFPUB_ENRICHED.csv"
PARTIAL_FILE = DATA_DIR / "PRIORITY_SELFPUB_ENRICHED_partial.csv"
LOG_FILE = DATA_DIR / "gemini_enrich_all.log"

# ── Args ───────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Gemini Batch Enrichment for Priority Dataset")
parser.add_argument(
    "--phase",
    type=str,
    default="all",
    choices=["1", "2", "3", "all"],
    help="Which phase to run: 1 (metadata), 2 (validation), 3 (contacts), or all",
)
args = parser.parse_args()

# ── Logging ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("gemini_enrich")

# ── Gemini Setup ───────────────────────────────────────────────
def get_gemini_key():
    """Load API key from .env file or environment variable."""
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")


GEMINI_KEY = get_gemini_key()

# ── Config ─────────────────────────────────────────────────────
PHASE1_BATCH_SIZE = 50
PHASE2_BATCH_SIZE = 30
PHASE3_BATCH_SIZE = 40
SLEEP_BETWEEN = 0.5
SAVE_INTERVAL = 5  # Save after every N batches
BACKOFF_START = 5   # Initial backoff in seconds
BACKOFF_MAX = 60    # Maximum backoff in seconds
MAX_RETRIES = 5     # Max retries per batch on rate limit


# ── Robust JSON Parser (from gemini_fast_verify.py) ───────────
def _robust_json_parse(text):
    """Parse JSON from Gemini response, handling markdown fences and trailing commas."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    text = re.sub(r',\s*([}\]])', r'\1', text)

    try:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        pass

    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        try:
            return json.loads(re.sub(r',\s*([}\]])', r'\1', match.group()))
        except json.JSONDecodeError:
            pass

    objects = []
    for m in re.finditer(r'\{[^{}]+\}', text):
        try:
            objects.append(json.loads(m.group()))
        except json.JSONDecodeError:
            continue
    return objects


# ── Helper: Check if a value is effectively missing ───────────
def is_missing(val):
    """Return True if value is NaN, None, empty string, or 'nan'."""
    if val is None:
        return True
    if isinstance(val, float) and np.isnan(val):
        return True
    s = str(val).strip().lower()
    return s in ("", "nan", "none", "null", "nat")


# ── Helper: Gemini call with exponential backoff ──────────────
def call_gemini_with_backoff(model, prompt, batch_label=""):
    """Call Gemini with retry and exponential backoff on rate limit errors."""
    backoff = BACKOFF_START
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = model.generate_content(
                prompt,
                request_options={"timeout": 180},
            )
            text = response.text.strip()
            return _robust_json_parse(text)
        except Exception as e:
            err_str = str(e).lower()
            is_rate_limit = any(keyword in err_str for keyword in [
                "429", "rate", "quota", "resource_exhausted", "too many requests"
            ])
            if is_rate_limit and attempt < MAX_RETRIES:
                log.warning(
                    f"    Rate limit on {batch_label} (attempt {attempt}/{MAX_RETRIES}). "
                    f"Backing off {backoff}s..."
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            else:
                log.warning(f"    Gemini error on {batch_label} (attempt {attempt}): {e}")
                return []
    return []


# ── Helper: Match result to entry by title ────────────────────
def match_result_to_title(results, target_title):
    """Find the result dict that matches the given title (case-insensitive)."""
    target_lower = str(target_title).lower().strip()
    for r in results:
        if str(r.get("title", "")).lower().strip() == target_lower:
            return r
    return None


# ══════════════════════════════════════════════════════════════
#  PHASE 1: Book Metadata Enrichment
# ══════════════════════════════════════════════════════════════
def phase1_metadata(df, model):
    """Fill missing Subjective Analysis, Primary Trope, Series_Era, Type, Total Pages, Differentiator."""
    log.info("=" * 70)
    log.info("  PHASE 1: Book Metadata Enrichment")
    log.info("=" * 70)

    # Ensure columns exist
    for col in ["Subjective Analysis", "Primary Trope", "Series_Era", "Type",
                 "Total Pages", "Differentiator"]:
        if col not in df.columns:
            df[col] = ""

    # Find rows that need enrichment (any of the target fields missing)
    def needs_phase1(row):
        return (
            is_missing(row.get("Subjective Analysis"))
            or is_missing(row.get("Primary Trope"))
            or is_missing(row.get("Series_Era"))
            or is_missing(row.get("Differentiator"))
            or is_missing(row.get("Total Pages"))
            or is_missing(row.get("Type"))
        )

    mask = df.apply(needs_phase1, axis=1)
    indices = df[mask].index.tolist()
    log.info(f"  Phase 1: {len(indices)} entries need enrichment (out of {len(df)} total)")

    if not indices:
        log.info("  Phase 1: Nothing to do, all fields populated.")
        return df

    total_batches = 0
    total_updated = 0

    for batch_start in range(0, len(indices), PHASE1_BATCH_SIZE):
        batch_end = min(batch_start + PHASE1_BATCH_SIZE, len(indices))
        batch_idx = indices[batch_start:batch_end]
        batch_num = batch_start // PHASE1_BATCH_SIZE + 1
        total_batches_needed = (len(indices) + PHASE1_BATCH_SIZE - 1) // PHASE1_BATCH_SIZE

        # Build entries for this batch
        entries_text_parts = []
        for i, idx in enumerate(batch_idx):
            row = df.loc[idx]
            series_name = str(row.get("Book Series Name", "Unknown"))
            author = str(row.get("Author Name", "Unknown"))
            books_count = row.get("Books in Series", "")
            try:
                books_count = int(float(books_count)) if not is_missing(books_count) else "unknown"
            except (ValueError, TypeError):
                books_count = "unknown"
            subgenre = str(row.get("Primary Subgenre", "Romance"))
            if is_missing(subgenre):
                subgenre = "Romance"
            entries_text_parts.append(
                f'{i + 1}. "{series_name}" by {author} ({books_count} books, Subgenre: {subgenre})'
            )

        entries_text = "\n".join(entries_text_parts)

        prompt = f"""You are a romance book expert. For each series below, provide metadata.

Series:
{entries_text}

For EACH series return:
- "title": series name as given
- "description": 1-2 sentence compelling hook/description
- "trope": primary romance trope (e.g., enemies-to-lovers, second-chance, forbidden-love, fake-dating, marriage-of-convenience, friends-to-lovers, boss-employee, age-gap, forced-proximity, grumpy-sunshine, etc.)
- "era": "Contemporary", "Historical", or "Mixed"
- "differentiator": What makes this unique (1 sentence)
- "est_pages_per_book": estimated pages per book (integer)

Return ONLY a JSON array."""

        results = call_gemini_with_backoff(model, prompt, batch_label=f"Phase1 batch {batch_num}")

        batch_updated = 0
        for i, idx in enumerate(batch_idx):
            row = df.loc[idx]
            series_name = str(row.get("Book Series Name", ""))

            # Try positional match first, then title match
            result = results[i] if i < len(results) else None
            if result and str(result.get("title", "")).lower().strip() != series_name.lower().strip():
                # Positional mismatch — try title match
                matched = match_result_to_title(results, series_name)
                if matched:
                    result = matched

            if not result:
                result = match_result_to_title(results, series_name)

            if not result:
                continue

            updated = False

            # Subjective Analysis
            desc = result.get("description")
            if desc and not is_missing(desc) and is_missing(df.at[idx, "Subjective Analysis"]):
                df.at[idx, "Subjective Analysis"] = str(desc)
                updated = True

            # Primary Trope
            trope = result.get("trope")
            if trope and not is_missing(trope) and is_missing(df.at[idx, "Primary Trope"]):
                df.at[idx, "Primary Trope"] = str(trope)
                updated = True

            # Series_Era
            era = result.get("era")
            if era and not is_missing(era) and is_missing(df.at[idx, "Series_Era"]):
                df.at[idx, "Series_Era"] = str(era)
                updated = True

            # Differentiator
            diff = result.get("differentiator")
            if diff and not is_missing(diff) and is_missing(df.at[idx, "Differentiator"]):
                df.at[idx, "Differentiator"] = str(diff)
                updated = True

            # Total Pages — estimate if missing
            est_pages = result.get("est_pages_per_book")
            if est_pages and is_missing(df.at[idx, "Total Pages"]):
                try:
                    pages_per = int(float(est_pages))
                    books_count = df.at[idx, "Books in Series"]
                    try:
                        n_books = int(float(books_count)) if not is_missing(books_count) else 1
                    except (ValueError, TypeError):
                        n_books = 1
                    total_pages = pages_per * max(n_books, 1)
                    df.at[idx, "Total Pages"] = total_pages
                    # Also compute adaptation hours (33.33 pages/hour)
                    if "Length of Adaption in Hours" in df.columns:
                        df.at[idx, "Length of Adaption in Hours"] = round(total_pages / 33.33, 1)
                    updated = True
                except (ValueError, TypeError):
                    pass

            # Type — derive from Books in Series if missing
            if is_missing(df.at[idx, "Type"]):
                try:
                    books_count = df.at[idx, "Books in Series"]
                    n_books = int(float(books_count)) if not is_missing(books_count) else 0
                    if n_books >= 7:
                        df.at[idx, "Type"] = "Long Series"
                        updated = True
                    elif n_books >= 3:
                        df.at[idx, "Type"] = "Series"
                        updated = True
                except (ValueError, TypeError):
                    pass

            if updated:
                batch_updated += 1

        total_updated += batch_updated
        total_batches += 1
        log.info(
            f"    Phase 1 Batch {batch_num}/{total_batches_needed}: "
            f"updated {batch_updated}/{len(batch_idx)} entries "
            f"(total: {total_updated})"
        )

        # Save partial progress
        if total_batches % SAVE_INTERVAL == 0:
            df.to_csv(PARTIAL_FILE, index=False)
            log.info(f"    [Partial save: {PARTIAL_FILE.name}]")

        time.sleep(SLEEP_BETWEEN)

    log.info(f"  Phase 1 COMPLETE: Updated {total_updated} entries in {total_batches} batches")
    return df


# ══════════════════════════════════════════════════════════════
#  PHASE 2: Objective Validation
# ══════════════════════════════════════════════════════════════
def phase2_validation(df, model):
    """Check bestseller lists and notable recognition for all entries."""
    log.info("=" * 70)
    log.info("  PHASE 2: Objective Validation (Bestseller/Recognition)")
    log.info("=" * 70)

    # Ensure column exists
    if "Objective_Validation_Source" not in df.columns:
        df["Objective_Validation_Source"] = ""

    # Find rows needing validation (skip those already filled)
    def needs_phase2(row):
        return is_missing(row.get("Objective_Validation_Source"))

    mask = df.apply(needs_phase2, axis=1)
    indices = df[mask].index.tolist()
    log.info(f"  Phase 2: {len(indices)} entries need validation (out of {len(df)} total)")

    if not indices:
        log.info("  Phase 2: Nothing to do, all entries already validated.")
        return df

    total_batches = 0
    total_updated = 0

    for batch_start in range(0, len(indices), PHASE2_BATCH_SIZE):
        batch_end = min(batch_start + PHASE2_BATCH_SIZE, len(indices))
        batch_idx = indices[batch_start:batch_end]
        batch_num = batch_start // PHASE2_BATCH_SIZE + 1
        total_batches_needed = (len(indices) + PHASE2_BATCH_SIZE - 1) // PHASE2_BATCH_SIZE

        # Build entries text
        entries_text_parts = []
        for i, idx in enumerate(batch_idx):
            row = df.loc[idx]
            series_name = str(row.get("Book Series Name", "Unknown"))
            author = str(row.get("Author Name", "Unknown"))
            entries_text_parts.append(f'{i + 1}. "{series_name}" by {author}')

        entries_text = "\n".join(entries_text_parts)

        prompt = f"""You are a book industry expert. For each series, identify if it has appeared on any bestseller lists or received notable recognition.

Series:
{entries_text}

For EACH, return:
- "title": series name
- "validation": semicolon-separated list of recognitions (e.g., "NYT Bestseller; Amazon #1 Romance; BookTok Viral; Goodreads Choice Nominee 2023"). Return "" if none known.

Return ONLY a JSON array."""

        results = call_gemini_with_backoff(model, prompt, batch_label=f"Phase2 batch {batch_num}")

        batch_updated = 0
        for i, idx in enumerate(batch_idx):
            row = df.loc[idx]
            series_name = str(row.get("Book Series Name", ""))

            result = results[i] if i < len(results) else None
            if result and str(result.get("title", "")).lower().strip() != series_name.lower().strip():
                matched = match_result_to_title(results, series_name)
                if matched:
                    result = matched

            if not result:
                result = match_result_to_title(results, series_name)

            if result:
                validation = result.get("validation", "")
                if validation is None:
                    validation = ""
                df.at[idx, "Objective_Validation_Source"] = str(validation)
                batch_updated += 1
            else:
                # Mark as checked but no data returned
                df.at[idx, "Objective_Validation_Source"] = ""

        total_updated += batch_updated
        total_batches += 1
        log.info(
            f"    Phase 2 Batch {batch_num}/{total_batches_needed}: "
            f"validated {batch_updated}/{len(batch_idx)} entries "
            f"(total: {total_updated})"
        )

        if total_batches % SAVE_INTERVAL == 0:
            df.to_csv(PARTIAL_FILE, index=False)
            log.info(f"    [Partial save: {PARTIAL_FILE.name}]")

        time.sleep(SLEEP_BETWEEN)

    log.info(f"  Phase 2 COMPLETE: Validated {total_updated} entries in {total_batches} batches")
    return df


# ══════════════════════════════════════════════════════════════
#  PHASE 3: Author Contact Enrichment
# ══════════════════════════════════════════════════════════════
def phase3_contacts(df, model):
    """Enrich author contact information (email, website, social links)."""
    log.info("=" * 70)
    log.info("  PHASE 3: Author Contact Enrichment")
    log.info("=" * 70)

    # Ensure columns exist
    contact_cols = [
        "Author Email", "Author Website", "Social Links",
        "Twitter", "Instagram", "Facebook", "BookBub", "TikTok", "Literary Agent",
    ]
    for col in contact_cols:
        if col not in df.columns:
            df[col] = ""

    # Find rows that need contact enrichment (all contact fields missing)
    def needs_phase3(row):
        return (
            is_missing(row.get("Author Email"))
            and is_missing(row.get("Author Website"))
            and is_missing(row.get("Social Links"))
        )

    mask = df.apply(needs_phase3, axis=1)
    indices_needing = df[mask].index.tolist()
    log.info(f"  Phase 3: {len(indices_needing)} entries need contact enrichment")

    if not indices_needing:
        log.info("  Phase 3: Nothing to do, all contacts populated.")
        return df

    # De-duplicate authors: only query each unique author once
    author_to_indices = {}
    for idx in indices_needing:
        author = str(df.at[idx, "Author Name"]).strip()
        if author and author.lower() not in ("nan", "none", "unknown", ""):
            if author not in author_to_indices:
                author_to_indices[author] = []
            author_to_indices[author].append(idx)

    unique_authors = list(author_to_indices.keys())
    log.info(f"  Phase 3: {len(unique_authors)} unique authors to query")

    # For each author, pick a representative subgenre for context
    author_subgenre = {}
    for author in unique_authors:
        idx = author_to_indices[author][0]
        subgenre = str(df.at[idx, "Primary Subgenre"])
        if is_missing(subgenre):
            subgenre = "Romance"
        author_subgenre[author] = subgenre

    # Store results keyed by author
    author_results = {}
    total_batches = 0
    total_found = 0

    for batch_start in range(0, len(unique_authors), PHASE3_BATCH_SIZE):
        batch_end = min(batch_start + PHASE3_BATCH_SIZE, len(unique_authors))
        batch_authors = unique_authors[batch_start:batch_end]
        batch_num = batch_start // PHASE3_BATCH_SIZE + 1
        total_batches_needed = (len(unique_authors) + PHASE3_BATCH_SIZE - 1) // PHASE3_BATCH_SIZE

        entries_text_parts = []
        for i, author in enumerate(batch_authors):
            subgenre = author_subgenre[author]
            entries_text_parts.append(f"{i + 1}. {author} (writes: {subgenre})")

        entries_text = "\n".join(entries_text_parts)

        prompt = f"""You are a publishing industry researcher. For each author, provide their known public contact information for business/licensing inquiries.

Authors:
{entries_text}

For EACH author return:
- "author": author name
- "email": public/business email if known (null if not)
- "website": author website URL if known (null if not)
- "twitter": Twitter/X handle if known (null if not)
- "instagram": Instagram handle if known (null if not)
- "facebook": Facebook page URL if known (null if not)
- "bookbub": BookBub profile URL if known (null if not)
- "tiktok": TikTok handle if known (null if not)
- "agent": literary agent name and agency if known (null if not)

Return ONLY a JSON array."""

        results = call_gemini_with_backoff(model, prompt, batch_label=f"Phase3 batch {batch_num}")

        batch_found = 0
        for i, author in enumerate(batch_authors):
            # Try positional match
            result = results[i] if i < len(results) else None
            if result and str(result.get("author", "")).lower().strip() != author.lower().strip():
                # Try author-name match
                matched = None
                for r in results:
                    if str(r.get("author", "")).lower().strip() == author.lower().strip():
                        matched = r
                        break
                if matched:
                    result = matched

            if not result:
                for r in results:
                    if str(r.get("author", "")).lower().strip() == author.lower().strip():
                        result = r
                        break

            if result:
                author_results[author] = result
                # Check if any useful contact info was found
                has_info = any(
                    not is_missing(result.get(k))
                    for k in ["email", "website", "twitter", "instagram", "facebook",
                              "bookbub", "tiktok", "agent"]
                )
                if has_info:
                    batch_found += 1

        total_found += batch_found
        total_batches += 1
        log.info(
            f"    Phase 3 Batch {batch_num}/{total_batches_needed}: "
            f"found contacts for {batch_found}/{len(batch_authors)} authors "
            f"(total with info: {total_found})"
        )

        if total_batches % SAVE_INTERVAL == 0:
            # Apply results so far and save
            _apply_contact_results(df, author_to_indices, author_results)
            df.to_csv(PARTIAL_FILE, index=False)
            log.info(f"    [Partial save: {PARTIAL_FILE.name}]")

        time.sleep(SLEEP_BETWEEN)

    # Apply all results to the dataframe
    _apply_contact_results(df, author_to_indices, author_results)

    log.info(
        f"  Phase 3 COMPLETE: Found contact info for {total_found} authors "
        f"in {total_batches} batches"
    )
    return df


def _apply_contact_results(df, author_to_indices, author_results):
    """Apply author contact results to all rows for each author."""
    for author, result in author_results.items():
        if author not in author_to_indices:
            continue

        email = result.get("email")
        website = result.get("website")
        twitter = result.get("twitter")
        instagram = result.get("instagram")
        facebook = result.get("facebook")
        bookbub = result.get("bookbub")
        tiktok = result.get("tiktok")
        agent = result.get("agent")

        # Build social links string from individual fields
        social_parts = []
        if twitter and not is_missing(twitter):
            social_parts.append(f"Twitter: {twitter}")
        if instagram and not is_missing(instagram):
            social_parts.append(f"Instagram: {instagram}")
        if facebook and not is_missing(facebook):
            social_parts.append(f"Facebook: {facebook}")
        if bookbub and not is_missing(bookbub):
            social_parts.append(f"BookBub: {bookbub}")
        if tiktok and not is_missing(tiktok):
            social_parts.append(f"TikTok: {tiktok}")
        social_links = "; ".join(social_parts) if social_parts else ""

        for idx in author_to_indices[author]:
            # Only fill if currently empty
            if not is_missing(email) and is_missing(df.at[idx, "Author Email"]):
                df.at[idx, "Author Email"] = str(email)
            if not is_missing(website) and is_missing(df.at[idx, "Author Website"]):
                df.at[idx, "Author Website"] = str(website)
            if social_links and is_missing(df.at[idx, "Social Links"]):
                df.at[idx, "Social Links"] = social_links

            # Also fill individual social columns if they exist
            if "Twitter" in df.columns and not is_missing(twitter) and is_missing(df.at[idx, "Twitter"]):
                df.at[idx, "Twitter"] = str(twitter)
            if "Instagram" in df.columns and not is_missing(instagram) and is_missing(df.at[idx, "Instagram"]):
                df.at[idx, "Instagram"] = str(instagram)
            if "Facebook" in df.columns and not is_missing(facebook) and is_missing(df.at[idx, "Facebook"]):
                df.at[idx, "Facebook"] = str(facebook)
            if "BookBub" in df.columns and not is_missing(bookbub) and is_missing(df.at[idx, "BookBub"]):
                df.at[idx, "BookBub"] = str(bookbub)
            if "TikTok" in df.columns and not is_missing(tiktok) and is_missing(df.at[idx, "TikTok"]):
                df.at[idx, "TikTok"] = str(tiktok)
            if "Literary Agent" in df.columns and not is_missing(agent) and is_missing(df.at[idx, "Literary Agent"]):
                df.at[idx, "Literary Agent"] = str(agent)


# ══════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════
def run():
    start_time = datetime.now()
    phases_to_run = args.phase

    log.info("=" * 70)
    log.info("  GEMINI BATCH ENRICHMENT — Priority Dataset")
    log.info(f"  Phase(s): {phases_to_run}")
    log.info(f"  Input: {INPUT_FILE.name}")
    log.info(f"  Started: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.info("=" * 70)

    # Validate API key
    if not GEMINI_KEY:
        log.error("No GEMINI_API_KEY found! Check .env file at project root.")
        sys.exit(1)

    # Check input file
    if not INPUT_FILE.exists():
        log.error(f"Input file not found: {INPUT_FILE}")
        sys.exit(1)

    # Load data
    log.info(f"  Loading {INPUT_FILE.name}...")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    log.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")

    # Initialize Gemini
    log.info("  Initializing Gemini 2.5 Flash...")
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")
    log.info("  Gemini ready.")

    # Run phases
    if phases_to_run in ("1", "all"):
        df = phase1_metadata(df, model)
        # Save after phase 1
        df.to_csv(INPUT_FILE, index=False)
        log.info(f"  Saved after Phase 1 -> {INPUT_FILE.name}")

    if phases_to_run in ("2", "all"):
        df = phase2_validation(df, model)
        # Save after phase 2
        df.to_csv(INPUT_FILE, index=False)
        log.info(f"  Saved after Phase 2 -> {INPUT_FILE.name}")

    if phases_to_run in ("3", "all"):
        df = phase3_contacts(df, model)
        # Save after phase 3
        df.to_csv(INPUT_FILE, index=False)
        log.info(f"  Saved after Phase 3 -> {INPUT_FILE.name}")

    # Clean up partial file if everything succeeded
    if PARTIAL_FILE.exists():
        try:
            PARTIAL_FILE.unlink()
            log.info(f"  Cleaned up partial file: {PARTIAL_FILE.name}")
        except Exception:
            pass

    elapsed = datetime.now() - start_time
    log.info("=" * 70)
    log.info(f"  ALL DONE — Completed in {elapsed}")
    log.info(f"  Output: {INPUT_FILE}")
    log.info("=" * 70)


if __name__ == "__main__":
    run()
