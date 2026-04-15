#!/usr/bin/env python3
"""
Final Aggregation, Scoring & Output Pipeline
=============================================
Reads the enriched self-pub data, cleans/standardises columns to the target
38-column Ice Hockey reference format, computes a fresh Commissioning Score
(0-100), generates per-subgenre output files and a combined Excel workbook.

Usage:
    python final_aggregate_and_score.py
"""

import logging
import re
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

# ── Paths ─────────────────────────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"
SUBGENRE_OUT_DIR = OUTPUT_DIR / "subgenre_outputs"
OUTPUT_DIR.mkdir(exist_ok=True)
SUBGENRE_OUT_DIR.mkdir(exist_ok=True)

# ── Logging ───────────────────────────────────────────────────────────────────
LOG_PATH = OUTPUT_DIR / "final_aggregate_and_score.log"
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_PATH, mode="w"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("final_aggregate")

# ── Input candidates (most recent first) ──────────────────────────────────────
INPUT_CANDIDATES = [
    OUTPUT_DIR / "PRIORITY_SELFPUB_ENRICHED.csv",
    BASE_DIR / "All_9_Subgenres_Scout_Top25_V2_FINAL.csv",
    BASE_DIR / "All_9_Subgenres_Scout_Top25_ENRICHED_FINAL.csv",
]

# ── 9 canonical subgenres ─────────────────────────────────────────────────────
CANONICAL_SUBGENRES = [
    "Christian Drama/Romance",
    "Dark & Forbidden Romance",
    "Historical Romance & Fiction",
    "Ice Hockey & Sports Romance",
    "Mafia Drama/Romance",
    "Military Drama/Romance",
    "Political Drama/Romance",
    "Romantic Suspense/Psychological Thriller",
    "Small Town Drama/Romance",
]

# ── Target column order (Ice Hockey 38-col reference + extensions) ────────────
TARGET_COLUMNS_BASE = [
    "Book Series Name",
    "Author Name",
    "Primary Subgenre",
    "Type",
    "Books in Series",
    "Total Pages",
    "Length of Adaption in Hours",
    "First Book Name",
    "First Book Rating",
    "First Book Rating Count",
    "Last Book Name",
    "Last Book Rating",
    "Last Book Rating Count",
    "Highest Rated Book Name",
    "Highest Rated Book Rating",
    "Highest Rated Book Rating Count",
    "Lowest Rated Book Name",
    "Lowest Rated Book Rating",
    "Lowest Rated Book Rating Count",
    "Publisher Name",
    "Self Pub Flag",
    "Commissioning_Score",
    "Commissioning_Rank",
    "Subjective Analysis",
    "Differentiator",
    "Series_Era",
    "Rationale",
    "Goodreads Series URL",
    "Email",
    "Website",
    "Twitter",
    "Instagram",
    "Facebook",
    "BookBub",
    "TikTok",
    "Literary Agent",
    "Contact Source",
    "Discovery Status",
]

EXTENSION_COLUMNS = [
    "Primary Trope",
    "Objective_Validation_Source",
    "Amazon_Bestseller_Tag",
    "Amazon_Best_Rank",
    "Books_Featured_Rank_Validation",
    "First_Book_Pub_Year",
    "Adaptation_Length_Flag",
    "First_Book_Rating_Flag",
    "Appeal Flag",
    "Lowest_Book_Rating_Flag",
    "Rating_Stability_Flag",
]

ALL_OUTPUT_COLUMNS = TARGET_COLUMNS_BASE + EXTENSION_COLUMNS


# ═══════════════════════════════════════════════════════════════════════════════
# 1. LOAD & CLEAN
# ═══════════════════════════════════════════════════════════════════════════════

def load_data() -> pd.DataFrame:
    """Load the best available input file."""
    for path in INPUT_CANDIDATES:
        if path.exists():
            log.info(f"Loading data from: {path.name}")
            df = pd.read_csv(path, on_bad_lines="skip")
            log.info(f"  Loaded {len(df)} rows, {len(df.columns)} columns")
            return df
    raise FileNotFoundError(
        "No input CSV found. Expected one of: "
        + ", ".join(p.name for p in INPUT_CANDIDATES)
    )


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove junk columns, fix known naming mismatches, and ensure every
    target column exists (filled with empty string where missing).
    """
    # ── Drop Unnamed columns ──────────────────────────────────────────────
    unnamed_cols = [c for c in df.columns if c.startswith("Unnamed")]
    if unnamed_cols:
        log.info(f"  Dropping {len(unnamed_cols)} Unnamed columns: {unnamed_cols}")
        df = df.drop(columns=unnamed_cols)

    # ── Fix column name quirks from upstream CSVs ─────────────────────────
    rename_map = {
        # Newline-contaminated names from PRIORITY_SELFPUB_ENRICHED
        "Last Book \nRating": "Last Book Rating",
        "Last Book\nRating": "Last Book Rating",
        # Outreach / contact columns that should map to the target format
        "Author Email": "Email",
        "Author Website": "Website",
        "Social Links": "_Social_Links_Raw",
        # Amazon enrichment mapping
        "T100_Mapping": "Amazon_Bestseller_Tag",
        "Num_Books_Featured": "Amazon_Best_Rank",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # ── Handle duplicate 'Last Book Rating' after rename ──────────────────
    dup_cols = df.columns[df.columns.duplicated()].unique().tolist()
    if dup_cols:
        log.info(f"  Resolving duplicate columns: {dup_cols}")
        # Keep first occurrence of each
        df = df.loc[:, ~df.columns.duplicated(keep="first")]

    # ── Parse Social Links into individual contact columns ────────────────
    if "_Social_Links_Raw" in df.columns:
        _parse_social_links(df)
        df = df.drop(columns=["_Social_Links_Raw"], errors="ignore")

    # ── Drop columns we no longer need ────────────────────────────────────
    drop_cols = [
        "Books_In_Series_List", "Universe Type", "Universe Reasoning",
        "Verfied Flag", "Outreach Sent Date", "Duplicate Rightsholders",
        "Outreach Channel", "Contact Info", "Response Status",
        "Response Date", "Licensing Status", "Draft Email",
        "Source Platform", "Series_Verified", "Verification_Method",
        "Verified_Series_Name", "Verified_Books_Count",
        "Amazon Series URL",
    ]
    existing_drop = [c for c in drop_cols if c in df.columns]
    if existing_drop:
        log.info(f"  Dropping {len(existing_drop)} unnecessary columns")
        df = df.drop(columns=existing_drop)

    # ── Ensure every target column exists ─────────────────────────────────
    for col in ALL_OUTPUT_COLUMNS:
        if col not in df.columns:
            df[col] = ""

    # ── Coerce numeric columns ────────────────────────────────────────────
    numeric_cols = [
        "Books in Series", "Total Pages", "Length of Adaption in Hours",
        "First Book Rating", "First Book Rating Count",
        "Last Book Rating", "Last Book Rating Count",
        "Highest Rated Book Rating", "Highest Rated Book Rating Count",
        "Lowest Rated Book Rating", "Lowest Rated Book Rating Count",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    log.info(f"  After column cleanup: {len(df.columns)} columns")
    return df


def _parse_social_links(df: pd.DataFrame) -> None:
    """
    Split a raw 'Social Links' blob into Twitter, Instagram, Facebook,
    BookBub, TikTok columns (only fills blanks).
    """
    social_col = "_Social_Links_Raw"
    patterns = {
        "Twitter":   [r"twitter\.com", r"x\.com"],
        "Instagram": [r"instagram\.com"],
        "Facebook":  [r"facebook\.com"],
        "BookBub":   [r"bookbub\.com"],
        "TikTok":    [r"tiktok\.com"],
    }

    for _, row in df.iterrows():
        raw = str(row.get(social_col, ""))
        if not raw or raw == "nan":
            continue
        # Split on comma, semicolon, space, or pipe
        links = re.split(r"[,;\|\s]+", raw)
        for link in links:
            link = link.strip()
            if not link:
                continue
            for col_name, regexes in patterns.items():
                for rgx in regexes:
                    if re.search(rgx, link, re.IGNORECASE):
                        # Only fill if the cell is currently empty
                        if col_name not in df.columns:
                            df[col_name] = ""
                        idx = row.name
                        existing = str(df.at[idx, col_name])
                        if not existing or existing == "nan":
                            df.at[idx, col_name] = link
                        break


# ═══════════════════════════════════════════════════════════════════════════════
# 2. COMMISSIONING SCORE (0-100)
# ═══════════════════════════════════════════════════════════════════════════════

def _safe_float(val, default=0.0):
    """Safely cast to float, returning default on failure."""
    try:
        v = float(val)
        return v if not np.isnan(v) else default
    except (ValueError, TypeError):
        return default


def compute_commissioning_score(row: pd.Series) -> dict:
    """
    Compute the 0-100 Commissioning Score and its sub-component values.
    Returns a dict with the score and all generated flags.
    """
    # ── Raw values ────────────────────────────────────────────────────────
    adapt_hrs     = _safe_float(row.get("Length of Adaption in Hours"))
    first_rating  = _safe_float(row.get("First Book Rating"))
    first_count   = _safe_float(row.get("First Book Rating Count"))
    lowest_rating = _safe_float(row.get("Lowest Rated Book Rating"))
    highest_rating = _safe_float(row.get("Highest Rated Book Rating"))
    series_era    = str(row.get("Series_Era", "")).strip()
    books_count   = _safe_float(row.get("Books in Series"))
    validation    = str(row.get("Objective_Validation_Source", "")).lower()
    amazon_tag    = str(row.get("Amazon_Bestseller_Tag", "")).lower()
    featured_rank = str(row.get("Books_Featured_Rank_Validation", "")).lower()

    # ── 2a. Adaptation Length Score (max 15) ──────────────────────────────
    if adapt_hrs >= 100:
        adapt_score = 15
    elif adapt_hrs >= 75:
        adapt_score = 13
    elif adapt_hrs >= 50:
        adapt_score = 11
    elif adapt_hrs >= 30:
        adapt_score = 8
    elif adapt_hrs >= 15:
        adapt_score = 5
    else:
        adapt_score = 2

    # ── 2b. First Book Rating Score (max 20) ──────────────────────────────
    if first_rating >= 4.5:
        rating_score = 20
    elif first_rating >= 4.2:
        rating_score = 17
    elif first_rating >= 4.0:
        rating_score = 14
    elif first_rating >= 3.8:
        rating_score = 10
    elif first_rating >= 3.5:
        rating_score = 6
    else:
        rating_score = 3

    # ── 2c. Rating Volume Score (max 15) ──────────────────────────────────
    if first_count >= 100000:
        volume_score = 15
    elif first_count >= 50000:
        volume_score = 13
    elif first_count >= 10000:
        volume_score = 11
    elif first_count >= 5000:
        volume_score = 9
    elif first_count >= 1000:
        volume_score = 6
    elif first_count >= 100:
        volume_score = 3
    else:
        volume_score = 1

    # ── 2d. Lowest Book Rating Score (max 10) ─────────────────────────────
    if lowest_rating <= 0:
        # Missing — estimate from first book rating
        lowest_rating = first_rating * 0.8
    if lowest_rating >= 4.2:
        lowest_score = 10
    elif lowest_rating >= 4.0:
        lowest_score = 8
    elif lowest_rating >= 3.8:
        lowest_score = 6
    elif lowest_rating >= 3.5:
        lowest_score = 4
    else:
        lowest_score = 2

    # ── 2e. Rating Stability Score (max 10) ───────────────────────────────
    if highest_rating > 0 and lowest_rating > 0:
        diff = abs(highest_rating - lowest_rating)
        if diff < 0.2:
            stability_score = 10
        elif diff < 0.4:
            stability_score = 8
        elif diff < 0.6:
            stability_score = 6
        elif diff < 0.8:
            stability_score = 4
        else:
            stability_score = 2
    else:
        # Only one rating available
        stability_score = 5
        diff = 0.0

    # ── 2f. Series Era Score (max 10) ─────────────────────────────────────
    era_lower = series_era.lower()
    if "contemporary" in era_lower or "after 2020" in era_lower or "2020" in era_lower:
        era_score = 10
    elif "mixed" in era_lower or "2010" in era_lower:
        era_score = 7
    elif "historical" in era_lower or "before" in era_lower or "classic" in era_lower:
        era_score = 5
    else:
        # Default for empty or unrecognised
        era_score = 6

    # ── 2g. Books in Series Score (max 10) ────────────────────────────────
    if books_count >= 10:
        books_score = 10
    elif books_count >= 7:
        books_score = 9
    elif books_count >= 5:
        books_score = 7
    elif books_count >= 3:
        books_score = 5
    else:
        books_score = 3

    # ── 2h. Objective Validation Bonus (max 10) ──────────────────────────
    bonus = 0
    # Combine all available validation text for matching
    all_validation_text = f"{validation} {amazon_tag} {featured_rank}"

    # +3 each for top-tier
    if "nyt bestseller" in all_validation_text or "new york times" in all_validation_text:
        bonus += 3
    if "usa today bestseller" in all_validation_text or "usa today" in all_validation_text:
        bonus += 3
    if re.search(r"amazon\s*top\s*10\b", all_validation_text):
        bonus += 3

    # +2 each for mid-tier
    if re.search(r"amazon\s*top\s*50\b", all_validation_text):
        bonus += 2
    if "goodreads choice" in all_validation_text:
        bonus += 2
    if "booktok" in all_validation_text or "tiktok viral" in all_validation_text:
        bonus += 2

    # +1 for Amazon Top 100
    if re.search(r"amazon\s*top\s*100\b", all_validation_text):
        bonus += 1

    bonus = min(bonus, 10)

    # ── TOTAL ─────────────────────────────────────────────────────────────
    total = (
        adapt_score
        + rating_score
        + volume_score
        + lowest_score
        + stability_score
        + era_score
        + books_score
        + bonus
    )
    # Clamp to 0-100
    total = max(0, min(100, total))

    return {
        "Commissioning_Score": total,
        "_adapt_score": adapt_score,
        "_rating_score": rating_score,
        "_volume_score": volume_score,
        "_lowest_score": lowest_score,
        "_stability_score": stability_score,
        "_era_score": era_score,
        "_books_score": books_score,
        "_bonus_score": bonus,
        "_rating_diff": diff if (highest_rating > 0 and lowest_rating > 0) else None,
        "_effective_lowest": lowest_rating,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 3. FLAGS
# ═══════════════════════════════════════════════════════════════════════════════

def generate_flags(row: pd.Series, rating_diff, eff_lowest: float) -> dict:
    """Generate the five categorical flag columns."""
    adapt_hrs     = _safe_float(row.get("Length of Adaption in Hours"))
    first_rating  = _safe_float(row.get("First Book Rating"))
    first_count   = _safe_float(row.get("First Book Rating Count"))

    # Adaptation Length Flag
    if adapt_hrs >= 50:
        adapt_flag = "Long"
    elif adapt_hrs >= 20:
        adapt_flag = "Medium"
    else:
        adapt_flag = "Short"

    # First Book Rating Flag
    if first_rating >= 4.3:
        rating_flag = "Excellent"
    elif first_rating >= 3.8:
        rating_flag = "Good"
    else:
        rating_flag = "Average"

    # Appeal Flag
    if first_rating >= 4.2 and first_count >= 5000:
        appeal_flag = "High"
    elif first_rating >= 3.8 or first_count >= 1000:
        appeal_flag = "Medium"
    else:
        appeal_flag = "Low"

    # Lowest Book Rating Flag
    if eff_lowest >= 4.0:
        lowest_flag = "Strong"
    elif eff_lowest >= 3.5:
        lowest_flag = "Acceptable"
    else:
        lowest_flag = "Weak"

    # Rating Stability Flag
    if rating_diff is None:
        stability_flag = "Stable"  # only one rating
    elif rating_diff < 0.3:
        stability_flag = "Very Stable"
    elif rating_diff < 0.5:
        stability_flag = "Stable"
    else:
        stability_flag = "Variable"

    return {
        "Adaptation_Length_Flag": adapt_flag,
        "First_Book_Rating_Flag": rating_flag,
        "Appeal Flag": appeal_flag,
        "Lowest_Book_Rating_Flag": lowest_flag,
        "Rating_Stability_Flag": stability_flag,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# 4. COMMISSIONING RANK
# ═══════════════════════════════════════════════════════════════════════════════

def assign_rank(score: float) -> str:
    """P0-P3 based on Commissioning Score thresholds."""
    if score >= 75:
        return "P0"
    elif score >= 55:
        return "P1"
    elif score >= 35:
        return "P2"
    else:
        return "P3"


# ═══════════════════════════════════════════════════════════════════════════════
# 5. RATIONALE AUTO-GENERATION
# ═══════════════════════════════════════════════════════════════════════════════

def build_rationale(row: pd.Series) -> str:
    """
    Build a 1-line human-readable rationale string.
    Format: "{Rank}: {Rating}* ({Count} ratings), {N} books, {Era}. {Validation}"
    """
    rank   = str(row.get("Commissioning_Rank", ""))
    rating = _safe_float(row.get("First Book Rating"))
    count  = _safe_float(row.get("First Book Rating Count"))
    books  = _safe_float(row.get("Books in Series"))
    era    = str(row.get("Series_Era", "")).strip()
    validation = str(row.get("Objective_Validation_Source", "")).strip()

    # Format count nicely
    if count >= 1000:
        count_str = f"{count/1000:.1f}k"
    else:
        count_str = str(int(count))

    rating_str = f"{rating:.1f}" if rating > 0 else "N/A"
    books_str = str(int(books)) if books > 0 else "?"
    era_str = era if era and era != "nan" else "Unknown era"

    parts = [f"{rank}: {rating_str}* ({count_str} ratings), {books_str} books, {era_str}"]

    if validation and validation != "nan" and validation.strip():
        parts.append(validation)

    return ". ".join(parts)


# ═══════════════════════════════════════════════════════════════════════════════
# 6. SUBGENRE SLUG HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def subgenre_to_slug(name: str) -> str:
    """Convert a subgenre name to a filesystem-safe slug."""
    slug = name.lower().strip()
    slug = re.sub(r"[&/]", "_and_", slug)
    slug = re.sub(r"[^a-z0-9]+", "_", slug)
    slug = slug.strip("_")
    # Collapse multiple underscores
    slug = re.sub(r"_+", "_", slug)
    return slug


# ═══════════════════════════════════════════════════════════════════════════════
# AMAZON TOP 100 MAPPING
# ═══════════════════════════════════════════════════════════════════════════════

def apply_amazon_tags(df: pd.DataFrame, amazon_raw_path: Path) -> pd.DataFrame:
    """Map Amazon Top 100 raw scraped data to our dataset."""
    amazon_df = pd.read_csv(amazon_raw_path)
    log.info(f"  Loaded {len(amazon_df)} Amazon bestseller entries from {amazon_raw_path.name}")

    if "Amazon_Bestseller_Tag" not in df.columns:
        df["Amazon_Bestseller_Tag"] = ""
    if "Amazon_Best_Rank" not in df.columns:
        df["Amazon_Best_Rank"] = ""

    # Build lookup indexes
    title_to_idx = {}
    author_to_idx = {}
    for idx, row in df.iterrows():
        for col in ["Book Series Name", "First Book Name"]:
            val = str(row.get(col, "")).lower().strip()
            if val and val != "nan":
                norm = re.sub(r'[^\w\s]', '', val).strip()
                if len(norm) > 3:
                    title_to_idx.setdefault(norm, []).append(idx)
        author = str(row.get("Author Name", "")).lower().strip()
        if author and author != "nan":
            author_to_idx.setdefault(author, []).append(idx)

    matched = 0
    for _, amz in amazon_df.iterrows():
        amz_title = re.sub(r'[^\w\s]', '', str(amz["title"]).lower().strip())
        amz_author = str(amz.get("author", "")).lower().strip()
        rank = int(amz["rank"]) if pd.notna(amz.get("rank")) else 999
        category = str(amz.get("category", ""))

        if rank <= 10:
            tag = f"Amazon Top 10 {category}"
        elif rank <= 50:
            tag = f"Amazon Top 50 {category}"
        else:
            tag = f"Amazon Top 100 {category}"

        matched_indices = set()

        if amz_title in title_to_idx:
            matched_indices.update(title_to_idx[amz_title])

        if not matched_indices and len(amz_title) > 8:
            for our_title, indices in title_to_idx.items():
                if len(our_title) > 5:
                    if our_title in amz_title or amz_title in our_title:
                        matched_indices.update(indices)
                        break

        if not matched_indices and amz_author and len(amz_author) > 5:
            if amz_author in author_to_idx:
                matched_indices.update(author_to_idx[amz_author])

        for idx in matched_indices:
            current_tag = str(df.at[idx, "Amazon_Bestseller_Tag"]).strip()
            if current_tag and current_tag not in ["", "nan"]:
                if tag not in current_tag:
                    df.at[idx, "Amazon_Bestseller_Tag"] = current_tag + "; " + tag
            else:
                df.at[idx, "Amazon_Bestseller_Tag"] = tag

            current_rank = df.at[idx, "Amazon_Best_Rank"]
            try:
                cr = int(float(current_rank)) if pd.notna(current_rank) and str(current_rank).strip() not in ["", "nan"] else 999
            except (ValueError, TypeError):
                cr = 999
            if rank < cr:
                df.at[idx, "Amazon_Best_Rank"] = rank
            matched += 1

    has_tag = df["Amazon_Bestseller_Tag"].notna() & (~df["Amazon_Bestseller_Tag"].astype(str).str.strip().isin(["", "nan"]))
    log.info(f"  Matched {matched} Amazon entries -> {has_tag.sum()} series tagged")
    return df


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE
# ═══════════════════════════════════════════════════════════════════════════════

def run():
    log.info("=" * 70)
    log.info("FINAL AGGREGATION, SCORING & OUTPUT PIPELINE")
    log.info("=" * 70)
    start_time = datetime.now()

    # ── Load ──────────────────────────────────────────────────────────────
    df = load_data()

    # ── Clean columns ─────────────────────────────────────────────────────
    log.info("\nSTEP 1: Clean and standardise columns")
    df = clean_columns(df)

    # ── Deduplicate ───────────────────────────────────────────────────────
    log.info("\nSTEP 2: Deduplicate")
    before = len(df)
    dedup_key = (
        df["Book Series Name"].astype(str).str.lower().str.strip()
        + "|"
        + df["Author Name"].astype(str).str.lower().str.strip()
    )
    df = df.loc[~dedup_key.duplicated(keep="first")].copy()
    log.info(f"  Removed {before - len(df)} duplicates, {len(df)} rows remaining")

    # ── Apply Amazon Top 100 data if raw CSV exists ──────────────────────
    amazon_raw_path = OUTPUT_DIR / "amazon_top100_raw.csv"
    if amazon_raw_path.exists():
        log.info("\nSTEP 2.5: Apply Amazon Top 100 bestseller data")
        df = apply_amazon_tags(df, amazon_raw_path)
    else:
        log.info("\nSTEP 2.5: No Amazon raw data found, skipping")

    # ── Compute Commissioning Score ───────────────────────────────────────
    log.info("\nSTEP 3: Compute Commissioning Score (0-100)")
    score_results = df.apply(compute_commissioning_score, axis=1, result_type="expand")

    df["Commissioning_Score"] = score_results["Commissioning_Score"]
    df["Commissioning_Rank"] = df["Commissioning_Score"].apply(assign_rank)

    # ── Generate flags ────────────────────────────────────────────────────
    log.info("\nSTEP 4: Generate flags")
    flag_rows = []
    for idx, row in df.iterrows():
        rd = score_results.loc[idx, "_rating_diff"]
        el = score_results.loc[idx, "_effective_lowest"]
        flag_rows.append(generate_flags(row, rd, el))

    flag_df = pd.DataFrame(flag_rows, index=df.index)
    for col in flag_df.columns:
        df[col] = flag_df[col]

    # ── Build Rationale ───────────────────────────────────────────────────
    log.info("\nSTEP 5: Build Rationale column")
    df["Rationale"] = df.apply(build_rationale, axis=1)

    # ── Log score distribution ────────────────────────────────────────────
    log.info("\nScore distribution:")
    for rank in ["P0", "P1", "P2", "P3"]:
        count = (df["Commissioning_Rank"] == rank).sum()
        log.info(f"  {rank}: {count} series")

    log.info(f"\n  Score stats: mean={df['Commissioning_Score'].mean():.1f}, "
             f"median={df['Commissioning_Score'].median():.1f}, "
             f"min={df['Commissioning_Score'].min()}, "
             f"max={df['Commissioning_Score'].max()}")

    # ── Subgenre breakdown ────────────────────────────────────────────────
    log.info("\nSubgenre breakdown:")
    if "Primary Subgenre" in df.columns:
        for sg, cnt in df["Primary Subgenre"].value_counts().items():
            log.info(f"  {sg}: {cnt}")

    # ── Reorder to target column set ──────────────────────────────────────
    log.info("\nSTEP 6: Reorder to target column format")
    # Keep only the columns we want, in order
    final_cols = [c for c in ALL_OUTPUT_COLUMNS if c in df.columns]
    # Also retain any extra columns present but not in target (append at end)
    extra_cols = [c for c in df.columns if c not in ALL_OUTPUT_COLUMNS and not c.startswith("_")]
    all_cols = final_cols + extra_cols
    df = df[all_cols]

    # Sort by score descending globally
    df = df.sort_values("Commissioning_Score", ascending=False).reset_index(drop=True)

    # ══════════════════════════════════════════════════════════════════════
    # OUTPUT
    # ══════════════════════════════════════════════════════════════════════

    # ── Per-subgenre CSVs ─────────────────────────────────────────────────
    log.info("\nSTEP 7: Generate per-subgenre output files")
    subgenre_col = "Primary Subgenre"
    subgenres_present = df[subgenre_col].dropna().unique().tolist()

    for sg in sorted(subgenres_present):
        slug = subgenre_to_slug(sg)
        sg_df = (
            df[df[subgenre_col] == sg]
            .sort_values("Commissioning_Score", ascending=False)
            .reset_index(drop=True)
        )
        out_path = SUBGENRE_OUT_DIR / f"{slug}_master.csv"
        sg_df.to_csv(out_path, index=False)
        log.info(f"  {sg}: {len(sg_df)} series -> {out_path.name}")

    # ── Combined CSV ──────────────────────────────────────────────────────
    log.info("\nSTEP 8: Save combined outputs")
    csv_path = OUTPUT_DIR / "FINAL_SELFPUB_SCORED.csv"
    df.to_csv(csv_path, index=False)
    log.info(f"  CSV: {csv_path}  ({len(df)} rows)")

    # ── Combined Excel with per-subgenre sheets ───────────────────────────
    xlsx_path = OUTPUT_DIR / "FINAL_SELFPUB_SCORED.xlsx"
    log.info(f"  Writing Excel workbook: {xlsx_path.name}")

    try:
        with pd.ExcelWriter(xlsx_path, engine="openpyxl") as writer:
            # Combined sheet first
            df.to_excel(writer, sheet_name="Combined", index=False)
            _auto_size_columns(writer, "Combined", df)

            # Per-subgenre sheets
            for sg in sorted(subgenres_present):
                sg_df = (
                    df[df[subgenre_col] == sg]
                    .sort_values("Commissioning_Score", ascending=False)
                    .reset_index(drop=True)
                )
                # Excel sheet name max 31 chars, no slashes
                sheet_name = sg.replace("/", "-")[:31]
                sg_df.to_excel(writer, sheet_name=sheet_name, index=False)
                _auto_size_columns(writer, sheet_name, sg_df)

        # Amazon Top 100 Lists tab
        amazon_raw_path = OUTPUT_DIR / "amazon_full_crawl_raw.csv"
        if amazon_raw_path.exists():
            amazon_df = pd.read_csv(amazon_raw_path)
            amazon_df.to_excel(writer, sheet_name="Amazon Top 100 Lists", index=False)
            _auto_size_columns(writer, "Amazon Top 100 Lists", amazon_df)
            log.info(f"  Added Amazon Top 100 Lists tab ({len(amazon_df)} entries)")

        log.info(f"  Excel written with {len(subgenres_present) + 1} sheets + Amazon tab")

    except ImportError:
        log.warning("  openpyxl not installed — skipping Excel output. "
                     "Install with: pip install openpyxl")
    except Exception as e:
        log.error(f"  Excel write failed: {e}")

    # ── Summary ───────────────────────────────────────────────────────────
    elapsed = datetime.now() - start_time
    log.info("\n" + "=" * 70)
    log.info("PIPELINE COMPLETE")
    log.info(f"  Total series: {len(df)}")
    log.info(f"  Subgenres: {len(subgenres_present)}")
    log.info(f"  Output columns: {len(df.columns)}")
    log.info(f"  Files written:")
    log.info(f"    - {csv_path}")
    log.info(f"    - {xlsx_path}")
    log.info(f"    - {len(subgenres_present)} subgenre CSVs in {SUBGENRE_OUT_DIR}")
    log.info(f"  Elapsed: {elapsed}")
    log.info("=" * 70)

    return df


def _auto_size_columns(writer, sheet_name: str, df: pd.DataFrame) -> None:
    """Best-effort auto-sizing of Excel columns based on header + sample data."""
    try:
        worksheet = writer.sheets[sheet_name]
        for i, col in enumerate(df.columns):
            # Use header length or max data length (sampled), whichever is bigger
            max_len = len(str(col))
            if len(df) > 0:
                sample = df[col].astype(str).head(50)
                data_max = sample.str.len().max()
                if pd.notna(data_max):
                    max_len = max(max_len, int(data_max))
            # Cap column width at 60 chars to keep sheets manageable
            width = min(max_len + 2, 60)
            col_letter = worksheet.cell(row=1, column=i + 1).column_letter
            worksheet.column_dimensions[col_letter].width = width
    except Exception:
        pass  # Non-critical — skip silently


# ═══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    run()
