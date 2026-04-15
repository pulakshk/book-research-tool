#!/usr/bin/env python3
"""
Merge & Filter Pipeline
========================
1. Merge all verification group files into one master
2. Save full master (all data, for reference)
3. Apply triple filter: series >= 3 books + self-pub/indie + no fantasy
4. Save filtered dataset ready for Goodreads enrichment

Can be run incrementally — merges whatever group files exist.
"""

import re
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "output"

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "merge_filter.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("merge_filter")

# ── Fantasy/Romantasy detection (from cleanup_and_validate.py) ──
HARD_FANTASY_TERMS = [
    "werewolf", "shifter", "vampire", "fae", "fairy", "dragon",
    "witch", "wizard", "sorcerer", "mage", "elf", "elven",
    "demon", "paranormal", "supernatural", "magic system",
    "shapeshifter", "lycan", "alpha mate", "omega verse",
    "chosen one", "prophecy", "necromancer", "warlock",
    "romantasy", "high fantasy", "epic fantasy", "urban fantasy",
    "paranormal romance", "fantasy romance", "shifter romance",
    "orc", "goblin", "centaur", "mermaid", "siren",
    "undead", "zombie", "werewolves", "vampires",
    "coven", "dark magic", "blood magic", "enchantment",
    "faerie", "fey", "elemental", "portal fantasy",
    "dystopian", "post-apocalyptic", "sci-fi romance",
    "omegaverse", "omega verse", "alpha omega",
]

# These terms need extra context — only flag if combined with other fantasy markers
# (removed: angel, kingdom, throne, realm, enchant, mythical, immortal, spell, magic,
#  alien — too many false positives in mafia/dark/historical romance)
CONTEXT_FANTASY_TERMS = [
    "angel", "kingdom", "throne", "realm", "enchant", "mythical",
    "immortal", "spell", "alien",
]

LIGHT_FANTASY_OK = [
    "magical realism", "time travel romance", "ghost romance",
    "second chance", "light fantasy", "lite fantasy",
    "christmas magic", "holiday magic", "lucky charm",
    "fairy tale retelling", "modern fairy tale",
    "touch of magic", "bit of magic",
]

NON_ROMANCE_TERMS = [
    "textbook", "cookbook", "self-help", "business",
    "programming", "technical", "reference guide",
    "children's", "picture book", "coloring book",
    "non-fiction", "nonfiction", "biography", "memoir",
    "travel guide", "how to", "for dummies",
    "academic", "scholarly", "journal",
]


def has_fantasy_elements(row):
    """Check if a row has fantasy/romantasy/irrelevant markers."""
    text_fields = [
        str(row.get("Book Series Name", "")),
        str(row.get("First Book Name", "")),
        str(row.get("Subjective Analysis", "")),
        str(row.get("Differentiator", "")),
        str(row.get("Primary Trope", "")),
        str(row.get("Verified_Series_Name", "")),
    ]
    combined = " ".join(text_fields).lower()

    # Check non-romance first
    for term in NON_ROMANCE_TERMS:
        if term in combined:
            return True, f"non-romance: {term}"

    # Check for light fantasy exceptions (OK to keep)
    for exc in LIGHT_FANTASY_OK:
        if exc in combined:
            return False, ""

    # Check for hard fantasy terms (high confidence)
    for term in HARD_FANTASY_TERMS:
        if len(term) <= 4:
            if re.search(r'\b' + re.escape(term) + r'\b', combined):
                return True, f"fantasy: {term}"
        else:
            if term in combined:
                return True, f"fantasy: {term}"

    # Check context-dependent terms — only flag if 2+ context terms present
    # or if combined with a hard fantasy indicator
    context_hits = []
    for term in CONTEXT_FANTASY_TERMS:
        if len(term) <= 4:
            if re.search(r'\b' + re.escape(term) + r'\b', combined):
                context_hits.append(term)
        else:
            if term in combined:
                context_hits.append(term)

    if len(context_hits) >= 2:
        return True, f"fantasy-context: {', '.join(context_hits)}"

    return False, ""


# ── Subgenre-to-group mapping ─────────────────────────────
GROUP_SUBGENRES = {
    1: ["Dark & Forbidden Romance", "Romantic Suspense/Psychological Thriller", "Military Drama/Romance"],
    2: ["Political Drama/Romance", "Mafia Drama/Romance", "Ice Hockey & Sports Romance"],
    3: ["Historical Romance & Fiction", "Small Town Drama/Romance", "Christian Drama/Romance"],
}

# Dark & Forbidden also has its own fix file
DARK_SUBGENRE = "Dark & Forbidden Romance"


def merge_groups():
    """Merge verification results from all group files."""
    log.info("=" * 70)
    log.info("  MERGE & FILTER PIPELINE")
    log.info("=" * 70)

    # Start with the base partial file (has layers 1-3 + earlier Gemini results)
    base_candidates = [
        DATA_DIR / "selfpub_master_gemini_verified_partial.csv",
        DATA_DIR / "selfpub_master_series_verified_partial.csv",
        DATA_DIR / "selfpub_master_mega_expanded.csv",
    ]

    base_file = None
    for c in base_candidates:
        if c.exists():
            base_file = c
            break

    if not base_file:
        log.error("No base file found!")
        return None

    df = pd.read_csv(base_file, low_memory=False)
    log.info(f"  Base file: {base_file.name} ({len(df)} rows)")

    # Columns to merge from group files
    merge_cols = ["Series_Verified", "Verification_Method", "Verified_Series_Name",
                  "Verified_Books_Count", "Self Pub Flag", "Publisher Name", "Books in Series"]

    # Ensure all columns exist in base
    for col in merge_cols:
        if col not in df.columns:
            df[col] = ""

    # Merge each group file
    group_files = {}
    for g in [1, 2, 3]:
        done_file = DATA_DIR / f"gemini_fast_g{g}_done.csv"
        partial_file = DATA_DIR / f"gemini_fast_g{g}_partial.csv"
        if done_file.exists():
            group_files[g] = done_file
        elif partial_file.exists():
            group_files[g] = partial_file

    # Dark fix file
    dark_done = DATA_DIR / "gemini_dark_fix_done.csv"
    dark_partial = DATA_DIR / "gemini_dark_fix_partial.csv"
    dark_file = dark_done if dark_done.exists() else (dark_partial if dark_partial.exists() else None)

    for g, gfile in group_files.items():
        gdf = pd.read_csv(gfile, low_memory=False)
        subgenres = GROUP_SUBGENRES[g]

        # For group 1, skip Dark & Forbidden (handled separately)
        if g == 1:
            subgenres = [s for s in subgenres if s != DARK_SUBGENRE]

        updated = 0
        for idx, row in gdf.iterrows():
            sg = str(row.get("Primary Subgenre", ""))
            if sg not in subgenres:
                continue

            # Only update if group file has a verification result
            gv = str(row.get("Series_Verified", "")).strip()
            if gv and gv not in ["", "nan"]:
                for col in merge_cols:
                    val = row.get(col)
                    if pd.notna(val) and str(val).strip() not in ["", "nan"]:
                        df.at[idx, col] = val
                updated += 1

        log.info(f"  Merged Group {g} ({gfile.name}): {updated} entries updated for {', '.join(subgenres)}")

    # Merge Dark fix
    if dark_file:
        ddf = pd.read_csv(dark_file, low_memory=False)
        updated = 0
        for idx, row in ddf.iterrows():
            sg = str(row.get("Primary Subgenre", ""))
            if sg != DARK_SUBGENRE:
                continue
            gv = str(row.get("Series_Verified", "")).strip()
            if gv and gv not in ["", "nan"]:
                for col in merge_cols:
                    val = row.get(col)
                    if pd.notna(val) and str(val).strip() not in ["", "nan"]:
                        df.at[idx, col] = val
                updated += 1
        log.info(f"  Merged Dark fix ({dark_file.name}): {updated} entries updated")

    return df


def run_merge_and_filter():
    df = merge_groups()
    if df is None:
        return

    # ── Save full master (all data, for reference) ────────
    log.info(f"\n  SAVING FULL MASTER (all data for reference)")

    master_output = DATA_DIR / "MASTER_ALL_BOOKS_VERIFIED.csv"
    df.to_csv(master_output, index=False)
    log.info(f"  Full master saved: {master_output.name} ({len(df)} rows)")

    # Stats on full master
    log.info(f"\n  Full master stats:")
    log.info(f"    Total entries: {len(df)}")

    sv = df["Series_Verified"].value_counts()
    for k, c in sv.items():
        if k and str(k) not in ["", "nan"]:
            log.info(f"    {k}: {c}")

    log.info(f"\n  By subgenre:")
    for sg, c in df["Primary Subgenre"].value_counts().items():
        log.info(f"    {sg}: {c}")

    pub_counts = df["Self Pub Flag"].value_counts()
    log.info(f"\n  By publisher type:")
    for pt, c in pub_counts.items():
        if pt and str(pt) not in ["", "nan"]:
            log.info(f"    {pt}: {c}")

    # ── FILTER 1: Series >= 3 books ───────────────────────
    log.info(f"\n  {'='*60}")
    log.info(f"  FILTER 1: Series >= 3 books")
    log.info(f"  {'='*60}")

    def has_3plus_books(row):
        """Strictly check if entry has 3+ books."""
        best_count = 0
        for col in ["Books in Series", "Verified_Books_Count"]:
            val = row.get(col)
            try:
                c = int(float(val)) if pd.notna(val) else 0
                if c > best_count:
                    best_count = c
            except:
                pass

        # If we have a count, it must be >= 3
        if best_count >= 3:
            return True

        # If count is known and < 3, reject regardless of verification
        if best_count > 0 and best_count < 3:
            return False

        # Unknown count (0) — only keep if verified as series (benefit of doubt)
        verified = str(row.get("Series_Verified", "")).strip()
        method = str(row.get("Verification_Method", "")).strip()

        if verified == "Yes":
            return True
        if verified == "Likely":
            return True
        if "title_pattern" in method or "box" in method.lower():
            return True

        return False

    before = len(df)
    series_mask = df.apply(has_3plus_books, axis=1)
    df_series = df[series_mask].copy()
    removed_not_series = df[~series_mask].copy()

    log.info(f"  Before: {before}")
    log.info(f"  After (series >= 3 or verified series): {len(df_series)}")
    log.info(f"  Removed (standalone/short): {len(removed_not_series)}")

    # Save removed for reference
    if len(removed_not_series) > 0:
        removed_not_series.to_csv(DATA_DIR / "removed_standalone_and_short.csv", index=False)

    # ── FILTER 2: Self-Pub / Indie ────────────────────────
    log.info(f"\n  {'='*60}")
    log.info(f"  FILTER 2: Self-Pub / Indie / Small Press")
    log.info(f"  {'='*60}")

    def is_selfpub_indie(row):
        pub_type = str(row.get("Self Pub Flag", "")).strip().lower()
        # Explicitly reject Traditional
        if pub_type == "traditional":
            return False
        # Accept self-pub variants
        if any(kw in pub_type for kw in ["self-pub", "self pub", "indie", "small press", "self-published"]):
            return True
        # Check publisher name for self-pub indicators
        publisher = str(row.get("Publisher Name", "")).strip().lower()
        selfpub_keywords = [
            "independently", "self-publish", "createspace", "draft2digital",
            "smashwords", "kindle direct", "kdp", "lulu", "bookbaby",
            "authorhouse", "xlibris", "iuniverse",
        ]
        if any(kw in publisher for kw in selfpub_keywords):
            return True
        # Unknown publisher type — keep (might be self-pub)
        if not pub_type or pub_type in ["", "nan"]:
            return True
        return False

    selfpub_mask = df_series.apply(is_selfpub_indie, axis=1)
    df_selfpub = df_series[selfpub_mask].copy()
    df_traditional = df_series[~selfpub_mask].copy()

    log.info(f"  Self-Pub/Indie/Small Press/Unknown: {len(df_selfpub)}")
    log.info(f"  Traditional (kept in master, excluded from priority): {len(df_traditional)}")

    # Save traditional separately (for later enrichment)
    if len(df_traditional) > 0:
        df_traditional.to_csv(DATA_DIR / "traditional_pub_series.csv", index=False)
        log.info(f"  Traditional titles saved to: traditional_pub_series.csv")

    # ── FILTER 2.5a: Remove junk/non-fiction entries ─────
    log.info(f"\n  {'='*60}")
    log.info(f"  FILTER 2.5a: Remove junk/non-fiction entries")
    log.info(f"  {'='*60}")

    JUNK_AUTHORS = [
        "library of congress", "carnegie library", "u.s. coast guard",
        "united states government, us coast guard", "copyright office",
        "the law library",
    ]
    JUNK_TITLE_PATTERNS = [
        "catalog of copyright", "classified catalogue", "coast guard",
        "navigation rules", "seamanship manual", "federal register",
        "marine events", "safety zones", "pilotage rates",
        "special local regulations", "oil pollution act",
        "marine inspection zone", "ballast water",
        "fire suppression systems", "regulated navigation",
        "cargo residue", "mariner qualification",
        "subject catalog", "monthly bulletin of the carnegie",
        "library of congress catalogs",
    ]

    def is_junk(row):
        title = str(row.get("Book Series Name", "")).lower().strip()
        author = str(row.get("Author Name", "")).lower().strip()
        if any(ja in author for ja in JUNK_AUTHORS):
            return True
        if any(jt in title for jt in JUNK_TITLE_PATTERNS):
            return True
        return False

    before_junk = len(df_selfpub)
    junk_mask = df_selfpub.apply(is_junk, axis=1)
    df_selfpub = df_selfpub[~junk_mask].copy()
    junk_removed = before_junk - len(df_selfpub)
    log.info(f"  Junk/non-fiction removed: {junk_removed}")

    # ── FILTER 2.5b: English only ─────────────────────────
    log.info(f"\n  {'='*60}")
    log.info(f"  FILTER 2.5b: English language only")
    log.info(f"  {'='*60}")

    NON_ENGLISH_PATTERNS = [
        r'[\u4e00-\u9fff]',          # Chinese
        r'[\u3040-\u309f\u30a0-\u30ff]',  # Japanese
        r'[\uac00-\ud7af]',          # Korean
        r'[\u0600-\u06ff]',          # Arabic
        r'[\u0400-\u04ff]',          # Cyrillic
    ]
    NON_ENGLISH_KEYWORDS = [
        "(portuguese edition)", "(german edition)", "(french edition)",
        "(spanish edition)", "(italian edition)", "(dutch edition)",
        "(japanese edition)", "(chinese edition)", "(korean edition)",
        "(swedish edition)", "(norwegian edition)", "(danish edition)",
        "(turkish edition)", "(polish edition)", "(russian edition)",
        "(hindi edition)", "(arabic edition)", "(hebrew edition)",
        "edição", "ausgabe", "édition", "edición",
    ]

    def is_english(row):
        title = str(row.get("Book Series Name", "")).strip()
        title_lower = title.lower()
        # Check non-English keywords
        for kw in NON_ENGLISH_KEYWORDS:
            if kw in title_lower:
                return False
        # Check non-Latin scripts
        for pat in NON_ENGLISH_PATTERNS:
            if re.search(pat, title):
                return False
        return True

    before_eng = len(df_selfpub)
    eng_mask = df_selfpub.apply(is_english, axis=1)
    non_english_removed = df_selfpub[~eng_mask]
    df_selfpub = df_selfpub[eng_mask].copy()
    log.info(f"  Non-English removed: {before_eng - len(df_selfpub)}")
    if len(non_english_removed) > 0:
        for _, r in non_english_removed.head(5).iterrows():
            log.info(f"    Removed: '{r.get('Book Series Name', '')}'")

    # ── FILTER 3: No fantasy/romantasy ────────────────────
    log.info(f"\n  {'='*60}")
    log.info(f"  FILTER 3: No fantasy/romantasy/paranormal")
    log.info(f"  {'='*60}")

    fantasy_removed = []
    keep_mask = []

    for idx, row in df_selfpub.iterrows():
        is_fantasy, reason = has_fantasy_elements(row)
        keep_mask.append(not is_fantasy)
        if is_fantasy:
            fantasy_removed.append({
                "Book Series Name": row.get("Book Series Name", ""),
                "Author Name": row.get("Author Name", ""),
                "Primary Subgenre": row.get("Primary Subgenre", ""),
                "Reason": reason,
            })

    df_clean = df_selfpub[keep_mask].copy()

    log.info(f"  Before: {len(df_selfpub)}")
    log.info(f"  After (no fantasy): {len(df_clean)}")
    log.info(f"  Fantasy/irrelevant removed: {len(fantasy_removed)}")

    if fantasy_removed:
        pd.DataFrame(fantasy_removed).to_csv(DATA_DIR / "removed_fantasy_final.csv", index=False)
        for r in fantasy_removed[:10]:
            log.info(f"    Removed: '{r['Book Series Name']}' — {r['Reason']}")

    # ── Final dedup ───────────────────────────────────────
    log.info(f"\n  Final deduplication...")
    df_clean["_key"] = (
        df_clean["Book Series Name"].astype(str).str.lower().str.strip()
        + "|"
        + df_clean["Author Name"].astype(str).str.lower().str.strip()
    )
    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["_key"], keep="first").drop(columns=["_key"])
    log.info(f"  Dedup: {before_dedup} -> {len(df_clean)} ({before_dedup - len(df_clean)} removed)")

    # ── Final summary ─────────────────────────────────────
    log.info(f"\n  {'='*60}")
    log.info(f"  FINAL FILTERED DATASET")
    log.info(f"  {'='*60}")
    log.info(f"  Total entries: {len(df_clean)}")

    log.info(f"\n  By subgenre:")
    for sg, c in df_clean["Primary Subgenre"].value_counts().items():
        log.info(f"    {sg}: {c}")

    log.info(f"\n  By publisher type:")
    for pt, c in df_clean["Self Pub Flag"].value_counts().items():
        if pt and str(pt) not in ["", "nan"]:
            log.info(f"    {pt}: {c}")

    # Book count distribution
    valid_counts = []
    for _, row in df_clean.iterrows():
        for col in ["Books in Series", "Verified_Books_Count"]:
            try:
                c = int(float(row.get(col, 0)))
                if c > 0:
                    valid_counts.append(c)
                    break
            except:
                pass

    if valid_counts:
        log.info(f"\n  Book count distribution ({len(valid_counts)} with known counts):")
        log.info(f"    3-5 books: {sum(1 for c in valid_counts if 3 <= c <= 5)}")
        log.info(f"    6-10 books: {sum(1 for c in valid_counts if 6 <= c <= 10)}")
        log.info(f"    11-20 books: {sum(1 for c in valid_counts if 11 <= c <= 20)}")
        log.info(f"    20+ books: {sum(1 for c in valid_counts if c > 20)}")
        log.info(f"    Unknown count (verified series): {len(df_clean) - len(valid_counts)}")

    # Save filtered dataset
    filtered_output = DATA_DIR / "PRIORITY_SELFPUB_SERIES_FOR_ENRICHMENT.csv"
    df_clean.to_csv(filtered_output, index=False)
    log.info(f"\n  Priority dataset saved: {filtered_output.name}")
    log.info(f"  This is the dataset to enrich via Goodreads next.")

    # Also save the full series dataset (including traditional) for reference
    all_series = pd.concat([df_clean, df_traditional], ignore_index=True)
    all_series_output = DATA_DIR / "ALL_SERIES_3PLUS_BOOKS.csv"
    all_series.to_csv(all_series_output, index=False)
    log.info(f"  All series (incl. traditional): {all_series_output.name} ({len(all_series)} rows)")

    return df_clean


if __name__ == "__main__":
    start = datetime.now()
    run_merge_and_filter()
    log.info(f"\n  Completed in {datetime.now() - start}")
