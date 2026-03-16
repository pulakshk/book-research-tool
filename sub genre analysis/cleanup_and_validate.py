#!/usr/bin/env python3
"""
Data Cleanup & Validation Pipeline
===================================
1. Filter out fantasy/romantasy/paranormal titles from all subgenres
2. Deduplicate across files and within the master sheet
3. Validate titles using Gemini to check subgenre relevance
4. Flag and remove irrelevant entries
"""

import os
import json
import re
import time
import logging
from pathlib import Path
from datetime import datetime

import pandas as pd

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
        logging.FileHandler(DATA_DIR / "cleanup.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("cleanup")

# ── Gemini Setup ───────────────────────────────────────────
def get_gemini_key():
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()

# ── Fantasy/Romantasy detection ────────────────────────────
HARD_FANTASY_TERMS = [
    "werewolf", "shifter", "vampire", "fae", "fairy", "dragon",
    "witch", "wizard", "sorcerer", "mage", "elf", "elven",
    "demon", "angel", "paranormal", "supernatural", "magic system",
    "kingdom", "throne", "realm", "enchant", "mythical",
    "shapeshifter", "lycan", "alpha mate", "omega verse",
    "chosen one", "prophecy", "necromancer", "warlock",
    "romantasy", "high fantasy", "epic fantasy", "urban fantasy",
    "paranormal romance", "fantasy romance", "shifter romance",
    "alien", "orc", "goblin", "centaur", "mermaid", "siren",
    "immortal", "undead", "zombie", "werewolves", "vampires",
    "coven", "dark magic", "blood magic", "spell", "enchantment",
    "faerie", "fey", "elemental", "portal fantasy",
    "dystopian", "post-apocalyptic", "sci-fi romance",
]

# Light fantasy exceptions (OK to keep)
LIGHT_FANTASY_OK = [
    "magical realism", "time travel romance", "ghost romance",
    "second chance", "light fantasy", "lite fantasy",
    "christmas magic", "holiday magic", "lucky charm",
    "fairy tale retelling", "modern fairy tale",
]

# Non-romance / non-drama indicators (should be filtered)
NON_ROMANCE_TERMS = [
    "textbook", "cookbook", "self-help", "business",
    "programming", "technical", "reference guide",
    "children's", "picture book", "coloring book",
    "non-fiction", "nonfiction", "biography", "memoir",
    "travel guide", "how to", "for dummies",
    "academic", "scholarly", "journal",
]


def is_fantasy_or_irrelevant(row):
    """Check if a row has fantasy/romantasy/irrelevant markers."""
    text_fields = [
        str(row.get("Book Series Name", "")),
        str(row.get("First Book Name", "")),
        str(row.get("Subjective Analysis", "")),
        str(row.get("Differentiator", "")),
        str(row.get("Primary Trope", "")),
        str(row.get("Books_In_Series_List", "")),
        str(row.get("Universe Reasoning", "")),
    ]
    combined = " ".join(text_fields).lower()

    # Check non-romance first
    for term in NON_ROMANCE_TERMS:
        if term in combined:
            return True, f"non-romance: {term}"

    # Check for light fantasy exceptions
    for exc in LIGHT_FANTASY_OK:
        if exc in combined:
            return False, ""

    # Check for hard fantasy terms
    for term in HARD_FANTASY_TERMS:
        if term in combined:
            return True, f"fantasy: {term}"

    return False, ""


def gemini_validate_batch(titles_batch, subgenre):
    """Use Gemini to validate if titles belong to the stated subgenre."""
    if not GEMINI_KEY:
        return {}

    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    titles_text = "\n".join(
        f'{i+1}. "{t["series"]}" by {t["author"]} (listed as: {t["subgenre"]})'
        for i, t in enumerate(titles_batch)
    )

    prompt = f"""You are a book classification expert. Validate whether these titles truly belong
to their stated subgenre. We need DRAMA ROMANCE and CONTEMPORARY ROMANCE titles only.

REJECT if the title is:
- High fantasy, romantasy, paranormal, sci-fi, dystopian
- Not romance/drama (e.g., pure thriller, horror, literary fiction, non-fiction)
- Heavy supernatural elements (werewolves, vampires, fae, shifters, magic systems)

KEEP if:
- Contemporary romance/drama, even with light supernatural elements
- Romantic suspense with grounded/realistic settings
- Historical romance in real-world settings
- Light fantasy/magical realism where romance is the primary genre

Titles to validate:
{titles_text}

Return JSON array:
[
  {{
    "title": "series name",
    "author": "author name",
    "verdict": "KEEP" or "REJECT",
    "reason": "1 sentence explanation",
    "correct_subgenre": "what it actually is (if REJECT)"
  }}
]

ONLY the JSON array."""

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()

        # Parse JSON robustly
        if text.startswith("```json"):
            text = text[7:]
        if text.startswith("```"):
            text = text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()

        # Fix trailing commas
        text = re.sub(r',\s*([}\]])', r'\1', text)

        try:
            data = json.loads(text)
            if isinstance(data, list):
                return {d.get("title", "").lower().strip(): d for d in data}
        except json.JSONDecodeError:
            match = re.search(r'\[[\s\S]*\]', text)
            if match:
                try:
                    data = json.loads(match.group())
                    return {d.get("title", "").lower().strip(): d for d in data}
                except json.JSONDecodeError:
                    pass

    except Exception as e:
        log.error(f"  Gemini validation error: {e}")

    return {}


def run_cleanup():
    """Run full cleanup pipeline."""
    log.info("=" * 60)
    log.info("DATA CLEANUP & VALIDATION PIPELINE")
    log.info("=" * 60)

    # Find the latest master file
    candidates = [
        DATA_DIR / "selfpub_master_expanded_v2.csv",
        DATA_DIR / "selfpub_master_expanded.csv",
        DATA_DIR / "selfpub_master_consolidated.csv",
    ]

    source = None
    for c in candidates:
        if c.exists():
            source = c
            break

    if not source:
        log.error("No master CSV found!")
        return

    df = pd.read_csv(source, on_bad_lines="skip")
    log.info(f"  Loaded {len(df)} series from {source.name}")

    # ── Step 1: Rule-based fantasy/irrelevant filter ────────
    log.info("\n  STEP 1: Rule-based fantasy/irrelevant filtering")

    removed_rules = []
    keep_mask = []

    for idx, row in df.iterrows():
        is_bad, reason = is_fantasy_or_irrelevant(row)
        keep_mask.append(not is_bad)
        if is_bad:
            removed_rules.append({
                "Book Series Name": row.get("Book Series Name", ""),
                "Author Name": row.get("Author Name", ""),
                "Primary Subgenre": row.get("Primary Subgenre", ""),
                "Reason": reason,
            })

    removed_count = len(removed_rules)
    df_clean = df[keep_mask].copy()
    log.info(f"  Rule-based filter removed: {removed_count} series")
    log.info(f"  Remaining: {len(df_clean)} series")

    if removed_rules:
        pd.DataFrame(removed_rules).to_csv(DATA_DIR / "removed_rule_based.csv", index=False)
        log.info(f"  Removed titles saved to: removed_rule_based.csv")

        # Show some examples
        for r in removed_rules[:10]:
            log.info(f"    Removed: '{r['Book Series Name']}' ({r['Primary Subgenre']}) - {r['Reason']}")

    # ── Step 2: Deduplication ──────────────────────────────
    log.info("\n  STEP 2: Deduplication")

    # Create dedup key
    df_clean["_dedup"] = (
        df_clean["Book Series Name"].astype(str).str.lower().str.strip()
        + "|"
        + df_clean["Author Name"].astype(str).str.lower().str.strip()
    )

    before_dedup = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["_dedup"], keep="first")
    df_clean = df_clean.drop(columns=["_dedup"])
    dupes_removed = before_dedup - len(df_clean)
    log.info(f"  Duplicates removed: {dupes_removed}")
    log.info(f"  After dedup: {len(df_clean)} series")

    # ── Step 3: Gemini validation for suspicious titles ────
    log.info("\n  STEP 3: Gemini validation of ambiguous titles")

    # Find titles that are ambiguous (from certain subgenres prone to fantasy crossover)
    ambiguous_subgenres = [
        "Dark & Forbidden Romance",
        "Historical Romance & Fiction",
        "Mafia Drama/Romance",
        "Romantic Suspense/Psychological Thriller",
    ]

    ambiguous = df_clean[df_clean["Primary Subgenre"].isin(ambiguous_subgenres)].copy()
    log.info(f"  Titles to validate via Gemini: {len(ambiguous)} (from {', '.join(ambiguous_subgenres)})")

    # Process in batches of 20
    BATCH_SIZE = 20
    all_rejections = set()
    total_validated = 0

    for batch_start in range(0, len(ambiguous), BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, len(ambiguous))
        batch = ambiguous.iloc[batch_start:batch_end]

        batch_data = [
            {
                "series": str(row.get("Book Series Name", "")),
                "author": str(row.get("Author Name", "")),
                "subgenre": str(row.get("Primary Subgenre", "")),
            }
            for _, row in batch.iterrows()
        ]

        results = gemini_validate_batch(batch_data, "mixed")

        for _, row in batch.iterrows():
            key = str(row.get("Book Series Name", "")).lower().strip()
            if key in results:
                verdict = results[key]
                if verdict.get("verdict", "").upper() == "REJECT":
                    all_rejections.add(key)
                    log.info(f"    REJECT: '{row.get('Book Series Name', '')}' - {verdict.get('reason', '')}")

        total_validated += len(batch)
        log.info(f"    Validated {total_validated}/{len(ambiguous)} titles")
        time.sleep(1.5)

    # Remove Gemini-rejected titles
    if all_rejections:
        before_gemini = len(df_clean)
        df_clean = df_clean[
            ~df_clean["Book Series Name"].astype(str).str.lower().str.strip().isin(all_rejections)
        ]
        gemini_removed = before_gemini - len(df_clean)
        log.info(f"\n  Gemini validation removed: {gemini_removed} titles")
    else:
        log.info("\n  Gemini validation: no additional removals")

    # ── Step 4: Final stats ────────────────────────────────
    log.info("\n  FINAL CLEANUP RESULTS:")
    log.info(f"  Original: {len(df)} series")
    log.info(f"  After cleanup: {len(df_clean)} series")
    log.info(f"  Total removed: {len(df) - len(df_clean)}")

    log.info(f"\n  Subgenre breakdown:")
    for sg, count in df_clean["Primary Subgenre"].value_counts().items():
        target = 500
        status = "OK" if count >= target else f"need {target - count} more"
        log.info(f"    {sg}: {count} ({status})")

    # Save
    output_path = DATA_DIR / "selfpub_master_cleaned.csv"
    df_clean.to_csv(output_path, index=False)
    log.info(f"\n  Cleaned data saved to: {output_path}")

    return df_clean


if __name__ == "__main__":
    start = datetime.now()
    run_cleanup()
    elapsed = datetime.now() - start
    log.info(f"\n  Cleanup completed in {elapsed}")
