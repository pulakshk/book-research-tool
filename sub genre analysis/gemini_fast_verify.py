#!/usr/bin/env python3
"""
Fast Gemini Series Verification — Parallelized
================================================
Processes a specific subset of subgenres with larger batches and minimal sleep.
Run multiple instances with different --group arguments for parallelism.

Usage:
  python3 gemini_fast_verify.py --group 1   # Dark & Forbidden, Romantic Suspense, Military
  python3 gemini_fast_verify.py --group 2   # Political, Mafia, Ice Hockey
  python3 gemini_fast_verify.py --group 3   # Historical, Small Town, Christian (small remainder)
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
from collections import defaultdict

import pandas as pd
import numpy as np

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"

# ── Args ───────────────────────────────────────────────────
parser = argparse.ArgumentParser()
parser.add_argument("--group", type=int, required=True, choices=[1, 2, 3])
args = parser.parse_args()

GROUP = args.group

SUBGENRE_GROUPS = {
    1: ["Dark & Forbidden Romance", "Romantic Suspense/Psychological Thriller", "Military Drama/Romance"],
    2: ["Political Drama/Romance", "Mafia Drama/Romance", "Ice Hockey & Sports Romance"],
    3: ["Historical Romance & Fiction", "Small Town Drama/Romance", "Christian Drama/Romance"],
}

MY_SUBGENRES = SUBGENRE_GROUPS[GROUP]

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / f"gemini_fast_g{GROUP}.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger(f"gemini_fast_g{GROUP}")

# ── Gemini Setup ───────────────────────────────────────────
def get_gemini_key():
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()

# ── Config ─────────────────────────────────────────────────
BATCH_SIZE = 100
SLEEP_BETWEEN = 0.5
CONSECUTIVE_MISS_THRESHOLD = 400
SAVE_INTERVAL = 3


def _robust_json_parse(text):
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


def gemini_check_batch(model, entries, subgenre):
    titles_text = "\n".join(
        f'{i+1}. "{e["title"]}" by {e["author"]}'
        f'{" (First book: " + e["first_book"] + ")" if e.get("first_book") and e["first_book"] != "nan" else ""}'
        for i, e in enumerate(entries)
    )

    prompt = f"""You are a book database expert. For each title below, determine whether it is a STANDALONE book or part of a book SERIES.

Context: These are {subgenre} titles.

Titles:
{titles_text}

For EACH title, return:
- "title": the title as given
- "is_series": true if this book is part of a series with 2+ books, false if standalone
- "books_in_series": integer number of books in the complete series (null if standalone or unknown)
- "series_name": the name of the series if different from the title (null if standalone)
- "pub_type": "Self-Pub", "Indie", "Small Press", or "Traditional"
- "publisher": the publisher or imprint name if known (null if unknown)

RULES:
- A "series" = multiple books by same author in connected storyline or shared world
- Box sets, omnibus editions count as series
- "Book 1", "#1", "Volume 1" = definitely series
- If uncertain, mark is_series as null (not false)
- Only mark false if confident it's truly standalone
- pub_type: "Self-Pub"=KDP/indie published, "Indie"=small indie publisher, "Small Press"=small established press, "Traditional"=Big 5 or major imprints
- Return data for ALL {len(entries)} titles in order

Return ONLY a JSON array:
[
  {{"title": "...", "is_series": true/false/null, "books_in_series": N_or_null, "series_name": "..._or_null", "pub_type": "...", "publisher": "..._or_null"}}
]"""

    try:
        response = model.generate_content(
            prompt,
            request_options={"timeout": 180},
        )
        text = response.text.strip()
        return _robust_json_parse(text)
    except Exception as e:
        log.warning(f"    Gemini error: {e}")
        return []


def run():
    log.info(f"=" * 60)
    log.info(f"  FAST VERIFY — Group {GROUP}: {', '.join(MY_SUBGENRES)}")
    log.info(f"  Batch size: {BATCH_SIZE}, Sleep: {SLEEP_BETWEEN}s")
    log.info(f"=" * 60)

    if not GEMINI_KEY:
        log.error("No GEMINI_API_KEY!")
        return

    # Load latest partial
    candidates = [
        DATA_DIR / "selfpub_master_gemini_verified_partial.csv",
        DATA_DIR / "selfpub_master_series_verified_partial.csv",
        DATA_DIR / "selfpub_master_mega_expanded.csv",
    ]
    source = None
    for c in candidates:
        if c.exists():
            source = c
            break
    if not source:
        log.error("No data file!")
        return

    df = pd.read_csv(source, low_memory=False)
    log.info(f"  Loaded {len(df)} from {source.name}")

    # Ensure columns
    for col in ["Series_Verified", "Verification_Method", "Verified_Series_Name", "Verified_Books_Count"]:
        if col not in df.columns:
            df[col] = ""

    # Find entries needing check IN MY SUBGENRES ONLY
    def needs_check(row):
        if str(row.get("Primary Subgenre", "")) not in MY_SUBGENRES:
            return False
        verified = str(row.get("Series_Verified", "")).strip()
        if verified in ["Yes", "No", "Unknown"]:
            return False
        method = str(row.get("Verification_Method", "")).strip()
        if method.startswith("gemini_bulk"):
            return False
        try:
            if int(float(row.get("Books in Series", 0))) >= 3:
                return False
        except:
            pass
        return True

    mask = df.apply(needs_check, axis=1)
    indices = df[mask].index.tolist()
    log.info(f"  Need checking: {len(indices)}")

    for sg in MY_SUBGENRES:
        sg_count = sum(1 for i in indices if df.at[i, "Primary Subgenre"] == sg)
        log.info(f"    {sg}: {sg_count}")

    # Init Gemini
    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    # Group by subgenre
    sg_indices = defaultdict(list)
    for idx in indices:
        sg = str(df.at[idx, "Primary Subgenre"])
        sg_indices[sg].append(idx)

    total_series = 0
    total_standalone = 0
    total_batches = 0

    for sg in MY_SUBGENRES:
        sg_list = sg_indices.get(sg, [])
        if not sg_list:
            log.info(f"\n  [{sg}] Nothing to check, skipping")
            continue

        log.info(f"\n  ── {sg} ({len(sg_list)} entries) ──")
        consecutive_misses = 0
        sg_series = 0

        for batch_start in range(0, len(sg_list), BATCH_SIZE):
            batch_end = min(batch_start + BATCH_SIZE, len(sg_list))
            batch_idx = sg_list[batch_start:batch_end]

            entries = []
            for idx in batch_idx:
                row = df.loc[idx]
                entries.append({
                    "title": str(row.get("Book Series Name", "")),
                    "author": str(row.get("Author Name", "")),
                    "first_book": str(row.get("First Book Name", "")),
                })

            results = gemini_check_batch(model, entries, sg)

            batch_series = 0
            batch_standalone = 0

            for i, idx in enumerate(batch_idx):
                result = results[i] if i < len(results) else None

                # Fallback: match by title
                if not result:
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

                    # Apply pub info
                    if pub_type and str(pub_type).lower() not in ["null", "none", ""]:
                        df.at[idx, "Self Pub Flag"] = str(pub_type)
                    if publisher and str(publisher).lower() not in ["null", "none", ""]:
                        cur_pub = str(df.at[idx, "Publisher Name"]) if pd.notna(df.at[idx, "Publisher Name"]) else ""
                        if not cur_pub or cur_pub in ["", "nan"]:
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
                                    cur = df.at[idx, "Books in Series"]
                                    try:
                                        cc = int(float(cur)) if pd.notna(cur) else 0
                                    except:
                                        cc = 0
                                    if bc > cc:
                                        df.at[idx, "Books in Series"] = bc
                            except:
                                pass
                        batch_series += 1
                        consecutive_misses = 0

                    elif is_series is False:
                        df.at[idx, "Series_Verified"] = "No"
                        df.at[idx, "Verification_Method"] = "gemini_bulk_standalone"
                        batch_standalone += 1
                        consecutive_misses += 1
                    else:
                        df.at[idx, "Series_Verified"] = "Unknown"
                        df.at[idx, "Verification_Method"] = "gemini_bulk_uncertain"
                        consecutive_misses += 1
                else:
                    consecutive_misses += 1

            sg_series += batch_series
            total_series += batch_series
            total_standalone += batch_standalone
            total_batches += 1

            bn = batch_start // BATCH_SIZE + 1
            tb = (len(sg_list) + BATCH_SIZE - 1) // BATCH_SIZE
            log.info(f"    Batch {bn}/{tb}: +{batch_series} series, {batch_standalone} standalone (misses: {consecutive_misses})")

            # Auto-stop
            if consecutive_misses >= CONSECUTIVE_MISS_THRESHOLD:
                remaining = len(sg_list) - batch_end
                log.info(f"    AUTO-STOP: skipping {remaining} remaining for {sg}")
                for skip_idx in sg_list[batch_end:]:
                    df.at[skip_idx, "Series_Verified"] = "Skipped"
                    df.at[skip_idx, "Verification_Method"] = "auto_stop_skipped"
                break

            # Save
            if total_batches % SAVE_INTERVAL == 0:
                out = DATA_DIR / f"gemini_fast_g{GROUP}_partial.csv"
                df.to_csv(out, index=False)
                log.info(f"    [Saved: {out.name}]")

            time.sleep(SLEEP_BETWEEN)

        log.info(f"  [{sg}] Done: {sg_series} series found")

    # Final save
    out = DATA_DIR / f"gemini_fast_g{GROUP}_done.csv"
    df.to_csv(out, index=False)
    log.info(f"\n  DONE — Group {GROUP}: {total_series} series, {total_standalone} standalone in {total_batches} batches")
    log.info(f"  Saved to: {out}")


if __name__ == "__main__":
    start = datetime.now()
    run()
    log.info(f"  Completed in {datetime.now() - start}")
