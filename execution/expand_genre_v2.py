#!/usr/bin/env python3
"""
EXPAND GENRE V2 — Deeper Gemini discovery with more prompts + better validation.
Runs additional discovery rounds for subgenres that need more titles.
Focuses on finding real, verifiable self-pub series (not fantasy/romantasy).
"""

import asyncio
import json
import os
import re
import random
import sys
import time

import pandas as pd
import numpy as np
from loguru import logger
from playwright.async_api import async_playwright
from dotenv import load_dotenv

PROJECT_ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..')
load_dotenv(os.path.join(PROJECT_ROOT, '.env'))
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

try:
    import google.generativeai as genai
    if GEMINI_API_KEY:
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel('gemini-2.5-flash')
    else:
        gemini_model = None
except ImportError:
    gemini_model = None

# Import from the main expansion script
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from expand_genre_discovery import (
    SUBGENRES, TRADITIONAL_PUBLISHERS, FANTASY_EXCLUSION_KEYWORDS,
    SELF_PUB_KEYWORDS, OUTPUT_DIR, USER_AGENTS, WORDS_PER_PAGE, WORDS_PER_HOUR,
    normalize_name, determine_self_pub, is_fantasy_or_excluded,
    create_stealth_context, safe_goto, validate_series_on_goodreads,
    filter_validated_series, format_to_xlsx_schema, gemini_enrich_series,
    compute_commissioning_score, load_existing_series, _save_checkpoint,
    validation_worker,
)

HEADLESS = True
WORKER_COUNT = 4
SLEEP_MIN = 2
SLEEP_MAX = 5

# ============================================================================
# ENHANCED DISCOVERY PROMPTS — More specific, more grounded
# ============================================================================

V2_PROMPTS = [
    # Prompt 1: Kindle Unlimited top performers
    """I need you to list 25 self-published {subgenre} book SERIES that are currently popular on Amazon Kindle Unlimited.

Requirements:
- Each series must have AT LEAST 3 published books
- The author must be self-published or indie (NOT with Penguin, HarperCollins, Simon & Schuster, Hachette, Macmillan, Random House, Harlequin, Montlake, Avon, Berkley, St. Martin's)
- The series must be contemporary drama/romance ONLY
- ABSOLUTELY NO: fantasy, romantasy, paranormal, sci-fi, vampires, werewolves, shifters, fae, magic, dystopian, YA
- Focus on series that readers actually discuss on BookTok, Goodreads, or romance blogs

For each, provide the EXACT series name as it appears on Goodreads, the author's full name, and the number of books.

Return ONLY valid JSON: [{{"series_name": "...", "author_name": "...", "estimated_books": N}}, ...]""",

    # Prompt 2: By specific tropes within the subgenre
    """List 25 self-published {subgenre} book series organized by popular romance tropes.

Include series featuring these tropes: enemies-to-lovers, fake relationship, forced proximity, second chance, age gap, grumpy/sunshine, brother's best friend, single dad/mom.

Requirements:
- 3+ books per series, self-pub/indie only
- Contemporary/realistic ONLY — no fantasy, paranormal, supernatural, sci-fi, romantasy
- Include the EXACT Goodreads series name

Return ONLY valid JSON: [{{"series_name": "...", "author_name": "...", "estimated_books": N}}, ...]""",

    # Prompt 3: By author productivity (prolific indie authors)
    """List 25 prolific self-published romance/drama authors who write in the {subgenre} subgenre and have multiple series with 3+ books each.

For each author, pick their BEST or MOST POPULAR series.

Requirements:
- Self-pub/indie only (NOT traditional publishers)
- The series must be findable on Goodreads with its own series page
- Contemporary/realistic only — NO fantasy, paranormal, sci-fi, romantasy, YA
- Focus on authors with 10,000+ Goodreads ratings across their catalog

Return ONLY valid JSON: [{{"series_name": "...", "author_name": "...", "estimated_books": N}}, ...]""",

    # Prompt 4: Reader recommendation style
    """Imagine you're a romance book blogger recommending {subgenre} series to readers. List 25 self-published series that you would recommend.

Each must:
- Have 3+ books published
- Be by an indie/self-pub author
- Be pure contemporary drama/romance (no fantasy, paranormal, sci-fi, fae, vampires, etc.)
- Be a real series that exists on Goodreads and Amazon

Include mix of well-known and hidden gems. Use the EXACT series name from Goodreads.

Return ONLY valid JSON: [{{"series_name": "...", "author_name": "...", "estimated_books": N}}, ...]""",

    # Prompt 5: Amazon category specific
    """List 25 self-published book series that appear in Amazon's {subgenre} Kindle bestseller categories.

These should be series where:
- The author independently publishes via KDP
- The series has 3 or more books
- They regularly appear in Amazon's Top 100 for their category
- They are drama/romance genre — NOT fantasy, NOT paranormal, NOT sci-fi, NOT romantasy

Use exact Goodreads series names.

Return ONLY valid JSON: [{{"series_name": "...", "author_name": "...", "estimated_books": N}}, ...]""",

    # Prompt 6: Emerging 2024-2025 releases
    """List 25 self-published {subgenre} book series where the first book was published in 2023, 2024, or 2025.

Focus on NEWER series that are building momentum:
- 3+ books already published
- Self-pub/indie authors only
- Contemporary drama/romance only — NO fantasy, paranormal, sci-fi, romantasy, YA
- Preferably with 1000+ Goodreads ratings on book 1

Return ONLY valid JSON: [{{"series_name": "...", "author_name": "...", "estimated_books": N}}, ...]""",

    # Prompt 7: Connected standalones / shared universe
    """List 25 self-published {subgenre} "interconnected standalone" or "shared universe" romance series where each book features a different couple but they're all connected.

Requirements:
- 3+ books, self-pub/indie
- Contemporary/realistic romance/drama ONLY (no paranormal, fantasy, sci-fi)
- These are the most adaptable format for serialized audio
- Use exact Goodreads series names

Return ONLY valid JSON: [{{"series_name": "...", "author_name": "...", "estimated_books": N}}, ...]""",

    # Prompt 8: High word count / long series
    """List 25 self-published {subgenre} book series that have 5 or more books — the LONGEST series you know of.

Focus on:
- Series with 5-20+ books
- Self-pub/indie authors
- Pure contemporary drama/romance (absolutely NO fantasy, paranormal, sci-fi)
- Strong reader engagement on Goodreads

Return ONLY valid JSON: [{{"series_name": "...", "author_name": "...", "estimated_books": N}}, ...]""",
]


async def gemini_discover_v2(subgenre, existing_names):
    """Run V2 discovery prompts for a subgenre."""
    if not gemini_model:
        return []

    all_series = []
    seen_keys = set(existing_names)  # Start with existing to avoid dupes

    for i, prompt_template in enumerate(V2_PROMPTS):
        logger.info(f"  V2 Prompt {i+1}/{len(V2_PROMPTS)} for {subgenre}...")
        prompt = prompt_template.format(subgenre=subgenre)

        for attempt in range(3):
            try:
                response = await asyncio.to_thread(gemini_model.generate_content, prompt)
                text = response.text.strip()
                if '```json' in text:
                    text = text.split('```json')[1].split('```')[0]
                elif '```' in text:
                    text = text.split('```')[1].split('```')[0]

                series_list = json.loads(text)
                if not isinstance(series_list, list):
                    break

                new_count = 0
                for s in series_list:
                    name = str(s.get('series_name', '')).strip()
                    author = str(s.get('author_name', '')).strip()
                    if not name or not author:
                        continue
                    if is_fantasy_or_excluded(name):
                        continue
                    key = f"{normalize_name(name)}|||{normalize_name(author)}"
                    if key in seen_keys or normalize_name(name) in seen_keys:
                        continue
                    seen_keys.add(key)
                    seen_keys.add(normalize_name(name))
                    all_series.append({
                        'series_name': name,
                        'author_name': author,
                        'estimated_books': int(s.get('estimated_books', 3)) if s.get('estimated_books') else 3,
                        'source_prompt': f'v2_{i+1}',
                    })
                    new_count += 1

                logger.info(f"    Got {new_count} new series (total new: {len(all_series)})")
                break

            except json.JSONDecodeError:
                if attempt < 2:
                    await asyncio.sleep(2)
            except Exception as e:
                if "429" in str(e):
                    logger.warning("  Rate limit — waiting 15s")
                    await asyncio.sleep(15)
                else:
                    logger.error(f"  Error: {e}")
                    break

        await asyncio.sleep(random.uniform(2, 4))

    return all_series


async def run_v2_for_subgenre(subgenre, existing_keys):
    """Run V2 expansion for a single subgenre."""
    safe_name = re.sub(r'[/\\:*?"<>| &]', '_', subgenre)

    # Load existing discoveries + expansions to avoid dupes
    existing_names = set(existing_keys)

    # Also load V1 results
    v1_path = os.path.join(OUTPUT_DIR, f"{safe_name}_expanded.csv")
    if os.path.exists(v1_path):
        v1 = pd.read_csv(v1_path)
        for _, r in v1.iterrows():
            existing_names.add(normalize_name(str(r.get('Book Series Name', ''))))

    # Phase 1: Discover
    v2_discovery_path = os.path.join(OUTPUT_DIR, f"{safe_name}_v2_discovered.json")

    if os.path.exists(v2_discovery_path):
        with open(v2_discovery_path) as f:
            discovered = json.load(f)
        logger.info(f"  Loaded {len(discovered)} existing V2 discoveries for {subgenre}")
    else:
        logger.info(f"\n{'='*60}")
        logger.info(f"V2 DISCOVERY: {subgenre}")
        logger.info(f"{'='*60}")
        discovered = await gemini_discover_v2(subgenre, existing_names)
        with open(v2_discovery_path, 'w') as f:
            json.dump(discovered, f, indent=2)
        logger.success(f"  Discovered {len(discovered)} new candidates for {subgenre}")

    if not discovered:
        logger.warning(f"  No new candidates for {subgenre}")
        return 0

    # Phase 2: Validate on Goodreads
    logger.info(f"\n  VALIDATING {len(discovered)} candidates for {subgenre}...")
    v2_validated_path = os.path.join(OUTPUT_DIR, f"{safe_name}_v2_validated.csv")

    # Check resume
    already_validated = set()
    existing_validated = []
    if os.path.exists(v2_validated_path):
        vdf = pd.read_csv(v2_validated_path)
        already_validated = set(vdf['series_name'].str.lower().str.strip())
        for _, row in vdf.iterrows():
            existing_validated.append({
                'series_name': row.get('series_name', ''),
                'author_name': row.get('author_name', ''),
                'gr_series_name': row.get('series_name', ''),
                'gr_series_url': row.get('gr_series_url', ''),
                'num_books': int(row.get('num_books', 0)),
                'publisher': row.get('publisher', ''),
                'self_pub_flag': row.get('self_pub_flag', ''),
                'genres': row.get('genres', ''),
                'description': row.get('description', ''),
                'gr_validated': bool(row.get('gr_validated', False)),
                'books': json.loads(row.get('books_json', '[]')),
            })

    to_validate = [s for s in discovered if normalize_name(s['series_name']) not in already_validated]

    if to_validate:
        queue = asyncio.Queue()
        for i, s in enumerate(to_validate):
            queue.put_nowait((i, s))

        validated_results = list(existing_validated)
        lock = asyncio.Lock()

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            workers = [
                validation_worker(i, browser, queue, validated_results, lock, v2_validated_path)
                for i in range(min(WORKER_COUNT, len(to_validate)))
            ]
            await asyncio.gather(*workers)
            await browser.close()

        _save_checkpoint(validated_results, v2_validated_path)
    else:
        validated_results = existing_validated

    # Phase 3: Filter
    filtered = filter_validated_series(validated_results, existing_keys)
    logger.info(f"  {len(validated_results)} validated -> {len(filtered)} after filtering")

    if not filtered:
        return 0

    # Phase 4: Enrich + save
    v2_final_path = os.path.join(OUTPUT_DIR, f"{safe_name}_v2_expanded.csv")
    final_rows = []
    for i, series in enumerate(filtered):
        enrichment = await gemini_enrich_series(
            series.get('gr_series_name', series.get('series_name', '')),
            series.get('author_name', ''),
            series.get('description', ''),
            subgenre,
        )
        await asyncio.sleep(random.uniform(0.5, 1.5))
        row = format_to_xlsx_schema(series, subgenre, enrichment)
        if row:
            final_rows.append(row)
        if (i + 1) % 10 == 0:
            logger.info(f"  Enriched {i+1}/{len(filtered)}")

    if final_rows:
        result_df = pd.DataFrame(final_rows)
        result_df = result_df.sort_values('Commissioning_Score', ascending=False)
        result_df.to_csv(v2_final_path, index=False)
        logger.success(f"  V2: {len(result_df)} new series for {subgenre} -> {v2_final_path}")

        # Merge with V1
        combined_path = os.path.join(OUTPUT_DIR, f"{safe_name}_combined.csv")
        frames = [result_df]
        if os.path.exists(v1_path):
            frames.insert(0, pd.read_csv(v1_path))
        combined = pd.concat(frames, ignore_index=True)
        combined['_key'] = combined['Book Series Name'].apply(normalize_name)
        combined = combined.drop_duplicates(subset='_key', keep='first').drop(columns='_key')
        combined = combined.sort_values('Commissioning_Score', ascending=False)
        combined.to_csv(combined_path, index=False)
        logger.info(f"  Combined (V1+V2): {len(combined)} series -> {combined_path}")

        return len(result_df)
    return 0


async def main():
    """Run V2 expansion for all subgenres."""
    # Load existing data for dedup
    xlsx_path = os.path.join(PROJECT_ROOT, "subgenre-pipeline", "source-data", "Sub genre analysis- Self Pub universe.xlsx")
    existing_keys = load_existing_series(xlsx_path) if os.path.exists(xlsx_path) else set()

    # Also add V1 results
    for sg in SUBGENRES:
        safe_name = re.sub(r'[/\\:*?"<>| &]', '_', sg)
        for suffix in ['_expanded.csv', '_v2_expanded.csv']:
            path = os.path.join(OUTPUT_DIR, f"{safe_name}{suffix}")
            if os.path.exists(path):
                try:
                    df = pd.read_csv(path)
                    for _, r in df.iterrows():
                        s = normalize_name(str(r.get('Book Series Name', '')))
                        a = normalize_name(str(r.get('Author Name', '')))
                        existing_keys.add(f"{s}|||{a}")
                        existing_keys.add(s)
                except:
                    pass

    logger.info(f"Existing keys for dedup: {len(existing_keys)}")

    total_new = 0
    for subgenre in SUBGENRES:
        try:
            n = await run_v2_for_subgenre(subgenre, existing_keys)
            total_new += n

            # Update existing keys
            safe_name = re.sub(r'[/\\:*?"<>| &]', '_', subgenre)
            v2_path = os.path.join(OUTPUT_DIR, f"{safe_name}_v2_expanded.csv")
            if os.path.exists(v2_path):
                df = pd.read_csv(v2_path)
                for _, r in df.iterrows():
                    s = normalize_name(str(r.get('Book Series Name', '')))
                    a = normalize_name(str(r.get('Author Name', '')))
                    existing_keys.add(f"{s}|||{a}")
                    existing_keys.add(s)
        except Exception as e:
            logger.error(f"Failed for {subgenre}: {e}")

    # Build combined master
    logger.info(f"\n{'='*60}")
    logger.info("BUILDING COMBINED MASTER (V1 + V2)")
    logger.info(f"{'='*60}")

    all_dfs = []
    for sg in SUBGENRES:
        safe_name = re.sub(r'[/\\:*?"<>| &]', '_', sg)
        combined_path = os.path.join(OUTPUT_DIR, f"{safe_name}_combined.csv")
        expanded_path = os.path.join(OUTPUT_DIR, f"{safe_name}_expanded.csv")
        # Prefer combined, fallback to V1
        path = combined_path if os.path.exists(combined_path) else expanded_path
        if os.path.exists(path):
            df = pd.read_csv(path)
            all_dfs.append(df)
            logger.info(f"  {sg}: {len(df)} series")

    if all_dfs:
        master = pd.concat(all_dfs, ignore_index=True)
        master['_key'] = master['Book Series Name'].apply(normalize_name)
        master = master.drop_duplicates(subset='_key', keep='first').drop(columns='_key')
        master_path = os.path.join(OUTPUT_DIR, "all_genres_expanded_master_v2.csv")
        master.to_csv(master_path, index=False)
        logger.success(f"\nMaster V2: {master_path} ({len(master)} total series)")
        logger.info(f"  V2 added {total_new} new series on top of V1")


if __name__ == "__main__":
    asyncio.run(main())
