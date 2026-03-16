#!/usr/bin/env python3
"""
RUN GENRE PIPELINE — Orchestrator for the full genre crawl workflow.
Runs: Discovery → Enrichment → Aggregation for a given subgenre.
"""

import asyncio
import os
import sys
import argparse
from loguru import logger

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)

from genre_crawl import crawl_subgenre, SUBGENRE_URLS
from genre_enrichment import enrich_subgenre
from genre_aggregate import aggregate_subgenre


async def run_pipeline(genre_name, phase=None, visible=False):
    """Run the full pipeline or a specific phase for a subgenre."""
    import re
    safe_name = re.sub(r'[/\\:*?"<>|]', '_', genre_name)
    
    raw_csv = os.path.join(SCRIPT_DIR, f"{safe_name}_raw_discovery.csv")
    enriched_csv = os.path.join(SCRIPT_DIR, f"{safe_name}_enriched.csv")
    final_csv = os.path.join(SCRIPT_DIR, f"{safe_name}_final.csv")
    
    if phase is None or phase == 'crawl':
        logger.info(f"\n{'='*60}")
        logger.info(f"🚀 PHASE 1: AMAZON DISCOVERY — {genre_name}")
        logger.info(f"{'='*60}")
        raw_csv = await crawl_subgenre(genre_name)
        if not raw_csv:
            logger.error("Phase 1 failed — no output file.")
            return
    
    if phase is None or phase == 'enrich':
        logger.info(f"\n{'='*60}")
        logger.info(f"📚 PHASE 2: ENRICHMENT — {genre_name}")
        logger.info(f"{'='*60}")
        if not os.path.exists(raw_csv):
            logger.error(f"Raw CSV not found: {raw_csv}")
            return
        enriched_csv = await enrich_subgenre(raw_csv, genre_name)
        if not enriched_csv:
            logger.error("Phase 2 failed — no output file.")
            return
    
    if phase is None or phase == 'aggregate':
        logger.info(f"\n{'='*60}")
        logger.info(f"📊 PHASE 3: AGGREGATION — {genre_name}")
        logger.info(f"{'='*60}")
        if not os.path.exists(enriched_csv):
            logger.error(f"Enriched CSV not found: {enriched_csv}")
            return
        final_csv = aggregate_subgenre(enriched_csv, genre_name)
    
    logger.success(f"\n🎉 Pipeline complete for {genre_name}!")
    logger.info(f"  📄 Raw discovery: {raw_csv}")
    logger.info(f"  📄 Enriched (book-level): {enriched_csv}")
    logger.info(f"  📄 Final (series-level): {final_csv}")


def main():
    parser = argparse.ArgumentParser(description="Genre Crawl Pipeline Orchestrator")
    parser.add_argument("--genre", type=str, help="Subgenre to process")
    parser.add_argument("--all", action="store_true", help="Process all 9 subgenres")
    parser.add_argument("--phase", type=str, choices=['crawl', 'enrich', 'aggregate'],
                        help="Run only a specific phase")
    parser.add_argument("--visible", action="store_true", help="Run browser in visible mode")
    parser.add_argument("--list", action="store_true", help="List available subgenres")
    args = parser.parse_args()
    
    if args.list:
        print("\nAvailable subgenres:")
        for i, name in enumerate(SUBGENRE_URLS.keys(), 1):
            urls = SUBGENRE_URLS[name]
            print(f"  {i}. {name} ({len(urls.get('bestseller', []))} bestseller + {len(urls.get('search', []))} search URLs)")
        return
    
    if args.all:
        for genre in SUBGENRE_URLS.keys():
            asyncio.run(run_pipeline(genre, args.phase, args.visible))
    elif args.genre:
        asyncio.run(run_pipeline(args.genre, args.phase, args.visible))
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
