#!/usr/bin/env python3
import argparse
from loguru import logger
from src.pipeline.data import load_dataset, save_dataset
from src.pipeline.cleaning import deduplicate_dataset, filter_unrelated_content
from src.pipeline.scrapers import run_scraping_pipeline
from src.pipeline.enrichment import run_enrichment_pipeline
from src.pipeline.analysis import generate_report

def main():
    parser = argparse.ArgumentParser(description="Book Research Pipeline (Consolidated)")
    parser.add_argument("--all", action="store_true", help="Run full pipeline")
    parser.add_argument("--clean", action="store_true", help="Run cleaning (dedupe + filter)")
    parser.add_argument("--scrape", action="store_true", help="Run scraping (series exhaustion)")
    parser.add_argument("--enrich", action="store_true", help="Run enrichment (Gemini)")
    parser.add_argument("--analyze", action="store_true", help="Run analysis report")
    args = parser.parse_args()
    
    # Load
    df = load_dataset(use_backup=False) 
    if df.empty: df = load_dataset(use_backup=True)

    if args.scrape or args.all:
        logger.info("=== PHASE 0: SCRAPING (SERIES EXHAUSTION) ===")
        df = run_scraping_pipeline(df)
        save_dataset(df, "After Scraping")

    if args.clean or args.all:
        logger.info("=== PHASE 1: CLEANING ===")
        df = deduplicate_dataset(df)
        df = filter_unrelated_content(df)
        save_dataset(df, "After Cleaning")
        
    if args.enrich or args.all:
        logger.info("=== PHASE 2: ENRICHMENT ===")
        df = run_enrichment_pipeline(df)
        save_dataset(df, "After Enrichment")
        
    if args.analyze or args.all:
        logger.info("=== PHASE 3: ANALYSIS ===")
        generate_report(df)
        
    if not any([args.all, args.clean, args.enrich, args.analyze]):
        parser.print_help()

if __name__ == "__main__":
    main()
