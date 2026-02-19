#!/usr/bin/env python3
import argparse
from loguru import logger
from execution.pipeline.data import load_dataset, save_dataset
from execution.pipeline.cleaning import deduplicate_dataset, filter_unrelated_content
from execution.pipeline.scrapers import run_scraping_pipeline
from execution.pipeline.enrichment import run_enrichment_pipeline
from execution.pipeline.analysis import generate_report

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
        logger.info("=== PHASE 2: ENRICHMENT (P0/P1 FOCUS) ===")
        # Identify P0/P1 for advanced qualitative work
        try:
            temp_report = generate_report(df)
            p0_p1 = temp_report[temp_report['Commissioning Rank'].isin(['P0', 'P1'])]['Book Series Name'].tolist()
            logger.info(f"Targeting {len(p0_p1)} P0/P1 series for advanced enrichment.")
        except Exception as e:
            logger.warning(f"Could not pre-identify P0/P1: {e}")
            p0_p1 = None
            
        df = run_enrichment_pipeline(df, p0_p1_series=p0_p1)
        save_dataset(df, "After Enrichment")
        
    if args.analyze or args.all:
        logger.info("=== PHASE 3: FINAL ANALYSIS ===")
        generate_report(df)
        
    if not any([args.all, args.clean, args.enrich, args.analyze]):
        parser.print_help()

if __name__ == "__main__":
    main()
