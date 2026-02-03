#!/usr/bin/env python3
"""
Quick Test Script for Book Research Tool Pipeline
Tests new fuzzy matching, extraction patterns, and validation
"""
import sys
import os
import pandas as pd
from loguru import logger

# Add project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from utils.text_normalizer import normalize_title, normalize_author, fuzzy_match_titles
from utils.matcher import BookMatcher
from utils.validator import validate_file
import config

logger.info("=" * 80)
logger.info("BOOK RESEARCH TOOL - QUICK INTEGRATION TEST")
logger.info("=" * 80)

# ===== TEST 1: Text Normalization =====
logger.info("\n✓ TEST 1: Text Normalization")
test_titles = [
    "Pucking Around (Jacksonville Rays, Book 1)",
    "Pucking Around: Jacksonville Rays - Book #1",
    "PUCKING AROUND (Jacksonville Rays #1)",
    "Pucking Around"
]

logger.info("  Testing title variations:")
normalized = [normalize_title(t) for t in test_titles]
for i, t in enumerate(test_titles):
    logger.info(f"    {i+1}. '{t}'")
    logger.info(f"       → core: '{normalized[i]['core_title']}'")
    logger.info(f"       → fuzzy: '{normalized[i]['fuzzy']}'")

# Test fuzzy matching
logger.info("\n  Testing fuzzy matching:")
for i in range(1, len(test_titles)):
    match = fuzzy_match_titles(test_titles[0], test_titles[i], threshold=0.85)
    logger.info(f"    '{test_titles[0]}' ↔ '{test_titles[i]}': {match}")

# ===== TEST 2: BookMatcher =====
logger.info("\n✓ TEST 2: BookMatcher (Fuzzy + Author Preservation)")

# Create test dataset with duplicates
test_data = pd.DataFrame([
    {'Book Name': 'Pucking Around', 'Author Name': 'Emily Rath'},
    {'Book Name': 'Pucking Around (Jacksonville Rays #1)', 'Author Name': 'Emily Rath'},  # Should match
    {'Book Name': 'Pucking Around', 'Author Name': 'Different Author'},  # Should NOT match (different author)
    {'Book Name': 'The Deal', 'Author Name': 'Elle Kennedy'},
    {'Book Name': 'THE DEAL', 'Author Name': 'Elle Kennedy'},  # Should match
])

logger.info(f"  Test dataset: {len(test_data)} books")
matcher = BookMatcher()

# Test finding matches
search_book = {'title': 'Pucking Around (Book 1)', 'author': 'Emily Rath'}
result = matcher.find_best_match(test_data, search_book)
if result.matched:
    logger.success(f"  ✓ Found match for '{search_book['title']}' by {search_book['author']}")
    logger.info(f"    → Strategy: {result.strategy}, Confidence: {result.confidence:.2f}")
    logger.info(f"    → Matched to row {result.index}: '{test_data.iloc[result.index]['Book Name']}'")
else:
    logger.error(f"  ✗ No match found")

# Test deduplication
logger.info("\n  Testing deduplication (should preserve different authors):")
before_count = len(test_data)
deduped = matcher.deduplicate_books(test_data, preserve_different_authors=True)
after_count = len(deduped)
logger.info(f"    Before: {before_count} books")
logger.info(f"    After: {after_count} books")
logger.info(f"    Removed: {before_count - after_count} duplicates")

if after_count ==4:  # Should remove 1 (Emily Rath duplicate), keep different author
    logger.success("  ✓ PASS: Different authors preserved!")
else:
    logger.error(f"  ✗ FAIL: Expected 4 books, got {after_count}")

# ===== TEST 3: Validation =====
logger.info("\n✓ TEST 3: Data Validation")

# Find a real data file
data_file = None
for f in ['unified_book_data_enriched_ultra.csv', 'unified_book_data_enriched_mega.csv', 'unified_book_data_enriched_final.csv']:
    if os.path.exists(f):
        data_file = f
        break

if data_file:
    logger.info(f"  Validating: {data_file}")
    try:
        report = validate_file(data_file)
        logger.info(f"    Total issues: {report.total_issues}")
        logger.info(f"    Errors: {report.error_count}")
        logger.info(f"    Warnings: {report.warning_count}")
        logger.info(f"    Valid: {report.is_valid}")
        
        if report.is_valid:
            logger.success("  ✓ PASS: Dataset is valid!")
        else:
            logger.warning("  ⚠ Dataset has validation issues (see above)")
    except Exception as e:
        logger.error(f"  ✗ Validation error: {e}")
else:
    logger.warning("  ⚠ No data file found, skipping validation test")

# ===== TEST 4: Config Loading =====
logger.info("\n✓ TEST 4: Configuration")
logger.info(f"  Fuzzy title threshold: {config.FUZZY_TITLE_THRESHOLD}")
logger.info(f"  Fuzzy author threshold: {config.FUZZY_AUTHOR_THRESHOLD}")
logger.info(f"  Enable fuzzy dedup: {config.ENABLE_FUZZY_DEDUP}")
logger.info(f"  Enable auto backup: {config.ENABLE_AUTO_BACKUP}")
logger.info(f"  Preserve author duplicates: {config.PRESERVE_AUTHOR_DUPLICATES}")
logger.info(f"  Convergence threshold: {config.CONVERGENCE_THRESHOLD * 100}%")
logger.success("  ✓ Config loaded successfully!")

# ===== SUMMARY =====
logger.info("\n" + "=" * 80)
logger.success("✓ ALL INTEGRATION TESTS COMPLETE!")
logger.info("=" * 80)
logger.info("\nNext Steps:")
logger.info("  1. Run full pipeline on test data (10-20 books)")
logger.info("  2. Benchmark before/after data completeness")
logger.info("  3. Manual verification of fuzzy matches")
logger.info("")
logger.info("To test full pipeline:")
logger.info("  python3 orchestrator.py")
logger.info("=" * 80)
