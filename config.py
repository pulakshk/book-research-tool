#!/usr/bin/env python3
"""
Configuration Settings for Book Research Tool
Centralized configuration for all scripts
"""

# ============================================================================
# FILE PATHS
# ============================================================================

# Primary data file (gets updated by pipeline)
MASTER_FILE = "unified_book_data_enriched_ultra.csv"

# Alternative file names for different pipeline stages
ENRICHMENT_STAGES = {
    'raw': 'unified_book_data_raw.csv',
    'cleaned': 'unified_book_data_cleaned.csv',
    'enriched': 'unified_book_data_enriched.csv',
    'aligned': 'unified_book_data_enriched_aligned.csv',
    'mega': 'unified_book_data_enriched_mega.csv',
    'ultra': 'unified_book_data_enriched_ultra.csv',
    'final': 'unified_book_data_enriched_final.csv'
}

# Backup directory
BACKUP_DIR = "backups/auto"

# Data directory
DATA_DIR = "data"

# Log directory  
LOG_DIR = "logs"

# ============================================================================
# MATCHING THRESHOLDS
# ============================================================================

# Fuzzy matching threshold for titles (0.0 to 1.0)
# Lower = more permissive (more matches, more false positives)
# Higher = stricter (fewer matches, more false negatives)
FUZZY_TITLE_THRESHOLD = 0.85

# Fuzzy matching threshold for authors (0.0 to 1.0)
# Higher than title since author names are typically shorter
FUZZY_AUTHOR_THRESHOLD = 0.90

# Core title matching threshold (for subtitle variations)
CORE_TITLE_THRESHOLD = 0.90

# Series name matching threshold
SERIES_NAME_THRESHOLD = 0.80

# ============================================================================
# SCRAPING SETTINGS
# ============================================================================

# Maximum number of retries for failed requests
MAX_RETRIES = 3

# Number of concurrent workers for parallel scraping
WORKER_CONCURRENCY = {
    'ultra_recovery': 4,      # Amazon/Goodreads recovery
    'series_enrichment': 1,   # Series bulk scraping
    'detailed_scrape': 6,     # Deep metadata scraping
    'series_exhaustion': 1    # Series completion
}

# How often to rotate browser contexts (per worker)
CONTEXT_ROTATION_INTERVAL = 20  # Every N books

# Random sleep range between requests (seconds) 
SLEEP_MIN = 2
SLEEP_MAX = 7

# Timeout for page loads (milliseconds)
PAGE_LOAD_TIMEOUT = 45000

# Run scrapers in headless mode?
HEADLESS_MODE = True

# ============================================================================
# DATA VALIDATION RULES
# ============================================================================

# Required fields (must not be null)
REQUIRED_FIELDS = [
    'Book Name',
    'Primary Subgenre'
]

# Recommended fields (should be filled but not critical)
RECOMMENDED_FIELDS = [
    'Author Name',
    'Series Name',
    'Goodreads Link',
    'Amazon Link'
]

# Fields that should be numeric
NUMERIC_FIELDS = [
    'Book Number',
    'Total Books in Series',
    'Goodreads # of Ratings',
    'Goodreads Rating',
    'Amazon # of Ratings',
    'Amazon Rating',
    'Pages'
]

# Valid rating ranges
RATING_RANGES = {
    'Goodreads Rating': (0.0, 5.0),
    'Amazon Rating': (0.0, 5.0)
}

# Fields that should be URLs
URL_FIELDS = [
    'Goodreads Link',
    'Amazon Link'
]

# URL validation patterns
URL_PATTERNS = {
    'Goodreads Link': r'https?://(?:www\.)?goodreads\.com/book/show/\d+',
    'Amazon Link': r'https?://(?:www\.)?amazon\.com/[^/]+/dp/[A-Z0-9]+'
}

# ============================================================================
# ORCHESTRATOR SETTINGS
# ============================================================================

# Maximum iterations for orchestration loop
MAX_ORCHESTRATION_ITERATIONS = 3

# Gap thresholds to trigger specific phases (as percentage 0.0-1.0)
PHASE_TRIGGERS = {
    'recovery': {
        'author': 0.05,      # Trigger if >5% missing authors
        'amz_link': 0.05,    # Trigger if >5% missing Amazon links
        'amz_rating': 0.10   # Trigger if >10% missing Amazon ratings
    },
    'series_bulk': {
        'gr_link': 0.10,     # Trigger if >10% missing Goodreads links
        'series': 0.10,      # Trigger if >10% missing series info
        'gr_rating': 0.10    # Trigger if >10% missing Goodreads ratings
    },
    'detailed_scrape': {
        'description': 0.10,  # Trigger if >10% missing descriptions
        'pages': 0.10,        # Trigger if >10% missing page counts
        'publisher': 0.10     # Trigger if >10% missing publishers
    }
}

# Convergence threshold (stop when all gaps below this %)
CONVERGENCE_THRESHOLD = 0.02  # 2%

# Cooldown between orchestration loops (seconds)
ORCHESTRATION_COOLDOWN = 30

# ============================================================================
# DATASET MANAGER SETTINGS
# ============================================================================

# Master schema - enforced column order
MASTER_SCHEMA_COLUMNS = [
    'Series Name',
    'Author Name',
    'Book Name',
    'Book Number',
    'Total Books in Series',
    'Goodreads Link',
    'Goodreads # of Ratings',
    'Goodreads Rating',
    'First Published',
    'Original Published',
    'Pages',
    'Description',
    'Primary Trope',
    'Primary Subgenre',
    'Series Status',
    'Amazon Link',
    'Amazon # of Ratings',
    'Amazon Rating',
    'Publisher',
    'Self Pub flag',
    'Short Synopsis',
    'Publication Date',
    'Top Lists',
    'Featured List'
]

# Values to treat as null/missing
NULL_PLACEHOLDERS = [
    '0.0', '0', 'nan', 'NAN', 'None', 'null', 'unknown', 
    'Unknown Publisher', 'N/A', 'n/a'
]

# ============================================================================
# BACKUP & RECOVERY SETTINGS
# ============================================================================

# Create automatic backup before each major operation?
AUTO_BACKUP = True

# Backup file naming format (uses strftime)
BACKUP_TIMESTAMP_FORMAT = "%Y%m%d_%H%M%S"

# Maximum number of backups to keep (oldest deleted first)
MAX_BACKUPS = 50

# ============================================================================
# LOGGING SETTINGS
# ============================================================================

# Logging level for console output
# Options: 'DEBUG', 'INFO', 'SUCCESS', 'WARNING', 'ERROR', 'CRITICAL'
LOG_LEVEL = 'INFO'

# Log format
LOG_FORMAT = "{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}"

# Enable logging to file?
LOG_TO_FILE = True

# Log rotation settings
LOG_ROTATION = "10 MB"  # Rotate when file reaches this size
LOG_RETENTION = "30 days"  # Keep logs for this long

# ============================================================================
# SCRAPER LIBRARY PREFERENCES
# ============================================================================

# After benchmarking, choose preferred library for each task
# Options: 'playwright', 'drissionpage', 'selenium_uc'
PREFERRED_SCRAPER = {
    'amazon': 'playwright',  # Will benchmark and update
    'goodreads': 'playwright',  # Will benchmark and update
}

# User agents for rotation
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
]

# ============================================================================
# FEATURE FLAGS
# ============================================================================

# Enable experimental features
ENABLE_FUZZY_MATCHING = True
ENABLE_SERIES_EXHAUSTION = True
ENABLE_VALIDATION_PHASE = True
ENABLE_INCREMENTAL_PROCESSING = True

# Preserve duplicate titles with different authors?
PRESERVE_AUTHOR_DUPLICATES = True  # User requirement!

# NEW: Enable fuzzy deduplication in dataset_manager beautify()?
ENABLE_FUZZY_DEDUP = True

# NEW: Create automatic backup before beautify()?
ENABLE_AUTO_BACKUP = True

# ============================================================================
# PERFORMANCE TUNING
# ============================================================================

# Save progress every N books (during scraping)
SAVE_INTERVAL = 5

# Batch size for bulk operations
BATCH_SIZE = 100

# ============================================================================
# TESTING & DEBUG
# ============================================================================

# Test mode (use small subset of data)
TEST_MODE = False

# Test data size
TEST_DATA_SIZE = 100

# Verbose logging for debugging?
VERBOSE = False

# Dry run mode (don't actually modify data)
DRY_RUN = False
