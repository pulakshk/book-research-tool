import os
from pathlib import Path

# Paths
BASE_DIR = Path(os.getcwd())
DATA_DIR = BASE_DIR
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

# Secrets
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# Files
MASTER_FILE = DATA_DIR / "unified_book_data_enriched_final.csv"
BACKUP_FILE = DATA_DIR / "unified_book_data_enriched_ultra.csv"
ANALYSIS_FILE = DATA_DIR / "series_commissioning_analysis.csv"

# Columns
COLS_REQUIRED = [
    'Series Name', 'Book Name', 'Author Name', 'Book Number', 
    'Amazon Rating', 'Amazon # of Ratings', 'Publication Date',
    'Primary Subgenre', 'Primary Trope', 'Self Pub Flag'
]

# Scoring Weights (Commissioning)
WEIGHTS = {
    'volume': 0.30,
    'quality_first': 0.15,
    'quality_avg': 0.10,
    'retention': 0.25,
    'appeal': 0.20
}

# Concurrency
MAX_WORKERS = 5
GEMINI_BATCH_SIZE = 10

# Metrics
WORDS_PER_PAGE = 300
WORDS_PER_HOUR = 10000
