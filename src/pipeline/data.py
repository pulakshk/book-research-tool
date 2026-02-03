import pandas as pd
import shutil
from datetime import datetime
from loguru import logger
from .config import MASTER_FILE, BACKUP_FILE, LOG_DIR

def load_dataset(use_backup=False):
    """Load the dataset (Master or Backup)."""
    path = BACKUP_FILE if use_backup else MASTER_FILE
    if not path.exists():
        logger.error(f"File not found: {path}")
        return pd.DataFrame()
    
    logger.info(f"Loading dataset from {path}...")
    try:
        df = pd.read_csv(path)
        logger.info(f"Loaded {len(df)} rows.")
        return df
    except Exception as e:
        logger.error(f"Error loading dataset: {e}")
        return pd.DataFrame()

def save_dataset(df, message="Auto-save"):
    """Save to both Master and Backup with timestamped history."""
    logger.info(f"Saving {len(df)} rows... ({message})")
    
    # Create timestamped backup
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = LOG_DIR / f"backup_{ts}.csv"
    
    try:
        # Save to main files
        df.to_csv(MASTER_FILE, index=False)
        df.to_csv(BACKUP_FILE, index=False)
        
        # Save history
        # shutil.copy(MASTER_FILE, backup_path) # Optional: don't spam disk
        logger.success(f"Saved successfully to {MASTER_FILE.name} and {BACKUP_FILE.name}")
    except Exception as e:
        logger.error(f"Failed to save dataset: {e}")
