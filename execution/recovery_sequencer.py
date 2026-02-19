#!/usr/bin/env python3
import subprocess
import sys
import logging

# Configure logging
logging.basicConfig(
    filename='recovery.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler(sys.stdout))

def run_script(script_name):
    logger.info(f"Starting {script_name}...")
    try:
        # Run script and capture output
        with open(f"logs/{script_name.replace('.py', '.log')}", "w") as log_file:
            process = subprocess.run(
                ["python3", script_name],
                check=True,
                stdout=log_file,
                stderr=subprocess.STDOUT
            )
        logger.info(f"✓ {script_name} completed successfully.")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {script_name} failing with exit code {e.returncode}")
        return False
    except Exception as e:
        logger.error(f"❌ Error running {script_name}: {e}")
        return False

def main():
    logger.info("Starting Sequential Recovery Sequence...")
    
    # 1. Targeted Enrichment
    if not run_script("targeted_metadata_enricher.py"):
        logger.error("Stopping sequence due to enrichment failure.")
        return
        
    # 2. Description Truncation
    if not run_script("description_truncator.py"):
        logger.error("Description truncation failed.")
        return
        
    logger.info("All recovery scripts completed successfully!")

if __name__ == "__main__":
    main()
