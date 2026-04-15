#!/usr/bin/env python3
"""
RUN ALL PARALLEL
Launches the pipeline for the 8 remaining subgenres simultaneously.
Note: This will launch multiple background processes.
"""

import subprocess
import sys
import os
import time

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

SUBGENRES = [
    "Historic Fiction & Romance",
    "Military Drama/Romance",
    "Small Town Drama/Romance",
    "Christian Drama/Romance",
    "Mafia Drama/Romance",
    "Dark Romance",
    "Forbidden Romance",
    "Romantic Suspense / Psychological Thriller"
]

def main():
    print(f"Launching pipeline for {len(SUBGENRES)} remaining subgenres in parallel...")
    processes = []
    
    pipeline_script = os.path.join(SCRIPT_DIR, "run_genre_pipeline.py")
    
    for genre in SUBGENRES:
        print(f"Starting background process for: {genre}")
        # Build command: python3 run_genre_pipeline.py --genre "..." 
        cmd = [sys.executable, pipeline_script, "--genre", genre]
        
        # Open standard output and error to independent logs to avoid terminal spam
        log_file = os.path.join(SCRIPT_DIR, f"{genre.replace('/', '_').replace(' ', '_')}_pipeline.log")
        with open(log_file, 'w') as f:
            p = subprocess.Popen(cmd, stdout=f, stderr=subprocess.STDOUT)
            processes.append((genre, p))
            
        # Slight delay to avoid hammering the OS all on the exact same millisecond
        time.sleep(2)
        
    print("All processes launched!")
    print("Waiting for them to complete... You can check the individual .log files for progress.")
    
    for genre, p in processes:
        p.wait()
        if p.returncode == 0:
            print(f"✅ SUCCESS: {genre}")
        else:
            print(f"❌ FAILED: {genre} (Exit code {p.returncode})")
            
    print("All parallel pipelines finished.")

if __name__ == "__main__":
    main()
