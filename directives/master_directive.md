# Book Research Tool: Master Directive

This directive outlines how to use the deterministic tools in `execution/` to fulfill book research tasks.

## The 3-Layer Workflow

1.  **Directives (You are here)**: Look into `directives/` for SOPs (Standard Operating Procedures).
2.  **Orchestration (决策层)**: Use `main.py` as the primary entry point for high-level pipeline tasks.
3.  **Execution (执行层)**: Call scripts in `execution/` directly for specific, lower-level operations.

## Key Tools

### Orchestration
- `python main.py --all`: Runs the full pipeline (Scrape -> Clean -> Enrich -> Analyze).
- `python main.py --clean`: Runs deduplication and filtering.
- `python main.py --scrape`: Runs series exhaustion (Phase 0).
- `python main.py --enrich`: Runs Gemini-based qualitative enrichment (Phase 2).
- `python main.py --analyze`: Generates the commissioning report (Phase 3).

### Execution Modules
- `execution/core/`: Internal logic for matching, filtering, and validation.
- `execution/utils/`: Helper utilities.
- `execution/extractors/`: Pattern matchers for Amazon and Goodreads.
- `execution/pipeline/`: The core pipeline stages.

### Specific Scripts
- `execution/series_exhaustion.py`: Use this to find missing books in a series.
- `execution/ultra_recovery.py`: Use this for deep metadata recovery from Amazon/Goodreads.
- `execution/gemini_series_enrichment.py`: Use this for LLM-driven qualitative analysis (Synopses, Tropes).

## Data Management
- **Primary Data**: Located in `data/`. Master file is `unified_book_data_enriched_ultra.csv`.
- **Subgenre Final Outputs**: Located in `subgenre-pipeline/output/`.
- **Outreach Workspaces**: Located in `outreach/`.
- **Runtime Temp/Logs**: Some scripts still use `.tmp/` for scratch files and backups.
- **Archived Intermediates/Legacy Files**: Located in `_archive/`.

---
*Refer to `directives/AGENTS.md` for broader architectural principles.*
