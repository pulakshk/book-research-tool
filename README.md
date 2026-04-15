# Book Research Tool

Pipeline workspace for three connected jobs:

1. Book research and scraping
2. Validation and subjective/objective analysis
3. Outreach and licensing prep

## Start Here

- LLM path: `OUTLINE.md` -> one module skill in `.agent/skills/` -> one target script/file
- Human path: `README.md` -> `OUTLINE.md` -> relevant folder README
- Ignore `_archive/` unless you explicitly need legacy or intermediate files.

## Current Layout

- `subgenre-pipeline/` - Multi-genre discovery, enrichment, scoring, and final outputs
- `execution/` - Reusable scripts, repairs, utilities, and one-off operational jobs
- `outreach/` - Ice hockey, sports romance, and licensing-sheet workspaces
- `data/` - Canonical shared datasets and master CSVs
- `reference/` - Reference workbooks and learnings
- `directives/` - SOP-style instructions and agent guidance
- `_archive/` - Legacy, intermediate, and archived artifacts
- `.tmp/` - Runtime scratch area some scripts still recreate for logs and backups

## Quick Commands

```bash
# Original pipeline
python3 main.py --all

# Multi-genre pipeline
python3 run_v2_pipeline.py

# Final scoring output
python3 subgenre-pipeline/final_aggregate_and_score.py
```

## Environment Notes

- `GEMINI_API_KEY` is present in `.env` and passed a live Gemini check.
- Codex Google Drive is connected for `khimesara.pulaksh@pocketfm.com`.
- Local Python Google Sheets OAuth for `gspread` does not appear to be configured in this repo yet.

## Reorg Notes

- High-noise intermediates were moved into `_archive/`.
- Historical reports were moved out of active folders.
- Active Python files no longer reference the old folder names.
- The project is organized by module in docs, while keeping script paths stable enough to avoid breaking the working code.
- `.ignore` is configured so common search tools skip archive, worktrees, runtime scratch, and archived report docs.
