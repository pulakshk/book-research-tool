# Book Research Tool — LLM Router

Read this first. It is the shortest safe route through the repo.

## Default Read Order

1. `OUTLINE.md`
2. One module skill in `.agent/skills/`
3. One folder README if needed
4. The exact script or data file you need

Do not read the whole repo by default.

## Do Not Scan By Default

- `_archive/` - legacy and intermediate history
- `.claude/worktrees/` - duplicate tree from another tool
- `.tmp/` - runtime scratch, logs, backups
- `outreach/*/reports/` - historical report docs
- Large CSV/XLSX files unless the task is explicitly data-focused

## The 3 Modules

### 1. Research & Scraping
- Main folders: `subgenre-pipeline/`, `subgenre-pipeline/genre-crawl/`
- Use for: discovery, Amazon crawling, Goodreads enrichment, raw metadata
- Start with: `.agent/skills/research_and_scraping/SKILL.md`
- Common entry points:
  - `run_v2_pipeline.py`
  - `scrape_bestsellers.py`
  - `subgenre-pipeline/mega_discovery.py`
  - `subgenre-pipeline/amazon_full_crawl.py`

### 2. Validation & Analysis
- Main folders: `subgenre-pipeline/`, `data/`
- Use for: Gemini enrichment, quality checks, scoring, final ranking
- Start with: `.agent/skills/validation_and_analysis/SKILL.md`
- Common entry points:
  - `subgenre-pipeline/gemini_enrich_all.py`
  - `subgenre-pipeline/deep_enrich.py`
  - `subgenre-pipeline/final_aggregate_and_score.py`
  - `execution/generate_commissioning_report.py`

### 3. Outreach
- Main folders: `outreach/`, `data/`, selected scripts in `execution/`
- Use for: author contacts, outreach-ready exports, licensing sheets
- Start with: `.agent/skills/outreach_operations/SKILL.md`
- Common entry points:
  - `execution/repair_ice_hockey_outreach.py`
  - `execution/build_sports_romance_master.py`
  - `execution/author_email_discovery.py`
  - `execution/series_intel_warm_leads.py`

## Canonical Data And Outputs

- Shared master data:
  - `data/unified_book_data_enriched_ultra.csv`
  - `data/unified_book_data_enriched_final.csv`
  - `data/author_contacts_all_subgenres.csv`
- Multi-genre final outputs:
  - `subgenre-pipeline/output/FINAL_SELFPUB_SCORED.csv`
  - `subgenre-pipeline/output/FINAL_SELFPUB_SCORED.xlsx`
  - `subgenre-pipeline/output/subgenre_outputs/`
- Outreach outputs:
  - `outreach/ice-hockey/exports/ice_hockey_OUTREACH_READY.csv`
  - `outreach/ice-hockey/exports/ice_hockey_master_contacts_verified.csv`
  - `outreach/sports-romance/exports/Sports_Romance_Combined_Master.csv`

## Folder Rules

- Treat `data/`, `subgenre-pipeline/output/`, and `outreach/*/exports/` as the active truth.
- Treat `_archive/` as cold storage.
- Treat reports as historical context, not default inputs.
- Many `genre-crawl/` scripts are tightly coupled; inspect neighboring files before editing.

## Environment Status

- `GEMINI_API_KEY` exists in `.env` and passed a live check
- Codex Google Drive is connected for `khimesara.pulaksh@pocketfm.com`
- Local Python `gspread` OAuth files do not appear to be configured in this repo
