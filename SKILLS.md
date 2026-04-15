# Skills Reference

Use this file as a task chooser, not a full manual.

## Fast Rule

Do not read every guide. Read:

1. `OUTLINE.md`
2. One module skill
3. One target script

## Choose By Task

| If the task is... | Read this first | Typical files |
|---|---|---|
| Discover new books or genres | `.agent/skills/research_and_scraping/SKILL.md` | `run_v2_pipeline.py`, `subgenre-pipeline/`, `scrape_bestsellers.py` |
| Enrich, validate, or score books | `.agent/skills/validation_and_analysis/SKILL.md` | `subgenre-pipeline/`, `data/`, `execution/generate_commissioning_report.py` |
| Find contacts or prep outreach | `.agent/skills/outreach_operations/SKILL.md` | `outreach/`, `execution/author_email_discovery.py`, `execution/series_intel_warm_leads.py` |
| The task spans multiple modules | `.agent/skills/book_research_pipeline/SKILL.md` | `OUTLINE.md` plus one module skill |

## Module Entry Points

### Research & Scraping
- `run_v2_pipeline.py`
- `scrape_bestsellers.py`
- `subgenre-pipeline/mega_discovery.py`
- `subgenre-pipeline/amazon_full_crawl.py`
- `subgenre-pipeline/enrich_with_goodreads.py`

### Validation & Analysis
- `subgenre-pipeline/gemini_enrich_all.py`
- `subgenre-pipeline/deep_enrich.py`
- `subgenre-pipeline/final_aggregate_and_score.py`
- `subgenre-pipeline/quality_audit.py`
- `execution/generate_commissioning_report.py`

### Outreach
- `execution/repair_ice_hockey_outreach.py`
- `execution/build_sports_romance_master.py`
- `execution/author_email_discovery.py`
- `execution/batch_email_discovery.py`
- `execution/series_intel_warm_leads.py`

## Canonical Active Files

- Shared masters:
  - `data/unified_book_data_enriched_ultra.csv`
  - `data/author_contacts_all_subgenres.csv`
- Final multi-genre outputs:
  - `subgenre-pipeline/output/FINAL_SELFPUB_SCORED.csv`
  - `subgenre-pipeline/output/subgenre_outputs/`
- Outreach outputs:
  - `outreach/ice-hockey/exports/ice_hockey_OUTREACH_READY.csv`
  - `outreach/sports-romance/exports/Sports_Romance_Combined_Master.csv`

## Avoid By Default

- `_archive/`
- `.claude/worktrees/`
- `.tmp/`
- Historical report folders
- Large CSV/XLSX files unless the task needs them

## Important Distinction

- Codex Google Drive connector is connected
- Local Python `gspread` OAuth is not obviously configured in the repo
