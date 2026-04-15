---
name: Book Research Pipeline
description: Slim router directive for the book research workspace. Use with OUTLINE.md and the module skills.
---

# Book Research Pipeline Directive

This directive has been intentionally slimmed for token efficiency.

## Default Route

1. Read `OUTLINE.md`
2. Pick one module skill in `.agent/skills/`
3. Open only the exact script or dataset you need

## Modules

### Research & Scraping
- Main area: `subgenre-pipeline/`
- Jobs: discovery, Amazon crawling, Goodreads enrichment

### Validation & Analysis
- Main areas: `subgenre-pipeline/`, `data/`
- Jobs: Gemini enrichment, audits, ranking, final scoring

### Outreach
- Main areas: `outreach/`, `data/`, selected scripts in `execution/`
- Jobs: contacts, exports, licensing intel

## Active Truth

- Shared masters: `data/`
- Final scored outputs: `subgenre-pipeline/output/`
- Outreach-ready files: `outreach/*/exports/`

## Avoid By Default

- `_archive/`
- `.claude/worktrees/`
- `.tmp/`
- Historical report folders

## Environment Notes

- Gemini key exists in `.env`
- Codex Drive connector may be available even if local `gspread` credentials are absent
