---
name: Book Research Pipeline
description: Router skill for the full book research workspace. Use when a task spans research, analysis, and outreach.
---

# Book Research Pipeline

Use this skill when the task cuts across the full project and you need to route between modules.

## Read Order

1. `OUTLINE.md` - quickest full-project context
2. One module skill in `.agent/skills/`
3. `SKILLS.md` only if you still need a task chooser

## The 3 Modules

### 1. Research and Scraping
- Main areas: `subgenre-pipeline/`, selected scripts in `execution/`
- Jobs: discovery, Amazon crawling, Goodreads enrichment, raw metadata collection
- Best skill: `.agent/skills/research_and_scraping/SKILL.md`

### 2. Validation and Analysis
- Main areas: `subgenre-pipeline/`, `data/`, selected scripts in `execution/`
- Jobs: Gemini enrichment, audits, cleanup, ranking, final scoring
- Best skill: `.agent/skills/validation_and_analysis/SKILL.md`

### 3. Outreach Operations
- Main areas: `outreach/`, `data/`, outreach scripts in `execution/`
- Jobs: contact discovery, workbook enrichment, final outreach exports, licensing intel
- Best skill: `.agent/skills/outreach_operations/SKILL.md`

## Repo Rules

- Treat `_archive/` as cold storage unless the user asks for legacy context.
- Treat `.claude/worktrees/` and `.tmp/` as noise unless the task is explicitly about tool state or runtime logs.
- Prefer active masters in `data/`, `subgenre-pipeline/output/`, and `outreach/*/exports/`.
- Keep script paths stable; use docs and skills to explain the three-module split.
- Distinguish between Codex connector access and local script credentials.

## Key Status

- Google Drive connector in Codex: connected for `khimesara.pulaksh@pocketfm.com`
- Local `gspread` OAuth files: not found in repo
- Gemini key: present in `.env`
