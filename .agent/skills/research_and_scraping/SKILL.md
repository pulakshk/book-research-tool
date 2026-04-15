---
name: Research And Scraping
description: Use for discovery, crawling, and raw metadata collection across Amazon, Goodreads, and multi-platform research.
---

# Research And Scraping

Use this skill for module 1 work.

## Primary Folders

- `subgenre-pipeline/`
- `subgenre-pipeline/genre-crawl/`
- `execution/`
- `data/genre_expansion/`

## Best Entry Points

- `run_v2_pipeline.py`
- `scrape_bestsellers.py`
- `subgenre-pipeline/mega_discovery.py`
- `subgenre-pipeline/multi_platform_discovery.py`
- `subgenre-pipeline/amazon_top100.py`
- `subgenre-pipeline/amazon_full_crawl.py`
- `subgenre-pipeline/enrich_with_goodreads.py`

## Start Here

1. Read `OUTLINE.md`
2. Check `subgenre-pipeline/README.md`
3. Use `SKILLS.md` for command examples

## Important Outputs

- `subgenre-pipeline/output/amazon_full_crawl_raw.csv`
- `subgenre-pipeline/output/amazon_top100_raw.csv`
- `subgenre-pipeline/genre-crawl/All_9_Subgenres_Scout_Top25_AGGREGATED.csv`
- `data/genre_expansion/all_genres_expanded_master_v2.csv`

## Guardrails

- Do not treat `_archive/genre-crawl-intermediates/` as active input.
- Prefer the renamed `subgenre-pipeline/` paths over old folder names.
- Many `genre-crawl/` scripts are tightly coupled and use neighboring files directly.
