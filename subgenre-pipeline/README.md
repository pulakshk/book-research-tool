# Subgenre Pipeline (Modules 1 & 2)

Multi-subgenre analysis pipeline covering 9 romance subgenres, 10,544 series.

## Scripts — Research & Discovery

| Script | Purpose | Runtime |
|---|---|---|
| `mega_discovery.py` | Multi-platform discovery (Goodreads, Google, Reddit, BookTok) | ~2h |
| `multi_platform_discovery.py` | Additional discovery sources | ~1h |
| `selfpub_expansion_pipeline.py` | Self-pub focused expansion | ~3h |
| `amazon_top100.py` | Amazon Top 100 across 26 categories | ~30min |
| `amazon_full_crawl.py` | Full Amazon crawl (37 categories, paid+free) | ~45min |
| `enrich_with_goodreads.py` | Scrape GR ratings, pages, pub dates | ~4h |

## Scripts — Enrichment & Analysis

| Script | Purpose | Runtime |
|---|---|---|
| `gemini_enrich_all.py` | 3-phase AI enrichment (metadata, validation, contacts) | ~7.5h |
| `deep_enrich.py` | Fill remaining gaps (book details, ratings, contacts) | ~10h |
| `gemini_series_check.py` | Verify discovered titles are real series | ~1h |
| `gemini_fast_verify.py` | Fast Gemini verification | ~1h |
| `final_aggregate_and_score.py` | Score, rank, deduplicate, generate Excel | ~2min |
| `quality_audit.py` | Data completeness audit | instant |
| `cleanup_and_validate.py` | Data quality checks | instant |
| `merge_and_filter.py` | Merge multiple data sources | instant |
| `series_verification.py` | Series data verification | ~1h |
| `reenrich_failed.py` | Re-enrich failed entries | varies |

## Subdirectories

- `genre-crawl/` — Genre-specific crawl scripts (18 scripts, tightly coupled via internal imports)
- `output/` — Final outputs only (historical audits archived)
- `source-data/` — Reference xlsx, licensing info docs

Historical scout summary docs were moved to `_archive/reports/subgenre-pipeline/genre-crawl/`.

## Output Files

- `output/FINAL_SELFPUB_SCORED.csv` — 10,544 rows, 49 columns
- `output/FINAL_SELFPUB_SCORED.xlsx` — Excel: Combined + 9 genre sheets + Amazon Top 100
- `output/subgenre_outputs/` — 9 per-genre master CSVs
- `output/amazon_full_crawl_raw.csv` — Raw Amazon bestseller data
- `output/amazon_top100_raw.csv` — Amazon Top 100 raw data

Detailed structural audit notes were moved to `_archive/reports/subgenre-pipeline/output/`.

## Gemini Enrichment Phases

1. **Metadata** (batch 50): Synopsis, tropes, era, pages, differentiator
2. **Validation** (batch 30): NYT, Amazon, BookTok, GR Choice cross-reference
3. **Contacts** (batch 40): Email, social, agents (deduped by author)
