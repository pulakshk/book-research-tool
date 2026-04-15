---
name: Validation And Analysis
description: Use for Gemini enrichment, data validation, audits, ranking, and final scoring.
---

# Validation And Analysis

Use this skill for module 2 work.

## Primary Folders

- `subgenre-pipeline/`
- `subgenre-pipeline/output/`
- `data/`
- `execution/`

## Best Entry Points

- `subgenre-pipeline/gemini_enrich_all.py`
- `subgenre-pipeline/deep_enrich.py`
- `subgenre-pipeline/gemini_series_check.py`
- `subgenre-pipeline/gemini_fast_verify.py`
- `subgenre-pipeline/final_aggregate_and_score.py`
- `subgenre-pipeline/quality_audit.py`
- `subgenre-pipeline/cleanup_and_validate.py`
- `execution/generate_commissioning_report.py`

## Canonical Outputs

- `subgenre-pipeline/output/FINAL_SELFPUB_SCORED.csv`
- `subgenre-pipeline/output/FINAL_SELFPUB_SCORED.xlsx`
- `subgenre-pipeline/output/subgenre_outputs/`
- `data/unified_book_data_enriched_ultra.csv`
- `data/unified_book_data_enriched_final.csv`

## Guardrails

- Use final outputs, not archived intermediates like `PRIORITY_SELFPUB_ENRICHED.csv`.
- Distinguish objective validation from subjective Gemini analysis.
- If you need the score logic, read `OUTLINE.md` first, then inspect `final_aggregate_and_score.py`.
