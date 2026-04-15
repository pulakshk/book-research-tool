# Outreach (Module 3)

All outreach workspaces for author contact discovery and licensing preparation.

## Workspaces

### ice-hockey/
Ice hockey romance outreach — the most mature dataset.
- `source/` — Original input workbook (`Final self-pub scored.xlsx`)
- `verified/` — Fact-checked workbooks (ice-hockey-verified, Amazon-repaired, outreach-ready)
- `exports/` — CSV exports: outreach-verified, master contacts, per-genre CLEANED, FINAL_OUTREACH, OUTREACH_READY
- `reports/` — Short index only; detailed historical reports were archived

Some historical reports still mention the pre-reorg folder names. Treat the current `outreach/` paths as canonical.

Key exports:
- `ice_hockey_OUTREACH_READY.csv` — Ready for GMass/outreach
- `ice_hockey_master_contacts_verified.csv` — All contacts verified
- `amazon_hockey_cleaned_titles_repaired.csv` — Amazon working file repaired

### sports-romance/
Sports romance outreach (includes ice hockey + KU expansion).
- `source/` — KU enriched data, email discovery caches
- `exports/` — `Sports_Romance_Combined_Master.csv/xlsx` (649 rows)
- `reports/` — Short index only; detailed build reports were archived

### sheets/
Cross-genre licensing and intel workbooks.
- `Sports Romance_ Outreach.xlsx` — Main outreach workbook with contact columns
- `All-Genre Licensing Tracker.xlsx` — Cross-genre licensing tracker
- `licensing warm leads analysis.xlsx` — Warm leads with 6-criteria analysis
- `Series_Intel.xlsx` / `RD_Series_Intel.xlsx` / `Warm_Leads_Series_Intel.xlsx` — AI-enriched series intelligence
- `docs/` — Short index only; detailed work logs were archived

## Key Scripts (in execution/)
- `repair_ice_hockey_outreach.py` — Ice hockey data repair
- `build_combined_sports_master.py` — Combined sports master builder
- `author_email_discovery.py` — Email discovery (web scraping)
- `sports_outreach_enrich_contacts.py` — Contact enrichment for outreach workbook
- `series_intel_warm_leads.py` — Series intel for warm leads
