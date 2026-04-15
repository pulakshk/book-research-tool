# Ice Hockey Outreach Workspace

This folder now keeps the ice hockey outreach project in a predictable layout.

## Where To Look

- `source/`
  - Original workbook inputs.
- `reports/`
  - Short pointer only. Detailed historical reports were archived.
- `verified/`
  - Rebuilt workbook copies with fact-checked outreach data.
- `exports/`
  - CSV exports produced by the verification script.

Archived checkpoints from older long-running jobs now live in `_archive/checkpoints/`.
Archived detailed reports now live in `_archive/reports/outreach/ice-hockey/`.

## Current Key Files

- Source workbook: `source/Final self-pub scored.xlsx`
- Verified workbook: `verified/Final self-pub scored.ice-hockey-verified.xlsx`
- Verified outreach CSV: `exports/ice_hockey_outreach_verified.csv`
- Verified master CSV: `exports/ice_hockey_master_contacts_verified.csv`
- Verified author contacts CSV: `exports/ice_hockey_author_contacts_verified.csv`

## Script

- Main repair script: `../../execution/repair_ice_hockey_outreach.py`
