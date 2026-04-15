# Book Research Tool — Business Overview

This document is for the business team. It explains what this tool does, why it exists, and how to read its outputs — without requiring any technical knowledge.

---

## What This Tool Does

The Book Research Tool is an automated pipeline that helps the content licensing team:

1. **Discover** high-potential self-published romance books and series across multiple subgenres
2. **Score and rank** them using AI-based analysis (story quality, tropes, series depth, audience signals)
3. **Find author contact information** and prepare outreach-ready sheets for licensing conversations

It replaces hours of manual Amazon/Goodreads research with a repeatable, data-driven process. The output is a scored, contact-enriched list of books ready for the licensing team to act on.

---

## The Three Stages

### Stage 1 — Research & Discovery

The tool crawls Amazon bestseller lists and Goodreads ratings across subgenres like:
- Ice Hockey Romance, Sports Romance
- Dark Romance, Mafia Romance, Forbidden Romance
- Historical Romance, Small Town Romance, Military Romance
- Christian Romance, Political Romance, and more

For each subgenre it pulls: book titles, series names, author names, Amazon rankings, Goodreads ratings and review counts, publication dates, and Kindle Unlimited availability.

**Output:** Raw book lists per subgenre, deduplicated and combined into a master dataset.

---

### Stage 2 — Validation & Scoring

Once raw data is collected, an AI layer (Google Gemini) enriches each title with:
- Series completion status and total book count
- Story synopsis and key tropes
- Qualitative signals: writing quality, audience engagement, licensing viability
- A composite score that ranks books against each other within and across subgenres

Books are filtered for quality — removing duplicates, incomplete data, and low-signal titles — and then ranked into a final scored output.

**Output:** `FINAL_SELFPUB_SCORED.xlsx` — a scored, ranked list of the best self-pub titles by subgenre.

---

### Stage 3 — Outreach Preparation

For the top-ranked titles, the tool attempts to find:
- Author contact emails (via website scraping and web search)
- Social media profiles
- Agent or publisher information where available

This data is combined with the scoring data into outreach-ready exports that can be loaded directly into GMass or other email tools.

**Output:**
- `outreach/ice-hockey/exports/ice_hockey_OUTREACH_READY.csv` — ready for email outreach
- `outreach/sports-romance/exports/Sports_Romance_Combined_Master.csv` — 649 titles with contacts
- `outreach/sheets/` — licensing tracker workbooks and warm leads analysis

---

## Key Output Files (Where to Look)

| What you need | Where to find it |
|---|---|
| Final ranked list across all subgenres | `subgenre-pipeline/output/FINAL_SELFPUB_SCORED.xlsx` |
| Ice hockey romance outreach list | `outreach/ice-hockey/exports/ice_hockey_OUTREACH_READY.csv` |
| Sports romance combined master | `outreach/sports-romance/exports/Sports_Romance_Combined_Master.csv` |
| All-genre licensing tracker | `outreach/sheets/All-Genre Licensing Tracker.xlsx` |
| Warm leads with scoring analysis | `outreach/sheets/licensing warm leads analysis.xlsx` |
| Series intelligence (AI-enriched) | `outreach/sheets/Series_Intel.xlsx` |

---

## How a Full Research Cycle Works

```
Amazon/Goodreads
      ↓
  Discovery
  (crawl bestseller lists, pull metadata)
      ↓
  Enrichment
  (Gemini AI adds tropes, synopses, series data)
      ↓
  Scoring
  (composite rank: ratings × reviews × series depth × KU availability)
      ↓
  Contact Discovery
  (scrape author websites, run web searches for emails)
      ↓
  Outreach-Ready Export
  (CSV/XLSX loaded into email tool or licensing tracker)
```

Each stage builds on the previous one. The team can also run individual stages independently — for example, just refreshing email contacts for an existing scored list.

---

## Coverage Today

- **Subgenres tracked:** 9+ romance subgenres (see list in Stage 1)
- **Current sports romance dataset:** ~649 titles with contacts
- **Ice hockey dataset:** Verified, Amazon-repaired, outreach-ready
- **Cross-genre licensing tracker:** Active in `outreach/sheets/`

---

## Who Runs This

The tool is run by the content/licensing team with AI assistance (Claude or Gemini as the orchestration layer). It does not require manual coding to operate — the AI reads the SOPs and runs the right scripts. New subgenres or research tasks can be added by writing a new directive in plain English.

---

## Limitations to Know

- **Email discovery is best-effort.** Not every author has a public email. The tool finds what is publicly available; the team may need to supplement with LinkedIn or direct outreach.
- **Amazon/Goodreads data reflects a point in time.** Rankings shift. Re-run discovery periodically to keep the dataset fresh.
- **Scores are relative, not absolute.** A score of 85 means "top of its subgenre," not a universal quality rating. Use scores to prioritize, not to filter out books entirely.
- **KU availability changes.** Authors move in and out of Kindle Unlimited. Verify current status before finalizing licensing terms.
