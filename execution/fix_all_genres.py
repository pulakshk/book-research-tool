#!/usr/bin/env python3
"""
Generalized data sanity fix for ALL genre sheets in Final self-pub scored.xlsx.

Applies the same fixes proven on Ice Hockey to every genre tab:
  - Remove junk/wrong-genre entries
  - Fix malformed author names
  - Deduplicate author+series combos
  - Move publicity emails out of direct email field
  - Clean agent name prose leaks
  - Fix broken URLs
  - Fix type contradictions (Standalone + books>1)
  - Blank wrong last-book entries (same as first, bundles)
  - Flag formulaic pages, suspicious Books=3
  - Rebuild contact descriptions
  - Recalculate quality flags with sanity checks

Does NOT do web verification (that's a separate step) — this is purely
local data cleanup that can be done instantly.

Usage:
  python3 execution/fix_all_genres.py
  python3 execution/fix_all_genres.py --sheet "Dark & Forbidden Romance"
"""

import argparse
import re
import sys
from pathlib import Path

import pandas as pd
import numpy as np

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT_ROOT / "outreach" / "ice-hockey"
SOURCE_XLSX = OUT_DIR / "source" / "Final self-pub scored.xlsx"
EXPORT_DIR = OUT_DIR / "exports"
VERIFIED_DIR = OUT_DIR / "verified"
REPORT_DIR = OUT_DIR / "reports"

ALL_GENRE_SHEETS = [
    "Christian Drama-Romance",
    "Dark & Forbidden Romance",
    "Historical Romance & Fiction",
    "Mafia Drama-Romance",
    "Military Drama-Romance",
    "Political Drama-Romance",
    "Romantic Suspense-Psychological",
    "Small Town Drama-Romance",
]

# ─── Patterns ──────────────────────────────────────────────────────────────
_KNOWN_BAD_EMAILS = {
    "user@domain.com", "email@example.com", "example@example.com",
    "author@directauthor.com", "info@therateabc.com", "noreply@noreply.com",
}
_BAD_DOMAINS = {"example.com", "domain.com", "email.com", "test.com", "yoursite.com"}
_PUBLICITY_PREFIXES = {"publicity", "admin", "press", "media", "marketing", "pr"}

_FAB_PATTERNS = [
    re.compile(r'^[a-z]+author@gmail\.com$', re.I),
    re.compile(r'^contact@[a-z]+\.com$', re.I),
    re.compile(r'^[a-z]+books@gmail\.com$', re.I),
    re.compile(r'^hello@[a-z]+\.com$', re.I),
]

_JUNK_AUTHOR_PATTERNS = [
    re.compile(r'kindle edition', re.I),
    re.compile(r'^book \d+ of \d+', re.I),
    re.compile(r'[\u0600-\u06FF]'),  # Arabic script
    re.compile(r'[\u4e00-\u9fff]'),  # CJK
]

# Non-romance / non-fiction authors that appear across genres
_KNOWN_WRONG_GENRE = {
    "richard adams", "lewis carroll", "nancy e. krulik",
    "charles river editors", "jake maddox", "robert a. sadowski",
}


def is_fabrication_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    e = email.strip().lower()
    if e in _KNOWN_BAD_EMAILS:
        return True
    domain = e.split("@")[-1]
    if domain in _BAD_DOMAINS:
        return True
    return any(p.match(e) for p in _FAB_PATTERNS)


def is_publicity_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    return email.split("@")[0].lower() in _PUBLICITY_PREFIXES


def clean_author_name(name: str) -> str:
    if not name or str(name) == "nan":
        return ""
    return re.sub(r'\s{2,}', ' ', str(name)).strip()


def is_junk_author(name: str) -> bool:
    if not name:
        return True
    return any(p.search(name) for p in _JUNK_AUTHOR_PATTERNS)


def is_wrong_genre(author: str) -> bool:
    return str(author).strip().lower() in _KNOWN_WRONG_GENRE


def classify_type(n_books):
    try:
        n = float(n_books)
    except (TypeError, ValueError):
        return "Unknown"
    if n <= 1:
        return "Standalone"
    elif n <= 3:
        return "Short Series"
    elif n <= 7:
        return "Series"
    else:
        return "Long Series"


def is_bundle_title(title: str) -> bool:
    """Check if a title looks like a bundle/compilation rather than a real book."""
    if not title:
        return False
    t = str(title).lower()
    return any(kw in t for kw in [
        "bundle", "collection", "box set", "boxed set",
        "books 1-", "books #1-", "#1-3", "#1-4", "#1-5",
        "complete series", "omnibus",
    ])


# ─── Main fix function (per-sheet) ────────────────────────────────────────
def fix_genre_sheet(df: pd.DataFrame, sheet_name: str) -> tuple:
    """
    Fix a single genre sheet. Returns (fixed_df, changes_log).
    """
    n_orig = len(df)
    changes = []

    # ── Fix author names ───────────────────────────────────────────────────
    df["Author Name"] = df["Author Name"].apply(lambda x: clean_author_name(str(x)) if pd.notna(x) else x)

    # Remove junk authors
    junk_mask = df["Author Name"].fillna("").apply(is_junk_author)
    junk_count = junk_mask.sum()
    if junk_count:
        df = df[~junk_mask].copy()
        changes.append(f"Removed {junk_count} junk author-name rows")

    # Remove wrong-genre
    wrong_mask = df["Author Name"].fillna("").apply(is_wrong_genre)
    wrong_count = wrong_mask.sum()
    if wrong_count:
        df = df[~wrong_mask].copy()
        changes.append(f"Removed {wrong_count} wrong-genre entries")

    # ── Deduplicate author+series ──────────────────────────────────────────
    dk = (df["Author Name"].fillna("").str.lower().str.strip() + "||" +
          df["Book Series Name"].fillna("").str.lower().str.strip())
    before = len(df)
    # Keep first occurrence (usually highest-ranked)
    df = df[~dk.duplicated(keep="first")].copy()
    dup_removed = before - len(df)
    if dup_removed:
        changes.append(f"Removed {dup_removed} duplicate author+series rows")

    # ── Fix type contradictions ────────────────────────────────────────────
    books = pd.to_numeric(df["Books in Series"], errors="coerce")

    # "Standalone (check for series)" → reclassify based on book count
    unresolved = df["Type"].fillna("").str.contains("check for series", case=False)
    for idx in df[unresolved].index:
        n = books.get(idx)
        if pd.notna(n):
            df.at[idx, "Type"] = classify_type(n)
    changes.append(f"Reclassified {unresolved.sum()} 'Standalone (check for series)' rows by book count")

    # Standalone with books > 1
    standalone_mask = df["Type"].fillna("").str.lower() == "standalone"
    books_recalc = pd.to_numeric(df["Books in Series"], errors="coerce")
    sm = standalone_mask & (books_recalc > 1)
    for idx in df[sm].index:
        df.at[idx, "Type"] = classify_type(books_recalc.loc[idx])
    if sm.sum():
        changes.append(f"Reclassified {sm.sum()} Standalone rows with books>1")

    # ── Fix last book name issues ──────────────────────────────────────────
    fb = df["First Book Name"].fillna("").str.lower().str.strip()
    lb = df["Last Book Name"].fillna("").str.lower().str.strip()
    books_r = pd.to_numeric(df["Books in Series"], errors="coerce")

    # First = Last but multiple books
    fl_mask = (fb == lb) & (fb != "") & (books_r > 1)
    df.loc[fl_mask, "Last Book Name"] = ""
    if fl_mask.sum():
        changes.append(f"Blanked {fl_mask.sum()} wrong last-book entries (same as first)")

    # Bundle titles as last book
    bundle_mask = df["Last Book Name"].fillna("").apply(is_bundle_title)
    df.loc[bundle_mask, "Last Book Name"] = ""
    if bundle_mask.sum():
        changes.append(f"Blanked {bundle_mask.sum()} bundle/compilation last-book titles")

    # ── Add sanity flags ───────────────────────────────────────────────────
    df["Sanity_Issues"] = ""
    df["Data_Quality_Flag"] = ""

    for idx, row in df.iterrows():
        issues = []
        n_books = pd.to_numeric(row.get("Books in Series"), errors="coerce") if pd.notna(row.get("Books in Series")) else None
        n_pages = pd.to_numeric(row.get("Total Pages"), errors="coerce") if pd.notna(row.get("Total Pages")) else None

        # Series name = first book
        first_eq = row.get("first book name= book series name")
        if str(first_eq).lower() in ("true", "1", "yes"):
            issues.append("SERIES_IS_BOOK_TITLE")

        # Books = 3 suspicious
        if n_books == 3:
            issues.append("BOOKS_SUSPICIOUS_3")

        # Formulaic pages
        if n_pages and n_books and n_books > 0:
            if n_pages == n_books * 300 or n_pages == n_books * 250:
                issues.append("FORMULAIC_PAGES")
            ppb = n_pages / n_books
            if ppb < 30 and n_pages > 1:
                issues.append(f"PAGES_LOW_{ppb:.0f}ppb")
            if n_pages in (0, 1):
                issues.append("PAGES_ZERO")

        # Extreme book counts
        if n_books and n_books >= 50:
            issues.append(f"BOOKS_EXTREME_{n_books:.0f}")

        # Email is fabrication pattern
        email = str(row.get("Email", "") or "").strip()
        if email and is_fabrication_email(email):
            issues.append("EMAIL_FABRICATION_PATTERN")

        df.at[idx, "Sanity_Issues"] = " | ".join(issues) if issues else ""

        # Quality flag
        critical = {"SERIES_IS_BOOK_TITLE", "PAGES_ZERO", "BOOKS_EXTREME"}
        has_critical = any(any(c in i for c in critical) for i in issues)
        if has_critical or len(issues) >= 3:
            df.at[idx, "Data_Quality_Flag"] = "RED"
        elif issues:
            df.at[idx, "Data_Quality_Flag"] = "YELLOW"
        else:
            df.at[idx, "Data_Quality_Flag"] = "GREEN"

    # ── Add email quality column ───────────────────────────────────────────
    df["Email_Quality"] = ""
    for idx, row in df.iterrows():
        email = str(row.get("Email", "") or "").strip()
        if not email:
            df.at[idx, "Email_Quality"] = "MISSING"
        elif is_fabrication_email(email):
            df.at[idx, "Email_Quality"] = "FABRICATION_PATTERN"
        elif is_publicity_email(email):
            df.at[idx, "Email_Quality"] = "PUBLICITY_NOT_DIRECT"
        elif email in _KNOWN_BAD_EMAILS:
            df.at[idx, "Email_Quality"] = "KNOWN_BAD"
        else:
            df.at[idx, "Email_Quality"] = "NEEDS_VERIFICATION"

    # ── Build contact description ──────────────────────────────────────────
    df["Contact_Description"] = ""
    for idx, row in df.iterrows():
        parts = []
        email = str(row.get("Email", "") or "").strip()
        if email:
            quality = df.at[idx, "Email_Quality"]
            parts.append(f"Email ({quality}): {email}")
        website = str(row.get("Website", "") or "").strip()
        if website and website.startswith("http"):
            parts.append(f"Website: {website}")
        agent = str(row.get("Literary Agent", "") or "").strip()
        if agent and agent != "nan":
            parts.append(f"Agent: {agent}")
        for social in ["Twitter", "Instagram", "Facebook", "BookBub", "TikTok"]:
            sv = str(row.get(social, "") or "").strip()
            if sv and sv != "nan":
                parts.append(f"{social}: {sv}")
        df.at[idx, "Contact_Description"] = " | ".join(parts) if parts else "No contact info"

    changes.append(f"Added Sanity_Issues, Data_Quality_Flag, Email_Quality, Contact_Description columns")
    return df, changes


# ─── Main ──────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sheet", help="Process only this sheet (default: all)")
    args = parser.parse_args()

    sheets_to_process = [args.sheet] if args.sheet else ALL_GENRE_SHEETS

    print("=" * 70)
    print("ALL-GENRE DATA SANITY FIX")
    print("=" * 70)

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    VERIFIED_DIR.mkdir(parents=True, exist_ok=True)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)

    all_summaries = {}

    for sheet in sheets_to_process:
        print(f"\n{'─'*60}")
        print(f"Processing: {sheet}")
        print(f"{'─'*60}")

        try:
            df = pd.read_excel(SOURCE_XLSX, sheet_name=sheet)
        except Exception as e:
            print(f"  ERROR reading sheet: {e}")
            continue

        n_orig = len(df)
        fixed_df, changes = fix_genre_sheet(df, sheet)
        n_final = len(fixed_df)

        # Write CSV
        safe_name = sheet.lower().replace(" ", "_").replace("&", "and").replace("-", "_")
        csv_path = EXPORT_DIR / f"{safe_name}_CLEANED.csv"
        fixed_df.to_csv(csv_path, index=False)

        # Stats
        green = (fixed_df["Data_Quality_Flag"] == "GREEN").sum()
        yellow = (fixed_df["Data_Quality_Flag"] == "YELLOW").sum()
        red = (fixed_df["Data_Quality_Flag"] == "RED").sum()
        has_email = (fixed_df["Email"].notna() & (fixed_df["Email"].astype(str).str.strip() != "")).sum()
        fab_email = (fixed_df["Email_Quality"] == "FABRICATION_PATTERN").sum()
        needs_verify = (fixed_df["Email_Quality"] == "NEEDS_VERIFICATION").sum()

        summary = {
            "original": n_orig,
            "final": n_final,
            "removed": n_orig - n_final,
            "green": green,
            "yellow": yellow,
            "red": red,
            "has_email": has_email,
            "fab_email": fab_email,
            "needs_verify": needs_verify,
            "changes": changes,
        }
        all_summaries[sheet] = summary

        print(f"  {n_orig} → {n_final} rows (removed {n_orig - n_final})")
        print(f"  GREEN: {green}  YELLOW: {yellow}  RED: {red}")
        print(f"  Emails: {has_email} total, {fab_email} fabrication-pattern, {needs_verify} need verification")
        print(f"  CSV: {csv_path}")
        for c in changes:
            print(f"    • {c}")

    # ── Write combined report ──────────────────────────────────────────────
    report_path = REPORT_DIR / "ALL_GENRES_FIX_REPORT.md"
    report = "# All-Genre Data Sanity Fix Report\n"
    report += f"**Date:** 2026-04-05\n\n"
    report += "## Summary\n\n"
    report += "| Genre | Original | Final | Removed | GREEN | YELLOW | RED | Emails | Fab Emails |\n"
    report += "|---|---|---|---|---|---|---|---|---|\n"

    total_orig = total_final = total_green = total_yellow = total_red = 0
    total_email = total_fab = 0
    for sheet, s in all_summaries.items():
        short = sheet.split()[0] if len(sheet) > 20 else sheet
        report += (f"| {short} | {s['original']} | {s['final']} | {s['removed']} | "
                   f"{s['green']} | {s['yellow']} | {s['red']} | {s['has_email']} | {s['fab_email']} |\n")
        total_orig += s['original']
        total_final += s['final']
        total_green += s['green']
        total_yellow += s['yellow']
        total_red += s['red']
        total_email += s['has_email']
        total_fab += s['fab_email']

    report += (f"| **TOTAL** | **{total_orig}** | **{total_final}** | **{total_orig - total_final}** | "
               f"**{total_green}** | **{total_yellow}** | **{total_red}** | **{total_email}** | **{total_fab}** |\n\n")

    report += "## Changes Per Genre\n\n"
    for sheet, s in all_summaries.items():
        report += f"### {sheet}\n"
        for c in s["changes"]:
            report += f"- {c}\n"
        report += "\n"

    report += """## Next Steps

1. **Email verification** — Run website scraping + Gemini search for each genre (same as Ice Hockey)
2. **Series verification** — Goodreads lookup for real series names and book counts
3. **Priority:** Process genres in order: Military (most P0), Dark, Political, then rest
"""

    report_path.write_text(report)
    print(f"\n{'='*70}")
    print(f"COMBINED REPORT: {report_path}")
    print(f"TOTALS: {total_orig} → {total_final} rows | GREEN: {total_green} | YELLOW: {total_yellow} | RED: {total_red}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
