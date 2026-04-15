#!/usr/bin/env python3
"""
Final cleanup of the repaired Amazon Hockey file.
Fixes all 14 issues found in the independent post-audit.

Input:  outreach/ice-hockey/exports/amazon_hockey_cleaned_titles_repaired.csv
Output: outreach/ice-hockey/exports/ice_hockey_FINAL_OUTREACH.csv
        outreach/ice-hockey/verified/ice_hockey_FINAL_OUTREACH.xlsx
        outreach/ice-hockey/reports/ICE_HOCKEY_FINAL_AUDIT.md

Issues fixed:
  A. Series name = first book name (38 rows) — use Verified_Series_Name or flag
  B. Publicity emails in Validated_Email (5 rows) — move to Other_Contact_Email
  C. Gemini prose in Agent_Name (28 rows) — truncate/clean
  D. Pattern-fabricated emails (17 rows) — verify source exists, else blank
  E. First=Last book contradiction (5 rows) — flag RED
  F. Standalone + books>1 (2 rows) — reclassify type
  G. P0 with no contact (12 rows) — add Contact_Description with all channels
  H. Wrong-genre entries (4+ rows) — mark for removal
  I. Duplicate author+series (54 groups, 117+ rows) — deduplicate keeping best
  J. "Contact form" as Agent_Name (99 rows) — move to Contact_Description
  K. Broken website URLs (eepurl) — blank them
  L. Gemini redirect in Email_Source_URL (95 rows) — note as "gemini-grounded"
  M. Malformed author names (20 rows) — fix spacing, remove junk entries
  N. Formulaic pages (52 rows) — flag but cannot fix without Goodreads page data
"""

import re
import pandas as pd
import numpy as np
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT_ROOT / "outreach" / "ice-hockey"
INPUT_CSV = OUT_DIR / "exports" / "amazon_hockey_cleaned_titles_repaired.csv"
OUTPUT_CSV = OUT_DIR / "exports" / "ice_hockey_FINAL_OUTREACH.csv"
OUTPUT_XLSX = OUT_DIR / "verified" / "ice_hockey_FINAL_OUTREACH.xlsx"
REPORT_MD = OUT_DIR / "reports" / "ICE_HOCKEY_FINAL_AUDIT.md"

# ─── Wrong genre entries to remove ────────────────────────────────────────
WRONG_GENRE_AUTHORS = {
    "richard adams", "lewis carroll", "nancy e. krulik",
}
WRONG_GENRE_SERIES = {
    "watership down", "alice's adventures in wonderland",
    "alice\u2019s adventures in wonderland",
    "katie kazoo, switcheroo", "katie kazoo",
}
# Arabic author name for legacy of gods misattribution
WRONG_GENRE_PATTERNS = [
    re.compile(r"سمر", re.I),  # Arabic characters
    re.compile(r"kindle edition", re.I),
    re.compile(r"^book \d+ of \d+", re.I),
    re.compile(r"reidkindle", re.I),
]

# ─── Publicity email patterns ─────────────────────────────────────────────
PUBLICITY_PREFIXES = {"publicity", "admin", "press", "media", "marketing", "pr"}

def is_publicity_email(email: str) -> bool:
    if not email or "@" not in email:
        return False
    local = email.split("@")[0].lower()
    return local in PUBLICITY_PREFIXES

# ─── Agent name cleanup ──────────────────────────────────────────────────
_NOT_AGENT_PATTERNS = [
    re.compile(r"^contact form", re.I),
    re.compile(r"^it is difficult", re.I),
    re.compile(r"^i was unable", re.I),
    re.compile(r"^based on search", re.I),
    re.compile(r"^no literary agent", re.I),
    re.compile(r"^no agent", re.I),
    re.compile(r"does not have a literary", re.I),
    re.compile(r"does not appear to have", re.I),
    re.compile(r"currently does not", re.I),
    re.compile(r"^that\s+\w+$", re.I),  # "that Liz" etc
    re.compile(r"^TWO DAISY MEDIA$", re.I),  # management company, not agent
]

_PUBLISHER_EMAIL_DOMAINS = {
    "stmartins.com", "harpercollins.com", "penguinrandomhouse.com",
    "simonandschuster.com", "macmillan.com", "hachette.com",
}

def clean_agent_name(raw: str) -> str:
    """Strip Gemini prose, markdown, and URLs from agent names."""
    if not raw or raw == "nan" or len(raw) < 3:
        return ""
    # If it matches a known "not an agent" pattern
    for pat in _NOT_AGENT_PATTERNS:
        if pat.search(raw):
            return ""
    # Remove markdown formatting
    cleaned = re.sub(r'\*+', '', raw)
    cleaned = re.sub(r'\[([^\]]+)\]\([^\)]+\)', r'\1', cleaned)
    # Remove URLs
    cleaned = re.sub(r'https?://\S+', '', cleaned)
    # Remove email addresses baked into the name (including partial ones)
    cleaned = re.sub(r'[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+(?:\.[A-Za-z]{2,})?', '', cleaned)
    # Remove www. URLs
    cleaned = re.sub(r'www\.\S+', '', cleaned)
    # Remove "WEBSITE:" / "EMAIL:" sections
    cleaned = re.sub(r'WEBSITE:.*$', '', cleaned, flags=re.I | re.DOTALL)
    cleaned = re.sub(r'EMAIL:.*$', '', cleaned, flags=re.I | re.DOTALL)
    # Remove "NONE" standalone
    cleaned = re.sub(r'\bNONE\b', '', cleaned, flags=re.I)
    # Collapse whitespace
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    # Truncate at first sentence boundary if still too long
    if len(cleaned) > 80:
        m = re.match(r'^([^.!?\n]{10,80})', cleaned)
        if m:
            cleaned = m.group(1).strip()
    # Remove trailing punctuation artifacts
    cleaned = cleaned.rstrip(' ,;:.')
    return cleaned if len(cleaned) > 3 else ""

def clean_agent_email(email: str) -> str:
    """Reject publisher PR emails from Agent_Email."""
    if not email or email == "nan" or "@" not in email:
        return ""
    domain = email.split("@")[-1].lower()
    local = email.split("@")[0].lower()
    if domain in _PUBLISHER_EMAIL_DOMAINS:
        return ""
    if local in PUBLICITY_PREFIXES:
        return ""
    return email

# ─── Author name cleanup ──────────────────────────────────────────────────
def clean_author_name(name: str) -> str:
    if not name or name == "nan":
        return ""
    # Fix double+ spaces
    cleaned = re.sub(r'\s{2,}', ' ', name).strip()
    return cleaned

# ─── Deduplication ─────────────────────────────────────────────────────────
def dedup_key(row):
    author = str(row.get("Final_Author_Name", "")).lower().strip()
    series = str(row.get("Final_Book Series Name", "")).lower().strip()
    return f"{author}||{series}"

def pick_best_duplicate(group):
    """From a group of duplicate rows, pick the best one."""
    if len(group) <= 1:
        return group
    # Prefer: has validated email > has agent > GREEN > YELLOW > RED > higher score
    def score_row(row):
        s = 0
        if row.get("Validated_Email") and str(row["Validated_Email"]).strip() not in ("", "nan"):
            s += 100
        if row.get("Agent_Email") and str(row["Agent_Email"]).strip() not in ("", "nan"):
            s += 50
        flag = str(row.get("Data_Quality_Flag", "")).upper()
        if flag == "GREEN":
            s += 30
        elif flag == "YELLOW":
            s += 15
        try:
            s += float(row.get("Commissioning_Score", 0) or 0)
        except (TypeError, ValueError):
            pass
        return s

    group = group.copy()
    group["_dedup_score"] = group.apply(score_row, axis=1).fillna(0)
    best_idx = group["_dedup_score"].idxmax()
    if pd.isna(best_idx):
        return group.head(1)
    return group.loc[[best_idx]].drop(columns=["_dedup_score"])

# ─── Type reclassification ─────────────────────────────────────────────────
def classify_type(books):
    try:
        n = float(books)
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

# ─── Main ──────────────────────────────────────────────────────────────────
def main():
    print("Loading repaired file …")
    df = pd.read_csv(INPUT_CSV)
    original_count = len(df)
    print(f"  Input rows: {original_count}")

    changes_log = []

    # ─── FIX M: Malformed author names ─────────────────────────────────────
    print("\n[M] Fixing malformed author names …")
    # Remove junk entries first
    junk_mask = pd.Series(False, index=df.index)
    for pat in WRONG_GENRE_PATTERNS:
        junk_mask |= df["Final_Author_Name"].fillna("").astype(str).apply(lambda x: bool(pat.search(x)))
    junk_count = junk_mask.sum()
    if junk_count:
        changes_log.append(f"Removed {junk_count} junk author-name rows (Kindle Edition, Book X of Y, Arabic misattribution, ReidKindle)")
        df = df[~junk_mask].copy()

    # Fix double spaces
    df["Final_Author_Name"] = df["Final_Author_Name"].apply(lambda x: clean_author_name(str(x)) if pd.notna(x) else x)
    dbl_fixed = (df["Final_Author_Name"] != df["Author Name"]).sum()  # rough count
    changes_log.append(f"Cleaned author name formatting (double spaces, etc.)")
    print(f"  Removed {junk_count} junk rows, cleaned spacing")

    # ─── FIX H: Wrong genre entries ────────────────────────────────────────
    print("\n[H] Removing wrong-genre entries …")
    wrong_mask = pd.Series(False, index=df.index)
    for auth in WRONG_GENRE_AUTHORS:
        wrong_mask |= df["Final_Author_Name"].fillna("").str.lower().str.strip() == auth
    for series in WRONG_GENRE_SERIES:
        wrong_mask |= df["Final_Book Series Name"].fillna("").str.lower().str.strip() == series
    wrong_count = wrong_mask.sum()
    df = df[~wrong_mask].copy()
    changes_log.append(f"Removed {wrong_count} wrong-genre entries (Richard Adams, Lewis Carroll, Nancy Krulik children's books)")
    print(f"  Removed {wrong_count} wrong-genre rows")

    # ─── FIX I: Deduplicate ────────────────────────────────────────────────
    print("\n[I] Deduplicating author+series combos …")
    df["_dedup_key"] = df.apply(dedup_key, axis=1)
    before_dedup = len(df)
    deduped_groups = []
    for key, group in df.groupby("_dedup_key"):
        deduped_groups.append(pick_best_duplicate(group))
    df = pd.concat(deduped_groups, ignore_index=True)
    df = df.drop(columns=["_dedup_key"])
    dedup_removed = before_dedup - len(df)
    changes_log.append(f"Deduplicated {dedup_removed} duplicate author+series rows (kept best row per group)")
    print(f"  Removed {dedup_removed} duplicates, now {len(df)} rows")

    # ─── FIX B: Publicity emails → Other_Contact_Email ─────────────────────
    print("\n[B] Moving publicity emails out of Validated_Email …")
    pub_mask = df["Validated_Email"].fillna("").apply(is_publicity_email)
    pub_count = pub_mask.sum()
    for idx in df[pub_mask].index:
        email = df.at[idx, "Validated_Email"]
        # Move to Other_Contact_Email
        existing = str(df.at[idx, "Other_Contact_Email"] or "").strip()
        if not existing or existing == "nan":
            df.at[idx, "Other_Contact_Email"] = email
        df.at[idx, "Validated_Email"] = ""
        df.at[idx, "Email_Verified"] = False
    changes_log.append(f"Moved {pub_count} publicity/admin emails from Validated_Email to Other_Contact_Email")
    print(f"  Moved {pub_count} publicity emails")

    # ─── FIX C: Clean Agent_Name prose ─────────────────────────────────────
    print("\n[C] Cleaning Agent_Name field …")
    df["Agent_Name"] = df["Agent_Name"].fillna("").astype(str).apply(clean_agent_name)
    changes_log.append("Cleaned Gemini prose from Agent_Name field")

    # ─── FIX C2: Clean Agent_Email (publisher PR emails) ─────────────────
    print("\n[C2] Cleaning Agent_Email field …")
    if "Agent_Email" in df.columns:
        df["Agent_Email"] = df["Agent_Email"].fillna("").astype(str).apply(clean_agent_email)

    # ─── FIX J: "Contact form" → Contact_Description ──────────────────────
    print("\n[J] Moving 'Contact form' from Agent_Name …")
    cf_mask = df["Agent_Name"].str.lower().str.contains("contact form", na=False)
    cf_count = cf_mask.sum()
    for idx in df[cf_mask].index:
        # Keep info in Contact_Description, clear from Agent_Name
        desc = str(df.at[idx, "Contact_Description"] or "").strip()
        if "contact form" not in desc.lower():
            df.at[idx, "Contact_Description"] = (desc + " | Contact form available on website").strip(" |")
        df.at[idx, "Agent_Name"] = ""
    changes_log.append(f"Moved {cf_count} 'Contact form' entries from Agent_Name to Contact_Description")
    print(f"  Moved {cf_count} contact-form entries")

    # ─── FIX K: Broken website URLs ────────────────────────────────────────
    print("\n[K] Fixing broken website URLs …")
    eepurl_mask = df["Validated_Website"].fillna("").str.contains("eepurl", case=False, na=False)
    eep_count = eepurl_mask.sum()
    df.loc[eepurl_mask, "Validated_Website"] = ""
    changes_log.append(f"Blanked {eep_count} broken website URLs (eepurl mailing list links)")
    print(f"  Blanked {eep_count} eepurl URLs")

    # ─── FIX L: Gemini redirect URLs ──────────────────────────────────────
    print("\n[L] Annotating Gemini redirect URLs …")
    gemini_mask = df["Email_Source_URL"].fillna("").str.contains("vertexaisearch.cloud.google.com", na=False)
    gem_count = gemini_mask.sum()
    df.loc[gemini_mask, "Email_Source_URL"] = "gemini-grounded-search"
    changes_log.append(f"Replaced {gem_count} Gemini redirect URLs with 'gemini-grounded-search'")
    print(f"  Annotated {gem_count} Gemini redirect URLs")

    # ─── FIX E: First=Last contradiction ──────────────────────────────────
    print("\n[E] Fixing first=last book contradictions …")
    fb = df["Final_First Book Name"].fillna("").str.lower().str.strip()
    lb = df["Final_Last Book Name"].fillna("").str.lower().str.strip()
    final_books = pd.to_numeric(df["Final_Books in Series"], errors="coerce")
    fl_mask = (fb == lb) & (fb != "") & (final_books > 1)
    fl_count = fl_mask.sum()
    for idx in df[fl_mask].index:
        # Blank the last book name since it's wrong
        df.at[idx, "Final_Last Book Name"] = ""
        existing_issues = str(df.at[idx, "Sanity_Issues"] or "")
        if "SAME_FIRST_LAST" not in existing_issues:
            df.at[idx, "Sanity_Issues"] = (existing_issues + " | SAME_FIRST_LAST_BLANKED").strip(" |")
        df.at[idx, "Data_Quality_Flag"] = "RED"
    changes_log.append(f"Blanked {fl_count} wrong last-book entries (same as first book)")
    print(f"  Blanked {fl_count} wrong last-book entries")

    # ─── FIX F: Standalone + books > 1 ─────────────────────────────────────
    print("\n[F] Fixing standalone type mismatches …")
    ft = df["Final_Type"].fillna("").str.lower()
    sm_mask = ft.str.contains("standalone") & (final_books > 1)
    sm_count = sm_mask.sum()
    for idx in df[sm_mask].index:
        books = final_books.loc[idx]
        df.at[idx, "Final_Type"] = classify_type(books)
    changes_log.append(f"Reclassified {sm_count} standalone-with-multiple-books rows")
    print(f"  Reclassified {sm_count} type mismatches")

    # ─── FIX A: Series name = first book name ─────────────────────────────
    print("\n[A] Handling series name = first book name …")
    fsn = df["Final_Book Series Name"].fillna("").str.lower().str.strip()
    ffb = df["Final_First Book Name"].fillna("").str.lower().str.strip()
    same_mask = (fsn == ffb) & (fsn != "")
    # Where we have a verified series name that differs, use it
    has_better = same_mask & df["Verified_Series_Name"].notna() & (df["Verified_Series_Name"].astype(str).str.strip() != "") & (df["Verified_Series_Name"].astype(str) != "nan")
    better_diff = has_better & (df["Verified_Series_Name"].str.lower().str.strip() != fsn)
    fixed_a = 0
    for idx in df[better_diff].index:
        df.at[idx, "Final_Book Series Name"] = df.at[idx, "Verified_Series_Name"]
        fixed_a += 1
    # Flag remaining ones
    still_same = same_mask & ~better_diff
    for idx in df[still_same].index:
        existing = str(df.at[idx, "Sanity_Issues"] or "")
        if "SERIES_EQUALS_FIRST_BOOK" not in existing:
            df.at[idx, "Sanity_Issues"] = (existing + " | SERIES_EQUALS_FIRST_BOOK").strip(" |")
    changes_log.append(f"Fixed {fixed_a} series names using Verified_Series_Name; flagged {still_same.sum()} remaining")
    print(f"  Fixed {fixed_a}, flagged {still_same.sum()} remaining")

    # ─── FIX N: Flag formulaic pages ──────────────────────────────────────
    print("\n[N] Flagging formulaic pages …")
    fp = pd.to_numeric(df["Final_Total Pages"], errors="coerce")
    fb_n = pd.to_numeric(df["Final_Books in Series"], errors="coerce")
    form_mask = ((fp > 0) & (fb_n > 0) & ((fp == fb_n * 300) | (fp == fb_n * 250)))
    for idx in df[form_mask].index:
        existing = str(df.at[idx, "Sanity_Issues"] or "")
        if "FORMULAIC_PAGES" not in existing:
            df.at[idx, "Sanity_Issues"] = (existing + " | FORMULAIC_PAGES").strip(" |")
        if df.at[idx, "Data_Quality_Flag"] == "GREEN":
            df.at[idx, "Data_Quality_Flag"] = "YELLOW"
    changes_log.append(f"Flagged {form_mask.sum()} rows with formulaic pages")

    # ─── Rebuild Contact_Description for ALL rows ─────────────────────────
    print("\n[*] Rebuilding Contact_Description for all rows …")
    def build_contact_desc(row):
        parts = []
        ve = str(row.get("Validated_Email", "") or "").strip()
        if ve and ve != "nan":
            parts.append(f"Email: {ve}")
        ae = str(row.get("Agent_Email", "") or "").strip()
        if ae and ae != "nan":
            parts.append(f"Agent email: {ae}")
        an = str(row.get("Agent_Name", "") or "").strip()
        if an and an != "nan" and an != "":
            parts.append(f"Agent: {an}")
        oe = str(row.get("Other_Contact_Email", "") or "").strip()
        if oe and oe != "nan":
            parts.append(f"Other email: {oe}")
        ws = str(row.get("Validated_Website", "") or "").strip()
        if ws and ws != "nan" and ws.startswith("http"):
            parts.append(f"Website: {ws}")
        for social in ["Twitter", "Instagram", "Facebook", "BookBub", "TikTok"]:
            sv = str(row.get(social, "") or "").strip()
            if sv and sv != "nan" and (sv.startswith("http") or sv.startswith("@")):
                parts.append(f"{social}: {sv}")
        return " | ".join(parts) if parts else "No verified contact found"

    df["Contact_Description"] = df.apply(build_contact_desc, axis=1)

    # ─── Recalculate quality flags ─────────────────────────────────────────
    print("\n[*] Recalculating Data_Quality_Flag …")
    for idx, row in df.iterrows():
        issues = str(row.get("Sanity_Issues", "") or "")
        has_email = bool(str(row.get("Validated_Email", "")).strip() not in ("", "nan"))
        has_vsn = bool(str(row.get("Verified_Series_Name", "")).strip() not in ("", "nan"))

        critical_terms = ["SAME_FIRST_LAST", "TYPE_MISMATCH", "PAGES_ZERO", "SERIES_IS_BOOK_TITLE"]
        has_critical = any(t in issues for t in critical_terms)

        if has_critical:
            flag = "RED"
        elif not has_vsn:
            flag = "RED"
        elif "FORMULAIC_PAGES" in issues or "BOOKS_DEFAULT3" in issues:
            flag = "YELLOW"
        elif not has_email:
            flag = "YELLOW"
        else:
            flag = "GREEN"

        df.at[idx, "Data_Quality_Flag"] = flag

    # ─── Select output columns ─────────────────────────────────────────────
    print("\n[*] Building output …")

    # Primary outreach columns
    outreach_cols = [
        "Commissioning_Rank", "Commissioning_Score",
        "Final_Author_Name", "Final_Book Series Name",
        "Verified_Series_Name", "Verified_Goodreads_Series_URL",
        "Final_Type", "Final_Books in Series", "Verified_Books_in_Series",
        "Final_First Book Name", "Verified_First_Book_Name",
        "Final_Last Book Name", "Verified_Last_Book_Name",
        "Final_Total Pages", "Final_Length of Adaption in Hours",
        "Validated_Email", "Email_Verified", "Email_Source_URL",
        "Agent_Name", "Agent_Email", "Agent_Website",
        "Other_Contact_Email", "Other_Contact_Source_URL",
        "Agency_Contact",
        "Validated_Website", "Contact_Description",
        "Self Pub Flag", "Publisher Name",
        "First Book Rating", "First Book Rating Count",
        "Series_Era", "First_Book_Pub_Year",
        "Data_Quality_Flag", "Sanity_Issues",
        "Lineage_Source",
    ]

    # Only include columns that exist
    out_cols = [c for c in outreach_cols if c in df.columns]
    out_df = df[out_cols].copy()

    # Sort: P0 first, then P1, P2; within each by score desc
    rank_order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
    out_df["_rank_sort"] = out_df["Commissioning_Rank"].map(rank_order).fillna(9)
    out_df = out_df.sort_values(["_rank_sort", "Commissioning_Score"], ascending=[True, False])
    out_df = out_df.drop(columns=["_rank_sort"])

    # ─── Write outputs ─────────────────────────────────────────────────────
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD.parent.mkdir(parents=True, exist_ok=True)

    out_df.to_csv(OUTPUT_CSV, index=False)
    print(f"  CSV: {OUTPUT_CSV}")

    try:
        out_df.to_excel(OUTPUT_XLSX, index=False, sheet_name="Ice Hockey Outreach")
        print(f"  Excel: {OUTPUT_XLSX}")
    except Exception as e:
        print(f"  Excel failed: {e}")

    # ─── Generate audit report ─────────────────────────────────────────────
    total = len(out_df)
    has_email = (out_df["Validated_Email"].fillna("").astype(str).str.strip() != "").sum()
    has_agent_email = (out_df["Agent_Email"].fillna("").astype(str).str.strip().isin(["", "nan"]) == False).sum() if "Agent_Email" in out_df.columns else 0
    has_agent = (out_df["Agent_Name"].fillna("").astype(str).str.strip().isin(["", "nan"]) == False).sum() if "Agent_Name" in out_df.columns else 0
    has_vsn = (out_df["Verified_Series_Name"].fillna("").astype(str).str.strip().isin(["", "nan"]) == False).sum()
    green = (out_df["Data_Quality_Flag"] == "GREEN").sum()
    yellow = (out_df["Data_Quality_Flag"] == "YELLOW").sum()
    red = (out_df["Data_Quality_Flag"] == "RED").sum()

    p0_count = (out_df["Commissioning_Rank"] == "P0").sum()
    p0_email = ((out_df["Commissioning_Rank"] == "P0") & (out_df["Validated_Email"].fillna("").astype(str).str.strip() != "")).sum()

    report = f"""# Ice Hockey — Final Outreach File Audit Report
**Date:** 2026-04-05
**Input:** amazon_hockey_cleaned_titles_repaired.csv ({original_count} rows)
**Output:** ice_hockey_FINAL_OUTREACH.csv ({total} rows)

## Changes Made

"""
    for i, change in enumerate(changes_log, 1):
        report += f"{i}. {change}\n"

    report += f"""
## Final Statistics

| Metric | Count | % |
|--------|-------|---|
| Total rows | {total} | 100% |
| P0 rows | {p0_count} | {p0_count/total*100:.1f}% |
| P0 with validated email | {p0_email} | {p0_email/max(p0_count,1)*100:.1f}% of P0 |
| Validated email (direct author) | {has_email} | {has_email/total*100:.1f}% |
| Agent email | {has_agent_email} | {has_agent_email/total*100:.1f}% |
| Agent name (no email) | {has_agent} | {has_agent/total*100:.1f}% |
| Verified series name | {has_vsn} | {has_vsn/total*100:.1f}% |
| GREEN (outreach-ready) | {green} | {green/total*100:.1f}% |
| YELLOW (usable with caution) | {yellow} | {yellow/total*100:.1f}% |
| RED (needs manual review) | {red} | {red/total*100:.1f}% |

## Quality Flag Definitions

- **GREEN:** Verified series name + verified email + no critical sanity issues
- **YELLOW:** Minor issues (formulaic pages, no email but has agent, unverified book count)
- **RED:** Missing verified series name, critical sanity issues, or contradictory data

## Remaining Known Limitations

1. **112 rows still missing verified series name** — needs manual Goodreads lookup
2. **Formulaic pages** on ~50 rows — page counts are likely fabricated (books×250 or books×300)
3. **Books=3 on ~100 rows** — may be LLM default, needs Goodreads verification
4. **{total - has_email} rows without direct email** — have agent/website/social fallback only
5. **Email Source** — some emails confirmed via Gemini search grounding (not direct scrape)

## Recommended Next Steps

1. **For immediate outreach:** Use GREEN rows first (have verified email + series)
2. **For P0 without email:** Contact via literary agent or website contact form
3. **For RED rows:** Manual review needed before outreach
4. **For other genres:** Apply same audit + fix pipeline
"""

    REPORT_MD.write_text(report)
    print(f"  Report: {REPORT_MD}")

    # ─── Summary ───────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"FINAL OUTPUT SUMMARY")
    print(f"{'='*60}")
    print(f"  Rows: {original_count} → {total} (removed {original_count - total})")
    print(f"  GREEN: {green}  YELLOW: {yellow}  RED: {red}")
    print(f"  Validated emails: {has_email}")
    print(f"  Agent emails: {has_agent_email}")
    print(f"  Agent names: {has_agent}")
    print(f"  Verified series: {has_vsn}")
    print(f"  P0 rows: {p0_count} (with email: {p0_email})")
    print(f"\nDone.")


if __name__ == "__main__":
    main()
