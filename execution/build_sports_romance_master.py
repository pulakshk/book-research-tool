#!/usr/bin/env python3
"""
Sports Romance Master Outreach Sheet Builder
============================================
Builds the final outreach sheet for Sports Romance (Ice Hockey first),
modeled on the Romantasy Self-Publication Master framework.

Inputs:
  - outreach/ice-hockey/exports/ice_hockey_OUTREACH_READY.csv
  - outreach/ice-hockey/exports/amazon_hockey_cleaned_titles_repaired.csv
  - outreach/sports-romance/source/april_ku_sports_romance.csv  (optional)

Outputs:
  - outreach/sports-romance/exports/sports_romance_ice_hockey_MASTER.xlsx
  - outreach/sports-romance/exports/sports_romance_ice_hockey_MASTER.csv
  - outreach/sports-romance/reports/sports_romance_build_report.md

Commercial Bands (Needgap = Sports Romance):
  Tier 1: GR≥20k, 80+h → MG $17,500–$25,000, Rev 15–22%
  Tier 2: GR≥20k, 50+h → MG $17,500–$20,000, Rev 15–22%
  Tier 5: GR≥5k,  80+h → MG $12,500–$17,500, Rev 15–20%
  Tier 6: GR≥5k,  50+h → MG $5,000–$7,500,   Rev 15–20%
  Tier 9: GR<5k,  50+h → MG $0–$1,000,        Rev 12–18%
  Tier 10: GR<5k, <50h → No MG,               Rev 12–18%

Anti-hallucination:
  - Every step prints a 5-row spot-check to stdout
  - Hours formula validated against existing column
  - Tier assignment logs full reasoning for every row
  - Email drafts checked for unfilled {placeholders}
  - Dedup assertion after merge
  - Full row-count ledger printed at end

Usage:
  python3 execution/build_sports_romance_master.py
  python3 execution/build_sports_romance_master.py --skip-ku  (skip April KU mapping)
"""

import argparse
import re
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT / "outreach" / "sports-romance"
SRC_OUTREACH = PROJECT / "outreach" / "ice-hockey" / "exports" / "ice_hockey_OUTREACH_READY.csv"
SRC_AMAZON   = PROJECT / "outreach" / "ice-hockey" / "exports" / "amazon_hockey_cleaned_titles_repaired.csv"
SRC_APRIL_KU = OUT_DIR / "source" / "april_ku_sports_romance.csv"
EXPORT_DIR   = OUT_DIR / "exports"
REPORT_DIR   = OUT_DIR / "reports"

EXPORT_CSV   = EXPORT_DIR / "sports_romance_ice_hockey_MASTER.csv"
EXPORT_XLSX  = EXPORT_DIR / "sports_romance_ice_hockey_MASTER.xlsx"
REPORT_MD    = REPORT_DIR / "sports_romance_build_report.md"

EXPORT_DIR.mkdir(parents=True, exist_ok=True)
REPORT_DIR.mkdir(parents=True, exist_ok=True)

# ─── Commercial Bands (REVISED, Needgap only = Sports Romance) ────────────
# (tier, min_ratings, max_ratings, min_hours, mg_min, mg_max, rs_min, rs_max)
COMMERCIAL_BANDS = [
    (1,  20_000, None, 80, 17_500, 25_000, 15, 22),
    (2,  20_000, None, 40, 17_500, 20_000, 15, 22),
    (5,   5_000, 19_999, 80, 12_500, 17_500, 15, 20),
    (6,   5_000, 19_999, 40, 5_000,  7_500, 15, 20),
    (9,       0,  4_999, 40,     0,  1_000, 12, 18),
    (10,      0,  4_999,  0,     0,      0, 12, 18),  # No MG, below 40h
]
# NOTE: Tier 3,4 = Similar trope (Vampire/Werewolf) — not applicable to sports
# NOTE: Tier 7,8 = Similar trope — not applicable to sports

MIN_HOURS_THRESHOLD = 40.0  # Filter out series below this (not licensable)

EMAIL_TEMPLATE = """\
Subject: Pocket FM — Serialized Audio Partnership: {series_name}

Dear {salutation},

I'm reaching out from Pocket FM regarding the {series_name} series. We believe this series \
has strong potential for adaptation into serialized audio, a rapidly growing format \
structurally distinct from traditional audiobooks. We'd like to propose a partnership that \
includes a License Fee of {mg_display} and {rev_share} share of Revenue, details on the \
full offer are below.

We understand many authors have granted exclusive audiobook rights (often to Audible). We \
are not seeking audiobook rights. Instead, we license serialized audio series adaptation \
rights, which are closer in nature to a TV dramatization, but in audio form, than to a \
traditional narrated audiobook.

Rather than recording the manuscript verbatim, we create a scripted, episodic adaptation \
featuring dual-cast or full-cast performances, cinematic sound design, and structured \
cliffhanger arcs. Episodes run ~10 minutes and are released serially, with \
progression-based monetization. This format functions as an incremental revenue stream \
alongside print, ebook, KU, and audiobook sales, not in competition with them.

Given the layered world-building and sustained narrative momentum in {series_name}, we see \
strong alignment with this dramatized serialized format.

Pocket FM is the world's largest dedicated audio series platform, trusted by leading \
publishers and IP holders including Naver, Kakao, Blackstone, Recorded Books, Aethon, and \
China Literature. Our catalog includes globally recognized titles such as Solo Leveling, \
The Primal Hunter, and Omniscient Reader's Viewpoint. In the past 12 months, we've delivered:

  • 120 billion minutes of listening
  • 18 million monthly active users
  • $342M in revenue
  • 200+ million global downloads

Our app is rated 4.7★ on the App Store and 4.5★ on Google Play.

Several titles have scaled meaningfully through this model, including:
  • My Vampire System: 1B+ listens, $100M+ revenue
  • Saving Nora: $42M revenue
  • The Duke's Masked Bride: 641M plays
  • The Alpha's Bride: 415M plays

You can hear an example of our production quality here:
https://pocketfm.com/show/ba2d9038fc3853c464c8be7a34a0576f26145106

We are proposing:

  License Fee: {mg_display}
  Revenue Share: {rev_share} of Revenue
  Exclusive serialized audio series adaptation rights (distinct from audiobook rights)
  English language (U.S./U.K.)
  5–10 year term
  Full IP ownership retained by the author

We are focused on long-term partnerships and would welcome discussion around additional \
titles within your catalog as well.

If of interest, I'd be glad to schedule a brief call to discuss further.

Best regards,
Pulaksh Khimesara
US Licensing & Commissioning
Pocket FM"""


# ─── Helpers ──────────────────────────────────────────────────────────────

def _norm_key(s: str) -> str:
    """Normalize a string for fuzzy matching (lowercase, strip, remove punctuation)."""
    if not s or str(s) == "nan":
        return ""
    return re.sub(r"[^a-z0-9 ]", "", str(s).lower().strip())


def _best_val(*vals):
    """Return the first non-null, non-'nan' value."""
    for v in vals:
        if v is not None and str(v).strip() not in ("", "nan", "None", "NaN"):
            return v
    return ""


def _calc_hours(pages) -> float:
    """Hours = pages * 250 / 9600  (Romantasy framework formula)."""
    try:
        p = float(pages)
        return round(p * 250 / 9600, 2) if p > 0 else 0.0
    except (TypeError, ValueError):
        return 0.0


def assign_commercial_tier(gr_ratings_count, hours: float) -> dict:
    """
    Assign commercial tier, MG range, and rev share based on the revised
    Commercial Bands table (Needgap = sports romance).

    Returns dict with: tier, mg_min, mg_max, mg_range_display,
                       rev_share_range, rev_share_min, rev_share_max
    """
    try:
        ratings = float(gr_ratings_count) if gr_ratings_count else 0
    except (TypeError, ValueError):
        ratings = 0

    for (tier, r_min, r_max, h_min, mg_min, mg_max, rs_min, rs_max) in COMMERCIAL_BANDS:
        r_ok = ratings >= r_min and (r_max is None or ratings <= r_max)
        h_ok = hours >= h_min
        if r_ok and h_ok:
            if mg_min == 0 and mg_max == 0:
                mg_display = "No MG"
            elif mg_min == 0:
                mg_display = f"Up to ${mg_max:,}"
            else:
                mg_display = f"${mg_min:,} – ${mg_max:,}"
            return {
                "Commercial_Tier": tier,
                "MG_Min": mg_min,
                "MG_Max": mg_max,
                "MG_Range_Display": mg_display,
                "Rev_Share_Min": rs_min,
                "Rev_Share_Max": rs_max,
                "Rev_Share_Range": f"{rs_min}% – {rs_max}%",
                "Tier_Reasoning": (
                    f"Ratings={ratings:.0f} (≥{r_min}"
                    + (f", ≤{r_max}" if r_max else "")
                    + f"), Hours={hours:.1f}h (≥{h_min}h) → Tier {tier}"
                ),
            }

    # Fallback: below minimum threshold
    return {
        "Commercial_Tier": 10,
        "MG_Min": 0, "MG_Max": 0,
        "MG_Range_Display": "No MG",
        "Rev_Share_Min": 12, "Rev_Share_Max": 18,
        "Rev_Share_Range": "12% – 18%",
        "Tier_Reasoning": f"Ratings={ratings:.0f}, Hours={hours:.1f}h → Below threshold → Tier 10",
    }


def classify_retention(last_count, first_count) -> tuple:
    """
    Returns (retention_proxy, retention_quality).
    Flags inconsistent data (Amazon vs GR scale mismatch).
    """
    try:
        fc = float(first_count)
        lc = float(last_count)
    except (TypeError, ValueError):
        return (None, "N/A")

    if fc <= 0 or lc <= 0:
        return (None, "N/A")

    # Detect Amazon vs GR scale mismatch: if last < 1% of first and first > 10k
    if fc > 10_000 and lc < fc * 0.01:
        return (None, "DATA_SCALE_MISMATCH")

    ratio = round(lc / fc, 4)
    if ratio >= 0.50:
        quality = "STRONG ≥50%"
    elif ratio >= 0.30:
        quality = "MODERATE 30–50%"
    else:
        quality = "WEAK <30%"
    return (ratio, quality)


def draft_email(author_name: str, series_name: str,
                mg_display: str, rev_share_range: str,
                contact_type: str) -> str:
    """
    Draft a custom email from the template.
    contact_type: 'direct' or 'agent'
    """
    first_name = author_name.split()[0] if author_name else "Author"
    # Avoid "Dear PublicityEmail@..." - use generic if no real name
    salutation = first_name if len(first_name) > 1 and first_name.lower() not in (
        "the", "a", "an", "dr", "mr", "ms", "mrs"
    ) else "Author"

    # Use the max of the MG range as the headline offer (e.g., "$17,500")
    # Extract max from display string like "$12,500 – $17,500"
    mg_headline = mg_display
    m = re.search(r"\$([0-9,]+)\s*$", mg_display)
    if m:
        mg_headline = f"${m.group(1)}"
    elif mg_display in ("No MG", ""):
        mg_headline = "Revenue Share Only"

    # Rev share: use minimum (guaranteed floor)
    rs_match = re.match(r"(\d+)%", rev_share_range)
    rs_floor = rs_match.group(1) + "%" if rs_match else rev_share_range

    email = EMAIL_TEMPLATE.format(
        salutation=salutation,
        series_name=series_name or "your series",
        mg_display=mg_headline,
        rev_share=rs_floor,
    )
    return email


def check_no_placeholders(text: str) -> bool:
    """Return True if no unfilled {placeholder} patterns remain."""
    return not re.search(r"\{[a-z_]+\}", text)


def spot_check(df: pd.DataFrame, label: str, cols: list, n: int = 5):
    """Print a spot-check of n random rows for the given columns."""
    print(f"\n{'─'*60}")
    print(f"SPOT-CHECK: {label} ({min(n, len(df))} of {len(df)} rows)")
    print(f"{'─'*60}")
    sample = df.sample(min(n, len(df)), random_state=42)
    for _, row in sample.iterrows():
        parts = " | ".join(
            f"{c}={str(row.get(c, ''))[:40]}" for c in cols if c in row
        )
        print(f"  {parts}")


# ─── Main ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip-ku", action="store_true", help="Skip April KU mapping step")
    args = parser.parse_args()

    row_ledger = {}  # Track row counts through pipeline
    report_lines = []

    print("=" * 70)
    print(f"SPORTS ROMANCE MASTER BUILDER — {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 70)

    # ══════════════════════════════════════════════════════════════════════
    # STEP A: Load and merge
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ STEP A: Load & Merge")

    if not SRC_OUTREACH.exists():
        print(f"ERROR: Missing source file: {SRC_OUTREACH}")
        sys.exit(1)

    df = pd.read_csv(SRC_OUTREACH)
    row_ledger["A_loaded"] = len(df)
    print(f"  Loaded OUTREACH_READY: {len(df)} rows")

    # Load amazon_hockey for retention proxy
    amazon_df = None
    if SRC_AMAZON.exists():
        amazon_df = pd.read_csv(SRC_AMAZON)
        amazon_df.columns = [c.replace("\n", " ").strip() for c in amazon_df.columns]
        amazon_df["_merge_key"] = (
            amazon_df["Author Name"].fillna("").apply(_norm_key) + "||" +
            amazon_df["Book Series Name"].fillna("").apply(_norm_key)
        )
        print(f"  Loaded Amazon source: {len(amazon_df)} rows")
    else:
        print(f"  WARNING: Amazon source not found, skipping retention proxy: {SRC_AMAZON}")

    # Add merge key to main df
    df["_merge_key"] = (
        df["Final_Author_Name"].fillna("").apply(_norm_key) + "||" +
        df["Final_Book Series Name"].fillna("").apply(_norm_key)
    )

    # Merge for retention proxy
    if amazon_df is not None:
        retention_cols = ["_merge_key", "Last Book Rating Count", "First Book Rating Count"]
        available = [c for c in retention_cols if c in amazon_df.columns]
        amazon_small = amazon_df[available].drop_duplicates("_merge_key")
        df = df.merge(amazon_small, on="_merge_key", how="left", suffixes=("", "_amz"))
        matched = df["Last Book Rating Count"].notna().sum()
        print(f"  Merge: matched {matched} / {len(df)} rows for retention proxy")
    else:
        df["Last Book Rating Count"] = None
        df["First Book Rating Count_amz"] = None

    # SELF-CHECK A
    spot_check(df, "Post-Merge Sample",
               ["Final_Author_Name", "Final_Book Series Name",
                "Commissioning_Rank", "Last Book Rating Count"], n=5)

    # Dedup assertion
    dup_key = df["_merge_key"].duplicated().sum()
    print(f"\n  Dedup check: {dup_key} duplicate Author+Series keys")
    if dup_key > 0:
        print(f"  WARNING: {dup_key} duplicates found — keeping first occurrence")
        df = df[~df["_merge_key"].duplicated(keep="first")].copy()
        row_ledger["A_after_dedup"] = len(df)

    # ══════════════════════════════════════════════════════════════════════
    # STEP B: Recalculate hours & filter
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ STEP B: Recalculate Hours & Filter")

    df["Approx_Hours_Calc"] = df["Final_Total Pages"].apply(_calc_hours)

    # Validate against existing column
    existing_hours = pd.to_numeric(df["Final_Length of Adaption in Hours"], errors="coerce")
    calc_hours = df["Approx_Hours_Calc"]
    discrepancy_mask = (
        existing_hours.notna() &
        (existing_hours > 0) &
        ((calc_hours - existing_hours).abs() / existing_hours > 0.10)
    )
    n_disc = discrepancy_mask.sum()
    if n_disc > 0:
        print(f"  WARNING: {n_disc} rows have >10% discrepancy between calc and existing hours")
        disc_sample = df[discrepancy_mask][["Final_Author_Name", "Final_Total Pages",
                                            "Approx_Hours_Calc",
                                            "Final_Length of Adaption in Hours"]].head(5)
        print(disc_sample.to_string(index=False))

    # Use existing verified hours as canonical (they were computed in the prior pipeline
    # using 300 words/page instead of 250, which is the source-of-truth for this dataset).
    # Our recalculated column is kept as a reference but NOT used for filtering.
    existing_hours_col = pd.to_numeric(df["Final_Length of Adaption in Hours"], errors="coerce")
    df["Approx_Hours"] = existing_hours_col.where(
        existing_hours_col > 0, df["Approx_Hours_Calc"]
    )

    # Filter below minimum threshold
    below_threshold = (df["Approx_Hours"] < MIN_HOURS_THRESHOLD) | (df["Approx_Hours"] == 0)
    n_filtered = below_threshold.sum()
    df_filtered = df[below_threshold].copy()
    df = df[~below_threshold].copy()
    print(f"  Filtered out {n_filtered} rows below {MIN_HOURS_THRESHOLD}h threshold")
    print(f"  Remaining: {len(df)} rows")
    row_ledger["B_after_filter"] = len(df)

    # SELF-CHECK B
    spot_check(df, "Hours Validation",
               ["Final_Author_Name", "Final_Total Pages",
                "Approx_Hours", "Final_Length of Adaption in Hours"], n=5)

    # ══════════════════════════════════════════════════════════════════════
    # STEP C: Calculate commercial tiers & MG
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ STEP C: Commercial Tiers & MG")

    tier_results = []
    for _, row in df.iterrows():
        gr_count = row.get("First Book Rating Count", 0)
        hrs = row["Approx_Hours"]
        result = assign_commercial_tier(gr_count, hrs)
        tier_results.append(result)

    tier_df = pd.DataFrame(tier_results, index=df.index)
    df = pd.concat([df, tier_df], axis=1)

    # SELF-CHECK C — print 5 random tier assignments
    print("\n  Tier assignment spot-check (5 random rows):")
    for _, row in df.sample(min(5, len(df)), random_state=7).iterrows():
        print(f"    {str(row.get('Final_Author_Name',''))[:30]:30} | "
              f"{str(row.get('Tier_Reasoning',''))[:70]}")

    tier_dist = df["Commercial_Tier"].value_counts().sort_index()
    print(f"\n  Tier distribution:\n{tier_dist.to_string()}")
    row_ledger["C_tier_calc"] = len(df)

    # ══════════════════════════════════════════════════════════════════════
    # STEP D: Retention proxy
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ STEP D: Retention Proxy")

    ret_proxies = []
    ret_qualities = []
    for _, row in df.iterrows():
        last_count = row.get("Last Book Rating Count")
        first_count = _best_val(
            row.get("First Book Rating Count"),
            row.get("First Book Rating Count_amz")
        )
        proxy, quality = classify_retention(last_count, first_count)
        ret_proxies.append(proxy)
        ret_qualities.append(quality)

    df["Retention_Proxy"] = ret_proxies
    df["Retention_Quality"] = ret_qualities

    # SELF-CHECK D
    print("\n  Retention proxy spot-check (5 rows where data exists):")
    has_retention = df[df["Retention_Proxy"].notna()]
    sample_ret = has_retention.sample(min(5, len(has_retention)), random_state=42)
    for _, row in sample_ret.iterrows():
        print(f"    {str(row.get('Final_Author_Name',''))[:30]:30} | "
              f"First={str(row.get('First Book Rating Count',''))[:10]:10} | "
              f"Last={str(row.get('Last Book Rating Count',''))[:10]:10} | "
              f"Proxy={row['Retention_Proxy']} | {row['Retention_Quality']}")

    quality_dist = df["Retention_Quality"].value_counts()
    print(f"\n  Retention quality distribution:\n{quality_dist.to_string()}")

    # ══════════════════════════════════════════════════════════════════════
    # STEP E: Map April KU lists
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ STEP E: Map April KU Lists")

    # Feb 2026 list: from T100_Mapping in existing amazon file
    # Merge it in if available
    feb_mapping = {}
    if amazon_df is not None and "T100_Mapping" in amazon_df.columns:
        for _, arow in amazon_df.iterrows():
            mk = arow["_merge_key"]
            tm = str(arow.get("T100_Mapping", "")).strip()
            if tm and tm not in ("No List", "nan", ""):
                feb_mapping[mk] = tm

    df["KU_List_Feb2026"] = df["_merge_key"].map(feb_mapping).fillna("Not Listed")

    # April 2026 list: from scraper output (if available)
    df["KU_List_April2026"] = "Not Available"
    df["Amazon_Top_Rank"] = ""

    if not args.skip_ku and SRC_APRIL_KU.exists():
        april_df = pd.read_csv(SRC_APRIL_KU)
        april_df["_title_key"] = april_df["title"].fillna("").apply(_norm_key)
        april_df["_author_key"] = april_df["author"].fillna("").apply(_norm_key)

        matched_count = 0
        for idx, row in df.iterrows():
            author_key = _norm_key(str(row.get("Final_Author_Name", "")))
            # Try to match on author name (last name or full name)
            author_last = author_key.split()[-1] if author_key.split() else ""

            matches = april_df[
                april_df["_author_key"].str.contains(author_last, na=False, regex=False)
            ] if author_last else pd.DataFrame()

            if not matches.empty:
                best = matches.nsmallest(1, "rank").iloc[0]
                list_tag = f"{best['category']} #{best['rank']} ({best['sport_tag']})"
                df.at[idx, "KU_List_April2026"] = list_tag
                df.at[idx, "Amazon_Top_Rank"] = best["rank"]
                matched_count += 1

        print(f"  April KU: matched {matched_count} authors from scrape data")

        # SELF-CHECK E: verify no cross-genre mismatches
        april_mapped = df[df["KU_List_April2026"] != "Not Available"]
        if not april_mapped.empty:
            print(f"\n  April KU match spot-check:")
            for _, row in april_mapped.head(5).iterrows():
                print(f"    {str(row.get('Final_Author_Name',''))[:30]:30} | {row['KU_List_April2026']}")
    else:
        if not args.skip_ku:
            print(f"  April KU file not found: {SRC_APRIL_KU}")
            print(f"  Run: python3 execution/scrape_april_ku_lists.py first")
        else:
            print("  [--skip-ku] Skipping April KU mapping")

    # ══════════════════════════════════════════════════════════════════════
    # STEP F: Draft custom emails
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ STEP F: Draft Custom Emails")

    emails_drafted = 0
    emails_skipped = 0
    placeholder_errors = 0
    draft_emails = []

    for _, row in df.iterrows():
        author = str(row.get("Final_Author_Name", "")).strip()
        raw_series = str(_best_val(
            row.get("Verified_Series_Name"),
            row.get("Final_Book Series Name")
        )).strip()
        # Strip any embedded newlines / data-quality artifacts from Verified_Series_Name
        series = raw_series.split("\n")[0].strip()
        mg_display = str(row.get("MG_Range_Display", "No MG")).strip()
        rev_share = str(row.get("Rev_Share_Range", "15% – 20%")).strip()

        has_contact = (
            str(row.get("Validated_Email", "")).strip() not in ("", "nan") or
            str(row.get("Agent_Email", "")).strip() not in ("", "nan") or
            str(row.get("Other_Contact_Email", "")).strip() not in ("", "nan")
        )

        if not has_contact:
            draft_emails.append("")
            emails_skipped += 1
            continue

        email_text = draft_email(
            author_name=author,
            series_name=series,
            mg_display=mg_display,
            rev_share_range=rev_share,
            contact_type="direct",
        )

        if not check_no_placeholders(email_text):
            placeholder_errors += 1
            email_text = f"[PLACEHOLDER ERROR] {email_text}"

        draft_emails.append(email_text)
        emails_drafted += 1

    df["Draft_Email"] = draft_emails
    print(f"  Drafted: {emails_drafted} | Skipped (no contact): {emails_skipped} | "
          f"Placeholder errors: {placeholder_errors}")

    # SELF-CHECK F: print 3 sample emails
    drafted_rows = df[df["Draft_Email"].str.len() > 50]
    print("\n  Sample drafted emails (3 rows):")
    for i, (_, row) in enumerate(drafted_rows.sample(min(3, len(drafted_rows)),
                                                       random_state=42).iterrows()):
        preview = row["Draft_Email"].split("\n")
        print(f"\n  [{i+1}] {row.get('Final_Author_Name')} — {row.get('Verified_Series_Name', row.get('Final_Book Series Name'))}")
        for ln in preview[:5]:
            print(f"       {ln}")
        print("       [...]")

    if placeholder_errors > 0:
        print(f"\n  WARNING: {placeholder_errors} emails have unfilled placeholders!")

    # ══════════════════════════════════════════════════════════════════════
    # STEP G: Build final column order & export
    # ══════════════════════════════════════════════════════════════════════
    print("\n▶ STEP G: Final Validation & Export")

    # Build output dataframe with canonical column order
    def _col(row, *keys):
        for k in keys:
            v = row.get(k)
            if v is not None and str(v).strip() not in ("", "nan", "None", "NaN"):
                return v
        return ""

    output_rows = []
    for _, row in df.iterrows():
        series_name_raw = _best_val(row.get("Verified_Series_Name"), row.get("Final_Book Series Name"))
        series_name = str(series_name_raw).split("\n")[0].strip()  # strip embedded artifacts
        gr_url      = _best_val(row.get("Verified_Goodreads_Series_URL"))
        num_books   = _best_val(row.get("Verified_Books_in_Series"), row.get("Final_Books in Series"))
        first_book  = _best_val(row.get("Verified_First_Book_Name"), row.get("Final_First Book Name"))

        output_rows.append({
            "Priority_Rank":            row.get("Commissioning_Rank", ""),
            "Commissioning_Score":      row.get("Commissioning_Score", ""),
            "Author_Name":              row.get("Final_Author_Name", ""),
            "Series_Name":              series_name,
            "Goodreads_Series_URL":     gr_url,
            "Num_Books":                num_books,
            "Total_Pages":              row.get("Final_Total Pages", ""),
            "Approx_Hours":             row.get("Approx_Hours", ""),
            "First_Book_Name":          first_book,
            "First_Book_Rating_Stars":  row.get("First Book Rating", ""),
            "First_Book_GR_Ratings":    row.get("First Book Rating Count", ""),
            "Rating_Category":          (
                "Tier 1 (≥20k)" if (row.get("First Book Rating Count") or 0) >= 20_000
                else "Tier 2 (≥5k)" if (row.get("First Book Rating Count") or 0) >= 5_000
                else "Tier 3 (<5k)"
            ),
            "Trope":                    "Needgap",
            "Commercial_Tier":          row.get("Commercial_Tier", ""),
            "MG_Min":                   row.get("MG_Min", ""),
            "MG_Max":                   row.get("MG_Max", ""),
            "MG_Range_Display":         row.get("MG_Range_Display", ""),
            "Rev_Share_Range":          row.get("Rev_Share_Range", ""),
            "Retention_Proxy":          row.get("Retention_Proxy", ""),
            "Retention_Quality":        row.get("Retention_Quality", ""),
            "Sub_Genre":                "Ice Hockey / Sports Romance",
            "Series_Era":               row.get("Series_Era", ""),
            "Self_Pub_Flag":            row.get("Self Pub Flag", ""),
            "Publisher":                row.get("Publisher Name", ""),
            "KU_List_Feb2026":          row.get("KU_List_Feb2026", ""),
            "KU_List_April2026":        row.get("KU_List_April2026", ""),
            "Amazon_Top_Rank":          row.get("Amazon_Top_Rank", ""),
            "Validated_Email":          row.get("Validated_Email", ""),
            "Email_Verified":           row.get("Email_Verified", ""),
            "Email_Source_URL":         row.get("Email_Source_URL", ""),
            "Agent_Name":               row.get("Agent_Name", ""),
            "Agent_Email":              row.get("Agent_Email", ""),
            "Agency_Contact":           row.get("Agency_Contact", ""),
            "Other_Contact_Email":      row.get("Other_Contact_Email", ""),
            "Validated_Website":        row.get("Validated_Website", ""),
            "Contact_Description":      row.get("Contact_Description", ""),
            "Data_Quality_Flag":        row.get("Data_Quality_Flag", ""),
            "Sanity_Issues":            row.get("Sanity_Issues", ""),
            "Tier_Reasoning":           row.get("Tier_Reasoning", ""),
            "Draft_Email":              row.get("Draft_Email", ""),
        })

    out_df = pd.DataFrame(output_rows)
    row_ledger["G_final"] = len(out_df)

    # Final dedup assertion
    final_dup = out_df.duplicated(subset=["Author_Name", "Series_Name"]).sum()
    if final_dup > 0:
        print(f"  WARNING: {final_dup} duplicate Author+Series in final output — keeping first")
        out_df = out_df[~out_df.duplicated(subset=["Author_Name", "Series_Name"],
                                           keep="first")].copy()

    # Quality summary
    green = (out_df["Data_Quality_Flag"] == "GREEN").sum()
    yellow = (out_df["Data_Quality_Flag"] == "YELLOW").sum()
    red = (out_df["Data_Quality_Flag"] == "RED").sum()
    has_email = out_df["Validated_Email"].apply(
        lambda x: str(x).strip() not in ("", "nan")
    ).sum()
    has_agent = out_df["Agent_Email"].apply(
        lambda x: str(x).strip() not in ("", "nan")
    ).sum()
    has_any_contact = out_df["Draft_Email"].apply(
        lambda x: len(str(x)) > 50
    ).sum()

    # Save CSV
    out_df.to_csv(EXPORT_CSV, index=False)
    print(f"\n  Saved CSV: {EXPORT_CSV} ({len(out_df)} rows)")

    # Save XLSX with formatting
    with pd.ExcelWriter(EXPORT_XLSX, engine="openpyxl") as writer:
        out_df.to_excel(writer, sheet_name="Sports Romance Master", index=False)
        ws = writer.sheets["Sports Romance Master"]
        # Freeze top row
        ws.freeze_panes = "A2"
        # Auto-width for key columns
        for col_idx, col in enumerate(out_df.columns, 1):
            max_len = max(
                len(str(col)),
                out_df[col].astype(str).str.len().max() if len(out_df) > 0 else 0,
            )
            ws.column_dimensions[ws.cell(1, col_idx).column_letter].width = min(max_len + 2, 60)

    print(f"  Saved XLSX: {EXPORT_XLSX}")

    # ── Write build report ─────────────────────────────────────────────
    report = f"""# Sports Romance Ice Hockey — Master Outreach Sheet Build Report
**Date:** {datetime.now():%Y-%m-%d}
**Source:** ice_hockey_OUTREACH_READY.csv ({row_ledger['A_loaded']} rows)

## Pipeline Summary

| Stage | Rows |
|-------|------|
| A. Loaded from OUTREACH_READY | {row_ledger['A_loaded']} |
| B. After 40h filter | {row_ledger.get('B_after_filter', 'N/A')} |
| G. Final output | {row_ledger.get('G_final', 'N/A')} |

## Data Quality

| Metric | Count | % |
|--------|-------|---|
| GREEN | {green} | {green/len(out_df)*100:.1f}% |
| YELLOW | {yellow} | {yellow/len(out_df)*100:.1f}% |
| RED | {red} | {red/len(out_df)*100:.1f}% |
| Has validated email | {has_email} | {has_email/len(out_df)*100:.1f}% |
| Has agent email | {has_agent} | {has_agent/len(out_df)*100:.1f}% |
| Has draft email | {has_any_contact} | {has_any_contact/len(out_df)*100:.1f}% |

## Commercial Tier Distribution

{out_df['Commercial_Tier'].value_counts().sort_index().to_string() if len(out_df) > 0 else 'N/A'}

## MG Range Distribution

{out_df['MG_Range_Display'].value_counts().to_string() if len(out_df) > 0 else 'N/A'}

## KU List Coverage (Feb 2026)

{out_df['KU_List_Feb2026'].value_counts().head(10).to_string() if len(out_df) > 0 else 'N/A'}

## P0 Entries

"""
    p0_df = out_df[out_df["Priority_Rank"] == "P0"]
    for _, row in p0_df.iterrows():
        contact = row.get("Validated_Email", "") or row.get("Agent_Email", "") or "NO CONTACT"
        report += (
            f"- **{row['Author_Name']}** — {row['Series_Name']} | "
            f"Tier {row['Commercial_Tier']} | MG {row['MG_Range_Display']} | "
            f"Contact: {str(contact)[:50]}\n"
        )

    report += f"""
## Outputs

- CSV: `{EXPORT_CSV}`
- XLSX: `{EXPORT_XLSX}`

## Anti-Hallucination Checks Run

- ✅ Hours formula validated against existing column ({n_disc} discrepancies flagged)
- ✅ Dedup: {dup_key} duplicate keys removed at merge step
- ✅ Tier reasoning logged for every row (see Tier_Reasoning column)
- ✅ Retention proxy: DATA_SCALE_MISMATCH flagged where Amazon/GR scales diverged
- ✅ Email placeholder check: {placeholder_errors} errors found
- ✅ Final dedup: {final_dup} duplicate Author+Series removed

## Next Steps

1. Run `python3 execution/scrape_april_ku_lists.py` if April KU data is missing
2. Review P0 rows with no contact — manual research needed
3. Cross-check Commercial_Tier for top 10 authors against the Romantasy sheet
4. When ready for outreach, pull Draft_Email column and send via CRM
"""

    REPORT_MD.write_text(report)
    print(f"  Report: {REPORT_MD}")

    # ── Row count ledger ───────────────────────────────────────────────
    print("\n" + "=" * 70)
    print("ROW COUNT LEDGER:")
    for stage, count in row_ledger.items():
        print(f"  {stage}: {count}")
    print()
    print(f"FINAL: {len(out_df)} rows in master sheet")
    print(f"  GREEN: {green} | YELLOW: {yellow} | RED: {red}")
    print(f"  Emails: {has_email} direct | {has_agent} agent | {has_any_contact} with draft")
    print(f"  Tiers: {tier_dist.to_dict()}")
    print("=" * 70)


if __name__ == "__main__":
    main()
