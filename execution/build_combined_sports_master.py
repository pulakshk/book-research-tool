#!/usr/bin/env python3
"""
Combined Sports Romance Master — Multi-Sheet XLSX
===================================================
Creates a Romantasy-style multi-sheet workbook combining:
  - ALL 615 existing ice hockey rows (no hour filter)
  - New titles discovered from April 2026 KU scrape
  - Commercial tier/MG/rev share for every row
  - Custom email drafts for rows with contacts
  - KU list rankings (Feb + April)
  - Retention proxies where data exists

Sheets (mirroring Romantasy Self-Publication Master):
  1. Picks for Licensing — main outreach sheet (Romantasy-aligned columns)
  2. Working Sheet       — detailed data with retention + sanity flags
  3. Commercial Bands    — reference tier table
  4. April KU Rankings   — raw April 2026 scrape data
  5. Selection Logic     — criteria and filtering explanation

Output:
  outreach/sports-romance/exports/Sports_Romance_Combined_Master.xlsx

Usage:
  python3 execution/build_combined_sports_master.py
"""

import re
import sys
from pathlib import Path
from datetime import datetime

import numpy as np
import pandas as pd

PROJECT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT / "outreach" / "sports-romance"

# ── Inputs ──────────────────────────────────────────────────────────────
SRC_OUTREACH = PROJECT / "outreach" / "ice-hockey" / "exports" / "ice_hockey_OUTREACH_READY.csv"
SRC_AMAZON   = PROJECT / "outreach" / "ice-hockey" / "exports" / "amazon_hockey_cleaned_titles_repaired.csv"
SRC_APRIL_KU = OUT_DIR / "source" / "april_ku_sports_romance.csv"

# ── Outputs ─────────────────────────────────────────────────────────────
EXPORT_DIR = OUT_DIR / "exports"
EXPORT_DIR.mkdir(parents=True, exist_ok=True)
COMBINED_XLSX = EXPORT_DIR / "Sports_Romance_Combined_Master.xlsx"
COMBINED_CSV  = EXPORT_DIR / "Sports_Romance_Combined_Master.csv"

# ── Commercial Bands (REVISED, Needgap = Sports) ───────────────────────
BANDS = [
    (1,  20_000, None,   80, 17_500, 25_000, 15, 22),
    (2,  20_000, None,   40, 17_500, 20_000, 15, 22),
    (5,   5_000, 19_999, 80, 12_500, 17_500, 15, 20),
    (6,   5_000, 19_999, 40,  5_000,  7_500, 15, 20),
    (9,       0,  4_999, 40,      0,  1_000, 12, 18),
    (10,      0,  4_999,  0,      0,      0, 12, 18),
]

EMAIL_TEMPLATE = """\
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

120 billion minutes of listening
18 million monthly active users
$342M in revenue
200+ million global downloads

Our app is rated 4.7 on the App Store and 4.5 on Google Play.

Several titles have scaled meaningfully through this model, including:
My Vampire System: 1B+ listens, $100M+ revenue
Saving Nora: $42M revenue
The Duke's Masked Bride: 641M plays
The Alpha's Bride: 415M plays

You can hear an example of our production quality here:
https://pocketfm.com/show/ba2d9038fc3853c464c8be7a34a0576f26145106

We are proposing:

License Fee: {mg_display}
Revenue Share: {rev_share} of Revenue
Exclusive serialized audio series adaptation rights (distinct from audiobook rights)
English language (U.S./U.K.)
5-10 year term
Full IP ownership retained by the author

We are focused on long-term partnerships and would welcome discussion around additional \
titles within your catalog as well.

If of interest, I'd be glad to schedule a brief call to discuss further.

Best regards,
Pulaksh Khimesara
US Licensing & Commissioning
Pocket FM"""


# ─── Helpers ──────────────────────────────────────────────────────────────

def _norm(s):
    if not s or str(s) == "nan":
        return ""
    return re.sub(r"[^a-z0-9]", "", str(s).lower().strip())


def _best(*vals):
    for v in vals:
        if v is not None and str(v).strip() not in ("", "nan", "None", "NaN"):
            return v
    return ""


def _clean_series(s):
    """Strip embedded newlines/artifacts from series names."""
    return str(s).split("\n")[0].strip() if s else ""


def calc_hours(pages):
    try:
        return round(float(pages) * 300 / 9600, 2) if float(pages) > 0 else 0.0
    except (TypeError, ValueError):
        return 0.0


def assign_tier(gr_ratings, hours):
    try:
        ratings = float(gr_ratings) if gr_ratings else 0
    except (TypeError, ValueError):
        ratings = 0

    for (tier, r_min, r_max, h_min, mg_min, mg_max, rs_min, rs_max) in BANDS:
        r_ok = ratings >= r_min and (r_max is None or ratings <= r_max)
        h_ok = hours >= h_min
        if r_ok and h_ok:
            if mg_min == 0 and mg_max == 0:
                mg_disp = "No MG"
            elif mg_min == 0:
                mg_disp = f"Up to ${mg_max:,}"
            else:
                mg_disp = f"${mg_min:,} - ${mg_max:,}"
            return tier, mg_min, mg_max, mg_disp, f"{rs_min}%", f"{rs_min}% - {rs_max}%"

    return 10, 0, 0, "No MG", "12%", "12% - 18%"


def classify_retention(last_c, first_c):
    try:
        fc, lc = float(first_c), float(last_c)
    except (TypeError, ValueError):
        return None, "N/A"
    if fc <= 0 or lc <= 0:
        return None, "N/A"
    if fc > 10_000 and lc < fc * 0.01:
        return None, "DATA_SCALE_MISMATCH"
    ratio = round(lc / fc, 4)
    if ratio >= 0.50:
        return ratio, "STRONG >=50%"
    elif ratio >= 0.30:
        return ratio, "MODERATE 30-50%"
    return ratio, "WEAK <30%"


def draft_email(author, series, mg_display, rev_share):
    first = author.split()[0] if author else "Author"
    if first.lower() in ("the", "a", "an", "dr", "mr", "ms", "mrs") or len(first) <= 1:
        first = "Author"

    # Use max of MG range as headline
    m = re.search(r"\$([0-9,]+)\s*$", mg_display)
    mg_headline = f"${m.group(1)}" if m else ("Revenue Share Only" if "No MG" in mg_display else mg_display)

    rs_match = re.match(r"(\d+)%", rev_share)
    rs_floor = rs_match.group(1) + "%" if rs_match else rev_share

    return EMAIL_TEMPLATE.format(
        salutation=first,
        series_name=series or "your series",
        mg_display=mg_headline,
        rev_share=rs_floor,
    )


# ─── Main ────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print(f"COMBINED SPORTS ROMANCE MASTER — {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 70)

    # ── Load sources ────────────────────────────────────────────────────
    df = pd.read_csv(SRC_OUTREACH)
    print(f"Loaded OUTREACH_READY: {len(df)} rows")

    amazon_df = None
    if SRC_AMAZON.exists():
        amazon_df = pd.read_csv(SRC_AMAZON)
        amazon_df.columns = [c.replace("\n", " ").strip() for c in amazon_df.columns]
        print(f"Loaded Amazon source: {len(amazon_df)} rows")

    april_df = None
    if SRC_APRIL_KU.exists():
        april_df = pd.read_csv(SRC_APRIL_KU)
        print(f"Loaded April KU: {len(april_df)} rows")

    # ── Merge retention proxy from Amazon ───────────────────────────────
    if amazon_df is not None:
        amazon_df["_mk"] = (
            amazon_df["Author Name"].fillna("").apply(_norm) + "||" +
            amazon_df["Book Series Name"].fillna("").apply(_norm)
        )
        df["_mk"] = (
            df["Final_Author_Name"].fillna("").apply(_norm) + "||" +
            df["Final_Book Series Name"].fillna("").apply(_norm)
        )
        merge_cols = ["_mk"]
        for c in ["Last Book Rating Count", "First Book Rating Count",
                   "T100_Mapping", "Books_Featured_Rank_Validation", "Num_Books_Featured"]:
            if c in amazon_df.columns:
                merge_cols.append(c)
        amz_small = amazon_df[merge_cols].drop_duplicates("_mk")
        df = df.merge(amz_small, on="_mk", how="left", suffixes=("", "_amz"))
        print(f"  Merged retention/T100 data for {df['Last Book Rating Count'].notna().sum()} rows")

    # ── Find NEW titles from April KU ───────────────────────────────────
    new_titles = []
    if april_df is not None:
        existing_authors = set(df["Final_Author_Name"].fillna("").apply(_norm))

        for _, r in april_df.iterrows():
            author_key = _norm(r.get("author", ""))
            if not author_key:
                continue
            # Check if any existing author contains this key (fuzzy)
            found = any(author_key == ea or
                        (len(author_key) > 4 and author_key in ea) or
                        (len(ea) > 4 and ea in author_key)
                        for ea in existing_authors if ea)
            if not found:
                new_titles.append(r)

        print(f"  New authors from April KU not in existing: {len(new_titles)}")
        for t in new_titles[:5]:
            print(f"    #{t['rank']:3d} | {t['title'][:50]:50} | {t['author']}")

    # ── Build unified dataframe ─────────────────────────────────────────
    print("\n▶ Building unified rows...")

    rows = []
    for _, r in df.iterrows():
        series = _clean_series(_best(r.get("Verified_Series_Name"),
                                     r.get("Final_Book Series Name")))
        gr_url = _best(r.get("Verified_Goodreads_Series_URL"))
        num_books = _best(r.get("Verified_Books_in_Series"),
                          r.get("Final_Books in Series"))
        first_book = _best(r.get("Verified_First_Book_Name"),
                           r.get("Final_First Book Name"))
        last_book = _best(r.get("Verified_Last_Book_Name"),
                          r.get("Final_Last Book Name"))
        hours = float(r.get("Final_Length of Adaption in Hours", 0) or 0)
        if hours == 0:
            hours = calc_hours(r.get("Final_Total Pages", 0))
        gr_ratings = float(r.get("First Book Rating Count", 0) or 0)

        tier, mg_min, mg_max, mg_disp, rs_pct, rs_range = assign_tier(gr_ratings, hours)

        # Rating category
        if gr_ratings >= 20_000:
            rate_cat = "Tier 1 (>=20k)"
        elif gr_ratings >= 5_000:
            rate_cat = "Tier 2 (>=5k)"
        else:
            rate_cat = "Tier 3 (<5k)"

        # Retention proxy
        ret_proxy, ret_quality = classify_retention(
            r.get("Last Book Rating Count"),
            r.get("First Book Rating Count")
        )

        # KU list data
        t100 = str(r.get("T100_Mapping", "")).strip()
        feb_ku = t100 if t100 and t100 not in ("nan", "No List") else ""

        apr_ku = ""
        apr_rank = ""
        if april_df is not None:
            author_key = _norm(str(r.get("Final_Author_Name", "")))
            author_last = author_key.split()[-1] if author_key.split() else ""
            if author_last and len(author_last) > 2:
                matches = april_df[
                    april_df["author"].fillna("").apply(_norm).str.contains(
                        author_last, regex=False)
                ]
                if not matches.empty:
                    best = matches.nsmallest(1, "rank").iloc[0]
                    apr_ku = f"{best['category']} #{best['rank']}"
                    apr_rank = str(best["rank"])

        # Email
        has_contact = (
            str(r.get("Validated_Email", "")).strip() not in ("", "nan") or
            str(r.get("Agent_Email", "")).strip() not in ("", "nan") or
            str(r.get("Other_Contact_Email", "")).strip() not in ("", "nan")
        )
        email_draft = ""
        if has_contact and mg_disp:
            email_draft = draft_email(
                str(r.get("Final_Author_Name", "")),
                series, mg_disp, rs_range
            )

        # Determine outreach-readiness
        if hours >= 40 and gr_ratings >= 2000 and has_contact:
            outreach_status = "READY"
        elif hours >= 40 and has_contact:
            outreach_status = "READY (low ratings)"
        elif hours >= 40:
            outreach_status = "NEED CONTACT"
        elif hours > 0:
            outreach_status = "BELOW LENGTH THRESHOLD"
        else:
            outreach_status = "MISSING DATA"

        rows.append({
            # ── Picks for Licensing columns (Romantasy-aligned) ──
            "Show Name":                    series,
            "Author Name":                  str(r.get("Final_Author_Name", "")),
            "Approx Length (Hrs)":          hours,
            "KU 100 Category":              feb_ku,
            "Source List":                  "Ice Hockey & Sports Romance",
            "Genre":                        "Drama Romance",
            "Sub Genre":                    "Ice Hockey / Sports Romance",
            "Goodreads Series URL":         gr_url,
            "# of Books in Series":         num_books,
            "First Book Name":              first_book,
            "Last Book Name":               last_book,
            "First Book Rating (Stars)":    r.get("First Book Rating", ""),
            "First Book GR Ratings (#)":    gr_ratings if gr_ratings > 0 else "",
            "Rating Categorisation":        rate_cat,
            "20-40H Avg Retention":         ret_proxy if ret_quality not in ("N/A", "DATA_SCALE_MISMATCH") else "",
            "Retention Quality":            ret_quality,
            "Total Pages":                  r.get("Final_Total Pages", ""),
            "Series Type":                  _best(r.get("Final_Type")),
            "Series Era":                   r.get("Series_Era", ""),
            "Self Pub Flag":                r.get("Self Pub Flag", ""),
            "Publisher / Author Name":      r.get("Publisher Name", ""),

            # ── Priority & Commissioning ──
            "Priority Band":                r.get("Commissioning_Rank", ""),
            "Priority Order":               r.get("Commissioning_Score", ""),
            "Outreach Status":              outreach_status,

            # ── Commercial Bands (MG/Rev Share) ──
            "Trope":                        "Needgap",
            "Commercial Tier":              tier,
            "MG Min ($)":                   mg_min,
            "MG Max ($)":                   mg_max,
            "MG Range":                     mg_disp,
            "Rev Share (%)":                rs_pct,
            "Rev Share Range":              rs_range,

            # ── KU Rankings ──
            "KU Feb 2026":                  feb_ku,
            "KU April 2026":                apr_ku,
            "Amazon Best Rank":             apr_rank,
            "Featured Books":               str(r.get("Books_Featured_Rank_Validation", "")).replace("nan", ""),

            # ── Contact & Outreach ──
            "Author Email ID":              _best(r.get("Validated_Email")),
            "Email Verified":               r.get("Email_Verified", ""),
            "Email Source":                 _best(r.get("Email_Source_URL")),
            "Agency Email ID":              _best(r.get("Agent_Email")),
            "Agent Name":                   _best(r.get("Agent_Name")),
            "Agency Contact":               _best(r.get("Agency_Contact")),
            "Other Contact Email":          _best(r.get("Other_Contact_Email")),
            "Author Website":               _best(r.get("Validated_Website")),
            "Contact Description":          _best(r.get("Contact_Description")),

            # ── Outreach Tracking ──
            "First Outreach Sent Date":     "",
            "Outreach Channel":             "",
            "Response Status":              "",
            "Response Date":                "",
            "Status":                       "",
            "Warm Lead?":                   "",
            "Boilerplate Sent?":            "",

            # ── Data Quality ──
            "Data Quality Flag":            r.get("Data_Quality_Flag", ""),
            "Sanity Issues":                str(r.get("Sanity_Issues", "")).replace("nan", ""),

            # ── Draft Email ──
            "Draft Email":                  email_draft,
        })

    # ── Add new April KU titles ──────────────────────────────────────
    for r in new_titles:
        title = str(r.get("title", ""))
        author = str(r.get("author", ""))
        rank = r.get("rank", 0)
        sport_tag = r.get("sport_tag", "Sports Romance")

        rows.append({
            "Show Name":                    title,
            "Author Name":                  author,
            "Approx Length (Hrs)":          "",
            "KU 100 Category":              "",
            "Source List":                  "April 2026 KU Scrape",
            "Genre":                        "Drama Romance",
            "Sub Genre":                    f"{sport_tag}",
            "Goodreads Series URL":         "",
            "# of Books in Series":         "",
            "First Book Name":              title,
            "Last Book Name":               "",
            "First Book Rating (Stars)":    "",
            "First Book GR Ratings (#)":    "",
            "Rating Categorisation":        "",
            "20-40H Avg Retention":         "",
            "Retention Quality":            "",
            "Total Pages":                  "",
            "Series Type":                  "",
            "Series Era":                   "",
            "Self Pub Flag":                "",
            "Publisher / Author Name":      author,
            "Priority Band":                "NEW — NEEDS RESEARCH",
            "Priority Order":               "",
            "Outreach Status":              "NEW — NEEDS ENRICHMENT",
            "Trope":                        "Needgap",
            "Commercial Tier":              "",
            "MG Min ($)":                   "",
            "MG Max ($)":                   "",
            "MG Range":                     "",
            "Rev Share (%)":                "",
            "Rev Share Range":              "",
            "KU Feb 2026":                  "",
            "KU April 2026":                f"Sports Romance #{rank}",
            "Amazon Best Rank":             rank,
            "Featured Books":               "",
            "Author Email ID":              "",
            "Email Verified":               "",
            "Email Source":                 "",
            "Agency Email ID":              "",
            "Agent Name":                   "",
            "Agency Contact":               "",
            "Other Contact Email":          "",
            "Author Website":               "",
            "Contact Description":          "",
            "First Outreach Sent Date":     "",
            "Outreach Channel":             "",
            "Response Status":              "",
            "Response Date":                "",
            "Status":                       "",
            "Warm Lead?":                   "",
            "Boilerplate Sent?":            "",
            "Data Quality Flag":            "NEW — UNVERIFIED",
            "Sanity Issues":                "",
            "Draft Email":                  "",
        })

    master = pd.DataFrame(rows)
    print(f"\n  Combined master: {len(master)} rows "
          f"({len(df)} existing + {len(new_titles)} new)")

    # ── Sort: Priority Band then Score ──────────────────────────────
    rank_order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3, "P5": 4,
                  "NEW — NEEDS RESEARCH": 5}
    master["_sort"] = master["Priority Band"].map(rank_order).fillna(6)
    master = master.sort_values(["_sort", "Priority Order"],
                                ascending=[True, False]).drop(columns=["_sort"])

    # ── Self-checks ─────────────────────────────────────────────────
    print("\n▶ Self-checks:")

    # Check no unfilled placeholders in emails
    drafted = master[master["Draft Email"].str.len() > 50]
    placeholder_issues = drafted["Draft Email"].apply(
        lambda x: bool(re.search(r"\{[a-z_]+\}", str(x)))
    ).sum()
    print(f"  Email placeholder check: {placeholder_issues} issues")

    # Dedup check
    dups = master.duplicated(subset=["Author Name", "Show Name"]).sum()
    print(f"  Duplicate Author+Series: {dups}")

    # Tier distribution
    print(f"  Tier distribution: {master['Commercial Tier'].value_counts().to_dict()}")

    # Outreach status
    print(f"  Outreach status:")
    for status, count in master["Outreach Status"].value_counts().items():
        print(f"    {status}: {count}")

    # ── Write XLSX ──────────────────────────────────────────────────
    print(f"\n▶ Writing multi-sheet XLSX...")

    with pd.ExcelWriter(COMBINED_XLSX, engine="openpyxl") as writer:

        # ── Sheet 1: Picks for Licensing (main outreach) ─────────
        picks_cols = [
            "Priority Band", "Priority Order", "Show Name", "Author Name",
            "Approx Length (Hrs)", "# of Books in Series", "First Book Name",
            "Last Book Name", "First Book Rating (Stars)", "First Book GR Ratings (#)",
            "Rating Categorisation", "20-40H Avg Retention", "Retention Quality",
            "Sub Genre", "Series Type", "Series Era",
            "KU 100 Category", "KU Feb 2026", "KU April 2026", "Amazon Best Rank",
            "Trope", "Commercial Tier", "MG Range", "Rev Share (%)", "Rev Share Range",
            "Outreach Status",
            "Publisher / Author Name", "Self Pub Flag",
            "Author Email ID", "Email Verified", "Agency Email ID", "Agent Name",
            "Author Website", "Contact Description",
            "First Outreach Sent Date", "Outreach Channel",
            "Response Status", "Response Date", "Status",
            "Warm Lead?", "Boilerplate Sent?",
            "Data Quality Flag",
            "Draft Email",
        ]
        master[picks_cols].to_excel(writer, sheet_name="Picks for Licensing",
                                    index=False, freeze_panes=(1, 0))

        # ── Sheet 2: Working Sheet (detailed data) ────────────────
        working_cols = [
            "Show Name", "Author Name", "Goodreads Series URL",
            "Source List", "Genre", "Sub Genre",
            "# of Books in Series", "Total Pages", "Approx Length (Hrs)",
            "First Book Name", "First Book Rating (Stars)", "First Book GR Ratings (#)",
            "Last Book Name",
            "Rating Categorisation", "20-40H Avg Retention", "Retention Quality",
            "Series Type", "Series Era", "Self Pub Flag",
            "Trope", "Commercial Tier", "MG Min ($)", "MG Max ($)", "MG Range",
            "Rev Share (%)", "Rev Share Range",
            "KU Feb 2026", "KU April 2026", "Amazon Best Rank", "Featured Books",
            "Priority Band", "Priority Order", "Outreach Status",
            "Data Quality Flag", "Sanity Issues",
            "Email Source", "Other Contact Email", "Agency Contact",
        ]
        master[working_cols].to_excel(writer, sheet_name="Working Sheet",
                                      index=False, freeze_panes=(1, 0))

        # ── Sheet 3: Commercial Bands ─────────────────────────────
        bands_data = pd.DataFrame([
            {"Tier": 1, "GR Ratings": ">=20,000", "Trope": "Needgap", "Length (Hrs)": "80+",
             "MG Min ($)": 17500, "MG Max ($)": 25000, "Rev Share Min": "15%", "Rev Share Max": "22%"},
            {"Tier": 2, "GR Ratings": ">=20,000", "Trope": "Needgap", "Length (Hrs)": "50+",
             "MG Min ($)": 17500, "MG Max ($)": 20000, "Rev Share Min": "15%", "Rev Share Max": "22%"},
            {"Tier": 3, "GR Ratings": ">=20,000", "Trope": "Similar", "Length (Hrs)": "80+",
             "MG Min ($)": 10000, "MG Max ($)": 15000, "Rev Share Min": "15%", "Rev Share Max": "22%"},
            {"Tier": 4, "GR Ratings": ">=20,000", "Trope": "Similar", "Length (Hrs)": "50+",
             "MG Min ($)": 10000, "MG Max ($)": 12500, "Rev Share Min": "15%", "Rev Share Max": "22%"},
            {"Tier": 5, "GR Ratings": ">=5,000", "Trope": "Needgap", "Length (Hrs)": "80+",
             "MG Min ($)": 12500, "MG Max ($)": 17500, "Rev Share Min": "15%", "Rev Share Max": "20%"},
            {"Tier": 6, "GR Ratings": ">=5,000", "Trope": "Needgap", "Length (Hrs)": "50+",
             "MG Min ($)": 5000, "MG Max ($)": 7500, "Rev Share Min": "15%", "Rev Share Max": "20%"},
            {"Tier": 7, "GR Ratings": ">=5,000", "Trope": "Similar", "Length (Hrs)": "80+",
             "MG Min ($)": 5000, "MG Max ($)": 7500, "Rev Share Min": "15%", "Rev Share Max": "20%"},
            {"Tier": 8, "GR Ratings": ">=5,000", "Trope": "Similar", "Length (Hrs)": "50+",
             "MG Min ($)": 0, "MG Max ($)": 1000, "Rev Share Min": "12%", "Rev Share Max": "18%"},
            {"Tier": 9, "GR Ratings": "<5,000", "Trope": "Needgap", "Length (Hrs)": "50+",
             "MG Min ($)": 0, "MG Max ($)": 1000, "Rev Share Min": "12%", "Rev Share Max": "18%"},
            {"Tier": 10, "GR Ratings": "<5,000", "Trope": "Similar", "Length (Hrs)": "Any",
             "MG Min ($)": 0, "MG Max ($)": 0, "Rev Share Min": "12%", "Rev Share Max": "18%"},
        ])
        bands_data.to_excel(writer, sheet_name="Commercial Bands",
                            index=False, freeze_panes=(1, 0))

        # ── Sheet 4: April KU Rankings ────────────────────────────
        if april_df is not None:
            april_df.to_excel(writer, sheet_name="April KU Rankings",
                              index=False, freeze_panes=(1, 0))

        # ── Sheet 5: Selection Logic ──────────────────────────────
        logic = pd.DataFrame([
            {"Step": 1, "Filter": "Source Universe",
             "Criteria": "KU Top 100 (Sports Romance), BookScan, Goodreads shelf crawl",
             "Result": f"{len(df)} titles from prior pipeline + {len(new_titles)} new from April KU"},
            {"Step": 2, "Filter": "Format Classification",
             "Criteria": "Series only (no standalones). Minimum 2+ books in series.",
             "Result": "Standalones removed"},
            {"Step": 3, "Filter": "Show Length",
             "Criteria": "40+ hours (basis book word count at 300 words/page, 160wpm)",
             "Result": f"{(master['Approx Length (Hrs)'].apply(lambda x: float(x) if str(x).replace('.','').isdigit() else 0) >= 40).sum()} titles pass"},
            {"Step": 4, "Filter": "Audience Scale",
             "Criteria": "Book 1 GR ratings >= 2,000 (consideration), >= 3,000 (strong interest), >= 20,000 (top priority)",
             "Result": "Tiers 1-3 assigned"},
            {"Step": 5, "Filter": "Retention",
             "Criteria": "Ratio of Last Book ratings / First Book ratings (proxy for engagement)",
             "Result": "Strong/Moderate/Weak/N/A classification"},
            {"Step": 6, "Filter": "Commercial Tier",
             "Criteria": "GR Ratings x Length x Trope -> Tier 1-10, then MG range + Rev Share",
             "Result": "MG and Rev Share assigned per row"},
            {"Step": 7, "Filter": "Outreach Readiness",
             "Criteria": "Has verified email or agent contact -> Draft email generated",
             "Result": f"{(master['Draft Email'].str.len() > 50).sum()} draft emails generated"},
        ])
        logic.to_excel(writer, sheet_name="Selection Logic",
                        index=False, freeze_panes=(1, 0))

        # ── Format column widths ──────────────────────────────────
        for ws_name in writer.sheets:
            ws = writer.sheets[ws_name]
            for col_cells in ws.columns:
                max_len = max(len(str(c.value or "")) for c in col_cells)
                col_letter = col_cells[0].column_letter
                ws.column_dimensions[col_letter].width = min(max_len + 3, 50)

    # Also save flat CSV
    master.to_csv(COMBINED_CSV, index=False)

    print(f"\n  XLSX: {COMBINED_XLSX}")
    print(f"  CSV:  {COMBINED_CSV}")

    # ── Final summary ───────────────────────────────────────────────
    ready = (master["Outreach Status"] == "READY").sum()
    ready_low = (master["Outreach Status"] == "READY (low ratings)").sum()
    need_contact = (master["Outreach Status"] == "NEED CONTACT").sum()
    below = (master["Outreach Status"] == "BELOW LENGTH THRESHOLD").sum()
    new = (master["Outreach Status"] == "NEW — NEEDS ENRICHMENT").sum()

    print(f"\n{'='*70}")
    print(f"COMBINED MASTER: {len(master)} total rows")
    print(f"  READY for outreach:     {ready}")
    print(f"  READY (low ratings):    {ready_low}")
    print(f"  Need contact research:  {need_contact}")
    print(f"  Below 40h threshold:    {below}")
    print(f"  New (need enrichment):  {new}")
    print(f"  Draft emails generated: {(master['Draft Email'].str.len() > 50).sum()}")
    print(f"{'='*70}")


if __name__ == "__main__":
    main()
