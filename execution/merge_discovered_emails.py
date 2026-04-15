#!/usr/bin/env python3
"""
Merge Discovered Emails into Combined Master
=============================================
Takes results from:
  1. Gemini grounded search (email_discovery_cache.json)
  2. Google AI search (google_ai_search_cache.json)

Merges into the Combined Master CSV/XLSX and regenerates
draft emails for newly-found contacts.

Priority: Gemini result > Google AI result > existing data
Personal email > Agent email > Generic email

Usage:
  python3 execution/merge_discovered_emails.py
"""

import json, re
from pathlib import Path
from datetime import datetime

import pandas as pd
from openpyxl import load_workbook

PROJECT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT / "outreach" / "sports-romance"
MASTER_CSV = OUT_DIR / "exports" / "Sports_Romance_Combined_Master.csv"
MASTER_XLSX = OUT_DIR / "exports" / "Sports_Romance_Combined_Master.xlsx"
GEMINI_CACHE = OUT_DIR / "source" / "email_discovery_cache.json"
GOOGLE_CACHE = OUT_DIR / "source" / "google_ai_search_cache.json"

EMAIL_TPL = """\
Dear {sal},

I'm reaching out from Pocket FM regarding the {series} series. We believe this series \
has strong potential for adaptation into serialized audio, a rapidly growing format \
structurally distinct from traditional audiobooks. We'd like to propose a partnership that \
includes a License Fee of {mg} and {rs} share of Revenue, details on the full offer are below.

We understand many authors have granted exclusive audiobook rights (often to Audible). We \
are not seeking audiobook rights. Instead, we license serialized audio series adaptation \
rights, which are closer in nature to a TV dramatization, but in audio form, than to a \
traditional narrated audiobook.

Rather than recording the manuscript verbatim, we create a scripted, episodic adaptation \
featuring dual-cast or full-cast performances, cinematic sound design, and structured \
cliffhanger arcs. Episodes run ~10 minutes and are released serially, with \
progression-based monetization. This format functions as an incremental revenue stream \
alongside print, ebook, KU, and audiobook sales, not in competition with them.

Given the layered world-building and sustained narrative momentum in {series}, we see \
strong alignment with this dramatized serialized format.

Pocket FM is the world's largest dedicated audio series platform, trusted by leading \
publishers and IP holders including Naver, Kakao, Blackstone, Recorded Books, Aethon, and \
China Literature.

We are proposing:

License Fee: {mg}
Revenue Share: {rs} of Revenue
Exclusive serialized audio series adaptation rights (distinct from audiobook rights)
English language (U.S./U.K.)
5-10 year term
Full IP ownership retained by the author

If of interest, I'd be glad to schedule a brief call to discuss further.

Best regards,
Pulaksh Khimesara
US Licensing & Commissioning
Pocket FM"""


def draft_email(author, series, mg, rs):
    first = author.split()[0] if author else "Author"
    if first.lower() in ("the", "a", "an", "dr", "mr", "ms", "mrs") or len(first) <= 1:
        first = "Author"
    m = re.search(r"\$([0-9,]+)\s*$", str(mg))
    mg_h = f"${m.group(1)}" if m else ("Revenue Share Only" if "No MG" in str(mg) else str(mg))
    rs_m = re.match(r"(\d+)%", str(rs))
    rs_f = rs_m.group(1) + "%" if rs_m else str(rs)
    return EMAIL_TPL.format(sal=first, series=series or "your series", mg=mg_h, rs=rs_f)


def main():
    print("=" * 70)
    print(f"MERGE DISCOVERED EMAILS — {datetime.now():%Y-%m-%d %H:%M}")
    print("=" * 70)

    df = pd.read_csv(MASTER_CSV)
    print(f"Master: {len(df)} rows")

    # Load caches
    gemini = {}
    if GEMINI_CACHE.exists():
        gemini = json.loads(GEMINI_CACHE.read_text())
        print(f"Gemini cache: {len(gemini)} entries")

    google = {}
    if GOOGLE_CACHE.exists():
        google = json.loads(GOOGLE_CACHE.read_text())
        print(f"Google AI cache: {len(google)} entries")

    # Merge
    updated = 0
    new_emails = 0
    new_agents = 0
    new_drafts = 0

    for idx, row in df.iterrows():
        author = str(row.get("Author Name", "")).strip()
        key = author.lower().strip()

        has_email = str(row.get("Author Email ID", "")).strip() not in ("", "nan")
        has_agent = str(row.get("Agency Email ID", "")).strip() not in ("", "nan")

        if has_email and has_agent:
            continue

        # Look up in caches (Gemini first, then Google)
        found = gemini.get(key, {})
        gfound = google.get(key, {})

        new_email = ""
        new_agent_email = ""
        new_agent_name = ""
        new_website = ""
        source = ""

        # Author email: prefer Gemini, fallback Google
        if not has_email:
            if found.get("email"):
                new_email = found["email"]
                source = found.get("email_source", "gemini")
            elif gfound.get("email"):
                new_email = gfound["email"]
                source = gfound.get("email_source", "google_ai")

        # Agent email
        if not has_agent:
            if found.get("agent_email"):
                new_agent_email = found["agent_email"]
                new_agent_name = found.get("agent_name", "")
            elif gfound.get("agent_email"):
                new_agent_email = gfound["agent_email"]
                new_agent_name = gfound.get("agent_name", "")

        # Website
        if str(row.get("Author Website", "")).strip() in ("", "nan"):
            new_website = found.get("website", "") or gfound.get("website", "")

        # Apply updates
        if new_email:
            df.at[idx, "Author Email ID"] = new_email
            df.at[idx, "Email Verified"] = f"Discovered ({source})"
            if source:
                df.at[idx, "Email Source"] = source
            new_emails += 1
            updated += 1

        if new_agent_email:
            df.at[idx, "Agency Email ID"] = new_agent_email
            if new_agent_name:
                df.at[idx, "Agent Name"] = new_agent_name
            new_agents += 1
            updated += 1

        if new_website:
            df.at[idx, "Author Website"] = new_website

        # Regenerate draft email if we now have contact
        has_contact_now = (
            str(df.at[idx, "Author Email ID"]).strip() not in ("", "nan") or
            str(df.at[idx, "Agency Email ID"]).strip() not in ("", "nan")
        )
        existing_draft = str(row.get("Draft Email", "")).strip()
        if has_contact_now and (not existing_draft or existing_draft == "nan" or len(existing_draft) < 50):
            series = str(row.get("Show Name", ""))
            mg = str(row.get("MG Range", ""))
            rs = str(row.get("Rev Share Range", ""))
            if mg and mg != "nan":
                df.at[idx, "Draft Email"] = draft_email(author, series, mg, rs)
                new_drafts += 1

        # Update outreach status
        if has_contact_now and str(row.get("Outreach Status", "")) in ("NEED CONTACT", "MISSING DATA"):
            hours = float(row.get("Approx Length (Hrs)", 0) or 0) if str(row.get("Approx Length (Hrs)", "")) not in ("", "nan") else 0
            if hours >= 40:
                df.at[idx, "Outreach Status"] = "READY"

    # Save
    df.to_csv(MASTER_CSV, index=False)
    print(f"\nUpdated CSV: {MASTER_CSV}")

    # Rebuild XLSX (overwrite Picks for Licensing and Working Sheet)
    # For now just save CSV — full XLSX rebuild can be done separately

    # Summary
    print(f"\n{'='*70}")
    print(f"MERGE COMPLETE")
    print(f"  Rows updated: {updated}")
    print(f"  New author emails: {new_emails}")
    print(f"  New agent emails: {new_agents}")
    print(f"  New draft emails: {new_drafts}")
    print()

    # Contact coverage after merge
    has_email_final = df["Author Email ID"].apply(lambda x: str(x).strip() not in ("", "nan")).sum()
    has_agent_final = df["Agency Email ID"].apply(lambda x: str(x).strip() not in ("", "nan")).sum()
    has_any = df.apply(lambda r: str(r.get("Author Email ID", "")).strip() not in ("", "nan") or
                                  str(r.get("Agency Email ID", "")).strip() not in ("", "nan"), axis=1).sum()
    has_draft = df["Draft Email"].apply(lambda x: len(str(x)) > 50).sum()

    print(f"  Contact coverage (final):")
    print(f"    Author email: {has_email_final}")
    print(f"    Agent email: {has_agent_final}")
    print(f"    ANY contact: {has_any} / {len(df)}")
    print(f"    Draft emails: {has_draft}")
    print(f"{'='*70}")

    # Show by priority
    print(f"\n  By priority band:")
    for p in ["P0", "P1", "P2", "P2 (April KU)", "P3", "P5"]:
        sub = df[df["Priority Band"] == p]
        contact = sub.apply(lambda r: str(r.get("Author Email ID","")).strip() not in ("","nan") or
                                       str(r.get("Agency Email ID","")).strip() not in ("","nan"), axis=1).sum()
        print(f"    {p}: {contact}/{len(sub)} have contact")


if __name__ == "__main__":
    main()
