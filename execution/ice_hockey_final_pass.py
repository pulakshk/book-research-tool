#!/usr/bin/env python3
"""
Ice Hockey — Final verification pass.

Merges all existing verified data from prior runs, then uses Gemini+Goodreads
ONLY for the remaining gaps. Produces the truly final outreach-ready file.

Input:  exports/ice_hockey_FINAL_OUTREACH.csv (615 rows, locally cleaned)
        exports/amazon_hockey_cleaned_titles_repaired.csv (prior verified data)
        exports/ice_hockey_master_contacts_verified.csv (prior verified data)
Output: exports/ice_hockey_OUTREACH_READY.csv
        verified/ice_hockey_OUTREACH_READY.xlsx
"""

import json, os, re, time, warnings
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

PROJECT = Path(__file__).resolve().parents[1]
OUT = PROJECT / "outreach" / "ice-hockey"
CLEANED_CSV = OUT / "exports" / "ice_hockey_FINAL_OUTREACH.csv"
AMAZON_CSV = OUT / "exports" / "amazon_hockey_cleaned_titles_repaired.csv"
MASTER_CSV = OUT / "exports" / "ice_hockey_master_contacts_verified.csv"
OUTPUT_CSV = OUT / "exports" / "ice_hockey_OUTREACH_READY.csv"
OUTPUT_XLSX = OUT / "verified" / "ice_hockey_OUTREACH_READY.xlsx"
CACHE = OUT / "progress" / "final_pass_cache.json"

# API
def _api_key():
    f = PROJECT / ".env"
    if f.exists():
        for line in f.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = _api_key()
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"}
SESSION = requests.Session()
SESSION.headers.update(HEADERS)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_LAST_CALL = 0.0

_BAD_EMAILS = {"user@domain.com", "email@example.com", "author@directauthor.com", "info@therateabc.com"}
_BAD_DOMAINS = {"example.com", "domain.com", "email.com", "test.com"}
_PUBLICITY = {"publicity", "admin", "press", "media", "marketing", "pr"}
_SOCIAL = {"facebook.com", "twitter.com", "x.com", "instagram.com", "tiktok.com",
           "bookbub.com", "goodreads.com", "amazon.com", "wikipedia.org", "youtube.com"}

def _valid_email(e):
    e = e.strip().lower()
    if not e or "@" not in e or e in _BAD_EMAILS:
        return False
    return e.split("@")[-1] not in _BAD_DOMAINS

def _pick_email(emails, author=""):
    valid = [e for e in emails if _valid_email(e)]
    if not valid: return ""
    parts = [p.lower() for p in author.split() if len(p) > 2]
    for e in valid:
        local = e.split("@")[0].lower().replace(".", "").replace("_", "")
        if any(p in local for p in parts): return e
    return valid[0]

def _safe_get(url, timeout=12):
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        return r if r.status_code == 200 else None
    except: return None

def _emails_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style"]): t.decompose()
    return EMAIL_RE.findall(soup.get_text(" ", strip=True))

def scrape_author_email(website, author=""):
    if not website or not str(website).startswith("http"): return "", ""
    base = str(website).rstrip("/")
    for path in ["", "/contact", "/contact-me", "/about", "/connect"]:
        r = _safe_get(base + path)
        if not r: time.sleep(0.3); continue
        emails = [e for e in _emails_from_html(r.text)
                  if _valid_email(e) and e.split("@")[0].lower() not in _PUBLICITY]
        email = _pick_email(emails, author)
        if email: return email, base + path
        time.sleep(0.3)
    return "", ""

def _gemini(prompt, retries=2):
    global _LAST_CALL
    if not GEMINI_KEY: return {}, ""
    wait = 4.0 - (time.time() - _LAST_CALL)
    if wait > 0: time.sleep(wait)
    _LAST_CALL = time.time()
    payload = {"contents": [{"parts": [{"text": prompt}]}],
               "tools": [{"google_search": {}}],
               "generationConfig": {"temperature": 0.0, "maxOutputTokens": 500}}
    for attempt in range(retries):
        try:
            r = requests.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload, timeout=30)
            if r.status_code == 429:
                time.sleep(45 * (attempt + 1)); continue
            if r.status_code != 200: return {}, ""
            data = r.json()
            cands = data.get("candidates", [])
            if not cands: return {}, ""
            text = " ".join(p.get("text","") for p in cands[0].get("content",{}).get("parts",[]))
            sources = []
            try:
                meta = cands[0].get("groundingMetadata", {})
                for c in meta.get("groundingChunks", []):
                    u = c.get("web", {}).get("uri", "")
                    if u: sources.append(u)
            except: pass
            return {"text": text, "sources": sources}, text
        except: time.sleep(5)
    return {}, ""

def gemini_find_email(author, website=""):
    prompt = (f'Find the official contact email for romance/fiction author "{author}". '
              f'Check official website, newsletter, BookBub profile. '
              f'Report ONLY exact email if publicly listed. Do NOT invent. '
              f'Format: EMAIL: <email or NONE> | AGENT: <agent or NONE> | WEBSITE: <url or NONE>')
    raw, text = _gemini(prompt)
    if not text: return {"email": "", "agent": "", "website": website, "source": ""}
    result = {"email": "", "agent": "", "website": website, "source": ""}
    em = re.search(r"EMAIL:\s*([^\|]+)", text, re.I)
    ag = re.search(r"AGENT:\s*([^\|]+)", text, re.I)
    ws = re.search(r"WEBSITE:\s*([^\|]+)", text, re.I)
    if em:
        found = EMAIL_RE.findall(em.group(1))
        candidate = _pick_email(found, author) if found else ""
        if candidate and _valid_email(candidate):
            non_social = [s for s in raw.get("sources", [])
                          if not any(d in s for d in _SOCIAL)]
            if non_social:
                result["email"] = candidate
                result["source"] = non_social[0]
            elif raw.get("sources"):
                # Try cross-validate
                resp = _safe_get(raw["sources"][0])
                if resp:
                    cross = [e for e in _emails_from_html(resp.text) if _valid_email(e)]
                    if candidate.lower() in [e.lower() for e in cross]:
                        result["email"] = candidate
                        result["source"] = raw["sources"][0]
    if ag:
        v = ag.group(1).strip()
        if v.upper() not in ("NONE", "N/A", "NOT FOUND", "UNKNOWN", ""):
            # Clean agent prose
            v = re.sub(r'https?://\S+', '', v)
            v = re.sub(r'[A-Za-z0-9._%+\-]+@\S+', '', v)
            v = re.sub(r'\*+', '', v)
            v = re.sub(r'\s+', ' ', v).strip().rstrip(' ,;:.')
            if len(v) > 3 and len(v) < 80: result["agent"] = v
    if ws and not website:
        v = ws.group(1).strip()
        if v.startswith("http") and v.upper() != "NONE": result["website"] = v
    return result

def gemini_verify_series(series, author, first_book=""):
    prompt = (f'On Goodreads, find the series containing "{first_book or series}" by "{author}". '
              f'What is the exact series name? How many primary works? '
              f'First and last book? Goodreads series URL? '
              f'If uncertain say UNKNOWN. '
              f'Format: SERIES: <name> | BOOKS: <number> | FIRST: <title> | LAST: <title> | URL: <url>')
    raw, text = _gemini(prompt)
    r = {"name": "", "books": None, "first": "", "last": "", "url": "", "source": "gemini"}
    if not text: return r
    def _c(m): v = m.group(1).strip() if m else ""; return "" if v.upper() in ("UNKNOWN","NONE","N/A","") else v
    sn = _c(re.search(r"SERIES:\s*([^\|]+)", text, re.I))
    bn = _c(re.search(r"BOOKS:\s*([^\|]+)", text, re.I))
    fn = _c(re.search(r"FIRST:\s*([^\|]+)", text, re.I))
    ln = _c(re.search(r"LAST:\s*([^\|]+)", text, re.I))
    un = _c(re.search(r"URL:\s*([^\|]+)", text, re.I))
    if sn: r["name"] = sn
    if bn:
        try: r["books"] = int(re.search(r"\d+", bn).group())
        except: pass
    if fn: r["first"] = fn
    if ln: r["last"] = ln
    if un and "goodreads.com/series" in un: r["url"] = un
    return r

# ─── Cache ─────────────────────────────────────────────────────────────────
def load_cache():
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    if CACHE.exists():
        try: return json.loads(CACHE.read_text())
        except: pass
    return {"authors": {}, "series": {}}

def save_cache(c):
    CACHE.write_text(json.dumps(c, indent=2, ensure_ascii=False))

# ─── Main ──────────────────────────────────────────────────────────────────
def main():
    print("=" * 70)
    print("ICE HOCKEY — FINAL PASS (merge + fill gaps)")
    print("=" * 70)

    # 1. Load cleaned file
    df = pd.read_csv(CLEANED_CSV)
    print(f"Cleaned file: {len(df)} rows")

    # 2. Build lookup from previous verified data
    print("\n[1] Building lookup from prior verified data …")
    author_lookup = {}  # author_key -> {email, agent, website, ...}
    series_lookup = {}  # author||series -> {name, books, first, last, url}

    # From Amazon repaired
    try:
        adf = pd.read_csv(AMAZON_CSV)
        for _, r in adf.iterrows():
            akey = str(r.get("_author_key", "") or "").strip().lower()
            if not akey: continue
            email = str(r.get("Validated_Email", "") or "").strip()
            agent_name = str(r.get("Agent_Name", "") or "").strip()
            agent_email = str(r.get("Agent_Email", "") or "").strip()
            website = str(r.get("Validated_Website", "") or "").strip()
            vsn = str(r.get("Verified_Series_Name", "") or "").strip()
            vbooks = r.get("Verified_Books_in_Series")
            vfb = str(r.get("Verified_First_Book_Name", "") or "").strip()
            vlb = str(r.get("Verified_Last_Book_Name", "") or "").strip()
            vurl = str(r.get("Verified_Goodreads_Series_URL", "") or "").strip()

            if akey not in author_lookup or (email and email != "nan"):
                if email and email != "nan" and _valid_email(email):
                    author_lookup[akey] = {"email": email, "agent_name": agent_name if agent_name != "nan" else "",
                                           "agent_email": agent_email if agent_email != "nan" else "",
                                           "website": website if website != "nan" else ""}
                elif akey not in author_lookup:
                    author_lookup[akey] = {"email": "", "agent_name": agent_name if agent_name != "nan" else "",
                                           "agent_email": agent_email if agent_email != "nan" else "",
                                           "website": website if website != "nan" else ""}

            skey = str(r.get("_series_key", "") or "").strip().lower()
            if vsn and vsn != "nan" and skey:
                series_lookup[skey] = {"name": vsn, "books": vbooks if pd.notna(vbooks) else None,
                                       "first": vfb if vfb != "nan" else "",
                                       "last": vlb if vlb != "nan" else "",
                                       "url": vurl if vurl != "nan" else ""}
        print(f"  Amazon: {len(adf)} rows → {len(author_lookup)} author contacts, {len(series_lookup)} series")
    except Exception as e:
        print(f"  Amazon load error: {e}")

    # From master contacts
    try:
        mdf = pd.read_csv(MASTER_CSV)
        for _, r in mdf.iterrows():
            akey = str(r.get("_author_key", "") or "").strip().lower()
            email = str(r.get("Validated_Email", "") or "").strip()
            if akey and email and email != "nan" and _valid_email(email) and akey not in author_lookup:
                website = str(r.get("Validated_Website", "") or "").strip()
                agent = str(r.get("Agency_Contact", "") or "").strip()
                author_lookup[akey] = {"email": email, "agent_name": agent if agent != "nan" else "",
                                       "agent_email": "", "website": website if website != "nan" else ""}
            skey = str(r.get("_series_key", "") or "").strip().lower()
            vsn = str(r.get("Verified_Series_Name", "") or "").strip()
            if skey and vsn and vsn != "nan" and skey not in series_lookup:
                vbooks = r.get("Verified_Books_in_Series")
                series_lookup[skey] = {"name": vsn, "books": vbooks if pd.notna(vbooks) else None,
                                       "first": str(r.get("Verified_First_Book_Name", "") or ""),
                                       "last": str(r.get("Verified_Last_Book_Name", "") or ""),
                                       "url": str(r.get("Verified_Goodreads_Series_URL", "") or "")}
        print(f"  Master: extra entries merged → total {len(author_lookup)} authors, {len(series_lookup)} series")
    except Exception as e:
        print(f"  Master load error: {e}")

    # 3. Apply existing verified data to our cleaned file
    print("\n[2] Applying existing verified data …")
    applied_email = applied_series = 0
    for idx, row in df.iterrows():
        author = str(row.get("Final_Author_Name", "") or "").strip()
        akey = author.lower()
        series = str(row.get("Final_Book Series Name", "") or "").strip()

        # Try author lookup
        ad = author_lookup.get(akey, {})
        if ad.get("email") and (not row.get("Validated_Email") or str(row.get("Validated_Email", "")).strip() in ("", "nan")):
            df.at[idx, "Validated_Email"] = ad["email"]
            df.at[idx, "Email_Verified"] = True
            df.at[idx, "Email_Source_URL"] = "prior-verified-run"
            applied_email += 1
        if ad.get("agent_name") and (not row.get("Agent_Name") or str(row.get("Agent_Name", "")).strip() in ("", "nan")):
            df.at[idx, "Agent_Name"] = ad["agent_name"]
        if ad.get("agent_email") and (not row.get("Agent_Email") or str(row.get("Agent_Email", "")).strip() in ("", "nan")):
            df.at[idx, "Agent_Email"] = ad["agent_email"]
        if ad.get("website") and (not row.get("Validated_Website") or str(row.get("Validated_Website", "")).strip() in ("", "nan")):
            df.at[idx, "Validated_Website"] = ad["website"]

        # Try series lookup - multiple key formats
        for sk in [f"{akey}||{series.lower()}", f"{akey}||{str(row.get('Final_First Book Name','')).strip().lower()}"]:
            sd = series_lookup.get(sk, {})
            if sd.get("name") and (not row.get("Verified_Series_Name") or str(row.get("Verified_Series_Name", "")).strip() in ("", "nan")):
                df.at[idx, "Verified_Series_Name"] = sd["name"]
                if sd.get("books"): df.at[idx, "Verified_Books_in_Series"] = sd["books"]
                if sd.get("first"): df.at[idx, "Verified_First_Book_Name"] = sd["first"]
                if sd.get("last"): df.at[idx, "Verified_Last_Book_Name"] = sd["last"]
                if sd.get("url"): df.at[idx, "Verified_Goodreads_Series_URL"] = sd["url"]
                applied_series += 1
                break

    print(f"  Applied {applied_email} emails, {applied_series} series from prior data")

    # 4. Find remaining gaps
    no_email_mask = df["Validated_Email"].fillna("").astype(str).str.strip().isin(["", "nan"])
    no_vsn_mask = df["Verified_Series_Name"].fillna("").astype(str).str.strip().isin(["", "nan"])
    print(f"\n[3] Remaining gaps: {no_email_mask.sum()} no email, {no_vsn_mask.sum()} no series")

    # 5. Run Gemini for remaining author emails
    cache = load_cache()
    authors_needing_email = df[no_email_mask]["Final_Author_Name"].dropna().unique().tolist()
    print(f"\n[4] Verifying {len(authors_needing_email)} authors via web+Gemini …")

    for i, author in enumerate(authors_needing_email):
        akey = author.strip().lower()
        if akey in cache["authors"]: continue

        # Get website from sheet
        author_rows = df[df["Final_Author_Name"] == author]
        website = ""
        for _, r in author_rows.iterrows():
            w = str(r.get("Validated_Website", "") or "").strip()
            if w.startswith("http"): website = w; break

        print(f"  [{i+1}/{len(authors_needing_email)}] {author}", end="")

        # Step A: scrape website
        email, source = scrape_author_email(website, author)
        if email:
            cache["authors"][akey] = {"email": email, "source": source, "agent": "", "confirmed": True}
            print(f" → website: {email}")
        else:
            # Step B: Gemini
            gem = gemini_find_email(author, website)
            if gem["email"]:
                cache["authors"][akey] = {"email": gem["email"], "source": gem["source"],
                                          "agent": gem["agent"], "confirmed": True}
                print(f" → gemini: {gem['email']}")
            else:
                cache["authors"][akey] = {"email": "", "source": "", "agent": gem.get("agent", ""),
                                          "confirmed": False}
                if gem.get("agent"):
                    print(f" → no email, agent: {gem['agent'][:40]}")
                else:
                    print(f" → no contact found")

        if (i + 1) % 15 == 0: save_cache(cache)

    save_cache(cache)

    # 6. Run Gemini for remaining unverified series
    series_needing = (df[no_vsn_mask][["Final_Author_Name", "Final_Book Series Name", "Final_First Book Name"]]
                      .dropna(subset=["Final_Author_Name", "Final_Book Series Name"])
                      .drop_duplicates(subset=["Final_Author_Name", "Final_Book Series Name"])
                      .values.tolist())
    print(f"\n[5] Verifying {len(series_needing)} series via Gemini …")

    for i, (author, series, first_book) in enumerate(series_needing):
        skey = f"{str(author).lower()}||{str(series).lower()}"
        if skey in cache["series"]: continue

        fb = str(first_book) if pd.notna(first_book) else ""
        print(f"  [{i+1}/{len(series_needing)}] '{series}' by {author}", end="")

        sd = gemini_verify_series(str(series), str(author), fb)
        cache["series"][skey] = sd

        if sd["name"]:
            print(f" → '{sd['name']}', {sd['books']} books")
        else:
            print(f" → could not verify")

        if (i + 1) % 15 == 0: save_cache(cache)

    save_cache(cache)

    # 7. Apply new verified data
    print(f"\n[6] Applying new verified data …")
    new_emails = new_series = new_agents = 0
    for idx, row in df.iterrows():
        author = str(row.get("Final_Author_Name", "") or "").strip()
        akey = author.lower()
        series = str(row.get("Final_Book Series Name", "") or "").strip()
        skey = f"{akey}||{series.lower()}"

        # Author contact
        ad = cache["authors"].get(akey, {})
        if ad.get("email") and str(row.get("Validated_Email", "")).strip() in ("", "nan"):
            df.at[idx, "Validated_Email"] = ad["email"]
            df.at[idx, "Email_Verified"] = ad.get("confirmed", False)
            df.at[idx, "Email_Source_URL"] = ad.get("source", "")
            new_emails += 1
        if ad.get("agent") and str(row.get("Agent_Name", "")).strip() in ("", "nan"):
            df.at[idx, "Agent_Name"] = ad["agent"]
            new_agents += 1

        # Series
        sd = cache["series"].get(skey, {})
        if sd.get("name") and str(row.get("Verified_Series_Name", "")).strip() in ("", "nan"):
            df.at[idx, "Verified_Series_Name"] = sd["name"]
            if sd.get("books"): df.at[idx, "Verified_Books_in_Series"] = sd["books"]
            if sd.get("first"): df.at[idx, "Verified_First_Book_Name"] = sd["first"]
            if sd.get("last"): df.at[idx, "Verified_Last_Book_Name"] = sd["last"]
            if sd.get("url"): df.at[idx, "Verified_Goodreads_Series_URL"] = sd["url"]
            new_series += 1

    print(f"  New: {new_emails} emails, {new_agents} agents, {new_series} series")

    # 8. Rebuild Contact_Description and quality flags
    print(f"\n[7] Rebuilding quality flags …")
    for idx, row in df.iterrows():
        # Contact description
        parts = []
        for col, label in [("Validated_Email", "Email"), ("Agent_Email", "Agent email"),
                           ("Agent_Name", "Agent"), ("Validated_Website", "Website")]:
            v = str(row.get(col, "") or "").strip()
            if v and v != "nan":
                parts.append(f"{label}: {v}")
        df.at[idx, "Contact_Description"] = " | ".join(parts) if parts else "No contact found"

        # Quality flag
        has_email = str(row.get("Validated_Email", "")).strip() not in ("", "nan")
        has_vsn = str(row.get("Verified_Series_Name", "")).strip() not in ("", "nan")
        issues = str(row.get("Sanity_Issues", "") or "")
        critical = any(k in issues for k in ["SAME_FIRST_LAST", "TYPE_MISMATCH", "PAGES_ZERO"])

        if critical:
            df.at[idx, "Data_Quality_Flag"] = "RED"
        elif not has_vsn:
            df.at[idx, "Data_Quality_Flag"] = "RED"
        elif not has_email and not str(row.get("Agent_Email", "")).strip() not in ("", "nan"):
            df.at[idx, "Data_Quality_Flag"] = "YELLOW"
        elif "FORMULAIC_PAGES" in issues or "BOOKS_SUSPICIOUS" in issues:
            df.at[idx, "Data_Quality_Flag"] = "YELLOW"
        else:
            df.at[idx, "Data_Quality_Flag"] = "GREEN" if has_email else "YELLOW"

    # 9. Sort and write
    print(f"\n[8] Writing final output …")
    rank_order = {"P0": 0, "P1": 1, "P2": 2, "P3": 3}
    df["_sort"] = df["Commissioning_Rank"].map(rank_order).fillna(9)
    df = df.sort_values(["_sort", "Commissioning_Score"], ascending=[True, False]).drop(columns=["_sort"])

    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"  CSV: {OUTPUT_CSV}")

    try:
        df.to_excel(OUTPUT_XLSX, index=False, sheet_name="Ice Hockey Outreach")
        print(f"  Excel: {OUTPUT_XLSX}")
    except Exception as e:
        print(f"  Excel error: {e}")

    # 10. Summary
    print(f"\n{'='*60}")
    print(f"FINAL SUMMARY")
    print(f"{'='*60}")
    total = len(df)
    has_email = (df["Validated_Email"].fillna("").astype(str).str.strip() != "").sum()
    has_agent_email = (df["Agent_Email"].fillna("").astype(str).str.strip().isin(["", "nan"]) == False).sum()
    has_agent = (df["Agent_Name"].fillna("").astype(str).str.strip().isin(["", "nan"]) == False).sum()
    has_vsn = (df["Verified_Series_Name"].fillna("").astype(str).str.strip().isin(["", "nan"]) == False).sum()
    green = (df["Data_Quality_Flag"] == "GREEN").sum()
    yellow = (df["Data_Quality_Flag"] == "YELLOW").sum()
    red = (df["Data_Quality_Flag"] == "RED").sum()
    p0 = (df["Commissioning_Rank"] == "P0").sum()
    p0e = ((df["Commissioning_Rank"] == "P0") & (df["Validated_Email"].fillna("") != "")).sum()

    print(f"  Total rows: {total}")
    print(f"  GREEN: {green} ({green/total*100:.0f}%)")
    print(f"  YELLOW: {yellow} ({yellow/total*100:.0f}%)")
    print(f"  RED: {red} ({red/total*100:.0f}%)")
    print(f"  Direct email: {has_email} ({has_email/total*100:.0f}%)")
    print(f"  Agent email: {has_agent_email}")
    print(f"  Agent name: {has_agent}")
    print(f"  Verified series: {has_vsn} ({has_vsn/total*100:.0f}%)")
    print(f"  P0: {p0} ({p0e} with email)")
    print(f"\nDone.")

if __name__ == "__main__":
    main()
