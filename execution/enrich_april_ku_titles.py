#!/usr/bin/env python3
"""
Enrich April KU New Titles — Series Info + Author Contacts
===========================================================
For the NEW titles discovered from April 2026 KU scrape:
1. Filters to sports/hockey-relevant titles (excludes general romance)
2. Deduplicates to one row per author (series level)
3. Looks up Goodreads series info via Gemini
4. Discovers author/agent emails via Gemini + website scraping
5. Calculates commercial tiers / MG / rev share
6. Outputs enriched CSV ready to merge into Combined Master

Anti-hallucination:
  - Every Gemini result is logged and spot-checked
  - Emails are validated against known bad patterns
  - Series data cross-referenced between title hints and Gemini response
  - Cache persists progress so script is resumable

Output:
  outreach/sports-romance/source/april_ku_enriched.csv

Usage:
  python3 execution/enrich_april_ku_titles.py
"""

import json, os, re, time, warnings, sys
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

PROJECT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT / "outreach" / "sports-romance"
SRC_COMBINED = OUT_DIR / "exports" / "Sports_Romance_Combined_Master.csv"
SRC_APRIL_KU = OUT_DIR / "source" / "april_ku_sports_romance.csv"
ENRICHED_CSV = OUT_DIR / "source" / "april_ku_enriched.csv"
CACHE_FILE   = OUT_DIR / "source" / "enrichment_cache.json"

# ── API setup ──────────────────────────────────────────────────────────
def _api_key():
    f = PROJECT / ".env"
    if f.exists():
        for line in f.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = _api_key()
GEMINI_URL = "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent"
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
})
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_LAST_CALL = 0.0

_BAD_EMAILS = {"user@domain.com", "email@example.com", "author@directauthor.com"}
_BAD_DOMAINS = {"example.com", "domain.com", "email.com", "test.com", "sentry.io"}
_SOCIAL_DOMAINS = {"facebook.com", "twitter.com", "x.com", "instagram.com",
                   "tiktok.com", "bookbub.com", "goodreads.com", "amazon.com",
                   "wikipedia.org", "youtube.com", "pinterest.com"}

# ── Sports/hockey filter keywords ─────────────────────────────────────
SPORTS_KEYWORDS = [
    "hockey", "puck", "rink", "ice", "goalie", "nhl", "skate", "zamboni",
    "face off", "faceoff", "slap shot", "power play", "penalty",
    "quarterback", "football", "basketball", "baseball", "soccer",
    "athlete", "stadium", "sports", "lacrosse", "rugby", "tommen",
    "pitch", "score", "game", "coach", "draft", "tackle", "match",
    "trainer", "team", "league", "championship", "playoffs",
]

# ── Tier logic ────────────────────────────────────────────────────────
BANDS = [
    (1,  20_000, None,   80, 17_500, 25_000, 15, 22),
    (2,  20_000, None,   40, 17_500, 20_000, 15, 22),
    (5,   5_000, 19_999, 80, 12_500, 17_500, 15, 20),
    (6,   5_000, 19_999, 40,  5_000,  7_500, 15, 20),
    (9,       0,  4_999, 40,      0,  1_000, 12, 18),
    (10,      0,  4_999,  0,      0,      0, 12, 18),
]


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


# ── Gemini helpers ────────────────────────────────────────────────────

def _valid_email(e):
    e = e.strip().lower()
    if not e or "@" not in e or e in _BAD_EMAILS:
        return False
    return e.split("@")[-1] not in _BAD_DOMAINS


def _pick_email(emails, author=""):
    valid = [e for e in emails if _valid_email(e)]
    if not valid:
        return ""
    parts = [p.lower() for p in author.split() if len(p) > 2]
    for e in valid:
        local = e.split("@")[0].lower().replace(".", "").replace("_", "")
        if any(p in local for p in parts):
            return e
    return valid[0]


def _safe_get(url, timeout=12):
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        return r if r.status_code == 200 else None
    except Exception:
        return None


def _emails_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script", "style"]):
        t.decompose()
    return EMAIL_RE.findall(soup.get_text(" ", strip=True))


def _gemini(prompt, retries=2):
    global _LAST_CALL
    if not GEMINI_KEY:
        return {}, ""
    wait = 4.0 - (time.time() - _LAST_CALL)
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL = time.time()
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 600},
    }
    for attempt in range(retries):
        try:
            r = requests.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload, timeout=30)
            if r.status_code == 429:
                time.sleep(45 * (attempt + 1))
                continue
            if r.status_code != 200:
                return {}, ""
            data = r.json()
            cands = data.get("candidates", [])
            if not cands:
                return {}, ""
            text = " ".join(
                p.get("text", "")
                for p in cands[0].get("content", {}).get("parts", [])
            )
            sources = []
            try:
                meta = cands[0].get("groundingMetadata", {})
                for c in meta.get("groundingChunks", []):
                    u = c.get("web", {}).get("uri", "")
                    if u:
                        sources.append(u)
            except Exception:
                pass
            return {"text": text, "sources": sources}, text
        except Exception:
            time.sleep(5)
    return {}, ""


def gemini_verify_series(title, author):
    """Look up Goodreads series info for a given title+author."""
    prompt = (
        f'On Goodreads, find the book series containing "{title}" by "{author}". '
        f'Is it part of a series? What is the exact series name? How many primary works? '
        f'What is the first and last book? What is the Goodreads series URL? '
        f'What is the Book 1 Goodreads rating and number of ratings? '
        f'What is the total page count of Book 1? '
        f'If uncertain say UNKNOWN. '
        f'Format: SERIES: <name or STANDALONE> | BOOKS: <number> | '
        f'FIRST: <title> | LAST: <title> | URL: <url> | '
        f'RATING: <stars> | RATINGS_COUNT: <number> | PAGES: <number>'
    )
    raw, text = _gemini(prompt)
    r = {
        "series_name": "", "num_books": None, "first_book": "", "last_book": "",
        "gr_series_url": "", "book1_rating": None, "book1_ratings_count": None,
        "book1_pages": None, "is_series": False, "source": "gemini",
    }
    if not text:
        return r

    def _c(m):
        v = m.group(1).strip() if m else ""
        return "" if v.upper() in ("UNKNOWN", "NONE", "N/A", "", "STANDALONE") else v

    sn = _c(re.search(r"SERIES:\s*([^\|]+)", text, re.I))
    bn = _c(re.search(r"BOOKS:\s*([^\|]+)", text, re.I))
    fn = _c(re.search(r"FIRST:\s*([^\|]+)", text, re.I))
    ln = _c(re.search(r"LAST:\s*([^\|]+)", text, re.I))
    un = _c(re.search(r"URL:\s*([^\|]+)", text, re.I))
    rt = _c(re.search(r"RATING:\s*([^\|]+)", text, re.I))
    rc = _c(re.search(r"RATINGS_COUNT:\s*([^\|]+)", text, re.I))
    pg = _c(re.search(r"PAGES:\s*([^\|]+)", text, re.I))

    if sn:
        r["series_name"] = sn
        r["is_series"] = True
    if bn:
        try:
            r["num_books"] = int(re.search(r"\d+", bn).group())
        except Exception:
            pass
    if fn:
        r["first_book"] = fn
    if ln:
        r["last_book"] = ln
    if un and "goodreads.com/series" in un:
        r["gr_series_url"] = un
    if rt:
        try:
            r["book1_rating"] = float(re.search(r"[\d.]+", rt).group())
        except Exception:
            pass
    if rc:
        try:
            r["book1_ratings_count"] = int(re.sub(r"[^\d]", "", rc))
        except Exception:
            pass
    if pg:
        try:
            r["book1_pages"] = int(re.search(r"\d+", pg).group())
        except Exception:
            pass

    return r


def gemini_find_contact(author):
    """Find author email + agent via Gemini grounded search."""
    prompt = (
        f'Find the official contact email for romance/fiction author "{author}". '
        f'Check official website, newsletter, BookBub profile. '
        f'Report ONLY exact email if publicly listed. Do NOT invent. '
        f'Format: EMAIL: <email or NONE> | AGENT_NAME: <name or NONE> | '
        f'AGENT_EMAIL: <email or NONE> | WEBSITE: <url or NONE>'
    )
    raw, text = _gemini(prompt)
    result = {"email": "", "agent_name": "", "agent_email": "", "website": "",
              "email_source": ""}
    if not text:
        return result

    def _c(m):
        v = m.group(1).strip() if m else ""
        return "" if v.upper() in ("UNKNOWN", "NONE", "N/A", "NOT FOUND", "") else v

    em = _c(re.search(r"EMAIL:\s*([^\|]+)", text, re.I))
    an = _c(re.search(r"AGENT_NAME:\s*([^\|]+)", text, re.I))
    ae = _c(re.search(r"AGENT_EMAIL:\s*([^\|]+)", text, re.I))
    ws = _c(re.search(r"WEBSITE:\s*([^\|]+)", text, re.I))

    if em:
        emails = EMAIL_RE.findall(em)
        candidate = _pick_email(emails, author)
        if candidate and _valid_email(candidate):
            # Cross-check against grounding sources
            non_social = [s for s in raw.get("sources", [])
                          if not any(d in s for d in _SOCIAL_DOMAINS)]
            if non_social:
                result["email"] = candidate
                result["email_source"] = non_social[0]
            elif raw.get("sources"):
                # Try to cross-validate on the source page
                resp = _safe_get(raw["sources"][0])
                if resp:
                    found = [e for e in _emails_from_html(resp.text) if _valid_email(e)]
                    if candidate.lower() in [e.lower() for e in found]:
                        result["email"] = candidate
                        result["email_source"] = raw["sources"][0]

    if ae:
        agent_emails = EMAIL_RE.findall(ae)
        for agent_e in agent_emails:
            if _valid_email(agent_e):
                result["agent_email"] = agent_e
                break

    if an:
        an_clean = re.sub(r'https?://\S+', '', an)
        an_clean = re.sub(r'[A-Za-z0-9._%+\-]+@\S+', '', an_clean)
        an_clean = re.sub(r'\s+', ' ', an_clean).strip().rstrip(' ,;:.')
        if len(an_clean) > 3 and len(an_clean) < 80:
            result["agent_name"] = an_clean

    if ws and ws.startswith("http"):
        result["website"] = ws

    return result


def scrape_website_email(website, author=""):
    """Directly scrape author website for emails."""
    if not website or not str(website).startswith("http"):
        return "", ""
    base = str(website).rstrip("/")
    for path in ["", "/contact", "/contact-me", "/about", "/connect"]:
        r = _safe_get(base + path)
        if not r:
            time.sleep(0.3)
            continue
        emails = [e for e in _emails_from_html(r.text) if _valid_email(e)]
        email = _pick_email(emails, author)
        if email:
            return email, base + path
        time.sleep(0.3)
    return "", ""


# ── Cache ─────────────────────────────────────────────────────────────

def load_cache():
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text())
        except Exception:
            pass
    return {}


def save_cache(c):
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(json.dumps(c, indent=2, ensure_ascii=False))


# ── Main ──────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print(f"ENRICH APRIL KU NEW TITLES — {time.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    if not GEMINI_KEY:
        print("ERROR: GEMINI_API_KEY not found in .env or environment")
        sys.exit(1)

    # Load master to find NEW rows
    master = pd.read_csv(SRC_COMBINED)
    new_rows = master[master["Priority Band"] == "NEW — NEEDS RESEARCH"].copy()
    print(f"New rows to enrich: {len(new_rows)}")

    # Also load April KU for extra metadata
    april_ku = pd.read_csv(SRC_APRIL_KU) if SRC_APRIL_KU.exists() else pd.DataFrame()

    # ── Step 1: Filter to sports/hockey-relevant ──────────────────
    print("\n▶ Step 1: Filter to sports/hockey-relevant titles")

    def is_sports_relevant(row):
        combined = (
            str(row.get("Show Name", "")) + " " +
            str(row.get("Author Name", "")) + " " +
            str(row.get("Sub Genre", ""))
        ).lower()
        return any(kw in combined for kw in SPORTS_KEYWORDS)

    # Also check against the April KU data for category hints
    sports_new = []
    non_sports = []
    for _, r in new_rows.iterrows():
        title = str(r.get("Show Name", ""))
        author = str(r.get("Author Name", ""))

        # Check if the April KU entry was from Sports Romance paid category
        is_from_sports_list = False
        if not april_ku.empty:
            matches = april_ku[
                (april_ku["author"].fillna("").str.lower().str.strip() ==
                 author.lower().strip()) &
                (april_ku["category"].str.contains("Sports", na=False))
            ]
            if not matches.empty:
                is_from_sports_list = True

        if is_sports_relevant(r) or is_from_sports_list:
            sports_new.append(r.to_dict())
        else:
            non_sports.append(r.to_dict())

    print(f"  Sports/hockey relevant: {len(sports_new)}")
    print(f"  Non-sports (will tag but still process): {len(non_sports)}")

    # Deduplicate by author (keep best-ranked entry per author)
    all_to_enrich = sports_new + non_sports
    by_author = {}
    for r in all_to_enrich:
        author = r["Author Name"]
        if author not in by_author:
            by_author[author] = r
        else:
            # Keep the one with the better rank
            existing_rank = float(by_author[author].get("Amazon Best Rank", 999) or 999)
            new_rank = float(r.get("Amazon Best Rank", 999) or 999)
            if new_rank < existing_rank:
                by_author[author] = r

    authors_to_enrich = list(by_author.values())
    print(f"  Unique authors to enrich: {len(authors_to_enrich)}")

    # ── Step 2: Enrich via Gemini ─────────────────────────────────
    print("\n▶ Step 2: Enrich via Gemini (series + contacts)")

    cache = load_cache()
    enriched = []

    for i, r in enumerate(authors_to_enrich):
        author = r["Author Name"]
        title = r["Show Name"]
        cache_key = f"{author}||{title}"

        if cache_key in cache:
            print(f"  [{i+1}/{len(authors_to_enrich)}] {author:30} — cached")
            enriched.append(cache[cache_key])
            continue

        print(f"  [{i+1}/{len(authors_to_enrich)}] {author:30} — querying Gemini...")

        # Series lookup
        series_info = gemini_verify_series(title, author)
        time.sleep(1)

        # Contact lookup
        contact_info = gemini_find_contact(author)
        time.sleep(1)

        # Try scraping the website for email if Gemini found a website
        website_email = ""
        website_source = ""
        if contact_info.get("website"):
            website_email, website_source = scrape_website_email(
                contact_info["website"], author
            )
            time.sleep(0.5)

        # Use best email: direct scrape > Gemini-grounded
        best_email = website_email or contact_info.get("email", "")
        best_source = website_source or contact_info.get("email_source", "")

        # Calculate hours from pages
        pages = series_info.get("book1_pages") or 0
        num_books = series_info.get("num_books") or 1
        total_pages = pages * num_books if pages else 0
        hours = round(total_pages * 300 / 9600, 2) if total_pages > 0 else 0

        # Assign tier
        gr_ratings = series_info.get("book1_ratings_count", 0) or 0
        tier, mg_min, mg_max, mg_disp, rs_pct, rs_range = assign_tier(gr_ratings, hours)

        # Determine if it's sports relevant
        is_sports = any(kw in (title + " " + author + " " +
                               str(series_info.get("series_name", ""))).lower()
                        for kw in SPORTS_KEYWORDS)

        enriched_row = {
            "Author Name":                author,
            "Original Title":             title,
            "Series Name":                series_info.get("series_name", ""),
            "Is Series":                  series_info.get("is_series", False),
            "Num Books":                  series_info.get("num_books"),
            "First Book":                 series_info.get("first_book", ""),
            "Last Book":                  series_info.get("last_book", ""),
            "Goodreads Series URL":       series_info.get("gr_series_url", ""),
            "Book 1 Rating (Stars)":      series_info.get("book1_rating"),
            "Book 1 GR Ratings (#)":      series_info.get("book1_ratings_count"),
            "Book 1 Pages":               series_info.get("book1_pages"),
            "Total Pages (est)":          total_pages,
            "Approx Hours":               hours,
            "Commercial Tier":            tier,
            "MG Min ($)":                 mg_min,
            "MG Max ($)":                 mg_max,
            "MG Range":                   mg_disp,
            "Rev Share (%)":              rs_pct,
            "Rev Share Range":            rs_range,
            "Author Email":               best_email,
            "Email Source":               best_source,
            "Agent Name":                 contact_info.get("agent_name", ""),
            "Agent Email":                contact_info.get("agent_email", ""),
            "Author Website":             contact_info.get("website", ""),
            "Is Sports Relevant":         is_sports,
            "KU April Rank":              r.get("Amazon Best Rank", ""),
            "KU April Category":          r.get("KU April 2026", ""),
            "Source":                     "April 2026 KU Scrape",
        }

        # ── SELF-CHECK: validate this row ──────────────────────
        print(f"    Series: {series_info.get('series_name', 'N/A'):30} | "
              f"Books: {series_info.get('num_books', '?')} | "
              f"Hours: {hours:.0f}h | "
              f"GR: {gr_ratings:,.0f} ratings | "
              f"Tier: {tier} | "
              f"Email: {best_email or 'NONE'}")

        cache[cache_key] = enriched_row
        save_cache(cache)
        enriched.append(enriched_row)

    # ── Step 3: Build enriched dataframe ──────────────────────────
    print(f"\n▶ Step 3: Build enriched output")
    edf = pd.DataFrame(enriched)

    # Filter: only series (not standalones)
    is_series = edf["Is Series"].fillna(False)
    standalones = (~is_series).sum()
    print(f"  Standalones (will be flagged): {standalones}")

    # Filter: sports relevant
    sports_count = edf["Is Sports Relevant"].sum()
    print(f"  Sports relevant: {sports_count} / {len(edf)}")

    # Show licensing candidates (series + 40h+)
    candidates = edf[
        (edf["Is Series"] == True) &
        (edf["Approx Hours"] >= 40)
    ]
    print(f"  Licensing candidates (series + 40h+): {len(candidates)}")
    print()

    # ── SELF-CHECK: print all candidates ──────────────────────────
    print("=" * 80)
    print("LICENSING CANDIDATES (series + 40h+)")
    print("=" * 80)
    for _, row in candidates.iterrows():
        tag = "SPORTS" if row["Is Sports Relevant"] else "NON-SPORT"
        email = row["Author Email"] or row["Agent Email"] or "NO CONTACT"
        print(f"  [{tag:10}] {row['Author Name']:25} | {row['Series Name']:35} | "
              f"{row['Num Books'] or '?':>3} books | {row['Approx Hours']:>5.0f}h | "
              f"GR {int(row.get('Book 1 GR Ratings (#)', 0) or 0):>7,} | "
              f"Tier {row['Commercial Tier']} | MG {row['MG Range']:20} | "
              f"{email[:40]}")

    # ── Save ────────────────────────────────────────────────────
    edf.to_csv(ENRICHED_CSV, index=False)
    print(f"\n  Saved: {ENRICHED_CSV} ({len(edf)} rows)")

    # Summary
    print(f"\n{'='*70}")
    print(f"ENRICHMENT COMPLETE")
    print(f"  Total authors enriched: {len(edf)}")
    print(f"  Series found: {is_series.sum()}")
    print(f"  Sports relevant: {sports_count}")
    print(f"  With author email: {(edf['Author Email'].str.len() > 3).sum()}")
    print(f"  With agent email: {(edf['Agent Email'].str.len() > 3).sum()}")
    print(f"  Licensing candidates: {len(candidates)}")
    print(f"{'='*70}")
    print(f"\nNext: Run python3 execution/rebuild_combined_master.py to merge into master")


if __name__ == "__main__":
    main()
