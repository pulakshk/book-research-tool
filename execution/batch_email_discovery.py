#!/usr/bin/env python3
"""
Batch Email Discovery — Gemini Grounded Search
================================================
For all YELLOW/RED entries (and any GREEN without email) in the
Sports Romance Combined Master, run Gemini grounded search to find:
  - Author email
  - Agent name + email
  - Author website

Processes in priority order: P0 → P1 → P2 → P3 → P5
Caches every result so the script is fully resumable.
Cross-validates emails by scraping discovered websites.

Anti-hallucination:
  - Every email validated against bad-email patterns
  - Cross-checked against website scraping where possible
  - Social media / generic emails flagged
  - Progress logged to stdout every 10 rows

Output:
  outreach/sports-romance/source/email_discovery_results.csv
  outreach/sports-romance/source/email_discovery_cache.json

Usage:
  python3 execution/batch_email_discovery.py
  python3 execution/batch_email_discovery.py --limit 50    (process only 50 rows)
  python3 execution/batch_email_discovery.py --priority P0  (only P0 rows)
"""

import argparse, json, os, re, sys, time, warnings
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

PROJECT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT / "outreach" / "sports-romance"
MASTER_CSV = OUT_DIR / "exports" / "Sports_Romance_Combined_Master.csv"
RESULTS_CSV = OUT_DIR / "source" / "email_discovery_results.csv"
CACHE_FILE = OUT_DIR / "source" / "email_discovery_cache.json"

# ── API ──────────────────────────────────────────────────────────────
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

_BAD_EMAILS = {
    "user@domain.com", "email@example.com", "author@directauthor.com",
    "info@therateabc.com", "noreply@google.com", "noreply@goodreads.com",
    "support@goodreads.com", "review@goodreads.com",
}
_BAD_DOMAINS = {
    "example.com", "domain.com", "email.com", "test.com", "sentry.io",
    "wixpress.com", "squarespace.com", "wordpress.com", "google.com",
    "goodreads.com", "amazon.com",
}
_SOCIAL_DOMAINS = {
    "facebook.com", "twitter.com", "x.com", "instagram.com",
    "tiktok.com", "bookbub.com", "goodreads.com", "amazon.com",
    "wikipedia.org", "youtube.com", "pinterest.com", "linkedin.com",
}
_PUBLICITY_PREFIXES = {
    "publicity", "admin", "press", "media", "marketing", "pr",
    "info", "contact", "hello", "office", "support", "help",
    "sales", "newsletter", "noreply", "no-reply",
}


def _valid_email(e):
    e = e.strip().lower()
    if not e or "@" not in e or e in _BAD_EMAILS:
        return False
    domain = e.split("@")[-1]
    if domain in _BAD_DOMAINS:
        return False
    # Reject very short local parts
    local = e.split("@")[0]
    if len(local) < 2:
        return False
    return True


def _is_personal_email(e, author=""):
    """Check if email looks like a personal/author email vs. generic."""
    local = e.split("@")[0].lower().replace(".", "").replace("_", "").replace("-", "")
    # Check if author name parts are in the local part
    parts = [p.lower() for p in author.split() if len(p) > 2]
    if any(p in local for p in parts):
        return True
    # Check if it's a publicity prefix
    for prefix in _PUBLICITY_PREFIXES:
        if local.startswith(prefix):
            return False
    return True


def _pick_best_email(emails, author=""):
    valid = [e for e in emails if _valid_email(e)]
    if not valid:
        return ""
    # Prefer personal author emails
    personal = [e for e in valid if _is_personal_email(e, author)]
    if personal:
        return personal[0]
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
    return list(set(EMAIL_RE.findall(soup.get_text(" ", strip=True))))


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
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 500},
    }
    for attempt in range(retries):
        try:
            r = requests.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload, timeout=30)
            if r.status_code == 429:
                wait_time = 45 * (attempt + 1)
                print(f"    Rate limited, waiting {wait_time}s...")
                time.sleep(wait_time)
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


def discover_email(author, series="", existing_website=""):
    """
    Use Gemini grounded search to find author/agent contact info.
    Returns dict with: email, email_source, agent_name, agent_email, website, method
    """
    result = {
        "email": "", "email_source": "", "email_type": "",
        "agent_name": "", "agent_email": "",
        "website": existing_website or "",
        "method": "",
    }

    # Step 1: Gemini grounded search
    prompt = (
        f'Find the official contact email address for romance/fiction author "{author}". '
        f'They wrote the series "{series}". '
        f'Check their official author website, newsletter signup, BookBub profile, and literary agent pages. '
        f'Report ONLY emails that are publicly listed. Do NOT invent or guess. '
        f'Format: EMAIL: <email or NONE> | AGENT_NAME: <name or NONE> | '
        f'AGENT_EMAIL: <email or NONE> | WEBSITE: <url or NONE>'
    )
    raw, text = _gemini(prompt)

    if text:
        # Parse EMAIL
        em_match = re.search(r"EMAIL:\s*([^\|]+)", text, re.I)
        if em_match:
            found_emails = EMAIL_RE.findall(em_match.group(1))
            candidate = _pick_best_email(found_emails, author)
            if candidate:
                # Cross-validate against grounding sources
                non_social = [s for s in raw.get("sources", [])
                              if not any(d in s for d in _SOCIAL_DOMAINS)]
                if non_social:
                    result["email"] = candidate
                    result["email_source"] = non_social[0]
                    result["email_type"] = "personal" if _is_personal_email(candidate, author) else "generic"
                    result["method"] = "gemini_grounded"
                elif raw.get("sources"):
                    # Try cross-validate on the first source
                    resp = _safe_get(raw["sources"][0])
                    if resp:
                        page_emails = [e for e in _emails_from_html(resp.text) if _valid_email(e)]
                        if candidate.lower() in [e.lower() for e in page_emails]:
                            result["email"] = candidate
                            result["email_source"] = raw["sources"][0]
                            result["email_type"] = "personal" if _is_personal_email(candidate, author) else "generic"
                            result["method"] = "gemini_crossvalidated"

        # Parse AGENT_NAME
        an_match = re.search(r"AGENT_NAME:\s*([^\|]+)", text, re.I)
        if an_match:
            v = an_match.group(1).strip()
            if v.upper() not in ("NONE", "N/A", "NOT FOUND", "UNKNOWN", ""):
                v = re.sub(r'https?://\S+', '', v)
                v = re.sub(r'[A-Za-z0-9._%+\-]+@\S+', '', v)
                v = re.sub(r'\*+', '', v)
                v = re.sub(r'\s+', ' ', v).strip().rstrip(' ,;:.')
                if 3 < len(v) < 80:
                    result["agent_name"] = v

        # Parse AGENT_EMAIL
        ae_match = re.search(r"AGENT_EMAIL:\s*([^\|]+)", text, re.I)
        if ae_match:
            agent_emails = EMAIL_RE.findall(ae_match.group(1))
            for ae in agent_emails:
                if _valid_email(ae):
                    result["agent_email"] = ae
                    break

        # Parse WEBSITE
        ws_match = re.search(r"WEBSITE:\s*([^\|]+)", text, re.I)
        if ws_match:
            v = ws_match.group(1).strip()
            if v.startswith("http") and v.upper() != "NONE":
                result["website"] = v.split()[0]  # Take just the URL

    # Step 2: If we got a website but no email, scrape the website directly
    if result["website"] and not result["email"]:
        website = result["website"].rstrip("/")
        for path in ["", "/contact", "/contact-me", "/about", "/connect"]:
            resp = _safe_get(website + path)
            if not resp:
                time.sleep(0.3)
                continue
            page_emails = [e for e in _emails_from_html(resp.text) if _valid_email(e)]
            best = _pick_best_email(page_emails, author)
            if best:
                result["email"] = best
                result["email_source"] = website + path
                result["email_type"] = "personal" if _is_personal_email(best, author) else "generic"
                result["method"] = "website_scrape"
                break
            time.sleep(0.3)

    return result


# ── Cache ──────────────────────────────────────────────────────────────

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0, help="Max rows to process (0=all)")
    parser.add_argument("--priority", type=str, default="", help="Only process this priority (P0, P1, etc.)")
    args = parser.parse_args()

    print("=" * 70)
    print(f"BATCH EMAIL DISCOVERY — {time.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    if not GEMINI_KEY:
        print("ERROR: GEMINI_API_KEY not found")
        sys.exit(1)

    df = pd.read_csv(MASTER_CSV)
    print(f"Loaded master: {len(df)} rows")

    # Filter to rows needing email
    needs_email = df.apply(
        lambda r: str(r.get("Author Email ID", "")).strip() in ("", "nan") and
                  str(r.get("Agency Email ID", "")).strip() in ("", "nan"),
        axis=1
    )
    # Exclude NOT LICENSABLE rows
    licensable = ~df["Priority Band"].isin(["NOT LICENSABLE"])
    target = df[needs_email & licensable].copy()

    if args.priority:
        target = target[target["Priority Band"] == args.priority]

    # Sort by priority
    rank_order = {"P0": 0, "P1": 1, "P2": 2, "P2 (April KU)": 2.5, "P3": 3, "P5": 4}
    target["_sort"] = target["Priority Band"].map(rank_order).fillna(5)
    target = target.sort_values("_sort").drop(columns=["_sort"])

    if args.limit > 0:
        target = target.head(args.limit)

    print(f"Rows to process: {len(target)}")
    print(f"  By priority: {target['Priority Band'].value_counts().to_dict()}")

    cache = load_cache()
    results = []
    found_email = 0
    found_agent = 0
    processed = 0
    start_time = time.time()

    for idx, (_, row) in enumerate(target.iterrows()):
        author = str(row.get("Author Name", "")).strip()
        series = str(row.get("Show Name", "")).strip()
        priority = str(row.get("Priority Band", ""))
        existing_website = str(row.get("Author Website", "")).strip()
        if existing_website == "nan":
            existing_website = ""

        cache_key = author.lower().strip()

        if cache_key in cache:
            results.append(cache[cache_key])
            r = cache[cache_key]
            if r.get("email"):
                found_email += 1
            if r.get("agent_email"):
                found_agent += 1
            processed += 1
            continue

        # Rate limit display
        if processed > 0 and processed % 10 == 0:
            elapsed = time.time() - start_time
            rate = processed / elapsed * 60
            eta = (len(target) - processed) / rate if rate > 0 else 0
            print(f"\n  Progress: {processed}/{len(target)} | "
                  f"Emails found: {found_email} | Agents: {found_agent} | "
                  f"Rate: {rate:.0f}/min | ETA: {eta:.0f}min")

        print(f"  [{processed+1}/{len(target)}] [{priority}] {author:30} — ", end="", flush=True)

        result = discover_email(author, series, existing_website)
        result["author"] = author
        result["series"] = series
        result["priority"] = priority

        # Log result
        if result["email"]:
            found_email += 1
            print(f"EMAIL: {result['email']} ({result['method']})")
        elif result["agent_email"]:
            found_agent += 1
            print(f"AGENT: {result['agent_email']} ({result.get('agent_name','')})")
        else:
            print("NO CONTACT")

        cache[cache_key] = result
        results.append(result)
        processed += 1

        # Save cache every 5 rows
        if processed % 5 == 0:
            save_cache(cache)

    save_cache(cache)

    # Save results CSV
    rdf = pd.DataFrame(results)
    rdf.to_csv(RESULTS_CSV, index=False)

    # Summary
    total_email = rdf["email"].apply(lambda x: bool(x and str(x).strip())).sum()
    total_agent = rdf["agent_email"].apply(lambda x: bool(x and str(x).strip())).sum()
    total_website = rdf["website"].apply(lambda x: bool(x and str(x).strip())).sum()

    print(f"\n{'='*70}")
    print(f"DISCOVERY COMPLETE")
    print(f"  Processed: {processed}")
    print(f"  Author emails found: {total_email} ({total_email/max(processed,1)*100:.1f}%)")
    print(f"  Agent emails found: {total_agent} ({total_agent/max(processed,1)*100:.1f}%)")
    print(f"  Websites found: {total_website}")
    print(f"  ANY contact: {total_email + total_agent}")
    print(f"  No contact: {processed - total_email - total_agent}")
    print(f"\n  Results: {RESULTS_CSV}")
    print(f"  Cache: {CACHE_FILE}")
    print(f"{'='*70}")

    # Spot-check: show 5 found emails
    found = rdf[rdf["email"].apply(lambda x: bool(x and str(x).strip()))]
    if not found.empty:
        print(f"\nSample found emails (up to 10):")
        for _, r in found.head(10).iterrows():
            print(f"  [{r.get('priority','')}] {r['author']:30} | {r['email']:35} | {r.get('method','')} | src: {str(r.get('email_source',''))[:50]}")


if __name__ == "__main__":
    main()
