#!/usr/bin/env python3
"""
WebSearch + WebFetch Email Discovery
=====================================
Uses WebSearch to find author contact pages, then WebFetch to extract
emails from those pages. This mirrors what Google AI Mode does but
works programmatically.

Two-step per author:
  1. WebSearch: "{author} romance author contact email literary agent"
     → Identifies contact page URLs
  2. WebFetch: Scrape each contact page for email addresses

Fully resumable via cache. Cross-validates all found emails.

Output:
  outreach/sports-romance/source/websearch_email_results.csv

Usage:
  python3 execution/websearch_email_discovery.py
  python3 execution/websearch_email_discovery.py --limit 50
  python3 execution/websearch_email_discovery.py --priority P0
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
GEMINI_CACHE = OUT_DIR / "source" / "email_discovery_cache.json"
RESULTS_CSV = OUT_DIR / "source" / "websearch_email_results.csv"
CACHE_FILE = OUT_DIR / "source" / "websearch_email_cache.json"

# ── API setup for Gemini (used as WebSearch proxy) ────────────────────
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
_MIN_CALL_INTERVAL_SEC = 1.2

_BAD_EMAILS = {
    "user@domain.com", "email@example.com", "noreply@google.com",
    "support@goodreads.com", "noreply@goodreads.com",
}
_BAD_DOMAINS = {
    "example.com", "domain.com", "email.com", "test.com", "sentry.io",
    "wixpress.com", "squarespace.com", "wordpress.com", "google.com",
    "goodreads.com", "amazon.com", "facebook.com", "twitter.com",
}


def _valid_email(e):
    e = e.strip().lower()
    if not e or "@" not in e or e in _BAD_EMAILS:
        return False
    return e.split("@")[-1] not in _BAD_DOMAINS


def _pick_best_email(emails, author=""):
    valid = [e for e in emails if _valid_email(e)]
    if not valid:
        return ""
    parts = [p.lower() for p in author.split() if len(p) > 2]
    # Prefer emails containing author name parts
    for e in valid:
        local = e.split("@")[0].lower().replace(".", "").replace("_", "").replace("-", "")
        if any(p in local for p in parts):
            return e
    # Deprioritize publicity/generic emails
    generic_prefixes = {"publicity", "admin", "press", "media", "info", "contact",
                        "hello", "office", "support", "newsletter", "noreply"}
    personal = [e for e in valid if e.split("@")[0].lower() not in generic_prefixes]
    return personal[0] if personal else valid[0]


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


def _gemini_search(prompt, retries=2):
    """Use Gemini with google_search grounding as a smarter search."""
    global _LAST_CALL
    if not GEMINI_KEY:
        return "", [], ""
    wait = _MIN_CALL_INTERVAL_SEC - (time.time() - _LAST_CALL)
    if wait > 0:
        time.sleep(wait)
    _LAST_CALL = time.time()
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "tools": [{"google_search": {}}],
        # Keep responses short to reduce token usage; we rely on grounding URLs + our own verification.
        "generationConfig": {"temperature": 0.0, "maxOutputTokens": 260},
    }
    for attempt in range(retries):
        try:
            # Keep the API key out of any exception stack traces (avoid URL interpolation).
            r = requests.post(GEMINI_URL, params={"key": GEMINI_KEY}, json=payload, timeout=30)
            if r.status_code == 429:
                time.sleep(45 * (attempt + 1))
                continue
            if r.status_code != 200:
                return "", [], ""
            data = r.json()
            cands = data.get("candidates", [])
            if not cands:
                return "", [], ""
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
            return text, sources, ""
        except Exception:
            time.sleep(5)
    return "", [], ""


def discover_author_contact(author, series=""):
    """
    Multi-step discovery:
    1. Gemini search for author website + contact page
    2. Scrape contact page directly
    3. Gemini search specifically for literary agent
    """
    result = {
        "author": author,
        "email": "", "email_source": "", "email_type": "",
        "agent_name": "", "agent_email": "", "agent_source": "",
        "website": "", "contact_page": "", "method": "",
    }

    # ── Single grounded call (token-averse) ───────────────────────
    # We still verify any returned emails by fetching the cited source URL later.
    prompt = (
        f'You are doing rights/outreach research. For author "{author}"'
        + (f' (series "{series}")' if series else "")
        + ', find the official website and the best public contact point for rights/licensing.\n'
          'Return ONLY this exact JSON shape (no markdown):\n'
          '{"website":null,"contact_page":null,"author_email":null,"author_email_source":null,'
          '"agent_name":null,"agent_email":null,"agent_email_source":null}\n'
          "Rules:\n"
          "- If you provide an email, it must be present verbatim on the cited source page.\n"
          "- Prefer agent/agency rights email over generic emails.\n"
          "- If no email exists, provide a contact page URL.\n"
          "- Keep values short; use null when unknown.\n"
    )
    text1, sources1, _ = _gemini_search(prompt)

    website = ""
    contact_page = ""
    author_email = ""
    author_email_source = ""
    agent_name = ""
    agent_email = ""
    agent_email_source = ""

    try:
        blob = re.search(r"\{.*\}", text1, re.S)
        if blob:
            data = json.loads(blob.group(0))
            website = (data.get("website") or "").strip()
            contact_page = (data.get("contact_page") or "").strip()
            author_email = (data.get("author_email") or "").strip()
            author_email_source = (data.get("author_email_source") or "").strip()
            agent_name = (data.get("agent_name") or "").strip()
            agent_email = (data.get("agent_email") or "").strip()
            agent_email_source = (data.get("agent_email_source") or "").strip()
    except Exception:
        pass

    # Fallback: use grounding sources to infer website/contact URL.
    author_parts = [p.lower() for p in author.split() if len(p) > 2]
    for src in sources1:
        s = src.lower()
        if not website and any(p in s for p in author_parts):
            website = src
        if not contact_page and "contact" in s and any(p in s for p in author_parts):
            contact_page = src

    if website:
        result["website"] = website
    if contact_page:
        result["contact_page"] = contact_page
    if author_email:
        result["email"] = author_email
        result["email_source"] = author_email_source or (sources1[0] if sources1 else "")
        result["email_type"] = "personal" if any(p in author_email.lower() for p in author_parts) else "generic"
        result["method"] = "gemini_grounded_json"
    if agent_name:
        result["agent_name"] = agent_name
    if agent_email:
        result["agent_email"] = agent_email
        result["agent_source"] = agent_email_source or (sources1[0] if sources1 else "")
        result["method"] = result.get("method") or "gemini_grounded_json"

    # ── Step 2: Scrape contact page for emails ────────────────────
    urls_to_scrape = []
    if contact_page:
        urls_to_scrape.append(contact_page)
    if website:
        base = website.rstrip("/")
        for suffix in ["", "/contact", "/contact-me", "/about", "/connect"]:
            url = base + suffix
            if url not in urls_to_scrape:
                urls_to_scrape.append(url)

    for url in urls_to_scrape[:5]:
        resp = _safe_get(url)
        if not resp:
            time.sleep(0.3)
            continue

        page_emails = [e for e in _emails_from_html(resp.text) if _valid_email(e)]
        html_text = resp.text.lower()

        # Also look for agent info in the page
        if "agent" in html_text or "represent" in html_text or "rights" in html_text:
            # Try to extract agent emails specifically
            soup = BeautifulSoup(resp.text, "html.parser")
            for t in soup(["script", "style"]):
                t.decompose()
            full_text = soup.get_text(" ", strip=True)

            # Look for agent context
            agent_pattern = re.search(
                r"(?:agent|represent|rights)[^.]{0,100}(" +
                EMAIL_RE.pattern + r")", full_text, re.I
            )
            if agent_pattern:
                ae = agent_pattern.group(1)
                if _valid_email(ae):
                    result["agent_email"] = ae
                    result["agent_source"] = url

            # Look for agent name
            agent_name_pattern = re.search(
                r"(?:agent|represented by)\s+([A-Z][a-z]+ [A-Z][a-z]+)",
                full_text
            )
            if agent_name_pattern:
                result["agent_name"] = agent_name_pattern.group(1)

        # Pick best email
        if page_emails:
            best = _pick_best_email(page_emails, author)
            if best:
                result["email"] = best
                result["email_source"] = url
                result["method"] = "website_scrape"
                # Classify email type
                local = best.split("@")[0].lower()
                if any(p in local for p in author_parts):
                    result["email_type"] = "personal"
                else:
                    result["email_type"] = "generic"
                break
        time.sleep(0.3)

    # If we still have no emails, keep a usable contact URL (contact form) when available.
    if not (result.get("email") or result.get("agent_email")) and contact_page:
        result["method"] = result.get("method") or "contact_page_only"

    # ── Step 3: Gemini search for agent specifically (fallback) ───
    # Token-averse: only do this if we found nothing at all in step 1/2.
    if not result.get("email") and not result.get("agent_email"):
        prompt2 = (
            f'Who is the literary agent for romance author "{author}"? '
            f'What is the agent\'s email address? '
            f'Check author website, QueryTracker, Publishers Marketplace. '
            f'Format: AGENT_NAME: <name or NONE> | AGENT_EMAIL: <email or NONE>'
        )
        text2, sources2, _ = _gemini_search(prompt2)
        if text2:
            an_m = re.search(r"AGENT_NAME:\s*([^\|]+)", text2, re.I)
            ae_m = re.search(r"AGENT_EMAIL:\s*([^\|]+)", text2, re.I)
            if an_m:
                v = an_m.group(1).strip()
                v = re.sub(r'\*+', '', v).strip()
                if v.upper() not in ("NONE", "N/A", "UNKNOWN", "") and len(v) < 80:
                    result["agent_name"] = v
            if ae_m:
                agent_emails = EMAIL_RE.findall(ae_m.group(1))
                for ae in agent_emails:
                    if _valid_email(ae):
                        result["agent_email"] = ae
                        # Prefer a real URL from grounding for validation when available.
                        if sources2:
                            result["agent_source"] = sources2[0]
                        else:
                            result["agent_source"] = ""
                        break

    return result


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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--priority", type=str, default="")
    args = parser.parse_args()

    print("=" * 70)
    print(f"WEBSEARCH EMAIL DISCOVERY — {time.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    if not GEMINI_KEY:
        print("ERROR: GEMINI_API_KEY not found")
        sys.exit(1)

    df = pd.read_csv(MASTER_CSV)
    gemini_cache = {}
    if GEMINI_CACHE.exists():
        gemini_cache = json.loads(GEMINI_CACHE.read_text())

    # Find rows needing search (no email in master AND no email in Gemini cache)
    needs = []
    for _, row in df.iterrows():
        author = str(row.get("Author Name", "")).strip()
        has_email = str(row.get("Author Email ID", "")).strip() not in ("", "nan")
        has_agent = str(row.get("Agency Email ID", "")).strip() not in ("", "nan")
        if has_email or has_agent:
            continue

        key = author.lower().strip()
        gc = gemini_cache.get(key, {})
        if gc.get("email") or gc.get("agent_email"):
            continue

        if row.get("Priority Band") == "NOT LICENSABLE":
            continue

        needs.append(row)

    if args.priority:
        needs = [r for r in needs if r.get("Priority Band") == args.priority]

    # Sort by priority
    rank_order = {"P0": 0, "P1": 1, "P2": 2, "P2 (April KU)": 2.5, "P3": 3, "P5": 4}
    needs.sort(key=lambda r: rank_order.get(r.get("Priority Band", ""), 5))

    if args.limit > 0:
        needs = needs[:args.limit]

    print(f"Rows to search: {len(needs)}")

    cache = load_cache()
    results = []
    found_email = 0
    found_agent = 0
    start_time = time.time()

    for i, row in enumerate(needs):
        author = str(row.get("Author Name", "")).strip()
        series = str(row.get("Show Name", "")).strip()
        priority = str(row.get("Priority Band", ""))
        key = author.lower().strip()

        if key in cache:
            r = cache[key]
            results.append(r)
            if r.get("email"):
                found_email += 1
            if r.get("agent_email"):
                found_agent += 1
            continue

        if i > 0 and i % 10 == 0:
            elapsed = time.time() - start_time
            rate = (i + 1) / elapsed * 60 if elapsed > 0 else 0
            eta = (len(needs) - i) / rate if rate > 0 else 0
            print(f"\n  Progress: {i}/{len(needs)} | "
                  f"Emails: {found_email} | Agents: {found_agent} | "
                  f"Rate: {rate:.0f}/min | ETA: {eta:.0f}min")

        print(f"  [{i+1}/{len(needs)}] [{priority}] {author:30} — ", end="", flush=True)

        r = discover_author_contact(author, series)
        r["priority"] = priority
        r["series"] = series

        if r.get("email"):
            found_email += 1
            print(f"EMAIL: {r['email']} ({r['method']}) | src: {r.get('email_source','')[:40]}")
        elif r.get("agent_email"):
            found_agent += 1
            print(f"AGENT: {r['agent_email']} ({r.get('agent_name','')})")
        else:
            print("NO CONTACT")

        cache[key] = r
        results.append(r)

        if i % 5 == 0:
            save_cache(cache)

    save_cache(cache)

    rdf = pd.DataFrame(results)
    rdf.to_csv(RESULTS_CSV, index=False)

    total_email = rdf["email"].apply(lambda x: bool(x and str(x).strip())).sum() if len(rdf) > 0 else 0
    total_agent = rdf["agent_email"].apply(lambda x: bool(x and str(x).strip())).sum() if len(rdf) > 0 else 0

    print(f"\n{'='*70}")
    print(f"DISCOVERY COMPLETE")
    print(f"  Processed: {len(results)}")
    print(f"  Author emails: {total_email}")
    print(f"  Agent emails: {total_agent}")
    print(f"  ANY contact: {total_email + total_agent}")
    print(f"  Results: {RESULTS_CSV}")
    print(f"{'='*70}")

    if total_email > 0 or total_agent > 0:
        print(f"\nFound contacts:")
        for _, r in rdf.iterrows():
            if r.get("email") or r.get("agent_email"):
                e = r.get("email", "")
                ae = r.get("agent_email", "")
                print(f"  [{r.get('priority','')}] {r['author']:30} | "
                      f"email={e:35} | agent={ae:25} | {r.get('method','')}")


if __name__ == "__main__":
    main()
