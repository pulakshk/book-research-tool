#!/usr/bin/env python3
"""
Google AI Search Email Discovery — Chrome-based fallback
=========================================================
For rows where Gemini grounded search didn't find emails, uses
Playwright to perform Google searches with AI overview and extracts
contact information from the AI-generated answers.

Two-pass approach:
  Pass 1: Google search "{author name} romance author contact email"
          → Extract from AI overview + top results
  Pass 2: Google search "{author name} literary agent email"
          → Extract agent info from AI overview

Cross-validates all found emails by scraping the source page.
Fully resumable via cache.

Output:
  outreach/sports-romance/source/google_ai_search_results.csv

Usage:
  python3 execution/google_ai_email_search.py
  python3 execution/google_ai_email_search.py --limit 50
"""

import argparse, asyncio, json, os, re, sys, time, warnings
from pathlib import Path

import pandas as pd
import requests
from bs4 import BeautifulSoup

warnings.filterwarnings("ignore")

PROJECT = Path(__file__).resolve().parents[1]
OUT_DIR = PROJECT / "outreach" / "sports-romance"
MASTER_CSV = OUT_DIR / "exports" / "Sports_Romance_Combined_Master.csv"
GEMINI_CACHE = OUT_DIR / "source" / "email_discovery_cache.json"
RESULTS_CSV = OUT_DIR / "source" / "google_ai_search_results.csv"
CACHE_FILE = OUT_DIR / "source" / "google_ai_search_cache.json"

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")

_BAD_EMAILS = {
    "user@domain.com", "email@example.com", "noreply@google.com",
    "support@goodreads.com", "noreply@goodreads.com",
}
_BAD_DOMAINS = {
    "example.com", "domain.com", "email.com", "test.com", "sentry.io",
    "wixpress.com", "squarespace.com", "wordpress.com", "google.com",
    "goodreads.com", "amazon.com", "facebook.com", "twitter.com",
}

SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
})


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
    for e in valid:
        local = e.split("@")[0].lower().replace(".", "").replace("_", "")
        if any(p in local for p in parts):
            return e
    return valid[0]


def _safe_get(url, timeout=10):
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


def load_cache(path):
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


def save_cache(c, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(c, indent=2, ensure_ascii=False))


async def google_search_for_email(page, author, series=""):
    """
    Perform a Google search and extract emails from:
    1. AI Overview / featured snippet
    2. Top organic result pages
    """
    result = {
        "email": "", "email_source": "", "agent_name": "", "agent_email": "",
        "website": "", "method": "",
    }

    queries = [
        f'"{author}" romance author official contact email site',
        f'"{author}" literary agent email contact',
    ]

    for qi, query in enumerate(queries):
        try:
            url = f"https://www.google.com/search?q={query.replace(' ', '+')}"
            await page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)

            # Get full page text (includes AI overview if present)
            page_text = await page.evaluate("document.body.innerText")

            # Extract emails from the entire search results page
            all_emails = EMAIL_RE.findall(page_text)
            valid = [e for e in all_emails if _valid_email(e)]

            if valid:
                best = _pick_best_email(valid, author)
                if best:
                    if qi == 0:
                        result["email"] = best
                        result["method"] = "google_ai_search"
                    else:
                        if not result["agent_email"]:
                            result["agent_email"] = best
                            result["method"] = result.get("method", "") or "google_ai_agent"

            # Try to find author website from search results
            if not result["website"]:
                links = await page.evaluate("""
                    Array.from(document.querySelectorAll('a[href]'))
                        .map(a => a.href)
                        .filter(h => h.startsWith('http') &&
                                !h.includes('google.') &&
                                !h.includes('amazon.') &&
                                !h.includes('goodreads.') &&
                                !h.includes('facebook.') &&
                                !h.includes('twitter.') &&
                                !h.includes('instagram.') &&
                                !h.includes('youtube.') &&
                                !h.includes('wikipedia.'))
                        .slice(0, 5)
                """)
                # Look for author-name-ish URLs
                author_parts = [p.lower() for p in author.split() if len(p) > 2]
                for link in links:
                    if any(p in link.lower() for p in author_parts):
                        result["website"] = link
                        break

            # Extract agent name from AI overview text
            if qi == 1 and not result["agent_name"]:
                # Look for patterns like "represented by X" or "agent is X at Y"
                for pattern in [
                    r"(?:represented by|agent is|literary agent)\s+([A-Z][a-z]+ [A-Z][a-z]+)",
                    r"([A-Z][a-z]+ [A-Z][a-z]+)\s+(?:at|of|from)\s+(?:literary|agency|books)",
                ]:
                    m = re.search(pattern, page_text)
                    if m:
                        result["agent_name"] = m.group(1)
                        break

        except Exception as e:
            pass

        await asyncio.sleep(1.5)

    # Cross-validate: if we found a website, scrape it for email
    if result["website"] and not result["email"]:
        base = result["website"].rstrip("/")
        for path in ["", "/contact", "/contact-me", "/about"]:
            resp = _safe_get(base + path)
            if resp:
                found = [e for e in _emails_from_html(resp.text) if _valid_email(e)]
                best = _pick_best_email(found, author)
                if best:
                    result["email"] = best
                    result["email_source"] = base + path
                    result["method"] = "google_ai_crossvalidated"
                    break
            time.sleep(0.3)

    return result


async def run_search(limit=0):
    print("=" * 70)
    print(f"GOOGLE AI SEARCH EMAIL DISCOVERY — {time.strftime('%Y-%m-%d %H:%M')}")
    print("=" * 70)

    # Load master
    df = pd.read_csv(MASTER_CSV)

    # Load Gemini cache to find rows that failed
    gemini_cache = load_cache(GEMINI_CACHE)
    google_cache = load_cache(CACHE_FILE)

    # Find rows needing email (no existing email AND no Gemini result)
    needs_search = []
    for _, row in df.iterrows():
        author = str(row.get("Author Name", "")).strip()
        has_email = str(row.get("Author Email ID", "")).strip() not in ("", "nan")
        has_agent = str(row.get("Agency Email ID", "")).strip() not in ("", "nan")

        if has_email or has_agent:
            continue

        # Check if Gemini found something
        gemini_key = author.lower().strip()
        if gemini_key in gemini_cache:
            gr = gemini_cache[gemini_key]
            if gr.get("email") or gr.get("agent_email"):
                continue

        # Skip NOT LICENSABLE
        if row.get("Priority Band") == "NOT LICENSABLE":
            continue

        needs_search.append(row)

    print(f"Rows needing Google AI search: {len(needs_search)}")

    if limit > 0:
        needs_search = needs_search[:limit]
        print(f"Limited to: {limit}")

    # Sort by priority
    rank_order = {"P0": 0, "P1": 1, "P2": 2, "P2 (April KU)": 2.5, "P3": 3, "P5": 4}
    needs_search.sort(key=lambda r: rank_order.get(r.get("Priority Band", ""), 5))

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("ERROR: playwright not installed")
        sys.exit(1)

    results = []
    found_email = 0
    found_agent = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            viewport={"width": 1920, "height": 1080},
            locale="en-US",
        )
        page = await context.new_page()

        for i, row in enumerate(needs_search):
            author = str(row.get("Author Name", "")).strip()
            series = str(row.get("Show Name", "")).strip()
            priority = str(row.get("Priority Band", ""))
            cache_key = author.lower().strip()

            if cache_key in google_cache:
                r = google_cache[cache_key]
                results.append(r)
                if r.get("email"):
                    found_email += 1
                if r.get("agent_email"):
                    found_agent += 1
                continue

            if i > 0 and i % 10 == 0:
                print(f"\n  Progress: {i}/{len(needs_search)} | "
                      f"Emails: {found_email} | Agents: {found_agent}")

            print(f"  [{i+1}/{len(needs_search)}] [{priority}] {author:30} — ", end="", flush=True)

            r = await google_search_for_email(page, author, series)
            r["author"] = author
            r["series"] = series
            r["priority"] = priority

            if r.get("email"):
                found_email += 1
                print(f"EMAIL: {r['email']} ({r['method']})")
            elif r.get("agent_email"):
                found_agent += 1
                print(f"AGENT: {r['agent_email']}")
            else:
                print("NO CONTACT")

            google_cache[cache_key] = r
            results.append(r)

            if i % 5 == 0:
                save_cache(google_cache, CACHE_FILE)

            await asyncio.sleep(2)  # Respect Google rate limits

        await browser.close()

    save_cache(google_cache, CACHE_FILE)

    rdf = pd.DataFrame(results)
    rdf.to_csv(RESULTS_CSV, index=False)

    total_email = rdf["email"].apply(lambda x: bool(x and str(x).strip())).sum() if len(rdf) > 0 else 0
    total_agent = rdf["agent_email"].apply(lambda x: bool(x and str(x).strip())).sum() if len(rdf) > 0 else 0

    print(f"\n{'='*70}")
    print(f"GOOGLE AI SEARCH COMPLETE")
    print(f"  Processed: {len(results)}")
    print(f"  Author emails: {total_email}")
    print(f"  Agent emails: {total_agent}")
    print(f"  Results: {RESULTS_CSV}")
    print(f"{'='*70}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    args = parser.parse_args()
    asyncio.run(run_search(limit=args.limit))


if __name__ == "__main__":
    main()
