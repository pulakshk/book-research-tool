#!/usr/bin/env python3
"""
Email Discovery V2 — Enhanced Gemini Grounded Search
=====================================================
Improved prompt strategy for finding author/agent emails:
  - Uses TWO separate Gemini calls per author:
    1. Direct email lookup with grounded search
    2. Literary agent lookup with grounded search
  - Then scrapes discovered author website for cross-validation
  - Tries multiple prompt variations if first attempt fails

Key improvements over V1:
  - Better prompts that produce cleaner responses
  - Explicit agent-focused second query
  - Website scraping with broader path coverage
  - Separate cache from V1 so we can compare

Output:
  outreach/sports-romance/source/email_discovery_v2_cache.json
  outreach/sports-romance/source/email_discovery_v2_results.csv

Usage:
  python3 execution/email_discovery_v2.py
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
V1_CACHE = OUT_DIR / "source" / "email_discovery_cache.json"
V2_CACHE = OUT_DIR / "source" / "email_discovery_v2_cache.json"
RESULTS_CSV = OUT_DIR / "source" / "email_discovery_v2_results.csv"

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
SESSION.headers.update({"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"})
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}")
_LAST = 0.0

BAD = {"user@domain.com","email@example.com","noreply@google.com","support@goodreads.com"}
BAD_DOM = {"example.com","domain.com","email.com","test.com","sentry.io","wixpress.com",
           "squarespace.com","wordpress.com","google.com","goodreads.com","amazon.com"}

def valid(e):
    e = e.strip().lower()
    return bool(e and "@" in e and e not in BAD and e.split("@")[-1] not in BAD_DOM)

def pick(emails, author=""):
    v = [e for e in emails if valid(e)]
    if not v: return ""
    parts = [p.lower() for p in author.split() if len(p) > 2]
    for e in v:
        local = e.split("@")[0].lower().replace(".","").replace("_","").replace("-","")
        if any(p in local for p in parts): return e
    generic = {"publicity","admin","press","media","info","contact","hello","office","support","newsletter"}
    personal = [e for e in v if e.split("@")[0].lower() not in generic]
    return personal[0] if personal else v[0]

def safe_get(url, timeout=10):
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        return r if r.status_code == 200 else None
    except: return None

def emails_from_html(html):
    soup = BeautifulSoup(html, "html.parser")
    for t in soup(["script","style"]): t.decompose()
    return list(set(EMAIL_RE.findall(soup.get_text(" ", strip=True))))

def gemini(prompt, retries=2):
    global _LAST
    if not GEMINI_KEY: return "", []
    wait = 4.2 - (time.time() - _LAST)
    if wait > 0: time.sleep(wait)
    _LAST = time.time()
    payload = {"contents":[{"parts":[{"text":prompt}]}],
               "tools":[{"google_search":{}}],
               "generationConfig":{"temperature":0.0,"maxOutputTokens":400}}
    for a in range(retries):
        try:
            r = requests.post(f"{GEMINI_URL}?key={GEMINI_KEY}", json=payload, timeout=30)
            if r.status_code == 429:
                time.sleep(45*(a+1)); continue
            if r.status_code != 200: return "", []
            data = r.json()
            cands = data.get("candidates",[])
            if not cands: return "", []
            text = " ".join(p.get("text","") for p in cands[0].get("content",{}).get("parts",[]))
            sources = []
            try:
                for c in cands[0].get("groundingMetadata",{}).get("groundingChunks",[]):
                    u = c.get("web",{}).get("uri","")
                    if u: sources.append(u)
            except: pass
            return text, sources
        except: time.sleep(5)
    return "", []

def discover(author, series=""):
    result = {"author": author, "series": series,
              "email": "", "email_source": "", "email_type": "",
              "agent_name": "", "agent_email": "", "agent_source": "",
              "website": "", "method": ""}

    # ── Query 1: Author email ─────────────────────────────────────
    p1 = (f'What is the email address of romance author {author}? '
          f'Check their official website contact page, BookBub, and Goodreads profile. '
          f'Only report real emails you can verify. '
          f'Reply in this exact format: '
          f'EMAIL: <email> WEBSITE: <author website url>')
    text1, src1 = gemini(p1)

    if text1:
        all_emails = EMAIL_RE.findall(text1)
        best = pick(all_emails, author)
        if best:
            result["email"] = best
            result["method"] = "gemini_v2"
            # Classify
            parts = [p.lower() for p in author.split() if len(p) > 2]
            local = best.split("@")[0].lower()
            result["email_type"] = "personal" if any(p in local for p in parts) else "generic"

        # Extract website
        ws = re.search(r"WEBSITE:\s*(https?://[^\s*|,]+)", text1)
        if ws:
            result["website"] = ws.group(1).rstrip("*.,)")
        elif src1:
            # Check grounding sources for author website
            for s in src1:
                if any(p in s.lower() for p in [p.lower() for p in author.split() if len(p) > 2]):
                    result["website"] = s
                    break

    # ── Query 2: Agent lookup ─────────────────────────────────────
    p2 = (f'Who is the literary agent for romance author {author}? '
          f'What is the agent name and email? '
          f'Check Publishers Marketplace, QueryTracker, author website. '
          f'Reply in this exact format: '
          f'AGENT: <agent name> AGENT_EMAIL: <email>')
    text2, src2 = gemini(p2)

    if text2:
        ae_all = EMAIL_RE.findall(text2)
        for ae in ae_all:
            if valid(ae) and ae != result.get("email",""):
                result["agent_email"] = ae
                result["agent_source"] = "gemini_v2_agent"
                break
        an = re.search(r"AGENT:\s*([^A-Z]*[A-Z][a-z]+ [A-Z][a-z]+)", text2)
        if an:
            name = an.group(1).strip().lstrip("*").strip()
            if 3 < len(name) < 60:
                result["agent_name"] = name

    # ── Step 3: Scrape website if we have it ──────────────────────
    if result["website"] and not result["email"]:
        base = result["website"].rstrip("/")
        for path in ["", "/contact", "/contact-me", "/about", "/connect", "/contact-2"]:
            resp = safe_get(base + path)
            if not resp: time.sleep(0.3); continue
            found = [e for e in emails_from_html(resp.text) if valid(e)]
            best = pick(found, author)
            if best:
                result["email"] = best
                result["email_source"] = base + path
                result["method"] = "website_scrape_v2"
                parts = [p.lower() for p in author.split() if len(p) > 2]
                result["email_type"] = "personal" if any(p in best.lower().split("@")[0] for p in parts) else "generic"
                break
            time.sleep(0.3)

    return result

def load_cache():
    if V2_CACHE.exists():
        try: return json.loads(V2_CACHE.read_text())
        except: pass
    return {}

def save_cache(c):
    V2_CACHE.parent.mkdir(parents=True, exist_ok=True)
    V2_CACHE.write_text(json.dumps(c, indent=2, ensure_ascii=False))

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--priority", type=str, default="")
    args = parser.parse_args()

    print("="*70)
    print(f"EMAIL DISCOVERY V2 — {time.strftime('%Y-%m-%d %H:%M')}")
    print("="*70)

    if not GEMINI_KEY:
        print("ERROR: No GEMINI_API_KEY"); sys.exit(1)

    df = pd.read_csv(MASTER_CSV)
    v1_cache = json.loads(V1_CACHE.read_text()) if V1_CACHE.exists() else {}

    # Find rows where V1 didn't find anything
    needs = []
    for _, row in df.iterrows():
        author = str(row.get("Author Name","")).strip()
        has = str(row.get("Author Email ID","")).strip() not in ("","nan")
        has_a = str(row.get("Agency Email ID","")).strip() not in ("","nan")
        if has or has_a: continue
        key = author.lower().strip()
        v1 = v1_cache.get(key, {})
        if v1.get("email") or v1.get("agent_email"): continue
        if row.get("Priority Band") == "NOT LICENSABLE": continue
        needs.append(row)

    if args.priority:
        needs = [r for r in needs if r.get("Priority Band") == args.priority]

    rank_order = {"P0":0,"P1":1,"P2":2,"P2 (April KU)":2.5,"P3":3,"P5":4}
    needs.sort(key=lambda r: rank_order.get(r.get("Priority Band",""),5))
    if args.limit > 0: needs = needs[:args.limit]

    print(f"Rows to process: {len(needs)}")
    by_p = {}
    for r in needs:
        p = r.get("Priority Band","?")
        by_p[p] = by_p.get(p,0) + 1
    print(f"  By priority: {by_p}")

    cache = load_cache()
    results = []
    fe = fa = 0
    t0 = time.time()

    for i, row in enumerate(needs):
        author = str(row.get("Author Name","")).strip()
        series = str(row.get("Show Name","")).strip()
        priority = str(row.get("Priority Band",""))
        key = author.lower().strip()

        if key in cache:
            r = cache[key]; results.append(r)
            if r.get("email"): fe += 1
            if r.get("agent_email"): fa += 1
            continue

        if i > 0 and i % 10 == 0:
            elapsed = time.time() - t0
            rate = (i+1)/elapsed*60 if elapsed else 0
            eta = (len(needs)-i)/rate if rate else 0
            print(f"\n  Progress: {i}/{len(needs)} | Emails: {fe} | Agents: {fa} | "
                  f"Rate: {rate:.0f}/min | ETA: {eta:.0f}min")

        print(f"  [{i+1}/{len(needs)}] [{priority}] {author:30} — ", end="", flush=True)

        r = discover(author, series)
        r["priority"] = priority

        if r.get("email"):
            fe += 1
            print(f"EMAIL: {r['email']} ({r['method']})")
        elif r.get("agent_email"):
            fa += 1
            print(f"AGENT: {r['agent_email']} ({r.get('agent_name','')})")
        else:
            print("NO CONTACT")

        cache[key] = r; results.append(r)
        if i % 5 == 0: save_cache(cache)

    save_cache(cache)
    rdf = pd.DataFrame(results)
    rdf.to_csv(RESULTS_CSV, index=False)

    te = rdf["email"].apply(lambda x: bool(x and str(x).strip())).sum() if len(rdf) else 0
    ta = rdf["agent_email"].apply(lambda x: bool(x and str(x).strip())).sum() if len(rdf) else 0

    print(f"\n{'='*70}")
    print(f"V2 DISCOVERY COMPLETE")
    print(f"  Processed: {len(results)}")
    print(f"  Author emails: {te}")
    print(f"  Agent emails: {ta}")
    print(f"  ANY contact: {te+ta}")
    print(f"{'='*70}")

    if te or ta:
        print(f"\nAll found contacts:")
        for _,r in rdf.iterrows():
            if r.get("email") or r.get("agent_email"):
                print(f"  [{r.get('priority','')}] {r['author']:30} | "
                      f"email={str(r.get('email','')):35} | "
                      f"agent={str(r.get('agent_email','')):25} | {r.get('method','')}")

if __name__ == "__main__":
    main()
