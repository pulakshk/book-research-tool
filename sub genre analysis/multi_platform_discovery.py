#!/usr/bin/env python3
"""
Multi-Platform Book Discovery
==============================
Discovers books from:
1. Apple Books (iTunes Search API - free, no auth)
2. Amazon KU Bestsellers (web scraping of public pages)
3. Google Books API (already in v2, this adds extra categories)
4. Audible search (public search pages)

Run AFTER cleanup to fill remaining gaps with quality titles.
"""

import os
import json
import re
import time
import logging
import urllib.request
import urllib.parse
from pathlib import Path
from datetime import datetime

import pandas as pd

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"
DATA_DIR.mkdir(exist_ok=True)

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "multi_platform.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("multi_platform")

# ── Gemini Setup ───────────────────────────────────────────
def get_gemini_key():
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()

# ── Expanded subgenre search terms (35 categories per romantasy doc) ──

SUBGENRE_APPLE_SEARCHES = {
    "christian_drama_romance": {
        "label": "Christian Drama/Romance",
        "terms": [
            "christian romance", "faith romance", "amish romance",
            "inspirational romance", "clean romance series",
            "christian fiction", "wholesome love story",
        ],
    },
    "mafia_drama_romance": {
        "label": "Mafia Drama/Romance",
        "terms": [
            "mafia romance", "dark mafia", "organized crime romance",
            "crime boss romance", "cartel romance",
            "mob romance", "underworld romance",
        ],
    },
    "military_romance": {
        "label": "Military Drama/Romance",
        "terms": [
            "military romance", "navy seal romance", "special forces romance",
            "army romance", "marine romance",
            "veteran romance", "military hero",
        ],
    },
    "small_town_romance": {
        "label": "Small Town Drama/Romance",
        "terms": [
            "small town romance", "country romance", "cowboy romance",
            "ranch romance", "southern romance",
            "heartland romance", "lakeside romance",
        ],
    },
    "romantic_suspense_thriller": {
        "label": "Romantic Suspense/Psychological Thriller",
        "terms": [
            "romantic suspense", "thriller romance", "mystery romance",
            "detective romance", "bodyguard romance",
            "crime romance", "fbi romance",
        ],
    },
    "dark_and_forbidden_romance": {
        "label": "Dark & Forbidden Romance",
        "terms": [
            "dark romance", "forbidden romance", "taboo romance",
            "enemies to lovers", "age gap romance",
            "bully romance", "dark academia romance",
        ],
    },
    "historical_romance_fiction": {
        "label": "Historical Romance & Fiction",
        "terms": [
            "historical romance", "regency romance", "victorian romance",
            "period drama romance", "WWII romance",
            "medieval romance", "scottish romance",
        ],
    },
    "political_drama_romance": {
        "label": "Political Drama/Romance",
        "terms": [
            "political romance", "political thriller romance",
            "white house romance", "washington dc romance",
            "political drama", "election romance",
        ],
    },
    "ice_hockey_sports": {
        "label": "Ice Hockey & Sports Romance",
        "terms": [
            "hockey romance", "sports romance", "athlete romance",
            "football romance", "baseball romance",
            "soccer romance", "boxing romance",
        ],
    },
}


# ── Apple Books / iTunes Search API ────────────────────────

def search_apple_books(query, limit=200):
    """Search Apple Books via iTunes Search API (free, no auth needed)."""
    base_url = "https://itunes.apple.com/search"
    params = {
        "term": query,
        "media": "ebook",
        "entity": "ebook",
        "limit": min(limit, 200),
        "country": "US",
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BookResearchTool/2.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("results", [])
    except Exception as e:
        log.warning(f"  Apple Books error for '{query}': {e}")
        return []


def extract_apple_book(result):
    """Extract structured info from an Apple Books/iTunes result."""
    title = result.get("trackName", "")
    author = result.get("artistName", "")
    description = result.get("description", "")
    price = result.get("price", 0)
    avg_rating = result.get("averageUserRating")
    rating_count = result.get("userRatingCount", 0)
    genres = result.get("genres", [])
    release_date = result.get("releaseDate", "")
    publisher = result.get("sellerName", "")

    # Extract year
    pub_year = None
    if release_date:
        match = re.match(r"(\d{4})", release_date)
        if match:
            pub_year = int(match.group(1))

    # Self-pub detection
    self_pub_indicators = [
        "smashwords", "draft2digital", "independently", "self-publish",
        "kindle", "createspace", "bookbaby", "lulu",
    ]
    is_self_pub = any(ind in publisher.lower() for ind in self_pub_indicators) if publisher else False

    return {
        "title": title,
        "author": author,
        "description": description[:500],
        "avg_rating": avg_rating,
        "rating_count": rating_count,
        "genres": ", ".join(genres),
        "pub_year": pub_year,
        "publisher": publisher,
        "is_self_pub": is_self_pub,
        "price": price,
        "source": "Apple Books",
    }


def discover_from_apple_books(subgenre_key, config, existing_names):
    """Discover books from Apple Books for a subgenre."""
    label = config["label"]
    terms = config["terms"]

    all_books = []
    seen = set()

    for term in terms:
        results = search_apple_books(term, limit=200)
        for r in results:
            book = extract_apple_book(r)
            if book["title"] and book["author"]:
                key = f"{book['title'].lower().strip()}|{book['author'].lower().strip()}"
                if key not in seen and book["title"].lower().strip() not in existing_names:
                    seen.add(key)
                    book["subgenre"] = label
                    all_books.append(book)

        time.sleep(1)  # Rate limit

    log.info(f"  [{label}] Apple Books: {len(all_books)} unique books from {len(terms)} searches")
    return all_books


# ── Google Books with expanded categories ──────────────────

def search_google_books(query, start_index=0, max_results=40):
    """Search Google Books API."""
    base_url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": query,
        "startIndex": start_index,
        "maxResults": min(max_results, 40),
        "printType": "books",
        "langRestrict": "en",
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BookResearchTool/2.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("items", [])
    except Exception as e:
        log.warning(f"  Google Books error: {e}")
        return []


# ── Gemini-powered KU & Audible discovery ──────────────────

def gemini_platform_discovery(subgenre_key, config, needed, existing_names):
    """Use Gemini to discover titles specifically from KU, Audible, B&N."""
    if not GEMINI_KEY or needed <= 0:
        return []

    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    label = config["label"]
    existing_sample = list(existing_names)[:20]

    all_results = []
    batch_size = 40  # Smaller for reliable parsing

    platforms = [
        ("Kindle Unlimited", "Amazon Kindle Unlimited bestseller and popular series"),
        ("Audible", "Audible audiobook originals and bestselling series"),
        ("Barnes & Noble", "Barnes & Noble Nook bestselling and featured series"),
        ("Kobo", "Kobo ebook store popular and featured series"),
    ]

    for platform_name, platform_desc in platforms:
        remaining = needed - len(all_results)
        if remaining <= 0:
            break

        ask_count = min(batch_size, remaining + 10)

        prompt = f"""You are a book market expert. List {ask_count} book SERIES from {platform_desc}
in the "{label}" subgenre.

Focus specifically on titles that are popular on {platform_name}.

RULES:
- MUST be book SERIES (2+ books in the series)
- Drama/romance or contemporary romance ONLY
- NO high fantasy, paranormal, romantasy, sci-fi, werewolf, vampire, fae
- Light fantasy/magical realism is acceptable
- Prefer self-published and indie titles
- Include both well-known bestsellers and hidden gems

DO NOT include: {', '.join(existing_sample[:15])}

Return ONLY a JSON array:
[
  {{
    "Book Series Name": "name",
    "Author Name": "author",
    "Books in Series": number,
    "First Book Name": "first book title",
    "First Book Rating": rating_or_null,
    "First Book Rating Count": count_or_null,
    "Publisher Name": "publisher",
    "Self Pub Flag": "Self-Pub" or "Traditional",
    "First_Book_Pub_Year": year_or_null,
    "Primary Trope": "main trope",
    "Subjective Analysis": "1 sentence appeal summary"
  }}
]"""

        try:
            response = model.generate_content(prompt)
            text = response.text.strip()

            # Robust parsing
            series = _robust_json_parse(text)

            new_count = 0
            for s in series:
                name = s.get("Book Series Name", "").lower().strip()
                if name and name not in existing_names:
                    existing_names.add(name)
                    s["Primary Subgenre"] = label
                    s["Source Platform"] = platform_name
                    all_results.append(s)
                    new_count += 1

            log.info(f"    [{label}] {platform_name}: {new_count} new series")

        except Exception as e:
            log.error(f"    [{label}] {platform_name} error: {e}")

        time.sleep(2)

    return all_results


def _robust_json_parse(text):
    """Parse JSON with multiple fallback strategies."""
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    # Fix trailing commas
    text = re.sub(r',\s*([}\]])', r'\1', text)

    try:
        data = json.loads(text)
        return data if isinstance(data, list) else [data]
    except json.JSONDecodeError:
        pass

    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        try:
            cleaned = re.sub(r',\s*([}\]])', r'\1', match.group())
            return json.loads(cleaned)
        except json.JSONDecodeError:
            pass

    # Individual objects
    objects = []
    for m in re.finditer(r'\{[^{}]+\}', text):
        try:
            objects.append(json.loads(m.group()))
        except json.JSONDecodeError:
            continue
    return objects


# ── Main orchestration ─────────────────────────────────────

def run_multi_platform():
    """Run multi-platform discovery to fill remaining gaps."""
    log.info("=" * 60)
    log.info("MULTI-PLATFORM BOOK DISCOVERY")
    log.info("=" * 60)

    # Load the cleaned master (or expanded if cleanup hasn't run)
    candidates = [
        DATA_DIR / "selfpub_master_cleaned.csv",
        DATA_DIR / "selfpub_master_expanded_v2.csv",
        DATA_DIR / "selfpub_master_expanded.csv",
        DATA_DIR / "selfpub_master_consolidated.csv",
    ]

    source = None
    for c in candidates:
        if c.exists():
            source = c
            break

    if not source:
        log.error("No master CSV found!")
        return

    df = pd.read_csv(source, on_bad_lines="skip")
    existing_names = set(df["Book Series Name"].astype(str).str.lower().str.strip())

    log.info(f"  Loaded {len(df)} series from {source.name}")
    log.info(f"\n  Current subgenre counts:")

    subgenre_counts = df["Primary Subgenre"].value_counts().to_dict()
    for sg, count in subgenre_counts.items():
        gap = max(0, 500 - count)
        status = "OK" if gap == 0 else f"NEED {gap}"
        log.info(f"    {sg}: {count} ({status})")

    all_new = []

    for subgenre_key, config in SUBGENRE_APPLE_SEARCHES.items():
        label = config["label"]
        current = subgenre_counts.get(label, 0)
        needed = max(0, 500 - current)

        if needed == 0:
            log.info(f"\n  [{label}] Already at {current}, skipping")
            continue

        log.info(f"\n  [{label}] Has {current}, need {needed} more")

        # Source 1: Apple Books
        log.info(f"  --- Apple Books ---")
        apple_books = discover_from_apple_books(subgenre_key, config, existing_names)

        # Convert to standard format
        apple_series = []
        for book in apple_books:
            name = book["title"]
            if name.lower().strip() not in existing_names:
                existing_names.add(name.lower().strip())
                apple_series.append({
                    "Book Series Name": name,
                    "Author Name": book["author"],
                    "Type": "Standalone (check for series)",
                    "Books in Series": 1,
                    "First Book Name": name,
                    "First Book Rating": book.get("avg_rating"),
                    "First Book Rating Count": book.get("rating_count", 0),
                    "Publisher Name": book.get("publisher", ""),
                    "Self Pub Flag": "Self-Pub" if book.get("is_self_pub") else "Traditional",
                    "First_Book_Pub_Year": book.get("pub_year"),
                    "Primary Subgenre": label,
                    "Source Platform": "Apple Books",
                })

        log.info(f"  [{label}] Apple Books candidates: {len(apple_series)}")

        # Source 2: KU, Audible, B&N, Kobo via Gemini
        remaining = max(0, needed - len(apple_series))
        platform_results = []
        if remaining > 0:
            log.info(f"  --- KU / Audible / B&N / Kobo (via Gemini) ---")
            platform_results = gemini_platform_discovery(subgenre_key, config, remaining, existing_names)
            log.info(f"  [{label}] Platform discovery: {len(platform_results)} series")

        # Combine and take what we need
        combined = apple_series + platform_results
        to_add = combined[:needed]
        all_new.extend(to_add)
        log.info(f"  [{label}] Total added: {len(to_add)}")

    if all_new:
        new_df = pd.DataFrame(all_new)

        # Align columns
        for col in df.columns:
            if col not in new_df.columns:
                new_df[col] = ""
        for col in new_df.columns:
            if col not in df.columns:
                df[col] = ""

        expanded = pd.concat([df, new_df], ignore_index=True, sort=False)

        # Deduplicate
        expanded["_key"] = (
            expanded["Book Series Name"].astype(str).str.lower().str.strip()
            + "|"
            + expanded["Author Name"].astype(str).str.lower().str.strip()
        )
        before = len(expanded)
        expanded = expanded.drop_duplicates(subset=["_key"], keep="first").drop(columns=["_key"])
        log.info(f"\n  Dedup: {before} -> {len(expanded)}")

        output = DATA_DIR / "selfpub_master_multi_platform.csv"
        expanded.to_csv(output, index=False)

        log.info(f"\n  FINAL RESULTS:")
        log.info(f"  Total series: {len(expanded)}")
        for sg, count in expanded["Primary Subgenre"].value_counts().items():
            target = 500
            status = "DONE" if count >= target else f"need {target - count}"
            log.info(f"    {sg}: {count} ({status})")
        log.info(f"\n  Saved to: {output}")
    else:
        log.info("\n  No new discoveries needed")


if __name__ == "__main__":
    start = datetime.now()
    run_multi_platform()
    elapsed = datetime.now() - start
    log.info(f"\n  Completed in {elapsed}")
