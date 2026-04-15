#!/usr/bin/env python3
"""
Enhanced Book Discovery Pipeline v2
====================================
Uses Google Books API for real discovery + Gemini for enrichment.
Incorporates best practices from the romantasy commissioning framework:
- Series format filter
- 40H+ show length threshold
- GR ratings >= 1K for audience scale
- Need-gap driven portfolio approach

Sources: Google Books API, Open Library API, Gemini enrichment
"""

import os
import sys
import json
import re
import time
import csv
import logging
import urllib.request
import urllib.parse
import urllib.error
from pathlib import Path
from datetime import datetime

import pandas as pd

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"
DATA_DIR.mkdir(exist_ok=True)

MASTER_CSV = DATA_DIR / "selfpub_master_expanded.csv"
if not MASTER_CSV.exists():
    MASTER_CSV = DATA_DIR / "selfpub_master_consolidated.csv"
DISCOVERY_CSV = DATA_DIR / "discovery_v2_raw.csv"
FINAL_EXPANDED_CSV = DATA_DIR / "selfpub_master_expanded_v2.csv"

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "discovery_v2.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("discovery_v2")

# ── Gemini Setup ───────────────────────────────────────────
def get_gemini_key():
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()

# ── Subgenre search configurations (expanded to 35 categories per romantasy team) ──
SUBGENRE_SEARCHES = {
    "christian_drama_romance": {
        "label": "Christian Drama/Romance",
        "queries": [
            "christian romance series",
            "faith based romance novels",
            "amish romance series",
            "inspirational romance fiction",
            "clean christian romance series",
            "religious romance novels",
            "christian suspense romance",
            "wholesome romance series",
            "church romance fiction",
            "christian women fiction series",
            "prairie romance series",
            "christian historical romance",
            "mennonite romance fiction",
            "christian military romance",
            "grace filled romance series",
        ],
    },
    "mafia_drama_romance": {
        "label": "Mafia Drama/Romance",
        "queries": [
            "mafia romance series",
            "organized crime romance novels",
            "dark mafia romance",
            "cartel romance series",
            "crime family romance fiction",
            "mafia arranged marriage romance",
            "italian mafia romance",
            "russian mafia romance series",
            "crime boss romance",
            "underworld romance fiction",
            "mafia enemies to lovers",
            "mafia dark romance series",
            "bratva romance novels",
            "mob boss romance series",
            "crime syndicate romance",
        ],
    },
    "military_romance": {
        "label": "Military Drama/Romance",
        "queries": [
            "military romance series",
            "navy seal romance novels",
            "army romance series",
            "special forces romance",
            "veteran romance fiction",
            "military suspense romance",
            "delta force romance series",
            "marine romance novels",
            "military hero romance",
            "wounded warrior romance",
            "military romantic suspense series",
            "air force romance fiction",
            "coast guard romance",
            "military brotherhood romance",
            "soldier homecoming romance",
        ],
    },
    "small_town_romance": {
        "label": "Small Town Drama/Romance",
        "queries": [
            "small town romance series",
            "small town contemporary romance",
            "rural romance novels",
            "hometown romance series",
            "country romance fiction",
            "cozy small town romance",
            "ranch romance series",
            "cowboy romance novels",
            "southern romance fiction",
            "lakeside romance series",
            "mountain town romance",
            "vineyard romance series",
            "coastal town romance",
            "farming community romance",
            "heartland romance fiction",
        ],
    },
    "romantic_suspense_thriller": {
        "label": "Romantic Suspense/Psychological Thriller",
        "queries": [
            "romantic suspense series",
            "psychological thriller romance",
            "fbi romance series",
            "detective romance novels",
            "mystery romance series",
            "crime thriller romance",
            "kidnapping romance thriller",
            "witness protection romance",
            "serial killer romance thriller",
            "bodyguard romance series",
            "private investigator romance",
            "forensic romance thriller",
            "stalker romance suspense",
            "undercover romance series",
            "cold case romance thriller",
        ],
    },
}

# ── Google Books API (free, no auth) ───────────────────────

def search_google_books(query, start_index=0, max_results=40):
    """Search Google Books API. Returns list of volume dicts."""
    base_url = "https://www.googleapis.com/books/v1/volumes"
    params = {
        "q": query,
        "startIndex": start_index,
        "maxResults": min(max_results, 40),
        "printType": "books",
        "langRestrict": "en",
        "orderBy": "relevance",
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BookResearchTool/2.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("items", [])
    except Exception as e:
        log.warning(f"  Google Books API error for '{query}': {e}")
        return []


def extract_book_info(volume):
    """Extract structured info from a Google Books volume."""
    info = volume.get("volumeInfo", {})
    sale = volume.get("saleInfo", {})

    authors = info.get("authors", [])
    author = authors[0] if authors else ""

    # Try to detect series from title
    title = info.get("title", "")
    subtitle = info.get("subtitle", "")
    full_title = f"{title}: {subtitle}" if subtitle else title

    # Series detection from description or title
    description = info.get("description", "")
    categories = info.get("categories", [])
    page_count = info.get("pageCount", 0)
    avg_rating = info.get("averageRating", None)
    ratings_count = info.get("ratingsCount", 0)
    publisher = info.get("publisher", "")
    published_date = info.get("publishedDate", "")
    isbn_list = info.get("industryIdentifiers", [])
    isbn = ""
    for i in isbn_list:
        if i.get("type") == "ISBN_13":
            isbn = i.get("identifier", "")
            break

    # Detect self-pub
    self_pub_indicators = [
        "independently published", "createspace", "kindle direct",
        "smashwords", "draft2digital", "lulu", "bookbaby",
        "self-published", "amazon publishing",
    ]
    is_self_pub = any(ind in publisher.lower() for ind in self_pub_indicators) if publisher else False

    # Extract year
    pub_year = None
    if published_date:
        match = re.match(r"(\d{4})", published_date)
        if match:
            pub_year = int(match.group(1))

    return {
        "title": title,
        "full_title": full_title,
        "author": author,
        "description": description,
        "categories": ", ".join(categories),
        "page_count": page_count,
        "avg_rating": avg_rating,
        "ratings_count": ratings_count,
        "publisher": publisher,
        "published_date": published_date,
        "pub_year": pub_year,
        "isbn": isbn,
        "is_self_pub": is_self_pub,
        "google_books_id": volume.get("id", ""),
    }


def discover_from_google_books(subgenre_key, config, existing_series):
    """Discover new book series from Google Books for a subgenre."""
    label = config["label"]
    queries = config["queries"]

    all_books = []
    seen_titles = set()

    for query in queries:
        # Search multiple pages per query
        for start_idx in [0, 40]:
            results = search_google_books(query, start_index=start_idx)
            for vol in results:
                book = extract_book_info(vol)
                if book["title"] and book["author"]:
                    dedup_key = f"{book['title'].lower().strip()}|{book['author'].lower().strip()}"
                    if dedup_key not in seen_titles:
                        seen_titles.add(dedup_key)
                        book["source_query"] = query
                        book["subgenre"] = label
                        all_books.append(book)

            time.sleep(0.5)  # Rate limit

    log.info(f"  [{label}] Google Books returned {len(all_books)} unique books from {len(queries)} queries")

    # Group books by author to identify series
    author_books = {}
    for book in all_books:
        author = book["author"].strip()
        if author:
            if author not in author_books:
                author_books[author] = []
            author_books[author].append(book)

    # Authors with multiple books likely have series
    series_candidates = []
    for author, books in author_books.items():
        if len(books) >= 2:
            # Group into potential series by looking at common title patterns
            series_groups = _group_into_series(books)
            for series_name, series_books in series_groups.items():
                if len(series_books) >= 2:
                    key = f"{series_name.lower().strip()}|{author.lower().strip()}"
                    if key not in existing_series:
                        total_pages = sum(b.get("page_count", 300) for b in series_books)
                        first_book = series_books[0]
                        series_candidates.append({
                            "Book Series Name": series_name,
                            "Author Name": author,
                            "Type": "Long Series" if len(series_books) >= 6 else "Series",
                            "Books in Series": len(series_books),
                            "Total Pages": total_pages,
                            "Length of Adaption in Hours": round(total_pages / 33.33, 1),
                            "First Book Name": first_book["title"],
                            "First Book Rating": first_book.get("avg_rating"),
                            "First Book Rating Count": first_book.get("ratings_count", 0),
                            "Publisher Name": first_book.get("publisher", ""),
                            "Self Pub Flag": "Self-Pub" if first_book.get("is_self_pub") else "Traditional",
                            "First_Book_Pub_Year": first_book.get("pub_year"),
                            "Primary Subgenre": label,
                            "Source Platform": "Google Books",
                        })

        # Even single books by prolific self-pub authors are worth capturing
        elif len(books) == 1 and books[0].get("is_self_pub"):
            book = books[0]
            if book.get("page_count", 0) >= 200:
                key = f"{book['title'].lower().strip()}|{author.lower().strip()}"
                if key not in existing_series:
                    series_candidates.append({
                        "Book Series Name": book["title"],
                        "Author Name": author,
                        "Type": "Standalone (check for series)",
                        "Books in Series": 1,
                        "Total Pages": book.get("page_count", 300),
                        "Length of Adaption in Hours": round(book.get("page_count", 300) / 33.33, 1),
                        "First Book Name": book["title"],
                        "First Book Rating": book.get("avg_rating"),
                        "First Book Rating Count": book.get("ratings_count", 0),
                        "Publisher Name": book.get("publisher", ""),
                        "Self Pub Flag": "Self-Pub",
                        "First_Book_Pub_Year": book.get("pub_year"),
                        "Primary Subgenre": label,
                        "Source Platform": "Google Books",
                    })

    log.info(f"  [{label}] Identified {len(series_candidates)} series/title candidates")
    return series_candidates


def _group_into_series(books):
    """Group books by common title patterns to identify series."""
    series = {}

    # Strategy 1: Common prefix in titles (e.g., "The Mafia King's...", "The Mafia King's...")
    titles = [b["title"] for b in books]

    # Find common 2+ word prefixes
    for i, book in enumerate(books):
        words = book["title"].split()
        for j, other_book in enumerate(books):
            if i == j:
                continue
            other_words = other_book["title"].split()
            # Find common prefix length
            common_len = 0
            for k in range(min(len(words), len(other_words))):
                if words[k].lower() == other_words[k].lower():
                    common_len = k + 1
                else:
                    break

            if common_len >= 2:
                series_name = " ".join(words[:common_len])
                if series_name not in series:
                    series[series_name] = []
                if book not in series[series_name]:
                    series[series_name].append(book)
                if other_book not in series[series_name]:
                    series[series_name].append(other_book)

    # Strategy 2: Same author, similar categories = potential series
    if not series:
        # Treat all books by this author as a potential series
        author = books[0]["author"]
        series_name = f"{author}'s Series"
        series[series_name] = books

    return series


# ── Gemini enrichment for series discovery ──────────────────

def gemini_discover_series(subgenre_key, config, needed, existing_series):
    """Use Gemini with smaller batches and better parsing to discover series."""
    if not GEMINI_KEY:
        log.error("No Gemini API key!")
        return []

    import google.generativeai as genai
    genai.configure(api_key=GEMINI_KEY)
    model = genai.GenerativeModel("gemini-2.5-flash")

    label = config["label"]
    queries = config["queries"]

    # Split into smaller batches of 50 series each (fixes parse failures)
    batch_size = 50
    num_batches = max(1, (needed + batch_size - 1) // batch_size)
    all_series = []

    existing_sample = list(existing_series)[:30]

    for batch_idx in range(num_batches):
        remaining = needed - len(all_series)
        if remaining <= 0:
            break

        batch_needed = min(batch_size, remaining + 20)  # Ask for a few extra

        # Vary the search angle per batch
        angle_queries = queries[batch_idx % len(queries): batch_idx % len(queries) + 3]

        prompt = f"""You are a book market research expert. Find {batch_needed} self-published book SERIES
in the "{label}" subgenre.

Focus search terms: {', '.join(angle_queries)}

RULES:
- MUST be book SERIES (2+ books)
- Drama/romance or contemporary only — NO high fantasy, NO romantasy, NO paranormal
- Light fantasy/magical realism OK
- Prefer self-published / indie
- Mix well-known and lesser-known

DO NOT include: {', '.join(existing_sample[:15])}

Return a JSON array (ONLY the array, nothing else):
[
  {{
    "Book Series Name": "name",
    "Author Name": "author",
    "Books in Series": number,
    "First Book Name": "title",
    "First Book Rating": rating_or_null,
    "First Book Rating Count": count_or_null,
    "Publisher Name": "publisher",
    "Self Pub Flag": "Self-Pub" or "Traditional",
    "First_Book_Pub_Year": year_or_null,
    "Primary Trope": "main trope",
    "Subjective Analysis": "1 sentence appeal"
  }}
]"""

        try:
            response = model.generate_content(prompt)
            text = response.text.strip()

            # Robust JSON extraction
            series_list = _robust_json_parse(text)

            new_count = 0
            for s in series_list:
                key = s.get("Book Series Name", "").lower().strip()
                if key and key not in existing_series:
                    existing_series.add(key)
                    s["Primary Subgenre"] = label
                    s["Source Platform"] = "Gemini Discovery"
                    all_series.append(s)
                    new_count += 1

            log.info(f"    [{label}] Batch {batch_idx+1}/{num_batches}: {new_count} new series")

        except Exception as e:
            log.error(f"    [{label}] Batch {batch_idx+1} error: {e}")

        time.sleep(2)

    return all_series


def _robust_json_parse(text):
    """Robust JSON parsing with multiple fallback strategies."""
    text = text.strip()

    # Remove markdown code fences
    if text.startswith("```json"):
        text = text[7:]
    elif text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()

    # Strategy 1: Direct parse
    try:
        data = json.loads(text)
        if isinstance(data, list):
            return data
        return [data]
    except json.JSONDecodeError:
        pass

    # Strategy 2: Find JSON array with regex
    match = re.search(r'\[[\s\S]*\]', text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    # Strategy 3: Try to fix common issues (trailing commas, etc)
    try:
        # Remove trailing commas before ] or }
        cleaned = re.sub(r',\s*([}\]])', r'\1', text)
        match = re.search(r'\[[\s\S]*\]', cleaned)
        if match:
            return json.loads(match.group())
    except json.JSONDecodeError:
        pass

    # Strategy 4: Parse line by line for individual JSON objects
    objects = []
    for m in re.finditer(r'\{[^{}]+\}', text):
        try:
            obj = json.loads(m.group())
            objects.append(obj)
        except json.JSONDecodeError:
            continue

    if objects:
        return objects

    log.warning(f"  All JSON parse strategies failed. Response length: {len(text)}")
    return []


# ── Open Library API (free, community data) ────────────────

def search_open_library(query, limit=100):
    """Search Open Library for books."""
    base_url = "https://openlibrary.org/search.json"
    params = {
        "q": query,
        "limit": limit,
        "language": "eng",
    }
    url = f"{base_url}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "BookResearchTool/2.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("docs", [])
    except Exception as e:
        log.warning(f"  Open Library error for '{query}': {e}")
        return []


def discover_from_open_library(subgenre_key, config, existing_series):
    """Discover books from Open Library."""
    label = config["label"]
    queries = config["queries"][:5]  # Use first 5 queries

    all_books = []
    seen = set()

    for query in queries:
        results = search_open_library(query, limit=100)
        for doc in results:
            title = doc.get("title", "")
            authors = doc.get("author_name", [])
            author = authors[0] if authors else ""

            if not title or not author:
                continue

            key = f"{title.lower().strip()}|{author.lower().strip()}"
            if key in seen or key in existing_series:
                continue
            seen.add(key)

            # Check for series
            num_editions = doc.get("edition_count", 1)
            first_publish_year = doc.get("first_publish_year")
            subjects = doc.get("subject", [])
            pages = doc.get("number_of_pages_median", 300)
            ratings_avg = doc.get("ratings_average")
            ratings_count = doc.get("ratings_count", 0)

            all_books.append({
                "title": title,
                "author": author,
                "first_publish_year": first_publish_year,
                "pages": pages,
                "ratings_avg": ratings_avg,
                "ratings_count": ratings_count,
                "editions": num_editions,
                "subjects": subjects[:10],
                "subgenre": label,
            })

        time.sleep(1)

    log.info(f"  [{label}] Open Library returned {len(all_books)} books")
    return all_books


# ── Main discovery orchestration ───────────────────────────

def run_discovery():
    """Run the full v2 discovery pipeline."""
    log.info("=" * 60)
    log.info("ENHANCED DISCOVERY PIPELINE v2")
    log.info("=" * 60)

    # Load existing master
    if MASTER_CSV.exists():
        master = pd.read_csv(MASTER_CSV, on_bad_lines="skip")
    else:
        log.error("No master CSV found! Run consolidation first.")
        return

    existing_series = set(
        (master["Book Series Name"].astype(str).str.lower().str.strip()
         + "|"
         + master["Author Name"].astype(str).str.lower().str.strip())
    )
    existing_names = set(master["Book Series Name"].astype(str).str.lower().str.strip())

    log.info(f"  Existing master: {len(master)} series")
    log.info(f"\n  Current subgenre counts:")
    for sg, count in master["Primary Subgenre"].value_counts().items():
        target = 500
        gap = max(0, target - count)
        status = "OK" if gap == 0 else f"NEED {gap} more"
        log.info(f"    {sg}: {count} ({status})")

    # Discover for each subgenre that needs more
    all_new = []

    for subgenre_key, config in SUBGENRE_SEARCHES.items():
        label = config["label"]
        current_count = len(master[master["Primary Subgenre"] == label])
        needed = max(0, 500 - current_count)

        if needed == 0:
            log.info(f"\n  [{label}] Already has {current_count} series, skipping")
            continue

        log.info(f"\n  [{label}] Has {current_count}, need {needed} more")
        log.info(f"  --- Searching Google Books ---")

        # Source 1: Google Books API
        google_results = discover_from_google_books(subgenre_key, config, existing_names)

        # Source 2: Open Library
        log.info(f"  --- Searching Open Library ---")
        ol_results = discover_from_open_library(subgenre_key, config, existing_names)

        # Convert Open Library results to our format
        for book in ol_results:
            key = book["title"].lower().strip()
            if key not in existing_names:
                existing_names.add(key)
                google_results.append({
                    "Book Series Name": book["title"],
                    "Author Name": book["author"],
                    "Type": "Standalone (check for series)",
                    "Books in Series": 1,
                    "Total Pages": book.get("pages", 300),
                    "Length of Adaption in Hours": round(book.get("pages", 300) / 33.33, 1),
                    "First Book Name": book["title"],
                    "First Book Rating": book.get("ratings_avg"),
                    "First Book Rating Count": book.get("ratings_count", 0),
                    "First_Book_Pub_Year": book.get("first_publish_year"),
                    "Primary Subgenre": label,
                    "Source Platform": "Open Library",
                })

        api_count = len(google_results)
        log.info(f"  [{label}] Total from APIs: {api_count} candidates")

        # Source 3: Gemini for remaining gap
        still_needed = max(0, needed - api_count)
        if still_needed > 0:
            log.info(f"  --- Gemini discovery for {still_needed} remaining ---")
            gemini_results = gemini_discover_series(subgenre_key, config, still_needed, existing_names)
            google_results.extend(gemini_results)

        # Take what we need
        all_new.extend(google_results[:needed])
        log.info(f"  [{label}] Added {min(len(google_results), needed)} series")

    if all_new:
        new_df = pd.DataFrame(all_new)

        # Ensure all master columns exist
        master_cols = list(master.columns)
        for col in master_cols:
            if col not in new_df.columns:
                new_df[col] = ""
        for col in new_df.columns:
            if col not in master.columns:
                master[col] = ""

        expanded = pd.concat([master, new_df], ignore_index=True, sort=False)

        # Deduplicate
        expanded["_key"] = (
            expanded["Book Series Name"].astype(str).str.lower().str.strip()
            + "|"
            + expanded["Author Name"].astype(str).str.lower().str.strip()
        )
        before = len(expanded)
        expanded = expanded.drop_duplicates(subset=["_key"], keep="first").drop(columns=["_key"])
        log.info(f"\n  Dedup: {before} -> {len(expanded)} ({before - len(expanded)} removed)")

        expanded.to_csv(FINAL_EXPANDED_CSV, index=False)

        log.info(f"\n  FINAL RESULTS:")
        log.info(f"  Total series: {len(expanded)}")
        for sg, count in expanded["Primary Subgenre"].value_counts().items():
            target = 500
            status = "DONE" if count >= target else f"need {target - count} more"
            log.info(f"    {sg}: {count} ({status})")

        log.info(f"\n  Saved to: {FINAL_EXPANDED_CSV}")
    else:
        log.info("  No new discoveries to add")


if __name__ == "__main__":
    start = datetime.now()
    run_discovery()
    elapsed = datetime.now() - start
    log.info(f"\n  Pipeline completed in {elapsed}")
