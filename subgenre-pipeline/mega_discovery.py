#!/usr/bin/env python3
"""
Mega Discovery Pipeline
========================
Massively expanded book discovery targeting 1500-2000+ series per subgenre
across 9 subgenres, casting the widest possible net.

Sources:
  1. Apple Books (iTunes Search API) - 25-30 terms per subgenre, limit=200 each
  2. Google Books API - 30+ terms per subgenre, paginated (4 pages per query)
  3. Gemini-powered discovery for KU, Audible, B&N, Kobo, Smashwords, Draft2Digital

Run AFTER multi_platform_discovery.py to massively expand the dataset.
"""

import os
import json
import re
import time
import logging
import urllib.request
import urllib.parse
import traceback
from pathlib import Path
from datetime import datetime

import pandas as pd

# ── Paths ──────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
DATA_DIR = BASE_DIR / "output"
DATA_DIR.mkdir(exist_ok=True)

OUTPUT_FILE = DATA_DIR / "selfpub_master_mega_expanded.csv"
EXISTING_MASTER = DATA_DIR / "selfpub_master_multi_platform.csv"

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(DATA_DIR / "mega_discovery.log"),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("mega_discovery")

# ── Gemini Setup ───────────────────────────────────────────
def get_gemini_key():
    """Load GEMINI_API_KEY from .env or environment."""
    env_path = PROJECT_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")

GEMINI_KEY = get_gemini_key()

# ── Exclusion filters ─────────────────────────────────────
EXCLUDE_KEYWORDS = [
    "werewolf", "vampire", "shifter", "fae", "dragon", "witch",
    "wizard", "sorcerer", "demon", "angel wings", "paranormal",
    "fantasy realm", "romantasy", "high fantasy", "urban fantasy",
    "fairy", "faerie", "elf", "elven", "troll", "orc", "goblin",
    "sci-fi", "science fiction", "space opera", "alien", "cyborg",
    "dystopian", "post-apocalyptic", "zombie",
]

def is_excluded(title, description=""):
    """Check if a title/description contains excluded fantasy/paranormal terms."""
    combined = f"{title} {description}".lower()
    return any(kw in combined for kw in EXCLUDE_KEYWORDS)


# ══════════════════════════════════════════════════════════
#  SUBGENRE DEFINITIONS - 25-30+ search terms each
# ══════════════════════════════════════════════════════════

SUBGENRES = {
    "christian_drama_romance": {
        "label": "Christian Drama/Romance",
        "apple_terms": [
            "christian romance", "faith romance", "amish romance",
            "inspirational romance", "clean romance series", "christian fiction",
            "wholesome love story", "amish fiction series", "christian love story",
            "faith-based fiction", "church romance", "pastor romance",
            "missionary romance", "mennonite romance", "christian contemporary",
            "grace-filled romance", "prayer romance", "biblical fiction romance",
            "christian women fiction", "heartwarming christian",
            "christian historical romance", "clean sweet romance",
            "clean billionaire romance", "christian cowboy romance",
            "christian military romance",
        ],
        "google_terms": [
            'subject:"christian romance"', 'subject:"inspirational romance"',
            'subject:"amish romance"', 'subject:"faith" "romance"',
            'subject:"clean romance"', '"christian fiction" romance series',
            '"amish fiction" series', '"inspirational" "love story"',
            'subject:"christian fiction" romance', '"wholesome romance" series',
            '"church" "romance" fiction', '"pastor" "romance"',
            '"missionary" "romance"', '"mennonite" "romance" fiction',
            '"christian contemporary" romance', '"grace" "romance" fiction',
            '"biblical fiction" romance', '"christian women" fiction romance',
            '"heartwarming christian" fiction', '"christian historical romance"',
            '"clean sweet romance"', '"clean billionaire" romance',
            '"christian cowboy" romance', '"christian military" romance',
            'subject:"inspirational fiction" romance',
            '"faith-based" "love story"', '"prayer" "romance" fiction',
            '"christian" "second chance" romance', '"christian" "small town" romance',
            '"Bethany House" romance', '"Revell" christian romance',
        ],
        "gemini_extra_context": "Christian, faith-based, inspirational, clean/sweet, Amish",
    },
    "mafia_drama_romance": {
        "label": "Mafia Drama/Romance",
        "apple_terms": [
            "mafia romance", "dark mafia", "organized crime romance",
            "crime boss romance", "cartel romance", "mob romance",
            "underworld romance", "italian mafia romance",
            "russian mafia romance", "bratva romance", "yakuza romance",
            "crime family romance", "don romance", "godfather romance",
            "mafia arranged marriage", "mafia forced proximity",
            "mafia enemies to lovers", "mafia dark romance",
            "sicilian romance", "mafia boss", "cosa nostra romance",
            "mafia heir", "underground crime romance", "mafia princess",
            "mafia captive romance",
        ],
        "google_terms": [
            'subject:"mafia romance"', 'subject:"dark mafia"',
            '"organized crime" romance', '"crime boss" romance',
            '"cartel" romance fiction', '"mob" romance series',
            '"underworld" romance', '"italian mafia" romance',
            '"russian mafia" romance', '"bratva" romance',
            '"yakuza" romance', '"crime family" romance fiction',
            '"mafia arranged marriage"', '"mafia" "enemies to lovers"',
            '"mafia dark romance"', '"sicilian" romance',
            '"cosa nostra" romance', '"mafia heir" romance',
            '"mafia princess"', '"mafia captive" romance',
            '"mafia" "forced proximity"', '"crime lord" romance',
            '"kingpin" romance', '"mob boss" romance series',
            '"irish mob" romance', '"gang" romance contemporary',
            '"mafia" "second chance" romance', '"mafia" "secret baby"',
            '"criminal underworld" romance', '"syndicate" romance',
        ],
        "gemini_extra_context": "Mafia, mob, cartel, bratva, crime family, organized crime",
    },
    "military_romance": {
        "label": "Military Drama/Romance",
        "apple_terms": [
            "military romance", "navy seal romance", "special forces romance",
            "army romance", "marine romance", "veteran romance",
            "military hero", "air force romance", "coast guard romance",
            "military wife", "military family", "soldier romance",
            "deployment romance", "military suspense", "combat romance",
            "military contemporary", "military small town",
            "wounded warrior romance", "military second chance",
            "delta force romance", "ranger romance",
            "military protector", "military commander romance",
            "navy romance", "green beret romance",
        ],
        "google_terms": [
            'subject:"military romance"', '"navy seal" romance',
            '"special forces" romance', 'subject:"army" romance',
            '"marine" romance fiction', '"veteran" romance series',
            '"air force" romance', '"coast guard" romance',
            '"military wife" fiction', '"soldier" romance',
            '"deployment" romance', '"military suspense"',
            '"wounded warrior" romance', '"delta force" romance',
            '"ranger" romance fiction', '"military protector"',
            '"military commander" romance', '"navy" romance series',
            '"green beret" romance', '"military" "second chance" romance',
            '"military" "small town" romance', '"combat" "romance"',
            '"military family" fiction', '"special ops" romance',
            '"sniper" romance', '"military" "enemies to lovers"',
            '"pilot" "military" romance', '"military" "fake marriage"',
            '"military" "homecoming" romance', '"military hero" series',
        ],
        "gemini_extra_context": "Military, Navy SEAL, special forces, veteran, Army, Marines",
    },
    "small_town_romance": {
        "label": "Small Town Drama/Romance",
        "apple_terms": [
            "small town romance", "country romance", "cowboy romance",
            "ranch romance", "southern romance", "heartland romance",
            "lakeside romance", "mountain town romance", "farm romance",
            "rural romance", "hometown romance", "country life romance",
            "orchard romance", "vineyard romance", "bakery romance",
            "café romance", "bookshop romance", "inn romance",
            "lodge romance", "beach town romance", "island romance",
            "coastal romance", "fishing village romance",
            "summer romance small town", "holiday small town",
        ],
        "google_terms": [
            'subject:"small town romance"', '"country" romance series',
            '"cowboy" romance fiction', '"ranch" romance series',
            '"southern" romance', '"heartland" romance',
            '"lakeside" romance', '"mountain town" romance',
            '"farm" romance fiction', '"rural" romance',
            '"hometown" romance', '"orchard" romance',
            '"vineyard" romance', '"bakery" romance fiction',
            '"bookshop" romance', '"inn" romance fiction',
            '"lodge" romance', '"beach town" romance',
            '"island" romance contemporary', '"coastal" romance',
            '"fishing village" romance', '"summer" "small town" romance',
            '"holiday" "small town" romance', '"country life" romance',
            '"small town" "second chance"', '"small town" "fake dating"',
            '"small town" "enemies to lovers"', '"Hallmark" style romance',
            '"country" "love story" series', '"rancher" romance',
        ],
        "gemini_extra_context": "Small town, cowboy, ranch, southern, lakeside, mountain, coastal, rural",
    },
    "romantic_suspense_thriller": {
        "label": "Romantic Suspense/Psychological Thriller",
        "apple_terms": [
            "romantic suspense", "thriller romance", "mystery romance",
            "detective romance", "bodyguard romance", "crime romance",
            "fbi romance", "cia romance", "police romance",
            "undercover romance", "witness protection romance",
            "stalker romance", "kidnapping romance",
            "missing person romance", "cold case romance",
            "forensic romance", "profiler romance", "agent romance",
            "spy romance", "suspense series", "romantic thriller series",
            "danger romance", "protector romance", "security romance",
            "private investigator romance",
        ],
        "google_terms": [
            'subject:"romantic suspense"', '"thriller" romance fiction',
            '"mystery" romance series', '"detective" romance',
            '"bodyguard" romance', 'subject:"crime" romance',
            '"fbi" romance fiction', '"cia" romance',
            '"police" romance series', '"undercover" romance',
            '"witness protection" romance', '"stalker" romance suspense',
            '"kidnapping" romance', '"missing person" romance',
            '"cold case" romance', '"forensic" romance',
            '"profiler" romance', '"spy" romance fiction',
            '"romantic thriller" series', '"protector" romance',
            '"security" romance', '"private investigator" romance',
            '"agent" romance suspense', '"danger" romance series',
            '"mystery" "love story" suspense', '"crime" "thriller" romance',
            '"serial killer" romance', '"suspense" "second chance"',
            '"dark suspense" romance', '"psychological" romance thriller',
        ],
        "gemini_extra_context": "Romantic suspense, thriller, mystery, FBI, bodyguard, detective, spy",
    },
    "dark_and_forbidden_romance": {
        "label": "Dark & Forbidden Romance",
        "apple_terms": [
            "dark romance", "forbidden romance", "taboo romance",
            "enemies to lovers", "age gap romance", "bully romance",
            "dark academia romance", "possessive hero romance",
            "captive romance", "forced proximity dark",
            "dark billionaire", "stalker romance dark",
            "obsessive love", "toxic romance", "anti-hero romance",
            "villain romance", "dark contemporary", "forbidden love story",
            "secret relationship romance", "power imbalance romance",
            "morally grey hero", "dark possessive",
            "kidnapped romance", "dark college romance",
            "dark enemies to lovers",
        ],
        "google_terms": [
            'subject:"dark romance"', '"forbidden romance" fiction',
            '"taboo romance"', '"enemies to lovers" romance',
            '"age gap" romance', '"bully romance"',
            '"dark academia" romance', '"possessive hero" romance',
            '"captive romance"', '"forced proximity" "dark"',
            '"dark billionaire" romance', '"obsessive love" fiction',
            '"anti-hero" romance', '"villain" romance contemporary',
            '"dark contemporary" romance', '"forbidden love" story',
            '"secret relationship" romance', '"morally grey" romance',
            '"dark possessive" romance', '"dark college" romance',
            '"dark" "enemies to lovers"', '"power imbalance" romance',
            '"toxic" romance fiction', '"kidnapped" romance dark',
            '"bully" "romance" series', '"dark" "second chance" romance',
            '"forbidden" "love story" series', '"dark" "fake dating"',
            '"dark" "arranged marriage"', '"alpha" "dark" romance',
        ],
        "gemini_extra_context": "Dark romance, forbidden, taboo, enemies to lovers, bully romance, possessive",
    },
    "historical_romance_fiction": {
        "label": "Historical Romance & Fiction",
        "apple_terms": [
            "historical romance", "regency romance", "victorian romance",
            "period drama romance", "WWII romance", "medieval romance",
            "scottish romance", "duke romance", "earl romance",
            "lord romance", "lady romance", "georgian romance",
            "edwardian romance", "1920s romance", "gilded age romance",
            "civil war romance", "frontier romance", "colonial romance",
            "tudor romance", "Bridgerton style", "ton romance",
            "debutante romance", "rake romance", "wallflower romance",
            "bluestocking romance",
        ],
        "google_terms": [
            'subject:"historical romance"', 'subject:"regency romance"',
            '"victorian" romance fiction', '"period drama" romance',
            '"WWII" romance', '"medieval" romance fiction',
            '"scottish" romance series', '"duke" romance',
            '"earl" romance fiction', '"lord" romance historical',
            '"georgian" romance', '"edwardian" romance',
            '"1920s" romance fiction', '"gilded age" romance',
            '"civil war" romance', '"frontier" romance',
            '"colonial" romance fiction', '"tudor" romance',
            '"Bridgerton" style romance', '"ton" romance regency',
            '"debutante" romance', '"rake" romance',
            '"wallflower" romance', '"bluestocking" romance',
            '"highland" romance series', '"pirate" romance historical',
            '"Napoleonic" romance', '"Roaring Twenties" romance',
            '"duchess" romance', '"marquess" romance',
            '"viscount" romance', '"historical" "enemies to lovers"',
        ],
        "gemini_extra_context": "Historical, regency, Victorian, duke, Scottish, WWII, medieval, Bridgerton",
    },
    "political_drama_romance": {
        "label": "Political Drama/Romance",
        "apple_terms": [
            "political romance", "political thriller romance",
            "white house romance", "washington dc romance",
            "political drama", "election romance", "senator romance",
            "congressman romance", "lobbyist romance",
            "political campaign romance", "capitol hill romance",
            "governor romance", "political scandal romance",
            "diplomat romance", "embassy romance",
            "first lady romance", "political intrigue romance",
            "activist romance", "journalist political romance",
            "power couple washington",
        ],
        "google_terms": [
            'subject:"political romance"', '"political thriller" romance',
            '"white house" romance fiction', '"washington dc" romance',
            '"political drama" fiction', '"election" romance',
            '"senator" romance fiction', '"congressman" romance',
            '"lobbyist" romance', '"political campaign" romance',
            '"capitol hill" romance', '"governor" romance',
            '"political scandal" romance', '"diplomat" romance',
            '"embassy" romance fiction', '"first lady" romance',
            '"political intrigue" romance', '"activist" romance',
            '"journalist" "political" romance', '"power couple" "washington"',
            '"political" "love story"', '"politics" romance series',
            '"political" "enemies to lovers"', '"political" "forbidden"',
            '"presidential" romance', '"attorney general" romance',
            '"mayor" romance fiction', '"campaign trail" romance',
            '"political" "second chance"', '"political power" romance',
        ],
        "gemini_extra_context": "Political, White House, Washington DC, senator, diplomat, election, Capitol Hill",
    },
    "ice_hockey_sports": {
        "label": "Ice Hockey & Sports Romance",
        "apple_terms": [
            "hockey romance", "sports romance", "athlete romance",
            "football romance", "baseball romance", "soccer romance",
            "boxing romance", "basketball romance", "rugby romance",
            "racing romance", "MMA romance", "fighter romance",
            "coach romance", "sports contemporary", "college sports romance",
            "professional athlete romance", "stadium romance",
            "team romance", "rival teams romance",
            "sports enemies to lovers", "championship romance",
            "underdog romance", "sports second chance",
            "locker room romance", "sports fake dating",
        ],
        "google_terms": [
            'subject:"hockey romance"', 'subject:"sports romance"',
            '"athlete" romance fiction', '"football" romance series',
            '"baseball" romance fiction', '"soccer" romance',
            '"boxing" romance', '"basketball" romance series',
            '"rugby" romance', '"racing" romance fiction',
            '"MMA" romance', '"fighter" romance',
            '"coach" romance fiction', '"college sports" romance',
            '"professional athlete" romance', '"team" romance',
            '"rival teams" romance', '"sports" "enemies to lovers"',
            '"championship" romance', '"underdog" romance sports',
            '"sports" "second chance" romance', '"locker room" romance',
            '"sports" "fake dating"', '"ice hockey" romance series',
            '"sports" "forbidden" romance', '"NFL" romance',
            '"quarterback" romance', '"pitcher" romance fiction',
            '"swimmer" romance', '"olympics" romance',
            '"sports" "arranged" romance',
        ],
        "gemini_extra_context": "Hockey, sports, football, baseball, athlete, college sports, MMA, boxing",
    },
}

# Variation suffixes for Apple Books searches
APPLE_VARIATIONS = ["series", "book 1", "box set"]

# ── Standard output columns ───────────────────────────────
STANDARD_COLUMNS = [
    "Book Series Name", "Author Name", "Type", "Books in Series",
    "First Book Name", "First Book Rating", "First Book Rating Count",
    "Publisher Name", "Self Pub Flag", "First_Book_Pub_Year",
    "Primary Subgenre", "Primary Trope", "Subjective Analysis",
    "Source Platform",
]


# ══════════════════════════════════════════════════════════
#  ROBUST JSON PARSER (from multi_platform_discovery.py)
# ══════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════
#  SOURCE 1: APPLE BOOKS (iTunes Search API)
# ══════════════════════════════════════════════════════════

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
        req = urllib.request.Request(url, headers={"User-Agent": "BookResearchTool/3.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("results", [])
    except Exception as e:
        log.warning(f"    Apple Books error for '{query}': {e}")
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
        year_match = re.match(r"(\d{4})", release_date)
        if year_match:
            pub_year = int(year_match.group(1))

    # Self-pub detection
    self_pub_indicators = [
        "smashwords", "draft2digital", "independently", "self-publish",
        "kindle", "createspace", "bookbaby", "lulu", "ingram",
        "kdp", "amazon digital", "d2d",
    ]
    is_self_pub = any(ind in publisher.lower() for ind in self_pub_indicators) if publisher else False

    return {
        "title": title,
        "author": author,
        "description": description[:500] if description else "",
        "avg_rating": avg_rating,
        "rating_count": rating_count,
        "genres": ", ".join(genres) if genres else "",
        "pub_year": pub_year,
        "publisher": publisher,
        "is_self_pub": is_self_pub,
        "price": price,
    }


def discover_apple_books(subgenre_key, config, existing_names, seen_keys):
    """Discover books from Apple Books for a subgenre with expanded terms + variations."""
    label = config["label"]
    base_terms = config["apple_terms"]
    results_list = []
    search_count = 0

    # Build full search list: base terms + variation combos
    all_queries = list(base_terms)
    for term in base_terms:
        for variation in APPLE_VARIATIONS:
            all_queries.append(f"{term} {variation}")

    log.info(f"    Apple Books: {len(all_queries)} queries for '{label}'")

    for query in all_queries:
        raw_results = search_apple_books(query, limit=200)
        search_count += 1
        batch_new = 0

        for r in raw_results:
            book = extract_apple_book(r)
            if not book["title"] or not book["author"]:
                continue

            # Exclusion filter
            if is_excluded(book["title"], book.get("description", "")):
                continue

            key = f"{book['title'].lower().strip()}|{book['author'].lower().strip()}"
            name_key = book["title"].lower().strip()

            if key not in seen_keys and name_key not in existing_names:
                seen_keys.add(key)
                existing_names.add(name_key)
                results_list.append({
                    "Book Series Name": book["title"],
                    "Author Name": book["author"],
                    "Type": "Standalone (check for series)",
                    "Books in Series": 1,
                    "First Book Name": book["title"],
                    "First Book Rating": book.get("avg_rating"),
                    "First Book Rating Count": book.get("rating_count", 0),
                    "Publisher Name": book.get("publisher", ""),
                    "Self Pub Flag": "Self-Pub" if book.get("is_self_pub") else "Traditional",
                    "First_Book_Pub_Year": book.get("pub_year"),
                    "Primary Subgenre": label,
                    "Primary Trope": "",
                    "Subjective Analysis": "",
                    "Source Platform": "Apple Books",
                })
                batch_new += 1

        if search_count % 10 == 0:
            log.info(f"      ... {search_count}/{len(all_queries)} queries done, "
                     f"{len(results_list)} unique so far")

        time.sleep(1)  # Rate limit: 1s per request

    log.info(f"    Apple Books DONE for '{label}': {len(results_list)} unique from "
             f"{search_count} searches")
    return results_list


# ══════════════════════════════════════════════════════════
#  SOURCE 2: GOOGLE BOOKS API
# ══════════════════════════════════════════════════════════

def search_google_books(query, start_index=0, max_results=40):
    """Search Google Books API (free, no auth needed)."""
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
        req = urllib.request.Request(url, headers={"User-Agent": "BookResearchTool/3.0"})
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("items", [])
    except Exception as e:
        log.warning(f"    Google Books error for '{query}' (start={start_index}): {e}")
        return []


def extract_google_book(item):
    """Extract structured info from a Google Books result."""
    volume = item.get("volumeInfo", {})
    title = volume.get("title", "")
    authors = volume.get("authors", [])
    author = ", ".join(authors) if authors else ""
    description = volume.get("description", "")
    publisher = volume.get("publisher", "")
    published_date = volume.get("publishedDate", "")
    categories = volume.get("categories", [])
    avg_rating = volume.get("averageRating")
    rating_count = volume.get("ratingsCount", 0)

    # Extract year
    pub_year = None
    if published_date:
        year_match = re.match(r"(\d{4})", published_date)
        if year_match:
            pub_year = int(year_match.group(1))

    # Self-pub detection
    self_pub_indicators = [
        "smashwords", "draft2digital", "independently published",
        "self-publish", "kindle", "createspace", "bookbaby", "lulu",
        "ingram", "kdp", "amazon digital", "d2d",
    ]
    is_self_pub = any(ind in publisher.lower() for ind in self_pub_indicators) if publisher else False

    return {
        "title": title,
        "author": author,
        "description": description[:500] if description else "",
        "avg_rating": avg_rating,
        "rating_count": rating_count,
        "categories": ", ".join(categories) if categories else "",
        "pub_year": pub_year,
        "publisher": publisher,
        "is_self_pub": is_self_pub,
    }


def discover_google_books(subgenre_key, config, existing_names, seen_keys):
    """Discover books from Google Books for a subgenre with pagination."""
    label = config["label"]
    terms = config["google_terms"]
    results_list = []
    search_count = 0
    pages_per_query = 4  # startIndex = 0, 40, 80, 120

    log.info(f"    Google Books: {len(terms)} terms x {pages_per_query} pages "
             f"= {len(terms) * pages_per_query} requests for '{label}'")

    for term in terms:
        for page in range(pages_per_query):
            start_index = page * 40
            raw_results = search_google_books(term, start_index=start_index, max_results=40)
            search_count += 1

            if not raw_results:
                break  # No more results for this term

            for item in raw_results:
                book = extract_google_book(item)
                if not book["title"] or not book["author"]:
                    continue

                # Exclusion filter
                if is_excluded(book["title"], book.get("description", "")):
                    continue

                key = f"{book['title'].lower().strip()}|{book['author'].lower().strip()}"
                name_key = book["title"].lower().strip()

                if key not in seen_keys and name_key not in existing_names:
                    seen_keys.add(key)
                    existing_names.add(name_key)
                    results_list.append({
                        "Book Series Name": book["title"],
                        "Author Name": book["author"],
                        "Type": "Standalone (check for series)",
                        "Books in Series": 1,
                        "First Book Name": book["title"],
                        "First Book Rating": book.get("avg_rating"),
                        "First Book Rating Count": book.get("rating_count", 0),
                        "Publisher Name": book.get("publisher", ""),
                        "Self Pub Flag": "Self-Pub" if book.get("is_self_pub") else "Traditional",
                        "First_Book_Pub_Year": book.get("pub_year"),
                        "Primary Subgenre": label,
                        "Primary Trope": "",
                        "Subjective Analysis": "",
                        "Source Platform": "Google Books",
                    })

            time.sleep(1.5)  # Rate limit: 1.5s per request

        if search_count % 20 == 0:
            log.info(f"      ... {search_count} requests done, "
                     f"{len(results_list)} unique so far")

    log.info(f"    Google Books DONE for '{label}': {len(results_list)} unique from "
             f"{search_count} requests")
    return results_list


# ══════════════════════════════════════════════════════════
#  SOURCE 3: GEMINI-POWERED DISCOVERY
#  (KU, Audible, B&N, Kobo, Smashwords, Draft2Digital)
# ══════════════════════════════════════════════════════════

GEMINI_PLATFORMS = [
    ("Kindle Unlimited", "Amazon Kindle Unlimited bestseller and popular series"),
    ("Audible", "Audible audiobook originals and bestselling series"),
    ("Barnes & Noble", "Barnes & Noble Nook bestselling and featured series"),
    ("Kobo", "Kobo ebook store popular and featured series"),
    ("Smashwords", "Smashwords indie ebook store popular and featured series"),
    ("Draft2Digital", "Draft2Digital distributed indie ebook series"),
]


def gemini_discover_batch(model, platform_name, platform_desc, label, extra_context,
                          batch_num, ask_count, exclude_sample):
    """Ask Gemini for a single batch of titles from a platform."""
    prompt = f"""You are a book market research expert with deep knowledge of {platform_name}.

List exactly {ask_count} book SERIES available on {platform_desc} in the "{label}" subgenre.

Focus areas: {extra_context}

Batch {batch_num}: Provide DIFFERENT titles than typical top-10 lists. Dig deep into the catalog.
Include a mix of bestsellers, mid-list gems, and hidden indie finds.

STRICT RULES:
- MUST be book SERIES (2+ books in the series)
- Drama/romance and contemporary romance ONLY
- NO high fantasy, paranormal, romantasy, sci-fi, werewolf, vampire, fae, shifter
- Light fantasy/magical realism is acceptable
- Include BOTH self-published AND traditionally published titles
- Each entry must be a REAL published book series

DO NOT include any of these (already in our database):
{chr(10).join('- ' + t for t in exclude_sample[:25])}

Return ONLY a valid JSON array with no other text:
[
  {{
    "Book Series Name": "series name",
    "Author Name": "author name",
    "Books in Series": number,
    "First Book Name": "title of first book",
    "First Book Rating": rating_float_or_null,
    "First Book Rating Count": count_int_or_null,
    "Publisher Name": "publisher name",
    "Self Pub Flag": "Self-Pub" or "Traditional",
    "First_Book_Pub_Year": year_int_or_null,
    "Primary Trope": "main trope in 3-5 words",
    "Subjective Analysis": "1 sentence appeal summary"
  }}
]"""

    try:
        response = model.generate_content(prompt)
        text = response.text.strip()
        series = _robust_json_parse(text)
        return series
    except Exception as e:
        log.error(f"      Gemini error ({platform_name}, batch {batch_num}): {e}")
        return []


def discover_gemini_platforms(subgenre_key, config, existing_names, seen_keys):
    """Use Gemini to discover titles from KU, Audible, B&N, Kobo, Smashwords, D2D."""
    if not GEMINI_KEY:
        log.warning("    Gemini API key not found, skipping Gemini discovery")
        return []

    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_KEY)
        model = genai.GenerativeModel("gemini-2.5-flash")
    except ImportError:
        log.error("    google-generativeai package not installed. Run: pip install google-generativeai")
        return []
    except Exception as e:
        log.error(f"    Gemini setup error: {e}")
        return []

    label = config["label"]
    extra_context = config.get("gemini_extra_context", label)
    results_list = []

    # Build exclude sample from existing names
    exclude_sample = sorted(list(existing_names))[:50]

    for platform_name, platform_desc in GEMINI_PLATFORMS:
        log.info(f"      Gemini -> {platform_name} for '{label}'")
        platform_new = 0

        # Request 50 titles per platform, in a single batch
        ask_count = 50
        series_list = gemini_discover_batch(
            model, platform_name, platform_desc, label, extra_context,
            batch_num=1, ask_count=ask_count, exclude_sample=exclude_sample,
        )

        for s in series_list:
            name = s.get("Book Series Name", "")
            author = s.get("Author Name", "")
            if not name or not author:
                continue

            name_key = name.lower().strip()
            key = f"{name_key}|{author.lower().strip()}"

            # Exclusion filter
            if is_excluded(name):
                continue

            if key not in seen_keys and name_key not in existing_names:
                seen_keys.add(key)
                existing_names.add(name_key)
                s["Primary Subgenre"] = label
                s["Source Platform"] = platform_name
                # Ensure standard fields exist
                for col in STANDARD_COLUMNS:
                    if col not in s:
                        s[col] = ""
                results_list.append(s)
                platform_new += 1

        log.info(f"        {platform_name}: {platform_new} new series")
        # Update exclude sample with newly found titles
        exclude_sample = sorted(list(existing_names))[:50]

        time.sleep(2)  # Rate limit: 2s between Gemini requests

    log.info(f"    Gemini DONE for '{label}': {len(results_list)} total new series")
    return results_list


# ══════════════════════════════════════════════════════════
#  INTERMEDIATE SAVE
# ══════════════════════════════════════════════════════════

def save_intermediate(all_new_records, existing_df, save_count):
    """Save intermediate results to prevent data loss."""
    if not all_new_records:
        return

    new_df = pd.DataFrame(all_new_records)

    # Ensure all standard columns exist
    for col in STANDARD_COLUMNS:
        if col not in new_df.columns:
            new_df[col] = ""

    # Merge with existing
    if existing_df is not None and len(existing_df) > 0:
        # Align columns
        for col in existing_df.columns:
            if col not in new_df.columns:
                new_df[col] = ""
        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([existing_df, new_df], ignore_index=True, sort=False)
    else:
        combined = new_df

    # Dedup
    combined["_key"] = (
        combined["Book Series Name"].astype(str).str.lower().str.strip()
        + "|"
        + combined["Author Name"].astype(str).str.lower().str.strip()
    )
    combined = combined.drop_duplicates(subset=["_key"], keep="first").drop(columns=["_key"])

    intermediate_path = DATA_DIR / f"selfpub_master_mega_intermediate_{save_count}.csv"
    combined.to_csv(intermediate_path, index=False)
    log.info(f"  ** Intermediate save #{save_count}: {len(combined)} total records -> {intermediate_path.name}")


# ══════════════════════════════════════════════════════════
#  MAIN ORCHESTRATION
# ══════════════════════════════════════════════════════════

def run_mega_discovery():
    """Run the mega discovery pipeline across all sources and subgenres."""
    log.info("=" * 70)
    log.info("  MEGA DISCOVERY PIPELINE")
    log.info("  Target: 1500-2000+ series per subgenre across 9 subgenres")
    log.info("=" * 70)

    # ── Load existing data ────────────────────────────────
    existing_df = None
    existing_names = set()

    # Try loading the existing master (from multi_platform_discovery.py)
    if EXISTING_MASTER.exists():
        try:
            existing_df = pd.read_csv(EXISTING_MASTER, on_bad_lines="skip")
            existing_names = set(
                existing_df["Book Series Name"].astype(str).str.lower().str.strip()
            )
            log.info(f"  Loaded existing master: {len(existing_df)} records from "
                     f"{EXISTING_MASTER.name}")
        except Exception as e:
            log.warning(f"  Could not load existing master: {e}")

    # Also check if a previous mega run exists
    if OUTPUT_FILE.exists():
        try:
            prev_mega = pd.read_csv(OUTPUT_FILE, on_bad_lines="skip")
            prev_names = set(
                prev_mega["Book Series Name"].astype(str).str.lower().str.strip()
            )
            existing_names.update(prev_names)
            # Use the larger dataset as base
            if existing_df is None or len(prev_mega) > len(existing_df):
                existing_df = prev_mega
            log.info(f"  Also loaded previous mega output: {len(prev_mega)} records")
        except Exception as e:
            log.warning(f"  Could not load previous mega output: {e}")

    log.info(f"  Total known titles for dedup: {len(existing_names)}")

    # Print current state
    if existing_df is not None and "Primary Subgenre" in existing_df.columns:
        log.info("\n  Current subgenre counts:")
        for sg, count in existing_df["Primary Subgenre"].value_counts().items():
            log.info(f"    {sg}: {count}")

    # ── Shared dedup state ────────────────────────────────
    seen_keys = set()  # title|author pairs seen in this run
    all_new_records = []
    save_counter = 0
    subgenre_tallies = {}

    # ══════════════════════════════════════════════════════
    #  PHASE 1: APPLE BOOKS
    # ══════════════════════════════════════════════════════
    log.info("\n" + "=" * 70)
    log.info("  PHASE 1: APPLE BOOKS (iTunes Search API)")
    log.info("=" * 70)

    for subgenre_key, config in SUBGENRES.items():
        label = config["label"]
        log.info(f"\n  >> [{label}] - Apple Books")

        try:
            apple_results = discover_apple_books(
                subgenre_key, config, existing_names, seen_keys
            )
            all_new_records.extend(apple_results)
            subgenre_tallies[label] = subgenre_tallies.get(label, 0) + len(apple_results)
            log.info(f"  << [{label}] Apple Books: +{len(apple_results)} "
                     f"(running total new: {len(all_new_records)})")
        except Exception as e:
            log.error(f"  !! [{label}] Apple Books FAILED: {e}")
            log.error(traceback.format_exc())

        # Intermediate save every 500 new records
        if len(all_new_records) >= (save_counter + 1) * 500:
            save_counter += 1
            save_intermediate(all_new_records, existing_df, save_counter)

    log.info(f"\n  Phase 1 complete. Total new from Apple Books: {len(all_new_records)}")

    # ══════════════════════════════════════════════════════
    #  PHASE 2: GOOGLE BOOKS
    # ══════════════════════════════════════════════════════
    log.info("\n" + "=" * 70)
    log.info("  PHASE 2: GOOGLE BOOKS API")
    log.info("=" * 70)

    pre_google_count = len(all_new_records)

    for subgenre_key, config in SUBGENRES.items():
        label = config["label"]
        log.info(f"\n  >> [{label}] - Google Books")

        try:
            google_results = discover_google_books(
                subgenre_key, config, existing_names, seen_keys
            )
            all_new_records.extend(google_results)
            subgenre_tallies[label] = subgenre_tallies.get(label, 0) + len(google_results)
            log.info(f"  << [{label}] Google Books: +{len(google_results)} "
                     f"(running total new: {len(all_new_records)})")
        except Exception as e:
            log.error(f"  !! [{label}] Google Books FAILED: {e}")
            log.error(traceback.format_exc())

        # Intermediate save every 500 new records
        if len(all_new_records) >= (save_counter + 1) * 500:
            save_counter += 1
            save_intermediate(all_new_records, existing_df, save_counter)

    google_total = len(all_new_records) - pre_google_count
    log.info(f"\n  Phase 2 complete. Total new from Google Books: {google_total}")

    # ══════════════════════════════════════════════════════
    #  PHASE 3: GEMINI-POWERED PLATFORMS
    # ══════════════════════════════════════════════════════
    log.info("\n" + "=" * 70)
    log.info("  PHASE 3: GEMINI-POWERED DISCOVERY")
    log.info("  Platforms: KU, Audible, B&N, Kobo, Smashwords, Draft2Digital")
    log.info("=" * 70)

    pre_gemini_count = len(all_new_records)

    for subgenre_key, config in SUBGENRES.items():
        label = config["label"]
        log.info(f"\n  >> [{label}] - Gemini Platforms")

        try:
            gemini_results = discover_gemini_platforms(
                subgenre_key, config, existing_names, seen_keys
            )
            all_new_records.extend(gemini_results)
            subgenre_tallies[label] = subgenre_tallies.get(label, 0) + len(gemini_results)
            log.info(f"  << [{label}] Gemini: +{len(gemini_results)} "
                     f"(running total new: {len(all_new_records)})")
        except Exception as e:
            log.error(f"  !! [{label}] Gemini FAILED: {e}")
            log.error(traceback.format_exc())

        # Intermediate save every 500 new records
        if len(all_new_records) >= (save_counter + 1) * 500:
            save_counter += 1
            save_intermediate(all_new_records, existing_df, save_counter)

    gemini_total = len(all_new_records) - pre_gemini_count
    log.info(f"\n  Phase 3 complete. Total new from Gemini: {gemini_total}")

    # ══════════════════════════════════════════════════════
    #  FINAL MERGE & SAVE
    # ══════════════════════════════════════════════════════
    log.info("\n" + "=" * 70)
    log.info("  FINAL MERGE & SAVE")
    log.info("=" * 70)

    if not all_new_records:
        log.info("  No new discoveries. Nothing to save.")
        return

    new_df = pd.DataFrame(all_new_records)

    # Ensure all standard columns exist in new data
    for col in STANDARD_COLUMNS:
        if col not in new_df.columns:
            new_df[col] = ""

    # Merge with existing
    if existing_df is not None and len(existing_df) > 0:
        # Align columns between existing and new
        for col in existing_df.columns:
            if col not in new_df.columns:
                new_df[col] = ""
        for col in new_df.columns:
            if col not in existing_df.columns:
                existing_df[col] = ""

        combined = pd.concat([existing_df, new_df], ignore_index=True, sort=False)
    else:
        combined = new_df

    # Final dedup on title+author
    combined["_key"] = (
        combined["Book Series Name"].astype(str).str.lower().str.strip()
        + "|"
        + combined["Author Name"].astype(str).str.lower().str.strip()
    )
    before_dedup = len(combined)
    combined = combined.drop_duplicates(subset=["_key"], keep="first").drop(columns=["_key"])
    after_dedup = len(combined)

    log.info(f"  Dedup: {before_dedup} -> {after_dedup} "
             f"(removed {before_dedup - after_dedup} duplicates)")

    # Save final output
    combined.to_csv(OUTPUT_FILE, index=False)
    log.info(f"  Saved final output: {OUTPUT_FILE}")

    # ── Final summary ─────────────────────────────────────
    log.info("\n" + "=" * 70)
    log.info("  MEGA DISCOVERY - FINAL SUMMARY")
    log.info("=" * 70)

    log.info(f"\n  Total records in final file: {len(combined)}")
    existing_count = len(existing_df) if existing_df is not None else 0
    log.info(f"  Previously existing:         {existing_count}")
    log.info(f"  New discoveries this run:    {len(all_new_records)}")
    log.info(f"  After dedup (net new):       {after_dedup - existing_count}")

    log.info(f"\n  New discoveries by source:")
    log.info(f"    Apple Books:  {pre_google_count}")
    log.info(f"    Google Books: {google_total}")
    log.info(f"    Gemini:       {gemini_total}")

    log.info(f"\n  New discoveries by subgenre (this run):")
    for label, count in sorted(subgenre_tallies.items(), key=lambda x: -x[1]):
        log.info(f"    {label}: +{count}")

    log.info(f"\n  Final counts by subgenre (all data):")
    if "Primary Subgenre" in combined.columns:
        final_counts = combined["Primary Subgenre"].value_counts()
        for sg in sorted(final_counts.index):
            count = final_counts[sg]
            target = 1500
            if count >= target:
                status = "TARGET MET"
            else:
                status = f"need {target - count} more"
            log.info(f"    {sg}: {count} ({status})")

    log.info(f"\n  Output file: {OUTPUT_FILE}")
    log.info("=" * 70)


# ══════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════

if __name__ == "__main__":
    start_time = datetime.now()
    log.info(f"  Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        run_mega_discovery()
    except KeyboardInterrupt:
        log.warning("\n  !! Interrupted by user. Partial results may be saved in intermediate files.")
    except Exception as e:
        log.error(f"\n  !! Fatal error: {e}")
        log.error(traceback.format_exc())

    elapsed = datetime.now() - start_time
    hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    log.info(f"\n  Completed in {hours}h {minutes}m {seconds}s")
