#!/usr/bin/env python3
"""
Repair the Amazon hockey working file using the stricter ice-hockey verification
pipeline plus selected Claude ideas:

- strict no-hallucination email handling
- explicit contact description
- sanity issue flags
- agent / representation capture when publicly available
- lineage mapping back to the verified workbook outputs
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from execution import repair_ice_hockey_outreach as base
from execution import verify_ice_hockey_complete as claude

AMAZON_SOURCE_CSV = (
    PROJECT_ROOT
    / "subgenre-pipeline"
    / "genre-crawl"
    / "Amazon Bestsellers _ Jan 2026 _ Hockey Romance - Cleaned Titles_ Sports & Hockey.csv"
)

OUT_DIR = PROJECT_ROOT / "outreach" / "ice-hockey"
OUTPUT_CSV = OUT_DIR / "exports" / "amazon_hockey_cleaned_titles_repaired.csv"
OUTPUT_XLSX = OUT_DIR / "verified" / "amazon_hockey_cleaned_titles_repaired.xlsx"
REPORT_MD = OUT_DIR / "reports" / "AMAZON_HOCKEY_REPAIR_REPORT_2026-04-05.md"

SUPPORT_FILES = [
    OUT_DIR / "exports" / "ice_hockey_outreach_verified.csv",
    OUT_DIR / "exports" / "ice_hockey_master_contacts_verified.csv",
    OUT_DIR / "exports" / "ice_hockey_author_contacts_verified.csv",
    PROJECT_ROOT / "data" / "subgenre_masters" / "ice_hockey_sports_romance_master.csv",
    PROJECT_ROOT / "data" / "subgenre_outreach" / "ice_hockey_sports_romance_outreach.csv",
    PROJECT_ROOT / "subgenre-pipeline" / "output" / "subgenre_outputs" / "ice_hockey_and_sports_romance_master.csv",
]

EXTRA_CONTACT_PATHS = [
    "",
    "/contact",
    "/about",
    "/representation",
    "/rights",
    "/media",
    "/press",
]

REPRESENTATION_KEYWORDS = [
    "represented by",
    "representation",
    "literary agent",
    "agent",
    "rights",
    "foreign rights",
    "film rights",
    "subrights",
    "publicity",
    "publicist",
]

EMAIL_AGENT_KEYWORDS = [
    "agent",
    "agency",
    "representation",
    "bookcase agency",
    "brower literary",
    "park fine brower",
    "jabberwocky",
    "knight agency",
]

EMAIL_PUBLICITY_KEYWORDS = [
    "represented by",
    "rights",
    "subrights",
    "foreign rights",
    "film rights",
    "publicity",
    "publicist",
    "media",
    "press",
    "pr ",
    "marketing",
]

EMAIL_OTHER_KEYWORDS = [
    "assistant",
    "admin",
    "office",
    "team",
    "newsletter",
    "general inquiries",
    "general enquiries",
]

GENERIC_LOCAL_PARTS = {"admin", "contact", "hello", "info", "office", "team", "books", "rights"}


SEVERITY_RANK = {"GREEN": 0, "YELLOW": 1, "RED": 2}


def load_amazon_working_file(path: Path) -> pd.DataFrame:
    with path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    if len(rows) < 3:
        raise ValueError(f"Unexpected file structure in {path}")

    headers = rows[1]
    width = len(headers)
    body = [row + [""] * (width - len(row)) if len(row) < width else row[:width] for row in rows[2:]]
    df = pd.DataFrame(body, columns=headers)
    df["_source_row"] = list(range(3, 3 + len(df)))
    return df


def dataframe_to_rows(df: pd.DataFrame, source_name: str) -> List[Dict[str, object]]:
    rows: List[Dict[str, object]] = []
    for idx, record in enumerate(df.to_dict("records"), start=2):
        row = dict(record)
        author = row.get("Author Name")
        series = row.get("Book Series Name")
        first = row.get("First Book Name")
        row["_sheet"] = source_name
        row["_excel_row"] = idx
        row["_author_key"] = base.norm_key(author)
        row["_series_key"] = base.norm_key(series)
        row["_first_book_key"] = base.norm_key(first)
        rows.append(row)
    return rows


def load_support_dataframes() -> Dict[str, pd.DataFrame]:
    support: Dict[str, pd.DataFrame] = {}
    for path in SUPPORT_FILES:
        if not path.exists():
            continue
        support[path.stem] = pd.read_csv(path, low_memory=False)
    return support


def norm(text: object) -> str:
    return re.sub(r"[^a-z0-9]+", " ", str(text or "").lower()).strip()


def exact_lineage_match(row: pd.Series, df: pd.DataFrame) -> Tuple[Optional[pd.Series], str]:
    author = norm(row.get("Author Name"))
    series = norm(row.get("Book Series Name"))
    first_book = norm(row.get("First Book Name"))

    if "Author Name" not in df.columns or "Book Series Name" not in df.columns:
        return None, ""

    author_match = df["Author Name"].astype(str).map(norm) == author
    series_match = df["Book Series Name"].astype(str).map(norm) == series
    if author and series:
        exact = df[author_match & series_match]
        if len(exact) == 1:
            return exact.iloc[0], "author+series"

    if "First Book Name" in df.columns and author and first_book:
        first_match = df["First Book Name"].astype(str).map(norm) == first_book
        exact = df[author_match & first_match]
        if len(exact) == 1:
            return exact.iloc[0], "author+first-book"

    return None, ""


def choose_author_candidate_locally(
    row: Dict[str, object],
    series_map: Dict[Tuple[str, str], base.LocalSeries],
    by_book_author: Dict[Tuple[str, str], base.LocalBook],
) -> Tuple[str, str]:
    local_series, reason = base.match_local_series(row, series_map, by_book_author)
    if local_series:
        return local_series.author, reason

    first_book_key = row.get("_first_book_key") or ""
    series_name = str(row.get("Book Series Name") or "")
    author_name = str(row.get("Author Name") or "")

    exact_title_candidates: List[base.LocalBook] = []
    for (title_key, _author_key), book in by_book_author.items():
        if title_key == first_book_key and first_book_key:
            exact_title_candidates.append(book)

    if exact_title_candidates:
        scored = []
        for book in exact_title_candidates:
            score = 4.0 * base.title_similarity(book.title, row.get("First Book Name"))
            score += 2.5 * base.title_similarity(book.series, series_name)
            score += 1.0 * base.title_similarity(book.author, author_name)
            scored.append((score, book))
        scored.sort(key=lambda item: item[0], reverse=True)
        if scored[0][0] >= 5.0:
            return scored[0][1].author, "local-first-book-any-author"

    best_series = None
    best_score = 0.0
    for local_series in series_map.values():
        score = 3.0 * base.title_similarity(local_series.series, series_name)
        if local_series.books and row.get("First Book Name"):
            first_titles = [book.title for book in local_series.books[:3]]
            score += max(base.title_similarity(title, row.get("First Book Name")) for title in first_titles)
        score += 0.5 * base.title_similarity(local_series.author, author_name)
        if score > best_score:
            best_score = score
            best_series = local_series

    if best_series and best_score >= 3.4:
        return best_series.author, "local-series-any-author"

    return author_name, ""


def confirm_email_on_page(http: base.CachedHttp, email: str, url: str) -> bool:
    if not email or not url.startswith("http"):
        return False
    html = http.get(url)
    if not html:
        return False
    emails = {normalize_email_value(e).lower() for e in base.extract_emails_from_html(html)}
    return normalize_email_value(email).lower() in emails


def normalize_email_value(email: object) -> str:
    clean = base.clean_email(str(email or "")).strip()
    if "?" in clean:
        clean = clean.split("?", 1)[0]
    return clean.strip()


def extract_agent_name_from_text(text: str) -> str:
    patterns = [
        r"represented by\s+([^.;|\n]+)",
        r"literary agent\s*[:\-]?\s*([^.;|\n]+)",
        r"for (?:foreign|film|audio|translation|subrights) rights(?: inquiries)?(?:,)?\s*contact\s+([^.;|\n]+)",
        r"contact\s+([^.;|\n]+)\s+for (?:foreign|film|audio|translation|subrights) rights",
    ]
    for pattern in patterns:
        match = re.search(pattern, text, re.I)
        if not match:
            continue
        candidate = re.sub(r"\s+", " ", match.group(1)).strip(" :-,")
        candidate = re.sub(r"\b(?:at|via)\b.*$", "", candidate, flags=re.I).strip(" :-,")
        if base.is_plausible_agent_text(candidate):
            return candidate
    return ""


def collect_email_contexts(soup: BeautifulSoup, email: str) -> List[str]:
    email_l = normalize_email_value(email).lower()
    contexts: List[str] = []
    for link in soup.select("a[href^='mailto:']"):
        href_email = normalize_email_value(link.get("href") or "").lower()
        if href_email != email_l:
            continue
        snippets = [
            link.get_text(" ", strip=True),
            link.parent.get_text(" ", strip=True) if link.parent else "",
            link.find_parent(["p", "div", "section", "li"]).get_text(" ", strip=True)
            if link.find_parent(["p", "div", "section", "li"])
            else "",
        ]
        contexts.extend([snippet for snippet in snippets if snippet])

    page_text = soup.get_text("\n", strip=True)
    lower_text = page_text.lower()
    start = 0
    while True:
        index = lower_text.find(email_l, start)
        if index == -1:
            break
        contexts.append(page_text[max(0, index - 180) : index + len(email_l) + 180])
        start = index + len(email_l)

    deduped: List[str] = []
    seen = set()
    for context in contexts:
        clean = re.sub(r"\s+", " ", context).strip()
        key = clean.lower()
        if clean and key not in seen:
            deduped.append(clean)
            seen.add(key)
    return deduped[:6]


def classify_contact_email(
    email: str,
    contexts: List[str],
    candidate: base.AuthorCandidate,
    website: str,
) -> str:
    clean = normalize_email_value(email)
    if not clean or "@" not in clean:
        return ""
    local_part, domain = clean.lower().split("@", 1)
    site_host = base.host_from_url(website)
    context_blob = " | ".join(contexts).lower()
    author_tokens = [token for token in base.norm_key(candidate.author).split() if len(token) > 2]
    compact_local = re.sub(r"[^a-z0-9]+", "", local_part)

    strong_agent_signal = any(keyword in context_blob for keyword in EMAIL_PUBLICITY_KEYWORDS)
    generic_agent_signal = any(keyword in context_blob for keyword in EMAIL_AGENT_KEYWORDS)

    if site_host and domain.endswith(site_host):
        if strong_agent_signal:
            return "agent"
        if local_part in GENERIC_LOCAL_PARTS:
            return "other"
        return "direct"
    if any(token in compact_local for token in author_tokens):
        if strong_agent_signal:
            return "agent"
        return "direct"
    if strong_agent_signal or generic_agent_signal:
        return "agent"
    if local_part in GENERIC_LOCAL_PARTS:
        return "other"
    if any(keyword in context_blob for keyword in EMAIL_OTHER_KEYWORDS):
        return "other"
    if site_host and website and context_blob:
        return "other"
    return ""


def has_strong_direct_email_signal(email: str, candidate: base.AuthorCandidate, website: str) -> bool:
    clean = normalize_email_value(email)
    if not clean or "@" not in clean:
        return False
    local_part, domain = clean.lower().split("@", 1)
    site_host = base.host_from_url(website)
    compact_local = re.sub(r"[^a-z0-9]+", "", local_part)
    author_tokens = [token for token in base.norm_key(candidate.author).split() if len(token) > 2]
    if site_host and domain.endswith(site_host):
        return True
    return any(token in compact_local for token in author_tokens)


def looks_like_representation_email(email: str) -> bool:
    clean = normalize_email_value(email).lower()
    if "@" not in clean:
        return False
    domain = clean.split("@", 1)[1]
    return any(token in domain for token in ["agency", "agent", "literary", "rights", "publicity", "media", "42west", "bookcase", "brower", "jabberwocky", "knight"])


def enforce_strict_direct_email(
    result: Dict[str, object],
    candidate: base.AuthorCandidate,
) -> Dict[str, object]:
    direct_email = normalize_email_value(result.get("Validated_Email") or "")
    if not direct_email:
        return result
    website = str(result.get("Validated_Website") or "")
    if has_strong_direct_email_signal(direct_email, candidate, website):
        return result

    source_url = str(result.get("Email_Source_URL") or "")
    if looks_like_representation_email(direct_email) or result.get("Agent_Name"):
        if not result.get("Agent_Email"):
            result["Agent_Email"] = direct_email
            result["Agent_Source_URL"] = source_url
    else:
        if not result.get("Other_Contact_Email"):
            result["Other_Contact_Email"] = direct_email
            result["Other_Contact_Source_URL"] = source_url

    result["Validated_Email"] = ""
    result["Email_Verified"] = False
    result["Email_Source_URL"] = ""
    result["Email_Source_Type"] = ""
    return result


def reconcile_secondary_contact_channels(result: Dict[str, object]) -> Dict[str, object]:
    agent_email = normalize_email_value(result.get("Agent_Email") or "")
    if not agent_email or "@" not in agent_email:
        return result

    site_host = base.host_from_url(str(result.get("Validated_Website") or ""))
    domain = agent_email.lower().split("@", 1)[1]
    should_demote = False
    if site_host and domain.endswith(site_host):
        should_demote = True
    if any(token in domain for token in ["pr", "publicity", "media", "marketing", "42west"]):
        should_demote = True

    if should_demote:
        if not result.get("Other_Contact_Email"):
            result["Other_Contact_Email"] = agent_email
            result["Other_Contact_Source_URL"] = result.get("Agent_Source_URL") or result.get("Agency_Source") or ""
        result["Agent_Email"] = ""
    return result


def normalize_representation_fields(result: Dict[str, object]) -> Dict[str, object]:
    agent_name = str(result.get("Agent_Name") or "").strip()
    site_host = base.host_from_url(str(result.get("Validated_Website") or ""))
    if not agent_name:
        return result

    partial_email_fragment = re.search(r":\s*[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+$", agent_name)
    if partial_email_fragment and not re.search(r"\.[A-Za-z]{2,}$", partial_email_fragment.group(0)):
        agent_name = agent_name.split(":", 1)[0].strip()
        result["Agent_Name"] = agent_name
        if result.get("Agency_Contact") and "@" in str(result.get("Agency_Contact")):
            result["Agency_Contact"] = agent_name

    embedded_match = re.search(r"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}", agent_name)
    if embedded_match:
        embedded_email = normalize_email_value(embedded_match.group(0))
        current_agent_email = normalize_email_value(result.get("Agent_Email") or "")
        current_is_site_host = bool(current_agent_email and site_host and current_agent_email.lower().split("@", 1)[1].endswith(site_host))
        if embedded_email and (not current_agent_email or current_is_site_host):
            if current_is_site_host and not result.get("Other_Contact_Email"):
                result["Other_Contact_Email"] = current_agent_email
                result["Other_Contact_Source_URL"] = result.get("Agent_Source_URL") or result.get("Agency_Source") or ""
            result["Agent_Email"] = embedded_email
        cleaned_name = re.sub(r"[,:]?\s*" + re.escape(embedded_match.group(0)), "", agent_name).strip(" ,:-")
        if cleaned_name:
            result["Agent_Name"] = cleaned_name
            if result.get("Agency_Contact") and str(result.get("Agency_Contact")).strip() == agent_name:
                result["Agency_Contact"] = cleaned_name
    return result


def choose_page_contact_channels(
    http: base.CachedHttp,
    page_url: str,
    candidate: base.AuthorCandidate,
    website: str,
) -> Dict[str, str]:
    result = {
        "direct_email": "",
        "direct_source": "",
        "other_email": "",
        "other_source": "",
        "agent_email": "",
        "agent_source": "",
        "agent_name": "",
    }
    if not page_url.startswith("http"):
        return result
    html = http.get(page_url)
    if not html:
        return result

    soup = BeautifulSoup(html, "html.parser")
    email_candidates = []
    for email in {normalize_email_value(item) for item in base.extract_emails_from_html(html)}:
        if not base.is_valid_public_email(email):
            continue
        contexts = collect_email_contexts(soup, email)
        role = classify_contact_email(email, contexts, candidate, website)
        if not role:
            continue
        local_part, domain = email.lower().split("@", 1)
        score = 0
        if role == "direct":
            if base.host_from_url(website) and domain.endswith(base.host_from_url(website)):
                score += 4
            score += sum(1 for token in base.norm_key(candidate.author).split() if len(token) > 2 and token in re.sub(r"[^a-z0-9]+", "", local_part))
        elif role == "other":
            score += 2 if local_part in GENERIC_LOCAL_PARTS else 0
        elif role == "agent":
            score += 3
            if any(keyword in " | ".join(contexts).lower() for keyword in EMAIL_AGENT_KEYWORDS):
                score += 2
        email_candidates.append((role, score, email, contexts))

    by_role: Dict[str, List[Tuple[int, str, List[str]]]] = {"direct": [], "other": [], "agent": []}
    for role, score, email, contexts in email_candidates:
        by_role[role].append((score, email, contexts))

    for role in by_role:
        by_role[role].sort(key=lambda item: (-item[0], item[1]))

    if by_role["direct"]:
        result["direct_email"] = by_role["direct"][0][1]
        result["direct_source"] = page_url
    if by_role["other"]:
        result["other_email"] = by_role["other"][0][1]
        result["other_source"] = page_url
    if by_role["agent"]:
        result["agent_email"] = by_role["agent"][0][1]
        result["agent_source"] = page_url
        contexts = by_role["agent"][0][2]
        extracted = extract_agent_name_from_text(" ".join(contexts))
        if extracted:
            result["agent_name"] = extracted
    return result


def scan_representation_details(
    http: base.CachedHttp,
    candidate: base.AuthorCandidate,
    website: str,
    author_email: str,
) -> Dict[str, object]:
    result = {
        "Agent_Name": "",
        "Agent_Email": "",
        "Agent_Website": "",
        "Agent_Source_URL": "",
        "Has_Contact_Form": False,
    }
    if not website:
        return result

    base_host = base.host_from_url(website)
    page_urls = []
    for suffix in EXTRA_CONTACT_PATHS:
        page_urls.append(base.urljoin(website.rstrip("/") + "/", suffix.lstrip("/")))
    for page_url in list(dict.fromkeys(page_urls)):
        html = http.get(page_url)
        if not html:
            continue
        soup = BeautifulSoup(html, "html.parser")
        if not base.page_mentions_author(soup, candidate.author):
            continue
        if soup.select_one("form"):
            result["Has_Contact_Form"] = True

        text = soup.get_text("\n", strip=True)
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for index, line in enumerate(lines):
            low = line.lower()
            if not any(keyword in low for keyword in REPRESENTATION_KEYWORDS):
                continue
            window = " ".join(lines[max(0, index - 1) : index + 2])
            emails = [email for email in base.extract_emails_from_html(window) if email.lower() != author_email.lower()]
            if emails and not result["Agent_Email"]:
                result["Agent_Email"] = emails[0]
                result["Agent_Source_URL"] = page_url
            if not result["Agent_Name"]:
                extracted = extract_agent_name_from_text(window)
                if extracted:
                    result["Agent_Name"] = extracted
                    result["Agent_Source_URL"] = page_url

        if not result["Agent_Name"]:
            for known_agent in candidate.literary_agents:
                tokens = [token for token in base.norm_key(known_agent).split() if len(token) > 3]
                if tokens and all(token in text.lower() for token in tokens[:2]):
                    result["Agent_Name"] = known_agent
                    result["Agent_Source_URL"] = page_url
                    break

        for link in soup.select("a[href]"):
            raw_href = (link.get("href") or "").strip()
            if not raw_href:
                continue
            href = base.urljoin(page_url, raw_href)
            if not href.startswith("http"):
                continue
            label = link.get_text(" ", strip=True).lower()
            href_lower = href.lower()
            if any(keyword in label or keyword in href_lower for keyword in ["agency", "literary", "rights", "agent", "subrights"]):
                host = base.host_from_url(href)
                if base.is_candidate_website(href) and host != base_host:
                    result["Agent_Website"] = href
                    if not result["Agent_Source_URL"]:
                        result["Agent_Source_URL"] = page_url
                    break

    return result


def gemini_contact_enrichment(
    http: base.CachedHttp,
    author_name: str,
    website: str,
) -> Dict[str, object]:
    gem = claude.gemini_find_author_contact(author_name, website)
    result = {
        "Validated_Email": "",
        "Email_Source_URL": "",
        "Email_Source_Type": "",
        "Validated_Website": "",
        "Agent_Name": "",
        "Agent_Source_URL": "",
        "Gemini_Notes": gem.get("notes", ""),
    }
    if gem.get("website") and base.is_candidate_website(gem["website"]):
        result["Validated_Website"] = gem["website"]
    if gem.get("agent"):
        result["Agent_Name"] = gem["agent"]
        result["Agent_Source_URL"] = "gemini-grounded-search"
    email = gem.get("email") or ""
    source = gem.get("email_source") or ""
    if email and source.startswith("http") and confirm_email_on_page(http, email, source):
        result["Validated_Email"] = email
        result["Email_Source_URL"] = source
        result["Email_Source_Type"] = "gemini-grounded+page-confirmed"
    return result


def validate_author_contact_enhanced(
    http: base.CachedHttp,
    candidate: base.AuthorCandidate,
    use_gemini: bool,
) -> Dict[str, object]:
    result = base.validate_author_contact(http, candidate)
    result["Agent_Name"] = ""
    result["Agent_Email"] = ""
    result["Agent_Website"] = ""
    result["Agent_Source_URL"] = ""
    result["Other_Contact_Email"] = ""
    result["Other_Contact_Source_URL"] = ""

    website = result.get("Validated_Website") or ""
    author_email = normalize_email_value(result.get("Validated_Email") or "")
    result["Validated_Email"] = author_email

    rep = scan_representation_details(http, candidate, website, author_email)
    source_page = str(result.get("Email_Source_URL") or "")
    if source_page.startswith("http"):
        page_channels = choose_page_contact_channels(http, source_page, candidate, website)
        if page_channels["agent_email"] and normalize_email_value(result.get("Validated_Email") or "") == page_channels["agent_email"]:
            result["Validated_Email"] = ""
            result["Email_Verified"] = False
            result["Email_Source_URL"] = ""
            result["Email_Source_Type"] = ""
        if page_channels["direct_email"]:
            result["Validated_Email"] = page_channels["direct_email"]
            result["Email_Verified"] = True
            result["Email_Source_URL"] = page_channels["direct_source"]
            result["Email_Source_Type"] = "official-website-context-verified"
        elif page_channels["other_email"]:
            result["Other_Contact_Email"] = page_channels["other_email"]
            result["Other_Contact_Source_URL"] = page_channels["other_source"]
        if page_channels["agent_email"]:
            rep["Agent_Email"] = rep["Agent_Email"] or page_channels["agent_email"]
            rep["Agent_Source_URL"] = rep["Agent_Source_URL"] or page_channels["agent_source"]
        if page_channels["agent_name"]:
            rep["Agent_Name"] = rep["Agent_Name"] or page_channels["agent_name"]

    if rep["Agent_Name"]:
        result["Agent_Name"] = rep["Agent_Name"]
        result["Agent_Source_URL"] = rep["Agent_Source_URL"]
        result["Agency_Contact"] = rep["Agent_Name"]
        result["Agency_Source"] = rep["Agent_Source_URL"]
    if rep["Agent_Email"]:
        result["Agent_Email"] = rep["Agent_Email"]
        if not result.get("Agency_Source"):
            result["Agency_Source"] = rep["Agent_Source_URL"]
    if rep["Agent_Website"]:
        result["Agent_Website"] = rep["Agent_Website"]
    if rep["Has_Contact_Form"] and not result.get("Agency_Contact"):
        result["Agency_Contact"] = "Contact form on official website"
        result["Agency_Source"] = "official-site"

    if use_gemini and (not result.get("Validated_Email") or not result.get("Agent_Name")):
        gem = gemini_contact_enrichment(http, candidate.author, website)
        if not result.get("Validated_Email") and gem["Validated_Email"]:
            result["Validated_Email"] = gem["Validated_Email"]
            result["Email_Verified"] = True
            result["Email_Source_URL"] = gem["Email_Source_URL"]
            result["Email_Source_Type"] = gem["Email_Source_Type"]
            result["Author_Verification_Notes"] = "; ".join(
                filter(
                    None,
                    [
                        result.get("Author_Verification_Notes", ""),
                        "gemini grounded search confirmed on fetched page",
                    ],
                )
            ).strip("; ")
        if not result.get("Validated_Website") and gem["Validated_Website"]:
            result["Validated_Website"] = gem["Validated_Website"]
        if not result.get("Agent_Name") and gem["Agent_Name"]:
            result["Agent_Name"] = gem["Agent_Name"]
            result["Agent_Source_URL"] = gem["Agent_Source_URL"]
            result["Agency_Contact"] = gem["Agent_Name"]
            result["Agency_Source"] = gem["Agent_Source_URL"]

    result = enforce_strict_direct_email(result, candidate)
    result["Validated_Email"] = normalize_email_value(result.get("Validated_Email") or "")
    result["Agent_Email"] = normalize_email_value(result.get("Agent_Email") or "")
    result["Other_Contact_Email"] = normalize_email_value(result.get("Other_Contact_Email") or "")
    result = normalize_representation_fields(result)
    result = reconcile_secondary_contact_channels(result)

    notes = [str(result.get("Author_Verification_Notes") or "").strip()]
    if result.get("Agent_Name"):
        notes.append("representation identified on public source")
    if result.get("Agent_Email"):
        notes.append("agent email found on public source")
    if result.get("Other_Contact_Email"):
        notes.append("website contact email found on public source")
    if result.get("Agent_Website"):
        notes.append("agent website found on public source")
    result["Author_Verification_Notes"] = "; ".join([note for note in notes if note])

    quality = result.get("Author_Data_Quality_Flag") or "RED"
    if result.get("Validated_Email"):
        quality = "GREEN"
    elif result.get("Agent_Name") or result.get("Agent_Email") or result.get("Agency_Contact") or result.get("Validated_Website"):
        quality = "YELLOW"
    result["Author_Data_Quality_Flag"] = quality
    return result


def combine_quality_flags(*flags: object) -> str:
    cleaned = [str(flag or "").strip().upper() for flag in flags if str(flag or "").strip()]
    if not cleaned:
        return "RED"
    return max(cleaned, key=lambda flag: SEVERITY_RANK.get(flag, -1))


def build_contact_description(row: pd.Series, author_result: Dict[str, object]) -> str:
    parts: List[str] = []
    if author_result.get("Validated_Email"):
        parts.append(
            f"Author email: {author_result['Validated_Email']}"
        )
    if author_result.get("Other_Contact_Email"):
        parts.append(f"Website contact email: {author_result['Other_Contact_Email']}")
    if author_result.get("Validated_Website"):
        parts.append(f"Author website: {author_result['Validated_Website']}")

    agent_bits = []
    if author_result.get("Agent_Name"):
        agent_bits.append(str(author_result["Agent_Name"]))
    elif author_result.get("Agency_Contact") and author_result.get("Agency_Contact") != "Contact form on official website":
        agent_bits.append(str(author_result["Agency_Contact"]))
    if author_result.get("Agent_Email"):
        agent_bits.append(f"email {author_result['Agent_Email']}")
    if author_result.get("Agent_Website"):
        agent_bits.append(f"site {author_result['Agent_Website']}")
    if agent_bits:
        parts.append("Representation: " + ", ".join(agent_bits))
    elif author_result.get("Agency_Contact") == "Contact form on official website":
        parts.append("No public email found; official site contact form available")

    for field in ["Twitter", "Instagram", "Facebook", "BookBub", "TikTok"]:
        value = str(row.get(field, "") or "").strip()
        if value and value not in {"nan", "None"}:
            parts.append(f"{field}: {value}")

    if not parts:
        parts.append("No verified direct email, agent email, or public contact form found")
    return " | ".join(parts)


def sanity_check_row(row: pd.Series) -> Tuple[str, str]:
    issues: List[str] = []

    source_series = str(row.get("Book Series Name") or "").strip()
    source_first = str(row.get("First Book Name") or "").strip()
    verified_series = str(row.get("Verified_Series_Name") or source_series).strip()
    verified_first = str(row.get("Verified_First_Book_Name") or source_first).strip()
    verified_last = str(row.get("Verified_Last_Book_Name") or row.get("Last Book Name") or "").strip()
    verified_type = str(row.get("Verified_Type") or row.get("Type") or "").strip()

    def _to_float(value: object) -> Optional[float]:
        try:
            num = float(value)
            return None if pd.isna(num) else num
        except Exception:
            return None

    n_books = _to_float(row.get("Verified_Books_in_Series")) or _to_float(row.get("Books in Series"))
    n_pages = _to_float(row.get("Verified_Total_Pages")) or _to_float(row.get("Total Pages"))
    source_books = _to_float(row.get("Books in Series"))
    source_pages = _to_float(row.get("Total Pages"))

    if "standalone" in verified_type.lower() and n_books and n_books > 1:
        issues.append(f"TYPE_MISMATCH: {verified_type} with {n_books} books")
    if verified_first and verified_last and verified_first.lower() == verified_last.lower() and n_books and n_books > 1:
        issues.append(f"SAME_FIRST_LAST: {verified_first}")
    if source_series and source_first and norm(source_series) == norm(source_first) and norm(source_series) != norm(verified_series):
        issues.append("SOURCE_SERIES_WAS_BOOK_TITLE")
    if source_books == 3.0 and (row.get("Verified_Books_in_Series") in {"", None} or pd.isna(row.get("Verified_Books_in_Series"))):
        issues.append("SOURCE_BOOKS_DEFAULT3")
    if source_pages and source_books and source_pages in {source_books * 250, source_books * 300}:
        issues.append("SOURCE_FORMULAIC_PAGES")
    if n_pages and n_books and n_books > 0:
        ppb = n_pages / n_books
        if n_pages in {0, 1}:
            issues.append("PAGES_ZERO")
        elif ppb < 50:
            issues.append(f"PAGES_LOW: {ppb:.0f}pp/book")
        elif ppb > 900:
            issues.append(f"PAGES_HIGH: {ppb:.0f}pp/book")
    if row.get("Verified_Author_Name") and norm(row.get("Verified_Author_Name")) != norm(row.get("Author Name")):
        issues.append("AUTHOR_CORRECTED")

    critical = {"TYPE_MISMATCH", "SAME_FIRST_LAST", "PAGES_ZERO", "SOURCE_SERIES_WAS_BOOK_TITLE"}
    has_critical = any(any(token in issue for token in critical) for issue in issues)
    if has_critical or len(issues) >= 3:
        flag = "RED"
    elif issues:
        flag = "YELLOW"
    else:
        flag = "GREEN"
    return flag, " | ".join(issues)


def build_repair_summary(row: pd.Series, author_result: Dict[str, object]) -> str:
    notes: List[str] = []
    if row.get("Verified_Author_Name") and norm(row.get("Verified_Author_Name")) != norm(row.get("Author Name")):
        notes.append(f"author corrected to {row.get('Verified_Author_Name')}")
    if row.get("Verified_Series_Name") and norm(row.get("Verified_Series_Name")) != norm(row.get("Book Series Name")):
        notes.append(f"series corrected to {row.get('Verified_Series_Name')}")
    if row.get("Verified_Books_in_Series") and str(row.get("Verified_Books_in_Series")) != str(row.get("Books in Series")):
        notes.append(f"books corrected to {row.get('Verified_Books_in_Series')}")
    if row.get("Verified_First_Book_Name") and norm(row.get("Verified_First_Book_Name")) != norm(row.get("First Book Name")):
        notes.append(f"first book corrected to {row.get('Verified_First_Book_Name')}")
    if row.get("Verified_Last_Book_Name") and norm(row.get("Verified_Last_Book_Name")) != norm(row.get("Last Book Name")):
        notes.append(f"last book corrected to {row.get('Verified_Last_Book_Name')}")
    if author_result.get("Validated_Email"):
        notes.append("direct author email verified")
    elif author_result.get("Other_Contact_Email"):
        notes.append("website contact email verified")
    elif author_result.get("Agent_Email"):
        notes.append("agent email verified")
    elif author_result.get("Agent_Name"):
        notes.append("representation identified")
    elif author_result.get("Agency_Contact") == "Contact form on official website":
        notes.append("official site contact form only")
    return "; ".join(notes)


def rows_from_dataframe(df: pd.DataFrame, label: str) -> List[Dict[str, object]]:
    return dataframe_to_rows(df, label)


def build_author_candidates(
    amazon_rows: List[Dict[str, object]],
    support_rows: List[Dict[str, object]],
    target_authors: Iterable[str],
) -> Dict[str, base.AuthorCandidate]:
    candidates = base.build_author_candidates(amazon_rows + support_rows)
    target_keys = {base.norm_key(author) for author in target_authors if author}
    filtered = {key: value for key, value in candidates.items() if key in target_keys}
    for author in target_authors:
        key = base.norm_key(author)
        if key and key not in filtered:
            filtered[key] = base.AuthorCandidate(author=str(author).strip(), author_key=key)
    return filtered


def build_author_results(
    authors: Dict[str, base.AuthorCandidate],
    http: base.CachedHttp,
    use_gemini: bool,
    limit_authors: Optional[int],
) -> pd.DataFrame:
    items = list(authors.items())
    if limit_authors is not None:
        items = items[:limit_authors]
    results: List[Dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {
            pool.submit(validate_author_contact_enhanced, http, candidate, use_gemini): key
            for key, candidate in items
        }
        for future in as_completed(futures):
            results.append(future.result())
    return pd.DataFrame(results)


def prepare_working_row(row: Dict[str, object], verified_author_name: str) -> Dict[str, object]:
    prepared = dict(row)
    prepared["Author Name"] = verified_author_name or row.get("Author Name")
    prepared["_author_key"] = base.norm_key(prepared["Author Name"])
    prepared["_series_key"] = base.norm_key(prepared.get("Book Series Name"))
    prepared["_first_book_key"] = base.norm_key(prepared.get("First Book Name"))
    return prepared


def build_lineage_metadata(row: Dict[str, object], support: Dict[str, pd.DataFrame]) -> Dict[str, object]:
    for label in ["ice_hockey_outreach_verified", "ice_hockey_master_contacts_verified"]:
        df = support.get(label)
        if df is None:
            continue
        match, reason = exact_lineage_match(pd.Series(row), df)
        if match is not None:
            return {
                "Lineage_Source": label,
                "Lineage_Match_Type": reason,
                "Lineage_Matched_Series": match.get("Book Series Name", ""),
                "Lineage_Matched_Author": match.get("Author Name", ""),
            }
    return {
        "Lineage_Source": "",
        "Lineage_Match_Type": "",
        "Lineage_Matched_Series": "",
        "Lineage_Matched_Author": "",
    }


def apply_lineage_fallback(
    result: Dict[str, object],
    lineage_match: Optional[pd.Series],
) -> Dict[str, object]:
    if lineage_match is None:
        return result
    fallback_map = {
        "Verified_Series_Name": "Verified_Series_Name",
        "Verified_Goodreads_Series_URL": "Verified_Goodreads_Series_URL",
        "Verified_Books_in_Series": "Verified_Books_in_Series",
        "Verified_Type": "Verified_Type",
        "Verified_First_Book_Name": "Verified_First_Book_Name",
        "Verified_Last_Book_Name": "Verified_Last_Book_Name",
        "Verified_Total_Pages": "Verified_Total_Pages",
        "Verified_Length_of_Adaption_in_Hours": "Verified_Length_of_Adaption_in_Hours",
        "Series_Primary_Works": "Series_Primary_Works",
        "Series_Total_Works": "Series_Total_Works",
    }
    for target, source in fallback_map.items():
        current = result.get(target)
        if pd.notna(current) and str(current).strip() not in {"", "nan"}:
            continue
        fallback = lineage_match.get(source, "")
        if pd.notna(fallback) and str(fallback).strip() not in {"", "nan"}:
            result[target] = fallback
    return result


def verify_row(
    row: Dict[str, object],
    author_lookup: Dict[str, Dict[str, object]],
    series_map: Dict[Tuple[str, str], base.LocalSeries],
    by_book_author: Dict[Tuple[str, str], base.LocalBook],
    gr: base.GoodreadsClient,
    support: Dict[str, pd.DataFrame],
) -> Dict[str, object]:
    suggested_author, author_reason = choose_author_candidate_locally(row, series_map, by_book_author)
    working_row = prepare_working_row(row, suggested_author)
    author_result = author_lookup.get(base.norm_key(suggested_author), {})
    verified = base.verify_outreach_row(working_row, author_result, series_map, by_book_author, gr)
    result = dict(row)
    result.update(verified)
    result["Verified_Author_Name"] = suggested_author
    result["Author_Match_Reason"] = author_reason
    lineage_match = None
    lineage_meta = {}
    for label in ["ice_hockey_outreach_verified", "ice_hockey_master_contacts_verified"]:
        df = support.get(label)
        if df is None:
            continue
        match, reason = exact_lineage_match(pd.Series(result), df)
        if match is not None:
            lineage_match = match
            lineage_meta = {
                "Lineage_Source": label,
                "Lineage_Match_Type": reason,
                "Lineage_Matched_Series": match.get("Book Series Name", ""),
                "Lineage_Matched_Author": match.get("Author Name", ""),
            }
            break
    if not lineage_meta:
        lineage_meta = build_lineage_metadata(result, support)
    result.update(lineage_meta)
    result = apply_lineage_fallback(result, lineage_match)
    result["Contact_Description"] = build_contact_description(pd.Series(result), verified)
    sanity_flag, issues = sanity_check_row(pd.Series(result))
    result["Data_Quality_Flag"] = combine_quality_flags(
        result.get("Data_Quality_Flag"),
        result.get("Author_Data_Quality_Flag"),
        sanity_flag,
    )
    result["Sanity_Issues"] = issues
    result["Repair_Summary"] = build_repair_summary(pd.Series(result), verified)
    return result


def apply_final_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["Final_Author_Name"] = df["Author Name"]
    if "Verified_Author_Name" in df.columns:
        mask = df["Verified_Author_Name"].fillna("").astype(str).str.strip().ne("")
        df.loc[mask, "Final_Author_Name"] = df.loc[mask, "Verified_Author_Name"]

    mapping = {
        "Final_Book Series Name": "Verified_Series_Name",
        "Final_Type": "Verified_Type",
        "Final_Books in Series": "Verified_Books_in_Series",
        "Final_Total Pages": "Verified_Total_Pages",
        "Final_First Book Name": "Verified_First_Book_Name",
        "Final_Last Book Name": "Verified_Last_Book_Name",
        "Final_Length of Adaption in Hours": "Verified_Length_of_Adaption_in_Hours",
    }
    for target, source in mapping.items():
        df[target] = df[source].where(df[source].notna() & df[source].astype(str).str.strip().ne(""), df[target.replace("Final_", "")] if target.replace("Final_", "") in df.columns else "")

    if "Contact Info" in df.columns:
        df["Contact Info"] = df["Contact_Description"]
    return df


def write_outputs(df: pd.DataFrame, report_lines: List[str]) -> None:
    OUTPUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    REPORT_MD.parent.mkdir(parents=True, exist_ok=True)

    df.to_csv(OUTPUT_CSV, index=False)

    report_df = pd.DataFrame({"Notes": report_lines})
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Repaired Amazon Hockey")
        report_df.to_excel(writer, index=False, sheet_name="Repair Notes")

    REPORT_MD.write_text("\n".join(report_lines))


def build_report_lines(df: pd.DataFrame, support: Dict[str, pd.DataFrame]) -> List[str]:
    amazon_authors = df["Author Name"].astype(str).map(norm)
    outreach = support.get("ice_hockey_outreach_verified", pd.DataFrame())
    master = support.get("ice_hockey_master_contacts_verified", pd.DataFrame())
    outreach_authors = outreach.get("Author Name", pd.Series(dtype=str)).astype(str).map(norm) if not outreach.empty else pd.Series(dtype=str)
    master_authors = master.get("Author Name", pd.Series(dtype=str)).astype(str).map(norm) if not master.empty else pd.Series(dtype=str)

    lines = [
        "# Amazon Hockey Repair Report",
        "",
        f"- Source file: `{AMAZON_SOURCE_CSV}`",
        f"- Repaired CSV: `{OUTPUT_CSV}`",
        f"- Repaired workbook: `{OUTPUT_XLSX}`",
        "",
        "## Lineage",
        f"- Amazon working rows: {len(df)}",
        f"- Unique Amazon authors: {amazon_authors.nunique()}",
        f"- Author overlap with verified outreach: {len(set(amazon_authors) & set(outreach_authors)) if not outreach.empty else 0}",
        f"- Author overlap with verified master: {len(set(amazon_authors) & set(master_authors)) if not master.empty else 0}",
        f"- Rows with lineage match to verified outreach/master: {int(df['Lineage_Source'].fillna('').astype(str).str.strip().ne('').sum())}",
        "",
        "## Repair summary",
        f"- Rows with verified series: {int(df['Verified_Series_Name'].fillna('').astype(str).str.strip().ne('').sum())}",
        f"- Rows with verified direct email: {int(df['Validated_Email'].fillna('').astype(str).str.strip().ne('').sum())}",
        f"- Rows with representation identified: {int(df['Agent_Name'].fillna('').astype(str).str.strip().ne('').sum())}",
        f"- Rows with contact form fallback: {int((df['Agency_Contact'].fillna('') == 'Contact form on official website').sum())}",
        f"- Rows with author correction: {int((df['Verified_Author_Name'].fillna('').astype(str).map(norm) != df['Author Name'].fillna('').astype(str).map(norm)).sum())}",
        f"- GREEN rows: {int((df['Data_Quality_Flag'] == 'GREEN').sum())}",
        f"- YELLOW rows: {int((df['Data_Quality_Flag'] == 'YELLOW').sum())}",
        f"- RED rows: {int((df['Data_Quality_Flag'] == 'RED').sum())}",
        "",
        "## Notes",
        "- Direct author emails were only retained when confirmed on a fetched public page.",
        "- Representation details were captured when found on public pages or identified through grounded search fallback.",
        "- Series verification used the stricter ice-hockey repair workflow and local Goodreads-backed catalogs first.",
    ]
    return lines


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit-authors", type=int, default=None)
    parser.add_argument("--limit-rows", type=int, default=None)
    parser.add_argument("--use-gemini", action="store_true")
    args = parser.parse_args()

    amazon_df = load_amazon_working_file(AMAZON_SOURCE_CSV)
    if args.limit_rows is not None:
        amazon_df = amazon_df.head(args.limit_rows).copy()

    support = load_support_dataframes()
    amazon_rows = rows_from_dataframe(amazon_df, "amazon_hockey_cleaned_titles")

    support_rows: List[Dict[str, object]] = []
    for label, df in support.items():
        if "Author Name" in df.columns and "Book Series Name" in df.columns:
            support_rows.extend(rows_from_dataframe(df, label))

    series_map, by_book_author = base.load_local_books()

    target_authors: List[str] = []
    for row in amazon_rows:
        suggested_author, _reason = choose_author_candidate_locally(row, series_map, by_book_author)
        target_authors.append(suggested_author or str(row.get("Author Name") or ""))

    http = base.CachedHttp()
    gr = base.GoodreadsClient(http)

    author_candidates = build_author_candidates(amazon_rows, support_rows, target_authors)
    author_df = build_author_results(author_candidates, http, args.use_gemini, args.limit_authors)
    author_lookup = {base.norm_key(row["Author Name"]): row for row in author_df.to_dict("records")}

    records: List[Dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {
            pool.submit(verify_row, row, author_lookup, series_map, by_book_author, gr, support): row
            for row in amazon_rows
        }
        for future in as_completed(futures):
            records.append(future.result())

    result_df = pd.DataFrame(records).sort_values("_source_row")
    result_df = apply_final_columns(result_df)
    report_lines = build_report_lines(result_df, support)
    write_outputs(result_df, report_lines)

    summary = {
        "rows": len(result_df),
        "validated_email_rows": int(result_df["Validated_Email"].fillna("").astype(str).str.strip().ne("").sum()),
        "agent_rows": int(result_df["Agent_Name"].fillna("").astype(str).str.strip().ne("").sum()),
        "verified_series_rows": int(result_df["Verified_Series_Name"].fillna("").astype(str).str.strip().ne("").sum()),
        "green_rows": int((result_df["Data_Quality_Flag"] == "GREEN").sum()),
        "yellow_rows": int((result_df["Data_Quality_Flag"] == "YELLOW").sum()),
        "red_rows": int((result_df["Data_Quality_Flag"] == "RED").sum()),
        "outputs": {
            "csv": str(OUTPUT_CSV),
            "xlsx": str(OUTPUT_XLSX),
            "report": str(REPORT_MD),
        },
    }
    print(pd.Series(summary).to_json())


if __name__ == "__main__":
    main()
