#!/usr/bin/env python3
"""
Gemini-grounded audit for Seymour Agency romance/drama series.

What this does:
1. Starts from the current Seymour working-list authors plus a few known nearby candidates.
2. Uses Gemini grounded Google Search to discover any additional Seymour-represented
   romance/drama authors with series at the 5+ threshold.
3. Verifies each candidate author and returns only series with >= 5 Goodreads primary works.
4. Captures per-series metadata:
   - author
   - series
   - primary works
   - total books
   - first-book Goodreads rating + ratings count
   - genre bucket, sub-genre, pairing, primary trope
   - Seymour representation evidence
   - RBmedia sanity check with strict series-level vs author-level classification
5. Saves raw response cache, JSON report, and a normalized CSV.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional
from urllib.parse import urlparse

import pandas as pd
import requests
from loguru import logger


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ENV_FILE = PROJECT_ROOT / ".env"
OUTPUT_DIR = PROJECT_ROOT / "outreach" / "sports-romance" / "reports" / "seymour_gemini_audit"
CACHE_DIR = OUTPUT_DIR / "cache"

GEMINI_ENDPOINT = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "gemini-2.0-flash:generateContent"
)

DEFAULT_SEED_AUTHORS = [
    "Lexi Blake",
    "Amy Daws",
    "Mira Lyn Kelly",
    "Rebecca Jenshak",
    "Bella Matthews",
    "Willow Aster",
    "Fiona Cole",
    "Samantha Christy",
    "Kait Ballenger",
    "Charity Ferrell",
    "Adriana Locke",
    "Karla Sorensen",
    "Devney Perry",
    "Giana Darling",
    "Jennifer Hartmann",
]

KNOWN_SEYMOUR_SERIES = [
    "Masters and Mercenaries",
    "Nights in Bliss, Colorado",
    "Harris Brothers",
    "Slayers Hockey",
    "Smart Jocks",
    "Red Lips & White Lies",
    "Voyeur",
    "Seven Range Shifters",
    "Blue Beech",
    "The Edens",
    "The Fallen Men",
    "Brewer Family",
    "Landry Family",
    "Wilder Family",
    "Bachelors of the Ridge",
    "Washington Wolves",
]

RBMEDIA_DOMAINS = [
    "rbmediaglobal.com",
    "recordedbooks.com",
    "tantor.com",
    "dreamscapepublishing.com",
    "highbridgeaudio.com",
    "christianaudio.com",
    "graphicaudio.net",
    "wavesound.com.au",
    "wfhowes.co.uk",
]


def load_gemini_key() -> str:
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text().splitlines():
            if line.startswith("GEMINI_API_KEY="):
                return line.split("=", 1)[1].strip()
    return os.environ.get("GEMINI_API_KEY", "")


def normalize_text(value: Any) -> str:
    text = str(value or "").strip()
    return re.sub(r"\s+", " ", text)


def normalize_key(value: Any) -> str:
    text = normalize_text(value).lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def unique_preserve(items: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen = set()
    for item in items:
        norm = normalize_key(item)
        if not norm or norm in seen:
            continue
        seen.add(norm)
        out.append(normalize_text(item))
    return out


def robust_json_parse(text: str) -> Any:
    raw = (text or "").strip()
    if raw.startswith("```json"):
        raw = raw[7:]
    elif raw.startswith("```"):
        raw = raw[3:]
    if raw.endswith("```"):
        raw = raw[:-3]
    raw = raw.strip()
    raw = re.sub(r",\s*([}\]])", r"\1", raw)

    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass

    for pattern in (r"\{[\s\S]*\}", r"\[[\s\S]*\]"):
        match = re.search(pattern, raw)
        if match:
            candidate = re.sub(r",\s*([}\]])", r"\1", match.group(0))
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                continue
    raise ValueError("Could not parse Gemini JSON response")


def extract_text(response: Dict[str, Any]) -> str:
    candidates = response.get("candidates", [])
    if not candidates:
        return ""
    parts = candidates[0].get("content", {}).get("parts", [])
    return " ".join(p.get("text", "") for p in parts).strip()


def extract_grounding_sources(response: Dict[str, Any]) -> List[str]:
    sources: List[str] = []
    try:
        meta = response.get("candidates", [{}])[0].get("groundingMetadata", {})
        for chunk in meta.get("groundingChunks", []):
            web = chunk.get("web", {})
            uri = web.get("uri")
            if uri and uri not in sources:
                sources.append(uri)
    except Exception:
        pass
    return sources


def coerce_int(value: Any) -> Optional[int]:
    if value in (None, "", "null", "None"):
        return None
    try:
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        return int(float(text))
    except Exception:
        return None


def coerce_float(value: Any) -> Optional[float]:
    if value in (None, "", "null", "None"):
        return None
    try:
        text = str(value).strip().replace(",", "")
        if not text:
            return None
        return float(text)
    except Exception:
        return None


def domain_for_url(url: Any) -> str:
    text = normalize_text(url)
    if not text.startswith("http"):
        return ""
    try:
        return urlparse(text).netloc.lower()
    except Exception:
        return ""


def is_rbmedia_url(url: Any) -> bool:
    domain = domain_for_url(url)
    return any(domain.endswith(d) for d in RBMEDIA_DOMAINS)
    try:
        return float(str(value).strip().replace(",", ""))
    except Exception:
        return None


@dataclass
class GeminiClient:
    api_key: str
    min_gap_seconds: float = 4.0
    last_call_ts: float = 0.0

    def __post_init__(self) -> None:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _wait_for_slot(self) -> None:
        elapsed = time.time() - self.last_call_ts
        if elapsed < self.min_gap_seconds:
            time.sleep(self.min_gap_seconds - elapsed)
        self.last_call_ts = time.time()

    def grounded_json(self, prompt: str, cache_label: str, max_output_tokens: int = 4096) -> Dict[str, Any]:
        cache_key = hashlib.md5(f"{cache_label}\n{prompt}".encode("utf-8")).hexdigest()
        cache_path = CACHE_DIR / f"{cache_key}.json"
        if cache_path.exists():
            return json.loads(cache_path.read_text())

        self._wait_for_slot()
        payload = {
            "contents": [{"parts": [{"text": prompt}]}],
            "tools": [{"google_search": {}}],
            "generationConfig": {
                "temperature": 0.0,
                "maxOutputTokens": max_output_tokens,
            },
        }
        url = f"{GEMINI_ENDPOINT}?key={self.api_key}"
        last_error = ""
        for attempt in range(1, 6):
            try:
                resp = requests.post(url, json=payload, timeout=90)
                if resp.status_code == 429:
                    wait = 20 * attempt
                    logger.warning(f"Rate limited on {cache_label}; sleeping {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code >= 400:
                    logger.warning(f"Gemini HTTP {resp.status_code} on {cache_label}: {resp.text[:500]}")
                resp.raise_for_status()
                data = resp.json()
                cache_path.write_text(json.dumps(data, indent=2))
                return data
            except Exception as exc:
                last_error = str(exc)
                logger.warning(f"Gemini call failed for {cache_label} (attempt {attempt}): {exc}")
                time.sleep(8 * attempt)
        raise RuntimeError(f"Gemini call failed for {cache_label}: {last_error}")


def discovery_prompt(seed_authors: List[str], known_series: List[str], threshold: int) -> str:
    return f"""You are auditing The Seymour Agency's romance/drama roster.

Goal:
Find Seymour-represented authors who have at least one romance/drama series with Goodreads PRIMARY works >= {threshold}.

Important:
- Be conservative and evidence-backed.
- Prioritize Seymour Agency roster/profile pages, Goodreads series pages, and official author sites.
- Do not include an author unless both are true:
  1. You can point to Seymour representation evidence.
  2. You can point to at least one Goodreads series page showing a qualifying series.
- Exclude authors already in the provided seed list.

Seed authors already on our radar:
{json.dumps(seed_authors, ensure_ascii=True)}

Known series already on our radar:
{json.dumps(known_series, ensure_ascii=True)}

Return JSON only in this exact shape:
{{
  "additional_authors": [
    {{
      "author_name": "string",
      "representation_evidence_url": "string",
      "qualifying_series": [
        {{
          "series_name": "string",
          "goodreads_series_url": "string",
          "primary_works": 0
        }}
      ],
      "notes": "string"
    }}
  ],
  "rejected_or_borderline": [
    {{
      "author_name": "string",
      "reason": "string"
    }}
  ]
}}

Rules:
- Keep the list exhaustive but cautious.
- If an author only has 5 exactly, still include them because this audit is for 5+ coverage, but note that in primary_works.
- If uncertain, leave the author out and put them in rejected_or_borderline with the reason.
"""


def completeness_prompt(current_authors: List[str], threshold: int) -> str:
    return f"""You are doing a final completeness check for The Seymour Agency romance/drama audit.

Current covered authors:
{json.dumps(current_authors, ensure_ascii=True)}

Task:
Find any additional Seymour-represented author NOT in the covered list who appears to have at least one romance/drama series with Goodreads PRIMARY works >= {threshold}.

Use conservative grounded evidence. Prioritize Seymour representation plus Goodreads series pages.

Return JSON only:
{{
  "missing_authors": [
    {{
      "author_name": "string",
      "representation_evidence_url": "string",
      "candidate_series_name": "string",
      "goodreads_series_url": "string",
      "primary_works": 0,
      "notes": "string"
    }}
  ]
}}

If none are found, return an empty array.
"""


def author_verification_prompt(author_name: str, threshold: int) -> str:
    return f"""You are doing a conservative series-level audit for one author.

Author: {author_name}

Task:
1. Confirm whether this author is represented by The Seymour Agency.
2. Enumerate the author's romance/drama series with Goodreads PRIMARY works >= {threshold}.
3. For each qualifying series, capture both Goodreads data and RBmedia sanity.

Critical RBmedia classification rules:
- "yes_series_level": Use ONLY if a title in this exact series has a direct RBmedia or RBmedia-imprint product/catalog page.
  RBmedia imprints include Recorded Books, Tantor, Dreamscape, HighBridge, Christian Audio, GraphicAudio, Ascent Audio, Wavesound, W. F. Howes, BookaVivo, Kalorama Audio.
- "yes_author_level": Use if the author clearly has an RBmedia author page or other RBmedia-distributed audiobook(s), but you cannot tie RBmedia directly to this audited series.
- "no": Use if neither of the above is supported.

Output canonical public URLs only.
Do NOT output Google grounding redirect URLs or tracking URLs.
Prefer canonical URLs like:
- https://www.goodreads.com/series/...
- https://www.goodreads.com/book/show/...
- https://www.theseymouragency.com/...
- https://rbmediaglobal.com/...
- https://tantor.com/...
- https://www.recordedbooks.com/...
- https://www.dreamscapepublishing.com/...

Return JSON only in this exact shape:
{{
  "author_name": "{author_name}",
  "represented_by_seymour": true,
  "representation_evidence_url": "string",
  "qualifying_series": [
    {{
      "series_name": "string",
      "goodreads_series_url": "string",
      "primary_works": 0,
      "total_books": 0,
      "threshold_flag": "gt5_or_more|eq5_included",
      "first_book_title": "string",
      "first_book_goodreads_rating": 0.0,
      "first_book_goodreads_rating_count": 0,
      "genre_bucket": "romance|drama|romance/drama",
      "lead_pairing": "M/F|F/F|M/M|reverse harem|poly|unknown",
      "sub_genre": "string",
      "primary_trope": "string",
      "rbmedia_sanity_check": "yes_series_level|yes_author_level|no",
      "rbmedia_evidence_url": "string",
      "rbmedia_evidence_note": "string"
    }}
  ],
  "author_level_rbmedia_url": "string"
}}

Rules:
- Only include romance/drama oriented series.
- If a series has exactly 5 PRIMARY works, keep it and set threshold_flag to "eq5_included".
- Use Goodreads PRIMARY works, not box sets or total works, for thresholding.
- If Goodreads rating count is approximate, still provide the best grounded estimate as an integer.
- If represented_by_seymour is false or uncertain, return qualifying_series as an empty array.
- Keep notes short and factual.
"""


def normalize_discovery_authors(parsed: Dict[str, Any]) -> List[str]:
    authors: List[str] = []
    for item in parsed.get("additional_authors", []) or []:
        name = normalize_text(item.get("author_name"))
        if name:
            authors.append(name)
    return unique_preserve(authors)


def ensure_dict(payload: Any) -> Dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list) and payload:
        first = payload[0]
        if isinstance(first, dict):
            return first
    return {}


def normalize_author_payload(payload: Any, author_name: str) -> Dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    if isinstance(payload, list) and payload and all(isinstance(x, dict) for x in payload):
        if any("series_name" in x for x in payload):
            return {
                "author_name": author_name,
                "represented_by_seymour": True,
                "representation_evidence_url": "",
                "qualifying_series": payload,
                "author_level_rbmedia_url": "",
            }
        first = payload[0]
        if isinstance(first, dict):
            return first
    return {}


def normalize_series_rows(author_payload: Dict[str, Any], threshold: int) -> List[Dict[str, Any]]:
    author_name = normalize_text(author_payload.get("author_name"))
    represented = bool(author_payload.get("represented_by_seymour"))
    if not author_name or not represented:
        return []

    rows: List[Dict[str, Any]] = []
    for series in author_payload.get("qualifying_series", []) or []:
        primary_works = coerce_int(series.get("primary_works"))
        if primary_works is None or primary_works < threshold:
            continue

        total_books = coerce_int(series.get("total_books"))
        rating = coerce_float(series.get("first_book_goodreads_rating"))
        rating_count = coerce_int(series.get("first_book_goodreads_rating_count"))
        rb_status = normalize_text(series.get("rbmedia_sanity_check")).lower()
        if rb_status not in {"yes_series_level", "yes_author_level", "no"}:
            rb_status = "no"

        threshold_flag = normalize_text(series.get("threshold_flag"))
        if not threshold_flag:
            threshold_flag = "eq5_included" if primary_works == threshold else "gt5_or_more"

        support_urls = series.get("support_urls") or []
        if not isinstance(support_urls, list):
            support_urls = []

        row = {
            "Author Name": author_name,
            "Represented By": "The Seymour Agency",
            "Representation Evidence URL": normalize_text(author_payload.get("representation_evidence_url")),
            "Series Name": normalize_text(series.get("series_name")),
            "Goodreads Series URL": normalize_text(series.get("goodreads_series_url")),
            "Primary Works": primary_works,
            "Total Books in Series": total_books,
            "Threshold Flag": threshold_flag,
            "First Book Title": normalize_text(series.get("first_book_title")),
            "First Book GR Rating": rating,
            "First Book GR Rating Count": rating_count,
            "Genre": normalize_text(series.get("genre_bucket")),
            "Lead Pairing": normalize_text(series.get("lead_pairing")),
            "Sub-Genre": normalize_text(series.get("sub_genre")),
            "Primary Trope": normalize_text(series.get("primary_trope")),
            "RB Media Sanity Check": rb_status,
            "RB Media Evidence URL": normalize_text(series.get("rbmedia_evidence_url")),
            "RB Media Evidence Note": normalize_text(series.get("rbmedia_evidence_note")),
            "Author-Level RB Media URL": normalize_text(author_payload.get("author_level_rbmedia_url")),
            "Support URLs": " | ".join(unique_preserve(support_urls)),
            "Confidence Notes": normalize_text(series.get("confidence_notes")),
        }
        row = apply_rbmedia_domain_sanity(row)
        rows.append(row)
    return rows


def apply_rbmedia_domain_sanity(row: Dict[str, Any]) -> Dict[str, Any]:
    status = normalize_text(row.get("RB Media Sanity Check")).lower()
    series_url = normalize_text(row.get("RB Media Evidence URL"))
    author_url = normalize_text(row.get("Author-Level RB Media URL"))

    series_valid = is_rbmedia_url(series_url)
    author_valid = is_rbmedia_url(author_url) or (status == "yes_author_level" and is_rbmedia_url(series_url))

    if status == "yes_series_level":
        if series_valid:
            return row
        if author_valid:
            row["RB Media Sanity Check"] = "yes_author_level"
            if not is_rbmedia_url(row.get("RB Media Evidence URL")):
                row["RB Media Evidence URL"] = author_url
            row["RB Media Evidence Note"] = normalize_text(row.get("RB Media Evidence Note")) or "Author-level RBmedia evidence only after domain sanity check."
            return row
        row["RB Media Sanity Check"] = "no"
        row["RB Media Evidence URL"] = ""
        row["RB Media Evidence Note"] = ""
        return row

    if status == "yes_author_level":
        if author_valid:
            if not is_rbmedia_url(series_url) and is_rbmedia_url(author_url):
                row["RB Media Evidence URL"] = author_url
            return row
        row["RB Media Sanity Check"] = "no"
        row["RB Media Evidence URL"] = ""
        row["RB Media Evidence Note"] = ""
        return row

    row["RB Media Sanity Check"] = "no"
    if not series_valid:
        row["RB Media Evidence URL"] = ""
        row["RB Media Evidence Note"] = ""
    return row


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    best_by_key: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = f"{normalize_key(row.get('Author Name'))}::{normalize_key(row.get('Series Name'))}"
        current = best_by_key.get(key)
        if current is None:
            best_by_key[key] = row
            continue
        current_score = int(current.get("Primary Works") or 0), int(current.get("Total Books in Series") or 0), len(current.get("Support URLs", ""))
        new_score = int(row.get("Primary Works") or 0), int(row.get("Total Books in Series") or 0), len(row.get("Support URLs", ""))
        if new_score > current_score:
            best_by_key[key] = row
    return sorted(best_by_key.values(), key=lambda r: (normalize_key(r["Author Name"]), -int(r.get("Primary Works") or 0), normalize_key(r["Series Name"])))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--threshold", type=int, default=5, help="Minimum Goodreads primary works to keep; default 5 for 5+ coverage.")
    parser.add_argument("--output-prefix", type=str, default=f"seymour_audit_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    args = parser.parse_args()

    api_key = load_gemini_key()
    if not api_key:
        raise SystemExit("GEMINI_API_KEY not found in .env or environment.")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    client = GeminiClient(api_key=api_key)

    logger.info("Starting Seymour Gemini audit")

    seed_authors = unique_preserve(DEFAULT_SEED_AUTHORS)

    discovery_raw = client.grounded_json(
        discovery_prompt(seed_authors, KNOWN_SEYMOUR_SERIES, args.threshold),
        cache_label="v2_discovery_round1",
        max_output_tokens=4096,
    )
    discovery_text = extract_text(discovery_raw)
    discovery_parsed = robust_json_parse(discovery_text)
    discovery_parsed = ensure_dict(discovery_parsed)
    discovered_authors = normalize_discovery_authors(discovery_parsed)
    logger.info(f"Discovery round 1 found {len(discovered_authors)} additional authors")

    candidate_authors = unique_preserve(seed_authors + discovered_authors)

    author_payloads: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []

    for idx, author in enumerate(candidate_authors, start=1):
        logger.info(f"[{idx}/{len(candidate_authors)}] Verifying {author}")
        raw = client.grounded_json(
            author_verification_prompt(author, args.threshold),
            cache_label=f"v2_author_{normalize_key(author)}",
            max_output_tokens=3072,
        )
        text = extract_text(raw)
        try:
            parsed = robust_json_parse(text)
        except Exception as exc:
            logger.warning(f"Could not parse author payload for {author}: {exc}")
            parsed = {
                "author_name": author,
                "represented_by_seymour": False,
                "qualifying_series": [],
                "parse_error": str(exc),
            }
        parsed = normalize_author_payload(parsed, author)
        parsed["_grounding_sources"] = extract_grounding_sources(raw)
        author_payloads.append(parsed)
        all_rows.extend(normalize_series_rows(parsed, args.threshold))

    covered_authors = unique_preserve([p.get("author_name", "") for p in author_payloads if p.get("author_name")])
    try:
        completeness_raw = client.grounded_json(
            completeness_prompt(covered_authors, args.threshold),
            cache_label="v2_discovery_round2_completeness",
            max_output_tokens=2048,
        )
        completeness_text = extract_text(completeness_raw)
        completeness_parsed = robust_json_parse(completeness_text)
        completeness_parsed = ensure_dict(completeness_parsed)
    except Exception as exc:
        logger.warning(f"Completeness round unavailable, defaulting to no extra authors: {exc}")
        completeness_parsed = {"missing_authors": []}

    missing_authors = unique_preserve(
        [normalize_text(item.get("author_name")) for item in completeness_parsed.get("missing_authors", []) or []]
    )
    missing_authors = [a for a in missing_authors if normalize_key(a) not in {normalize_key(x) for x in covered_authors}]
    logger.info(f"Completeness round found {len(missing_authors)} more authors")

    for idx, author in enumerate(missing_authors, start=1):
        logger.info(f"[extra {idx}/{len(missing_authors)}] Verifying {author}")
        raw = client.grounded_json(
            author_verification_prompt(author, args.threshold),
            cache_label=f"v2_author_extra_{normalize_key(author)}",
            max_output_tokens=3072,
        )
        text = extract_text(raw)
        try:
            parsed = robust_json_parse(text)
        except Exception as exc:
            logger.warning(f"Could not parse extra author payload for {author}: {exc}")
            parsed = {
                "author_name": author,
                "represented_by_seymour": False,
                "qualifying_series": [],
                "parse_error": str(exc),
            }
        parsed = normalize_author_payload(parsed, author)
        parsed["_grounding_sources"] = extract_grounding_sources(raw)
        author_payloads.append(parsed)
        all_rows.extend(normalize_series_rows(parsed, args.threshold))

    final_rows = dedupe_rows(all_rows)
    df = pd.DataFrame(final_rows)

    if not df.empty:
        df["Primary Works"] = pd.to_numeric(df["Primary Works"], errors="coerce").astype("Int64")
        df["Total Books in Series"] = pd.to_numeric(df["Total Books in Series"], errors="coerce").astype("Int64")
        df["First Book GR Rating"] = pd.to_numeric(df["First Book GR Rating"], errors="coerce")
        df["First Book GR Rating Count"] = pd.to_numeric(df["First Book GR Rating Count"], errors="coerce").astype("Int64")
        df = df.sort_values(["Author Name", "Primary Works", "Series Name"], ascending=[True, False, True])

    prefix = args.output_prefix
    report = {
        "generated_at": datetime.now().isoformat(),
        "threshold": args.threshold,
        "seed_authors": seed_authors,
        "discovery_round1": discovery_parsed,
        "completeness_round": completeness_parsed,
        "candidate_authors": candidate_authors,
        "final_verified_authors": unique_preserve(df["Author Name"].tolist()) if not df.empty else [],
        "author_payloads": author_payloads,
        "final_row_count": int(len(df)),
    }

    json_path = OUTPUT_DIR / f"{prefix}.json"
    csv_path = OUTPUT_DIR / f"{prefix}.csv"
    report_path = OUTPUT_DIR / f"{prefix}_summary.txt"

    json_path.write_text(json.dumps(report, indent=2, ensure_ascii=False))
    df.to_csv(csv_path, index=False)

    lines = [
        f"Generated: {report['generated_at']}",
        f"Threshold: {args.threshold}+ primary works",
        f"Series rows: {len(df)}",
        f"Verified authors: {', '.join(report['final_verified_authors'])}",
        "",
        "RBmedia breakdown:",
    ]
    if not df.empty:
        counts = df["RB Media Sanity Check"].fillna("no").value_counts().to_dict()
        for key in ("yes_series_level", "yes_author_level", "no"):
            lines.append(f"- {key}: {counts.get(key, 0)}")
    report_path.write_text("\n".join(lines))

    logger.success(f"Wrote CSV: {csv_path}")
    logger.success(f"Wrote JSON: {json_path}")
    logger.success(f"Wrote summary: {report_path}")


if __name__ == "__main__":
    main()
