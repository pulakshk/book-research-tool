# Post-Mortem & Best Practices: Book Series Research

This document captures the lessons learned from the Sports & Hockey Romance commissioning project. These principles should be applied when expanding to new genres (e.g., Paranormal, Dark Romance).

## 🏆 Key Successes

### 1. Hybrid Ranking Model (P0-P5)
The implementation of a composite scoring model successfully balanced **Volume** (Books), **Quality** (Ratings), **Velocity** (Amazon Rank), and **Retention** (Read-through). This allowed for the immediate identification of "Elite Indie" targets vs. "Capped" Big Pub titles.

### 2. Automated Series Hub Extraction
Using Playwright/Gemini to scrape entire series hubs (e.g., from Goodreads) ensured that the dataset was exhaustive, identifying missing books that initial keyword searches missed.

### 3. Mass Metadata Merging
The `mass_series_merger.py` approach proved faster and more accurate than manual injection. By using `difflib` for fuzzy matching, we could sync 100+ series with high-density research data in seconds.

---

## ❌ Failures & Resolutions

### 1. The Author Hallucination Trap
**Failure**: LLMs occasionally attributed popular books to the wrong series (e.g., *Icebreaker* to Elle Kennedy).
**Resolution**: Implement a **Cross-List Validation** step. Any series in a Top 100 list must have its author verified against the T100 CSV before being promoted to P0.

### 2. Novella/Prequel Inflation
**Failure**: Including "0.5" prequels and anthologies in the `Books in Series` count inflated the perceived volume of short series.
**Resolution**: Use regex to exclude titles containing "Novella", "Prequel", "Duet", or "Box Set" from the primary count.

### 3. Dependency Fragility
**Failure**: Scripts failed due to missing 3rd party libraries like `rapidfuzz` or `fuzzywuzzy`.
**Resolution**: **Standard Library First**. For critical data mergers, use Python's built-in `difflib` and `re` modules to ensure scripts remain portable across all environments.

---

## 🛠 Best Practices for Future Genres

### ✅ Data Normalization
Always normalize `Series Name` and `Author Name` using a standard routine:
- Lowercase everything.
- Remove suffixes: "Series", "Collection", "(Book 1)".
- Trim all whitespace and special characters (#, ,).

### ✅ Density First, Analysis Second
Never run the ranking algorithm on sparse data. Use a **Discovery -> Enrichment -> Merge** loop to ensure 100% density for Ratings and Counts before calculating scores.

### ✅ Defensive Reporting
Include a `Source Validation` column that stores raw proof (e.g., "Hockey #18, Sports #56"). This allows stakeholders to verify Rank claims without re-running the pipeline.

### ✅ The Page Count Proxy
When exact total series length is unknown, use the formula:
`Total Series Pages = (Books in Series) * (Average Page Count of First 3 Books)`
This normalization prevents "Unknown" values from breaking the sorting logic.
