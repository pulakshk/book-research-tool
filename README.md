# Book Research Pipeline

A robust, improved data pipeline for scraping, matching, and enriching book metadata (Goodreads + Amazon).

## 🚀 Features

-   **Consolidated Pipeline**: A single `main.py` entry point.
-   **Turbo Scraping**: Uses Playwright + Gemini for massive scalability (extracts entire series hubs).
-   **AI Enrichment**: Fills metadata gaps (Publisher, Tropes, Descriptions) using Gemini 1.5.
-   **Commissioning Analysis**: Weighted scoring model (Volume, Quality, Retention, Appeal) to identify P0 series.

## 🛠️ Installation

1.  Clone the repo.
2.  Install dependencies:
    ```bash
    pip install -r requirements.txt
    playwright install
    ```
3.  Set up env:
    ```bash
    cp .env.example .env
    # Add your GEMINI_API_KEY
    ```

## 🏃 Usage

```bash
# Run Everything (Scrape -> Clean -> Enrich -> Analyze)
python3 main.py --all

# Run Individual Phases
python3 main.py --scrape   # Series Exhaustion
python3 main.py --clean    # Filter & Dedupe
python3 main.py --enrich   # Gemini Metadata
python3 main.py --analyze  # Generate Report
```

## 📂 Structure

-   `src/pipeline/`: Core logic modules.
-   `main.py`: CLI entry point.
-   `utils/`: Helper utilities (legacy).

## 🔒 Security

-   **Never commit `.env`**.
-   Ensure `GEMINI_API_KEY` is kept secret.
