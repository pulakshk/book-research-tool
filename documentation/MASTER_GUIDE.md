
# Book Data Enrichment Project Guide

This project is a high-performance, autonomous scraping and recovery pipeline designed to enrich book datasets (specifically for Hockey Romance) with metadata from Amazon and Goodreads.

---

## 🚀 The Autonomous Pipeline (Turbo Mode)

The project is managed by a self-healing **Orchestrator** that coordinates multiple scrapers to achieve 100% data coverage.

| Component | Script | Goal | Concurrency |
| :--- | :--- | :--- | :--- |
| **Orchestrator** | `orchestrator.py` | The master "brain". Audits gaps and triggers scripts in iterative loops. | 1 Master |
| **Turbo Recovery** | `ultra_recovery.py` | Finds missing Authors, Amazon Links, and Amazon Ratings. | 4 Workers |
| **Series Bulk** | `enrich_by_series.py` | Scrapes entire Goodreads Series pages for bulk mapping. | 1 Worker |
| **Deep Scrape** | `core/detailed_scrape.py` | Extracts Pages, Descriptions, Publisher, and Pub Dates. | 6 Workers |

---

## ⚡ Key Performance Stats
- **Sequential Speed**: ~15 books/min
- **Turbo Speed**: **~50-60 books/min** (4x - 6x faster)
- **Target Completion**: ~1.5 hours for the full dataset.

---

## 🛠 Project Structure

- `orchestrator.py`: Main entry point for the autonomous cycle.
- `core/on_demand_discoverer.py`: CLI tool for seed-based series expansion.
- `utils/dataset_manager.py`: Central utility for auditing, alignment, and beautification.
- `/logs`: 
    - `orchestrator.log`: Main pipeline status.
    - `recovery_orch.log`: Parallel worker logs for recovery.

---

## 🔧 Essential Commands

### 1. Start the Autonomous Pipeline
Run the master orchestrator in the background:
```bash
nohup python3 orchestrator.py > logs/orchestrator.log 2>&1 &
```

### 2. On-Demand Series Discovery
Enrich an entire series from just one book title:
```bash
python3 core/on_demand_discoverer.py "Pucking Around"
```

### 3. Data Auditing & Cleaning
Always audit before and after major runs:
```bash
python3 utils/dataset_manager.py audit [file_path]
python3 utils/dataset_manager.py beautify [file_path]
```

---

## 📊 The 24-Column Master Schema

The pipeline enforces a strict 24-column alignment for the `unified_book_data_enriched_ultra.csv` file:

1. **Series Name**
2. **Author Name**
3. **Book Name**
4. **Book Number**
5. **Total Books in Series**
6. **Goodreads Link**
7. **Goodreads # of Ratings**
8. **Goodreads Rating**
9. **First Published**
10. **Original Published**
11. **Pages**
12. **Description**
13. **Primary Trope**
14. **Primary Subgenre**
15. **Series Status**
16. **Amazon Link**
17. **Amazon # of Ratings**
18. **Amazon Rating**
19. **Publisher**
20. **Self Pub flag**
21. **Short Synopsis** (Auto-generated)
22. **Publication Date** (Mapped)
23. **Top Lists**
24. **Featured List** (Amazon Rank)

---

## ⚠️ Safety & Bot Detection
- **Context Rotation**: Workers automatically rotate browser contexts every 20 books.
- **Dynamic Throttling**: Each worker uses random sleeps (2-7s) to maintain a human-like profile.

---

*Last Updated: 2026-01-30 (v2.0 Turbo)*
