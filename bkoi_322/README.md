# Modular Web Scraper Curriculum

A production-grade Python web scraping project built on Ubuntu Linux.

## Tech Stack
* **Language:** Python 3
* **Virtual Environment:** `venv` (`.venv`)
* **Libraries:** `requests`, `BeautifulSoup4`, `urllib3`
* **Formatting:** `ruff`

## Project Structure
* `01_single_page_scraper.py`: Basic single-page HTML parsing.
* `02_paginated_scraper.py`: Automatic next-page navigation and relative URL joining.
* `03_nested_detail_scraper.py`: Multi-level catalog crawling for item details.
* `04_production_scraper.py`: Robust HTTP session pooling, exponential retries, User-Agent headers, and rate limiting.

## Quickstart
```bash
source .venv/bin/activate
pip install -r requirements.txt
python 04_production_scraper.py
