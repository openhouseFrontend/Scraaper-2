# Google Maps Lead Extractor (Pure Python)

Production-style Google Maps lead scraper for Delhi/NCR real estate queries using:
- Selenium 4 + undetected-chromedriver
- BeautifulSoup4
- asyncio + concurrent.futures
- pandas
- rich

No third-party scraping APIs are used.

## Setup

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

### 1) CLI queries
```bash
python main.py --queries "real estate agent Delhi" "property dealer Dwarka"
```

### 2) Query file
```bash
python main.py --query-file queries.txt
```

### 3) Interactive mode
```bash
python main.py
```
If you press Enter without input, built-in default Delhi/NCR queries are used.

## Helpful flags

- `--headless` : run Chrome headless
- `--max-workers 3` : number of parallel query workers
- `--output-dir output` : output folder
- `--log-level DEBUG` : verbose logs

## Output fields

`business_name, category, rating, review_count, address, locality, city, phone, website, google_maps_url, plus_code, hours, services, query_source, scraped_at`

Exports are generated in both CSV and JSON under `output/`.

