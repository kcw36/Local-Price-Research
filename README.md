# Local Pricing Research

A local tool for researching market rates charged by UK tradespeople. Search by area and trade type, and the tool scrapes Checkatrade for business listings and priced services, visits each business website, extracts pricing information, and produces an LLM-generated market rate summary. Yell.com scraping is supported but disabled by default.

## Quick start

```bash
# 1. Clone and install dependencies
pip install -r requirements.txt
playwright install chromium

# 2. Configure (optional — defaults work for Solihull plumbers)
cp .env.example .env
# Edit .env — set ANTHROPIC_API_KEY for LLM summary and optional LLM extraction

# 3. Run
uvicorn app:app --reload

# 4. Open http://localhost:8000
```

## How it works

```
User submits search (area + trade type)
        │
        ▼
POST /search → creates job → returns job_id
        │
        ▼ (background, async)
scrape_checkatrade(area, trade) ← Playwright, bypasses Cloudflare
  ├─ businesses list
  └─ priced_services (structured £ data from search page carousel)
[scrape_directory / Yell.com — only if YELL_ENABLED=true]
        │
        ▼ dedup + merge
        │
        ▼ visit Checkatrade profile pages (≤10) → phone + website
        │
        ▼ for each business
visit_business_site(url)        ← Playwright headless browser (JS-rendered pages)
        │
        ▼
extract_prices(page_text, url)  ← regex pass → LLM fallback (if enabled)
        │
        ▼
generate_summary(businesses, priced_services)  ← single Claude API call (or plain-text fallback)
        │
        ▼
GET /results/{job_id}           ← results page with price table + summary card
```

Frontend polls `GET /status/{job_id}` every 3 seconds and shows a progress bar while the job runs.

## Configuration

All settings are via environment variables (see `.env.example`):

| Variable | Default | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | — | Required for LLM summary and LLM extraction fallback |
| `AREA` | `Solihull` | Default area pre-filled in search form |
| `TRADE_TYPE` | `plumbers` | Default trade type |
| `MAX_BUSINESSES` | `20` | Max businesses to scrape per search |
| `POLITENESS_DELAY_SECONDS` | `1.5` | Delay between website visits |
| `JOB_TIMEOUT_SECONDS` | `600` | Max time per scrape job |
| `LLM_FALLBACK_ENABLED` | `false` | Enable LLM price extraction on pages where regex finds nothing |
| `YELL_ENABLED` | `false` | Enable Yell.com scraper (disabled by default; Checkatrade is primary) |
| `DATABASE_PATH` | `jobs.db` | SQLite database file path |

## Running tests

```bash
pytest tests/ -v
```

124 tests across 5 test files. All network calls are mocked — no live scraping during tests.

## Architecture

- **`app.py`** — FastAPI routes, input validation, background job runner
- **`config.py`** — Settings dataclass, shared Anthropic client singleton
- **`database.py`** — SQLite job persistence (`jobs` table: pending → running → done/error/timeout; `priced_services` table for structured Checkatrade pricing)
- **`scraper.py`** — Checkatrade + optional Yell.com directory scrapers (Playwright, bypasses Cloudflare); priced services parser; business site visitor; unified `_browser_session()` lifecycle
- **`extractor.py`** — Regex price extraction + optional LLM fallback
- **`summary.py`** — LLM market rate summary combining priced services + website prices; plain-text fallback
- **`templates/`** — Jinja2 HTML templates (index, loading, results, error)
