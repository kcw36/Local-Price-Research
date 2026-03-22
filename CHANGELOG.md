# Changelog

All notable changes to this project will be documented in this file.

## [0.1.1.0] - 2026-03-22

### Changed
- Yell.com directory scraper switched from `requests` + BeautifulSoup to Playwright (bypasses Cloudflare JS challenges)
- `scrape_directory()` is now async; removed `run_in_executor` wrapper from `scrape_all()`
- Real browser User-Agent (`Chrome/121`) used for all Playwright calls (required for Cloudflare)

### Added
- Checkatrade as a second concurrent directory source (`scrape_checkatrade()`)
- Shared `_fetch_page_html()` Playwright helper — `domcontentloaded` + 2s wait for reliable Cloudflare bypass
- Pure HTML parsing functions (`_parse_yell_html`, `_parse_checkatrade_html`) for testability with static fixtures
- `_dedup_businesses()` — deduplicates across sources by phone (exact) then name (case-insensitive); Yell takes precedence
- `source` field on all business dicts (`"yell"` or `"checkatrade"`)
- `checkatrade_url` field on Checkatrade results
- Checkatrade HTML fixture for tests (`tests/fixtures/checkatrade_solihull_plumbers.html`)
- 22 new scraper tests; total 92 tests across 5 suites

## [0.1.0.0] - 2026-03-21

### Added
- FastAPI web application with async background job pattern (`app.py`)
- SQLite job persistence with 5-state lifecycle: pending → running → done/error/timeout (`database.py`)
- Yell.com + Checkatrade directory scrapers using Playwright (bypasses Cloudflare JS challenges) with pure-function HTML parsers (`scraper.py`)
- Deduplication across directory sources by phone (exact) then name (case-insensitive); Yell takes precedence (`scraper.py`)
- Concurrent directory scraping via `asyncio.gather` — both sources run simultaneously (`scraper.py`)
- Playwright headless browser for visiting business websites with 15s timeout and partial-load fallback (`scraper.py`)
- Regex-based price extraction pipeline with confidence scoring (High/Med/Low) (`extractor.py`)
- Optional LLM fallback extraction via Anthropic API, guarded by `LLM_FALLBACK_ENABLED` feature flag (`extractor.py`)
- LLM-powered pricing summary via Claude, with plain-text fallback when no API key is set (`summary.py`)
- Input validation for area and trade type with regex guards (`app.py`)
- Frontend: search form, live progress polling (3s interval), and results page with price table and summary card (`templates/`)
- Job timeout support via `asyncio.wait_for()` with configurable `JOB_TIMEOUT_SECONDS`
- `.env.example` with all configurable settings documented
- 92 tests across 5 test suites with full new code path coverage (pytest + pytest-asyncio)
- GitHub Actions CI workflow running full test suite on push and pull requests
