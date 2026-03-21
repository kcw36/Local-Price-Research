# Changelog

All notable changes to this project will be documented in this file.

## [0.1.0.0] - 2026-03-21

### Added
- FastAPI web application with async background job pattern (`app.py`)
- SQLite job persistence with 5-state lifecycle: pending → running → done/error/timeout (`database.py`)
- Yell.com directory scraper using requests + BeautifulSoup (`scraper.py`)
- Playwright headless browser for visiting business websites with 15s timeout and partial-load fallback (`scraper.py`)
- Regex-based price extraction pipeline with confidence scoring (High/Med/Low) (`extractor.py`)
- Optional LLM fallback extraction via Anthropic API, guarded by `LLM_FALLBACK_ENABLED` feature flag (`extractor.py`)
- LLM-powered pricing summary via Claude, with plain-text fallback when no API key is set (`summary.py`)
- Input validation for area and trade type with regex guards (`app.py`)
- Frontend: search form, live progress polling (3s interval), and results page with price table and summary card (`templates/`)
- Job timeout support via `asyncio.wait_for()` with configurable `JOB_TIMEOUT_SECONDS`
- `run_in_executor` wrapping of synchronous `scrape_directory()` to avoid blocking the async event loop
- `.env.example` with all configurable settings documented
- 70 tests across 5 test suites with 100% new code path coverage (pytest + pytest-asyncio)
- GitHub Actions CI workflow running full test suite on push and pull requests
