"""
Tests for scraper.py — Yell.com directory parsing and business scraping.

All tests use static HTML fixtures — no network calls, no Playwright.
"""

import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from bs4 import BeautifulSoup

from scraper import scrape_directory, scrape_all


# ---------------------------------------------------------------------------
# scrape_directory — HTML fixture parsing
# ---------------------------------------------------------------------------


def test_scrape_directory_parses_fixture(monkeypatch):
    """
    scrape_directory should parse the Yell fixture and return businesses.
    We mock requests.get to return the fixture HTML.
    """
    import os
    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "yell_solihull_plumbers.html")
    with open(fixture_path, encoding="utf-8") as f:
        html_content = f.read()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = html_content

    with patch("scraper.requests.get", return_value=mock_resp):
        businesses = scrape_directory("Solihull", "plumbers")

    assert len(businesses) >= 3
    names = [b["name"] for b in businesses]
    assert any("Solihull Plumbing" in n for n in names)
    assert any("West Midlands Heating" in n for n in names)


def test_scrape_directory_includes_phone(monkeypatch):
    import os
    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "yell_solihull_plumbers.html")
    with open(fixture_path, encoding="utf-8") as f:
        html_content = f.read()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = html_content

    with patch("scraper.requests.get", return_value=mock_resp):
        businesses = scrape_directory("Solihull", "plumbers")

    # At least one business should have a phone
    phones = [b["phone"] for b in businesses if b["phone"]]
    assert len(phones) >= 1


def test_scrape_directory_includes_website_url(monkeypatch):
    import os
    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "yell_solihull_plumbers.html")
    with open(fixture_path, encoding="utf-8") as f:
        html_content = f.read()

    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = html_content

    with patch("scraper.requests.get", return_value=mock_resp):
        businesses = scrape_directory("Solihull", "plumbers")

    websites = [b["website"] for b in businesses if b["website"]]
    assert len(websites) >= 2


def test_scrape_directory_handles_http_error_gracefully():
    """Non-200 response should return empty list, not raise."""
    mock_resp = MagicMock()
    mock_resp.status_code = 503
    mock_resp.text = ""

    with patch("scraper.requests.get", return_value=mock_resp):
        businesses = scrape_directory("Solihull", "plumbers")

    assert businesses == []


def test_scrape_directory_handles_network_error_gracefully():
    """Network exception should return empty list, not raise."""
    import requests as req_lib

    with patch("scraper.requests.get", side_effect=req_lib.ConnectionError("no network")):
        businesses = scrape_directory("Solihull", "plumbers")

    assert businesses == []


def test_scrape_directory_handles_429_with_retry(monkeypatch):
    """429 response should back off and retry once."""
    import time

    mock_resp_429 = MagicMock()
    mock_resp_429.status_code = 429
    mock_resp_429.text = ""

    mock_resp_ok = MagicMock()
    mock_resp_ok.status_code = 200
    mock_resp_ok.text = "<html><body><div class='searchResults'></div></body></html>"

    call_count = {"n": 0}

    def mock_get(url, headers, timeout):
        call_count["n"] += 1
        if call_count["n"] == 1:
            return mock_resp_429
        return mock_resp_ok

    monkeypatch.setattr("scraper.time.sleep", lambda x: None)  # don't actually sleep
    with patch("scraper.requests.get", side_effect=mock_get):
        businesses = scrape_directory("Solihull", "plumbers")

    assert call_count["n"] == 2  # retried once


# ---------------------------------------------------------------------------
# visit_business_site — Playwright mocked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_visit_business_site_returns_text_on_success(monkeypatch):
    """Mock Playwright to return fixture HTML text."""
    import os
    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "business_with_price.html")
    with open(fixture_path, encoding="utf-8") as f:
        html = f.read()

    from bs4 import BeautifulSoup
    expected_text = BeautifulSoup(html, "html.parser").get_text()

    # Build a minimal Playwright mock
    mock_page = AsyncMock()
    mock_page.goto = AsyncMock()
    mock_page.inner_text = AsyncMock(return_value=expected_text)

    mock_context = AsyncMock()
    mock_context.new_page = AsyncMock(return_value=mock_page)

    mock_browser = AsyncMock()
    mock_browser.new_context = AsyncMock(return_value=mock_context)
    mock_browser.close = AsyncMock()

    mock_chromium = AsyncMock()
    mock_chromium.launch = AsyncMock(return_value=mock_browser)

    mock_playwright = AsyncMock()
    mock_playwright.__aenter__ = AsyncMock(return_value=mock_playwright)
    mock_playwright.__aexit__ = AsyncMock(return_value=None)
    mock_playwright.chromium = mock_chromium

    with patch("scraper.async_playwright", return_value=mock_playwright):
        from scraper import visit_business_site
        result = await visit_business_site("https://example.com")

    assert "£" in result or "price" in result.lower() or len(result) > 10


@pytest.mark.asyncio
async def test_visit_business_site_returns_empty_on_error():
    """Playwright exception → return "", not raise."""
    with patch("scraper.async_playwright", side_effect=Exception("browser crash")):
        from scraper import visit_business_site
        result = await visit_business_site("https://example.com")

    assert result == ""


@pytest.mark.asyncio
async def test_visit_business_site_returns_empty_for_empty_url():
    from scraper import visit_business_site
    result = await visit_business_site("")
    assert result == ""


# ---------------------------------------------------------------------------
# scrape_all — orchestration (directory → visit → extract → progress)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scrape_all_happy_path(monkeypatch):
    """
    scrape_all should visit each business website, extract prices,
    and call the progress callback once per business.
    """
    fake_businesses = [
        {"name": "Ace Plumbing", "website": "https://aceplumbing.example.com", "phone": "0121 111 1111", "yell_url": ""},
        {"name": "Best Drains", "website": "https://bestdrains.example.com", "phone": "0121 222 2222", "yell_url": ""},
    ]
    fake_prices = [{"price": 75.0, "service": "hourly rate", "unit": "per hour", "confidence": "Med", "raw_text": "£75/hr"}]

    monkeypatch.setattr("scraper.scrape_directory", lambda area, trade: fake_businesses)
    monkeypatch.setattr("scraper.visit_business_site", AsyncMock(return_value="£75 per hour"))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: fake_prices)
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    progress_calls = []

    def track_progress(current, total):
        progress_calls.append((current, total))

    results = await scrape_all("Solihull", "plumbers", progress_cb=track_progress)

    assert len(results) == 2
    assert results[0]["name"] == "Ace Plumbing"
    assert results[0]["prices"] == fake_prices
    assert results[0]["extraction_method"] == "regex"
    assert results[1]["name"] == "Best Drains"

    # Progress callback should fire once per business
    assert progress_calls == [(1, 2), (2, 2)]


@pytest.mark.asyncio
async def test_scrape_all_returns_empty_when_no_businesses(monkeypatch):
    """If scrape_directory finds nothing, scrape_all returns [] without visiting any sites."""
    monkeypatch.setattr("scraper.scrape_directory", lambda area, trade: [])
    mock_visit = AsyncMock()
    monkeypatch.setattr("scraper.visit_business_site", mock_visit)

    results = await scrape_all("Solihull", "plumbers")

    assert results == []
    mock_visit.assert_not_called()


@pytest.mark.asyncio
async def test_scrape_all_skips_visit_for_business_without_website(monkeypatch):
    """Business with no website URL should not trigger a Playwright visit."""
    fake_businesses = [
        {"name": "No Site Ltd", "website": "", "phone": "0121 333 3333", "yell_url": ""},
    ]
    monkeypatch.setattr("scraper.scrape_directory", lambda area, trade: fake_businesses)
    mock_visit = AsyncMock(return_value="")
    monkeypatch.setattr("scraper.visit_business_site", mock_visit)
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: [])
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    results = await scrape_all("Solihull", "plumbers")

    assert len(results) == 1
    assert results[0]["prices"] == []
    assert results[0]["extraction_method"] == "none"
    mock_visit.assert_not_called()
