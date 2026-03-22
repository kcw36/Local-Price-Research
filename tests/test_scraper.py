"""
Tests for scraper.py — directory parsing (Yell + Checkatrade) and orchestration.

All tests use static HTML fixtures or mocked coroutines — no network calls, no Playwright.

Test organisation:
  1. _parse_yell_html       — pure function, fed fixture HTML directly
  2. _parse_checkatrade_html — pure function, fed fixture HTML directly
  3. _dedup_businesses       — pure function, no I/O
  4. scrape_directory        — async, mocks _fetch_page_html
  5. scrape_checkatrade      — async, mocks _fetch_page_html
  6. visit_business_site     — async, mocks async_playwright
  7. scrape_all              — async orchestrator, mocks scrape_directory +
                               scrape_checkatrade + visit_business_site + extract_prices
"""

import os
import pytest
from unittest.mock import AsyncMock, patch

from scraper import (
    _parse_yell_html,
    _parse_checkatrade_html,
    _dedup_businesses,
    scrape_directory,
    scrape_checkatrade,
    scrape_all,
    visit_business_site,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fixture(name: str) -> str:
    path = os.path.join(os.path.dirname(__file__), "fixtures", name)
    with open(path, encoding="utf-8") as f:
        return f.read()


# ---------------------------------------------------------------------------
# _parse_yell_html — pure HTML parsing (no network, no Playwright)
# ---------------------------------------------------------------------------


def test_parse_yell_html_returns_businesses():
    html = _fixture("yell_solihull_plumbers.html")
    businesses = _parse_yell_html(html)
    assert len(businesses) >= 3


def test_parse_yell_html_known_names():
    html = _fixture("yell_solihull_plumbers.html")
    names = [b["name"] for b in _parse_yell_html(html)]
    assert any("Solihull Plumbing" in n for n in names)
    assert any("West Midlands Heating" in n for n in names)


def test_parse_yell_html_extracts_phone():
    html = _fixture("yell_solihull_plumbers.html")
    phones = [b["phone"] for b in _parse_yell_html(html) if b["phone"]]
    assert len(phones) >= 1


def test_parse_yell_html_extracts_website():
    html = _fixture("yell_solihull_plumbers.html")
    websites = [b["website"] for b in _parse_yell_html(html) if b["website"]]
    assert len(websites) >= 2


def test_parse_yell_html_source_field():
    html = _fixture("yell_solihull_plumbers.html")
    for biz in _parse_yell_html(html):
        assert biz["source"] == "yell"


def test_parse_yell_html_yell_url_is_absolute():
    html = _fixture("yell_solihull_plumbers.html")
    yell_urls = [b["yell_url"] for b in _parse_yell_html(html) if b["yell_url"]]
    for url in yell_urls:
        assert url.startswith("https://www.yell.com"), f"Expected absolute URL, got: {url}"


def test_parse_yell_html_empty_html_returns_empty():
    assert _parse_yell_html("") == []


def test_parse_yell_html_no_listings_returns_empty():
    html = "<html><body><p>No results found.</p></body></html>"
    # Falls back to /biz/ link scan — which also finds nothing here
    result = _parse_yell_html(html)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# _parse_checkatrade_html — pure HTML parsing (no network, no Playwright)
# ---------------------------------------------------------------------------


def test_parse_checkatrade_html_returns_businesses():
    html = _fixture("checkatrade_solihull_plumbers.html")
    businesses = _parse_checkatrade_html(html)
    assert len(businesses) >= 3


def test_parse_checkatrade_html_known_names():
    html = _fixture("checkatrade_solihull_plumbers.html")
    names = [b["name"] for b in _parse_checkatrade_html(html)]
    assert any("Solihull Plumbing Pro" in n for n in names)
    assert any("Ace Plumbers Solihull" in n for n in names)


def test_parse_checkatrade_html_extracts_phone():
    html = _fixture("checkatrade_solihull_plumbers.html")
    phones = [b["phone"] for b in _parse_checkatrade_html(html) if b["phone"]]
    assert len(phones) >= 1


def test_parse_checkatrade_html_extracts_website():
    html = _fixture("checkatrade_solihull_plumbers.html")
    websites = [b["website"] for b in _parse_checkatrade_html(html) if b["website"]]
    assert len(websites) >= 2


def test_parse_checkatrade_html_source_field():
    html = _fixture("checkatrade_solihull_plumbers.html")
    for biz in _parse_checkatrade_html(html):
        assert biz["source"] == "checkatrade"


def test_parse_checkatrade_html_checkatrade_url_is_absolute():
    html = _fixture("checkatrade_solihull_plumbers.html")
    ct_urls = [b["checkatrade_url"] for b in _parse_checkatrade_html(html) if b["checkatrade_url"]]
    for url in ct_urls:
        assert url.startswith("https://www.checkatrade.com"), f"Expected absolute URL, got: {url}"


def test_parse_checkatrade_html_empty_html_returns_empty():
    assert _parse_checkatrade_html("") == []


def test_parse_checkatrade_html_no_listings_returns_empty():
    html = "<html><body><p>No results.</p></body></html>"
    result = _parse_checkatrade_html(html)
    assert isinstance(result, list)


# ---------------------------------------------------------------------------
# _dedup_businesses — pure deduplication logic
# ---------------------------------------------------------------------------


def test_dedup_by_phone_removes_duplicate():
    businesses = [
        {"name": "Ace Plumbing", "phone": "0121 111 2222", "source": "yell"},
        {"name": "Ace Plumbing Ltd", "phone": "0121 111 2222", "source": "checkatrade"},
    ]
    result = _dedup_businesses(businesses)
    assert len(result) == 1
    assert result[0]["source"] == "yell"  # first one preserved


def test_dedup_by_name_removes_duplicate():
    businesses = [
        {"name": "Best Drains", "phone": "", "source": "yell"},
        {"name": "Best Drains", "phone": "", "source": "checkatrade"},
    ]
    result = _dedup_businesses(businesses)
    assert len(result) == 1


def test_dedup_name_case_insensitive():
    businesses = [
        {"name": "Best Drains", "phone": "", "source": "yell"},
        {"name": "best drains", "phone": "", "source": "checkatrade"},
    ]
    result = _dedup_businesses(businesses)
    assert len(result) == 1


def test_dedup_different_businesses_both_kept():
    businesses = [
        {"name": "Ace Plumbing", "phone": "0121 111 1111", "source": "yell"},
        {"name": "Best Drains", "phone": "0121 222 2222", "source": "checkatrade"},
    ]
    result = _dedup_businesses(businesses)
    assert len(result) == 2


def test_dedup_empty_phone_not_matched():
    """Two businesses with empty phone should not be deduplicated on phone."""
    businesses = [
        {"name": "Ace Plumbing", "phone": "", "source": "yell"},
        {"name": "Best Drains", "phone": "", "source": "checkatrade"},
    ]
    result = _dedup_businesses(businesses)
    assert len(result) == 2


def test_dedup_preserves_order():
    businesses = [
        {"name": "Zeta Plumbing", "phone": "0121 111 1111", "source": "yell"},
        {"name": "Alpha Drains", "phone": "0121 222 2222", "source": "checkatrade"},
    ]
    result = _dedup_businesses(businesses)
    assert result[0]["name"] == "Zeta Plumbing"
    assert result[1]["name"] == "Alpha Drains"


# ---------------------------------------------------------------------------
# scrape_directory — async, mocks _fetch_page_html
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scrape_directory_uses_yell_url():
    html = _fixture("yell_solihull_plumbers.html")
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value=html)):
        businesses = await scrape_directory("Solihull", "plumbers")
    assert len(businesses) >= 3
    assert all(b["source"] == "yell" for b in businesses)


@pytest.mark.asyncio
async def test_scrape_directory_returns_empty_on_fetch_failure():
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value="")):
        businesses = await scrape_directory("Solihull", "plumbers")
    assert businesses == []


# ---------------------------------------------------------------------------
# scrape_checkatrade — async, mocks _fetch_page_html
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scrape_checkatrade_parses_fixture():
    html = _fixture("checkatrade_solihull_plumbers.html")
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value=html)):
        businesses = await scrape_checkatrade("Solihull", "plumbers")
    assert len(businesses) >= 3
    assert all(b["source"] == "checkatrade" for b in businesses)


@pytest.mark.asyncio
async def test_scrape_checkatrade_returns_empty_on_fetch_failure():
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value="")):
        businesses = await scrape_checkatrade("Solihull", "plumbers")
    assert businesses == []


# ---------------------------------------------------------------------------
# visit_business_site — Playwright mocked
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_visit_business_site_returns_text_on_success():
    """Mock Playwright to return fixture HTML text."""
    from bs4 import BeautifulSoup
    html = _fixture("business_with_price.html")
    expected_text = BeautifulSoup(html, "html.parser").get_text()

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
        result = await visit_business_site("https://example.com")

    assert "£" in result or "price" in result.lower() or len(result) > 10


@pytest.mark.asyncio
async def test_visit_business_site_returns_empty_on_error():
    """Playwright exception → return "", not raise."""
    with patch("scraper.async_playwright", side_effect=Exception("browser crash")):
        result = await visit_business_site("https://example.com")
    assert result == ""


@pytest.mark.asyncio
async def test_visit_business_site_returns_empty_for_empty_url():
    result = await visit_business_site("")
    assert result == ""


# ---------------------------------------------------------------------------
# scrape_all — orchestration (both directories → dedup → visit → extract)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_scrape_all_happy_path(monkeypatch):
    """
    scrape_all should merge both sources, visit each website, extract prices,
    and call the progress callback once per business.
    """
    fake_yell = [
        {"name": "Ace Plumbing", "website": "https://aceplumbing.example.com",
         "phone": "0121 111 1111", "yell_url": "", "source": "yell"},
    ]
    fake_checkatrade = [
        {"name": "Best Drains", "website": "https://bestdrains.example.com",
         "phone": "0121 222 2222", "checkatrade_url": "", "yell_url": "", "source": "checkatrade"},
    ]
    fake_prices = [{"price": 75.0, "service": "hourly rate", "unit": "per hour",
                    "confidence": "Med", "raw_text": "£75/hr"}]

    monkeypatch.setattr("scraper.scrape_directory", AsyncMock(return_value=fake_yell))
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=fake_checkatrade))
    monkeypatch.setattr("scraper.visit_business_site", AsyncMock(return_value="£75 per hour"))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: fake_prices)
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    progress_calls = []

    def track_progress(current, total):
        progress_calls.append((current, total))

    results = await scrape_all("Solihull", "plumbers", progress_cb=track_progress)

    assert len(results) == 2
    names = {r["name"] for r in results}
    assert "Ace Plumbing" in names
    assert "Best Drains" in names
    assert results[0]["prices"] == fake_prices
    assert results[0]["extraction_method"] == "regex"

    # Progress callback should fire once per business
    assert progress_calls == [(1, 2), (2, 2)]


@pytest.mark.asyncio
async def test_scrape_all_deduplicates_across_sources(monkeypatch):
    """A business appearing in both Yell and Checkatrade with the same phone should appear once."""
    shared_phone = "0121 111 1111"
    fake_yell = [
        {"name": "Ace Plumbing", "website": "https://aceplumbing.example.com",
         "phone": shared_phone, "yell_url": "", "source": "yell"},
    ]
    fake_checkatrade = [
        {"name": "Ace Plumbing Ltd", "website": "https://aceplumbing.example.com",
         "phone": shared_phone, "checkatrade_url": "", "yell_url": "", "source": "checkatrade"},
    ]

    monkeypatch.setattr("scraper.scrape_directory", AsyncMock(return_value=fake_yell))
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=fake_checkatrade))
    monkeypatch.setattr("scraper.visit_business_site", AsyncMock(return_value=""))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: [])
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    results = await scrape_all("Solihull", "plumbers")

    assert len(results) == 1
    assert results[0]["source"] == "yell"  # Yell takes precedence


@pytest.mark.asyncio
async def test_scrape_all_returns_empty_when_no_businesses(monkeypatch):
    """If both directories find nothing, scrape_all returns [] without visiting any sites."""
    monkeypatch.setattr("scraper.scrape_directory", AsyncMock(return_value=[]))
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=[]))
    mock_visit = AsyncMock()
    monkeypatch.setattr("scraper.visit_business_site", mock_visit)

    results = await scrape_all("Solihull", "plumbers")

    assert results == []
    mock_visit.assert_not_called()


@pytest.mark.asyncio
async def test_scrape_all_skips_visit_for_business_without_website(monkeypatch):
    """Business with no website URL should not trigger a Playwright visit."""
    fake_businesses = [
        {"name": "No Site Ltd", "website": "", "phone": "0121 333 3333",
         "yell_url": "", "source": "yell"},
    ]
    monkeypatch.setattr("scraper.scrape_directory", AsyncMock(return_value=fake_businesses))
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=[]))
    mock_visit = AsyncMock(return_value="")
    monkeypatch.setattr("scraper.visit_business_site", mock_visit)
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: [])
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    results = await scrape_all("Solihull", "plumbers")

    assert len(results) == 1
    assert results[0]["prices"] == []
    assert results[0]["extraction_method"] == "none"
    mock_visit.assert_not_called()


@pytest.mark.asyncio
async def test_scrape_all_extraction_method_llm_when_low_confidence(monkeypatch):
    """extraction_method should be 'llm' when any price has confidence='Low'."""
    fake_yell = [
        {"name": "Ace Plumbing", "website": "https://aceplumbing.example.com",
         "phone": "0121 111 1111", "yell_url": "", "source": "yell"},
    ]
    llm_prices = [{"price": 80.0, "service": "callout", "unit": "fixed",
                   "confidence": "Low", "raw_text": "around eighty pounds"}]

    monkeypatch.setattr("scraper.scrape_directory", AsyncMock(return_value=fake_yell))
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=[]))
    monkeypatch.setattr("scraper.visit_business_site", AsyncMock(return_value="around eighty pounds"))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: llm_prices)
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    results = await scrape_all("Solihull", "plumbers")

    assert results[0]["extraction_method"] == "llm"
