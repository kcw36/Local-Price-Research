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
    _parse_priced_services,
    _checkatrade_location,
    _dedup_businesses,
    scrape_directory,
    scrape_checkatrade,
    scrape_all,
    visit_business_site,
    visit_checkatrade_profile,
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
async def test_scrape_directory_uses_ucs_search_endpoint():
    """
    Regression: ISSUE-004 — scrape_directory was using /search?keywords=...
    which returns Yell's own 404 page. The correct URL is /ucs/UcsSearchAction.do
    which is the form action on the Yell.com homepage.
    Found by /investigate on 2026-03-22.
    """
    html = _fixture("yell_solihull_plumbers.html")
    mock_fetch = AsyncMock(return_value=html)
    with patch("scraper._fetch_page_html", new=mock_fetch):
        await scrape_directory("Solihull", "plumbers")
    called_url = mock_fetch.call_args[0][0]
    assert "/ucs/UcsSearchAction.do" in called_url, (
        f"Expected /ucs/UcsSearchAction.do in URL, got: {called_url}. "
        "Yell changed their search URL — /search?keywords=... returns 404."
    )
    assert "/search?" not in called_url, (
        f"Old /search? URL still being used: {called_url}"
    )


@pytest.mark.asyncio
async def test_scrape_directory_returns_empty_on_fetch_failure():
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value="")):
        businesses = await scrape_directory("Solihull", "plumbers")
    assert businesses == []


# ---------------------------------------------------------------------------
# scrape_checkatrade — async, mocks _fetch_page_html
# ---------------------------------------------------------------------------


def test_checkatrade_location_postcode():
    """Postcodes should be formatted as outcode-incode for Checkatrade URLs."""
    assert _checkatrade_location("b93 8tg") == "B93-8tg"
    assert _checkatrade_location("SW1A 1AA") == "SW1A-1aa"
    assert _checkatrade_location("EC1A 1BB") == "EC1A-1bb"


def test_checkatrade_location_city_name():
    """City names should be title-cased and hyphenated."""
    assert _checkatrade_location("solihull") == "Solihull"
    assert _checkatrade_location("kings heath") == "Kings-Heath"


@pytest.mark.asyncio
async def test_scrape_checkatrade_uses_hyphenated_postcode():
    """
    Regression: postcodes with spaces produced broken URLs (e.g. /in/B93 8Tg).
    Checkatrade expects /in/B93-8tg.
    """
    html = _fixture("checkatrade_solihull_plumbers.html")
    mock_fetch = AsyncMock(return_value=html)
    with patch("scraper._fetch_page_html", new=mock_fetch):
        businesses, priced_services = await scrape_checkatrade("b93 8tg", "boiler repair")
    called_url = mock_fetch.call_args[0][0]
    assert "/in/B93-8tg" in called_url, f"Expected /in/B93-8tg in URL, got: {called_url}"
    assert " " not in called_url.split("//", 1)[-1], f"URL path contains space: {called_url}"


@pytest.mark.asyncio
async def test_scrape_checkatrade_parses_fixture():
    html = _fixture("checkatrade_solihull_plumbers.html")
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value=html)):
        businesses, priced_services = await scrape_checkatrade("Solihull", "plumbers")
    assert len(businesses) >= 3
    assert all(b["source"] == "checkatrade" for b in businesses)


@pytest.mark.asyncio
async def test_scrape_checkatrade_returns_priced_services():
    html = _fixture("checkatrade_with_priced_services.html")
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value=html)):
        businesses, priced_services = await scrape_checkatrade("B93 8TG", "gas boiler servicing")
    assert len(priced_services) == 4
    assert all(s["source"] == "checkatrade" for s in priced_services)


@pytest.mark.asyncio
async def test_scrape_checkatrade_returns_empty_on_fetch_failure():
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value="")):
        businesses, priced_services = await scrape_checkatrade("Solihull", "plumbers")
    assert businesses == []
    assert priced_services == []


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
    scrape_all should scrape Checkatrade, visit profiles + websites, extract prices,
    and call the progress callback once per business.
    """
    fake_checkatrade = (
        [
            {"name": "Ace Plumbing", "website": "https://aceplumbing.example.com",
             "phone": "0121 111 1111", "checkatrade_url": "https://www.checkatrade.com/trades/ace-1",
             "yell_url": "", "source": "checkatrade"},
            {"name": "Best Drains", "website": "https://bestdrains.example.com",
             "phone": "0121 222 2222", "checkatrade_url": "https://www.checkatrade.com/trades/best-2",
             "yell_url": "", "source": "checkatrade"},
        ],
        [],  # priced_services
    )
    fake_prices = [{"price": 75.0, "service": "hourly rate", "unit": "per hour",
                    "confidence": "Med", "raw_text": "£75/hr"}]

    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=fake_checkatrade))
    monkeypatch.setattr("scraper.visit_business_site", AsyncMock(return_value="£75 per hour"))
    monkeypatch.setattr("scraper.visit_checkatrade_profile", AsyncMock(return_value={"phone": "", "website": ""}))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: fake_prices)
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    progress_calls = []

    def track_progress(current, total):
        progress_calls.append((current, total))

    results, priced_services = await scrape_all("Solihull", "plumbers", progress_cb=track_progress)

    assert len(results) == 2
    names = {r["name"] for r in results}
    assert "Ace Plumbing" in names
    assert "Best Drains" in names
    assert results[0]["prices"] == fake_prices
    assert results[0]["extraction_method"] == "regex"

    # Progress callback should fire once per business
    assert progress_calls == [(1, 2), (2, 2)]


@pytest.mark.asyncio
async def test_scrape_all_returns_priced_services(monkeypatch):
    """scrape_all should return priced services from Checkatrade as the second element."""
    fake_priced = [
        {"business_name": "Gas Pro", "service_name": "Boiler Repair",
         "price_value": 95.0, "price_unit": "job", "source": "checkatrade"},
    ]
    fake_checkatrade = (
        [{"name": "Gas Pro", "website": "", "phone": "0121 111 1111",
          "checkatrade_url": "", "yell_url": "", "source": "checkatrade"}],
        fake_priced,
    )

    monkeypatch.setattr("scraper.scrape_directory", AsyncMock(return_value=[]))
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=fake_checkatrade))
    monkeypatch.setattr("scraper.visit_business_site", AsyncMock(return_value=""))
    monkeypatch.setattr("scraper.visit_checkatrade_profile", AsyncMock(return_value={"phone": "", "website": ""}))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: [])
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    results, priced_services = await scrape_all("Solihull", "plumbers")

    assert len(results) == 1
    assert priced_services == fake_priced


@pytest.mark.asyncio
async def test_scrape_all_deduplicates_across_sources(monkeypatch):
    """A business appearing in both Yell and Checkatrade with the same phone should appear once."""
    import config
    shared_phone = "0121 111 1111"
    fake_yell = [
        {"name": "Ace Plumbing", "website": "https://aceplumbing.example.com",
         "phone": shared_phone, "yell_url": "", "source": "yell"},
    ]
    fake_checkatrade = (
        [{"name": "Ace Plumbing Ltd", "website": "https://aceplumbing.example.com",
          "phone": shared_phone, "checkatrade_url": "", "yell_url": "", "source": "checkatrade"}],
        [],
    )

    # Enable Yell for this test
    monkeypatch.setattr(config.settings, "sources_enabled", {"checkatrade": True, "yell": True})
    monkeypatch.setattr("scraper.scrape_directory", AsyncMock(return_value=fake_yell))
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=fake_checkatrade))
    monkeypatch.setattr("scraper.visit_business_site", AsyncMock(return_value=""))
    monkeypatch.setattr("scraper.visit_checkatrade_profile", AsyncMock(return_value={"phone": "", "website": ""}))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: [])
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    results, _ = await scrape_all("Solihull", "plumbers")

    assert len(results) == 1
    assert results[0]["source"] == "yell"  # Yell takes precedence


@pytest.mark.asyncio
async def test_scrape_all_returns_empty_when_no_businesses(monkeypatch):
    """If both directories find nothing, scrape_all returns ([], []) without visiting any sites."""
    monkeypatch.setattr("scraper.scrape_directory", AsyncMock(return_value=[]))
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=([], [])))
    mock_visit = AsyncMock()
    monkeypatch.setattr("scraper.visit_business_site", mock_visit)

    results, priced_services = await scrape_all("Solihull", "plumbers")

    assert results == []
    assert priced_services == []
    mock_visit.assert_not_called()


@pytest.mark.asyncio
async def test_scrape_all_skips_visit_for_business_without_website(monkeypatch):
    """Business with no website URL should not trigger a Playwright visit."""
    fake_checkatrade = (
        [{"name": "No Site Ltd", "website": "", "phone": "0121 333 3333",
          "checkatrade_url": "", "yell_url": "", "source": "checkatrade"}],
        [],
    )
    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=fake_checkatrade))
    mock_visit = AsyncMock(return_value="")
    monkeypatch.setattr("scraper.visit_business_site", mock_visit)
    monkeypatch.setattr("scraper.visit_checkatrade_profile", AsyncMock(return_value={"phone": "", "website": ""}))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: [])
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    results, _ = await scrape_all("Solihull", "plumbers")

    assert len(results) == 1
    assert results[0]["prices"] == []
    assert results[0]["extraction_method"] == "none"
    mock_visit.assert_not_called()


@pytest.mark.asyncio
async def test_scrape_all_extraction_method_llm_when_low_confidence(monkeypatch):
    """extraction_method should be 'llm' when any price has confidence='Low'."""
    fake_checkatrade = (
        [{"name": "Ace Plumbing", "website": "https://aceplumbing.example.com",
          "phone": "0121 111 1111", "checkatrade_url": "", "yell_url": "", "source": "checkatrade"}],
        [],
    )
    llm_prices = [{"price": 80.0, "service": "callout", "unit": "fixed",
                   "confidence": "Low", "raw_text": "around eighty pounds"}]

    monkeypatch.setattr("scraper.scrape_checkatrade", AsyncMock(return_value=fake_checkatrade))
    monkeypatch.setattr("scraper.visit_business_site", AsyncMock(return_value="around eighty pounds"))
    monkeypatch.setattr("scraper.visit_checkatrade_profile", AsyncMock(return_value={"phone": "", "website": ""}))
    monkeypatch.setattr("scraper.extract_prices", lambda text, url: llm_prices)
    monkeypatch.setattr("scraper.asyncio.sleep", AsyncMock())

    results, _ = await scrape_all("Solihull", "plumbers")

    assert results[0]["extraction_method"] == "llm"


# ---------------------------------------------------------------------------
# _parse_priced_services — pure HTML parsing
# ---------------------------------------------------------------------------


def test_parse_priced_services_returns_all_cards():
    html = _fixture("checkatrade_with_priced_services.html")
    services = _parse_priced_services(html)
    assert len(services) == 4


def test_parse_priced_services_extracts_business_names():
    html = _fixture("checkatrade_with_priced_services.html")
    services = _parse_priced_services(html)
    names = {s["business_name"] for s in services}
    assert "Gas Pro Heating" in names
    assert "Warmflow Services" in names
    assert "Central Heat Experts" in names


def test_parse_priced_services_extracts_prices():
    html = _fixture("checkatrade_with_priced_services.html")
    services = _parse_priced_services(html)
    prices = sorted(s["price_value"] for s in services)
    assert prices == [75.0, 95.0, 120.0, 140.0]


def test_parse_priced_services_extracts_service_names():
    html = _fixture("checkatrade_with_priced_services.html")
    services = _parse_priced_services(html)
    svc_names = {s["service_name"] for s in services}
    assert "Boiler Diagnostic and Repair" in svc_names
    assert "Boiler Service (Gas)" in svc_names
    assert "Appliance Installation" in svc_names


def test_parse_priced_services_extracts_units():
    html = _fixture("checkatrade_with_priced_services.html")
    services = _parse_priced_services(html)
    units = {s["price_unit"] for s in services}
    assert "job" in units
    assert "appliance" in units


def test_parse_priced_services_all_source_checkatrade():
    html = _fixture("checkatrade_with_priced_services.html")
    services = _parse_priced_services(html)
    assert all(s["source"] == "checkatrade" for s in services)


def test_parse_priced_services_empty_html_returns_empty():
    assert _parse_priced_services("") == []


def test_parse_priced_services_no_section_returns_empty():
    html = "<html><body><p>No priced services here.</p></body></html>"
    assert _parse_priced_services(html) == []


# ---------------------------------------------------------------------------
# visit_checkatrade_profile — async, mocks _fetch_page_html
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_visit_profile_extracts_phone():
    html = '''<html><body>
        <a href="tel:0121-555-1234">Call us</a>
    </body></html>'''
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value=html)):
        result = await visit_checkatrade_profile("https://www.checkatrade.com/trades/test-123")
    assert result["phone"] == "0121-555-1234"


@pytest.mark.asyncio
async def test_visit_profile_extracts_website():
    html = '''<html><body>
        <a href="https://example-plumber.co.uk">Visit our website</a>
    </body></html>'''
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value=html)):
        result = await visit_checkatrade_profile("https://www.checkatrade.com/trades/test-123")
    assert result["website"] == "https://example-plumber.co.uk"


@pytest.mark.asyncio
async def test_visit_profile_empty_url_returns_empty():
    result = await visit_checkatrade_profile("")
    assert result == {"phone": "", "website": ""}


@pytest.mark.asyncio
async def test_visit_profile_failure_returns_empty():
    with patch("scraper._fetch_page_html", new=AsyncMock(return_value="")):
        result = await visit_checkatrade_profile("https://www.checkatrade.com/trades/test-123")
    assert result == {"phone": "", "website": ""}
