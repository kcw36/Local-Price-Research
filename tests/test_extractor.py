"""
Tests for extractor.py — regex price extraction and LLM fallback.

All tests use static text — no network calls, no API calls.
"""

import pytest

from extractor import extract_prices, regex_pass, llm_pass


# ---------------------------------------------------------------------------
# regex_pass — happy path
# ---------------------------------------------------------------------------


def test_regex_finds_simple_price():
    text = "Our call-out charge is £60 per hour for labour."
    results = regex_pass(text)
    assert len(results) >= 1
    assert any(r["price"] == 60.0 for r in results)


def test_regex_finds_from_price():
    text = "Boiler service from £85. Call for a quote on larger jobs."
    results = regex_pass(text)
    assert len(results) >= 1
    assert any(r["price"] == 85.0 for r in results)


def test_regex_finds_multiple_prices():
    text = (
        "Call-out charge: £60 (first hour)\n"
        "Hourly rate: £75 per hour after first hour\n"
        "Boiler service: from £85\n"
        "Annual gas safety certificate: £70 fixed"
    )
    results = regex_pass(text)
    prices = [r["price"] for r in results]
    assert 60.0 in prices
    assert 75.0 in prices
    assert 85.0 in prices


def test_regex_confidence_high_for_pricing_url():
    text = "Labour rate: £80 per hour"
    results = regex_pass(text, url="https://example.com/prices")
    assert all(r["confidence"] == "High" for r in results)


def test_regex_confidence_med_for_generic_url():
    text = "Labour rate: £80 per hour"
    results = regex_pass(text, url="https://example.com/about")
    assert all(r["confidence"] == "Med" for r in results)


def test_regex_with_fixture_business_with_price():
    """Full fixture: dedicated pricing page should return multiple prices."""
    import os

    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "business_with_price.html")
    with open(fixture_path, encoding="utf-8") as f:
        html = f.read()

    from bs4 import BeautifulSoup
    text = BeautifulSoup(html, "html.parser").get_text(separator=" ")
    results = regex_pass(text, url="https://example.com/prices")

    assert len(results) >= 3, f"Expected ≥3 prices, got {len(results)}: {results}"
    prices = [r["price"] for r in results]
    assert 60.0 in prices or 75.0 in prices or 85.0 in prices


# ---------------------------------------------------------------------------
# regex_pass — edge cases / empty states
# ---------------------------------------------------------------------------


def test_regex_returns_empty_when_no_pound_sign():
    results = regex_pass("We offer competitive rates. Call for a quote.")
    assert results == []


def test_regex_returns_empty_when_no_pricing_context():
    # Has £ but no pricing context words — stray currency mention
    results = regex_pass("We are a £5m turnover company.")
    assert results == []


def test_regex_ignores_zero_price():
    text = "Call-out charge: £0 per visit"
    results = regex_pass(text)
    assert all(r["price"] > 0 for r in results)


def test_regex_ignores_implausibly_high_price():
    text = "Boiler replacement: £99999"
    results = regex_pass(text)
    assert all(r["price"] <= 9999 for r in results)


def test_regex_deduplicates_same_price():
    text = "£75 per hour. Hourly rate is £75. Our rate is £75/hr."
    results = regex_pass(text)
    count_75 = sum(1 for r in results if r["price"] == 75.0)
    assert count_75 == 1, f"Expected deduplication, got {count_75} entries for £75"


def test_regex_empty_text():
    assert regex_pass("") == []


def test_regex_fixture_no_price():
    """Fixture with no prices should return empty list."""
    import os

    fixture_path = os.path.join(os.path.dirname(__file__), "fixtures", "business_no_price.html")
    with open(fixture_path, encoding="utf-8") as f:
        html = f.read()

    from bs4 import BeautifulSoup
    text = BeautifulSoup(html, "html.parser").get_text(separator=" ")
    results = regex_pass(text)
    assert results == []


# ---------------------------------------------------------------------------
# extract_prices — orchestration
# ---------------------------------------------------------------------------


def test_extract_prices_returns_regex_results_when_found():
    text = "Labour rate: £80 per hour, call-out charge £60"
    results = extract_prices(text)
    assert len(results) >= 1
    assert all(r["confidence"] in ("High", "Med", "Low") for r in results)


def test_extract_prices_returns_empty_for_no_price_page():
    text = "We offer a range of plumbing services. Call us for a quote."
    results = extract_prices(text)
    assert results == []


def test_extract_prices_short_text_skips_llm():
    """Text under 200 chars with no prices: no LLM call attempted."""
    text = "Call for quote."  # short, no prices
    # Should return [] without attempting LLM (no API key needed)
    results = extract_prices(text)
    assert results == []


# ---------------------------------------------------------------------------
# llm_pass — disabled by default
# ---------------------------------------------------------------------------


def test_llm_pass_disabled_by_default(monkeypatch):
    """LLM_FALLBACK_ENABLED defaults to false — llm_pass should return []."""
    import config
    monkeypatch.setattr(config.settings, "llm_fallback_enabled", False)

    results = llm_pass("Hourly rate £75 per hour. Call for a quote.", url="")
    assert results == []


def test_llm_pass_skips_when_no_api_key(monkeypatch):
    """Even if enabled, llm_pass should return [] when no API key."""
    import config
    monkeypatch.setattr(config.settings, "llm_fallback_enabled", True)
    monkeypatch.setattr(config.settings, "anthropic_api_key", "")

    results = llm_pass("Call-out charge £60, hourly rate £75.", url="")
    assert results == []


def test_llm_pass_handles_invalid_json_gracefully(monkeypatch):
    """LLM returns garbage JSON — should return [] without raising."""
    import config

    monkeypatch.setattr(config.settings, "llm_fallback_enabled", True)
    monkeypatch.setattr(config.settings, "anthropic_api_key", "test-key")

    # Mock the anthropic client to return invalid JSON
    class MockContent:
        text = "this is not json at all {{"

    class MockResponse:
        content = [MockContent()]

    class MockMessages:
        def create(self, **kwargs):
            return MockResponse()

    class MockClient:
        messages = MockMessages()

    monkeypatch.setattr(config, "_anthropic_client", MockClient())

    results = llm_pass("Some page text longer than 200 chars " + "x" * 200, url="")
    assert results == []


def test_llm_pass_handles_llm_exception_gracefully(monkeypatch):
    """LLM call raises exception — should return [] without raising."""
    import config

    monkeypatch.setattr(config.settings, "llm_fallback_enabled", True)
    monkeypatch.setattr(config.settings, "anthropic_api_key", "test-key")

    class MockMessages:
        def create(self, **kwargs):
            raise ConnectionError("API unreachable")

    class MockClient:
        messages = MockMessages()

    monkeypatch.setattr(config, "_anthropic_client", MockClient())

    results = llm_pass("Some page text longer than 200 chars " + "x" * 200, url="")
    assert results == []


def test_llm_pass_marks_confidence_low(monkeypatch):
    """Prices extracted by LLM get confidence=Low."""
    import config, json

    monkeypatch.setattr(config.settings, "llm_fallback_enabled", True)
    monkeypatch.setattr(config.settings, "anthropic_api_key", "test-key")

    class MockContent:
        text = json.dumps([
            {"service": "boiler service", "price": 85, "unit": "fixed", "raw_text": "£85 for a boiler service"}
        ])

    class MockResponse:
        content = [MockContent()]

    class MockMessages:
        def create(self, **kwargs):
            return MockResponse()

    class MockClient:
        messages = MockMessages()

    monkeypatch.setattr(config, "_anthropic_client", MockClient())

    results = llm_pass("Boiler service available. Contact us for pricing. " + "x" * 200, url="")
    assert len(results) == 1
    assert results[0]["confidence"] == "Low"
    assert results[0]["price"] == 85.0
