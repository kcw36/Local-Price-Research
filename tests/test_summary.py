"""
Tests for summary.py — market rate summary generation.

LLM calls are mocked. No real API calls.
"""

import pytest

from summary import generate_summary, _collect_prices, _plain_text_summary, _summarize_priced_services


# ---------------------------------------------------------------------------
# Helper builders
# ---------------------------------------------------------------------------


def _make_biz(name: str, prices: list[float]) -> dict:
    return {
        "name": name,
        "area": "Solihull",
        "trade_type": "plumbers",
        "prices": [
            {"price": p, "service": "general work", "unit": "per hour", "confidence": "Med"}
            for p in prices
        ],
        "extraction_method": "regex" if prices else "none",
        "source_url": "https://example.com",
    }


# ---------------------------------------------------------------------------
# _collect_prices
# ---------------------------------------------------------------------------


def test_collect_prices_flattens_all_businesses():
    businesses = [
        _make_biz("A", [60.0, 75.0]),
        _make_biz("B", [80.0]),
        _make_biz("C", []),
    ]
    prices = _collect_prices(businesses)
    assert sorted(prices) == [60.0, 75.0, 80.0]


def test_collect_prices_empty():
    assert _collect_prices([]) == []


def test_collect_prices_all_no_price():
    businesses = [_make_biz("A", []), _make_biz("B", [])]
    assert _collect_prices(businesses) == []


# ---------------------------------------------------------------------------
# _plain_text_summary
# ---------------------------------------------------------------------------


def test_plain_text_summary_zero_prices():
    businesses = [_make_biz("A", []), _make_biz("B", [])]
    result = _plain_text_summary(businesses)
    assert result["sample_size"] == 0
    assert result["price_range"] is None
    assert result["summary_text"] == ""
    assert result["low_sample_warning"] is False


def test_plain_text_summary_with_prices():
    businesses = [_make_biz("A", [60.0, 80.0, 75.0])]
    result = _plain_text_summary(businesses)
    assert result["sample_size"] == 3
    assert result["price_range"]["min"] == 60.0
    assert result["price_range"]["max"] == 80.0
    assert result["price_range"]["median"] == 75.0
    assert "£60" in result["summary_text"]
    assert "£80" in result["summary_text"]


def test_plain_text_summary_low_sample_warning():
    businesses = [_make_biz("A", [75.0])]
    result = _plain_text_summary(businesses)
    assert result["low_sample_warning"] is True
    assert "small sample" in result["summary_text"].lower() or "indicative" in result["summary_text"].lower()


def test_plain_text_summary_no_low_sample_warning_when_enough():
    businesses = [_make_biz("A", [60.0, 65.0, 70.0, 75.0, 80.0])]
    result = _plain_text_summary(businesses)
    assert result["low_sample_warning"] is False


# ---------------------------------------------------------------------------
# generate_summary — no API key (plain-text fallback)
# ---------------------------------------------------------------------------


def test_generate_summary_falls_back_when_no_api_key(monkeypatch):
    import config
    monkeypatch.setattr(config.settings, "anthropic_api_key", "")

    businesses = [_make_biz("A", [60.0, 75.0, 80.0, 85.0, 90.0])]
    result = generate_summary(businesses)

    assert result["sample_size"] == 5
    assert result["price_range"] is not None
    assert "£60" in result["summary_text"] or "60" in result["summary_text"]


def test_generate_summary_zero_prices_no_api_needed(monkeypatch):
    import config
    monkeypatch.setattr(config.settings, "anthropic_api_key", "")

    businesses = [_make_biz("A", []), _make_biz("B", [])]
    result = generate_summary(businesses)

    assert result["sample_size"] == 0
    assert result["price_range"] is None


# ---------------------------------------------------------------------------
# generate_summary — with mock LLM
# ---------------------------------------------------------------------------


def test_generate_summary_uses_llm_when_api_key_set(monkeypatch):
    import config

    monkeypatch.setattr(config.settings, "anthropic_api_key", "test-key")

    class MockContent:
        text = "Solihull plumbers charge £60–£90/hr, with a median of £75."

    class MockResponse:
        content = [MockContent()]

    class MockMessages:
        def create(self, **kwargs):
            return MockResponse()

    class MockClient:
        messages = MockMessages()

    monkeypatch.setattr(config, "_anthropic_client", MockClient())

    businesses = [_make_biz("A", [60.0, 75.0, 80.0, 85.0, 90.0])]
    result = generate_summary(businesses)

    assert "75" in result["summary_text"] or "plumbers" in result["summary_text"].lower()
    assert result["sample_size"] == 5


def test_generate_summary_falls_back_when_llm_fails(monkeypatch):
    import config

    monkeypatch.setattr(config.settings, "anthropic_api_key", "test-key")

    class MockMessages:
        def create(self, **kwargs):
            raise ConnectionError("API down")

    class MockClient:
        messages = MockMessages()

    monkeypatch.setattr(config, "_anthropic_client", MockClient())

    businesses = [_make_biz("A", [60.0, 75.0, 80.0, 85.0, 90.0])]
    result = generate_summary(businesses)

    # Should fall back to plain-text, not raise
    assert result["sample_size"] == 5
    assert result["price_range"] is not None


# ---------------------------------------------------------------------------
# _summarize_priced_services
# ---------------------------------------------------------------------------


def _make_svc(biz: str, service: str, price: float, unit: str = "job") -> dict:
    return {
        "business_name": biz,
        "service_name": service,
        "price_value": price,
        "price_unit": unit,
        "source": "checkatrade",
    }


def test_summarize_groups_by_service():
    services = [
        _make_svc("A", "Boiler Repair", 95.0),
        _make_svc("B", "Boiler Repair", 120.0),
        _make_svc("C", "Boiler Service", 75.0),
    ]
    result = _summarize_priced_services(services)
    names = {r["service_name"] for r in result}
    assert "Boiler Repair" in names
    assert "Boiler Service" in names


def test_summarize_computes_stats():
    services = [
        _make_svc("A", "Boiler Repair", 80.0),
        _make_svc("B", "Boiler Repair", 100.0),
        _make_svc("C", "Boiler Repair", 120.0),
    ]
    result = _summarize_priced_services(services)
    repair = [r for r in result if r["service_name"] == "Boiler Repair"][0]
    assert repair["min"] == 80.0
    assert repair["max"] == 120.0
    assert repair["median"] == 100.0
    assert repair["count"] == 3


def test_summarize_empty_returns_empty():
    assert _summarize_priced_services([]) == []


def test_summarize_skips_zero_prices():
    services = [_make_svc("A", "Repair", 0.0)]
    result = _summarize_priced_services(services)
    assert result == []


# ---------------------------------------------------------------------------
# _plain_text_summary with priced services
# ---------------------------------------------------------------------------


def test_plain_text_summary_with_priced_services():
    businesses = [_make_biz("A", [])]
    priced = [
        _make_svc("A", "Boiler Repair", 80.0),
        _make_svc("B", "Boiler Repair", 100.0),
        _make_svc("C", "Boiler Repair", 120.0),
    ]
    result = _plain_text_summary(businesses, priced)
    assert result["sample_size"] == 3
    assert result["price_range"]["min"] == 80.0
    assert result["price_range"]["max"] == 120.0
    assert len(result["by_service"]) == 1
    assert "Boiler Repair" in result["summary_text"]


def test_plain_text_summary_combines_both_sources():
    businesses = [_make_biz("A", [60.0])]
    priced = [_make_svc("B", "Repair", 100.0)]
    result = _plain_text_summary(businesses, priced)
    assert result["sample_size"] == 2
    assert result["price_range"]["min"] == 60.0
    assert result["price_range"]["max"] == 100.0


def test_generate_summary_with_priced_services_no_api_key(monkeypatch):
    import config
    monkeypatch.setattr(config.settings, "anthropic_api_key", "")

    businesses = [_make_biz("A", [])]
    priced = [
        _make_svc("A", "Service", 75.0),
        _make_svc("B", "Service", 85.0),
        _make_svc("C", "Service", 95.0),
    ]
    result = generate_summary(businesses, priced)
    assert result["sample_size"] == 3
    assert result["by_service"] is not None
    assert len(result["by_service"]) == 1
