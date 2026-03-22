"""
summary.py — Market rate summary layer.

Called once per job after all businesses have been scraped.
Uses two data sources for pricing:
  1. Priced services — structured data from Checkatrade search page (primary)
  2. Website-extracted prices — regex/LLM extraction from business websites (secondary)

Handles gracefully:
  - n=0 prices: returns empty summary (sample_size=0)
  - n<3 prices: shows individual prices instead of ranges
  - n<5 prices: includes low sample size warning
  - No API key: returns plain-text aggregate instead of LLM prose
  - LLM failure: falls back to plain-text aggregate

Output:
  {
    "summary_text": str,        # narrative for the UI header card
    "sample_size": int,         # total prices found (priced services + website)
    "price_range": {            # None if no prices
      "min": float,
      "max": float,
      "median": float
    },
    "low_sample_warning": bool, # True if sample_size < 5
    "by_service": [...]         # per-service breakdown from priced services
  }
"""

import logging
import statistics
from typing import Optional

from config import settings

logger = logging.getLogger(__name__)

LOW_SAMPLE_THRESHOLD = 5


def _collect_prices(businesses: list[dict]) -> list[float]:
    """Flatten all extracted prices across all businesses into a single list."""
    prices = []
    for biz in businesses:
        for p in biz.get("prices", []):
            val = p.get("price")
            if val and isinstance(val, (int, float)) and val > 0:
                prices.append(float(val))
    return prices


def _summarize_priced_services(priced_services: list[dict]) -> list[dict]:
    """
    Group priced services by service_name and compute per-service statistics.

    Returns list of:
      {service_name, min, max, median, count, unit, prices: [float]}
    """
    by_service: dict[str, list[dict]] = {}
    for svc in priced_services:
        key = svc.get("service_name", "general service")
        by_service.setdefault(key, []).append(svc)

    result = []
    for service_name, items in sorted(by_service.items()):
        prices = [s["price_value"] for s in items if s.get("price_value", 0) > 0]
        if not prices:
            continue

        unit = items[0].get("price_unit", "")
        entry = {
            "service_name": service_name,
            "count": len(prices),
            "unit": unit,
            "prices": prices,
        }

        if len(prices) >= 3:
            entry["min"] = min(prices)
            entry["max"] = max(prices)
            entry["median"] = statistics.median(prices)
        else:
            # Too few data points for meaningful range — show individual prices
            entry["min"] = min(prices)
            entry["max"] = max(prices)
            entry["median"] = statistics.median(prices)

        result.append(entry)

    return result


def _plain_text_summary(
    businesses: list[dict], priced_services: list[dict] | None = None
) -> dict:
    """
    Fallback summary when LLM is unavailable or fails.
    Combines priced services + website-extracted prices into a summary.
    """
    website_prices = _collect_prices(businesses)
    service_prices = [s.get("price_value", 0) for s in (priced_services or []) if s.get("price_value", 0) > 0]
    all_prices = service_prices + website_prices
    n = len(all_prices)

    by_service = _summarize_priced_services(priced_services or [])

    if n == 0:
        return {
            "summary_text": "",
            "sample_size": 0,
            "price_range": None,
            "low_sample_warning": False,
            "by_service": [],
        }

    price_min = min(all_prices)
    price_max = max(all_prices)
    price_median = statistics.median(all_prices)

    warning = ""
    if n < LOW_SAMPLE_THRESHOLD:
        warning = (
            f" Note: only {n} price{'s' if n != 1 else ''} found — "
            "treat this as indicative rather than definitive."
        )

    # Build summary text from priced services if available
    if by_service:
        parts = []
        for svc in by_service:
            if svc["count"] >= 3:
                parts.append(
                    f"{svc['service_name']}: £{svc['min']:.0f}–£{svc['max']:.0f}"
                    f" (median £{svc['median']:.0f}, {svc['count']} quotes)"
                )
            else:
                price_strs = [f"£{p:.0f}" for p in svc["prices"]]
                parts.append(f"{svc['service_name']}: {', '.join(price_strs)}")

        summary = (
            f"Based on {n} price{'s' if n != 1 else ''} from "
            f"{len(businesses)} businesses: " + "; ".join(parts) + f".{warning}"
        )
    else:
        summary = (
            f"Based on {n} price{'s' if n != 1 else ''} found across "
            f"{len(businesses)} businesses: prices range from "
            f"£{price_min:.0f} to £{price_max:.0f}, "
            f"with a median of £{price_median:.0f}.{warning}"
        )

    return {
        "summary_text": summary,
        "sample_size": n,
        "price_range": {
            "min": price_min,
            "max": price_max,
            "median": price_median,
        },
        "low_sample_warning": n < LOW_SAMPLE_THRESHOLD,
        "by_service": by_service,
    }


def generate_summary(
    businesses: list[dict], priced_services: list[dict] | None = None
) -> dict:
    """
    Generate market rate summary for the given businesses.

    Combines priced services (structured Checkatrade data) with website-extracted
    prices. Uses LLM if ANTHROPIC_API_KEY is set; falls back to plain-text aggregate.
    Never raises — all errors produce the plain-text fallback.
    """
    website_prices = _collect_prices(businesses)
    service_prices = [
        s.get("price_value", 0) for s in (priced_services or [])
        if s.get("price_value", 0) > 0
    ]
    all_prices = service_prices + website_prices
    n = len(all_prices)

    # No prices at all — return empty summary
    if n == 0:
        return _plain_text_summary(businesses, priced_services)

    # No API key → plain-text fallback
    if not settings.has_api_key():
        logger.info("No API key configured — using plain-text summary")
        return _plain_text_summary(businesses, priced_services)

    price_min = min(all_prices)
    price_max = max(all_prices)
    price_median = statistics.median(all_prices)
    by_service = _summarize_priced_services(priced_services or [])

    low_sample_note = ""
    if n < LOW_SAMPLE_THRESHOLD:
        low_sample_note = (
            f"\nIMPORTANT: Only {n} prices were found. Flag this as a small "
            "sample in your summary and advise the reader to treat it with caution."
        )

    # Build a compact price table for the prompt
    price_rows = []

    # Priced services first (higher quality data)
    for svc in (priced_services or []):
        price_rows.append(
            f"- {svc.get('business_name', 'Unknown')}: £{svc['price_value']:.0f} "
            f"({svc.get('service_name', 'general')}, {svc.get('price_unit', '')}, "
            f"source: Checkatrade priced services)"
        )

    # Website-extracted prices
    for biz in businesses:
        for p in biz.get("prices", []):
            if p.get("price"):
                price_rows.append(
                    f"- {biz['name']}: £{p['price']:.0f} ({p['service']}, "
                    f"{p['unit']}, confidence: {p['confidence']})"
                )

    price_table = "\n".join(price_rows[:30])  # cap at 30 rows to limit tokens

    prompt = f"""You are writing a market rate summary for a UK {businesses[0].get('trade_type', 'tradesperson') if businesses else 'tradesperson'}
pricing research report. The target reader is a sole trader in {businesses[0].get('area', 'the local area') if businesses else 'the local area'}
who wants to know what competitors charge so they can price their own services competitively.

Here are the prices found:
{price_table}

Statistics:
- Sample size: {n} prices from {len(businesses)} businesses
- Range: £{price_min:.0f} – £{price_max:.0f}
- Median: £{price_median:.0f}
{low_sample_note}

Write a 2-3 sentence market rate summary. Be specific (use the actual numbers).
Be direct — the reader is a professional, not a consumer.
Do NOT invent prices not in the data. Do NOT give business advice beyond what the data shows."""

    try:
        from config import get_anthropic_client

        client = get_anthropic_client()
        response = client.messages.create(
            model=settings.model,
            max_tokens=256,
            messages=[{"role": "user", "content": prompt}],
        )
        summary_text = response.content[0].text.strip()

    except Exception as e:
        logger.warning("LLM summary failed (%s) — using plain-text fallback", e)
        return _plain_text_summary(businesses, priced_services)

    return {
        "summary_text": summary_text,
        "sample_size": n,
        "price_range": {
            "min": price_min,
            "max": price_max,
            "median": price_median,
        },
        "low_sample_warning": n < LOW_SAMPLE_THRESHOLD,
        "by_service": by_service,
    }
