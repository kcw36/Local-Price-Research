"""
summary.py — LLM aggregate summary layer.

Called once per job after all businesses have been scraped.
Takes the full list of business results and produces a market rate narrative.

Handles gracefully:
  - n=0 prices: returns "no pricing data found" message
  - n<5 prices: includes low sample size warning in summary
  - No API key: returns plain-text aggregate (min/max/median) instead of LLM prose
  - LLM failure: falls back to plain-text aggregate, logs warning

Output:
  {
    "summary_text": str,        # narrative for the UI header card
    "sample_size": int,         # number of prices found
    "price_range": {            # None if no prices
      "min": float,
      "max": float,
      "median": float
    },
    "low_sample_warning": bool  # True if sample_size < 5
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


def _plain_text_summary(businesses: list[dict]) -> dict:
    """
    Fallback summary when LLM is unavailable or fails.
    Produces human-readable aggregate from raw numbers.
    """
    prices = _collect_prices(businesses)
    n = len(prices)

    if n == 0:
        return {
            "summary_text": (
                "No pricing data was found publicly published by businesses "
                "in this area. Most tradespeople in this sample appear to "
                "quote on request rather than publishing prices online."
            ),
            "sample_size": 0,
            "price_range": None,
            "low_sample_warning": False,
        }

    price_min = min(prices)
    price_max = max(prices)
    price_median = statistics.median(prices)

    warning = ""
    if n < LOW_SAMPLE_THRESHOLD:
        warning = (
            f" Note: only {n} price{'s' if n != 1 else ''} found — "
            "treat this as indicative rather than definitive."
        )

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
    }


def generate_summary(businesses: list[dict]) -> dict:
    """
    Generate market rate summary for the given businesses.

    Uses LLM if ANTHROPIC_API_KEY is set; falls back to plain-text aggregate.
    Never raises — all errors produce the plain-text fallback.
    """
    prices = _collect_prices(businesses)
    n = len(prices)

    # No prices at all — skip LLM, return explicit empty state
    if n == 0:
        return _plain_text_summary(businesses)

    # No API key → plain-text fallback
    if not settings.has_api_key():
        logger.info("No API key configured — using plain-text summary")
        return _plain_text_summary(businesses)

    price_min = min(prices)
    price_max = max(prices)
    price_median = statistics.median(prices)

    low_sample_note = ""
    if n < LOW_SAMPLE_THRESHOLD:
        low_sample_note = (
            f"\nIMPORTANT: Only {n} prices were found. Flag this as a small "
            "sample in your summary and advise the reader to treat it with caution."
        )

    # Build a compact price table for the prompt
    price_rows = []
    for biz in businesses:
        for p in biz.get("prices", []):
            if p.get("price"):
                price_rows.append(
                    f"- {biz['name']}: £{p['price']:.0f} ({p['service']}, "
                    f"{p['unit']}, confidence: {p['confidence']})"
                )

    price_table = "\n".join(price_rows[:30])  # cap at 30 rows to limit tokens

    prompt = f"""You are writing a market rate summary for a UK {businesses[0].get('trade_type', 'tradesperson')}
pricing research report. The target reader is a sole trader in {businesses[0].get('area', 'the local area')}
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
        return _plain_text_summary(businesses)

    return {
        "summary_text": summary_text,
        "sample_size": n,
        "price_range": {
            "min": price_min,
            "max": price_max,
            "median": price_median,
        },
        "low_sample_warning": n < LOW_SAMPLE_THRESHOLD,
    }
