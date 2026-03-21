"""
extractor.py — Price extraction and structured parsing.

Pipeline per page:
  1. regex_pass()  — fast, zero cost, covers majority of cases
  2. llm_pass()    — fallback only when regex finds nothing AND text > 200 chars
                     AND LLM_FALLBACK_ENABLED=true

Confidence scoring:
  High — found on a dedicated prices/rates page (URL contains 'price', 'rate', 'cost')
  Med  — regex matched on general page content
  Low  — LLM extracted (model may hallucinate on ambiguous text)

Output schema per price:
  {
    "service":    str,   # e.g. "boiler service", "call-out charge"
    "price":      float, # e.g. 75.0
    "unit":       str,   # e.g. "per hour", "fixed", "from"
    "raw_text":   str,   # original matched text for auditability
    "confidence": str    # "High" | "Med" | "Low"
  }
"""

import json
import logging
import re
from typing import Optional

from config import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Regex patterns
# ---------------------------------------------------------------------------

# Matches: £75, £75.00, £75/hr, £75 per hour, from £75, £75-£100
_PRICE_PATTERN = re.compile(
    r"""
    (?:from\s+)?            # optional "from"
    £\s*(\d+(?:\.\d{1,2})?) # £ followed by digits (e.g. £75, £75.00)
    (?:
        \s*[-–]\s*£\s*(\d+(?:\.\d{1,2})?)  # optional range: £75-£100
    )?
    (?:
        \s*/?\s*             # optional separator
        (per\s+hour|per\s+hr|/hr|/hour|p/h|
         per\s+day|per\s+job|fixed|call.?out|
         labour|inc\.?\s*vat|ex\.?\s*vat)  # optional unit
    )?
    """,
    re.VERBOSE | re.IGNORECASE,
)

# Context words that suggest a pricing section — higher signal than stray £ signs
_PRICING_CONTEXT = re.compile(
    r"\b(price|rate|charge|cost|fee|quote|labour|call.?out|hourly|fixed|from\s+£)\b",
    re.IGNORECASE,
)

# URL patterns that suggest a dedicated pricing page
_PRICING_URL = re.compile(
    r"/(price|pricing|rates|costs?|fees?|charges?)", re.IGNORECASE
)

# Common service types to help label extracted prices
_SERVICE_PATTERNS = [
    (re.compile(r"boiler\s+(service|repair|install)", re.I), "boiler {0}"),
    (re.compile(r"call.?out", re.I), "call-out charge"),
    (re.compile(r"hourly\s+rate|labour\s+rate|per\s+hour", re.I), "hourly rate"),
    (re.compile(r"annual\s+service|gas\s+service", re.I), "annual gas service"),
    (re.compile(r"power\s+flush", re.I), "power flush"),
    (re.compile(r"landlord\s+(cert|gas\s+safe)", re.I), "landlord gas certificate"),
    (re.compile(r"leak|pipe|drain", re.I), "general plumbing"),
]


def _infer_service(surrounding_text: str) -> str:
    """Guess service label from text near the price."""
    for pattern, label_template in _SERVICE_PATTERNS:
        m = pattern.search(surrounding_text)
        if m:
            return label_template.format(m.group(1).lower() if "{0}" in label_template else "")
    return "general work"


def _infer_unit(match_text: str, raw_unit: Optional[str]) -> str:
    if raw_unit:
        text = raw_unit.lower().strip()
        if any(k in text for k in ("hour", "hr", "p/h")):
            return "per hour"
        if "day" in text:
            return "per day"
        if "job" in text:
            return "per job"
        if "fixed" in text:
            return "fixed"
        if any(k in text for k in ("vat", "labour")):
            return text
    if "from" in match_text.lower():
        return "from"
    return "unknown"


def regex_pass(page_text: str, url: str = "") -> list[dict]:
    """
    Extract prices using regex. Returns list of structured price dicts.
    Returns empty list if no prices found.
    """
    # Quick bail: no £ sign means definitely no prices
    if "£" not in page_text:
        return []

    # Check if there's pricing context around the £ signs
    if not _PRICING_CONTEXT.search(page_text):
        return []

    is_pricing_page = bool(_PRICING_URL.search(url))

    results = []
    for m in _PRICE_PATTERN.finditer(page_text):
        low_price = float(m.group(1))

        # Sanity check: ignore obviously wrong values (£0, £9999+)
        if low_price <= 0 or low_price > 9999:
            continue

        # Get ~100 chars of surrounding context to infer service type
        start = max(0, m.start() - 80)
        end = min(len(page_text), m.end() + 80)
        context = page_text[start:end]

        raw_unit = m.group(3)
        service = _infer_service(context)
        unit = _infer_unit(m.group(0), raw_unit)

        confidence = "High" if is_pricing_page else "Med"

        results.append(
            {
                "service": service,
                "price": low_price,
                "unit": unit,
                "raw_text": m.group(0).strip(),
                "confidence": confidence,
            }
        )

    # Deduplicate: same service + price → keep first occurrence
    seen = set()
    deduped = []
    for r in results:
        key = (r["service"], r["price"])
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    return deduped


def llm_pass(page_text: str, url: str = "") -> list[dict]:
    """
    LLM extraction fallback. Only called when:
      - regex found nothing
      - page_text > 200 chars (worth sending)
      - LLM_FALLBACK_ENABLED=true
      - ANTHROPIC_API_KEY is set

    Returns list of structured price dicts with confidence="Low".
    Returns empty list on any error (never raises).
    """
    if not settings.llm_fallback_enabled:
        return []

    if not settings.has_api_key():
        logger.warning("llm_pass called but ANTHROPIC_API_KEY not set — skipping")
        return []

    try:
        from config import get_anthropic_client

        client = get_anthropic_client()

        # Truncate to avoid excessive token usage (~1500 words max)
        truncated = page_text[:6000] if len(page_text) > 6000 else page_text

        prompt = f"""You are extracting pricing information from a UK tradesperson's website.

Page URL: {url}
Page text (truncated):
---
{truncated}
---

Extract any specific prices mentioned. Return a JSON array. Each item must have:
- "service": what the price is for (e.g. "boiler service", "hourly rate")
- "price": numeric value in pounds sterling (number, not string)
- "unit": "per hour", "fixed", "from", "per job", or "unknown"
- "raw_text": the exact text you found the price in

Rules:
- Only extract prices that are explicitly stated (e.g. "£75 per hour", "from £150")
- Do NOT invent or estimate prices
- Do NOT include prices that are clearly for products, not labour
- If no prices found, return empty array []

Return ONLY the JSON array, no explanation."""

        response = client.messages.create(
            model=settings.model,
            max_tokens=512,
            messages=[{"role": "user", "content": prompt}],
        )

        raw = response.content[0].text.strip()

        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = re.sub(r"^```(?:json)?\n?", "", raw)
            raw = re.sub(r"\n?```$", "", raw)

        extracted = json.loads(raw)

        if not isinstance(extracted, list):
            logger.warning("LLM returned non-list: %s", raw[:200])
            return []

        results = []
        for item in extracted:
            if not isinstance(item, dict):
                continue
            price_val = item.get("price")
            if price_val is None:
                continue
            try:
                price_float = float(price_val)
            except (TypeError, ValueError):
                continue
            if price_float <= 0 or price_float > 9999:
                continue
            results.append(
                {
                    "service": str(item.get("service", "general work")),
                    "price": price_float,
                    "unit": str(item.get("unit", "unknown")),
                    "raw_text": str(item.get("raw_text", "")),
                    "confidence": "Low",
                }
            )
        return results

    except json.JSONDecodeError as e:
        logger.warning("LLM returned invalid JSON: %s", e)
        return []
    except Exception as e:
        logger.warning("llm_pass failed: %s", e)
        return []


def extract_prices(page_text: str, url: str = "") -> list[dict]:
    """
    Main extraction entry point.
    Tries regex first; falls back to LLM if configured and regex finds nothing.
    """
    results = regex_pass(page_text, url)

    if not results and len(page_text) > 200:
        results = llm_pass(page_text, url)

    return results
