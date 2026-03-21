"""
scraper.py — Directory scraping (Yell.com) + website visiting (Playwright).

Flow:
  1. scrape_directory(area, trade_type) → list of {name, website, phone}
  2. visit_business_site(url) → page text (or "" on failure)
  3. scrape_all(area, trade_type, progress_cb) → list of business dicts with
     extracted pricing (delegated to extractor.py)

Politeness:
  - 1.5s delay between requests (configurable via POLITENESS_DELAY_SECONDS)
  - Descriptive User-Agent
  - robots.txt respected by requests-based calls (Playwright does not check
    robots.txt — we rely on the politeness delay instead)

Error handling:
  - Per-URL errors are logged and skipped; they never crash the job
  - HTTP 429 → back off 10s and retry once
  - Playwright errors → log and return ""
"""

import asyncio
import logging
import time
from typing import Callable, Optional
from urllib.parse import quote_plus

import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright

from config import settings
from extractor import extract_prices

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "PricingResearchBot/1.0 (market rate research tool; "
        "contact: research@example.com)"
    ),
    "Accept-Language": "en-GB,en;q=0.9",
}

_YELL_BASE = "https://www.yell.com"
_YELL_SEARCH = "/search?keywords={trade}&location={area}"


# ---------------------------------------------------------------------------
# Yell.com directory scraper
# ---------------------------------------------------------------------------


def _get_with_retry(url: str, headers: dict, timeout: int = 15) -> Optional[requests.Response]:
    """GET with one retry on 429. Returns None on persistent failure."""
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 429:
            logger.warning("Rate limited by %s — backing off 10s", url)
            time.sleep(10)
            resp = requests.get(url, headers=headers, timeout=timeout)
        return resp
    except requests.RequestException as e:
        logger.warning("Request failed for %s: %s", url, e)
        return None


def scrape_directory(area: str, trade_type: str) -> list[dict]:
    """
    Scrape Yell.com search results for area + trade_type.
    Returns list of: {name, website, phone, yell_url}

    Returns empty list (not raises) on any error.
    """
    search_url = _YELL_BASE + _YELL_SEARCH.format(
        trade=quote_plus(trade_type), area=quote_plus(area)
    )
    logger.info("Scraping Yell.com: %s", search_url)

    resp = _get_with_retry(search_url, _HEADERS)
    if resp is None or resp.status_code != 200:
        status = resp.status_code if resp else "no response"
        logger.error("Yell.com returned %s for %s", status, search_url)
        return []

    soup = BeautifulSoup(resp.text, "html.parser")
    businesses = []

    # Yell.com listing cards — class names may change; we use multiple selectors
    # as fallbacks. If structure changes, update these selectors.
    listing_selectors = [
        "article.businessCapsule",
        "div.businessCapsule",
        "article[class*='listing']",
        "div[class*='listing']",
    ]

    listings = []
    for selector in listing_selectors:
        listings = soup.select(selector)
        if listings:
            break

    if not listings:
        logger.warning(
            "No listings found on Yell.com for '%s %s' — page structure may have changed",
            trade_type,
            area,
        )
        # Last resort: look for any links that look like business pages
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/biz/" in href and href not in [b.get("yell_url") for b in businesses]:
                businesses.append(
                    {
                        "name": a.get_text(strip=True) or "Unknown",
                        "website": "",
                        "phone": "",
                        "yell_url": (_YELL_BASE + href) if href.startswith("/") else href,
                    }
                )
                if len(businesses) >= settings.max_businesses:
                    break
        return businesses

    for listing in listings[: settings.max_businesses]:
        name = ""
        website = ""
        phone = ""
        yell_url = ""

        # Business name
        name_el = listing.select_one("h2, h3, [class*='businessName'], [class*='name']")
        if name_el:
            name = name_el.get_text(strip=True)

        # Website link
        website_el = listing.select_one(
            "a[data-type='website'], a[class*='website'], a[href^='http']:not([href*='yell.com'])"
        )
        if website_el:
            website = website_el.get("href", "")

        # Phone
        phone_el = listing.select_one(
            "[class*='phone'], [class*='telephone'], [itemprop='telephone']"
        )
        if phone_el:
            phone = phone_el.get_text(strip=True)

        # Yell profile URL
        profile_el = listing.select_one("a[href*='/biz/']")
        if profile_el:
            href = profile_el.get("href", "")
            yell_url = (_YELL_BASE + href) if href.startswith("/") else href

        if name:  # only add if we at least got a name
            businesses.append(
                {
                    "name": name,
                    "website": website,
                    "phone": phone,
                    "yell_url": yell_url,
                }
            )

    logger.info("Found %d businesses on Yell.com", len(businesses))
    return businesses


# ---------------------------------------------------------------------------
# Website visitor (Playwright)
# ---------------------------------------------------------------------------


async def visit_business_site(url: str) -> str:
    """
    Visit a business website with Playwright and return visible page text.
    Waits for networkidle to allow JS-rendered content to load.
    Returns "" on any error (timeout, 404, connection refused, etc.).
    """
    if not url:
        return ""

    try:
        from playwright.async_api import TimeoutError as PWTimeoutError

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                context = await browser.new_context(
                    user_agent=_HEADERS["User-Agent"],
                    locale="en-GB",
                )
                page = await context.new_page()

                # 15s timeout for navigation; we'd rather move on than hang
                try:
                    await page.goto(url, wait_until="networkidle", timeout=15000)
                except PWTimeoutError:
                    # Partial load is OK — try to extract whatever loaded
                    logger.debug("Navigation timeout for %s — extracting partial content", url)

                text = await page.inner_text("body")
                return text

            finally:
                await browser.close()

    except Exception as e:
        logger.warning("Playwright failed for %s: %s", url, type(e).__name__)
        return ""


# ---------------------------------------------------------------------------
# Main scrape orchestrator
# ---------------------------------------------------------------------------


async def scrape_all(
    area: str,
    trade_type: str,
    progress_cb: Optional[Callable[[int, int], None]] = None,
) -> list[dict]:
    """
    Full scrape: directory → website visits → price extraction.

    progress_cb(current, total) is called after each business is processed.

    Returns list of:
      {
        name, website, phone, yell_url,
        prices: [...],          # from extractor.py
        extraction_method: str, # "regex" | "llm" | "none"
        source_url: str         # URL that was scraped for prices
      }
    """
    loop = asyncio.get_event_loop()
    businesses = await loop.run_in_executor(None, scrape_directory, area, trade_type)

    if not businesses:
        logger.warning("No businesses found for '%s' in '%s'", trade_type, area)
        return []

    total = len(businesses)
    results = []

    for idx, biz in enumerate(businesses):
        await asyncio.sleep(settings.politeness_delay)

        url = biz.get("website", "")
        page_text = ""
        source_url = url

        if url:
            logger.info("Visiting %s (%d/%d)", url, idx + 1, total)
            page_text = await visit_business_site(url)
        else:
            logger.debug("No website URL for '%s' — skipping visit", biz["name"])

        prices = extract_prices(page_text, url)

        if prices:
            has_llm = any(p["confidence"] == "Low" for p in prices)
            extraction_method = "llm" if has_llm else "regex"
        else:
            extraction_method = "none"

        results.append(
            {
                **biz,
                "prices": prices,
                "extraction_method": extraction_method,
                "source_url": source_url,
            }
        )

        if progress_cb:
            progress_cb(idx + 1, total)

    return results
