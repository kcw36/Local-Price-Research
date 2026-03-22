"""
scraper.py — Directory scraping (Yell.com, Checkatrade) + website visiting (Playwright).

Flow:
  1. scrape_directory(area, trade_type)   → list of {name, website, phone, yell_url, source}
  2. scrape_checkatrade(area, trade_type) → list of {name, website, phone, checkatrade_url, source}
  3. visit_business_site(url)             → page text (or "" on failure)
  4. scrape_all(area, trade_type, progress_cb) → merged, deduped businesses with extracted pricing

Both directory scrapers use Playwright to bypass Cloudflare JS challenges.
The parsing logic is extracted into pure functions (_parse_yell_html,
_parse_checkatrade_html) so tests can feed static HTML fixtures directly.

Politeness:
  - 1.5s delay between website visits (configurable via POLITENESS_DELAY_SECONDS)
  - Real browser User-Agent (required for Cloudflare)

Error handling:
  - Per-URL errors are logged and skipped; they never crash the job
  - Playwright errors → log and return ""
"""

import asyncio
import logging
from typing import Callable, Optional
from urllib.parse import quote_plus

from bs4 import BeautifulSoup
from playwright.async_api import TimeoutError as PWTimeoutError
from playwright.async_api import async_playwright

from config import settings
from extractor import extract_prices

logger = logging.getLogger(__name__)

# Real browser UA — required to pass Cloudflare JS challenges
_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36"
)

_YELL_BASE = "https://www.yell.com"
_YELL_SEARCH = "/ucs/UcsSearchAction.do?keywords={trade}&location={area}"

_CHECKATRADE_BASE = "https://www.checkatrade.com"
# URL format: /Search/{TradeSlug}/in/{Location}
# TradeSlug must be singular, title-cased, spaces as hyphens (Checkatrade's own taxonomy).
# Use _checkatrade_slug() to convert user input to the correct slug.
_CHECKATRADE_SEARCH = "/Search/{trade}/in/{area}"

# Mapping from common user-facing trade names to Checkatrade's URL slug taxonomy.
# Checkatrade uses singular, hyphenated category names — these don't derive predictably
# from user input (e.g. "boiler repair" → "Gas-Boiler-Servicing-Repair"), so we maintain
# an explicit table for the predefined dropdown trades.
_CHECKATRADE_SLUG_MAP: dict[str, str] = {
    "plumbers": "Plumber",
    "plumber": "Plumber",
    "gas engineers": "Central-Heating-Engineer",
    "gas engineer": "Central-Heating-Engineer",
    "electricians": "Electrician",
    "electrician": "Electrician",
    "builders": "Builder",
    "builder": "Builder",
    "roofers": "Roofer",
    "roofer": "Roofer",
    "boiler repair": "Gas-Boiler-Servicing-Repair",
    "heating engineers": "Central-Heating-Engineer",
    "heating engineer": "Central-Heating-Engineer",
    "painters": "Painter-Decorator",
    "decorators": "Painter-Decorator",
    "painters and decorators": "Painter-Decorator",
    "painter decorator": "Painter-Decorator",
    "handyman": "Handyman",
    "locksmiths": "Locksmith",
    "locksmith": "Locksmith",
    "gardeners": "Gardener",
    "gardener": "Gardener",
    "cleaners": "Cleaner",
    "tilers": "Tiler",
    "tiler": "Tiler",
    "plasterers": "Plasterer",
    "plasterer": "Plasterer",
    "carpenters": "Carpenter",
    "carpenter": "Carpenter",
}


def _checkatrade_slug(trade_type: str) -> str:
    """
    Convert a user-supplied trade type string to a Checkatrade URL slug.

    Uses an explicit mapping for known trade names. For unknown inputs, falls back
    to a best-effort conversion: Title-Case, remove trailing 's', replace spaces
    with hyphens. If the fallback also misses, Checkatrade will 404 silently and
    scrape_checkatrade() will return an empty list.
    """
    key = trade_type.strip().lower()
    if key in _CHECKATRADE_SLUG_MAP:
        return _CHECKATRADE_SLUG_MAP[key]

    # Best-effort fallback: Title-Case + hyphenate + basic de-pluralise
    words = key.split()
    if words and words[-1].endswith("s") and len(words[-1]) > 3:
        words[-1] = words[-1][:-1]  # strip trailing 's' (rough singulariser)
    return "-".join(w.capitalize() for w in words)


# ---------------------------------------------------------------------------
# Shared Playwright page fetcher
# ---------------------------------------------------------------------------


async def _fetch_page_html(url: str, wait_ms: int = 2000) -> str:
    """
    Fetch a page's full HTML using Playwright (handles JS rendering and Cloudflare).

    Uses domcontentloaded + a short wait rather than networkidle so that
    Cloudflare's JS challenge has time to complete without a long hang.
    Applies stealth args to reduce headless browser fingerprinting.
    Returns "" on any error.
    """
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            try:
                context = await browser.new_context(
                    user_agent=_USER_AGENT,
                    locale="en-GB",
                    # Viewport matching a common desktop resolution
                    viewport={"width": 1366, "height": 768},
                    # Additional headers to look more like a real browser
                    extra_http_headers={
                        "Accept-Language": "en-GB,en;q=0.9",
                        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
                    },
                )
                page = await context.new_page()
                # Remove the navigator.webdriver flag that Cloudflare uses to detect
                # headless browsers — must be added before the first navigation.
                await page.add_init_script(
                    "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
                )
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                    await page.wait_for_timeout(wait_ms)
                except PWTimeoutError:
                    logger.debug("Page load timeout for %s — extracting partial content", url)
                html = await page.content()
                return html
            finally:
                await browser.close()
    except Exception as e:
        logger.warning("Playwright failed fetching %s: %s", url, type(e).__name__)
        return ""


# ---------------------------------------------------------------------------
# Yell.com directory scraper
# ---------------------------------------------------------------------------


def _parse_yell_html(html: str) -> list[dict]:
    """
    Parse Yell.com search results HTML into a list of business dicts.
    Pure function — takes raw HTML, returns businesses. Testable with fixtures.

    Selectors use multiple fallbacks because Yell.com changes class names
    periodically. If all return 0 results, check the current Yell HTML
    and add the new selector at the top of listing_selectors.
    """
    soup = BeautifulSoup(html, "html.parser")
    businesses = []

    listing_selectors = [
        "article.businessCapsule",
        "div.businessCapsule",
        "article[class*='BusinessCard']",
        "div[class*='BusinessCard']",
        "article[class*='listing']",
        "div[class*='listing']",
        "li[class*='listing']",
    ]

    listings = []
    for selector in listing_selectors:
        listings = soup.select(selector)
        if listings:
            break

    if not listings:
        logger.warning("No Yell listings found — page structure may have changed")
        # Last resort: any /biz/ links
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "/biz/" in href and href not in [b.get("yell_url") for b in businesses]:
                businesses.append(
                    {
                        "name": a.get_text(strip=True) or "Unknown",
                        "website": "",
                        "phone": "",
                        "yell_url": (_YELL_BASE + href) if href.startswith("/") else href,
                        "source": "yell",
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

        name_el = listing.select_one("h2, h3, [class*='businessName'], [class*='name']")
        if name_el:
            name = name_el.get_text(strip=True)

        website_el = listing.select_one(
            "a[data-type='website'], a[class*='website'], a[href^='http']:not([href*='yell.com'])"
        )
        if website_el:
            website = website_el.get("href", "")

        phone_el = listing.select_one(
            "[class*='phone'], [class*='telephone'], [itemprop='telephone']"
        )
        if phone_el:
            phone = phone_el.get_text(strip=True)

        profile_el = listing.select_one("a[href*='/biz/']")
        if profile_el:
            href = profile_el.get("href", "")
            yell_url = (_YELL_BASE + href) if href.startswith("/") else href

        if name:
            businesses.append(
                {
                    "name": name,
                    "website": website,
                    "phone": phone,
                    "yell_url": yell_url,
                    "source": "yell",
                }
            )

    logger.info("Parsed %d businesses from Yell.com", len(businesses))
    return businesses


async def scrape_directory(area: str, trade_type: str) -> list[dict]:
    """
    Scrape Yell.com for businesses using Playwright (bypasses Cloudflare).
    Returns list of: {name, website, phone, yell_url, source}
    """
    search_url = _YELL_BASE + _YELL_SEARCH.format(
        trade=quote_plus(trade_type), area=quote_plus(area)
    )
    logger.info("Scraping Yell.com: %s", search_url)
    html = await _fetch_page_html(search_url)
    if not html:
        return []
    return _parse_yell_html(html)


# ---------------------------------------------------------------------------
# Checkatrade directory scraper
# ---------------------------------------------------------------------------


def _parse_checkatrade_html(html: str) -> list[dict]:
    """
    Parse Checkatrade search results HTML into a list of business dicts.
    Pure function — takes raw HTML, returns businesses. Testable with fixtures.

    Checkatrade search page (URL: /Search/{Trade}/in/{Location}) renders
    each result as a <li class="... bg-card rounded-2xl ..."> element.
    The business name and Checkatrade profile URL come from the /trades/ link
    inside the card. Phone numbers are hidden behind a "Reveal" button and
    are not available in static HTML. External websites are not shown on the
    search listing — they require visiting the /trades/ profile page.

    Selector strategy:
      Primary:  li elements containing a /trades/ link (current structure)
      Fallback: any element containing a /trades/ link (handles future restructures)
    """
    soup = BeautifulSoup(html, "html.parser")
    businesses = []
    seen_urls: set[str] = set()

    # Primary strategy: <li> cards — each contains exactly one /trades/ link (the business)
    li_cards = [
        li for li in soup.find_all("li")
        if li.find("a", href=lambda h: h and "/trades/" in h)
    ]

    if li_cards:
        for card in li_cards[: settings.max_businesses]:
            profile_el = card.find("a", href=lambda h: h and "/trades/" in h)
            if not profile_el:
                continue

            href = profile_el.get("href", "")
            checkatrade_url = (_CHECKATRADE_BASE + href) if href.startswith("/") else href
            # Strip query/fragment from URL for dedup purposes
            url_key = checkatrade_url.split("#")[0].split("?")[0]
            if url_key in seen_urls:
                continue
            seen_urls.add(url_key)

            name = profile_el.get_text(strip=True)
            if not name:
                continue

            # External website — Checkatrade sometimes surfaces it in the card
            website_el = card.find(
                "a", href=lambda h: h and h.startswith("http") and "checkatrade.com" not in h
            )
            website = website_el.get("href", "") if website_el else ""

            # Phone — usually behind a JS "Reveal" button; grab if present
            # Use CSS attribute selectors — BS4 lambda class matching is unreliable
            # when the class value is a bare string (join('phone') → 'p h o n e').
            phone_el = card.select_one(
                '[class*="phone"], [class*="telephone"], [class*="tel-"]'
            )
            phone = phone_el.get_text(strip=True) if phone_el else ""

            businesses.append(
                {
                    "name": name,
                    "website": website,
                    "phone": phone,
                    "checkatrade_url": checkatrade_url,
                    "yell_url": "",
                    "source": "checkatrade",
                }
            )
    else:
        # Fallback: collect all unique /trades/ links on the page
        logger.warning("No Checkatrade <li> cards found — falling back to /trades/ link scan")
        for a in soup.find_all("a", href=lambda h: h and "/trades/" in h):
            href = a.get("href", "")
            checkatrade_url = (_CHECKATRADE_BASE + href) if href.startswith("/") else href
            url_key = checkatrade_url.split("#")[0].split("?")[0]
            if url_key in seen_urls:
                continue
            seen_urls.add(url_key)
            name = a.get_text(strip=True)
            if name and len(name) > 2:
                businesses.append(
                    {
                        "name": name,
                        "website": "",
                        "phone": "",
                        "checkatrade_url": checkatrade_url,
                        "yell_url": "",
                        "source": "checkatrade",
                    }
                )
            if len(businesses) >= settings.max_businesses:
                break

    logger.info("Parsed %d businesses from Checkatrade", len(businesses))
    return businesses


def _parse_checkatrade_html_old_selectors(html: str) -> list[dict]:
    """Legacy selector path kept for reference — no longer called."""
    soup = BeautifulSoup(html, "html.parser")
    businesses = []

    for listing in soup.select("article")[: settings.max_businesses]:
        name = ""
        website = ""
        phone = ""
        checkatrade_url = ""

        name_el = listing.select_one(
            "h2, h3, [class*='name'], [class*='Name'], [class*='tradeName'], [class*='TradeName']"
        )
        if name_el:
            name = name_el.get_text(strip=True)

        # External website — Checkatrade sometimes links to the member's own site
        website_el = listing.select_one(
            "a[href^='http']:not([href*='checkatrade.com'])"
        )
        if website_el:
            website = website_el.get("href", "")

        phone_el = listing.select_one(
            "[class*='phone'], [class*='Phone'], [class*='telephone'], [class*='Telephone']"
        )
        if phone_el:
            phone = phone_el.get_text(strip=True)

        profile_el = listing.select_one("a[href*='/trades/']")
        if profile_el:
            href = profile_el.get("href", "")
            checkatrade_url = (_CHECKATRADE_BASE + href) if href.startswith("/") else href

        if name:
            businesses.append(
                {
                    "name": name,
                    "website": website,
                    "phone": phone,
                    "checkatrade_url": checkatrade_url,
                    "yell_url": "",
                    "source": "checkatrade",
                }
            )

    return businesses


async def scrape_checkatrade(area: str, trade_type: str) -> list[dict]:
    """
    Scrape Checkatrade for businesses using Playwright.
    Returns list of: {name, website, phone, checkatrade_url, yell_url, source}

    URL format: /Search/{TradeSlug}/in/{Location}
    The trade slug is derived via _checkatrade_slug() which maps user input to
    Checkatrade's own category taxonomy (singular, hyphenated).
    """
    slug = _checkatrade_slug(trade_type)
    # Checkatrade location: title-case city name (spaces allowed in path)
    location = area.strip().title()
    search_url = _CHECKATRADE_BASE + _CHECKATRADE_SEARCH.format(
        trade=slug, area=location
    )
    logger.info("Scraping Checkatrade: %s (trade slug: %s)", search_url, slug)
    html = await _fetch_page_html(search_url, wait_ms=4000)
    if not html:
        return []
    return _parse_checkatrade_html(html)


# ---------------------------------------------------------------------------
# Deduplication across sources
# ---------------------------------------------------------------------------


def _dedup_businesses(businesses: list[dict]) -> list[dict]:
    """
    Remove duplicate businesses across directory sources.
    Deduplicates by phone (exact match, if non-empty) then by name (case-insensitive).
    Preserves order — Yell results come first and take precedence over Checkatrade.
    """
    seen_phones: set[str] = set()
    seen_names: set[str] = set()
    unique: list[dict] = []

    for biz in businesses:
        phone = biz.get("phone", "").strip()
        name_key = biz.get("name", "").strip().lower()

        if phone and phone in seen_phones:
            continue
        if name_key and name_key in seen_names:
            continue

        if phone:
            seen_phones.add(phone)
        if name_key:
            seen_names.add(name_key)
        unique.append(biz)

    return unique


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
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            try:
                context = await browser.new_context(
                    user_agent=_USER_AGENT,
                    locale="en-GB",
                )
                page = await context.new_page()

                # 15s timeout; we'd rather move on than hang
                try:
                    await page.goto(url, wait_until="networkidle", timeout=15000)
                except PWTimeoutError:
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
    Full scrape: two directories → dedup → website visits → price extraction.

    Sources run concurrently (asyncio.gather). Yell results take precedence
    in deduplication. Combined pool is capped at settings.max_businesses.

    progress_cb(current, total) is called after each business is processed.

    Returns list of:
      {
        name, website, phone, yell_url, checkatrade_url, source,
        prices: [...],           # from extractor.py
        extraction_method: str,  # "regex" | "llm" | "none"
        source_url: str          # URL that was scraped for prices
      }
    """
    yell_results, checkatrade_results = await asyncio.gather(
        scrape_directory(area, trade_type),
        scrape_checkatrade(area, trade_type),
    )

    # Yell first — takes precedence in dedup
    all_businesses = yell_results + checkatrade_results
    businesses = _dedup_businesses(all_businesses)[: settings.max_businesses]

    if not businesses:
        logger.warning("No businesses found for '%s' in '%s'", trade_type, area)
        return []

    logger.info(
        "Found %d unique businesses (%d Yell, %d Checkatrade)",
        len(businesses),
        len(yell_results),
        len(checkatrade_results),
    )

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
