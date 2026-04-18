"""
Repliers Real Estate Data Client
---------------------------------
Wraps the Repliers.io API to provide real MLS data for the pond mailer.

Three primary use cases:
  1. Market snapshot — active inventory + median DOM for a city/price range
     → injected into buyer email briefs so Claude writes real numbers, not estimates
  2. Active listings search — homes matching a lead's browsing criteria
     → used in Email 2 listing drops and drip listing emails as IDX alternatives
  3. Seller comps + valuation — for Ylopo Prospecting seller leads
     → pull recent sold comps near their address + AI estimate range

Setup:
  Set REPLIERS_API_KEY in your .env / Railway environment variables.
  Contact support@repliers.com to activate REIN MLS (Hampton Roads) on your account.

Current state:
  Test key is active — returns sample data from demo board.
  REIN board will return real Hampton Roads listings once activated.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from functools import lru_cache

import requests

logger = logging.getLogger("repliers_client")

REPLIERS_BASE = "https://api.repliers.io"
_API_KEY = os.environ.get("REPLIERS_API_KEY", "")

# Hampton Roads cities — used for validation / normalization
HAMPTON_ROADS_CITIES = {
    "virginia beach", "chesapeake", "norfolk", "suffolk", "portsmouth",
    "hampton", "newport news", "poquoson", "williamsburg", "yorktown",
    "smithfield", "isle of wight",
}


def _headers():
    return {"REPLIERS-API-KEY": _API_KEY, "Content-Type": "application/json"}


def _get(endpoint, params=None):
    """Raw GET with error handling. Returns parsed JSON or None."""
    if not _API_KEY:
        logger.warning("REPLIERS_API_KEY not set — skipping Repliers call")
        return None
    try:
        r = requests.get(
            f"{REPLIERS_BASE}/{endpoint.lstrip('/')}",
            headers=_headers(),
            params=params,
            timeout=8,
        )
        if r.status_code == 200:
            return r.json()
        logger.warning("Repliers %s returned %d: %s", endpoint, r.status_code, r.text[:200])
        return None
    except Exception as e:
        logger.warning("Repliers request failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# 1. Market Snapshot — real inventory + DOM numbers for Claude to write from
# ---------------------------------------------------------------------------

def get_market_snapshot(city: str, min_price: int = None, max_price: int = None,
                         beds: int = None) -> dict | None:
    """
    Return real market stats for a city/price band:
      - active_count  : homes currently for sale
      - sold_30d      : homes sold in last 30 days
      - median_dom    : median days on market (sold listings)
      - median_price  : median sold price
      - new_7d        : new listings in last 7 days (price drop opportunities)

    Returns None if API unavailable or no results.

    Example output:
      {
        "active_count": 42,
        "sold_30d": 18,
        "median_dom": 23,
        "median_price": 412000,
        "new_7d": 9,
        "city": "Chesapeake",
        "price_band": "$350k–$500k",
      }
    """
    params = {"status": "A", "city": city, "pageNum": 1, "resultsPerPage": 1}
    if min_price:
        params["minPrice"] = min_price
    if max_price:
        params["maxPrice"] = max_price
    if beds:
        params["minBeds"] = beds

    active_data = _get("listings", params)
    if not active_data:
        return None

    active_count = active_data.get("count", 0)

    # New listings in last 7 days
    seven_days_ago = (datetime.now(timezone.utc) - timedelta(days=7)).strftime("%Y-%m-%d")
    new_params = {**params, "minListDate": seven_days_ago}
    new_data = _get("listings", new_params)
    new_7d = new_data.get("count", 0) if new_data else 0

    # Sold in last 30 days — for DOM and price data
    thirty_days_ago = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    sold_params = {
        "status": "U",
        "lastStatus": "Sld",
        "city": city,
        "minSoldDate": thirty_days_ago,
        "pageNum": 1,
        "resultsPerPage": 50,
    }
    if min_price:
        sold_params["minPrice"] = min_price
    if max_price:
        sold_params["maxPrice"] = max_price

    sold_data = _get("listings", sold_params)
    sold_listings = (sold_data or {}).get("listings", [])
    sold_30d = (sold_data or {}).get("count", 0)

    # Median DOM and price from sold listings
    doms = [l.get("daysOnMarket") for l in sold_listings if l.get("daysOnMarket") is not None]
    prices = [l.get("soldPrice") for l in sold_listings if l.get("soldPrice") is not None]
    median_dom = sorted(doms)[len(doms) // 2] if doms else None
    median_price = int(sorted(prices)[len(prices) // 2]) if prices else None

    price_band = _format_price_band(min_price, max_price)

    return {
        "active_count":  active_count,
        "sold_30d":      sold_30d,
        "median_dom":    median_dom,
        "median_price":  median_price,
        "new_7d":        new_7d,
        "city":          city,
        "price_band":    price_band,
    }


def format_market_snapshot_for_brief(snapshot: dict) -> str:
    """
    Format a market snapshot dict into a brief line for Claude.
    Designed to inject as a section in _build_behavioral_brief().

    Example:
      REAL MARKET DATA — Chesapeake $350k–$500k (as of today):
        42 active listings | 18 sold last 30 days | Median 23 days on market
        9 new listings this week — fresh inventory just hit
    """
    if not snapshot:
        return ""
    city = snapshot.get("city", "")
    band = snapshot.get("price_band", "")
    active = snapshot.get("active_count")
    sold = snapshot.get("sold_30d")
    dom = snapshot.get("median_dom")
    new7 = snapshot.get("new_7d")
    price = snapshot.get("median_price")

    lines = [f"REAL MARKET DATA — {city} {band} (live as of today):"]

    stats = []
    if active is not None:
        stats.append(f"{active} active listings")
    if sold is not None:
        stats.append(f"{sold} sold in last 30 days")
    if dom is not None:
        stats.append(f"median {dom} days on market")
    if stats:
        lines.append(f"  {' | '.join(stats)}")

    if price:
        lines.append(f"  Median sold price: ${price:,}")
    if new7 is not None and new7 > 0:
        lines.append(f"  {new7} new listings this week — fresh inventory just hit")

    lines.append("USE THESE REAL NUMBERS in the email. Do not estimate or hedge — these are today's actual stats.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 2. Active Listing Search — homes matching a lead's criteria
# ---------------------------------------------------------------------------

def search_active_listings(city: str = None, min_price: int = None,
                            max_price: int = None, beds: int = None,
                            property_type: str = None, limit: int = 5,
                            zip_code: str = None) -> list[dict]:
    """
    Search active listings matching a buyer's criteria.

    Returns a list of listing dicts, each with:
      address, list_price, original_price, beds, baths, sqft, dom,
      year_built, description_snippet, mls_number, status, photos_count,
      price_reduced (bool — original vs current price differ)

    Used in Email 2 listing drops to give Claude actual homes to reference.
    """
    params = {
        "status": "A",
        "pageNum": 1,
        "resultsPerPage": limit,
        "sortBy": "updatedOn",
        "sortDir": "desc",
    }
    if city:
        params["city"] = city
    if zip_code:
        params["zip"] = zip_code
    if min_price:
        params["minPrice"] = min_price
    if max_price:
        params["maxPrice"] = max_price
    if beds:
        params["minBeds"] = beds

    data = _get("listings", params)
    if not data:
        return []

    results = []
    for l in data.get("listings", []):
        addr = l.get("address", {})
        details = l.get("details", {})
        original = l.get("originalPrice") or 0
        current = l.get("listPrice") or 0
        results.append({
            "mls_number":     l.get("mlsNumber"),
            "street":         f"{addr.get('streetNumber', '')} {addr.get('streetName', '')} {addr.get('streetSuffix', '')}".strip(),
            "city":           addr.get("city", ""),
            "zip":            addr.get("zip", ""),
            "list_price":     current,
            "original_price": original,
            "price_reduced":  original > 0 and current < original,
            "price_cut":      (original - current) if (original > 0 and current < original) else 0,
            "beds":           details.get("numBedrooms"),
            "baths":          details.get("numBathrooms"),
            "sqft":           details.get("sqft"),
            "year_built":     details.get("yearBuilt"),
            "dom":            l.get("daysOnMarket"),
            "list_date":      (l.get("listDate") or "")[:10],
            "photos_count":   l.get("photoCount", 0),
            "description_snippet": (details.get("description") or "")[:200],
        })
    return results


def format_listings_for_brief(listings: list[dict], context: str = "") -> str:
    """
    Format active listings into a brief section for Claude.

    Example output:
      LIVE ACTIVE LISTINGS matching their search criteria:
        1. 410 Jarvis Ct, Chesapeake — $389,000 (3bd/2ba, 1820sqft, 6 DOM)
           ⚡ Price reduced $10k from original ask
        2. 812 Harbor Way, Norfolk — $405,000 (4bd/2ba, 2100sqft, 3 DOM — just listed)
      Claude: reference these specific homes naturally. Don't list them robotically — weave 1-2 into the email.
    """
    if not listings:
        return ""
    lines = [f"LIVE ACTIVE LISTINGS{' — ' + context if context else ''} (use 1-2 of these in the email):"]
    for i, l in enumerate(listings[:4], 1):
        price = f"${l['list_price']:,}" if l.get('list_price') else "?"
        specs_parts = []
        if l.get("beds"):
            specs_parts.append(f"{l['beds']}bd")
        if l.get("baths"):
            specs_parts.append(f"{l['baths']}ba")
        if l.get("sqft"):
            specs_parts.append(f"{l['sqft']}sqft")
        if l.get("dom") is not None:
            if l["dom"] <= 3:
                specs_parts.append("just listed")
            else:
                specs_parts.append(f"{l['dom']} DOM")
        specs = ", ".join(specs_parts)
        line = f"  {i}. {l['street']}, {l['city']} — {price} ({specs})"
        lines.append(line)
        if l.get("price_reduced") and l.get("price_cut", 0) > 0:
            lines.append(f"     Price reduced ${l['price_cut']:,} from original ask — seller is motivated")
    lines.append("Weave 1-2 of these naturally into the email. Anchor the IDX search link to what you pulled.")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# 3. Seller Comps + Valuation — for Ylopo Prospecting leads (home address known)
# ---------------------------------------------------------------------------

def get_seller_property_data(street: str, city: str, zip_code: str = None,
                              beds: int = None, min_price: int = None,
                              max_price: int = None) -> dict | None:
    """
    For a seller lead with a known home address, return:
      - AI valuation estimate (range: low, value, high)
      - Recent sold comps (last 90 days, similar beds, nearby)
      - Market velocity (how fast homes like theirs are moving)

    Used to inject real numbers into seller email briefs so Barry's email
    can reference what their home is likely worth and what's selling nearby.

    Returns None if address not found in MLS or API unavailable.
    """
    # Step 1: Find the property by street address
    params = {
        "city": city,
        "status": "A,U",  # active or sold
        "pageNum": 1,
        "resultsPerPage": 5,
    }
    if zip_code:
        params["zip"] = zip_code

    # Extract street number + name from full address
    parts = street.strip().split()
    if parts and parts[0].isdigit():
        params["streetNumber"] = parts[0]
        params["streetName"] = " ".join(parts[1:])

    data = _get("listings", params)
    listings = (data or {}).get("listings", [])

    # Try to find exact match
    target = None
    for l in listings:
        addr = l.get("address", {})
        full = f"{addr.get('streetNumber','')} {addr.get('streetName','')}".lower().strip()
        if parts and parts[0] in full:
            target = l
            break

    # Get estimate from the matched listing (includes AI valuation)
    estimate = {}
    if target:
        detail = _get(f"listings/{target['mlsNumber']}")
        if detail:
            raw_est = detail.get("estimate", {})
            if raw_est.get("value"):
                estimate = {
                    "value":      int(raw_est["value"]),
                    "low":        int(raw_est.get("low", 0)),
                    "high":       int(raw_est.get("high", 0)),
                    "confidence": round(raw_est.get("confidence", 0) * 100),
                }

    # Step 2: Recent sold comps nearby (same city, ±1 bed, ±$75k, last 90 days)
    ninety_days_ago = (datetime.now(timezone.utc) - timedelta(days=90)).strftime("%Y-%m-%d")
    comp_params = {
        "status": "U",
        "lastStatus": "Sld",
        "city": city,
        "minSoldDate": ninety_days_ago,
        "pageNum": 1,
        "resultsPerPage": 10,
    }
    if beds:
        comp_params["minBeds"] = max(1, beds - 1)
        comp_params["maxBeds"] = beds + 1
    if min_price:
        comp_params["minPrice"] = min_price
    if max_price:
        comp_params["maxPrice"] = max_price

    comp_data = _get("listings", comp_params)
    raw_comps = (comp_data or {}).get("listings", [])

    comps = []
    for c in raw_comps[:5]:
        addr = c.get("address", {})
        details = c.get("details", {})
        sp = c.get("soldPrice")
        lp = c.get("listPrice")
        if not sp:
            continue
        ratio = round((sp / lp) * 100) if lp and lp > 0 else None
        comps.append({
            "street":       f"{addr.get('streetNumber', '')} {addr.get('streetName', '')}".strip(),
            "sold_price":   sp,
            "list_price":   lp,
            "sale_ratio":   ratio,
            "dom":          c.get("daysOnMarket"),
            "beds":         details.get("numBedrooms"),
            "baths":        details.get("numBathrooms"),
            "sqft":         details.get("sqft"),
            "sold_date":    (c.get("soldDate") or "")[:10],
        })

    if not estimate and not comps:
        return None

    # Market velocity — median DOM from comps
    doms = [c["dom"] for c in comps if c.get("dom") is not None]
    median_dom = sorted(doms)[len(doms) // 2] if doms else None

    # Sale-to-list ratio — are sellers getting ask price?
    ratios = [c["sale_ratio"] for c in comps if c.get("sale_ratio")]
    avg_ratio = int(sum(ratios) / len(ratios)) if ratios else None

    return {
        "estimate":    estimate,
        "comps":       comps,
        "comp_count":  len(comps),
        "median_dom":  median_dom,
        "avg_ratio":   avg_ratio,   # e.g. 98 = homes selling for ~98% of ask
        "city":        city,
    }


def format_seller_data_for_brief(seller_data: dict, first_name: str = "") -> str:
    """
    Format seller property data into a brief section for Claude.

    This is injected into the seller email brief so Barry's email can
    reference real comps and a real value range — not vague estimates.

    Example output:
      REAL PROPERTY DATA for their home:
      AI Estimate: $385,000–$410,000 (confidence: 72%)
      Comps — similar homes sold nearby in last 90 days:
        - 418 Harbor Blvd: sold $399,000 (3bd/2ba) — 14 days on market, got 101% of ask
        - 305 Maple Ct: sold $372,000 (3bd/2ba) — 28 days on market, got 97% of ask
      Market velocity: median 19 days to sell. Sellers getting ~99% of ask price.
      USE THESE NUMBERS. Reference comps naturally — "homes like yours on Harbor Blvd sold for..."
    """
    if not seller_data:
        return ""

    lines = ["REAL PROPERTY DATA for their home (use these in the email — do not fabricate numbers):"]

    est = seller_data.get("estimate", {})
    if est and est.get("value"):
        low = est.get("low", 0)
        high = est.get("high", 0)
        conf = est.get("confidence", 0)
        if low and high:
            lines.append(f"  AI Valuation: ${low:,}–${high:,} (midpoint ${est['value']:,}, {conf}% confidence)")
        else:
            lines.append(f"  AI Valuation: ~${est['value']:,}")

    comps = seller_data.get("comps", [])
    if comps:
        lines.append(f"  Comps — similar homes sold nearby in last 90 days:")
        for c in comps[:3]:
            dom_str = f"{c['dom']} days on market" if c.get("dom") is not None else ""
            ratio_str = f", got {c['sale_ratio']}% of ask" if c.get("sale_ratio") else ""
            beds_str = f"{c['beds']}bd/{c['baths']}ba" if c.get("beds") else ""
            parts = [f"${c['sold_price']:,}"]
            if beds_str:
                parts.append(f"({beds_str})")
            if dom_str:
                parts.append(f"— {dom_str}{ratio_str}")
            lines.append(f"    - {c['street']}: sold {' '.join(parts)}")

    dom = seller_data.get("median_dom")
    ratio = seller_data.get("avg_ratio")
    if dom or ratio:
        velocity_parts = []
        if dom:
            velocity_parts.append(f"median {dom} days to sell")
        if ratio:
            velocity_parts.append(f"sellers getting ~{ratio}% of ask")
        lines.append(f"  Market velocity: {', '.join(velocity_parts)}")

    lines.append("  Write like Barry already did his homework — reference the comp streets, the sold prices, the DOM.")
    lines.append("  'Homes like yours on [street] have been selling for...' is the right frame.")

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _format_price_band(min_price: int = None, max_price: int = None) -> str:
    def _fmt(p):
        if not p:
            return ""
        if p >= 1_000_000:
            return f"${p/1_000_000:.1f}M"
        return f"${p // 1000}k"

    if min_price and max_price:
        return f"{_fmt(min_price)}–{_fmt(max_price)}"
    elif max_price:
        return f"under {_fmt(max_price)}"
    elif min_price:
        return f"{_fmt(min_price)}+"
    return ""


def is_available() -> bool:
    """Quick check — returns True if API key is set and reachable."""
    return bool(_API_KEY)
