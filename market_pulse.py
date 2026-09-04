"""
market_pulse.py — hyperlocal market data for the 7 Hampton Roads cities.

Feeds the public /market/<city>/<audience> pages (buyer + seller angle per
city, 14 pages). Data comes from two verified public research feeds:

  1. Realtor.com Economic Research county CSV (listing-side: median list
     price, active/new listings, DOM, price-cut share). VA independent
     cities are county-equivalents, so the county file covers all 7 cities.
  2. Zillow Research public CSVs (sale-side: median sale price, % sold
     above list, days to pending, price-cut share, inventory).

Attribution is REQUIRED by both sources and rendered on every page.
Refresh cadence: 1st + 15th, 5:10am ET (both sources publish monthly).
No fabrication: pages only render metrics actually present in the feeds.
"""

import csv
import io
import json
import logging
from datetime import datetime, timezone

import requests

import db as _db

logger = logging.getLogger(__name__)

REALTOR_COUNTY_URL = ("https://econdata.s3-us-west-2.amazonaws.com/Reports/Core/"
                      "RDC_Inventory_Core_Metrics_County.csv")
ZILLOW_BASE = "https://files.zillowstatic.com/research/public_csvs/"
ZILLOW_FILES = {
    "median_sale_price":  "median_sale_price/City_median_sale_price_uc_sfrcondo_sm_month.csv",
    "pct_sold_above_list": "pct_sold_above_list/City_pct_sold_above_list_uc_sfrcondo_sm_month.csv",
    "days_to_pending":    "med_doz_pending/City_med_doz_pending_uc_sfrcondo_sm_month.csv",
    "pct_price_cut":      "perc_listings_price_cut/City_perc_listings_price_cut_uc_sfrcondo_sm_month.csv",
    "new_listings":       "new_listings/City_new_listings_uc_sfrcondo_sm_month.csv",
}

# slug → (display name, realtor county_fips, zillow RegionName)
CITIES = {
    "virginia-beach": ("Virginia Beach", "51810", "Virginia Beach"),
    "norfolk":        ("Norfolk",        "51710", "Norfolk"),
    "chesapeake":     ("Chesapeake",     "51550", "Chesapeake"),
    "suffolk":        ("Suffolk",        "51800", "Suffolk"),
    "portsmouth":     ("Portsmouth",     "51740", "Portsmouth"),
    "hampton":        ("Hampton",        "51650", "Hampton"),
    "newport-news":   ("Newport News",   "51700", "Newport News"),
}

_HTTP_TIMEOUT = 90
_UA = {"User-Agent": "LegacyHomeTeam-MarketPulse/1.0 (barry@yourfriendlyagent.net)"}


def _get_csv(url):
    r = requests.get(url, timeout=_HTTP_TIMEOUT, headers=_UA)
    r.raise_for_status()
    return csv.reader(io.StringIO(r.text))


def _f(val):
    """Parse a CSV number that may be empty."""
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def fetch_realtor():
    """Latest-month listing metrics for our 7 FIPS. Returns {fips: {...}}."""
    reader = _get_csv(REALTOR_COUNTY_URL)
    header = next(reader)
    idx = {name: i for i, name in enumerate(header)}
    want = {fips for _, (_, fips, _) in CITIES.items()}
    out = {}
    for row in reader:
        fips = row[idx["county_fips"]].strip().zfill(5)
        if fips not in want:
            continue
        out[fips] = {
            "month": row[idx["month_date_yyyymm"]],
            "median_list_price": _f(row[idx["median_listing_price"]]),
            "median_list_price_yy": _f(row[idx["median_listing_price_yy"]]),
            "active_listings": _f(row[idx["active_listing_count"]]),
            "active_listings_yy": _f(row[idx["active_listing_count_yy"]]),
            "median_dom": _f(row[idx["median_days_on_market"]]),
            "median_dom_yy": _f(row[idx["median_days_on_market_yy"]]),
            "new_listings": _f(row[idx["new_listing_count"]]),
            "price_reduced_share": _f(row[idx.get("price_reduced_share", -1)])
                                   if "price_reduced_share" in idx else None,
        }
    return out


def _zillow_metric(path):
    """Latest value + value 12 months back per city. Returns
    {region_name: (latest_month, latest, year_ago)}."""
    reader = _get_csv(ZILLOW_BASE + path)
    header = next(reader)
    idx = {name: i for i, name in enumerate(header)}
    date_cols = [(i, name) for i, name in enumerate(header)
                 if name[:2] == "20" and "-" in name]
    want_names = {rn for _, (_, _, rn) in CITIES.items()}
    out = {}
    for row in reader:
        if row[idx["State"]] != "VA":
            continue
        rn = row[idx["RegionName"]]
        if rn not in want_names:
            continue
        # Norfolk/Hampton exist in other states; State==VA filter above plus
        # CountyName sanity keeps us honest (VA independent city == own county)
        county = row[idx.get("CountyName", 0)] if "CountyName" in idx else ""
        if county and rn.split()[0].lower() not in county.lower():
            continue
        latest_i = None
        for i, _name in reversed(date_cols):
            if row[i].strip():
                latest_i = date_cols.index((i, _name))
                break
        if latest_i is None:
            continue
        li, lname = date_cols[latest_i]
        year_ago = None
        if latest_i - 12 >= 0:
            yi, _ = date_cols[latest_i - 12]
            year_ago = _f(row[yi])
        out[rn] = (lname, _f(row[li]), year_ago)
    return out


def fetch_zillow():
    """All sale-side metrics. Returns {region_name: {metric: {...}}}."""
    out = {}
    for metric, path in ZILLOW_FILES.items():
        try:
            data = _zillow_metric(path)
        except Exception as e:
            logger.error("zillow %s fetch failed: %s", metric, e)
            continue
        for rn, (month, latest, year_ago) in data.items():
            out.setdefault(rn, {})[metric] = {
                "month": month, "latest": latest, "year_ago": year_ago,
            }
    return out


def refresh_all():
    """Fetch both sources, build one snapshot per city, store in Postgres.
    Returns {city_slug: ok_bool}."""
    realtor = {}
    try:
        realtor = fetch_realtor()
    except Exception as e:
        logger.error("realtor fetch failed: %s", e)
    zillow = fetch_zillow()

    results = {}
    for slug, (display, fips, region) in CITIES.items():
        snap = {
            "city": display,
            "slug": slug,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "realtor": realtor.get(fips) or {},
            "zillow": zillow.get(region) or {},
        }
        has_data = bool(snap["realtor"]) or bool(snap["zillow"])
        if has_data:
            ok = _db.save_market_snapshot(slug, snap)
        else:
            ok = False
            logger.warning("market pulse: no data for %s, keeping prior snapshot", slug)
        results[slug] = bool(ok)
    return results


def get_snapshot(slug):
    """Latest stored snapshot for a city, or None."""
    if slug not in CITIES:
        return None
    return _db.get_market_snapshot(slug)
