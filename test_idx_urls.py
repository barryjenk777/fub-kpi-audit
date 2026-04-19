"""
Test: IDX search URL builder — verify all link variations are well-formed and clickable.

Prints every URL so Barry can open them in a browser and confirm the search
results page loads with the right filters applied.

Run: python test_idx_urls.py
"""

import sys
import os
from urllib.parse import urlparse, parse_qs

sys.path.insert(0, os.path.dirname(__file__))
from pond_mailer import build_idx_search_url, build_lead_search_urls

PASS = "✅"
FAIL = "❌"
WARN = "⚠️ "

errors = []

def check(label, url, expected_params=None):
    """Print URL and validate expected query params are present."""
    parsed  = urlparse(url)
    qs      = parse_qs(parsed.query, keep_blank_values=True)

    ok = True
    missing = []
    if expected_params:
        for k, v in expected_params.items():
            flat = {kk: vv[0] for kk, vv in qs.items()}
            if k not in flat:
                missing.append(k)
                ok = False
            elif v is not None and flat[k] != str(v):
                missing.append(f"{k}={flat[k]} (expected {v})")
                ok = False

    icon = PASS if ok else FAIL
    print(f"\n{icon} {label}")
    print(f"   {url}")
    if missing:
        print(f"   {FAIL} Missing / wrong: {', '.join(missing)}")
        errors.append(label)
    return url


# ─────────────────────────────────────────────────────────────────────────────
print("=" * 70)
print("  IDX SEARCH URL TESTS — legacyhomesearch.com")
print("=" * 70)

# ── 1. Basic city + beds + price ─────────────────────────────────────────────
print("\n── 1. City + beds + price range ────────────────────────────────────────")

check(
    "Chesapeake — 3bd, $350k–$500k",
    build_idx_search_url(city="Chesapeake", beds=3, min_price=350000, max_price=500000),
    {
        "s[locations][0][city]": "Chesapeake",
        "s[locations][0][state]": "VA",
        "s[beds]": "3",
        "s[minPrice]": "350000",
        "s[maxPrice]": "500000",
    }
)

check(
    "Virginia Beach — 4bd, $450k–$650k",
    build_idx_search_url(city="Virginia Beach", beds=4, min_price=450000, max_price=650000),
    {
        # parse_qs decodes %20 → space, so check the decoded value
        "s[locations][0][city]": "Virginia Beach",
        "s[beds]": "4",
        "s[minPrice]": "450000",
        "s[maxPrice]": "650000",
    }
)

check(
    "Suffolk — 3bd, no price filter",
    build_idx_search_url(city="Suffolk", beds=3),
    {"s[locations][0][city]": "Suffolk", "s[beds]": "3"},
)

# ── 2. Property types ─────────────────────────────────────────────────────────
print("\n── 2. Property type filter ──────────────────────────────────────────────")

for pt in ("house", "condo", "townhouse", "land"):
    check(
        f"Norfolk — {pt}",
        build_idx_search_url(city="Norfolk", property_type=pt),
        {"s[propertyTypes][0]": pt},
    )

# ── 3. Zip code fallback ──────────────────────────────────────────────────────
print("\n── 3. Zip code fallback (no city) ───────────────────────────────────────")

check(
    "Zip 23320 (Chesapeake) — 3bd",
    build_idx_search_url(zip_code="23320", beds=3),
    {"s[locations][0][zip]": "23320", "s[beds]": "3"},
)

check(
    "Zip 23454 (Virginia Beach) — no beds",
    build_idx_search_url(zip_code="23454"),
    {"s[locations][0][zip]": "23454"},
)

# ── 4. City with spaces / apostrophes ────────────────────────────────────────
print("\n── 4. City names with spaces ────────────────────────────────────────────")

for city in ("Hampton", "Newport News", "Virginia Beach", "Isle of Wight"):
    check(
        f"City encoding: {city}",
        build_idx_search_url(city=city, beds=3),
        {},  # just check it builds without error
    )

# ── 5. build_lead_search_urls() — from behavior dict ─────────────────────────
print("\n── 5. build_lead_search_urls() — behavior-driven ────────────────────────")

MOCK_BEHAVIORS = [
    {
        "label": "Active buyer — Chesapeake/Suffolk, 3bd house, $350k–$500k",
        "behavior": {
            "cities": ["Chesapeake", "Suffolk"],
            "beds_seen": {3, 4},
            "price_min": 360000,
            "price_max": 490000,
            "property_type": "house",
            "zips": set(),
            "search_filters": {},
        },
    },
    {
        "label": "Condo searcher — Virginia Beach, 2bd, $250k–$350k",
        "behavior": {
            "cities": ["Virginia Beach"],
            "beds_seen": {2},
            "price_min": 255000,
            "price_max": 345000,
            "property_type": "condo",
            "zips": set(),
            "search_filters": {},
        },
    },
    {
        "label": "No city data — falls back to zip 23322",
        "behavior": {
            "cities": [],
            "beds_seen": {3},
            "price_min": None,
            "price_max": None,
            "property_type": None,
            "zips": {"23322"},
            "search_filters": {},
        },
    },
    {
        "label": "Minimal data — no beds, no price, no city",
        "behavior": {
            "cities": [],
            "beds_seen": set(),
            "price_min": None,
            "price_max": None,
            "property_type": None,
            "zips": set(),
            "search_filters": {},
        },
    },
    {
        "label": "Property type from search_filters (not views)",
        "behavior": {
            "cities": ["Norfolk"],
            "beds_seen": set(),
            "price_min": 200000,
            "price_max": 300000,
            "property_type": None,
            "zips": set(),
            "search_filters": {"property_types": ["townhouse", "townhouse", "condo"]},
        },
    },
]

for m in MOCK_BEHAVIORS:
    urls = build_lead_search_urls(m["behavior"])
    if not urls:
        if "Minimal" in m["label"]:
            # Expected: no city, no zip, no behavior → nothing to build. Not a bug.
            print(f"\n{PASS} {m['label']}")
            print("   (no URL — correct, zero behavior data means no search to build)")
        else:
            print(f"\n{FAIL} {m['label']}")
            print("   (unexpected: no URL generated)")
            errors.append(m["label"])
    else:
        for i, u in enumerate(urls):
            check(
                f"{m['label']} — link {i+1}: {u['label']}",
                u["url"],
                {},
            )

# ── 6. Price rounding ─────────────────────────────────────────────────────────
print("\n── 6. Price rounding (nearest $5k, 10% min cushion) ─────────────────────")

raw_behavior = {
    "cities": ["Chesapeake"],
    "beds_seen": {3},
    "price_min": 363_000,   # → 10% cushion → 326700 → rounds to $325,000
    "price_max": 487_000,   # → rounds to $485,000
    "property_type": "house",
    "zips": set(),
    "search_filters": {},
}
urls = build_lead_search_urls(raw_behavior)
if urls:
    u = urls[0]["url"]
    qs = parse_qs(urlparse(u).query)
    min_p = int(qs.get("s[minPrice]", [0])[0])
    max_p = int(qs.get("s[maxPrice]", [0])[0])
    price_ok = (min_p % 5000 == 0) and (max_p % 5000 == 0) and (min_p < 363000) and (max_p <= 490000)
    icon = PASS if price_ok else FAIL
    print(f"\n{icon} Price rounding: raw $363k–$487k → URL ${min_p:,}–${max_p:,}")
    if not price_ok:
        errors.append("Price rounding")
    print(f"   {u}")

# ── Summary ───────────────────────────────────────────────────────────────────
print("\n" + "=" * 70)
if errors:
    print(f"  {FAIL} {len(errors)} test(s) failed: {', '.join(errors)}")
    print("=" * 70)
    sys.exit(1)
else:
    print(f"  {PASS} All URL structure tests passed.")
    print()
    print("  Next step: click a few of the URLs above in a browser and confirm")
    print("  the legacyhomesearch.com search results page loads with the right")
    print("  city, beds, price range, and property type applied.")
    print("=" * 70)
