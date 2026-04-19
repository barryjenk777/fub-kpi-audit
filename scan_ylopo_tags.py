"""
scan_ylopo_tags.py — READ-ONLY Ylopo tag frequency scan over the last 60 days.

Paginates FUB /v1/people sorted newest-first, stops once we pass the 60-day
boundary, then counts occurrences of a target set of Ylopo/ISA/AI_* tags.
Also captures any "unknown" tags matching the Ylopo prefix patterns for
discovery of tags we don't yet track.

Produces a printed report only — does NOT write to FUB, does NOT mutate tags.
Usage:
    python scan_ylopo_tags.py
"""

import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timedelta, timezone

# Reuse the existing env loader and FUB client
from pond_mailer import _load_env
from fub_client import FUBClient


# ---------------------------------------------------------------------------
# Target tag set (canonical / normalized form)
# ---------------------------------------------------------------------------

TARGET_TAGS = {
    # AI Voice Call Dispositions
    "AI_VOICE_NEEDS_FOLLOW_UP": "AI Voice",
    "AI_VOICE_TRANSFERRED": "AI Voice",
    "ISA_TRANSFER_SUCCESSFUL": "AI Voice",
    "ISA_TRANSFER_UNSUCCESSFUL": "AI Voice",
    "ISA_ATTEMPTED_TRANSFER": "AI Voice",
    "ISA_ATTEMPTED_TRANSFER_REALTOR_UNAVAILABLE": "AI Voice",
    "CALLBACK_SCHEDULED": "AI Voice",
    "NURTURE": "AI Voice",
    "YLOPO_AI_VOICE_COMPLETED": "AI Voice",
    "DO_NOT_CALL": "AI Voice",
    "NOT_INTERESTED": "AI Voice",
    "NON_ENGLISH_SPEAKER": "AI Voice",

    # AI Text
    "AI_NEEDS_FOLLOW_UP": "AI Text",
    "AI_ENGAGED": "AI Text",
    "AI_OPT_OUT": "AI Text",
    "AI_NOT_INTERESTED": "AI Text",

    # Behavioral (already scored)
    "HANDRAISER": "Behavioral",
    "YPRIORITY": "Behavioral",
    "Y_HOME_3_VIEW": "Behavioral",
    "HVB": "Behavioral",
    "Y_SHARED_LISTING": "Behavioral",
    "RETURNED": "Behavioral",
    "Y_REMARKETING_ENGAGED": "Behavioral",
    "Y_SELLER_REPORT_VIEWED": "Behavioral",
    "Y_ADDRESS_FOUND": "Behavioral",

    # Seller Intent
    "Y_SELLER_REPORT_ENGAGED": "Seller Intent",
    "Y_SELLER_3_VIEW": "Seller Intent",
    "Y_SELLER_CASH_OFFER_REQUESTED": "Seller Intent",
    "Y_SELLER_LEARN_MORE_EQUITY": "Seller Intent",
    "Y_SELLER_TUNE_HOME_VALUE": "Seller Intent",
    "Y_AI_PRIORITY": "Seller Intent",

    # Z-Buyer
    "ZLEAD": "Z-Buyer",
    "Z_BUYER": "Z-Buyer",
    "YLOPO_Z_BUYER": "Z-Buyer",
}

# Prefixes used to classify "unknown Ylopo-like" tags for discovery.
UNKNOWN_PREFIXES = (
    "AI_", "ISA_", "Y_", "YLOPO_",
    "HAND", "HVB", "YPRIOR", "NURTURE",
    "CALLBACK_", "DO_NOT", "NOT_INTER", "NON_ENGL",
)


# ---------------------------------------------------------------------------
# Normalization
# ---------------------------------------------------------------------------

def normalize_tag(tag):
    """Canonicalize tag: uppercase, replace hyphens/spaces with underscores, collapse repeats."""
    if tag is None:
        return ""
    t = str(tag).strip().upper()
    # Replace hyphens and whitespace runs with underscore
    out = []
    prev_us = False
    for ch in t:
        if ch in ("-", " ", "\t"):
            if not prev_us:
                out.append("_")
                prev_us = True
        else:
            out.append(ch)
            prev_us = (ch == "_")
    return "".join(out).strip("_")


def looks_like_ylopo(norm_tag):
    """True if the normalized tag starts with one of the Ylopo/ISA/AI prefixes."""
    return any(norm_tag.startswith(p) for p in UNKNOWN_PREFIXES)


# ---------------------------------------------------------------------------
# Main scan
# ---------------------------------------------------------------------------

def parse_created(s):
    """Parse an FUB 'created' timestamp like '2026-03-12T18:02:44+00:00'
    or with 'Z' suffix. Returns a UTC datetime or None."""
    if not s:
        return None
    try:
        # FUB returns ISO8601 with tz offset
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def scan(client, days=60, page_size=100, hard_cap_pages=400):
    """Paginate newest-first, break once past the created-since boundary.

    Uses direct _request to bypass the 2000-offset cap in _get_paginated by
    paging until we see a `created` older than the window (or until FUB
    refuses higher offsets — in which case we warn and stop).
    """
    now = datetime.now(timezone.utc)
    since = now - timedelta(days=days)

    counts = defaultdict(int)                 # normalized target tag -> count
    samples = defaultdict(list)               # normalized target tag -> list of person IDs
    unknown_counts = defaultdict(int)         # normalized unknown-ish tag -> count
    scanned = 0
    offset = 0
    hit_offset_cap = False

    params_base = {
        "limit": page_size,
        "sort": "-created",            # newest-first
    }

    page_idx = 0
    while page_idx < hard_cap_pages:
        params = dict(params_base)
        params["offset"] = offset
        try:
            data = client._request("GET", "people", params=params)
        except Exception as e:
            msg = str(e)
            # Offset cap — FUB returns 400 once past 2000
            if "400" in msg and offset >= 2000:
                hit_offset_cap = True
                print(f"[warn] FUB offset cap hit at offset={offset}; "
                      f"further records in-window cannot be fetched this way.",
                      file=sys.stderr)
                break
            # Otherwise back off once and retry, per the spec
            print(f"[warn] request failed at offset={offset}: {e}; backing off 5s",
                  file=sys.stderr)
            time.sleep(5)
            try:
                data = client._request("GET", "people", params=params)
            except Exception as e2:
                print(f"[warn] retry failed at offset={offset}: {e2}; stopping",
                      file=sys.stderr)
                break

        people = data.get("people", []) if isinstance(data, dict) else []
        if not people:
            break

        past_window = False
        for person in people:
            created = parse_created(person.get("created"))
            if created is None:
                # Skip records with no parseable created date but keep going
                continue
            if created < since:
                past_window = True
                break

            scanned += 1
            pid = person.get("id")
            tags = person.get("tags") or []
            seen_norm_this_person = set()
            for raw in tags:
                norm = normalize_tag(raw)
                if not norm or norm in seen_norm_this_person:
                    continue
                seen_norm_this_person.add(norm)

                if norm in TARGET_TAGS:
                    counts[norm] += 1
                    if len(samples[norm]) < 5 and pid is not None:
                        samples[norm].append(pid)
                elif looks_like_ylopo(norm):
                    unknown_counts[norm] += 1

        if past_window:
            break
        if len(people) < page_size:
            break

        offset += page_size
        page_idx += 1

        # Soft guard against the FUB offset cap
        if offset >= 2000:
            # Try one more page (offset=1900 already returned), but we can't
            # go past 2000. If window isn't exhausted, warn.
            hit_offset_cap = True
            break

    return {
        "now": now,
        "since": since,
        "scanned": scanned,
        "counts": dict(counts),
        "samples": {k: list(v) for k, v in samples.items()},
        "unknown_counts": dict(unknown_counts),
        "hit_offset_cap": hit_offset_cap,
        "requests": client.request_count,
    }


# ---------------------------------------------------------------------------
# Reporting
# ---------------------------------------------------------------------------

def print_report(result):
    now = result["now"]
    since = result["since"]
    scanned = result["scanned"]
    counts = result["counts"]
    samples = result["samples"]
    unknowns = result["unknown_counts"]

    print("=" * 78)
    print("YLOPO TAG FREQUENCY SCAN — read-only")
    print("=" * 78)
    print()

    # ---- Section 1 ----
    print("Section 1: scan summary")
    print("-" * 78)
    print(f"  Date range (UTC) : {since.isoformat()}  ->  {now.isoformat()}")
    print(f"  Lookback         : 60 days")
    print(f"  People scanned   : {scanned}")
    print(f"  FUB requests     : {result['requests']}")
    if result["hit_offset_cap"]:
        print("  [warn] hit FUB offset cap (2000) — scan may be incomplete for")
        print("         very high-volume windows; most recent 2000 leads covered.")
    print()

    # ---- Section 2: target tag frequency ----
    print("Section 2: target tag frequency")
    print("-" * 78)
    denom = max(scanned, 1)

    # Group target tags by category for readability
    categories = {}
    for tag, cat in TARGET_TAGS.items():
        categories.setdefault(cat, []).append(tag)

    # Column header
    print(f"  {'TAG':<46} {'COUNT':>7} {'PCT':>7}   SAMPLE_IDS")
    print(f"  {'-'*46} {'-'*7} {'-'*7}   {'-'*30}")

    for cat in ["AI Voice", "AI Text", "Behavioral", "Seller Intent", "Z-Buyer"]:
        print()
        print(f"  [{cat}]")
        for tag in categories.get(cat, []):
            c = counts.get(tag, 0)
            pct = (c / denom) * 100.0
            sids = ", ".join(str(x) for x in samples.get(tag, []))
            print(f"  {tag:<46} {c:>7} {pct:>6.2f}%   {sids}")

    print()

    # ---- Section 3: unknown Ylopo-like tags ----
    print("Section 3: unknown Ylopo-like tags discovered (not in target list)")
    print("-" * 78)
    if not unknowns:
        print("  (none)")
    else:
        print(f"  {'TAG':<60} {'COUNT':>7}")
        print(f"  {'-'*60} {'-'*7}")
        for tag, c in sorted(unknowns.items(), key=lambda x: (-x[1], x[0])):
            print(f"  {tag:<60} {c:>7}")

    print()
    print("=" * 78)
    print("Done — no writes were made to FUB.")
    print("=" * 78)


def main():
    _load_env()
    if not os.environ.get("FUB_API_KEY"):
        print("ERROR: FUB_API_KEY not set (checked env + .env)", file=sys.stderr)
        sys.exit(2)

    client = FUBClient()
    result = scan(client, days=60)
    print_report(result)


if __name__ == "__main__":
    main()
