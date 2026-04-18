"""
Pond Mailer — LeadStream Email Marketing Engine

Sends behaviorally-personalized emails to Shark Tank pond leads based on
their actual IDX activity: which properties they viewed, which they saved,
price range drift, and behavioral intent signals from Ylopo tags.

Each email is written by Claude based on the lead's real behavior —
not a template with merge tags. Every email is unique.

Usage:
    python pond_mailer.py                  # Dry run (no emails sent)
    python pond_mailer.py --apply          # Send live emails
    python pond_mailer.py --person 105456  # Single lead (by FUB person ID)
    python pond_mailer.py --limit 10       # Process first N leads only

Requirements:
    ANTHROPIC_API_KEY — set in Railway environment variables
    SENDGRID_API_KEY  — already set
    FUB_API_KEY       — already set
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse, parse_qs

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] pond_mailer: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pond_mailer")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# ── Phase 1: Reply Sprint (emails 1-3) ──────────────────────────────────────
# 3-day cadence → Day 1, Day 4, Day 7
# Short, direct, reply-optimized. Goal: start a conversation NOW.
EMAIL_COOLDOWN_DAYS = 3

# ── Phase 2: Long-term Drip (emails 4-9) ────────────────────────────────────
# 15-day cadence → roughly every 2 weeks
# Alternates: longer content email (4, 6, 8) → listing link email (5, 7, 9)
# Goal: stay top of mind for leads who weren't ready in the sprint.
DRIP_COOLDOWN_DAYS = 15   # 5 gaps × 15 days = 75-day drip (emails 4-9)

# Minimum IDX events needed to write a meaningful email
# Counts ALL event types (page views, property views, saves, registration)
MIN_EVENTS_TO_EMAIL = 1

# Max leads to email per run
# Weekdays: 3x/day (8am, 1pm, 6pm ET) → 45 max/day
# Weekends: 2x/day (Sat 10am+3pm, Sun 1pm+6pm) → 30 max/day
MAX_PER_RUN = 15

# Ylopo tags that indicate behavioral intent (scored separately from LeadStream)
YLOPO_INTENT_TAGS = {
    "Y_HOME_3_VIEW":          "viewed a specific home 3+ times",
    "Y_SHARED_LISTING":       "shared a listing (likely with a partner/spouse)",
    "Y_SELLER_REPORT_VIEWED": "viewed a seller home value report",
    "Y_ADDRESS_FOUND":        "Ylopo identified their current home address",
    "Y_REMARKETING_ENGAGED":  "re-engaged via remarketing ads",
    "AI_NEEDS_FOLLOW_UP":     "Ylopo AI flagged for immediate follow-up",
    "AI_VOICE_NEEDS_FOLLOW_UP": "Ylopo AI voice call needs human follow-up",
    "HANDRAISER":             "raised their hand — expressed direct interest",
    "YPRIORITY":              "Ylopo top-priority buyer signal",
    "HVB":                    "high-value buyer",
    "RETURNED":               "returned to site after a gap",
}

SELL_BEFORE_BUY_TAGS = {
    "I_NEED_TO_SELL_BEFORE_I_CAN_BUY",
    "sell_before_buy=Yes",
}

LOGO_URL = "https://web-production-3363cc.up.railway.app/static/logo-blue.png"
PHYSICAL_ADDRESS = "LPT Realty · 1545 Crossways Blvd Chesapeake, VA 23320"
FROM_EMAIL = "barry@yourfriendlyagent.net"
FROM_NAME  = "Barry Jenkins | Legacy Home Team"


# ---------------------------------------------------------------------------
# Behavior Analyzer
# ---------------------------------------------------------------------------

def _safe_int(val, default=0):
    try:
        return int(float(val)) if val else default
    except (TypeError, ValueError):
        return default


def analyze_behavior(events, tags):
    """
    Parse FUB event data into a structured behavioral profile.

    Returns a dict with:
        views           — list of viewed property dicts (deduped by address)
        saves           — list of saved property dicts
        view_count      — total raw view events
        save_count      — total raw save events
        most_viewed     — property viewed most times (or None)
        most_viewed_ct  — how many times they viewed it
        price_min/max   — price range across all viewed homes
        price_drift     — positive if moving up, negative if moving down (dollars)
        beds_seen       — set of bedroom counts they viewed
        zips            — set of zip codes browsed
        cities          — set of cities browsed
        last_active_dt  — most recent event datetime
        hours_since_last — hours since last IDX event
        session_count   — estimated number of separate browsing sessions
        recent_session  — True if active within 48h
        sell_before_buy — True if they need to sell first
        intent_signals  — list of human-readable intent descriptions from tags
        search_filters  — parsed from Viewed Page URL (price, beds, location)
        registration_prop — property they registered on (if any)
    """
    prop_views = {}   # address → {prop dict, count}
    prop_saves = {}   # address → {prop dict}
    all_prices = []
    time_ordered = []  # (datetime, price) for drift calc
    registration_prop = None
    prop_type_counts = {}  # IDX type string → count, e.g. {"condo": 3, "house": 1}

    # FUB/IDX property type → legacyhomesearch URL value
    _PROP_TYPE_MAP = {
        "condominium":      "condo",
        "condo":            "condo",
        "condo/townhome":   "condo",
        "townhouse":        "townhouse",
        "townhome":         "townhouse",
        "single family":    "house",
        "single family residential": "house",
        "single family residence":   "house",
        "house":            "house",
        "residential":      "house",
        "land":             "land",
        "multi-family":     "multi-family",
        "multifamily":      "multi-family",
    }

    for e in events:
        e_type = e.get("type", "")
        prop   = e.get("property") or {}
        occurred = e.get("occurred") or e.get("created", "")

        if e_type == "Registration" and prop.get("street"):
            registration_prop = prop

        if e_type in ("Viewed Property", "Property Saved", "Saved Property") and prop.get("street"):
            addr  = f"{prop['street']}, {prop.get('city','')} {prop.get('code','')}"
            price = _safe_int(prop.get("price"))

            # Track property type — FUB may use "type", "propertyType", or "subType"
            raw_ptype = (
                prop.get("type") or prop.get("propertyType") or prop.get("subType") or ""
            ).lower().strip()
            mapped = _PROP_TYPE_MAP.get(raw_ptype)
            if mapped:
                prop_type_counts[mapped] = prop_type_counts.get(mapped, 0) + 1

            if e_type == "Viewed Property":
                if addr not in prop_views:
                    prop_views[addr] = {"prop": prop, "count": 0}
                prop_views[addr]["count"] += 1
                if price:
                    all_prices.append(price)
                    try:
                        dt = datetime.fromisoformat(occurred.replace("Z", "+00:00"))
                        time_ordered.append((dt, price))
                    except Exception:
                        pass

            elif e_type in ("Property Saved", "Saved Property"):
                prop_saves[addr] = prop

    # Most-viewed property
    most_viewed = None
    most_viewed_ct = 0
    if prop_views:
        best_addr = max(prop_views, key=lambda a: prop_views[a]["count"])
        most_viewed = prop_views[best_addr]["prop"]
        most_viewed_ct = prop_views[best_addr]["count"]

    # Price range
    price_min = min(all_prices) if all_prices else None
    price_max = max(all_prices) if all_prices else None

    # Price drift — compare avg of earliest half vs latest half of views
    price_drift = 0
    if len(time_ordered) >= 4:
        sorted_by_time = sorted(time_ordered, key=lambda x: x[0])
        half = len(sorted_by_time) // 2
        early_avg = sum(p for _, p in sorted_by_time[:half]) / half
        late_avg  = sum(p for _, p in sorted_by_time[half:]) / (len(sorted_by_time) - half)
        price_drift = int(late_avg - early_avg)

    # Geographic
    zips   = set(v["prop"].get("code","") for v in prop_views.values() if v["prop"].get("code"))
    cities = set(v["prop"].get("city","") for v in prop_views.values() if v["prop"].get("city"))
    beds_seen = set(
        _safe_int(v["prop"].get("bedrooms"))
        for v in prop_views.values() if v["prop"].get("bedrooms")
    )

    # Recency
    all_times = []
    for e in events:
        occ = e.get("occurred") or e.get("created", "")
        try:
            all_times.append(datetime.fromisoformat(occ.replace("Z", "+00:00")))
        except Exception:
            pass
    last_active_dt = max(all_times) if all_times else None
    now_utc = datetime.now(timezone.utc)
    hours_since = (
        (now_utc - last_active_dt).total_seconds() / 3600
        if last_active_dt else None
    )

    # Session estimation: gap > 3h = new session
    session_count = 0
    if all_times:
        sorted_times = sorted(all_times)
        session_count = 1
        for i in range(1, len(sorted_times)):
            gap = (sorted_times[i] - sorted_times[i-1]).total_seconds() / 3600
            if gap > 3:
                session_count += 1

    # Search filters from Viewed Page URLs
    search_filters = _parse_search_urls(events)

    # Intent signals from Ylopo tags
    intent_signals = [
        YLOPO_INTENT_TAGS[t] for t in tags if t in YLOPO_INTENT_TAGS
    ]

    # Sell-before-buy flag
    sell_before_buy = bool(SELL_BEFORE_BUY_TAGS & set(tags))

    # Dominant property type — most-viewed wins; fall back to search URL filter
    dominant_prop_type = None
    if prop_type_counts:
        dominant_prop_type = max(prop_type_counts, key=prop_type_counts.get)

    return {
        "views":            [v["prop"] for v in prop_views.values()],
        "saves":            list(prop_saves.values()),
        "view_count":       sum(v["count"] for v in prop_views.values()),
        "save_count":       len(prop_saves),
        "most_viewed":      most_viewed,
        "most_viewed_ct":   most_viewed_ct,
        "price_min":        price_min,
        "price_max":        price_max,
        "price_drift":      price_drift,
        "beds_seen":        sorted(beds_seen),
        "zips":             sorted(zips),
        "cities":           sorted(cities),
        "last_active_dt":   last_active_dt,
        "hours_since_last": hours_since,
        "session_count":    session_count,
        "recent_session":   bool(hours_since is not None and hours_since <= 48),
        "sell_before_buy":  sell_before_buy,
        "intent_signals":   intent_signals,
        "search_filters":   search_filters,
        "registration_prop": registration_prop,
        "property_type":    dominant_prop_type,   # "condo", "house", "townhouse", or None
    }


def _parse_search_urls(events):
    """Extract search filter params from Viewed Page URL events."""
    filters = {}
    for e in events:
        if e.get("type") != "Viewed Page":
            continue
        url = e.get("pageUrl") or ""
        if "legacyhomesearch" not in url:
            continue
        try:
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            # Price range
            if "s[maxPrice]" in qs:
                filters["max_price"] = _safe_int(qs["s[maxPrice]"][0])
            if "s[minPrice]" in qs:
                filters["min_price"] = _safe_int(qs["s[minPrice]"][0])
            # Beds / baths
            if "s[beds]" in qs:
                filters["min_beds"] = _safe_int(qs["s[beds]"][0])
            if "s[baths]" in qs:
                filters["min_baths"] = _safe_int(qs["s[baths]"][0])
            # Location
            for key, vals in qs.items():
                if "[city]" in key:
                    filters.setdefault("cities", []).append(vals[0])
                if "[zip]" in key:
                    filters.setdefault("zips", []).append(vals[0])
                # Property type from search URL (s[propertyTypes][0]=condo etc.)
                if "propertyTypes" in key and vals:
                    filters.setdefault("property_types", []).append(vals[0].lower())
        except Exception:
            continue
    return filters


# ---------------------------------------------------------------------------
# Email Strategy Selector
# ---------------------------------------------------------------------------

def select_strategy(behavior, leadstream_tier, tags):
    """
    Choose the right email approach based on behavioral signals.
    Returns (strategy_name, priority) — higher priority = more urgent.
    """
    b = behavior

    # Highest priority: saved a property
    if b["save_count"] > 0:
        return "saved_property", 100

    # Came back to the same property 2+ times
    if b["most_viewed_ct"] >= 2:
        return "repeat_view", 90

    # Active session within 48h with 4+ properties viewed
    if b["recent_session"] and b["view_count"] >= 4:
        return "active_session", 80

    # Sell-before-buy with any activity
    if b["sell_before_buy"] and b["view_count"] >= 2:
        return "seller_buyer", 75

    # Ylopo high-intent tag with any activity
    if leadstream_tier in ("AI_NEEDS_FOLLOW_UP", "HANDRAISER", "YPRIORITY"):
        return "signal_followup", 70

    # Price drift (moved up more than $25K) — searching for more
    if b["price_drift"] > 25000 and b["view_count"] >= 4:
        return "price_drift_up", 60

    # Price drift down — might be tightening budget
    if b["price_drift"] < -25000 and b["view_count"] >= 4:
        return "price_drift_down", 55

    # Has some views but nothing else notable
    if b["view_count"] >= 3:
        return "general_activity", 40

    # Browsed search pages — we have their price/bed filters from URL params
    if b["search_filters"]:
        return "search_browse", 35

    # Registered but minimal activity
    if b["registration_prop"]:
        return "registration_followup", 30

    # Any IDX activity at all — use as a light touch
    if b["session_count"] >= 1:
        return "any_activity", 25

    return "none", 0


# ---------------------------------------------------------------------------
# Claude Email Generator
# ---------------------------------------------------------------------------

SIGN_OFF = (
    "Barry Jenkins\n"
    "Legacy Home Team · LPT Realty\n"
    "(757) 919-8874\n"
    "www.legacyhomesearch.com"
)

# Sequence-specific angle instructions fed to Claude
_SEQUENCE_GUIDE = {
    1: """EMAIL 1 — The Pattern Interrupt.

One observation so specific they think you noticed them personally.
One question so easy they'd feel weird not answering.

Rules:
- DO NOT give market insight, helpful tips, or education
- DO NOT offer to help or explain yourself
- DO NOT use "just" or "reach out" or "let me know"
- One specific observation. One question. Stop.

The observation should make them think: "how does he know that?"
The question should be answerable in 2–5 words.

Example format (do not copy verbatim — personalize to the lead):
  "You've been back to that Chesapeake listing three times. Still on the fence about it?"

Or for a search-pattern lead with no specific property:
  "Four sessions in Norfolk under $250k — what's holding you back?"
""",

    2: """EMAIL 2 — The Gap.

They didn't reply to email 1. Completely different angle — do NOT reference the prior email.

This email teases something they can only get by replying. You've "found something."
You describe it just enough to create desire, then put it behind a reply.

DO NOT include a link. DO NOT deliver the value in the email.
The whole point: make them reply "yes, send it" — THEN you send the link.

Example format (personalize to actual lead data):
  "Found 6 homes in Chesapeake that hit everything you've been searching — 3bd, around your price,
   listed in the last two weeks. Want me to send them over?"

Or for a saved-property lead:
  "Something came up on that property you saved — might be worth a quick conversation.
   Want the details?"

End with a yes/no question. Nothing else.
""",

    3: """EMAIL 3 — The Breakup.

The most important email in the sequence. It gets the most replies. Here's why it works:
people who've been ignoring you feel guilty, and a graceful exit gives them safe permission
to finally respond — often with "wait, actually —"

Rules:
- 2–3 sentences max. Under 30 words.
- DO NOT say "this is my last email"
- DO NOT sound desperate, passive-aggressive, or like you're guilt-tripping
- DO NOT add a P.S., a link, or market data
- Tone: genuinely unbothered, warm, completely fine either way

The formula: give them explicit permission to say no or go quiet + leave a door open.

Example (personalize name/area — do not copy verbatim):
  "Timing's probably off — totally get it. I'll check back in a few months
   unless you want to connect before then."

Or:
  "No worries if the search is on pause. I'll be here when it picks back up."
""",

    # ── Phase 2: Long-term Drip ───────────────────────────────────────────────
    # Emails 4-9. 15-day cadence. Alternates content (4,6,8) and listing drops (5,7,9).
    # Lead didn't engage with the sprint — now we play the long game.

    4: """EMAIL 4 — First Drip (content). They didn't bite on the sprint. That's fine.
Completely different gear now. No urgency. No gap. Just Barry being genuinely useful.

This email is longer than the sprint emails — 90-120 words. It should feel like a
note from someone who's been paying attention to the market on your behalf.

Rules:
- Open with a specific market observation tied to their search area or price range
- One useful insight — something they probably don't know — that makes them think
- Soft question at the end. Not "what's your timeline?" — something more curious.
  E.g. "What would have to change for it to make sense?" or "Still keeping an eye on Chesapeake?"
- Warm, unhurried, confident. No pressure. Sounds like a smart friend, not a pitch.
- No links. No P.S. No urgency language.
- DO NOT reference the earlier emails.

Voice: Barry's authentic teaching voice — "too nice for sales" but genuinely knows Hampton Roads.
""",

    5: """EMAIL 5 — First Listing Drop. Short. Direct. Here are homes. No fluff.

40-55 words max. This email is a gift: you did the work of finding homes for them.
Frame it as personal curation, not a system search.

Rules:
- One warm sentence explaining why you pulled these specifically (ties to their behavior)
- Include the IDX search link from the brief as [descriptive anchor text](url)
  Make the anchor text specific: "3bd homes in Chesapeake under $350k" not "click here"
- One easy yes/no question: "Anything worth a closer look?" or "Want to tour any of these?"
- DO NOT explain yourself. DO NOT pad with market context.
- The link IS the value. Get out of the way.

Subject lines should reference what you pulled: "6 homes in Chesapeake" or "found a few in Norfolk"
""",

    6: """EMAIL 6 — Drip content. Different angle from email 4.

90-120 words. Story-first this time. Open with something human — a client situation
(anonymized), a local quirk about Hampton Roads real estate, or a counterintuitive insight
about their specific search area that most buyers get wrong.

Rules:
- Teach one thing. Don't teach three things.
- The insight should be specific to their city, price range, or property type
- End with a genuinely curious question — not a sales ask
- No links. No urgency. No P.S.
- Should feel like a 9pm email from a friend who just thought of something relevant
""",

    7: """EMAIL 7 — Second Listing Drop. Same format as email 5.

40-55 words. Different from email 5 — different search angle (new listings, slight price
shift, or a different city from their browsing history if they searched multiple areas).

Use the IDX search data in the brief to build the right link.
Same rules as email 5: personal, specific anchor text, one question, nothing else.
""",

    8: """EMAIL 8 — Drip content. Seasonal or situational.

90-120 words. Tie it to something real: the time of year (spring inventory, summer moves,
school-year timing), a rate environment note, or a shift in Hampton Roads inventory.
Keep it grounded — no doom and gloom, no hype.

One observation → one implication for their search → one soft question.
Should feel timely, like you wrote it this week specifically for them.
No links. No P.S.
""",

    9: """EMAIL 9 — Final Drip. Warm close. Leave the door open.

75-90 words. Acknowledge it's been a while without guilt-tripping.
Tone: genuinely warm, unbothered, respectful of their time.

Something like: "Still keeping an eye on [their city] for you — the market's shifted
a bit since we first connected. No pressure to do anything with it, but worth a quick
catch-up if the timing ever feels right."

This is the last email in the drip. Leave them feeling good about you, not chased.
Soft close: "I'll be around whenever it makes sense."
No links. No P.S. No urgency.
""",
}


def _get_seq_guide(sequence_num):
    """Return the right sequence guide for any email number.

    For sequence_num > 9 (edge case), cycle through the drip pattern:
    even = content email, odd = listing drop.
    """
    if sequence_num in _SEQUENCE_GUIDE:
        return _SEQUENCE_GUIDE[sequence_num]
    # Beyond 9: alternate content/listing indefinitely
    return _SEQUENCE_GUIDE[6] if sequence_num % 2 == 0 else _SEQUENCE_GUIDE[7]


def _is_listing_drop(sequence_num):
    """True for listing-drop emails (5, 7, 9, 11…) — these include IDX links."""
    return sequence_num >= 5 and sequence_num % 2 == 1


def generate_email(person, behavior, strategy, leadstream_tier,
                   sequence_num=1, dry_run=False):
    """
    Generate a personalized email using Claude.

    sequence_num: 1+ — controls phase, tone, and angle.
      1-3: reply sprint (short, direct, no links)
      4,6,8: drip content (longer, warm, no links)
      5,7,9: drip listing drop (short, IDX link included)
    Returns {subject, body_text, body_html} or raises on failure.
    """
    first_name = person.get("firstName") or "there"
    tags = person.get("tags", [])

    # Listing-drop emails (5, 7, 9…) include IDX links — all others do not.
    # Sprint emails (1-3) optimize for reply; content drip (4,6,8) teaches.
    listing_drop = _is_listing_drop(sequence_num)
    search_urls  = build_lead_search_urls(behavior) if listing_drop else []

    brief     = _build_behavioral_brief(first_name, behavior, strategy, leadstream_tier, tags,
                                        search_urls=search_urls)
    seq_guide = _get_seq_guide(sequence_num)

    # Phase label for logging / dry-run display
    if sequence_num <= 3:
        phase_label = f"sprint #{sequence_num}/3"
    else:
        drip_num = sequence_num - 3
        phase_label = f"drip #{drip_num} ({'listing' if listing_drop else 'content'})"

    if dry_run:
        logger.info("[DRY RUN] Would call Claude for %s (strategy: %s, %s)",
                    first_name, strategy, phase_label)
        return {
            "subject":   f"[DRY RUN] {phase_label} · {strategy} · {first_name}",
            "body_text": f"[DRY RUN {phase_label}]\n\n{brief[:300]}...",
            "body_html": f"<p>[DRY RUN {phase_label}]</p><pre>{brief[:300]}</pre>",
        }

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set — add it to Railway environment variables")

    try:
        import anthropic
    except ImportError:
        raise RuntimeError("anthropic package not installed — run: pip install anthropic")

    client = anthropic.Anthropic(api_key=api_key)

    # Phase-specific word count and link rules
    if sequence_num <= 3:
        length_rule = "25-45 words for emails 1-2. 20-35 words for email 3. No links. Short is good — but never so clipped it reads as rude. A fragment without warmth is just cold."
        link_rule   = "NO LINKS — ever. They click instead of reply."
        max_tokens  = 400
    elif listing_drop:
        length_rule = "40-55 words. Short. Get out of the way and let the link do the work."
        link_rule   = "INCLUDE the IDX link from the brief as [descriptive anchor text](url). This is the value."
        max_tokens  = 400
    else:
        length_rule = "90-120 words. Long enough to be genuinely useful, short enough to finish in 30 seconds."
        link_rule   = "NO LINKS. The value is the insight, not a search result."
        max_tokens  = 600

    prompt = f"""You are writing a nurture email from Barry Jenkins, realtor in Hampton Roads VA.

PHASE: {"Reply Sprint" if sequence_num <= 3 else ("Listing Drop" if listing_drop else "Long-term Drip")}
EMAIL #{sequence_num} in the sequence.

━━━━ LENGTH ━━━━
{length_rule}

━━━━ LINK RULE ━━━━
{link_rule}

━━━━ WHO THIS PERSON IS (ground truth — everything else flows from this) ━━━━
This is a potential home BUYER. They were browsing homes for sale on Barry's IDX
home search website (legacyhomesearch.com) in the Hampton Roads, VA area — cities
like Virginia Beach, Chesapeake, Norfolk, Suffolk, Portsmouth, Hampton, Newport News.

They are NOT a generic web visitor. They are someone considering buying a home, who
looked at specific properties, possibly saved a few, and then went quiet. Barry is a
local real estate agent following up personally on their home search.

Every sentence must reflect this. The reader should feel:
"This agent actually looked at what I was searching for, not just that I clicked a website."

Language that always fits:
  "browsing homes" / "your home search" / "homes you've been looking at"
  "buyers in [city]" / "that price range" / "the [neighborhood] market"
  "worth a look in person?" / "still looking in [city]?"

Language that NEVER fits:
  "browsed my site" / "visited once" / "your web activity" / "you browsed"
  Anything that sounds like generic email marketing — this is a personal agent follow-up
  Treating them like a stranger — they looked at real homes, reference what you know

━━━━ VOICE (always) ━━━━
- Barry Jenkins: conversational, teaching > pushing, warm, direct, a little wry
- Sounds like a smart friend who happens to know Hampton Roads deeply
- Fragments fine. Contractions always. Never over-explain.
- Never: "dream home", "perfect fit", "hot market", "reach out", "just checking in",
  "I hope this finds you well", "happy to help", "feel free to", "I'd love to"
- NEVER say "Ylopo" — the lead has no idea what that is. Say "my home search website" instead.
- Never open with "I noticed" — lead with the observation itself

━━━━ TRANSLATE DATA → HUMAN LANGUAGE (critical) ━━━━
The brief contains internal data labels. Never use them verbatim in the email.
This is a HOME BUYER. Translate all browsing data into real estate language:

  ✗ "Two sessions"                  → ✓ "You've been on my home search site twice"
  ✗ "3 sessions"                    → ✓ "You've come back to look at homes a few times"
  ✗ "browsed once"                  → ✓ "You were looking at homes in [city]"
  ✗ "you browsed"                   → ✓ "you were searching for homes" / "you were looking at properties"
  ✗ "visited my site"               → ✓ "were searching for homes on my site"
  ✗ "12 views"                      → ✓ "you've been looking at homes in Chesapeake"
  ✗ "save_count: 2"                 → ✓ "you saved a couple of homes"
  ✗ "price drift UP $40,000"        → ✓ "looks like your budget has some room"
  ✗ "hours_since_last: 36"          → ✓ "yesterday" or "a day ago"
  ✗ "session_count"                 → never use this word
  ✗ "behavior signals"              → never reference the tracking system
  ✗ "most_viewed_ct: 3"             → ✓ "you've gone back to that one three times"
  ✗ "web activity" / "online activity" → ✓ "your search" / "the homes you've been looking at"
  ✗ "a month ago"                   → ✓ "about a month ago you were looking at homes in [city]"
  ✗ "You were searching"            → ✓ "You were looking at homes in [city] in [price range]"

The reader should never feel like they're reading a database entry or a marketing email.
They should feel like their agent noticed what they were looking at and reached out.
Specific is good. Clinical is not. Generic is worst of all.

━━━━ WHAT KILLS REPLIES (never do these) ━━━━
- Explaining your process or credentials
- Padding with market stats to prove you know things
- Sounding like you're trying — trying reads as desperation
- "Just checking in" / "reaching out" / "circling back" / "following up"
- "Dream home", "perfect fit", "hot market", "I hope this finds you well"
- Opening with "I noticed" — lead with the observation itself

WHAT GETS REPLIES (non-negotiable rules):
1. Specificity — prove you looked at THIS person, not a persona
2. A gap or a gift — leave something they want to close, or give them something real
3. An easy question — answerable in 2–5 words, yes/no if possible

━━━━ LOCAL INSIDER P.S. (optional — use ~50% of the time) ━━━━
About half the time, add a P.S. with a single piece of genuine local market
intelligence. This is the "I actually know Hampton Roads" signal that mass email
cannot replicate. Good P.S. lines feel like a text from a friend who sells real
estate — not a market report excerpt.

Rules for the P.S.:
- One sentence only. Conversational. Specific to their search area if possible.
- It should make them think "this agent knows what they're talking about"
- Never: a CTA, a credential, "don't hesitate to reach out", or a generic market stat
- Good examples:
    "P.S. — Great Bridge is seeing multiple offers on anything under $375k right now."
    "P.S. — Suffolk's Harbour View area has been moving fast. Might be worth a look."
    "P.S. — A lot of buyers in the $400s are getting pushed to Newport News right now. VB inventory is tight."
    "P.S. — Chesapeake has some of the best schools in Hampton Roads if that's a factor."
- Skip the P.S. the other half of the time — silence is also a choice.

FORMAT:
- First line: first name + comma only. Nothing else. ("Marcus,")
- Blank line
- Body: 2–4 sentences. That's it. No closing line. No explanation.
- Optional P.S. on its own line after the body (see above).
- Signature is added automatically — stop writing before the sign-off.

SUBJECT LINES — these determine if it's opened:
- 3–6 words. Should feel like a text from a saved contact.
- Best performers: property address, "[City] — quick question", just their name,
  "still searching?", a specific number ("3 homes in Chesapeake")
- No ALL CAPS. No emojis. No clever hooks. Direct beats clever every time.
- Generate 3 options.

SEQUENCE POSITION AND SPECIFIC INSTRUCTIONS:
{seq_guide}

LEAD DATA (personalize to THIS person — generic = failure):
{brief}

OUTPUT (JSON only, no markdown fences, no code blocks):
{{
  "subject_options": ["option 1", "option 2", "option 3"],
  "body": "Marcus,\\n\\n[2–4 sentences]\\n\\n[P.S. — local insight, or omit entirely]"
}}

The first sentence of the body must reference something specific to this lead:
their property address, the number of times they came back, their city, their price range.
If you can't tell it was written for exactly this person, rewrite it."""

    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=max_tokens,   # 400 for sprint/listing, 600 for content drip
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()

    import json, re
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)

    data = json.loads(raw)
    subject_options = data.get("subject_options", [])
    subject = subject_options[0] if subject_options else f"Following up — {first_name}"
    claude_body = data.get("body", "")

    # Plain text: convert markdown links to readable "label (url)" format
    import re as _re
    body_text_clean = _re.sub(
        r'\[([^\]]+)\]\((https?://[^\)]+)\)',
        r'\1 ( \2 )',
        claude_body,
    )
    body_text = body_text_clean + "\n\n" + SIGN_OFF

    # HTML: render Claude body only — the HTML template has its own signature footer
    # so we do NOT include SIGN_OFF here, which would create a double signature.
    body_html = _render_html(claude_body)

    return {
        "subject":      subject,
        "body_text":    body_text,
        "body_html":    body_html,
        "all_subjects": subject_options,
    }


# ---------------------------------------------------------------------------
# IDX Search URL Builder
# ---------------------------------------------------------------------------

BASE_IDX_SEARCH_URL = "https://listings.legacyhomesearch.com/search"


def build_idx_search_url(city=None, state="VA", beds=None, baths=None,
                          min_price=None, max_price=None, property_type=None,
                          subdivision=None, zip_code=None):
    """Build a legacyhomesearch.com IDX search URL from structured parameters.

    URL schema decoded from Barry's examples:
      s[locations][0][city]         — city name (URL-encoded)
      s[locations][0][state]        — 2-char state
      s[locations][0][subdivision]  — subdivision name
      s[locations][0][zip]          — zip code
      s[beds]                       — bedroom count
      s[baths]                      — bathroom count
      s[minPrice] / s[maxPrice]     — price bounds (integers, no commas)
      s[propertyTypes][0]           — house | condo | townhouse | land
      ip=t                          — include pending listings
    """
    from urllib.parse import quote as _q
    params = []
    loc = 0

    if city:
        params.append(f"s[locations][{loc}][city]={_q(city)}")
        params.append(f"s[locations][{loc}][state]={state}")
        if subdivision:
            params.append(f"s[locations][{loc}][subdivision]={_q(subdivision)}")
        loc += 1
    elif zip_code:
        params.append(f"s[locations][{loc}][zip]={zip_code}")
        loc += 1

    params += [
        "s[orderBy]=sourceCreationDate%2Cdesc",
        "s[page]=1",
        "s[limit]=18",
    ]

    if beds:
        params.append(f"s[beds]={int(beds)}")
    if baths:
        params.append(f"s[baths]={int(baths)}")
    if min_price:
        params.append(f"s[minPrice]={int(min_price)}")
    if max_price:
        params.append(f"s[maxPrice]={int(max_price)}")
    if property_type:
        params.append(f"s[propertyTypes][0]={property_type}")

    params.append("ip=t")
    return BASE_IDX_SEARCH_URL + "?" + "&".join(params)


def build_lead_search_urls(behavior):
    """Return 1–2 IDX search URLs tailored to this lead's actual browsing data.

    Uses their city, typical bed count, price range, and property type to build
    a search that shows them homes genuinely similar to what they've been browsing.
    """
    b = behavior
    urls = []

    cities = list(b["cities"]) if b["cities"] else []
    beds   = min(b["beds_seen"]) if b["beds_seen"] else None

    # Property type: prefer dominant type from viewed listings; fall back to
    # search page URL filters (e.g. lead searched for condos on the IDX site)
    prop_type = b.get("property_type")
    if not prop_type:
        sf = b.get("search_filters", {})
        page_types = sf.get("property_types", [])
        if page_types:
            # Most common from page views
            prop_type = max(set(page_types), key=page_types.count)

    # Round price range: 10% cushion below min, stay at their max
    min_price = max_price = None
    if b["price_min"] and b["price_max"]:
        min_price = int(b["price_min"] * 0.9 / 5000) * 5000   # nearest $5k
        max_price = int(b["price_max"]        / 5000) * 5000

    for i, city in enumerate(cities[:2]):
        url = build_idx_search_url(
            city=city,
            beds=beds,
            min_price=min_price if i == 0 else None,
            max_price=max_price if i == 0 else None,
            property_type=prop_type,
        )
        bed_str   = f"{beds}bd " if beds else ""
        type_str  = f"{prop_type} " if prop_type and prop_type != "house" else ""
        price_str = f" around ${min_price:,}" if (min_price and i == 0) else ""
        label = f"latest {bed_str}{type_str}listings in {city}{price_str}"
        urls.append({"url": url, "label": label})

    # Fallback: zip code only (when no city data in events)
    if not urls and b["zips"]:
        zip_code = list(b["zips"])[0]
        url = build_idx_search_url(zip_code=zip_code, beds=beds, property_type=prop_type)
        type_str = f"{prop_type} " if prop_type and prop_type != "house" else ""
        urls.append({"url": url, "label": f"latest {type_str}listings in {zip_code}"})

    return urls


def _build_behavioral_brief(first_name, behavior, strategy, leadstream_tier, tags,
                             search_urls=None):
    """Build a clear behavioral summary to feed Claude."""
    b = behavior
    strategy_notes = {
        "search_browse":   "Lead browsed search pages but didn't click into specific listings. Write from their search filters (price range, beds, location). Reference what the market looks like for what they're searching.",
        "any_activity":    "Lead registered or visited the site but minimal data. Keep it light — acknowledge you noticed them, offer something useful, low-friction ask.",
        "registration_followup": "Lead registered on a specific property. Reference that home as the hook.",
    }

    lines = [f"LEAD: {first_name}"]
    lines.append(f"LEADSTREAM TIER: {leadstream_tier}")
    lines.append(f"EMAIL STRATEGY: {strategy}")
    if strategy in strategy_notes:
        lines.append(f"STRATEGY NOTE: {strategy_notes[strategy]}")

    # Properties viewed
    if b["views"]:
        lines.append(f"\nPROPERTIES VIEWED ({b['view_count']} views across {len(b['views'])} unique homes):")
        for p in b["views"][:6]:  # cap at 6 to keep prompt tight
            addr  = f"{p.get('street')}, {p.get('city')} {p.get('code')}"
            price = f"${_safe_int(p.get('price')):,}"
            specs = f"{p.get('bedrooms','?')}bd/{p.get('bathrooms','?')}ba, {p.get('area','?')}sf"
            lines.append(f"  - {addr} | {price} | {specs}")

        if b["most_viewed_ct"] >= 2 and b["most_viewed"]:
            p = b["most_viewed"]
            lines.append(f"\nNOTE: They viewed {p.get('street')} {b['most_viewed_ct']} times — unusually high interest.")

    # Saved properties
    if b["saves"]:
        lines.append(f"\nSAVED (highest intent — {len(b['saves'])} saved):")
        for p in b["saves"][:3]:
            addr  = f"{p.get('street')}, {p.get('city')} {p.get('code')}"
            price = f"${_safe_int(p.get('price')):,}"
            specs = f"{p.get('bedrooms','?')}bd/{p.get('bathrooms','?')}ba"
            lines.append(f"  - {addr} | {price} | {specs}")

    # Price context
    if b["price_min"] and b["price_max"]:
        price_range = f"${b['price_min']:,}–${b['price_max']:,}"
        lines.append(f"\nPRICE RANGE BROWSED: {price_range}")
    if abs(b["price_drift"]) > 15000:
        direction = "UP" if b["price_drift"] > 0 else "DOWN"
        lines.append(f"PRICE DRIFT: moved {direction} ${abs(b['price_drift']):,} over their search history")

    # Geography
    if b["cities"]:
        lines.append(f"CITIES: {', '.join(b['cities'])}")
    if b["zips"]:
        lines.append(f"ZIP CODES: {', '.join(b['zips'])}")

    # Recency
    if b["hours_since_last"] is not None:
        hrs = b["hours_since_last"]
        if hrs < 2:
            recency = "ACTIVE RIGHT NOW (within 2 hours)"
        elif hrs < 24:
            recency = f"active {int(hrs)} hours ago"
        elif hrs < 48:
            recency = "active yesterday"
        else:
            recency = f"last active {int(hrs/24)} days ago"
        lines.append(f"RECENCY: {recency}")
    # Translate session count into natural language for Claude — "Two sessions" verbatim is too clinical
    sc = b['session_count']
    if sc == 1:
        session_str = "came to the site once"
    elif sc == 2:
        session_str = "came back to the site twice (two separate visits)"
    elif sc <= 4:
        session_str = f"returned to the site {sc} times (separate visits)"
    else:
        session_str = f"been searching on the site {sc} times — clearly still looking"
    lines.append(f"VISIT PATTERN (translate naturally — never say 'sessions'): {session_str}")

    # Seller-buyer situation
    if b["sell_before_buy"]:
        lines.append("\nIMPORTANT: This lead needs to sell their current home before buying. "
                     "Acknowledge this — it's actually workable and they may not know that.")

    # Registration property
    if b["registration_prop"]:
        p = b["registration_prop"]
        addr  = f"{p.get('street')}, {p.get('city')}"
        price = f"${_safe_int(p.get('price')):,}"
        lines.append(f"\nREGISTERED ON: {addr} ({price}) — this was the original hook property.")

    # Intent signals — internal label only; never expose platform name to consumer
    if b["intent_signals"]:
        lines.append(f"\nBEHAVIOR SIGNALS (do NOT say 'Ylopo' in the email — use 'my home search website'): {'; '.join(b['intent_signals'])}")

    # IDX search links — for email 2 "gap" strategy only
    # Do NOT include links in the email body. Use this data to describe what you found
    # in email 2 ("Found 6 homes in Chesapeake matching your search — want me to send them?")
    # The actual link goes in the NEXT email AFTER they reply.
    if search_urls:
        lines.append("\nIDX SEARCH DATA (for email 2 gap tease — describe, don't link):")
        for su in search_urls:
            lines.append(f'  {su["label"]} → {su["url"]}')

    return "\n".join(lines)


def _md_links_to_html(text):
    """Convert [label](url) markdown links → HTML anchor tags.

    Claude writes links in markdown format when given IDX search URLs.
    This converts them before rendering so they're clickable in the email.
    """
    import re
    return re.sub(
        r'\[([^\]]+)\]\((https?://[^\)]+)\)',
        r'<a href="\2" style="color:#1a5fb4;text-decoration:underline">\1</a>',
        text,
    )


def _render_html(body_text):
    """
    Render a personal-email style HTML — NOT a marketing template.

    Looks like a real email from a person: clean white background, plain
    readable font, no colored header blocks. Logo lives in the signature
    footer only — subtle brand presence without screaming 'campaign'.

    This format consistently outperforms marketing templates for:
    - Primary inbox placement (fewer HTML signals = lower spam score)
    - Reply rates (feels like a real person wrote it)
    - Open rates for cold/warm outreach
    """
    paragraphs = body_text.strip().split("\n\n")
    html_parts = []
    for para in paragraphs:
        stripped = _md_links_to_html(para.strip())
        if stripped.startswith("P.S"):
            # P.S. gets a subtle separator
            html_parts.append(
                f'<p style="margin:20px 0 0;padding-top:16px;border-top:1px solid #e8e8e8;'
                f'font-size:14px;color:#555;line-height:1.7">'
                f'{stripped.replace(chr(10), "<br>")}</p>'
            )
        else:
            html_parts.append(
                f'<p style="margin:0 0 16px;font-size:15px;line-height:1.8;color:#222">'
                f'{stripped.replace(chr(10), "<br>")}</p>'
            )

    body_html_inner = "\n".join(html_parts)

    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#ffffff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif">
<div style="max-width:560px;margin:0 auto;padding:32px 24px">

  <!-- Email body — plain personal style -->
  <div style="color:#222;font-size:15px;line-height:1.8">
    {body_html_inner}
  </div>

  <!-- Signature divider -->
  <div style="margin-top:32px;padding-top:20px;border-top:1px solid #e8e8e8">
    <!-- Logo — small, signature-style, not a marketing banner -->
    <img src="{LOGO_URL}" alt="Legacy Home Team" width="90"
         style="display:block;margin:0 0 10px;height:auto;opacity:0.9">
    <p style="margin:0;font-size:13px;color:#666;line-height:1.6">
      Barry Jenkins, Realtor &nbsp;|&nbsp; LPT Realty<br>
      (757) 919-8874 &nbsp;|&nbsp;
      <a href="https://www.legacyhomesearch.com"
         style="color:#666;text-decoration:none">www.legacyhomesearch.com</a><br>
      1545 Crossways Blvd, Chesapeake, VA 23320<br>
      <a href="mailto:{FROM_EMAIL}?subject=Unsubscribe"
         style="color:#999;font-size:11px;text-decoration:none">Unsubscribe</a>
    </p>
  </div>

</div>
</body></html>"""


# ---------------------------------------------------------------------------
# Sender
# ---------------------------------------------------------------------------

def send_email(to_email, subject, body_text, body_html, dry_run=False):
    """Send via SendGrid. Returns {status, sg_message_id}."""
    if dry_run:
        logger.info("[DRY RUN] Would send to %s | %s", to_email, subject)
        return {"status": "dry_run", "sg_message_id": None}

    api_key = os.environ.get("SENDGRID_API_KEY")
    if not api_key:
        raise RuntimeError("SENDGRID_API_KEY not set")

    from sendgrid import SendGridAPIClient
    from sendgrid.helpers.mail import (
        Mail, Email as SgEmail, Bcc,
        TrackingSettings, ClickTracking, OpenTracking,
    )

    msg = Mail(
        from_email=SgEmail(FROM_EMAIL, FROM_NAME),
        to_emails=to_email,
        subject=subject,
        plain_text_content=body_text,
        html_content=body_html,
    )
    # Reply-to: SendGrid inbound parse intercepts replies for sentiment routing
    msg.reply_to = SgEmail("reply@inbound.yourfriendlyagent.net", FROM_NAME)

    # BCC Barry on every email so he can see exactly what's going out
    msg.add_bcc(Bcc(FROM_EMAIL))

    # Disable click + open tracking — tracking URLs rewritten through sendgrid.net
    # are a major spam signal for Gmail/Outlook filters on lead-facing emails.
    tracking = TrackingSettings()
    tracking.click_tracking = ClickTracking(enable=False, enable_text=False)
    tracking.open_tracking = OpenTracking(enable=False)
    msg.tracking_settings = tracking

    sg = SendGridAPIClient(api_key)
    resp = sg.send(msg)
    sg_id = resp.headers.get("X-Message-Id") if resp.headers else None
    return {"status": "sent", "code": resp.status_code, "sg_message_id": sg_id}


# ---------------------------------------------------------------------------
# New Lead Immediate Mailer — "I caught you at the computer"
# ---------------------------------------------------------------------------

def generate_new_lead_email(person, behavior, tags, dry_run=False):
    """
    Generate a first-contact "caught at the computer" email for a brand-new lead.

    Tone: direct, real-time energy — like Barry just saw them pop up and grabbed his laptop.
    Includes a local Hampton Roads market insight and one clear CTA.
    No P.S. — immediacy is the whole vibe.
    """
    import anthropic
    import json, re

    _load_env()
    client = anthropic.Anthropic()

    first_name = (person.get("firstName") or "").strip() or "there"

    # Detect likely intent: seller vs. buyer
    # YLOPO Z-buyer tag signals a seller lead (home valuation form)
    seller_tags = {"ZLEAD", "Z_BUYER", "YLOPO_Z_BUYER", "SELLER", "LISTING_LEAD",
                   "HOME_VALUE", "HOME_VALUATION", "WHAT_IS_MY_HOME_WORTH"}
    is_likely_seller = any(t.upper().replace("-","_") in seller_tags for t in tags)

    # Build a compact behavior summary for the prompt
    lines = []
    if behavior.get("city"):
        lines.append(f"City of interest: {behavior['city']}")
    if behavior.get("price_min") or behavior.get("price_max"):
        lo = f"${behavior['price_min']:,}" if behavior.get("price_min") else ""
        hi = f"${behavior['price_max']:,}" if behavior.get("price_max") else ""
        price_str = f"{lo}–{hi}" if lo and hi else (lo or hi)
        lines.append(f"Price range: {price_str}")
    if behavior.get("view_count"):
        lines.append(f"Properties viewed: {behavior['view_count']}")
    if behavior.get("save_count"):
        lines.append(f"Properties saved: {behavior['save_count']}")
    if behavior.get("most_viewed"):
        mv = behavior["most_viewed"]
        addr = mv.get("street") or mv.get("address") or ""
        if addr:
            lines.append(f"Most-viewed property: {addr} ({behavior.get('most_viewed_ct',1)}x)")
    if behavior.get("search_areas"):
        lines.append(f"Search areas: {', '.join(behavior['search_areas'][:3])}")

    data_brief = "\n".join(lines) if lines else "Minimal data — lead just arrived. Use city/area if available."

    intent_context = (
        "This lead likely came through a home VALUATION form — they may be a homeowner "
        "considering selling, not a buyer. Frame the email around market knowledge and "
        "what homes are doing in their area, not about searching for homes to buy."
        if is_likely_seller else
        "This lead came through a home SEARCH site — they were looking at homes to buy "
        "in Hampton Roads. Frame around their home search and what's available in their area."
    )

    prompt = f"""You are writing a first-contact email from Barry Jenkins, realtor in Hampton Roads VA.

This lead JUST appeared in Barry's system — active within the last hour. Barry happened to see
them come through while he was at his computer. The email should feel like he grabbed his laptop
right then and wrote this in 90 seconds.

━━━━ VIBE ━━━━
"I caught you at the computer" — not a follow-up, not a campaign email. Real-time energy.
Direct and confident. Not salesy. Not desperate. Not scripted.
Sounds like a smart friend who happens to know Hampton Roads cold.

━━━━ LEAD INTENT ━━━━
{intent_context}

━━━━ LEAD DATA ━━━━
{data_brief}

━━━━ WHAT THE EMAIL MUST DO ━━━━
1. Open with a specific observation (their city, price range, or what they were looking at).
   Prove you actually saw their activity — not a generic blast.
2. One sentence of local Hampton Roads market intel that only an insider would know.
   (Inventory tightness, multiple-offer situations, a specific neighborhood trend, etc.)
3. One clear CTA — a direct question answerable in 2–5 words. Not "feel free to reach out."
   Something like: "Worth a quick call?" or "Want me to pull what's actually available?"

━━━━ HARD RULES ━━━━
- No P.S. — immediacy is the vibe; P.S. signals a campaign
- No "I hope this finds you well", "just checking in", "dream home", "perfect fit"
- No "I noticed" — lead with the observation itself
- Fragments fine. Contractions always. 3–5 sentences max.
- Signature added automatically — stop before the sign-off.

FORMAT:
- First line: first name + comma only. ("Sarah,")
- Blank line
- Body: 3–5 sentences. CTA at the end.

SUBJECT LINES (3 options — 3–6 words, feel like a text from a saved contact):
- Best: property address, "[City] — quick question", just their name
- No ALL CAPS, no emojis, direct beats clever

OUTPUT (JSON only, no markdown fences):
{{
  "subject_options": ["option 1", "option 2", "option 3"],
  "body": "{first_name},\\n\\n[3–5 sentences]"
}}"""

    if dry_run:
        logger.info("[DRY RUN] Would generate new lead email for %s", first_name)
        return {
            "subject_options": ["[dry run] quick question", "[dry run] saw you come through", "[dry run] Hampton Roads"],
            "body": f"{first_name},\n\n[DRY RUN — email not generated]"
        }

    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=400,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)
    return json.loads(raw)


def run_new_lead_mailer(dry_run=True):
    """
    Check Shark Tank (pond 4) for leads created in the last NEW_LEAD_LOOKBACK_MINUTES
    minutes who haven't received the new_lead_immediate email yet, and fire the
    "I caught you at the computer" opener after a NEW_LEAD_EMAIL_DELAY_MINUTES delay.

    Runs every 5 minutes via scheduler. Typically processes 0–3 leads per run.
    """
    _load_env()

    import db as _db
    from fub_client import FUBClient
    from config import (SHARK_TANK_POND_ID, NEW_LEAD_EMAIL_DELAY_MINUTES,
                        NEW_LEAD_LOOKBACK_MINUTES)

    _db.ensure_pond_email_log_table()
    client = FUBClient()
    now = datetime.now(timezone.utc)

    # Window: leads created between (now - lookback) and (now - delay)
    # The delay buffer is what makes it feel human instead of instant.
    window_start = now - timedelta(minutes=NEW_LEAD_LOOKBACK_MINUTES)
    min_age      = now - timedelta(minutes=NEW_LEAD_EMAIL_DELAY_MINUTES)

    # Fetch new leads in Shark Tank created since window_start
    new_leads = client.get_people(
        pond_id=SHARK_TANK_POND_ID,
        created_since=window_start,
        limit=50,
    )

    eligible = [
        p for p in new_leads
        if _parse_iso(p.get("created", "")) <= min_age  # old enough to not feel instant
    ]

    if not eligible:
        logger.debug("New lead mailer: no eligible leads this run")
        return {"checked": len(new_leads), "sent": 0}

    sent = 0
    for person in eligible:
        pid   = person.get("id")
        first = person.get("firstName") or ""
        last  = person.get("lastName") or ""
        name  = f"{first} {last}".strip() or f"ID:{pid}"
        tags  = person.get("tags") or []

        # Already sent the immediate email?
        if _db.has_received_new_lead_immediate(pid):
            logger.debug("Skipping %s — already got new_lead_immediate", name)
            continue

        # Already in the drip? (had any pond email)
        history = _db.get_lead_email_history(pid)
        if history["emails_sent"] > 0:
            logger.debug("Skipping %s — already in drip (%d emails)", name, history["emails_sent"])
            continue

        # Get email address
        emails = person.get("emails") or []
        to_email = next(
            (e["value"] for e in emails if e.get("isPrimary") or e.get("status") == "Valid"),
            None
        )
        if not to_email and emails:
            to_email = emails[0].get("value")
        if not to_email:
            logger.debug("Skipping %s — no email", name)
            continue

        # Pull IDX events — new leads may have very few, that's OK
        events = client.get_events_for_person(pid, days=7, limit=50)
        behavior = analyze_behavior(events, tags)

        # Generate the email
        try:
            email_data = generate_new_lead_email(person, behavior, tags, dry_run=dry_run)
        except Exception as e:
            logger.error("New lead email generation failed for %s: %s", name, e)
            continue

        subject_options = email_data.get("subject_options", [])
        subject = subject_options[0] if subject_options else f"Quick question — {first or 'you'}"
        body_text = email_data.get("body", "")

        # Build HTML version (same as drip mailer)
        body_html = _render_html(body_text)

        print(f"\n  [NEW LEAD] {name} (ID: {pid})")
        print(f"    Email: {to_email}")
        print(f"    Created: {person.get('created', 'unknown')}")
        print(f"    Subject: {subject}")

        # Log before send
        log_id = _db.log_pond_email(
            person_id=pid, person_name=name, email_address=to_email,
            subject=subject, strategy="new_lead_immediate",
            leadstream_tier="NEW_LEAD",
            behavior_summary=f"views:{behavior.get('view_count',0)} saves:{behavior.get('save_count',0)}",
            sequence_num=1, dry_run=dry_run, sg_message_id=None,
        )

        try:
            result = send_email(to_email, subject, body_text, body_html, dry_run=dry_run)
        except Exception as e:
            logger.error("Send failed for new lead %s: %s", name, e)
            continue

        if result.get("sg_message_id") and log_id:
            _db.update_pond_email_sg_id(log_id, result["sg_message_id"])

        sent += 1
        logger.info("New lead immediate email sent to %s (%s)", name, to_email)

    return {"checked": len(new_leads), "eligible": len(eligible), "sent": sent}


def _parse_iso(ts):
    """Parse an ISO 8601 timestamp string to a UTC-aware datetime."""
    if not ts:
        return datetime.min.replace(tzinfo=timezone.utc)
    ts = ts.rstrip("Z")
    try:
        dt = datetime.fromisoformat(ts)
    except ValueError:
        return datetime.min.replace(tzinfo=timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_pond_mailer(dry_run=True, person_id=None, limit=None, daily_cap=None):
    """
    Main entry point. Processes LeadStream pond leads and sends personalized emails.

    daily_cap: if set, check how many emails have already been sent today (ET) and
               cap this run so the daily total doesn't exceed that number.
               Mon-Fri = 45, Sat/Sun = 30.

    1. Pull tagged pond leads from FUB
    2. Skip leads emailed within cooldown period
    3. Pull their IDX events
    4. Analyze behavior
    5. Select email strategy
    6. Generate email with Claude
    7. Send via SendGrid
    8. Log to DB
    """
    # Load .env for local dev
    _load_env()

    import db as _db
    from fub_client import FUBClient
    from config import LEADSTREAM_ALLOWED_POND_IDS

    _db.ensure_pond_email_log_table()

    # Daily cap enforcement — count emails already sent today (ET) and reduce
    # this run's limit to stay within the daily ceiling.
    if daily_cap is not None and not dry_run:
        already_sent = _db.count_pond_emails_today()
        remaining = daily_cap - already_sent
        if remaining <= 0:
            logger.info("Daily cap of %d reached (%d sent). Skipping run.", daily_cap, already_sent)
            print(f"[POND MAILER] Daily cap of {daily_cap} reached ({already_sent} sent today). Skipping.")
            return {"skipped": True, "reason": "daily_cap_reached", "daily_cap": daily_cap, "already_sent": already_sent}
        if limit is None or remaining < limit:
            limit = remaining
            logger.info("Daily cap %d — %d sent today — capping this run at %d.", daily_cap, already_sent, limit)

    # Auto-resolve any zombie jobs from previous Railway redeploys
    stale = _db.timeout_stale_pond_jobs(max_minutes=30)
    if stale:
        logger.info("Auto-resolved %d stale job(s) on startup", stale)

    client = FUBClient()
    now = datetime.now(timezone.utc)

    print(f"\n{'='*60}")
    print(f"  Pond Mailer — {'DRY RUN' if dry_run else 'LIVE'}")
    print(f"  {now.strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}\n")

    # Pull recent leads from allowed ponds sorted by LeadStream-equivalent signal:
    # most recently active leads first (same intent as LeadStream score ordering).
    # We limit to the first POND_FETCH_LIMIT leads per pond — enough candidates
    # to fill our run without paginating 2000+ records at 0.35s/request.
    POND_FETCH_LIMIT = 200  # candidates per pond; covers 9 sends easily
    all_pond_leads = []
    seen_ids = set()
    for pond_id in sorted(LEADSTREAM_ALLOWED_POND_IDS):
        leads = client.get_people_recent(pond_id=pond_id, limit=POND_FETCH_LIMIT)
        for lead in leads:
            lid = lead.get("id")
            if lid and lid not in seen_ids:
                seen_ids.add(lid)
                all_pond_leads.append(lead)

    # Sort by lastActivity descending — mirrors LeadStream score ordering.
    # Most-recently-active leads are highest priority for outreach.
    if not person_id:
        all_pond_leads.sort(
            key=lambda p: p.get("lastActivity") or "0000-00-00",
            reverse=True
        )

    # Filter to single person if specified
    if person_id:
        all_pond_leads = [p for p in all_pond_leads if p.get("id") == person_id]
        if not all_pond_leads:
            person = client.get_person(person_id)
            if person:
                all_pond_leads = [person]

    logger.info("Found %d pond leads to evaluate (ponds %s)",
                len(all_pond_leads), sorted(LEADSTREAM_ALLOWED_POND_IDS))

    sent = 0
    skipped_cooldown = 0
    skipped_no_email = 0
    skipped_no_activity = 0
    skipped_no_strategy = 0
    skipped_generation_error = 0
    max_to_process = limit or MAX_PER_RUN

    # Hard cap on how many leads we'll check per run — prevents runaway loops
    # on large ponds (each event fetch = 1 API call at 0.35s rate limit).
    # Check at most 100 leads per run; we only need MAX_PER_RUN emails.
    MAX_CANDIDATES = 100
    candidates_checked = 0

    for person in all_pond_leads:
        if sent >= max_to_process:
            logger.info("Reached max per run (%d)", max_to_process)
            break
        if not person_id and candidates_checked >= MAX_CANDIDATES:
            logger.info("Checked %d candidates — stopping to stay fast", MAX_CANDIDATES)
            break

        pid   = person.get("id")
        first = person.get("firstName") or ""
        last  = person.get("lastName") or ""
        name  = f"{first} {last}".strip() or f"ID:{pid}"
        tags  = person.get("tags") or []

        # Get email address
        emails = person.get("emails") or []
        to_email = next(
            (e["value"] for e in emails if e.get("isPrimary") or e.get("status") == "Valid"),
            None
        )
        if not to_email:
            to_email = emails[0]["value"] if emails else None

        if not to_email:
            logger.debug("Skipping %s — no email address", name)
            skipped_no_email += 1
            continue

        # Opt-out check — lead replied negatively to a previous email
        if "PondMailer_Unsubscribed" in tags:
            logger.debug("Skipping %s — opted out", name)
            skipped_cooldown += 1
            continue

        # Sequence check — max 3 emails without a reply, then 30-day quiet period
        history = _db.get_lead_email_history(pid)
        if history["suppressed"]:
            logger.debug("Skipping %s — sequence complete (%d emails, no reply, in quiet period)",
                         name, history["emails_sent"])
            skipped_cooldown += 1
            continue
        if history["has_replied"]:
            logger.debug("Skipping %s — replied, agent handling", name)
            skipped_cooldown += 1
            continue

        sequence_num = history["sequence_num"]

        # Phase-aware cooldown: sprint emails 1-3 = 3 days, drip emails 4-9 = 15 days
        cooldown = DRIP_COOLDOWN_DAYS if sequence_num >= 4 else EMAIL_COOLDOWN_DAYS
        days_ago = _db.days_since_last_pond_email(pid)
        if days_ago is not None and days_ago < cooldown:
            logger.debug("Skipping %s — emailed %.1f days ago (need %dd)", name, days_ago, cooldown)
            skipped_cooldown += 1
            continue

        # Pull IDX events — each fetch = 1 FUB API call, so count against cap
        candidates_checked += 1
        events = client.get_events_for_person(pid, days=60, limit=100)

        # Any IDX event counts for qualification (Viewed Page, Viewed Property,
        # Property Saved, Registration). We write from search filters if no
        # specific property views exist.
        idx_events = [e for e in events if e.get("type")]

        if len(idx_events) < MIN_EVENTS_TO_EMAIL:
            logger.debug("Skipping %s — only %d IDX events", name, len(idx_events))
            skipped_no_activity += 1
            continue

        # Analyze behavior
        behavior = analyze_behavior(events, tags)

        # Determine LeadStream tier from tags
        leadstream_tier = "POND"
        for tier_tag in ("AI_NEEDS_FOLLOW_UP", "HANDRAISER", "YPRIORITY",
                          "AI_VOICE_NEEDS_FOLLOW_UP", "Y_HOME_3_VIEW"):
            if tier_tag in tags:
                leadstream_tier = tier_tag
                break

        # Select strategy
        strategy, priority = select_strategy(behavior, leadstream_tier, tags)

        if strategy == "none" or priority < 20:
            logger.debug("Skipping %s — no compelling email strategy (score %d)", name, priority)
            skipped_no_strategy += 1
            continue

        # Log what we found
        if sequence_num <= 3:
            seq_label = f"sprint #{sequence_num}/3"
        else:
            drip_n = sequence_num - 3
            seq_label = f"drip #{drip_n}/6 · {'listing' if _is_listing_drop(sequence_num) else 'content'}"
        print(f"\n  [{strategy.upper()}] {name} (ID: {pid}) — {seq_label}")
        print(f"    Email: {to_email}")
        print(f"    Tier: {leadstream_tier} | Views: {behavior['view_count']} | Saves: {behavior['save_count']}")
        if behavior["most_viewed"]:
            p = behavior["most_viewed"]
            print(f"    Most viewed: {p.get('street')} ({behavior['most_viewed_ct']}x)")
        if behavior["sell_before_buy"]:
            print(f"    ⚠ Sell-before-buy")
        if behavior["hours_since_last"]:
            hrs = behavior["hours_since_last"]
            print(f"    Last active: {int(hrs)}h ago" if hrs < 48 else f"    Last active: {int(hrs/24)}d ago")

        # Generate email with Claude
        try:
            email_data = generate_email(person, behavior, strategy, leadstream_tier,
                                        sequence_num=sequence_num, dry_run=dry_run)
        except Exception as e:
            logger.error("Claude generation failed for %s: %s", name, e)
            skipped_generation_error += 1
            continue

        print(f"    Subject: {email_data['subject']}")
        if dry_run:
            print(f"\n    --- PREVIEW ---")
            print("    " + email_data["body_text"].replace("\n", "\n    ")[:400])
            print(f"    --- END PREVIEW ---\n")

        # ── Log BEFORE send ─────────────────────────────────────────────────────
        # Writing the cooldown record first prevents duplicate emails if SendGrid
        # times out or Railway restarts mid-send. The sg_message_id is backfilled
        # after a confirmed send; it stays NULL on failure (acceptable trade-off).
        price_min = behavior.get('price_min') or 0
        price_max = behavior.get('price_max') or 0
        behavior_summary = (
            f"Views:{behavior['view_count']} Saves:{behavior['save_count']} "
            f"Price:${price_min:,}-${price_max:,} "
            f"Cities:{','.join(behavior['cities'])}"
        )
        log_id = _db.log_pond_email(
            person_id=pid,
            person_name=name,
            email_address=to_email,
            subject=email_data["subject"],
            strategy=strategy,
            leadstream_tier=leadstream_tier,
            behavior_summary=behavior_summary,
            sequence_num=sequence_num,
            dry_run=dry_run,
            sg_message_id=None,
        )

        # ── Send ────────────────────────────────────────────────────────────────
        try:
            result = send_email(
                to_email=to_email,
                subject=email_data["subject"],
                body_text=email_data["body_text"],
                body_html=email_data["body_html"],
                dry_run=dry_run,
            )
        except Exception as e:
            logger.error("Send failed for %s: %s", name, e)
            # Log record already exists → cooldown applies → no duplicate on retry
            continue

        # Backfill SendGrid message ID now that send is confirmed
        if result.get("sg_message_id") and log_id:
            _db.update_pond_email_sg_id(log_id, result["sg_message_id"])

        sent += 1
        print(f"    ✓ {'[DRY RUN] Would send' if dry_run else 'Sent'}")

        # Brief pause between leads to stay friendly to FUB rate limits
        import time as _t; _t.sleep(1.5)

    print(f"\n{'='*60}")
    print(f"  Done: {sent} {'would send' if dry_run else 'sent'} | "
          f"Cooldown: {skipped_cooldown} | No activity: {skipped_no_activity} | "
          f"No email: {skipped_no_email} | No strategy: {skipped_no_strategy} | "
          f"Generation error: {skipped_generation_error}")
    print(f"{'='*60}\n")

    return {
        "sent":                 sent,
        "skipped_cooldown":          skipped_cooldown,
        "skipped_no_email":          skipped_no_email,
        "skipped_no_activity":       skipped_no_activity,
        "skipped_no_strategy":       skipped_no_strategy,
        "skipped_generation_error":  skipped_generation_error,
        "dry_run":              dry_run,
    }


def _load_env():
    """Load .env for local dev. No-op if already set or file missing."""
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Pond Mailer: LeadStream email marketing")
    parser.add_argument("--apply",  action="store_true", help="Send live emails (default: dry run)")
    parser.add_argument("--person", type=int, default=None, help="FUB person ID for single-lead test")
    parser.add_argument("--limit",  type=int, default=None, help="Max leads to process")
    args = parser.parse_args()

    run_pond_mailer(
        dry_run=not args.apply,
        person_id=args.person,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
