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
DRIP_COOLDOWN_DAYS = 10   # 5 gaps × 10 days = 50-day drip (emails 4-9)
# Was 15 days — tightened because active searchers go cold waiting that long.

# Minimum IDX events needed to write a meaningful email
# Counts ALL event types (page views, property views, saves, registration)
MIN_EVENTS_TO_EMAIL = 1

# Max leads to email per run
# Weekdays: 3x/day (8am, 1pm, 6pm ET) → 45 max/day
# Weekends: 2x/day (Sat 10am+3pm, Sun 1pm+6pm) → 30 max/day
MAX_PER_RUN = 15

# Ylopo tags that indicate behavioral intent or give context about who the
# lead is and how they came in. These descriptions get surfaced to Claude in
# the email brief so the copy can reference specifics instead of writing
# generic nurture. IMPORTANT: never mention "Ylopo", "rAIya", ad names, or
# platform names in the actual email — Claude translates these into natural
# language ("from our Facebook ad about military homes" → "saw you came in
# through our military buyer ad"; "my assistant" for rAIya).
YLOPO_INTENT_TAGS = {
    # --- Behavioral / site-activity signals ---
    "Y_REQUESTED_TOUR":       "submitted a tour request — ready to see a home in person",
    "Y_FAVORITED_LISTING":    "favorited a listing — attached enough to save it",
    "Y_HOME_3_VIEW":          "viewed a specific home 3+ times",
    "Y_SHARED_LISTING":       "shared a listing (likely with a partner/spouse)",
    "Y_ADDRESS_FOUND":        "their current home address was identified",
    "Y_REMARKETING_ENGAGED":  "re-engaged via remarketing ads",
    "Y_AI_PRIORITY":          "flagged as high-interest across multiple signals",
    "RETURNED":               "returned to site after a gap",
    "REQUESTED_RATE":         "asked for a real payment estimate — budgeting seriously",
    "NEW_NUMBER":             "fixed a previously-bad phone number — actively re-engaging",
    # --- Seller Report engagement (Ylopo Seller Experience 2.0) ---
    "Y_SELLER_REPORT_VIEWED":         "viewed a home value report",
    "Y_SELLER_REPORT_ENGAGED":        "clicked a call-to-action on their home value report",
    "Y_SELLER_3_VIEW":                "returned to the seller report 3+ times in a week — high seller intent",
    "Y_SELLER_CASH_OFFER_REQUESTED":  "clicked the cash offer button on the seller report",
    "Y_SELLER_LEARN_MORE_EQUITY":     "submitted a message via the equity CTA",
    "Y_SELLER_TUNE_HOME_VALUE":       "tuned/adjusted their home value estimate — thinking about the number",
    "Y_SELLER_UNDERSTAND_TREND":      "submitted a message via the home-trend CTA",
    "Y_SELLER_NEW_HOME_UPGRADES":     "submitted details about upgrades they've made",
    "Y_SELLER_SEARCH_MORE_PROPERTIES":"used the search widget inside the seller report",
    "Y_SELLER_VIEWED_SIMILAR_LISTINGS":"clicked a similar-listing example on the report",
    "Y_SELLER_HEATMAP_INQUIRY":       "submitted a Private Showing inquiry",
    "Y_SELLER_EMAIL_AGENT":           "clicked the Email Agent CTA on the seller report",
    "Y_SELLER_CALL_AGENT":            "clicked the Call Agent CTA on the seller report",
    "Y_SELLER_SELF_GENERATED":        "generated their own home value report",
    "SELLER_ALERT":                   "enrolled in seller alerts — latent seller signal",
    # --- AI text signals ---
    "AI_NEEDS_FOLLOW_UP":     "had a text conversation with Barry's assistant — flagged for immediate follow-up",
    "AI_ENGAGED":             "currently in a text conversation with Barry's assistant",
    "AI_RESPONDED":           "responded to a text from Barry's assistant (not an opt-out)",
    # --- AI voice call outcomes (what happened on the call) ---
    "AI_VOICE_NEEDS_FOLLOW_UP":                   "had a voice conversation with Barry's assistant — flagged for human follow-up",
    "ISA_TRANSFER_UNSUCCESSFUL":                  "agreed to be connected to Barry, then got stuck on hold and disconnected — strong intent but friction hit",
    "ISA_ATTEMPTED_TRANSFER_REALTOR_UNAVAILABLE": "was ready to be connected to Barry, but no agent was available to take the call",
    "ISA_ATTEMPTED_TRANSFER":                     "a call transfer to Barry was attempted but failed mid-process",
    "DECLINED_BY_REALTOR":                        "the transfer was declined on the agent side — treat like a missed connection",
    "CALLBACK_SCHEDULED":                         "explicitly requested a specific callback time — honor that commitment",
    "NURTURE":                                    "said they're interested but not ready to talk yet — play the long game",
    "VOICEMAIL":                                  "the last call attempt reached voicemail",
    "NO_ANSWER":                                  "the last call attempt got no answer",
    "HUNG_UP":                                    "hung up during a previous call attempt — lead the email gently",
    "LEAD_UNAVAILABLE":                           "picked up but asked to be called back later",
    "GHOST_CALL":                                 "the last call attempt got no one on the line",
    "BUSY_TONE":                                  "the last call attempt hit a busy tone",
    # --- Hand-raise / high-intent signals ---
    "HANDRAISER":             "raised their hand — expressed direct interest on the site",
    "YPRIORITY":              "completed a high-intent action on the site",
    "HVB":                    "came in on a high-value buyer campaign",
    # --- Direct Connect / dynamic registration explicit asks ---
    "call_now=yes":                   "explicitly asked to be called within the hour at registration",
    "call_now=another_time":          "wants a call, flexible on timing",
    "call_for_preapproval=yes":       "asked to be called about preapproval",
    "call_about_homes=yes":           "asked to be called about home search (not immediately)",
    "second_opinion_preapproval=yes": "already preapproved, wants a second opinion on rates",
    "cash_buyer=yes":                 "self-identified as a cash buyer",
    "cash_offer=Yes":                 "requested a cash offer on their home (Direct Connect)",
    # --- Timeline signals ---
    "timeline=within 90 days":  "timeline: within 90 days",
    "timeline=within90days":    "timeline: within 90 days",
    "timeline=within 6 months": "timeline: within 6 months",
    "timeline=within6months":   "timeline: within 6 months",
    "timeline=over 6 months":   "timeline: more than 6 months out — long-game nurture",
    "timeline=over6months":     "timeline: more than 6 months out — long-game nurture",
    # --- Preapproval / equity status ---
    "PREAPPROVED_FOR_LOAN=YES":     "already preapproved for a loan",
    "PREAPPROVED_FOR_LOAN=NO":      "not yet preapproved — may need a lender referral",
    "USE_HOME_EQUITY=YES":          "planning to use home equity to buy",
    "USE_HOME_EQUITY=NO":           "not planning to use home equity",
    "USE_HOME_EQUITY=I Don't Own a Home": "doesn't own a home currently",
    "DO_YOU_OWN_A_HOME=YES":        "currently owns a home",
    "DO_YOU_OWN_A_HOME=NO":         "does not currently own a home",
    "I_NEED_TO_SELL_BEFORE_I_CAN_BUY": "needs to sell their current home before buying the next",
    "SELL_BEFORE_BUY_NO":           "does NOT need to sell before buying",
    # --- Dynamic reg classifiers ---
    "BUYER":                        "identified as a buyer at registration",
    "SELLER":                       "identified as a seller at registration",
    # --- Source / ad-subtype context (how they came in) ---
    # The actual ad context helps Claude write an opener that reflects the lead's
    # entry point — "saw you came through our military-buyer campaign" reads very
    # differently from "saw you browsing luxury homes".
    "HNW":                    "came in on a luxury (high-net-worth) ad",
    "HOU":                    "came in on a homeowner trade-up ad (selling to buy a bigger home)",
    "HOD":                    "came in on a homeowner trade-down ad (selling to buy a smaller home)",
    "LTM":                    "came in on a 'likely to move' ad",
    "NC":                     "came in on a new-construction ad",
    "M":                      "came in on a military-buyer ad",
    "RB":                     "came in on an out-of-town / relocation-buyer ad",
    "CNEW":                   "came in on a city-specific listings ad",
    "CUS":                    "came in on a custom marketing campaign",
    "VIDEO":                  "came in through a video / 3D-tour listing ad",
    "LISTING_ROCKET":         "came in through a carousel listing ad",
    "YLOPO_FACEBOOK":         "registered on a Facebook ad",
    "YLOPO_ADWORDS":          "registered on a Google search ad",
    "YLOPO_GBP_ADS":          "registered on a Google Business Profile ad",
    "YLOPO_LSA":              "called in through a Google Local Service Ad",
    "YLOPO_ORGANIC":          "registered organically on the home-search site",
    "YLOPO_DIRECT_CONNECT_FB":  "Direct Connect lead from Facebook (dynamic registration)",
    "YLOPO_DIRECT_CONNECT_PPC": "Direct Connect lead from Google (dynamic registration)",
    "YLOPO_REACTIVATED":      "re-engaged via a database-texting blast",
}

SELL_BEFORE_BUY_TAGS = {
    "I_NEED_TO_SELL_BEFORE_I_CAN_BUY",
    "sell_before_buy=Yes",
}


def _email_suppression_tags(tags):
    """Return the list of tags that should prevent an email from being sent.

    Combines LeadStream-level suppression (opt-outs, disqualifications) with
    pond-email-specific suppression (NO_EMAIL, NO_MARKETING, listing-alert
    unsubscribes). If anything is returned, we must NOT email this lead.
    """
    from config import LEADSTREAM_SUPPRESSION_TAGS, POND_EMAIL_EXTRA_SUPPRESSION_TAGS
    block_set = LEADSTREAM_SUPPRESSION_TAGS | POND_EMAIL_EXTRA_SUPPRESSION_TAGS
    return [t for t in (tags or []) if t in block_set]


LOGO_URL = "https://web-production-3363cc.up.railway.app/static/logo-blue.png"
PHYSICAL_ADDRESS = "LPT Realty · 1545 Crossways Blvd Chesapeake, VA 23320"
FROM_EMAIL = "barry@yourfriendlyagent.net"
FROM_NAME  = "Barry Jenkins | Legacy Home Team"

# Base URL for one-click unsubscribe links (uses BASE_URL env var, same as Railway public URL)
_APP_BASE_URL = os.environ.get("BASE_URL", "https://web-production-3363cc.up.railway.app").rstrip("/")


def _unsub_url(email: str) -> str:
    """Return a one-click unsubscribe URL for the given email address.
    Encodes the email as base64url so it survives email client link mangling.
    The /unsubscribe endpoint in app.py decodes it, tags PondMailer_Unsubscribed
    in FUB, and shows a confirmation page — no compose window required.
    """
    import base64
    token = base64.urlsafe_b64encode(email.lower().encode()).decode().rstrip("=")
    return f"{_APP_BASE_URL}/unsubscribe?e={token}"


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


def _hg_watch_url(raw_mp4_url: str) -> str:
    """
    Wrap a HeyGen MP4 URL in the Railway /v/<token> landing page.

    Uses base64url token so heygen.com never appears in email bodies —
    a significant spam signal since heygen.com is a known bulk-video SaaS.
    Desktop and mobile both get our HTML5 player page.
    """
    from heygen_client import make_video_landing_url as _make_landing
    return _make_landing(raw_mp4_url)


# ---------------------------------------------------------------------------
# Z-Buyer sequence guide — for cash-offer leads (ZLEAD / Z_BUYER / YLOPO_Z_BUYER)
# These are homeowners who requested a cash offer, not buyers searching for homes.
# Tone: calm, competent, educational. Barry's edge = cash AND licensed agent = both options.
# No listing-drop emails — Z-buyers are sellers, no IDX links apply.
# ---------------------------------------------------------------------------
_Z_BUYER_SEQUENCE_GUIDE = {

    1: """EMAIL 1 — Already handled by the new_lead_immediate mailer ("caught at the computer").
If this runs, treat it as a re-introduction: got their cash offer request, can do cash or list, want to run both numbers?
3 sentences max. Direct. No hype.""",

    2: """EMAIL 2 — The Net Sheet.

Most sellers make one mistake: they compare OFFER PRICES instead of NET PROCEEDS.
Barry's job here: flip the frame. The number on the contract isn't what you walk away with.

What eats into the listing number:
- Agent commissions (typically 5-6%)
- Repairs sellers are usually asked to make after inspection
- Carrying costs during the 30-45 day closing period (mortgage, taxes, insurance)
- Risk: financed buyers fall through — appraisal issues, loan denials, cold feet

A cash offer at a slightly lower price can net MORE in total. Barry can show them that math.
Barry's unique angle: he can do BOTH — show the cash number AND run the listing math in the same conversation. Most people in their inbox can only show one.

Rules:
- Don't make it a lecture. Make it a "here's something most people don't realize."
- One key insight: net proceeds ≠ offer price
- Weave in Barry's edge naturally: he shows both numbers, most can't
- CTA must be a direct question: "Want me to put together a side-by-side for your place?"
- 60-80 words. Teaching voice, not sales voice.
- No urgency language. No pressure.
- Last sentence ends with a question mark. Non-negotiable.
""",

    3: """EMAIL 3 — The Timeline Check.

Z-buyers had urgency when they submitted. Either:
  A) The urgency is still there → great, let's talk
  B) Something changed → life moved on, that's fine, say so

This email doesn't assume either. It genuinely asks.
Short. Warm. Not a guilt trip, not a chase. Just a neutral check-in.

Rules:
- 2-3 sentences max. Under 40 words.
- No recap of earlier emails.
- Tone: genuinely unbothered, curious, not desperate.
- The question should make it easy to reply either way — yes OR no both work.

Example (personalize — do not copy verbatim):
  "Just checking in — did the move end up happening, or is the timeline still open?
   Either way is fine, just want to make sure I'm not bothering you if things resolved."
""",

    4: """EMAIL 4 — Certainty Has a Price.

Cash buyers close. Financed buyers sometimes don't.

This email is about the hidden cost of choosing a financed buyer over a cash offer:
appraisal contingencies, loan denials, inspection re-negotiations, a 30-45 day wait
that can stretch to 60+ when deals fall through and restart.

Barry's angle: you might get $10k more from a financed buyer on paper — but if that deal falls through 3 weeks before closing, you're back to square one, plus 2 months of carrying costs. That math often flips.

Rules:
- Story-first if possible: "Had a seller in [city] last year who passed on a cash offer to wait for more..." — short, anonymized, real.
- One key insight: certainty has dollar value. Spell it out simply.
- Soft question at the end: "What matters more right now — top dollar or a guaranteed close?"
- 80-100 words. Teaching > selling.
- No urgency. No pressure. Just a smart friend running the real math.
""",

    5: """EMAIL 5 — The Monthly Cost of Waiting.

Every month you keep a home you want to sell costs real money.

The "holding cost" math for a typical Hampton Roads home:
- Mortgage: the biggest line
- Property taxes (pro-rated monthly)
- Homeowner's insurance
- Maintenance / utilities if vacant
- Opportunity cost on the equity sitting in the property

For most sellers this runs $1,500–$2,500+/month in pure carrying cost.
Waiting three months to see if the market goes higher means betting $4,500-$7,500
that the market will move enough to cover those costs AND net more. It often doesn't.

Rules:
- Present the math as a "here's what I always tell people" — not a threat
- Don't use their specific numbers (you don't have them) — use ranges
- Make it concrete: "$1,800/month in holding costs" hits harder than "it costs money to wait"
- Soft CTA: "Want me to run the actual holding cost estimate for your place?"
- 80-100 words. Smart friend voice, not investor pitch.
""",

    6: """EMAIL 6 — Story. The client who almost waited too long.

Anonymized story. Real situation. Don't make it up — use a believable Hampton Roads scenario.

A homeowner wanted to sell. Got a solid cash offer. Decided to hold out and list for more.
Market shifted (happens). Three months later they accepted less than the original cash offer,
PLUS three months of carrying costs. Net result: came out $15-20k behind where they would have been.

Barry's framing: "I share this not to scare you — most people navigate this fine.
But it comes up more often than sellers expect, and I'd rather you have the full picture."

Rules:
- Story first: setup → decision → what happened → what they wished they'd known
- One sentence of honest self-awareness: "Sometimes listing is clearly the right call. Sometimes cash wins. It depends on your specific situation."
- Soft close: "Where are you in the process right now?"
- 90-110 words. No pitch. No urgency.
""",

    7: """EMAIL 7 — Updated Numbers.

Markets move. It's been a couple months. Comparable sales in their area have shifted —
either up or down — and their cash offer estimate AND listing estimate may have changed.

Barry can give them fresh numbers with no commitment required. Both numbers — cash AND what
listing might net — because that's the comparison most people never see side by side.

Rules:
- Frame it as: "The market's moved since we first connected — wanted to give you a heads-up."
- Don't make it sound like you're tracking them. Make it sound like you're paying attention to the market on their behalf.
- Reference Barry's edge: he can run both numbers (cash offer AND listing estimate) in the same conversation. This is the differentiator. One sentence, natural, not a pitch.
- 60-80 words. Light. No pressure.
- CTA MUST be a direct question ending with a question mark. Examples:
    "Want me to pull fresh numbers on your place?"
    "Worth a quick look at what both options net right now?"
    "Want me to run the updated math on your place?"
  DO NOT end with a statement like "Worth getting a fresh number if you're still weighing it." That is not a CTA. It's a sentence that lets them ignore you.
""",

    8: """EMAIL 8 — The Comparison Nobody Makes.

Most people compare: Cash offer price vs. Listing price.
Barry compares the total picture: net proceeds after EVERYTHING.

Side A (cash): offer price, minus nothing (no commissions, no repairs, no contingencies)
Side B (listing): offer price, minus 5-6% commissions, minus repair credits from inspection,
minus carrying costs during 30-45 day close, minus risk of deal falling through

The gap between these two paths is almost always smaller than sellers expect.
Sometimes it flips completely in favor of cash.

Rules:
- Present this as "here's the comparison most agents don't walk you through" — not a lecture, a gift
- Keep it concrete with sample math (use generic Hampton Roads numbers if no specifics available)
- One CTA: "Worth 15 minutes to look at the real numbers side by side?"
- 90-110 words. Educational. Confident but not pushy.
""",

    9: """EMAIL 9 — The Warm Door.

It's been a few months. No guilt. No chase. Just: the door is still open.

Barry's tone: genuinely unbothered. He's done fine. He's not desperate for this listing.
He just wants them to know the option exists if their situation changes.

Life changes: job moves, divorces, financial shifts, family situations — these create urgency
that wasn't there before. When that happens, Barry wants to be the first name they think of.

Rules:
- Acknowledge the time without guilt-tripping: "It's been a while — hope things are going well."
- One sentence on what's changed (market, rates, their opportunity) — optional
- The core message: "If anything shifts on your end, I'm still here. Same offer, same options."
- No hard CTA. Just warmth and an open door.
- 60-80 words. This is a relationship maintenance email, not a sales email.
- Tone: the friend you want to call when you're finally ready.
""",
}


# ---------------------------------------------------------------------------
# Ylopo Prospecting Seller sequence — rAIya AI-converted homeowners.
# These leads talked to Ylopo AI about their home value and were live-transferred
# into FUB. They are NOT buyers. Never reference IDX or home searching.
# Hook: Barry is the human follow-up to the AI conversation.
# ---------------------------------------------------------------------------
_YLOPO_SELLER_SEQUENCE_GUIDE = {

    1: """EMAIL 1 — The Human Follow-Up.

This person spoke with our AI assistant about their home. They engaged.
Now Barry is the real human following up on that conversation.

The hook: "You spoke with my assistant about your home — I wanted to personally follow up."
This is warm, not cold. They said yes to a conversation. Remind them of that gently.

Rules:
- Reference the AI conversation naturally: "my assistant", "the conversation you had about your home"
- One specific question about where they are: timeline, situation, still thinking about it?
- DO NOT mention home searches, IDX, buying, or properties to view
- DO NOT sound like a cold outreach — this is a warm hand-off, not a pitch
- 2–3 sentences max. Under 50 words.
- No urgency. No "I'd love to help." Just: I got your conversation, I'm the person behind it.

Example tone (personalize — do not copy verbatim):
  "You spoke with my assistant a little while back about your home in [city].
   Just wanted to personally follow up — are you still thinking about making a move, or did the timing change?"
""",

    2: """EMAIL 2 — What's Happening in Their Market.

They didn't reply to email 1. Completely different angle — do NOT reference the prior email.

This email is market intel specific to their area. What homes like theirs are doing RIGHT NOW.
Feels like a note from someone who's been watching their neighborhood on their behalf.

Rules:
- Open with a specific, local observation about their city or neighborhood
- One useful insight — something a Hampton Roads homeowner would actually want to know
  (inventory, days on market, price trends, what's moving vs. sitting)
- Soft question: "Wondering if this changes your thinking at all?" or "Worth a conversation?"
- 60-80 words. Smart friend voice, not a market report.
- No links. No urgency. No P.S.
- Never reference the AI conversation or prior email.
""",

    3: """EMAIL 3 — The Clean Exit.

2-3 sentences. Under 35 words. This is the breakup.

Give them permission to stay quiet. Leave the door warm and wide open.

Rules:
- Tone: genuinely unbothered, warm, no guilt
- Don't say "this is my last email"
- Don't reference prior emails
- The message: timing's probably off, totally fine, I'll be here when it makes sense

Example (personalize — do not copy verbatim):
  "Sounds like the timing might not be right — totally get it. I'll check back in a few months
   unless something changes on your end."
""",

    4: """EMAIL 4 — First Drip: The Net Sheet Preview.

They didn't engage with the sprint. Long game now. No urgency, no pressure.
This email plants a seed: most homeowners don't know what they'd actually net.

The concept: the number on the "for sale" sign isn't what they walk away with.
After commissions, closing costs, and carrying time, the real number is often surprising.
Barry can show them that math with no commitment required.

Rules:
- Open with: "here's something most people don't think about when they're considering selling"
- One clear insight: offer price ≠ net proceeds
- Soft CTA: "Want me to put together a rough estimate for your place? No commitment, just numbers."
- 70-90 words. Teaching voice. No pressure.
- No links. This is purely educational.
""",

    5: """EMAIL 5 — Market Update.

It's been a while. The market has moved. Their home's value may have shifted.
Barry's been watching. He can give them updated numbers with zero commitment.

Rules:
- Frame as: "Wanted to give you a heads-up — the market in [their area] has shifted since we spoke."
- One concrete observation (inventory, price movement, buyer demand) — Hampton Roads specific
- Offer updated comps: "I can pull current numbers on your place in about 10 minutes if you're curious."
- 60-80 words. Light, informative, no pressure.
- Sounds like you're doing them a favor, not chasing a listing.
""",

    6: """EMAIL 6 — Story. The Seller Who Waited.

Anonymized real-ish situation. Someone who thought about selling, sat on it, and either
benefited from moving OR missed a window. Keep it honest — not a scare tactic.

Rules:
- Story first: setup → what they decided → what happened
- One line of honest reflection: "sometimes waiting is the right call — sometimes it isn't"
- Soft close: "Where are you in the process right now?"
- 80-100 words. No urgency. Just a thoughtful note from someone who's seen it play out both ways.
""",

    7: """EMAIL 7 — The Holding Cost Math.

Every month they stay in a home they want to eventually sell costs real money.
Mortgage, taxes, insurance, maintenance. It adds up. The math is usually surprising.

Rules:
- Frame as "here's what I always walk sellers through" — not a threat
- Use Hampton Roads ranges: "$1,600–$2,400/month in carrying costs is typical"
- "Waiting three months hoping the market goes up means betting $5-7k that it will go up enough to cover that"
- Soft CTA: "Want me to run the actual estimate for your place?"
- 80-100 words. Smart friend voice. Concrete numbers.
""",

    8: """EMAIL 8 — The Comparison Most Agents Skip.

Cash vs. listing: most people compare offer prices. Barry compares net proceeds after everything.
This is the comparison that actually matters, and most sellers never see it laid out clearly.

Rules:
- Present this as "here's what I walk every seller through before they decide"
- Two columns in plain language: cash path (fast, certain, lower gross) vs. listing path (higher gross, slower, more variables)
- The gap is almost always smaller than sellers expect
- "Worth 15 minutes to look at both side by side?"
- 90-110 words. Educational. Confident but never pushy.
""",

    9: """EMAIL 9 — The Warm Door.

It's been months. No guilt. No chase. Just: I'm still here, door's still open.

Life changes — job moves, family shifts, financial situations. When something changes,
Barry wants to be the first name they think of.

Rules:
- Acknowledge time without guilt: "It's been a while — hope things are going well."
- One line on what's changed in the market (optional)
- Core message: "If anything shifts on your end, I'm still here. Same conversation, same options."
- No hard CTA. Just warmth and an open door.
- 60-75 words. This is a relationship maintenance email. Tone: the advisor you call when you're finally ready.
""",
}


def _get_ylopo_seller_seq_guide(sequence_num):
    """Return the right sequence guide for Ylopo Prospecting seller emails."""
    if sequence_num in _YLOPO_SELLER_SEQUENCE_GUIDE:
        return _YLOPO_SELLER_SEQUENCE_GUIDE[sequence_num]
    # Beyond 9: cycle between market update / story / math
    cycle = [
        _YLOPO_SELLER_SEQUENCE_GUIDE[5],
        _YLOPO_SELLER_SEQUENCE_GUIDE[6],
        _YLOPO_SELLER_SEQUENCE_GUIDE[7],
    ]
    return cycle[sequence_num % 3]


def _is_sms_blocked_source(person):
    """Return True if this lead's source blocks automated SMS.

    The rule is source-based, not tag-based:

      BLOCKED:
        • "Ylopo Prospecting" — rAIya homeowner leads; Barry follows up personally;
                                opt-in process is less explicit, higher TCPA risk

      ALLOWED (text them):
        • "Ylopo" / "Ylopo PPC+" / "Ylopo GBP Ads" — buyer leads (bottom of funnel)
        • "Ylopo Seller"  — dedicated seller campaign leads; SMS is appropriate
        • "Zbuyer"        — urgent sellers requesting cash offers; often distressed;
                            fast outreach is critical
        • "Qazzoo"        — buyer leads
        • Any Ylopo buyer also tagged as sell-to-buy (SELLER, Y_SELLER_*,
          USE_HOME_EQUITY=YES, I_NEED_TO_SELL_BEFORE_I_CAN_BUY) — these are
          buyers who need to find their next home; SMS is exactly right

    Source list is in config.SMS_BLOCKED_SOURCES so it can be updated without
    touching this file if FUB source strings ever change.
    """
    from config import SMS_BLOCKED_SOURCES
    source = (person.get("source") or "").strip()
    return source in SMS_BLOCKED_SOURCES


def _is_ylopo_prospecting_seller(person, tags):
    """Return True if this is a valid Ylopo Prospecting rAIya-converted seller lead.

    Requires BOTH:
      1. Source = "Ylopo Prospecting" (configurable via config.YLOPO_PROSPECTING_SOURCES)
      2. Any AI text or AI voice conversation tag — a conversation did happen and
         we have a "my assistant" hook to reference. Older Ylopo Prospecting leads
         without any AI tag predate the rAIya conversation feature.
    """
    from config import YLOPO_PROSPECTING_SOURCES
    source = (person.get("source") or "").strip()
    if source not in YLOPO_PROSPECTING_SOURCES:
        return False
    ai_tags = {
        "AI_NEEDS_FOLLOW_UP",
        "AI_ENGAGED",
        "AI_RESPONDED",
        "AI_VOICE_NEEDS_FOLLOW_UP",
        "ISA_TRANSFER_UNSUCCESSFUL",
        "ISA_ATTEMPTED_TRANSFER_REALTOR_UNAVAILABLE",
        "ISA_ATTEMPTED_TRANSFER",
        "DECLINED_BY_REALTOR",
        "CALLBACK_SCHEDULED",
        "NURTURE",
        "VOICEMAIL",
        "LEAD_UNAVAILABLE",
    }
    return any(t in ai_tags for t in (tags or []))


def _build_seller_brief(first_name, person, tags):
    """Build a compact behavioral brief for Ylopo Prospecting seller leads.

    These leads have no IDX data — they're homeowners, not buyers.
    The brief draws from the FUB person record: home address, city, tags.
    """
    lines = [f"LEAD: {first_name}"]
    lines.append("LEAD TYPE: Ylopo Prospecting Seller — homeowner who spoke with Barry's AI assistant about their home value. NOT a buyer. Do NOT reference home searches, IDX, or properties for sale.")

    # Home address — Ylopo Prospecting leads should have this from rAIya conversation
    street = person.get("streetAddress") or person.get("street") or ""
    city   = person.get("city") or ""
    state  = person.get("state") or "VA"
    if street and city:
        lines.append(f"HOME ADDRESS: {street}, {city}, {state} — reference their specific home/neighborhood")
    elif city:
        lines.append(f"CITY: {city} — reference their area/neighborhood in Hampton Roads")
    else:
        lines.append("LOCATION: Hampton Roads area (city unknown — write generically for the region)")

    # Relevant tags (no Ylopo platform names in emails)
    seller_signals = []
    # AI text conversation signals
    if "AI_NEEDS_FOLLOW_UP" in tags:
        seller_signals.append("had a text conversation with Barry's assistant about their home (strong engagement)")
    if "AI_ENGAGED" in tags:
        seller_signals.append("is currently in an active text conversation with Barry's assistant")
    if "AI_RESPONDED" in tags:
        seller_signals.append("replied to a text from Barry's assistant")
    # AI voice presence + call outcomes — these change the email angle substantially
    if "AI_VOICE_NEEDS_FOLLOW_UP" in tags:
        seller_signals.append("had a voice conversation with Barry's assistant about their home")
    if "ISA_TRANSFER_UNSUCCESSFUL" in tags:
        seller_signals.append(
            "AGREED to be connected to Barry on a live call, then got stuck on hold and disconnected — "
            "acknowledge the friction directly ('sorry we missed each other — here's a direct path')"
        )
    if "ISA_ATTEMPTED_TRANSFER_REALTOR_UNAVAILABLE" in tags or "DECLINED_BY_REALTOR" in tags:
        seller_signals.append(
            "was ready to be connected to Barry but the call didn't go through on our end — "
            "acknowledge it openly ('we tried to connect you live and dropped the ball — fully on us')"
        )
    if "ISA_ATTEMPTED_TRANSFER" in tags:
        seller_signals.append(
            "a transfer to Barry was attempted on the call but didn't complete — "
            "treat like a missed connection and offer a direct path forward"
        )
    if "CALLBACK_SCHEDULED" in tags:
        seller_signals.append(
            "explicitly requested a callback at a specific time — the email should respect that "
            "commitment, not override it ('still on for [time]' or 'confirming our call')"
        )
    if "NURTURE" in tags:
        seller_signals.append(
            "said they're interested in selling in the future but not ready yet — long-game nurture, "
            "no urgency, no hard CTA — just stay useful"
        )
    if "VOICEMAIL" in tags:
        seller_signals.append(
            "a previous call attempt reached voicemail — email should be casual follow-up, "
            "not 'we've been trying to reach you' desperation energy"
        )
    if "LEAD_UNAVAILABLE" in tags:
        seller_signals.append(
            "picked up a previous call but asked to be reached later — brief email, "
            "respect that they're busy, make re-connecting easy"
        )
    # Seller-report engagement signals
    if "Y_SELLER_REPORT_VIEWED" in tags:
        seller_signals.append("viewed a home value report")
    if "Y_SELLER_REPORT_ENGAGED" in tags:
        seller_signals.append("clicked a call-to-action on their home value report — actively engaged with the numbers")
    if "Y_SELLER_3_VIEW" in tags:
        seller_signals.append("returned to their home value report 3+ times in a week — high seller intent")
    if "Y_SELLER_CASH_OFFER_REQUESTED" in tags:
        seller_signals.append("clicked the cash offer button — investigating speed/certainty, not retail listing")
    if "Y_SELLER_LEARN_MORE_EQUITY" in tags:
        seller_signals.append("submitted a message via the equity CTA — curious about what their home is worth net")
    if "Y_SELLER_TUNE_HOME_VALUE" in tags:
        seller_signals.append("adjusted/tuned their home value estimate — engaged, thinking about the number")
    if "Y_SELLER_UNDERSTAND_TREND" in tags:
        seller_signals.append("submitted a message about home-value trends — curious where the market is going")
    if "Y_SELLER_NEW_HOME_UPGRADES" in tags:
        seller_signals.append("told us about upgrades they've made — preparing the value story for a future sale")
    if "Y_SELLER_HEATMAP_INQUIRY" in tags:
        seller_signals.append("submitted a Private Showing inquiry — thinking about interested parties already")
    if "Y_SELLER_EMAIL_AGENT" in tags:
        seller_signals.append("clicked the Email Agent CTA on their home value report — reached out directly")
    if "Y_SELLER_CALL_AGENT" in tags:
        seller_signals.append("clicked the Call Agent CTA on their home value report — wants a phone conversation")
    if "Y_SELLER_VIEWED_SIMILAR_LISTINGS" in tags:
        seller_signals.append("clicked into similar-listing comps on their report — comparing to the market")
    if "Y_SELLER_SEARCH_MORE_PROPERTIES" in tags:
        seller_signals.append("used the search widget inside their seller report — possibly looking at a next move")
    if "Y_SELLER_SELF_GENERATED" in tags:
        seller_signals.append("generated their own home value report — self-starter, not prompted")
    if "SELLER_ALERT" in tags:
        seller_signals.append("enrolled in seller alerts — staying plugged in to market data")
    if "YPRIORITY" in tags or "Y_AI_PRIORITY" in tags:
        seller_signals.append("high-priority seller signal")
    if seller_signals:
        lines.append(f"\nSELLER SIGNALS: {'; '.join(seller_signals)}")

    lines.append("\nCRITICAL REMINDERS:")
    lines.append("- Never say 'Ylopo', 'rAIya', or any platform name — say 'my assistant'")
    lines.append("- Never reference home searches, listings to buy, or IDX activity")
    lines.append("- This person engaged about SELLING their home, not buying one")

    return "\n".join(lines)


# Sequence-specific angle instructions fed to Claude
_SEQUENCE_GUIDE = {
    1: """EMAIL 1 — The Pattern Interrupt + Local Intel.

Three short paragraphs. Feels like Barry grabbed his phone when he saw them come through.
NOT a stripped-down one-liner — specificity requires space. Cell phone energy, not template energy.

━━ PARAGRAPH 1: The behavioral observation ━━
Lead with something specific enough they wonder how you knew.
Use the most specific data point available, in this priority order:
  1. Property address they registered on or viewed multiple times
  2. Specific price range + city ("Virginia Beach in the $600K range")
  3. Search behavior translated naturally ("browsed without clicking into anything yet")
NEVER open with just "Hampton Roads" — that tells them nothing and could be any lead.
Translate what the behavior tells you about WHERE THEY ARE mentally:
  "browsed without clicking" = still orienting, getting a feel for what's out there
  "came back to the same listing 3x" = attached but something is holding them back
  "saved 2 homes and went quiet" = something changed, life got in the way

━━ PARAGRAPH 2: Local market intelligence ━━
One piece of hyper-local knowledge about their specific area or price range.
This is what makes them think "this agent actually knows Hampton Roads."
It should be something they couldn't get from Zillow — specific enough to their neighborhood,
price point, or property type that it reads as insider knowledge, not generic market copy.
Good examples (no em or en dashes):
  "Inventory south of Shore Drive is tight. Homes near [their street] tend to sit a few days longer though. That's leverage if you time it right."
  "Most buyers in the $400s in Chesapeake are getting pushed toward [area] right now. VB inventory is slim."
  "The Great Neck corridor is underpriced relative to the rest of VB right now. Most buyers skip it because it doesn't show up at the top of the price filters."
  "Landstown is moving faster than the VB average right now. Worth being on the new listing alert for that zip."

━━ PARAGRAPH 3: One easy CTA ━━
One question. Yes/no or answerable in 2-5 words.
Something that makes them feel like replying is easier than not.

RULES:
- DO NOT use "just" / "reach out" / "let me know" / "happy to help" / "I'd love to"
- Never open with "I noticed" — lead with the observation itself
- Every sentence references THIS person, not a buyer persona
- Subject line: use the property address or price + city — never the lead's name alone

P.S. FOR EMAIL 1: Include a P.S. about 70% of the time (higher than other emails).
The P.S. is the "I know this market cold" closer. One sentence. Specific. Conversational.
Something about their area, their price range, or a hidden opportunity they wouldn't find on their own.
""",

    2: """EMAIL 2 — The Listing Drop. Give them houses.

They didn't reply to email 1. Completely different angle — do NOT reference the prior email.

Buyers don't click because of the agent. They click because of the house. This email is
short, personal, and leads with listings — not with Barry.

Rules:
- 40-55 words max. One warm sentence of curation context, then the link, then one yes/no question.
- Frame the link as personal curation: "pulled these for you specifically" not "here are some listings"
- Anchor text must be specific: "3bd homes in Chesapeake under $350k" not "click here" or "view listings"
- One sentence of insider edge — something about that market or those homes they wouldn't get from Zillow
- End with a single yes/no question. Pick the strongest option for this specific lead:
    "Anything worth a closer look?" — neutral, low commitment
    "Want to tour any of these?" — slightly higher intent signal
    "One of these stood out to me for your situation — want me to flag it?" — best when you can genuinely make a recommendation; raises curiosity without giving everything away and almost always gets a yes
- DO NOT explain yourself. DO NOT mention email 1. DO NOT pad with agent intro.
- The link IS the value. Get out of the way.

Subject lines should be about what you found: "found a few in Norfolk" or "6 homes in your range"

Good example format (personalize to actual data — do not copy verbatim):
  "Pulled some homes in Chesapeake that match what you've been searching — 3bd, in your price range,
   most listed in the last week or two.

   [3bd homes in Chesapeake under $400k](url)

   Anything catch your eye?"
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

    # ── Phase 1.5: Bridge (email 4) ──────────────────────────────────────────
    # 3-day cadence — fires on sprint timing, 3 days after Email 3 (the breakup).
    # Not a drip email. A quiet re-entry that makes the breakup feel even MORE genuine.
    # Email 4 has no CTA, no question — just a useful piece of intel that arrived.

    4: """EMAIL 4 — The Bridge. One thing I forgot to mention.

They got the breakup. The door is technically closed. This email re-opens it with
zero pressure — just something specific and useful that arrives 3 days later, as if
Barry was thinking about something else and remembered a detail about their area.

The effect: it makes the breakup feel even more real. Barry genuinely moved on.
But then one specific market fact came to mind and he shared it anyway. That's the
action of someone who actually knows the market — not someone chasing a commission.

Rules:
- 35-50 words max. This is NOT a drip email. Short is the point.
- One hyper-local piece of market intelligence. Specific to their city, price range, or neighborhood.
- NO CTA. No question. No "let me know." Just the intel, then nothing.
- Tone: genuinely unbothered, almost offhand. Like a text Barry typed while thinking about something else.
- DO NOT reference prior emails. Do not acknowledge the gap. Do not apologize for reaching out again.
- Do NOT ask about their timeline, status, or readiness. That would undercut the whole energy.
- No P.S. Silence is more powerful here.

Subject line: should feel like an afterthought — "one more thing" or just the city name or a
  specific detail. Not a sales hook. Not a question.

Example tone (personalize to their city/price range — do not copy verbatim):
  "[First name],

  One thing worth knowing about [their area] right now — [specific market insight].

  Thought you'd want to have that in your back pocket."

Or:
  "[First name],

  Quick note — [specific local fact about their price range or neighborhood]. Not asking you
  to do anything with it. Just thought you'd want to know."
""",

    # ── Phase 2: Long-term Drip ───────────────────────────────────────────────
    # Emails 5-9. 10-day cadence. Alternates listing drops (5,7,9) and content (6,8).
    # Lead didn't engage with the sprint or bridge — now we play the long game.

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


def _get_z_buyer_seq_guide(sequence_num):
    if sequence_num in _Z_BUYER_SEQUENCE_GUIDE:
        return _Z_BUYER_SEQUENCE_GUIDE[sequence_num]
    # Beyond 9: cycle between story/math/update angles
    cycle = [_Z_BUYER_SEQUENCE_GUIDE[6], _Z_BUYER_SEQUENCE_GUIDE[7], _Z_BUYER_SEQUENCE_GUIDE[8]]
    return cycle[sequence_num % 3]


_HR_CITIES = {
    "virginia beach", "chesapeake", "norfolk", "portsmouth", "hampton",
    "newport news", "suffolk", "williamsburg", "james city", "york county",
    "poquoson", "isle of wight", "smithfield", "gloucester", "mathews",
    "surry", "franklin", "emporia",
}

# ZIP → city lookup for common Hampton Roads zips stored as tags by Ylopo
_HR_ZIP_TO_CITY = {
    "23451": "Virginia Beach", "23452": "Virginia Beach", "23453": "Virginia Beach",
    "23454": "Virginia Beach", "23455": "Virginia Beach", "23456": "Virginia Beach",
    "23457": "Virginia Beach", "23459": "Virginia Beach", "23460": "Virginia Beach",
    "23461": "Virginia Beach", "23462": "Virginia Beach", "23464": "Virginia Beach",
    "23301": "Accomack", "23320": "Chesapeake", "23321": "Chesapeake",
    "23322": "Chesapeake", "23323": "Chesapeake", "23324": "Chesapeake",
    "23325": "Chesapeake", "23420": "Chesapeake",
    "23501": "Norfolk", "23502": "Norfolk", "23503": "Norfolk", "23504": "Norfolk",
    "23505": "Norfolk", "23507": "Norfolk", "23508": "Norfolk", "23509": "Norfolk",
    "23510": "Norfolk", "23511": "Norfolk", "23513": "Norfolk", "23517": "Norfolk",
    "23518": "Norfolk", "23523": "Norfolk",
    "23701": "Portsmouth", "23702": "Portsmouth", "23703": "Portsmouth",
    "23704": "Portsmouth", "23707": "Portsmouth", "23708": "Portsmouth",
    "23660": "Hampton", "23661": "Hampton", "23662": "Hampton", "23663": "Hampton",
    "23664": "Hampton", "23665": "Hampton", "23666": "Hampton", "23669": "Hampton",
    "23601": "Newport News", "23602": "Newport News", "23603": "Newport News",
    "23604": "Newport News", "23605": "Newport News", "23606": "Newport News",
    "23607": "Newport News", "23608": "Newport News",
    "23434": "Suffolk", "23435": "Suffolk", "23436": "Suffolk", "23437": "Suffolk",
    "23438": "Suffolk", "23439": "Suffolk",
    "23185": "Williamsburg", "23186": "Williamsburg", "23187": "Williamsburg",
    "23188": "Williamsburg", "23168": "Toano",
}


def _city_from_tags(tags):
    """Extract the best available city name from a lead's FUB tags.

    Ylopo stores location data as tags — both city names ("Virginia Beach",
    "Norfolk") and raw ZIP codes ("23453", "23503"). FUB's addresses array
    is usually empty for Ylopo leads, so this is the most reliable source
    of location context for personalized email/video scripts.

    Returns a city string or "" if nothing found.
    """
    city_from_name = ""
    city_from_zip = ""
    for tag in (tags or []):
        t = tag.strip()
        # ZIP code tag → city lookup
        if t.isdigit() and len(t) == 5 and t in _HR_ZIP_TO_CITY:
            if not city_from_zip:
                city_from_zip = _HR_ZIP_TO_CITY[t]
        # City name tag (exact or starts-with match)
        elif t.lower() in _HR_CITIES:
            city_from_name = t  # use as-is (preserves capitalisation from tag)
            break  # prefer first city-name tag found
        # Tags like "Virginia Beach city, VA" or "Looking for homes in: NORFOLK, VA"
        elif "city, va" in t.lower():
            city_from_name = t.split("city,")[0].strip().title()
            break
        elif "looking for homes in:" in t.lower():
            raw = t.lower().replace("looking for homes in:", "").strip()
            city_part = raw.split(",")[0].strip().title()
            if not city_from_name:
                city_from_name = city_part
    return city_from_name or city_from_zip


def _is_z_buyer(tags, person=None):
    """Return True if this lead is a Z-buyer (cash offer request from Ylopo).

    Checks BOTH tags AND the lead source string so leads like Dixon's —
    who arrive from the 'Zbuyer' source without the expected tags — still
    route to the cash-offer sequence instead of the buyer IDX track.
    """
    z_tags = {"ZLEAD", "Z_BUYER", "YLOPO_Z_BUYER"}
    if any(t.upper().replace("-", "_") in z_tags for t in (tags or [])):
        return True
    if person:
        raw_source = (person.get("source") or "").strip().lower()
        # Normalise: strip spaces, hyphens, underscores for fuzzy matching
        src = raw_source.replace("-", "").replace(" ", "").replace("_", "")
        # Match known Z-buyer source strings only. Do NOT include bare "ylopo" —
        # that matches every Ylopo lead (buyers, sellers, etc.) not just cash-offer leads.
        if src in {"zbuyer", "ylopozbuyer", "ylopozbuyer2", "zbuyerlead"}:
            return True
        # Also catch partial matches like "Zbuyer Lead", "Z-Buyer Source", "DixonZbuyer" etc.
        # but only when "zbuyer" substring is actually present.
        if "zbuyer" in src:
            return True
    return False


def _is_listing_drop(sequence_num, tags=None, person=None):
    """True for listing-drop emails (2, 5, 7, 9, 11…) — these include IDX links.
    Email 2 is an early listing drop: buyers want houses, not more agent intro.
    Seller leads (Z-buyer, Ylopo Prospecting) never get listing drops."""
    if _is_z_buyer(tags, person):
        return False
    if person and _is_ylopo_prospecting_seller(person, tags or []):
        return False
    if sequence_num == 2:
        return True
    return sequence_num >= 5 and sequence_num % 2 == 1


def generate_email(person, behavior, strategy, leadstream_tier,
                   sequence_num=1, dry_run=False):
    """
    Generate a personalized email using Claude.

    sequence_num: 1+ — controls phase, tone, and angle.
      1: reply sprint — behavioral observation + local intel (no link)
      2: listing drop — short, IDX link, personal curation angle
      3: reply sprint — breakup (no link)
      4,6,8: drip content (longer, warm, no links)
      5,7,9: drip listing drop (short, IDX link included)

    Lead type routing (in priority order):
      Z-buyer (ZLEAD/Z_BUYER)          → cash offer + both-options track
      Ylopo Prospecting + AI tag       → rAIya seller follow-up track
      Everyone else                    → buyer IDX track
    Returns {subject, body_text, body_html} or raises on failure.
    """
    first_name = person.get("firstName") or "there"
    tags = person.get("tags", [])
    is_z      = _is_z_buyer(tags, person)
    is_seller = _is_ylopo_prospecting_seller(person, tags)

    # Seller leads never get listing drops
    listing_drop = _is_listing_drop(sequence_num, tags, person)
    search_urls  = build_lead_search_urls(behavior) if listing_drop else []

    # Build the right brief for each lead type
    if is_seller:
        brief = _build_seller_brief(first_name, person, tags)
    else:
        brief = _build_behavioral_brief(first_name, behavior, strategy, leadstream_tier, tags,
                                        search_urls=search_urls)
    if is_z:
        seq_guide = _get_z_buyer_seq_guide(sequence_num)
    elif is_seller:
        seq_guide = _get_ylopo_seller_seq_guide(sequence_num)
    else:
        seq_guide = _get_seq_guide(sequence_num)

    # Phase label for logging / dry-run display
    _lead_type = "z-buyer" if is_z else ("ylopo-seller" if is_seller else "")
    if sequence_num <= 3:
        phase_label = f"{(_lead_type + ' ') if _lead_type else ''}sprint #{sequence_num}/3"
    elif sequence_num == 4:
        phase_label = f"{(_lead_type + ' ') if _lead_type else ''}bridge"
    else:
        drip_num = sequence_num - 4
        phase_label = f"{(_lead_type + ' ') if _lead_type else ''}drip #{drip_num} ({'listing' if listing_drop else 'content'})"

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
    if is_z:
        # Z-buyer emails: all content, no links, educational/teaching voice
        if sequence_num <= 3:
            length_rule = "30-50 words. Short and direct — they're overwhelmed, don't add to the noise."
            max_tokens  = 400
        else:
            length_rule = "70-110 words. Substantive enough to teach something, short enough to read in 30 seconds."
            max_tokens  = 600
        link_rule = "NO LINKS. This is a seller lead — no IDX searches apply."
    elif is_seller and sequence_num <= 3:
        length_rule = "50-80 words for emails 1-2. 25-40 words for email 3. No links. Cell phone energy — short paragraphs, nothing polished."
        link_rule   = "NO LINKS. This is a homeowner lead, not a buyer."
        max_tokens  = 500
    elif sequence_num == 1:
        # Email 1 — the Tayler format: observation + local intel + CTA.
        # Specificity costs words. 25-word cap was producing generic output.
        length_rule = """3 short paragraphs. Cell phone energy — feels like Barry typed this in 90 seconds.
Para 1 (1-2 sentences): Behavioral observation with specific detail — property address, exact price + city, search pattern. NEVER just 'Hampton Roads.'
Para 2 (1-2 sentences): One piece of local market intel about THEIR specific neighborhood/price range — something they couldn't get from Zillow.
Para 3 (1 sentence): One easy question — yes/no or answerable in 2-5 words.
Total: 65-110 words. Specificity is worth every word. Short paragraphs, not short email."""
        link_rule   = "NO LINKS — ever. They click instead of reply."
        max_tokens  = 500
    elif sequence_num == 2:
        length_rule = "40-55 words. Short and personal. One curation line, the IDX link, one question. Nothing else."
        link_rule   = "INCLUDE the IDX link from the brief as [descriptive anchor text](url). Anchor text must be specific — city, beds, price. The link is the value."
        max_tokens  = 400
    elif sequence_num == 3:
        length_rule = "20-35 words. The breakup. Short is the point. Never so clipped it reads as rude."
        link_rule   = "NO LINKS — ever. This is the breakup email. A link undercuts the message."
        max_tokens  = 400
    elif sequence_num == 4:
        # Bridge email — fires 3 days after the breakup, same sprint cadence.
        # No CTA, no question. Pure intel. Makes the breakup feel real.
        length_rule = "35-50 words. One specific market insight. No question, no CTA, no P.S. Offhand, like Barry just thought of something."
        link_rule   = "NO LINKS — ever. A link would signal sales intent and undercut the entire energy of this email."
        max_tokens  = 350
    elif listing_drop:
        length_rule = "40-55 words. Short. Get out of the way and let the link do the work."
        link_rule   = "INCLUDE the IDX link from the brief as [descriptive anchor text](url). This is the value."
        max_tokens  = 400
    else:
        length_rule = "90-120 words. Long enough to be genuinely useful, short enough to finish in 30 seconds."
        link_rule   = "NO LINKS. The value is the insight, not a search result."
        max_tokens  = 600

    # Lead-type-specific context injected into the prompt
    if is_z:
        lead_type_context = """
━━━━ WHO THIS PERSON IS ━━━━
This is a SELLER lead — they requested a cash offer on their home.
They are NOT looking to buy. Do NOT reference home searches, IDX, or properties to buy.

Barry's position: he is both a licensed realtor AND a cash buyer.
His edge: he can show them TWO options — cash now or list for potentially more.
Most cash buyers can only offer one. Barry can show the full picture.

━━━━ TONE FOR SELLER LEADS ━━━━
- Calm, competent, educational. Not a pitch. Like a smart advisor.
- Teaching voice: "here's what most people don't realize"
- No WE-BUY-HOUSES energy. No exclamation points.
- No "I'd love to help" or "feel free to reach out"
- If you're sharing math or data, make it concrete and Hampton Roads-specific
"""
    elif is_seller:
        lead_type_context = """
━━━━ WHO THIS PERSON IS ━━━━
This is a HOMEOWNER who spoke with Barry's AI assistant about their home value and was
transferred to Barry's team. They engaged — they're warm, not cold. They are NOT a buyer
browsing homes. Do NOT reference home searches, IDX, listings for sale, or anything buyer-related.

Barry is the human follow-up to that AI conversation. He's the real person behind it.
His value: he knows their neighborhood, he knows what homes like theirs are selling for, and
he can give them real numbers — no pressure, no commitment required.

━━━━ TONE FOR YLOPO PROSPECTING SELLER LEADS ━━━━
- Warm, competent, personal. Like the friend who happens to be a top Hampton Roads agent.
- Reference "my assistant" naturally — never say Ylopo or rAIya or any platform name.
- No "WE BUY HOUSES" energy, no cash offer push — this is a traditional listing conversation.
- Teaching voice. Market intel feels like a gift, not a sales move.
- Never: "I'd love to help", "feel free to reach out", "happy to assist"
"""
    else:
        lead_type_context = ""

    _phase_label_str = "Z-Buyer Drip" if is_z else ("Ylopo Seller" if is_seller else ("Listing Drop" if listing_drop else ("Reply Sprint" if sequence_num <= 3 else ("Bridge" if sequence_num == 4 else "Long-term Drip"))))

    prompt = f"""You are writing a nurture email from Barry Jenkins, realtor in Hampton Roads VA.
{lead_type_context}
PHASE: {_phase_label_str}
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

━━━━ VOICE (Barry Jenkins — "Too Nice for Sales") ━━━━
Barry is a 20-year Hampton Roads real estate veteran who built Virginia's #1 team by teaching,
not pushing. His entire philosophy: serve people genuinely, and the sales follow. He writes
like he talks — conversational, warm, a little self-deprecating, never slick.

Rules for his voice:
- Short sentences. One thought, then a period. Like this.
- Fragments are fine. Contractions always.
- Teaching voice: "here's what I'd want to know if I were you" beats any sales pitch
- Genuinely curious questions, not rhetorical ones. He actually wants to know.
- A little wry. Self-aware. Never takes himself too seriously.
- Sounds like a text from a knowledgeable friend, not a marketing email.
- Never: "dream home", "perfect fit", "hot market", "reach out", "just checking in",
  "I hope this finds you well", "happy to help", "feel free to", "I'd love to"
- NEVER say "Ylopo" or "my home search website" as a phrase — say "my site" or reference
  the specific thing they were looking at
- Never open with "I noticed" — lead with the observation itself

PUNCTUATION RULES (critical for authenticity):
- NO em dashes (—) and NO en dashes (–). Ever. They look designed, not typed.
- Use a period and a new sentence instead. Or a comma.
- No semicolons. One thought per sentence, then stop.
- No ellipsis (...) unless it's genuinely trailing off, and even then sparingly.
- P.S. with no dash after it. Just "P.S. Great Bridge is seeing multiple offers..."

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
They should feel like their agent noticed what they were looking at and reached out personally.
Specific is good. Clinical is not. Generic is worst of all.

WHAT BARRY ACTUALLY SOUNDS LIKE (write this way):
  "You were looking at homes in Virginia Beach in the $600k range but hadn't clicked into anything yet.
   That tells me you're still getting a feel for what's out there."

  "You went back to that Chesapeake listing three times. Still on the fence, or did something change?"

  "Here's what I'd want to know if I were searching that price point right now..."

WHAT HE DOES NOT SOUND LIKE:
  "I noticed you were browsing homes in Hampton Roads — I'd love to help you find your dream home!"
  "As a top Hampton Roads agent, I wanted to reach out about your home search."
  Any sentence with an em dash, semicolon, or three-syllable adjective.

━━━━ TAG-AWARE COPYWRITING (translate, never name) ━━━━
The BEHAVIOR SIGNALS list in the brief below is derived from Ylopo tags. Never
name the source, the ad, the campaign, or the tag itself. Translate each signal
into a detail only someone paying attention would mention. The reader should
feel seen, not tracked.

NEVER SAY:
  "I see you came in through our Facebook ad" / "our luxury program"
  "you requested a callback" / "our system shows"
  "I noticed you're preapproved / a cash buyer"
IMPLY these through detail, word choice, and tone. Never by naming them.

ONE FRAMING SIGNAL. When 2+ signals fire, pick ONE to frame the email; the
others become invisible context (tone, word choice, pacing) — not sentences.
Example — [HNW + call_now=yes + timeline within 90d]:
  Wrong: three-line list naming all three.
  Right: frame on the call request. Luxury band lives in the tone (understated,
         round numbers, no exclamation). 90-day timeline lives in the CTA pace
         ("good window to see places this weekend?").

PER-SOURCE TONE (the invisible layer, not topics to mention):
  HNW (luxury)              understated. Round numbers. No exclamation. Adult prose.
  M (military)              direct. Timeline-respecting. PCS fluency if relevant.
                            Never say "thank you for your service." Assume competence.
  HOU / HOD (trade up/down) acknowledge sell-then-buy sequencing without lecturing.
  NC (new construction)     build-timeline awareness. Lender choice matters.
  RB (relo / out-of-town)   regional fluency sells the relationship. Answer what
                            a local knows that Zillow can't.
  LTM (likely-to-move)      warm, not decided. Useful > urgent. No push.
  YLOPO_FACEBOOK            lifestyle-emotional lead. Scene-first > stats-first.
  YLOPO_ADWORDS / PPC       researched, intentional. Data and specifics land.
  YLOPO_LSA (called in)     urgency was their signal. Match it. Phone-forward CTA.
  YLOPO_ORGANIC             they found you. Keep it humble.

SELLER-REPORT PSYCHOGRAPHIC READ (when Y_SELLER_* tags are in the brief):
  TUNE_HOME_VALUE           anchoring on the number. Data over emotion.
  CASH_OFFER_REQUESTED      speed > top dollar. Don't pitch the MLS flow.
  LEARN_MORE_EQUITY         net-proceeds thinker. "Take-home" not "list price."
  CALL_AGENT / EMAIL_AGENT  ready to talk. Don't re-pitch. Acknowledge, ease in.
  3_VIEW (returned 3+ times) add clarity, not pressure.
  UNDERSTAND_TREND          future-oriented. Trajectory > current comp.
  NEW_HOME_UPGRADES         they've invested. Reflect that in the number.

DIRECT-CONNECT EXPLICIT ASKS (when these tags are present, the email changes shape):
  call_now=yes              the call IS the email. Short. Confirm the window.
                            Don't re-introduce yourself.
  cash_buyer=yes            their offer is the differentiator. Don't make them
                            explain it. Don't mention financing.
  second_opinion_preapproval=yes  already has a lender. Never suggest one.
                            Position around the home, not the money.
  I_NEED_TO_SELL_BEFORE_I_CAN_BUY  sequencing is the whole problem. Acknowledge
                            that it's workable without making it a sales line.

Test: if this email could go to a lead who shares NONE of these tags and still
make sense, it's not specific enough. The tag is never named — but the copy
should feel impossible to send to anyone else.

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

━━━━ CTA ENFORCEMENT (mandatory — no exceptions) ━━━━
Every email must end with a QUESTION. Not a statement. Not a passive observation. A question.
Before outputting, read your last sentence. Does it end with a question mark? If not, rewrite it.

WRONG — passive endings that kill replies:
  "Worth getting a fresh number if you're still weighing it."
  "Something to think about as you consider your options."
  "Hampton Roads cash offers have tightened up since spring."
  "Either way, I'm here when you're ready."
  "Just wanted to touch base and see where things stand."
  Any sentence that ends with a period and lets them off the hook.

RIGHT — direct questions that make replying easier than not:
  "Want me to pull fresh numbers on your place?"
  "Still thinking about selling, or did something change?"
  "Worth a quick call to look at both options?"
  "Did the move end up happening, or is the timeline still open?"
  "Want me to put together a side-by-side for your place?"
  "Still searching in [city], or did something shift?"
  The question should be answerable in one word. "Yes." "No." "Still looking." Done.

━━━━ LOCAL INSIDER P.S. ━━━━
Email 1: include a P.S. ~70% of the time — it's the "I know this market cold" signal.
Emails 2-3: include ~30% of the time — silence works here too.
Drip emails: include ~50% of the time.

The P.S. is what separates this from any email a Zillow algorithm could send.
It should make them think: "how does this agent know this about my specific area?"

Rules:
- One sentence only. Conversational. Specific to their city, neighborhood, or price range.
- No em dashes, no en dashes. Just "P.S." then a space and the sentence.
- Never: a CTA, a credential, "don't hesitate to reach out", or a stat that could apply anywhere
- GOOD — feels like a text from a friend who sells real estate:
    "P.S. Great Bridge is seeing multiple offers on anything under $375k right now."
    "P.S. Suffolk's Harbour View area has been moving fast. Worth a look if Chesapeake is on your list."
    "P.S. A lot of buyers in the $400s are getting pushed toward Newport News. VB inventory is tight."
    "P.S. The Great Neck corridor is underpriced relative to the rest of VB right now — most buyers skip it without knowing."
    "P.S. Landstown is running hotter than the Virginia Beach average. Worth having new listing alerts set there if you haven't already."
    "P.S. Chesapeake schools are some of the best in Hampton Roads, if that's part of the decision."
- BAD: "P.S. — The Hampton Roads market is moving fast right now." (generic, could go to anyone, has a dash)

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
If you can't tell it was written for exactly this person, rewrite it.

FINAL QUALITY CHECK — run this before outputting:
1. Last sentence ends with a question mark? If no, rewrite it.
2. Could this exact email go to a different lead and still make sense? If yes, it's too generic — add the specific detail that makes it impossible to send to anyone else.
3. Does any sentence sound like a marketing email? Strip it. Barry sounds like a knowledgeable friend who noticed something, not a system that sent a campaign."""

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

    return {
        "subject":      subject,
        "body_text":    body_text,
        "body_html":    None,   # plain-text-only — HTML template removed for deliverability
        "all_subjects": subject_options,
    }


# ---------------------------------------------------------------------------
# SMS Body Generator (Claude-powered, purpose-built for text engagement)
# ---------------------------------------------------------------------------

def generate_sms_body(person, behavior, strategy, leadstream_tier,
                      tags=None, is_seller=False, is_z=False,
                      channel="sms_only", needs_optout=False, dry_run=False):
    """
    Generate a standalone SMS body (25-40 words) via Claude.

    This is NOT a condensed email. It's written from scratch specifically for
    the SMS channel — two sentences, curiosity gap, yes/no CTA, reads like
    Barry typed it from his truck in 30 seconds.

    channel values:
        "sms_only"  — this is the ONLY outreach (lead has no email address)
        "dual"      — SMS alongside an email on the same day (high-priority leads)
        "new_lead"  — immediate text to a brand new lead at peak interest

    needs_optout: True on first SMS ever sent to a lead, and every 5th SMS after
        that. Claude weaves casual TCPA opt-out language into the message end.
    """
    tags = tags or []
    first_name = person.get("firstName") or "there"

    if dry_run:
        tier_label = "Z-buyer" if is_z else ("Seller" if is_seller else "Buyer")
        optout_tag = " + OPT-OUT" if needs_optout else ""
        return f"[DRY RUN SMS — {tier_label} / {strategy} / {channel}{optout_tag}]"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set")

    try:
        import anthropic as _ant
    except ImportError:
        raise RuntimeError("anthropic package not installed")

    # ── Build a tight, SMS-relevant brief ────────────────────────────────────
    brief_lines = [f"LEAD: {first_name}"]

    if is_z:
        street = person.get("streetAddress") or person.get("street") or ""
        city   = person.get("city") or ""
        if street and city:
            brief_lines.append(f"PROPERTY: {street}, {city}")
        elif street:
            brief_lines.append(f"PROPERTY: {street}")
        brief_lines.append("ACTION: Submitted a cash offer request on this property.")
        brief_lines.append("NOTE: They want to know what their home is worth in cash vs. listing.")

    elif is_seller:
        street = person.get("streetAddress") or person.get("street") or ""
        city   = person.get("city") or ""
        if street and city:
            brief_lines.append(f"HOME: {street}, {city}")
        elif city:
            brief_lines.append(f"AREA: {city}")
        brief_lines.append("LEAD TYPE: Homeowner who engaged with Barry's AI assistant about home value. NOT a buyer.")
        # Surface the most specific engagement signal — this drives the text angle
        if "ISA_TRANSFER_UNSUCCESSFUL" in tags or "ISA_ATTEMPTED_TRANSFER_REALTOR_UNAVAILABLE" in tags or "DECLINED_BY_REALTOR" in tags:
            brief_lines.append("CONVERSATION STATE: Was ready for live transfer to Barry — call dropped on our end. Missed connection. Text acknowledges this and offers easy retry.")
        elif "CALLBACK_SCHEDULED" in tags:
            brief_lines.append("CONVERSATION STATE: Explicitly scheduled a callback. Text is a light confirmation, not a new pitch.")
        elif "AI_VOICE_NEEDS_FOLLOW_UP" in tags:
            brief_lines.append("CONVERSATION STATE: Had a VOICE CALL with AI assistant about home value. Very warm. Barry is the human follow-up to that call.")
        elif "AI_NEEDS_FOLLOW_UP" in tags or "AI_RESPONDED" in tags or "AI_ENGAGED" in tags:
            brief_lines.append("CONVERSATION STATE: Had a TEXT CONVERSATION with AI assistant about home value. Warm lead. Barry is the human following up on that text exchange.")
        elif "NURTURE" in tags:
            brief_lines.append("CONVERSATION STATE: Interested in selling but not ready yet. Nurture mode — useful market update, not a listing pitch.")
        elif "VOICEMAIL" in tags:
            brief_lines.append("CONVERSATION STATE: Previous call went to voicemail. Low-pressure re-open. Easier to text than play phone tag.")
        else:
            brief_lines.append("CONVERSATION STATE: Engaged with Barry's platform about home value. Warm, not cold.")

    else:
        # Buyer — lead with the most specific data point available
        b = behavior
        if b.get("most_viewed") and b.get("most_viewed_ct", 0) >= 2:
            p = b["most_viewed"]
            addr = f"{p.get('street')}, {p.get('city')}" if p.get("city") else p.get("street", "")
            brief_lines.append(f"REPEAT VIEWER: {addr} — viewed {b['most_viewed_ct']} times. High attachment.")
        elif b.get("saves"):
            p = b["saves"][0]
            addr = f"{p.get('street')}, {p.get('city')}" if p.get("city") else p.get("street", "")
            price_str = f" (${_safe_int(p.get('price')):,})" if p.get("price") else ""
            brief_lines.append(f"SAVED: {addr}{price_str} — highest intent signal.")
        elif b.get("views"):
            p = b["views"][0]
            addr = f"{p.get('street')}, {p.get('city')}" if p.get("city") else p.get("street", "")
            brief_lines.append(f"REGISTERED ON / FIRST VIEWED: {addr}")

        cities = list(b.get("cities") or [])
        if cities:
            brief_lines.append(f"SEARCHING IN: {', '.join(cities[:3])}")

        if b.get("price_min") and b.get("price_max"):
            brief_lines.append(f"PRICE RANGE: ${b['price_min']:,}–${b['price_max']:,}")

        beds = b.get("beds_seen")
        if beds:
            brief_lines.append(f"BEDS: {min(beds)}-{max(beds)}br range" if len(beds) > 1 else f"BEDS: {list(beds)[0]}br")

        if b.get("hours_since_last") is not None:
            hrs = b["hours_since_last"]
            if hrs < 2:
                brief_lines.append("RECENCY: active RIGHT NOW (within 2 hours)")
            elif hrs < 24:
                brief_lines.append(f"RECENCY: active {int(hrs)}h ago — hot window")
            elif hrs < 48:
                brief_lines.append("RECENCY: active yesterday")
            else:
                brief_lines.append(f"RECENCY: last active {int(hrs/24)} days ago")

        if b.get("sell_before_buy"):
            brief_lines.append("NOTE: Needs to sell current home first. Don't ignore this — it's actually workable.")

    brief = "\n".join(brief_lines)

    # ── Channel context ───────────────────────────────────────────────────────
    channel_notes = {
        "sms_only": (
            "SMS-ONLY: No email is going out. This text IS the outreach. Make it count.\n"
            "They haven't heard from Barry yet — this is first contact via phone."
        ),
        "dual": (
            "DUAL-CHANNEL: They're also receiving a longer email today. The SMS hits their phone "
            "BEFORE they see the email. Take a DIFFERENT angle — don't summarize the email. "
            "The SMS is the tap on the shoulder. The email is the follow-through."
        ),
        "new_lead": (
            "NEW LEAD: Just registered. This is FIRST CONTACT at peak interest — they're looking "
            "at homes right now. Fast, warm, specific. Match their energy."
        ),
    }
    channel_note = channel_notes.get(channel, channel_notes["sms_only"])

    # ── Lead type context ─────────────────────────────────────────────────────
    if is_z:
        lead_context = (
            "Z-BUYER SELLER: Owns the property listed above. Requested a cash offer. "
            "NOT a buyer. Barry's edge: he can show them the cash offer AND the listing option. "
            "Most cash buyers can only show one number. Barry can show the full picture."
        )
    elif is_seller:
        # Determine the specific conversation state so Claude writes the right angle
        _had_text_convo   = "AI_NEEDS_FOLLOW_UP" in tags or "AI_RESPONDED" in tags or "AI_ENGAGED" in tags
        _had_voice_convo  = "AI_VOICE_NEEDS_FOLLOW_UP" in tags
        _missed_transfer  = "ISA_TRANSFER_UNSUCCESSFUL" in tags or "ISA_ATTEMPTED_TRANSFER_REALTOR_UNAVAILABLE" in tags or "DECLINED_BY_REALTOR" in tags
        _callback_sched   = "CALLBACK_SCHEDULED" in tags
        _future_seller    = "NURTURE" in tags
        _voicemail        = "VOICEMAIL" in tags

        if _missed_transfer:
            _convo_note = (
                "CRITICAL: This person was actively connected or nearly connected to Barry on a live call — "
                "it dropped or didn't go through on our end. That is a MISS on Barry's side, not theirs. "
                "The text must acknowledge it directly and offer a simple path to try again. "
                "Tone: apologetic but not groveling. 'We missed each other' energy, not 'sorry to bother you' energy."
            )
        elif _callback_sched:
            _convo_note = (
                "This person explicitly scheduled a callback. "
                "The text should feel like a light confirmation — 'just making sure we're still on.' "
                "Not a new pitch. The conversation is already in motion."
            )
        elif _had_voice_convo:
            _convo_note = (
                "This person had a VOICE CALL with Barry's AI assistant about their home value. "
                "They SPOKE with the AI — this is a very warm lead. "
                "Barry is the human following up on that voice conversation. "
                "The text should feel like a natural handoff: 'I heard you spoke with my assistant — I'm the person behind it.'"
            )
        elif _had_text_convo:
            _convo_note = (
                "This person had a TEXT CONVERSATION with Barry's AI assistant about their home value. "
                "They are WARM — they engaged. They know they're talking to a real estate agent's AI. "
                "Barry is the human following up on that conversation. "
                "The text should feel like: 'I'm the person behind that conversation — I pulled the real numbers.'"
            )
        elif _future_seller:
            _convo_note = (
                "This person said they're interested in selling but NOT ready yet. "
                "Long-game nurture. No urgency, no hard pitch. "
                "The text is a useful market update, not a 'ready to list?' ask. "
                "Tone: friend who keeps an eye on things. 'Thought you'd want to know.'"
            )
        elif _voicemail:
            _convo_note = (
                "A previous call attempt went to voicemail. "
                "The text should be casual and low-pressure — not 'I've been trying to reach you' desperation. "
                "Just a light re-open: 'easier to text than play phone tag.'"
            )
        else:
            _convo_note = (
                "This homeowner engaged with Barry's platform about their home value. "
                "They're warm — not a cold prospect. Barry is following up as the human agent. "
                "The text should feel like: 'I saw your home came up and wanted to reach out personally.'"
            )

        lead_context = f"""YLOPO PROSPECTING SELLER — HOMEOWNER (not a buyer):
This person OWNS the home at the address in the brief. They are NOT browsing for homes to buy.
Do NOT reference IDX, home searches, listings for sale, or buying homes. Ever.

THIS IS A WARM LEAD — NOT COLD OUTREACH:
{_convo_note}

Barry's role: he is the real human agent behind the AI assistant. The AI started the conversation.
Barry closes the loop with real, specific market data the AI couldn't provide.

His value to this homeowner: he knows their specific street, he knows what homes like theirs
are actually selling for right now, and he can give them a real number — no pressure, no commitment.

TONE: Warm, competent, personal. Like the friend who happens to be Hampton Roads' top agent.
Teaching voice. Market intel as a gift. Never pushy. Never "WE BUY HOUSES" energy.
Reference "my assistant" to tie back to the AI conversation they remember."""
    else:
        lead_context = (
            "BUYER: Browsing homes on legacyhomesearch.com in Hampton Roads. "
            "Looking to purchase — cities like Virginia Beach, Chesapeake, Norfolk, Suffolk, "
            "Portsmouth, Hampton, Newport News."
        )

    # ── TCPA opt-out section (first text ever, or every 5th) ─────────────────
    word_limit = "25-50" if needs_optout else "25-40"
    optout_section = ""
    if needs_optout:
        optout_section = """
━━ OPT-OUT LANGUAGE (MANDATORY — TCPA compliance) ━━

This message MUST include a casual opt-out at the END of the text.
Barry's preferred style — pick one or write your own in the same spirit:
  "...we can end this anytime, just say the word."
  "...and you can stop me anytime — just say so."
  "...or just tell me to stop if you'd rather I didn't reach out."
  "...we can stop this whenever — just say stop."

Rules for the opt-out:
• It must be at the END of the message (after your CTA)
• One short clause — not a new sentence on its own line
• Keep it human, no-pressure — a genuine offer, not a disclaimer
• Do NOT write "reply STOP" — too robotic. Barry's voice is casual.
Word limit for this message: up to 50 words (normal messages max out at 40).
"""

    # ── The prompt ────────────────────────────────────────────────────────────
    prompt = f"""Write a {word_limit} word text message from Barry Jenkins, Hampton Roads realtor.

WHO THIS PERSON IS:
{lead_context}

CHANNEL:
{channel_note}

━━ SMS RULES (these are absolute) ━━

LENGTH: 25-40 words. Never more. Two sentences MAX.

SENTENCE 1 — The curiosity gap:
This is EVERYTHING. Create tension between what you know and what they don't yet.
Mention something SPECIFIC — property address, their exact price range + city, their
neighborhood. Then imply you have something they need to know without giving it away.
The specific detail proves you're not a bot. The withholding makes them reply.

SENTENCE 2 — One CTA:
One yes/no question only. Answerable in a single word.
Make replying feel easier than ignoring.

OPENING: Start with their first name followed by a comma. Example: "Jordan," or "Sarah,"
No em dashes. No "Hey" or "Hi". Just the name then a comma.

NO LINKS. NO sign-off (added automatically). NO credentials.

NEVER USE:
- "just checking in" / "reaching out" / "following up" / "circling back"
- "I noticed" — lead with the observation, not yourself
- "I'd love to" / "happy to help" / "feel free to"
- "dream home" / "perfect fit" / "hot market"
- Anything that could apply to a different person

━━ WHAT MAKES A GREAT REAL ESTATE TEXT ━━

The CURIOSITY GAP is the whole game. You hint at something specific to their situation
but don't give it away. They reply to get the answer. That's the conversion.

GREAT examples by lead type (personalize to actual data — do not copy verbatim):

BUYER (IDX browser):
  "Jordan, went back to 812 Copperfield three times and I think I know what's holding you up. Worth a call?"
  "Brittany, inventory under $450k in Virginia Beach just shifted. What you were seeing two weeks ago is different now. Still looking?"
  "Marcus, the homes you saved in Chesapeake are moving faster than the calendar shows. Worth a quick conversation?"

SELLER — YLOPO PROSPECTING (warm handoff from AI — THESE LEADS ALREADY TALKED TO THE AI):
  The text CONTINUES the AI conversation. Barry is the human following up.
  NEVER write cold market intel as if they've never heard from us.
  "Sarah, my assistant mentioned your place on Harbour View. I pulled the actual numbers on your street. Worth a quick call to go through them?"
  "Sarah, picking up where my assistant left off on your home value question. The numbers on Harbour View are more interesting than I expected. Have a few minutes?"
  "David, my assistant flagged your place on Wythe Creek. I looked into it — something in that area is worth knowing about. Want me to share?"

SELLER — MISSED TRANSFER (they were ready, call dropped on our end):
  "David, we tried to connect you with Barry after your home value conversation and the call dropped on our end. Sorry about that. Want to try again?"

SELLER — FUTURE SELLER (not ready yet, nurture):
  "Sarah, keeping an eye on the Harbour View market like I mentioned. Something shifted this month I thought you'd want to know about."

Z-BUYER (cash offer request):
  "Marcus, got your request on Kempsville. Before I give you that number there's something you should hear first. Got 5 minutes?"
  "Lisa, ran your place on Shore Drive. The gap between cash and listed is smaller than most sellers expect right now. Want to see both numbers?"

BAD examples (these kill engagement):
  "Hi Jordan, I saw you were looking at homes in Chesapeake. I'd love to help you find your dream home! Let me know if you'd like to chat."
  "Sarah, just checking in on your home value question. I'm a top Hampton Roads agent and would love to help. Feel free to reach out!"
  "Marcus, I noticed you submitted a cash offer request. I can help you with that process. Just reach out when you're ready."

━━ LEAD DATA ━━
{brief}
{optout_section}
Output ONLY the raw SMS body text. No explanation. No subject line. No sign-off. Just the text."""

    ant_client = _ant.Anthropic(api_key=api_key)
    response = ant_client.messages.create(
        model="claude-opus-4-5",
        max_tokens=160 if needs_optout else 120,  # extra headroom for opt-out clause
        messages=[{"role": "user", "content": prompt}],
    )

    sms_text = response.content[0].text.strip()

    # Strip any accidental sign-off Claude might add
    for stop_phrase in ("Barry Jenkins", "Barry\nLegacy", "— Barry", "Legacy Home Team"):
        idx = sms_text.find(stop_phrase)
        if idx > 0:
            sms_text = sms_text[:idx].strip()

    return sms_text


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

    # IDX search links — included in listing-drop emails (2, 5, 7, 9)
    # Embed the link directly in the email body using markdown: [anchor text](url)
    # Anchor text must be specific: beds, city, price range — never generic "click here"
    if search_urls:
        lines.append("\nIDX SEARCH LINKS (embed directly in email body as [specific anchor text](url)):")
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


def _render_html(body_text, to_email=""):  # to_email kept for backwards compat, placeholder used below
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
            # P.S. — same style as body, just slightly smaller and offset.
            # No divider line — looks designed, not personal.
            html_parts.append(
                f'<p style="margin:20px 0 0;font-size:14px;color:#444;line-height:1.7">'
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
      <a href="__UNSUB_URL__"
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

    # Inject one-click unsubscribe URL — replaces __UNSUB_URL__ placeholder set by
    # _render_html(), and also fixes any inline templates that still use the old mailto:.
    _recipient = to_email.lower().strip() if isinstance(to_email, str) else ""
    _unsub     = _unsub_url(_recipient) if _recipient else "#"
    if body_html:
        body_html = body_html.replace("__UNSUB_URL__", _unsub)
        body_html = body_html.replace(
            "mailto:reply@inbound.yourfriendlyagent.net?subject=Unsubscribe",
            _unsub,
        )

    # Always append unsubscribe to plain text — required for CAN-SPAM compliance.
    # The HTML version gets it via the footer template; plain-text-only sends need it here.
    if body_text and "unsubscribe" not in body_text.lower():
        body_text = body_text.rstrip("\n") + f"\n\n--\nTo stop receiving these emails: {_unsub}"

    msg = Mail(
        from_email=SgEmail(FROM_EMAIL, FROM_NAME),
        to_emails=to_email,
        subject=subject,
        plain_text_content=body_text,
        html_content=body_html or None,  # None = plain-text-only send (no HTML part)
    )
    # Reply-to: SendGrid inbound parse intercepts replies for sentiment routing
    msg.reply_to = SgEmail("reply@inbound.yourfriendlyagent.net", FROM_NAME)

    # BCC Barry on every email so he can see exactly what's going out
    # Skip BCC if TO is already Barry (e.g. test sends) — SendGrid rejects duplicate addresses
    _to_normalized = to_email.lower().strip() if isinstance(to_email, str) else ""
    if _to_normalized != FROM_EMAIL.lower():
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

def generate_new_lead_email(person, behavior, tags, dry_run=False, time_bucket="normal"):
    """
    Generate a first-contact email for a brand-new lead.

    time_bucket: "late_night"    — 11pm–4am ET  (can't sleep / you're up too energy)
                 "early_morning" — 4am–7am ET   (early riser energy)
                 "normal"        — 7am–11pm ET  (caught at the computer)

    Tone adapts to the hour — the lead gets a human who was actually up at the same time.
    """
    import anthropic
    import json, re

    _load_env()
    client = anthropic.Anthropic()

    first_name = (person.get("firstName") or "").strip() or "there"

    # Detect Z-buyer (cash offer request) vs. general seller vs. buyer
    z_buyer_tags  = {"ZLEAD", "Z_BUYER", "YLOPO_Z_BUYER"}
    seller_tags   = {"SELLER", "LISTING_LEAD", "HOME_VALUE", "HOME_VALUATION",
                     "WHAT_IS_MY_HOME_WORTH"}
    is_z_buyer    = any(t.upper().replace("-","_") in z_buyer_tags for t in tags)
    is_seller     = any(t.upper().replace("-","_") in seller_tags  for t in tags)

    # Build a compact data brief
    lines = []
    if behavior.get("city"):
        lines.append(f"City: {behavior['city']}")
    if behavior.get("price_min") or behavior.get("price_max"):
        lo = f"${behavior['price_min']:,}" if behavior.get("price_min") else ""
        hi = f"${behavior['price_max']:,}" if behavior.get("price_max") else ""
        lines.append(f"Price range: {(lo+'–'+hi) if lo and hi else (lo or hi)}")
    if behavior.get("search_areas"):
        lines.append(f"Area: {', '.join(behavior['search_areas'][:2])}")
    data_brief = "\n".join(lines) if lines else "City/area unknown — write generically for Hampton Roads."

    # ── Time context blocks — injected into each prompt ──────────────────────
    if time_bucket == "late_night":
        _zbuyer_time = """━━━━ TIME CONTEXT ━━━━
This request came in late at night (after 11pm ET). One brief human acknowledgment of the
hour — a phrase, not a sentence about it. Something like "late-night request" or "saw this
come through just now." Then straight to the point. They want a number, not chitchat.

CTA EMPHASIS: Speed wins. Barry can have a cash number TODAY. In motivated-seller situations,
the first serious credible offer often wins — not the highest one.
Subject line idea: something that nods at the hour, e.g. "up late?" or "late-night request".
"""
        _seller_time = """━━━━ TIME CONTEXT ━━━━
This lead came through late at night. They're probably lying awake thinking about their
home. Brief, warm acknowledgment of the hour — one phrase only (e.g. "up late thinking
about this" or "late-night research"). Then straight to the value. Show you're human too.
Subject line idea: nod at the hour, e.g. "couldn't sleep either?" or "late-night question".
"""
        _buyer_time = """━━━━ TIME CONTEXT ━━━━
Late-night browser — researching homes when the house is finally quiet. That's focus.
Brief, warm acknowledgment (one phrase): "up late looking at homes too" energy, not a
3am blast email. Show Barry was working too. Then straight to market intel.
Subject line must hint at the hour: something like "up late?" or "couldn't sleep either?"
"""
    elif time_bucket == "early_morning":
        _zbuyer_time = """━━━━ TIME CONTEXT ━━━━
Early morning request — before 7am ET. Serious seller, up early and ready to move.
Brief "early start" acknowledgment (one phrase). CTA: Barry can have a cash number TODAY.
First serious offer often wins in motivated-seller situations — that's the angle.
Subject line idea: nod at the hour, e.g. "early morning request" or "up early — me too".
"""
        _seller_time = """━━━━ TIME CONTEXT ━━━━
Early morning lead — before 7am ET. Up before their day starts thinking about their home.
That's focus. Brief "early riser" acknowledgment — one phrase, then into the value.
Tone: quiet confidence, not a morning pep talk.
Subject line idea: "early riser?" or "up before 7 thinking about this?"
"""
        _buyer_time = """━━━━ TIME CONTEXT ━━━━
Early morning buyer — before 7am ET. Researching before their day starts. That's commitment.
Brief acknowledgment of the hour (one phrase). Show Barry's up too. Then straight to intel.
Subject line must hint at the hour: "early start?" or "up before 7 searching homes?"
"""
    else:  # normal hours
        _zbuyer_time = """━━━━ TIME CONTEXT ━━━━
Normal business hours. Standard real-time energy — Barry just got their request.
CTA: emphasize speed. Barry can have a cash number fast. First serious offer often wins.
"""
        _seller_time = """━━━━ TIME CONTEXT ━━━━
Normal hours — "caught you at the computer" energy. Real-time, like Barry just saw them
pop up and grabbed his laptop.
"""
        _buyer_time = """━━━━ TIME CONTEXT ━━━━
Normal hours — real-time energy. Barry saw the search come through and typed this fast.
"""

    # ── Z-BUYER PROMPT ──────────────────────────────────────────────────────
    if is_z_buyer:
        prompt = f"""You are writing a first-contact email from Barry Jenkins in Hampton Roads VA.

This person just submitted a CASH OFFER REQUEST for their home. They want to sell fast.
Barry is both a licensed realtor AND a cash buyer who can close in 7 days.

━━━━ WHO THIS PERSON IS ━━━━
Their inbox is already flooded with "WE BUY HOUSES" responses. They submitted to multiple
investors and agents within the last hour. Overwhelmed. Skeptical. Looking for the one
response that sounds different.

Barry's edge — he can do BOTH. Most can only do one.
  Option A: Cash offer, close in 7 days. No showings, no financing risk. Done.
  Option B: Quick MLS listing — a few more weeks might net significantly more. Barry shows that math.

SPEED IS THE CTA ANGLE: In motivated-seller situations the first serious, credible offer
often wins — not the highest one. Barry can have a real number TODAY. That beats every
investor saying "we'll call you Monday."

{_zbuyer_time}
━━━━ TONE ━━━━
Calm. Confident. Not desperate. Not a pitch — more like: "Here's what I can actually do."
No hype. No "WE BUY HOUSES" energy. No "I'd love to help."
Be the quiet, competent one in a sea of noise.

━━━━ WHAT THE EMAIL MUST INCLUDE ━━━━
1. Confirm their cash offer request specifically (not a blast)
2. Yes, cash, can close fast
3. One-sentence differentiator: Barry also shows what listing might net — most can't
4. Speed-forward CTA: get them a number today / being first often wins

━━━━ WHAT KILLS THIS EMAIL ━━━━
- ANY "WE BUY HOUSES" energy
- More than 4 sentences in the body
- Exclamation points
- "I'd love to", "feel free to", "don't hesitate"
- Vague CTA that doesn't signal speed and first-mover advantage

━━━━ LEAD DATA ━━━━
{data_brief}

FORMAT:
- First name + comma only on line 1. Blank line. Body: 3–4 sentences max.
- Signature added automatically — stop before sign-off.

SUBJECT LINES (3 options — feel like a direct text, under 7 words, no ALL CAPS):
- Good: "your cash offer request", "got your request — can move fast", "cash offer — [City]"

OUTPUT (JSON only, no markdown fences):
{{{{
  "subject_options": ["option 1", "option 2", "option 3"],
  "body": "{first_name},\\n\\n[3–4 sentences]"
}}}}"""

    # ── BUYER PROMPT ──────────────────────────────────────────────────────────────────────────
    elif not is_seller:
        prompt = f"""You are writing a first-contact email from Barry Jenkins, realtor in Hampton Roads VA.

This buyer JUST came through a home search site — active within the last hour.

{_buyer_time}
━━━━ LEAD DATA ━━━━
{data_brief}

━━━━ WHAT THE EMAIL MUST DO ━━━━
1. Open with a specific observation (city, price range, what they searched)
2. One sentence of Hampton Roads market intel only a local would know — specific, not generic
3. One clear CTA answerable in 2–5 words

━━━━ HARD RULES ━━━━
- No P.S., no "I hope this finds you well", no "just checking in", no "dream home"
- No "I noticed" — lead with the observation itself
- Fragments fine. Contractions always. 3–5 sentences max.
- Signature added automatically — stop before sign-off.

FORMAT:
- First name + comma only on line 1. Blank line. Body.

SUBJECT LINES (3 options — 3–6 words, feel like a text not a campaign):
- Direct beats clever. No ALL CAPS, no emojis.

OUTPUT (JSON only, no markdown fences):
{{{{
  "subject_options": ["option 1", "option 2", "option 3"],
  "body": "{first_name},\\n\\n[3–5 sentences]"
}}}}"""

    # ── SELLER PROMPT ──────────────────────────────────────────────────────────────────────────
    else:
        prompt = f"""You are writing a first-contact email from Barry Jenkins, realtor in Hampton Roads VA.

This homeowner JUST came through a home valuation form — likely considering selling.
Barry saw them pop up and is responding now.

{_seller_time}
━━━━ LEAD DATA ━━━━
{data_brief}

━━━━ WHAT THE EMAIL MUST DO ━━━━
1. Open with a specific observation about their area or situation
2. One sentence of real Hampton Roads market intel — what sellers are actually seeing now
3. One clear CTA answerable in 2–5 words

━━━━ HARD RULES ━━━━
- No P.S., no "I hope this finds you well", no "just checking in"
- No "I noticed" — lead with the observation itself
- Fragments fine. Contractions always. 3–5 sentences max.
- Signature added automatically — stop before sign-off.

FORMAT:
- First name + comma only on line 1. Blank line. Body.

SUBJECT LINES (3 options — 3–6 words, feel like a text not a campaign):
- Direct beats clever. No ALL CAPS, no emojis.

OUTPUT (JSON only, no markdown fences):
{{{{
  "subject_options": ["option 1", "option 2", "option 3"],
  "body": "{first_name},\\n\\n[3–5 sentences]"
}}}}"""

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

    Sends immediately at any hour — 24/7. The lead gets the text now; the
    HeyGen video Email 1 follows at the next 8am pond_mailer run.
    The immediate email is logged as strategy='new_lead_immediate' which is
    intentionally excluded from the sequence count in db.py — so pond_mailer
    still sees sequence_num=1 and fires the HeyGen video on schedule.
    """
    _load_env()

    import db as _db
    from fub_client import FUBClient
    from config import (SHARK_TANK_POND_ID, NEW_LEAD_EMAIL_DELAY_MINUTES,
                        NEW_LEAD_LOOKBACK_MINUTES, NEW_LEAD_DAILY_CAP,
                        BARRY_FUB_USER_ID)

    _db.ensure_pond_email_log_table()
    client = FUBClient()
    now = datetime.now(timezone.utc)

    # Detect ET time bucket — shapes email tone and subject line
    from zoneinfo import ZoneInfo
    _et_hour = now.astimezone(ZoneInfo("America/New_York")).hour
    if _et_hour >= 23 or _et_hour < 4:
        _time_bucket = "late_night"      # 11pm–4am: "can't sleep either?"
    elif _et_hour < 7:
        _time_bucket = "early_morning"   # 4am–7am:  "early riser too?"
    else:
        _time_bucket = "normal"          # 7am–11pm: caught at the computer
    logger.debug("New lead mailer: time bucket = %s (%d:xx ET)", _time_bucket, _et_hour)

    # Daily cap: count how many new_lead_immediate emails already sent today (ET)
    if not dry_run:
        sent_today = _db.count_pond_emails_today_by_strategy("new_lead_immediate")
        if sent_today >= NEW_LEAD_DAILY_CAP:
            logger.info("New lead daily cap of %d reached (%d sent). Skipping.", NEW_LEAD_DAILY_CAP, sent_today)
            return {"skipped": True, "reason": "daily_cap_reached", "sent_today": sent_today}
        remaining_cap = NEW_LEAD_DAILY_CAP - sent_today
    else:
        remaining_cap = NEW_LEAD_DAILY_CAP

    # Window: leads created between (now - lookback) and (now - delay).
    # Timestamps are UTC-aware — FUBClient formats with Z suffix so FUB
    # doesn't misinterpret as local/Eastern time.
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
        if window_start <= _parse_iso(p.get("created", "")) <= min_age
        # window_start: not older than lookback (rejects leads from weeks/months ago)
        # min_age:      not newer than delay buffer (rejects leads < 12 min old)
    ]

    if not eligible:
        logger.debug("New lead mailer: no eligible leads this run")
        return {"checked": len(new_leads), "sent": 0}

    sent = 0
    for person in eligible:
        if sent >= remaining_cap:
            logger.info("New lead daily cap hit mid-run (%d sent). Stopping.", sent)
            break

        pid   = person.get("id")
        first = person.get("firstName") or ""
        last  = person.get("lastName") or ""
        name  = f"{first} {last}".strip() or f"ID:{pid}"
        tags  = person.get("tags") or []

        # Agent-claimed check — agent may have called and claimed this lead
        # during the 12-minute delay buffer before our immediate email fires.
        _assigned_uid = person.get("assignedUserId") or person.get("ownerId")
        if _assigned_uid and _assigned_uid != BARRY_FUB_USER_ID:
            logger.info("Skipping new lead %s — already claimed by agent (user %s)", name, _assigned_uid)
            continue

        # Compliance block — respect opt-outs before sending the immediate email.
        _blocking = _email_suppression_tags(tags)
        if _blocking:
            logger.info("Skipping new lead %s — email suppressed by tag(s): %s",
                        name, ", ".join(_blocking))
            continue

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
            email_data = generate_new_lead_email(person, behavior, tags, dry_run=dry_run,
                                                  time_bucket=_time_bucket)
        except Exception as e:
            logger.error("New lead email generation failed for %s: %s", name, e)
            continue

        subject_options = email_data.get("subject_options", [])
        subject = subject_options[0] if subject_options else f"Quick question — {first or 'you'}"
        body_text = email_data.get("body", "")

        # Plain-text-only send — HTML removed for deliverability
        body_html = None

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

        # Log to FUB timeline — 📧 note so agents see what went out.
        # (FUB's /v1/emails endpoint returns 403 for API integrations — notes work.)
        if not dry_run:
            try:
                from config import BARRY_FUB_USER_ID
                client.log_email_sent(
                    person_id=pid,
                    subject=subject,
                    message=body_text,
                    user_id=BARRY_FUB_USER_ID,
                )
            except Exception as _fub_err:
                logger.warning("FUB email log skipped for new lead %s: %s", name, _fub_err)

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
    from config import LEADSTREAM_ALLOWED_POND_IDS, BARRY_FUB_USER_ID, SHARK_TANK_POND_ID

    _db.ensure_pond_email_log_table()
    _db.ensure_pond_sms_log_table()

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
                # Stamp the pond source — FUB doesn't return assignedPondId on
                # the person object, so we track it here. Used to gate SMS to
                # Shark Tank (pond 4) only.
                lead["_pond_id"] = pond_id
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

    # ── HeyGen cost control: score-gated videos ───────────────────────────────
    # HeyGen videos are expensive (~$40/day when sent to everyone). We only
    # generate videos for leads in the top 50% by LeadStream score so we cut
    # Heygen spend roughly in half while still targeting the highest-intent leads.
    #
    # Score is computed from tags already on the person object — same weights
    # as LeadStream scoring (LEADSTREAM_SIGNAL_TAGS). No extra FUB API calls.
    # Leads below the median still receive the text email, just without a video.
    def _quick_ls_score(p):
        """Sum LeadStream signal tag points for a lead — fast, no FUB call."""
        from config import LEADSTREAM_SIGNAL_TAGS as _sig
        tag_names = {
            (t.get("name") or "").lower()
            for t in (p.get("tags") or [])
            if isinstance(t, dict)
        }
        return sum(pts for tag, pts in _sig.items() if tag.lower() in tag_names)

    _all_scores = [_quick_ls_score(p) for p in all_pond_leads]
    if _all_scores:
        _sorted_scores = sorted(_all_scores, reverse=True)
        # Top 50%: threshold is the score at the 50th percentile rank
        _hg_video_threshold = _sorted_scores[max(0, len(_sorted_scores) // 2 - 1)]
    else:
        _hg_video_threshold = 0
    logger.info(
        "HeyGen score gate: threshold=%d (top 50%% of %d leads, score range %d–%d)",
        _hg_video_threshold, len(_all_scores),
        min(_all_scores) if _all_scores else 0,
        max(_all_scores) if _all_scores else 0,
    )
    # Map person_id → score for fast lookup inside the loop
    _lead_score_map = {
        p.get("id"): _quick_ls_score(p) for p in all_pond_leads
    }

    sent = 0
    sms_only_sent = 0
    dual_sms_sent = 0
    skipped_cooldown = 0
    skipped_no_email = 0
    skipped_no_activity = 0
    skipped_no_strategy = 0
    skipped_generation_error = 0
    max_to_process = limit or MAX_PER_RUN

    import twilio_client as _tc
    _sms_ready = _tc.is_available()

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

        # Score gate: is this lead in the top 50% by LeadStream score?
        _this_ls_score   = _lead_score_map.get(pid, 0)
        _video_eligible  = _this_ls_score >= _hg_video_threshold

        # Get email address
        emails = person.get("emails") or []
        to_email = next(
            (e["value"] for e in emails if e.get("isPrimary") or e.get("status") == "Valid"),
            None
        )
        if not to_email:
            to_email = emails[0]["value"] if emails else None

        # Get phone for SMS channel (parallel to email)
        # Three gates before a lead is SMS-eligible:
        #   1. Shark Tank (pond 4) only
        #   2. Source not in SMS_BLOCKED_SOURCES (config):
        #        • "Ylopo Prospecting" — rAIya homeowner leads, Barry follows up personally
        #        • "Ylopo Seller"      — dedicated seller campaign leads
        #      Everything else is allowed, including Ylopo buyer leads that also
        #      have seller tags — those are sell-to-buy buyers, SMS is right for them.
        #   3. No suppression tags (opt-outs, wrong number, DO_NOT_CALL, etc.)
        _in_shark_tank    = person.get("_pond_id") == SHARK_TANK_POND_ID
        _sms_src_blocked  = _is_sms_blocked_source(person)
        to_phone = _tc.get_primary_phone(person) if (_sms_ready and _in_shark_tank and not _sms_src_blocked) else None
        _sms_blocked = _tc.sms_suppressed_by_tags(tags) if to_phone else []
        _sms_eligible = bool(to_phone and not _sms_blocked)

        if not to_email and not _sms_eligible:
            logger.debug("Skipping %s — no email, no SMS-eligible phone", name)
            skipped_no_email += 1
            continue

        # Agent-claimed check — if an agent has assigned this lead to themselves
        # (even while the pond tag is still set due to a FUB race or workflow quirk),
        # stop the drip immediately. Agents work these leads directly.
        _assigned_uid = person.get("assignedUserId") or person.get("ownerId")
        if _assigned_uid and _assigned_uid != BARRY_FUB_USER_ID:
            logger.info("Skipping %s — claimed by agent (user %s)", name, _assigned_uid)
            skipped_cooldown += 1
            continue

        # Opt-out check — lead replied negatively to a previous email
        if "PondMailer_Unsubscribed" in tags:
            logger.debug("Skipping %s — opted out", name)
            skipped_cooldown += 1
            continue

        # Compliance block — respect Ylopo/agent-applied opt-outs before we ever
        # generate or send an email (NO_EMAIL, NO_MARKETING, DO_NOT_CALL,
        # AI_OPT_OUT, LISTING_ALERT_UNSUB, etc.). These tags mean the lead has
        # explicitly or implicitly asked us to stop reaching out.
        _blocking = _email_suppression_tags(tags)
        if _blocking:
            # Email-specific suppression: if SMS is still eligible, fall through
            # to the SMS-only fork below; otherwise skip entirely.
            if not _sms_eligible or not to_phone:
                logger.info("Skipping %s — suppressed by tag(s): %s", name, ", ".join(_blocking))
                skipped_cooldown += 1
                continue
            # Email blocked but SMS still open — clear to_email so we drop into SMS-only fork
            to_email = None

        # ── SMS-only path ─────────────────────────────────────────────────────
        # Lead has no email address (or email was just suppressed) but has a
        # valid opted-in phone number. Run the same behavior/strategy/generation
        # pipeline but send a condensed SMS instead of an email.
        if not to_email and _sms_eligible:
            _sms_days = _db.days_since_last_pond_sms(pid)
            if _sms_days is not None and _sms_days < EMAIL_COOLDOWN_DAYS:
                logger.debug("Skipping %s (SMS-only) — texted %.1fd ago", name, _sms_days)
                skipped_cooldown += 1
                continue

            # Seller leads are excluded at _sms_eligible — if we reach this path
            # the lead is already confirmed buyer (non-seller, Shark Tank only).
            _is_z2  = _is_z_buyer(tags, person)
            _first2 = first or "there"

            candidates_checked += 1
            _ev2 = client.get_events_for_person(pid, days=60, limit=100)
            if len([e for e in _ev2 if e.get("type")]) < MIN_EVENTS_TO_EMAIL:
                logger.debug("Skipping %s (SMS-only) — insufficient IDX events", name)
                skipped_no_activity += 1
                continue
            _beh2 = analyze_behavior(_ev2, tags)
            _tier2 = "POND"
            for _tt in ("AI_NEEDS_FOLLOW_UP", "HANDRAISER", "YPRIORITY", "Y_HOME_3_VIEW"):
                if _tt in tags:
                    _tier2 = _tt
                    break
            _strat2, _pri2 = select_strategy(_beh2, _tier2, tags)
            if _strat2 == "none" or _pri2 < 20:
                logger.debug("Skipping %s (SMS-only) — no strategy", name)
                skipped_no_strategy += 1
                continue

            # TCPA opt-out: required on 1st text ever, then every 5th send
            _sms_hist_count = _db.count_pond_sms_sent(pid)
            _needs_optout   = (_sms_hist_count == 0) or (_sms_hist_count % 5 == 4)

            print(f"\n  [SMS-ONLY] {name} (ID: {pid}) · {to_phone}")
            print(f"    Tier: {_tier2} | Strategy: {_strat2}" +
                  (" | FIRST TEXT — opt-out required" if _sms_hist_count == 0 else ""))

            try:
                _sms_body = generate_sms_body(
                    person=person, behavior=_beh2, strategy=_strat2,
                    leadstream_tier=_tier2, tags=tags,
                    is_seller=False, is_z=_is_z2,   # no sellers reach SMS-only path
                    channel="sms_only", needs_optout=_needs_optout, dry_run=dry_run,
                )
            except Exception as _eg:
                logger.error("SMS-only generation failed for %s: %s", name, _eg)
                skipped_generation_error += 1
                continue

            if not _sms_body:
                logger.warning("SMS-only body empty for %s — skipping", name)
                continue

            _sms_result = _tc.send_sms(to_phone, _sms_body, dry_run=dry_run)
            if _sms_result.get("success"):
                _db.log_pond_sms(pid, name, to_phone, _sms_body,
                                 strategy=_strat2, leadstream_tier=_tier2,
                                 dry_run=dry_run,
                                 twilio_sid=_sms_result.get("twilio_sid"),
                                 status=_sms_result.get("status", "queued"),
                                 channel="sms_only")
                # Log to FUB timeline — 📱 note so agents see what text went out
                if not dry_run:
                    try:
                        from config import BARRY_FUB_USER_ID
                        _lt2 = "zbuyer" if _is_z2 else "buyer"
                        client.log_sms_sent(
                            person_id=pid,
                            sms_body=_sms_body,
                            lead_type=_lt2,
                            channel="sms_only",
                            user_id=BARRY_FUB_USER_ID,
                        )
                    except Exception as _fub_sms_err:
                        logger.warning("FUB SMS log skipped for %s: %s", name, _fub_sms_err)
                print(f"    ✓ {'[DRY RUN] Would send' if dry_run else 'Sent'} SMS ({len(_sms_body)+52} chars)")
                sms_only_sent += 1
                sent += 1
            elif _sms_result.get("status") == "quiet_hours":
                # TCPA block — outside 8am–9pm ET; not an error, just reschedule
                logger.info("SMS-only quiet_hours block for %s — will retry next run in window", name)
                print(f"    ⏸  SMS held — outside TCPA quiet hours (8am–9pm ET)")
            else:
                logger.error("SMS-only send failed for %s: %s", name, _sms_result.get("error"))
            continue  # Done — don't fall through to email path

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

        # Phase-aware cooldown:
        #   Sprint  emails 1-3 = 3 days (fast reply window)
        #   Bridge  email  4   = 3 days (fires right after the breakup while the window is open)
        #   Drip    emails 5-9 = 10 days (long game, unhurried)
        cooldown = DRIP_COOLDOWN_DAYS if sequence_num >= 5 else EMAIL_COOLDOWN_DAYS
        days_ago = _db.days_since_last_pond_email(pid)
        if days_ago is not None and days_ago < cooldown:
            logger.debug("Skipping %s — emailed %.1f days ago (need %dd)", name, days_ago, cooldown)
            skipped_cooldown += 1
            continue

        # Ylopo Prospecting seller leads: skip IDX event fetch entirely.
        # They're homeowners transferred from rAIya — no IDX activity expected.
        # Their data comes from the FUB person record (address, city, AI tags).
        is_ylopo_seller = _is_ylopo_prospecting_seller(person, tags)
        is_z            = _is_z_buyer(tags, person)  # checks source string too (fixes Zbuyer source routing)
        first_name      = first or "there"   # used in HeyGen script generators + wrappers

        if is_ylopo_seller:
            # No IDX events needed — use empty behavior for these leads
            behavior      = analyze_behavior([], tags)
            strategy      = "ylopo_prospecting"
            leadstream_tier = "AI_NEEDS_FOLLOW_UP" if "AI_NEEDS_FOLLOW_UP" in tags else "POND"
            logger.debug("%s — Ylopo Prospecting seller, skipping IDX fetch", name)
        else:
            # Pull IDX events — each fetch = 1 FUB API call, so count against cap
            candidates_checked += 1
            events = client.get_events_for_person(pid, days=60, limit=100)

            # Any IDX event counts for qualification
            idx_events = [e for e in events if e.get("type")]

            if len(idx_events) < MIN_EVENTS_TO_EMAIL:
                logger.debug("Skipping %s — only %d IDX events", name, len(idx_events))
                skipped_no_activity += 1
                continue

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
        _lead_type_label = "YLOPO-SELLER" if is_ylopo_seller else strategy.upper()
        if sequence_num == 1:
            seq_label = "sprint #1/3"
        elif sequence_num == 2:
            seq_label = "sprint #2/3 · listing drop"
        elif sequence_num == 3:
            seq_label = "sprint #3/3 · breakup"
        else:
            drip_n = sequence_num - 3
            seq_label = f"drip #{drip_n}/6 · {'listing' if _is_listing_drop(sequence_num, tags, person) else 'content'}"
        print(f"\n  [{_lead_type_label}] {name} (ID: {pid}) — {seq_label}")
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

        # ── Context from the immediate text (if one was sent overnight/early) ─────
        # If this lead got a "can't sleep?" or "early riser?" text before 7am,
        # the morning video wrapper acknowledges it briefly — shows continuity and
        # that Barry is the same human they heard from a few hours ago.
        _imm_ctx = _db.get_immediate_email_context(pid)  # "late_night" / "early_morning" / None

        # ── HeyGen daily cap check ───────────────────────────────────────────────
        # HeyGen videos are exempt from the main text-email daily cap but have
        # their own ceiling (HEYGEN_DAILY_CAP). We compute remaining slots once
        # here; each HeyGen block below checks it before attempting a render.
        from config import HEYGEN_DAILY_CAP as _HG_CAP
        _hg_used_today  = _db.count_heygen_today()
        _hg_slots_left  = max(0, _HG_CAP - _hg_used_today)
        if _hg_slots_left == 0:
            logger.info("HeyGen daily cap of %d reached (%d used) — video skipped for %s",
                        _HG_CAP, _hg_used_today, name)
        if not _video_eligible:
            logger.info("HeyGen score gate: %s score=%d < threshold=%d — text-only email",
                        name, _this_ls_score, _hg_video_threshold)

        # ── HeyGen personalized video — seller Email 1 only ─────────────────────
        # When video succeeds the ENTIRE email body is replaced with a short wrapper.
        # Falls back gracefully to the Claude-written text email if HeyGen fails.
        if is_ylopo_seller and sequence_num == 1 and not dry_run:
            try:
                from heygen_client import (
                    is_available as heygen_available,
                    generate_seller_video_script,
                    generate_and_wait,
                    get_background_url,
                    make_video_plain_text,
                    make_video_email_html,
                    DEFAULT_AVATAR, DEFAULT_VOICE,
                )
                if heygen_available() and _hg_slots_left > 0 and _video_eligible:
                    # FUB addresses array is usually empty for Ylopo leads.
                    # Try standard address fields, then fall back to tag-derived city.
                    _addrs   = person.get("addresses") or []
                    _addr0   = _addrs[0] if _addrs else {}
                    _street  = (_addr0.get("street") or "").strip()
                    _city_hg = (_addr0.get("city") or "").strip()
                    if not _city_hg:
                        _city_hg = _city_from_tags(tags)

                    logger.info("Generating HeyGen video for %s at %s, %s", name, _street, _city_hg)
                    script = generate_seller_video_script(
                        first_name=first_name,
                        street=_street or "your home",
                        city=_city_hg or "Hampton Roads",
                        tags=tags,
                    )
                    bg_url = get_background_url("seller", address=_street, city=_city_hg)
                    video_result = generate_and_wait(script, background_url=bg_url,
                                                    avatar_id=DEFAULT_AVATAR, voice_id=DEFAULT_VOICE, timeout_seconds=480)

                    if video_result and video_result.get("video_url"):
                        _city_display   = _city_hg or "Hampton Roads"
                        _street_display = _street or "your home"

                        # One line above + video link + one CTA line.
                        # If lead got an overnight text, briefly acknowledge it — same human, next step.
                        if _imm_ctx == "late_night":
                            _setup_sent = (
                                f"{first_name} — we reached out late last night and I knew I'd be "
                                f"pulling numbers on your place on {_street_display} this morning. "
                                f"Coffee's in — here's what I put together."
                            )
                        elif _imm_ctx == "early_morning":
                            _setup_sent = (
                                f"{first_name} — we caught each other early this morning. "
                                f"Put together a quick recording for your place on {_street_display} "
                                f"while I had the numbers in front of me."
                            )
                        else:
                            _setup_sent = (
                                f"{first_name} — I was pulling recent sale numbers for a few of my clients "
                                f"in {_city_display} when your place on {_street_display} came up. "
                                f"Put together a quick recording for you."
                            )

                        _cta_sent = "Would a quick 10-minute call make sense? Just reply here."
                        # Plain text (for spam scoring + fallback)
                        video_body_text = (
                            f"{_setup_sent}\n\n"
                            f"{make_video_plain_text(video_result['video_url'], first_name=first_name, video_id=video_result.get('video_id', ''))}\n"
                            f"{_cta_sent}\n\n"
                            + SIGN_OFF
                        )
                        # Slim HTML — thumbnail with play button baked in, curiosity-gap caption
                        video_body_html = make_video_email_html(
                            setup_text=_setup_sent,
                            video_url=video_result["video_url"],
                            thumbnail_url=video_result["thumbnail_url"],
                            cta_text=_cta_sent,
                            first_name=first_name,
                            caption=f"I looked into {_street_display} before I hit record. Here's what stood out.",
                            duration=video_result.get("duration", 0),
                            video_id=video_result.get("video_id", ""),
                        )
                        email_data["body_text"] = video_body_text
                        email_data["body_html"] = video_body_html

                        # Subject: first name + address = most personalized signal in inbox.
                        # Lowercase feels like a text, not a blast. Under 45 chars when possible.
                        _subj_options = [
                            f"{first_name} — quick video for {_street_display}",
                            f"quick video for {_street_display}",
                            f"was recording for clients, thought of you",
                        ]
                        email_data["subject"]      = _subj_options[0]
                        email_data["all_subjects"] = _subj_options

                        _avatar_used = DEFAULT_AVATAR
                        _hg_slots_left -= 1  # consume one HeyGen slot
                        logger.info("HeyGen video email built for %s (%.1fs video) — %d slots remain",
                                    name, video_result.get("duration", 0), _hg_slots_left)
                        print(f"    ▶ HeyGen video: {video_result['video_url'][:60]}...")
                    else:
                        logger.warning("HeyGen video not ready for %s — sending text-only", name)
            except Exception as _hg_err:
                logger.warning("HeyGen pipeline failed for %s — sending text-only: %s", name, _hg_err)

        # ── HeyGen personalized video — Z-buyer Email 1 ─────────────────────────
        # Z-buyers requested a cash offer — they took action, they want speed.
        # Their inbox is full of "WE BUY HOUSES" noise. Barry cuts through it by
        # being calm, competent, and showing BOTH options (cash OR list).
        # Same organic frame as seller video but higher energy in the content.
        if is_z and sequence_num == 1 and not dry_run:
            try:
                from heygen_client import (
                    is_available as heygen_available,
                    generate_zbuyer_video_script,
                    generate_and_wait,
                    get_background_url,
                    make_video_plain_text,
                    make_video_email_html,
                    DEFAULT_AVATAR, DEFAULT_VOICE,
                )
                if heygen_available() and _hg_slots_left > 0 and _video_eligible:
                    # FUB addresses array is usually empty for Ylopo leads.
                    # Try addresses array, then fall back to tag-derived city.
                    _addrs   = person.get("addresses") or []
                    _addr0   = _addrs[0] if _addrs else {}
                    _street  = (_addr0.get("street") or "").strip()
                    _city_hg = (_addr0.get("city") or "").strip()
                    if not _city_hg:
                        _city_hg = _city_from_tags(tags)

                    logger.info("Generating HeyGen Z-buyer video for %s at %s, %s",
                                name, _street, _city_hg)
                    script = generate_zbuyer_video_script(
                        first_name=first_name,
                        street=_street or "your home",
                        city=_city_hg or "Hampton Roads",
                        tags=tags,
                    )
                    bg_url = get_background_url("zbuyer", address=_street, city=_city_hg)
                    video_result = generate_and_wait(script, background_url=bg_url,
                                                    avatar_id=DEFAULT_AVATAR, voice_id=DEFAULT_VOICE, timeout_seconds=480)

                    if video_result and video_result.get("video_url"):
                        _city_display   = _city_hg or "Hampton Roads"
                        _street_display = _street or "your home"

                        if _imm_ctx == "late_night":
                            _z_setup = (
                                f"{first_name} — got your request late last night and wanted to get you "
                                f"something real, not just a form reply. Put together a quick recording "
                                f"on {_street_display} this morning."
                            )
                        elif _imm_ctx == "early_morning":
                            _z_setup = (
                                f"{first_name} — caught your early morning request. Put together a "
                                f"quick recording on {_street_display} — cash and list numbers both."
                            )
                        else:
                            _z_setup = (
                                f"{first_name} — saw your cash offer request for {_street_display} "
                                f"come through. Put together a quick recording for you."
                            )

                        _z_cta = (f"10 minutes on the phone and I'll run both numbers for "
                                  f"{_street_display}. Just reply here.")
                        video_body_text = (
                            f"{_z_setup}\n\n"
                            f"{make_video_plain_text(video_result['video_url'], first_name=first_name, video_id=video_result.get('video_id', ''))}\n"
                            f"{_z_cta}\n\n"
                            + SIGN_OFF
                        )
                        video_body_html = make_video_email_html(
                            setup_text=_z_setup,
                            video_url=video_result["video_url"],
                            thumbnail_url=video_result["thumbnail_url"],
                            cta_text=_z_cta,
                            first_name=first_name,
                            caption=f"cash offer vs. listing — I ran both numbers for {_street_display}.",
                            duration=video_result.get("duration", 0),
                            video_id=video_result.get("video_id", ""),
                        )
                        email_data["body_text"] = video_body_text
                        email_data["body_html"] = video_body_html

                        # Z-buyer subject — name + address, personal feel, lowercase
                        # When no street is known, avoid "your home on your home"
                        if _street and _street.lower() != "your home":
                            _subj_options = [
                                f"{first_name} — quick video on {_street_display}",
                                f"cash or list — quick video for {_street_display}",
                                f"two options for {_street_display}",
                            ]
                        else:
                            _subj_options = [
                                f"{first_name} — two options for your home",
                                f"{first_name} — cash or list? quick video",
                                f"quick video on your cash offer request",
                            ]
                        email_data["subject"]      = _subj_options[0]
                        email_data["all_subjects"] = _subj_options

                        _avatar_used = DEFAULT_AVATAR
                        _hg_slots_left -= 1  # consume one HeyGen slot
                        logger.info("HeyGen Z-buyer video email built for %s (%.1fs) — %d slots remain",
                                    name, video_result.get("duration", 0), _hg_slots_left)
                        print(f"    ▶ HeyGen Z-buyer video: {video_result['video_url'][:60]}...")
                    else:
                        logger.warning("HeyGen video not ready for Z-buyer %s — text-only", name)
            except Exception as _hg_err:
                logger.warning("HeyGen Z-buyer pipeline failed for %s — text-only: %s",
                               name, _hg_err)

        # ── Track which avatar was used (for A/B analysis) ──────────────────────
        _avatar_used = None

        # ── HeyGen personalized video — Buyer Email 1 ────────────────────────────
        # Buyer leads browsing IDX on legacyhomesearch.com.
        # Frame: Barry was already recording client videos, saw their search come through,
        # pulled one together on the spot.
        # Content: meaty Hampton Roads market intelligence specific to their city and
        # price range — inventory reality, competition, what buyers get wrong, what
        # Barry's 850+/year volume gives them that Zillow can't. No generic fluff.
        # Falls back gracefully to the Claude-written text email if HeyGen fails.
        if not is_ylopo_seller and not is_z and sequence_num == 1 and not dry_run:
            try:
                from heygen_client import (
                    is_available as heygen_available,
                    generate_buyer_video_script,
                    generate_and_wait,
                    get_background_url,
                    make_video_plain_text,
                    make_video_email_html,
                    DEFAULT_AVATAR, DEFAULT_VOICE,
                )
                if heygen_available() and _hg_slots_left > 0 and _video_eligible:
                    _beh_city      = (behavior.get("cities") or [])
                    _city_hg       = _beh_city[0] if _beh_city else (
                        _city_from_tags(tags) or "Hampton Roads"
                    )
                    _price_min     = behavior.get("price_min")
                    _price_max     = behavior.get("price_max")
                    _beds          = behavior.get("beds_seen") or []
                    _prop_type     = behavior.get("property_type")
                    _mv            = behavior.get("most_viewed") or {}
                    _mv_street     = _mv.get("street", "") if _mv else ""
                    _view_count    = behavior.get("view_count", 0)

                    logger.info("Generating HeyGen buyer video for %s, searching %s", name, _city_hg)
                    script = generate_buyer_video_script(
                        first_name=first_name,
                        city=_city_hg or "Hampton Roads",
                        price_min=_price_min,
                        price_max=_price_max,
                        beds=_beds,
                        property_type=_prop_type,
                        most_viewed_street=_mv_street,
                        strategy=strategy,
                        view_count=_view_count,
                        tags=tags,
                    )
                    bg_url = get_background_url("buyer", city=_city_hg)
                    video_result = generate_and_wait(script, background_url=bg_url,
                                                    avatar_id=DEFAULT_AVATAR, voice_id=DEFAULT_VOICE,
                                                    timeout_seconds=480)

                    if video_result and video_result.get("video_url"):
                        _city_display = _city_hg or "Hampton Roads"

                        # Personalize caption + wrapper + subject:
                        # — specific home vs. general search
                        # — overnight/early text reference if applicable
                        if _mv_street and strategy in ("saved_property", "repeat_view"):
                            _buyer_caption = f"&#9654; Barry's notes on {_mv_street} — for {first_name}"
                            if _imm_ctx == "late_night":
                                _setup_text = (
                                    f"{first_name} — we messaged you late last night. Had a few hours "
                                    f"to dig into {_mv_street} — put together a quick recording on what I found."
                                )
                            elif _imm_ctx == "early_morning":
                                _setup_text = (
                                    f"{first_name} — caught each other early this morning. "
                                    f"Put together a quick recording on {_mv_street} while I had it pulled up."
                                )
                            else:
                                _setup_text = (
                                    f"{first_name} — saw you've been looking in {_city_display} "
                                    f"and circling back to {_mv_street}. "
                                    f"Put together a quick recording for you."
                                )
                            _cta_text = (
                                f"10 minutes and I can walk you through what I'm actually seeing "
                                f"on that one — and your search overall. Just reply here."
                            )
                            _buyer_subj = f"{first_name} — I looked into {_mv_street}"
                        else:
                            _buyer_caption = f"&#9654; Barry's take on {_city_display} homes for {first_name}"
                            if _imm_ctx == "late_night":
                                _setup_text = (
                                    f"{first_name} — we reached out late last night. Put together a more "
                                    f"detailed recording this morning on your {_city_display} search."
                                )
                            elif _imm_ctx == "early_morning":
                                _setup_text = (
                                    f"{first_name} — caught each other early. Put together a quick "
                                    f"recording on your {_city_display} search while I had the numbers up."
                                )
                            else:
                                _setup_text = (
                                    f"{first_name} — saw your search come through for homes in {_city_display}. "
                                    f"Put together a quick recording for you."
                                )
                            _cta_text = (
                                f"10 minutes and I can walk you through exactly what I'm seeing "
                                f"right now. Just reply here."
                            )
                            _buyer_subj = f"{first_name} — your {_city_display} search"

                        video_body_text = (
                            f"{_setup_text}\n\n"
                            f"{make_video_plain_text(video_result['video_url'], first_name=first_name, video_id=video_result.get('video_id', ''))}\n"
                            f"{_cta_text}\n\n"
                            + SIGN_OFF
                        )
                        _buyer_caption = (
                            f"what I actually think about {_mv_street}."
                            if _mv_street and strategy in ("saved_property", "repeat_view")
                            else f"what the {_city_display} market actually looks like right now."
                        )
                        video_body_html = make_video_email_html(
                            setup_text=_setup_text,
                            video_url=video_result["video_url"],
                            thumbnail_url=video_result["thumbnail_url"],
                            cta_text=_cta_text,
                            first_name=first_name,
                            caption=_buyer_caption,
                            duration=video_result.get("duration", 0),
                            video_id=video_result.get("video_id", ""),
                        )
                        email_data["body_text"] = video_body_text
                        email_data["body_html"] = video_body_html

                        # Subject: name + specific search detail for max open rate
                        _subj_options = [
                            _buyer_subj,
                            f"put something together for your search",
                            f"was recording for clients, thought of you",
                        ]
                        email_data["subject"]      = _subj_options[0]
                        email_data["all_subjects"] = _subj_options

                        _avatar_used = DEFAULT_AVATAR
                        _hg_slots_left -= 1  # consume one HeyGen slot
                        logger.info("HeyGen buyer video email built for %s (%.1fs video) — %d slots remain",
                                    name, video_result.get("duration", 0), _hg_slots_left)
                        print(f"    ▶ HeyGen buyer video: {video_result['video_url'][:60]}...")
                    else:
                        logger.warning("HeyGen video not ready for buyer %s — text-only", name)
            except Exception as _hg_err:
                logger.warning("HeyGen buyer pipeline failed for %s — text-only: %s",
                               name, _hg_err)

        # ── HeyGen suit follow-up video — Email 2 (seller + Z-buyer only) ────────
        # Buyer Email 2 stays as the listing drop — too valuable to replace.
        # Suit avatar reads as "I take this seriously" — different energy from
        # the casual circle opener. Frame: "Not sure if you caught my last video,
        # but I wanted to add one more thing..." New piece of value, not a repeat.
        if sequence_num == 2 and (is_ylopo_seller or is_z) and not dry_run:
            try:
                from heygen_client import (
                    is_available as heygen_available,
                    generate_followup_video_script,
                    generate_and_wait,
                    get_background_url,
                    make_video_plain_text,
                    make_video_email_html,
                    AVATAR_SUIT, DEFAULT_VOICE,
                )
                if heygen_available() and _hg_slots_left > 0 and _video_eligible:
                    _addrs_fu   = person.get("addresses") or []
                    _addr0_fu   = _addrs_fu[0] if _addrs_fu else {}
                    _street_fu  = (_addr0_fu.get("street") or "").strip()
                    _city_fu    = (_addr0_fu.get("city") or "").strip()
                    if not _city_fu:
                        _city_fu = _city_from_tags(tags) or "Hampton Roads"
                    _lead_type  = "zbuyer" if is_z else "seller"

                    logger.info("Generating HeyGen Email 2 follow-up for %s (%s)", name, _lead_type)
                    script = generate_followup_video_script(
                        lead_type=_lead_type,
                        first_name=first_name,
                        city=_city_fu,
                        street=_street_fu,
                        tags=tags,
                    )
                    bg_url = get_background_url(_lead_type, address=_street_fu, city=_city_fu)
                    video_result = generate_and_wait(
                        script, background_url=bg_url,
                        avatar_id=AVATAR_SUIT, voice_id=DEFAULT_VOICE,
                        avatar_style="normal",   # show the suit, not a circle crop
                        timeout_seconds=480,
                    )

                    if video_result and video_result.get("video_url"):
                        # Wrapper: name + address → get out of the way → one CTA line.
                        # "Not sure if you caught my last video" is the hook — use it.
                        _p = 'margin:0 0 16px;font-size:15px;line-height:1.8;color:#222'
                        if is_z:
                            _fu_caption = f"&#9654; Barry's follow-up for {first_name} — {_street_fu}"
                            setup_line = (
                                f"{first_name} — not sure if you caught my last video on "
                                f"{_street_fu}, but I ran a few more numbers and wanted to share."
                            )
                            cta_line = f"10 minutes on the phone and I'll walk you through both. Just reply here."
                            _fu_subj   = f"{first_name} — one more thing on {_street_fu}"
                        else:
                            _fu_caption = f"&#9654; Barry's follow-up for {first_name} — {_street_fu}"
                            setup_line = (
                                f"{first_name} — not sure if you caught my last video, "
                                f"but I wanted to add one more thing about your place on {_street_fu}."
                            )
                            cta_line = f"Reply here and we'll find 10 minutes to walk through it."
                            _fu_subj   = f"{first_name} — one more thing about {_street_fu}"

                        video_body_text = (
                            f"{setup_line}\n\n"
                            f"{make_video_plain_text(video_result['video_url'], first_name=first_name, video_id=video_result.get('video_id', ''))}\n"
                            f"{cta_line}\n\n" + SIGN_OFF
                        )
                        video_body_html = make_video_email_html(
                            setup_text=setup_line,
                            video_url=video_result["video_url"],
                            thumbnail_url=video_result["thumbnail_url"],
                            cta_text=cta_line,
                            first_name=first_name,
                            caption="one more thing I didn't say in my last video.",
                            duration=video_result.get("duration", 0),
                            video_id=video_result.get("video_id", ""),
                        )
                        email_data["body_text"] = video_body_text
                        email_data["body_html"] = video_body_html

                        # Subject: name + specific address — "one more thing" alone is too vague
                        _subj_options = [
                            _fu_subj,
                            f"not sure if you caught my last video",
                            f"one more thing",
                        ]
                        email_data["subject"]      = _subj_options[0]
                        email_data["all_subjects"] = _subj_options

                        _avatar_used = AVATAR_SUIT
                        _hg_slots_left -= 1  # consume one HeyGen slot
                        logger.info("HeyGen Email 2 suit video built for %s (%.1fs) — %d slots remain",
                                    name, video_result.get("duration", 0), _hg_slots_left)
                        print(f"    ▶ HeyGen Email 2 suit: {video_result['video_url'][:60]}...")
                    else:
                        logger.warning("HeyGen Email 2 video not ready for %s — text-only", name)
            except Exception as _hg_err:
                logger.warning("HeyGen Email 2 pipeline failed for %s — text-only: %s", name, _hg_err)

        # ── HeyGen suit re-engagement video — Email 4 (all lead types) ───────────
        # Email 3 is a breakup text. Email 4 is the first drip content email —
        # perfect re-entry point with a video. Suit avatar signals a fresh angle,
        # not a repeat of Email 1. Buyers get their 2nd video ever (Email 1 was
        # circle). Sellers/Z-buyers get their 3rd. All three lead types are served
        # by generate_followup_video_script() which picks the right frame.
        # Falls back gracefully to the Claude text email if HeyGen fails.
        if sequence_num == 4 and not dry_run:
            try:
                from heygen_client import (
                    is_available as heygen_available,
                    generate_followup_video_script,
                    generate_and_wait,
                    get_background_url,
                    make_video_plain_text,
                    make_video_email_html,
                    AVATAR_SUIT, DEFAULT_VOICE,
                )
                if heygen_available() and _hg_slots_left > 0 and _video_eligible:
                    _addrs_e4   = person.get("addresses") or []
                    _addr0_e4   = _addrs_e4[0] if _addrs_e4 else {}
                    _street_e4  = (_addr0_e4.get("street") or "").strip()
                    # Buyers: prefer city from IDX behavior; sellers/Z-buyers: use tag-derived city
                    _beh_cities4 = behavior.get("cities") or []
                    _city_e4    = (_beh_cities4[0] if _beh_cities4
                                   else (_addr0_e4.get("city") or "").strip()
                                        or _city_from_tags(tags) or "Hampton Roads")
                    _lead_type4 = "zbuyer" if is_z else ("seller" if is_ylopo_seller else "buyer")

                    logger.info("Generating HeyGen Email 4 drip video for %s (%s)", name, _lead_type4)
                    script = generate_followup_video_script(
                        lead_type=_lead_type4,
                        first_name=first_name,
                        city=_city_e4,
                        street=_street_e4,
                        tags=tags,
                    )
                    bg_url = get_background_url(_lead_type4, address=_street_e4, city=_city_e4)
                    video_result = generate_and_wait(
                        script, background_url=bg_url,
                        avatar_id=AVATAR_SUIT, voice_id=DEFAULT_VOICE,
                        avatar_style="normal",   # show the suit, not a circle crop
                        timeout_seconds=480,
                    )

                    if video_result and video_result.get("video_url"):
                        _p4 = 'margin:0 0 16px;font-size:15px;line-height:1.8;color:#222'
                        if is_z:
                            _e4_caption = f"&#9654; Barry's update for {first_name}"
                            _setup4     = (
                                f"{first_name} — wanted to circle back with one more thought "
                                f"on your options for {_street_e4 or 'your home'}."
                            )
                            _cta4       = (
                                f"10 minutes on the phone and I'll walk through both paths. "
                                f"Just reply here."
                            )
                            _e4_subj    = f"{first_name} — one more thought"
                        elif is_ylopo_seller:
                            _e4_caption = f"&#9654; Barry's update for {first_name} — {_street_e4}"
                            _setup4     = (
                                f"{first_name} — had a thought on "
                                f"{_street_e4 or 'your home'} I wanted to share."
                            )
                            _cta4       = f"Reply here and we'll find 10 minutes to walk through it."
                            _e4_subj    = f"{first_name} — I had a thought"
                        else:  # buyer
                            _e4_caption = f"&#9654; Barry's update — {_city_e4} search"
                            _setup4     = (
                                f"{first_name} — just had a thought on the {_city_e4} market "
                                f"I wanted to pass along."
                            )
                            _cta4       = (
                                f"Reply here and I can walk you through what I'm seeing. "
                                f"Happy to help."
                            )
                            _e4_subj    = f"{first_name} — {_city_e4} market update"

                        video_body_text4 = (
                            f"{_setup4}\n\n"
                            f"{make_video_plain_text(video_result['video_url'], first_name=first_name, video_id=video_result.get('video_id', ''))}\n"
                            f"{_cta4}\n\n" + SIGN_OFF
                        )
                        video_body_html4 = make_video_email_html(
                            setup_text=_setup4,
                            video_url=video_result["video_url"],
                            thumbnail_url=video_result["thumbnail_url"],
                            cta_text=_cta4,
                            first_name=first_name,
                            caption="had a thought on this I wanted to say out loud.",
                            duration=video_result.get("duration", 0),
                            video_id=video_result.get("video_id", ""),
                        )
                        email_data["body_text"] = video_body_text4
                        email_data["body_html"] = video_body_html4

                        _subj_options4 = [
                            _e4_subj,
                            "had a thought",
                            "one more thing",
                        ]
                        email_data["subject"]      = _subj_options4[0]
                        email_data["all_subjects"] = _subj_options4

                        _avatar_used = AVATAR_SUIT
                        _hg_slots_left -= 1  # consume one HeyGen slot
                        logger.info("HeyGen Email 4 drip video built for %s (%.1fs) — %d slots remain",
                                    name, video_result.get("duration", 0), _hg_slots_left)
                        print(f"    ▶ HeyGen Email 4 drip: {video_result['video_url'][:60]}...")
                    else:
                        logger.warning("HeyGen Email 4 video not ready for %s — text-only", name)
            except Exception as _hg_err:
                logger.warning("HeyGen Email 4 pipeline failed for %s — text-only: %s", name, _hg_err)


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
            avatar_used=_avatar_used,
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

        # ── Dual-channel SMS ─────────────────────────────────────────────────
        # High-priority leads (AI_NEEDS_FOLLOW_UP, HANDRAISER, etc.) also get
        # a purpose-written SMS the same day — different angle than the email,
        # hits the phone BEFORE they open their inbox.
        # Ylopo Prospecting sellers are excluded — Barry follows up personally.
        # SMS cooldown is checked independently of email cooldown.
        if _sms_eligible and any(t in tags for t in _tc.DUAL_CHANNEL_TAGS):
            _dual_sms_days = _db.days_since_last_pond_sms(pid)
            if _dual_sms_days is None or _dual_sms_days >= EMAIL_COOLDOWN_DAYS:
                # TCPA opt-out: required on 1st text ever, then every 5th send
                _dual_sms_count  = _db.count_pond_sms_sent(pid)
                _dual_needs_optout = (_dual_sms_count == 0) or (_dual_sms_count % 5 == 4)
                try:
                    _dual_body = generate_sms_body(
                        person=person, behavior=behavior, strategy=strategy,
                        leadstream_tier=leadstream_tier, tags=tags,
                        is_seller=is_ylopo_seller, is_z=is_z,
                        channel="dual", needs_optout=_dual_needs_optout, dry_run=dry_run,
                    )
                except Exception as _dsms_err:
                    logger.warning("Dual SMS generation failed for %s: %s", name, _dsms_err)
                    _dual_body = None

                if _dual_body:
                    _dual_result = _tc.send_sms(to_phone, _dual_body, dry_run=dry_run)
                    if _dual_result.get("success"):
                        _db.log_pond_sms(pid, name, to_phone, _dual_body,
                                         strategy=strategy, leadstream_tier=leadstream_tier,
                                         dry_run=dry_run,
                                         twilio_sid=_dual_result.get("twilio_sid"),
                                         status=_dual_result.get("status", "queued"),
                                         channel="dual")
                        # Log to FUB timeline — 📱 note alongside the email note
                        if not dry_run:
                            try:
                                from config import BARRY_FUB_USER_ID
                                _lt_dual = "zbuyer" if is_z else "buyer"
                                client.log_sms_sent(
                                    person_id=pid,
                                    sms_body=_dual_body,
                                    lead_type=_lt_dual,
                                    channel="dual",
                                    user_id=BARRY_FUB_USER_ID,
                                )
                            except Exception as _fub_dual_err:
                                logger.warning("FUB dual SMS log skipped for %s: %s", name, _fub_dual_err)
                        print(f"    ✓ {'[DRY RUN] Would send' if dry_run else 'Sent'} dual SMS ({len(_dual_body)+52} chars)")
                        dual_sms_sent += 1
                    elif _dual_result.get("status") == "quiet_hours":
                        logger.info("Dual SMS quiet_hours block for %s — outside 8am–9pm ET", name)
                        print(f"    ⏸  Dual SMS held — outside TCPA quiet hours (8am–9pm ET)")
                    else:
                        logger.warning("Dual SMS failed for %s: %s", name, _dual_result.get("error"))

        # Log the outbound email to FUB timeline as a structured 📧 note.
        # Agents see email number, type, what it does, and clear next-action.
        # (FUB /v1/emails is blocked for API integrations — notes work fine.)
        if not dry_run:
            try:
                from config import BARRY_FUB_USER_ID, DRIP_COOLDOWN_DAYS
                _lt = ("zbuyer" if is_z
                       else ("seller" if is_ylopo_seller else "buyer"))
                client.log_email_sent(
                    person_id=pid,
                    subject=email_data["subject"],
                    message=email_data["body_text"],
                    user_id=BARRY_FUB_USER_ID,
                    sequence_num=sequence_num,
                    lead_type=_lt,
                    avatar_used=_avatar_used,
                    cooldown_days=DRIP_COOLDOWN_DAYS,
                )
            except Exception as _fub_err:
                logger.warning("FUB email log skipped for %s: %s", name, _fub_err)

        sent += 1
        print(f"    ✓ {'[DRY RUN] Would send' if dry_run else 'Sent'}")

        # ── Sequence completion signal — Email 9 (final drip) ───────────────
        # Apply NURTURE_COMPLETE tag + a clear FUB note so the agent knows
        # the automated sequence is finished and this lead needs human follow-up.
        if sequence_num >= 9 and not dry_run:
            try:
                client.add_tag(pid, "NURTURE_COMPLETE")
                _complete_note = (
                    f"🏁 NURTURE SEQUENCE COMPLETE — {name}\n"
                    f"{'─' * 48}\n"
                    f"Barry's AI nurture system has sent all {sequence_num} automated emails "
                    f"to this lead with no reply.\n\n"
                    f"AGENT ACTION REQUIRED\n"
                    f"This lead will NOT receive any more automated emails.\n"
                    f"They need a human touch now — call, text, or personal email.\n"
                    f"Tag NURTURE_COMPLETE has been applied for Smart List filtering.\n"
                    f"{'─' * 48}\n"
                    f"Barry Jenkins AI Nurture · Legacy Home Team"
                )
                client._request("POST", "notes", json_data={
                    "personId": int(pid),
                    "body": _complete_note,
                    "userId": BARRY_FUB_USER_ID,
                })
                logger.info("Nurture complete: tagged %s (seq %d)", name, sequence_num)
                print(f"    🏁 NURTURE_COMPLETE tag applied — agent follow-up needed")
            except Exception as _seq_err:
                logger.warning("Sequence completion signal failed for %s: %s", name, _seq_err)

        # Brief pause between leads to stay friendly to FUB rate limits
        import time as _t; _t.sleep(1.5)

    _sms_total = sms_only_sent + dual_sms_sent
    print(f"\n{'='*60}")
    print(f"  Done: {sent} {'would send' if dry_run else 'sent'} "
          f"({'email only' if _sms_total == 0 else f'{sms_only_sent} SMS-only + {dual_sms_sent} dual-channel'})")
    print(f"  Cooldown: {skipped_cooldown} | No activity: {skipped_no_activity} | "
          f"No contact: {skipped_no_email} | No strategy: {skipped_no_strategy} | "
          f"Generation error: {skipped_generation_error}")
    print(f"{'='*60}\n")

    return {
        "sent":                      sent,
        "sms_only_sent":             sms_only_sent,
        "dual_sms_sent":             dual_sms_sent,
        "skipped_cooldown":          skipped_cooldown,
        "skipped_no_email":          skipped_no_email,
        "skipped_no_activity":       skipped_no_activity,
        "skipped_no_strategy":       skipped_no_strategy,
        "skipped_generation_error":  skipped_generation_error,
        "dry_run":                   dry_run,
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
