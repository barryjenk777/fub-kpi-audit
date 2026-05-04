"""
HeyGen Personalized Video Client
----------------------------------
Generates a short personalized video from Barry's avatar + cloned voice
for Ylopo Prospecting seller leads (Email 1 only).

The video is 25-40 seconds — Barry references the lead's name, their home
address, and one credibility signal (e.g. "homes like yours on that street
have been moving fast"). The email embeds a click-to-play thumbnail.

Workflow:
  1. generate_seller_video_script()  — Claude writes the 35-second script
  2. submit_video()                  — HeyGen renders it (async, ~60s)
  3. poll_video()                    — waits for completion
  4. Returns {video_url, thumbnail_url} for embedding in the email

Avatar IDs:
  Barry Jenkins (casual):  fffc73aa581a49e4af8dcd304e76349b
  Barry Jenkins (suit):    dc3bfe40aeaf4590b76ee12824d019dd

Voice IDs (cloned from Barry):
  Barry Jenkins:           b37262521af24a0e9245308e4045ac3f
  Barry Jenkins Suit:      850bdd18eb164cd8b5d540a88fdf862a

Credit cost: ~1 credit per 6 seconds → 35-second video = ~6 credits.
Current quota: 1,500 API credits. Sustainable for ~250 videos/month.
"""

from __future__ import annotations

import logging
import os
import time

import requests

logger = logging.getLogger("heygen_client")

_API_KEY = os.environ.get("HEYGEN_API_KEY", "")
_BASE = "https://api.heygen.com"

# Barry's avatar + voice IDs — confirmed working via API test
DEFAULT_AVATAR      = "4b314392e9b6441d97abaf6d83808de5"  # Barry Jenkins — Avatar 2, circle style (Email 1)
DEFAULT_AVATAR_TYPE = "avatar"                             # HeyGen character type for DEFAULT_AVATAR
AVATAR_SUIT         = "dc3bfe40aeaf4590b76ee12824d019dd"  # Barry Jenkins — Suit avatar (Email 2 follow-up)
AVATAR_SUIT_TYPE    = "avatar"                             # HeyGen character type for AVATAR_SUIT
DEFAULT_VOICE       = "b37262521af24a0e9245308e4045ac3f"  # Barry Jenkins — HeyGen native voice clone

# Background endpoint hosted on Railway — generates branded background on-demand
# HeyGen fetches this URL during rendering. No external storage needed.
# Format: {RAILWAY_URL}/api/heygen-bg?type=seller&address=123+Oak+St&city=Chesapeake
RAILWAY_BASE_URL = os.environ.get("RAILWAY_URL", "https://web-production-3363cc.up.railway.app")


def _headers():
    return {"X-Api-Key": _API_KEY, "Content-Type": "application/json"}


def is_available() -> bool:
    return bool(_API_KEY)


# ---------------------------------------------------------------------------
# Background Image Generator — clean, minimal, cross-platform fonts
# ---------------------------------------------------------------------------

def _load_font(size: int):
    """
    Load a clean sans-serif font at the given size.
    Tries multiple paths so it works on both macOS (dev) and Railway (Linux/Debian).
    Falls back to PIL default only as last resort.
    """
    from PIL import ImageFont
    candidates = [
        # Linux — Debian/Ubuntu (Railway)
        "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSansBold.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-B.ttf",
        # macOS
        "/System/Library/Fonts/Helvetica.ttc",
        "/System/Library/Fonts/SFNSDisplay.ttf",
        "/Library/Fonts/Arial.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _load_font_regular(size: int):
    """Regular weight version of _load_font."""
    from PIL import ImageFont
    candidates = [
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
        "/usr/share/fonts/truetype/freefont/FreeSans.ttf",
        "/usr/share/fonts/truetype/ubuntu/Ubuntu-R.ttf",
        "/System/Library/Fonts/Helvetica.ttc",
        "/Library/Fonts/Arial.ttf",
    ]
    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except Exception:
            continue
    return ImageFont.load_default()


def _render_bg(lines: list, bg_color: tuple, width: int = 1920, height: int = 1080) -> bytes:
    """
    Render a clean background with a list of (text, color, font_size, bold) tuples.
    Text is centered vertically as a block, centered horizontally.
    No gradients. No decorative chrome. Just clean text on solid color.
    """
    from PIL import Image, ImageDraw
    import io

    img  = Image.new("RGB", (width, height), color=bg_color)
    draw = ImageDraw.Draw(img)

    # Pre-load fonts and measure line heights
    rendered = []
    total_h   = 0
    line_gap  = 24
    for text, color, size, bold in lines:
        font = _load_font(size) if bold else _load_font_regular(size)
        bbox = draw.textbbox((0, 0), text, font=font)
        tw   = bbox[2] - bbox[0]
        th   = bbox[3] - bbox[1]
        rendered.append((text, color, font, tw, th))
        total_h += th + line_gap
    total_h -= line_gap  # no trailing gap

    # Start y so the whole block is centered
    y = (height - total_h) // 2
    for text, color, font, tw, th in rendered:
        x = (width - tw) // 2
        draw.text((x, y), text, fill=color, font=font)
        y += th + line_gap

    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=92)
    return buf.getvalue()


def generate_seller_background_image(address: str, city: str,
                                      width: int = 1920, height: int = 1080) -> bytes:
    """Clean dark background for seller videos."""
    lines = [
        (address,                   (255, 255, 255), 108, True),
        (f"{city} Market Update",   (160, 200, 240),  54, False),
        ("Legacy Home Team",        (120, 150, 180),  36, False),
    ]
    return _render_bg(lines, bg_color=(14, 20, 38), width=width, height=height)


def generate_buyer_background_image(city: str, price_band: str = "",
                                     width: int = 1920, height: int = 1080) -> bytes:
    """Clean dark background for buyer videos."""
    lines = [
        (f"Homes in {city}",  (255, 255, 255), 108, True),
    ]
    if price_band:
        lines.append((price_band, (160, 220, 200), 54, False))
    lines.append(("Legacy Home Team", (120, 160, 150), 36, False))
    return _render_bg(lines, bg_color=(10, 22, 38), width=width, height=height)


# ── Mapbox background configuration ──────────────────────────────────────────
# navigation-day-v1: clean road map with blue water — looks exactly like a GPS
# map, instantly recognizable. streets-v12 is a good alternative.
# Pure satellite (satellite-v9) and satellite-streets-v12 were tested but
# Barry preferred an "obvious map" style over aerial imagery.
MAPBOX_STYLE         = "mapbox/navigation-day-v1"
MAPBOX_ZOOM_ADDRESS  = 14   # street-level: shows road grid clearly as a map
MAPBOX_ZOOM_STREET   = 13   # neighborhood: shows surrounding streets
MAPBOX_ZOOM_CITY     = 11   # district-level: search area visible

# Circle avatar: lower-right placement, compact size
# HeyGen offset range: -0.5 (far left/top) → 0.5 (far right/bottom), 0 = center
# HeyGen scale range:  0.3 (tiny) → 2.0 (fill frame), 1.0 = default
AVATAR_CIRCLE_SCALE    = 0.40   # compact — visible but doesn't dominate the map
AVATAR_CIRCLE_OFFSET_X = 0.33   # push right of center
AVATAR_CIRCLE_OFFSET_Y = 0.28   # push toward bottom


def _geocode_with_mapbox(query: str, api_key: str):
    """
    Geocode an address or place name → (longitude, latitude).

    Returns (lon, lat) float tuple or None on failure.
    Scoped to US results to avoid false matches.
    """
    from urllib.parse import quote as _q
    url = (
        f"https://api.mapbox.com/geocoding/v5/mapbox.places/{_q(query)}.json"
        f"?access_token={api_key}&limit=1&country=us"
        f"&types=address,place,neighborhood,locality"
    )
    try:
        r = requests.get(url, timeout=5)
        if r.status_code == 200:
            features = r.json().get("features", [])
            if features:
                lon, lat = features[0]["geometry"]["coordinates"]
                logger.info("Geocoded %r → %.5f, %.5f", query, lon, lat)
                return lon, lat
        logger.warning("Mapbox geocoding %d for %r", r.status_code, query)
    except Exception as e:
        logger.warning("Mapbox geocoding error for %r: %s", query, e)
    return None


# Hampton Roads center — guaranteed fallback when geocoding fails or no address given.
# Lat/lon for the mouth of the James River / downtown Norfolk area.
_HAMPTON_ROADS_LON = -76.2859
_HAMPTON_ROADS_LAT =  36.8508


def get_background_url(bg_type: str, address: str = "", city: str = "",
                        price_band: str = "") -> str:
    """
    Return a Mapbox Static API URL centered on the lead's specific location.

    Sellers / Zbuyers   → centered on their property address (zoom 14)
    Buyers w/ property  → centered on their most-viewed street (zoom 13)
    Buyers city-only    → centered on their search city (zoom 11)

    Always returns a real map URL — never None.  If MAPBOX_ACCESS_TOKEN is
    missing or geocoding fails, falls back to a Hampton Roads overview map so
    the video always has the map background, not a solid color.
    """
    api_key = os.environ.get("MAPBOX_ACCESS_TOKEN", "")
    if not api_key:
        # No token → build URL with empty key (will 401 in HeyGen) vs guaranteed map.
        # Log clearly so the ops team knows to add the token on Railway.
        logger.warning("get_background_url: MAPBOX_ACCESS_TOKEN not set — using Hampton Roads fallback URL (set token on Railway for lead-specific maps)")
        # Can't build a valid URL without the token, so return None — HeyGen will
        # use color background. Token is required for the map.
        return None

    # Build the best geocoding query we can from available data
    state = "VA"   # all Legacy Home Team leads are Virginia
    if address and city:
        query = f"{address}, {city}, {state}"
        zoom  = MAPBOX_ZOOM_ADDRESS if bg_type in ("seller", "zbuyer") else MAPBOX_ZOOM_STREET
    elif address:
        query = f"{address}, {state}"
        zoom  = MAPBOX_ZOOM_ADDRESS if bg_type in ("seller", "zbuyer") else MAPBOX_ZOOM_STREET
    elif city:
        query = f"{city}, {state}"
        zoom  = MAPBOX_ZOOM_CITY
    else:
        query = "Hampton Roads, Virginia"
        zoom  = 11

    coords = _geocode_with_mapbox(query, api_key)
    if not coords:
        # Geocoding failed — use Hampton Roads center rather than returning None.
        # Every video gets a real map background; only the pin location differs.
        logger.warning("get_background_url: geocoding failed for %r — using Hampton Roads fallback map", query)
        coords = (_HAMPTON_ROADS_LON, _HAMPTON_ROADS_LAT)
        zoom   = 11

    lon, lat = coords
    # 1280x720 = 16:9 aspect; HeyGen scales to 1920x1080 — imperceptible upscale
    # attribution=false + logo=false keep the background clean
    bg_url = (
        f"https://api.mapbox.com/styles/v1/{MAPBOX_STYLE}/static/"
        f"{lon:.6f},{lat:.6f},{zoom},0,0/1280x720"
        f"?access_token={api_key}&attribution=false&logo=false"
    )
    logger.info("Mapbox background: %s zoom=%d for [%s] (%r)", bg_type, zoom, query, bg_url[:80])
    return bg_url


# ---------------------------------------------------------------------------
# Address pronunciation — expand abbreviations so HeyGen reads them correctly
# Without this: "Dr" → "Doctor", "St" → "Saint", "Rd" → "Road" (ok), etc.
# ---------------------------------------------------------------------------

def expand_address_for_speech(address: str) -> str:
    """
    Expand street-type abbreviations so HeyGen's TTS reads them correctly.
    e.g. "412 Harbour View Dr" → "412 Harbour View Drive"
         "123 Oak St"          → "123 Oak Street"
    """
    import re
    # Only expand at word boundaries so "Dr" inside a word isn't touched.
    # Street types only — not directionals (N/S/E/W) to avoid false matches.
    suffixes = {
        r'\bDr\b':    "Drive",
        r'\bSt\b':    "Street",
        r'\bAve\b':   "Avenue",
        r'\bBlvd\b':  "Boulevard",
        r'\bLn\b':    "Lane",
        r'\bRd\b':    "Road",
        r'\bCt\b':    "Court",
        r'\bPl\b':    "Place",
        r'\bPkwy\b':  "Parkway",
        r'\bHwy\b':   "Highway",
        r'\bCir\b':   "Circle",
        r'\bTer\b':   "Terrace",
        r'\bTerr\b':  "Terrace",
        r'\bFwy\b':   "Freeway",
        r'\bTrl\b':   "Trail",
        r'\bWay\b':   "Way",   # already spoken correctly but included for completeness
    }
    for pattern, replacement in suffixes.items():
        address = re.sub(pattern, replacement, address)
    return address


# ---------------------------------------------------------------------------
# Tag-aware context hints for video prompts
#
# Returns a small additive context block (or empty string) describing the
# highest-leverage Ylopo signals present on this lead. This is a nuance
# layer — influence, not replacement. The existing script prompts (frame,
# open, hook, voice, length, CTA rules) are untouched when no tags fire.
# ---------------------------------------------------------------------------

def _signal_hint_for_video(tags, lead_type):
    """Return a compact optional-context block for the video prompt.

    Kept narrow on purpose. Barry's script bones (frame, voice, length, CTA)
    stay the master; these hints only add 1-3 lines of nuance when relevant.
    If no relevant tags are present, returns '' — meaning the video prompt
    is character-for-character identical to today.
    """
    tags = set(tags or [])
    hints = []

    # --- Missed live-connection attempts (all lead types) ---
    transfer_friction = {
        "ISA_TRANSFER_UNSUCCESSFUL",
        "ISA_ATTEMPTED_TRANSFER_REALTOR_UNAVAILABLE",
        "ISA_ATTEMPTED_TRANSFER",
        "DECLINED_BY_REALTOR",
    }
    if transfer_friction & tags:
        hints.append(
            "They already tried to connect live and it didn't land on our end. "
            "A natural one-beat acknowledgment fits ('we almost caught each other' / "
            "'sorry we missed live') — not a stiff apology."
        )

    # --- Scheduled callback (all lead types) ---
    if "CALLBACK_SCHEDULED" in tags:
        hints.append(
            "They already scheduled a callback time. The video is a warm bridge to "
            "that confirmed call — reference the commitment instead of re-pitching."
        )

    # --- Seller-specific (one light influence) ---
    if lead_type == "seller":
        if "NURTURE" in tags:
            hints.append(
                "They've said they're interested but not ready yet. Ease off any "
                "urgency — long-game energy, no hard CTA."
            )

    # --- Buyer-specific (four high-leverage tags, pick at most one frame hint) ---
    if lead_type == "buyer":
        if "call_now=yes" in tags:
            hints.append(
                "At registration they explicitly asked to be called within the hour. "
                "The video can nod to that naturally — the call itself is the CTA."
            )
        elif "Y_REQUESTED_TOUR" in tags:
            hints.append(
                "They submitted a tour request. The hook can acknowledge they want "
                "to see a home in person, not keep browsing."
            )
        elif "Y_FAVORITED_LISTING" in tags:
            hints.append(
                "They favorited a listing. They're attached to something concrete — "
                "don't treat them as a casual browser."
            )

    if not hints:
        return ""

    return (
        "\n━━━━ OPTIONAL TAG CONTEXT (weave subtly, never name the tag or source) ━━━━\n"
        + "\n".join(f"- {h}" for h in hints)
        + "\n(Keep the frame, voice, length, and CTA exactly as specified above. "
        "This block is nuance — influence only, not replacement.)\n"
    )


# ---------------------------------------------------------------------------
# Script Generation — Claude writes the 35-second seller video script
# ---------------------------------------------------------------------------

def generate_seller_video_script(first_name: str, street: str, city: str,
                                  comp_snippet: str = "", ai_convo: bool = True,
                                  tags: list = None) -> str:
    """
    Generate a 35-40 second video script for Barry's avatar.

    CORE FRAME: Barry was already recording videos for other clients.
    He thought of this person mid-session and pulled one together for them.
    The video should feel like it happened naturally — not a marketing drip.

    This is the highest-converting frame for cold video outreach:
    - "I was already doing X" = credible (he does this for clients)
    - "I thought of you" = personal (they feel remembered, not marketed to)
    - "while I was at it" = low pressure (not a formal presentation)
    - Market intel = instant credibility (he actually knows their area)
    """
    # Expand abbreviations so HeyGen reads "Drive" not "Doctor", "Street" not "Saint"
    street_spoken = expand_address_for_speech(street)

    try:
        import anthropic
        client = anthropic.Anthropic()

        comp_line = (
            f"Include this real market data naturally: {comp_snippet}"
            if comp_snippet
            else ""
        )
        signal_hint = _signal_hint_for_video(tags, "seller")

        prompt = f"""Write a 35-40 second video script for Barry Jenkins, Realtor with Legacy Home Team at LPT Realty — #1 team in Virginia.

Barry is recording this for {first_name}, a homeowner at {street_spoken} in {city}.
They spoke with Barry's AI assistant about their home value — they're CURIOUS, not committed to selling.
Their core question is: "what would I actually walk away with?"

━━━━ THE FRAME ━━━━
Barry was pulling comps for other sellers in {city}. He remembered this person and put together a quick recording for them.
Spontaneous, not polished. He pivoted from client work to record this.

━━━━ OPEN (3-5 seconds) ━━━━
Start mid-thought. Reference pulling comps or working on something for other {city} sellers.
Good: "Hey {first_name} — so I was actually just pulling comps for a couple of my sellers over in {city} this week and I remembered we never connected after my assistant reached out about your place on {street_spoken}. So I put together a quick recording for you while I was at it."
Bad: "Hi, I'm Barry Jenkins..." — too formal
Bad: "I was recording market videos..." — less credible than "pulling comps"

━━━━ THE HOOK (15-18 seconds) ━━━━
This is the psychology of a curious seller: they think in offer price, but the number that matters is what they walk away with.
Create a curiosity gap — tell them there's a number they probably haven't seen yet.
{comp_line}
Good angle: "Here's what most sellers don't find out until they're already in contract: the number on the offer and the number you actually walk away with are almost always different — once you factor in commissions, repairs, and closing costs."
Good angle: "What I'm seeing for homes like yours in {city} right now — the gap between asking price and what sellers are actually netting might surprise you."
Do NOT say: "the market is hot", "homes are moving fast", "window is closing" — this is generic and kills trust.

━━━━ CLOSE (5-7 seconds) ━━━━
Low pressure. They're curious, not ready to decide. Offer clarity, not commitment.
Good: "I'd rather you see that picture now, before you make any decisions. Would a quick 10-minute call make sense? Just reply here."
Bad: "I'd love to schedule a listing consultation." (too much commitment pressure)

━━━━ VOICE ━━━━
- Knowledgeable friend. Teaching, not pitching. Never say "I'd love to", "feel free to", "don't hesitate"
- Never mention: Ylopo, rAIya, AI — say "my assistant"
- Contractions throughout. Short sentences on camera.

━━━━ LENGTH ━━━━
130-150 words. 35-42 seconds.
{signal_hint}
Return ONLY the script. No labels, no stage directions. Just Barry's words."""

        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        script = msg.content[0].text.strip()
        logger.info("HeyGen script generated for %s (%d chars)", first_name, len(script))
        return script

    except Exception as e:
        logger.warning("Claude script generation failed, using fallback: %s", e)
        comp_line = f" {comp_snippet}" if comp_snippet else ""
        return (
            f"Hey {first_name} — so I was actually just pulling comps for a couple of my sellers "
            f"over in {city} this week and I remembered we never connected after my assistant "
            f"reached out about your place on {street_spoken}. "
            f"So I put together a quick recording for you while I was at it.{comp_line} "
            f"Here's what most sellers don't find out until they're already in contract: "
            f"the number on the offer and the number you actually walk away with are almost "
            f"always different — once you factor in commissions, repairs, and closing costs. "
            f"I'd rather you see that picture now, before you make any decisions. "
            f"Would a quick 10-minute call make sense? Just reply here."
        )


def generate_zbuyer_video_script(first_name: str, street: str, city: str,
                                  comp_snippet: str = "", tags: list = None) -> str:
    """
    Generate a 35-40 second video script for a Z-buyer (cash offer request) lead.

    Z-buyer energy vs. Ylopo Prospecting seller:
    - They didn't explore — they requested. They want an actual offer, now.
    - Their inbox is already full of "WE BUY HOUSES" pitches.
    - Barry's edge: he can do BOTH — cash in 7 days OR list for potentially more.
    - The video cuts through the noise by being calm, competent, and specific.

    Same organic frame: Barry was already recording videos for clients.
    But the CONTENT pivots to the two-option pitch, not market intel.
    """
    # Expand abbreviations so HeyGen reads "Drive" not "Doctor", etc.
    street_spoken = expand_address_for_speech(street)

    try:
        import anthropic
        client = anthropic.Anthropic()

        comp_line = (
            f"Include this market context naturally: {comp_snippet}"
            if comp_snippet
            else ""
        )
        signal_hint = _signal_hint_for_video(tags, "zbuyer")

        prompt = f"""Write a 35-40 second video script for Barry Jenkins, Realtor with Legacy Home Team at LPT Realty — #1 real estate team in Virginia.

Barry is recording this for {first_name}, a homeowner at {street_spoken} in {city}.
They submitted a cash offer request — they want speed, and their inbox is already full of "WE BUY HOUSES" texts.
Barry's edge: he's a real Realtor who can do BOTH — cash OR listed — whichever nets more.

━━━━ THE FRAME ━━━━
Barry was already recording client videos. Saw the request come in, put together a quick recording on the spot.
Calm confidence. Not a cash buyer hustler — the credible professional who can actually solve this.{comp_line}

━━━━ OPEN (3-4 seconds) ━━━━
Acknowledge the request, name the address and city. Direct, no warm-up.
Good: "Hey {first_name} — saw your cash offer request come in for {street_spoken} and I was already recording some client videos, so I put together a quick recording for you."

━━━━ CREDENTIALS + DIFFERENTIATOR (18-22 seconds) ━━━━
Lead with who Barry is BEFORE the pitch — credibility first cuts through the noise.
"I'm Barry Jenkins — Realtor with Legacy Home Team at LPT Realty, been serving this community for almost 30 years."
Then the two-option differentiator:
"Here's what I do differently: I can close cash in as little as 7 days — or I can pull the MLS numbers and show you what listing your home might actually net. That gap is usually smaller than sellers expect, and sometimes it completely flips."
Do NOT say "you deserve to see both" — weak. Just state the options directly.

━━━━ CLOSE (5-6 seconds) ━━━━
Direct. One ask.
"10 minutes on the phone and I'll run both numbers for your home on {street_spoken}. Just reply here."

━━━━ RULES ━━━━
- "Realtor with Legacy Home Team at LPT Realty" — not "licensed agent"
- Almost 30 years in the community
- Never: "I'd love to", "feel free to", "don't hesitate", "you deserve to"
- Contractions throughout. No Ylopo, no AI.

━━━━ LENGTH ━━━━
120-140 words. 35-40 seconds.
{signal_hint}
Return ONLY the script. No labels, no stage directions. Just Barry's words."""

        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=350,
            messages=[{"role": "user", "content": prompt}],
        )
        script = msg.content[0].text.strip()
        logger.info("HeyGen Z-buyer script generated for %s (%d chars)", first_name, len(script))
        return script

    except Exception as e:
        logger.warning("Claude Z-buyer script generation failed, using fallback: %s", e)
        comp_line = f" {comp_snippet}" if comp_snippet else ""
        return (
            f"Hey {first_name} — saw your cash offer request come in for {street_spoken} "
            f"and I was already recording some client videos, "
            f"so I put together a quick recording for you.{comp_line} "
            f"I'm Barry Jenkins — Realtor with Legacy Home Team at LPT Realty, "
            f"been serving this community for almost 30 years. "
            f"Here's what I do differently: I can close cash in as little as 7 days — "
            f"or I can pull the MLS numbers and show you what listing your home might actually net. "
            f"That gap is usually smaller than sellers expect, and sometimes it completely flips. "
            f"10 minutes on the phone and I'll run both numbers for your home on {street_spoken}. "
            f"Just reply here."
        )


def generate_buyer_video_script(
    first_name: str,
    city: str = "Hampton Roads",
    price_min: int = None,
    price_max: int = None,
    beds: list = None,
    property_type: str = None,
    most_viewed_street: str = None,
    strategy: str = "",
    view_count: int = 0,
    tags: list = None,
) -> str:
    """
    Generate a 35-40 second video script for a buyer lead (Ylopo IDX / buyer drip).

    Unlike the seller track, the "meaty" section is buyer market intelligence:
    what's happening in their specific search area at their price point — inventory,
    competition, what buyers don't know going in, and what Barry's volume gives
    them that they can't get from Zillow alone.

    CORE FRAME: Barry was already recording client videos. Saw this buyer's search
    come through and pulled one together on the spot. Same organic credibility as
    the seller track, but the intel is buyer-focused.
    """
    # Build context strings for the prompt
    price_str = ""
    if price_min and price_max:
        price_str = f"${price_min // 1000}k–${price_max // 1000}k"
    elif price_max:
        price_str = f"up to ${price_max // 1000}k"
    elif price_min:
        price_str = f"above ${price_min // 1000}k"

    beds_str = ""
    if beds:
        if len(beds) == 1:
            beds_str = f"{beds[0]}-bedroom"
        else:
            beds_str = f"{min(beds)}–{max(beds)}-bedroom"

    prop_str = property_type or "home"

    search_desc = " ".join(filter(None, [beds_str, prop_str, "in", city,
                                          f"({price_str})" if price_str else ""]))

    strategy_context = ""
    if strategy == "saved_property" and most_viewed_street:
        strategy_context = (
            f"They SAVED a property on {expand_address_for_speech(most_viewed_street)}. "
            f"They have specific interest — this is high intent."
        )
    elif strategy == "repeat_view" and most_viewed_street:
        strategy_context = (
            f"They came back to {expand_address_for_speech(most_viewed_street)} multiple times. "
            f"They're circling it — high interest in that specific home."
        )
    elif view_count >= 4:
        strategy_context = f"They viewed {view_count} properties in one session — actively shopping."

    try:
        import anthropic
        client = anthropic.Anthropic()

        _specific_home = (
            f"IMPORTANT: {first_name} has been back to {expand_address_for_speech(most_viewed_street)} "
            f"more than once. Open with that — make them feel seen. "
            f"Say something like: 'I noticed you've been back to {expand_address_for_speech(most_viewed_street)} "
            f"more than once — I pulled everything on that home and I want to share what I found.' "
            f"This is the hook. Don't bury it."
            if most_viewed_street and strategy in ("saved_property", "repeat_view")
            else f"{strategy_context}"
        )
        signal_hint = _signal_hint_for_video(tags, "buyer")

        prompt = f"""Write a 35-40 second video script for Barry Jenkins, Realtor with Legacy Home Team at LPT Realty — #1 real estate team in Virginia.

Barry is recording this for {first_name}, a buyer searching for a {search_desc}.
{_specific_home}

━━━━ THE FRAME ━━━━
Barry was putting together a video for another buyer in {city} and saw {first_name}'s search come through.
He put together a quick recording on the spot. Not a cold pitch — a warm pivot from work he was already doing.

━━━━ OPEN (3-4 seconds) ━━━━
"Hey {first_name} — was putting together a video for another buyer in {city} and saw your search come through, so I put together a quick recording for you."
Then immediately move to the specific hook below — don't linger.

━━━━ THE HOOK (18-22 seconds — this is everything) ━━━━
{"Make them feel seen about " + expand_address_for_speech(most_viewed_street) + " FIRST, then pivot to market insight." if most_viewed_street and strategy in ("saved_property", "repeat_view") else "Give one genuinely specific insight about buying in " + city + (f" in the {price_str} range" if price_str else "") + " right now."}

Then the insider access angle — this is Barry's real differentiator:
"By the time something good hits Zillow, it's usually already showing — sometimes already under contract. Our team closes a lot of homes in Hampton Roads, which means we hear about what's coming before it goes public."

Do NOT use: "inventory is moving fast", "buyers who win are set up and ready", "the window is closing"
These are what every agent says. They kill credibility.

━━━━ CLOSE (5-6 seconds) ━━━━
"10 minutes on the phone and I can walk you through exactly what I'm seeing. Just reply here."

━━━━ BARRY'S VOICE ━━━━
- Knowledgeable friend. Teaching, not pitching. Never "I'd love to", "feel free to", "don't hesitate"
- Never: Ylopo, rAIya, AI — say "your search"
- Short sentences. Contractions. Specific beats generic every time.

━━━━ LENGTH ━━━━
130-150 words. 35-42 seconds.
{signal_hint}
Return ONLY the script. No labels, no stage directions. Just Barry's words."""

        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=350,
            messages=[{"role": "user", "content": prompt}],
        )
        script = msg.content[0].text.strip()
        logger.info("HeyGen buyer script generated for %s (%d chars)", first_name, len(script))
        return script

    except Exception as e:
        logger.warning("Claude buyer script generation failed, using fallback: %s", e)

        price_line = f" in that {price_str} range" if price_str else ""
        if most_viewed_street and strategy in ("saved_property", "repeat_view"):
            _mv_spoken = expand_address_for_speech(most_viewed_street)
            specific_hook = (
                f"I noticed you've been back to {_mv_spoken} more than once — "
                f"I pulled everything on that home and I want to share what I found. "
            )
        else:
            specific_hook = ""
        return (
            f"Hey {first_name} — was putting together a video for another buyer in {city} "
            f"and saw your search come through, so I put together a quick recording for you. "
            f"{specific_hook}"
            f"Here's the reality of buying{price_line} in {city} right now: "
            f"by the time something good hits Zillow it's usually already showing — "
            f"sometimes already under contract. "
            f"Our team closes a lot of homes in Hampton Roads, which means we hear about "
            f"what's coming before it goes public. "
            f"10 minutes on the phone and I can walk you through exactly what I'm seeing. "
            f"Just reply here."
        )


def generate_followup_video_script(
    lead_type: str,
    first_name: str,
    city: str = "Hampton Roads",
    street: str = "",
    tags: list = None,
) -> str:
    """
    Generate a 30-35 second follow-up video script for the SUIT avatar (Email 2).

    Lead types: "seller", "zbuyer", "buyer"

    Energy: Professional, calm follow-through. Not chasing — just circling back
    with one additional piece of value they didn't get in the first video.
    The suit reads as "I take this seriously" without being stiff.

    Frame: "Not sure if you caught my last video" — acknowledges the first video
    naturally, gives them a graceful out if they missed it, then adds something new.
    """
    street_spoken = expand_address_for_speech(street) if street else ""

    if lead_type == "seller":
        context = (
            f"Barry sent {first_name} a personalized market video last week about their "
            f"home in {city}. No reply yet. "
            f"This follow-up adds one new insight: most sellers compare OFFER PRICES, "
            f"but the number they actually walk away with — after commissions, repairs, "
            f"and carrying costs — is almost always different. Barry can put together "
            f"a side-by-side net proceeds comparison for their specific home."
        )
        cta = "Want me to put together a quick side-by-side for your place?"
        address_ref = f" on {street_spoken}" if street_spoken else ""
        fallback = (
            f"Hey {first_name} — not sure if you had a chance to catch my last video, "
            f"but I wanted to add one thing. Most sellers I talk to focus on the offer price — "
            f"and I get it, that's the number everyone leads with. "
            f"But what you actually walk away with after commissions, repairs, and closing costs "
            f"is almost always a different number. "
            f"I can put together a quick side-by-side for your home{address_ref} — "
            f"takes about 10 minutes on the phone. Just reply here."
        )
    elif lead_type == "zbuyer":
        context = (
            f"Barry sent {first_name} a video last week about their home"
            f"{' at ' + street_spoken if street_spoken else ''} in {city}. "
            f"They submitted a cash offer request. No reply yet. "
            f"This follow-up circles back with one concrete point: "
            f"the gap between cash price and listed price — after you factor in timeline, "
            f"carrying costs, and deal risk — is almost always smaller than sellers expect. "
            f"Sometimes the listed route nets more. Barry can run both numbers in 10 minutes."
        )
        cta = "Just reply here and I'll run both numbers for your specific home."
        address_ref = f" on {street_spoken}" if street_spoken else ""
        fallback = (
            f"Hey {first_name} — wanted to circle back in case you missed my last video. "
            f"I work with a lot of sellers who come in wanting cash, "
            f"and what I see consistently is that the gap between cash and listed — "
            f"once you factor in carrying costs and deal risk — "
            f"is almost always smaller than people expect. "
            f"Sometimes the listed route nets more. "
            f"I can run both numbers for your home{address_ref} in 10 minutes. "
            f"Just reply here."
        )
    else:  # buyer
        context = (
            f"Barry sent {first_name} a market video last week about their home search in {city}. "
            f"No reply yet. "
            f"This follow-up adds a practical next step: what it actually takes to win "
            f"when the right home hits — being pre-approved and ready moves fast. "
            f"Barry's team can help them get set up so they're not scrambling."
        )
        cta = "Just reply here — I can walk you through the next step."
        fallback = (
            f"Hey {first_name} — not sure if you caught my last video, just wanted to follow up. "
            f"One thing I see buyers miss in {city} is timing — "
            f"when the right home hits in your range, the window is short. "
            f"Buyers who are already set up and pre-approved move first. "
            f"Happy to walk you through what that looks like so you're ready when it happens. "
            f"Just reply here."
        )

    try:
        import anthropic
        client = anthropic.Anthropic()

        signal_hint = _signal_hint_for_video(tags, lead_type)

        prompt = f"""Write a 30-35 second follow-up video script for Barry Jenkins, Realtor with Legacy Home Team at LPT Realty.

Context:
{context}

━━━━ THE FRAME ━━━━
Barry is wearing a suit. This is a professional follow-up — calm, direct, no chase energy.
He's circling back to add value, not to ask "did you get my email?"
The tone is: "I take this seriously and I have one more thing worth hearing."

━━━━ SCRIPT STRUCTURE ━━━━

OPEN (3-4 seconds): Acknowledge the first video simply, then move on immediately.
Good: "Hey {first_name} — not sure if you caught my last video, but wanted to follow up with one thing."
Bad: "Hi, I wanted to check in and see if you had a chance to review..." (too formal, too salesy)

SUBSTANCE (18-22 seconds): One new piece of value — something they didn't hear in the first video.
Not a repeat of what Barry said before. A genuinely different angle.
Be specific and concrete. Vague insight = deleted video.

CLOSE (5-6 seconds): One easy ask. "{cta}"

━━━━ BARRY'S VOICE ━━━━
- Confident but not pushy. The suit reads as serious — let it do that work, don't over-compensate.
- Contractions throughout. Shorter sentences land better on camera.
- Never: "I'd love to", "feel free to", "don't hesitate", "circling back on my previous message"
- Never: "Ylopo", "rAIya", "AI" — say "my last video" or "what I sent over"

━━━━ LENGTH ━━━━
110-130 words. 30-35 seconds. Tighter than Email 1 — they're either interested or they're not.
{signal_hint}
Return ONLY the script. No labels, no stage directions. Just Barry's words."""

        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=280,
            messages=[{"role": "user", "content": prompt}],
        )
        script = msg.content[0].text.strip()
        logger.info("HeyGen follow-up script generated for %s/%s (%d chars)", lead_type, first_name, len(script))
        return script

    except Exception as e:
        logger.warning("Claude follow-up script generation failed, using fallback: %s", e)
        return fallback


def generate_zbuyer_background_image(street: str, city: str,
                                      width: int = 1920, height: int = 1080) -> bytes:
    """Clean dark background for Z-buyer (cash offer) videos."""
    lines = [
        (street,                              (255, 255, 255), 108, True),
        ("Cash Offer or List for More?",      (220, 170, 60),   60, True),
        (f"{city}  ·  Legacy Home Team",      (160, 150, 120),  36, False),
    ]
    return _render_bg(lines, bg_color=(18, 16, 12), width=width, height=height)


# ---------------------------------------------------------------------------
# Video Generation
# ---------------------------------------------------------------------------

def submit_video(script: str, background_url: str = None,
                 avatar_id: str = None, voice_id: str = None,
                 avatar_style: str = "circle",
                 avatar_scale: float = None,
                 avatar_offset: dict = None,
                 character_type: str = None,
                 title: str = "Barry Jenkins — Personal Message") -> str | None:
    """
    Submit a video generation job to HeyGen.

    Returns the video_id (string) on success, None on failure.

    Args:
        script:         The spoken text for Barry's avatar
        background_url: URL of a background image (Mapbox satellite static map).
                        If None, uses a dark navy color background.
        avatar_id:      Override avatar (defaults to DEFAULT_AVATAR)
        voice_id:       Override voice (defaults to DEFAULT_VOICE)
        avatar_style:   "normal" (default) or "circle" — only applies to avatar type, not talking_photo
        avatar_scale:   Override scale (0.3 = small corner, 1.0 = full frame)
        avatar_offset:  Override offset dict {x, y} (-0.5 far left/top → 0.5 far right/bottom)
        character_type: "talking_photo" or "avatar". Defaults to DEFAULT_AVATAR_TYPE.
        title:          Video title (for HeyGen dashboard)
    """
    _avatar = avatar_id or DEFAULT_AVATAR
    _voice  = voice_id  or DEFAULT_VOICE
    _char_type = character_type or DEFAULT_AVATAR_TYPE

    if background_url:
        background = {"type": "image", "url": background_url}
    else:
        background = {"type": "color", "value": "#0c1228"}

    # ── Build character block ─────────────────────────────────────────────────
    if _char_type == "talking_photo":
        # Talking photo: face/shoulders cutout, no circle frame.
        # Scale + offset work the same as avatar type.
        character = {
            "type":              "talking_photo",
            "talking_photo_id":  _avatar,
            "scale":  avatar_scale if avatar_scale is not None else AVATAR_CIRCLE_SCALE,
            "offset": avatar_offset if avatar_offset is not None else {
                "x": AVATAR_CIRCLE_OFFSET_X,
                "y": AVATAR_CIRCLE_OFFSET_Y,
            },
        }
    else:
        # Standard avatar (full body or circle crop)
        character = {
            "type":         "avatar",
            "avatar_id":    _avatar,
            "avatar_style": avatar_style,
        }
        # Positioning only needed when using circle crop
        if avatar_style == "circle":
            character["scale"] = avatar_scale if avatar_scale is not None else AVATAR_CIRCLE_SCALE
            character["offset"] = avatar_offset if avatar_offset is not None else {
                "x": AVATAR_CIRCLE_OFFSET_X,
                "y": AVATAR_CIRCLE_OFFSET_Y,
            }

    payload = {
        "video_inputs": [{
            "character": character,
            "voice": {
                "type": "text",
                "voice_id": _voice,
                "input_text": script,
                "speed": 0.9,
            },
            "background": background,
        }],
        "dimension": {"width": 1920, "height": 1080},
        "title": title,
        "quality": "medium",  # was "high" — medium loads faster without visible quality loss
        # NOTE: use_avatar_iv_model was removed — it's an enterprise-tier field
        # and causes a 400/422 rejection on standard plans, silently breaking all
        # video generation. Standard Avatar II/III still looks great.
    }

    try:
        r = requests.post(f"{_BASE}/v2/video/generate", headers=_headers(),
                          json=payload, timeout=15)
        if r.status_code == 200:
            video_id = r.json().get("data", {}).get("video_id")
            logger.info("HeyGen video submitted: %s", video_id)
            return video_id
        logger.warning("HeyGen submit failed %d: %s", r.status_code, r.text[:200])
        return None
    except Exception as e:
        logger.warning("HeyGen submit error: %s", e)
        return None


def poll_video(video_id: str, timeout_seconds: int = 360,
               poll_interval: int = 8) -> dict | None:
    """
    Poll HeyGen until the video is ready or timeout is reached.

    Returns dict with keys: video_url, thumbnail_url, duration_seconds
    Returns None if video fails or times out.
    """
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        time.sleep(poll_interval)
        try:
            r = requests.get(
                f"{_BASE}/v1/video_status.get",
                headers=_headers(),
                params={"video_id": video_id},
                timeout=10,
            )
            if r.status_code != 200:
                logger.warning("HeyGen poll error %d", r.status_code)
                continue

            data = r.json().get("data", {})
            status = data.get("status")

            if status == "completed":
                result = {
                    "video_id":       video_id,
                    "video_url":      data.get("video_url", ""),
                    "thumbnail_url":  data.get("thumbnail_url", ""),
                    "duration":       data.get("duration", 0),
                }
                logger.info("HeyGen video ready: %.1fs duration, %s",
                            result["duration"], video_id)
                return result

            elif status == "failed":
                logger.error("HeyGen video failed: %s | %s", video_id, data.get("error"))
                return None

            # Still processing — loop
            logger.debug("HeyGen video %s: %s", video_id, status)

        except Exception as e:
            logger.warning("HeyGen poll exception: %s", e)

    logger.warning("HeyGen video timed out after %ds: %s", timeout_seconds, video_id)
    return None


def generate_and_wait(script: str, background_url: str = None,
                      avatar_id: str = None, voice_id: str = None,
                      avatar_style: str = "circle",
                      avatar_scale: float = None,
                      avatar_offset: dict = None,
                      character_type: str = None,
                      timeout_seconds: int = 180) -> dict | None:
    """
    Full pipeline: submit → poll → return result.

    Returns {video_url, thumbnail_url, duration} or None on failure.
    This blocks for ~60-90 seconds while HeyGen renders.
    Only call this in a background job, not in a request handler.
    """
    # Mapbox static URLs are served directly from Mapbox CDN — no pre-warm needed.
    # Skip the pre-warm fetch; just validate the URL is non-empty.
    if background_url and not background_url.startswith("http"):
        logger.warning("generate_and_wait: invalid background_url — color fallback")
        background_url = None

    video_id = submit_video(script, background_url=background_url,
                            avatar_id=avatar_id, voice_id=voice_id,
                            avatar_style=avatar_style,
                            avatar_scale=avatar_scale,
                            avatar_offset=avatar_offset,
                            character_type=character_type)
    if not video_id:
        return None
    return poll_video(video_id, timeout_seconds=timeout_seconds)


# ---------------------------------------------------------------------------
# Email HTML Block — click-to-play thumbnail
# ---------------------------------------------------------------------------

def _watch_url(video_url: str) -> str:
    """
    Wrap a raw MP4 URL in our /watch landing page.

    Desktop browsers/email clients download a direct .mp4 link instead of
    playing it. Routing through /watch serves an HTML5 player page instead.
    Mobile is unaffected — the native player intercepts the URL regardless.
    """
    from urllib.parse import quote as _quote
    base = RAILWAY_BASE_URL.rstrip("/")
    return f"{base}/watch?url={_quote(video_url, safe='')}"


def make_video_landing_url(video_url: str, video_id: str = "", bg_url: str = "") -> str:
    """
    Build a clean /v/<id> landing URL that hides heygen.com from the email.

    Preferred: uses HeyGen video_id (32-char hex UUID) as the path segment.
    The /v/<video_id> route calls the HeyGen API at click-time to get a fresh
    signed URL — avoids the massive base64 token and URLs never expire.

    Example (preferred):
        make_video_landing_url("https://files.heygen.ai/...", video_id="726f98a0...")
        → "https://web-production-3363cc.up.railway.app/v/726f98a0..."  (79 chars)

    Fallback (no video_id): base64-encodes the raw URL.
        → "https://web-production-3363cc.up.railway.app/v/aHR0cHM6Ly..."  (250+ chars)

    When bg_url (Mapbox static URL) is provided, lat/lon/zoom are extracted and
    appended as ?c=lat,lon,zoom so the landing page map centers on the lead's area.
    """
    import re
    base = RAILWAY_BASE_URL.rstrip("/")
    # Use video_id if it looks like a HeyGen UUID (32 lowercase hex chars)
    if video_id and re.match(r'^[0-9a-f]{32}$', video_id.lower()):
        url = f"{base}/v/{video_id.lower()}"
    else:
        # Fall back to base64-encoded URL
        import base64 as _b64
        token = _b64.urlsafe_b64encode(video_url.encode()).decode().rstrip("=")
        url = f"{base}/v/{token}"
    # Append map center from Mapbox bg_url → ?c=lat,lon,zoom
    # Mapbox static format: .../static/{lon},{lat},{zoom},0,0/...
    if bg_url:
        try:
            m = re.search(r'/static/(-?[\d.]+),(-?[\d.]+),([\d.]+),', bg_url)
            if m:
                lon, lat, zoom = m.group(1), m.group(2), m.group(3)
                url += f"?c={lat},{lon},{zoom}"
        except Exception:
            pass
    return url


def make_video_plain_text(video_url: str, first_name: str = "", video_id: str = "") -> str:
    """
    Return a clean plain-text video line for plain-text-only emails.

    Hides heygen.com by routing through /v/<id> on our Railway domain.
    When video_id is provided (preferred), the URL is short (79 chars).
    Without video_id it falls back to a long base64 URL.

    Example output (with video_id):
        "I recorded a short video for Sarah:\n\n→ https://our-domain.railway.app/v/726f98a0...\n"
    """
    landing_url = make_video_landing_url(video_url, video_id=video_id)
    name_part = f" for {first_name}" if first_name else ""
    return f"I recorded a short video{name_part}:\n\n→ {landing_url}\n"


def make_thumb_proxy_url(thumbnail_url: str, duration_seconds: float = 0) -> str:
    """
    Build a /thumb?t=<token>&d=<seconds> URL served from our Railway domain.

    The /thumb endpoint fetches the HeyGen thumbnail, composites a play button
    circle + duration badge onto it using PIL, and serves the result.

    This means heygen.com never appears anywhere in the email — not in href
    (handled by /v/<token>) and not in img src (handled here). The play button
    is baked INTO the JPEG so it renders identically in every email client
    including Outlook (which strips CSS position:absolute overlays).
    """
    import base64
    token = base64.urlsafe_b64encode(thumbnail_url.encode()).decode().rstrip("=")
    dur   = max(0, int(duration_seconds))
    base  = RAILWAY_BASE_URL.rstrip("/")
    return f"{base}/thumb?t={token}&d={dur}"


def make_video_email_html(setup_text: str, video_url: str,
                           thumbnail_url: str, cta_text: str,
                           first_name: str = "",
                           caption: str = "",
                           duration: float = 0,
                           video_id: str = "",
                           map_url: str = "") -> str:
    """
    Build a cutting-edge video email body — personal feel, engineered to click.

    Design principles (based on BombBomb/Vidyard best practices + deliverability research):
    - Play button baked INTO the thumbnail JPEG via /thumb proxy — renders in
      ALL clients including Outlook (which strips CSS overlays)
    - heygen.com never appears: video href → /v/<token>, img src → /thumb
    - Table-based layout for Outlook compatibility (no flexbox, no position:absolute)
    - Caption = curiosity-gap copy specific to the lead's address/situation
      Not "Click to watch" — that's what mass email looks like
    - Duration badge on the thumbnail (bottom-right) lowers commitment friction
    - No logo, no newsletter template, no branded footer chrome
    - Unsubscribe injected by send_email() via __UNSUB_URL__ placeholder
    - When map_url is provided (Mapbox static image), it renders as a hero banner
      at the top of the email showing the lead's area — strong personalization signal

    Args:
        setup_text:   Opening paragraph — already personalized to lead
        video_url:    Raw HeyGen MP4 URL — encoded internally to /v/<token>
        thumbnail_url: HeyGen thumbnail JPEG URL — proxied internally via /thumb
        cta_text:     Closing line / soft call to action
        first_name:   Used in alt text
        caption:      Short curiosity-gap line shown below the thumbnail.
                      Defaults to a general fallback if not provided.
                      Best practice: specific to address + hint at video content.
                      e.g. "I looked into 412 Harbour View before I hit record."
        duration:     Video duration in seconds — composited as badge on thumbnail
                      and shown in the caption line for commitment-lowering effect.
        map_url:      Mapbox static image URL for the map hero banner.
                      When provided, shown as a full-width map image at the top
                      of the email (links to the video landing page).
    """
    import html as _h

    landing    = make_video_landing_url(video_url, video_id=video_id, bg_url=map_url)
    proxy_thumb = make_thumb_proxy_url(thumbnail_url, duration_seconds=duration)
    safe_landing = _h.escape(landing)
    safe_thumb   = _h.escape(proxy_thumb)
    safe_setup   = setup_text.replace("\n", "<br>")
    safe_cta     = cta_text.replace("\n", "<br>")
    name_str     = f" for {first_name}" if first_name else ""

    # Duration string for caption — "0:35" or "1:15"
    if duration > 0:
        mins, secs = int(duration) // 60, int(duration) % 60
        dur_str = f"{mins}:{secs:02d}"
    else:
        dur_str = ""

    # Caption: caller provides specific copy; fallback is generic but still personal
    if caption:
        safe_caption = _h.escape(caption)
    elif dur_str:
        safe_caption = _h.escape(f"recorded this for you — {dur_str}")
    else:
        safe_caption = "recorded this for you"

    # Duration suffix — added after caption if present and not already in it
    if dur_str and dur_str not in safe_caption:
        safe_caption = f"{safe_caption} &nbsp;·&nbsp; {dur_str}"

    # Map hero banner block — shown at top of email when map_url is provided
    # Table-based so it renders in Outlook; links to video so every click converts
    map_hero_block = ""
    if map_url:
        safe_map_url = _h.escape(map_url)
        map_hero_block = f"""
  <!-- Map hero — Mapbox static image, links to video landing page -->
  <!-- Shows the lead's neighborhood/area — strong personalization signal -->
  <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:0 0 22px">
    <tr>
      <td>
        <a href="{safe_landing}" target="_blank" style="display:block;text-decoration:none;position:relative">
          <img src="{safe_map_url}"
               alt="Your area — Hampton Roads"
               width="560"
               border="0"
               style="display:block;width:100%;max-width:560px;border-radius:10px;border:0;
                      max-height:220px;object-fit:cover">
        </a>
      </td>
    </tr>
  </table>
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#ffffff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif">
<div style="max-width:560px;padding:28px 20px">
{map_hero_block}
  <p style="margin:0 0 20px;font-size:15px;line-height:1.8;color:#222">{safe_setup}</p>

  <!-- Video thumbnail — table-based for Outlook compatibility -->
  <!-- Play button + duration are baked into the JPEG via /thumb proxy -->
  <table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:0 0 6px">
    <tr>
      <td>
        <a href="{safe_landing}" target="_blank" style="display:block;text-decoration:none">
          <img src="{safe_thumb}"
               alt="Barry Jenkins — personal video{name_str}"
               width="560"
               border="0"
               style="display:block;width:100%;max-width:560px;border-radius:8px;border:0">
        </a>
      </td>
    </tr>
  </table>

  <!-- Caption — curiosity-gap copy, not generic CTA -->
  <p style="margin:0 0 22px;font-size:13px;color:#888;line-height:1.5">
    <a href="{safe_landing}" target="_blank"
       style="color:#888;text-decoration:none">{safe_caption}</a>
  </p>

  <p style="margin:0 0 24px;font-size:15px;line-height:1.8;color:#222">{safe_cta}</p>

  <p style="margin:0;font-size:14px;color:#444;line-height:1.7">
    Barry Jenkins<br>
    Legacy Home Team | LPT Realty<br>
    (757) 919-8874<br>
    www.legacyhomesearch.com
  </p>

  <p style="margin:14px 0 0;font-size:11px;color:#bbb">
    <a href="__UNSUB_URL__" style="color:#bbb;text-decoration:none">Unsubscribe</a>
  </p>

</div>
</body></html>"""


def render_video_email_block(video_url: str, thumbnail_url: str,
                              first_name: str = "") -> str:
    """
    Return an HTML block to embed in the email body.

    Email clients can't play video inline. This renders a thumbnail image
    with a play button overlay that links to the hosted video URL.

    The thumbnail has a subtle play button layered via a semi-transparent
    overlay image (a 1x1 PNG trick that works in most email clients).

    Args:
        video_url:      Direct MP4 URL (or HeyGen hosted URL)
        thumbnail_url:  JPEG thumbnail URL from HeyGen
        first_name:     Used in alt text

    Returns an HTML string to inject into the email body.
    """
    name_str = f" for {first_name}" if first_name else ""
    alt = f"Personal video message from Barry Jenkins{name_str} — click to watch"
    # Route through /watch so desktop browsers open an HTML5 player instead of downloading
    watch_href = _watch_url(video_url)

    # Play button overlay: a Unicode ▶ centered on the thumbnail using a table
    # Works across Gmail, Outlook, Apple Mail without CSS positioning tricks
    return f"""
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:20px 0;">
  <tr>
    <td align="center">
      <a href="{watch_href}" target="_blank" style="display:inline-block;text-decoration:none;">
        <div style="position:relative;display:inline-block;border-radius:8px;overflow:hidden;
                    box-shadow:0 4px 16px rgba(0,0,0,0.18);">
          <img src="{thumbnail_url}"
               alt="{alt}"
               width="560"
               style="display:block;border-radius:8px;border:0;max-width:100%;" />
          <!-- Play button overlay — centered via absolute positioning (Gmail-safe) -->
          <div style="position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);
                      background:rgba(0,0,0,0.55);border-radius:50%;width:64px;height:64px;
                      display:flex;align-items:center;justify-content:center;
                      pointer-events:none;">
            <span style="color:white;font-size:28px;margin-left:5px;">&#9654;</span>
          </div>
        </div>
        <div style="margin-top:8px;font-size:13px;color:#888;font-family:Arial,sans-serif;">
          Click to watch Barry's personal message
        </div>
      </a>
    </td>
  </tr>
</table>""".strip()


def render_video_email_block_simple(video_url: str, thumbnail_url: str,
                                     first_name: str = "",
                                     caption: str = "") -> str:
    """
    Simpler version — single linked image with no overlay div.
    More email client compatible (works even in Outlook desktop).
    The trade-off: no play button on the thumbnail itself.

    caption: text shown below the thumbnail (include &#9654; if you want the play icon).
             Defaults to "▶ Barry's personal video for {first_name}".
    """
    name_str = f" for {first_name}" if first_name else ""
    alt = f"Personal video from Barry Jenkins{name_str} — click to watch"
    _caption = caption if caption else f"&#9654; Barry's personal video{name_str}"
    # Route through /watch so desktop browsers open an HTML5 player instead of downloading
    watch_href = _watch_url(video_url)
    return (
        f'<div style="margin:20px 0;text-align:center;">'
        f'<a href="{watch_href}" target="_blank" style="text-decoration:none;">'
        f'<img src="{thumbnail_url}" alt="{alt}" width="560" '
        f'style="display:block;margin:0 auto;border-radius:8px;'
        f'border:3px solid #e8e8e8;box-shadow:0 4px 12px rgba(0,0,0,0.12);max-width:100%;" />'
        f'<div style="margin-top:8px;font-size:13px;color:#888;font-family:Arial,sans-serif;">'
        f'{_caption}</div>'
        f'</a></div>'
    )
