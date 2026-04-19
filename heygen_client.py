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
DEFAULT_AVATAR = "4b314392e9b6441d97abaf6d83808de5"   # Barry Jenkins — Avatar 2 (confirmed best)
DEFAULT_VOICE  = "b37262521af24a0e9245308e4045ac3f"   # Barry Jenkins — HeyGen native voice clone

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


def get_background_url(bg_type: str, address: str = "", city: str = "",
                        price_band: str = "") -> str:
    """
    Return the URL HeyGen will fetch as the video background.

    Z-buyer + buyer use static pre-rendered JPEGs (faster, always available).
    Seller uses the dynamic endpoint so the lead's address appears on screen.
    """
    from urllib.parse import quote

    # Circle avatar covers centered text — use solid color backgrounds for now.
    # TODO: reposition text to top or bottom thirds once circle placement is confirmed.
    return None


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
# Script Generation — Claude writes the 35-second seller video script
# ---------------------------------------------------------------------------

def generate_seller_video_script(first_name: str, street: str, city: str,
                                  comp_snippet: str = "", ai_convo: bool = True) -> str:
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
            f"Include this real market data naturally in the middle section: {comp_snippet}"
            if comp_snippet
            else f"Include one specific, credible insight about the {city} market — "
                 f"something about how fast homes are moving, what sellers are netting, "
                 f"or what the inventory looks like right now. Be specific, not vague."
        )

        prompt = f"""Write a 35-40 second video script for Barry Jenkins, #1 real estate team in Virginia.

Barry is recording this for {first_name}, a homeowner at {street_spoken} in {city}.
Context: they spoke with Barry's AI assistant about their home value and got transferred to his team.
They are warm — they reached out first. This is Barry's human follow-up.

━━━━ THE FRAME (non-negotiable) ━━━━
Barry was already recording market update videos for a few of his seller clients.
Mid-session, he remembered this person and pulled one together for them on the spot.

The video should feel SPONTANEOUS — like he pivoted from client work to record this.
NOT polished. NOT formal. NOT a marketing video.
The believability of "I was already doing this" is the entire hook.

━━━━ SCRIPT STRUCTURE ━━━━

OPEN (3-5 seconds): Catch them off-guard. Start mid-thought, not with a formal intro.
Good: "Hey {first_name} — so I was literally just finishing up a market video for one of my clients over in {city}..."
Good: "Hey {first_name}, I was recording a few of these for some of my sellers this week..."
Bad: "Hi, I'm Barry Jenkins with Legacy Home Team..." (too formal, kills the frame)
Bad: "I wanted to reach out about your home..." (sounds like a script)

PIVOT (5-7 seconds): The "remembered you" moment. Natural, not salesy.
"...and I realized my assistant and I never actually followed up with you after that conversation about your place on {street}. So I figured — let me just do one for you while I'm at it."
Or: "...and it hit me — we spoke about your home on {street} but never actually connected."

MARKET INTEL (15-20 seconds): This is the credibility section. Barry knows {city}.
{comp_line}
Frame it as: "here's what I'm actually seeing right now for homes in your area."
This is why the video is worth watching. Be specific — vague market commentary kills trust.

SOFT CLOSE (5-8 seconds): One low-friction ask. Not a pitch. Not an appointment demand.
Good: "Would it make sense to do a quick call? Even 10 minutes — I can walk you through the full picture."
Good: "Text me back or reply here — whatever's easier."
Bad: "I'd love to schedule a time to discuss your real estate needs." (too formal)
Bad: "Feel free to reach out" (passive, sounds like it's on them)

━━━━ VOICE ━━━━
- Barry is 20+ years in Hampton Roads. He talks like a knowledgeable friend, not a pitch man.
- Conversational pace. Short sentences on camera land better than long ones.
- Never say: "Ylopo", "rAIya", "AI", any platform name — say "my assistant"
- Never say: "I'd love to", "feel free to", "don't hesitate", "happy to help"
- Contractions throughout. "I'm", "I've", "you're", "let's" — not formal language.

━━━━ LENGTH ━━━━
Target: 130-150 words. At normal speaking pace = 35-42 seconds. That's the sweet spot.
Too short = doesn't build credibility. Too long = they stop watching.

Return ONLY the script. No labels, no stage directions, no quotes around it. Just Barry's words."""

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
        comp_line = f" {comp_snippet}" if comp_snippet else (
            f" Right now in {city}, homes are moving — sellers are getting strong offers "
            f"and the window is real."
        )
        return (
            f"Hey {first_name} — so I was literally just finishing up a market video for one of my clients "
            f"over in {city} and I realized my assistant and I never actually followed up with you after "
            f"that conversation about your place on {street_spoken}. So I figured, let me just do one for you "
            f"while I'm at it.{comp_line} "
            f"I can walk you through exactly what I'm seeing for homes like yours right now — "
            f"what sellers are actually netting, how fast things are moving. "
            f"Would it make sense to do a quick 10-minute call? Just reply here and we'll find a time."
        )


def generate_zbuyer_video_script(first_name: str, street: str, city: str,
                                  comp_snippet: str = "") -> str:
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
            else (
                f"Include one grounding line about what homes in {city} are actually moving for "
                f"right now — cash vs. listed. Something specific, not vague."
            )
        )

        prompt = f"""Write a 35-40 second video script for Barry Jenkins, Realtor with Legacy Home Team at LPT Realty — #1 real estate team in Virginia.

Barry is recording this for {first_name}, a homeowner at {street_spoken} in {city}.
They submitted a CASH OFFER REQUEST online — they want to sell, they want speed,
and their inbox is already flooded with "WE BUY HOUSES" pitches.

━━━━ THE FRAME (non-negotiable) ━━━━
Barry was already recording client videos. Saw the request come in, pulled one together on the spot.
Calm confidence. The "I can actually solve this" guy, not the "WE BUY HOUSES" guy.

━━━━ BARRY'S CREDENTIALS (weave in naturally) ━━━━
- Realtor with Legacy Home Team at LPT Realty (say it this way, not just "licensed agent")
- Almost 30 years serving this community
- Can do BOTH: cash close OR list on MLS — whichever puts more money in {first_name}'s pocket

━━━━ SCRIPT STRUCTURE ━━━━

OPEN (3-5 seconds): Mid-thought, saw the request come in.
Good: "Hey {first_name} — saw your cash offer request come through for your home at {street_spoken} in {city}..."

THE PITCH (15-20 seconds): Cash option + the differentiator.
"I can close cash in as little as 7 days — no showings, no stress, done.
But here's what sets me apart: as a Realtor with Legacy Home Team at LPT Realty, I can also
pull the MLS numbers and show you what listing might net. Sometimes that's significantly more.
I've been serving this community for almost 30 years — I know this market."

MARKET CREDIBILITY (8-10 seconds):
{comp_line}

CLOSE (5-7 seconds): Direct. Easy ask.
"10 minutes on the phone — I'll run both numbers for your home at {street_spoken}. Just reply here."

━━━━ RULES ━━━━
- Say "Realtor with Legacy Home Team at LPT Realty" — not "licensed agent"
- Mention almost 30 years in the community
- Reference the street address and city naturally
- Never: "I'd love to", "feel free to", "don't hesitate"
- Contractions throughout. Shorter sentences on camera.
- No Ylopo, no platform names, no "AI"

━━━━ LENGTH ━━━━
130-150 words. 35-42 seconds.

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
            f"Hey {first_name} — saw your cash offer request come through for your home at "
            f"{street_spoken} in {city}, and I was already recording some videos for clients "
            f"so I pulled one together for you right now. "
            f"I can close cash in as little as 7 days — no showings, no financing falling through, done. "
            f"But here's what separates me from everyone else who responded: "
            f"I'm a Realtor with Legacy Home Team at LPT Realty, and I've been serving this "
            f"community for almost 30 years. I can also pull the MLS numbers and show you "
            f"what listing your home might actually net.{comp_line} "
            f"Sometimes that number is significantly higher than cash. "
            f"Either way, you deserve to see both before you decide. "
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

        prompt = f"""Write a 35-40 second video script for Barry Jenkins, Realtor with Legacy Home Team at LPT Realty — #1 real estate team in Virginia (850+ homes a year).

Barry is recording this for {first_name}, a buyer searching for a {search_desc}.
{strategy_context}

━━━━ THE FRAME (non-negotiable) ━━━━
Barry was already recording market update videos for other buyer clients.
He saw {first_name}'s search come through and pulled one together on the spot.
Same "I was already doing this" energy — not a cold pitch, a warm pivot.

━━━━ SCRIPT STRUCTURE ━━━━

OPEN (3-5 seconds): Mid-thought, natural. Reference the city or search.
Good: "Hey {first_name} — was just finishing up a video for another buyer looking in {city} and saw your search come through, so I pulled one together for you."
Bad: "Hi, I'm Barry Jenkins..." (too formal)

MARKET INTEL (20-25 seconds — this is the heart of the video):
Give a genuinely MEATY, specific snapshot of what buyers in {city} {f"in the {price_str} range" if price_str else ""} are actually navigating right now.

Pick 2-3 of these angles and make them specific — not vague:
• Inventory reality: how much is actually available in that city/price range, and how fast it moves
• Competition dynamics: are buyers seeing multiple offers? Is it softening? What price points are hottest?
• What buyers get wrong going in — the mistake most make before working with an agent
• What Barry's volume (850+ homes/year at Legacy Home Team) gives them that Zillow can't — pocket listings, relationships with listing agents, knowing what's coming before it's public
• Something neighborhood-specific or price-tier-specific that sounds like inside knowledge
• The rate/affordability reality if relevant — not a prediction, just what buyers are actually doing now

This is the credibility section. Specific beats generic every time.
{"If they're circling " + expand_address_for_speech(most_viewed_street) + ", mention you can get them more info on that specific one." if most_viewed_street and strategy in ("saved_property", "repeat_view") else ""}

CLOSE (5-7 seconds): Low friction. One soft ask.
Good: "Happy to walk you through what we're actually seeing right now — just reply here."
Good: "10 minutes on the phone and I can show you what's real in your search right now."
Bad: "I'd love to schedule an appointment to discuss your needs." (too formal)

━━━━ BARRY'S VOICE ━━━━
- Almost 30 years in Hampton Roads. Talks like a knowledgeable friend, not a pitch man.
- Short sentences. Contractions throughout. No fluff.
- Never: "I'd love to", "feel free to", "don't hesitate", "happy to help", "reach out"
- Never: "Ylopo", "rAIya", "AI", "platform" — say "your search" or "you were looking at"
- Credibility comes from specificity, not self-promotion

━━━━ LENGTH ━━━━
130-150 words. At normal speaking pace = 35-42 seconds.

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

        # Fallback — still references their city and price range specifically
        price_line = f" in the {price_str} range" if price_str else ""
        viewed_line = (
            f" I also saw you were circling {expand_address_for_speech(most_viewed_street)} — "
            f"I can pull the full picture on that one too."
            if most_viewed_street and strategy in ("saved_property", "repeat_view")
            else ""
        )
        return (
            f"Hey {first_name} — was just finishing up a video for another buyer looking in {city} "
            f"and saw your search come through, so I pulled one together for you. "
            f"Here's what I'm actually seeing right now{price_line} in {city}: "
            f"inventory is moving fast — homes in that price range are averaging a short window before "
            f"they're gone, and the buyers who win are the ones who are already set up and ready to move "
            f"when the right one hits. "
            f"Our team closes 850-plus homes a year here in Hampton Roads, which means I'm hearing about "
            f"homes before they even hit the market.{viewed_line} "
            f"10 minutes on the phone and I can walk you through what's real in your search right now. "
            f"Just reply here."
        )


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
                 title: str = "Barry Jenkins — Personal Message") -> str | None:
    """
    Submit a video generation job to HeyGen.

    Returns the video_id (string) on success, None on failure.

    Args:
        script:         The spoken text for Barry's avatar
        background_url: URL of a background image (our Railway /api/heygen-bg endpoint).
                        If None, uses a dark navy color background.
        avatar_id:      Override avatar (defaults to AVATAR_SELLER)
        voice_id:       Override voice (defaults to VOICE_SUIT)
        title:          Video title (for HeyGen dashboard)
    """
    _avatar = avatar_id or DEFAULT_AVATAR
    _voice  = voice_id  or DEFAULT_VOICE

    if background_url:
        background = {"type": "image", "url": background_url}
    else:
        background = {"type": "color", "value": "#0c1228"}

    payload = {
        "video_inputs": [{
            "character": {
                "type": "avatar",
                "avatar_id": _avatar,
                "avatar_style": "circle",
            },
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
        "quality": "high",
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
                      timeout_seconds: int = 180) -> dict | None:
    """
    Full pipeline: submit → poll → return result.

    Returns {video_url, thumbnail_url, duration} or None on failure.
    This blocks for ~60-90 seconds while HeyGen renders.
    Only call this in a background job, not in a request handler.
    """
    # Pre-warm the background URL so Railway has it cached before HeyGen fetches it.
    # Without this, Railway generates the PIL image on HeyGen's first fetch (~400ms),
    # which may exceed HeyGen's asset download timeout and silently drop the background.
    if background_url:
        try:
            r = requests.get(background_url, timeout=10)
            if r.status_code == 200:
                logger.info("Background pre-warmed (%d bytes): %s",
                            len(r.content), background_url[:60])
            else:
                logger.warning("Background pre-warm returned %d — using color fallback",
                               r.status_code)
                background_url = None
        except Exception as e:
            logger.warning("Background pre-warm failed (%s) — using color fallback", e)
            background_url = None

    video_id = submit_video(script, background_url=background_url,
                            avatar_id=avatar_id, voice_id=voice_id)
    if not video_id:
        return None
    return poll_video(video_id, timeout_seconds=timeout_seconds)


# ---------------------------------------------------------------------------
# Email HTML Block — click-to-play thumbnail
# ---------------------------------------------------------------------------

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

    # Play button overlay: a Unicode ▶ centered on the thumbnail using a table
    # Works across Gmail, Outlook, Apple Mail without CSS positioning tricks
    return f"""
<table cellpadding="0" cellspacing="0" border="0" width="100%" style="margin:20px 0;">
  <tr>
    <td align="center">
      <a href="{video_url}" target="_blank" style="display:inline-block;text-decoration:none;">
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
                                     first_name: str = "") -> str:
    """
    Simpler version — single linked image with no overlay div.
    More email client compatible (works even in Outlook desktop).
    The trade-off: no play button on the thumbnail itself.
    """
    name_str = f" for {first_name}" if first_name else ""
    alt = f"Personal video from Barry Jenkins{name_str} — click to watch"
    return (
        f'<div style="margin:20px 0;text-align:center;">'
        f'<a href="{video_url}" target="_blank" style="text-decoration:none;">'
        f'<img src="{thumbnail_url}" alt="{alt}" width="560" '
        f'style="display:block;margin:0 auto;border-radius:8px;'
        f'border:3px solid #e8e8e8;box-shadow:0 4px 12px rgba(0,0,0,0.12);max-width:100%;" />'
        f'<div style="margin-top:8px;font-size:13px;color:#888;font-family:Arial,sans-serif;">'
        f'&#9654; Watch Barry\'s personal message</div>'
        f'</a></div>'
    )
