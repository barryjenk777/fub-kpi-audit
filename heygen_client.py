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

import logging
import os
import time

import requests

logger = logging.getLogger("heygen_client")

_API_KEY = os.environ.get("HEYGEN_API_KEY", "")
_BASE = "https://api.heygen.com"

# Barry's avatar + voice IDs — confirmed working via API test
AVATAR_CASUAL      = "fffc73aa581a49e4af8dcd304e76349b"   # Barry Jenkins (casual)
AVATAR_SUIT        = "dc3bfe40aeaf4590b76ee12824d019dd"   # Barry Jenkins Suit
AVATAR_MICROPHONE  = "622b6e55473e4b5e821c9cabc8830366"   # Barry Jenkins Suit + Microphone ← DEFAULT

VOICE_CASUAL  = "b37262521af24a0e9245308e4045ac3f"   # Barry Jenkins voice
VOICE_SUIT    = "850bdd18eb164cd8b5d540a88fdf862a"   # Barry Jenkins Suit voice ← DEFAULT for all

# Default: microphone avatar + suit voice (confirmed working, 16s render time)
DEFAULT_AVATAR = AVATAR_MICROPHONE
DEFAULT_VOICE  = VOICE_SUIT

# Background endpoint hosted on Railway — generates branded background on-demand
# HeyGen fetches this URL during rendering. No external storage needed.
# Format: {RAILWAY_URL}/api/heygen-bg?type=seller&address=123+Oak+St&city=Chesapeake
RAILWAY_BASE_URL = os.environ.get("RAILWAY_URL", "https://web-production-3363cc.up.railway.app")


def _headers():
    return {"X-Api-Key": _API_KEY, "Content-Type": "application/json"}


def is_available() -> bool:
    return bool(_API_KEY)


# ---------------------------------------------------------------------------
# Background Image Generator — branded cards served from Railway
# ---------------------------------------------------------------------------

def generate_seller_background_image(address: str, city: str,
                                      width: int = 1280, height: int = 720) -> bytes:
    """
    Generate a branded dark-navy background image for a seller lead video.

    Layout:
      - Dark navy gradient background
      - Legacy Home Team branding (top-left)
      - "Your home at" label + address in large text (upper center)
      - "{City} Market Analysis" subtitle in blue
      - Blue accent bars top/bottom

    Returns raw JPEG bytes. Served via /api/heygen-bg?type=seller&address=...&city=...
    """
    from PIL import Image, ImageDraw, ImageFont
    import io

    img = Image.new('RGB', (width, height), color=(12, 18, 40))
    draw = ImageDraw.Draw(img)

    # Subtle center gradient
    for y in range(height):
        lift = int(18 * (1 - abs(y - height / 2) / (height / 2)))
        for x in range(0, width, 2):
            r, g, b = img.getpixel((x, y))
            draw.point((x, y), fill=(
                min(255, r + lift // 4),
                min(255, g + lift // 3),
                min(255, b + lift),
            ))

    # Accent bars
    draw.rectangle([0, 0, width, 6], fill=(41, 128, 185))
    draw.rectangle([0, height - 6, width, height], fill=(41, 128, 185))

    # Fonts
    try:
        font_xl   = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 80)
        font_lg   = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 38)
        font_sm   = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 26)
        font_brand = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 30)
    except Exception:
        font_xl = font_lg = font_sm = font_brand = ImageFont.load_default()

    # Legacy Home Team branding (top-left)
    draw.text((44, 38), "LEGACY", fill=(255, 255, 255), font=font_brand)
    draw.text((44, 74), "HOME TEAM", fill=(41, 128, 185), font=font_brand)

    # "Your home at" label
    draw.text((width // 2, 190), "Your home at", fill=(160, 195, 220), font=font_sm, anchor="mm")

    # Address — large center
    draw.text((width // 2, 285), address, fill=(255, 255, 255), font=font_xl, anchor="mm")

    # City Market Analysis subtitle
    draw.text((width // 2, 370), f"{city} Market Analysis",
              fill=(41, 128, 185), font=font_lg, anchor="mm")

    # Subtle divider line
    draw.line([(width // 2 - 220, 410), (width // 2 + 220, 410)],
              fill=(41, 90, 145), width=1)

    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=90)
    return buf.getvalue()


def generate_buyer_background_image(city: str, price_band: str = "",
                                     width: int = 1280, height: int = 720) -> bytes:
    """
    Branded background for buyer lead videos.

    Layout: dark navy, "Homes in {city}" headline, price band subtitle.
    Barry's avatar sits circle-bottom-left; text fills upper center.
    """
    from PIL import Image, ImageDraw, ImageFont
    import io

    img = Image.new('RGB', (width, height), color=(10, 22, 38))
    draw = ImageDraw.Draw(img)

    for y in range(height):
        lift = int(15 * (1 - abs(y - height / 2) / (height / 2)))
        for x in range(0, width, 2):
            r, g, b = img.getpixel((x, y))
            draw.point((x, y), fill=(
                min(255, r + lift // 3),
                min(255, g + lift // 2),
                min(255, b + lift),
            ))

    draw.rectangle([0, 0, width, 6], fill=(26, 188, 156))
    draw.rectangle([0, height - 6, width, height], fill=(26, 188, 156))

    try:
        font_xl    = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 76)
        font_lg    = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 36)
        font_sm    = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 26)
        font_brand = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 30)
    except Exception:
        font_xl = font_lg = font_sm = font_brand = ImageFont.load_default()

    draw.text((44, 38), "LEGACY", fill=(255, 255, 255), font=font_brand)
    draw.text((44, 74), "HOME TEAM", fill=(26, 188, 156), font=font_brand)

    draw.text((width // 2, 190), "Homes in", fill=(160, 205, 220), font=font_sm, anchor="mm")
    draw.text((width // 2, 285), city, fill=(255, 255, 255), font=font_xl, anchor="mm")

    if price_band:
        draw.text((width // 2, 370), price_band, fill=(26, 188, 156), font=font_lg, anchor="mm")
        draw.line([(width // 2 - 180, 410), (width // 2 + 180, 410)],
                  fill=(20, 100, 80), width=1)

    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=90)
    return buf.getvalue()


def get_background_url(bg_type: str, address: str = "", city: str = "",
                        price_band: str = "") -> str:
    """
    Return the Railway endpoint URL that serves the branded background image.
    HeyGen fetches this URL when rendering the video.
    """
    from urllib.parse import quote
    base = f"{RAILWAY_BASE_URL}/api/heygen-bg"
    params = [f"type={bg_type}"]
    if address:
        params.append(f"address={quote(address)}")
    if city:
        params.append(f"city={quote(city)}")
    if price_band:
        params.append(f"price_band={quote(price_band)}")
    return base + "?" + "&".join(params)


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

Barry is recording this for {first_name}, a homeowner at {street} in {city}.
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
        # Fallback preserves the same organic frame
        comp_line = f" {comp_snippet}" if comp_snippet else (
            f" Right now in {city}, homes are moving — sellers are getting strong offers "
            f"and the window is real."
        )
        return (
            f"Hey {first_name} — so I was literally just finishing up a market video for one of my clients "
            f"over in {city} and I realized my assistant and I never actually followed up with you after "
            f"that conversation about your place on {street}. So I figured, let me just do one for you "
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

        prompt = f"""Write a 35-40 second video script for Barry Jenkins, #1 real estate team in Virginia.

Barry is recording this for {first_name}, a homeowner at {street} in {city}.
They submitted a CASH OFFER REQUEST online — they want to sell, they want speed,
and their inbox is already flooded with "WE BUY HOUSES" pitches.

━━━━ THE FRAME (non-negotiable) ━━━━
Same organic setup as his other client videos — Barry was already recording for clients,
thought of this person, pulled one together. BUT the tone is different here.

This lead took action. They're not curious — they're ready to move.
The video needs ENERGY behind it. Not hype. Not desperate. More like:
"I can actually solve this for you. Here's exactly how."

━━━━ BARRY'S EDGE (the whole point) ━━━━
Most investors can ONLY offer cash. Most agents can ONLY list.
Barry can do BOTH — and he can show {first_name} which one nets more for their specific home.

That's it. That's the differentiator. The video should land this clearly.

━━━━ SCRIPT STRUCTURE ━━━━

OPEN (3-5 seconds): Start mid-thought, slightly more energized than the seller video.
Good: "Hey {first_name} — so I was wrapping up a couple videos for some clients and your cash offer request came through..."
Good: "Hey {first_name}, saw your request come in — was already recording some client videos so I figured let me just do one for you right now."
Bad: "Hi, I'm Barry Jenkins and I'd like to talk about your home." (kills the energy)

THE PIVOT (5-7 seconds): Get to the point — you can actually do this.
"I can close cash in as little as 7 days. Done. No showings, no stress, no financing falling through."

THE DIFFERENTIATOR (8-10 seconds): This is what separates him from every other response they got.
"Here's what most of the other investors who reached out can't do: I'm also a licensed agent,
so I can pull the MLS numbers and show you what listing might net — and sometimes that number
is significantly higher. Most people don't know they can compare both options before deciding."

MARKET CREDIBILITY (8-10 seconds): Ground it in {city}.
{comp_line}
Barry knows this market. One specific insight about what cash buyers are paying vs. what sellers are listing for.

CLOSE (5-7 seconds): Direct but not pushy. Make the call feel easy.
Good: "10 minutes on the phone — I'll run both numbers for your specific address. That's it."
Good: "Reply here or call me directly. Let's figure out which option actually makes more sense for you."
Bad: "I'd love to schedule a consultation at your earliest convenience."

━━━━ TONE ━━━━
Calm confidence, not hustle. The "I can actually solve this" guy, not the "WE BUY HOUSES" guy.
Never: "I'd love to", "feel free to", "don't hesitate", "I'm happy to"
Contractions throughout. Conversational pace. He's on camera — shorter sentences land better.
No Ylopo, no platform names.

━━━━ LENGTH ━━━━
130-150 words. 35-42 seconds at normal pace.

Return ONLY the script. No labels, no stage directions, no quotes. Just Barry's words."""

        msg = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}],
        )
        script = msg.content[0].text.strip()
        logger.info("HeyGen Z-buyer script generated for %s (%d chars)", first_name, len(script))
        return script

    except Exception as e:
        logger.warning("Claude Z-buyer script generation failed, using fallback: %s", e)
        comp_line = f" {comp_snippet}" if comp_snippet else ""
        return (
            f"Hey {first_name} — saw your cash offer request come in and I was already "
            f"recording some videos for clients so I figured let me do one for you right now. "
            f"I can close cash in as little as 7 days — no showings, no financing falling through, done. "
            f"Here's what most of the people who responded to you can't offer: "
            f"I'm also a licensed agent, so I can pull the MLS numbers and show you "
            f"what listing your home might actually net.{comp_line} "
            f"Sometimes that number is significantly higher than cash — sometimes it's not worth the wait. "
            f"Either way, you deserve to see both before you decide. "
            f"10 minutes on the phone and I'll run both numbers for your specific place on {street}. "
            f"Just reply here."
        )


def generate_zbuyer_background_image(street: str, city: str,
                                      width: int = 1280, height: int = 720) -> bytes:
    """
    Branded background for Z-buyer (cash offer) videos.

    Layout: dark charcoal background, "Cash Offer vs. Listing" headline,
    address below, city context line. More direct energy than seller background.
    Barry's avatar sits in the corner; content fills the upper portion.
    """
    from PIL import Image, ImageDraw, ImageFont
    import io

    img = Image.new('RGB', (width, height), color=(18, 18, 24))
    draw = ImageDraw.Draw(img)

    # Subtle warm gradient (slightly warmer than navy — more urgency)
    for y in range(height):
        lift = int(14 * (1 - abs(y - height / 2) / (height / 2)))
        for x in range(0, width, 2):
            r, g, b = img.getpixel((x, y))
            draw.point((x, y), fill=(
                min(255, r + lift),
                min(255, g + lift // 2),
                min(255, b + lift // 4),
            ))

    # Accent bars — warmer gold/amber tone to signal action
    draw.rectangle([0, 0, width, 6], fill=(230, 160, 30))
    draw.rectangle([0, height - 6, width, height], fill=(230, 160, 30))

    try:
        font_xl    = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 58)
        font_lg    = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 76)
        font_med   = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 36)
        font_sm    = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 26)
        font_brand = ImageFont.truetype("/System/Library/Fonts/Helvetica.ttc", 30)
    except Exception:
        font_xl = font_lg = font_med = font_sm = font_brand = ImageFont.load_default()

    # Legacy Home Team branding
    draw.text((44, 38), "LEGACY", fill=(255, 255, 255), font=font_brand)
    draw.text((44, 74), "HOME TEAM", fill=(230, 160, 30), font=font_brand)

    # "Cash Offer vs. Listing" — the two options framing
    draw.text((width // 2, 175), "Cash Offer  vs.  Listing", fill=(230, 160, 30), font=font_xl, anchor="mm")

    # Address
    draw.text((width // 2, 275), street, fill=(255, 255, 255), font=font_lg, anchor="mm")

    # City context
    draw.text((width // 2, 360), f"{city} — We'll run both numbers",
              fill=(200, 200, 200), font=font_med, anchor="mm")

    draw.line([(width // 2 - 240, 400), (width // 2 + 240, 400)],
              fill=(100, 80, 20), width=1)

    buf = io.BytesIO()
    img.save(buf, format='JPEG', quality=90)
    return buf.getvalue()


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
        avatar_id:      Override avatar (defaults to AVATAR_MICROPHONE)
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
                "avatar_style": "normal",
            },
            "voice": {
                "type": "text",
                "voice_id": _voice,
                "input_text": script,
                "speed": 1.0,
            },
            "background": background,
        }],
        "dimension": {"width": 1280, "height": 720},
        "title": title,
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


def poll_video(video_id: str, timeout_seconds: int = 180,
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


def generate_and_wait(script: str, use_suit: bool = False,
                      timeout_seconds: int = 180) -> dict | None:
    """
    Full pipeline: submit → poll → return result.

    Returns {video_url, thumbnail_url, duration} or None on failure.
    This blocks for ~60-90 seconds while HeyGen renders.
    Only call this in a background job, not in a request handler.
    """
    video_id = submit_video(script, use_suit=use_suit)
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
