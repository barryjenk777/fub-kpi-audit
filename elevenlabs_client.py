"""
ElevenLabs client — generates Barry's voice notes for iMessage delivery.

Flow:
  1. Lead replies "yes" to permission question via Project Blue webhook
  2. Claude Haiku writes a short, personal voice note script from behavioral data
  3. ElevenLabs renders it in Barry's cloned voice (~1-2 seconds)
  4. Audio is uploaded to Railway and served via /audio/<id> endpoint
  5. Project Blue sends it as an iMessage audio bubble via audioAttachmentUrl

Requirements:
  ELEVENLABS_API_KEY  -- from elevenlabs.io dashboard
  ELEVENLABS_VOICE_ID -- Barry's cloned voice ID (Voices > your clone > copy ID)
"""

import logging
import os
import uuid

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.elevenlabs.io/v1"
REQUEST_TIMEOUT = 30  # voice generation can take a couple seconds

# In-memory store for generated audio blobs keyed by a short ID.
# Railway filesystem is ephemeral so we keep audio in memory.
# Project Blue fetches the audio within seconds; entries expire after 10 min.
_audio_store: dict[str, bytes] = {}
_audio_expiry: dict[str, float] = {}
_AUDIO_TTL_SECONDS = 600  # 10 minutes


def _api_key() -> str:
    return os.environ.get("ELEVENLABS_API_KEY", "")


def _voice_id() -> str:
    return os.environ.get("ELEVENLABS_VOICE_ID", "")


def is_available() -> bool:
    """Return True if ElevenLabs is configured with both key and voice ID."""
    return bool(_api_key() and _voice_id())


def generate_audio(script: str, stability: float = 0.5, similarity_boost: float = 0.85) -> bytes | None:
    """
    Render script in Barry's cloned voice. Returns MP3 bytes or None on failure.

    Args:
        script:           Text to speak. Keep under 250 chars for fastest response.
        stability:        Voice consistency (0-1). Lower = more expressive.
        similarity_boost: How closely to match the original voice (0-1).
    """
    if not is_available():
        logger.warning("ElevenLabs not configured (missing API key or voice ID)")
        return None

    try:
        resp = requests.post(
            f"{BASE_URL}/text-to-speech/{_voice_id()}",
            headers={
                "xi-api-key": _api_key(),
                "Content-Type": "application/json",
                "Accept": "audio/mpeg",
            },
            json={
                "text": script,
                "model_id": "eleven_turbo_v2_5",   # fastest model, best for short clips
                "voice_settings": {
                    "stability": stability,
                    "similarity_boost": similarity_boost,
                    "style": 0.0,
                    "use_speaker_boost": True,
                },
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        logger.info("ElevenLabs generated %d bytes for %d char script", len(resp.content), len(script))
        return resp.content
    except Exception as e:
        logger.error("ElevenLabs generation failed: %s", e)
        return None


def store_audio(audio_bytes: bytes) -> str:
    """
    Store audio bytes in memory and return a short ID for serving.
    The ID is used to build a public URL: /audio/<id>
    """
    import time
    audio_id = uuid.uuid4().hex[:12]
    _audio_store[audio_id] = audio_bytes
    _audio_expiry[audio_id] = time.time() + _AUDIO_TTL_SECONDS
    _cleanup_expired()
    logger.info("Stored audio %s (%d bytes, TTL %ds)", audio_id, len(audio_bytes), _AUDIO_TTL_SECONDS)
    return audio_id


def get_audio(audio_id: str) -> bytes | None:
    """Retrieve stored audio bytes by ID. Returns None if expired or not found."""
    import time
    if audio_id not in _audio_store:
        return None
    if time.time() > _audio_expiry.get(audio_id, 0):
        _audio_store.pop(audio_id, None)
        _audio_expiry.pop(audio_id, None)
        return None
    return _audio_store[audio_id]


def _cleanup_expired():
    """Remove audio entries past their TTL."""
    import time
    now = time.time()
    expired = [k for k, exp in _audio_expiry.items() if now > exp]
    for k in expired:
        _audio_store.pop(k, None)
        _audio_expiry.pop(k, None)
    if expired:
        logger.debug("Cleaned up %d expired audio entries", len(expired))


def generate_voice_note_script(
    person_name: str,
    behavior: dict,
    strategy: str,
    is_seller: bool = False,
    is_zbuyer: bool = False,
) -> str:
    """
    Use Claude Haiku to write the voice note script Barry will deliver.

    This fires AFTER the lead says yes to the permission question.
    The script should feel like Barry left them a personal voice message --
    warm, specific to what they were looking at, ends with a clear next step.

    is_zbuyer: True for cash-offer request leads (Zbuyer source / ZLEAD tag).
               These are homeowners who want a cash number on their property.
               Script is entirely different from a standard seller or buyer.
    is_seller: True for Ylopo Prospecting homeowner leads (home value inquiry).
               Different from Zbuyer — they want market data, not a cash offer.

    Returns a plain text script under 80 words (about 30-45 seconds of audio).
    """
    try:
        import anthropic as _ant
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return _fallback_script(person_name, behavior, is_seller, is_zbuyer)

        b = behavior or {}
        first = (person_name or "there").split()[0]

        client = _ant.Anthropic(api_key=api_key)

        # ── Zbuyer: cash offer request ────────────────────────────────────────
        if is_zbuyer:
            # Pull property address from behavior if available
            prop_hint = ""
            mv = b.get("most_viewed") or {}
            street = mv.get("street") or ""
            city   = mv.get("city") or ""
            if street and city:
                prop_hint = f"their property is at {street}, {city}"
            elif city:
                prop_hint = f"they're in {city}"

            resp = client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=250,
                messages=[{"role": "user", "content": f"""Write a short voice note script for Barry Jenkins to send to {first}, a homeowner in Hampton Roads Virginia who just requested a cash offer on their home.

Property context: {prop_hint if prop_hint else "Hampton Roads area home"}

The lead just said yes to receiving a quick voice recording. Barry is leaving them a voice note.

Barry's position: he works with the top cash buyers in Hampton Roads and can get them a real number as quickly as they want. He just needs to know when he can stop by briefly to review the offer process with them — no commitment, just so the offer is based on the actual condition of the home.

Rules:
- written to be SPOKEN, not read. natural speech patterns.
- all lowercase except proper nouns
- 3-5 sentences only. under 80 words.
- start with "hey {first},"
- establish that we can move as fast as they want on the cash offer
- end with a soft ask about scheduling a quick stop-by to review the offer process — frame it as a formality so the number is accurate, not a sales call
- warm and direct, like a friend who does this every week
- Barry's style: confident without being pushy. the offer is real and fast.

Output only the script text. Nothing else."""}],
            )
            return resp.content[0].text.strip()

        # ── Regular seller: home value / Ylopo Prospecting ────────────────────
        if is_seller:
            notes = []
            mv = b.get("most_viewed") or {}
            if mv.get("street") and mv.get("city"):
                notes.append(f"home is at {mv['street']}, {mv['city']}")
            elif mv.get("city"):
                notes.append(f"in {mv['city']}")

            beh_summary = ". ".join(notes) if notes else "Hampton Roads area"

            resp = client.messages.create(
                model="claude-3-5-haiku-20241022",
                max_tokens=250,
                messages=[{"role": "user", "content": f"""Write a short voice note script for Barry Jenkins to send to {first}, a homeowner in Hampton Roads Virginia who inquired about their home's value.

What we know: {beh_summary}

The lead just said yes to receiving a quick voice recording. Barry is leaving them a voice note.

Rules:
- written to be SPOKEN, not read. natural speech patterns.
- all lowercase except proper nouns
- 3-5 sentences only. under 80 words.
- start with "hey {first},"
- reference what the market is doing in their area specifically
- end with a clear next step — "just text me back" or invite them to connect so Barry can pull the real numbers for their street
- warm and knowledgeable, like a friend who happens to be Hampton Roads' top agent
- Barry's style: direct, no fluff, market intel as a gift not a pitch

Output only the script text. Nothing else."""}],
            )
            return resp.content[0].text.strip()

        # ── Buyer ─────────────────────────────────────────────────────────────
        notes = []
        if b.get("most_viewed"):
            mv = b["most_viewed"]
            addr = mv.get("street") or mv.get("city") or ""
            ct = b.get("most_viewed_ct", 0)
            if addr:
                notes.append(f"viewed {addr} {ct} time{'s' if ct != 1 else ''}")
        if b.get("save_count", 0) > 0:
            notes.append(f"{b['save_count']} saved properties")
        if b.get("price_min") and b.get("price_max"):
            notes.append(f"searching ${b['price_min']:,}-${b['price_max']:,}")
        elif b.get("price_max"):
            notes.append(f"up to ${b['price_max']:,}")
        if b.get("cities"):
            notes.append(f"looking in {', '.join(list(b['cities'])[:2])}")
        if b.get("price_drift", 0) > 15000:
            notes.append(f"search range moved up ${b['price_drift']:,.0f}")

        beh_summary = ". ".join(notes) if notes else "browsing homes in Hampton Roads"

        resp = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=250,
            messages=[{"role": "user", "content": f"""Write a short voice note script for Barry Jenkins to send to {first}, an active home buyer in Hampton Roads Virginia.

What we know about them: {beh_summary}

The lead just said yes to receiving a quick voice recording. Barry is leaving them a voice note.

Rules:
- written to be SPOKEN, not read. natural speech patterns.
- all lowercase except proper nouns
- 3-5 sentences only. under 80 words.
- start with "hey {first},"
- reference one specific thing from their actual search (property, area, or price range)
- end with a clear soft CTA — "just text me back" or "give me a call" — give them a path
- warm and personal, like a friend who knows the market cold
- Barry's style: direct, knowledgeable, never pushy

Output only the script text. Nothing else."""}],
        )
        return resp.content[0].text.strip()

    except Exception as e:
        logger.warning("Voice note script generation failed: %s", e)
        return _fallback_script(person_name, behavior, is_seller, is_zbuyer)


def _fallback_script(person_name: str, behavior: dict, is_seller: bool, is_zbuyer: bool = False) -> str:
    """Generic fallback script when Claude is unavailable."""
    first = (person_name or "there").split()[0]
    b = behavior or {}

    if is_zbuyer:
        mv = b.get("most_viewed") or {}
        area = f" on {mv['street']}" if mv.get("street") else (" in " + mv["city"] if mv.get("city") else "")
        return (
            f"hey {first}, so i work with the top cash buyers in hampton roads and we can move as fast as you want on this. "
            f"the only thing i need is to stop by{area} real quick to review the offer process with you so the number we give you is accurate. "
            f"just text me back and let me know when works."
        )
    elif is_seller:
        mv = b.get("most_viewed") or {}
        area = f" in {mv['city']}" if mv.get("city") else ""
        return (
            f"hey {first}, just wanted to drop you a quick note about what i'm seeing{area} right now. "
            f"the market has been moving and i wanted to give you a real picture of where things stand on your street. "
            f"just text me back and we can dig into the actual numbers together."
        )
    else:
        mv = b.get("most_viewed") or {}
        area = f" around {mv['city']}" if mv.get("city") else (f" around {list(b['cities'])[0]}" if b.get("cities") else "")
        return (
            f"hey {first}, thanks for getting back to me. "
            f"i've been watching the inventory{area} pretty closely and wanted to give you a real rundown on what i'm seeing. "
            f"there are a couple things worth knowing before you make any moves. just text me back or give me a call and we'll go through it."
        )
