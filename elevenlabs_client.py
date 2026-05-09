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
) -> str:
    """
    Use Claude Haiku to write the voice note script Barry will deliver.

    This fires AFTER the lead says yes to the permission question.
    The script should feel like Barry left them a personal voice message --
    warm, specific to what they were looking at, ends with an open door.

    Returns a plain text script under ~200 words (about 60-90 seconds of audio).
    """
    try:
        import anthropic as _ant
        api_key = os.environ.get("ANTHROPIC_API_KEY", "")
        if not api_key:
            return _fallback_script(person_name, behavior, is_seller)

        b = behavior or {}
        first = (person_name or "there").split()[0]

        # Build a compact behavioral summary for the prompt
        notes = []
        if b.get("most_viewed"):
            mv = b["most_viewed"]
            addr = mv.get("address") or mv.get("city") or ""
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

        lead_type = "homeowner interested in selling" if is_seller else "active home buyer"

        client = _ant.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=300,
            messages=[{"role": "user", "content": f"""Write a short voice note script for Barry Jenkins to send to {first}, a {lead_type} in Hampton Roads Virginia.

What we know about them: {beh_summary}

The lead just said yes to receiving a quick voice recording. Barry is leaving them a voice note.

Rules:
- written to be SPOKEN, not read. natural speech patterns.
- all lowercase except proper nouns
- 3-5 sentences only. under 80 words. voice notes should be short.
- start with "hey {first},"
- reference one specific thing from what they were looking at
- end with an open, easy question or "just lmk" -- not a hard sell
- warm and personal, like a friend who knows the market
- Barry's style: direct, knowledgeable, never pushy

Output only the script text. Nothing else."""}],
        )
        return resp.content[0].text.strip()

    except Exception as e:
        logger.warning("Voice note script generation failed: %s", e)
        return _fallback_script(person_name, behavior, is_seller)


def _fallback_script(person_name: str, behavior: dict, is_seller: bool) -> str:
    """Generic fallback script when Claude is unavailable."""
    first = (person_name or "there").split()[0]
    b = behavior or {}

    if is_seller:
        area = ""
        mv = b.get("most_viewed") or {}
        if mv.get("city"):
            area = f" in {mv['city']}"
        return (
            f"hey {first}, just wanted to drop you a quick note about what i'm seeing{area} right now. "
            f"the market has been moving and i wanted to give you a real picture of where things stand on your street. "
            f"just let me know if you want to dig into the actual numbers."
        )
    else:
        area = ""
        mv = b.get("most_viewed") or {}
        if mv.get("city"):
            area = f" around {mv['city']}"
        elif b.get("cities"):
            area = f" around {list(b['cities'])[0]}"
        return (
            f"hey {first}, thanks for getting back to me. "
            f"i've been watching the inventory{area} pretty closely and wanted to give you a real rundown on what i'm seeing. "
            f"there are a couple things worth knowing before you make any moves. just lmk if you want to talk through it."
        )
