"""
Sendblue iMessage Client — Lead Nurture Channel

Sends blue-bubble iMessages to pond leads via Sendblue API.
Falls back to SMS automatically for Android users (Sendblue handles this).

Key advantage over Twilio SMS:
  - iMessage delivers the HeyGen video as a native inline attachment —
    recipient sees and taps to play directly in the Messages thread,
    with no link, no browser, no friction.
  - Blue bubbles read as personal outreach, not a business SMS blast.
  - ~2x reply rate vs green-bubble SMS on iPhone (57%+ of US phones).

Required Railway env vars:
    SENDBLUE_API_KEY      — sb-api-key-id
    SENDBLUE_SECRET_KEY   — sb-api-secret-key
    SENDBLUE_PHONE_NUMBER — our outbound number (e.g. +17572XXXXXX)

Usage:
    from sendblue_client import send_imessage, is_available

    result = send_imessage(
        to_number="+17579198874",
        content="Hey Sarah, recorded a quick update for you...",
        media_url="https://web-production-3363cc.up.railway.app/vp/abc123",
        dry_run=False,
    )
    # result = {"success": True, "was_downgraded": False, "status": "QUEUED", ...}
"""

from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger("sendblue_client")

SENDBLUE_API_BASE = "https://api.sendblue.co"

# Hard stop words — CTIA-standard opt-out keywords
# Sendblue handles STOP at the carrier level, but we also gate locally so
# opt-outs are applied immediately even if the Sendblue webhook is delayed.
HARD_STOP_WORDS = {"stop", "stopall", "unsubscribe", "cancel", "end", "quit"}

# Tags that block iMessage (same as Twilio suppression — unified opt-out)
SENDBLUE_SUPPRESSION_TAGS = {
    "AI_OPT_OUT",
    "AI_NOT_INTERESTED",
    "NO_MARKETING",
    "WRONG_NUMBER",
    "DISCONNECTED_NUMBER",
    "NOT_INTERESTED",
    "DO_NOT_CALL",
    "PondMailer_Unsubscribed",
    "SMS_OptOut",
}


# ── Availability ──────────────────────────────────────────────────────────────

def is_available() -> bool:
    """Return True if Sendblue env vars are configured."""
    return bool(
        os.environ.get("SENDBLUE_API_KEY") and
        os.environ.get("SENDBLUE_SECRET_KEY")
    )


def get_from_number() -> str:
    """Return our configured Sendblue outbound number."""
    return os.environ.get("SENDBLUE_PHONE_NUMBER", "+13107285158")


# ── Phone formatting ──────────────────────────────────────────────────────────

def format_e164(phone: str) -> str | None:
    """Normalize US phone number to E.164 (+1XXXXXXXXXX). Returns None if invalid."""
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"+1{digits}"


# ── Suppression ───────────────────────────────────────────────────────────────

def suppressed_by_tags(tags: list) -> list:
    """Return blocking tags. Empty list = clear to send."""
    return [t for t in (tags or []) if t in SENDBLUE_SUPPRESSION_TAGS]


# ── TCPA quiet hours ──────────────────────────────────────────────────────────

def is_within_quiet_hours(tz_name: str = "America/New_York") -> bool:
    """Return True if within TCPA-safe hours: 8:00am–9:00pm ET."""
    from datetime import datetime
    try:
        try:
            import zoneinfo
            tz = zoneinfo.ZoneInfo(tz_name)
        except (ImportError, ModuleNotFoundError):
            import pytz
            tz = pytz.timezone(tz_name)
        local_now = datetime.now(tz)
        return 8 <= local_now.hour < 21
    except Exception as e:
        logger.warning("is_within_quiet_hours: check failed (%s) — blocking", e)
        return False


# ── Send ──────────────────────────────────────────────────────────────────────

def send_imessage(
    to_number: str,
    content: str,
    media_url: str = None,
    dry_run: bool = False,
) -> dict:
    """
    Send an iMessage (or SMS fallback) via Sendblue.

    When media_url is a video URL (e.g. our /vp/<video_id> proxy), Sendblue
    downloads it and delivers it as a native inline video in the Messages app —
    no link, no browser required. Plays on tap right in the thread.

    For Android recipients, Sendblue automatically falls back to SMS.
    was_downgraded=True in the response indicates SMS fallback was used.

    Args:
        to_number:  Any parseable US phone — normalized to E.164.
        content:    Message text body.
        media_url:  Optional publicly accessible URL to attach inline.
                    Use /vp/<video_id> (our proxy) not raw HeyGen URLs
                    (HeyGen signed URLs expire; our proxy always works).
        dry_run:    Log but do not send.

    Returns dict:
        success          — bool
        was_downgraded   — True if delivered via SMS (Android fallback)
        message_handle   — Sendblue message ID (for status lookup)
        status           — "QUEUED" | "dry_run" | "quiet_hours" | "failed"
        error            — present on failure
    """
    to_e164 = format_e164(to_number)
    if not to_e164:
        logger.warning("send_imessage: invalid phone %r", to_number)
        return {"success": False, "status": "failed",
                "error": f"Invalid phone: {to_number!r}"}

    media_note = f" + video ({media_url[:60]}...)" if media_url else ""

    if dry_run:
        logger.info("[DRY RUN] Sendblue iMessage to %s (%d chars)%s: %s...",
                    to_e164, len(content), media_note, content[:80].replace("\n", " "))
        return {"success": True, "was_downgraded": False,
                "message_handle": None, "status": "dry_run"}

    if not is_available():
        logger.warning("send_imessage: Sendblue not configured — SENDBLUE_* env vars missing")
        return {"success": False, "status": "failed", "error": "Sendblue not configured"}

    if not is_within_quiet_hours():
        logger.warning("send_imessage: blocked — outside TCPA quiet hours for %s", to_e164)
        return {"success": False, "status": "quiet_hours",
                "error": "Outside TCPA quiet hours (8am–9pm ET)"}

    try:
        import requests as _req

        base_url = os.environ.get("BASE_URL", "https://web-production-3363cc.up.railway.app")
        payload: dict = {
            "number":          to_e164,
            "content":         content,
            "status_callback": f"{base_url}/webhook/sendblue",
        }
        if media_url:
            payload["media_url"] = media_url

        r = _req.post(
            f"{SENDBLUE_API_BASE}/api/send-message",
            headers={
                "sb-api-key-id":     os.environ["SENDBLUE_API_KEY"],
                "sb-api-secret-key": os.environ["SENDBLUE_SECRET_KEY"],
                "Content-Type":      "application/json",
            },
            json=payload,
            timeout=20,
        )
        r.raise_for_status()
        data = r.json()

        downgraded = data.get("was_downgraded", False)
        handle     = data.get("message_handle")
        status     = data.get("status", "QUEUED")
        channel    = "SMS (downgraded)" if downgraded else "iMessage"

        logger.info("Sendblue %s → %s | %s | handle: %s%s",
                    status, to_e164, channel, handle, media_note)

        return {
            "success":        True,
            "was_downgraded": downgraded,
            "message_handle": handle,
            "status":         status,
        }

    except Exception as e:
        logger.error("send_imessage failed to %s: %s", to_e164, e)
        return {"success": False, "status": "failed", "error": str(e)}
