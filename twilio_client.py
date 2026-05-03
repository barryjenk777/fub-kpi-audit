"""
Twilio SMS Client — Lead Nurture Channel

Sends personalized text messages to pond leads via the Legacy Home Team
verified 10DLC campaign (Low Volume Mixed, verified 4/22/2026).

Campaign use case: lead nurture SMS to opted-in buyers/sellers at
legacyhomesearch.com. Messages include property updates, market insights,
home value information, and personalized follow-up.

Required Railway env vars:
    TWILIO_ACCOUNT_SID           — Twilio account SID (AC...)
    TWILIO_AUTH_TOKEN            — Twilio auth token
    TWILIO_MESSAGING_SERVICE_SID — Verified messaging service SID (MG99037...)

Usage:
    from twilio_client import send_sms, is_available, get_primary_phone, email_to_sms

    if is_available():
        phone = get_primary_phone(fub_person)
        if phone:
            body = email_to_sms(email_body_text, first_name="Sarah")
            result = send_sms(phone, body, dry_run=False)
            # result = {"success": True, "twilio_sid": "SMxxx", "status": "queued"}
"""

from __future__ import annotations

import logging
import os
import re

logger = logging.getLogger("twilio_client")

# ── SMS sign-off ─────────────────────────────────────────────────────────────
# ASCII-only so messages stay in GSM-7 encoding (160 chars/seg, 153 for
# concatenated). Em dash (—) and middle dot (·) are NOT in GSM-7 — they force
# Unicode (UCS-2) encoding which drops capacity to 70/67 chars per segment and
# turns a short 200-char message into 3 segments. Use plain hyphen and pipe.
SMS_SIGN_OFF = "- Barry Jenkins, Legacy Home Team | (757) 919-8874"

# Body portion max before sign-off + newline.
# GSM-7 2-segment budget: 306 chars total.
# Sign-off = 50 chars + 1 newline = 51 chars overhead.
# Usable body = 306 - 51 = 255 chars.
SMS_MAX_BODY = 255

# ── Tags that block SMS ───────────────────────────────────────────────────────
# Separate from email suppression: DO_NOT_CALL blocks calls AND texts (belt-and-
# suspenders), but NO_EMAIL does not block SMS (different channel).
SMS_SUPPRESSION_TAGS = {
    "AI_OPT_OUT",           # Opted out of AI text
    "AI_NOT_INTERESTED",    # Declined on AI text
    "NO_MARKETING",         # All outreach blocked
    "WRONG_NUMBER",         # Number confirmed invalid
    "DISCONNECTED_NUMBER",  # Number no longer in service
    "NOT_INTERESTED",       # Declined outreach entirely
    "DO_NOT_CALL",          # Covers texts too — TCPA caution
    "PondMailer_Unsubscribed",  # Unsubscribed from nurture sequence
    "SMS_OptOut",           # Replied negatively to an AI text (SMS-specific opt-out)
}

# High-priority tags that trigger dual-channel (email + SMS same send)
DUAL_CHANNEL_TAGS = {
    "AI_NEEDS_FOLLOW_UP",   # Ylopo rAIya flagged high intent
    "HANDRAISER",           # Explicit hand-raise signal
    "Y_HOME_3_VIEW",        # Viewed same home 3+ times
    "Y_AI_PRIORITY",        # Multi-signal high-interest flag
    "Y_REQUESTED_TOUR",     # Submitted a tour request
    "YPRIORITY",            # AI voice priority flag
}


# ── Availability check ────────────────────────────────────────────────────────

def is_available() -> bool:
    """Return True if all required Twilio env vars are set."""
    return bool(
        os.environ.get("TWILIO_ACCOUNT_SID") and
        os.environ.get("TWILIO_AUTH_TOKEN") and
        os.environ.get("TWILIO_MESSAGING_SERVICE_SID")
    )


# ── Phone number utilities ────────────────────────────────────────────────────

def format_e164(phone: str) -> str | None:
    """
    Normalize a US phone number to E.164 format (+1XXXXXXXXXX).
    Returns None if it can't be parsed as a valid 10-digit US number.

    Examples:
        "(757) 919-8874" → "+17579198874"
        "7579198874"     → "+17579198874"
        "17579198874"    → "+17579198874"
        "212-555-0100"   → "+12125550100"
    """
    if not phone:
        return None
    digits = re.sub(r"\D", "", str(phone))
    if len(digits) == 11 and digits.startswith("1"):
        digits = digits[1:]
    if len(digits) != 10:
        return None
    return f"+1{digits}"


def get_primary_phone(person: dict) -> str | None:
    """
    Extract the best available US phone number from a FUB person object.

    FUB returns phones as: [{"type": "mobile", "value": "7579198874", "isPrimary": True}, ...]

    Priority order:
        1. isPrimary + mobile type
        2. mobile type (any)
        3. isPrimary (any type)
        4. First valid number

    Returns E.164 formatted string or None.
    """
    phones = person.get("phones") or []
    if not phones:
        return None

    def _score(p):
        ptype = (p.get("type") or "").lower()
        is_primary = bool(p.get("isPrimary"))
        is_mobile = "mobile" in ptype or ptype == "cell"
        if is_primary and is_mobile:
            return 0
        if is_mobile:
            return 1
        if is_primary:
            return 2
        return 3

    for p in sorted(phones, key=_score):
        number = format_e164(p.get("value", ""))
        if number:
            return number
    return None


# ── SMS body builder ──────────────────────────────────────────────────────────

def email_to_sms(email_body_text: str, first_name: str = "") -> str:
    """
    Condense a pond mailer email body into an SMS-length message (~260 chars).

    Rules:
    - Strip the sign-off block (Barry Jenkins / Legacy Home Team / phone lines)
    - Strip video link lines (/v/ token, "Watch:", "▶")
    - Take the first ~SMS_MAX_BODY chars at a sentence boundary
    - Sign-off is appended by send_sms(), not here

    The condensed body goes out as-is — no HTML, no links, just words.
    """
    if not email_body_text:
        return ""

    lines = email_body_text.strip().splitlines()

    # Drop sign-off block: stop collecting once we hit "Barry Jenkins" or "Legacy Home Team"
    content_lines = []
    for line in lines:
        stripped = line.strip()
        if (stripped.startswith("Barry Jenkins") or
                stripped.startswith("Legacy Home Team") or
                stripped.startswith("LPT Realty") or
                stripped.startswith("(757)") or
                stripped.startswith("www.")):
            break
        content_lines.append(line)

    # Drop video link lines
    content_lines = [
        l for l in content_lines
        if not any(s in l for s in ("/v/", "Watch:", "▶", "heygen.com", "watch.heygen"))
    ]

    # Collapse into single string
    body = " ".join(l.strip() for l in content_lines if l.strip())

    if len(body) <= SMS_MAX_BODY:
        return body.strip()

    # Trim to sentence boundary
    truncated = body[:SMS_MAX_BODY]
    for punct in (".", "!", "?"):
        idx = truncated.rfind(punct)
        if idx > SMS_MAX_BODY * 0.5:
            return truncated[:idx + 1].strip()

    # Fall back to word boundary
    idx = truncated.rfind(" ")
    return (truncated[:idx].strip() + "...") if idx > 0 else truncated.strip()


def sms_suppressed_by_tags(tags: list) -> list:
    """
    Return list of tags that block SMS (parallel to _email_suppression_tags).
    Empty list = clear to send.
    """
    return [t for t in (tags or []) if t in SMS_SUPPRESSION_TAGS]


# ── TCPA quiet hours ──────────────────────────────────────────────────────────

def is_within_sms_quiet_hours(tz_name: str = "America/New_York") -> bool:
    """
    Return True if it's currently within TCPA-safe hours: 8:00am–9:00pm local.

    TCPA prohibits automated marketing texts before 8am or after 9pm in the
    recipient's local timezone. Hampton Roads leads are Eastern Time.

    Returns False (blocked) if the timezone cannot be resolved — safe default.
    """
    from datetime import datetime
    try:
        try:
            import zoneinfo  # Python 3.9+ stdlib
            tz = zoneinfo.ZoneInfo(tz_name)
        except (ImportError, ModuleNotFoundError):
            import pytz  # fallback for older Python
            tz = pytz.timezone(tz_name)
        local_now = datetime.now(tz)
        # 8:00am (hour=8) through 8:59pm (hour=20) inclusive — 9pm = hour 21 = blocked
        return 8 <= local_now.hour < 21
    except Exception as e:
        logger.warning("is_within_sms_quiet_hours: timezone check failed (%s) — blocking send", e)
        return False


# ── Send ──────────────────────────────────────────────────────────────────────

def send_sms(to_number: str, body: str, media_url: str = None, dry_run: bool = False) -> dict:
    """
    Send an SMS/MMS via the verified 10DLC messaging service.

    The SMS_SIGN_OFF is appended to body automatically so callers don't have to
    worry about it. Total length = len(body) + 2 + len(SMS_SIGN_OFF).

    When media_url is provided, Twilio sends as MMS — the video/image attaches
    inline in the recipient's Messages app. Works on both iPhone and Android.
    Use our /vp/<video_id> proxy URL (not raw HeyGen URLs, which expire).

    Args:
        to_number: Any parseable US phone format — will be normalized to E.164.
        body:      Message text WITHOUT sign-off (use email_to_sms() to build it).
        media_url: Optional publicly accessible URL to attach as MMS media.
        dry_run:   Log but do not actually send.

    Returns dict with keys:
        success    — bool
        twilio_sid — "SMxxx..." or None (dry_run / failure)
        status     — "queued" | "sent" | "dry_run" | "failed"
        error      — present only on failure
    """
    to_e164 = format_e164(to_number)
    if not to_e164:
        logger.warning("send_sms: invalid phone number %r", to_number)
        return {"success": False, "twilio_sid": None, "status": "failed",
                "error": f"Invalid phone: {to_number!r}"}

    full_body = f"{body}\n{SMS_SIGN_OFF}"
    media_note = f" + media ({media_url[:60]}...)" if media_url else ""

    if dry_run:
        logger.info("[DRY RUN] SMS to %s (%d chars)%s: %s...",
                    to_e164, len(full_body), media_note, full_body[:100].replace("\n", " "))
        return {"success": True, "twilio_sid": None, "status": "dry_run"}

    # Fast-fail if Twilio env vars aren't configured (check before TCPA so the
    # error message is "not configured" rather than a misleading "quiet hours").
    if not is_available():
        logger.warning("send_sms: Twilio not configured — TWILIO_* env vars missing")
        return {"success": False, "twilio_sid": None, "status": "failed",
                "error": "Twilio not configured"}

    # TCPA quiet hours: no automated texts before 8am or after 9pm ET
    if not is_within_sms_quiet_hours():
        logger.warning("send_sms: blocked — outside TCPA quiet hours (8am–9pm ET) for %s", to_e164)
        return {"success": False, "twilio_sid": None, "status": "quiet_hours",
                "error": "Outside TCPA quiet hours (8am–9pm ET)"}

    try:
        from twilio.rest import Client  # noqa: PLC0415
        client = Client(
            os.environ["TWILIO_ACCOUNT_SID"],
            os.environ["TWILIO_AUTH_TOKEN"],
        )
        create_kwargs: dict = {
            "messaging_service_sid": os.environ["TWILIO_MESSAGING_SERVICE_SID"],
            "to":   to_e164,
            "body": full_body,
        }
        if media_url:
            create_kwargs["media_url"] = [media_url]  # Twilio expects a list

        msg = client.messages.create(**create_kwargs)
        logger.info("SMS%s sent to %s | SID: %s | Status: %s | %d chars",
                    " (MMS)" if media_url else "", to_e164, msg.sid, msg.status, len(full_body))
        return {"success": True, "twilio_sid": msg.sid, "status": msg.status}

    except ImportError:
        logger.error("send_sms: twilio package not installed. Run: pip install twilio")
        return {"success": False, "twilio_sid": None, "status": "failed",
                "error": "twilio package not installed"}
    except Exception as e:
        logger.error("send_sms failed to %s: %s", to_e164, e)
        return {"success": False, "twilio_sid": None, "status": "failed", "error": str(e)}
