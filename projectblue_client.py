"""
Project Blue client — iMessage-first messaging (blue bubble, SMS fallback).

Replaces Twilio for all pond SMS outbound. Preserves phone-number utility
functions (get_primary_phone, format_e164, sms_suppressed_by_tags,
is_within_sms_quiet_hours) so pond_mailer.py needs no structural changes
beyond swapping the import alias.

API docs: https://api.tryprojectblue.com
"""

import logging
import os
import re

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.tryprojectblue.com"
REQUEST_TIMEOUT = 15  # seconds


# ── Auth ──────────────────────────────────────────────────────────────────────

def _api_key() -> str:
    return os.environ.get("PROJECT_BLUE_API_KEY", "")


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {_api_key()}",
        "Content-Type": "application/json",
    }


def is_available() -> bool:
    """Return True if the Project Blue API key is configured."""
    return bool(_api_key())


# ── Phone number utilities (carried from twilio_client) ──────────────────────

def format_e164(phone: str) -> str | None:
    """
    Normalize a US phone number to E.164 format (+1XXXXXXXXXX).
    Returns None if it cannot be parsed as a valid 10-digit US number.
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


# ── Suppression utilities (carried from twilio_client) ───────────────────────

# Tags that block SMS outbound (opt-outs, wrong numbers, DNC flags, etc.)
SMS_SUPPRESSION_TAGS = {
    "DO_NOT_CALL",
    "WRONG_NUMBER",
    "SMS_OptOut",
    "NO_SMS",
    "AI_OPT_OUT",
    "PondMailer_Unsubscribed",
    "NO_CONTACT",
    "DNC",
}


def sms_suppressed_by_tags(tags: list) -> list:
    """Return list of tags that block SMS. Empty list = clear to send."""
    return [t for t in (tags or []) if t in SMS_SUPPRESSION_TAGS]


# ── TCPA quiet hours ──────────────────────────────────────────────────────────

def is_within_sms_quiet_hours(tz_name: str = "America/New_York") -> bool:
    """
    Return True if current time is within TCPA-safe hours: 8am-9pm local.
    Returns False (blocked) on timezone errors -- safe default.
    """
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
        logger.warning("is_within_sms_quiet_hours: check failed (%s) -- blocking send", e)
        return False


# ── iMessage availability ─────────────────────────────────────────────────────

def check_imessage(phone: str) -> bool:
    """
    Return True if the phone number supports iMessage.
    Returns False on any error (safe default -- falls back to SMS delivery).
    """
    if not is_available():
        return False
    try:
        resp = requests.post(
            f"{BASE_URL}/api-check-imessage-availability",
            json={"phone": phone},
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return bool(resp.json().get("isIMessageAvailable", False))
    except Exception as e:
        logger.warning("check_imessage failed for %s: %s", phone, e)
        return False


# ── Lines ─────────────────────────────────────────────────────────────────────

def get_lines() -> list:
    """Return list of available sending lines from Project Blue account."""
    if not is_available():
        return []
    try:
        resp = requests.get(
            f"{BASE_URL}/get-lines",
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("get_lines failed: %s", e)
        return []


# ── Send ──────────────────────────────────────────────────────────────────────

def send_message(
    to_number: str,
    body: str,
    media_url: str = None,
    audio_url: str = None,
    voice_memo: bool = False,
    line_id: str = None,
    dry_run: bool = False,
) -> dict:
    """
    Send a message via Project Blue (iMessage preferred, SMS fallback).

    Args:
        to_number:   E.164 phone number (+1XXXXXXXXXX)
        body:        Message text (plain ASCII, GSM-7 safe)
        media_url:   Optional URL to image/video to attach as MMS thumbnail
        audio_url:   Optional URL to an MP3/audio file to send as an iMessage
                     audio bubble (audioAttachmentUrl in PB API). Used for
                     ElevenLabs voice notes. Requires iMessage delivery.
        voice_memo:  If True, Project Blue generates an AI voice memo from body
        line_id:     Optional UUID to force a specific sending line
        dry_run:     If True, log and return success without actually sending

    Returns dict with keys:
        success:      bool
        message_type: "iMessage" | "SMS" (from PB response, or "dry_run")
        pb_handle:    Project Blue message handle (pbm_... or None)
        status:       "queued" | "dry_run" | "failed"
        error:        error string if failed
    """
    if not is_available():
        return {"success": False, "error": "PROJECT_BLUE_API_KEY not configured"}

    if not is_within_sms_quiet_hours():
        logger.info("Project Blue send blocked -- outside TCPA quiet hours (8am-9pm ET)")
        return {"success": False, "status": "quiet_hours", "error": "Outside TCPA quiet hours"}

    if dry_run:
        logger.info("[DRY RUN] Would send Project Blue message to %s (%d chars)", to_number, len(body))
        return {"success": True, "status": "dry_run", "message_type": "dry_run", "pb_handle": None}

    payload = {
        "phone": to_number,
        "message": body,
        "shouldAutoCreateContact": False,  # contacts live in FUB, not Project Blue CRM
    }
    if media_url:
        payload["mediaAttachmentUrl"] = media_url
    if audio_url:
        payload["audioAttachmentUrl"] = audio_url
    if voice_memo:
        payload["enableAiVoiceMemo"] = True
    if line_id:
        payload["lineId"] = line_id

    try:
        resp = requests.post(
            f"{BASE_URL}/send-api-message",
            json=payload,
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()

        if data.get("success"):
            logger.info(
                "Project Blue sent to %s via %s (line: %s)",
                to_number,
                data.get("messageType", "unknown"),
                data.get("devicePhoneNumber", "?"),
            )
            return {
                "success": True,
                "status": "queued",
                "message_type": data.get("messageType", "unknown"),
                "pb_handle": data.get("messageId"),
                "device_phone": data.get("devicePhoneNumber"),
            }
        else:
            logger.warning("Project Blue returned success=False for %s: %s", to_number, data)
            return {"success": False, "status": "failed", "error": str(data)}

    except requests.exceptions.HTTPError as e:
        logger.error("Project Blue HTTP error for %s: %s -- %s", to_number, e, e.response.text if e.response else "")
        return {"success": False, "status": "failed", "error": str(e)}
    except Exception as e:
        logger.error("Project Blue send failed for %s: %s", to_number, e)
        return {"success": False, "status": "failed", "error": str(e)}


# ── Message history ───────────────────────────────────────────────────────────

def get_messages(
    limit: int = 100,
    offset: int = 0,
    direction: str = None,
    service: str = None,
    from_number: str = None,
    to_number: str = None,
) -> dict:
    """
    List messages from Project Blue. Returns API response dict.
    direction: "inbound" | "outbound"
    service:   "iMessage" | "SMS" | "RCS"
    """
    if not is_available():
        return {"status": "unavailable", "data": [], "pagination": {}}
    params = {"limit": limit, "offset": offset}
    if direction:
        params["direction"] = direction
    if service:
        params["service"] = service
    if from_number:
        params["from_number"] = from_number
    if to_number:
        params["to_number"] = to_number
    try:
        resp = requests.get(
            f"{BASE_URL}/get-messages-api",
            params=params,
            headers=_headers(),
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning("get_messages failed: %s", e)
        return {"status": "error", "data": [], "pagination": {}}
