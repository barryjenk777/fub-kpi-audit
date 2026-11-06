"""Pure Lead Desk route decisions and delivery-log formatting.

No network, database, Slack, or FUB imports. app.py owns the I/O and calls
these helpers so the qualification rules can be tested without booting the
web process.

Current qualification (Nov 4 2026 truth test, as implemented):
  a claim-board offer requires a human-ask tag other than y_ai_priority,
  a native inbound text or connected inbound call inside 24h, an unowned
  lead, and dispatch_ai_live=1.
  y_ai_priority alone is a priority signal (ops mirror + soft owner ping).
  A human-ask tag with no native inbound is a follow-up attempt, not a
  drop-everything conversion.

Ylopo note transcripts are not an input. Notes are not FUB textMessages.
Whether a fresh affirmative "From:" line should count as inbound is an
unresolved policy question; this module does not invent that rule.
"""

import re
from datetime import datetime, timedelta, timezone

_LOG_TOKEN = re.compile(r"[^A-Za-z0-9_.:@+-]")

_VOICE_SOURCE_TAGS = (
    "ai_voice_needs_follow_up",
    "isa_transfer_unsuccessful",
    "isa_attempted_transfer_realtor_unavailable",
)


def created_within(created, now, hours):
    """True when `created` is within `hours` of `now`.

    Matches the historical gate: the first 19 characters are parsed as
    UTC. That is correct for FUB's `Z` timestamps. A numeric offset is
    sliced off and interpreted as UTC. ValueError (empty/garbage) is
    False. Other type errors propagate so the caller can keep its
    fail-loud behavior.
    """
    try:
        dt = datetime.strptime((created or "")[:19], "%Y-%m-%dT%H:%M:%S").replace(
            tzinfo=timezone.utc)
    except ValueError:
        return False
    return (now - dt) <= timedelta(hours=hours)


def text_is_recent_inbound(text, now, hours=24):
    """Native FUB textMessages row: isIncoming and inside the window.

    A note dict (no isIncoming) is not a text. Message body is never read,
    so this does not classify affirmative vs decline language.
    """
    if not text.get("isIncoming"):
        return False
    return created_within(text.get("created"), now, hours)


def call_is_recent_inbound(call, now, hours=24):
    """Native FUB call: isIncoming, duration > 0, inside the window.

    `isOutgoing` is not consulted. A null duration does not qualify.
    Non-numeric duration raises, same as the inlined int() it replaced.
    """
    if not call.get("isIncoming"):
        return False
    if int(call.get("duration") or 0) <= 0:
        return False
    return created_within(call.get("created"), now, hours)


def evaluate_desk_route(fresh_tags, *, live, owned, inbound_recent):
    """Return the route for tags that are already new to this lead.

    `inbound_recent` must be the native-text/call gate. Pass False when
    that gate was not run (no human-ask tag) so a priority-only lead
    cannot become a conversion.
    """
    fresh = [str(t).lower() for t in (fresh_tags or [])]
    ask = [t for t in fresh if t != "y_ai_priority"]
    engaged = bool(ask) and bool(inbound_recent)
    claim = (str(live or "").strip() == "1" and (not owned)
             and bool(ask) and engaged)
    if ask and engaged:
        lane = "AI conversion"
    elif ask:
        lane = "AI follow-up attempt"
    else:
        lane = "AI priority"
    if "ai_voice_needs_follow_up" in fresh:
        detail = "voice"
    elif ask:
        detail = "text"
    else:
        detail = "priority"
    if any(t in fresh for t in _VOICE_SOURCE_TAGS):
        source_kind = "ai_voice"
    elif (("sms_conversion" in fresh or "claude_text_converted" in fresh)
          and "ai_needs_follow_up" not in fresh):
        source_kind = "blue_text"
    else:
        source_kind = "ai_text"
    if claim:
        decision = "claim_offer"
        reason = "unowned_human_ask_native_inbound_24h"
    elif ask and engaged and owned:
        decision = "mirror_owner"
        reason = "owned_human_ask_native_inbound_24h"
    elif ask and engaged:
        decision = "mirror_only"
        reason = "human_ask_native_inbound_desk_not_live"
    elif ask:
        decision = "mirror_only"
        reason = "human_ask_without_native_inbound_24h"
    else:
        decision = "mirror_only"
        reason = "priority_signal_not_human_ask"
    return {
        "ask": ask,
        "engaged": engaged,
        "claim_offer": claim,
        "lane": lane,
        "detail": detail,
        "source_kind": source_kind,
        "decision": decision,
        "reason": reason,
    }


def routing_delivery_line(*, person_id, event_class, decision, reason,
                          destination, attempted_at, slack_ts="", error=""):
    """One single-line diagnostic. No message body parameter.

    Values are forced onto one token so a reason cannot smuggle a
    conversation, a newline, or a second log record.
    """
    parts = (
        ("person_id", person_id),
        ("class", event_class),
        ("decision", decision),
        ("reason", reason),
        ("destination", destination),
        ("attempted_at", attempted_at),
        ("slack_ts", slack_ts),
        ("error", error),
    )
    return "routing_delivery " + " ".join(
        "%s=%s" % (key, _LOG_TOKEN.sub("_", str("" if value is None else value))[:120])
        for key, value in parts)
