"""
Serendipity Clause — Behavioral Trigger Email Engine
Legacy Home Team · LPT Realty

Runs alongside the pond_mailer drip without interfering with it.
Monitors FUB event activity and fires one-off, context-aware emails
when a lead takes a notable action: saves a property, keeps revisiting
a listing, or returns after going dark.

The email should feel like Barry happened to notice and typed something.
Not a follow-up. Not a drip. A moment.

Trigger types
-------------
  saved_property    — lead saves/favorites a listing
  repeat_view       — lead views the same property 3+ times in 7 days
  inactivity_return — lead returns after 14+ days of silence

Usage
-----
  from serendipity import run_serendipity
  result = run_serendipity(dry_run=False)
"""

import json
import logging
import os
import random
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TRIGGER_DELAY_MIN        = 30    # minutes before email fires (feels human)
TRIGGER_DELAY_MAX        = 90
REPEAT_VIEW_THRESHOLD    = 3     # views of same property to trigger
INACTIVITY_DAYS          = 14    # days of silence before "return" trigger
SERENDIPITY_COOLDOWN_DAYS = 7   # max 1 serendipity email per lead per week
EMAIL_COOLDOWN_HOURS     = 48    # don't fire if ANY email sent within 48h
MAX_CANDIDATES_PER_RUN   = 30   # max person lookups per detect run (API budget)

# Event types FUB uses (from fub_client.get_events docstring + analyze_behavior)
SAVE_EVENT_TYPES   = {"Property Saved", "Saved Property"}
VIEW_EVENT_TYPES   = {"Viewed Property"}
ACTIVE_EVENT_TYPES = {"Viewed Property", "Property Saved", "Saved Property",
                       "Viewed Page", "Searched Properties", "Registration"}

# Suppression tags — same set as pond_mailer
SUPPRESSION_TAGS = {
    "PondMailer_Unsubscribed",
    "NO_MARKETING", "NO_EMAIL",
    "LISTING_ALERT_UNSUB", "LISTING_ALERT_SUNSET",
    "DO_NOT_CALL", "AI_OPT_OUT", "AI_NOT_INTERESTED",
    "NOT_INTERESTED", "WRONG_NUMBER", "DISCONNECTED_NUMBER",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _format_address(prop):
    """Return 'street, city' from a FUB property dict, or empty string."""
    if not prop:
        return ""
    parts = []
    street = (prop.get("street") or "").strip()
    city   = (prop.get("city")   or "").strip()
    if street:
        parts.append(street)
    if city:
        parts.append(city)
    return ", ".join(parts)


def _parse_event_ts(event):
    """Parse the 'created' timestamp from a FUB event. Returns UTC datetime."""
    raw = event.get("created") or ""
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, AttributeError):
        return datetime.min.replace(tzinfo=timezone.utc)


def _fire_after(now):
    """Return a fire_after timestamp: now + random 30-90 minutes."""
    delay = random.randint(TRIGGER_DELAY_MIN, TRIGGER_DELAY_MAX)
    return now + timedelta(minutes=delay)


def _tag_names(person):
    """Extract tag name strings from a FUB person dict."""
    raw = person.get("tags") or []
    names = set()
    for t in raw:
        if isinstance(t, dict):
            names.add(t.get("name", ""))
        elif isinstance(t, str):
            names.add(t)
    return names


def _get_email(person):
    """Return best email address from a FUB person dict."""
    emails = person.get("emails") or []
    primary = next(
        (e.get("value") for e in emails
         if e.get("isPrimary") or e.get("status") == "Valid"),
        None
    )
    return primary or (emails[0].get("value") if emails else None)


# ---------------------------------------------------------------------------
# Eligibility
# ---------------------------------------------------------------------------

def _is_eligible(person, db_module):
    """
    Check whether a lead is eligible for a serendipity email.
    Returns (True, None) or (False, reason_string).
    """
    pid  = person.get("id")
    tags = _tag_names(person)

    # Must have an email address
    if not _get_email(person):
        return False, "no email"

    # Suppression tags
    blocked = tags & SUPPRESSION_TAGS
    if blocked:
        return False, f"suppressed: {', '.join(blocked)}"

    # Claimed by an agent — stop the drip
    from config import BARRY_FUB_USER_ID
    assigned = person.get("assignedUserId") or person.get("ownerId")
    if assigned and assigned != BARRY_FUB_USER_ID:
        return False, f"claimed by agent {assigned}"

    # 7-day serendipity cooldown
    if db_module.has_recent_serendipity_sent(pid, hours=SERENDIPITY_COOLDOWN_DAYS * 24):
        return False, "serendipity cooldown (7d)"

    # 48-hour email collision guard (any email — drip or serendipity)
    if db_module.has_any_recent_email(pid, hours=EMAIL_COOLDOWN_HOURS):
        return False, "email cooldown (48h)"

    return True, None


# ---------------------------------------------------------------------------
# Email generation
# ---------------------------------------------------------------------------

def _render_serendipity_html(body_text):
    """Render plain body text into the standard personal-email HTML shell."""
    from pond_mailer import LOGO_URL  # reuse constant
    lines = (body_text or "").split("\n")
    html_parts = []
    for i, line in enumerate(lines):
        stripped = line.strip()
        if not stripped:
            continue
        if i == 0:
            # First line is "FirstName," — style as salutation
            html_parts.append(
                f'<p style="margin:0 0 16px;font-size:15px;line-height:1.8;color:#222">'
                f'<strong>{stripped}</strong></p>'
            )
        else:
            html_parts.append(
                f'<p style="margin:0 0 16px;font-size:15px;line-height:1.8;color:#222">'
                f'{stripped.replace(chr(10), "<br>")}</p>'
            )

    inner = "\n".join(html_parts)
    return f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
</head>
<body style="margin:0;padding:0;background:#ffffff;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif">
<div style="max-width:560px;margin:0 auto;padding:32px 24px">
  <div style="color:#222;font-size:15px;line-height:1.8">
    {inner}
  </div>
  <div style="margin-top:32px;padding-top:20px;border-top:1px solid #e8e8e8">
    <img src="{LOGO_URL}" alt="Legacy Home Team" width="90"
         style="display:block;margin:0 0 10px;height:auto;opacity:0.9">
    <p style="margin:0;font-size:13px;color:#666;line-height:1.6">
      Barry Jenkins, Realtor &nbsp;|&nbsp; LPT Realty<br>
      (757) 919-8874 &nbsp;|&nbsp;
      <a href="https://www.legacyhomesearch.com"
         style="color:#666;text-decoration:none">www.legacyhomesearch.com</a><br>
      1545 Crossways Blvd, Chesapeake, VA 23320<br>
      <a href="mailto:reply@inbound.yourfriendlyagent.net?subject=Unsubscribe"
         style="color:#999;font-size:11px;text-decoration:none">Unsubscribe</a>
    </p>
  </div>
</div>
</body></html>"""


def generate_serendipity_email(person, trigger_type, trigger_data, tags, dry_run=False):
    """
    Generate a one-off serendipity email via Claude Opus.
    Returns dict: {subject, body_text, body_html, subject_options}
    """
    first_name = (person.get("firstName") or "").strip() or "there"

    # Property context (may be empty for inactivity_return)
    address    = _format_address(trigger_data)
    city       = (trigger_data.get("city") or "").strip()
    price      = trigger_data.get("price")
    beds       = trigger_data.get("bedrooms")
    baths      = trigger_data.get("bathrooms")
    area       = trigger_data.get("area")
    view_count = trigger_data.get("view_count", 0)
    url        = trigger_data.get("url") or ""

    prop_label = address or city or "Hampton Roads"

    prop_detail_parts = []
    if price:
        prop_detail_parts.append(f"${int(price):,}")
    if beds:
        prop_detail_parts.append(f"{int(beds)}bd")
    if baths:
        prop_detail_parts.append(f"{int(baths)}ba")
    if area:
        prop_detail_parts.append(f"{int(area):,} sqft")
    prop_detail = " · ".join(prop_detail_parts)

    # Tags as readable signals for Claude context
    tag_list = ", ".join(sorted(tags)) if tags else "none"

    # Build trigger-specific situation + guidance
    if trigger_type == "saved_property":
        situation = f"""The lead just saved/favorited this property:
  {prop_label}{(' — ' + prop_detail) if prop_detail else ''}
  {('Listing URL: ' + url) if url else ''}

Trigger: they saved it, which means they took a deliberate action. That is stronger
than a casual browse. They want to remember it."""
        why_surfaced = (
            f"Reference the specific behavior that surfaced this: they saved {prop_label}. "
            "Explain, in one short line, why you're mentioning it now."
        )
        specific_question = (
            "Close with ONE specific question about this property or their situation — "
            "not 'let me know what you think.' Something like: 'Is the price range working "
            "for you, or are you waiting to see where things land?' or 'What drew you to that "
            "neighborhood?' Ask about one preference you're genuinely uncertain about."
        )

    elif trigger_type == "repeat_view":
        situation = f"""The lead has been back to this property {view_count} times in the last 7 days:
  {prop_label}{(' — ' + prop_detail) if prop_detail else ''}
  {('Listing URL: ' + url) if url else ''}

Trigger: {view_count} views of the same listing. Something about it keeps pulling their eye.
That is behavioral signal — more honest than what people say they want."""
        why_surfaced = (
            f"Reference the repeat behavior naturally — they've come back to {prop_label} "
            f"more than once. Don't say '3 times' or name the count — translate it: "
            "'you keep coming back to this one' or 'something about it sticks.'"
        )
        specific_question = (
            "Close with a specific question that gets at what keeps drawing them back — "
            "the neighborhood? The layout? The price point? "
            "If their behavior might conflict with what they said they want (e.g., "
            "revisiting a fixer when they said move-in ready), name the tradeoff honestly. "
            "Leave room for them to have changed their mind."
        )

    elif trigger_type == "inactivity_return":
        if address:
            situation = f"""The lead went quiet for {INACTIVITY_DAYS}+ days and just came back,
and their recent activity included:
  {prop_label}{(' — ' + prop_detail) if prop_detail else ''}

Trigger: returning after a long silence is a real signal. Something changed for them."""
            why_surfaced = (
                f"They came back after being quiet, and their attention is on {prop_label}. "
                "Reference the return lightly — warm, not 'where have you been.' "
                "The return itself is the behavior worth acknowledging."
            )
        else:
            situation = f"""The lead went quiet for {INACTIVITY_DAYS}+ days and just came back
to the site — browsing again without saving anything specific yet.

Trigger: returning after a long silence. Something changed. Don't make it a big deal."""
            why_surfaced = (
                "They came back after going quiet. Don't overthink it. "
                "A warm, short check-in that opens a door is enough."
            )
        specific_question = (
            "Close with one open question about where they are in the process right now — "
            "not generic. Something that gives them room to say 'timing changed' or "
            "'I found a house' or 'still looking, just got busy.' "
            "Avoid implying certainty about what they want — buyers change their minds."
        )
    else:
        situation = f"Lead took a notable action involving {prop_label}."
        why_surfaced = "Reference the specific action naturally."
        specific_question = "Ask one specific, genuine question."

    # Assemble prompt
    prompt = f"""Write a serendipity email from Barry Jenkins (Legacy Home Team, Hampton Roads VA) to {first_name}.

This is NOT a drip email. NOT a follow-up sequence. It's a one-off moment — Barry happened
to notice something and reached out. It should feel personal and observant, not automated.

SITUATION:
{situation}

IMPORTANT FRAMING — read before writing:
Home search is high-stakes with real constraints: budget, schools, commute, timeline.
Serendipity research comes mostly from low-stakes domains like music and food. In real
estate, too much variety signals "you're not listening" and kills reply rates faster than
almost anything. Serendipity here means small, well-reasoned stretches — not surprise picks.
A buyer who feels heard clicks and replies. A buyer who feels like the system is guessing
unsubscribes.

RECOMMENDATION COPY RULES (apply these, do not override the voice rules below):
- {why_surfaced}
- Never include match scores, compatibility percentages, or AI-sounding prediction language.
- If suggesting a stretch beyond what they've been looking at, stay inside hard constraints
  (budget, beds/baths, commute zone) and stretch only ONE dimension: neighborhood, style,
  or layout. Never stretch budget or basic needs. Name the stretch explicitly and tie it to
  something they've actually shown: "this one's a little outside the area you've been in,
  but the layout matches almost everything you've come back to."
- {specific_question}
- Avoid language that implies certainty about what they want. Buyers change their minds —
  leave room for that.

BARRY'S VOICE — non-negotiable:
- Warm, local expert who pays attention. Not a salesperson.
- Conversational. Fragments fine. Contractions always.
- Maximum 4 sentences in the body. Short is better.
- No P.S. No "I hope this finds you well." No "just checking in."
- No em dashes or en dashes — use commas or periods instead.
- Never say: "dream home", "perfect fit", "hot market", "I'd love to",
  "feel free to", "don't hesitate", "I noticed"
- Never mention Ylopo, rAIya, AI, or any platform name.
- No corporate language. Nothing that sounds like a template.
- Signature added automatically. Stop before any sign-off.

FORMAT:
- Line 1: first name + comma only ("{first_name},")
- Blank line
- Body (3-4 sentences max)

SUBJECT LINES — 3 options:
- 5 words or fewer. Lowercase. Personal. Feels like a text, not a campaign.
- Reference the specific property or situation.
- Goal: clicks and replies, not just opens.

OUTPUT — JSON only, no markdown fences:
{{"subject_options": ["opt1", "opt2", "opt3"], "body": "{first_name},\\n\\n[body]"}}"""

    if dry_run:
        subjects = [
            f"[dry run] {trigger_type} — {prop_label or first_name}",
            f"[dry run] serendipity test",
            f"[dry run] {first_name}",
        ]
        return {
            "subject": subjects[0],
            "subject_options": subjects,
            "body_text": f"{first_name},\n\n[DRY RUN — {trigger_type} email not generated for {prop_label}]",
            "body_html": _render_serendipity_html(
                f"{first_name},\n\n[DRY RUN — {trigger_type} for {prop_label}]"
            ),
        }

    import anthropic
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY not set")

    client = anthropic.Anthropic(api_key=api_key)
    response = client.messages.create(
        model="claude-opus-4-5",
        max_tokens=500,
        messages=[{"role": "user", "content": prompt}],
    )

    raw = response.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw, flags=re.MULTILINE)
    raw = re.sub(r"\s*```$", "", raw, flags=re.MULTILINE)
    data = json.loads(raw)

    subject_options = data.get("subject_options") or []
    subject  = subject_options[0] if subject_options else f"{first_name} — saw something"
    body_text = data.get("body", "")
    body_html = _render_serendipity_html(body_text)

    return {
        "subject":         subject,
        "subject_options": subject_options,
        "body_text":       body_text,
        "body_html":       body_html,
    }


# ---------------------------------------------------------------------------
# Trigger detection
# ---------------------------------------------------------------------------

def _queue_trigger(db_module, person, trigger_type, prop_data, now, dry_run=False):
    """
    Log a pending trigger to the DB. Returns the new trigger id or None.
    prop_data may be empty for inactivity_return with no specific property.
    """
    pid   = person.get("id")
    name  = f"{person.get('firstName','')} {person.get('lastName','')}".strip()
    email = _get_email(person)

    address = _format_address(prop_data)
    trigger_data = {
        "address":   address,
        "street":    prop_data.get("street", ""),
        "city":      prop_data.get("city", ""),
        "price":     prop_data.get("price"),
        "bedrooms":  prop_data.get("bedrooms"),
        "bathrooms": prop_data.get("bathrooms"),
        "area":      prop_data.get("area"),
        "url":       prop_data.get("url", ""),
        "mls_number":prop_data.get("mlsNumber", ""),
        "view_count":prop_data.get("view_count", 0),
    }

    fire_at = _fire_after(now)
    trigger_id = db_module.log_serendipity_trigger(
        person_id=pid,
        person_name=name,
        email_address=email,
        trigger_type=trigger_type,
        trigger_data=trigger_data,
        fire_after=fire_at,
        dry_run=dry_run,
    )

    delay_min = int((fire_at - now).total_seconds() / 60)
    logger.info(
        "Serendipity trigger queued: pid=%s type=%s address=%s fire_in=%dm trigger_id=%s",
        pid, trigger_type, address or "(none)", delay_min, trigger_id
    )
    return trigger_id


def detect_triggers(client, db_module, dry_run=False):
    """
    Scan FUB events since the last cursor timestamp. For each person with new
    activity, evaluate all three trigger conditions and queue matching ones.

    Returns a summary dict.
    """
    import db as _db  # re-import for cursor methods
    now    = datetime.now(timezone.utc)
    cursor = db_module.get_serendipity_cursor()

    # Bootstrap: if no cursor exists, start from 15 minutes ago so we don't
    # flood on first run.
    if cursor is None:
        cursor = now - timedelta(minutes=15)
        logger.info("Serendipity: no cursor found — bootstrapping from -15min")

    logger.info("Serendipity detect: scanning events since %s", cursor.isoformat())

    # Pull all new events since cursor (paginated, cap at 10 pages / 1000 events)
    try:
        new_events = client.get_events(since=cursor, max_pages=10)
    except Exception as e:
        logger.error("Serendipity: failed to fetch events: %s", e)
        return {"error": str(e)}

    logger.info("Serendipity: %d new events since cursor", len(new_events))

    if not new_events:
        db_module.set_serendipity_cursor(now)
        return {"scanned_people": 0, "new_events": 0, "triggered": 0}

    # Group events by person_id
    by_person = defaultdict(list)
    for event in new_events:
        pid = event.get("personId")
        if pid:
            by_person[int(pid)].append(event)

    counts = defaultdict(int)
    people_checked = 0

    for pid, events in by_person.items():
        if people_checked >= MAX_CANDIDATES_PER_RUN:
            logger.info("Serendipity: hit MAX_CANDIDATES_PER_RUN=%d, stopping detect",
                        MAX_CANDIDATES_PER_RUN)
            break

        # Fetch full person record for eligibility check
        try:
            person = client.get_person(pid)
        except Exception as e:
            logger.warning("Serendipity: get_person(%s) failed: %s", pid, e)
            continue

        if not person:
            continue
        people_checked += 1

        eligible, reason = _is_eligible(person, db_module)
        if not eligible:
            logger.debug("Serendipity: skip pid=%s — %s", pid, reason)
            continue

        # ── Trigger 1: saved_property ────────────────────────────────────────
        save_events = [e for e in events if e.get("type") in SAVE_EVENT_TYPES]
        for ev in save_events:
            prop    = ev.get("property") or {}
            address = _format_address(prop)
            if not address:
                continue
            if not db_module.has_pending_serendipity_trigger(pid, "saved_property", address):
                _queue_trigger(db_module, person, "saved_property", prop, now, dry_run)
                counts["saved_property"] += 1
                break  # one trigger per person per run

        # ── Trigger 2: repeat_view ───────────────────────────────────────────
        if any(e.get("type") in VIEW_EVENT_TYPES for e in events):
            try:
                all_views = client.get_events_for_person(pid, days=7, limit=200)
            except Exception as e:
                logger.warning("Serendipity: get_events_for_person(%s) failed: %s", pid, e)
                all_views = []

            view_counts = defaultdict(lambda: {"count": 0, "prop": {}})
            for e in all_views:
                if e.get("type") in VIEW_EVENT_TYPES:
                    prop = e.get("property") or {}
                    addr = _format_address(prop)
                    if addr:
                        view_counts[addr]["count"] += 1
                        view_counts[addr]["prop"]   = prop

            # Sort most-viewed first; trigger the strongest signal
            for addr, info in sorted(view_counts.items(), key=lambda x: -x[1]["count"]):
                if info["count"] >= REPEAT_VIEW_THRESHOLD:
                    if not db_module.has_pending_serendipity_trigger(pid, "repeat_view", addr):
                        data = {**info["prop"], "view_count": info["count"]}
                        _queue_trigger(db_module, person, "repeat_view", data, now, dry_run)
                        counts["repeat_view"] += 1
                        break  # one per person per run

        # ── Trigger 3: inactivity_return ─────────────────────────────────────
        if any(e.get("type") in ACTIVE_EVENT_TYPES for e in events):
            try:
                history = client.get_events_for_person(pid, days=INACTIVITY_DAYS + 2, limit=200)
            except Exception as e:
                logger.warning("Serendipity: history fetch for pid=%s failed: %s", pid, e)
                history = []

            cutoff_fresh = now - timedelta(days=1)
            cutoff_dark_start = now - timedelta(days=INACTIVITY_DAYS + 2)
            cutoff_dark_end   = now - timedelta(days=2)

            fresh_activity = [
                e for e in history
                if _parse_event_ts(e) >= cutoff_fresh
                and e.get("type") in ACTIVE_EVENT_TYPES
            ]
            dark_period_activity = [
                e for e in history
                if cutoff_dark_start <= _parse_event_ts(e) <= cutoff_dark_end
                and e.get("type") in ACTIVE_EVENT_TYPES
            ]

            was_dark   = len(dark_period_activity) == 0
            is_back    = len(fresh_activity) > 0

            if is_back and was_dark:
                if not db_module.has_pending_serendipity_trigger(pid, "inactivity_return"):
                    # Use the most recent property if one is available
                    prop_events = [e for e in fresh_activity if e.get("property")]
                    prop = prop_events[0].get("property", {}) if prop_events else {}
                    _queue_trigger(db_module, person, "inactivity_return", prop, now, dry_run)
                    counts["inactivity_return"] += 1

    # Advance cursor to now
    db_module.set_serendipity_cursor(now)

    total = sum(counts.values())
    logger.info(
        "Serendipity detect complete: %d people, %d triggers queued %s",
        people_checked, total, dict(counts)
    )
    return {
        "scanned_people": people_checked,
        "new_events":     len(new_events),
        "triggered":      total,
        "by_type":        dict(counts),
    }


# ---------------------------------------------------------------------------
# Trigger processing (fire emails for ready triggers)
# ---------------------------------------------------------------------------

def process_pending_triggers(client, db_module, dry_run=False):
    """
    Fire emails for any pending triggers whose fire_after time has passed.
    Does a fresh eligibility check at fire time — things may have changed
    since the trigger was queued (agent claimed lead, lead opted out, etc.).

    Returns a summary dict.
    """
    from pond_mailer import send_email  # reuse existing SendGrid sender
    from fub_client import FUBClient
    from config import BARRY_FUB_USER_ID

    now     = datetime.now(timezone.utc)
    pending = db_module.get_pending_serendipity_triggers(now)

    logger.info("Serendipity process: %d triggers ready to fire", len(pending))

    sent    = 0
    skipped = 0
    failed  = 0

    for trigger in pending:
        tid          = trigger["id"]
        pid          = trigger["person_id"]
        email_addr   = trigger["email_address"]
        trigger_type = trigger["trigger_type"]
        trigger_data = trigger["trigger_data"] or {}
        is_dry       = trigger["dry_run"] or dry_run

        # Fresh person record — eligibility can change between queue and fire
        try:
            person = client.get_person(pid)
        except Exception as e:
            logger.warning("Serendipity: get_person(%s) at fire time failed: %s", pid, e)
            db_module.mark_serendipity_skipped(tid, f"get_person failed: {e}")
            skipped += 1
            continue

        if not person:
            db_module.mark_serendipity_skipped(tid, "person not found in FUB")
            skipped += 1
            continue

        eligible, reason = _is_eligible(person, db_module)
        if not eligible:
            logger.info("Serendipity: skip trigger %s (pid %s) — %s", tid, pid, reason)
            db_module.mark_serendipity_skipped(tid, reason)
            skipped += 1
            continue

        # Use freshest email address in case it changed
        email_addr = _get_email(person) or email_addr
        if not email_addr:
            db_module.mark_serendipity_skipped(tid, "no email at fire time")
            skipped += 1
            continue

        tags = _tag_names(person)

        # Generate the email
        try:
            email_data = generate_serendipity_email(
                person, trigger_type, trigger_data, tags, dry_run=is_dry
            )
        except Exception as e:
            logger.error("Serendipity: email generation failed for pid %s: %s", pid, e)
            db_module.mark_serendipity_skipped(tid, f"generation error: {e}")
            failed += 1
            continue

        subject   = email_data["subject"]
        body_text = email_data["body_text"]
        body_html = email_data["body_html"]

        # Send
        try:
            result = send_email(email_addr, subject, body_text, body_html, dry_run=is_dry)
            sg_id  = (result or {}).get("sg_message_id")
        except Exception as e:
            logger.error("Serendipity: send_email failed for pid %s: %s", pid, e)
            db_module.mark_serendipity_skipped(tid, f"send failed: {e}")
            failed += 1
            continue

        db_module.mark_serendipity_sent(tid, sg_id)

        # Log to FUB timeline so the lead's record reflects the outreach
        if not is_dry:
            try:
                fub = FUBClient()
                fub.log_email_sent(pid, subject, body_text, user_id=BARRY_FUB_USER_ID)
            except Exception as e:
                logger.warning("Serendipity: FUB note failed for trigger %s: %s", tid, e)

        logger.info(
            "Serendipity sent: pid=%s type=%s subject='%s' sg_id=%s",
            pid, trigger_type, subject, sg_id
        )
        sent += 1

    return {
        "processed": len(pending),
        "sent":      sent,
        "skipped":   skipped,
        "failed":    failed,
    }


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_serendipity(dry_run=True):
    """
    Detect new triggers + fire ready ones. Called by the scheduler every 10 min
    and by the /api/serendipity/run endpoint.

    Returns a combined result dict.
    """
    import db as _db
    from fub_client import FUBClient

    _db.ensure_serendipity_tables()

    client = FUBClient()

    detect_result  = detect_triggers(client, _db, dry_run=dry_run)
    process_result = process_pending_triggers(client, _db, dry_run=dry_run)

    return {
        "detect":  detect_result,
        "process": process_result,
        "dry_run": dry_run,
    }
