"""
savebot.py — Appointment Save-Bot, Stage 1 (AGENT-facing only).

One job: morning SCRIPT PROMPTS. Every day at 7:45am ET, each agent with a
FUB appointment today or tomorrow gets ONE bundled iMessage containing, per
appointment, a ready-to-send text for the LEAD. The script is never a bare
"are we still on" confirmation. It implies prep work is already underway
(home search setup, CMA pull) and ends in one easy question, personalized
from the lead's real data: buyer or seller side from tags/stage/source, and
a real detail mined from AI transcripts, the Lead Memory note, other FUB
notes, or site activity property specifics.

Stage 1 HARD SAFETY:
  - This module NEVER contacts a lead. Only agents get texts, through the
    existing iMessage queue (db.queue_agent_imessage), same as coaching texts.
  - config.SAVEBOT_DRY_RUN defaults ON: everything is computed, the would-be
    texts are emailed to Barry, ZERO texts are queued.
  - config.EXCLUDED_USERS and config.COACHING_TEXT_EXCLUDED_AGENTS never get
    a text.
  - One scripts text per agent per day (savebot_log dedupe on live queues).

Script generation: Claude (claude-haiku-4-5, temperature 0.4 via extra_body,
same client pattern as lead_memory.py) with a hard no-fabrication rule. The
detail must come from supplied data. If no real detail exists, or the API
fails, a deterministic template that implies preparation without inventing
specifics is used instead. Every string passes coach_voice._strip_dashes.
"""

import json
import logging
import os
import zoneinfo
from datetime import datetime, timedelta, timezone

import config
import db as _db
from coach_voice import _strip_dashes
from fub_client import FUBClient

logger = logging.getLogger("savebot")

_ET = zoneinfo.ZoneInfo("America/New_York")

_MEMORY_NOTE_SUBJECTS = ({getattr(config, "LEAD_MEMORY_NOTE_SUBJECT", "CALL OPENER (auto)")}
                         | set(getattr(config, "LEAD_MEMORY_LEGACY_SUBJECTS", set())))

# Same loose conversation markers lead_memory.py uses to spot AI transcripts.
_CONVERSATION_MARKERS = (
    "transcript", "raiya", "ylopo ai", "ai call", "call summary",
    "conversation summary", "call recording",
)

# Event types worth mining for a property detail. Bare "Visited Website" hits
# carry nothing quotable.
_DETAIL_EVENT_TYPES = {
    "Viewed Property", "Saved Property", "Seller Inquiry",
    "Property Inquiry", "General Inquiry", "Cash Offer Request",
}

_SELLER_MARKERS = ("seller", "listing", "cma", "home value", "cash offer", "zbuyer")


def _client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        logger.warning("savebot: anthropic client init failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Appointment collection (today + tomorrow, ET)
# ---------------------------------------------------------------------------

def _fmt_time(dt_et):
    hour = dt_et.hour % 12 or 12
    ampm = "am" if dt_et.hour < 12 else "pm"
    return f"{hour}:{dt_et.minute:02d}{ampm}"


def _parse_start(appt):
    try:
        raw = (appt.get("start") or "").replace("Z", "+00:00")
        dt = datetime.fromisoformat(raw)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(_ET)
    except (ValueError, AttributeError):
        return None


def _collect_window_appointments(fub):
    """FUB appointments starting today or tomorrow (ET). Returns list of
    {start_et, person_id, lead_name, assigned_user_id}."""
    now_utc = datetime.now(timezone.utc)
    today = datetime.now(_ET).date()
    tomorrow = today + timedelta(days=1)

    # get_appointments paginates newest-created-first and filters on start;
    # a 30 day since keeps us clear of its early-break heuristic while still
    # returning every future-start appointment.
    all_appts = fub.get_appointments(since=now_utc - timedelta(days=30))

    out = []
    for appt in all_appts:
        start_et = _parse_start(appt)
        if start_et is None or start_et.date() not in (today, tomorrow):
            continue

        created_by = appt.get("createdById")
        person_id, lead_name, assigned_user_id = None, "Unknown", None
        for inv in appt.get("invitees") or []:
            if inv.get("personId") and not person_id:
                person_id = inv["personId"]
                lead_name = inv.get("name") or "Unknown"
            if inv.get("userId") and inv["userId"] != created_by and not assigned_user_id:
                assigned_user_id = inv["userId"]
        if not assigned_user_id:
            for inv in appt.get("invitees") or []:
                uid = inv.get("userId")
                if uid and uid != getattr(config, "ISA_USER_ID", None):
                    assigned_user_id = uid
                    break

        out.append({
            "start_et": start_et,
            "is_tomorrow": start_et.date() == tomorrow,
            "person_id": person_id,
            "lead_name": lead_name,
            "assigned_user_id": assigned_user_id,
        })

    out.sort(key=lambda a: a["start_et"])
    return out


# ---------------------------------------------------------------------------
# Per-lead data: side + real detail sources
# ---------------------------------------------------------------------------

def _classify_side(person, events):
    """buyer | seller from tags/stage/source, seller inquiry events last."""
    probe = " ".join(
        [str(t) for t in (person.get("tags") or [])]
        + [str(person.get("stage") or ""), str(person.get("source") or "")]
    ).lower()
    if any(m in probe for m in _SELLER_MARKERS):
        return "seller"
    if any((e.get("type") in ("Seller Inquiry", "Cash Offer Request")) for e in events):
        return "seller"
    return "buyer"


def _compact_event(ev):
    out = {"created": (ev.get("created") or "")[:16], "type": ev.get("type")}
    prop = ev.get("property") or {}
    if isinstance(prop, dict) and prop:
        out["property"] = {k: prop.get(k) for k in
                           ("street", "city", "state", "price", "bedrooms",
                            "bathrooms", "area", "type")
                           if prop.get(k) not in (None, "", 0)}
    msg = ev.get("message")
    if msg:
        out["message"] = str(msg)[:200]
    return out


def _assemble_lead_data(fub, person_id, lead_name):
    """Everything real we have on one lead, compact. Returns (data, has_detail).
    has_detail=False means nothing quotable exists and the deterministic
    fallback should be used without spending an LLM call."""
    person = {}
    try:
        person = fub.get_person(person_id) or {}
    except Exception as e:
        logger.warning("savebot: get_person failed for %s: %s", person_id, e)

    notes = []
    try:
        notes = fub.get_person_notes(str(person_id), limit=40) or []
    except Exception as e:
        logger.warning("savebot: notes fetch failed for %s: %s", person_id, e)

    memory_note, convo_notes, other_notes = None, [], []
    for n in notes:
        subject = (n.get("subject") or "").strip()
        if subject in _MEMORY_NOTE_SUBJECTS:
            if memory_note is None:
                memory_note = n
            continue
        probe = (subject + " " + (n.get("body") or "")[:400]).lower()
        (convo_notes if any(m in probe for m in _CONVERSATION_MARKERS)
         else other_notes).append(n)

    events = []
    try:
        events = fub.get_events_for_person(str(person_id), days=30) or []
    except Exception as e:
        logger.warning("savebot: events fetch failed for %s: %s", person_id, e)

    detail_events = [e for e in events if e.get("type") in _DETAIL_EVENT_TYPES]
    side = _classify_side(person, events)

    def _note_block(n, cap):
        return {"created": (n.get("created") or "")[:10],
                "subject": (n.get("subject") or "")[:120],
                "body": (n.get("body") or "")[:cap]}

    data = {
        "lead": {
            "name": person.get("name") or lead_name,
            "stage": person.get("stage"),
            "source": person.get("source"),
            "tags": (person.get("tags") or [])[:30],
            "price": person.get("price"),
        },
        "side": side,
        "lead_memory_note": _note_block(memory_note, 900) if memory_note else None,
        "ai_conversation_notes": [_note_block(n, 1800) for n in convo_notes[:2]],
        "other_fub_notes": [_note_block(n, 500) for n in other_notes[:4]],
        "site_activity_last_30d": [_compact_event(e) for e in detail_events[:12]],
    }
    has_detail = bool(memory_note or convo_notes or other_notes or detail_events)
    return data, has_detail


# ---------------------------------------------------------------------------
# Script generation
# ---------------------------------------------------------------------------

_SYSTEM = """You write ONE short text message a real estate agent will send to a lead ahead of their appointment. It is never a bare confirmation. It shows the agent is already doing prep work for THIS lead, weaves in ONE real detail about them, and ends with one easy question the lead can answer in seconds.

HARD RULES (breaking any of these ruins the text):
- Use ONLY details present in the supplied data. NEVER invent a preference, bed or bath count, price, street, renovation, timeline, or anything else. If the data contains no usable personal detail, output exactly NO_DETAIL and nothing else.
- One or two short sentences, under 220 characters total. First person as the agent. Plain casual texting tone. No greeting, no sign off, no emojis.
- NEVER use em dashes or en dashes. Periods, commas, and question marks only.
- End with ONE easy question that confirms or clarifies the real detail. Never "are we still on" and never a question that requires homework to answer.
- Never mention AI, notes, transcripts, systems, or where the detail came from.

SHAPE EXAMPLES (match the shape, never copy verbatim):
Buyer: "I am working on your home search setup for our meeting tomorrow. Did you say you wanted 2 bathrooms?"
Seller: "I am working on your cma for our appt at 2pm tomorrow, i cant remember if you updated your kitchen?"
"""


def _fallback_script(side, when_phrase):
    """Deterministic script when no real detail exists or the API fails.
    Implies preparation without inventing specifics."""
    if side == "seller":
        return (f"I am putting your home value file together for {when_phrase}. "
                "Anything change on your end since we talked?")
    if side == "buyer":
        return (f"I am setting up your home search for {when_phrase}. "
                "Anything change on your end since we talked?")
    return (f"I am putting together your file for {when_phrase}. "
            "Anything change on your end since we talked?")


def _when_phrase(appt):
    if appt["is_tomorrow"]:
        return f"our {_fmt_time(appt['start_et'])} tomorrow"
    return f"our {_fmt_time(appt['start_et'])} today"


def _generate_script(client, data, when_phrase, errs=None):
    """One Claude call. Returns the script line, or None (caller falls back)."""
    if client is None:
        return None
    prompt = f"""Write the pre-appointment text for this lead.

Side: {data['side']}
The appointment: {when_phrase}. Refer to timing naturally using exactly that phrase or a tighter version of it.

THE ONLY DATA YOU MAY USE (anything not in here does not exist):
{json.dumps(data, indent=1, default=str)[:9000]}

OUTPUT: the text message only, nothing before or after. If no real personal detail exists in the data, output exactly NO_DETAIL."""
    try:
        resp = client.messages.create(
            model=getattr(config, "SAVEBOT_MODEL", "claude-haiku-4-5"),
            max_tokens=200,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
            # anthropic SDK 1.x dropped the temperature kwarg from
            # Messages.create(); Haiku 4.5 still honors it at the API level.
            extra_body={"temperature": float(getattr(config, "SAVEBOT_TEMPERATURE", 0.4))},
        )
        raw = _strip_dashes(resp.content[0].text.strip().strip('"').strip())
    except Exception as e:
        logger.warning("savebot: generation failed: %s", e)
        if errs is not None:
            errs.append(f"generation: {str(e)[:160]}")
        return None
    if not raw or "NO_DETAIL" in raw.upper() or len(raw) > 300 or "\n" in raw:
        return None
    return raw


# ---------------------------------------------------------------------------
# Message assembly
# ---------------------------------------------------------------------------

def _time_label(appt):
    label = _fmt_time(appt["start_et"])
    return f"Tomorrow {label}" if appt["is_tomorrow"] else label


def _build_agent_message(first, entries):
    """entries: list of (appt, side, script). One bundled text per agent."""
    parts = [f"{first}, appointment prep."]
    for i, (appt, side, script) in enumerate(entries):
        lead = appt["lead_name"]
        if i == 0:
            parts.append(f"{_time_label(appt)} {lead} ({side}). "
                         f"Send this now: '{script}'")
        else:
            parts.append(f"{_time_label(appt)} {lead} ({side}): '{script}'")
    parts.append("A lead who replies shows up.")
    return _strip_dashes(" ".join(parts))


# ---------------------------------------------------------------------------
# Dry-run email to Barry
# ---------------------------------------------------------------------------

def _email_dry_run(messages, summary):
    import postmark_client as _pm
    parts = [
        "<p><b>DRY RUN.</b> Zero texts queued. Below is the exact script "
        "prompt each agent WOULD get this morning, one bundled text per "
        "agent, scripts personalized from real lead data or the safe "
        "generic when nothing real exists.</p>"
    ]
    for m in messages:
        phone_note = "" if m["phone_on_file"] else \
            " <span style='color:#b91c1c'>(no phone on file, would be skipped live)</span>"
        parts.append(f"<h3>{m['agent']} <span style='color:#888;font-weight:normal'>"
                     f"{m['appt_count']} appt{'s' if m['appt_count'] != 1 else ''}</span>"
                     f"{phone_note}</h3>")
        parts.append("<pre style='background:#f4f4f4;padding:12px;border-radius:8px;"
                     f"white-space:pre-wrap'>{m['message']}</pre>")
    parts.append(f"<p>Window: today + tomorrow. {summary['appointments_considered']} "
                 f"appointments considered, {summary['llm']['personalized']} scripts "
                 f"personalized by Claude, {summary['llm']['fallback']} on the safe "
                 "generic template.</p>")
    parts.append("<p style='color:#888'>Set SAVEBOT_DRY_RUN=0 in Railway "
                 "(env edits are staged, click Deploy on the banner) to go live.</p>")
    _pm.send(to=config.BARRY_EMAIL, from_email=config.EMAIL_FROM,
             subject=f"[DRY RUN] Save-Bot scripts: {len(messages)} agent text"
                     f"{'s' if len(messages) != 1 else ''}",
             html="".join(parts))


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def run_scripts(dry_run=None):
    """Morning script prompts. Returns the summary dict."""
    if dry_run is None:
        dry_run = bool(getattr(config, "SAVEBOT_DRY_RUN", True))
    dry_run = bool(dry_run)

    _db.ensure_savebot_log_table()
    run_date = datetime.now(_ET).date()

    fub = FUBClient()
    appts = _collect_window_appointments(fub)

    summary = {
        "ok": True,
        "kind": "scripts",
        "dry_run": dry_run,
        "run_date": run_date.isoformat(),
        "appointments_considered": len(appts),
        "messages": [],
        "skipped": {"excluded_agent": 0, "no_agent": 0, "no_person": 0,
                    "no_phone": 0, "deduped": 0},
        "llm": {"personalized": 0, "fallback": 0, "errors": []},
        "queued": 0,
        "emailed_barry": False,
    }

    if not appts:
        summary["note"] = "No appointments today or tomorrow. No texts, no email."
        return summary

    profiles = {p["fub_user_id"]: p
                for p in (_db.get_agent_profiles(active_only=True) or [])
                if p.get("fub_user_id")}
    excluded = set(getattr(config, "EXCLUDED_USERS", [])) \
             | set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))
    already = _db.savebot_agents_logged("scripts", run_date, statuses=("queued",))

    # Group appointments by agent
    by_agent = {}
    for a in appts:
        uid = a["assigned_user_id"]
        profile = profiles.get(uid)
        if not profile:
            summary["skipped"]["no_agent"] += 1
            continue
        if profile["agent_name"] in excluded:
            summary["skipped"]["excluded_agent"] += 1
            continue
        if not a["person_id"]:
            summary["skipped"]["no_person"] += 1
            continue
        by_agent.setdefault(profile["agent_name"], {"profile": profile,
                                                    "appts": []})["appts"].append(a)

    client = _client()
    cap = int(getattr(config, "SAVEBOT_MAX_SCRIPTS_PER_RUN", 40))
    llm_calls = 0

    for agent_name in sorted(by_agent):
        bundle = by_agent[agent_name]
        profile = bundle["profile"]

        if not dry_run and agent_name in already:
            summary["skipped"]["deduped"] += 1
            continue

        entries = []
        for a in bundle["appts"]:
            when = _when_phrase(a)
            data, has_detail = _assemble_lead_data(fub, a["person_id"], a["lead_name"])
            script = None
            if has_detail and llm_calls < cap:
                llm_calls += 1
                script = _generate_script(client, data, when,
                                          errs=summary["llm"]["errors"])
            if script:
                summary["llm"]["personalized"] += 1
            else:
                script = _fallback_script(data["side"], when)
                summary["llm"]["fallback"] += 1
            entries.append((a, data["side"], _strip_dashes(script)))

        first = (agent_name.split() or ["there"])[0]
        message = _build_agent_message(first, entries)
        phone = profile.get("phone")

        row = {"agent": agent_name, "appt_count": len(entries),
               "message": message, "phone_on_file": bool(phone)}
        summary["messages"].append(row)

        if dry_run:
            _db.log_savebot(run_date, "scripts", agent_name, len(entries),
                            message[:500], "dry_run")
            continue

        if not phone:
            summary["skipped"]["no_phone"] += 1
            continue
        _db.queue_agent_imessage(agent_name, profile.get("fub_user_id"),
                                 phone, message)
        _db.log_savebot(run_date, "scripts", agent_name, len(entries),
                        message[:500], "queued")
        summary["queued"] += 1

    if dry_run and summary["messages"]:
        try:
            _email_dry_run(summary["messages"], summary)
            summary["emailed_barry"] = True
        except Exception as e:
            logger.error("savebot: dry-run email failed: %s", e)
            summary["email_error"] = str(e)[:200]

    return summary
