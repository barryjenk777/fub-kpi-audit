"""
lead_memory.py — auto-maintained "Lead Memory" prep note on priority FUB leads.

An agent opening a priority lead should not spend 3 minutes scrolling the
timeline before dialing. Lead Memory compiles what matters into one short
"LEAD MEMORY (auto)" note per lead, updated in place nightly, so prep is a
30-second read: what we KNOW, what's MISSING (the Motivation / Timeframe /
Location rubric MaverickRE grades calls on), and the NEXT MOVE.

Three tiers, because most leads have never spoken to anyone yet:
  TALKED      — an AI call transcript lives in the notes. Full brief.
  BEHAVIORAL  — no conversation, but real site activity. KNOW opens with
                "Never reached, no conversation yet" then summarizes behavior
                (properties, price band, areas, recency) from FUB events.
  THIN        — no conversation AND fewer than 3 meaningful events in 30 days.
                NO brief is written. A padded brief on a dead lead erodes
                trust in all briefs; silence is the honest output. Logged with
                brief_hash 'SKIPPED_THIN' so the delta rule doesn't retry.

Hard guardrails (enforced in code, not just prompt):
  - NO em/en dashes (Barry's rule) — stripped from every output.
  - NO fabrication — Claude gets only the lead's real FUB data and must say
    "unknown" for anything not present.
  - Returns None from generation on any failure; the nightly job never crashes
    on a bad lead, it counts the error and moves on.

Scope: union of LeadStream + LeadStream_Pond + PHOENIX + ISA_TRANSFER_FRESH
tagged leads, capped at config.LEAD_MEMORY_MAX_LEADS per run.

Delta rule: a lead is only re-compiled when its FUB activity marker moved
since the last run (lead_briefs.last_activity_seen). Max
config.LEAD_MEMORY_MAX_GENERATIONS LLM calls per run; the rest wait a night.

Dry run (config.LEAD_MEMORY_DRY_RUN, default ON): first 5 changed leads only,
zero FUB writes, sample briefs emailed to Barry.
"""

import hashlib
import json
import logging
import os
import zoneinfo
from datetime import datetime, timezone

import requests as _requests

import config
import db as _db
from coach_voice import _strip_dashes
from fub_client import FUBClient

logger = logging.getLogger("lead_memory")

_ET = zoneinfo.ZoneInfo("America/New_York")

NOTE_SUBJECT = getattr(config, "LEAD_MEMORY_NOTE_SUBJECT", "LEAD MEMORY (auto)")

# Event types that count as "meaningful" for the minimum-data floor.
# Visited Website alone does not count — everyone trips that.
_MEANINGFUL_EVENT_TYPES = {
    "Viewed Property",
    "Saved Property",
    "Seller Inquiry",
    "Property Inquiry",
    "General Inquiry",
    "Cash Offer Request",
}

# A note is treated as a conversation record (TALKED tier) when it looks like
# a Ylopo AI call transcript or logged conversation. Keyword match on subject
# + first chunk of body; deliberately loose, verified by a human via dry run.
_CONVERSATION_MARKERS = (
    "transcript", "raiya", "ylopo ai", "ai call", "call summary",
    "conversation summary", "call recording",
)


def _client():
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return None
    try:
        import anthropic
        return anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        logger.warning("lead_memory: anthropic client init failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Scope + delta
# ---------------------------------------------------------------------------

def _collect_scope(fub):
    """Union of priority leads by tag, deduped, capped. FUB's tag= filter is
    unreliable (returns extras), so membership is verified client-side."""
    tags = [
        getattr(config, "LEADSTREAM_TAG", "LeadStream"),
        getattr(config, "LEADSTREAM_POND_TAG", "LeadStream_Pond"),
        getattr(config, "PHOENIX_TAG", "PHOENIX"),
        getattr(config, "ISA_TRANSFER_FRESH_TAG", "ISA_TRANSFER_FRESH"),
    ]
    cap = int(getattr(config, "LEAD_MEMORY_MAX_LEADS", 250))
    seen = {}
    for tag in tags:
        if len(seen) >= cap:
            break
        try:
            people = fub.get_people(tag=tag, limit=100)
        except Exception as e:
            logger.warning("[LEAD MEMORY] scope fetch failed for tag %s: %s", tag, e)
            continue
        for p in people or []:
            pid = p.get("id")
            if not pid:
                continue
            if tag not in (p.get("tags") or []):
                continue  # FUB tag filter over-returned; skip non-members
            pid = str(pid)
            if pid not in seen:
                seen[pid] = p
            if len(seen) >= cap:
                break
    return list(seen.values())


def _activity_marker(person):
    """The freshest activity stamp FUB exposes on the person record itself.
    Used for the delta rule without any per-lead API calls."""
    last_comm = (person.get("lastCommunication") or {}).get("createdAt")
    return (person.get("lastActivity")
            or last_comm
            or person.get("updated")
            or person.get("created")
            or "")


# ---------------------------------------------------------------------------
# Per-lead data assembly
# ---------------------------------------------------------------------------

def _is_conversation_note(note):
    probe = ((note.get("subject") or "") + " " + (note.get("body") or "")[:400]).lower()
    return any(m in probe for m in _CONVERSATION_MARKERS)


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


def _assemble_lead_data(fub, person):
    """Gather everything real we have on one lead. Returns
    (data_dict, tier, memory_note) where tier is 'talked' | 'behavioral' | 'thin'
    and memory_note is our existing LEAD MEMORY note dict if one exists."""
    pid = str(person.get("id"))

    notes = fub.get_person_notes(pid, limit=60)
    memory_note = None
    convo_notes, other_notes = [], []
    for n in notes:
        if (n.get("subject") or "").strip() == NOTE_SUBJECT:
            if memory_note is None:
                memory_note = n
            continue  # never feed our own brief back into the next brief
        (convo_notes if _is_conversation_note(n) else other_notes).append(n)

    try:
        events = fub.get_events_for_person(pid, days=30) or []
    except Exception as e:
        logger.warning("[LEAD MEMORY] events fetch failed for %s: %s", pid, e)
        events = []
    meaningful = [e for e in events if e.get("type") in _MEANINGFUL_EVENT_TYPES]

    # Tier decision (the floor protects trust in every other brief)
    if convo_notes:
        tier = "talked"
    elif len(meaningful) >= 3:
        tier = "behavioral"
    else:
        tier = "thin"

    if tier == "thin":
        return None, tier, memory_note

    addresses = []
    for a in (person.get("addresses") or []):
        line = ", ".join(str(a.get(k)) for k in ("street", "city", "state")
                         if a.get(k))
        if line:
            addresses.append(line)

    apt_tags = [t for t in (person.get("tags") or []) if t.startswith("APT_")]
    phoenix = _db.get_phoenix_latest_for_person(pid)

    def _note_block(n, body_cap):
        return {
            "created": (n.get("created") or "")[:10],
            "subject": (n.get("subject") or "")[:120],
            "body": (n.get("body") or "")[:body_cap],
        }

    data = {
        "person": {
            "name": person.get("name") or
                    f"{person.get('firstName', '')} {person.get('lastName', '')}".strip(),
            "stage": person.get("stage"),
            "source": person.get("source"),
            "tags": (person.get("tags") or [])[:40],
            "price": person.get("price"),
            "addresses": addresses,
            "has_email": bool(person.get("emails")),
            "has_phone": bool(person.get("phones")),
            "created": (person.get("created") or "")[:10],
            "last_communication": person.get("lastCommunication") or None,
        },
        "appointment_tags": apt_tags,
        # Transcripts first and biggest — they are the richest source we have.
        "ai_conversation_notes": [_note_block(n, 2500) for n in convo_notes[:3]],
        "other_fub_notes": [_note_block(n, 800) for n in other_notes[:5]],
        "site_activity_last_30d": [_compact_event(e) for e in events[:20]],
        "phoenix_resurrection": phoenix,
        "text_messages": "not available in this system, never cite texts",
    }
    return data, tier, memory_note


# ---------------------------------------------------------------------------
# Generation
# ---------------------------------------------------------------------------

_SYSTEM = """You compile a short prep note ("Lead Memory") on one real estate lead so the agent can prep a call in 30 seconds instead of 3 minutes of timeline scrolling.

HARD RULES (breaking any of these ruins the note):
- Use ONLY facts present in the supplied data. If something is unknown, say it is unknown. NEVER invent a name, date, price, area, timeline, or intent. NEVER soften "unknown" into a guess.
- NEVER use em dashes or en dashes. Periods and commas only.
- Punchy telegraphic utility copy. Fragments over sentences. No filler, no coaching, no cheerleading, no restating the obvious.
- MISSING grades against the Motivation / Timeframe / Location rubric (what MaverickRE grades calls on), plus financing when unknown. Name only what is actually missing.
- NEXT MOVE is one specific opening question grounded in a real detail that appears in KNOW, followed by the bridge toward a face to face meeting (see NEXT MOVE DOCTRINE).
- SOURCE names only data sources actually supplied and used. Text messages were NOT supplied, never cite texts.

NEXT MOVE DOCTRINE (Barry Jenkins, "Too Nice for Sales"):
The goal of every call is a face to face meeting, not information gathering. Most of these leads think they do not need an agent yet. Many are 6 to 12 months out. The team's proven accelerator: get the not-ready lead to see the value of education NOW, because early conversations prevent expensive mistakes later and quietly win the relationship before any competitor exists.
So NEXT MOVE has two beats, written as one short flow:
1. The opener: a permission-based, disarming question grounded in a real detail from KNOW. Teach, do not push. The signature reframe for not-ready leads, adapt it to their situation, never verbatim every time: acknowledging they are not ready yet, then offering the one or two things they need to know so they do not make a mistake when they are ready.
2. The bridge: if they engage, move to the meeting with a low-pressure, assumptive, two-option close tied to something concrete for THEM (seeing the house to give a real number, a 20 minute walk-through, a sit-down on their numbers). Example shape: "Worth 20 minutes at the house so you are not guessing? I can do Thursday evening or Saturday morning."
Rules for the doctrine: never fake urgency, never pressure, never make the meeting about the agent's need. The meeting is framed as the lead getting clarity earlier than everyone else. If the lead already has a meeting on the books, NEXT MOVE protects or advances that meeting instead."""

_TIER_STEER = {
    "talked": """This lead HAS a conversation record (AI call transcript in the notes). Full brief:
- KNOW: facts from the transcript and CRM fields. Timeline, motivation, price band, area, financing, decision makers, personal details, last real touch. The transcript is the richest source, mine it.
- NEXT MOVE: one opening question that picks up a real thread from the conversation, then the face to face bridge per the doctrine.
- SOURCE: cite "AI transcript" with the note's real date, plus "site activity" if events were used.""",
    "behavioral": """This lead has NEVER been reached. No conversation exists. Behavioral brief:
- KNOW: open with exactly "Never reached, no conversation yet." Then summarize BEHAVIOR from the site activity: property types viewed, price band (and whether it drifted), areas, view and save counts with recency, time-of-day pattern only if clearly evident, plus source and lead age from CRM fields.
- MISSING: state plainly that everything conversational is unknown, then the rubric items.
- NEXT MOVE: a behavioral opener referencing a specific property or pattern from the events, with its real street or area (the home they keep returning to), then the face to face bridge per the doctrine, for example offering to get them inside that specific house.
- SOURCE: "site activity" plus "CRM fields". No transcript exists, never claim one.""",
}


def _generate_brief(client, data, tier, errs=None):
    """Call Claude to compile the 4 content lines. Returns the full note text
    (header + 4 lines, dash-stripped) or None on any failure. When errs (a
    list) is passed, failure detail is appended so the run summary can show
    WHY briefs failed instead of a bare error count."""
    today_label = datetime.now(_ET).strftime("%b %d")
    prompt = f"""Compile the Lead Memory note for this lead.

{_TIER_STEER[tier]}

THE ONLY DATA YOU MAY USE (anything not in here is unknown):
{json.dumps(data, indent=1, default=str)[:14000]}

OUTPUT exactly these four lines, plain text, nothing before or after:
KNOW: ...
MISSING: ...
NEXT MOVE: ...
SOURCE: ...

Keep the four lines under 600 characters combined. No em dashes or en dashes anywhere. Never invent a name, date, price, or intent."""
    try:
        resp = client.messages.create(
            model=getattr(config, "LEAD_MEMORY_MODEL", "claude-haiku-4-5"),
            max_tokens=400,
            system=_SYSTEM,
            messages=[{"role": "user", "content": prompt}],
            # anthropic SDK 1.x dropped the temperature kwarg from
            # Messages.create(); Haiku 4.5 still honors it at the API level,
            # so send it through extra_body to keep extraction literal.
            extra_body={"temperature": float(getattr(config, "LEAD_MEMORY_TEMPERATURE", 0.3))},
        )
        raw = resp.content[0].text.strip()
    except Exception as e:
        logger.warning("lead_memory generation failed: %s", e)
        if errs is not None:
            errs.append(f"generation: {str(e)[:200]}")
        return None

    lines = {}
    current = None
    for line in raw.splitlines():
        stripped = line.strip()
        matched = False
        for label in ("KNOW:", "MISSING:", "NEXT MOVE:", "SOURCE:"):
            if stripped.upper().startswith(label):
                current = label
                lines[label] = stripped[len(label):].strip()
                matched = True
                break
        if not matched and current and stripped:
            lines[current] += " " + stripped  # wrapped continuation
    if any(not lines.get(l) for l in ("KNOW:", "MISSING:", "NEXT MOVE:", "SOURCE:")):
        logger.warning("lead_memory: weak output, dropping. raw=%r", raw[:160])
        if errs is not None:
            errs.append(f"weak output: {raw[:120]}")
        return None

    body = "\n".join(f"{l} {lines[l]}" for l in
                     ("KNOW:", "MISSING:", "NEXT MOVE:", "SOURCE:"))
    body = _strip_dashes(body)
    if len(body) > 900:
        body = body[:900].rsplit(" ", 1)[0]
    header = f"LEAD MEMORY (auto) . updated {today_label}"
    return f"{header}\n{body}"


def _brief_hash(brief):
    """Hash of the content lines only, so a date-only header change never
    counts as a new brief."""
    content = brief.split("\n", 1)[1] if "\n" in brief else brief
    return hashlib.sha256(content.encode("utf-8")).hexdigest()[:32]


# ---------------------------------------------------------------------------
# Note write (live mode only)
# ---------------------------------------------------------------------------

def _write_note(fub, pid, brief, stored_note_id, memory_note):
    """Create or update the lead's single LEAD MEMORY note. Returns note_id.
    Raises on hard failure so the caller counts it as an error."""
    note_id = stored_note_id
    if not note_id and memory_note:
        # Adopt an existing note (e.g. table was reset) instead of duplicating.
        note_id = str(memory_note.get("id") or "") or None
    if note_id:
        try:
            fub.update_note(note_id, subject=NOTE_SUBJECT, body=brief)
            return note_id
        except _requests.HTTPError as e:
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status != 404:
                raise
            # Note was deleted in FUB. Create fresh below and store the new id.
    created = fub.create_note(pid, NOTE_SUBJECT, brief)
    new_id = str((created or {}).get("id") or "") or None
    if not new_id:
        raise RuntimeError(f"FUB create_note returned no id for person {pid}")
    return new_id


# ---------------------------------------------------------------------------
# Emails to Barry
# ---------------------------------------------------------------------------

def _email_dry_run_samples(samples, summary):
    """samples: list of (lead_name, tier, brief). Dash rule applies to copy."""
    import postmark_client as _pm
    tier_label = {"talked": "TALKED", "behavioral": "BEHAVIORAL"}
    parts = [
        "<p><b>DRY RUN.</b> No FUB notes were written. Below are the sample "
        "briefs Lead Memory WOULD have posted, one note per lead, updated in "
        "place. TALKED means an AI transcript exists. BEHAVIORAL means never "
        "reached, site activity only. Leads with no conversation and under 3 "
        f"meaningful events get NO brief on purpose ({summary.get('skipped_thin', 0)} "
        "such leads this run).</p>"
    ]
    for name, tier, brief in samples:
        parts.append(f"<h3>{name} <span style='color:#888;font-weight:normal'>"
                     f"[{tier_label.get(tier, tier)}]</span></h3>")
        parts.append("<pre style='background:#f4f4f4;padding:12px;border-radius:8px;"
                     f"white-space:pre-wrap'>{brief}</pre>")
    parts.append(f"<p>Scope: {summary['scope']} priority leads, "
                 f"{summary['changed']} changed since last run, "
                 f"{summary['skipped_unchanged']} unchanged and skipped.</p>")
    parts.append("<p style='color:#888'>Set LEAD_MEMORY_DRY_RUN=0 in Railway "
                 "(then Deploy the staged change) to go live.</p>")
    _pm.send(to=config.BARRY_EMAIL, from_email=config.EMAIL_FROM,
             subject=f"[DRY RUN] Lead Memory: {len(samples)} sample briefs",
             html="".join(parts))


def _email_live_summary(summary):
    import postmark_client as _pm
    line = (f"Lead Memory refreshed {summary['written']} briefs, "
            f"{summary['new']} new")
    _pm.send(to=config.BARRY_EMAIL, from_email=config.EMAIL_FROM,
             subject=line, html=f"<p>{line}.</p>")


def _pick_samples(samples, cap):
    """Choose the dry-run email set: one of each tier first (so Barry sees
    both the TALKED and BEHAVIORAL shapes when both exist), then fill the
    remaining slots in scan order."""
    picked, seen_tiers = [], set()
    for s in samples:
        if s[1] not in seen_tiers:
            picked.append(s)
            seen_tiers.add(s[1])
    for s in samples:
        if len(picked) >= cap:
            break
        if s not in picked:
            picked.append(s)
    return picked[:cap]


# ---------------------------------------------------------------------------
# The run
# ---------------------------------------------------------------------------

def run_lead_memory_refresh(dry_run=None, include_briefs=False, send_email=True):
    """Compile/update Lead Memory briefs. Returns a JSON-safe summary dict.

    dry_run=None reads config.LEAD_MEMORY_DRY_RUN (defaults ON). Dry run:
    first 5 changed leads, ZERO writes anywhere, samples emailed to Barry.
    include_briefs=True puts the sample brief texts in the summary (dry run
    only); send_email=False suppresses the dry-run sample email (preview)."""
    if dry_run is None:
        dry_run = bool(getattr(config, "LEAD_MEMORY_DRY_RUN", True))
    dry_run = bool(dry_run)

    _db.ensure_lead_briefs_table()
    summary = {
        "ok": True, "dry_run": dry_run,
        "run_date": datetime.now(_ET).date().isoformat(),
        "scope": 0, "changed": 0, "skipped_unchanged": 0,
        "generated": 0, "written": 0, "new": 0, "unchanged_brief": 0,
        "skipped_thin": 0, "deferred": 0, "errors": 0,
        "tiers": {"talked": 0, "behavioral": 0},
        "error_detail": [],   # first few failure reasons, for diagnosability
    }

    llm = _client()
    if llm is None:
        summary["generated"] = 0
        summary["reason"] = "ANTHROPIC_API_KEY not set, no briefs generated"
        logger.warning("[LEAD MEMORY] %s", summary["reason"])
        return summary

    fub = FUBClient()
    leads = _collect_scope(fub)
    summary["scope"] = len(leads)

    stored = _db.get_all_lead_briefs()
    changed = []
    for person in leads:
        pid = str(person.get("id"))
        marker = _activity_marker(person)
        row = stored.get(pid)
        if row and marker and row.get("last_activity_seen") == marker:
            summary["skipped_unchanged"] += 1
            continue
        changed.append(person)
    summary["changed"] = len(changed)

    samples = []          # dry run: (lead_name, tier, brief)
    sample_cap = int(getattr(config, "LEAD_MEMORY_DRY_RUN_SAMPLES", 5))
    gen_cap = int(getattr(config, "LEAD_MEMORY_MAX_GENERATIONS", 150))
    # Dry run scans past the first 5 (bounded) so the email can show Barry at
    # least one TALKED and one BEHAVIORAL shape when both exist in the pool.
    scan_cap = 40 if dry_run else len(changed)

    processed = 0
    for person in changed:
        if dry_run:
            if len(samples) >= sample_cap and len({t for _, t, _ in samples}) >= 2:
                break
            if processed >= scan_cap or len(samples) >= sample_cap * 2:
                break
        elif summary["generated"] >= gen_cap:
            summary["deferred"] = len(changed) - processed
            break
        processed += 1
        pid = str(person.get("id"))
        marker = _activity_marker(person)
        row = stored.get(pid)
        try:
            data, tier, memory_note = _assemble_lead_data(fub, person)
        except Exception as e:
            logger.warning("[LEAD MEMORY] assembly failed for %s: %s", pid, e)
            summary["errors"] += 1
            if len(summary["error_detail"]) < 3:
                summary["error_detail"].append(f"assembly {pid}: {str(e)[:200]}")
            continue

        if tier == "thin":
            # Minimum-data floor: no brief, and remember that so the delta
            # rule doesn't burn a lookup on this lead again until it moves.
            summary["skipped_thin"] += 1
            if not dry_run:
                _db.upsert_lead_brief(pid, (row or {}).get("note_id"),
                                      marker, "SKIPPED_THIN")
            continue

        gen_errs = [] if len(summary["error_detail"]) < 3 else None
        brief = _generate_brief(llm, data, tier, errs=gen_errs)
        if brief is None:
            summary["errors"] += 1
            if gen_errs:
                summary["error_detail"].extend(gen_errs[:1])
            continue
        summary["generated"] += 1
        summary["tiers"][tier] += 1

        if dry_run:
            name = data["person"]["name"] or f"Lead {pid}"
            samples.append((name, tier, brief))
            continue

        new_hash = _brief_hash(brief)
        if row and row.get("brief_hash") == new_hash and row.get("note_id"):
            # Same content as the note already in FUB. Move the marker, skip the write.
            summary["unchanged_brief"] += 1
            _db.upsert_lead_brief(pid, row.get("note_id"), marker, new_hash)
            continue

        try:
            note_id = _write_note(fub, pid, brief,
                                  (row or {}).get("note_id"), memory_note)
        except Exception as e:
            logger.warning("[LEAD MEMORY] note write failed for %s: %s", pid, e)
            summary["errors"] += 1
            continue
        summary["written"] += 1
        if row is None:
            summary["new"] += 1

        # Re-read the marker AFTER our write so our own note's ripple on the
        # person record can't make tomorrow's delta check see a phantom change.
        try:
            fresh = fub.get_person(pid)
            marker = _activity_marker(fresh) or marker
        except Exception:
            pass
        _db.upsert_lead_brief(pid, note_id, marker, new_hash)

    if dry_run:
        samples = _pick_samples(samples, sample_cap)
        summary["samples"] = [
            dict({"lead_name": n, "tier": t}, **({"brief": b} if include_briefs else {}))
            for n, t, b in samples
        ]
        if not send_email:
            summary["samples_emailed"] = 0
        elif samples:
            try:
                _email_dry_run_samples(samples, summary)
                summary["samples_emailed"] = len(samples)
            except Exception as e:
                logger.warning("[LEAD MEMORY] dry-run email failed: %s", e)
                summary["samples_emailed"] = 0
        else:
            summary["samples_emailed"] = 0
    elif summary["written"] > 0:
        try:
            _email_live_summary(summary)
        except Exception as e:
            logger.warning("[LEAD MEMORY] summary email failed: %s", e)

    logger.info("[LEAD MEMORY] run done: %s", summary)
    return summary
