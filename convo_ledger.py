"""AI conversation ledger: one trustworthy record per AI conversation.

Barry's definitions (Nov 2026, settled): an AI text reply is a live
transfer (the lead is asking to talk), and an AI voice conversation is one
too. The weekly numbers must reconcile to Ylopo's own export.

Three writers, one table (db.ai_conversations):
  1. Ylopo's export CSVs (import_export)   the source of truth
  2. FUB evidence between imports (fill_from_fub): dated AI voice
     dispositions and Ylopo AI Text alert notes, stamped with the evidence
     time, never the scan time
  3. Follow-up (refresh_touches): the first human call or text after the
     conversation, who made it, and how many minutes it took

Verified against the Nov 9 to 15 export (22 rows, 20 people):
  - Voice: FUB's dated Call Disposition matched 8 of 8, to the minute.
  - Text: Ylopo's export is exactly the AI_NEEDS_FOLLOW_UP stage (the AI
    handed off to a human). The "has engaged X and they need follow up"
    alert note matched it 6 of 6 with no extras, but 8 of 14 left no
    dated trace in FUB at all, so text is only complete after an import.
  - The "started to successfully engage" alert is an EARLIER stage
    (AI_ENGAGED: the lead replied and the AI is still working them).
    12 people had it that week and none were in Ylopo's export. It is
    recorded as 'engaged' and never counted as a transfer.
"""
import csv
import io
import json
import logging
import re
from datetime import datetime, timedelta, timezone

import db as _db
from fub_client import FUBClient

logger = logging.getLogger(__name__)

try:
    from zoneinfo import ZoneInfo
    ET = ZoneInfo("America/New_York")
except Exception:  # pragma: no cover
    ET = timezone(timedelta(hours=-5))

TEXT_DATE_COL = "Last Lead Reply Date (America/New_York)"
VOICE_DATE_COL = "Conversation Date (America/New_York)"

# Dispositions that are attempts or dead ends, never a conversation.
NON_CONVO = {
    "NO ANSWER", "VOICEMAIL", "LEFT VOICEMAIL", "BUSY", "FAILED",
    "UNRESPONSIVE", "WRONG NUMBER", "BAD NUMBER", "DNC", "DO NOT CALL",
    "DO NOT CONTACT", "NOT INTERESTED", "SPAM", "NOT A LEAD", "DEAD",
    "GHOST CALL", "HUNG UP", "LEAD UNAVAILABLE"}

OUTCOME_LABELS = {
    "NURTURE": "Nurture",
    "ISA TRANSFER SUCCESSFUL": "Transfer Successful",
    "ISA TRANSFER UNSUCCESSFUL": "Transfer Unsuccessful",
    "ISA ATTEMPTED TRANSFER REALTOR UNAVAILABLE": "Transfer Unsuccessful",
    "CALLBACK SCHEDULED": "Callback Scheduled"}

# Evidence strong enough to count without Ylopo's export agreeing.
CONFIRMING_EVIDENCE = {"fub_disposition", "fub_followup_alert",
                       "fub_ai_reply", "isa_call", "isa_appointment"}

_DISP_RE = re.compile(r"Call Disposition:\s*([A-Za-z_ ]+)")


def _parse_fub_dt(s):
    try:
        return datetime.strptime((s or "")[:19], "%Y-%m-%dT%H:%M:%S") \
            .replace(tzinfo=timezone.utc)
    except ValueError:
        return None


def _et_day(dt):
    return dt.astimezone(ET).date()


def is_confirmed(row):
    return bool(row.get("in_export")) or \
        (row.get("fub_evidence") in CONFIRMING_EVIDENCE)


# ── 1. Ylopo export ─────────────────────────────────────────────────────────

def parse_export(text):
    """Rows from one Ylopo export CSV. The channel comes from which date
    column the file has; the FUB person id is the tail of 'CRM Link'."""
    text = (text or "").lstrip("﻿")
    reader = csv.DictReader(io.StringIO(text))
    cols = reader.fieldnames or []
    if TEXT_DATE_COL in cols:
        channel, date_col = "text", TEXT_DATE_COL
    elif VOICE_DATE_COL in cols:
        channel, date_col = "voice", VOICE_DATE_COL
    else:
        raise ValueError("Not a Ylopo text or call export: no conversation "
                         "date column found")
    rows, skipped = [], []
    for r in reader:
        m = re.search(r"/people/view/(\d+)", r.get("CRM Link") or "")
        raw = (r.get(date_col) or "").strip()
        if not m or not raw:
            skipped.append((r.get("Name") or "?").strip())
            continue
        try:
            at = datetime.strptime(raw[:16], "%Y-%m-%d %H:%M") \
                .replace(tzinfo=ET)
        except ValueError:
            skipped.append((r.get("Name") or "?").strip())
            continue
        rows.append({
            "person_id": int(m.group(1)), "channel": channel,
            "convo_at": at.astimezone(timezone.utc),
            "convo_day": at.date(),
            "outcome": (r.get("Call Outcome") or "").strip() or None,
            "lead_name": (r.get("Name") or "").strip() or None,
            "lead_source": (r.get("Source") or "").strip() or None,
            "owner": (r.get("CRM Agent / Pond") or "").strip() or None,
            "stage": (r.get("CRM Stage") or "").strip() or None})
    return {"channel": channel, "rows": rows, "skipped": skipped}


def import_export(text):
    parsed = parse_export(text)
    new = merged = 0
    for r in parsed["rows"]:
        res = _db.upsert_ai_conversation(
            r["person_id"], r["channel"], r["convo_day"], r["convo_at"],
            outcome=r["outcome"], lead_name=r["lead_name"],
            lead_source=r["lead_source"], owner=r["owner"],
            stage=r["stage"], in_export=True)
        if res == "new":
            new += 1
        elif res == "merged":
            merged += 1
    days = sorted(r["convo_day"] for r in parsed["rows"])
    if days:
        try:
            _db.set_app_state("ylopo_export_last_import", json.dumps({
                "at": datetime.now(timezone.utc).isoformat(),
                "channel": parsed["channel"],
                "from": days[0].isoformat(), "to": days[-1].isoformat()}))
        except Exception:
            pass
    return {"channel": parsed["channel"], "rows": len(parsed["rows"]),
            "new": new, "already_known": merged,
            "skipped": parsed["skipped"],
            "from": days[0].isoformat() if days else None,
            "to": days[-1].isoformat() if days else None}


# ── 2. FUB evidence between imports ─────────────────────────────────────────

def fub_events_for(person_id, since_utc, client=None):
    """Dated conversation evidence FUB holds for one person since a time.
    Returns [{channel, convo_at, outcome, evidence}] plus the person."""
    client = client or FUBClient()
    events = []
    try:
        cd = client._request("GET", "calls", params={
            "personId": person_id, "limit": 50}) or {}
        for c in next((v for v in cd.values() if isinstance(v, list)), []):
            m = _DISP_RE.search(c.get("note") or "")
            when = _parse_fub_dt(c.get("created"))
            if not m or not when or when < since_utc:
                continue
            disp = m.group(1).strip().upper().replace("_", " ")
            if disp in NON_CONVO:
                continue
            events.append({"channel": "voice", "convo_at": when,
                           "outcome": OUTCOME_LABELS.get(disp, disp.title()),
                           "evidence": "fub_disposition"})
    except Exception as e:
        logger.warning("ledger calls read failed for %s: %s", person_id, e)
    try:
        nd = client._request("GET", "notes", params={
            "personId": person_id, "limit": 50}) or {}
        for n in nd.get("notes") or []:
            body = n.get("body") or ""
            low = body.lower()
            when = _parse_fub_dt(n.get("created"))
            if not when or when < since_utc:
                continue
            m = _DISP_RE.search(body)
            if m:
                disp = m.group(1).strip().upper().replace("_", " ")
                if disp not in NON_CONVO:
                    events.append({
                        "channel": "voice", "convo_at": when,
                        "outcome": OUTCOME_LABELS.get(disp, disp.title()),
                        "evidence": "fub_disposition"})
                continue
            if "ylopo ai text has" not in low:
                continue
            if "need follow up" in low:
                ev_kind = "fub_followup_alert"     # the handoff
            elif "successfully engage" in low:
                ev_kind = "fub_engaged_alert"      # AI still talking
            else:
                continue
            events.append({"channel": "text", "convo_at": when,
                           "outcome": None, "evidence": ev_kind})
    except Exception as e:
        logger.warning("ledger notes read failed for %s: %s", person_id, e)
    # One event per (channel, ET day): strongest evidence, earliest time.
    best = {}
    for ev in events:
        key = (ev["channel"], _et_day(ev["convo_at"]))
        cur = best.get(key)
        strong = ev["evidence"] in CONFIRMING_EVIDENCE
        if cur is None:
            best[key] = dict(ev)
            continue
        if strong and cur["evidence"] not in CONFIRMING_EVIDENCE:
            cur["evidence"] = ev["evidence"]
        if ev["convo_at"] < cur["convo_at"]:
            cur["convo_at"] = ev["convo_at"]
        if ev.get("outcome") and (not cur.get("outcome")
                                  or cur["outcome"] == "Nurture"):
            cur["outcome"] = ev["outcome"]
    return list(best.values())


def fill_from_fub(person_ids, days=3):
    """Walk candidate people and write any dated FUB evidence into the
    ledger. Safe to re-run: the unique key makes it idempotent."""
    client = FUBClient()
    since = datetime.now(timezone.utc) - timedelta(days=days)
    wrote = 0
    for pid in person_ids:
        evs = fub_events_for(pid, since, client=client)
        if not evs:
            continue
        try:
            p = client._request("GET", "people/%s" % pid, params={
                "fields": "id,name,source,assignedTo,stage"}) or {}
        except Exception:
            p = {}
        for ev in evs:
            res = _db.upsert_ai_conversation(
                pid, ev["channel"], _et_day(ev["convo_at"]), ev["convo_at"],
                outcome=ev.get("outcome"),
                lead_name=(p.get("name") or "").strip() or None,
                lead_source=(p.get("source") or "").strip() or None,
                owner=(p.get("assignedTo") or "").strip() or None,
                stage=(p.get("stage") or "").strip() or None,
                in_export=False, fub_evidence=ev["evidence"])
            if res:
                wrote += 1
    return {"people": len(person_ids), "events_written": wrote}


HANDOFF_TAG = "AI_NEEDS_FOLLOW_UP"
_REPLY_FIELDS = ("id,name,source,assignedTo,stage,updated,"
                 "lastReceivedInboxAppMessage,lastCommunication")


def fill_text_from_fub_replies(days=3):
    """The automatic text source (found Nov 16 2026). Ylopo's AI text
    thread is an inbox app in FUB: the messages never reach the message
    APIs, but every person record carries lastReceivedInboxAppMessage, the
    time of the lead's last reply to the AI. A person tagged
    AI_NEEDS_FOLLOW_UP with a reply inside the window IS a row of Ylopo's
    text follow-up export: checked against Nov 9 to 15, 14 of 14 matched
    to the minute. No upload needed.

    Walks everyone carrying the tag (about 2,200 people, 22 pages)."""
    client = FUBClient()
    since = datetime.now(timezone.utc) - timedelta(days=days)
    seen = wrote = 0
    offset = 0
    while offset < 6000:
        r = client._request("GET", "people", params={
            "tags": HANDOFF_TAG, "sort": "-updated", "limit": 100,
            "offset": offset, "fields": _REPLY_FIELDS}) or {}
        people = r.get("people") or []
        for p in people:
            seen += 1
            raw = p.get("lastReceivedInboxAppMessage") or ""
            when = _parse_fub_dt(raw)
            if not when or when < since:
                continue
            lc = p.get("lastCommunication") or {}
            said = None
            if lc.get("direction") == "incoming" \
                    and (lc.get("date") or "")[:16] == raw[:16]:
                said = (lc.get("content") or "").strip()[:300] or None
            res = _db.upsert_ai_conversation(
                p["id"], "text", _et_day(when), when,
                lead_name=(p.get("name") or "").strip() or None,
                lead_source=(p.get("source") or "").strip() or None,
                owner=(p.get("assignedTo") or "").strip() or None,
                stage=(p.get("stage") or "").strip() or None,
                in_export=False, fub_evidence="fub_ai_reply",
                reply_text=said)
            if res:
                wrote += 1
        if len(people) < 100:
            break
        offset += 100
    return {"tagged_people_read": seen, "replies_written": wrote}


# ── Fhalen's transfers (Barry, Nov 16 2026: "I want Fhalen's transfers
# to be included") ───────────────────────────────────────────────────────────

ISA_NAME = "Fhalen Tendencia"


def fill_isa(days, is_dark_owner):
    """Fhalen's handoffs to agents, channel 'isa'. Two proofs, both only
    when a working agent owns the lead (her own pipeline, recruits and
    cash-offer sellers sit on her name or nobody's and never count):
      - a call of 3+ minutes she had with the lead
      - an appointment she is on that has the lead attached"""
    import config
    isa_uid = int(getattr(config, "ISA_USER_ID", 0) or 0)
    if not isa_uid:
        return {"isa_written": 0}
    client = FUBClient()
    found = {}   # (pid, day) -> (when, outcome, evidence)
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT person_id, created, duration FROM calls_cache
                    WHERE user_id = %s AND duration >= 180
                      AND created >= NOW() - make_interval(days => %s)
                      AND person_id IS NOT NULL
                    ORDER BY created
                """, (isa_uid, int(days)))
                for pid, created, duration in cur.fetchall():
                    if created.tzinfo is None:
                        created = created.replace(tzinfo=timezone.utc)
                    found.setdefault((int(pid), _et_day(created)), (
                        created, "Live transfer (%d min call)"
                        % round(duration / 60), "isa_call"))
    except Exception as e:
        logger.warning("ledger isa calls read failed: %s", e)
    try:
        since = datetime.now(timezone.utc) - timedelta(days=days)
        for a in client.get_appointments(
                since=since - timedelta(days=1),
                until=datetime.now(timezone.utc) + timedelta(days=60)) or []:
            inv = a.get("invitees") or []
            if not any(i.get("userId") == isa_uid for i in inv):
                continue
            made = _parse_fub_dt(a.get("created") or a.get("createdAt"))
            if not made or made < since:
                continue
            for i in inv:
                if i.get("personId"):
                    key = (int(i["personId"]), _et_day(made))
                    found[key] = (found.get(key, (made,))[0],
                                  "Appointment set", "isa_appointment")
    except Exception as e:
        logger.warning("ledger isa appointments read failed: %s", e)
    wrote = 0
    for (pid, day), (when, outcome, evidence) in found.items():
        try:
            p = client._request("GET", "people/%s" % pid, params={
                "fields": "id,name,source,assignedTo,stage"}) or {}
        except Exception:
            continue
        owner = (p.get("assignedTo") or "").strip()
        if is_dark_owner(owner):
            continue
        if _db.upsert_ai_conversation(
                pid, "isa", day, when, outcome=outcome,
                lead_name=(p.get("name") or "").strip() or None,
                lead_source=(p.get("source") or "").strip() or None,
                owner=owner, stage=(p.get("stage") or "").strip() or None,
                in_export=False, fub_evidence=evidence):
            wrote += 1
    return {"isa_written": wrote}


# ── 3. Follow-up: the first human touch after the conversation ──────────────

def first_human_touch(person_id, convo_at, client=None, exclude=()):
    """Earliest human call or text at or after the conversation. AI calls
    (they carry a Call Disposition) and unanswered inbound calls never
    count. Calls from a personal cell do not reach FUB and cannot be seen."""
    client = client or FUBClient()
    start = convo_at - timedelta(minutes=3)
    touches = []
    try:
        cd = client._request("GET", "calls", params={
            "personId": person_id, "limit": 50}) or {}
        for c in next((v for v in cd.values() if isinstance(v, list)), []):
            when = _parse_fub_dt(c.get("created"))
            who = (c.get("userName") or "").strip()
            if not when or when < start or not who or who in exclude:
                continue
            if "Call Disposition" in (c.get("note") or ""):
                continue
            incoming = bool(c.get("isIncoming"))
            if incoming and not (c.get("duration") or 0):
                continue
            touches.append((when, who, "answered the call" if incoming
                            else "called"))
    except Exception as e:
        logger.warning("ledger touch calls failed for %s: %s", person_id, e)
    try:
        td = client._request("GET", "textMessages", params={
            "personId": person_id, "limit": 50}) or {}
        for t in next((v for v in td.values() if isinstance(v, list)), []):
            when = _parse_fub_dt(t.get("created"))
            who = (t.get("userName") or "").strip()
            if not when or when < start or not who or t.get("isIncoming") \
                    or who in exclude:
                continue
            touches.append((when, who, "texted"))
    except Exception as e:
        logger.warning("ledger touch texts failed for %s: %s", person_id, e)
    if not touches:
        return None
    when, who, kind = min(touches)
    return {"at": when, "by": who, "kind": kind,
            "minutes": max(0, int((when - convo_at).total_seconds() // 60))}


def refresh_touches(start_day, end_day, limit=60):
    """Look for a first human touch on every row in range that has none."""
    client = FUBClient()
    checked = found = 0
    for row in _db.get_ai_conversations(start_day, end_day):
        if row.get("first_human_touch_at") or checked >= limit:
            continue
        checked += 1
        convo_at = datetime.fromisoformat(row["convo_at"])
        t = first_human_touch(
            row["person_id"], convo_at, client=client,
            exclude=(ISA_NAME,) if row["channel"] == "isa" else ())
        if t:
            found += 1
            _db.set_ai_conversation_touch(row["id"], t["at"], t["by"],
                                          t["kind"], t["minutes"])
        else:
            _db.set_ai_conversation_touch(row["id"])
    return {"checked": checked, "found": found}


# ── 4. The report: counts, reconciliation, and who nobody called ────────────

def report(start_day, end_day):
    rows = _db.get_ai_conversations(start_day, end_day)
    confirmed = [r for r in rows if is_confirmed(r)]
    text = [r for r in confirmed if r["channel"] == "text"]
    voice = [r for r in confirmed if r["channel"] == "voice"]
    isa = [r for r in confirmed if r["channel"] == "isa"]
    outcomes = {}
    for r in voice:
        k = r.get("outcome") or "Unknown"
        outcomes[k] = outcomes.get(k, 0) + 1
    touched = [r for r in confirmed if r.get("first_human_touch_at")]
    mins = sorted(r["minutes_to_touch"] for r in touched
                  if r.get("minutes_to_touch") is not None)
    last_import = None
    try:
        raw, _ = _db.get_app_state("ylopo_export_last_import")
        last_import = json.loads(raw) if raw else None
    except Exception:
        pass
    return {
        "start": str(start_day), "end": str(end_day),
        "counts": {
            "text": len(text), "voice": len(voice), "isa": len(isa),
            "conversations": len(confirmed),
            "unique_people": len({r["person_id"] for r in confirmed}),
            "voice_outcomes": outcomes,
            "touched": len(touched),
            "no_touch": len(confirmed) - len(touched),
            "median_minutes_to_touch": (mins[len(mins) // 2]
                                        if mins else None)},
        "reconciliation": {
            # Ylopo listed it and FUB showed us nothing. Should be empty
            # now that text comes from the reply field; anything here is a
            # gap worth a look.
            "ylopo_only": [r for r in rows if r.get("in_export")
                           and not r.get("fub_evidence")],
            # We saw strong evidence and Ylopo's export does not list it.
            "ledger_only": [r for r in rows if not r.get("in_export")
                            and r.get("fub_evidence") in CONFIRMING_EVIDENCE],
            # Replied to the AI, AI still working them: not a handoff yet,
            # so not counted. Shown so nobody wonders where they went.
            "engaged_not_handed_off": [r for r in rows
                                       if not is_confirmed(r)],
            "both": len([r for r in rows if r.get("in_export")
                         and r.get("fub_evidence")])},
        "no_touch": [r for r in confirmed
                     if not r.get("first_human_touch_at")],
        "rows": rows,
        "last_import": last_import}


def weekly_counts(days=7):
    """Text and voice conversation counts for the last N ET days, for the
    Sunday email. One per person per channel per day, confirmed only."""
    today = datetime.now(ET).date()
    start = today - timedelta(days=days - 1)
    rows = [r for r in _db.get_ai_conversations(start, today)
            if is_confirmed(r)]
    return {
        "text": len({r["person_id"] for r in rows if r["channel"] == "text"}),
        "voice": len({r["person_id"] for r in rows
                      if r["channel"] == "voice"}),
        "isa": len({r["person_id"] for r in rows if r["channel"] == "isa"}),
        "unique_people": len({r["person_id"] for r in rows}),
        "start": start.isoformat(), "end": today.isoformat()}


# ── 5. Handoff watch: nobody waits more than a day ──────────────────────────
# Barry, Nov 16 2026: "if the agent doesn't contact the lead within 24
# hours, as long as it's not sphere lead source, reassign lead." And: "I
# should not have any leads assigned to me as I am not selling."

NUDGE_HOURS = 4
REASSIGN_HOURS = 24
_NEGATIVE_REPLY = re.compile(
    r"^\s*(?:(?:no|nope|no thanks|no thank you)\s*[.!]*\s*$"
    r"|(?:stop|unsubscribe|remove me|not interested|wrong number"
    r"|wrong person|this is not \w+|who is this)\b)", re.I)
_DEAD_STAGES = ("closed", "trash", "archive", "do not contact", "dnc",
                "past client", "sphere")


def is_sphere(person):
    src = (person.get("source") or "").strip().lower()
    tags = " ".join(t.lower() for t in (person.get("tags") or []))
    return ("sphere" in src or src in ("soi", "s.o.i.")
            or "sphere" in tags or " soi" in " " + tags)


def watch_candidates(max_age_hours=72):
    """Untouched AI conversations, newest row per person, inside the
    window. Fhalen's transfers are tracked but not policed here."""
    today = datetime.now(ET).date()
    start = today - timedelta(days=max_age_hours // 24 + 1)
    now = datetime.now(timezone.utc)
    newest = {}
    for r in _db.get_ai_conversations(start, today):
        if r["channel"] not in ("text", "voice") or not is_confirmed(r):
            continue
        cur = newest.get(r["person_id"])
        if cur is None or r["convo_at"] > cur["convo_at"]:
            newest[r["person_id"]] = r
    out = []
    for r in newest.values():
        if r.get("first_human_touch_at"):
            continue
        age_h = (now - datetime.fromisoformat(r["convo_at"])) \
            .total_seconds() / 3600
        if age_h > max_age_hours:
            continue
        r = dict(r)
        r["age_hours"] = round(age_h, 1)
        out.append(r)
    return out


def reassign_note(row):
    when = datetime.fromisoformat(row["convo_at"]).astimezone(ET) \
        .strftime("%a %b %-d at %-I:%M %p")
    how = ("talked with the AI on the phone" if row["channel"] == "voice"
           else "replied to the AI by text")
    said = (' They said: "%s".' % row["reply_text"]) \
        if row.get("reply_text") else ""
    return ("%s %s on %s and has not had a call or text from us since.%s "
            "Pick up the thread naturally, no apology for the gap, just "
            "call." % ((row.get("lead_name") or "This lead").split()[0],
                       how, when, said))
