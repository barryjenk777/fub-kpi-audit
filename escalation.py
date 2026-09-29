"""
escalation.py — the Guarantee.

Doctrine (Barry, Sep 2026): the system never guarantees a notification, it
guarantees an OUTCOME. Every lead-needs-a-human event opens an escalation
that only closes on a verified human touch (an outbound call or text to the
lead, confirmed by the webhooks we already process — zero polling). Until
then the ladder climbs on a clock, and Barry is the last rung, reached only
with the full receipt trail of everything already tried.

Ladder for kind="owned_reengage" (an owned lead engaged the AI):
  rung 1 (0 min)   owner Slack DM            (sent by the caller, app.py)
  rung 2 (+15 min) owner alternate channel   (iMessage/email queue)
  rung 3 (+30 min) claim board offer         (ownership expires under fire)
  rung 4 (+45 min) Fhalen, the human backstop
  rung 5 (+60 min) Barry, with the trail

kind="unclaimed" (a conversion cascaded to terminal, roster never claimed)
enters at rung 4: the roster already had its chance.

process_due() rides the every-minute dispatch cascade job.
resolve_for_person() is called from the outbound-touch webhook, so ANY
verified call or text to the lead closes the incident, whoever made it.
"""

import json
import logging
import os
from datetime import datetime, timezone

import config
import db as _db

logger = logging.getLogger("escalation")

# rung -> minutes-after-open at which it fires
_RUNGS = {2: 15, 3: 30, 4: 45, 5: 60}


def _ensure():
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS desk_escalations (
                        id          SERIAL PRIMARY KEY,
                        kind        TEXT NOT NULL,
                        person_id   TEXT NOT NULL,
                        lead_name   TEXT,
                        owner_agent TEXT,
                        rung        INTEGER DEFAULT 1,
                        opened_at   TIMESTAMPTZ DEFAULT NOW(),
                        resolved_at TIMESTAMPTZ,
                        resolution  TEXT,
                        trace       TEXT DEFAULT ''
                    );
                    CREATE INDEX IF NOT EXISTS idx_esc_open
                        ON desk_escalations (person_id)
                        WHERE resolved_at IS NULL;
                """)
    except Exception as e:
        logger.warning("ensure desk_escalations failed: %s", e)


def _fub_link(person_id):
    return ("https://yourfriendlyagent.followupboss.com/2/people/view/%s"
            % person_id)


def _slack_dm(agent_name, text):
    try:
        import slack_client as _sl
        import dispatch as _dp
        sid = _dp._slack_user_id(agent_name)
        return bool(_sl.is_available() and sid and _sl.dm_user(sid, text))
    except Exception:
        return False


def _queue_text(agent_name, text):
    try:
        prof = next((p for p in (_db.get_agent_profiles(active_only=True) or [])
                     if p["agent_name"] == agent_name), None)
        if not prof:
            return False
        return bool(_db.queue_agent_imessage(
            agent_name, prof.get("fub_user_id"),
            prof.get("phone") or "", text, week_day="dispatch"))
    except Exception:
        return False


def _append_trace(esc_id, line):
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                stamp = datetime.now(timezone.utc).strftime("%H:%M")
                cur.execute("""
                    UPDATE desk_escalations
                    SET trace = trace || %s WHERE id = %s
                """, ("[%s] %s\n" % (stamp, line), esc_id))
    except Exception:
        pass


def open_escalation(kind, person_id, lead_name, owner_agent=None, note=""):
    """Open (or ignore if one is already open for this lead)."""
    _ensure()
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id FROM desk_escalations
                    WHERE person_id = %s AND resolved_at IS NULL
                """, (str(person_id),))
                if cur.fetchone():
                    return None
                rung = 4 if kind == "unclaimed" else 1
                cur.execute("""
                    INSERT INTO desk_escalations
                        (kind, person_id, lead_name, owner_agent, rung, trace)
                    VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
                """, (kind, str(person_id), lead_name, owner_agent, rung,
                      ("opened (%s) %s\n" % (kind, note)).lstrip()))
                eid = cur.fetchone()[0]
        logger.info("[GUARANTEE] opened #%s %s for %s (owner %s)",
                    eid, kind, lead_name, owner_agent)
        # Unclaimed conversions enter AT the Fhalen rung, so her ask fires
        # now, not on the next timer (sequencing gap caught by Barry's
        # pond-vs-owned question, Sep 2026).
        if kind == "unclaimed":
            lead1 = (lead_name or "the lead").split()[0]
            msg = ("Fhalen, %s converted and cascaded through the whole "
                   "roster with no claim. Can you call them? Any call from "
                   "anyone closes this out. %s"
                   % (lead1, _fub_link(person_id)))
            sent = _slack_dm("Fhalen Tendencia", msg)
            _append_trace(eid, "rung 4: Fhalen asked immediately (%s)"
                          % ("sent" if sent else "FAILED"))
        return eid
    except Exception as e:
        logger.warning("open_escalation failed: %s", e)
        return None


def resolve_for_person(person_id, how="verified outbound touch"):
    """Any verified human touch to the lead closes the incident."""
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE desk_escalations
                    SET resolved_at = NOW(), resolution = %s
                    WHERE person_id = %s AND resolved_at IS NULL
                    RETURNING id, lead_name,
                        EXTRACT(EPOCH FROM NOW() - opened_at)::int / 60
                """, (how, str(person_id)))
                row = cur.fetchone()
        if row:
            logger.info("[GUARANTEE] resolved #%s (%s) after %s min: %s",
                        row[0], row[1], row[2], how)
            try:
                import slack_client as _sl
                ops = os.environ.get("SLACK_DESK_OPS_CHANNEL", "")
                if ops and row[2] >= 15:
                    _sl.post_message(ops, "✅ Guarantee closed: %s reached "
                                          "a human %d min after raising their "
                                          "hand." % (row[1] or "lead", row[2]))
            except Exception:
                pass
        return bool(row)
    except Exception as e:
        logger.warning("resolve_for_person failed: %s", e)
        return False


def process_due():
    """Climb one rung on every open escalation whose clock has expired.
    Called every minute by the dispatch cascade job."""
    _ensure()
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, kind, person_id, lead_name, owner_agent, rung,
                           EXTRACT(EPOCH FROM NOW() - opened_at)::int / 60
                    FROM desk_escalations
                    WHERE resolved_at IS NULL AND rung < 6
                """)
                rows = cur.fetchall()
    except Exception as e:
        logger.warning("process_due read failed: %s", e)
        return

    for eid, kind, pid, lead, owner, rung, age_min in rows:
        next_rung = rung + 1
        due_at = _RUNGS.get(next_rung)
        if due_at is None or age_min < due_at:
            continue
        lead1 = (lead or "the lead").split()[0]
        link = _fub_link(pid)
        try:
            if next_rung == 2 and owner:
                msg = ("%s, second ping and this one matters. Your lead %s "
                       "asked our AI for a human %d minutes ago and is still "
                       "waiting. Call now: %s"
                       % (owner.split()[0], lead1, age_min, link))
                sent = _queue_text(owner, msg)
                _append_trace(eid, "rung 2: alternate channel to %s (%s)"
                              % (owner, "sent" if sent else "FAILED"))
            elif next_rung == 3:
                import dispatch as _dp
                res = _dp.make_offer(
                    "cover", str(pid), lead, None,
                    notes=("Waiting %d minutes for a callback. First to tap "
                           "takes the call." % age_min))
                _append_trace(eid, "rung 3: claim board offer -> %s"
                              % ((res or {}).get("agent_name") or "no eligible agent"))
            elif next_rung == 4:
                msg = ("Fhalen, nobody has reached %s%s %d minutes after "
                       "they engaged the AI. Can you call them? Any call "
                       "from anyone closes this out. %s"
                       % (lead1, (" (%s's lead)" % owner.split()[0]) if owner else "",
                          age_min, link))
                sent = _slack_dm("Fhalen Tendencia", msg)
                _append_trace(eid, "rung 4: Fhalen asked (%s)"
                              % ("sent" if sent else "FAILED"))
            elif next_rung == 5:
                trace = ""
                try:
                    with _db.get_conn() as conn:
                        with conn.cursor() as cur:
                            cur.execute("SELECT trace FROM desk_escalations "
                                        "WHERE id = %s", (eid,))
                            trace = (cur.fetchone() or [""])[0] or ""
                except Exception:
                    pass
                msg = ("\U0001f6a8 %s has waited %d minutes for a human and "
                       "the machine is out of rungs. Everything tried:\n%s"
                       "They are still waiting: %s"
                       % (lead1, age_min, trace, link))
                _slack_dm("Barry Jenkins", msg)
                try:
                    import postmark_client as _pm
                    _pm.send(to=config.EMAIL_FROM, from_email=config.EMAIL_FROM,
                             subject="GUARANTEE: %s still waiting after %d min"
                                     % (lead1, age_min),
                             html="<pre>%s</pre><p><a href='%s'>Open in FUB"
                                  "</a></p>" % (trace, link))
                except Exception:
                    pass
                _append_trace(eid, "rung 5: Barry paged with the trail")
            with _db.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("UPDATE desk_escalations SET rung = %s "
                                "WHERE id = %s", (next_rung, eid))
        except Exception as e:
            logger.warning("[GUARANTEE] rung %d failed for #%s: %s",
                           next_rung, eid, e)
