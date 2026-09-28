"""
dispatch.py — offer/accept/cascade assignment for converted leads.

The 76% problem: transfers and AI conversions get assigned and rot. This
engine replaces passive assignment with a claimed prize: the agent gets an
offer with a 5-minute window and a green Accept button; assignment in FUB
happens ON accept, never before. No accept, and the offer auto-advances
down the rotation (Barry: "auto advance for sure"). Three hops without an
owner and it lands back on Fhalen's board red, with Barry alerted, so a
converted appointment can never silently sit unowned.

Eligibility = the same weekly KPI priority group FUB round-robins from,
plus geography overrides (Chris August works Hampton and Newport News even
when out of rotation). Every offer, accept, pass, and expiry is logged with
latency: accept-rate becomes the leading indicator for speed-to-call.

Sources: 'fhalen' (dispatch board pick) today; 'ai_text' intake is wired
via the peopleTagsCreated webhook and queues to the board until Barry
switches off FUB's own round robin for that flow (dispatch_ai_live flag),
so the two systems never fight over the same lead.
"""

import logging
import os
import secrets
from datetime import datetime, timedelta, timezone

import config
import db as _db

logger = logging.getLogger(__name__)

OFFER_MINUTES = 5
MAX_HOPS = 3

# Geography overrides: agents offered leads in these cities even when out
# of rotation. Lowercase substring match against the lead's city.
DISPATCH_GEO_OVERRIDES = {
    "Christopher August": ["hampton", "newport news"],
}

_EXCLUDED = set(getattr(config, "EXCLUDED_USERS", [])) \
    | set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))


def eligible_agents(lead_city=None, audit=None):
    """Rotation-ordered eligible agents with their receipts.

    Base set: agents passing the current KPI audit (the priority group's
    source of truth) plus PROTECTED_AGENTS, minus excluded/gated. Geo
    overrides inject matching agents and float them to the front."""
    city = (lead_city or "").lower()
    gated = set()
    try:
        gated = _db.get_onboarding_gated()
    except Exception:
        pass
    passing = []
    for a in (audit or {}).get("agents", []):
        name = a.get("name")
        if not name or name in _EXCLUDED or name in gated:
            continue
        if (a.get("evaluation") or {}).get("overall_pass"):
            passing.append(name)
    if not passing:
        # Audit cache cold: fall back to the FUB Priority Agents group,
        # the same durable membership the AI's round robin draws from.
        try:
            from fub_client import FUBClient
            names = FUBClient().get_group_member_names(
                getattr(config, "PRIORITY_GROUP_ID", 12))
            passing = [n for n in names
                       if n not in _EXCLUDED and n not in gated]
        except Exception:
            pass
    for name in getattr(config, "PROTECTED_AGENTS", []):
        if name not in passing and name not in _EXCLUDED:
            passing.append(name)
    # New-hire grace (Barry, Sep 2026): unconditional desk access through
    # the listed date, KPI pass or not. Gated hires stay gated — docs first.
    today = datetime.now(timezone.utc).date().isoformat()
    for name, until in (getattr(config, "DESK_GRACE_UNTIL", {}) or {}).items():
        if today <= until and name not in passing \
                and name not in _EXCLUDED and name not in gated:
            passing.append(name)
    geo = [n for n, cities in DISPATCH_GEO_OVERRIDES.items()
           if city and any(c in city for c in cities)
           and n not in _EXCLUDED and n not in gated]
    # Rotation order: rotate the passing list by the stored pointer
    try:
        raw, _ = _db.get_app_state("dispatch_rr")
        ptr = int(raw or 0)
    except Exception:
        ptr = 0
    if passing:
        ptr = ptr % len(passing)
        ordered = passing[ptr:] + passing[:ptr]
    else:
        ordered = []
    # Geo agents lead for matching cities (dedupe, keep order)
    out = []
    for n in geo + ordered:
        if n not in out:
            out.append(n)
    return out


def advance_rotation():
    try:
        raw, _ = _db.get_app_state("dispatch_rr")
        _db.set_app_state("dispatch_rr", str((int(raw or 0) + 1) % 1000))
    except Exception:
        pass


def _accept_url(token):
    base = (os.environ.get("BASE_URL")
            or "https://web-production-3363cc.up.railway.app").rstrip("/")
    return "%s/a/%s" % (base, token)


def _slack_user_id(agent_name):
    try:
        import json as _json
        raw, _ = _db.get_app_state("slack_user_map")
        return (_json.loads(raw or "{}") or {}).get(agent_name)
    except Exception:
        return None


def fmt_secs(secs):
    """Human time: 47s under a minute, '1 min 32 sec' over (Barry, Sep 2026)."""
    try:
        secs = int(round(float(secs)))
    except (TypeError, ValueError):
        return "?"
    if secs < 60:
        return "%ds" % secs
    return "%d min %d sec" % (secs // 60, secs % 60)


def _notify(agent_name, offer_token, lead_name, lead_city, appt_time, source,
            hop, notes=None, person_id_for_copy=None):
    """The offer. Slack DM with Claim/Pass buttons is the primary channel
    (private offers, public wins — Barry, Sep 2026); the iMessage/email
    queue with the accept URL is the fallback so no offer ever depends on
    Slack being alive. The accept page stays channel-agnostic."""
    first = agent_name.split()[0]
    lead_first = (lead_name or "a new lead").split()[0]
    where = (" in %s" % lead_city.title()) if lead_city else ""
    when = (" %s" % appt_time) if appt_time else ""
    who = "Fhalen" if source == "fhalen" else \
          "The AI (voice)" if source == "ai_voice" else "The AI"
    again = " Second look, the first agent let it slide." if hop > 1 else ""

    # ── Slack DM first ────────────────────────────────────────────────────
    try:
        import json as _json
        import slack_client as _sl
        sid = _slack_user_id(agent_name)
        if _sl.is_available() and sid:
            evidence = ("\n>_%s_" % notes.strip()) if (notes or "").strip() else ""
            # Rotating headers, stable per lead (the same lab pattern as the
            # handoff texts: same message, slightly different, measured).
            headers = [
                "%s, %s just converted %s%s. First tap is yours.",
                "Hot one, %s. %s converted %s%s and you're up first.",
                "%s, fresh conversion: %s got %s%s to raise their hand.",
                "New money, %s. %s converted %s%s. Your button.",
                "%s, %s warmed up %s%s and you're first in line.",
            ]
            try:
                v = int("".join(c for c in str(person_id_for_copy)
                                if c.isdigit()) or 0) % len(headers)
            except Exception:
                v = 0
            head_line = headers[v] % (first, who, lead_first, where)
            if hop > 1:
                head_line += " Second look, the first agent let it slide."
            # One personal line from their own desk record. Positive or
            # neutral only: the moment of opportunity is never the moment
            # for a lecture.
            personal = ""
            try:
                snap = _db.get_desk_agent_snapshot(agent_name) or {}
                if not snap.get("offers"):
                    personal = ("\nYour first lead offer. Tap it and the "
                                "lead is yours in FUB instantly.")
                elif snap.get("median_secs") is not None:
                    personal = ("\nYou're claiming in about %s on average. "
                                "Fast hands eat first."
                                % fmt_secs(snap["median_secs"]))
            except Exception:
                pass
            appt_line = (" Appointment %s." % appt_time) if appt_time else ""
            head = ("\U0001f7e2 *%s*%s%s%s\n*%d minutes*, then it moves to "
                    "the next agent by name."
                    % (head_line, evidence, personal, appt_line,
                       OFFER_MINUTES))
            try:
                _db.log_variant("desk_offer", "v%d" % v,
                                person_id_for_copy, agent_name)
            except Exception:
                pass
            blocks = [
                {"type": "section", "text": {"type": "mrkdwn", "text": head}},
                {"type": "actions", "elements": [
                    {"type": "button", "style": "primary",
                     "text": {"type": "plain_text", "text": "CLAIM"},
                     "action_id": "desk_claim",
                     "value": _json.dumps({"token": offer_token})},
                    {"type": "button",
                     "text": {"type": "plain_text", "text": "Pass"},
                     "action_id": "desk_pass",
                     "value": _json.dumps({"token": offer_token})},
                ]},
                {"type": "context", "elements": [
                    {"type": "mrkdwn",
                     "text": "Buttons not working? <%s|Claim here instead>."
                             % _accept_url(offer_token)}]},
            ]
            if _sl.dm_user(sid, "New lead offer: %s" % lead_first, blocks=blocks):
                return True
    except Exception as e:
        logger.warning("[DISPATCH] slack offer failed for %s: %s", agent_name, e)

    # ── Fallback: the standard queue (Barry's cell / email for Android) ───
    profile = next((p for p in (_db.get_agent_profiles(active_only=True) or [])
                    if p["agent_name"] == agent_name), None)
    if not profile:
        return False
    msg = ("%s, %s converted %s%s and picked you.%s Appointment%s. "
           "Tap to accept in the next %d minutes or it moves to the next "
           "agent: %s"
           % (first, who, lead_first, where, again, when, OFFER_MINUTES,
              _accept_url(offer_token)))
    return bool(_db.queue_agent_imessage(
        agent_name, profile.get("fub_user_id"),
        profile.get("phone") or profile.get("fub_phone"), msg,
        week_day="dispatch"))


def make_offer(source, person_id, lead_name, lead_city=None, appt_time=None,
               notes=None, agent_name=None, hop=1, audit=None):
    """Create + send one offer. agent_name None = next eligible."""
    # Concurrent webhook threads in a burst can race the rotation pointer
    # and the open-claim cap. A short lock serializes candidate picking;
    # if the lock is busy for 3s we proceed unlocked rather than drop the
    # offer (the cap check still narrows the damage).
    _lock = False
    if not agent_name:
        import time as _time
        for _ in range(10):
            if _db.try_acquire_job_lock("desk_pick"):
                _lock = True
                break
            _time.sleep(0.3)
    try:
        _made = _make_offer_inner(source, person_id, lead_name, lead_city,
                                  appt_time, notes, agent_name, hop, audit)
    finally:
        if _lock:
            _db.release_job_lock("desk_pick")
    return _made


def _make_offer_inner(source, person_id, lead_name, lead_city, appt_time,
                      notes, agent_name, hop, audit):
    if not agent_name:
        prior = {o["agent"] for o in _db.dispatch_person_state(person_id)}
        for cand in eligible_agents(lead_city, audit=audit):
            if cand in prior:
                continue
            # Open-claim cap: an agent already holding 2 live offers is
            # skipped, so one fast thumb can't vacuum a burst (Salma took
            # all 5 of a Ylopo tagging sweep on go-live day, Sep 2026).
            try:
                if _db.count_open_dispatch_offers(cand) >= 2:
                    continue
            except Exception:
                pass
            agent_name = cand
            break
    if not agent_name:
        return None
    token = secrets.token_urlsafe(9)
    oid = _db.create_dispatch_offer(source, person_id, lead_name, lead_city,
                                    appt_time, notes, agent_name, token,
                                    minutes=OFFER_MINUTES, hop=hop)
    if not oid:
        return None
    # Rotation advances per fresh offer, not per accept: five conversions
    # arriving in one burst must land on five different agents. (Go-live
    # day, Sep 2026: the accept-only pointer sent an entire Ylopo batch to
    # the same agent.)
    if hop == 1:
        advance_rotation()
    sent = _notify(agent_name, token, lead_name, lead_city, appt_time,
                   source, hop, notes=notes, person_id_for_copy=person_id)
    # Fhalen's feedback loop: her pick let it slide, tell her where it went.
    if source == "fhalen" and hop > 1:
        try:
            import slack_client as _sl
            fh = _slack_user_id("Fhalen Tendencia")
            if _sl.is_available() and fh:
                _sl.dm_user(fh, "Heads up: your pick for %s didn't claim in "
                                "time. The offer moved to %s."
                                % ((lead_name or "the lead").split()[0],
                                   agent_name.split()[0]))
        except Exception:
            pass
    if not sent:
        # An offer nobody was told about is a 5-minute dead hop. Loud.
        try:
            _db.log_automation_event(
                event_type="desk_notify_fail", person_id=person_id,
                person_name=lead_name, agent_name=agent_name,
                payload={"hop": hop, "source": source},
                triggered_by="dispatch")
            import slack_client as _snf
            ops = os.environ.get("SLACK_DESK_OPS_CHANNEL", "")
            if ops:
                _snf.post_message(ops, "\u26a0\ufe0f Offer to %s for %s "
                                       "could not be delivered on any "
                                       "channel. It expires unseen in %d "
                                       "minutes." % (agent_name,
                                                     lead_name or person_id,
                                                     OFFER_MINUTES))
        except Exception:
            pass
    logger.info("[DISPATCH] offer %s: %s -> %s (hop %d, sent=%s)",
                oid, lead_name, agent_name, hop, sent)
    return {"offer_id": oid, "agent_name": agent_name, "token": token}
