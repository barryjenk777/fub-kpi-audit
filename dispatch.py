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


def _notify(agent_name, offer_token, lead_name, lead_city, appt_time, source,
            hop, notes=None):
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
            head = ("\U0001f7e2 *%s, %s converted %s%s and it's your look.*%s%s\n"
                    "Appointment%s. *%d minutes*, then it moves to the next "
                    "agent by name."
                    % (first, who, lead_first, where, again, evidence, when,
                       OFFER_MINUTES))
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
    if not agent_name:
        prior = {o["agent"] for o in _db.dispatch_person_state(person_id)}
        for cand in eligible_agents(lead_city, audit=audit):
            if cand not in prior:
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
    sent = _notify(agent_name, token, lead_name, lead_city, appt_time,
                   source, hop, notes=notes)
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
    logger.info("[DISPATCH] offer %s: %s -> %s (hop %d, sent=%s)",
                oid, lead_name, agent_name, hop, sent)
    return {"offer_id": oid, "agent_name": agent_name, "token": token}
