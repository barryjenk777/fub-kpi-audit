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


def roster_policy():
    """DB-backed roster policy (Nov 7 2026, audit punch list #7): grace
    dates and protected agents are rows Barry edits on /admin/policy,
    not code that waits for a deploy. Falls back to config until the
    page saves once. Returns (grace_until: {name: 'YYYY-MM-DD'},
    protected: [names])."""
    import json as _json
    grace, protected = None, None
    try:
        raw, _ = _db.get_app_state("policy_grace_until")
        if (raw or "").strip():
            grace = _json.loads(raw)
    except Exception:
        grace = None
    try:
        raw, _ = _db.get_app_state("policy_protected_agents")
        if (raw or "").strip():
            protected = _json.loads(raw)
    except Exception:
        protected = None
    if grace is None:
        grace = dict(getattr(config, "DESK_GRACE_UNTIL", {}) or {})
    if protected is None:
        protected = list(getattr(config, "PROTECTED_AGENTS", []) or [])
    return grace, protected

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
    # Roster status (Barry, Nov 4 2026): license_pending / non_lead /
    # test / inactive never receive offers, whatever the audit says.
    try:
        gated = gated | _db.get_lead_gated_agents()
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
    _grace, _protected = roster_policy()
    for name in _protected:
        if name not in passing and name not in _EXCLUDED:
            passing.append(name)
    # New-hire grace (Barry, Sep 2026): unconditional desk access through
    # the listed date, KPI pass or not. Gated hires stay gated — docs first.
    today = datetime.now(timezone.utc).date().isoformat()
    for name, until in _grace.items():
        if today <= (until or "") and name not in passing \
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


def _junk_url(token):
    base = (os.environ.get("BASE_URL")
            or "https://web-production-3363cc.up.railway.app").rstrip("/")
    return "%s/j/%s" % (base, token)


def evidence_line(person_id):
    """THE EVIDENCE STANDARD (Barry, Nov 7 2026: three junk-lead
    incidents share one root cause, trusting tags). What we can PROVE
    about a lead, with dates: newest inbound text, newest connected
    inbound call, newest AI voice outcome parsed from the deep note
    history, newest property inquiry. Tags are hints and never appear
    here. Every offer carries this line; a lead whose evidence reads
    thin goes out looking thin, by design."""
    import re as _re
    from fub_client import FUBClient
    bits = []
    try:
        client = FUBClient()
        try:
            data = client._request("GET", "textMessages", params={
                "personId": person_id, "limit": 30}) or {}
            txts = next((v for v in data.values()
                         if isinstance(v, list)), [])
            inb = [t for t in txts if t.get("isIncoming")]
            if inb:
                inb.sort(key=lambda t: t.get("created") or "")
                bits.append(("texted us %s"
                             % (inb[-1].get("created") or "")[:10],
                             inb[-1].get("created") or ""))
        except Exception:
            pass
        try:
            data = client._request("GET", "calls", params={
                "personId": person_id, "limit": 20}) or {}
            calls = next((v for v in data.values()
                          if isinstance(v, list)), [])
            inc = [c for c in calls if c.get("isIncoming")
                   and (c.get("duration") or 0) >= 30]
            if inc:
                inc.sort(key=lambda c: c.get("created") or "")
                bits.append(("called in %s"
                             % (inc[-1].get("created") or "")[:10],
                             inc[-1].get("created") or ""))
        except Exception:
            pass
        try:
            # AI voice outcomes hide deep under system notes; walk up to
            # 200 of them for the newest dated disposition.
            newest = None
            for offset in (0, 100):
                data = client._request("GET", "notes", params={
                    "personId": person_id, "limit": 100,
                    "offset": offset}) or {}
                notes = data.get("notes") or []
                for n in notes:
                    body = n.get("body") or ""
                    if "Call Disposition" in body:
                        disp = _re.search(
                            r"Call Disposition:\s*([A-Za-z_ ]+)", body)
                        when = (n.get("created") or "")[:10]
                        if disp and (newest is None or when > newest[1]):
                            newest = ("AI call %s on %s"
                                      % (disp.group(1).strip(), when),
                                      when)
                if len(notes) < 100:
                    break
            if newest:
                bits.append(newest)
        except Exception:
            pass
        try:
            data = client._request("GET", "events", params={
                "personId": person_id, "limit": 15}) or {}
            for ev in data.get("events") or []:
                pr = ev.get("property") or {}
                if pr.get("street"):
                    bits.append(("asked about %s on %s"
                                 % (pr["street"],
                                    (ev.get("created") or "")[:10]),
                                 ev.get("created") or ""))
                    break
        except Exception:
            pass
    except Exception as e:
        logger.warning("[EVIDENCE] line failed for %s: %s", person_id, e)
    if not bits:
        return "Evidence on record: none. No inbound, no AI conversation, no inquiry."
    bits.sort(key=lambda b: b[1], reverse=True)
    return "Evidence: " + "; ".join(b[0] for b in bits[:3])


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
    # Nurture lane (Barry, Nov 6 2026): honest calm copy. These leads talked
    # with the AI and read as longer-timeline buyers — "converted" would be
    # a lie and fire-drill framing devalues the real conversions.
    nurture = source == "ai_nurture"

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
            if nurture:
                headers = [
                    "%s, %s had a real conversation with %s%s. Nobody owns "
                    "them. First tap does.",
                    "%s, bonus pipeline: %s talked with %s%s. Longer "
                    "timeline, zero competition, your button.",
                    "%s, %s got %s%s talking. Unowned nurture, first claim "
                    "takes it.",
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
                     "text": "Buttons not working? <%s|Claim here "
                             "instead>. Claimed it and it was junk? "
                             "<%s|Flag it>."
                             % (_accept_url(offer_token),
                                _junk_url(offer_token))}]},
            ]
            if _sl.dm_user(sid, "New lead offer: %s" % lead_first, blocks=blocks):
                # Dual delivery (Barry, Oct 2026: a weekend of offers died
                # unseen in Slack). Every offer also sends a nudge TEXT with
                # no link, pointing back to the Slack button, so an agent
                # away from Slack still hears the bell. Claims stay in
                # Slack only.
                try:
                    _prof = next((p for p in
                                  (_db.get_agent_profiles(active_only=True) or [])
                                  if p["agent_name"] == agent_name), None)
                    if _prof:
                        # Deep link straight to the bot DM so the card is
                        # one tap away (Barry, Oct 2026: hop-1 offers were
                        # expiring while agents hunted for the message).
                        _dml = ""
                        try:
                            import json as _json2
                            _raw, _unused = _db.get_app_state("slack_dm_channels")
                            _chm = _json2.loads(_raw or "{}")
                            _ch = _chm.get(agent_name)
                            if not _ch:
                                _ch = _sl.open_dm_channel(sid)
                                if _ch:
                                    _chm[agent_name] = _ch
                                    _db.set_app_state("slack_dm_channels",
                                                      _json2.dumps(_chm))
                            if _ch:
                                _dml = (" Tap here, the button is on your "
                                        "screen: https://app.slack.com/"
                                        "client/T1LDE7TCG/%s" % _ch)
                        except Exception:
                            pass
                        _act = ("talked with" if nurture else "converted")
                        _nudge = ("%s, live one: %s %s %s%s. First "
                                  "tap wins and it moves to the next agent "
                                  "in %d minutes.%s"
                                  % (first, who, _act, lead_first, where,
                                     OFFER_MINUTES, _dml))
                        _db.queue_agent_imessage(
                            agent_name, _prof.get("fub_user_id"),
                            _prof.get("phone") or _prof.get("fub_phone"),
                            _nudge, week_day="dispatch")
                except Exception as _de:
                    logger.warning("[DISPATCH] dual nudge failed for %s: %s",
                                   agent_name, _de)
                return True
    except Exception as e:
        logger.warning("[DISPATCH] slack offer failed for %s: %s", agent_name, e)

    # ── Fallback: the standard queue (Barry's cell / email for Android) ───
    profile = next((p for p in (_db.get_agent_profiles(active_only=True) or [])
                    if p["agent_name"] == agent_name), None)
    if not profile:
        return False
    msg = ("%s, %s %s %s%s and picked you.%s Appointment%s. "
           "Tap to accept in the next %d minutes or it moves to the next "
           "agent: %s"
           % (first, who,
              "talked with" if nurture else "converted",
              lead_first, where, again, when, OFFER_MINUTES,
              _accept_url(offer_token)))
    return bool(_db.queue_agent_imessage(
        agent_name, profile.get("fub_user_id"),
        profile.get("phone") or profile.get("fub_phone"), msg,
        week_day="dispatch"))


def _notify_shootout(agent_name, offer_token, lead_name, lead_city,
                     round_no, rivals):
    """The shootout offer. Same two channels as _notify (Slack DM with
    CLAIM/Pass, nudge text pointing back at the button), different story:
    this lead is shared with competing agents and several teammates are
    seeing the same button right now. First tap wins."""
    first = agent_name.split()[0]
    lead_first = (lead_name or "a new lead").split()[0]
    where = (" in %s" % lead_city.title()) if lead_city else ""
    mins = int(getattr(config, "REALTOR_OFFER_MINUTES", 2))
    if round_no == 1:
        head_line = ("%s, shootout: a paid Realtor.com lead just hit the "
                     "board. %s%s. Realtor.com sends this lead to competing "
                     "agents outside our team too, so the first voice "
                     "usually wins the relationship. %d of you are seeing "
                     "this at the same time. First CLAIM takes it."
                     % (first, lead_first, where, max(rivals, 1)))
    else:
        head_line = ("%s, round two: %s%s is still unclaimed. The whole "
                     "board is seeing this now. A paid lead with no owner "
                     "is money burning. First CLAIM takes it."
                     % (first, lead_first, where))
    # ── Slack DM first ────────────────────────────────────────────────────
    try:
        import json as _json
        import slack_client as _sl
        sid = _slack_user_id(agent_name)
        if _sl.is_available() and sid:
            head = ("\U0001f534 *%s*\n*%d minutes* on the clock, and the "
                    "clock is shared." % (head_line, mins))
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
                     "text": "Buttons not working? <%s|Claim here "
                             "instead>. Claimed it and it was junk? "
                             "<%s|Flag it>."
                             % (_accept_url(offer_token),
                                _junk_url(offer_token))}]},
            ]
            if _sl.dm_user(sid, "Shootout lead: %s" % lead_first,
                           blocks=blocks):
                # Dual delivery, same doctrine as the conversion lane: a
                # nudge text with no link pointing back at the button.
                try:
                    _prof = next((p for p in
                                  (_db.get_agent_profiles(active_only=True) or [])
                                  if p["agent_name"] == agent_name), None)
                    if _prof:
                        _dml = ""
                        try:
                            import json as _json2
                            _raw, _unused = _db.get_app_state("slack_dm_channels")
                            _chm = _json2.loads(_raw or "{}")
                            _ch = _chm.get(agent_name)
                            if not _ch:
                                _ch = _sl.open_dm_channel(sid)
                                if _ch:
                                    _chm[agent_name] = _ch
                                    _db.set_app_state("slack_dm_channels",
                                                      _json2.dumps(_chm))
                            if _ch:
                                _dml = (" Tap here, the button is on your "
                                        "screen: https://app.slack.com/"
                                        "client/T1LDE7TCG/%s" % _ch)
                        except Exception:
                            pass
                        _nudge = ("%s, shootout: a paid Realtor.com lead "
                                  "(%s%s) just went to several of you at "
                                  "once. First CLAIM in Slack takes it, %d "
                                  "minutes on the clock.%s"
                                  % (first, lead_first, where, mins, _dml))
                        _db.queue_agent_imessage(
                            agent_name, _prof.get("fub_user_id"),
                            _prof.get("phone") or _prof.get("fub_phone"),
                            _nudge, week_day="dispatch")
                except Exception as _de:
                    logger.warning("[SHOOTOUT] dual nudge failed for %s: %s",
                                   agent_name, _de)
                return True
    except Exception as e:
        logger.warning("[SHOOTOUT] slack offer failed for %s: %s",
                       agent_name, e)
    # ── Fallback: the standard queue with the accept URL ──────────────────
    profile = next((p for p in (_db.get_agent_profiles(active_only=True) or [])
                    if p["agent_name"] == agent_name), None)
    if not profile:
        return False
    msg = ("%s, shootout: a paid Realtor.com lead (%s%s) went to several "
           "agents at once. First tap takes it, %d minutes on the clock: %s"
           % (first, lead_first, where, mins, _accept_url(offer_token)))
    return bool(_db.queue_agent_imessage(
        agent_name, profile.get("fub_user_id"),
        profile.get("phone") or profile.get("fub_phone"), msg,
        week_day="dispatch"))


def make_shootout(person_id, lead_name, lead_city=None, notes=None,
                  audit=None, round_no=1, exclude=None, dry_run=False):
    """Realtor.com lane (Barry, Nov 2 2026): shared paid leads get
    SIMULTANEOUS offers, first CLAIM wins. Round 1 = the next
    REALTOR_SHOOTOUT_SIZE eligible priority agents (rotation-ordered, so
    bursts spread). Round 2 = every active non-gated agent who hasn't had
    this lead yet: a shared lead rotting on doctrine still loses the
    deal. The desk_claim handler voids the losing siblings on first
    claim. Daily claim caps deliberately don't apply here — the fastest
    hands win a shootout, fairness yields to a dying lead."""
    exclude = set(exclude or ())
    prior = {o["agent"] for o in _db.dispatch_person_state(person_id)}
    size = int(getattr(config, "REALTOR_SHOOTOUT_SIZE", 3))
    if round_no <= 1:
        cands = []
        for cand in eligible_agents(lead_city, audit=audit):
            if cand in prior or cand in exclude:
                continue
            try:
                if _db.count_open_dispatch_offers(cand) >= 2:
                    continue
                _t = (_db.dispatch_agent_today([cand]) or {}).get(cand) or {}
                if _t.get("on_appt_now"):
                    continue
            except Exception:
                pass
            cands.append(cand)
            if len(cands) >= size:
                break
    else:
        gated = set()
        try:
            gated = _db.get_onboarding_gated()
        except Exception:
            pass
        try:
            gated = gated | _db.get_lead_gated_agents()
        except Exception:
            pass
        roster = [p["agent_name"] for p in
                  (_db.get_agent_profiles(active_only=True) or [])]
        cands = [n for n in roster
                 if n not in prior and n not in exclude
                 and n not in gated and n not in _EXCLUDED]
    if dry_run:
        return [{"agent_name": n, "dry_run": True} for n in cands]
    if not cands:
        return []
    if round_no <= 1:
        # Advance past the whole trio so the next lead's round 1 starts
        # on fresh names (same per-offer doctrine as make_offer).
        for _ in cands:
            advance_rotation()
    mins = int(getattr(config, "REALTOR_OFFER_MINUTES", 2))
    if "Evidence" not in (notes or ""):
        try:
            ev = evidence_line(person_id)
            notes = ("%s\n%s" % (notes, ev)) if notes else ev
        except Exception:
            pass
    made = []
    for agent in cands:
        token = secrets.token_urlsafe(9)
        oid = _db.create_dispatch_offer("realtor", person_id, lead_name,
                                        lead_city, None, notes, agent,
                                        token, minutes=mins, hop=round_no)
        if not oid:
            continue
        try:
            _db.set_offer_route(oid, "shootout:r%d" % round_no)
        except Exception:
            pass
        sent = _notify_shootout(agent, token, lead_name, lead_city,
                                round_no, rivals=len(cands))
        logger.info("[SHOOTOUT] offer %s: %s -> %s (round %d, sent=%s)",
                    oid, lead_name, agent, round_no, sent)
        made.append({"offer_id": oid, "agent_name": agent, "token": token,
                     "sent": sent})
    return made


def make_offer(source, person_id, lead_name, lead_city=None, appt_time=None,
               notes=None, agent_name=None, hop=1, audit=None,
               lead_source=None):
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
                                  appt_time, notes, agent_name, hop, audit,
                                  lead_source=lead_source)
    finally:
        if _lock:
            _db.release_job_lock("desk_pick")
    return _made


def _ordered_candidates(lead_city, audit, source, person_id, hop,
                        lead_source=None):
    """Candidate order + which arm chose it. The experiment (Barry, Sep
    2026): half of hop-1 offers are merit-ordered (best lane converter
    among the ELIGIBLE gets first look — merit orders within the earned
    pool, never expands it), half stay pure rotation. route_path is stamped
    on every offer so the data can prove which arm books more appointments
    per offered lead. Cascade hops always use rotation order (prior agents
    are excluded anyway)."""
    base = eligible_agents(lead_city, audit=audit)
    # Nurture lane (Barry, Nov 6 2026: "she never gets leads routed to her
    # and it's causing me problems as a team"): the KPI-audit gate had
    # quietly shrunk the rotation to one passer plus grace hires, so
    # veterans like Tanya never saw an offer. Hot conversion lanes keep
    # the earned gate; nurture leads route across the FULL active roster
    # (minus roster-gated and excluded) in the same rotation order. The
    # on-ramp is open even when the audit says the week was slow.
    if source == "ai_nurture":
        gated = set()
        try:
            gated = _db.get_onboarding_gated()
        except Exception:
            pass
        try:
            gated = gated | _db.get_lead_gated_agents()
        except Exception:
            pass
        roster = [p["agent_name"] for p in
                  (_db.get_agent_profiles(active_only=True) or [])
                  if p.get("agent_name") not in _EXCLUDED
                  and p.get("agent_name") not in gated]
        extras = [n for n in roster if n not in base]
        try:
            raw, _ = _db.get_app_state("dispatch_rr")
            ptr = int(raw or 0)
        except Exception:
            ptr = 0
        if extras:
            ptr = ptr % len(extras)
            extras = extras[ptr:] + extras[:ptr]
        # Interleave so the full roster shares the lane instead of the
        # audit-passers always eating first.
        merged, a, b = [], base, extras
        for i in range(max(len(a), len(b))):
            if i < len(b):
                merged.append(b[i])
            if i < len(a):
                merged.append(a[i])
        base = [n for n in merged if n]
    if hop != 1 or not base:
        return base, "rotation"
    # Experiment concluded Oct 20 2026: rotation only, unless the config
    # flag re-opens the A/B.
    if getattr(config, "DESK_HOP1_ARM", "rotation") != "experiment":
        return base, "rotation"
    try:
        arm_merit = (int("".join(c for c in str(person_id)
                                 if c.isdigit()) or 0) % 2 == 0)
    except Exception:
        arm_merit = False
    if not arm_merit:
        return base, "rotation"
    try:
        import merit as _merit
        scorecard = _merit.get_cached_scorecard()
        lane = _merit.lane_for_lead(lead_source or "",
                                    source in ("fhalen", "ai_voice"))
        ranked = [n for n, _m in _merit.rank_lane_agents(scorecard, lane, base)]
        if ranked:
            # merit-ranked first, then everyone else in rotation order
            out = ranked + [n for n in base if n not in ranked]
            return out, "merit:%s" % (lane or "unmapped")
    except Exception as e:
        logger.warning("[DISPATCH] merit ordering failed: %s", e)
    return base, "rotation"


def _make_offer_inner(source, person_id, lead_name, lead_city, appt_time,
                      notes, agent_name, hop, audit, lead_source=None):
    route_path = "named"
    if not agent_name:
        prior = {o["agent"] for o in _db.dispatch_person_state(person_id)}
        cands, route_path = _ordered_candidates(lead_city, audit, source,
                                                person_id, hop,
                                                lead_source=lead_source)
        capped_fallback = []
        for cand in cands:
            if cand in prior:
                continue
            # Burst-day fairness (Barry, Oct 20 2026): an agent who already
            # claimed today's cap rotates behind everyone who hasn't. If
            # every candidate is capped, the cap yields — a waiting lead
            # always beats fairness.
            try:
                _cap = int(getattr(config, "DESK_DAILY_CLAIM_CAP", 0) or 0)
                if _cap and _db.count_agent_claims_today(cand) >= _cap:
                    capped_fallback.append(cand)
                    continue
            except Exception:
                pass
            # Open-claim cap: an agent already holding 2 live offers is
            # skipped, so one fast thumb can't vacuum a burst (Salma took
            # all 5 of a Ylopo tagging sweep on go-live day, Sep 2026).
            try:
                if _db.count_open_dispatch_offers(cand) >= 2:
                    continue
            except Exception:
                pass
            # Busy-aware: an agent sitting in an appointment right now
            # can't tap anything — skip, don't burn a silent 5-minute hop.
            try:
                _t = (_db.dispatch_agent_today([cand]) or {}).get(cand) or {}
                if _t.get("on_appt_now"):
                    continue
            except Exception:
                pass
            agent_name = cand
            break
        if not agent_name:
            for cand in capped_fallback:
                try:
                    if _db.count_open_dispatch_offers(cand) >= 2:
                        continue
                except Exception:
                    pass
                agent_name = cand
                break
    if not agent_name:
        return None
    # The evidence standard rides every offer: dated, verifiable, never
    # tags. Weak evidence goes out looking weak, on purpose.
    if hop == 1 and "Evidence" not in (notes or ""):
        try:
            ev = evidence_line(person_id)
            notes = ("%s\n%s" % (notes, ev)) if notes else ev
        except Exception:
            pass
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
    try:
        _db.set_offer_route(oid, route_path)
    except Exception:
        pass
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
