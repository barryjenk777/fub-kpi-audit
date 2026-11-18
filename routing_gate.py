"""The front door (Barry, Nov 18 2026).

Nine lanes hand leads to agents and each grew its own copy of the rules,
so a rule made in one lane leaked in the others (offers at midnight, a
lead taken from the agent who had already talked to her). This module is
the one place those rules live. Every lane asks the same questions, in
the same order, and gets back one answer with a plain reason:

    1. Is this lead allowed to route?      (stage, opt out, sphere)
    2. Is it real and recent?              (dated proof, inside the window)
    3. Does someone already have a claim?  (owner, or an agent who talked)
    4. Is it the right time?               (9pm to 8am holds)

Question 5, who is next, stays with the offer engine. Pure functions, no
network and no database, so every rule is covered by a test.
"""
import re
from datetime import datetime, timezone

QUIET_START_HOUR = 21
QUIET_END_HOUR = 8

# A lead in one of these stages is never offered to anyone.
DEAD_STAGES = {"trash", "contact"}
# These belong to the agent doing the deal; never offered to anyone else.
DEAL_STAGES = {"closed", "under contract", "pending", "past client"}

# The lead told us to stop, or the number is not theirs.
STOP_TAGS = {
    "DO_NOT_CALL", "NOT_INTERESTED", "AI_OPT_OUT", "AI_NOT_INTERESTED",
    "NO_MARKETING", "WRONG_NUMBER", "DISCONNECTED_NUMBER",
}

# How old the newest dated proof may be, in hours. None = this lane is
# for leads that are already waiting, so age is the point, not a problem.
FRESH_HOURS = {
    "ai_text": 48, "ai_voice": 48, "blue_text": 48, "fhalen": 48,
    "realtor": 48, "callingly": 48,
    "ai_nurture": 14 * 24,
    "reassign": None, "recovery": None,
}

ROUTE, HOLD, BLOCK, KEEP = "route", "hold", "block", "keep_owner"


def is_quiet(now_et):
    """True from 9pm to 8am Eastern."""
    return now_et.hour >= QUIET_START_HOUR or now_et.hour < QUIET_END_HOUR


def working_hours_between(start_et, end_et):
    """Hours between two Eastern times, not counting 9pm to 8am (Barry,
    Nov 18 2026: the 24 hour clock pauses overnight, because an agent
    cannot call a lead then). Both arguments are Eastern wall-clock
    datetimes."""
    from datetime import timedelta
    if end_et <= start_et:
        return 0.0
    total = 0.0
    day = start_et.replace(hour=0, minute=0, second=0, microsecond=0)
    while day <= end_et:
        opens = day.replace(hour=QUIET_END_HOUR)
        closes = day.replace(hour=QUIET_START_HOUR)
        lo, hi = max(opens, start_et), min(closes, end_et)
        if hi > lo:
            total += (hi - lo).total_seconds() / 3600.0
        day += timedelta(days=1)
    return round(total, 2)


BACK_MIN_OFFERS = 10      # enough offers to judge fairly
BACK_MIN_RATE = 0.10      # claimed fewer than 1 in 10
BACK_RECENT_DAYS = 7      # one claim in the last week earns the spot back


def back_of_line(stats, now, min_offers=BACK_MIN_OFFERS,
                 min_rate=BACK_MIN_RATE, recent_days=BACK_RECENT_DAYS):
    """Agents who go to the back of the line (Barry, Nov 18 2026). An
    agent with at least 10 offers in 30 days who claimed fewer than 1 in
    10, and has not claimed anything in the last 7 days. They still get
    offers, after everyone else. One claim brings them straight back.
    Returns {agent: plain reason}."""
    from datetime import timedelta
    out = {}
    for name, s in (stats or {}).items():
        ignored, claimed = int(s.get("ignored") or 0), int(s.get("claimed") or 0)
        seen = ignored + claimed
        if seen < min_offers or claimed / float(seen) >= min_rate:
            continue
        last = s.get("last_claim")
        if last is not None and now - last <= timedelta(days=recent_days):
            continue
        out[name] = ("claimed %d of %d offers in the last 30 days"
                     % (claimed, seen))
    return out


def order_with_back(cands, back):
    """Same order, with back of the line agents moved behind the rest."""
    front = [c for c in cands if c not in back]
    return front + [c for c in cands if c in back]


def is_sphere(lead):
    src = (lead.get("source") or "").strip().lower()
    tags = " ".join(str(t).lower() for t in (lead.get("tags") or []))
    return ("sphere" in src or src in ("soi", "s.o.i.")
            or "sphere" in tags or " soi" in " " + tags)


# A lead who said no to the AI and then picks up the phone and calls us
# is open again (Barry, Nov 18 2026). These never reopen on their own.
HARD_STOP_TAGS = {"DO_NOT_CALL", "NO_MARKETING", "WRONG_NUMBER",
                  "DISCONNECTED_NUMBER"}
CALLED_IN_SECS = 30
CALLED_IN_DAYS = 14


def stop_tag(lead, called_in=False):
    """The first stop tag that still applies, or None. A lead who called
    in after opting out of the AI is only held by the hard stops."""
    have = {str(t).strip().upper() for t in (lead.get("tags") or [])}
    hit = sorted(have & (HARD_STOP_TAGS if called_in else STOP_TAGS))
    return hit[0] if hit else None


def lead_called_in(calls, now=None, days=CALLED_IN_DAYS,
                   min_secs=CALLED_IN_SECS):
    """True when the lead phoned us and connected, inside the window.
    `calls` are Follow Up Boss call records."""
    from datetime import timedelta
    now = now or datetime.now(timezone.utc)
    for c in calls or []:
        try:
            if not c.get("isIncoming") \
                    or int(c.get("duration") or 0) < min_secs:
                continue
            at = datetime.strptime((c.get("created") or "")[:19],
                                   "%Y-%m-%dT%H:%M:%S").replace(
                tzinfo=timezone.utc)
        except (TypeError, ValueError):
            continue
        if timedelta(0) <= now - at <= timedelta(days=days):
            return True
    return False


_DATE = re.compile(r"(20\d\d-\d\d-\d\d)")


def proof_age_hours(notes, now=None):
    """Hours since the newest date on the offer's Evidence line. The line
    carries days, not minutes, so a lead with proof dated today reads 0.
    None when the offer carries no dated proof."""
    line = ""
    for ln in (notes or "").splitlines():
        if ln.strip().startswith("Evidence"):
            line = ln
    dates = _DATE.findall(line)
    if not dates:
        return None
    now = now or datetime.now(timezone.utc)
    try:
        newest = max(datetime.strptime(d, "%Y-%m-%d").date() for d in dates)
    except ValueError:
        return None
    return max(0, (now.date() - newest).days) * 24


def _answer(action, question, reason, agent=None):
    return {"action": action, "question": question, "reason": reason,
            "agent": agent}


def decide(lane, lead, now_et, owner_is_working=False, talked_agent=None,
           proof_hours=None, check_proof=True, called_in=False):
    """One answer for one lead in one lane.

    lead            the Follow Up Boss person (stage, source, tags,
                    assignedTo).
    owner_is_working  True when assignedTo is an active agent who works
                    leads (not the pond, Barry, Fhalen or a paused agent).
    talked_agent    a working agent who already had a real conversation
                    with this lead, if any.
    proof_hours     age of the newest dated proof, None when there is none.
    """
    stage = (lead.get("stage") or "").strip().lower()
    owner = (lead.get("assignedTo") or "").strip()

    # 1. Allowed to route at all?
    if stage in DEAD_STAGES:
        return _answer(BLOCK, 1, "stage is %s" % stage.title())
    if stage in DEAL_STAGES:
        return _answer(BLOCK, 1, "stage is %s, the deal has an agent"
                       % stage.title())
    tag = stop_tag(lead, called_in=called_in)
    if tag:
        return _answer(BLOCK, 1, "the lead asked us to stop (%s)" % tag)
    if lane == "reassign" and is_sphere(lead):
        return _answer(BLOCK, 1, "sphere leads are never reassigned")

    # 2. Real and recent?
    window = FRESH_HOURS.get(lane, 48)
    if check_proof and window is not None:
        if proof_hours is None:
            return _answer(BLOCK, 2, "no dated proof of a conversation")
        if proof_hours > window:
            return _answer(BLOCK, 2, "newest proof is %d days old"
                           % (proof_hours // 24))

    # 3. Does someone already have a claim on it?
    if talked_agent and talked_agent != owner:
        return _answer(KEEP, 3, "%s already talked to this lead"
                       % talked_agent, agent=talked_agent)
    if owner_is_working:
        if lane != "reassign":
            return _answer(KEEP, 3, "%s already owns this lead" % owner,
                           agent=owner)
        if talked_agent == owner:
            return _answer(KEEP, 3, "%s owns this lead and has talked to "
                           "them" % owner, agent=owner)
    elif lane == "reassign":
        return _answer(BLOCK, 3, "nobody owns this lead, so there is "
                       "nothing to reassign")

    # 4. The right time?
    if is_quiet(now_et):
        return _answer(HOLD, 4, "it is between 9pm and 8am, hold for 8am")

    return _answer(ROUTE, 5, "clear to offer")
