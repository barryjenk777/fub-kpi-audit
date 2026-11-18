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


def is_sphere(lead):
    src = (lead.get("source") or "").strip().lower()
    tags = " ".join(str(t).lower() for t in (lead.get("tags") or []))
    return ("sphere" in src or src in ("soi", "s.o.i.")
            or "sphere" in tags or " soi" in " " + tags)


def stop_tag(lead):
    """The first stop tag on the lead, or None."""
    have = {str(t).strip().upper() for t in (lead.get("tags") or [])}
    hit = sorted(have & STOP_TAGS)
    return hit[0] if hit else None


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
           proof_hours=None, check_proof=True):
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
    tag = stop_tag(lead)
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
