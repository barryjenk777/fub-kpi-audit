"""Realtor.com attempt ladder (Barry, Nov 4 2026).

The shootout enforces call #1 (5 min nudge, 10 min yank). This module owns
calls 2 through 10: a paid SHARED lead is competed for by agents outside
the team, and the industry numbers say the average agent quits at 1.5
attempts while reaching most prospects takes 6 to 8. The ladder is the
minimum attempt count by day since claim, until two way contact:

    day 0           2 calls (the claim call plus one more, with a text)
    day 1           4
    day 2           5
    days 3 to 4     6
    days 5 to 6     7
    days 7 to 9     8
    days 10 to 13   9
    day 14          10, then the ladder closes to nurture

Contact means the lead actually engaged: an inbound text, an inbound call
that connected, or any call 120 seconds or longer. Ylopo AI texts never
count as attempts; AI keeps a lead warm, only the agent wins it.

Surfaces (no new habit for the agent):
  - hot sheet pin: active ladder leads become pick #1 on the 8:15 sheet
  - 7pm sweep (app.scheduled_realtor_ladder_sweep): one Slack DM per lead
    per day when the agent is behind
  - Friday ROI email to Barry (app.scheduled_realtor_roi_email)
"""

import logging
from datetime import datetime, timedelta, timezone

import db as _db

logger = logging.getLogger(__name__)

LADDER_WINDOW_DAYS = 14
LADDER_TOTAL_CALLS = 10
# A connected conversation: at or over this many seconds counts as contact.
CONTACT_CALL_SECS = 120
# An inbound call this long means the lead reached a human (shorter inbound
# is usually a hang up or a ring to voicemail).
CONTACT_INBOUND_SECS = 30
LEAD_COST_USD = 80


# ── After hours (Barry, Nov 17 2026) ─────────────────────────────────────
# No calls or texts to a lead between 9pm and 8am Eastern. A lead that
# registers in that window skips the phone ring, goes to the whole team on
# Slack so one agent owns it overnight, and the claimer is told to hold
# until 8am. Everything here is pure so it tests without a clock or FUB.
QUIET_START_HOUR = 21
QUIET_END_HOUR = 8
# Night offers carry this at the front of their notes; it is how every
# later step (claim copy, round advance, the 8am release) knows the offer
# was made after hours.
NIGHT_NOTE = "After hours"


def is_quiet(now_et, start=QUIET_START_HOUR, end=QUIET_END_HOUR):
    """True between 9pm and 8am Eastern (start inclusive, end exclusive)."""
    return now_et.hour >= start or now_et.hour < end


def last_open(now_et, end=QUIET_END_HOUR):
    """The most recent 8:00am Eastern at or before now."""
    opened = now_et.replace(hour=end, minute=0, second=0, microsecond=0)
    if now_et < opened:
        opened -= timedelta(days=1)
    return opened


def minutes_until_open(now_et, start=QUIET_START_HOUR, end=QUIET_END_HOUR):
    """Whole minutes from now until calling opens (0 when already open)."""
    if not is_quiet(now_et, start, end):
        return 0
    nxt = last_open(now_et, end) + timedelta(days=1)
    return max(1, int((nxt - now_et).total_seconds() // 60))


def call_clock(age_min, minutes_since_open):
    """The claim-to-call clock never runs while calling is closed.
    Returns (minutes on the clock, held_overnight). A claim older than
    this morning's 8am was held overnight, so its clock starts at 8am."""
    age = int(age_min or 0)
    since = int(minutes_since_open or 0)
    if age > since:
        return since, True
    return age, False


def is_night_offer(notes):
    return (notes or "").startswith(NIGHT_NOTE)


def answer_keeps_owner(fub_owner, answerer, owner_is_dark):
    """A phone-ring answer never takes a lead a working agent already
    holds. Returns that owner's name when the answer must not reassign."""
    owner = (fub_owner or "").strip()
    if not owner or owner == (answerer or "").strip() or owner_is_dark:
        return None
    return owner


def owner_blocks_claim(prior_owner, claimer, owner_is_dark,
                       released_claimers=()):
    """The accept re-check's one decision: does the name FUB shows on the
    lead stop this claim? A working agent other than the claimer blocks
    it. A yanked claimer does not: the yank released the claim in our
    ledger but FUB still carries their name, and without this the reopened
    shootout could never change hands (Nov 16 2026). Pure, so it is
    testable without FUB or a database."""
    owner = (prior_owner or "").strip()
    if not owner or owner == claimer or owner_is_dark:
        return False
    return owner not in set(released_claimers or ())


def due_calls(day_no):
    """Cumulative call attempts required by end of day `day_no` since claim."""
    if day_no <= 0:
        return 2
    if day_no == 1:
        return 4
    if day_no == 2:
        return 5
    if day_no <= 4:
        return 6
    if day_no <= 6:
        return 7
    if day_no <= 9:
        return 8
    if day_no <= 13:
        return 9
    return LADDER_TOTAL_CALLS


def get_claims(days=30):
    """Accepted Realtor.com shootout claims, newest first. terminal=TRUE rows
    (yanked claims) are excluded; a yank already reopened the lead."""
    if not _db.is_available():
        return []
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, person_id, lead_name, lead_city, agent_name,
                           offered_at, accepted_at, first_call_at
                    FROM dispatch_offers
                    WHERE source = 'realtor' AND accepted_at IS NOT NULL
                      AND terminal = FALSE
                      AND accepted_at >= NOW() - make_interval(days => %s)
                    ORDER BY accepted_at DESC
                """, (int(days),))
                keys = ("id", "person_id", "lead_name", "lead_city",
                        "agent_name", "offered_at", "accepted_at",
                        "first_call_at")
                return [dict(zip(keys, r)) for r in cur.fetchall()]
    except Exception as e:
        logger.warning("[LADDER] get_claims failed: %s", e)
        return []


def _fetch_texts(client, person_id):
    try:
        data = client._request("GET", "textMessages",
                               params={"personId": person_id, "limit": 100})
        return data.get("textmessages") or data.get("textMessages") or []
    except Exception as e:
        logger.warning("[LADDER] texts fetch failed for %s: %s", person_id, e)
        return []


def ladder_state(client, person_id, accepted_at):
    """Compute one lead's ladder position from real FUB activity.

    attempts = ALL outbound calls on the person since the claim (team dials
    count; the lead experiences every ring, and that is the number that
    converts). Contact = inbound text, connected inbound call, or any call
    at or over CONTACT_CALL_SECS."""
    now = datetime.now(timezone.utc)
    if accepted_at.tzinfo is None:
        accepted_at = accepted_at.replace(tzinfo=timezone.utc)
    since = accepted_at - timedelta(minutes=10)
    attempts, contacted, last_out = 0, False, None
    try:
        calls = client.get_calls(person_id=person_id, since=since) or []
    except Exception as e:
        logger.warning("[LADDER] calls fetch failed for %s: %s", person_id, e)
        calls = []
    for c in calls:
        dur = int(c.get("duration") or 0)
        if c.get("isIncoming"):
            if dur >= CONTACT_INBOUND_SECS:
                contacted = True
        else:
            attempts += 1
            created = c.get("created") or ""
            if not last_out or created > last_out:
                last_out = created
        if dur >= CONTACT_CALL_SECS:
            contacted = True
    if not contacted:
        for t in _fetch_texts(client, person_id):
            if not t.get("isIncoming"):
                continue
            created = (t.get("created") or "")
            try:
                tdt = datetime.fromisoformat(created.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue
            if tdt >= since:
                contacted = True
                break
    # Day boundaries live in ET, where the agents live: a 9:33pm claim is
    # on day 1 the next morning, not still on day 0 until 7pm.
    from zoneinfo import ZoneInfo
    _et = ZoneInfo("America/New_York")
    day_no = (now.astimezone(_et).date()
              - accepted_at.astimezone(_et).date()).days
    due = due_calls(day_no)
    return {
        "attempts": attempts,
        "contacted": contacted,
        "day_no": day_no,
        "due": due,
        "behind": max(due - attempts, 0),
        "last_out": last_out,
        "window_open": day_no <= LADDER_WINDOW_DAYS,
    }


def active_ladders(client, days=LADDER_WINDOW_DAYS + 1):
    """Claims still inside the 14 day window where contact has NOT happened,
    enriched with ladder state. Skips leads FUB no longer shows assigned to
    the claiming agent (a reassignment ends the agent's ladder)."""
    out = []
    for claim in get_claims(days=days):
        st = ladder_state(client, claim["person_id"], claim["accepted_at"])
        if not st["window_open"] or st["contacted"]:
            continue
        try:
            person = client.get_person(claim["person_id"]) or {}
            owner = (person.get("assignedTo") or "").strip()
            if owner and owner != claim["agent_name"]:
                continue
        except Exception:
            pass
        claim.update(st)
        out.append(claim)
    return out
