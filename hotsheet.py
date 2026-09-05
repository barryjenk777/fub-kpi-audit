"""
hotsheet.py — the morning "your 3 for today" text, per agent.

The priority engine tags leads all day; this is the push that stops agents
having to remember to look. One text, weekday mornings at 8:15am ET, three
leads max, each with the real reason it made the list. Deterministic (no
LLM): reasons come from signals we can prove.

Ranking per agent:
  1. Fresh ISA transfers with no first call logged (max 2) — these cool by
     the hour and Fhalen already qualified them.
  2. Their LeadStream-tagged leads by score, reason drawn from the lead's
     recent site events (viewed/saved property counts).

Leads Phoenix owner-alerted this morning are excluded (they already got
their own text at 7:30). Delivery rides the standard queue: iMessage via
the Mac poller, email for Android agents. claim_once guards one sheet per
agent per day against double-schedules and retries.
"""

import logging
from datetime import date, datetime, timedelta, timezone

import config
import db as _db
from fub_client import FUBClient

logger = logging.getLogger(__name__)

_EXCLUDED = set(getattr(config, "EXCLUDED_USERS", [])) \
    | set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))

_PEOPLE_FIELDS = "id,name,assignedUserId,tags,stage,lastCommunication,customLeadStreamScore"


_JUNK_NAMES = {"no", "yes", "unknown", "test", "n/a", "na", "none", "null",
               "lead", "buyer", "seller", "new"}


def _first(name):
    return (name or "").split()[0] if name else "there"


def _usable_name(name):
    """A lead we can name in a text. Kills junk FUB records (a lead literally
    named 'No' produced 'Start with No.' in the first dry run)."""
    f = _first(name)
    return (len(f) >= 2 and f.lower() not in _JUNK_NAMES
            and not f.isdigit() and f.replace("-", "").isalpha())


def _lead_reason(client, person, isa_days=None):
    """One provable reason this lead is on today's sheet."""
    if isa_days is not None:
        if isa_days <= 1:
            return "Fhalen handed them to you yesterday, no call logged yet"
        return "Fhalen's transfer from %d days ago, still no call logged" % isa_days
    pid = person.get("id")
    try:
        events = client.get_events_for_person(pid, days=7) or []
    except Exception:
        events = []
    views = sum(1 for e in events if "Viewed" in (e.get("type") or ""))
    saves = sum(1 for e in events if "Saved" in (e.get("type") or ""))
    if saves:
        return "saved %d propert%s this week" % (saves, "ies" if saves > 1 else "y")
    if views >= 3:
        return "viewed %d homes this week" % views
    if views:
        return "back on the site this week"
    score = person.get("customLeadStreamScore")
    if score:
        return "top score on your list (%s)" % int(score)
    return "highest priority on your list"


def _compose(agent_first, picks):
    """picks: [(lead_first, reason), ...] 1-3 entries. Barry voice, no dashes.
    Repeated identical reasons collapse so the text reads human, not robotic
    ('Jessica (top score), Edward (top score), Matthew (top score)' becomes
    'Jessica, Edward and Matthew, the top scores on your list')."""
    if len(picks) == 1:
        n, r = picks[0]
        return ("Morning %s. One lead needs you today: %s (%s). "
                "Make that call before the day gets loud." % (agent_first, n, r))

    reasons = [r for _, r in picks]
    names = [n for n, _ in picks]
    if len(set(reasons)) == 1 and reasons[0].startswith(("top score", "highest priority")):
        listed = "%s and %s" % (", ".join(names[:-1]), names[-1])
        body = ("Morning %s. Your %d for today: %s, the top scores on your "
                "list right now. Start with %s."
                % (agent_first, len(picks), listed, names[0]))
    else:
        seen, parts = set(), []
        for n, r in picks:
            if r in seen and r.startswith(("top score", "highest priority")):
                parts.append(n)
            else:
                parts.append("%s (%s)" % (n, r))
                seen.add(r)
        body = ("Morning %s. Your %d for today: %s. Start with %s."
                % (agent_first, len(picks), ", ".join(parts), names[0]))
    return body


def _verify_yesterday(client, uid_by_agent, today):
    """The accountability loop: check every unverified pick from prior sheets
    against actual FUB call logs. Returns (scoreboard, uncalled) where
    scoreboard = {agent: (called, total)} for the most recent prior sheet and
    uncalled = {agent: [(person_id, lead_name), ...]} for escalation."""
    scoreboard, uncalled = {}, {}
    for pick in _db.get_unchecked_hotsheet_picks(today):
        agent = pick["agent_name"]
        uid = uid_by_agent.get(agent)
        called = False
        if uid:
            try:
                since = datetime.combine(pick["sheet_date"], datetime.min.time(),
                                         tzinfo=timezone.utc)
                calls = client.get_calls(person_id=pick["person_id"], since=since) or []
                called = any(not c.get("isIncoming", True) and c.get("userId") == uid
                             for c in calls)
            except Exception as e:
                logger.warning("[HOT SHEET] call check failed for %s: %s",
                               pick["person_id"], e)
        _db.mark_hotsheet_pick(pick["id"], called)
        c, t = scoreboard.get(agent, (0, 0))
        scoreboard[agent] = (c + (1 if called else 0), t + 1)
        if not called:
            uncalled.setdefault(agent, []).append((pick["person_id"], pick["lead_name"]))
    return scoreboard, uncalled


def _scoreboard_line(agent_first, score):
    called, total = score
    if total == 0:
        return ""
    if called == total:
        return ("You called all %d from yesterday's sheet. That is the standard. "
                % total)
    if called == 0:
        return "Yesterday's %d went uncalled. They are back on your list. " % total
    return "Yesterday's %d: you called %d. " % (total, called)


def run_hot_sheets(dry_run=False):
    """Build and queue the morning sheet for every eligible agent."""
    client = FUBClient()
    today = date.today()
    summary = {"queued": 0, "skipped_no_leads": 0, "skipped_claimed": 0,
               "skipped_no_phone": 0, "verified_yesterday": 0, "messages": []}

    # Leads Phoenix already texted about this morning
    phoenix_pids = set()
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT person_id FROM phoenix_log
                    WHERE run_date = %s AND status IN ('owner_alerted', 'assigned')
                """, (today,))
                phoenix_pids = {str(r[0]) for r in cur.fetchall()}
    except Exception as e:
        logger.warning("[HOT SHEET] phoenix dedupe read failed: %s", e)

    # Fresh unworked ISA transfers, grouped by agent
    isa_by_agent = {}
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT agent_name, person_id, lead_name,
                           EXTRACT(day FROM NOW() - transfer_date)::int
                    FROM isa_transfers
                    WHERE first_call_at IS NULL
                      AND transfer_date >= NOW() - INTERVAL '7 days'
                      AND agent_name IS NOT NULL
                    ORDER BY transfer_date DESC
                """)
                for agent, pid, lead_name, days in cur.fetchall():
                    isa_by_agent.setdefault(agent, []).append(
                        {"person_id": str(pid), "name": lead_name, "days": days})
    except Exception as e:
        logger.warning("[HOT SHEET] isa read failed: %s", e)

    profiles = _db.get_agent_profiles(active_only=True) or []
    uid_by_agent = {p.get("agent_name"): p.get("fub_user_id") for p in profiles}

    # The loop: verify prior sheets against real call logs (skip in dry runs
    # so previews never consume the one-shot verification marks)
    scoreboard, uncalled = ({}, {}) if dry_run else _verify_yesterday(
        client, uid_by_agent, today)
    summary["verified_yesterday"] = sum(t for _, t in scoreboard.values())

    for profile in profiles:
        agent = profile.get("agent_name")
        uid = profile.get("fub_user_id")
        if not agent or not uid or agent in _EXCLUDED:
            continue

        picks, used_pids = [], set()

        # 0. Escalations first: yesterday's uncalled picks come back on top
        for pid, lead_name in (uncalled.get(agent) or [])[:2]:
            if str(pid) in phoenix_pids or not _usable_name(lead_name):
                continue
            picks.append((str(pid), _first(lead_name),
                          "second day on your sheet, still no call"))
            used_pids.add(str(pid))

        # 1. ISA transfers next (max 2)
        for t in (isa_by_agent.get(agent) or [])[:2]:
            if len(picks) >= 3:
                break
            if t["person_id"] in phoenix_pids or t["person_id"] in used_pids \
                    or not _usable_name(t.get("name")):
                continue
            picks.append((t["person_id"], _first(t["name"]),
                          _lead_reason(client, {}, isa_days=t["days"])))
            used_pids.add(t["person_id"])

        # 2. LeadStream list by score
        if len(picks) < 3:
            try:
                leads = client._get_paginated("people", {
                    "limit": 50, "assignedUserId": uid, "tag": config.LEADSTREAM_TAG,
                    "fields": _PEOPLE_FIELDS}, max_pages=1) or []
            except Exception as e:
                logger.warning("[HOT SHEET] leads fetch failed for %s: %s", agent, e)
                leads = []
            leads.sort(key=lambda p: float(p.get("customLeadStreamScore") or 0),
                       reverse=True)
            for p in leads:
                if len(picks) >= 3:
                    break
                pid = str(p.get("id"))
                if pid in used_pids or pid in phoenix_pids or not _usable_name(p.get("name")):
                    continue
                picks.append((pid, _first(p.get("name")), _lead_reason(client, p)))
                used_pids.add(pid)

        if not picks:
            summary["skipped_no_leads"] += 1
            continue

        message = (_scoreboard_line(_first(agent), scoreboard.get(agent, (0, 0)))
                   + _compose(_first(agent), [(n, r) for _, n, r in picks]))
        summary["messages"].append({"agent": agent, "message": message})

        if dry_run:
            continue
        if not _db.claim_once("hotsheet_%s_%s" % (today.isoformat(), agent)):
            summary["skipped_claimed"] += 1
            continue
        phone = profile.get("phone") or profile.get("fub_phone")
        if not phone:
            summary["skipped_no_phone"] += 1
            continue
        rid = _db.queue_agent_imessage(agent, uid, phone, message,
                                       week_day="hotsheet")
        if rid:
            summary["queued"] += 1
            for pid, lead_name, reason in picks:
                _db.log_hotsheet_pick(today, agent, pid, lead_name, reason)
            try:
                _db.log_savebot(today.isoformat(), "hotsheet", agent,
                                len(picks), message[:500], "queued")
            except Exception:
                pass

    logger.info("[HOT SHEET] done: %s", {k: v for k, v in summary.items()
                                         if k != "messages"})
    return summary
