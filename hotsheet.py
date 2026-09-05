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


def _first(name):
    return (name or "").split()[0] if name else "there"


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
    """picks: [(lead_first, reason), ...] 1-3 entries. Barry voice, no dashes."""
    parts = ["%s (%s)" % (n, r) for n, r in picks]
    if len(picks) == 1:
        body = ("Morning %s. One lead needs you today: %s. "
                "Make that call before the day gets loud." % (agent_first, parts[0]))
    else:
        body = ("Morning %s. Your %d for today: %s. Start with %s."
                % (agent_first, len(picks), ", ".join(parts), picks[0][0]))
    return body


def run_hot_sheets(dry_run=False):
    """Build and queue the morning sheet for every eligible agent."""
    client = FUBClient()
    today = date.today()
    summary = {"queued": 0, "skipped_no_leads": 0, "skipped_claimed": 0,
               "skipped_no_phone": 0, "messages": []}

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
    for profile in profiles:
        agent = profile.get("agent_name")
        uid = profile.get("fub_user_id")
        if not agent or not uid or agent in _EXCLUDED:
            continue

        picks, used_pids = [], set()

        # 1. ISA transfers first (max 2)
        for t in (isa_by_agent.get(agent) or [])[:2]:
            if t["person_id"] in phoenix_pids or not t.get("name"):
                continue
            picks.append((_first(t["name"]), _lead_reason(client, {}, isa_days=t["days"])))
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
                if pid in used_pids or pid in phoenix_pids or not p.get("name"):
                    continue
                picks.append((_first(p.get("name")), _lead_reason(client, p)))
                used_pids.add(pid)

        if not picks:
            summary["skipped_no_leads"] += 1
            continue

        message = _compose(_first(agent), picks)
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
            try:
                _db.log_savebot(today.isoformat(), "hotsheet", agent,
                                len(picks), message[:500], "queued")
            except Exception:
                pass

    logger.info("[HOT SHEET] done: %s", {k: v for k, v in summary.items()
                                         if k != "messages"})
    return summary
