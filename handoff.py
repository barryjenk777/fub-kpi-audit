"""
handoff.py — the Instant Handoff Protocol for ISA transfers.

Fhalen's transfers are the team's most expensive leads (25-40% referral)
and the most perishable. Verified data (Sep 2026): only 12% got an agent
call within 24 hours. This module is the fix — a ladder where automation
handles every incident and Barry's voice is reserved for rung 2:

  Rung 1 (T+0):  scan every 10 minutes for new ISA_TRANSFER_FRESH leads;
                 the agent gets an instant "call in the next 10 minutes"
                 text the moment a transfer lands.
  Rung 2 (T+4h): still no call logged (webhook-verified first_call_at) →
                 a Barry-voice accountability text. Gated behind
                 HANDOFF_RUNG2_ENABLED until Barry signs off the template.
  Rung 3 (next morning): hot sheet escalation — already live.
  Rung 4 (pattern): repeat offenders surface in Pulse and coaching packs.

claim_once guards every send. Delivery rides the standard queue (Android
agents get email). Deterministic; no LLM.
"""

import logging
from datetime import datetime, timedelta, timezone

import config
import db as _db
from fub_client import FUBClient

logger = logging.getLogger(__name__)

_EXCLUDED = set(getattr(config, "EXCLUDED_USERS", [])) \
    | set(getattr(config, "COACHING_TEXT_EXCLUDED_AGENTS", set()))


def _et_now():
    h = -4 if 3 <= datetime.now(timezone.utc).month <= 10 else -5
    return datetime.now(timezone(timedelta(hours=h)))


def _first(name):
    return (name or "").split()[0] if name else "there"


def _profiles_by_name():
    return {p.get("agent_name"): p for p in (_db.get_agent_profiles(active_only=True) or [])}


def _queue(profile, agent, message):
    phone = profile.get("phone") or profile.get("fub_phone")
    if not phone:
        logger.info("[HANDOFF] no phone for %s, skipping", agent)
        return False
    return bool(_db.queue_agent_imessage(agent, profile.get("fub_user_id"),
                                         phone, message, week_day="handoff"))


def run_handoff_scan(dry_run=False):
    """One pass: detect new transfers (rung 1) and stale unworked ones
    (rung 2). Called every 10 minutes during working hours."""
    client = FUBClient()
    profiles = _profiles_by_name()
    now_et = _et_now()
    summary = {"new_transfers": 0, "rung1_sent": 0, "rung2_sent": 0,
               "messages": []}

    # ── Rung 1: new ISA_TRANSFER_FRESH leads ────────────────────────────────
    try:
        fresh = client.get_people(tag=config.ISA_TRANSFER_FRESH_TAG, limit=200) or []
    except Exception as e:
        logger.warning("[HANDOFF] FUB scan failed: %s", e)
        fresh = []

    # Seed pass: the very first scan records every already-tagged transfer
    # WITHOUT alerting — otherwise week-old transfers would get a false
    # "just handed you, live and warm" text. Alerts start from scan #2.
    seeding = (not dry_run) and _db.claim_once("handoff_seed_v1")
    if seeding:
        summary["seeding"] = True
    for person in fresh:
        pid = str(person.get("id") or "")
        agent = (person.get("assignedTo") or "").strip()
        lead_name = (person.get("name") or "").strip()
        if not pid or not agent or agent in _EXCLUDED:
            continue
        is_new = _db.record_isa_transfer(pid, lead_name=lead_name, agent_name=agent)
        if not is_new:
            continue
        summary["new_transfers"] += 1
        profile = profiles.get(agent)
        if not profile or not lead_name:
            continue
        who = _first(lead_name) if lead_name else "a new lead"
        msg = ("%s, Fhalen just handed you %s, live and warm. Call in the "
               "next 10 minutes while their phone is still in their hand. "
               "Speed is the whole game on these." % (_first(agent), who))
        summary["messages"].append({"rung": 1, "agent": agent, "message": msg})
        if dry_run or seeding:
            continue
        if _db.claim_once("handoff1_%s" % pid) and _queue(profile, agent, msg):
            summary["rung1_sent"] += 1

    # ── Rung 2: 4+ hours old, still no verified call ────────────────────────
    # Only during the working day (9am-8pm ET) so the nudge lands when a call
    # is actually possible. Gated until Barry approves the template.
    if not getattr(config, "HANDOFF_RUNG2_ENABLED", False):
        return summary
    if not (9 <= now_et.hour < 20):
        return summary
    rows = []
    try:
        with _db.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT person_id, lead_name, agent_name, transfer_date
                    FROM isa_transfers
                    WHERE first_call_at IS NULL
                      AND transfer_date <= NOW() - INTERVAL '4 hours'
                      AND transfer_date >= NOW() - INTERVAL '30 hours'
                      AND agent_name IS NOT NULL AND lead_name IS NOT NULL
                """)
                rows = cur.fetchall()
    except Exception as e:
        logger.warning("[HANDOFF] rung2 query failed: %s", e)
    for pid, lead_name, agent, tdate in rows:
        if agent in _EXCLUDED:
            continue
        profile = profiles.get(agent)
        if not profile:
            continue
        when = "this morning" if (tdate and tdate.astimezone(
            now_et.tzinfo).date() == now_et.date() and tdate.astimezone(
            now_et.tzinfo).hour < 12) else "earlier today" if (
            tdate and tdate.astimezone(now_et.tzinfo).date() == now_et.date()) \
            else "yesterday"
        msg = ("%s, Fhalen set you up with %s %s. I'm not seeing a call in "
               "FUB. If something's in the way, tell me. If not, evenings are "
               "when they answer. Call before you close the laptop."
               % (_first(agent), _first(lead_name), when))
        summary["messages"].append({"rung": 2, "agent": agent, "message": msg})
        if dry_run:
            continue
        if _db.claim_once("handoff2_%s" % pid) and _queue(profile, agent, msg):
            summary["rung2_sent"] += 1

    return summary
